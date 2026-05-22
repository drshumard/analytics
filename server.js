// ============================================================================
// Dr Shumard Analytics — Production API Server
// ============================================================================

import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import { createClient } from '@supabase/supabase-js';
import crypto from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import cron from 'node-cron';
import { syncFacebookSpend, fetchFacebookSpend, fetchFacebookInsights, writeSpendToSupabase, writeInsightsToSupabase } from './fb-sync.js';
import 'dotenv/config';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ─── Config ──────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.error('❌ Missing SUPABASE_URL or SUPABASE_SERVICE_KEY');
    process.exit(1);
}
if (!API_KEY) {
    console.error('❌ Missing API_KEY — webhooks will be unprotected');
    process.exit(1);
}

// ─── Supabase Clients ────────────────────────────────────────────────────────
// Multi-tenant: the `analytics` funnel lives in schema `public`, the `native`
// funnel in schema `native`. Each request resolves to one funnel and gets a
// schema-scoped client via clientFor(req.funnel).
//
// supabasePublic is used for cross-funnel concerns: auth.getUser, the shared
// api_keys table, and user_funnel_access. It always targets `public`.
const FUNNEL_TO_SCHEMA = { analytics: 'public', native: 'native' };
const ALLOWED_FUNNELS = Object.keys(FUNNEL_TO_SCHEMA);

// Per-funnel branding used by the AI insights chat. Each entry shapes the
// system prompt so the model knows which business/channel it's analyzing.
const FUNNEL_BRANDS = {
    analytics: { brand: 'Dr Shumard', context: 'a medical practice', funnelName: 'Main (FB Ads) Funnel' },
    native:    { brand: 'Dr Shumard', context: 'a medical practice', funnelName: 'Native Ads Funnel' },
};

const supabasePublic = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const _funnelClients = new Map();
function clientFor(funnel) {
    const schema = FUNNEL_TO_SCHEMA[funnel];
    if (!schema) throw new Error(`Unknown funnel: ${funnel}`);
    if (!_funnelClients.has(funnel)) {
        _funnelClients.set(funnel, createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
            db: { schema },
        }));
    }
    return _funnelClients.get(funnel);
}

// Resolve funnel for unauthenticated routes (defaults to analytics — preserves
// pre-multitenant behavior). Authenticated routes use requireAuth which
// validates against user_funnel_access.
function resolveFunnel(req, fallback = 'analytics') {
    const f = req.headers['x-funnel'] || fallback;
    return ALLOWED_FUNNELS.includes(f) ? f : fallback;
}

// ─── In-Memory Cache (reduces Supabase egress by ~98%) ───────────────────────
// Funnel-keyed: each funnel has its own cache bucket so a write to one funnel
// can't invalidate another's cached state. Past days' dedup counts never
// change — cache them forever. Today's data is invalidated when webhooks
// write new events.
const cache = {
    byFunnel: {}, // funnel → { dedupCounts, dedupTimestamps, metricsResponse, ... }
    metricsTTL: 60_000,   // 60 seconds
    insightsTTL: 300_000, // 5 minutes

    // Global hit/miss stats — aggregated across funnels for /api/health
    hits: 0,
    misses: 0,
};

function getCacheBucket(funnel) {
    if (!cache.byFunnel[funnel]) {
        cache.byFunnel[funnel] = {
            // Deduplicated event counts per day: { "2026-04-09": { registrations: 5, ... } }
            dedupCounts: {},
            dedupTimestamps: {},
            // Full formatted GET /api/metrics response
            metricsResponse: null,
            metricsUpdatedAt: 0,
            // AI insights context (metrics + event summaries)
            insightsContext: null,
            insightsUpdatedAt: 0,
            // Bumped on every invalidation. Async readers snapshot this before a
            // Supabase fetch and skip writing back if it changed — prevents a
            // mid-flight invalidation from being clobbered by stale data.
            invalidationEpoch: 0,
        };
    }
    return cache.byFunnel[funnel];
}

function invalidateMetricsCache(funnel) {
    const b = getCacheBucket(funnel);
    b.metricsResponse = null;
    b.metricsUpdatedAt = 0;
    b.invalidationEpoch++;
    console.log(`🗑️  Cache[${funnel}]: metrics response invalidated`);
}

function invalidateDedupForDate(funnel, isoDate) {
    const b = getCacheBucket(funnel);
    delete b.dedupCounts[isoDate];
    invalidateMetricsCache(funnel);
    console.log(`🗑️  Cache[${funnel}]: dedup invalidated for ${isoDate}`);
}

function invalidateInsightsCache(funnel) {
    const b = getCacheBucket(funnel);
    b.insightsContext = null;
    b.insightsUpdatedAt = 0;
    b.invalidationEpoch++;
}

// ─── Express App ─────────────────────────────────────────────────────────────
const app = express();

// Trust first proxy (nginx) — required for express-rate-limit behind a reverse proxy
app.set('trust proxy', 1);

// Security headers
app.use(helmet({
    contentSecurityPolicy: false, // Allow inline styles for the React app
}));

// CORS
const corsOrigins = (process.env.CORS_ORIGINS || '').split(',').filter(Boolean);
app.use(cors({
    origin: corsOrigins.length > 0 ? corsOrigins : true,
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-API-Key', 'X-Funnel'],
}));

// Body parsing
app.use(express.json({ limit: '1mb' }));

// Request logging
app.use((req, res, next) => {
    const start = Date.now();
    res.on('finish', () => {
        const ms = Date.now() - start;
        if (req.path.startsWith('/api')) {
            console.log(`${req.method} ${req.path} → ${res.statusCode} (${ms}ms)`);
        }
    });
    next();
});

// API responses must never be cached by browsers, bfcache, or intermediaries.
app.use('/api', (req, res, next) => {
    res.set('Cache-Control', 'no-store');
    next();
});

// ─── Rate Limiting ───────────────────────────────────────────────────────────
const webhookLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 300,
    message: { error: 'Too many requests, try again later' },
});

const dashboardLimiter = rateLimit({
    windowMs: 1 * 60 * 1000,
    max: 120,
    message: { error: 'Too many requests' },
});

// ─── Auth Middleware ──────────────────────────────────────────────────────────
// Constant-time comparison helper — accepts buffers of differing lengths.
function constantTimeEq(a, b) {
    const aBuf = Buffer.from(String(a));
    const bBuf = Buffer.from(String(b));
    if (aBuf.length !== bBuf.length) return false;
    return crypto.timingSafeEqual(aBuf, bBuf);
}

// Unified webhook auth: checks env API_KEY (analytics) and NATIVE_API_KEY first
// (fast, no DB), then the public.api_keys table. Sets req.funnel from the matched
// key so the handler writes to the correct schema.
async function authenticateWebhook(req, res, next) {
    const key = req.headers['x-api-key'] || req.query.api_key || req.body?.api_key;
    if (!key) {
        return res.status(401).json({ error: 'Missing API key. Send X-API-Key header.' });
    }
    // Fast path: env keys, one per funnel
    if (constantTimeEq(key, API_KEY)) { req.funnel = 'analytics'; return next(); }
    if (process.env.NATIVE_API_KEY && constantTimeEq(key, process.env.NATIVE_API_KEY)) {
        req.funnel = 'native';
        return next();
    }
    // Slow path: check public.api_keys table (single source of truth across funnels)
    try {
        const hash = crypto.createHash('sha256').update(key).digest('hex');
        const { data: dbKey } = await supabasePublic
            .from('api_keys')
            .select('id, is_active, funnel')
            .eq('key_hash', hash)
            .single();
        if (dbKey && dbKey.is_active && ALLOWED_FUNNELS.includes(dbKey.funnel)) {
            await supabasePublic.from('api_keys').update({ last_used_at: new Date().toISOString() }).eq('id', dbKey.id);
            req.funnel = dbKey.funnel;
            return next();
        }
    } catch { /* DB key lookup failed, fall through */ }
    return res.status(403).json({ error: 'Invalid API key' });
}

// Supabase JWT auth — verifies the user is logged in, then resolves req.funnel
// from the X-Funnel header (defaulting to analytics) and validates the user
// has access to that funnel via public.user_funnel_access.
async function requireAuth(req, res, next) {
    const auth = req.headers.authorization;
    if (!auth || !auth.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Authentication required' });
    }
    const token = auth.replace('Bearer ', '');
    const { data: { user }, error } = await supabasePublic.auth.getUser(token);
    if (error || !user) {
        return res.status(401).json({ error: 'Invalid or expired session' });
    }
    req.user = user;

    const requested = resolveFunnel(req, 'analytics');
    // Check access in user_funnel_access. If the table is empty or has no row
    // for this user, fall back to analytics-only (preserves single-funnel
    // behavior pre-migration).
    const { data: accessRows } = await supabasePublic
        .from('user_funnel_access')
        .select('funnel')
        .eq('user_id', user.id);
    const allowed = (accessRows || []).map(r => r.funnel);
    const effectiveAllowed = allowed.length > 0 ? allowed : ['analytics'];
    if (!effectiveAllowed.includes(requested)) {
        return res.status(403).json({ error: `No access to funnel '${requested}'` });
    }
    req.funnel = requested;
    req.allowedFunnels = effectiveAllowed;
    next();
}

// Requires admin role within req.funnel's schema. Must run after requireAuth.
// Missing user_roles row in the funnel's schema → not admin (correct: viewer
// of a funnel is the default for users who have access but no explicit role).
async function requireAdmin(req, res, next) {
    const sb = clientFor(req.funnel);
    const { data } = await sb
        .from('user_roles')
        .select('role')
        .eq('user_id', req.user.id)
        .single();
    if (!data || data.role !== 'admin') {
        return res.status(403).json({ error: 'Admin access required' });
    }
    req.userRole = 'admin';
    next();
}

// ─── Helpers ─────────────────────────────────────────────────────────────────
function getLADate(dateStr) {
    const date = dateStr ? new Date(dateStr) : new Date();
    return date.toLocaleDateString('en-US', { timeZone: 'America/Los_Angeles' });
}

function getLADayOfWeek(dateStr) {
    const [m, d, y] = dateStr.split('/').map(Number);
    const date = new Date(y, m - 1, d);
    return date.toLocaleDateString('en-US', { weekday: 'long' });
}

function parseDateInput(input) {
    // Accept: MM/DD/YYYY, YYYY-MM-DD, or ISO strings
    if (!input) return null;
    const str = String(input).trim();

    let mmddyyyy = null;

    // MM/DD/YYYY
    if (/^\d{2}\/\d{2}\/\d{4}$/.test(str)) {
        mmddyyyy = str;
    }
    // YYYY-MM-DD
    else if (/^\d{4}-\d{2}-\d{2}/.test(str)) {
        const [y, m, d] = str.substring(0, 10).split('-');
        mmddyyyy = `${m}/${d}/${y}`;
    }
    // Try parsing as date
    else {
        const parsed = new Date(str);
        if (!isNaN(parsed.getTime())) {
            mmddyyyy = parsed.toLocaleDateString('en-US', { timeZone: 'America/Los_Angeles' });
        }
    }

    // Validate the resulting date is real (reject month 13, day 32, etc.)
    if (mmddyyyy) {
        const [m, d, y] = mmddyyyy.split('/').map(Number);
        const test = new Date(y, m - 1, d);
        if (test.getFullYear() !== y || test.getMonth() !== m - 1 || test.getDate() !== d) {
            return null; // e.g. 13/32/2026 → invalid
        }
    }

    return mmddyyyy;
}

function dateToISO(mmddyyyy) {
    const [m, d, y] = mmddyyyy.split('/');
    return `${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`;
}

// ─── Webhook Log ─────────────────────────────────────────────────────────────
async function logWebhook(funnel, source, payload, status, errorMessage = null) {
    try {
        await clientFor(funnel).from('webhook_log').insert({
            source,
            payload,
            status,
            error_message: errorMessage,
        });
    } catch (e) {
        console.error('Failed to log webhook:', e.message);
    }
}


// =============================================================================
// WEBHOOK ENDPOINTS (for Zapier / Make)
// =============================================================================

// POST /api/metrics — Upsert daily metrics
// Zapier/Make sends: { date, fb_spend, registrations, replays, viewedcta, clickedcta, purchases }
app.post('/api/metrics', webhookLimiter, async (req, res) => {
    try {
        // Accept EITHER webhook API-key auth OR admin Bearer-token auth
        const authHeader = req.headers.authorization;
        if (authHeader && authHeader.startsWith('Bearer ')) {
            // Admin auth path — funnel comes from X-Funnel header (default analytics)
            const token = authHeader.split(' ')[1];
            const { data: { user }, error } = await supabasePublic.auth.getUser(token);
            if (error || !user) return res.status(401).json({ error: 'Invalid or expired session' });
            const requestedFunnel = resolveFunnel(req, 'analytics');
            const { data: roleData } = await clientFor(requestedFunnel)
                .from('user_roles').select('role').eq('user_id', user.id).single();
            if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });
            req.user = user;
            req.funnel = requestedFunnel;
        } else {
            // Webhook API-key path — delegate to unified authenticateWebhook (sets req.funnel from key)
            const authResult = await new Promise((resolve) => {
                authenticateWebhook(req, res, () => resolve('ok'));
            });
            // If authenticateWebhook already sent a response (401/403), stop here
            if (authResult !== 'ok') return;
        }

        const supabase = clientFor(req.funnel);
        const body = req.body;
        await logWebhook(req.funnel, 'zapier', body, 'received');

        const date = parseDateInput(body.date);
        if (!date) {
            await logWebhook(req.funnel, 'zapier', body, 'error', 'Invalid or missing date');
            return res.status(400).json({ error: 'Invalid or missing date. Use MM/DD/YYYY or YYYY-MM-DD.' });
        }

        const isoDate = dateToISO(date);
        const dayOfWeek = getLADayOfWeek(date);

        const row = {
            date: isoDate,
            day_of_week: dayOfWeek,
            fb_spend: parseFloat(body.fb_spend) || 0,
            fb_link_clicks: parseInt(body.fb_link_clicks) || 0,
            registrations: parseInt(body.registrations) || 0,
            replays: parseInt(body.replays) || 0,
            viewedcta: parseInt(body.viewedcta) || 0,
            clickedcta: parseInt(body.clickedcta) || 0,
            attended: parseInt(body.attended) || 0,
        };

        // Only include purchase source columns if explicitly provided
        // Prevents admin form edits from clobbering webhook-sourced data to 0
        const PURCHASE_COLS = ['purchases', 'purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_postwebinar', 'purchases_cpa'];
        for (const col of PURCHASE_COLS) {
            if (body[col] !== undefined && body[col] !== '' && body[col] !== null) {
                row[col] = parseInt(body[col]) || 0;
            }
        }

        const { data, error } = await supabase
            .from('daily_metrics')
            .upsert(row, { onConflict: 'date' })
            .select()
            .single();

        if (error) throw error;

        await logWebhook(req.funnel, 'zapier', body, 'processed');
        invalidateMetricsCache(req.funnel);
        invalidateInsightsCache(req.funnel);
        console.log(`✅ [${req.funnel}] Upserted metrics for ${date} (${dayOfWeek})`);
        res.json({ success: true, date, day: dayOfWeek, data });

    } catch (err) {
        console.error('❌ POST /api/metrics error:', err.message);
        if (req.funnel) await logWebhook(req.funnel, 'zapier', req.body, 'error', err.message);
        res.status(500).json({ error: 'Internal server error', detail: err.message });
    }
});

// POST /api/metrics/batch — Bulk upsert (for backfills)
app.post('/api/metrics/batch', webhookLimiter, authenticateWebhook, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { entries } = req.body;
        if (!Array.isArray(entries) || entries.length === 0) {
            return res.status(400).json({ error: 'Send { entries: [...] } array' });
        }

        if (entries.length > 365) {
            return res.status(400).json({ error: 'Maximum 365 entries per batch' });
        }

        const rows = [];
        const errors = [];

        for (let i = 0; i < entries.length; i++) {
            const entry = entries[i];
            const date = parseDateInput(entry.date);
            if (!date) {
                errors.push({ index: i, error: 'Invalid date', input: entry.date });
                continue;
            }
            const row = {
                date: dateToISO(date),
                day_of_week: getLADayOfWeek(date),
                fb_spend: parseFloat(entry.fb_spend) || 0,
                fb_link_clicks: parseInt(entry.fb_link_clicks) || 0,
                registrations: parseInt(entry.registrations) || 0,
                replays: parseInt(entry.replays) || 0,
                viewedcta: parseInt(entry.viewedcta) || 0,
                clickedcta: parseInt(entry.clickedcta) || 0,
                attended: parseInt(entry.attended) || 0,
            };
            // Only include purchase columns if explicitly provided (prevents clobbering)
            const PURCHASE_COLS = ['purchases', 'purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_postwebinar', 'purchases_cpa'];
            for (const col of PURCHASE_COLS) {
                if (entry[col] !== undefined && entry[col] !== '' && entry[col] !== null) {
                    row[col] = parseInt(entry[col]) || 0;
                }
            }
            rows.push(row);
        }

        if (rows.length > 0) {
            const { error } = await supabase
                .from('daily_metrics')
                .upsert(rows, { onConflict: 'date' });
            if (error) throw error;
        }

        await logWebhook(req.funnel, 'batch', { count: entries.length }, 'processed');
        invalidateMetricsCache(req.funnel);
        // Invalidate dedup for all affected dates
        for (const r of rows) invalidateDedupForDate(req.funnel, r.date);
        invalidateInsightsCache(req.funnel);
        res.json({ success: true, inserted: rows.length, errors });

    } catch (err) {
        console.error('❌ POST /api/metrics/batch error:', err.message);
        res.status(500).json({ error: 'Internal server error', detail: err.message });
    }
});

// POST /api/metrics/increment — Add to today's totals (for real-time event tracking)
// Zapier sends: { field: "registrations", count: 1, name: "John Doe", email: "john@example.com" }
app.post('/api/metrics/increment', webhookLimiter, authenticateWebhook, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { field, count = 1, name, email, phone, execution_id, ...rest } = req.body;
        // Purchase source columns are NOT in validFields — purchases must always go through
        // field:'purchases' with a 'source' param so source routing and Post Webinar detection run.
        const validFields = ['fb_spend', 'fb_link_clicks', 'registrations', 'replays', 'viewedcta', 'clickedcta', 'purchases', 'attended', 'stayeduntil'];

        // ── Purchase source mapping ──────────────────────────────────────
        const PURCHASE_SOURCE_MAP = {
            'Paid Ads':    'purchases_fb',
            'Native':      'purchases_native',
            'Youtube':     'purchases_youtube',
            'AI Bot':      'purchases_aibot',
            'CPA Traffic': 'purchases_cpa',
        };

        // ── Webinar engagement milestone mapping ─────────────────────────
        // field='stayeduntil' + body.stayeduntil ∈ {45,60,80} → stayed_NN column
        const STAYED_MAP = { 45: 'stayed_45', 60: 'stayed_60', 80: 'stayed_80' };

        if (!validFields.includes(field)) {
            return res.status(400).json({ error: `Invalid field. Use: ${validFields.join(', ')}` });
        }

        // Dedup: registrations dedup by email + webinar day (PDT) so the same person
        // can register for different days' webinars in quick succession, but duplicate
        // registrations for the same day are blocked. All other event types use the
        // original 5-minute window as webhook retry protection.
        if (email) {
            if (field === 'registrations' && rest.webinar_datetime_utc) {
                // Parse the incoming webinar date to a PDT/PST calendar day
                const cleaned = rest.webinar_datetime_utc.replace(/(\d+)(st|nd|rd|th)/gi, '$1');
                const parsed = new Date(cleaned + ' UTC');
                if (!isNaN(parsed.getTime())) {
                    const webinarDayISO = parsed.toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
                    // Check if this email already has a registration whose webinar_datetime_utc
                    // resolves to the same PDT calendar day
                    const { data: existing } = await supabase
                        .from('events')
                        .select('id, metadata')
                        .eq('event_type', 'registrations')
                        .eq('email', email);

                    const isDup = (existing || []).some(ev => {
                        const evRaw = ev.metadata?.webinar_datetime_utc;
                        if (!evRaw) return false;
                        const evCleaned = evRaw.replace(/(\d+)(st|nd|rd|th)/gi, '$1');
                        const evParsed = new Date(evCleaned + ' UTC');
                        if (isNaN(evParsed.getTime())) return false;
                        const evDay = evParsed.toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
                        return evDay === webinarDayISO;
                    });

                    if (isDup) {
                        console.log(`⏭️  Dedup skip: registration for ${email} (already registered for webinar day ${webinarDayISO})`);
                        return res.json({ success: true, duplicate: true, message: 'Duplicate registration skipped (same webinar day)' });
                    }
                }
            } else {
                // All other event types: 5-minute dedup window (webhook retry protection).
                // For stayeduntil, the milestone (45/60/80) is part of the dedup key —
                // hitting 45 then 60 from the same user within 5 min are NOT duplicates.
                const fiveMinAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
                let q = supabase
                    .from('events')
                    .select('id')
                    .eq('event_type', field)
                    .eq('email', email)
                    .gte('event_time', fiveMinAgo);
                if (field === 'stayeduntil' && rest.stayeduntil !== undefined) {
                    q = q.eq('metadata->>stayeduntil', String(rest.stayeduntil));
                }
                const { data: existing } = await q.limit(1);

                if (existing && existing.length > 0) {
                    const detail = field === 'stayeduntil' ? `${field} ${rest.stayeduntil}min` : field;
                    console.log(`⏭️  Dedup skip: ${detail} for ${email} (duplicate within 5min)`);
                    return res.json({ success: true, duplicate: true, message: 'Duplicate event skipped' });
                }
            }
        }

        const today = getLADate();
        let targetDate = today;

        // If webinar_datetime_utc is provided, use the webinar date for counting
        // Format from Stealth: "March 31st 2026, 2:16:43 pm"
        if (rest.webinar_datetime_utc) {
            try {
                // Strip ordinal suffixes (st, nd, rd, th) for Date parsing
                // Append ' UTC' because Stealth sends this field in UTC despite the non-ISO format
                const cleaned = rest.webinar_datetime_utc.replace(/(\d+)(st|nd|rd|th)/gi, '$1');
                const parsed = new Date(cleaned + ' UTC');
                if (!isNaN(parsed.getTime())) {
                    targetDate = parsed.toLocaleDateString('en-US', { timeZone: 'America/Los_Angeles' });
                    console.log(`📅 Using webinar date: ${rest.webinar_datetime_utc} → ${targetDate}`);
                }
            } catch (e) {
                console.warn('⚠️ Could not parse webinar_datetime_utc, using today:', rest.webinar_datetime_utc);
            }
        }

        const isoDate = dateToISO(targetDate);
        const dayOfWeek = getLADayOfWeek(targetDate);

        // Ensure target date's row exists
        await supabase
            .from('daily_metrics')
            .upsert({ date: isoDate, day_of_week: dayOfWeek }, { onConflict: 'date' });

        // ── Purchase source routing ───────────────────────────────────
        let incrementField = field;
        let resolvedSource = rest.source || null;

        if (field === 'purchases') {
            const rawSource = rest.source || 'Paid Ads'; // default to Paid Ads
            let sourceColumn = PURCHASE_SOURCE_MAP[rawSource] || 'purchases_fb';

            // Post Webinar detection: if "Paid Ads" and buyer attended 12h+ ago
            if (rawSource === 'Paid Ads' && email) {
                try {
                    const { data: attendedEvt } = await supabase
                        .from('events')
                        .select('event_time')
                        .eq('event_type', 'attended')
                        .ilike('email', email)
                        .order('event_time', { ascending: false })
                        .limit(1);

                    if (attendedEvt?.length > 0) {
                        const hoursSince = (Date.now() - new Date(attendedEvt[0].event_time).getTime()) / 3600000;
                        if (hoursSince >= 12) {
                            sourceColumn = 'purchases_postwebinar';
                            resolvedSource = 'Post Webinar';
                            console.log(`🎯 Post Webinar purchase: ${email} attended ${Math.round(hoursSince)}h ago`);
                        }
                    }
                } catch (e) {
                    console.warn('⚠️ Post Webinar check failed:', e.message);
                }
            }

            if (!resolvedSource) resolvedSource = rawSource;
            incrementField = sourceColumn;
            console.log(`🛒 Purchase source: "${rawSource}" → ${sourceColumn}`);
        }

        // ── Engagement milestone routing ─────────────────────────────
        if (field === 'stayeduntil') {
            const minute = Number(rest.stayeduntil);
            const col = STAYED_MAP[minute];
            if (!col) {
                return res.status(400).json({ error: 'stayeduntil must be 45, 60, or 80' });
            }
            incrementField = col;
            console.log(`⏱️  Stayeduntil: ${minute}min → ${col}`);
        }

        // ── Step 1: Record the event FIRST (source of truth for dedup) ──
        // This must land before the counter increment so retries are safe:
        //   - Event fails → counter untouched → retry is clean
        //   - Event succeeds, counter fails → retry hits 5-min dedup → skip
        //   - Counter is 1 behind temporarily, but dashboard uses dedup > raw
        const metadata = { ...rest };
        delete metadata.api_key; // don't store keys
        if (field === 'purchases' && resolvedSource) {
            metadata.source = resolvedSource;
        }
        const { error: evErr } = await supabase.from('events').insert({
            event_type: field,
            name: name || null,
            email: email || null,
            phone: phone || null,
            execution_id: execution_id || null,
            metadata: Object.keys(metadata).length > 0 ? metadata : {},
        });
        if (evErr) {
            console.error('❌ Event insert FAILED:', evErr.message);
            throw new Error(`Event recording failed: ${evErr.message}`);
        }

        // ── Step 2: Increment the denormalized counter ──
        const { data, error } = await supabase.rpc('increment_field', {
            p_date: isoDate,
            p_field: incrementField,
            p_amount: Number(count)
        });

        if (error) {
            // Only fall back to read-modify-write for "function not found" (42883).
            // Transient/ambiguous errors must NOT fall through — they risk double-counting.
            if (error.code === '42883') {
                console.warn('⚠️ RPC increment_field not deployed, falling back to read-modify-write');
                const { data: current } = await supabase
                    .from('daily_metrics')
                    .select(incrementField)
                    .eq('date', isoDate)
                    .single();
                const newValue = (Number(current?.[incrementField]) || 0) + Number(count);
                const { error: updErr } = await supabase
                    .from('daily_metrics')
                    .update({ [incrementField]: newValue })
                    .eq('date', isoDate);
                if (updErr) throw updErr;
            } else {
                // Counter failed but event is recorded — dedup will show correct count.
                // Log and continue rather than returning 500 (which would trigger a retry
                // that dedup would skip, leaving the counter permanently behind).
                console.error('⚠️ Counter increment failed (event recorded, dedup is accurate):', error.message);
            }
        }

        // Invalidate caches — new event arrived
        invalidateDedupForDate(req.funnel, isoDate);
        invalidateInsightsCache(req.funnel);

        console.log(`✅ Increment ${incrementField} +${count} for ${targetDate}${name ? ` (${name})` : ''}`);
        res.json({ success: true, date: targetDate, field: incrementField, count: Number(count) });

    } catch (err) {
        console.error('❌ POST /api/metrics/increment error:', err.message);
        res.status(500).json({ error: 'Internal server error', detail: err.message });
    }
});


// POST /api/metrics/set — Set a field to an absolute value for today (or a given date)
// Use this when the source gives you the CURRENT TOTAL, not a delta.
// e.g. Facebook API returns today's cumulative spend — this replaces, not adds.
// Body: { field: "fb_spend", value: 312.75 }
// Optional: { field: "fb_spend", value: 312.75, date: "03/14/2026" }
app.post('/api/metrics/set', webhookLimiter, authenticateWebhook, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { field, value, date: dateInput } = req.body;
        const validFields = ['fb_spend', 'fb_link_clicks', 'registrations', 'replays', 'viewedcta', 'clickedcta', 'purchases', 'purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_postwebinar', 'purchases_cpa', 'stayed_45', 'stayed_60', 'stayed_80', 'attended'];

        if (!validFields.includes(field)) {
            return res.status(400).json({ error: `Invalid field. Use: ${validFields.join(', ')}` });
        }

        if (value === undefined || value === null) {
            return res.status(400).json({ error: 'Missing value' });
        }

        const targetDate = dateInput ? parseDateInput(dateInput) : getLADate();
        if (!targetDate) {
            return res.status(400).json({ error: 'Invalid date' });
        }

        const isoDate = dateToISO(targetDate);
        const dayOfWeek = getLADayOfWeek(targetDate);

        // Ensure the row exists
        await supabase
            .from('daily_metrics')
            .upsert({ date: isoDate, day_of_week: dayOfWeek }, { onConflict: 'date' });

        // Read previous value for the response
        const { data: current } = await supabase
            .from('daily_metrics')
            .select(field)
            .eq('date', isoDate)
            .single();

        const previous = current?.[field];
        const newValue = (field === 'fb_spend') ? parseFloat(value) : parseInt(value);

        // Set the absolute value
        const { data, error } = await supabase
            .from('daily_metrics')
            .update({ [field]: newValue })
            .eq('date', isoDate)
            .select()
            .single();

        if (error) throw error;

        await logWebhook(req.funnel, 'set', { field, value: newValue, date: targetDate }, 'processed');
        invalidateMetricsCache(req.funnel);
        invalidateInsightsCache(req.funnel);
        console.log(`✅ [${req.funnel}] Set ${field} = ${newValue} for ${targetDate} (was ${previous})`);
        res.json({ success: true, date: targetDate, field, previous, new: newValue });

    } catch (err) {
        console.error('❌ POST /api/metrics/set error:', err.message);
        res.status(500).json({ error: 'Internal server error', detail: err.message });
    }
});


// =============================================================================
// REFRESH — Direct Facebook Spend Sync (rate-limited: 3 per hour)
// =============================================================================

const refreshTimestamps = []; // in-memory log of recent refresh calls

app.post('/api/refresh', dashboardLimiter, async (req, res) => {
    const now = Date.now();
    const ONE_HOUR = 60 * 60 * 1000;

    // Purge timestamps older than 1 hour
    while (refreshTimestamps.length > 0 && now - refreshTimestamps[0] > ONE_HOUR) {
        refreshTimestamps.shift();
    }

    if (refreshTimestamps.length >= 3) {
        const oldestInWindow = refreshTimestamps[0];
        const waitMs = ONE_HOUR - (now - oldestInWindow);
        const waitMin = Math.ceil(waitMs / 60000);
        return res.status(429).json({
            error: `Rate limit reached. Please wait ${waitMin} minute${waitMin !== 1 ? 's' : ''} before refreshing again.`,
            retryAfterMs: waitMs,
        });
    }

    try {
        // FB sync writes to the analytics schema only (Taboola for native is future work)
        const result = await syncFacebookSpend();
        refreshTimestamps.push(now);
        invalidateMetricsCache('analytics');
        invalidateInsightsCache('analytics');

        const remaining = 3 - refreshTimestamps.length;
        return res.json({
            message: result
                ? `Spend updated: $${result.spend.toFixed(2)} for ${result.date}`
                : 'Sync skipped — Facebook credentials not configured.',
            remaining,
        });
    } catch (err) {
        refreshTimestamps.push(now); // still count failed attempts
        console.error('❌ Refresh sync error:', err.message);
        return res.status(502).json({ error: `Facebook sync failed: ${err.message}` });
    }
});

// POST /api/refresh-date — Re-fetch Facebook insights for a specific date
app.post('/api/refresh-date', dashboardLimiter, async (req, res) => {
    const { date } = req.body; // expects YYYY-MM-DD
    if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
        return res.status(400).json({ error: 'Send { date: "YYYY-MM-DD" }' });
    }

    try {
        const { fetchFacebookInsights, writeInsightsToSupabase } = await import('./fb-sync.js');
        const insights = await fetchFacebookInsights(date);
        await writeInsightsToSupabase(date, insights);
        invalidateMetricsCache('analytics');
        invalidateInsightsCache('analytics');

        console.log(`✅ Recalc for ${date}: $${insights.spend.toFixed(2)}, ${insights.linkClicks} link clicks`);
        return res.json({ message: `Insights for ${date} updated — $${insights.spend.toFixed(2)}, ${insights.linkClicks} link clicks`, spend: insights.spend, linkClicks: insights.linkClicks });
    } catch (err) {
        console.error(`❌ Recalc spend for ${date} failed:`, err.message);
        return res.status(502).json({ error: `Facebook API error: ${err.message}` });
    }
});

// GET /api/me/funnels — Return the list of funnels this user can access.
// Reads from public.user_funnel_access. If empty, defaults to ['analytics']
// for backward compat with users who pre-date the multitenant migration.
app.get('/api/me/funnels', dashboardLimiter, async (req, res) => {
    try {
        const auth = req.headers.authorization;
        if (!auth || !auth.startsWith('Bearer ')) {
            return res.status(401).json({ error: 'Authentication required' });
        }
        const token = auth.replace('Bearer ', '');
        const { data: { user }, error } = await supabasePublic.auth.getUser(token);
        if (error || !user) {
            return res.status(401).json({ error: 'Invalid or expired session' });
        }
        const { data: rows } = await supabasePublic
            .from('user_funnel_access')
            .select('funnel')
            .eq('user_id', user.id);
        const funnels = (rows || []).map(r => r.funnel).filter(f => ALLOWED_FUNNELS.includes(f));
        res.json({ funnels: funnels.length > 0 ? funnels : ['analytics'] });
    } catch (err) {
        console.error('❌ GET /api/me/funnels error:', err.message);
        res.status(500).json({ error: 'Failed to fetch funnels' });
    }
});

// GET /api/me — Return current user's role and preferences (within active funnel)
app.get('/api/me', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { data } = await supabase
            .from('user_roles')
            .select('role, preferences')
            .eq('user_id', req.user.id)
            .single();

        let defaultColOrder = null;

        // If user doesn't have their own col_order, look up the admin's global default
        if (!data?.preferences?.col_order) {
            const { data: adminRow } = await supabase
                .from('user_roles')
                .select('preferences')
                .eq('role', 'admin')
                .limit(1)
                .single();
            if (adminRow?.preferences?.global_col_order) {
                defaultColOrder = adminRow.preferences.global_col_order;
            }
        }

        res.json({
            id: req.user.id,
            email: req.user.email,
            role: data?.role || 'viewer',
            preferences: data?.preferences || {},
            default_col_order: defaultColOrder,
            funnel: req.funnel,
            allowed_funnels: req.allowedFunnels || [req.funnel],
        });
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch user info' });
    }
});

// PUT /api/me/preferences — Save user preferences (hidden columns, etc.)
app.put('/api/me/preferences', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { preferences } = req.body;
        if (!preferences || typeof preferences !== 'object') {
            return res.status(400).json({ error: 'Send { preferences: { ... } }' });
        }

        // Check if user_roles row exists
        const { data: existing } = await supabase
            .from('user_roles')
            .select('user_id, preferences')
            .eq('user_id', req.user.id)
            .single();

        // Merge with existing preferences (don't overwrite other keys)
        const merged = { ...(existing?.preferences || {}), ...preferences };

        let error;
        if (existing) {
            // Update only preferences, keep existing role
            ({ error } = await supabase
                .from('user_roles')
                .update({ preferences: merged })
                .eq('user_id', req.user.id));
        } else {
            // Insert new row with default viewer role
            ({ error } = await supabase
                .from('user_roles')
                .insert({ user_id: req.user.id, role: 'viewer', preferences: merged }));
        }

        if (error) throw error;
        res.json({ success: true });
    } catch (err) {
        console.error('❌ PUT /api/me/preferences error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// POST /api/settings/propagate-col-order — Admin pushes their column order to all users
app.post('/api/settings/propagate-col-order', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        // Verify admin
        const { data: roleData } = await supabase.from('user_roles').select('role, preferences').eq('user_id', req.user.id).single();
        if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });

        const colOrder = roleData.preferences?.col_order;
        if (!colOrder || !Array.isArray(colOrder)) {
            return res.status(400).json({ error: 'You have no column order saved. Open Edit Columns, arrange, and save first.' });
        }

        // Store as global default in admin's preferences
        const adminPrefs = { ...(roleData.preferences || {}), global_col_order: colOrder };
        await supabase.from('user_roles').update({ preferences: adminPrefs }).eq('user_id', req.user.id);

        // Push to all other users
        const { data: allUsers } = await supabase.from('user_roles').select('user_id, preferences').neq('user_id', req.user.id);
        let updated = 0;
        for (const user of (allUsers || [])) {
            const merged = { ...(user.preferences || {}), col_order: colOrder };
            await supabase.from('user_roles').update({ preferences: merged }).eq('user_id', user.user_id);
            updated++;
        }

        console.log(`✅ Admin propagated col_order to ${updated} users`);
        res.json({ success: true, updated });
    } catch (err) {
        console.error('❌ POST /api/settings/propagate-col-order error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// =============================================================================
// DASHBOARD LENSES (custom metric views)
// =============================================================================

// GET /api/lenses — list all lenses (any authenticated user)
app.get('/api/lenses', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { data, error } = await supabase
            .from('dashboard_lenses')
            .select('*')
            .order('sort_order', { ascending: true });
        if (error) throw error;
        res.json({ data: data || [] });
    } catch (err) {
        console.error('❌ GET /api/lenses error:', err.message);
        res.status(500).json({ error: 'Failed to fetch lenses' });
    }
});

// POST /api/lenses — create a lens (admin only)
app.post('/api/lenses', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { data: roleData } = await supabase.from('user_roles').select('role').eq('user_id', req.user.id).single();
        if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });

        const { name, metrics } = req.body;
        if (!name || !Array.isArray(metrics) || metrics.length === 0) {
            return res.status(400).json({ error: 'Send { name, metrics: ["fb_spend", ...] }' });
        }

        const { data, error } = await supabase
            .from('dashboard_lenses')
            .insert({ name, metrics, created_by: req.user.id })
            .select()
            .single();
        if (error) throw error;
        res.json({ data });
    } catch (err) {
        console.error('❌ POST /api/lenses error:', err.message);
        res.status(500).json({ error: 'Failed to create lens' });
    }
});

// PUT /api/lenses/:id — update a lens (admin only)
app.put('/api/lenses/:id', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { data: roleData } = await supabase.from('user_roles').select('role').eq('user_id', req.user.id).single();
        if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });

        const { name, metrics } = req.body;
        const updates = {};
        if (name) updates.name = name;
        if (Array.isArray(metrics)) updates.metrics = metrics;

        const { data, error } = await supabase
            .from('dashboard_lenses')
            .update(updates)
            .eq('id', req.params.id)
            .select()
            .single();
        if (error) throw error;
        res.json({ data });
    } catch (err) {
        console.error('❌ PUT /api/lenses error:', err.message);
        res.status(500).json({ error: 'Failed to update lens' });
    }
});

// DELETE /api/lenses/:id — delete a lens (admin only, can't delete default)
app.delete('/api/lenses/:id', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        if (req.params.id === 'default-all') return res.status(400).json({ error: 'Cannot delete the default lens' });

        const supabase = clientFor(req.funnel);
        const { data: roleData } = await supabase.from('user_roles').select('role').eq('user_id', req.user.id).single();
        if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });

        const { error } = await supabase.from('dashboard_lenses').delete().eq('id', req.params.id);
        if (error) throw error;
        res.json({ success: true });
    } catch (err) {
        console.error('❌ DELETE /api/lenses error:', err.message);
        res.status(500).json({ error: 'Failed to delete lens' });
    }
});

// =============================================================================
// DASHBOARD ENDPOINTS (for the frontend)
// =============================================================================

// ─── Cached Dedup Engine ─────────────────────────────────────────────────────
// Computes deduplicated event counts per day. Past days are cached forever
// (their counts can't change). Today's counts are recomputed when invalidated
// by a webhook. This eliminates ~95% of Supabase events-table egress.
const EVENT_TYPES = ['registrations', 'attended', 'replays', 'viewedcta', 'clickedcta', 'purchases', 'stayeduntil'];

// stayeduntil events use metadata.stayeduntil (45|60|80) as the sub-key
const STAYED_DEDUP_MAP = { 45: 'stayed_45', 60: 'stayed_60', 80: 'stayed_80' };

// Map purchase event metadata.source to the dedup sub-key
const PURCHASE_DEDUP_MAP = {
    'Paid Ads':      'purchases_fb',
    'Native':        'purchases_native',
    'Youtube':       'purchases_youtube',
    'AI Bot':        'purchases_aibot',
    'Post Webinar':  'purchases_postwebinar',
    'CPA Traffic':   'purchases_cpa',
};

// Allowed variant buckets. 'all' is computed as the sum of the others.
const VARIANT_BUCKETS = ['A', 'B', 'undetected'];

// Returns: { 'YYYY-MM-DD': { event_type: { all, A, B, undetected } } }
//
// Variant attribution rules:
//   - Registration events use their own metadata.variant if present
//   - All other events (attended, viewedcta, ..., purchases) inherit the
//     variant from the user's FIRST registration (by event_time), looked up
//     via lowercased email
//   - Events with no email or no matching registration → 'undetected'
//
// 'all' = A + B + undetected. This holds because first-registration-wins
// guarantees each user is bucketed into exactly one variant.
function computeDedupFromEvents(events) {
    // ── Pass 1: build email → variant map from registrations (first wins) ──
    const sortedRegs = events
        .filter(ev => ev.event_type === 'registrations' && ev.email)
        .sort((a, b) => new Date(a.event_time) - new Date(b.event_time));
    const emailToVariant = {};
    for (const ev of sortedRegs) {
        const raw = ev.metadata?.variant;
        if (raw === undefined || raw === null || raw === '') continue;
        const v = String(raw).trim().toUpperCase();
        if (v !== 'A' && v !== 'B') continue; // ignore unrecognized variants
        const k = ev.email.toLowerCase();
        if (!(k in emailToVariant)) emailToVariant[k] = v;
    }

    const variantOf = (ev) => {
        if (ev.event_type === 'registrations') {
            const raw = ev.metadata?.variant;
            if (raw !== undefined && raw !== null && raw !== '') {
                const v = String(raw).trim().toUpperCase();
                if (v === 'A' || v === 'B') return v;
            }
        }
        const k = (ev.email || '').toLowerCase();
        if (k && emailToVariant[k]) return emailToVariant[k];
        return 'undetected';
    };

    // ── Pass 2: bucket events into (date, event_type, variant) sets ──
    const sets = {}; // key: "YYYY-MM-DD|event_type|variant" → Set of user keys
    for (const ev of events) {
        let d;
        if (ev.metadata?.webinar_datetime_utc) {
            const cleaned = ev.metadata.webinar_datetime_utc.replace(/(\d+)(st|nd|rd|th)/gi, '$1');
            const parsed = new Date(cleaned + ' UTC');
            d = !isNaN(parsed.getTime())
                ? parsed.toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' })
                : new Date(ev.event_time).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
        } else {
            d = new Date(ev.event_time).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
        }

        let eventKey = ev.event_type;
        if (ev.event_type === 'purchases') {
            const src = ev.metadata?.source || 'Paid Ads';
            eventKey = PURCHASE_DEDUP_MAP[src] || 'purchases_fb';
        } else if (ev.event_type === 'stayeduntil') {
            const minute = Number(ev.metadata?.stayeduntil);
            eventKey = STAYED_DEDUP_MAP[minute];
            if (!eventKey) continue; // skip events with invalid milestone
        }

        const userKey = (ev.email || '').toLowerCase() || ev.phone || (ev.name || '').toLowerCase();
        if (!userKey) continue;

        const variant = variantOf(ev);
        const k = `${d}|${eventKey}|${variant}`;
        if (!sets[k]) sets[k] = new Set();
        sets[k].add(userKey);
    }

    // ── Pass 3: build result with per-variant breakdown + 'all' total ──
    const result = {};
    for (const [k, userSet] of Object.entries(sets)) {
        if (userSet.size === 0) continue;
        const [d, type, variant] = k.split('|');
        if (!result[d]) result[d] = {};
        if (!result[d][type]) result[d][type] = { all: 0, A: 0, B: 0, undetected: 0 };
        result[d][type][variant] = (result[d][type][variant] || 0) + userSet.size;
    }
    for (const d of Object.keys(result)) {
        for (const type of Object.keys(result[d])) {
            const b = result[d][type];
            b.all = (b.A || 0) + (b.B || 0) + (b.undetected || 0);
        }
    }
    return result;
}

async function fetchEventsForDateRange(funnel, minDate, maxDate) {
    const supabase = clientFor(funnel);
    // LA is UTC-7 (PDT) or UTC-8 (PST). Events occurring in the LA evening
    // have UTC timestamps on the next calendar day (e.g. Apr 11 8pm PDT = Apr 12 03:00 UTC).
    // Pad maxDate by 1 day so we don't miss them. The dedup grouping step
    // correctly assigns fetched events to their actual LA date regardless.
    const maxPadded = new Date(maxDate + 'T00:00:00Z');
    maxPadded.setUTCDate(maxPadded.getUTCDate() + 1);
    const maxDatePadded = maxPadded.toISOString().slice(0, 10);

    console.log(`📡 [${funnel}] Supabase fetch: events from ${minDate} to ${maxDatePadded} (padded from ${maxDate})`);

    let allEvents = [];
    // Supabase caps results at 1000 rows per query. Use 1000 as page size
    // and paginate through all results to ensure we get every event.
    const PAGE_SIZE = 1000;
    let page = 0;
    while (true) {
        const { data: batch, error } = await supabase
            .from('events')
            .select('event_type, email, name, phone, event_time, metadata')
            .in('event_type', EVENT_TYPES)
            .gte('event_time', `${minDate}T00:00:00`)
            .lte('event_time', `${maxDatePadded}T23:59:59`)
            .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1);

        if (error) {
            // Throw so /api/metrics surfaces a 500 and the dedup cache stays
            // empty for this date — better than caching a partial result that
            // makes today's row fall back to the raw counter.
            console.error(`❌ Supabase fetch error (page ${page}):`, error.message, error.code);
            throw new Error(`Events fetch failed (page ${page}): ${error.message}`);
        }
        if (!batch || batch.length === 0) break;

        console.log(`   Page ${page}: ${batch.length} events returned`);
        allEvents = allEvents.concat(batch);
        if (batch.length < PAGE_SIZE) break;
        page++;
    }

    console.log(`📡 Total events fetched: ${allEvents.length}`);
    return allEvents;
}

async function getDedupCounts(funnel, dates) {
    if (dates.length === 0) return {};
    const bucket = getCacheBucket(funnel);
    const today = dateToISO(getLADate());

    // Today's dedup expires after 3 seconds to handle concurrent webhook races (#14)
    // Past days are cached forever (their counts can't change)
    if (bucket.dedupCounts[today] && bucket.dedupTimestamps[today]) {
        if (Date.now() - bucket.dedupTimestamps[today] > 3000) {
            delete bucket.dedupCounts[today];
        }
    }

    // Find dates that are NOT in the cache
    const uncachedDates = dates.filter(d => !(d in bucket.dedupCounts));

    if (uncachedDates.length > 0) {
        cache.misses++;

        // Split uncached dates into contiguous ranges to avoid fetching
        // already-cached intermediate dates (#9)
        const sorted = [...uncachedDates].sort();
        const ranges = [];
        let rangeStart = sorted[0];
        let rangePrev = sorted[0];
        for (let i = 1; i < sorted.length; i++) {
            const prevDate = new Date(rangePrev + 'T00:00:00Z');
            const currDate = new Date(sorted[i] + 'T00:00:00Z');
            const gapDays = (currDate - prevDate) / 86400000;
            if (gapDays > 3) {
                // Gap too large — start a new range
                ranges.push([rangeStart, rangePrev]);
                rangeStart = sorted[i];
            }
            rangePrev = sorted[i];
        }
        ranges.push([rangeStart, rangePrev]);

        console.log(`📊 Cache[${funnel}] MISS: ${uncachedDates.length} uncached date(s) across ${ranges.length} range(s)`);

        // Snapshot the epoch before the fetch. If an invalidation runs while
        // we're awaiting Supabase, the epoch will change and we must NOT
        // write the now-stale results back into the cache.
        const epochAtStart = bucket.invalidationEpoch;

        // Fetch each range, log, and cache in a single pass
        const computedByDate = {};
        for (const [minDate, maxDate] of ranges) {
            const events = await fetchEventsForDateRange(funnel, minDate, maxDate);
            const computed = computeDedupFromEvents(events);

            // Log per-day breakdown for diagnostics
            const dayCounts = {};
            for (const [d, counts] of Object.entries(computed)) {
                dayCounts[d] = Object.entries(counts).map(([k, v]) => `${k}:${v}`).join(', ');
            }
            if (Object.keys(dayCounts).length > 0) {
                console.log(`📊 Dedup[${funnel}] [${minDate}→${maxDate}]:`, JSON.stringify(dayCounts));
            }

            // Collect computed results so we can return them even if we
            // skip the cache write below.
            for (const d of uncachedDates) {
                if (d >= minDate && d <= maxDate) {
                    computedByDate[d] = computed[d] || {};
                }
            }
        }

        if (bucket.invalidationEpoch === epochAtStart) {
            for (const [d, counts] of Object.entries(computedByDate)) {
                bucket.dedupCounts[d] = counts;
                if (d === today) bucket.dedupTimestamps[d] = Date.now();
            }
        } else {
            console.log(`⚠️  Cache[${funnel}]: skipped dedup write — invalidation occurred mid-fetch (epoch ${epochAtStart} → ${bucket.invalidationEpoch})`);
        }

        // Prune dedup cache: drop dates older than 120 days to prevent unbounded growth (#5)
        const pruneCutoff = new Date();
        pruneCutoff.setDate(pruneCutoff.getDate() - 120);
        const pruneISO = pruneCutoff.toISOString().slice(0, 10);
        for (const d of Object.keys(bucket.dedupCounts)) {
            if (d < pruneISO) delete bucket.dedupCounts[d];
        }

        // Caller gets accurate numbers regardless of whether we wrote them
        // to the cache. Merge freshly computed values into the result first,
        // then layer cache reads for dates we didn't refetch.
        const result = {};
        for (const d of dates) {
            if (computedByDate[d]) result[d] = computedByDate[d];
            else if (bucket.dedupCounts[d]) result[d] = bucket.dedupCounts[d];
        }
        return result;
    } else {
        cache.hits++;
    }

    // Build result from cache
    const result = {};
    for (const d of dates) {
        if (bucket.dedupCounts[d]) result[d] = bucket.dedupCounts[d];
    }
    return result;
}

// ─── Day Finalization ─────────────────────────────────────────────────────────
// Compute deduped event counts for a date and write them into the canonical
// columns of daily_metrics, then mark the row as finalized. After this runs,
// /api/metrics reads the row directly without consulting the events table.
//
// Safety: a field is overwritten when dedup found events for it, OR when the
// events table has at least one event of the same parent event_type for this
// date (the events table is authoritative for that type, so a recomputed zero
// must be written — otherwise deletions can't propagate). Days with no events
// of a given parent type keep their legacy raw values untouched. The overrides
// JSONB column is never touched.
async function finalizeDailyMetricsForDate(funnel, isoDate) {
    const supabase = clientFor(funnel);
    // 60-day lookback so we capture late-arriving events whose
    // webinar_datetime_utc resolves to isoDate (people register early).
    const target = new Date(isoDate + 'T12:00:00Z');
    const fromDate = new Date(target);
    fromDate.setUTCDate(fromDate.getUTCDate() - 60);
    const toDate = new Date(target);
    toDate.setUTCDate(toDate.getUTCDate() + 1);
    const fromISO = fromDate.toISOString().slice(0, 10);
    const toISO = toDate.toISOString().slice(0, 10);

    const events = await fetchEventsForDateRange(funnel, fromISO, toISO);
    const dedupMap = computeDedupFromEvents(events);
    const counts = dedupMap[isoDate] || {};

    // Which event_types are present in the events table for isoDate. If a
    // parent event_type has at least one row, the events table owns the truth
    // for that type and a recomputed zero must be written (otherwise deleting
    // the last event of a kind leaves the stale canonical column in place).
    // Date assignment mirrors computeDedupFromEvents.
    const eventTypesOnDate = new Set();
    for (const ev of events) {
        let d;
        if (ev.metadata?.webinar_datetime_utc) {
            const cleaned = ev.metadata.webinar_datetime_utc.replace(/(\d+)(st|nd|rd|th)/gi, '$1');
            const parsed = new Date(cleaned + ' UTC');
            d = !isNaN(parsed.getTime())
                ? parsed.toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' })
                : new Date(ev.event_time).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
        } else {
            d = new Date(ev.event_time).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
        }
        if (d === isoDate) eventTypesOnDate.add(ev.event_type);
    }

    const FIELDS = [
        'registrations', 'attended', 'replays', 'viewedcta', 'clickedcta',
        'purchases_fb', 'purchases_native', 'purchases_youtube',
        'purchases_aibot', 'purchases_postwebinar', 'purchases_cpa',
        'stayed_45', 'stayed_60', 'stayed_80',
    ];

    const FIELD_PARENT = {
        registrations: 'registrations', attended: 'attended', replays: 'replays',
        viewedcta: 'viewedcta', clickedcta: 'clickedcta',
        purchases_fb: 'purchases', purchases_native: 'purchases',
        purchases_youtube: 'purchases', purchases_aibot: 'purchases',
        purchases_postwebinar: 'purchases', purchases_cpa: 'purchases',
        stayed_45: 'stayeduntil', stayed_60: 'stayeduntil', stayed_80: 'stayeduntil',
    };

    const updates = { finalized_at: new Date().toISOString() };
    const written = {};
    for (const f of FIELDS) {
        // counts[f] is the per-variant breakdown; canonical columns store the total
        const v = Number(counts[f]?.all) || 0;
        const parentAuthoritative = eventTypesOnDate.has(FIELD_PARENT[f]);
        if (v > 0 || parentAuthoritative) {
            updates[f] = v;
            written[f] = v;
        }
    }

    // Ensure the row exists. Create with day_of_week if not.
    const { data: existing } = await supabase
        .from('daily_metrics').select('date').eq('date', isoDate).maybeSingle();
    if (!existing) {
        const dayOfWeek = new Date(isoDate + 'T12:00:00Z').toLocaleDateString('en-US', {
            weekday: 'long', timeZone: 'America/Los_Angeles',
        });
        await supabase.from('daily_metrics')
            .upsert({ date: isoDate, day_of_week: dayOfWeek }, { onConflict: 'date' });
    }

    const { error } = await supabase.from('daily_metrics')
        .update(updates).eq('date', isoDate);
    if (error) throw error;

    invalidateMetricsCache(funnel);
    invalidateInsightsCache(funnel);
    delete getCacheBucket(funnel).dedupCounts[isoDate];

    return { date: isoDate, written };
}

// GET /api/metrics — Fetch all daily metrics (with caching)
app.get('/api/metrics', dashboardLimiter, async (req, res) => {
    try {
        const funnel = resolveFunnel(req, 'analytics');
        const supabase = clientFor(funnel);
        const bucket = getCacheBucket(funnel);
        const { limit = 90, offset = 0, variant: variantRaw = 'all' } = req.query;
        const ALLOWED_VARIANTS = ['all', 'A', 'B', 'undetected'];
        const variant = ALLOWED_VARIANTS.includes(String(variantRaw)) ? String(variantRaw) : 'all';

        // ── Response cache: only used for variant='all' (default view) ────
        const now = Date.now();
        if (variant === 'all' && bucket.metricsResponse && (now - bucket.metricsUpdatedAt) < cache.metricsTTL) {
            // Only serve cache if request params match (default pagination)
            if (Number(limit) === 90 && Number(offset) === 0) {
                cache.hits++;
                return res.json(bucket.metricsResponse);
            }
        }

        // Snapshot the epoch before the Supabase fetch + dedup join so we
        // can detect a mid-flight invalidation and skip the cache write.
        const epochAtStart = bucket.invalidationEpoch;

        const { data, error, count } = await supabase
            .from('daily_metrics')
            .select('*', { count: 'exact' })
            .lte('date', dateToISO(getLADate()))  // hide future dates
            .order('date', { ascending: false })
            .range(Number(offset), Number(offset) + Number(limit) - 1);

        if (error) throw error;

        // ── Dedup counts ──────────────────────────────────────────────────
        // For variant='all', skip dedup on finalized rows (canonical columns
        // hold the total). For variant filtering, ALWAYS run dedup since the
        // canonical columns don't carry per-variant breakdowns.
        const dates = (data || [])
            .filter(r => variant !== 'all' || !r.finalized_at)
            .map(r => String(r.date).substring(0, 10));
        const dedupMap = await getDedupCounts(funnel, dates);

        // Convert to frontend format (MM/DD/YYYY)
        // Priority: manual override > deduped count > raw daily_metrics
        const formatted = (data || []).map(row => {
            const dateStr = String(row.date).substring(0, 10);
            const mmddyyyy = dateStr.replace(/(\d{4})-(\d{2})-(\d{2})/, '$2/$3/$1');
            const deduped = dedupMap[dateStr] || {};
            const hasDedup = Object.keys(deduped).length > 0;
            const ov = row.overrides || {};
            // Once dedup has run for a date (hasDedup), it is authoritative for
            // every event-type field. A missing key means 0 events — never fall
            // through to the raw daily_metrics column, which can be ahead of
            // dedup briefly (causing the today-row to flicker high → low) or
            // contain stale/legacy values from batch upserts.
            const DEDUP_COLS = new Set([
                'registrations', 'attended', 'replays', 'viewedcta', 'clickedcta',
                'purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_postwebinar', 'purchases_cpa',
                'stayed_45', 'stayed_60', 'stayed_80',
            ]);
            const pick = (field) => {
                // Overrides (admin manual edits) win — but only apply to the
                // 'all' view since they're variant-blind totals.
                if (variant === 'all' && ov[field] !== undefined) return ov[field];
                const dd = deduped[field];
                if (dd !== undefined) return dd[variant] ?? 0;
                if (hasDedup && DEDUP_COLS.has(field)) return 0;
                // For variant filtering, canonical row values are totals across
                // all variants — not meaningful for a single-variant view.
                if (variant !== 'all' && DEDUP_COLS.has(field)) return 0;
                return row[field];
            };
            return {
                date: mmddyyyy,
                day: row.day_of_week,
                fb_spend: ov.fb_spend !== undefined ? ov.fb_spend : Number(row.fb_spend),
                fb_link_clicks: ov.fb_link_clicks !== undefined ? ov.fb_link_clicks : Number(row.fb_link_clicks || 0),
                registrations: pick('registrations'),
                replays: pick('replays'),
                viewedcta: pick('viewedcta'),
                clickedcta: pick('clickedcta'),
                purchases_fb: pick('purchases_fb'),
                purchases_native: pick('purchases_native'),
                purchases_youtube: pick('purchases_youtube'),
                purchases_aibot: pick('purchases_aibot'),
                purchases_postwebinar: pick('purchases_postwebinar'),
                purchases_cpa: pick('purchases_cpa'),
                stayed_45: pick('stayed_45'),
                stayed_60: pick('stayed_60'),
                stayed_80: pick('stayed_80'),
                total_purchases: (pick('purchases_fb') || 0) + (pick('purchases_native') || 0) +
                                 (pick('purchases_youtube') || 0) + (pick('purchases_aibot') || 0) +
                                 (pick('purchases_postwebinar') || 0) + (pick('purchases_cpa') || 0),
                // 'purchases' is an alias for total_purchases (backward compat for custom formulas)
                purchases: (pick('purchases_fb') || 0) + (pick('purchases_native') || 0) +
                           (pick('purchases_youtube') || 0) + (pick('purchases_aibot') || 0) +
                           (pick('purchases_postwebinar') || 0) + (pick('purchases_cpa') || 0),
                attended: pick('attended'),
                created_at: row.created_at,
                updated_at: row.updated_at,
            };
        });

        const response = { data: formatted, total: count };

        // ── Store in response cache (only for default pagination) ────────
        // Skip the write if an invalidation ran mid-flight — the response
        // we just built reflects pre-invalidation Supabase state and would
        // mask the new data for up to metricsTTL.
        if (variant === 'all' && Number(limit) === 90 && Number(offset) === 0) {
            if (bucket.invalidationEpoch === epochAtStart) {
                bucket.metricsResponse = response;
                bucket.metricsUpdatedAt = now;
            } else {
                console.log(`⚠️  Cache[${funnel}]: skipped metricsResponse write — invalidation occurred mid-fetch (epoch ${epochAtStart} → ${bucket.invalidationEpoch})`);
            }
        }

        res.json(response);

    } catch (err) {
        console.error('❌ GET /api/metrics error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// PUT /api/metrics/:date — Update a specific day (from dashboard edit)
// Saves edited values into the 'overrides' column so they permanently take precedence
app.put('/api/metrics/:date', dashboardLimiter, requireAuth, requireAdmin, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const dateInput = parseDateInput(req.params.date);
        if (!dateInput) return res.status(400).json({ error: 'Invalid date' });

        const isoDate = dateToISO(dateInput);
        const body = req.body;

        // Only write to overrides — raw columns stay as the automated data source.
        // This prevents overrides from drifting separately from raw columns (#6).
        const OVERRIDE_FIELDS = ['fb_spend', 'fb_link_clicks', 'registrations', 'replays', 'viewedcta', 'clickedcta', 'purchases', 'attended', 'purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_postwebinar', 'purchases_cpa', 'stayed_45', 'stayed_60', 'stayed_80'];
        const newOverrides = {};
        for (const f of OVERRIDE_FIELDS) {
            if (body[f] !== undefined) {
                newOverrides[f] = (f === 'fb_spend') ? parseFloat(body[f]) || 0 : parseInt(body[f]) || 0;
            }
        }

        // Merge with existing overrides (don't wipe out other fields)
        const { data: existing } = await supabase.from('daily_metrics').select('overrides').eq('date', isoDate).single();
        const mergedOverrides = { ...(existing?.overrides || {}), ...newOverrides };

        const { data, error } = await supabase
            .from('daily_metrics')
            .update({ overrides: mergedOverrides })
            .eq('date', isoDate)
            .select()
            .single();

        if (error) throw error;
        invalidateMetricsCache(req.funnel);
        invalidateInsightsCache(req.funnel);
        res.json({ success: true, data });

    } catch (err) {
        console.error('❌ PUT /api/metrics error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// DELETE /api/metrics/:date
app.delete('/api/metrics/:date', dashboardLimiter, requireAuth, requireAdmin, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const dateInput = parseDateInput(req.params.date);
        if (!dateInput) return res.status(400).json({ error: 'Invalid date' });

        const isoDate = dateToISO(dateInput);
        const { error } = await supabase
            .from('daily_metrics')
            .delete()
            .eq('date', isoDate);

        if (error) throw error;
        invalidateDedupForDate(req.funnel, isoDate);
        invalidateInsightsCache(req.funnel);
        res.json({ success: true, deleted: dateInput });

    } catch (err) {
        console.error('❌ DELETE /api/metrics error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});


// =============================================================================
// CUSTOM METRICS ENDPOINTS
// =============================================================================

// GET /api/custom-metrics
app.get('/api/custom-metrics', dashboardLimiter, async (req, res) => {
    try {
        const supabase = clientFor(resolveFunnel(req, 'analytics'));
        const { data, error } = await supabase
            .from('custom_metrics')
            .select('*')
            .order('sort_order', { ascending: true });

        if (error) throw error;
        res.json({ data: data || [] });

    } catch (err) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

// POST /api/custom-metrics
app.post('/api/custom-metrics', dashboardLimiter, requireAuth, requireAdmin, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { name, formula, format = 'number' } = req.body;
        if (!name || !formula) {
            return res.status(400).json({ error: 'name and formula required' });
        }

        const { data, error } = await supabase
            .from('custom_metrics')
            .insert({ name, formula, format })
            .select()
            .single();

        if (error) throw error;
        res.json({ success: true, data });

    } catch (err) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

// PUT /api/custom-metrics/:id
app.put('/api/custom-metrics/:id', dashboardLimiter, requireAuth, requireAdmin, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { name, formula, format } = req.body;
        const updates = {};
        if (name) updates.name = name;
        if (formula) updates.formula = formula;
        if (format) updates.format = format;

        const { data, error } = await supabase
            .from('custom_metrics')
            .update(updates)
            .eq('id', req.params.id)
            .select()
            .single();

        if (error) throw error;
        res.json({ success: true, data });

    } catch (err) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

// DELETE /api/custom-metrics/:id
app.delete('/api/custom-metrics/:id', dashboardLimiter, requireAuth, requireAdmin, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { error } = await supabase
            .from('custom_metrics')
            .delete()
            .eq('id', req.params.id);

        if (error) throw error;
        res.json({ success: true });

    } catch (err) {
        res.status(500).json({ error: 'Internal server error' });
    }
});


// =============================================================================
// EVENTS LOG
// =============================================================================

app.get('/api/events', dashboardLimiter, async (req, res) => {
    try {
        const supabase = clientFor(resolveFunnel(req, 'analytics'));
        const { limit = 100, offset = 0, type } = req.query;
        let query = supabase
            .from('events')
            .select('*')
            .order('event_time', { ascending: false })
            .range(Number(offset), Number(offset) + Number(limit) - 1);

        if (type) query = query.eq('event_type', type);

        const { data, error } = await query;
        if (error) throw error;
        res.json({ data: data || [] });

    } catch (err) {
        console.error('❌ GET /api/events error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// =============================================================================
// WEBHOOK LOG (for debugging)
// =============================================================================

app.get('/api/webhook-log', dashboardLimiter, async (req, res) => {
    try {
        const supabase = clientFor(resolveFunnel(req, 'analytics'));
        const { limit = 50 } = req.query;
        const { data, error } = await supabase
            .from('webhook_log')
            .select('*')
            .order('created_at', { ascending: false })
            .limit(Number(limit));

        if (error) throw error;
        res.json({ data: data || [] });

    } catch (err) {
        res.status(500).json({ error: 'Internal server error' });
    }
});


// =============================================================================
// CACHE MANAGEMENT
// =============================================================================

// POST /api/cache/clear — Authenticated: flush this funnel's caches (use after direct DB edits)
app.post('/api/cache/clear', requireAuth, async (req, res) => {
    const bucket = getCacheBucket(req.funnel);
    const daysBefore = Object.keys(bucket.dedupCounts).length;
    bucket.dedupCounts = {};
    bucket.dedupTimestamps = {};
    invalidateMetricsCache(req.funnel);
    invalidateInsightsCache(req.funnel);
    console.log(`🧹 Cache[${req.funnel}]: full clear by ${req.user.email} (${daysBefore} days flushed)`);
    res.json({ success: true, message: `Cache cleared (${daysBefore} days flushed)` });
});

// POST /api/admin/finalize-past-days — One-shot backfill.
// Walks every daily_metrics row with date < today and finalized_at IS NULL,
// runs finalizeDailyMetricsForDate on each, and reports the result. Safe to
// re-run: rows already finalized are skipped. The 4:05 AM cron handles
// each new yesterday going forward, so this only needs to run once.
//
// Optional body: { force: true } — re-finalize rows even if finalized_at is set.
// POST /api/admin/finalize-date — Re-finalize a single day. Recomputes the
// dedup counts from events and overwrites the canonical columns (and
// finalized_at) in daily_metrics. Use after editing/inserting events for
// a past day. Body: { date: "YYYY-MM-DD" or "MM/DD/YYYY" }
app.post('/api/admin/finalize-date', requireAuth, requireAdmin, async (req, res) => {
    try {
        const dateInput = parseDateInput(req.body?.date);
        if (!dateInput) return res.status(400).json({ error: 'Send { date: "YYYY-MM-DD" or "MM/DD/YYYY" }' });
        const isoDate = dateToISO(dateInput);
        const result = await finalizeDailyMetricsForDate(req.funnel, isoDate);
        console.log(`🧊 [${req.funnel}] Finalize ${isoDate} by ${req.user.email}: ${JSON.stringify(result.written)}`);
        res.json({ success: true, ...result });
    } catch (err) {
        console.error('❌ POST /api/admin/finalize-date error:', err.message);
        res.status(500).json({ error: 'Finalize failed', detail: err.message });
    }
});

app.post('/api/admin/finalize-past-days', requireAuth, requireAdmin, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { force = false } = req.body || {};
        const todayISO = dateToISO(getLADate());

        let q = supabase.from('daily_metrics')
            .select('date, finalized_at')
            .lt('date', todayISO)
            .order('date', { ascending: true });
        if (!force) q = q.is('finalized_at', null);

        const { data: rows, error } = await q;
        if (error) throw error;

        const results = [];
        for (const row of rows || []) {
            const isoDate = String(row.date).substring(0, 10);
            try {
                const r = await finalizeDailyMetricsForDate(req.funnel, isoDate);
                results.push({ date: isoDate, ok: true, written: r.written });
            } catch (err) {
                console.error(`❌ Finalize ${isoDate} failed:`, err.message);
                results.push({ date: isoDate, ok: false, error: err.message });
            }
        }

        const okCount = results.filter(r => r.ok).length;
        console.log(`🧊 Backfill[${req.funnel}]: finalized ${okCount}/${results.length} past days (force=${force}) by ${req.user.email}`);
        res.json({ total: results.length, finalized: okCount, results });
    } catch (err) {
        console.error('❌ POST /api/admin/finalize-past-days error:', err.message);
        res.status(500).json({ error: 'Backfill failed', detail: err.message });
    }
});

// =============================================================================
// HEALTH CHECK
// =============================================================================

app.get('/api/health', async (req, res) => {
    try {
        // Health-check the analytics schema as the canonical liveness probe.
        const { error } = await supabasePublic.from('daily_metrics').select('id').limit(1);
        const cachePerFunnel = {};
        for (const [funnel, b] of Object.entries(cache.byFunnel)) {
            cachePerFunnel[funnel] = {
                dedup_days_cached: Object.keys(b.dedupCounts).length,
                metrics_cached: !!b.metricsResponse,
                metrics_age_sec: b.metricsUpdatedAt ? Math.round((Date.now() - b.metricsUpdatedAt) / 1000) : null,
                insights_cached: !!b.insightsContext,
            };
        }
        res.json({
            status: 'ok',
            timestamp: new Date().toISOString(),
            timezone: 'America/Los_Angeles',
            la_time: new Date().toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }),
            database: error ? 'error' : 'connected',
            cache: {
                per_funnel: cachePerFunnel,
                hits: cache.hits,
                misses: cache.misses,
                hit_rate: (cache.hits + cache.misses) > 0 ? `${Math.round(cache.hits / (cache.hits + cache.misses) * 100)}%` : 'N/A',
            },
        });
    } catch {
        res.status(503).json({ status: 'error', database: 'disconnected' });
    }
});


// =============================================================================
// FACEBOOK AD SPEND SYNC (every 30 minutes)
// =============================================================================

// Cron: at minute 0 and 30 of every hour, in LA timezone
// This pulls today's cumulative spend from Facebook and writes it to Supabase
if (process.env.FB_ACCESS_TOKEN && process.env.FB_AD_ACCOUNT_ID) {
    cron.schedule('0,30 * * * *', async () => {
        console.log('🔄 FB sync cron triggered (analytics funnel only)');
        try {
            await syncFacebookSpend();
            invalidateMetricsCache('analytics');
            invalidateInsightsCache('analytics');
        } catch (err) {
            console.error('❌ FB sync cron failed:', err.message);
        }
    }, { timezone: 'America/Los_Angeles' });

    // Helper: yesterday in LA as YYYY-MM-DD.
    const computeYesterdayLA = () => {
        const now = new Date();
        const y = new Date(now.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }));
        y.setDate(y.getDate() - 1);
        return `${y.getFullYear()}-${String(y.getMonth() + 1).padStart(2, '0')}-${String(y.getDate()).padStart(2, '0')}`;
    };

    // Cron: daily at 4:00 AM PST — fetch *yesterday's* final ad spend
    // By 4 AM the previous day's data is fully settled in Facebook's reporting
    cron.schedule('0 4 * * *', async () => {
        try {
            const yesterdayISO = computeYesterdayLA();
            console.log(`🌙 Daily 4 AM cron: fetching final ad insights for ${yesterdayISO} (analytics)`);
            const insights = await fetchFacebookInsights(yesterdayISO);
            await writeInsightsToSupabase(yesterdayISO, insights);
            invalidateMetricsCache('analytics');
            invalidateInsightsCache('analytics');
            console.log(`✅ Daily 4 AM cron: $${insights.spend.toFixed(2)}, ${insights.linkClicks} link clicks written for ${yesterdayISO}`);
        } catch (err) {
            console.error('❌ Daily 4 AM ad-spend cron failed:', err.message);
        }
    }, { timezone: 'America/Los_Angeles' });

    console.log('📡 Facebook ad spend sync enabled (every 30 min + daily 4 AM previous-day)');
} else {
    console.log('⚠️  Facebook sync disabled — set FB_ACCESS_TOKEN and FB_AD_ACCOUNT_ID to enable');
}

// Helper: yesterday in LA as YYYY-MM-DD (used by finalize cron below; also
// defined inside the FB block above for the FB-only crons).
const _computeYesterdayLA = () => {
    const now = new Date();
    const y = new Date(now.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }));
    y.setDate(y.getDate() - 1);
    return `${y.getFullYear()}-${String(y.getMonth() + 1).padStart(2, '0')}-${String(y.getDate()).padStart(2, '0')}`;
};

// Cron: daily at 4:05 AM PST — finalize yesterday's deduped event counts
// into the canonical daily_metrics columns for EVERY funnel. Runs regardless
// of FB config (the dedup engine doesn't depend on FB).
cron.schedule('5 4 * * *', async () => {
    const yesterdayISO = _computeYesterdayLA();
    for (const funnel of ALLOWED_FUNNELS) {
        try {
            console.log(`🧊 [${funnel}] Daily 4:05 AM cron: finalizing daily_metrics for ${yesterdayISO}`);
            const result = await finalizeDailyMetricsForDate(funnel, yesterdayISO);
            console.log(`✅ [${funnel}] Finalized ${yesterdayISO}: ${JSON.stringify(result.written)}`);
        } catch (err) {
            console.error(`❌ [${funnel}] Daily 4:05 AM finalize cron failed:`, err.message);
        }
    }
}, { timezone: 'America/Los_Angeles' });

// Manual trigger — force a FB sync right now (analytics-only since FB is
// not used by the native funnel; Taboola integration is future work)
app.post('/api/fb-sync', webhookLimiter, authenticateWebhook, async (req, res) => {
    try {
        const result = await syncFacebookSpend();
        if (!result) {
            return res.status(400).json({ error: 'FB sync not configured — check FB_ACCESS_TOKEN and FB_AD_ACCOUNT_ID' });
        }
        invalidateMetricsCache('analytics');
        invalidateInsightsCache('analytics');
        res.json({ success: true, ...result });
    } catch (err) {
        res.status(500).json({ error: 'FB sync failed', detail: err.message });
    }
});

// GET /api/fb-sync/status — check if sync is configured and last run.
// FB sync is analytics-only so we read from the analytics webhook_log.
app.get('/api/fb-sync/status', dashboardLimiter, async (req, res) => {
    const configured = !!(process.env.FB_ACCESS_TOKEN && process.env.FB_AD_ACCOUNT_ID);

    let lastSync = null;
    if (configured) {
        const { data } = await clientFor('analytics')
            .from('webhook_log')
            .select('*')
            .eq('source', 'fb-sync')
            .order('created_at', { ascending: false })
            .limit(1);
        lastSync = data?.[0] || null;
    }

    res.json({
        configured,
        account_id: configured ? process.env.FB_AD_ACCOUNT_ID : null,
        schedule: 'Every 30 minutes',
        timezone: 'America/Los_Angeles',
        last_sync: lastSync,
    });
});

// =============================================================================
// ADMIN — Visual Query Builder
// =============================================================================

app.post('/api/admin/query', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        // Verify admin role within the active funnel
        const { data: roleData } = await supabase.from('user_roles').select('role').eq('user_id', req.user.id).single();
        if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });

        const { table = 'events', dateFrom, dateTo, eventType, search, sortBy, sortDir = 'desc', limit = 500 } = req.body;

        // Only allow querying safe tables
        const ALLOWED_TABLES = ['events', 'daily_metrics', 'dashboard'];
        if (!ALLOWED_TABLES.includes(table)) return res.status(400).json({ error: 'Invalid table' });

        // ── Dashboard Events: the actual deduped people the dashboard counted ──
        if (table === 'dashboard') {
            // Fetch events for the requested date range
            const fetchFrom = dateFrom || '2020-01-01';
            const fetchTo = dateTo || dateToISO(getLADate());
            const events = await fetchEventsForDateRange(req.funnel, fetchFrom, fetchTo);

            // Apply the same dedup logic as computeDedupFromEvents, but keep the winning rows
            const seen = {}; // key: "YYYY-MM-DD|eventKey|userKey" → first event
            const dedupedEvents = [];

            for (const ev of events) {
                // Compute dashboard-attributed date (same logic as computeDedupFromEvents)
                let dashDate;
                if (ev.metadata?.webinar_datetime_utc) {
                    const cleaned = ev.metadata.webinar_datetime_utc.replace(/(\d+)(st|nd|rd|th)/gi, '$1');
                    const parsed = new Date(cleaned + ' UTC');
                    dashDate = !isNaN(parsed.getTime())
                        ? parsed.toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' })
                        : new Date(ev.event_time).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
                } else {
                    dashDate = new Date(ev.event_time).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
                }

                // Compute event key (purchases split by source)
                let eventKey = ev.event_type;
                if (ev.event_type === 'purchases') {
                    const src = ev.metadata?.source || 'Paid Ads';
                    eventKey = PURCHASE_DEDUP_MAP[src] || 'purchases_fb';
                }

                // Date filter: only include events attributed to the requested range
                if (dateFrom && dashDate < dateFrom) continue;
                if (dateTo && dashDate > dateTo) continue;

                // Event type filter
                if (eventType && ev.event_type !== eventType) continue;

                // Dedup: first unique user per date+eventKey wins
                const userKey = (ev.email || '').toLowerCase() || ev.phone || (ev.name || '').toLowerCase();
                if (!userKey) continue; // anonymous events can't be deduped

                const dedupKey = `${dashDate}|${eventKey}|${userKey}`;
                if (seen[dedupKey]) continue;
                seen[dedupKey] = true;

                dedupedEvents.push({
                    dashboard_date: dashDate,
                    event_type: ev.event_type,
                    source: ev.event_type === 'purchases' ? (ev.metadata?.source || 'Paid Ads') : null,
                    name: ev.name,
                    email: ev.email,
                    phone: ev.phone,
                    event_time: ev.event_time,
                });
            }

            // Sort
            const sField = sortBy || 'dashboard_date';
            dedupedEvents.sort((a, b) => {
                const av = a[sField] || '', bv = b[sField] || '';
                return sortDir === 'asc' ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
            });

            const capped = dedupedEvents.slice(0, Math.min(Number(limit) || 500, 5000));
            return res.json({ data: capped, count: dedupedEvents.length });
        }

        let query = supabase.from(table).select('*');

        if (table === 'events') {
            // Convert LA date boundaries to UTC so query matches how the dashboard counts
            const toLA_UTC = (dateStr, time) => {
                const d = new Date(`${dateStr}T${time}`);
                const utc = new Date(d.toLocaleString('en-US', { timeZone: 'UTC' }));
                const la = new Date(d.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }));
                const offset = (utc - la) / 60000;
                const offsetH = String(Math.floor(Math.abs(offset) / 60)).padStart(2, '0');
                const offsetM = String(Math.abs(offset) % 60).padStart(2, '0');
                const sign = offset <= 0 ? '+' : '-';
                return `${dateStr}T${time}${sign}${offsetH}:${offsetM}`;
            };
            if (dateFrom) query = query.gte('event_time', toLA_UTC(dateFrom, '00:00:00'));
            if (dateTo) query = query.lte('event_time', toLA_UTC(dateTo, '23:59:59'));
            if (eventType) query = query.eq('event_type', eventType);
            if (search) query = query.or(`name.ilike.%${search}%,email.ilike.%${search}%,phone.ilike.%${search}%`);
            query = query.order(sortBy || 'event_time', { ascending: sortDir === 'asc' });
        } else if (table === 'daily_metrics') {
            if (dateFrom) query = query.gte('date', dateFrom);
            if (dateTo) query = query.lte('date', dateTo);
            query = query.order(sortBy || 'date', { ascending: sortDir === 'asc' });
        }

        query = query.limit(Math.min(Number(limit) || 500, 5000));
        const { data, error } = await query;
        if (error) throw error;

        res.json({ data: data || [], count: data?.length || 0 });
    } catch (err) {
        console.error('❌ POST /api/admin/query error:', err.message);
        res.status(500).json({ error: 'Query failed' });
    }
});

// =============================================================================
// INSIGHTS — AI Business Analyst (Claude API)
// =============================================================================

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

// ─── AI Insights: data helpers (used by tools) ────────────────────────────────

// Build dedup-aware daily metrics for ALL history, cached per funnel.
// Multiple tool calls in one chat (or chat session) reuse the cache.
async function loadAllInsightsMetrics(funnel) {
    const supabase = clientFor(funnel);
    const bucket = getCacheBucket(funnel);
    const now = Date.now();

    if (bucket.insightsContext?.metrics && (now - bucket.insightsUpdatedAt) < cache.insightsTTL) {
        return bucket.insightsContext.metrics;
    }

    const epochAtStart = bucket.invalidationEpoch;
    // No limit — daily_metrics is one row per day, dataset is tiny.
    const metricsRes = await supabase
        .from('daily_metrics')
        .select('*')
        .order('date', { ascending: false });

    const rawRows = metricsRes.data || [];
    const dates = rawRows.filter(r => !r.finalized_at).map(r => String(r.date).substring(0, 10));
    const dedupMap = await getDedupCounts(funnel, dates);

    const PURCHASE_SUB_COLS = new Set(['purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_postwebinar', 'purchases_cpa', 'stayed_45', 'stayed_60', 'stayed_80']);
    const metrics = rawRows.map(r => {
        const dateStr = String(r.date).substring(0, 10);
        const deduped = dedupMap[dateStr] || {};
        const hasDedup = Object.keys(deduped).length > 0;
        const ov = r.overrides || {};
        const pick = (field) => {
            if (ov[field] !== undefined) return ov[field];
            const dd = deduped[field];
            if (dd !== undefined) return dd.all ?? 0;
            if (hasDedup && PURCHASE_SUB_COLS.has(field)) return 0;
            return r[field];
        };
        return {
            date: dateStr,
            day: r.day_of_week,
            fb_spend: ov.fb_spend !== undefined ? ov.fb_spend : Number(r.fb_spend),
            fb_link_clicks: ov.fb_link_clicks !== undefined ? ov.fb_link_clicks : Number(r.fb_link_clicks || 0),
            registrations: pick('registrations'),
            attended: pick('attended'),
            replays: pick('replays'),
            viewedcta: pick('viewedcta'),
            clickedcta: pick('clickedcta'),
            purchases_fb: pick('purchases_fb') || 0,
            purchases_native: pick('purchases_native') || 0,
            purchases_youtube: pick('purchases_youtube') || 0,
            purchases_aibot: pick('purchases_aibot') || 0,
            purchases_postwebinar: pick('purchases_postwebinar') || 0,
            purchases_cpa: pick('purchases_cpa') || 0,
            stayed_45: pick('stayed_45') || 0,
            stayed_60: pick('stayed_60') || 0,
            stayed_80: pick('stayed_80') || 0,
            total_purchases: (pick('purchases_fb') || 0) + (pick('purchases_native') || 0) +
                             (pick('purchases_youtube') || 0) + (pick('purchases_aibot') || 0) +
                             (pick('purchases_postwebinar') || 0) + (pick('purchases_cpa') || 0),
        };
    });

    if (bucket.invalidationEpoch === epochAtStart) {
        bucket.insightsContext = { ...(bucket.insightsContext || {}), metrics };
        bucket.insightsUpdatedAt = now;
    }

    return metrics;
}

async function getInsightsMetrics(funnel, from, to) {
    const all = await loadAllInsightsMetrics(funnel);
    return all.filter(r => (!from || r.date >= from) && (!to || r.date <= to));
}

// Aggregates daily metrics into weekly or monthly rollups. Sums additive
// columns; recomputes ratios from the sums so they stay correct.
async function getInsightsRollup(funnel, period, from, to) {
    const rows = await getInsightsMetrics(funnel, from, to);
    if (rows.length === 0) return [];

    const bucketKey = (dateStr) => {
        if (period === 'month') return dateStr.substring(0, 7);            // YYYY-MM
        if (period === 'week') {
            // ISO-ish: week starts Monday in LA tz; use Sunday-anchored start for simplicity
            const d = new Date(dateStr + 'T12:00:00Z');
            const day = d.getUTCDay(); // 0=Sun
            const diff = -day;          // shift back to Sunday
            d.setUTCDate(d.getUTCDate() + diff);
            return d.toISOString().slice(0, 10);                           // YYYY-MM-DD (Sunday)
        }
        return dateStr;
    };

    const buckets = new Map();
    const ADDITIVE = ['fb_spend','fb_link_clicks','registrations','attended','replays','viewedcta','clickedcta','purchases_fb','purchases_native','purchases_youtube','purchases_aibot','purchases_postwebinar','purchases_cpa','stayed_45','stayed_60','stayed_80','total_purchases'];

    for (const r of rows) {
        const k = bucketKey(r.date);
        if (!buckets.has(k)) {
            const init = { period_start: k, days: 0 };
            ADDITIVE.forEach(c => init[c] = 0);
            buckets.set(k, init);
        }
        const b = buckets.get(k);
        b.days++;
        ADDITIVE.forEach(c => b[c] += Number(r[c]) || 0);
    }

    return [...buckets.values()]
        .sort((a, b) => b.period_start.localeCompare(a.period_start))
        .map(b => ({
            ...b,
            cpa:          b.total_purchases > 0 ? b.fb_spend / b.total_purchases : null,
            cost_per_reg: b.registrations > 0   ? b.fb_spend / b.registrations   : null,
            landing_cvr:  b.fb_link_clicks > 0  ? b.registrations / b.fb_link_clicks * 100 : null,
            attendance:   b.registrations > 0   ? b.attended / b.registrations * 100       : null,
            cta_view:     (b.attended + b.replays) > 0 ? b.viewedcta / (b.attended + b.replays) * 100 : null,
            cta_click:    b.viewedcta > 0       ? b.clickedcta / b.viewedcta * 100         : null,
            conversion:   b.clickedcta > 0      ? b.total_purchases / b.clickedcta * 100   : null,
        }));
}

// Side-by-side comparison of two date ranges with totals and deltas.
async function compareInsightsPeriods(funnel, aFrom, aTo, bFrom, bTo) {
    const [a, b] = await Promise.all([
        getInsightsMetrics(funnel, aFrom, aTo),
        getInsightsMetrics(funnel, bFrom, bTo),
    ]);

    const ADDITIVE = ['fb_spend','fb_link_clicks','registrations','attended','replays','viewedcta','clickedcta','purchases_fb','purchases_native','purchases_youtube','purchases_aibot','purchases_postwebinar','purchases_cpa','total_purchases'];
    const sumOf = (rows) => {
        const t = { days: rows.length };
        ADDITIVE.forEach(c => t[c] = rows.reduce((s, r) => s + (Number(r[c]) || 0), 0));
        t.cpa = t.total_purchases > 0 ? t.fb_spend / t.total_purchases : null;
        t.landing_cvr = t.fb_link_clicks > 0 ? t.registrations / t.fb_link_clicks * 100 : null;
        t.attendance  = t.registrations > 0  ? t.attended / t.registrations * 100 : null;
        t.cta_click   = t.viewedcta > 0      ? t.clickedcta / t.viewedcta * 100 : null;
        t.conversion  = t.clickedcta > 0     ? t.total_purchases / t.clickedcta * 100 : null;
        return t;
    };

    const period_a = sumOf(a);
    const period_b = sumOf(b);

    const delta = {};
    [...ADDITIVE, 'cpa', 'landing_cvr', 'attendance', 'cta_click', 'conversion'].forEach(k => {
        const av = period_a[k];
        const bv = period_b[k];
        if (av == null || bv == null) { delta[k] = null; return; }
        delta[k] = {
            absolute: av - bv,
            pct_change: bv !== 0 ? ((av - bv) / Math.abs(bv)) * 100 : null,
        };
    });

    return {
        period_a: { from: aFrom, to: aTo, ...period_a },
        period_b: { from: bFrom, to: bTo, ...period_b },
        delta,
    };
}

// Per-day event counts for a date range, queried fresh from DB (no PII).
async function getInsightsEventCounts(funnel, from, to, eventType) {
    const supabase = clientFor(funnel);
    let q = supabase.from('events').select('event_type, event_time');
    if (from) q = q.gte('event_time', from + 'T00:00:00-08:00');
    if (to)   q = q.lt('event_time', to + 'T00:00:00-08:00' /* exclusive next day */);
    if (eventType) q = q.eq('event_type', eventType);
    q = q.limit(50000);

    const { data, error } = await q;
    if (error) throw error;

    const counts = {};
    (data || []).forEach(e => {
        const laDate = new Date(e.event_time).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
        counts[laDate] = counts[laDate] || {};
        counts[laDate][e.event_type] = (counts[laDate][e.event_type] || 0) + 1;
    });
    return counts;
}

// Strict read-only SQL guard. Allows a single SELECT or WITH ... SELECT, no
// DDL/DML keywords, no statement chaining. This is the primary defense; the
// DB function adds statement_timeout + row cap as belt-and-suspenders.
function isReadOnlySQL(sql) {
    if (typeof sql !== 'string') return false;
    const trimmed = sql.trim().replace(/;+\s*$/, ''); // strip trailing semicolons
    if (trimmed.length === 0) return false;
    if (trimmed.includes(';')) return false; // no statement chaining
    const lower = trimmed.toLowerCase();
    if (!lower.startsWith('select') && !lower.startsWith('with')) return false;
    // Block dangerous keywords as whole words. Conservative: rejects them
    // even in column/table names, which is fine for an analyst's tool.
    const dangerous = /\b(insert|update|delete|drop|alter|create|grant|revoke|truncate|copy|merge|do|call|vacuum|analyze|reindex|cluster|comment|listen|notify|reset|set\s+(?!local))\b/i;
    if (dangerous.test(lower)) return false;
    return true;
}

async function runReadOnlySQL(funnel, sql) {
    if (!isReadOnlySQL(sql)) {
        return { error: 'Query rejected: only single SELECT or WITH statements are allowed. No DDL/DML.' };
    }
    const supabase = clientFor(funnel);
    const { data, error } = await supabase.rpc('ai_run_sql', { query: sql });
    if (error) return { error: error.message };
    return { rows: data };
}

async function getInsightsCustomMetrics(funnel) {
    const supabase = clientFor(funnel);
    const { data } = await supabase.from('custom_metrics').select('name, formula, format, sort_order').order('sort_order', { ascending: true });
    return data || [];
}

async function loadMemory(funnel, userId) {
    const supabase = clientFor(funnel);
    const { data } = await supabase.from('ai_memory').select('key, value, updated_at').eq('user_id', userId).order('updated_at', { ascending: false });
    return data || [];
}

async function rememberFact(funnel, userId, key, value) {
    const supabase = clientFor(funnel);
    const { error } = await supabase
        .from('ai_memory')
        .upsert({ user_id: userId, key, value, updated_at: new Date().toISOString() }, { onConflict: 'user_id,key' });
    if (error) throw error;
    return { ok: true, key, value };
}

async function forgetFact(funnel, userId, key) {
    const supabase = clientFor(funnel);
    const { error } = await supabase.from('ai_memory').delete().eq('user_id', userId).eq('key', key);
    if (error) throw error;
    return { ok: true, key };
}

// ─── AI Insights: tool definitions ────────────────────────────────────────────
const INSIGHTS_TOOLS = [
    {
        name: 'get_metrics',
        description: 'Fetch daily funnel metrics for any date range in the historical data. Returns spend, registrations, attendance, CTA stages, and purchases broken down by source. Dates use YYYY-MM-DD format in Los Angeles timezone. Safe to call with multi-month ranges; result is one row per day.',
        input_schema: {
            type: 'object',
            properties: {
                date_from: { type: 'string', description: 'Inclusive start date (YYYY-MM-DD)' },
                date_to:   { type: 'string', description: 'Inclusive end date (YYYY-MM-DD)' },
            },
            required: ['date_from', 'date_to'],
        },
    },
    {
        name: 'get_metrics_rollup',
        description: 'Fetch metrics aggregated by week or month over a date range. Sums additive columns (spend, registrations, purchases, etc.) and recomputes ratios (CPA, attendance %, conversion %) from the sums so they stay accurate. Prefer this for multi-month or trend questions — it returns ~24 rows for two years of months instead of 730 daily rows.',
        input_schema: {
            type: 'object',
            properties: {
                period:    { type: 'string', enum: ['week', 'month'], description: 'Bucket size' },
                date_from: { type: 'string', description: 'Inclusive start date (YYYY-MM-DD)' },
                date_to:   { type: 'string', description: 'Inclusive end date (YYYY-MM-DD)' },
            },
            required: ['period', 'date_from', 'date_to'],
        },
    },
    {
        name: 'compare_periods',
        description: 'Compare two date ranges side by side. Returns totals for each period plus absolute and percent deltas for every metric. Use for week-over-week, month-over-month, year-over-year, "this 30 days vs prior 30 days", etc.',
        input_schema: {
            type: 'object',
            properties: {
                period_a_from: { type: 'string', description: 'Period A start (YYYY-MM-DD)' },
                period_a_to:   { type: 'string', description: 'Period A end (YYYY-MM-DD)' },
                period_b_from: { type: 'string', description: 'Period B (baseline/comparison) start' },
                period_b_to:   { type: 'string', description: 'Period B (baseline/comparison) end' },
            },
            required: ['period_a_from', 'period_a_to', 'period_b_from', 'period_b_to'],
        },
    },
    {
        name: 'get_event_counts',
        description: 'Aggregated event counts per day per event type (registrations, attended, replays, viewedcta, clickedcta, purchases, etc.). Use when you need raw event volumes — e.g. spot-checking dedup behavior — or for ad-hoc event-type questions. PII is never returned.',
        input_schema: {
            type: 'object',
            properties: {
                date_from:  { type: 'string', description: 'Inclusive start date (YYYY-MM-DD), optional' },
                date_to:    { type: 'string', description: 'Inclusive end date (YYYY-MM-DD), optional' },
                event_type: { type: 'string', description: 'Filter to a single event type, optional' },
            },
        },
    },
    {
        name: 'list_custom_metrics',
        description: 'Return the list of user-defined custom metrics — display name, formula, and number format. Use when the user references a metric by name and you need to know how it is computed.',
        input_schema: { type: 'object', properties: {} },
    },
    {
        name: 'run_sql',
        description: 'Run a read-only SQL query against the funnel\'s schema. Use this as an escape hatch for analytical questions the other tools don\'t cover — joins, grouping by day-of-week, filtering by arbitrary event attributes, etc. Tables available: daily_metrics, events, custom_metrics, dashboard_lenses. STRICT RULES: must be a single SELECT or WITH statement; no INSERT/UPDATE/DELETE/DDL; results capped at 500 rows and 5-second timeout. The query runs in the active funnel\'s schema (no need to prefix table names).',
        input_schema: {
            type: 'object',
            properties: {
                query: { type: 'string', description: 'A single SELECT or WITH ... SELECT query. No trailing semicolons needed.' },
            },
            required: ['query'],
        },
    },
    {
        name: 'remember',
        description: 'Save a durable fact you\'ve learned that future chats should know — campaigns the user ran, business context, definitions, preferences, anomalies the user explained, etc. Use a short kebab-case key. Overwrites any previous value for the same key.',
        input_schema: {
            type: 'object',
            properties: {
                key:   { type: 'string', description: 'Short kebab-case identifier, e.g. "spring-promo-2026"' },
                value: { type: 'string', description: 'The fact to remember, in plain prose' },
            },
            required: ['key', 'value'],
        },
    },
    {
        name: 'forget',
        description: 'Delete a previously remembered fact by key. Use when a remembered fact is wrong or no longer applicable.',
        input_schema: {
            type: 'object',
            properties: { key: { type: 'string', description: 'The key to forget' } },
            required: ['key'],
        },
    },
];

async function executeInsightsTool(name, input, ctx) {
    const { funnel, userId } = ctx;
    try {
        switch (name) {
            case 'get_metrics':
                return await getInsightsMetrics(funnel, input.date_from, input.date_to);
            case 'get_metrics_rollup':
                return await getInsightsRollup(funnel, input.period, input.date_from, input.date_to);
            case 'compare_periods':
                return await compareInsightsPeriods(funnel, input.period_a_from, input.period_a_to, input.period_b_from, input.period_b_to);
            case 'get_event_counts':
                return await getInsightsEventCounts(funnel, input.date_from, input.date_to, input.event_type);
            case 'list_custom_metrics':
                return await getInsightsCustomMetrics(funnel);
            case 'run_sql':
                return await runReadOnlySQL(funnel, input.query);
            case 'remember':
                if (!input.key || !input.value) return { error: 'key and value required' };
                return await rememberFact(funnel, userId, input.key, input.value);
            case 'forget':
                if (!input.key) return { error: 'key required' };
                return await forgetFact(funnel, userId, input.key);
            default:
                return { error: `Unknown tool: ${name}` };
        }
    } catch (err) {
        return { error: err.message || String(err) };
    }
}

app.post('/api/insights/chat', dashboardLimiter, requireAuth, async (req, res) => {
    if (!ANTHROPIC_API_KEY) {
        return res.status(503).json({ error: 'AI Insights not configured — set ANTHROPIC_API_KEY' });
    }

    try {
        const { messages = [] } = req.body;
        if (!Array.isArray(messages) || messages.length === 0) {
            return res.status(400).json({ error: 'Send { messages: [{ role, content }] }' });
        }

        const funnel = req.funnel;
        const brand = FUNNEL_BRANDS[funnel] || { brand: funnel, context: '', funnelName: funnel };
        const today = new Date().toLocaleDateString('en-US', { timeZone: 'America/Los_Angeles' });
        const memory = await loadMemory(funnel, req.user.id);
        const memoryBlock = memory.length === 0
            ? '(no remembered facts yet — use the `remember` tool to save things future chats should know)'
            : memory.map(m => `- ${m.key}: ${m.value}`).join('\n');

        const systemPrompt = `You are a senior business analyst for ${brand.brand}${brand.context ? `, ${brand.context}` : ''}. You are analyzing data from their ${brand.funnelName}.

TODAY'S DATE: ${today} (Los Angeles timezone)

THE FUNNEL STAGES (in order):
1. FB Spend — daily Facebook ad budget
2. Total Registration Page Visited (fb_link_clicks) — people who clicked the ad link to the registration page
3. Registrations — people who signed up for the webinar
4. Attended — people who actually attended the webinar
5. Replays — people who watched the replay
6. Viewed CTA — people who saw the call to action
7. Clicked CTA — people who clicked the call to action
8. Purchases — broken down by source:
   - purchases_fb (FB Purchases) — from Facebook Paid Ads
   - purchases_native (Native Ads) — from native ad placements
   - purchases_youtube (Youtube) — from Youtube campaigns
   - purchases_aibot (AI Chat Bot) — from AI chatbot interactions
   - purchases_postwebinar (Post Webinar) — Paid Ads purchases made 12+ hours AFTER attending a webinar
   - purchases_cpa (CPA Traffic Funnel) — purchases attributed to the CPA Traffic source
   - total_purchases — sum of all purchase sources above

KEY METRICS:
- Landing Page Conversion Rate = registrations / fb_link_clicks × 100
- Cost per Registration         = fb_spend / registrations
- Attendance Rate               = attended / registrations × 100
- CTA View Rate                 = viewedcta / (attended + replays) × 100
- CTA Click Rate                = clickedcta / viewedcta × 100
- Conversion Rate               = total_purchases / clickedcta × 100
- Cost per Acquisition          = fb_spend / total_purchases
- Post Webinar Rate             = purchases_postwebinar / total_purchases × 100

REMEMBERED FACTS:
${memoryBlock}

INSTRUCTIONS:
- Use tools to fetch only the data you need. Don't ask the user for date ranges — pick reasonable ones (last 7/14/30 days, last 3/6 months) based on the question.
- For multi-week or multi-month trends, prefer \`get_metrics_rollup\` over \`get_metrics\` — fewer rows, cleaner trend.
- For "X vs Y" or "this vs last" questions, use \`compare_periods\` — it returns deltas and % change for free.
- For analytical questions the named tools don't cover (day-of-week patterns, hour-of-day, joins, custom aggregations), use \`run_sql\` with a SELECT. Don't be shy about it — it's the escape hatch.
- For statistical work (forecasts, regressions, anomaly detection, t-tests), use the code execution tool. The data you've fetched is available as variables.
- Give specific, data-backed insights. Reference actual numbers and dates.
- When the user shares context worth remembering across chats (promos, definitions, business changes), use the \`remember\` tool silently.
- If asked about something not in the data, say so honestly.

FORMATTING:
- Use markdown: headers, bullets, bold for key numbers, tables when comparing.
- When a chart would communicate better than a table (any trend, distribution, or comparison across >5 data points), emit a fenced \`\`\`chart block with JSON. The frontend will render it as a real chart. Shape:
  \`\`\`chart
  {
    "type": "line",                                    // "line" | "bar" | "area"
    "title": "Daily Registrations (last 30 days)",
    "x": "date",
    "series": [
      {"key": "registrations", "label": "Registrations", "color": "#3B82F6"},
      {"key": "purchases",     "label": "Purchases",     "color": "#10B981"}
    ],
    "data": [
      {"date": "2026-05-01", "registrations": 30, "purchases": 2},
      {"date": "2026-05-02", "registrations": 35, "purchases": 3}
    ]
  }
  \`\`\`
  Use 2-4 series max. Keep it readable.`;

        // Build the message stack we'll send to Claude. Incoming history is
        // user/assistant text only; we'll grow it with tool_use / tool_result
        // pairs during the loop.
        // Trim to last 20 turns to bound token cost on long conversations.
        // Memory ([[remember]]) is the durable channel for older context.
        const TRIM_KEEP = 20;
        const trimmed = messages.length > TRIM_KEEP ? messages.slice(-TRIM_KEEP) : messages;
        const apiMessages = trimmed.map(m => ({
            role: m.role === 'user' ? 'user' : 'assistant',
            content: m.content,
        }));

        // Server-side tools (managed by Anthropic): code execution sandbox.
        // Beta gated — adjust the tool type + beta header if Anthropic
        // updates the dated identifier.
        const SERVER_TOOLS = [
            { type: 'code_execution_20250522', name: 'code_execution' },
        ];
        const allTools = [...INSIGHTS_TOOLS, ...SERVER_TOOLS];

        const MAX_ITERS = 10;
        let lastUsage = null;
        for (let iter = 0; iter < MAX_ITERS; iter++) {
            const response = await fetch('https://api.anthropic.com/v1/messages', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': ANTHROPIC_API_KEY,
                    'anthropic-version': '2023-06-01',
                    'anthropic-beta': 'code-execution-2025-05-22',
                },
                body: JSON.stringify({
                    model: 'claude-sonnet-4-6',
                    max_tokens: 4096,
                    system: systemPrompt,
                    tools: allTools,
                    messages: apiMessages,
                }),
            });

            if (!response.ok) {
                const errBody = await response.text();
                console.error('❌ Claude API error:', response.status, errBody);
                return res.status(502).json({ error: 'AI service error', detail: errBody });
            }

            const result = await response.json();
            lastUsage = result.usage;

            if (result.stop_reason === 'tool_use') {
                // Only client-side tools need execution here. Server-side
                // tools (code_execution) are already run by Anthropic and
                // their results are embedded in the same response.
                const ourToolNames = new Set(INSIGHTS_TOOLS.map(t => t.name));
                const toolUses = (result.content || []).filter(c => c.type === 'tool_use' && ourToolNames.has(c.name));
                const serverUses = (result.content || []).filter(c => c.type === 'tool_use' && !ourToolNames.has(c.name));
                console.log(`🛠️  Chat[${funnel}] iter ${iter}: ${toolUses.length} client + ${serverUses.length} server tool call(s): ${[...toolUses, ...serverUses].map(t => t.name).join(', ')}`);

                if (toolUses.length === 0) {
                    // Only server tools fired — nothing to do client-side.
                    // The next API call will continue from this state.
                    apiMessages.push({ role: 'assistant', content: result.content });
                    apiMessages.push({ role: 'user', content: 'continue' });
                    continue;
                }

                const toolResults = [];
                for (const tu of toolUses) {
                    const out = await executeInsightsTool(tu.name, tu.input || {}, { funnel, userId: req.user.id });
                    toolResults.push({
                        type: 'tool_result',
                        tool_use_id: tu.id,
                        content: JSON.stringify(out),
                    });
                }

                apiMessages.push({ role: 'assistant', content: result.content });
                apiMessages.push({ role: 'user', content: toolResults });
                continue;
            }

            // end_turn / max_tokens / stop_sequence — done
            const reply = (result.content || [])
                .filter(c => c.type === 'text')
                .map(c => c.text)
                .join('\n')
                .trim() || 'No response generated.';
            return res.json({ reply, usage: lastUsage });
        }

        console.error(`❌ Chat[${funnel}]: tool loop exceeded ${MAX_ITERS} iterations`);
        return res.status(500).json({ error: 'Tool loop exceeded max iterations' });

    } catch (err) {
        console.error('❌ POST /api/insights/chat error:', err.message);
        res.status(500).json({ error: 'Internal server error', detail: err.message });
    }
});

// =============================================================================
// CHAT CONVERSATION PERSISTENCE
// =============================================================================

// GET /api/insights/conversations — list all chats for the authenticated user
app.get('/api/insights/conversations', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { data, error } = await supabase
            .from('chat_conversations')
            .select('id, title, updated_at')
            .eq('user_id', req.user.id)
            .order('updated_at', { ascending: false });

        if (error) throw error;
        res.json({ data: data || [] });
    } catch (err) {
        console.error('❌ GET /api/insights/conversations error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// GET /api/insights/conversations/:id — fetch a single conversation with messages
app.get('/api/insights/conversations/:id', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { data, error } = await supabase
            .from('chat_conversations')
            .select('*')
            .eq('id', req.params.id)
            .eq('user_id', req.user.id)
            .single();

        if (error) {
            if (error.code === 'PGRST116') return res.status(404).json({ error: 'Conversation not found' });
            throw error;
        }
        res.json({ data });
    } catch (err) {
        console.error('❌ GET /api/insights/conversations/:id error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// PUT /api/insights/conversations/:id — create or update a conversation
app.put('/api/insights/conversations/:id', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { title, messages } = req.body;
        if (!Array.isArray(messages)) {
            return res.status(400).json({ error: 'messages must be an array' });
        }

        const { data, error } = await supabase
            .from('chat_conversations')
            .upsert({
                id: req.params.id,
                user_id: req.user.id,
                title: title || 'New chat',
                messages,
            }, { onConflict: 'id' })
            .select()
            .single();

        if (error) throw error;
        res.json({ success: true, data });
    } catch (err) {
        console.error('❌ PUT /api/insights/conversations error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// DELETE /api/insights/conversations/:id — delete a conversation
app.delete('/api/insights/conversations/:id', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { error } = await supabase
            .from('chat_conversations')
            .delete()
            .eq('id', req.params.id)
            .eq('user_id', req.user.id);

        if (error) throw error;
        res.json({ success: true });
    } catch (err) {
        console.error('❌ DELETE /api/insights/conversations error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});


// =============================================================================
// STATIC FILES (serve the React dashboard)
// =============================================================================

app.use(express.static(path.join(__dirname, 'public')));

// SPA fallback
app.get('*', (req, res) => {
    if (req.path.startsWith('/api')) {
        return res.status(404).json({ error: 'Not found' });
    }
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});


// =============================================================================
// START
// =============================================================================

const server = app.listen(PORT, () => {
    const fbStatus = (process.env.FB_ACCESS_TOKEN && process.env.FB_AD_ACCOUNT_ID) ? '✅ Active (every 30 min)' : '⚠️  Not configured';
    console.log(`
╔══════════════════════════════════════════════════╗
║   Dr Shumard Analytics — Production Server       ║
╠══════════════════════════════════════════════════╣
║                                                  ║
║   Dashboard:  http://localhost:${PORT}              ║
║   API:        http://localhost:${PORT}/api/metrics   ║
║   Health:     http://localhost:${PORT}/api/health    ║
║   Timezone:   America/Los_Angeles                ║
║   FB Sync:    ${fbStatus.padEnd(33)}║
║                                                  ║
╚══════════════════════════════════════════════════╝
  `);

    // Signal PM2 that this process is ready to accept traffic
    if (process.send) process.send('ready');
});

// ─── Graceful Shutdown ───────────────────────────────────────────────────────
// PM2 sends SIGINT during reload — finish in-flight requests, then exit cleanly
process.on('SIGINT', () => {
    console.log('⏳ Graceful shutdown: closing server…');
    server.close(() => {
        console.log('✅ Server closed — all requests finished');
        process.exit(0);
    });
    // Force shutdown after 4s if connections don't drain
    setTimeout(() => { process.exit(0); }, 4000);
});

export default app;