// ============================================================================
// Dr Shumard Analytics — Production API Server
// ============================================================================

import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import './ws-polyfill.js';   // must precede any @supabase import; see file for why
import { createClient } from '@supabase/supabase-js';
import ws from 'ws';
import crypto from 'crypto';
import path from 'path';
import { fileURLToPath } from 'url';
import { readFileSync } from 'fs';
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

const supabasePublic = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    realtime: { transport: ws },
});

const _funnelClients = new Map();
function clientFor(funnel) {
    const schema = FUNNEL_TO_SCHEMA[funnel];
    if (!schema) throw new Error(`Unknown funnel: ${funnel}`);
    if (!_funnelClients.has(funnel)) {
        _funnelClients.set(funnel, createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
            db: { schema },
            realtime: { transport: ws },
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
            // Full formatted GET /api/metrics response (default 'all' view)
            metricsResponse: null,
            metricsUpdatedAt: 0,
            // Same, but with per-variant (A/B/undetected) breakdowns embedded
            // (expand=variants) — powers the client-side split-test toggle
            metricsResponseExpanded: null,
            metricsExpandedUpdatedAt: 0,
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
    b.metricsResponseExpanded = null;
    b.metricsExpandedUpdatedAt = 0;
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
    b.journeyDigest = null;       // CRM/journey rollups refresh with the metrics cache
    b.journeyDigestAt = 0;
    b.dataDictionary = null;      // journey data dictionary refreshes too
    b.dataDictionaryAt = 0;
    b.invalidationEpoch++;
}

// A/B test start cutoff (ms epoch) for a funnel, or null = count all variant data.
// Stored in app_settings.key='ab_test_start' (ISO string). Cached on the bucket;
// the PUT endpoint updates the cache + clears dedup so counts recompute. Missing
// table (migration not applied) degrades safely to null (no cutoff).
async function getAbTestStart(funnel) {
    const b = getCacheBucket(funnel);
    if (b.abTestStart !== undefined) return b.abTestStart;
    try {
        const { data } = await clientFor(funnel).from('app_settings').select('value').eq('key', 'ab_test_start').maybeSingle();
        const iso = data && (typeof data.value === 'string' ? data.value : data.value?.start);
        const ms = iso ? new Date(iso).getTime() : null;
        b.abTestStart = (ms && !isNaN(ms)) ? ms : null;
    } catch { b.abTestStart = null; }
    return b.abTestStart;
}

// Manual identity links for a funnel: { aliasEmailLower -> canonicalEmailLower }. Lets
// an admin attribute a sale that arrived under a different email to the right
// registrant (so it counts in the correct variant). Cached on the bucket; cleared when
// a link changes. Missing table degrades to {} (no links).
async function getIdentityLinks(funnel) {
    const b = getCacheBucket(funnel);
    if (b.identityLinks !== undefined) return b.identityLinks;
    const map = {};
    try {
        const { data } = await clientFor(funnel).from('identity_links').select('alias_email, canonical_email');
        for (const r of data || []) {
            const a = (r.alias_email || '').toLowerCase().trim();
            const c = (r.canonical_email || '').toLowerCase().trim();
            if (a && c && a !== c) map[a] = c;
        }
    } catch { /* table missing → no links */ }
    b.identityLinks = map;
    return map;
}

// email (lowercased) → normalized phone, harvested from shumard tracking_contacts. Lets
// variant matching use the phone WE TRACKED even when the funnel webhook event carried
// none (e.g. a checkout that posted only email + name) — the whole point of tracking the
// phone. Cached briefly on the bucket (refreshes on the metrics cadence).
async function getTrackingPhones(funnel) {
    const b = getCacheBucket(funnel);
    const now = Date.now();
    if (b.trackingPhones && (now - (b.trackingPhonesAt || 0)) < cache.metricsTTL) return b.trackingPhones;
    const map = {};
    try {
        let from = 0; const page = 1000;
        for (;;) {
            const { data } = await clientFor(funnel).from('tracking_contacts')
                .select('email, phone').not('phone', 'is', null).range(from, from + page - 1);
            if (!data || !data.length) break;
            for (const r of data) {
                const e = (r.email || '').toLowerCase().trim();
                const p = normalizePhoneKey(r.phone);
                if (e && p && !(e in map)) map[e] = p;
            }
            if (data.length < page) break;
            from += page;
        }
    } catch { /* table missing → no enrichment */ }
    b.trackingPhones = map; b.trackingPhonesAt = now;
    return map;
}

// Ambient-graph "variant-via-stitch": from the shumard identity graph, derive aliases
// { otherEmail → canonicalRegistrantEmail } for emails that share a stitched identity
// cluster (merged_into) with a tagged registrant but aren't registrants themselves.
// This lets a purchase under a different email/phone inherit the registrant's variant
// with NO per-funnel wiring — it rides the same cluster shumard already builds (session
// → email → IP stitch, 3-hour window). Returns {} if the tables are absent.
//
// Guards against bad fan-out: only clusters that contain a tagged registrant produce
// aliases; we map to the EARLIEST tagged registrant's email; manual links always win
// (merged on top by getCombinedAliases). Cached on the metrics cadence.
async function getStitchAliases(funnel) {
    const b = getCacheBucket(funnel);
    const now = Date.now();
    if (b.stitchAliases && (now - (b.stitchAliasesAt || 0)) < cache.metricsTTL) return b.stitchAliases;
    const aliases = {};
    try {
        const sb = clientFor(funnel);
        // 1) Tagged registrants (small set) → earliest variant registration per email/phone.
        const { data: regs } = await sb.from('events')
            .select('email, phone, event_time, metadata')
            .eq('event_type', 'registrations')
            .or('metadata->>variant.eq.A,metadata->>variant.eq.B');
        const regByEmail = {}, regByPhone = {};
        for (const r of (regs || []).sort((a, c) => new Date(a.event_time) - new Date(c.event_time))) {
            const e = (r.email || '').toLowerCase().trim();
            const since = new Date(r.event_time).getTime();
            if (e && !(e in regByEmail)) regByEmail[e] = { email: e, since };
            const p = normalizePhoneKey(r.phone);
            if (p && e && !(p in regByPhone)) regByPhone[p] = { email: e, since };
        }
        if (Object.keys(regByEmail).length === 0 && Object.keys(regByPhone).length === 0) {
            b.stitchAliases = {}; b.stitchAliasesAt = now; return {};
        }
        // 2) Pull the contact graph and group emails/phones by merge-root.
        const rows = [];
        let from = 0; const page = 1000;
        for (;;) {
            const { data } = await sb.from('tracking_contacts')
                .select('contact_id, email, phone, merged_into').range(from, from + page - 1);
            if (!data || !data.length) break;
            rows.push(...data);
            if (data.length < page) break;
            from += page;
        }
        const byId = {};
        for (const r of rows) byId[r.contact_id] = r;
        const rootOf = (id) => {
            let cur = id; const guard = new Set();
            while (cur && byId[cur] && byId[cur].merged_into && !guard.has(cur)) { guard.add(cur); cur = byId[cur].merged_into; }
            return cur;
        };
        const clusters = {}; // rootId → { emails:Set, phones:Set }
        for (const r of rows) {
            const root = rootOf(r.contact_id) || r.contact_id;
            const c = clusters[root] || (clusters[root] = { emails: new Set(), phones: new Set() });
            const e = (r.email || '').toLowerCase().trim(); if (e) c.emails.add(e);
            const p = normalizePhoneKey(r.phone); if (p) c.phones.add(p);
        }
        // 3) Per cluster: find the earliest tagged registrant member; alias the cluster's
        //    OTHER (non-registrant) emails to that registrant's email.
        for (const root in clusters) {
            const c = clusters[root];
            let best = null;
            for (const e of c.emails) { const r = regByEmail[e]; if (r && (!best || r.since < best.since)) best = r; }
            for (const p of c.phones) { const r = regByPhone[p]; if (r && (!best || r.since < best.since)) best = r; }
            if (!best) continue;
            for (const e of c.emails) {
                if (e !== best.email && !(e in regByEmail) && !(e in aliases)) aliases[e] = best.email;
            }
        }
    } catch { /* tables missing → no stitch aliases */ }
    b.stitchAliases = aliases; b.stitchAliasesAt = now;
    return aliases;
}

// Combined email-canonicalization map for variant attribution: ambient stitch aliases
// (auto, from the identity graph) overlaid with manual admin links (deterministic, win).
async function getCombinedAliases(funnel) {
    const [stitch, manual] = await Promise.all([getStitchAliases(funnel), getIdentityLinks(funnel)]);
    return { ...stitch, ...manual };
}

// Landing-page → variant map. Keys = the reg-page URLs we COUNT for "Reg Page Visits";
// values = "A" / "B" (that page's variant) or "undetected" (a reg page that counts in
// 'all' but isn't part of the split, e.g. the pre-split /register entry). Excludes the
// joinnow embed, webinar.* attendance pages, /checkout. Editable per funnel via
// app_settings key 'reg_page_variants' (jsonb {url: "A"|"B"|"undetected"}); default below.
const REG_PAGE_VARIANT_MAP_DEFAULT = {
    'https://drshumardworkshop.com/webinar': 'A',
    'https://drshumardworkshop.com/webinar-page': 'B',
    // Non-variant reg page (the pre-split entry): its visitors COUNT in 'all' but land
    // in 'undetected' (they didn't see a variant). Value 'undetected' = count-but-no-variant.
    'https://drshumardworkshop.com/register': 'undetected',
};
async function getRegPageVariantMap(funnel) {
    const b = getCacheBucket(funnel);
    if (b.regPageMap !== undefined) return b.regPageMap;
    let map = { ...REG_PAGE_VARIANT_MAP_DEFAULT };
    try {
        const { data } = await clientFor(funnel).from('app_settings').select('value').eq('key', 'reg_page_variants').maybeSingle();
        if (data && data.value && typeof data.value === 'object' && Object.keys(data.value).length) map = data.value;
    } catch { /* no app_settings → default */ }
    b.regPageMap = map;
    return map;
}

// ─── Named sales pages (configurable, NOT hardcoded) ─────────────────────────
// Maps a full sales/checkout-page URL → a human label. Used to (a) label these
// pageviews on the CRM journey timeline, (b) drive the CRM "Sales Pages" column
// + filter, and (c) let AI insights answer "how many people hit Sales Page B".
// Overridable per-funnel via app_settings.key='sales_pages' (PUT /api/sales-pages)
// so a new split test / funnel needs zero code changes. Matching is path-only
// (query strings ignored) via split_part(current_url,'?',1), like the reg-page map.
const SALES_PAGE_MAP_DEFAULT = {
    'https://drshumardworkshop.com/checkout':  'Legacy',
    'https://drshumardworkshop.com/checkout1': 'Sales A',
    'https://drshumardworkshop.com/checkout2': 'Sales B',
};
async function getSalesPageMap(funnel) {
    const b = getCacheBucket(funnel);
    if (b.salesPageMap !== undefined) return b.salesPageMap;
    let map = { ...SALES_PAGE_MAP_DEFAULT };
    try {
        const { data } = await clientFor(funnel).from('app_settings').select('value').eq('key', 'sales_pages').maybeSingle();
        if (data && data.value && typeof data.value === 'object' && Object.keys(data.value).length) map = data.value;
    } catch { /* no app_settings → default */ }
    b.salesPageMap = map;
    return map;
}

// Unique reg-page visitors per LA day per variant, from shumard pageviews. A person is
// counted ONCE per day, attributed to the variant of the earliest A/B page they reached
// that day; if they only hit a non-variant reg page (e.g. /register) or only visited
// before the A/B-test-start cutoff, they're 'undetected' but still counted in 'all'. So
// all = A + B + undetected (every reg-page visitor). Returns { 'YYYY-MM-DD': {all,A,B,undetected} }.
// Independent of the FB-sourced fb_link_clicks.
async function getRegPageVisits(funnel, dates) {
    const out = {};
    const uniq = [...new Set((dates || []).filter(Boolean))];
    if (!uniq.length) return out;
    const map = await getRegPageVariantMap(funnel);
    const urls = Object.keys(map);
    if (!urls.length) return out;
    const cutoff = await getAbTestStart(funnel);
    const cutoffIso = cutoff ? new Date(cutoff).toISOString() : null;
    const esc = (s) => String(s).replace(/'/g, "''");
    const urlList = urls.map(u => `'${esc(u)}'`).join(',');
    const dateList = uniq.map(d => `'${esc(d)}'`).join(',');
    // Per-visit variant: pre-cutoff → NULL (undetected); A/B pages → their variant; any
    // other counted page (e.g. /register='undetected') → NULL. NULL = counts in 'all',
    // bucketed undetected.
    const cutoffGate = cutoffIso ? `WHEN timestamp < '${cutoffIso}'::timestamptz THEN NULL ` : '';
    const variantCase = urls.filter(u => map[u] === 'A' || map[u] === 'B')
        .map(u => `WHEN split_part(current_url,'?',1) = '${esc(u)}' THEN '${map[u]}'`).join(' ');
    const sql = `WITH v0 AS (
        SELECT (timestamp AT TIME ZONE 'America/Los_Angeles')::date AS day,
               contact_id, timestamp AS ts_utc,
               CASE ${cutoffGate}${variantCase} ELSE NULL END AS variant
        FROM tracking_page_visits
        WHERE contact_id IS NOT NULL
          AND split_part(current_url,'?',1) IN (${urlList})
          AND (timestamp AT TIME ZONE 'America/Los_Angeles')::date IN (${dateList})
    ), picked AS (
        -- one row per person/day: prefer their earliest A/B visit; else undetected
        SELECT DISTINCT ON (day, contact_id) day, variant
        FROM v0 ORDER BY day, contact_id, (variant IS NULL), ts_utc ASC
    )
    SELECT day::text AS day, coalesce(variant,'undetected') AS variant, count(*)::int AS visitors
    FROM picked GROUP BY day, variant`;
    try {
        const { data, error } = await clientFor(funnel).rpc('ai_run_sql', { query: sql });
        if (error) { console.warn(`⚠️  reg_page_visits query failed [${funnel}]:`, error.message); return out; }
        for (const r of (data || [])) {
            const d = String(r.day).slice(0, 10);
            if (!out[d]) out[d] = { all: 0, A: 0, B: 0, undetected: 0 };
            const v = (r.variant === 'A' || r.variant === 'B') ? r.variant : 'undetected';
            out[d][v] += Number(r.visitors) || 0;
            out[d].all += Number(r.visitors) || 0;
        }
    } catch (e) { console.warn(`⚠️  reg_page_visits error [${funnel}]:`, e.message); }
    return out;
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
// Dashboard/API requests honor the CORS_ORIGINS allowlist (or reflect any origin
// when unset). Tracking requests (/shumard.js + /api/sg/*) are embedded on
// arbitrary client sites, so they always get permissive CORS — they're
// unauthenticated and carry no credentials.
const corsOrigins = (process.env.CORS_ORIGINS || '').split(',').filter(Boolean);
const dashboardCors = cors({
    origin: corsOrigins.length > 0 ? corsOrigins : true,
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-API-Key', 'X-Funnel'],
});
app.use((req, res, next) => {
    if (req.path === '/shumard.js' || req.path.startsWith('/api/sg')) {
        res.header('Access-Control-Allow-Origin', '*');
        res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
        res.header('Access-Control-Allow-Headers', 'Content-Type');
        // Override Helmet's default Cross-Origin-Resource-Policy: same-origin — the
        // script + track endpoints are embedded on other origins (the funnel sites).
        // Without this the browser returns the response but blocks its use with
        // ERR_BLOCKED_BY_RESPONSE.NotSameOrigin.
        res.header('Cross-Origin-Resource-Policy', 'cross-origin');
        if (req.method === 'OPTIONS') return res.sendStatus(204);
        return next();
    }
    return dashboardCors(req, res, next);
});

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

// Tracking (shumard.js) fires more often than webhooks and many real visitors can
// share one IP (corporate NAT, carriers), so it gets its own higher per-IP ceiling.
const trackLimiter = rateLimit({
    windowMs: 1 * 60 * 1000,
    max: 1200,
    message: { error: 'Too many requests' },
});

// Drop non-human traffic (email link scanners, link-preview/headless bots) before it
// can create contacts or clicks. shumard.js only runs in real browsers; this catches
// JS-rendering scanners/bots that report a recognizable bot user-agent.
const BOT_UA_RE = /bot|crawl|spider|slurp|preview|scan|proofpoint|mimecast|barracuda|googleimageproxy|google-read-aloud|feedfetch|facebookexternalhit|slackbot|telegrambot|whatsapp|discordbot|bingpreview|headless|phantomjs|puppeteer|playwright|selenium|webdriver|curl|wget|python-requests|axios|okhttp|libwww|go-http|java\//i;
function isLikelyBot(req, payloadUA) {
    const ua = String(payloadUA || req.headers['user-agent'] || '');
    if (!ua) return true;                 // no user-agent at all → not a real browser
    return BOT_UA_RE.test(ua);
}

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
        // select('*') rather than naming columns so the lookup still works on a
        // DB where migrate_api_key_scopes.sql hasn't run yet (no scopes column).
        const { data: dbKey } = await supabasePublic
            .from('api_keys')
            .select('*')
            .eq('key_hash', hash)
            .single();
        if (dbKey && dbKey.is_active && ALLOWED_FUNNELS.includes(dbKey.funnel)) {
            await supabasePublic.from('api_keys').update({ last_used_at: new Date().toISOString() }).eq('id', dbKey.id);
            req.funnel = dbKey.funnel;
            req.apiKeyScopes = dbKey.scopes || []; // TEXT[] of allowed AI tool names; empty = none, ['*'] = all
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
        const PURCHASE_COLS = ['purchases', 'purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_aibot_b', 'purchases_postwebinar', 'purchases_cpa', 'purchases_sales_a', 'purchases_sales_b'];
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
            const PURCHASE_COLS = ['purchases', 'purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_aibot_b', 'purchases_postwebinar', 'purchases_cpa', 'purchases_sales_a', 'purchases_sales_b'];
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
            'AI Bot B':    'purchases_aibot_b',
            'CPA Traffic': 'purchases_cpa',
            'Sales A':     'purchases_sales_a',
            'Sales B':     'purchases_sales_b',
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
            } else if (field === 'purchases') {
                // Purchases: ONE per email per LA calendar day. A buyer purchases once;
                // re-sends / retries across the day — even hours apart, and regardless of
                // source — are duplicates (the 5-min window missed 15–20 min retries). We
                // compare the LA day of any existing purchase for this email (36h lookback
                // covers the timezone boundary). NOTE: this also blocks a genuine second
                // same-day purchase by the same email — accepted tradeoff for this funnel.
                const dayLookback = new Date(Date.now() - 36 * 60 * 60 * 1000).toISOString();
                const { data: recent } = await supabase
                    .from('events')
                    .select('id, event_time')
                    .eq('event_type', 'purchases')
                    .eq('email', email)
                    .gte('event_time', dayLookback);
                const todayLA = new Date().toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
                const isDup = (recent || []).some(ev =>
                    new Date(ev.event_time).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' }) === todayLA);
                if (isDup) {
                    console.log(`⏭️  Dedup skip: purchase for ${email} (already purchased today ${todayLA})`);
                    return res.json({ success: true, duplicate: true, message: 'Duplicate purchase skipped (one per email per day)' });
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

            // Post Webinar detection: if Paid Ads / Sales A / Sales B and the buyer's MOST
            // RECENT webinar engagement — attended OR replay — was 12h+ ago. Using the latest
            // of attended/replays (not attended alone) means a fresh replay re-engagement
            // counts as live (not post-webinar), and replay-only watchers who buy later are
            // still caught.
            if (['Paid Ads', 'Sales A', 'Sales B'].includes(rawSource) && email) {
                try {
                    const { data: engagedEvt } = await supabase
                        .from('events')
                        .select('event_time, event_type')
                        .in('event_type', ['attended', 'replays'])
                        .ilike('email', email)
                        .order('event_time', { ascending: false })
                        .limit(1);

                    if (engagedEvt?.length > 0) {
                        const hoursSince = (Date.now() - new Date(engagedEvt[0].event_time).getTime()) / 3600000;
                        if (hoursSince >= 12) {
                            sourceColumn = 'purchases_postwebinar';
                            resolvedSource = 'Post Webinar';
                            console.log(`🎯 Post Webinar purchase: ${email} ${engagedEvt[0].event_type} ${Math.round(hoursSince)}h ago`);
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
        const validFields = ['fb_spend', 'fb_link_clicks', 'registrations', 'replays', 'viewedcta', 'clickedcta', 'purchases', 'purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_aibot_b', 'purchases_postwebinar', 'purchases_cpa', 'purchases_sales_a', 'purchases_sales_b', 'stayed_45', 'stayed_60', 'stayed_80', 'attended'];

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
    'AI Bot B':      'purchases_aibot_b',
    'Post Webinar':  'purchases_postwebinar',
    'CPA Traffic':   'purchases_cpa',
    'Sales A':       'purchases_sales_a',
    'Sales B':       'purchases_sales_b',
};

// Allowed variant buckets. 'all' is computed as the sum of the others.
const VARIANT_BUCKETS = ['A', 'B', 'undetected'];

// Normalize a phone to a comparable key: digits only, last 10 (so "+1 (555)
// 123-4567" and "5551234567" match). Returns '' for anything under 10 digits so
// short/garbage values never become a join key. US-centric, which fits the practice.
function normalizePhoneKey(phone) {
    if (!phone) return '';
    const digits = String(phone).replace(/\D/g, '');
    return digits.length >= 10 ? digits.slice(-10) : '';
}

// Build the A/B variant resolver for a set of events — the SINGLE source of truth for
// variant attribution, used by the dashboard dedup, the AI tools, and Query Data.
// Returns variantOf(ev) → 'A' | 'B' | 'undetected':
//   1) An event's OWN metadata.variant (A/B) wins — registrations AND downstream
//      events (Stealth stamps the variant on attended/CTA/purchase webhooks too).
//   2) An untagged registration is 'undetected' — it NEVER inherits (otherwise an
//      untagged / pre-experiment registration gets retroactively painted a variant).
//   3) An untagged downstream event inherits the registrant's variant via email then
//      phone (last-10-digits) — but FORWARD-ONLY: only if the event occurs at/after the
//      variant was assigned (the first tagged registration's time). Earlier events, and
//      events with no matching tagged registration, are 'undetected'.
function buildVariantResolver(events, cutoffMs = null, aliasMap = null, emailPhones = null) {
    const sortedRegs = events
        .filter(ev => ev.event_type === 'registrations' && (ev.email || ev.phone))
        .sort((a, b) => new Date(a.event_time) - new Date(b.event_time));
    const emailToVariant = {};   // email -> { v, since }
    const phoneToVariant = {};   // phone -> { v, since }
    for (const ev of sortedRegs) {
        // Ignore registrations before the A/B test went live — they're pre-launch test
        // data and must not assign anyone a variant.
        if (cutoffMs && new Date(ev.event_time).getTime() < cutoffMs) continue;
        const raw = ev.metadata?.variant;
        if (raw === undefined || raw === null || raw === '') continue;
        const v = String(raw).trim().toUpperCase();
        if (v !== 'A' && v !== 'B') continue;
        const since = new Date(ev.event_time).getTime();
        const ek = (ev.email || '').toLowerCase();
        if (ek && !(ek in emailToVariant)) emailToVariant[ek] = { v, since };
        // Phone: the registration's own phone, else the phone we tracked for this email.
        const pk = normalizePhoneKey(ev.phone) || (emailPhones && ek ? emailPhones[ek] : '');
        if (pk && !(pk in phoneToVariant)) phoneToVariant[pk] = { v, since };
    }
    const ownVariant = (ev) => {
        const raw = ev.metadata?.variant;
        if (raw === undefined || raw === null || raw === '') return null;
        const v = String(raw).trim().toUpperCase();
        return (v === 'A' || v === 'B') ? v : null;
    };
    return (ev) => {
        const t = new Date(ev.event_time).getTime();
        // Before the test went live → not part of the experiment, tag or not.
        if (cutoffMs && t < cutoffMs) return 'undetected';
        const own = ownVariant(ev);
        if (own) return own;
        if (ev.event_type === 'registrations') return 'undetected';
        // Canonicalize the email via a manual admin link (e.g. a sale under a different
        // email mapped to its registrant) before looking up the variant.
        const ekRaw = (ev.email || '').toLowerCase();
        let ek = ekRaw;
        if (aliasMap && ek && aliasMap[ek]) ek = aliasMap[ek];
        const em = ek ? emailToVariant[ek] : null;
        if (em && t >= em.since) return em.v;
        // Phone candidates: the event's own phone, then the phone WE TRACKED for this
        // email (shumard) — so a checkout that posted no phone still matches.
        for (const pk of [normalizePhoneKey(ev.phone), emailPhones && ekRaw ? emailPhones[ekRaw] : null]) {
            if (!pk) continue;
            const pm = phoneToVariant[pk];
            if (pm && t >= pm.since) return pm.v;
        }
        return 'undetected';
    };
}

// Returns: { 'YYYY-MM-DD': { event_type: { all, A, B, undetected } } }.
// 'all' = A + B + undetected (each person resolves to exactly one bucket).
function computeDedupFromEvents(events, cutoffMs = null, aliasMap = null, emailPhones = null) {
    const variantOf = buildVariantResolver(events, cutoffMs, aliasMap, emailPhones);
    // The A/B-test-start as an LA calendar date. Any event bucketed to a day BEFORE this
    // is a pre-test row and never shows a variant — even if the event itself was recorded
    // after the cutoff (e.g. a replay of a pre-test webinar watched during the test, which
    // buckets to the old webinar's date). The cutoff DAY itself still uses variantOf's
    // precise event_time gate.
    const cutoffLADate = cutoffMs ? new Date(cutoffMs).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' }) : null;

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

        let userKey = (ev.email || '').toLowerCase();
        if (aliasMap && userKey && aliasMap[userKey]) userKey = aliasMap[userKey]; // canonicalize linked identities
        userKey = userKey || ev.phone || (ev.name || '').toLowerCase();
        if (!userKey) continue;

        // Pre-test rows (bucket date before the cutoff's day) are never a variant.
        const variant = (cutoffLADate && d < cutoffLADate) ? 'undetected' : variantOf(ev);
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

// Single-flight registry: funnel → in-flight dedup compute promise. Collapses concurrent
// cold-cache recomputes (e.g. a burst of dashboard polls right after a restart) so the
// expensive event-history fetch runs ONCE instead of N times — which is what caused the
// 20s+ pile-ups and "fetch failed" Supabase pressure (current-day showing 0 then filling).
const _dedupInflight = {};

// Heavy path: fetch + dedup the uncached dates, write to cache (epoch-guarded), prune.
// Returns the freshly computed values by date (so the caller still gets accurate numbers
// even when a mid-flight invalidation skips the cache write).
async function computeAndCacheUncached(funnel, bucket, uncachedDates, today) {
    cache.misses++;
    const abCutoff = await getAbTestStart(funnel);     // null = count all variant data
    const aliasMap = await getCombinedAliases(funnel); // ambient stitch + manual links
    const emailPhones = await getTrackingPhones(funnel); // email→shumard phone enrichment

    // Split uncached dates into contiguous ranges to avoid refetching cached gaps (#9)
    const sorted = [...uncachedDates].sort();
    const ranges = [];
    let rangeStart = sorted[0], rangePrev = sorted[0];
    for (let i = 1; i < sorted.length; i++) {
        const gapDays = (new Date(sorted[i] + 'T00:00:00Z') - new Date(rangePrev + 'T00:00:00Z')) / 86400000;
        if (gapDays > 3) { ranges.push([rangeStart, rangePrev]); rangeStart = sorted[i]; }
        rangePrev = sorted[i];
    }
    ranges.push([rangeStart, rangePrev]);
    console.log(`📊 Cache[${funnel}] MISS: ${uncachedDates.length} uncached date(s) across ${ranges.length} range(s)`);

    // Snapshot the epoch; if an invalidation lands mid-fetch, don't write stale results.
    const epochAtStart = bucket.invalidationEpoch;
    const computedByDate = {};
    for (const [minDate, maxDate] of ranges) {
        const events = await fetchEventsForDateRange(funnel, minDate, maxDate);
        const computed = computeDedupFromEvents(events, abCutoff, aliasMap, emailPhones);
        const dayCounts = {};
        for (const [d, counts] of Object.entries(computed)) {
            dayCounts[d] = Object.entries(counts).map(([k, v]) => `${k}:${v}`).join(', ');
        }
        if (Object.keys(dayCounts).length > 0) {
            console.log(`📊 Dedup[${funnel}] [${minDate}→${maxDate}]:`, JSON.stringify(dayCounts));
        }
        for (const d of uncachedDates) {
            if (d >= minDate && d <= maxDate) computedByDate[d] = computed[d] || {};
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

    // Prune dedup cache: drop dates older than 120 days to bound growth (#5)
    const pruneCutoff = new Date();
    pruneCutoff.setDate(pruneCutoff.getDate() - 120);
    const pruneISO = pruneCutoff.toISOString().slice(0, 10);
    for (const d of Object.keys(bucket.dedupCounts)) {
        if (d < pruneISO) delete bucket.dedupCounts[d];
    }
    return computedByDate;
}

async function getDedupCounts(funnel, dates) {
    if (dates.length === 0) return {};
    const bucket = getCacheBucket(funnel);
    const today = dateToISO(getLADate());

    // Today's dedup expires after 3 seconds to handle concurrent webhook races (#14).
    // Past days are cached forever (their counts can't change).
    if (bucket.dedupCounts[today] && bucket.dedupTimestamps[today] && Date.now() - bucket.dedupTimestamps[today] > 3000) {
        delete bucket.dedupCounts[today];
    }

    let uncachedDates = dates.filter(d => !(d in bucket.dedupCounts));
    let computed = {};
    if (uncachedDates.length > 0) {
        // Single-flight: if a compute is already running for this funnel, await it and
        // recheck — usually the dates we need were just filled in, so we skip a duplicate
        // full fetch. (No await before this check, so the assignment below is atomic.)
        if (_dedupInflight[funnel]) {
            await _dedupInflight[funnel].catch(() => {});
            uncachedDates = dates.filter(d => !(d in bucket.dedupCounts));
        }
        if (uncachedDates.length > 0) {
            const p = computeAndCacheUncached(funnel, bucket, uncachedDates, today);
            _dedupInflight[funnel] = p;
            try { computed = await p; } finally { if (_dedupInflight[funnel] === p) delete _dedupInflight[funnel]; }
        }
    } else {
        cache.hits++;
    }

    const result = {};
    for (const d of dates) {
        if (computed[d]) result[d] = computed[d];
        else if (bucket.dedupCounts[d]) result[d] = bucket.dedupCounts[d];
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
        'purchases_aibot', 'purchases_aibot_b', 'purchases_postwebinar', 'purchases_cpa',
        'purchases_sales_a', 'purchases_sales_b',
        'stayed_45', 'stayed_60', 'stayed_80',
    ];

    const FIELD_PARENT = {
        registrations: 'registrations', attended: 'attended', replays: 'replays',
        viewedcta: 'viewedcta', clickedcta: 'clickedcta',
        purchases_fb: 'purchases', purchases_native: 'purchases',
        purchases_youtube: 'purchases', purchases_aibot: 'purchases', purchases_aibot_b: 'purchases',
        purchases_postwebinar: 'purchases', purchases_cpa: 'purchases',
        purchases_sales_a: 'purchases', purchases_sales_b: 'purchases',
        stayed_45: 'stayeduntil', stayed_60: 'stayeduntil', stayed_80: 'stayeduntil',
    };

    const updates = { finalized_at: new Date().toISOString() };
    const written = {};
    const splits = {}; // per-field { A, B, undetected } — read by the expanded dashboard view
    for (const f of FIELDS) {
        // counts[f] is the per-variant breakdown; canonical columns store the total
        const c = counts[f];
        const v = Number(c?.all) || 0;
        const parentAuthoritative = eventTypesOnDate.has(FIELD_PARENT[f]);
        if (v > 0 || parentAuthoritative) {
            updates[f] = v;
            written[f] = v;
            splits[f] = { A: Number(c?.A) || 0, B: Number(c?.B) || 0, undetected: Number(c?.undetected) || 0 };
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

    // Persist per-variant splits separately + best-effort: if the variant_splits column
    // hasn't been migrated yet, this no-ops and finalize still succeeds (the expanded
    // view simply keeps deduping that day until the column exists and is backfilled).
    try {
        const { error: vsErr } = await supabase.from('daily_metrics')
            .update({ variant_splits: splits }).eq('date', isoDate);
        if (vsErr && !/variant_splits/.test(vsErr.message || '')) {
            console.warn(`⚠️ variant_splits write failed [${funnel} ${isoDate}]:`, vsErr.message);
        }
    } catch (e) { /* column not migrated yet — safe to ignore */ }

    invalidateMetricsCache(funnel);
    invalidateInsightsCache(funnel);
    delete getCacheBucket(funnel).dedupCounts[isoDate];

    return { date: isoDate, written };
}

// GET /api/ab-test/start — the funnel's A/B test start cutoff (null = count all)
app.get('/api/ab-test/start', dashboardLimiter, async (req, res) => {
    try {
        const funnel = resolveFunnel(req, 'analytics');
        const ms = await getAbTestStart(funnel);
        res.json({ start: ms ? new Date(ms).toISOString() : null });
    } catch (err) {
        console.error('❌ GET /api/ab-test/start error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// PUT /api/ab-test/start — admin sets/clears the cutoff.
// Body: { now: true } (set to current time) | { start: ISO } | { start: null } (clear).
// Clears the dedup cache so per-variant counts recompute under the new cutoff.
app.put('/api/ab-test/start', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { data: roleData } = await supabase.from('user_roles').select('role').eq('user_id', req.user.id).single();
        if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });

        let iso = null;
        if (req.body?.now) iso = new Date().toISOString();
        else if (req.body?.start) {
            const d = new Date(req.body.start);
            if (isNaN(d.getTime())) return res.status(400).json({ error: 'Invalid start timestamp' });
            iso = d.toISOString();
        } // else → clear (no cutoff)

        if (iso === null) {
            await supabase.from('app_settings').delete().eq('key', 'ab_test_start');
        } else {
            const { error } = await supabase.from('app_settings')
                .upsert({ key: 'ab_test_start', value: iso, updated_at: new Date().toISOString() }, { onConflict: 'key' });
            if (error) throw error;
        }
        // Refresh cached cutoff + force variant recompute on next read.
        const b = getCacheBucket(req.funnel);
        b.abTestStart = iso ? new Date(iso).getTime() : null;
        b.dedupCounts = {}; b.dedupTimestamps = {};
        invalidateMetricsCache(req.funnel);
        invalidateInsightsCache(req.funnel);
        console.log(`🅰️🅱️  [${req.funnel}] A/B test start set to ${iso || '(cleared)'}`);
        res.json({ start: iso });
    } catch (err) {
        console.error('❌ PUT /api/ab-test/start error:', err.message);
        res.status(500).json({ error: err.message || 'Internal server error' });
    }
});

// GET /api/reg-page-variants — the effective landing-page → variant map (for the
// "Reg Page Visits" metric), incl. whether it's the built-in default.
app.get('/api/reg-page-variants', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const funnel = req.funnel;
        const map = await getRegPageVariantMap(funnel);
        const isDefault = JSON.stringify(map) === JSON.stringify(REG_PAGE_VARIANT_MAP_DEFAULT);
        res.json({ map, isDefault, default: REG_PAGE_VARIANT_MAP_DEFAULT });
    } catch (err) {
        console.error('❌ GET /api/reg-page-variants error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// PUT /api/reg-page-variants — admin sets the landing-page → variant map.
// Body: { map: { "<full reg-page URL>": "A"|"B", ... } }. Empty/omitted map clears the
// override (reverts to the default). Recomputes the metric on next read.
app.put('/api/reg-page-variants', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { data: roleData } = await supabase.from('user_roles').select('role').eq('user_id', req.user.id).single();
        if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });

        const raw = req.body?.map;
        const clean = {};
        if (raw && typeof raw === 'object') {
            for (const [url, variant] of Object.entries(raw)) {
                const u = String(url || '').trim();
                const v = String(variant || '').trim().toUpperCase();
                if (!u) continue;
                if (v !== 'A' && v !== 'B' && v !== 'UNDETECTED') return res.status(400).json({ error: `Variant for "${u}" must be A, B, or Undetected` });
                if (!/^https?:\/\/.+/i.test(u)) return res.status(400).json({ error: `"${u}" must be a full URL (https://…)` });
                clean[u] = (v === 'UNDETECTED') ? 'undetected' : v;
            }
        }

        if (Object.keys(clean).length === 0) {
            await supabase.from('app_settings').delete().eq('key', 'reg_page_variants'); // revert to default
        } else {
            const { error } = await supabase.from('app_settings')
                .upsert({ key: 'reg_page_variants', value: clean, updated_at: new Date().toISOString() }, { onConflict: 'key' });
            if (error) throw error;
        }
        // Refresh the cached map + force the metric to recompute.
        const b = getCacheBucket(req.funnel);
        b.regPageMap = undefined;
        invalidateMetricsCache(req.funnel);
        const effective = Object.keys(clean).length ? clean : { ...REG_PAGE_VARIANT_MAP_DEFAULT };
        console.log(`🅰️🅱️  [${req.funnel}] reg-page-variant map set:`, JSON.stringify(effective));
        res.json({ map: effective, isDefault: Object.keys(clean).length === 0 });
    } catch (err) {
        console.error('❌ PUT /api/reg-page-variants error:', err.message);
        res.status(500).json({ error: err.message || 'Internal server error' });
    }
});

// GET /api/sales-pages — the effective sales-page URL → label map, plus whether
// it's the built-in default and what the default is (for the admin editor).
app.get('/api/sales-pages', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const map = await getSalesPageMap(req.funnel);
        const isDefault = JSON.stringify(map) === JSON.stringify(SALES_PAGE_MAP_DEFAULT);
        res.json({ map, isDefault, default: SALES_PAGE_MAP_DEFAULT });
    } catch (err) {
        console.error('❌ GET /api/sales-pages error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// PUT /api/sales-pages — admin sets the sales-page URL → label map.
// Body: { map: { "<full sales-page URL>": "<label>", ... } }. Labels are free text
// (e.g. "Sales A", "Sales B", "Legacy"). Empty/omitted map clears the override
// (reverts to SALES_PAGE_MAP_DEFAULT). Recomputes on next read.
app.put('/api/sales-pages', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { data: roleData } = await supabase.from('user_roles').select('role').eq('user_id', req.user.id).single();
        if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });

        const raw = req.body?.map;
        const clean = {};
        if (raw && typeof raw === 'object') {
            for (const [url, label] of Object.entries(raw)) {
                const u = String(url || '').trim();
                const l = String(label || '').trim();
                if (!u) continue;
                if (!/^https?:\/\/.+/i.test(u)) return res.status(400).json({ error: `"${u}" must be a full URL (https://…)` });
                if (!l) return res.status(400).json({ error: `Label for "${u}" cannot be empty` });
                clean[u] = l;
            }
        }

        if (Object.keys(clean).length === 0) {
            await supabase.from('app_settings').delete().eq('key', 'sales_pages'); // revert to default
        } else {
            const { error } = await supabase.from('app_settings')
                .upsert({ key: 'sales_pages', value: clean, updated_at: new Date().toISOString() }, { onConflict: 'key' });
            if (error) throw error;
        }
        // Refresh the cached map + force journey/insights context to recompute.
        getCacheBucket(req.funnel).salesPageMap = undefined;
        invalidateInsightsCache(req.funnel);
        const effective = Object.keys(clean).length ? clean : { ...SALES_PAGE_MAP_DEFAULT };
        console.log(`🛒 [${req.funnel}] sales-page map set:`, JSON.stringify(effective));
        res.json({ map: effective, isDefault: Object.keys(clean).length === 0 });
    } catch (err) {
        console.error('❌ PUT /api/sales-pages error:', err.message);
        res.status(500).json({ error: err.message || 'Internal server error' });
    }
});

// GET /api/metrics — Fetch all daily metrics (with caching)
app.get('/api/metrics', dashboardLimiter, async (req, res) => {
    try {
        const funnel = resolveFunnel(req, 'analytics');
        const supabase = clientFor(funnel);
        const bucket = getCacheBucket(funnel);
        const { limit = 90, offset = 0, variant: variantRaw = 'all', expand } = req.query;
        const ALLOWED_VARIANTS = ['all', 'A', 'B', 'undetected'];
        const variant = ALLOWED_VARIANTS.includes(String(variantRaw)) ? String(variantRaw) : 'all';
        // expand=variants embeds per-variant (A/B/undetected) breakdowns on every
        // row so the dashboard can switch the split-test toggle CLIENT-SIDE with no
        // refetch (kills the flicker + per-toggle latency). In this mode the
        // top-level values stay the 'all' view (unchanged — canonical columns still
        // back finalized days) and a `variants` object carries each bucket from live
        // dedup. For NON-finalized days all === A+B+undetected by construction.
        const expandVariants = String(expand) === 'variants';
        const isDefaultPage = Number(limit) === 90 && Number(offset) === 0;

        // ── Response cache (default pagination only) — the plain 'all' view and
        //    the expanded payload each get their own cache slot ──
        const now = Date.now();
        if (isDefaultPage) {
            if (expandVariants && bucket.metricsResponseExpanded && (now - bucket.metricsExpandedUpdatedAt) < cache.metricsTTL) {
                cache.hits++;
                return res.json(bucket.metricsResponseExpanded);
            }
            if (!expandVariants && variant === 'all' && bucket.metricsResponse && (now - bucket.metricsUpdatedAt) < cache.metricsTTL) {
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
        // Plain 'all' view: skip dedup on finalized rows (canonical columns hold the
        // durable total). Variant/expanded view: needs the A/B/undetected split — read it
        // from the persisted variant_splits column for finalized days, and only dedup days
        // that lack it (today + any finalized day not yet backfilled). This is what stops
        // the ~20s full-history recompute on a cold cache.
        const forceAllDedup = variant !== 'all' || expandVariants;
        const hasStoredSplits = (r) => r.variant_splits && typeof r.variant_splits === 'object' && Object.keys(r.variant_splits).length > 0;
        const dates = (data || [])
            .filter(r => !r.finalized_at || (forceAllDedup && !hasStoredSplits(r)))
            .map(r => String(r.date).substring(0, 10));
        const dedupMap = await getDedupCounts(funnel, dates);
        // Reg-page unique visitors per variant (shumard pageviews) — for ALL row dates,
        // independent of the FB fb_link_clicks total.
        const regVisits = await getRegPageVisits(funnel, (data || []).map(r => String(r.date).substring(0, 10)));

        const DEDUP_COLS = new Set([
            'registrations', 'attended', 'replays', 'viewedcta', 'clickedcta',
            'purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_aibot_b', 'purchases_postwebinar', 'purchases_cpa', 'purchases_sales_a', 'purchases_sales_b',
            'stayed_45', 'stayed_60', 'stayed_80',
        ]);
        const PURCHASE_SUB = ['purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_aibot_b', 'purchases_postwebinar', 'purchases_cpa', 'purchases_sales_a', 'purchases_sales_b'];

        // Convert to frontend format (MM/DD/YYYY)
        const formatted = (data || []).map(row => {
            const dateStr = String(row.date).substring(0, 10);
            const mmddyyyy = dateStr.replace(/(\d{4})-(\d{2})-(\d{2})/, '$2/$3/$1');
            const deduped = dedupMap[dateStr] || {};
            const hasDedup = Object.keys(deduped).length > 0;
            const rv = regVisits[dateStr] || {}; // reg-page visitors per variant for this day
            const ov = row.overrides || {};
            const isFinalized = !!row.finalized_at;
            const splits = (row.variant_splits && typeof row.variant_splits === 'object') ? row.variant_splits : null;
            // Resolve one event field for a given variant bucket.
            //   - 'all' view: admin override wins; finalized days use the canonical
            //     column (durable — survives event pruning); otherwise live dedup.
            //   - A/B/undetected: finalized days use the persisted variant_splits column
            //     (no dedup); otherwise live dedup. Canonical columns are variant-blind.
            // Once dedup has run for a date, a missing key means 0 events — never
            // fall through to the raw column (which can be briefly ahead of dedup).
            const pickV = (field, vnt) => {
                if (vnt === 'all') {
                    if (ov[field] !== undefined) return ov[field];
                    if (isFinalized && DEDUP_COLS.has(field)) return Number(row[field]) || 0;
                } else if (isFinalized && splits && DEDUP_COLS.has(field)) {
                    return (splits[field] && splits[field][vnt]) || 0;
                }
                const dd = deduped[field];
                if (dd !== undefined) return dd[vnt] ?? 0;
                if (hasDedup && DEDUP_COLS.has(field)) return 0;
                if (vnt !== 'all' && DEDUP_COLS.has(field)) return 0;
                return row[field];
            };
            // Build the per-bucket event-count fields incl. derived purchase totals.
            const buildVals = (vnt) => {
                const o = {
                    registrations: pickV('registrations', vnt),
                    replays: pickV('replays', vnt),
                    viewedcta: pickV('viewedcta', vnt),
                    clickedcta: pickV('clickedcta', vnt),
                    purchases_fb: pickV('purchases_fb', vnt),
                    purchases_native: pickV('purchases_native', vnt),
                    purchases_youtube: pickV('purchases_youtube', vnt),
                    purchases_aibot: pickV('purchases_aibot', vnt),
                    purchases_aibot_b: pickV('purchases_aibot_b', vnt),
                    purchases_postwebinar: pickV('purchases_postwebinar', vnt),
                    purchases_cpa: pickV('purchases_cpa', vnt),
                    purchases_sales_a: pickV('purchases_sales_a', vnt),
                    purchases_sales_b: pickV('purchases_sales_b', vnt),
                    stayed_45: pickV('stayed_45', vnt),
                    stayed_60: pickV('stayed_60', vnt),
                    stayed_80: pickV('stayed_80', vnt),
                    attended: pickV('attended', vnt),
                };
                const tp = PURCHASE_SUB.reduce((s, k) => s + (o[k] || 0), 0);
                o.total_purchases = tp;
                o.purchases = tp; // alias for backward-compat custom formulas
                o.reg_page_visits = rv[vnt] || 0; // unique tracked reg-page visitors
                return o;
            };
            // Non-expanded: top level reflects the requested `variant` (default 'all').
            // Expanded: top level is 'all' and we attach the per-variant breakdown.
            const topVariant = expandVariants ? 'all' : variant;
            const out = {
                date: mmddyyyy,
                day: row.day_of_week,
                // fb_spend & fb_link_clicks have no A/B dimension — always the day total.
                fb_spend: ov.fb_spend !== undefined ? ov.fb_spend : Number(row.fb_spend),
                fb_link_clicks: ov.fb_link_clicks !== undefined ? ov.fb_link_clicks : Number(row.fb_link_clicks || 0),
                ...buildVals(topVariant),
                created_at: row.created_at,
                updated_at: row.updated_at,
            };
            if (expandVariants) {
                out.variants = { A: buildVals('A'), B: buildVals('B'), undetected: buildVals('undetected') };
            }
            return out;
        });

        const response = { data: formatted, total: count };

        // ── Store in response cache (default pagination only) ────────────
        // Skip the write if an invalidation ran mid-flight — the response
        // we just built reflects pre-invalidation Supabase state and would
        // mask the new data for up to metricsTTL.
        if (isDefaultPage && bucket.invalidationEpoch === epochAtStart) {
            if (expandVariants) {
                bucket.metricsResponseExpanded = response;
                bucket.metricsExpandedUpdatedAt = now;
            } else if (variant === 'all') {
                bucket.metricsResponse = response;
                bucket.metricsUpdatedAt = now;
            }
        } else if (isDefaultPage) {
            console.log(`⚠️  Cache[${funnel}]: skipped metrics response write — invalidation occurred mid-fetch (epoch ${epochAtStart} → ${bucket.invalidationEpoch})`);
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
        const OVERRIDE_FIELDS = ['fb_spend', 'fb_link_clicks', 'registrations', 'replays', 'viewedcta', 'clickedcta', 'purchases', 'attended', 'purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_aibot_b', 'purchases_postwebinar', 'purchases_cpa', 'purchases_sales_a', 'purchases_sales_b', 'stayed_45', 'stayed_60', 'stayed_80'];
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

// Activity Log endpoint. Named "activity" rather than the "events" keyword that
// ad/content blockers (EasyPrivacy etc.) filter — that keyword broke this in
// blocker-enabled browsers (e.g. Arc). Reads per-person rows from the events table.
app.get('/api/activity', dashboardLimiter, async (req, res) => {
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
        console.error('❌ GET /api/activity error:', err.message);
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

// POST /api/admin/backfill-variant-splits — populate daily_metrics.variant_splits for
// already-finalized past days that lack it, so the expanded dashboard view stops re-deduping
// the whole event history on a cold cache. Writes ONLY variant_splits — canonical totals and
// finalized_at are left untouched (so historical numbers can't shift). Computes the whole
// span in one dedup pass. Idempotent: skips days that already have splits. Run once after the
// variant_splits migration is applied. Optional body: { force:true } to recompute all days.
app.post('/api/admin/backfill-variant-splits', requireAuth, requireAdmin, async (req, res) => {
    try {
        const funnel = req.funnel;
        const sb = clientFor(funnel);
        const force = !!(req.body && req.body.force);
        const todayISO = dateToISO(getLADate());

        const { data: rows, error } = await sb.from('daily_metrics')
            .select('date, finalized_at, variant_splits')
            .not('finalized_at', 'is', null)
            .lt('date', todayISO)
            .order('date', { ascending: true });
        if (error) {
            if (/variant_splits/.test(error.message || '')) {
                return res.status(400).json({ error: 'variant_splits column not found — run db/migrate_variant_splits.sql first' });
            }
            throw error;
        }
        const has = (r) => r.variant_splits && typeof r.variant_splits === 'object' && Object.keys(r.variant_splits).length > 0;
        const targets = (rows || []).filter(r => force || !has(r));
        if (!targets.length) {
            return res.json({ ok: true, finalized_days: (rows || []).length, backfilled: 0, message: 'nothing to backfill' });
        }

        // One wide dedup pass over the full span (+60-day lookback for early registrations).
        const minDate = String(targets[0].date).substring(0, 10);
        const from = new Date(minDate + 'T12:00:00Z'); from.setUTCDate(from.getUTCDate() - 60);
        const fromISO = from.toISOString().slice(0, 10);
        const abCutoff = await getAbTestStart(funnel);
        const aliasMap = await getCombinedAliases(funnel);
        const emailPhones = await getTrackingPhones(funnel);
        const events = await fetchEventsForDateRange(funnel, fromISO, todayISO);
        const dedupMap = computeDedupFromEvents(events, abCutoff, aliasMap, emailPhones);

        const FIELDS = [
            'registrations', 'attended', 'replays', 'viewedcta', 'clickedcta',
            'purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_aibot_b',
            'purchases_postwebinar', 'purchases_cpa', 'purchases_sales_a', 'purchases_sales_b',
            'stayed_45', 'stayed_60', 'stayed_80',
        ];
        let updated = 0;
        for (const t of targets) {
            const d = String(t.date).substring(0, 10);
            const counts = dedupMap[d] || {};
            const splits = {};
            for (const f of FIELDS) {
                const c = counts[f];
                if (c) splits[f] = { A: Number(c.A) || 0, B: Number(c.B) || 0, undetected: Number(c.undetected) || 0 };
            }
            const { error: upErr } = await sb.from('daily_metrics').update({ variant_splits: splits }).eq('date', d);
            if (!upErr) updated++;
            else console.warn(`⚠️ backfill splits failed [${funnel} ${d}]:`, upErr.message);
        }
        invalidateMetricsCache(funnel);
        console.log(`🧬 Backfill variant_splits[${funnel}]: ${updated}/${targets.length} day(s) by ${req.user.email}`);
        res.json({ ok: true, finalized_days: (rows || []).length, backfilled: updated, of: targets.length });
    } catch (err) {
        console.error('❌ POST /api/admin/backfill-variant-splits error:', err.message);
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

        const { table = 'events', dateFrom, dateTo, eventType, search, sortBy, sortDir = 'desc', limit = 500, variant: variantRaw } = req.body;
        // Optional split-test variant filter: 'A' | 'B' | 'undetected' (else all)
        const variantFilter = ['A', 'B', 'undetected'].includes(String(variantRaw)) ? String(variantRaw) : null;

        // Only allow querying safe tables
        const ALLOWED_TABLES = ['events', 'daily_metrics', 'dashboard'];
        if (!ALLOWED_TABLES.includes(table)) return res.status(400).json({ error: 'Invalid table' });

        // ── Dashboard Events: the actual deduped people the dashboard counted ──
        if (table === 'dashboard') {
            // Fetch events for the requested date range
            const fetchFrom = dateFrom || '2020-01-01';
            const fetchTo = dateTo || dateToISO(getLADate());
            const events = await fetchEventsForDateRange(req.funnel, fetchFrom, fetchTo);
            // Same variant attribution the dashboard uses (A/B start cutoff + manual
            // identity links), so a variant filter here matches the dashboard's counts.
            const dashAlias = await getCombinedAliases(req.funnel);
            const variantOf = buildVariantResolver(events, await getAbTestStart(req.funnel), dashAlias, await getTrackingPhones(req.funnel));

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
                let userKey = (ev.email || '').toLowerCase();
                if (dashAlias && userKey && dashAlias[userKey]) userKey = dashAlias[userKey];
                userKey = userKey || ev.phone || (ev.name || '').toLowerCase();
                if (!userKey) continue; // anonymous events can't be deduped

                const dedupKey = `${dashDate}|${eventKey}|${userKey}`;
                if (seen[dedupKey]) continue;
                seen[dedupKey] = true;

                // Variant filter (matches the dashboard's per-variant attribution)
                const variant = variantOf(ev);
                if (variantFilter && variant !== variantFilter) continue;

                dedupedEvents.push({
                    dashboard_date: dashDate,
                    event_type: ev.event_type,
                    variant,
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
            // Raw events carry the variant they arrived with (own tag). 'undetected'
            // here = no variant in the payload. (For dashboard-attributed variant —
            // incl. forward-only inheritance — use the Dashboard table.)
            if (variantFilter === 'A' || variantFilter === 'B') query = query.eq('metadata->>variant', variantFilter);
            else if (variantFilter === 'undetected') query = query.is('metadata->>variant', null);
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

// GoHighLevel MCP server (opportunities/pipelines for AI Insights).
// Attached to the chat only when token + location are configured.
const GHL_MCP_TOKEN = process.env.GHL_MCP_TOKEN;       // Private Integration token (pit-...)
const GHL_LOCATION_ID = process.env.GHL_LOCATION_ID;   // GHL sub-account (location) ID
const GHL_PIPELINE = process.env.GHL_PIPELINE;         // opportunity pipeline to analyze (name or ID)
const GHL_ENABLED = !!(GHL_MCP_TOKEN && GHL_LOCATION_ID);

// ─── GoHighLevel REST helpers (native join tool) ─────────────────────────────
// The MCP connector handles ad-hoc lookups, but joining a funnel segment
// against the pipeline needs the WHOLE opportunity list — paging it through
// MCP round-trips blows the chat deadline. These helpers pull it directly from
// GHL's REST API (parallel pages) and cache the snapshot for 5 minutes.
const GHL_API = 'https://services.leadconnectorhq.com';
const ghlSnapshot = { at: 0, pipeline: null, byEmail: null, stageCounts: null, total: 0, inflight: null };

async function ghlGet(path, params) {
    const url = new URL(GHL_API + path);
    for (const [k, v] of Object.entries(params || {})) if (v !== undefined) url.searchParams.set(k, v);
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 15_000);
    try {
        const res = await fetch(url, {
            headers: { Authorization: `Bearer ${GHL_MCP_TOKEN}`, Version: '2021-07-28', Accept: 'application/json' },
            signal: ctrl.signal,
        });
        if (!res.ok) throw new Error(`GHL ${path} → HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
        return await res.json();
    } finally {
        clearTimeout(timer);
    }
}

// Resolve GHL_PIPELINE (name or ID) → { id, name, stages: Map(stageId → name) }
async function ghlResolvePipeline() {
    const data = await ghlGet('/opportunities/pipelines', { locationId: GHL_LOCATION_ID });
    const pipelines = data.pipelines || [];
    const want = (GHL_PIPELINE || '').trim().toLowerCase();
    const p = want
        ? pipelines.find(x => x.id === GHL_PIPELINE || String(x.name || '').trim().toLowerCase() === want)
        : pipelines[0];
    if (!p) throw new Error(`GHL pipeline "${GHL_PIPELINE}" not found. Available: ${pipelines.map(x => x.name).join(', ')}`);
    return { id: p.id, name: p.name, stages: new Map((p.stages || []).map(s => [s.id, s.name])) };
}

// Snapshot of every opportunity in the configured pipeline, keyed by contact
// email (latest opportunity wins). Cached 5 min; concurrent callers share one fetch.
async function ghlPipelineSnapshot() {
    if (ghlSnapshot.byEmail && Date.now() - ghlSnapshot.at < 300_000) return ghlSnapshot;
    if (ghlSnapshot.inflight) return ghlSnapshot.inflight;
    ghlSnapshot.inflight = (async () => {
        const pipeline = await ghlResolvePipeline();
        const PAGE = 100;
        const search = page => ghlGet('/opportunities/search', {
            location_id: GHL_LOCATION_ID,
            pipeline_id: pipeline.id,
            limit: PAGE,
            page,
        });
        const first = await search(1);
        const total = first.meta?.total ?? (first.opportunities || []).length;
        const pages = Math.min(Math.ceil(total / PAGE), 150); // safety cap ~15k opps
        const all = [...(first.opportunities || [])];
        for (let start = 2; start <= pages; start += 6) {     // 6 parallel pages per burst
            const batch = [];
            for (let p = start; p < start + 6 && p <= pages; p++) batch.push(search(p));
            for (const r of await Promise.all(batch)) all.push(...(r.opportunities || []));
        }
        const byEmail = new Map();
        const stageCounts = {};
        for (const o of all) {
            const stage = pipeline.stages.get(o.pipelineStageId) || o.pipelineStageId || 'unknown';
            stageCounts[stage] = (stageCounts[stage] || 0) + 1;
            const email = String(o.contact?.email || '').trim().toLowerCase();
            if (!email) continue;
            const rec = { stage, status: o.status, name: o.contact?.name || o.name, updated_at: o.updatedAt, monetary_value: o.monetaryValue || 0 };
            const prev = byEmail.get(email);
            if (!prev || String(rec.updated_at) > String(prev.updated_at)) byEmail.set(email, rec);
        }
        Object.assign(ghlSnapshot, { at: Date.now(), pipeline: { id: pipeline.id, name: pipeline.name }, byEmail, stageCounts, total: all.length });
        return ghlSnapshot;
    })().finally(() => { ghlSnapshot.inflight = null; });
    return ghlSnapshot.inflight;
}

const GHL_JOINABLE_EVENTS = new Set(['purchases', 'registrations', 'attended', 'replays', 'viewedcta', 'clickedcta']);

// get_ghl_pipeline_status tool: join a funnel segment (event_type + LA-timezone
// date range, or explicit emails) against the GHL pipeline snapshot, by email.
async function getGhlPipelineStatus(funnel, input) {
    if (!GHL_ENABLED) return { error: 'GoHighLevel is not configured (GHL_MCP_TOKEN / GHL_LOCATION_ID)' };
    let emails, segmentLabel;
    let segmentCapped = false;
    if (Array.isArray(input.emails) && input.emails.length) {
        emails = [...new Set(input.emails.map(e => String(e).trim().toLowerCase()).filter(e => e.includes('@')))];
        segmentLabel = `explicit list (${emails.length} emails)`;
    } else {
        const eventType = String(input.event_type || 'purchases');
        if (!GHL_JOINABLE_EVENTS.has(eventType)) return { error: `event_type must be one of: ${[...GHL_JOINABLE_EVENTS].join(', ')}` };
        const dateOk = d => /^\d{4}-\d{2}-\d{2}$/.test(d);
        const conds = [`event_type = '${eventType}'`, 'email IS NOT NULL', "email <> ''"];
        if (input.date_from) {
            if (!dateOk(input.date_from)) return { error: 'date_from must be YYYY-MM-DD' };
            conds.push(`(event_time AT TIME ZONE 'America/Los_Angeles')::date >= '${input.date_from}'`);
        }
        if (input.date_to) {
            if (!dateOk(input.date_to)) return { error: 'date_to must be YYYY-MM-DD' };
            conds.push(`(event_time AT TIME ZONE 'America/Los_Angeles')::date <= '${input.date_to}'`);
        }
        const out = await runReadOnlySQL(funnel, `SELECT DISTINCT lower(email) AS email FROM events WHERE ${conds.join(' AND ')}`);
        if (out.error) return out;
        emails = (out.rows || []).map(r => r.email);
        segmentCapped = emails.length >= 500; // ai_run_sql caps at 500 rows
        segmentLabel = `${eventType}${input.date_from || input.date_to ? ` ${input.date_from || '…'} → ${input.date_to || '…'}` : ' (all time)'}`;
    }

    const snap = await ghlPipelineSnapshot();
    const matched = [];
    const unmatched = [];
    const countsByStage = {};
    for (const email of emails) {
        const rec = snap.byEmail.get(email);
        if (!rec) { unmatched.push(email); continue; }
        matched.push({ email, ...rec });
        countsByStage[rec.stage] = (countsByStage[rec.stage] || 0) + 1;
    }
    let people = matched;
    if (input.stage_filter) {
        const f = String(input.stage_filter).toLowerCase();
        people = matched.filter(p => String(p.stage).toLowerCase().includes(f));
    }
    return {
        pipeline: snap.pipeline.name,
        pipeline_total_opportunities: snap.total,
        segment: segmentLabel,
        segment_size: emails.length,
        segment_capped_at_500: segmentCapped || undefined,
        matched_in_pipeline: matched.length,
        not_in_pipeline: unmatched.length,
        counts_by_stage: countsByStage,
        stage_filter: input.stage_filter || undefined,
        stage_filter_matches: input.stage_filter ? people.length : undefined,
        people: people.slice(0, 150),
        people_truncated: people.length > 150 ? `showing 150 of ${people.length}` : undefined,
        unmatched_sample: unmatched.slice(0, 20),
    };
}

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

    const PURCHASE_SUB_COLS = new Set(['purchases_fb', 'purchases_native', 'purchases_youtube', 'purchases_aibot', 'purchases_aibot_b', 'purchases_postwebinar', 'purchases_cpa', 'purchases_sales_a', 'purchases_sales_b', 'stayed_45', 'stayed_60', 'stayed_80']);
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
            purchases_aibot_b: pick('purchases_aibot_b') || 0,
            purchases_postwebinar: pick('purchases_postwebinar') || 0,
            purchases_cpa: pick('purchases_cpa') || 0,
            purchases_sales_a: pick('purchases_sales_a') || 0,
            purchases_sales_b: pick('purchases_sales_b') || 0,
            stayed_45: pick('stayed_45') || 0,
            stayed_60: pick('stayed_60') || 0,
            stayed_80: pick('stayed_80') || 0,
            total_purchases: (pick('purchases_fb') || 0) + (pick('purchases_native') || 0) +
                             (pick('purchases_youtube') || 0) + (pick('purchases_aibot') || 0) + (pick('purchases_aibot_b') || 0) +
                             (pick('purchases_postwebinar') || 0) + (pick('purchases_cpa') || 0) +
                             (pick('purchases_sales_a') || 0) + (pick('purchases_sales_b') || 0),
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
    const ADDITIVE = ['fb_spend','fb_link_clicks','registrations','attended','replays','viewedcta','clickedcta','purchases_fb','purchases_native','purchases_youtube','purchases_aibot','purchases_aibot_b','purchases_postwebinar','purchases_cpa','purchases_sales_a','purchases_sales_b','stayed_45','stayed_60','stayed_80','total_purchases'];

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

    const ADDITIVE = ['fb_spend','fb_link_clicks','registrations','attended','replays','viewedcta','clickedcta','purchases_fb','purchases_native','purchases_youtube','purchases_aibot','purchases_aibot_b','purchases_postwebinar','purchases_cpa','purchases_sales_a','purchases_sales_b','total_purchases'];
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

// Compact, cached CRM/journey rollups so common journey + webinar-timing questions
// answer straight from the system prompt with no extra egress. Each query aggregates
// inside Postgres (via the read-only ai_run_sql RPC) and returns only a handful of
// rows. Refreshed on the same cadence/invalidation as the metrics cache.
async function getInsightsJourneyDigest(funnel) {
    const bucket = getCacheBucket(funnel);
    const now = Date.now();
    if (bucket.journeyDigest && (now - (bucket.journeyDigestAt || 0)) < cache.insightsTTL) {
        return bucket.journeyDigest;
    }
    const supabase = clientFor(funnel);
    const q = async (query) => {
        const { data } = await supabase.rpc('ai_run_sql', { query });
        return Array.isArray(data) ? data : [];  // ai_run_sql returns {error} (not an array) if a table is absent
    };
    const [funnelStages, webinarSlots, regsByDayOfWeek, purchasesBySource] = await Promise.all([
        q("SELECT stage, count(*)::int AS people FROM crm_people GROUP BY 1 ORDER BY people DESC"),
        q("SELECT metadata->>'webinar_datetime_utc' AS slot, count(*)::int AS regs FROM events WHERE event_type='registrations' AND metadata->>'webinar_datetime_utc' IS NOT NULL GROUP BY 1 ORDER BY regs DESC LIMIT 12"),
        q("SELECT trim(to_char(event_time AT TIME ZONE 'America/Los_Angeles','Day')) AS dow, count(*)::int AS regs FROM events WHERE event_type='registrations' GROUP BY 1 ORDER BY regs DESC"),
        q("SELECT coalesce(metadata->>'source','(unknown)') AS source, count(*)::int AS purchases FROM events WHERE event_type='purchases' GROUP BY 1 ORDER BY purchases DESC"),
    ]);
    const digest = { funnelStages, webinarSlots, regsByDayOfWeek, purchasesBySource };
    bucket.journeyDigest = digest;
    bucket.journeyDigestAt = now;
    return digest;
}

// Full funnel as DISTINCT PEOPLE per stage, with stage-to-stage conversion and
// drop-off. Optional date range (by event_time, LA tz). Aggregated in Postgres.
async function getJourneyFunnel(funnel, from, to) {
    const supabase = clientFor(funnel);
    let where = 'email IS NOT NULL';
    if (from) where += ` AND event_time >= '${String(from).replace(/'/g, '')}T00:00:00-08:00'`;
    if (to)   where += ` AND event_time < ('${String(to).replace(/'/g, '')}T00:00:00-08:00'::timestamptz + interval '1 day')`;
    const sql = `SELECT
        count(DISTINCT email) FILTER (WHERE event_type='registrations') AS registered,
        count(DISTINCT email) FILTER (WHERE event_type='attended')      AS attended,
        count(DISTINCT email) FILTER (WHERE event_type='replays')       AS watched_replay,
        count(DISTINCT email) FILTER (WHERE event_type='viewedcta')     AS saw_cta,
        count(DISTINCT email) FILTER (WHERE event_type='clickedcta')    AS clicked_cta,
        count(DISTINCT email) FILTER (WHERE event_type='purchases')     AS purchased
      FROM events WHERE ${where}`;
    const { data } = await supabase.rpc('ai_run_sql', { query: sql });
    const row = (Array.isArray(data) && data[0]) ? data[0] : {};
    const n = (v) => Number(v) || 0;
    const pct = (a, b) => (b > 0 ? Math.round((a / b) * 1000) / 10 : null);
    const reg = n(row.registered);
    return {
        date_from: from || null, date_to: to || null,
        stages: {
            registered: reg, attended: n(row.attended), watched_replay: n(row.watched_replay),
            saw_cta: n(row.saw_cta), clicked_cta: n(row.clicked_cta), purchased: n(row.purchased),
        },
        conversion_pct: {
            registered_to_attended: pct(n(row.attended), reg),
            attended_to_saw_cta: pct(n(row.saw_cta), n(row.attended)),
            saw_to_clicked_cta: pct(n(row.clicked_cta), n(row.saw_cta)),
            clicked_to_purchase: pct(n(row.purchased), n(row.clicked_cta)),
            registered_to_purchase: pct(n(row.purchased), reg),
        },
    };
}

// A/B split-test funnel: distinct people per stage, broken down by variant
// (A / B / undetected), with stage-to-stage conversion %. Attribution mirrors the
// dashboard (buildVariantResolver): own-tag-first (registrations + Stealth-stamped
// downstream), untagged registration → undetected, untagged downstream inherits the
// registrant's variant forward-only (email→phone). Honors the A/B test start cutoff:
// events before it (pre-launch test traffic) are 'undetected'. Optional date range
// filters which EVENTS count.
async function getVariantFunnel(funnel, from, to) {
    const supabase = clientFor(funnel);
    const abCutoff = await getAbTestStart(funnel);
    const cutoffIso = abCutoff ? new Date(abCutoff).toISOString() : null;
    const regCutoff = cutoffIso ? ` AND event_time >= '${cutoffIso}'` : '';
    const evCutoffCase = cutoffIso ? `WHEN e.event_time < '${cutoffIso}' THEN 'undetected'\n                   ` : '';
    // Manual sale→registrant links, inlined as a VALUES CTE (no dependency on the
    // table existing) so an event's email canonicalizes to its linked registrant.
    const aliasEntries = Object.entries(await getCombinedAliases(funnel));
    const linksPrefix = aliasEntries.length
        ? `links(alias, canon) AS (VALUES ${aliasEntries.map(([a, c]) => `('${a.replace(/'/g, "''")}','${c.replace(/'/g, "''")}')`).join(', ')}),\n    `
        : '';
    const canonEmail = aliasEntries.length
        ? `coalesce((SELECT canon FROM links WHERE alias = lower(e.email)), lower(e.email))`
        : `lower(e.email)`;
    let evWhere = '(e.email IS NOT NULL OR e.phone IS NOT NULL)';
    if (from) evWhere += ` AND e.event_time >= '${String(from).replace(/'/g, '')}T00:00:00-08:00'`;
    if (to)   evWhere += ` AND e.event_time < ('${String(to).replace(/'/g, '')}T00:00:00-08:00'::timestamptz + interval '1 day')`;
    // last-10-digits phone key (NULL if <10 digits) — mirrors normalizePhoneKey()
    const phoneKey = (col) => `(CASE WHEN length(regexp_replace(coalesce(${col},''),'[^0-9]','','g')) >= 10 THEN right(regexp_replace(coalesce(${col},''),'[^0-9]','','g'),10) END)`;
    // Person key for distinct counting: email if present, else phone (matches the
    // dashboard's email||phone dedup key so phone-only buyers are still counted).
    const personKey = `coalesce(nullif(${canonEmail},''), ${phoneKey('e.phone')})`;
    const sql = `WITH ${linksPrefix}tc_phone AS (
        -- email → a shumard-tracked phone (so events with no phone of their own still match)
        SELECT DISTINCT ON (lower(email)) lower(email) AS email_key, ${phoneKey('phone')} AS phone_key
        FROM tracking_contacts
        WHERE email IS NOT NULL AND ${phoneKey('phone')} IS NOT NULL
        ORDER BY lower(email)
    ), reg_v AS (
        SELECT lower(r.email) AS email_key,
               coalesce(${phoneKey('r.phone')}, rtc.phone_key) AS phone_key,
               upper(trim(r.metadata->>'variant')) AS variant, r.event_time
        FROM events r
        LEFT JOIN tc_phone rtc ON rtc.email_key = lower(r.email)
        WHERE r.event_type='registrations'
          AND upper(trim(coalesce(r.metadata->>'variant',''))) IN ('A','B')${regCutoff}
    ), email_map AS (
        SELECT DISTINCT ON (email_key) email_key, variant, event_time AS since FROM reg_v
        WHERE email_key IS NOT NULL AND email_key <> '' ORDER BY email_key, event_time ASC
    ), phone_map AS (
        SELECT DISTINCT ON (phone_key) phone_key, variant, event_time AS since FROM reg_v
        WHERE phone_key IS NOT NULL ORDER BY phone_key, event_time ASC
    ), ev AS (
        SELECT e.event_type, ${personKey} AS person_key,
               CASE
                   ${evCutoffCase}-- 1) the event's OWN tag wins (any type — Stealth stamps downstream too)
                   WHEN upper(trim(coalesce(e.metadata->>'variant',''))) IN ('A','B')
                        THEN upper(trim(e.metadata->>'variant'))
                   -- 2) an untagged registration never inherits
                   WHEN e.event_type = 'registrations' THEN 'undetected'
                   -- 3) untagged downstream: inherit via email then phone (own, then the
                   --    shumard-tracked phone for this email), FORWARD-ONLY
                   WHEN em.variant IS NOT NULL AND e.event_time >= em.since THEN em.variant
                   WHEN pm.variant IS NOT NULL AND e.event_time >= pm.since THEN pm.variant
                   ELSE 'undetected'
               END AS variant
        FROM events e
        LEFT JOIN email_map em ON em.email_key = ${canonEmail}
        LEFT JOIN tc_phone etc ON etc.email_key = lower(e.email)
        LEFT JOIN phone_map pm ON pm.phone_key = coalesce(${phoneKey('e.phone')}, etc.phone_key)
        WHERE ${evWhere}
    )
    SELECT variant,
        count(DISTINCT person_key) FILTER (WHERE event_type='registrations') AS registered,
        count(DISTINCT person_key) FILTER (WHERE event_type='attended')      AS attended,
        count(DISTINCT person_key) FILTER (WHERE event_type='replays')       AS watched_replay,
        count(DISTINCT person_key) FILTER (WHERE event_type='viewedcta')     AS saw_cta,
        count(DISTINCT person_key) FILTER (WHERE event_type='clickedcta')    AS clicked_cta,
        count(DISTINCT person_key) FILTER (WHERE event_type='purchases')     AS purchased
      FROM ev GROUP BY variant`;
    const { data } = await supabase.rpc('ai_run_sql', { query: sql });
    const rows = Array.isArray(data) ? data : [];
    const n = (v) => Number(v) || 0;
    const pct = (a, b) => (b > 0 ? Math.round((a / b) * 1000) / 10 : null);
    const build = (row) => {
        const reg = n(row.registered);
        return {
            stages: {
                registered: reg, attended: n(row.attended), watched_replay: n(row.watched_replay),
                saw_cta: n(row.saw_cta), clicked_cta: n(row.clicked_cta), purchased: n(row.purchased),
            },
            conversion_pct: {
                registered_to_attended: pct(n(row.attended), reg),
                attended_to_saw_cta: pct(n(row.saw_cta), n(row.attended)),
                saw_to_clicked_cta: pct(n(row.clicked_cta), n(row.saw_cta)),
                clicked_to_purchase: pct(n(row.purchased), n(row.clicked_cta)),
                registered_to_purchase: pct(n(row.purchased), reg),
            },
        };
    };
    const variants = {};
    for (const v of ['A', 'B', 'undetected']) {
        variants[v] = build(rows.find(r => r.variant === v) || {});
    }
    // Headline A-vs-B comparison on the registered→purchase rate (the bottom-line
    // metric for a split test). Only meaningful when both variants have registrants.
    const aRate = variants.A.conversion_pct.registered_to_purchase;
    const bRate = variants.B.conversion_pct.registered_to_purchase;
    let comparison = null;
    if (aRate !== null && bRate !== null) {
        comparison = {
            metric: 'registered_to_purchase_pct',
            A: aRate, B: bRate,
            absolute_diff_pts: Math.round((aRate - bRate) * 10) / 10,
            relative_lift_pct: bRate > 0 ? Math.round(((aRate - bRate) / bRate) * 1000) / 10 : null,
            leader: aRate === bRate ? 'tie' : (aRate > bRate ? 'A' : 'B'),
        };
    }
    return {
        date_from: from || null, date_to: to || null,
        variants,
        comparison,
        note: "Attribution rules: (1) any event with its OWN metadata.variant ('A'/'B') uses it — registrations AND downstream events (Stealth stamps the variant on attended/CTA/purchase too). (2) An untagged registration is 'undetected' and is NEVER back-filled. (3) An untagged downstream event inherits the registrant's variant via email then phone (last-10-digits), but FORWARD-ONLY — only if it occurred at/after the variant was assigned; events predating the person's experiment entry, and purchases whose checkout email/phone never matched a tagged registration, are 'undetected'. FB spend and reg-page link clicks have NO variant dimension — do NOT compute cost-per-variant (cost/reg, CPA, ROAS) from a single variant's counts.",
    };
}

// Live data dictionary so the AI knows the FULL shape of the journey data without
// guessing: table columns, the inventory of events.metadata keys (+ sample values),
// and the enum-ish values actually in use (event types, purchase sources, stages,
// tags) plus totals and date range. Cached on the metrics cadence.
async function getJourneyDataDictionary(funnel) {
    const bucket = getCacheBucket(funnel);
    const now = Date.now();
    if (bucket.dataDictionary && (now - (bucket.dataDictionaryAt || 0)) < cache.insightsTTL) {
        return bucket.dataDictionary;
    }
    const supabase = clientFor(funnel);
    const q = async (query) => { const { data } = await supabase.rpc('ai_run_sql', { query }); return Array.isArray(data) ? data : []; };
    const [eventTypes, metadataKeys, purchaseSources, stages, tags, totals] = await Promise.all([
        q("SELECT event_type, count(*)::int AS count FROM events GROUP BY 1 ORDER BY count DESC"),
        q("SELECT k AS key, count(*)::int AS occurrences, (array_agg(DISTINCT left(v, 80)))[1:4] AS sample_values FROM events e, LATERAL jsonb_each_text(e.metadata) AS kv(k, v) WHERE jsonb_typeof(e.metadata) = 'object' GROUP BY k ORDER BY occurrences DESC"),
        q("SELECT coalesce(metadata->>'source', '(unknown)') AS source, count(*)::int AS purchases FROM events WHERE event_type='purchases' GROUP BY 1 ORDER BY purchases DESC"),
        q("SELECT stage, count(*)::int AS people FROM crm_people GROUP BY 1 ORDER BY people DESC"),
        q("SELECT t AS tag, count(*)::int AS contacts FROM tracking_contacts, unnest(tags) AS t GROUP BY 1 ORDER BY contacts DESC LIMIT 50"),
        q("SELECT (SELECT count(*) FROM events)::int AS total_events, (SELECT count(*) FROM crm_people)::int AS total_people, (SELECT count(*) FROM crm_people WHERE is_tracked)::int AS tracked_people, (SELECT count(*) FROM crm_people WHERE NOT is_tracked)::int AS legacy_people, (SELECT min(event_time) FROM events) AS earliest_event, (SELECT max(event_time) FROM events) AS latest_event"),
    ]);
    const salesPages = await getSalesPageMap(funnel); // { "<url>": "<label>" }
    const dict = {
        tables: {
            events: 'id, event_type, name, email, phone, metadata (jsonb), event_time, created_at — one row per funnel action',
            crm_people: 'email, name, phone, contact_id, is_tracked, stage, has_registration, has_attended, has_replay, has_viewedcta, has_clickedcta, has_purchase, event_count, attribution (jsonb), tags, first_seen, last_activity — one row per person (by email)',
            tracking_contacts: 'contact_id, session_id, client_ip, name, email, phone, attribution (jsonb), tags, merged_into, merged_children, created_at, updated_at — shumard.js identities',
            tracking_page_visits: 'id, contact_id, session_id, current_url, referrer_url, page_title, attribution, timestamp — every page view',
            tracking_tag_events: 'id, contact_id, tag, current_url, timestamp — funnel tag fires',
        },
        event_types: eventTypes,
        events_metadata_keys: metadataKeys,
        purchase_sources: purchaseSources,
        crm_stages: stages,
        tracking_tags: tags,
        sales_pages: salesPages,
        totals: totals[0] || {},
        notes: [
            "events.metadata.variant ('A' or 'B') is the split-test bucket. Attribution: (1) any event with its OWN variant tag uses it — registrations AND downstream events, since Stealth now stamps it on attended/CTA/purchase; (2) an untagged registration is 'undetected' and is NEVER back-filled; (3) an untagged downstream event inherits the registrant's variant via email then phone (last-10-digits), FORWARD-ONLY — only if it occurred at/after the variant was assigned. Pre-experiment events and unmatched purchases are 'undetected'.",
            "For A/B counts/conversions use get_variant_funnel; for the LIST of people in a variant use get_journey_segment (variant:'A'). A naive `WHERE metadata->>'variant'='A'` is fine for registration ROWS but undercounts downstream events that were untagged and inherited.",
            "FB spend (daily_metrics.fb_spend) and reg-page link clicks (fb_link_clicks) have NO variant dimension — FB reports them at account/day level — so cost-per-variant (cost/reg, CPA, ROAS) is NOT computable per A/B variant.",
            "sales_pages maps each named sales/checkout page to the URL where its visits land in tracking_page_visits.current_url. To count how many people hit a sales page, use get_sales_page_visits (it collapses stitched identities to distinct people) rather than a raw COUNT — a naive count of tracking_page_visits would double-count stitched contacts and miss the page→label naming.",
        ],
    };
    bucket.dataDictionary = dict;
    bucket.dataDictionaryAt = now;
    return dict;
}

// List PEOPLE matching journey criteria (stage filters + optional webinar slot /
// purchase source / registration date range). Builds SQL from structured params,
// escapes interpolated strings, and routes through the read-only guard. Returns PII
// for the matched people, capped (default 100, max 500).
const JOURNEY_STAGE_COL = {
    registration: 'has_registration', registered: 'has_registration',
    attended: 'has_attended', attendance: 'has_attended',
    replay: 'has_replay', replays: 'has_replay',
    viewedcta: 'has_viewedcta', saw_cta: 'has_viewedcta', viewed_cta: 'has_viewedcta',
    clickedcta: 'has_clickedcta', clicked_cta: 'has_clickedcta',
    purchase: 'has_purchase', purchased: 'has_purchase', buyer: 'has_purchase',
};
async function getJourneySegment(funnel, params) {
    const p = params || {};
    const esc = (v) => String(v).replace(/['\\;]/g, '').slice(0, 120);   // strip quotes/semicolons/backslashes
    const mapStage = (s) => JOURNEY_STAGE_COL[String(s).toLowerCase().trim()];
    const conds = ['TRUE'];
    for (const s of (Array.isArray(p.reached_stages) ? p.reached_stages : [])) { const c = mapStage(s); if (c) conds.push(c); }
    for (const s of (Array.isArray(p.not_reached_stages) ? p.not_reached_stages : [])) { const c = mapStage(s); if (c) conds.push(`NOT ${c}`); }
    if (p.webinar_slot) conds.push(`email_key IN (SELECT lower(email) FROM events WHERE event_type='registrations' AND metadata->>'webinar_datetime_utc' ILIKE '%${esc(p.webinar_slot)}%' AND email IS NOT NULL)`);
    if (p.purchase_source) conds.push(`email_key IN (SELECT lower(email) FROM events WHERE event_type='purchases' AND metadata->>'source' ILIKE '${esc(p.purchase_source)}' AND email IS NOT NULL)`);
    if (p.variant && ['A', 'B'].includes(String(p.variant).toUpperCase())) {
        const vv = String(p.variant).toUpperCase();
        conds.push(`email_key IN (SELECT lower(email) FROM events WHERE event_type='registrations' AND upper(trim(coalesce(metadata->>'variant',''))) = '${vv}' AND email IS NOT NULL)`);
    }
    const dfrom = /^\d{4}-\d{2}-\d{2}$/.test(p.registered_from || '') ? p.registered_from : null;
    const dto = /^\d{4}-\d{2}-\d{2}$/.test(p.registered_to || '') ? p.registered_to : null;
    if (dfrom || dto) {
        let sub = "SELECT lower(email) FROM events WHERE event_type='registrations' AND email IS NOT NULL";
        if (dfrom) sub += ` AND event_time >= '${dfrom}T00:00:00-08:00'`;
        if (dto) sub += ` AND event_time < ('${dto}T00:00:00-08:00'::timestamptz + interval '1 day')`;
        conds.push(`email_key IN (${sub})`);
    }
    const limit = Math.min(Math.max(parseInt(p.limit, 10) || 100, 1), 500);
    const sql = `SELECT email, name, phone, stage, event_count, is_tracked, last_activity FROM crm_people WHERE ${conds.join(' AND ')} ORDER BY last_activity DESC NULLS LAST LIMIT ${limit}`;
    const result = await runReadOnlySQL(funnel, sql);
    if (result.error) return result;
    const rows = result.rows || [];
    return { count: rows.length, truncated: rows.length >= limit, filters: p, people: rows };
}

// Sales/checkout-page visits per NAMED page, from shumard pageviews. `people` counts
// DISTINCT STITCHED people (a stitched person's merged children point at the root via
// merged_into, so coalesce(merged_into, contact_id) collapses them to one). `visits` is
// raw rows. Page matching is path-only (query strings stripped). Optional date range
// (LA calendar days, inclusive) and optional `page` filter (a label like "Sales B" or a
// full URL). Returns { pages:[{label,url,people,visits}], date_range }.
async function getSalesPageVisits(funnel, params = {}) {
    const map = await getSalesPageMap(funnel);
    const entries = Object.entries(map); // [url, label]
    // Optional page filter: match a label (case-insensitive) or an exact URL.
    let chosen = entries;
    if (params.page) {
        const want = String(params.page).trim().toLowerCase();
        chosen = entries.filter(([url, label]) => label.toLowerCase() === want || url.toLowerCase() === want);
        if (!chosen.length) return { error: `No configured sales page matches "${params.page}". Known: ${entries.map(([, l]) => l).join(', ')}` };
    }
    const esc = (s) => String(s).replace(/'/g, "''");
    const urlList = chosen.map(([url]) => `'${esc(url)}'`).join(',');
    const conds = [`split_part(v.current_url,'?',1) IN (${urlList})`];
    if (params.date_from) conds.push(`(v.timestamp AT TIME ZONE 'America/Los_Angeles')::date >= '${esc(params.date_from)}'`);
    if (params.date_to)   conds.push(`(v.timestamp AT TIME ZONE 'America/Los_Angeles')::date <= '${esc(params.date_to)}'`);
    // `people` = all distinct tracked visitors (incl. anonymous contacts with no email).
    // `identified` = those whose person (root contact) carries an email — i.e. the ones
    // that appear as a row in the CRM people list (crm_people is keyed by email). The two
    // differ when anonymous visitors reach the page without ever identifying themselves.
    const sql = `WITH visits AS (
        SELECT split_part(v.current_url,'?',1) AS url,
               coalesce(c.merged_into, v.contact_id) AS person
        FROM tracking_page_visits v
        LEFT JOIN tracking_contacts c ON c.contact_id = v.contact_id
        WHERE v.contact_id IS NOT NULL AND ${conds.join(' AND ')}
    )
    SELECT vi.url,
           count(*)::int AS visits,
           count(DISTINCT vi.person)::int AS people,
           count(DISTINCT vi.person) FILTER (WHERE rc.email IS NOT NULL)::int AS identified
    FROM visits vi
    LEFT JOIN tracking_contacts rc ON rc.contact_id = vi.person
    GROUP BY vi.url`;
    const result = await runReadOnlySQL(funnel, sql);
    if (result.error) return result;
    const byUrl = {};
    for (const r of (result.rows || [])) byUrl[r.url] = r;
    const pages = chosen.map(([url, label]) => ({
        label, url,
        people: byUrl[url]?.people || 0,
        identified: byUrl[url]?.identified || 0,
        visits: byUrl[url]?.visits || 0,
    })).sort((a, b) => b.people - a.people);
    return { pages, date_range: { from: params.date_from || null, to: params.date_to || null } };
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
        description: 'Run a read-only SQL query against the funnel\'s schema. Use this as an escape hatch for analytical questions the other tools don\'t cover — joins, grouping by day-of-week, filtering by arbitrary event attributes, per-person journeys, etc. Tables/views: daily_metrics; events (one row per funnel action — columns id, event_type ∈ registrations/attended/replays/viewedcta/clickedcta/purchases/stayeduntil, name, email, phone, event_time, metadata jsonb); metadata holds webinar_datetime_utc (text like "April 11th 2026, 10:00:00 pm"), source (purchase source), stayeduntil, variant (A/B split-test bucket — present on registrations AND, going forward, on downstream events (Stealth-stamped); a registration counts under its OWN tag (untagged=undetected, never back-filled), and an untagged downstream event inherits the registrant\'s variant forward-only via email/phone; prefer the get_variant_funnel tool for A/B splits, or get_journey_segment with variant:"A" for the people list, rather than filtering metadata->>\'variant\' directly); crm_people (ONE ROW PER PERSON by email, merging shumard.js tracking + events — columns email, name, phone, stage, is_tracked, has_registration/has_attended/has_replay/has_viewedcta/has_clickedcta/has_purchase, event_count, attribution jsonb, tags, first_seen, last_activity); tracking_contacts / tracking_page_visits / tracking_tag_events (per-person attribution, page journey, tag fires); custom_metrics; dashboard_lenses. Examples: busiest webinar slots → SELECT metadata->>\'webinar_datetime_utc\', count(*) FROM events WHERE event_type=\'registrations\' GROUP BY 1 ORDER BY 2 DESC; registrations by weekday → GROUP BY to_char(event_time AT TIME ZONE \'America/Los_Angeles\',\'Day\'). STRICT RULES: single SELECT or WITH statement; no INSERT/UPDATE/DELETE/DDL; results capped at 500 rows and 5-second timeout. Runs in the active funnel\'s schema (no need to prefix table names).',
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
    {
        name: 'get_journey_funnel',
        description: 'The full customer-journey funnel as DISTINCT PEOPLE at each stage (registered → attended → watched_replay → saw_cta → clicked_cta → purchased), plus stage-to-stage conversion % and overall registered→purchase %. Optional date range filters by event time (LA timezone). Use for "what does my funnel look like", drop-off, and conversion questions — it gets the distinct-person math right (unlike summing daily event counts).',
        input_schema: {
            type: 'object',
            properties: {
                date_from: { type: 'string', description: 'Inclusive start date (YYYY-MM-DD), optional' },
                date_to:   { type: 'string', description: 'Inclusive end date (YYYY-MM-DD), optional' },
            },
        },
    },
    {
        name: 'get_variant_funnel',
        description: 'A/B SPLIT-TEST results: the funnel as distinct people per stage (registered → attended → watched_replay → saw_cta → clicked_cta → purchased) broken down by variant A, B, and undetected, plus per-variant stage-to-stage conversion % and a headline A-vs-B comparison on the registered→purchase rate (absolute point diff + relative lift + leader). Use for ANY split-test / A/B / "variant A vs B" question. It does the variant attribution correctly: a person\'s variant is their FIRST registration\'s metadata.variant, inherited by all their later events via email (or phone when the email doesn\'t match — e.g. a purchase under a different checkout email) — a naive WHERE metadata->>\'variant\'=\'A\' on purchases/attendance would undercount. NOTE: FB spend and reg-page link clicks cannot be split by variant, so this tool reports conversion rates only, not cost-per-variant.',
        input_schema: {
            type: 'object',
            properties: {
                date_from: { type: 'string', description: 'Inclusive start date (YYYY-MM-DD), optional — filters which events count by event time (LA tz)' },
                date_to:   { type: 'string', description: 'Inclusive end date (YYYY-MM-DD), optional' },
            },
        },
    },
    {
        name: 'get_contact_journey',
        description: 'One person\'s COMPLETE journey by email or contact_id: identity, attribution, current stage, and a single chronological timeline merging their page views, tag fires, and funnel events (registered/attended/CTA/purchase). Use for "show me what <email> did", sample buyer paths, or auditing a specific person. Returns PII (name/email/phone) for that one contact.',
        input_schema: {
            type: 'object',
            properties: {
                identifier: { type: 'string', description: 'The person\'s email address or tracking contact_id' },
            },
            required: ['identifier'],
        },
    },
    {
        name: 'describe_journey_data',
        description: 'Live data dictionary for the customer-journey data: every table/view and its columns, the full inventory of keys present in events.metadata (with sample values), and the actual enum values in use (event types, purchase sources, CRM stages, tracking tags) plus totals and date range. Call this FIRST when you need to know exactly what is queryable before writing a run_sql query — so you never guess a column or metadata key.',
        input_schema: { type: 'object', properties: {} },
    },
    {
        name: 'get_journey_segment',
        description: 'List the actual PEOPLE matching journey criteria — e.g. "registered but never attended" (reached_stages:["registration"], not_reached_stages:["attended"]), "buyers from the May 10 webinar" (reached_stages:["purchase"], webinar_slot:"May 10"), "everyone who registered in split-test variant A" (reached_stages:["registration"], variant:"A"). Returns a capped list with email, name, phone, and stage. Use when the user wants WHO matches a segment (the people, not just counts) — including the full roster of registrants in a variant. For counts/conversions prefer get_journey_funnel or get_variant_funnel. Returns PII for the matched people.',
        input_schema: {
            type: 'object',
            properties: {
                reached_stages:     { type: 'array', items: { type: 'string' }, description: 'Stages the person MUST have reached. Any of: registration, attended, replay, saw_cta, clicked_cta, purchase.' },
                not_reached_stages: { type: 'array', items: { type: 'string' }, description: 'Stages the person must NOT have reached (e.g. ["attended"] for registrants who were no-shows).' },
                webinar_slot:       { type: 'string', description: 'Substring of the registration webinar datetime, e.g. "May 10" or "April 11th 2026, 10:00:00 pm".' },
                purchase_source:    { type: 'string', description: 'Restrict to people who purchased from this source, e.g. "Paid Ads", "AI Bot".' },
                variant:            { type: 'string', description: 'Restrict to people who REGISTERED in split-test variant "A" or "B" (by the registration\'s own variant tag). Combine with reached_stages:["registration"] for the registrant roster of a variant.' },
                registered_from:    { type: 'string', description: 'Registration date range start (YYYY-MM-DD), optional.' },
                registered_to:      { type: 'string', description: 'Registration date range end (YYYY-MM-DD), optional.' },
                limit:              { type: 'number', description: 'Max people to return (default 100, max 500).' },
            },
        },
    },
    {
        name: 'get_email_report',
        description: 'Email-marketing performance: for each email source (the el= label on email links, where traffic_source=email) — clicks, unique people who clicked, how many then PURCHASED AFTER clicking (click→sale ordering; by default any purchase after the click counts, no time limit), and the click→buyer conversion %, plus totals. Use for "which emails convert best", "email click-through and sales", or comparing email sources. Counts only (no revenue captured yet). Empty until emails go out carrying the tracking link.',
        input_schema: {
            type: 'object',
            properties: {
                window_days: { type: 'number', description: 'Optional cap: only count a purchase if it happens within this many days after the click. Omit for no cap (any purchase after the click counts).' },
            },
        },
    },
    {
        name: 'get_sales_page_visits',
        description: 'How many people hit each NAMED sales/checkout page (e.g. "Sales Page A" = /checkout1, "Sales Page B" = /checkout2, "Legacy" = /checkout). For each configured page returns `people` (DISTINCT tracked visitors, with stitched/merged identities collapsed to one person — INCLUDING anonymous visitors who never gave an email), `identified` (the subset who have an email — i.e. the ones that show up as rows in the CRM people list / are filterable there), and `visits` (raw pageview count). Use for "how many people hit Sales Page B", "visits to checkout2", sales-page traffic comparisons, etc. When the count here is higher than what the CRM list shows, it is because anonymous-but-tracked visitors hit the page without identifying — cite `people` vs `identified` to explain. The page→URL mapping is configurable (admin-editable); call describe_journey_data for the current labels/URLs. A buyer never seen by the tracker is not counted here.',
        input_schema: {
            type: 'object',
            properties: {
                page:      { type: 'string', description: 'Optional: restrict to one page by its label (e.g. "Sales B") or full URL. Omit to get all configured sales pages.' },
                date_from: { type: 'string', description: 'Optional start date (YYYY-MM-DD, LA timezone, inclusive).' },
                date_to:   { type: 'string', description: 'Optional end date (YYYY-MM-DD, LA timezone, inclusive).' },
            },
        },
    },
];

// GoHighLevel join tool — registered only when GHL is configured.
if (GHL_ENABLED) INSIGHTS_TOOLS.push({
    name: 'get_ghl_pipeline_status',
    description: `PREFERRED for ANY question combining funnel people with the GoHighLevel${GHL_PIPELINE ? ` "${GHL_PIPELINE}"` : ''} opportunity pipeline — "status of everyone who purchased in July", "how many July purchasers have a Report Of Findings scheduled", "which registrants are in Testing". Give it a funnel segment (event_type + optional LA-timezone date range, or an explicit emails list); it fetches the ENTIRE pipeline server-side in seconds (cached 5 min) and returns the email join: counts by current stage, matched people (email, stage, status), and how many have no opportunity. Optional stage_filter narrows to stages whose name contains the text (e.g. "report of findings"). Use the ghl MCP tools only for single-contact drill-downs or data this doesn't return. Segments from event_type are capped at 500 distinct emails (flagged when hit).`,
    input_schema: {
        type: 'object',
        properties: {
            event_type:   { type: 'string', enum: ['purchases', 'registrations', 'attended', 'replays', 'viewedcta', 'clickedcta'], description: 'Funnel segment to join (default: purchases)' },
            date_from:    { type: 'string', description: 'Inclusive start date YYYY-MM-DD (LA timezone). Omit for all time.' },
            date_to:      { type: 'string', description: 'Inclusive end date YYYY-MM-DD (LA timezone). Omit for all time.' },
            emails:       { type: 'array', items: { type: 'string' }, description: 'Explicit emails to join instead of event_type + dates' },
            stage_filter: { type: 'string', description: 'Only report people whose current stage name contains this text (case-insensitive)' },
        },
    },
});

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
            case 'get_ghl_pipeline_status':
                return await getGhlPipelineStatus(funnel, input || {});
            case 'get_journey_funnel':
                return await getJourneyFunnel(funnel, input.date_from, input.date_to);
            case 'get_variant_funnel':
                return await getVariantFunnel(funnel, input.date_from, input.date_to);
            case 'get_contact_journey':
                if (!input.identifier) return { error: 'identifier (email or contact_id) required' };
                return (await buildContactJourney(clientFor(funnel), String(input.identifier), funnel)) || { error: 'No contact found for that identifier' };
            case 'describe_journey_data':
                return await getJourneyDataDictionary(funnel);
            case 'get_journey_segment':
                return await getJourneySegment(funnel, input || {});
            case 'get_sales_page_visits':
                return await getSalesPageVisits(funnel, input || {});
            case 'get_email_report':
                return await getEmailReportData(funnel, input && input.window_days);
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

        // CRM / journey digest — compact cached rollups injected into the prompt so
        // common journey/timing questions are answered with no extra egress. Best-effort:
        // if the CRM tables aren't present in this funnel yet, the block stays empty.
        let journeyBlock = '';
        try {
            const dg = await getInsightsJourneyDigest(funnel);
            if (dg && ((dg.funnelStages && dg.funnelStages.length) || (dg.webinarSlots && dg.webinarSlots.length))) {
                const lines = [];
                if (dg.funnelStages.length) lines.push('People by stage: ' + dg.funnelStages.map(r => `${r.stage} ${r.people}`).join(', '));
                if (dg.webinarSlots.length) lines.push('Busiest webinar slots (registrations): ' + dg.webinarSlots.slice(0, 6).map(r => `${r.slot} (${r.regs})`).join('; '));
                if (dg.regsByDayOfWeek && dg.regsByDayOfWeek.length) lines.push('Registrations by weekday: ' + dg.regsByDayOfWeek.map(r => `${r.dow} ${r.regs}`).join(', '));
                if (dg.purchasesBySource && dg.purchasesBySource.length) lines.push('Purchases by source: ' + dg.purchasesBySource.map(r => `${r.source} ${r.purchases}`).join(', '));
                journeyBlock = `\n\nCRM / CUSTOMER JOURNEY (per-person — query via run_sql):\nThe crm_people view = one row per person (by email), merging shumard.js tracking with funnel events: columns email, name, phone, stage (lead→registration→attended→replay→viewedcta→clickedcta→purchase), is_tracked (false = LEGACY, pre-tracking contact), has_* booleans, event_count, attribution (jsonb), first_seen, last_activity. The events.metadata jsonb holds webinar_datetime_utc, source, stayeduntil. For journey/timing deep-dives, run_sql against crm_people / events.metadata.\nCURRENT SNAPSHOT (refreshed every few minutes):\n- ${lines.join('\n- ')}`;
            }
        } catch { /* digest is best-effort — never block the chat */ }

        // GoHighLevel opportunities (via the ghl MCP server) — only when configured.
        const ghlBlock = !GHL_ENABLED ? '' : `

GOHIGHLEVEL CRM — OPPORTUNITIES (ghl MCP tools):
- locationId: ${GHL_LOCATION_ID} — provide it wherever a GHL tool expects a location.
${GHL_PIPELINE
        ? `- Scope: ONLY the "${GHL_PIPELINE}" opportunity pipeline. Resolve its pipeline ID once via the pipelines tool, then filter every opportunity search/report to it. If asked about other pipelines, say access is limited to this one.`
        : '- Use the pipelines tool to discover the available opportunity pipelines.'}
- USE GHL for any question about patient/deal pipeline stages or appointments — e.g. Day #1 (initial appointment), no-shows, testing/labs, "report of findings" (ROF) scheduled or needs scheduling, started / not started, financing, nurture, refunds, cancellations. That stage data exists ONLY in GoHighLevel, never in the funnel database.
- Joins with funnel data ("status of everyone who purchased in July", "how many July purchasers have a report of findings scheduled"): call get_ghl_pipeline_status ONCE with the segment (event_type + dates, optional stage_filter). NEVER page opportunities one-by-one through the ghl MCP tools for these — it is far too slow and will time out.
- Use the ghl MCP tools only for ad-hoc drill-downs get_ghl_pipeline_status doesn't cover (one contact's opportunity details/notes/tasks, non-pipeline data).
- Access is read-only reporting — never attempt to create or modify records.
- Typical asks: open opportunities by stage, pipeline value, win/loss rate, stalled deals, recent movement. Cross-reference with funnel data by contact email where useful.`;

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
   - purchases_aibot_b (AI Chat Bot B) — second AI chatbot source (webhook source:"AI Bot B")
   - purchases_postwebinar (Post Webinar) — Paid Ads / Sales A / Sales B purchases made 12+ hours AFTER attending a webinar
   - purchases_cpa (CPA Traffic Funnel) — purchases attributed to the CPA Traffic source
   - purchases_sales_a (Sales A) — purchases attributed to the Sales A source
   - purchases_sales_b (Sales B) — purchases attributed to the Sales B source
   - total_purchases — sum of all purchase sources above

KEY METRICS:
- Landing Page Conversion Rate = registrations / fb_link_clicks × 100
- Cost per Registration         = fb_spend / registrations
- Attendance Rate               = attended / registrations × 100
- CTA View Rate                 = viewedcta / (attended + replays) × 100
- CTA Click Rate                = clickedcta / viewedcta × 100
- Conversion Rate               = total_purchases / clickedcta × 100
- Cost per Acquisition          = fb_spend / total_purchases
- Post Webinar Rate             = purchases_postwebinar / total_purchases × 100${journeyBlock}${ghlBlock}

REMEMBERED FACTS:
${memoryBlock}

TIMEZONE (critical):
- The business operates in Pacific Time (America/Los_Angeles). ALWAYS report dates, hours, and day-of-week in Pacific time unless the user explicitly asks for UTC.
- All timestamps in the database are stored in UTC. \`events.event_time\` is a timestamptz; convert it with \`event_time AT TIME ZONE 'America/Los_Angeles'\`.
  • hour of day (PST):  \`EXTRACT(hour FROM event_time AT TIME ZONE 'America/Los_Angeles')\`
  • day of week (PST):  \`to_char(event_time AT TIME ZONE 'America/Los_Angeles','Dy')\`
  • date (PST):         \`(event_time AT TIME ZONE 'America/Los_Angeles')::date\`
- \`events.metadata->>'webinar_datetime_utc'\` is a UTC *text* string like "April 11th 2026, 10:00:00 pm" (note ordinal suffixes). To use it as a PST time, strip the ordinal, parse, declare UTC, then convert — copy this exactly:
  \`(to_timestamp(regexp_replace(metadata->>'webinar_datetime_utc','(\\\\d+)(st|nd|rd|th)','\\\\1','g'),'Month DD YYYY, HH12:MI:SS AM')::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'America/Los_Angeles'\`
  e.g. busiest webinar SLOTS in PST: \`SELECT to_char(<that expression>,'Dy HH12:MI AM') AS slot_pst, count(*) FROM events WHERE event_type='registrations' GROUP BY 1 ORDER BY 2 DESC\`
- Date-range filters: filter on the raw UTC \`event_time\` (e.g. \`event_time >= now() - interval '7 days'\`), then convert to PST for display/grouping. Don't wrap event_time in a timezone conversion inside the WHERE range bound — it's slower and unnecessary.

INSTRUCTIONS:
- Use tools to fetch only the data you need. Don't ask the user for date ranges — pick reasonable ones (last 7/14/30 days, last 3/6 months) based on the question.
- For multi-week or multi-month trends, prefer \`get_metrics_rollup\` over \`get_metrics\` — fewer rows, cleaner trend.
- For "X vs Y" or "this vs last" questions, use \`compare_periods\` — it returns deltas and % change for free.
- For analytical questions the named tools don't cover (day-of-week patterns, hour-of-day, joins, custom aggregations), use \`run_sql\` with a SELECT. Don't be shy about it — it's the escape hatch.${GHL_ENABLED ? `
- Pipeline-stage / appointment / opportunity questions (Day 1, testing, report of findings, started, refunds…) live ONLY in GoHighLevel — see the GOHIGHLEVEL section. Segment × pipeline joins ("everyone who purchased in July") → one get_ghl_pipeline_status call. Ad-hoc single-contact lookups → ghl MCP tools. Never answer stage questions from funnel data alone.` : ''}
- CUSTOMER JOURNEY — you can see the funnel end to end, per person:
  • \`get_journey_funnel\` — distinct-people funnel + stage-to-stage conversion/drop-off (optionally date-ranged). Use for funnel/conversion/drop-off questions.
  • \`get_variant_funnel\` — A/B split-test breakdown: the funnel per variant (A / B / undetected) + per-variant conversion % + a headline A-vs-B comparison on the registered→purchase rate. Use for any split-test / "variant A vs B" question. Attribution (handled for you): an event's own variant tag wins (registrations + Stealth-stamped downstream); untagged registrations are undetected; untagged downstream events inherit the registrant's variant forward-only (email→phone). For the actual LIST of people in a variant (e.g. everyone who registered in A), use \`get_journey_segment\` with variant:"A". Note: spend can't be split by variant — compare CONVERSION RATES, not cost; if asked for cost-per-variant, explain spend has no A/B dimension.
  • \`get_contact_journey\` — one person's full chronological timeline by email or contact_id. Use for "what did <email> do" or sample buyer/non-buyer paths.
  • \`get_journey_segment\` — the LIST of people matching a segment (registered-but-not-attended, buyers from a given webinar slot, attended-but-didn't-buy, etc.). Use when the user wants WHO, not just how many.
  • \`get_email_report\` — email-marketing performance by source (clicks → people → buyers → conversion %, traffic_source=email). Use for "which emails convert best".
  • \`get_sales_page_visits\` — how many people hit each NAMED sales/checkout page (e.g. "Sales Page B" = /checkout2): distinct stitched people + raw visits per page, optional date range. Use for "how many people hit Sales Page B / checkout2", sales-page traffic. Labels are admin-configurable (see describe_journey_data → sales_pages).
  • \`describe_journey_data\` — live schema + every \`events.metadata\` key (with samples) + enum values. Call it FIRST when unsure what's queryable, so you never guess a column or metadata key.
  • \`run_sql\` against \`crm_people\` / \`events.metadata\` for anything else (webinar-slot popularity, attribution, cohorts). The journey snapshot above already answers the most common ones — cite it directly when it suffices.
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

        // GoHighLevel MCP — Anthropic runs the MCP client server-side. The
        // mcp_servers entry and its mcp_toolset must travel together (the API
        // rejects one without the other), so both are dropped on outOfTime.
        const GHL_MCP_SERVERS = GHL_ENABLED ? [{
            type: 'url',
            name: 'ghl',
            url: 'https://services.leadconnectorhq.com/mcp/',
            authorization_token: GHL_MCP_TOKEN,
        }] : undefined;
        if (GHL_ENABLED) allTools.push({ type: 'mcp_toolset', mcp_server_name: 'ghl' });

        const MAX_ITERS = 10;
        // Overall wall-clock budget for the whole tool loop. Kept under the nginx
        // proxy_read_timeout so we return a clean answer instead of letting the
        // gateway 504. Once spent, the next call forces a final text answer
        // (tool_choice none) from whatever data was already gathered.
        const DEADLINE_MS = 110_000;
        const PER_CALL_MS = 90_000;   // MCP server-side loops (many GHL calls in one turn) legitimately run long
        const startedAt = Date.now();
        let lastUsage = null;
        for (let iter = 0; iter < MAX_ITERS; iter++) {
            const outOfTime = Date.now() - startedAt > DEADLINE_MS;
            const callCtrl = new AbortController();
            const callTimer = setTimeout(() => callCtrl.abort(), PER_CALL_MS);
            let response;
            try {
                response = await fetch('https://api.anthropic.com/v1/messages', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'x-api-key': ANTHROPIC_API_KEY,
                        'anthropic-version': '2023-06-01',
                        'anthropic-beta': 'code-execution-2025-05-22' + (GHL_ENABLED ? ',mcp-client-2025-11-20' : ''),
                    },
                    body: JSON.stringify({
                        model: 'claude-sonnet-4-6',
                        max_tokens: 4096,
                        // Prompt caching, two breakpoints: the system block caches the
                        // stable tools+system prefix (reused across loop iterations and
                        // across chats on the same funnel); the top-level marker
                        // auto-caches the last message block so each iteration re-reads
                        // the history the previous one wrote.
                        cache_control: { type: 'ephemeral' },
                        system: [{ type: 'text', text: systemPrompt, cache_control: { type: 'ephemeral' } }],
                        // Once the budget is spent, force a text answer via
                        // tool_choice 'none' rather than dropping the tools array —
                        // removing tools would invalidate the cached tools+system
                        // prefix right when time is shortest.
                        tools: allTools,
                        mcp_servers: GHL_MCP_SERVERS,
                        tool_choice: outOfTime ? { type: 'none' } : undefined,
                        messages: apiMessages,
                    }),
                    signal: callCtrl.signal,
                });
            } catch (e) {
                clearTimeout(callTimer);
                if (e.name === 'AbortError') {
                    console.error(`❌ Chat[${funnel}]: Anthropic call aborted after ${PER_CALL_MS}ms (iter ${iter})`);
                    return res.status(504).json({ error: 'The AI took too long to respond. Try a narrower question or a shorter date range.' });
                }
                throw e;
            }
            clearTimeout(callTimer);

            if (!response.ok) {
                const errBody = await response.text();
                console.error('❌ Claude API error:', response.status, errBody);
                return res.status(502).json({ error: 'AI service error', detail: errBody });
            }

            const result = await response.json();
            lastUsage = result.usage;

            // Server-side loop (code execution / MCP) paused mid-turn. Append the
            // assistant turn as-is and call again — the API resumes automatically;
            // adding a user message here would break the resume.
            if (result.stop_reason === 'pause_turn') {
                apiMessages.push({ role: 'assistant', content: result.content });
                continue;
            }

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

// ─── AI Tools API — external tool-calling access (X-API-Key) ─────────────────
// Exposes the same INSIGHTS_TOOLS the built-in analyst uses, so an external AI
// app can drive them from its own LLM loop: GET the definitions, hand them to
// its model, POST each tool_use here, feed the JSON back as the tool_result.
// remember/forget are excluded — they personalize the in-app chat per dashboard
// user — so this surface is strictly read-only. Access is DENY BY DEFAULT:
// a key must carry an explicit scopes TEXT[] in public.api_keys naming the
// tools it may call ('*' = all). Keys without scopes — including the env keys
// API_KEY/NATIVE_API_KEY, which live in third-party webhook configs — get
// nothing, so shipping this feature grants no existing credential access to
// PII tools like run_sql. The key picks the funnel.

const EXTERNAL_TOOL_NAMES = new Set(
    INSIGHTS_TOOLS.map(t => t.name).filter(n => n !== 'remember' && n !== 'forget')
);

function allowedExternalTools(req) {
    const scopes = req.apiKeyScopes || []; // env keys never set scopes → no access
    if (scopes.includes('*')) return EXTERNAL_TOOL_NAMES;
    return new Set([...EXTERNAL_TOOL_NAMES].filter(n => scopes.includes(n)));
}

app.get('/api/ai/tools', webhookLimiter, authenticateWebhook, (req, res) => {
    const allowed = allowedExternalTools(req);
    res.json({
        funnel: req.funnel,
        tools: INSIGHTS_TOOLS.filter(t => allowed.has(t.name)),
    });
});

async function handleExternalTool(req, res, name, input, transform) {
    if (!EXTERNAL_TOOL_NAMES.has(name)) {
        return res.status(404).json({ error: `Unknown tool: ${name}` });
    }
    if (!allowedExternalTools(req).has(name)) {
        return res.status(403).json({ error: `This API key has no access to tool: ${name}` });
    }
    try {
        // userId is only consumed by remember/forget, which aren't exposed here.
        let result = await executeInsightsTool(name, input, { funnel: req.funnel, userId: null });
        if (transform) result = transform(result);
        res.json({ tool: name, funnel: req.funnel, result });
    } catch (err) {
        console.error(`❌ ${req.method} /api/ai/tools/${name} error:`, err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
}

// The full journey easily runs to hundreds of KB — the same page re-visited
// over and over, replay-heartbeat events every 5 minutes — which overflows
// downstream bots' message-size caps and cuts off the NEWEST entries. So the
// email shortcut returns a CONDENSED journey: contact + linked identities in
// full; pageviews rolled up per page and repeat events rolled up per label
// (count + first/last seen, entry sits at its LAST occurrence); timeline
// newest-first so the freshest signal survives any client-side truncation;
// the raw `visits`/`events` arrays (full duplicate rows of the timeline)
// replaced by an events_by_type count. The raw tool —
// POST /api/ai/tools/get_contact_journey — still returns the full object.
function condenseJourney(j) {
    if (!j || j.error || !Array.isArray(j.timeline)) return j;
    const roll = new Map();
    const rest = []; // tags pass through untouched — they're rare and each one is signal
    for (const t of j.timeline) {
        let key, entry;
        if (t.kind === 'pageview') {
            const path = String(t.url || '').split('?')[0] || t.label; // query strings collapse together
            key = `pv|${path}`;
            entry = { ts: t.ts, kind: 'pageviews', label: t.label, url: path, sales_page: t.sales_page || null };
        } else if (t.kind === 'event') {
            key = `ev|${t.label}|${t.email || ''}`; // per-label; distinct labels (Stayed 60m, Purchased (Sales A)) stay apart
            entry = { ts: t.ts, kind: 'event', event_type: t.event_type, label: t.label, source: t.source || null, email: t.email || null };
        } else { rest.push(t); continue; }
        let p = roll.get(key);
        if (!p) { p = { ...entry, count: 0, first: t.ts, last: t.ts }; roll.set(key, p); }
        p.count++;
        if (new Date(t.ts) < new Date(p.first)) p.first = t.ts;
        if (new Date(t.ts) > new Date(p.last)) { p.last = t.ts; p.ts = t.ts; }
    }
    const timeline = rest.concat([...roll.values()])
        .sort((a, b) => new Date(b.ts) - new Date(a.ts)); // newest first
    const events_by_type = {};
    for (const e of (j.events || [])) events_by_type[e.event_type] = (events_by_type[e.event_type] || 0) + 1;
    const { visits, events, ...keep } = j;
    return { ...keep, timeline, events_by_type, condensed: true, timeline_order: 'newest_first' };
}

// An '@' in the path segment means it's a contact EMAIL, not a tool name —
// /api/ai/tools/jane@example.com (GET or POST) returns that person's journey
// via get_contact_journey (condensed — see above), no body needed.
app.post('/api/ai/tools/:name', webhookLimiter, authenticateWebhook, async (req, res) => {
    const { name } = req.params;
    if (name.includes('@')) return handleExternalTool(req, res, 'get_contact_journey', { identifier: name }, condenseJourney);
    return handleExternalTool(req, res, name, req.body || {});
});

app.get('/api/ai/tools/:identifier', webhookLimiter, authenticateWebhook, async (req, res) => {
    const id = req.params.identifier;
    if (!id.includes('@')) {
        return res.status(404).json({ error: `Not an email: ${id}. GET /api/ai/tools/<email> looks up a contact; tools are executed with POST /api/ai/tools/<tool_name>.` });
    }
    return handleExternalTool(req, res, 'get_contact_journey', { identifier: id }, condenseJourney);
});


// =============================================================================
// TRACKING + CRM — shumard.js identity stitching (ported from the `tether` engine)
//
// Public, unauthenticated endpoints called by shumard.js on the client site:
//   GET  /shumard.js              — the tracking script (?tag= sets an auto-tag)
//   POST /api/sg/pageview      — log a pageview
//   POST /api/sg/lead          — email/phone captured on a form field
//   POST /api/sg/registration  — a form was submitted
//   POST /api/sg/tag           — apply a funnel tag (attended, replay, cta…)
//
// Writes go to the request's funnel schema via clientFor(resolveFunnel(req)),
// defaulting to `analytics` (public). The stitching engine fuses browser
// identities into one contact by session_id > email > shared-IP, following a
// merged_into chain. Mirrors tether/backend/server.py.
// =============================================================================

const ATTR_KNOWN_FIELDS = [
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'utm_id', 'campaign_id', 'adset_id', 'ad_id',
    'fbclid', 'fbc', 'fbp', 'gclid', 'ttclid', 'source_link_tag', 'fb_ad_set_id', 'google_campaign_id',
    'source', 'traffic_source',   // email-link params: el → source, htrafficsource → traffic_source
];

// The tracker script is loaded once; per-request placeholders are swapped in.
const TRACKER_JS = (() => {
    try { return readFileSync(path.join(__dirname, 'tracking', 'shumard.js'), 'utf8'); }
    catch (e) { console.error('⚠️  Could not load tracking/shumard.js:', e.message); return ''; }
})();

function trackingClientIp(req) {
    const xff = req.headers['x-forwarded-for'];
    if (xff) return String(xff).split(',')[0].trim();
    const real = req.headers['x-real-ip'];
    if (real) return String(real).trim();
    return req.ip || null;
}

function normEmail(email) {
    if (!email || typeof email !== 'string') return null;
    return email.trim().toLowerCase() || null;
}

// Clean an attribution object: known fields (capped at 500 chars) + an `extra`
// map for every unrecognised param. Empty → {}.
function safeAttribution(raw) {
    if (!raw || typeof raw !== 'object') return {};
    const cap = (v) => String(v).slice(0, 500);
    const out = {};
    for (const k of ATTR_KNOWN_FIELDS) if (raw[k]) out[k] = cap(raw[k]);
    const extra = {};
    for (const [k, v] of Object.entries(raw)) {
        if (k === 'extra' || ATTR_KNOWN_FIELDS.includes(k)) continue;
        if (v) extra[k] = cap(v);
    }
    if (raw.extra && typeof raw.extra === 'object') {
        for (const [k, v] of Object.entries(raw.extra)) if (v) extra[k] = cap(v);
    }
    if (Object.keys(extra).length) out.extra = extra;
    return out;
}

// True only when there are real UTM/click-ID signals (ignores `extra`).
function hasRealAttribution(attr) {
    if (!attr || typeof attr !== 'object') return false;
    return Object.entries(attr).some(([k, v]) => k !== 'extra' && v);
}

// Fill base's empty fields from incoming (base wins); union the `extra` maps.
function mergeAttribution(base, incoming) {
    const merged = { ...(base && typeof base === 'object' ? base : {}) };
    if (!incoming || typeof incoming !== 'object') return merged;
    for (const [k, v] of Object.entries(incoming)) {
        if (k === 'extra') continue;
        if (v && !merged[k]) merged[k] = v;
    }
    if (incoming.extra && typeof incoming.extra === 'object') {
        const ex = { ...(merged.extra && typeof merged.extra === 'object' ? merged.extra : {}) };
        for (const [k, v] of Object.entries(incoming.extra)) if (v && !ex[k]) ex[k] = v;
        if (Object.keys(ex).length) merged.extra = ex;
    }
    return merged;
}

const NAME_SUFFIXES = new Set(['jr', 'jr.', 'sr', 'sr.', 'ii', 'iii', 'iv', 'v', 'phd', 'ph.d', 'ph.d.', 'md', 'm.d', 'm.d.', 'esq', 'esq.', 'dds', 'd.d.s', 'd.d.s.']);
function parseFullName(full) {
    if (!full || typeof full !== 'string') return [null, null];
    const name = full.trim().split(/\s+/).join(' ');
    if (!name) return [null, null];
    let parts = name.split(' ');
    while (parts.length > 1) {
        const last = parts[parts.length - 1].toLowerCase();
        if (NAME_SUFFIXES.has(last) || NAME_SUFFIXES.has(last.replace(/\.$/, ''))) parts = parts.slice(0, -1);
        else break;
    }
    if (parts.length === 0) return [null, null];
    if (parts.length === 1) return [parts[0], null];
    if (parts.length === 2) return [parts[0], parts[1]];
    return [parts.slice(0, -1).join(' '), parts[parts.length - 1]];
}

// Follow merged_into to the surviving root contact_id. Cycle-guarded.
async function resolveContactId(sb, contactId) {
    const visited = new Set();
    let cid = contactId;
    while (cid && !visited.has(cid)) {
        visited.add(cid);
        const { data } = await sb.from('tracking_contacts').select('merged_into').eq('contact_id', cid).maybeSingle();
        if (!data || !data.merged_into) return cid;
        cid = data.merged_into;
    }
    return contactId;
}

// Create or update a contact. Caller passes the resolved (non-merged) contact_id.
async function upsertContact(sb, data, nowIso, clientIp) {
    const cid = data.contact_id;
    if (!cid) return;

    let firstName = data.first_name || null;
    let lastName = data.last_name || null;
    if (data.name && !firstName && !lastName) [firstName, lastName] = parseFullName(data.name);
    const email = normEmail(data.email);

    const { data: existing } = await sb.from('tracking_contacts').select('*').eq('contact_id', cid).maybeSingle();

    if (existing) {
        const update = { updated_at: nowIso };
        if (data.name) update.name = data.name;
        if (email) update.email = email;
        if (data.phone) update.phone = data.phone;
        if (data.session_id) update.session_id = data.session_id;
        if (firstName && !existing.first_name) update.first_name = firstName;
        if (lastName && !existing.last_name) update.last_name = lastName;
        if (clientIp && !existing.client_ip) update.client_ip = clientIp;
        if (data.user_agent && !existing.user_agent) update.user_agent = String(data.user_agent).slice(0, 1000);
        if (data.attribution) {
            const merged = mergeAttribution(existing.attribution, safeAttribution(data.attribution));
            if (JSON.stringify(merged) !== JSON.stringify(existing.attribution || {})) update.attribution = merged;
        }
        await sb.from('tracking_contacts').update(update).eq('contact_id', cid);
    } else {
        // Only create a contact with identity OR meaningful attribution. Pure
        // anonymous loads are skipped — their visits are still logged and will be
        // attached once the contact is identified (and stitched).
        const attr = safeAttribution(data.attribution);
        const hasIdentity = !!(data.name || email || data.phone || firstName || lastName);
        const hasExtra = !!(attr.extra && Object.keys(attr.extra).length);
        if (!hasIdentity && !hasRealAttribution(attr) && !hasExtra) return;

        const row = {
            contact_id: cid,
            session_id: data.session_id || null,
            client_ip: clientIp || null,
            user_agent: data.user_agent ? String(data.user_agent).slice(0, 1000) : null,
            name: data.name || null,
            email,
            phone: data.phone || null,
            first_name: firstName,
            last_name: lastName,
            attribution: attr,
            created_at: nowIso,
            updated_at: nowIso,
        };
        const { error } = await sb.from('tracking_contacts').insert(row);
        if (error) {
            // Race: a concurrent request inserted first — fall back to update.
            const update = { updated_at: nowIso };
            if (data.name) update.name = data.name;
            if (email) update.email = email;
            if (data.phone) update.phone = data.phone;
            if (data.session_id) update.session_id = data.session_id;
            await sb.from('tracking_contacts').update(update).eq('contact_id', cid);
        }
    }
}

async function logVisit(sb, contactId, sessionId, currentUrl, referrerUrl, pageTitle, attribution, nowIso, clientIp) {
    if (!currentUrl) return null;
    const { data, error } = await sb.from('tracking_page_visits').insert({
        contact_id: contactId,
        session_id: sessionId || null,
        client_ip: clientIp || null,
        current_url: currentUrl,
        referrer_url: referrerUrl || null,
        page_title: pageTitle || null,
        attribution: safeAttribution(attribution),
        timestamp: nowIso,
    }).select('id').maybeSingle();
    if (error) { console.error('track: logVisit failed:', error.message); return null; }
    return data ? data.id : null;
}

// Merge child contact into parent: copy identity/attribution where the parent is
// empty, union tags, reassign the child's visits + tag-events to the parent, and
// mark the child merged_into the parent.
async function doStitch(sb, parentId, childId, nowIso) {
    if (parentId === childId) return { status: 'same', contact_id: parentId };
    const { data: parent } = await sb.from('tracking_contacts').select('*').eq('contact_id', parentId).maybeSingle();
    const { data: child } = await sb.from('tracking_contacts').select('*').eq('contact_id', childId).maybeSingle();
    if (!parent || !child) return { status: 'not_found' };
    if (child.merged_into === parentId) return { status: 'already_merged', merged_into: parentId };

    // Already merged into a DIFFERENT parent → un-merge first, then re-merge.
    if (child.merged_into && child.merged_into !== parentId) {
        const oldParentId = child.merged_into;
        const { data: oldParent } = await sb.from('tracking_contacts').select('merged_children').eq('contact_id', oldParentId).maybeSingle();
        if (oldParent) {
            await sb.from('tracking_contacts')
                .update({ merged_children: (oldParent.merged_children || []).filter((x) => x !== childId) })
                .eq('contact_id', oldParentId);
        }
        await sb.from('tracking_page_visits').update({ contact_id: childId })
            .eq('contact_id', oldParentId).eq('original_contact_id', childId);
        await sb.from('tracking_tag_events').update({ contact_id: childId })
            .eq('contact_id', oldParentId).eq('original_contact_id', childId);
        await sb.from('tracking_contacts').update({ merged_into: null }).eq('contact_id', childId);
    }

    const parentUpdate = { updated_at: nowIso };
    for (const f of ['name', 'email', 'phone', 'first_name', 'last_name', 'session_id', 'client_ip']) {
        if (child[f] && !parent[f]) parentUpdate[f] = child[f];
    }
    const mergedAttr = mergeAttribution(parent.attribution, child.attribution);
    if (JSON.stringify(mergedAttr) !== JSON.stringify(parent.attribution || {})) parentUpdate.attribution = mergedAttr;

    const unionTags = Array.from(new Set([...(parent.tags || []), ...(child.tags || [])]));
    if (unionTags.length !== (parent.tags || []).length) parentUpdate.tags = unionTags;

    const mc = parent.merged_children || [];
    if (!mc.includes(childId)) mc.push(childId);
    parentUpdate.merged_children = mc;

    await sb.from('tracking_contacts').update(parentUpdate).eq('contact_id', parentId);

    // Reassign child's visits + tag-events → parent (original_contact_id keeps provenance)
    await sb.from('tracking_page_visits').update({ contact_id: parentId, original_contact_id: childId }).eq('contact_id', childId);
    await sb.from('tracking_tag_events').update({ contact_id: parentId, original_contact_id: childId }).eq('contact_id', childId);

    await sb.from('tracking_contacts').update({ merged_into: parentId, updated_at: nowIso }).eq('contact_id', childId);
    console.log(`🔗 Stitched ${childId.slice(0, 8)} → ${parentId.slice(0, 8)}`);
    return { status: 'stitched', parent_contact_id: parentId, child_contact_id: childId };
}

// Stitch contacts sharing a session_id (strongest signal — shared cross-frame
// via postMessage). Prefer attribution-rich, then identity-rich, then older as parent.
async function sessionAutoStitch(sb, contactId, sessionId, nowIso) {
    if (!sessionId) return;
    const { data: contacts } = await sb.from('tracking_contacts')
        .select('*').eq('session_id', sessionId).is('merged_into', null).neq('contact_id', contactId).limit(20);
    if (!contacts || !contacts.length) return;
    let current = (await sb.from('tracking_contacts').select('*').eq('contact_id', contactId).maybeSingle()).data;
    if (!current || current.merged_into) return;
    const hasIdentity = (c) => !!(c.email || c.phone || c.name);
    for (const cand of contacts) {
        const cAttr = hasRealAttribution(current.attribution), candAttr = hasRealAttribution(cand.attribution);
        const cId = hasIdentity(current), candId = hasIdentity(cand);
        if (cAttr && !candAttr) await doStitch(sb, contactId, cand.contact_id, nowIso);
        else if (candAttr && !cAttr) await doStitch(sb, cand.contact_id, contactId, nowIso);
        else if (cId && !candId) await doStitch(sb, contactId, cand.contact_id, nowIso);
        else if (candId && !cId) await doStitch(sb, cand.contact_id, contactId, nowIso);
        else if ((current.created_at || '') <= (cand.created_at || '')) await doStitch(sb, contactId, cand.contact_id, nowIso);
        else await doStitch(sb, cand.contact_id, contactId, nowIso);
        current = (await sb.from('tracking_contacts').select('*').eq('contact_id', contactId).maybeSingle()).data;
        if (!current || current.merged_into) break;
    }
}

// Stitch contacts sharing an IP within a 15-minute window: attribution↔identity
// cross-match, or the iframe-companion rule (attribution-rich + fully anonymous).
// Skips entirely on crowded/shared IPs (guard below).
async function ipAutoStitch(sb, contactId, clientIp, nowIso) {
    if (!clientIp) return;
    // Crowded-IP guard: more than 3 non-merged contacts on one IP = a shared address
    // (office NAT, mobile CGNAT, VPN). Flag them all and skip IP-stitch so strangers
    // behind one IP are never fused. (Email + session stitching still apply.)
    const { count: ipCount } = await sb.from('tracking_contacts')
        .select('*', { count: 'exact', head: true })
        .eq('client_ip', clientIp).is('merged_into', null);
    if ((ipCount || 0) > 3) {
        await sb.from('tracking_contacts')
            .update({ flagged_shared_ip: true })
            .eq('client_ip', clientIp).is('merged_into', null).eq('flagged_shared_ip', false);
        return;
    }
    // 3-hour window: the funnel is register → ~2h webinar → checkout, so same-person
    // sessions on one IP can be hours apart. (Still guarded by the >3-contacts shared-IP
    // skip above and the same-user_agent check below, so strangers aren't fused.)
    const IP_STITCH_WINDOW_MS = 3 * 60 * 60 * 1000;
    const windowStart = new Date(Date.now() - IP_STITCH_WINDOW_MS).toISOString();
    const { data: candidates } = await sb.from('tracking_contacts')
        .select('*').eq('client_ip', clientIp).is('merged_into', null).neq('contact_id', contactId)
        .gte('created_at', windowStart).limit(10);
    if (!candidates || !candidates.length) return;
    const current = (await sb.from('tracking_contacts').select('*').eq('contact_id', contactId).maybeSingle()).data;
    if (!current || current.merged_into) return;
    const hasId = (c) => !!(c.email || c.phone);
    const cAttr = hasRealAttribution(current.attribution), cId = hasId(current);
    for (const cand of candidates) {
        // Shared-IP guard: never merge two different devices/browsers (NAT, carriers,
        // households). If both sides report a user agent and they differ, skip.
        if (current.user_agent && cand.user_agent && current.user_agent !== cand.user_agent) continue;
        const candAttr = hasRealAttribution(cand.attribution), candId = hasId(cand);
        if (cAttr && candId && !cId && !candAttr) { await doStitch(sb, contactId, cand.contact_id, nowIso); break; }
        else if (candAttr && cId && !cAttr && !candId) { await doStitch(sb, cand.contact_id, contactId, nowIso); break; }
        else if (cAttr && !candAttr && !candId) { await doStitch(sb, contactId, cand.contact_id, nowIso); break; }
        else if (candAttr && !cAttr && !cId) { await doStitch(sb, cand.contact_id, contactId, nowIso); break; }
    }
}

// Stitch contacts sharing an email — the richer contact (attribution/identity)
// becomes the parent. Returns the surviving contact_id.
async function emailAutoStitch(sb, contactId, email, nowIso) {
    const e = normEmail(email);
    if (!e) return contactId;
    const { data: existing } = await sb.from('tracking_contacts')
        .select('*').eq('email', e).is('merged_into', null).neq('contact_id', contactId).limit(1).maybeSingle();
    if (!existing) return contactId;
    const current = (await sb.from('tracking_contacts').select('*').eq('contact_id', contactId).maybeSingle()).data;
    if (!current || current.merged_into) return contactId;
    const richness = (c) => {
        let s = 0; const a = c.attribution || {};
        if (a.fbclid) s += 10;
        if (a.gclid) s += 10;
        if (a.utm_source || a.utm_medium || a.utm_campaign) s += 5;
        if (c.phone) s += 3;
        if (c.name) s += 2;
        if (c.first_name && c.last_name) s += 2;
        if (c.tags) s += c.tags.length;
        return s;
    };
    const [parentId, childId] = richness(current) > richness(existing)
        ? [contactId, existing.contact_id]
        : [existing.contact_id, contactId];
    await doStitch(sb, parentId, childId, nowIso);
    return parentId;
}

// Apply a tag to a contact (idempotent set membership). Logs a timeline event the
// first time the tag is seen for that contact.
async function applyTag(sb, contactId, tag, currentUrl, nowIso, clientIp) {
    const eid = await resolveContactId(sb, contactId);
    const { data: existing } = await sb.from('tracking_contacts').select('contact_id, tags').eq('contact_id', eid).maybeSingle();
    let firstTime = true;
    if (existing) {
        const tags = existing.tags || [];
        if (tags.includes(tag)) firstTime = false;
        else await sb.from('tracking_contacts').update({ tags: [...tags, tag], updated_at: nowIso }).eq('contact_id', eid);
    } else {
        await sb.from('tracking_contacts').insert({
            contact_id: eid, client_ip: clientIp || null, tags: [tag], attribution: {}, created_at: nowIso, updated_at: nowIso,
        });
    }
    if (firstTime) {
        await sb.from('tracking_tag_events').insert({ contact_id: eid, tag, current_url: currentUrl || null, timestamp: nowIso });
    }
    await ipAutoStitch(sb, eid, clientIp, nowIso);
    return eid;
}

// ─── Tracking script ─────────────────────────────────────────────────────────
app.get('/shumard.js', (req, res) => {
    const backendUrl = (process.env.TRACKING_PUBLIC_URL || `${req.protocol}://${req.get('host')}`).replace(/\/+$/, '');
    const tag = req.query.tag ? String(req.query.tag).replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 64) : '';
    const body = TRACKER_JS
        .replace(/__TRACKING_BACKEND_URL__/g, backendUrl)
        .replace(/__TRACKING_AUTO_TAG__/g, tag);
    res.set('Content-Type', 'application/javascript; charset=utf-8');
    res.set('Cache-Control', 'no-cache, no-store, must-revalidate');
    res.send(body);
});

// ─── Tracking ingestion ────────────────────────────────────────────────────────
// Bot/scanner guard for every tracking endpoint (replies ok so the bot won't retry).
app.use('/api/sg', (req, res, next) => {
    if (isLikelyBot(req, req.body && req.body.user_agent)) return res.json({ status: 'ok', skipped: 'bot' });
    next();
});

app.post('/api/sg/pageview', trackLimiter, async (req, res) => {
    try {
        const sb = clientFor(resolveFunnel(req));
        const now = new Date().toISOString();
        const ip = trackingClientIp(req);
        const d = req.body || {};
        if (!d.contact_id) return res.status(400).json({ error: 'contact_id required' });
        const eid = await resolveContactId(sb, d.contact_id);
        await upsertContact(sb, { contact_id: eid, session_id: d.session_id, attribution: d.attribution, user_agent: d.user_agent }, now, ip);
        const vid = await logVisit(sb, eid, d.session_id, d.current_url, d.referrer_url, d.page_title, d.attribution, now, ip);
        await ipAutoStitch(sb, eid, ip, now);
        res.json({ status: 'ok', visit_id: vid, contact_id: eid });
    } catch (e) { console.error('❌ POST /api/sg/pageview:', e.message); res.status(500).json({ error: 'tracking error' }); }
});

app.post('/api/sg/lead', trackLimiter, async (req, res) => {
    try {
        const sb = clientFor(resolveFunnel(req));
        const now = new Date().toISOString();
        const ip = trackingClientIp(req);
        const d = req.body || {};
        if (!d.contact_id) return res.status(400).json({ error: 'contact_id required' });
        let eid = await resolveContactId(sb, d.contact_id);
        await upsertContact(sb, {
            contact_id: eid, session_id: d.session_id, email: d.email, phone: d.phone,
            name: d.name, first_name: d.first_name, last_name: d.last_name,
            attribution: d.attribution, user_agent: d.user_agent,
        }, now, ip);
        if (d.email) eid = await emailAutoStitch(sb, eid, d.email, now);   // email is the most reliable match
        await sessionAutoStitch(sb, eid, d.session_id, now);
        await ipAutoStitch(sb, eid, ip, now);
        res.json({ status: 'ok', contact_id: eid });
    } catch (e) { console.error('❌ POST /api/sg/lead:', e.message); res.status(500).json({ error: 'tracking error' }); }
});

app.post('/api/sg/registration', trackLimiter, async (req, res) => {
    try {
        const sb = clientFor(resolveFunnel(req));
        const now = new Date().toISOString();
        const ip = trackingClientIp(req);
        const d = req.body || {};
        if (!d.contact_id) return res.status(400).json({ error: 'contact_id required' });
        let eid = await resolveContactId(sb, d.contact_id);
        await upsertContact(sb, {
            contact_id: eid, session_id: d.session_id, email: d.email, phone: d.phone,
            name: d.name, first_name: d.first_name, last_name: d.last_name,
            attribution: d.attribution, user_agent: d.user_agent,
        }, now, ip);
        if (d.current_url) await logVisit(sb, eid, d.session_id, d.current_url, d.referrer_url, d.page_title || 'Registration', d.attribution, now, ip);
        if (d.email) eid = await emailAutoStitch(sb, eid, d.email, now);
        await sessionAutoStitch(sb, eid, d.session_id, now);
        await ipAutoStitch(sb, eid, ip, now);
        res.json({ status: 'ok', contact_id: eid });
    } catch (e) { console.error('❌ POST /api/sg/registration:', e.message); res.status(500).json({ error: 'tracking error' }); }
});

app.post('/api/sg/tag', trackLimiter, async (req, res) => {
    try {
        const sb = clientFor(resolveFunnel(req));
        const now = new Date().toISOString();
        const ip = trackingClientIp(req);
        const d = req.body || {};
        if (!d.contact_id || !d.tag) return res.status(400).json({ error: 'contact_id and tag required' });
        const tag = String(d.tag).replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 64);
        if (!tag) return res.status(400).json({ error: 'invalid tag' });
        const eid = await applyTag(sb, d.contact_id, tag, d.current_url, now, ip);
        res.json({ status: 'ok', contact_id: eid, tag });
    } catch (e) { console.error('❌ POST /api/sg/tag:', e.message); res.status(500).json({ error: 'tracking error' }); }
});


// =============================================================================
// CRM — read endpoints (the people spine: tracking_contacts ⋈ events by email)
//
//   GET /api/crm/contacts        — paginated, searchable people list (crm_people view)
//   GET /api/crm/contacts/:id    — one person + full chronological journey
//   GET /api/crm/stats           — funnel counts for the CRM header
//
// Gated by requireAuth (PII). Reads the funnel schema via clientFor(req.funnel).
// =============================================================================

// Human labels for funnel events shown on the journey timeline.
const CRM_EVENT_LABELS = {
    registrations: 'Registered', attended: 'Attended', replays: 'Watched replay',
    viewedcta: 'Saw CTA', clickedcta: 'Clicked CTA', purchases: 'Purchased', stayeduntil: 'Stayed on webinar',
};

function crmStageFromTypes(types) {
    if (types.has('purchases')) return 'purchase';
    if (types.has('clickedcta')) return 'clickedcta';
    if (types.has('viewedcta')) return 'viewedcta';
    if (types.has('replays')) return 'replay';
    if (types.has('attended')) return 'attended';
    if (types.has('registrations')) return 'registration';
    return 'lead';
}

app.get('/api/crm/contacts', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const sb = clientFor(req.funnel);
        const { search = '', stage = '', sales_page = '' } = req.query;
        const sqlEsc = (s) => String(s).replace(/'/g, "''");
        const lim = Math.min(Math.max(parseInt(req.query.limit, 10) || 100, 1), 200);
        const off = Math.max(parseInt(req.query.offset, 10) || 0, 0);
        const salesMap = await getSalesPageMap(req.funnel);

        let rows, count;
        if (sales_page) {
            // Sales-page filter: people (root contact) who — themselves or via a stitched
            // child — hit the named page. Done entirely in SQL via an EXISTS subquery so the
            // candidate set is never marshalled into a PostgREST .in() (URL-length safe).
            const want = String(sales_page).trim().toLowerCase();
            const urls = Object.entries(salesMap)
                .filter(([url, label]) => label.toLowerCase() === want || url.toLowerCase() === want)
                .map(([url]) => url);
            if (!urls.length) return res.json({ data: [], total: 0 });
            const urlList = urls.map((u) => `'${sqlEsc(u)}'`).join(',');
            const conds = [
                `p.contact_id IS NOT NULL`,
                `EXISTS (SELECT 1 FROM tracking_page_visits v LEFT JOIN tracking_contacts c ON c.contact_id = v.contact_id
                         WHERE coalesce(c.merged_into, v.contact_id) = p.contact_id
                           AND split_part(v.current_url,'?',1) IN (${urlList}))`,
            ];
            if (stage) conds.push(`p.stage = '${sqlEsc(stage)}'`);
            if (search) {
                const s = sqlEsc(String(search).replace(/[%,()]/g, ' ').trim());
                if (s) conds.push(`(p.email ILIKE '%${s}%' OR p.name ILIKE '%${s}%' OR p.phone ILIKE '%${s}%')`);
            }
            const where = conds.join(' AND ');
            const cRes = await sb.rpc('ai_run_sql', { query: `SELECT count(*)::int AS n FROM crm_people p WHERE ${where}` });
            if (cRes.error) throw new Error(cRes.error.message);
            count = (Array.isArray(cRes.data) && cRes.data[0]?.n) || 0;
            const pRes = await sb.rpc('ai_run_sql', { query: `SELECT p.* FROM crm_people p WHERE ${where} ORDER BY p.last_activity DESC NULLS LAST LIMIT ${lim} OFFSET ${off}` });
            if (pRes.error) throw new Error(pRes.error.message);
            rows = pRes.data || [];
        } else {
            let q = sb.from('crm_people').select('*', { count: 'exact' });
            if (stage) q = q.eq('stage', stage);
            if (search) {
                // Strip PostgREST or()-breaking chars (commas separate conditions; % is a wildcard)
                const s = String(search).replace(/[%,()]/g, ' ').trim();
                if (s) q = q.or(`email.ilike.%${s}%,name.ilike.%${s}%,phone.ilike.%${s}%`);
            }
            q = q.order('last_activity', { ascending: false, nullsFirst: false }).range(off, off + lim - 1);
            const { data, error, count: c } = await q;
            if (error) throw error;
            rows = data || [];
            count = c || 0;
        }
        // Attach visit_count for just this page's tracked contacts (one query;
        // keeps the view free of a per-row subquery across all ~13k people).
        const ids = rows.filter((r) => r.contact_id).map((r) => r.contact_id);
        const counts = {};
        if (ids.length) {
            const { data: vrows } = await sb.from('tracking_page_visits').select('contact_id').in('contact_id', ids);
            for (const v of vrows || []) counts[v.contact_id] = (counts[v.contact_id] || 0) + 1;
        }
        for (const r of rows) r.visit_count = r.contact_id ? (counts[r.contact_id] || 0) : 0;
        // Flag people who have LINKED identities — merged child contacts that carry an
        // email or phone (a different identity fused into this person). One query for
        // this page's contacts, same pattern as visit_count (keeps the view lean).
        const linkedParents = new Set();
        if (ids.length) {
            const byId = {};
            for (const r of rows) if (r.contact_id) byId[r.contact_id] = { email: (r.email || '').toLowerCase().trim(), phone: normalizePhoneKey(r.phone) };
            const { data: krows } = await sb.from('tracking_contacts')
                .select('merged_into, email, phone').in('merged_into', ids);
            for (const k of krows || []) {
                const p = byId[k.merged_into]; if (!p) continue;
                const ce = (k.email || '').toLowerCase().trim();
                const cp = normalizePhoneKey(k.phone);
                // A real linked identity = a DIFFERENT EMAIL. Same email = same person
                // (journey already merged), so it never counts — even if a duplicate
                // contact carried a different phone. No-email child links only by phone.
                const differs = ce ? (ce !== p.email) : (!!cp && cp !== p.phone);
                if (differs) linkedParents.add(k.merged_into);
            }
        }
        for (const r of rows) r.has_linked = r.contact_id ? linkedParents.has(r.contact_id) : false;
        // Attach sales_pages_hit: the named sales/checkout pages each person (root contact,
        // including stitched children) has visited. One aggregated query for this page's
        // contacts, matched against the configured sales-page map.
        const salesByRoot = {};
        if (ids.length && Object.keys(salesMap).length) {
            const idList = ids.map((i) => `'${sqlEsc(i)}'`).join(',');
            const urlList = Object.keys(salesMap).map((u) => `'${sqlEsc(u)}'`).join(',');
            const { data: sprows } = await sb.rpc('ai_run_sql', {
                query: `SELECT DISTINCT coalesce(c.merged_into, v.contact_id) AS root, split_part(v.current_url,'?',1) AS url
                        FROM tracking_page_visits v
                        LEFT JOIN tracking_contacts c ON c.contact_id = v.contact_id
                        WHERE split_part(v.current_url,'?',1) IN (${urlList})
                          AND coalesce(c.merged_into, v.contact_id) IN (${idList})`,
            });
            for (const sr of sprows || []) {
                const label = salesMap[sr.url]; if (!label) continue;
                (salesByRoot[sr.root] || (salesByRoot[sr.root] = new Set())).add(label);
            }
        }
        for (const r of rows) r.sales_pages_hit = r.contact_id && salesByRoot[r.contact_id] ? [...salesByRoot[r.contact_id]] : [];
        res.json({ data: rows, total: count || 0 });
    } catch (err) {
        console.error('❌ GET /api/crm/contacts error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/api/crm/stats', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const sb = clientFor(req.funnel);
        // One pass over the view (tiny boolean columns) tallied in JS — a single DB
        // hit, ~2x faster than 8 separate count queries. If the people count ever
        // grows past a few hundred thousand, swap this for a crm_stats() RPC.
        const { data, error } = await sb
            .from('crm_people')
            .select('is_tracked,has_registration,has_attended,has_replay,has_viewedcta,has_clickedcta,has_purchase')
            .range(0, 199999);
        if (error) throw error;
        const rows = data || [];
        const s = { total: rows.length, tracked: 0, registrations: 0, attended: 0, replays: 0, viewedcta: 0, clickedcta: 0, purchases: 0 };
        for (const r of rows) {
            if (r.is_tracked) s.tracked++;
            if (r.has_registration) s.registrations++;
            if (r.has_attended) s.attended++;
            if (r.has_replay) s.replays++;
            if (r.has_viewedcta) s.viewedcta++;
            if (r.has_clickedcta) s.clickedcta++;
            if (r.has_purchase) s.purchases++;
        }
        res.json(s);
    } catch (err) {
        console.error('❌ GET /api/crm/stats error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Assemble one person's complete journey: identity + a single chronological timeline
// merging page views, tag fires (by contact_id) and funnel events (by email AND by
// matching phone — so a sale under a different email but same phone still attaches).
// Returns null when no such contact/email exists. Shared by the CRM detail endpoint and
// the get_contact_journey AI tool.
async function buildContactJourney(sb, raw, funnel = null) {
    let contactId = null, email = null, contact = null;
    if (raw.includes('@')) {
        email = normEmail(raw);
        const { data } = await sb.from('tracking_contacts').select('*').eq('email', email).is('merged_into', null).limit(1).maybeSingle();
        if (data) { contact = data; contactId = data.contact_id; }
    } else {
        contactId = await resolveContactId(sb, raw);
        const { data } = await sb.from('tracking_contacts').select('*').eq('contact_id', contactId).maybeSingle();
        if (data) { contact = data; email = data.email; }
    }
    if (!contactId && !email) return null;

    // ── Gather the full identity cluster: the root tracked contact + every contact
    //    merged INTO it (the stitch engine fuses by session_id / email / IP-window).
    //    This lets someone who registered under one email and purchased under another
    //    — linked by same browser/IP — read as ONE person/journey. Note: an alias
    //    email only links here if shumard.js actually captured it as a contact and the
    //    stitch merged it; a purchase email never seen by the tracker still won't link.
    let clusterContacts = contact ? [contact] : [];
    if (contactId) {
        const { data: kids } = await sb.from('tracking_contacts')
            .select('*').eq('merged_into', contactId).limit(200);
        clusterContacts = clusterContacts.concat(kids || []);
    }
    const clusterIds = [...new Set(clusterContacts.map(c => c.contact_id).filter(Boolean))];
    const clusterEmails = [...new Set(
        clusterContacts.map(c => (c.email || '').toLowerCase()).filter(Boolean)
            .concat(email ? [email.toLowerCase()] : [])
    )];

    // Timeline sources: page views + tag fires for ALL cluster contact_ids; funnel
    // events for ALL cluster emails (one ilike fetch per alias email, then merged).
    const visitQ = clusterIds.length
        ? sb.from('tracking_page_visits').select('*').in('contact_id', clusterIds).order('timestamp', { ascending: true }).limit(2000)
        : Promise.resolve({ data: [] });
    const tagQ = clusterIds.length
        ? sb.from('tracking_tag_events').select('*').in('contact_id', clusterIds).order('timestamp', { ascending: true }).limit(1000)
        : Promise.resolve({ data: [] });
    const eventQ = clusterEmails.length
        ? Promise.all(clusterEmails.map(e => sb.from('events').select('*').ilike('email', e).limit(1000)))
            .then(rs => ({ data: rs.flatMap(r => r.data || []) }))
        : Promise.resolve({ data: [] });
    const [{ data: visits }, { data: tags }, { data: rawEvents }] = await Promise.all([visitQ, tagQ, eventQ]);

    // ── Phone-based gather ──────────────────────────────────────────────────
    // Email-only matching orphans a sale made under a DIFFERENT email. Also pull events
    // whose phone (normalized to the last 10 digits) matches this person's phone —
    // anchored on the tracked contacts' phones AND the email-matched events' phones — so
    // a same-person purchase under another checkout email still attaches to the journey.
    const normPhone = (p) => { const d = String(p || '').replace(/\D/g, ''); return d.length >= 10 ? d.slice(-10) : ''; };
    const clusterPhones = [...new Set(
        clusterContacts.map(c => normPhone(c.phone))
            .concat((rawEvents || []).map(e => normPhone(e.phone)))
            .filter(Boolean)
    )];
    let phoneEvents = [];
    if (clusterPhones.length) {
        const list = clusterPhones.map(p => `'${p}'`).join(','); // 10-digit strings only — safe
        const { data } = await sb.rpc('ai_run_sql', {
            query: `SELECT * FROM events WHERE phone IS NOT NULL AND right(regexp_replace(phone, '\\D', '', 'g'), 10) IN (${list}) LIMIT 1000`,
        });
        phoneEvents = data || [];
    }

    // De-dupe by id (the email- and phone-matched sets overlap; id collapses them).
    const seenEv = new Set();
    const events = [...(rawEvents || []), ...phoneEvents].filter(e => {
        const k = e.id ?? `${e.event_type}|${(e.email || '').toLowerCase()}|${e.event_time}`;
        if (seenEv.has(k)) return false; seenEv.add(k); return true;
    }).sort((a, b) => new Date(a.event_time) - new Date(b.event_time));

    // Named sales/checkout pages → label a matching pageview "Visited <label>" (path-only
    // match, query strings ignored) so /checkout2 reads as "Visited Sales B" on the timeline.
    const salesMap = funnel ? await getSalesPageMap(funnel) : {};
    const salesLabelFor = (u) => (u ? (salesMap[String(u).split('?')[0]] || null) : null);

    const timeline = [];
    for (const v of (visits || [])) {
        const sp = salesLabelFor(v.current_url);
        timeline.push({ ts: v.timestamp, kind: 'pageview', label: sp ? `Visited ${sp}` : (v.page_title || 'Page view'), url: v.current_url, referrer: v.referrer_url || null, sales_page: sp });
    }
    for (const t of (tags || [])) timeline.push({ ts: t.timestamp, kind: 'tag', label: CRM_EVENT_LABELS[t.tag] || t.tag, tag: t.tag, url: t.current_url || null });
    for (const e of events) {
        let label = CRM_EVENT_LABELS[e.event_type] || e.event_type;
        if (e.event_type === 'stayeduntil' && e.metadata && e.metadata.stayeduntil) label = `Stayed ${e.metadata.stayeduntil}m`;
        if (e.event_type === 'purchases' && e.metadata && e.metadata.source) label = `Purchased (${e.metadata.source})`;
        timeline.push({ ts: e.event_time, kind: 'event', event_type: e.event_type, label, source: (e.metadata && e.metadata.source) || null, email: e.email || null });
    }
    timeline.sort((a, b) => new Date(a.ts) - new Date(b.ts));

    const types = new Set(events.map((e) => e.event_type));
    const evName = events.map((e) => e.name).find(Boolean) || null;
    const evPhone = events.map((e) => e.phone).find(Boolean) || null;

    // Linked identities = the OTHER identities fused into this person: tracked alias
    // contacts in the cluster, plus any distinct funnel-event email that isn't the
    // primary (e.g. a purchase made under a different checkout email).
    const primaryEmail = ((contact && contact.email) || email || '').toLowerCase().trim();
    const primaryPhone = normalizePhoneKey((contact && contact.phone) || evPhone);
    const linkedMap = new Map();
    const addLinked = (em, ph, cid, tracked) => {
        // A linked identity = a DIFFERENT EMAIL (a genuine second identity). Same email
        // is the same person — their journey is already merged here — so never list it,
        // even if a duplicate contact carried a different phone. A contact with NO email
        // is linked only via a different phone.
        const e = (em || '').toLowerCase().trim();
        const p = normalizePhoneKey(ph);
        const differs = e ? (e !== primaryEmail) : (!!p && p !== primaryPhone);
        if (!differs) return;
        const key = e || p;
        if (!linkedMap.has(key)) linkedMap.set(key, { email: em || null, phone: ph || null, contact_id: cid || null, tracked: !!tracked });
    };
    for (const cc of clusterContacts) {
        if (cc.contact_id === contactId) continue; // skip the primary/root
        addLinked(cc.email, cc.phone, cc.contact_id, true);
    }
    for (const e of events) {
        const le = (e.email || '').toLowerCase();
        if (le && le !== primaryEmail) addLinked(e.email, e.phone, null, false);
    }
    const linked = [...linkedMap.values()];

    return {
        contact: {
            contact_id: contactId,
            email: (contact && contact.email) || email,
            name: (contact && contact.name) || evName,
            phone: (contact && contact.phone) || evPhone,
            first_name: (contact && contact.first_name) || null,
            last_name: (contact && contact.last_name) || null,
            attribution: (contact && contact.attribution) || {},
            tags: (contact && contact.tags) || [],
            is_tracked: !!contactId,
            is_shared_ip: !!(contact && contact.flagged_shared_ip),
            merged_children: (contact && contact.merged_children) || [],
            created_at: (contact && contact.created_at) || null,
            stage: crmStageFromTypes(types),
        },
        timeline,
        visits: (visits || []).map((v) => ({ ...v, sales_page: salesLabelFor(v.current_url) })),
        events,
        linked,
        stats: { visit_count: (visits || []).length, event_count: events.length, tag_count: (tags || []).length, linked_count: linked.length },
    };
}

app.get('/api/crm/contacts/:id', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const journey = await buildContactJourney(clientFor(req.funnel), decodeURIComponent(req.params.id), req.funnel);
        if (!journey) return res.status(404).json({ error: 'Contact not found' });
        res.json(journey);
    } catch (err) {
        console.error('❌ GET /api/crm/contacts/:id error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// ─── Manual identity links (admin) — match a sale/event under one email to its
//     real registrant so it counts in the correct A/B variant ──────────────────
const variantOfRegistrantEmail = async (supabase, emailLower) => {
    // The own-tag variant of this person's first tagged registration (A/B) or null.
    const { data } = await supabase.from('events').select('event_time, metadata')
        .eq('event_type', 'registrations').ilike('email', emailLower).order('event_time', { ascending: true });
    for (const r of data || []) {
        const v = String(r.metadata?.variant || '').trim().toUpperCase();
        if (v === 'A' || v === 'B') return v;
    }
    return null;
};

// GET /api/crm/links — list all manual links (admin)
app.get('/api/crm/links', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { data: roleData } = await supabase.from('user_roles').select('role').eq('user_id', req.user.id).single();
        if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });
        const { data, error } = await supabase.from('identity_links').select('*').order('created_at', { ascending: false });
        if (error) throw error;
        res.json({ data: data || [] });
    } catch (err) {
        console.error('❌ GET /api/crm/links error:', err.message);
        res.status(500).json({ error: 'Failed to list links (is the identity_links migration applied?)' });
    }
});

// POST /api/crm/link — link alias_email → canonical_email (admin). The canonical must
// be a registrant; we return its variant so the UI can confirm what the sale now counts as.
app.post('/api/crm/link', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { data: roleData } = await supabase.from('user_roles').select('role').eq('user_id', req.user.id).single();
        if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });

        const alias = String(req.body?.alias_email || '').toLowerCase().trim();
        const canonical = String(req.body?.canonical_email || '').toLowerCase().trim();
        if (!alias || !canonical) return res.status(400).json({ error: 'alias_email and canonical_email are required' });
        if (alias === canonical) return res.status(400).json({ error: 'alias and canonical email must differ' });

        // The canonical must actually be a registrant (so it carries a variant to inherit).
        const { count: regCount } = await supabase.from('events').select('*', { count: 'exact', head: true })
            .eq('event_type', 'registrations').ilike('email', canonical);
        if (!regCount) return res.status(400).json({ error: `No registration found for ${canonical} — link to the registrant's email.` });

        const { error } = await supabase.from('identity_links').upsert(
            { alias_email: alias, canonical_email: canonical, note: req.body?.note || null, created_by: req.user.id, created_at: new Date().toISOString() },
            { onConflict: 'alias_email' });
        if (error) throw error;

        // Refresh cached links + force variant recompute.
        const b = getCacheBucket(req.funnel);
        b.identityLinks = undefined;
        b.stitchAliases = undefined;
        b.dedupCounts = {}; b.dedupTimestamps = {};
        invalidateMetricsCache(req.funnel);
        invalidateInsightsCache(req.funnel);
        const variant = await variantOfRegistrantEmail(supabase, canonical);
        console.log(`🔗 [${req.funnel}] linked ${alias} → ${canonical} (variant ${variant || 'undetected'})`);
        res.json({ ok: true, alias_email: alias, canonical_email: canonical, variant });
    } catch (err) {
        console.error('❌ POST /api/crm/link error:', err.message);
        res.status(500).json({ error: err.message || 'Failed to create link' });
    }
});

// DELETE /api/crm/link?alias_email=... — remove a link (admin)
app.delete('/api/crm/link', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const supabase = clientFor(req.funnel);
        const { data: roleData } = await supabase.from('user_roles').select('role').eq('user_id', req.user.id).single();
        if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });
        const alias = String(req.query.alias_email || '').toLowerCase().trim();
        if (!alias) return res.status(400).json({ error: 'alias_email required' });
        const { error } = await supabase.from('identity_links').delete().eq('alias_email', alias);
        if (error) throw error;
        const b = getCacheBucket(req.funnel);
        b.identityLinks = undefined;
        b.stitchAliases = undefined;
        b.dedupCounts = {}; b.dedupTimestamps = {};
        invalidateMetricsCache(req.funnel);
        invalidateInsightsCache(req.funnel);
        res.json({ ok: true });
    } catch (err) {
        console.error('❌ DELETE /api/crm/link error:', err.message);
        res.status(500).json({ error: err.message || 'Failed to delete link' });
    }
});


// Email performance — email clicks (tracking_page_visits where traffic_source=email)
// grouped by source (el), tied to purchasers by email. Counts only (no revenue data).
// Shared by the Email Report endpoint and the get_email_report AI tool.
async function getEmailReportData(funnel, windowDays) {
    const sb = clientFor(funnel);
    // Attribution = click → sale ordering: a "buyer" is anyone who PURCHASED AFTER
    // clicking the email. No time cap by default; pass windowDays to additionally
    // require the purchase within N days of the click.
    const win = (windowDays != null && windowDays !== '' && parseInt(windowDays, 10) > 0) ? Math.min(parseInt(windowDays, 10), 3650) : 0;
    const upperBound = win > 0 ? ` AND e.event_time <= cl.click_ts + (interval '1 day' * ${win})` : '';
    const clicksCTE = `clicks AS (
        SELECT v.attribution->>'source' AS source, lower(c.email) AS email, v.contact_id, v.timestamp AS click_ts
        FROM tracking_page_visits v
        LEFT JOIN tracking_contacts c ON c.contact_id = v.contact_id
        WHERE lower(v.attribution->>'traffic_source') = 'email'
    )`;
    const buyersCTE = `buyers AS (
        SELECT DISTINCT cl.source AS source, cl.email AS email
        FROM clicks cl
        JOIN events e ON e.event_type='purchases' AND e.email IS NOT NULL AND lower(e.email) = cl.email
             AND e.event_time >= cl.click_ts${upperBound}
        WHERE cl.email IS NOT NULL
    )`;
    const perSourceSql = `WITH ${clicksCTE}, ${buyersCTE}
        SELECT coalesce(cl.source, '(no source)') AS source,
               count(*)::int AS clicks,
               count(DISTINCT cl.email)::int AS people,
               count(DISTINCT b.email)::int AS buyers
        FROM clicks cl
        LEFT JOIN buyers b ON b.source IS NOT DISTINCT FROM cl.source AND b.email = cl.email
        GROUP BY coalesce(cl.source, '(no source)')
        ORDER BY clicks DESC`;
    const totalsSql = `WITH ${clicksCTE}, ${buyersCTE}
        SELECT count(*)::int AS clicks, count(DISTINCT cl.email)::int AS people, count(DISTINCT b.email)::int AS buyers
        FROM clicks cl
        LEFT JOIN buyers b ON b.email = cl.email`;
    const [r1, r2] = await Promise.all([
        sb.rpc('ai_run_sql', { query: perSourceSql }),
        sb.rpc('ai_run_sql', { query: totalsSql }),
    ]);
    const pct = (a, b) => (b > 0 ? Math.round((a / b) * 1000) / 10 : 0);
    const rows = Array.isArray(r1.data) ? r1.data : [];
    const sources = rows.map(r => ({ ...r, conversion: pct(r.buyers, r.people) }));
    const t = (Array.isArray(r2.data) && r2.data[0]) ? r2.data[0] : { clicks: 0, people: 0, buyers: 0 };
    const totals = { clicks: t.clicks || 0, people: t.people || 0, buyers: t.buyers || 0, conversion: pct(t.buyers || 0, t.people || 0), window_days: win || null };
    return { sources, totals };
}

app.get('/api/crm/email-report', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        res.json(await getEmailReportData(req.funnel, req.query.window));
    } catch (err) {
        console.error('❌ GET /api/crm/email-report error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Drill-down: every individual email click for one source — who, when, what page,
// and whether/when they purchased after. The granular view behind a source row.
async function getEmailReportClicks(funnel, source, windowDays) {
    const sb = clientFor(funnel);
    const win = (windowDays != null && windowDays !== '' && parseInt(windowDays, 10) > 0) ? Math.min(parseInt(windowDays, 10), 3650) : 0;
    const upperBound = win > 0 ? ` AND e.event_time <= v.timestamp + (interval '1 day' * ${win})` : '';
    // Exact-match the el= source; '(no source)' / empty means the source key was absent.
    const esc = (s) => String(s).replace(/'/g, "''").replace(/[;\\]/g, '');
    const sourceCond = (!source || source === '(no source)')
        ? "v.attribution->>'source' IS NULL"
        : `v.attribution->>'source' = '${esc(source)}'`;
    const sql = `
        SELECT v.timestamp AS click_ts,
               v.current_url,
               v.client_ip,
               c.email, c.name, c.contact_id,
               p.event_time AS purchased_at,
               p.metadata->>'source' AS purchase_source
        FROM tracking_page_visits v
        LEFT JOIN tracking_contacts c ON c.contact_id = v.contact_id
        LEFT JOIN LATERAL (
            SELECT e.event_time, e.metadata FROM events e
            WHERE e.event_type='purchases' AND e.email IS NOT NULL
                  AND lower(e.email) = lower(c.email)
                  AND e.event_time >= v.timestamp${upperBound}
            ORDER BY e.event_time ASC LIMIT 1
        ) p ON true
        WHERE lower(v.attribution->>'traffic_source') = 'email' AND ${sourceCond}
        ORDER BY v.timestamp DESC
        LIMIT 500`;
    // Funnel-stage breakdown: of the people who clicked this email source, how far
    // through the funnel are they overall (distinct people who reached each stage).
    // Post-webinar emails go to people who already registered/attended, so this is
    // their audience composition + drop-off, not strictly "stages after the click".
    const funnelSql = `
        WITH clickers AS (
            SELECT DISTINCT lower(c.email) AS email
            FROM tracking_page_visits v
            JOIN tracking_contacts c ON c.contact_id = v.contact_id
            WHERE lower(v.attribution->>'traffic_source') = 'email' AND ${sourceCond}
                  AND c.email IS NOT NULL
        )
        SELECT
            count(*)::int AS clickers,
            count(*) FILTER (WHERE ev.types ? 'registrations')::int AS registered,
            count(*) FILTER (WHERE ev.types ? 'attended')::int      AS attended,
            count(*) FILTER (WHERE ev.types ? 'replays')::int       AS replay,
            count(*) FILTER (WHERE ev.types ? 'viewedcta')::int     AS saw_cta,
            count(*) FILTER (WHERE ev.types ? 'clickedcta')::int    AS clicked_cta,
            count(*) FILTER (WHERE ev.types ? 'purchases')::int     AS purchased
        FROM clickers cl
        LEFT JOIN LATERAL (
            SELECT jsonb_object_agg(event_type, true) AS types
            FROM events e WHERE e.email IS NOT NULL AND lower(e.email) = cl.email
        ) ev ON true`;
    const [clicksRes, funnelRes] = await Promise.all([
        sb.rpc('ai_run_sql', { query: sql }),
        sb.rpc('ai_run_sql', { query: funnelSql }),
    ]);
    const clicks = Array.isArray(clicksRes.data) ? clicksRes.data : [];
    const people = new Set(clicks.map(c => (c.email || c.contact_id))).size;
    const buyers = new Set(clicks.filter(c => c.purchased_at).map(c => c.email)).size;
    const funnelStats = (Array.isArray(funnelRes.data) && funnelRes.data[0]) ? funnelRes.data[0] : null;
    return { source: source || '(no source)', window_days: win || null, total_clicks: clicks.length, people, buyers, funnel: funnelStats, clicks };
}

app.get('/api/crm/email-report/clicks', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        res.json(await getEmailReportClicks(req.funnel, req.query.source, req.query.window));
    } catch (err) {
        console.error('❌ GET /api/crm/email-report/clicks error:', err.message);
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

// Catch-all error handler. A malformed percent-encoding (e.g. /%c0) makes Express throw a
// URIError while decoding the path — return a clean 400 instead of letting it surface as an
// unhandled error (log noise, and a guard against anything that could crash → restart,
// since every restart cold-starts the in-memory cache).
app.use((err, req, res, next) => {
    if (err instanceof URIError) {
        if (!res.headersSent) res.status(400).json({ error: 'Bad request URL' });
        return;
    }
    console.error('❌ Unhandled request error:', err && err.message);
    if (res.headersSent) return next(err);
    res.status(500).json({ error: 'Internal server error' });
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

    // Warm the metrics caches in the background so the FIRST dashboard load after a
    // (re)start isn't a cold ~20s recompute that shows the current day as 0. Hits the
    // same open /api/metrics endpoint the dashboard uses (plain + expanded, per funnel),
    // populating both the dedup cache and the assembled-response cache. Best-effort.
    warmCaches();
});

async function warmCaches() {
    const base = `http://127.0.0.1:${PORT}`;
    for (const funnel of ALLOWED_FUNNELS) {
        for (const qs of ['limit=90&offset=0', 'limit=90&offset=0&expand=variants']) {
            try {
                const r = await fetch(`${base}/api/metrics?${qs}`, { headers: { 'x-funnel': funnel } });
                await r.text();
            } catch (e) { /* best-effort: a failed warm just means the first user request pays the cost */ }
        }
    }
    console.log('🔥 Cache warm complete (metrics primed for', ALLOWED_FUNNELS.join(', '), ')');
}

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