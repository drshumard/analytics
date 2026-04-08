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

// ─── Supabase Client ─────────────────────────────────────────────────────────
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

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
    allowedHeaders: ['Content-Type', 'Authorization', 'X-API-Key'],
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
function authenticateWebhook(req, res, next) {
    const key = req.headers['x-api-key'] || req.query.api_key;
    if (!key) {
        return res.status(401).json({ error: 'Missing API key. Send X-API-Key header.' });
    }
    const keyBuf = Buffer.from(key);
    const apiBuf = Buffer.from(API_KEY);
    if (keyBuf.length !== apiBuf.length || !crypto.timingSafeEqual(keyBuf, apiBuf)) {
        return res.status(403).json({ error: 'Invalid API key' });
    }
    next();
}

// Supabase JWT auth — verifies the user is logged in
async function requireAuth(req, res, next) {
    const auth = req.headers.authorization;
    if (!auth || !auth.startsWith('Bearer ')) {
        return res.status(401).json({ error: 'Authentication required' });
    }
    const token = auth.replace('Bearer ', '');
    const { data: { user }, error } = await supabase.auth.getUser(token);
    if (error || !user) {
        return res.status(401).json({ error: 'Invalid or expired session' });
    }
    req.user = user;
    next();
}

// Requires admin role — must be used after requireAuth
async function requireAdmin(req, res, next) {
    const { data } = await supabase
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

    // MM/DD/YYYY
    if (/^\d{2}\/\d{2}\/\d{4}$/.test(str)) return str;

    // YYYY-MM-DD
    if (/^\d{4}-\d{2}-\d{2}/.test(str)) {
        const [y, m, d] = str.substring(0, 10).split('-');
        return `${m}/${d}/${y}`;
    }

    // Try parsing as date
    const parsed = new Date(str);
    if (!isNaN(parsed.getTime())) {
        return parsed.toLocaleDateString('en-US', { timeZone: 'America/Los_Angeles' });
    }

    return null;
}

function dateToISO(mmddyyyy) {
    const [m, d, y] = mmddyyyy.split('/');
    return `${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`;
}

// ─── Webhook Log ─────────────────────────────────────────────────────────────
async function logWebhook(source, payload, status, errorMessage = null) {
    try {
        await supabase.from('webhook_log').insert({
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
app.post('/api/metrics', webhookLimiter, authenticateWebhook, async (req, res) => {
    try {
        const body = req.body;
        await logWebhook('zapier', body, 'received');

        const date = parseDateInput(body.date);
        if (!date) {
            await logWebhook('zapier', body, 'error', 'Invalid or missing date');
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
            purchases: parseInt(body.purchases) || 0,
            attended: parseInt(body.attended) || 0,
        };

        const { data, error } = await supabase
            .from('daily_metrics')
            .upsert(row, { onConflict: 'date' })
            .select()
            .single();

        if (error) throw error;

        await logWebhook('zapier', body, 'processed');
        console.log(`✅ Upserted metrics for ${date} (${dayOfWeek})`);
        res.json({ success: true, date, day: dayOfWeek, data });

    } catch (err) {
        console.error('❌ POST /api/metrics error:', err.message);
        await logWebhook('zapier', req.body, 'error', err.message);
        res.status(500).json({ error: 'Internal server error', detail: err.message });
    }
});

// POST /api/metrics/batch — Bulk upsert (for backfills)
app.post('/api/metrics/batch', webhookLimiter, authenticateWebhook, async (req, res) => {
    try {
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
            rows.push({
                date: dateToISO(date),
                day_of_week: getLADayOfWeek(date),
                fb_spend: parseFloat(entry.fb_spend) || 0,
                fb_link_clicks: parseInt(entry.fb_link_clicks) || 0,
                registrations: parseInt(entry.registrations) || 0,
                replays: parseInt(entry.replays) || 0,
                viewedcta: parseInt(entry.viewedcta) || 0,
                clickedcta: parseInt(entry.clickedcta) || 0,
                purchases: parseInt(entry.purchases) || 0,
                attended: parseInt(entry.attended) || 0,
            });
        }

        if (rows.length > 0) {
            const { error } = await supabase
                .from('daily_metrics')
                .upsert(rows, { onConflict: 'date' });
            if (error) throw error;
        }

        await logWebhook('batch', { count: entries.length }, 'processed');
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
        const { field, count = 1, name, email, phone, execution_id, ...rest } = req.body;
        const validFields = ['fb_spend', 'fb_link_clicks', 'registrations', 'replays', 'viewedcta', 'clickedcta', 'purchases', 'attended'];

        if (!validFields.includes(field)) {
            return res.status(400).json({ error: `Invalid field. Use: ${validFields.join(', ')}` });
        }

        // Dedup: skip if same email + event_type within last 5 minutes (webhook retry protection)
        if (email) {
            const fiveMinAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
            const { data: existing } = await supabase
                .from('events')
                .select('id')
                .eq('event_type', field)
                .eq('email', email)
                .gte('event_time', fiveMinAgo)
                .limit(1);

            if (existing && existing.length > 0) {
                console.log(`⏭️  Dedup skip: ${field} for ${email} (duplicate within 5min)`);
                return res.json({ success: true, duplicate: true, message: 'Duplicate event skipped' });
            }
        }

        const today = getLADate();
        let targetDate = today;

        // If webinar_datetime_utc is provided, use the webinar date for counting
        // Format from Stealth: "March 31st 2026, 2:16:43 pm"
        if (rest.webinar_datetime_utc) {
            try {
                // Strip ordinal suffixes (st, nd, rd, th) for Date parsing
                const cleaned = rest.webinar_datetime_utc.replace(/(\d+)(st|nd|rd|th)/gi, '$1');
                const parsed = new Date(cleaned);
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

        // Increment via read-modify-write
        const { data: current } = await supabase
            .from('daily_metrics')
            .select(field)
            .eq('date', isoDate)
            .single();

        const newValue = (Number(current?.[field]) || 0) + Number(count);

        const { data, error } = await supabase
            .from('daily_metrics')
            .update({ [field]: newValue })
            .eq('date', isoDate)
            .select()
            .single();

        if (error) throw error;

        // Store the individual event with user details
        const metadata = { ...rest };
        delete metadata.api_key; // don't store keys
        await supabase.from('events').insert({
            event_type: field,
            name: name || null,
            email: email || null,
            phone: phone || null,
            execution_id: execution_id || null,
            metadata: Object.keys(metadata).length > 0 ? metadata : {},
        }).then(({ error: evErr }) => {
            if (evErr) console.error('⚠️ Event insert error:', evErr.message);
        });

        console.log(`✅ Increment ${field} +${count} for ${targetDate}${name ? ` (${name})` : ''}`);
        res.json({ success: true, date: targetDate, field, previous: current?.[field], new: newValue });

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
        const { field, value, date: dateInput } = req.body;
        const validFields = ['fb_spend', 'fb_link_clicks', 'registrations', 'replays', 'viewedcta', 'clickedcta', 'purchases', 'attended'];

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

        await logWebhook('set', { field, value: newValue, date: targetDate }, 'processed');
        console.log(`✅ Set ${field} = ${newValue} for ${targetDate} (was ${previous})`);
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
        const result = await syncFacebookSpend();
        refreshTimestamps.push(now);

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

        console.log(`✅ Recalc for ${date}: $${insights.spend.toFixed(2)}, ${insights.linkClicks} link clicks`);
        return res.json({ message: `Insights for ${date} updated — $${insights.spend.toFixed(2)}, ${insights.linkClicks} link clicks`, spend: insights.spend, linkClicks: insights.linkClicks });
    } catch (err) {
        console.error(`❌ Recalc spend for ${date} failed:`, err.message);
        return res.status(502).json({ error: `Facebook API error: ${err.message}` });
    }
});

// GET /api/me — Return current user's role and preferences
app.get('/api/me', dashboardLimiter, requireAuth, async (req, res) => {
    try {
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
        });
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch user info' });
    }
});

// PUT /api/me/preferences — Save user preferences (hidden columns, etc.)
app.put('/api/me/preferences', dashboardLimiter, requireAuth, async (req, res) => {
    try {
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

// GET /api/metrics — Fetch all daily metrics
app.get('/api/metrics', dashboardLimiter, async (req, res) => {
    try {
        const { limit = 90, offset = 0 } = req.query;

        const { data, error, count } = await supabase
            .from('daily_metrics')
            .select('*', { count: 'exact' })
            .lte('date', dateToISO(getLADate()))  // hide future dates
            .order('date', { ascending: false })
            .range(Number(offset), Number(offset) + Number(limit) - 1);

        if (error) throw error;

        // Compute deduplicated counts per day for all event types from the events table
        // Groups by date (LA timezone) + event_type, counts distinct users (email → phone → name)
        const dates = (data || []).map(r => String(r.date).substring(0, 10));
        const dedupMap = {}; // { "2026-03-29": { registrations: 196, replays: 45, ... } }
        const EVENT_TYPES = ['registrations', 'attended', 'replays', 'viewedcta', 'clickedcta', 'purchases'];

        if (dates.length > 0) {
            // Paginate past Supabase's 1000-row default cap
            let allEvents = [];
            const PAGE_SIZE = 50000;
            let page = 0;
            while (true) {
                const { data: batch } = await supabase
                    .from('events')
                    .select('event_type, email, name, phone, event_time, metadata')
                    .in('event_type', EVENT_TYPES)
                    .gte('event_time', `${dates[dates.length - 1]}T00:00:00`)
                    .lte('event_time', `${dates[0]}T23:59:59`)
                    .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1);
                if (!batch || batch.length === 0) break;
                allEvents = allEvents.concat(batch);
                if (batch.length < PAGE_SIZE) break;
                page++;
            }

            if (allEvents) {
                // Group by date + event_type, track unique users
                // For registrations: use webinar_datetime_utc from metadata
                // For all other events: use event_time
                const sets = {}; // key: "YYYY-MM-DD|event_type" → Set of user keys
                for (const ev of allEvents) {
                    let d;
                    if (ev.event_type === 'registrations' && ev.metadata?.webinar_datetime_utc) {
                        // Parse webinar date: "March 31st 2026, 2:16:43 pm"
                        const cleaned = ev.metadata.webinar_datetime_utc.replace(/(\d+)(st|nd|rd|th)/gi, '$1');
                        const parsed = new Date(cleaned);
                        d = !isNaN(parsed.getTime())
                            ? parsed.toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' })
                            : new Date(ev.event_time).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
                    } else {
                        d = new Date(ev.event_time).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
                    }
                    const k = `${d}|${ev.event_type}`;
                    if (!sets[k]) sets[k] = new Set();
                    const userKey = (ev.email || '').toLowerCase() || ev.phone || (ev.name || '').toLowerCase();
                    if (userKey) sets[k].add(userKey);
                }
                // Convert to counts — only override if we found identifiable users
                for (const [k, userSet] of Object.entries(sets)) {
                    if (userSet.size === 0) continue; // no identifiable users, fall back to raw count
                    const [d, type] = k.split('|');
                    if (!dedupMap[d]) dedupMap[d] = {};
                    dedupMap[d][type] = userSet.size;
                }
            }
        }

        // Convert to frontend format (MM/DD/YYYY)
        // Priority: manual override > deduped count > raw daily_metrics
        const formatted = (data || []).map(row => {
            const dateStr = String(row.date).substring(0, 10);
            const mmddyyyy = dateStr.replace(/(\d{4})-(\d{2})-(\d{2})/, '$2/$3/$1');
            const deduped = dedupMap[dateStr] || {};
            const ov = row.overrides || {};
            const pick = (field) => ov[field] !== undefined ? ov[field] : (deduped[field] ?? row[field]);
            return {
                date: mmddyyyy,
                day: row.day_of_week,
                fb_spend: ov.fb_spend !== undefined ? ov.fb_spend : Number(row.fb_spend),
                fb_link_clicks: ov.fb_link_clicks !== undefined ? ov.fb_link_clicks : Number(row.fb_link_clicks || 0),
                registrations: pick('registrations'),
                replays: pick('replays'),
                viewedcta: pick('viewedcta'),
                clickedcta: pick('clickedcta'),
                purchases: pick('purchases'),
                attended: pick('attended'),
                created_at: row.created_at,
                updated_at: row.updated_at,
            };
        });

        res.json({ data: formatted, total: count });

    } catch (err) {
        console.error('❌ GET /api/metrics error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// PUT /api/metrics/:date — Update a specific day (from dashboard edit)
// Saves edited values into the 'overrides' column so they permanently take precedence
app.put('/api/metrics/:date', dashboardLimiter, requireAuth, requireAdmin, async (req, res) => {
    try {
        const dateInput = parseDateInput(req.params.date);
        if (!dateInput) return res.status(400).json({ error: 'Invalid date' });

        const isoDate = dateToISO(dateInput);
        const body = req.body;

        // Build the overrides object from the submitted fields
        const OVERRIDE_FIELDS = ['fb_spend', 'fb_link_clicks', 'registrations', 'replays', 'viewedcta', 'clickedcta', 'purchases', 'attended'];
        const newOverrides = {};
        const updates = {};
        for (const f of OVERRIDE_FIELDS) {
            if (body[f] !== undefined) {
                const val = (f === 'fb_spend') ? parseFloat(body[f]) || 0 : parseInt(body[f]) || 0;
                updates[f] = val;
                newOverrides[f] = val;
            }
        }

        // Merge with existing overrides (don't wipe out other fields)
        const { data: existing } = await supabase.from('daily_metrics').select('overrides').eq('date', isoDate).single();
        const mergedOverrides = { ...(existing?.overrides || {}), ...newOverrides };
        updates.overrides = mergedOverrides;

        const { data, error } = await supabase
            .from('daily_metrics')
            .update(updates)
            .eq('date', isoDate)
            .select()
            .single();

        if (error) throw error;
        res.json({ success: true, data });

    } catch (err) {
        console.error('❌ PUT /api/metrics error:', err.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// DELETE /api/metrics/:date
app.delete('/api/metrics/:date', dashboardLimiter, requireAuth, requireAdmin, async (req, res) => {
    try {
        const dateInput = parseDateInput(req.params.date);
        if (!dateInput) return res.status(400).json({ error: 'Invalid date' });

        const isoDate = dateToISO(dateInput);
        const { error } = await supabase
            .from('daily_metrics')
            .delete()
            .eq('date', isoDate);

        if (error) throw error;
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
// HEALTH CHECK
// =============================================================================

app.get('/api/health', async (req, res) => {
    try {
        const { data, error } = await supabase.from('daily_metrics').select('id').limit(1);
        res.json({
            status: 'ok',
            timestamp: new Date().toISOString(),
            timezone: 'America/Los_Angeles',
            la_time: new Date().toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }),
            database: error ? 'error' : 'connected',
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
        console.log('🔄 FB sync cron triggered');
        try {
            await syncFacebookSpend();
        } catch (err) {
            console.error('❌ FB sync cron failed:', err.message);
        }
    }, { timezone: 'America/Los_Angeles' });

    // Cron: daily at 4:00 AM PST — fetch *yesterday's* final ad spend
    // By 4 AM the previous day's data is fully settled in Facebook's reporting
    cron.schedule('0 4 * * *', async () => {
        try {
            const now = new Date();
            const yesterdayLA = new Date(now.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }));
            yesterdayLA.setDate(yesterdayLA.getDate() - 1);
            const y = yesterdayLA.getFullYear();
            const m = String(yesterdayLA.getMonth() + 1).padStart(2, '0');
            const d = String(yesterdayLA.getDate()).padStart(2, '0');
            const yesterdayISO = `${y}-${m}-${d}`;

            console.log(`🌙 Daily 4 AM cron: fetching final ad insights for ${yesterdayISO}`);
            const insights = await fetchFacebookInsights(yesterdayISO);
            await writeInsightsToSupabase(yesterdayISO, insights);
            console.log(`✅ Daily 4 AM cron: $${insights.spend.toFixed(2)}, ${insights.linkClicks} link clicks written for ${yesterdayISO}`);
        } catch (err) {
            console.error('❌ Daily 4 AM ad-spend cron failed:', err.message);
        }
    }, { timezone: 'America/Los_Angeles' });

    console.log('📡 Facebook ad spend sync enabled (every 30 min + daily 4 AM previous-day)');
} else {
    console.log('⚠️  Facebook sync disabled — set FB_ACCESS_TOKEN and FB_AD_ACCOUNT_ID to enable');
}

// Manual trigger — hit this to force a sync right now
app.post('/api/fb-sync', webhookLimiter, authenticateWebhook, async (req, res) => {
    try {
        const result = await syncFacebookSpend();
        if (!result) {
            return res.status(400).json({ error: 'FB sync not configured — check FB_ACCESS_TOKEN and FB_AD_ACCOUNT_ID' });
        }
        res.json({ success: true, ...result });
    } catch (err) {
        res.status(500).json({ error: 'FB sync failed', detail: err.message });
    }
});

// GET /api/fb-sync/status — check if sync is configured and last run
app.get('/api/fb-sync/status', dashboardLimiter, async (req, res) => {
    const configured = !!(process.env.FB_ACCESS_TOKEN && process.env.FB_AD_ACCOUNT_ID);

    let lastSync = null;
    if (configured) {
        const { data } = await supabase
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
        // Verify admin role
        const { data: roleData } = await supabase.from('user_roles').select('role').eq('user_id', req.user.id).single();
        if (roleData?.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });

        const { table = 'events', dateFrom, dateTo, eventType, search, sortBy, sortDir = 'desc', limit = 500 } = req.body;

        // Only allow querying safe tables
        const ALLOWED_TABLES = ['events', 'daily_metrics'];
        if (!ALLOWED_TABLES.includes(table)) return res.status(400).json({ error: 'Invalid table' });

        let query = supabase.from(table).select('*');

        if (table === 'events') {
            // Convert LA date boundaries to UTC so query matches how the dashboard counts
            const toLA_UTC = (dateStr, time) => {
                // Create a date in LA timezone, get its UTC equivalent
                const d = new Date(`${dateStr}T${time}`);
                const utc = new Date(d.toLocaleString('en-US', { timeZone: 'UTC' }));
                const la = new Date(d.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }));
                const offset = (utc - la) / 60000; // offset in minutes
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

app.post('/api/insights/chat', dashboardLimiter, requireAuth, async (req, res) => {
    if (!ANTHROPIC_API_KEY) {
        return res.status(503).json({ error: 'AI Insights not configured — set ANTHROPIC_API_KEY' });
    }

    try {
        const { messages = [] } = req.body;
        if (!Array.isArray(messages) || messages.length === 0) {
            return res.status(400).json({ error: 'Send { messages: [{ role, content }] }' });
        }

        // Fetch context: last 90 days of metrics + recent events (summarized)
        const [metricsRes, eventsRes] = await Promise.all([
            supabase.from('daily_metrics').select('*').order('date', { ascending: false }).limit(90),
            supabase.from('events').select('event_type, name, email, event_time').order('event_time', { ascending: false }).limit(500),
        ]);

        const metricsData = (metricsRes.data || []).map(r => ({
            date: r.date,
            day: r.day_of_week,
            fb_spend: Number(r.fb_spend),
            fb_link_clicks: Number(r.fb_link_clicks || 0),
            registrations: r.registrations,
            attended: r.attended,
            replays: r.replays,
            viewedcta: r.viewedcta,
            clickedcta: r.clickedcta,
            purchases: r.purchases,
        }));

        // Summarize events by date + type (instead of sending thousands of individual records)
        const eventSummary = {};
        const recentEvents = [];
        (eventsRes.data || []).forEach((e, i) => {
            const laDate = new Date(e.event_time).toLocaleDateString('en-CA', { timeZone: 'America/Los_Angeles' });
            const k = `${laDate}|${e.event_type}`;
            if (!eventSummary[k]) eventSummary[k] = 0;
            eventSummary[k]++;
            // Keep last 50 individual events for recent activity detail
            if (i < 50) {
                recentEvents.push({
                    type: e.event_type,
                    name: e.name,
                    email: e.email,
                    date: laDate,
                });
            }
        });
        // Convert summary to compact format
        const dailyEventCounts = {};
        for (const [k, count] of Object.entries(eventSummary)) {
            const [date, type] = k.split('|');
            if (!dailyEventCounts[date]) dailyEventCounts[date] = {};
            dailyEventCounts[date][type] = count;
        }

        const today = new Date().toLocaleDateString('en-US', { timeZone: 'America/Los_Angeles' });

        const systemPrompt = `You are a senior business analyst for Dr Shumard, a medical practice. You analyze their marketing funnel and provide actionable insights.

TODAY'S DATE: ${today} (Los Angeles timezone)

THE FUNNEL STAGES (in order):
1. FB Spend — daily Facebook ad budget  
2. Total Registration Page Visited (fb_link_clicks) — people who clicked the ad link to the registration page
3. Registrations — people who signed up for the webinar
4. Attended — people who actually attended the webinar
5. Replays — people who watched the replay
6. Viewed CTA — people who saw the call to action
7. Clicked CTA — people who clicked the call to action  
8. Purchases — people who purchased

KEY METRICS TO TRACK:
- Landing Page Conversion Rate (registrations / fb_link_clicks × 100)
- Cost per Registration (fb_spend / registrations)
- Attendance Rate (attended / registrations × 100)
- CTA View Rate (viewedcta / (attended + replays) × 100)
- CTA Click Rate (clickedcta / viewedcta × 100)
- Conversion Rate (purchases / clickedcta × 100)
- Cost per Acquisition (fb_spend / purchases)

DAILY METRICS (last ${metricsData.length} days, newest first):
${JSON.stringify(metricsData, null, 0)}

DAILY EVENT COUNTS (from individual event tracking):
${JSON.stringify(dailyEventCounts, null, 0)}

RECENT EVENTS (last ${recentEvents.length} individual events):
${JSON.stringify(recentEvents, null, 0)}

INSTRUCTIONS:
- Give specific, data-backed insights. Reference actual numbers and dates.
- Identify trends, anomalies, and opportunities.
- Compare periods (week-over-week, day-over-day) when relevant.
- Suggest concrete actions to improve funnel conversion.
- Format responses with markdown: use headers, bullet points, bold for key numbers.
- Keep responses focused and actionable — not overly long.
- If asked about something not in the data, say so honestly.`;

        // Call Claude API
        const response = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01',
            },
            body: JSON.stringify({
                model: 'claude-sonnet-4-20250514',
                max_tokens: 2048,
                system: systemPrompt,
                messages: messages.map(m => ({
                    role: m.role === 'user' ? 'user' : 'assistant',
                    content: m.content,
                })),
            }),
        });

        if (!response.ok) {
            const errBody = await response.text();
            console.error('❌ Claude API error:', response.status, errBody);
            return res.status(502).json({ error: 'AI service error', detail: errBody });
        }

        const result = await response.json();
        const reply = result.content?.[0]?.text || 'No response generated.';

        res.json({ reply, usage: result.usage });

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