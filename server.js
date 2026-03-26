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
import { syncFacebookSpend } from './fb-sync.js';
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
    max: 60,
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
    if (!crypto.timingSafeEqual(Buffer.from(key), Buffer.from(API_KEY))) {
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
            registrations: parseInt(body.registrations) || 0,
            replays: parseInt(body.replays) || 0,
            viewedcta: parseInt(body.viewedcta) || 0,
            clickedcta: parseInt(body.clickedcta) || 0,
            purchases: parseInt(body.purchases) || 0,
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
                registrations: parseInt(entry.registrations) || 0,
                replays: parseInt(entry.replays) || 0,
                viewedcta: parseInt(entry.viewedcta) || 0,
                clickedcta: parseInt(entry.clickedcta) || 0,
                purchases: parseInt(entry.purchases) || 0,
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
        const { field, count = 1, name, email, phone, ...rest } = req.body;
        const validFields = ['fb_spend', 'registrations', 'replays', 'viewedcta', 'clickedcta', 'purchases'];

        if (!validFields.includes(field)) {
            return res.status(400).json({ error: `Invalid field. Use: ${validFields.join(', ')}` });
        }

        const today = getLADate();
        const isoToday = dateToISO(today);
        const dayOfWeek = getLADayOfWeek(today);

        // Ensure today's row exists
        await supabase
            .from('daily_metrics')
            .upsert({ date: isoToday, day_of_week: dayOfWeek }, { onConflict: 'date' });

        // Increment via read-modify-write
        const { data: current } = await supabase
            .from('daily_metrics')
            .select(field)
            .eq('date', isoToday)
            .single();

        const newValue = (Number(current?.[field]) || 0) + Number(count);

        const { data, error } = await supabase
            .from('daily_metrics')
            .update({ [field]: newValue })
            .eq('date', isoToday)
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
            metadata: Object.keys(metadata).length > 0 ? metadata : {},
        }).then(({ error: evErr }) => {
            if (evErr) console.error('⚠️ Event insert error:', evErr.message);
        });

        console.log(`✅ Increment ${field} +${count} for ${today}${name ? ` (${name})` : ''}`);
        res.json({ success: true, date: today, field, previous: current?.[field], new: newValue });

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
        const validFields = ['fb_spend', 'registrations', 'replays', 'viewedcta', 'clickedcta', 'purchases'];

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
        const newValue = field === 'fb_spend' ? parseFloat(value) : parseInt(value);

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

// POST /api/refresh-date — Re-fetch Facebook spend for a specific date
app.post('/api/refresh-date', dashboardLimiter, async (req, res) => {
    const { date } = req.body; // expects YYYY-MM-DD
    if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
        return res.status(400).json({ error: 'Send { date: "YYYY-MM-DD" }' });
    }

    try {
        const { fetchFacebookSpend, writeSpendToSupabase } = await import('./fb-sync.js');
        const spend = await fetchFacebookSpend(date);
        await writeSpendToSupabase(date, spend);

        console.log(`✅ Recalc spend for ${date}: $${spend.toFixed(2)}`);
        return res.json({ message: `Spend for ${date} updated to $${spend.toFixed(2)}`, spend });
    } catch (err) {
        console.error(`❌ Recalc spend for ${date} failed:`, err.message);
        return res.status(502).json({ error: `Facebook API error: ${err.message}` });
    }
});

// GET /api/me — Return current user's role
app.get('/api/me', dashboardLimiter, requireAuth, async (req, res) => {
    try {
        const { data } = await supabase
            .from('user_roles')
            .select('role')
            .eq('user_id', req.user.id)
            .single();
        res.json({
            id: req.user.id,
            email: req.user.email,
            role: data?.role || 'viewer',
        });
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch user info' });
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
            .order('date', { ascending: false })
            .range(Number(offset), Number(offset) + Number(limit) - 1);

        if (error) throw error;

        // Convert to frontend format (MM/DD/YYYY) — pure string, no timezone shift
        const formatted = (data || []).map(row => {
            const dateStr = String(row.date).substring(0, 10);
            const mmddyyyy = dateStr.replace(/(\d{4})-(\d{2})-(\d{2})/, '$2/$3/$1');
            return {
                date: mmddyyyy,
                day: row.day_of_week,
                fb_spend: Number(row.fb_spend),
                registrations: row.registrations,
                replays: row.replays,
                viewedcta: row.viewedcta,
                clickedcta: row.clickedcta,
                purchases: row.purchases,
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
app.put('/api/metrics/:date', dashboardLimiter, requireAuth, requireAdmin, async (req, res) => {
    try {
        const dateInput = parseDateInput(req.params.date);
        if (!dateInput) return res.status(400).json({ error: 'Invalid date' });

        const isoDate = dateToISO(dateInput);
        const body = req.body;

        const updates = {};
        if (body.fb_spend !== undefined) updates.fb_spend = parseFloat(body.fb_spend) || 0;
        if (body.registrations !== undefined) updates.registrations = parseInt(body.registrations) || 0;
        if (body.replays !== undefined) updates.replays = parseInt(body.replays) || 0;
        if (body.viewedcta !== undefined) updates.viewedcta = parseInt(body.viewedcta) || 0;
        if (body.clickedcta !== undefined) updates.clickedcta = parseInt(body.clickedcta) || 0;
        if (body.purchases !== undefined) updates.purchases = parseInt(body.purchases) || 0;

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

    console.log('📡 Facebook ad spend sync enabled (every 30 min)');
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

app.listen(PORT, () => {
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
});

export default app;