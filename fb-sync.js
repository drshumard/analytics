// ============================================================================
// Facebook Ad Spend Sync
// Pulls today's spend from the Facebook Graph API and writes it to Supabase
// via the internal /api/metrics/set logic (direct DB, no HTTP round-trip)
// ============================================================================

import { createClient } from '@supabase/supabase-js';
import 'dotenv/config';

const FB_ACCESS_TOKEN = process.env.FB_ACCESS_TOKEN;
const FB_AD_ACCOUNT_ID = process.env.FB_AD_ACCOUNT_ID;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const FB_API_VERSION = process.env.FB_API_VERSION || 'v23.0';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ─── Get today's date in LA timezone ─────────────────────────────────────────
function getTodayLA() {
    const now = new Date();
    const laStr = now.toLocaleDateString('en-US', {
        timeZone: 'America/Los_Angeles',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
    });
    // Convert MM/DD/YYYY → YYYY-MM-DD for the Graph API and Supabase
    const [m, d, y] = laStr.split('/');
    return { iso: `${y}-${m}-${d}`, display: laStr };
}

function getDayOfWeek(isoDate) {
    const [y, m, d] = isoDate.split('-').map(Number);
    return new Date(y, m - 1, d).toLocaleDateString('en-US', { weekday: 'long' });
}

// ─── Fetch spend from Facebook Graph API ─────────────────────────────────────
async function fetchFacebookSpend(dateISO) {
    const url = new URL(`https://graph.facebook.com/${FB_API_VERSION}/${FB_AD_ACCOUNT_ID}/insights`);
    url.searchParams.set('access_token', FB_ACCESS_TOKEN);
    url.searchParams.set('fields', 'spend');
    url.searchParams.set('time_range', JSON.stringify({
        since: dateISO,
        until: dateISO,
    }));
    url.searchParams.set('level', 'account');

    const res = await fetch(url.toString());

    if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        const msg = err?.error?.message || `HTTP ${res.status}`;
        throw new Error(`Facebook API error: ${msg}`);
    }

    const json = await res.json();

    // If no data yet today (no impressions), Facebook returns empty data array
    if (!json.data || json.data.length === 0) {
        return 0;
    }

    return parseFloat(json.data[0].spend) || 0;
}

// ─── Write spend to Supabase ─────────────────────────────────────────────────
async function writeSpendToSupabase(isoDate, spend) {
    const dayOfWeek = getDayOfWeek(isoDate);

    // Ensure row exists for today
    await supabase
        .from('daily_metrics')
        .upsert({ date: isoDate, day_of_week: dayOfWeek }, { onConflict: 'date' });

    // Set the absolute spend value (Facebook returns the running total)
    const { data, error } = await supabase
        .from('daily_metrics')
        .update({ fb_spend: spend })
        .eq('date', isoDate)
        .select('date, fb_spend')
        .single();

    if (error) throw error;
    return data;
}

// ─── Main sync function ──────────────────────────────────────────────────────
export { fetchFacebookSpend, writeSpendToSupabase };
export async function syncFacebookSpend() {
    if (!FB_ACCESS_TOKEN || !FB_AD_ACCOUNT_ID) {
        console.warn('⚠️  FB sync skipped — FB_ACCESS_TOKEN or FB_AD_ACCOUNT_ID not set');
        return null;
    }

    const { iso, display } = getTodayLA();

    try {
        const spend = await fetchFacebookSpend(iso);
        const result = await writeSpendToSupabase(iso, spend);

        console.log(`✅ FB sync: $${spend.toFixed(2)} for ${display} (${getDayOfWeek(iso)})`);
        return { date: display, spend, result };

    } catch (err) {
        console.error(`❌ FB sync failed for ${display}:`, err.message);

        // Log the failure to webhook_log for visibility in the dashboard
        try {
            await supabase.from('webhook_log').insert({
                source: 'fb-sync',
                payload: { date: iso, error: err.message },
                status: 'error',
                error_message: err.message,
            });
        } catch (logErr) {
            console.error('Failed to log FB sync error:', logErr.message);
        }

        throw err;
    }
}

// ─── Debug: run standalone with `node fb-sync.js` ────────────────────────────
const isMain = process.argv[1]?.endsWith('fb-sync.js');
if (isMain) {
    syncFacebookSpend()
        .then(r => { console.log('Result:', r); process.exit(0); })
        .catch(e => { console.error('Failed:', e.message); process.exit(1); });
}