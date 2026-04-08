// Quick test: fetch inline_link_clicks and clicks for April 7th
import 'dotenv/config';

const FB_ACCESS_TOKEN = process.env.FB_ACCESS_TOKEN;
const FB_AD_ACCOUNT_ID = process.env.FB_AD_ACCOUNT_ID;
const FB_API_VERSION = process.env.FB_API_VERSION || 'v23.0';

const dateISO = '2026-04-07';

const url = new URL(`https://graph.facebook.com/${FB_API_VERSION}/${FB_AD_ACCOUNT_ID}/insights`);
url.searchParams.set('access_token', FB_ACCESS_TOKEN);
url.searchParams.set('fields', 'inline_link_clicks,clicks,spend');
url.searchParams.set('time_range', JSON.stringify({ since: dateISO, until: dateISO }));
url.searchParams.set('level', 'account');

console.log(`\n📊 Fetching inline_link_clicks & clicks for ${dateISO}...\n`);

const res = await fetch(url.toString());
const json = await res.json();

if (!res.ok) {
    console.error('❌ API Error:', JSON.stringify(json.error, null, 2));
    process.exit(1);
}

if (!json.data || json.data.length === 0) {
    console.log('No data returned for this date (no impressions/activity).');
} else {
    const row = json.data[0];
    console.log('Results:');
    console.log(`  spend:              ${row.spend ?? 'N/A'}`);
    console.log(`  inline_link_clicks: ${row.inline_link_clicks ?? 'N/A'}`);
    console.log(`  clicks:             ${row.clicks ?? 'N/A (field may not exist)'}`);
    console.log('\nFull response:');
    console.log(JSON.stringify(json.data[0], null, 2));
}
