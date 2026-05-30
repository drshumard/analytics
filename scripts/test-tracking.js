// ============================================================================
// Test script — exercises the shumard.js tracking + identity-stitching engine.
//
// Prereqs: run db/migrate_tracking_crm.sql against the analytics funnel, then
// start the server (npm start). The /api/track/* endpoints are public (no key).
//
// Usage: API_URL=http://localhost:3000 node scripts/test-tracking.js
//
// How assertions work: after a merge, any follow-up track call made with a
// CHILD contact_id resolves (server-side) to the surviving PARENT and echoes it
// back. So "child resolves to parent" proves the stitch happened.
//
// Note on IPs: every request carries an explicit X-Forwarded-For so each
// scenario gets a distinct "visitor" IP (otherwise all localhost calls would
// share one IP and IP-stitch across scenarios). The IP-companion scenario
// deliberately reuses one IP for both contacts.
// ============================================================================

import 'dotenv/config';                         // pick up PORT from .env so the URL matches your server
const API_URL = process.env.API_URL || `http://localhost:${process.env.PORT || 3000}`;
const run = Date.now().toString(36);           // unique suffix so reruns don't collide
const id = (label) => `test-${label}-${run}`;

let passed = 0, failed = 0;
function assert(cond, msg) {
  if (cond) { passed++; console.log(`   ✅ ${msg}`); }
  else { failed++; console.log(`   ❌ ${msg}`); }
}

// A real browser UA so the server-side bot guard treats these as human traffic.
const BROWSER_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
async function post(path, body, ip) {
  const res = await fetch(`${API_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Forwarded-For': ip || '10.9.0.1', 'User-Agent': BROWSER_UA },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${path} → ${res.status} ${await res.text()}`);
  return res.json();
}

// Resolve a contact_id to its surviving root via a no-op pageview on a throwaway
// IP (so the resolve call itself never triggers an IP merge).
let resolveSeq = 0;
async function resolve(contactId) {
  const r = await post('/api/track/pageview',
    { contact_id: contactId, current_url: 'https://example.com/_resolve' },
    `10.99.${(resolveSeq >> 8) & 255}.${resolveSeq++ & 255}`);
  return r.contact_id;
}

async function main() {
  console.log(`\n🧪 Tracking engine @ ${API_URL} (run ${run})\n`);

  // 0. Script serves
  console.log('0️⃣  GET /shumard.js');
  const js = await fetch(`${API_URL}/shumard.js?tag=attended`);
  const txt = await js.text();
  assert(js.ok && txt.includes('window.Shumard'), 'serves the tracker script');
  assert(txt.includes("AUTO_TAG    = 'attended'"), '?tag= is injected into the script');
  assert(!txt.includes('__TRACKING_BACKEND_URL__'), 'BACKEND_URL placeholder is replaced');

  // 1. Email stitch — two browsers (different IPs), same email → one contact
  console.log('\n1️⃣  Email stitch');
  const e = `buyer-${run}@example.com`;
  const a = await post('/api/track/lead', { contact_id: id('email-A'), email: e }, '10.0.1.1');
  assert(a.contact_id === id('email-A'), 'first lead keeps its contact_id');
  await post('/api/track/lead', { contact_id: id('email-B'), email: e }, '10.0.1.2');
  assert(await resolve(id('email-B')) === id('email-A'), 'second browser (same email) resolves to the first contact');

  // 2. Session stitch — attribution-rich pageview + identity lead sharing a session_id (different IPs)
  console.log('\n2️⃣  Session stitch (cross-frame postMessage shares session_id)');
  const sess = id('sess');
  await post('/api/track/pageview', {
    contact_id: id('sess-landing'), session_id: sess,
    current_url: 'https://drshumardworkshop.com/register?utm_source=facebook&fbclid=abc123',
    attribution: { utm_source: 'facebook', fbclid: 'abc123' },
  }, '10.0.2.1');
  await post('/api/track/lead', { contact_id: id('sess-iframe'), session_id: sess, email: `attendee-${run}@example.com` }, '10.0.2.2');
  assert(await resolve(id('sess-iframe')) === id('sess-landing'), 'identity-only contact merges into the attribution-rich one (same session)');

  // 3. IP iframe-companion — attribution-rich landing + anonymous companion on the SAME IP
  console.log('\n3️⃣  IP iframe-companion stitch (same IP, 30-min window)');
  const sharedIp = '10.0.3.1';
  await post('/api/track/pageview', {
    contact_id: id('ip-landing'),
    current_url: 'https://drshumardworkshop.com/register?utm_campaign=spring&fbclid=zzz999',
    attribution: { utm_campaign: 'spring', fbclid: 'zzz999' },
  }, sharedIp);
  await post('/api/track/pageview', { contact_id: id('ip-anon'), current_url: 'https://joinnow.live/embed/abc?layout=styled-0', attribution: { layout: 'styled-0' } }, sharedIp);
  assert(await resolve(id('ip-anon')) === id('ip-landing'), 'anonymous iframe companion merges into the attribution-rich landing (shared IP)');

  // 4. Tag application + idempotency
  console.log('\n4️⃣  Tag application');
  const t1 = await post('/api/track/tag', { contact_id: id('email-A'), tag: 'attended' }, '10.0.1.1');
  assert(t1.tag === 'attended' && t1.contact_id === id('email-A'), 'tag applied to contact');
  await post('/api/track/tag', { contact_id: id('email-A'), tag: 'attended' }, '10.0.1.1');   // duplicate — no-op on the set

  console.log(`\n${failed === 0 ? '✅' : '❌'} ${passed} passed, ${failed} failed\n`);
  process.exit(failed === 0 ? 0 : 1);
}

main().catch((err) => { console.error('\n❌ Test run failed:', err.message, '\n'); process.exit(1); });
