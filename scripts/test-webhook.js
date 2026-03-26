// ============================================================================
// Test script — sends sample data to your API
// Usage: API_KEY=your-key node scripts/test-webhook.js
// ============================================================================

const API_URL = process.env.API_URL || 'http://localhost:3000';
const API_KEY = process.env.API_KEY;

if (!API_KEY) {
  console.error('Set API_KEY env var first');
  process.exit(1);
}

async function test() {
  console.log(`\n🧪 Testing Dr Shumard API at ${API_URL}\n`);

  // 1. Health check
  console.log('1️⃣  Health check...');
  const health = await fetch(`${API_URL}/api/health`);
  console.log('   ', await health.json());

  // 2. Single entry
  console.log('\n2️⃣  POST /api/metrics (single entry)...');
  const single = await fetch(`${API_URL}/api/metrics`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-Key': API_KEY },
    body: JSON.stringify({
      date: '03/14/2026',
      fb_spend: 245.50,
      registrations: 42,
      replays: 18,
      viewedcta: 35,
      clickedcta: 12,
      purchases: 6,
    }),
  });
  console.log('   ', await single.json());

  // 3. Batch entries
  console.log('\n3️⃣  POST /api/metrics/batch (3 days)...');
  const batch = await fetch(`${API_URL}/api/metrics/batch`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-Key': API_KEY },
    body: JSON.stringify({
      entries: [
        { date: '03/11/2026', fb_spend: 180.00, registrations: 31, replays: 14, viewedcta: 28, clickedcta: 9, purchases: 4 },
        { date: '03/12/2026', fb_spend: 220.75, registrations: 38, replays: 21, viewedcta: 33, clickedcta: 15, purchases: 7 },
        { date: '03/13/2026', fb_spend: 195.25, registrations: 29, replays: 16, viewedcta: 25, clickedcta: 10, purchases: 5 },
      ],
    }),
  });
  console.log('   ', await batch.json());

  // 4. Increment
  console.log('\n4️⃣  POST /api/metrics/increment (add 1 registration)...');
  const inc = await fetch(`${API_URL}/api/metrics/increment`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-API-Key': API_KEY },
    body: JSON.stringify({ field: 'registrations', count: 1 }),
  });
  console.log('   ', await inc.json());

  // 5. Read back
  console.log('\n5️⃣  GET /api/metrics...');
  const read = await fetch(`${API_URL}/api/metrics`);
  const result = await read.json();
  console.log(`    ${result.total} entries:`);
  result.data.forEach(r => {
    console.log(`    ${r.day.padEnd(10)} ${r.date}  $${r.fb_spend.toFixed(2).padStart(8)}  regs:${r.registrations}  purchases:${r.purchases}`);
  });

  console.log('\n✅ All tests passed\n');
}

test().catch(err => {
  console.error('❌ Test failed:', err.message);
  process.exit(1);
});
