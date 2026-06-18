import 'dotenv/config';
import pg from 'pg';
const { Client } = pg;
const c = new Client({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
await c.connect();
for (const schema of ['public', 'native']) {
  try {
    await c.query(`ALTER TABLE ${schema}.daily_metrics ADD COLUMN IF NOT EXISTS variant_splits jsonb`);
    const r = await c.query(`SELECT column_name FROM information_schema.columns WHERE table_schema='${schema}' AND table_name='daily_metrics' AND column_name='variant_splits'`);
    console.log(`${schema}.daily_metrics.variant_splits:`, r.rows.length ? '✅ present' : '❌ missing');
  } catch (e) {
    console.log(`${schema}: ${e.message}`);
  }
}
await c.end();
