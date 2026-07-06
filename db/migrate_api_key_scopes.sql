-- Per-key tool scoping for the external AI Tools API (/api/ai/tools).
--
-- scopes = which INSIGHTS_TOOLS an X-API-Key may list and call. DENY BY DEFAULT:
--   NULL or '{}'   → NO AI tools (existing webhook keys keep webhook access only;
--                    the env keys API_KEY/NATIVE_API_KEY also get no AI access)
--   ARRAY['*']     → every external tool
--   e.g. ARRAY['get_metrics','get_journey_funnel'] → only those two
-- PII lives in run_sql, get_contact_journey, get_journey_segment (names/emails/
-- phones) — omit those three for a PII-free key.
--
-- api_keys lives only in the public schema (shared across funnels) — run once.
-- The server tolerates this migration not having run (it selects * and treats
-- a missing scopes column as no scopes = no AI access), so deploy order doesn't
-- matter and no backfill is needed.

ALTER TABLE public.api_keys ADD COLUMN IF NOT EXISTS scopes TEXT[];

-- ─── Minting a key for an external AI app ────────────────────────────────────
-- 1. Generate the raw key (give this to the external app, store only the hash):
--      npm run generate-key
-- 2. Hash it:
--      node -e "console.log(require('crypto').createHash('sha256').update('<raw key>').digest('hex'))"
-- 3. Insert (funnel ∈ 'analytics' | 'native'):
--      INSERT INTO public.api_keys (key_hash, label, funnel, scopes)
--      VALUES ('<hash>', 'external-ai', 'analytics', ARRAY['*']);  -- all tools, incl. PII
--    Or a PII-free key (everything except run_sql/get_contact_journey/get_journey_segment):
--      ... scopes = ARRAY['get_metrics','get_metrics_rollup','compare_periods',
--                         'get_event_counts','list_custom_metrics','get_journey_funnel',
--                         'get_variant_funnel','describe_journey_data','get_email_report',
--                         'get_sales_page_visits']
