-- Indexes that stop the two full-table scans behind the Supabase "high disk I/O" alerts.
--
-- 1) tracking_contacts (email): the tracking endpoints look contacts up with `email = $1`
--    (emailAutoStitch, CRM contact lookup). The only email index was on lower(email), which
--    a plain equality can't use, so every /api/sg/lead|registration call seq-scanned the
--    whole table (~315 MB, 5M+ scans by Sept 2026).
-- 2) tracking_page_visits ("timestamp"): getRegPageVisits (every dashboard load) bucketed
--    by (timestamp AT TIME ZONE 'America/Los_Angeles')::date with no usable index → full
--    scan of ~620 MB per request. server.js now adds sargable timestamp bounds so this
--    index applies (7-day view: 78k pages read → ~6k, 44 s cold → ~90 ms warm).
--
-- Applied to public on 2026-09-25 via a direct session (CONCURRENTLY can't run inside
-- the ai_run_sql_write RPC — it runs in a transaction with a 10 s timeout). Run for any
-- other funnel schema that has the shumard tracking tables.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tracking_contacts_email_plain ON public.tracking_contacts (email);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tracking_visits_ts ON public.tracking_page_visits ("timestamp");
