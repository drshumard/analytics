-- Run the AI's read-only SQL RPC in Pacific time.
--
-- Why: the AI Insights run_sql tool writes its own SQL. On 2026-09-28 it answered
-- "sales yesterday" (Sun 2026-09-27) with 5 rows instead of 7 because it shifted UTC
-- by hand the wrong way (Sat 17:00Z → Sun 17:00Z). With the function's session timezone
-- set to America/Los_Angeles, `event_time::date`, `current_date`, plain date literals and
-- even the classic double-shift `event_time::timestamp AT TIME ZONE '…'` all yield the
-- Pacific day, so the model can't mis-bucket by omitting or duplicating a conversion.
--
-- Server-side callers of ai_run_sql use explicit offsets / AT TIME ZONE and are unaffected.
-- Applied to public + eboov on 2026-09-29 (native has no ai_run_sql). Run for any new
-- funnel schema that gets its own copy of the function.
ALTER FUNCTION public.ai_run_sql(text) SET timezone = 'America/Los_Angeles';
ALTER FUNCTION eboov.ai_run_sql(text) SET timezone = 'America/Los_Angeles';
