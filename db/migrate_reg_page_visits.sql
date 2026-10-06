-- Stored reg-page visitor count per finalized day (written by finalize + the
-- /api/admin/backfill-reg-page-visits job). NULL = unknown / before tracking, shown as "—".
-- Per-variant split lives in variant_splits.reg_page_visits. Run once per funnel schema.
ALTER TABLE public.daily_metrics ADD COLUMN IF NOT EXISTS reg_page_visits INTEGER;
ALTER TABLE native.daily_metrics ADD COLUMN IF NOT EXISTS reg_page_visits INTEGER;
ALTER TABLE eboov.daily_metrics ADD COLUMN IF NOT EXISTS reg_page_visits INTEGER;
NOTIFY pgrst, 'reload schema';
