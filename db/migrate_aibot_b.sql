-- Migration: Add purchases_aibot_b column for the "AI Bot B" purchase source.
-- Run this in Supabase SQL Editor (both schemas), then deploy.
--
-- Purchases arriving with metadata.source = 'AI Bot B' are routed into this column by
-- the increment handler (PURCHASE_SOURCE_MAP) and counted under it by dedup/finalize
-- (PURCHASE_DEDUP_MAP). Mirrors purchases_aibot ("AI Bot"). increment_field is
-- column-dynamic (%I), so no function change is needed.

ALTER TABLE public.daily_metrics
    ADD COLUMN IF NOT EXISTS purchases_aibot_b INTEGER NOT NULL DEFAULT 0;

ALTER TABLE native.daily_metrics
    ADD COLUMN IF NOT EXISTS purchases_aibot_b INTEGER NOT NULL DEFAULT 0;

-- Refresh the default lens's metric list so the new column shows up by default
UPDATE public.dashboard_lenses
   SET metrics = '["fb_spend","fb_link_clicks","registrations","attended","replays","viewedcta","clickedcta","purchases_fb","purchases_native","purchases_youtube","purchases_aibot","purchases_aibot_b","purchases_postwebinar","purchases_cpa","purchases_sales_a","purchases_sales_b","total_purchases"]'::jsonb
 WHERE id = 'default-all';

UPDATE native.dashboard_lenses
   SET metrics = '["fb_spend","fb_link_clicks","registrations","attended","replays","viewedcta","clickedcta","purchases_fb","purchases_native","purchases_youtube","purchases_aibot","purchases_aibot_b","purchases_postwebinar","purchases_cpa","purchases_sales_a","purchases_sales_b","total_purchases"]'::jsonb
 WHERE id = 'default-all';

NOTIFY pgrst, 'reload schema';
