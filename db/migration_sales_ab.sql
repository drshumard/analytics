-- Migration: Add purchases_sales_a / purchases_sales_b columns for the
-- "Sales A" and "Sales B" purchase sources. Run this in Supabase SQL Editor.
--
-- Purchases arriving with metadata.source = 'Sales A' / 'Sales B' are routed
-- into these columns by the increment handler. Lives in both schemas: the
-- analytics funnel uses them; the native funnel keeps them at 0 (harmless,
-- avoids per-funnel branching).

ALTER TABLE public.daily_metrics
    ADD COLUMN IF NOT EXISTS purchases_sales_a INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS purchases_sales_b INTEGER NOT NULL DEFAULT 0;

ALTER TABLE native.daily_metrics
    ADD COLUMN IF NOT EXISTS purchases_sales_a INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS purchases_sales_b INTEGER NOT NULL DEFAULT 0;

-- Refresh the default lens's metric list so the new columns show up by default
UPDATE public.dashboard_lenses
   SET metrics = '["fb_spend","fb_link_clicks","registrations","attended","replays","viewedcta","clickedcta","purchases_fb","purchases_native","purchases_youtube","purchases_aibot","purchases_postwebinar","purchases_cpa","purchases_sales_a","purchases_sales_b","total_purchases"]'::jsonb
 WHERE id = 'default-all';

UPDATE native.dashboard_lenses
   SET metrics = '["fb_spend","fb_link_clicks","registrations","attended","replays","viewedcta","clickedcta","purchases_fb","purchases_native","purchases_youtube","purchases_aibot","purchases_postwebinar","purchases_cpa","purchases_sales_a","purchases_sales_b","total_purchases"]'::jsonb
 WHERE id = 'default-all';

NOTIFY pgrst, 'reload schema';
