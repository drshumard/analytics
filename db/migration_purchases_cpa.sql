-- Migration: Add purchases_cpa column for the "CPA Traffic Funnel" purchase source
-- Run this in Supabase SQL Editor.
--
-- Purchases arriving with metadata.source = 'CPA Traffic' are routed into this
-- column by the increment handler. Lives in both schemas: the analytics funnel
-- uses it; the native funnel keeps it at 0 (harmless, avoids per-funnel branching).

ALTER TABLE public.daily_metrics
    ADD COLUMN IF NOT EXISTS purchases_cpa INTEGER NOT NULL DEFAULT 0;

ALTER TABLE native.daily_metrics
    ADD COLUMN IF NOT EXISTS purchases_cpa INTEGER NOT NULL DEFAULT 0;

-- Refresh the default lens's metric list so the new column shows up by default
UPDATE public.dashboard_lenses
   SET metrics = '["fb_spend","fb_link_clicks","registrations","attended","replays","viewedcta","clickedcta","purchases_fb","purchases_native","purchases_youtube","purchases_aibot","purchases_postwebinar","purchases_cpa","total_purchases"]'::jsonb
 WHERE id = 'default-all';

UPDATE native.dashboard_lenses
   SET metrics = '["fb_spend","fb_link_clicks","registrations","attended","replays","viewedcta","clickedcta","purchases_fb","purchases_native","purchases_youtube","purchases_aibot","purchases_postwebinar","purchases_cpa","total_purchases"]'::jsonb
 WHERE id = 'default-all';

NOTIFY pgrst, 'reload schema';
