-- Migration: Add purchases_retargeting column for the "Retargeting" purchase source.
-- Run against both schemas (public + native), then deploy.
--
-- Purchases arriving with metadata.source = 'Retargeting' are routed into this column
-- by the increment handler (PURCHASE_SOURCE_MAP) and counted under it by dedup/finalize
-- (PURCHASE_DEDUP_MAP). Mirrors purchases_aibot_b. increment_field is column-dynamic
-- (%I), so no function change is needed. Retargeting is NOT subject to Post Webinar
-- re-routing (unlike Paid Ads / Sales A / Sales B) — retargeted buyers have engaged
-- before by definition, so the source tag is kept verbatim.

ALTER TABLE public.daily_metrics
    ADD COLUMN IF NOT EXISTS purchases_retargeting INTEGER NOT NULL DEFAULT 0;

ALTER TABLE native.daily_metrics
    ADD COLUMN IF NOT EXISTS purchases_retargeting INTEGER NOT NULL DEFAULT 0;

-- Append to the default lens WITHOUT overwriting the whole list (earlier migrations
-- replaced it wholesale, but default-all has since been customized — purchases_fb was
-- removed by the admin). Keeps total_purchases as the last column.
UPDATE public.dashboard_lenses
   SET metrics = (metrics - 'total_purchases') || '["purchases_retargeting","total_purchases"]'::jsonb
 WHERE id = 'default-all' AND NOT metrics ? 'purchases_retargeting';

UPDATE native.dashboard_lenses
   SET metrics = (metrics - 'total_purchases') || '["purchases_retargeting","total_purchases"]'::jsonb
 WHERE id = 'default-all' AND NOT metrics ? 'purchases_retargeting';

NOTIFY pgrst, 'reload schema';
