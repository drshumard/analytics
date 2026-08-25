-- Migration: Add purchases_promo column for the "Promo" purchase source.
-- Run against both schemas (public + native), then deploy.
--
-- Purchases arriving with metadata.source = 'Promo' are routed into this column
-- by the increment handler (PURCHASE_SOURCE_MAP) and counted under it by
-- dedup/finalize (PURCHASE_DEDUP_MAP). Mirrors purchases_retargeting.
-- increment_field is column-dynamic (%I), so no function change is needed.
-- Like Retargeting, Promo is NOT subject to Post Webinar re-routing — the
-- explicit source tag is kept verbatim.

ALTER TABLE public.daily_metrics
    ADD COLUMN IF NOT EXISTS purchases_promo INTEGER NOT NULL DEFAULT 0;

ALTER TABLE native.daily_metrics
    ADD COLUMN IF NOT EXISTS purchases_promo INTEGER NOT NULL DEFAULT 0;

-- Append to the default lens without overwriting admin customizations.
-- Keeps total_purchases as the last column.
UPDATE public.dashboard_lenses
   SET metrics = (metrics - 'total_purchases') || '["purchases_promo","total_purchases"]'::jsonb
 WHERE id = 'default-all' AND NOT metrics ? 'purchases_promo';

UPDATE native.dashboard_lenses
   SET metrics = (metrics - 'total_purchases') || '["purchases_promo","total_purchases"]'::jsonb
 WHERE id = 'default-all' AND NOT metrics ? 'purchases_promo';

NOTIFY pgrst, 'reload schema';
