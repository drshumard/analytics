-- Migration: Add stayed_45 / stayed_60 / stayed_80 columns
-- Tracks how many viewers stayed in the webinar through each milestone.
-- Populated by a `stayeduntil` body parameter (45 | 60 | 80) on the
-- `stayeduntil` webhook event — the increment handler routes the value
-- to the matching column.
--
-- Lives in both schemas: the native funnel uses these; the analytics
-- funnel keeps them at 0 (matches the purchases_cpa pattern, avoids
-- per-funnel branching in the read path).

ALTER TABLE public.daily_metrics
    ADD COLUMN IF NOT EXISTS stayed_45 INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stayed_60 INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stayed_80 INTEGER NOT NULL DEFAULT 0;

ALTER TABLE native.daily_metrics
    ADD COLUMN IF NOT EXISTS stayed_45 INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stayed_60 INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stayed_80 INTEGER NOT NULL DEFAULT 0;

NOTIFY pgrst, 'reload schema';
