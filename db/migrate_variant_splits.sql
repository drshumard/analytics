-- Persist per-variant (A/B/undetected) splits for FINALIZED days.
--
-- Why: the dashboard always loads /api/metrics?expand=variants, and the expanded view
-- needs the A/B/undetected breakdown per field. Finalized days store only the `all`
-- total in canonical columns, so the expanded view re-derives the split by deduping the
-- ENTIRE event history from `events` on every cold in-memory cache (~20s). Persisting the
-- split here lets the read path use the column for finalized days and dedup ONLY today.
--
-- Additive + nullable: old code ignores it; finalize writes it best-effort. Safe to run
-- anytime. Run for EACH funnel schema (analytics = public, native = native).
--
-- Shape: { "<field>": { "A": int, "B": int, "undetected": int }, ... }
--        fields = registrations, attended, replays, viewedcta, clickedcta,
--                 purchases_fb/native/youtube/aibot/postwebinar/cpa/sales_a/sales_b,
--                 stayed_45/60/80.

ALTER TABLE public.daily_metrics ADD COLUMN IF NOT EXISTS variant_splits jsonb;
ALTER TABLE native.daily_metrics ADD COLUMN IF NOT EXISTS variant_splits jsonb;
