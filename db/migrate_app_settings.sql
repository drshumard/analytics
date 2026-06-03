-- Per-funnel key/value settings. First use: 'ab_test_start' — the timestamp the
-- A/B split test "goes live". Variant attribution ignores any event before it
-- (tags revert to 'undetected'), so pre-launch test traffic doesn't pollute the
-- experiment. The events themselves are untouched and still count in the 'all' funnel.
--
-- Multi-tenant: run for EACH funnel schema (analytics = public, native = native).

-- ── analytics funnel (public schema) ──
CREATE TABLE IF NOT EXISTS public.app_settings (
    key        text PRIMARY KEY,
    value      jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- ── native funnel (native schema) ──
CREATE TABLE IF NOT EXISTS native.app_settings (
    key        text PRIMARY KEY,
    value      jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);
