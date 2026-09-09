-- Migration: funnel registry + per-schema purchase_sources config.
-- Foundation for admin-UI provisioning of funnels and purchase columns.
-- Purely additive: seeds mirror the currently hardcoded maps, so server
-- behavior is unchanged until the config-driven code deploys.
--
-- Applied via ai_run_sql_write (one statement per call).

-- ── 1. Funnel registry (public) ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.funnels (
    key              TEXT PRIMARY KEY CHECK (key ~ '^[a-z][a-z0-9_]{1,20}$'),
    schema_name      TEXT NOT NULL UNIQUE CHECK (schema_name ~ '^[a-z][a-z0-9_]{1,30}$'),
    label            TEXT NOT NULL,
    brand            TEXT NOT NULL DEFAULT 'Dr Shumard',
    brand_context    TEXT DEFAULT 'a medical practice',
    fb_ad_account_id TEXT,
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    created_by       UUID,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.funnels (key, schema_name, label, fb_ad_account_id)
VALUES ('analytics', 'public', 'Main (FB Ads) Funnel', 'act_506738957502269'),
       ('native',    'native', 'Native Ads Funnel',    NULL)
ON CONFLICT (key) DO NOTHING;

-- ── 2. Purchase-source config, one table per funnel schema ──────────────────
-- source_label = webhook metadata.source value; display_label = dashboard header.
CREATE TABLE IF NOT EXISTS public.purchase_sources (
    column_name   TEXT PRIMARY KEY CHECK (column_name ~ '^purchases_[a-z0-9_]{1,24}$'),
    source_label  TEXT NOT NULL UNIQUE,
    display_label TEXT NOT NULL,
    sort_order    INTEGER NOT NULL DEFAULT 0,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS native.purchase_sources (
    column_name   TEXT PRIMARY KEY CHECK (column_name ~ '^purchases_[a-z0-9_]{1,24}$'),
    source_label  TEXT NOT NULL UNIQUE,
    display_label TEXT NOT NULL,
    sort_order    INTEGER NOT NULL DEFAULT 0,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.purchase_sources (column_name, source_label, display_label, sort_order) VALUES
    ('purchases_fb',          'Paid Ads',     'FB Purchases',       10),
    ('purchases_native',      'Native',       'Native Ads',         20),
    ('purchases_youtube',     'Youtube',      'Youtube/Organic',    30),
    ('purchases_aibot',       'AI Bot',       'AI Chat Bot',        40),
    ('purchases_aibot_b',     'AI Bot B',     'AI Chat Bot B',      50),
    ('purchases_postwebinar', 'Post Webinar', 'Post Webinar',       60),
    ('purchases_cpa',         'CPA Traffic',  'CPA Traffic Funnel', 70),
    ('purchases_sales_a',     'Sales A',      'Sales A',            80),
    ('purchases_sales_b',     'Sales B',      'Sales B',            90),
    ('purchases_retargeting', 'Retargeting',  'Retargeting',       100),
    ('purchases_promo',       'Promo',        'Promo',             110)
ON CONFLICT (column_name) DO NOTHING;

INSERT INTO native.purchase_sources (column_name, source_label, display_label, sort_order)
SELECT column_name, source_label, display_label, sort_order FROM public.purchase_sources
ON CONFLICT (column_name) DO NOTHING;

-- ── 3. Funnel checks: hardcoded CHECK lists → FKs to the registry ───────────
ALTER TABLE public.api_keys DROP CONSTRAINT IF EXISTS api_keys_funnel_check;
ALTER TABLE public.api_keys
    ADD CONSTRAINT api_keys_funnel_fkey FOREIGN KEY (funnel) REFERENCES public.funnels(key);

ALTER TABLE public.user_funnel_access DROP CONSTRAINT IF EXISTS user_funnel_access_funnel_check;
ALTER TABLE public.user_funnel_access
    ADD CONSTRAINT user_funnel_access_funnel_fkey FOREIGN KEY (funnel) REFERENCES public.funnels(key);

NOTIFY pgrst, 'reload schema';
