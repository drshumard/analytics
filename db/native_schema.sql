-- ============================================================================
-- Native Funnel — Multi-tenant schema setup
--
-- Run this ONCE in Supabase SQL Editor (single paste).
-- It sets up:
--   1. The `native` schema with all tables/triggers/RPCs (mirrors `public`)
--   2. Cross-funnel control objects in `public`:
--        - api_keys.funnel column        (which funnel a webhook key belongs to)
--        - user_funnel_access table      (which funnels a user can view)
--   3. Backfill so existing users keep analytics access by default
--
-- After running this, also:
--   - Supabase Dashboard → Settings → API → Exposed schemas → add `native`
--   - Insert your own row(s) into user_funnel_access to grant native access
--   - Insert your own row into native.user_roles if you want admin on native
-- ============================================================================


-- ────────────────────────────────────────────────────────────────────────────
-- PART 1 — Cross-funnel control objects (live in public)
-- ────────────────────────────────────────────────────────────────────────────

-- Tag every API key with the funnel it can write to
ALTER TABLE public.api_keys
    ADD COLUMN IF NOT EXISTS funnel TEXT NOT NULL DEFAULT 'analytics'
    CHECK (funnel IN ('analytics', 'native'));

-- Per-user funnel visibility (gates the X-Funnel header on dashboard requests)
CREATE TABLE IF NOT EXISTS public.user_funnel_access (
    user_id  UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    funnel   TEXT NOT NULL CHECK (funnel IN ('analytics', 'native')),
    PRIMARY KEY (user_id, funnel)
);

-- Backfill: every existing user with a role row gets analytics access
INSERT INTO public.user_funnel_access (user_id, funnel)
SELECT user_id, 'analytics' FROM public.user_roles
ON CONFLICT DO NOTHING;


-- ────────────────────────────────────────────────────────────────────────────
-- PART 2 — Native schema (mirrors public, pre-consolidated columns)
-- ────────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS native;
SET search_path = native;


-- daily_metrics (consolidated: includes all column migrations)
CREATE TABLE IF NOT EXISTS daily_metrics (
    id                       BIGSERIAL PRIMARY KEY,
    date                     DATE NOT NULL UNIQUE,
    day_of_week              TEXT NOT NULL,
    fb_spend                 NUMERIC(12,2) NOT NULL DEFAULT 0,
    fb_link_clicks           INTEGER NOT NULL DEFAULT 0,
    registrations            INTEGER NOT NULL DEFAULT 0,
    replays                  INTEGER NOT NULL DEFAULT 0,
    viewedcta                INTEGER NOT NULL DEFAULT 0,
    clickedcta               INTEGER NOT NULL DEFAULT 0,
    purchases                INTEGER NOT NULL DEFAULT 0,
    purchases_fb             INTEGER NOT NULL DEFAULT 0,
    purchases_native         INTEGER NOT NULL DEFAULT 0,
    purchases_youtube        INTEGER NOT NULL DEFAULT 0,
    purchases_aibot          INTEGER NOT NULL DEFAULT 0,
    purchases_postwebinar    INTEGER NOT NULL DEFAULT 0,
    purchases_cpa            INTEGER NOT NULL DEFAULT 0,
    purchases_sales_a        INTEGER NOT NULL DEFAULT 0,
    purchases_sales_b        INTEGER NOT NULL DEFAULT 0,
    stayed_45                INTEGER NOT NULL DEFAULT 0,
    stayed_60                INTEGER NOT NULL DEFAULT 0,
    stayed_80                INTEGER NOT NULL DEFAULT 0,
    attended                 INTEGER NOT NULL DEFAULT 0,
    overrides                JSONB DEFAULT '{}',
    finalized_at             TIMESTAMPTZ,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics (date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_metrics_finalized_at ON daily_metrics (finalized_at);


-- custom_metrics
CREATE TABLE IF NOT EXISTS custom_metrics (
    id              TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    name            TEXT NOT NULL,
    formula         TEXT NOT NULL,
    format          TEXT NOT NULL DEFAULT 'number' CHECK (format IN ('number', 'percent', 'currency')),
    sort_order      INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- webhook_log (per-funnel audit trail)
CREATE TABLE IF NOT EXISTS webhook_log (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT NOT NULL DEFAULT 'unknown',
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'received' CHECK (status IN ('received', 'processed', 'error')),
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhook_log_created ON webhook_log (created_at DESC);


-- events (with execution_id pre-applied)
CREATE TABLE IF NOT EXISTS events (
    id            BIGSERIAL PRIMARY KEY,
    event_type    TEXT NOT NULL,
    name          TEXT,
    email         TEXT,
    phone         TEXT,
    metadata      JSONB DEFAULT '{}',
    event_time    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    execution_id  TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_type_time ON events (event_type, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_email ON events (email);
CREATE INDEX IF NOT EXISTS idx_events_execution_id ON events (execution_id);


-- user_roles (per-funnel; references shared auth.users)
CREATE TABLE IF NOT EXISTS user_roles (
    user_id      UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    role         TEXT NOT NULL DEFAULT 'viewer' CHECK (role IN ('admin', 'viewer')),
    preferences  JSONB NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- chat_conversations (per-funnel AI Insights history)
CREATE TABLE IF NOT EXISTS chat_conversations (
    id          TEXT PRIMARY KEY,
    user_id     UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    title       TEXT NOT NULL DEFAULT 'New chat',
    messages    JSONB NOT NULL DEFAULT '[]',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_conversations_user
    ON chat_conversations (user_id, updated_at DESC);

ALTER TABLE chat_conversations ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can manage own conversations"
    ON chat_conversations
    FOR ALL
    USING (auth.uid() = user_id)
    WITH CHECK (auth.uid() = user_id);


-- dashboard_lenses (per-funnel lens definitions; seeded with default)
CREATE TABLE IF NOT EXISTS dashboard_lenses (
    id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    name        TEXT NOT NULL,
    metrics     JSONB NOT NULL DEFAULT '[]',
    sort_order  INTEGER NOT NULL DEFAULT 0,
    created_by  UUID REFERENCES auth.users(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO dashboard_lenses (id, name, metrics, sort_order)
VALUES (
    'default-all',
    'All Metrics',
    '["fb_spend","fb_link_clicks","registrations","attended","replays","viewedcta","clickedcta","purchases_fb","purchases_native","purchases_youtube","purchases_aibot","purchases_postwebinar","purchases_cpa","purchases_sales_a","purchases_sales_b","total_purchases"]',
    0
) ON CONFLICT (id) DO UPDATE SET metrics = EXCLUDED.metrics;


-- updated_at trigger function (lives in native; resolved via search_path)
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_daily_metrics_updated
    BEFORE UPDATE ON daily_metrics
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_custom_metrics_updated
    BEFORE UPDATE ON custom_metrics
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_chat_conversations_updated
    BEFORE UPDATE ON chat_conversations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();


-- increment_field RPC (atomic counter increment for daily_metrics)
-- Resolves daily_metrics via search_path; PostgREST sets search_path=native
-- when called with db: { schema: 'native' }, so this works correctly.
CREATE OR REPLACE FUNCTION increment_field(p_date DATE, p_field TEXT, p_amount INTEGER)
RETURNS VOID AS $$
BEGIN
    EXECUTE format(
        'UPDATE daily_metrics SET %I = %I + $1 WHERE date = $2',
        p_field, p_field
    ) USING p_amount, p_date;
END;
$$ LANGUAGE plpgsql;


-- Note: api_keys is NOT created in native — it lives only in public.api_keys
-- with a `funnel` column to keep webhook key management in one place.


-- ────────────────────────────────────────────────────────────────────────────
-- PART 3 — Grants for Supabase roles
--
-- Without these, the service_role (used by the Express server) gets
-- "permission denied for schema native" on every query, even though the
-- schema is exposed via PostgREST. ALTER DEFAULT PRIVILEGES handles any
-- objects added to the schema later.
-- ────────────────────────────────────────────────────────────────────────────

GRANT USAGE ON SCHEMA native TO postgres, anon, authenticated, service_role;
GRANT ALL ON ALL TABLES    IN SCHEMA native TO postgres, anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA native TO postgres, anon, authenticated, service_role;
GRANT ALL ON ALL ROUTINES  IN SCHEMA native TO postgres, anon, authenticated, service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA native GRANT ALL ON TABLES    TO postgres, anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA native GRANT ALL ON SEQUENCES TO postgres, anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES IN SCHEMA native GRANT ALL ON ROUTINES  TO postgres, anon, authenticated, service_role;

-- Tell PostgREST to reload its schema cache so the native schema is picked up
-- without a project pause/unpause.
NOTIFY pgrst, 'reload schema';


-- ============================================================================
-- DONE. Next steps (manual):
--   1. Supabase Dashboard → Settings → API → Exposed schemas → add `native`
--   2. Grant yourself native access:
--        INSERT INTO public.user_funnel_access (user_id, funnel)
--        SELECT id, 'native' FROM auth.users WHERE email = 'your@email.com';
--   3. Make yourself admin on native:
--        INSERT INTO native.user_roles (user_id, role)
--        SELECT id, 'admin' FROM auth.users WHERE email = 'your@email.com';
-- ============================================================================
