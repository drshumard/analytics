-- ============================================================================
-- Funnel provisioning template — executed by POST /api/admin/funnels.
-- {{schema}} is replaced with the new funnel's schema name (validated
-- ^[a-z][a-z0-9_]{1,20}$ server-side). Statements are separated by
-- `-- @statement` lines (never split on semicolons — function bodies contain
-- them). Purchase-source columns are NOT here: the provisioner ALTERs them in
-- per the admin's selection, then seeds {{schema}}.purchase_sources.
-- Mirrors the live `native` schema (the proven minimal funnel) + ai_memory +
-- ai_run_sql, which native predates.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS {{schema}}

-- @statement
CREATE TABLE {{schema}}.daily_metrics (
    id            BIGSERIAL PRIMARY KEY,
    date          DATE NOT NULL UNIQUE,
    day_of_week   TEXT NOT NULL,
    fb_spend      NUMERIC(12,2) NOT NULL DEFAULT 0,
    fb_link_clicks INTEGER NOT NULL DEFAULT 0,
    registrations INTEGER NOT NULL DEFAULT 0,
    replays       INTEGER NOT NULL DEFAULT 0,
    viewedcta     INTEGER NOT NULL DEFAULT 0,
    clickedcta    INTEGER NOT NULL DEFAULT 0,
    purchases     INTEGER NOT NULL DEFAULT 0,
    attended      INTEGER NOT NULL DEFAULT 0,
    stayed_45     INTEGER NOT NULL DEFAULT 0,
    stayed_60     INTEGER NOT NULL DEFAULT 0,
    stayed_80     INTEGER NOT NULL DEFAULT 0,
    overrides     JSONB DEFAULT '{}',
    variant_splits JSONB,
    finalized_at  TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- @statement
CREATE INDEX ON {{schema}}.daily_metrics (date DESC)

-- @statement
CREATE INDEX ON {{schema}}.daily_metrics (finalized_at)

-- @statement
CREATE TABLE {{schema}}.custom_metrics (
    id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    name        TEXT NOT NULL,
    formula     TEXT NOT NULL,
    format      TEXT NOT NULL DEFAULT 'number' CHECK (format IN ('number', 'percent', 'currency')),
    sort_order  INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- @statement
CREATE TABLE {{schema}}.webhook_log (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT NOT NULL DEFAULT 'unknown',
    payload       JSONB NOT NULL,
    status        TEXT NOT NULL DEFAULT 'received' CHECK (status IN ('received', 'processed', 'error')),
    error_message TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- @statement
CREATE INDEX ON {{schema}}.webhook_log (created_at DESC)

-- @statement
CREATE TABLE {{schema}}.events (
    id           BIGSERIAL PRIMARY KEY,
    event_type   TEXT NOT NULL,
    name         TEXT,
    email        TEXT,
    phone        TEXT,
    metadata     JSONB DEFAULT '{}',
    event_time   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    execution_id TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- @statement
CREATE INDEX ON {{schema}}.events (event_type, event_time DESC)

-- @statement
CREATE INDEX ON {{schema}}.events (email)

-- @statement
CREATE INDEX ON {{schema}}.events (execution_id)

-- @statement
CREATE TABLE {{schema}}.user_roles (
    user_id     UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    role        TEXT NOT NULL DEFAULT 'viewer' CHECK (role IN ('admin', 'viewer')),
    preferences JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- @statement
CREATE TABLE {{schema}}.chat_conversations (
    id         TEXT PRIMARY KEY,
    user_id    UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    title      TEXT NOT NULL DEFAULT 'New chat',
    messages   JSONB NOT NULL DEFAULT '[]',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- @statement
CREATE INDEX ON {{schema}}.chat_conversations (user_id, updated_at DESC)

-- @statement
ALTER TABLE {{schema}}.chat_conversations ENABLE ROW LEVEL SECURITY

-- @statement
CREATE POLICY "Users can manage own conversations" ON {{schema}}.chat_conversations
    FOR ALL USING (auth.uid() = user_id) WITH CHECK (auth.uid() = user_id)

-- @statement
CREATE TABLE {{schema}}.dashboard_lenses (
    id         TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    name       TEXT NOT NULL,
    metrics    JSONB NOT NULL DEFAULT '[]',
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_by UUID REFERENCES auth.users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- @statement
CREATE TABLE {{schema}}.app_settings (
    key        TEXT PRIMARY KEY,
    value      JSONB,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- @statement
CREATE TABLE {{schema}}.identity_links (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alias_email     TEXT NOT NULL UNIQUE,
    canonical_email TEXT NOT NULL,
    note            TEXT,
    created_by      UUID,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- @statement
CREATE TABLE {{schema}}.contact_demographics (
    email         TEXT PRIMARY KEY,
    found         BOOLEAN NOT NULL DEFAULT FALSE,
    gender        TEXT,
    date_of_birth TEXT,
    age           INTEGER,
    city          TEXT,
    state         TEXT,
    matched_email TEXT,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- @statement
CREATE TABLE {{schema}}.worker_audit_log (
    id         BIGSERIAL PRIMARY KEY,
    user_email TEXT,
    tool       TEXT NOT NULL,
    input      JSONB NOT NULL DEFAULT '{}',
    result     JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- @statement
CREATE TABLE {{schema}}.ai_memory (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID NOT NULL,
    key        TEXT NOT NULL,
    value      TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, key)
)

-- @statement
CREATE TABLE {{schema}}.purchase_sources (
    column_name   TEXT PRIMARY KEY CHECK (column_name ~ '^purchases_[a-z0-9_]{1,24}$'),
    source_label  TEXT NOT NULL UNIQUE,
    display_label TEXT NOT NULL,
    sort_order    INTEGER NOT NULL DEFAULT 0,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
)

-- @statement
CREATE OR REPLACE FUNCTION {{schema}}.update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql

-- @statement
CREATE TRIGGER trg_daily_metrics_updated BEFORE UPDATE ON {{schema}}.daily_metrics
    FOR EACH ROW EXECUTE FUNCTION {{schema}}.update_updated_at()

-- @statement
CREATE TRIGGER trg_custom_metrics_updated BEFORE UPDATE ON {{schema}}.custom_metrics
    FOR EACH ROW EXECUTE FUNCTION {{schema}}.update_updated_at()

-- @statement
CREATE TRIGGER trg_chat_conversations_updated BEFORE UPDATE ON {{schema}}.chat_conversations
    FOR EACH ROW EXECUTE FUNCTION {{schema}}.update_updated_at()

-- @statement
CREATE OR REPLACE FUNCTION {{schema}}.increment_field(p_date DATE, p_field TEXT, p_amount INTEGER)
RETURNS VOID AS $$
BEGIN
    EXECUTE format(
        'UPDATE {{schema}}.daily_metrics SET %I = %I + $1 WHERE date = $2',
        p_field, p_field
    ) USING p_amount, p_date;
END;
$$ LANGUAGE plpgsql

-- @statement
CREATE OR REPLACE FUNCTION {{schema}}.ai_run_sql(query text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout TO '5s'
SET search_path TO {{schema}}
AS $function$
DECLARE
    result jsonb;
BEGIN
    EXECUTE format(
        'SELECT COALESCE(jsonb_agg(row_to_json(sub)), ''[]''::jsonb) FROM (SELECT * FROM (%s) AS user_query LIMIT 500) AS sub',
        query
    ) INTO result;
    RETURN result;
EXCEPTION
    WHEN OTHERS THEN
        RETURN jsonb_build_object('error', SQLERRM, 'sqlstate', SQLSTATE);
END;
$function$

-- @statement
CREATE OR REPLACE FUNCTION {{schema}}.ai_run_sql_write(query text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout TO '10s'
SET search_path TO {{schema}}
AS $function$
DECLARE
    result jsonb;
    affected int;
    head text := lower(ltrim(query));
BEGIN
    IF head LIKE 'select%' OR head LIKE 'with%' THEN
        EXECUTE format(
            'SELECT COALESCE(jsonb_agg(row_to_json(sub)), ''[]''::jsonb) FROM (SELECT * FROM (%s) AS user_query LIMIT 500) AS sub',
            query
        ) INTO result;
        RETURN jsonb_build_object('rows', result);
    ELSE
        EXECUTE query;
        GET DIAGNOSTICS affected = ROW_COUNT;
        RETURN jsonb_build_object('rows_affected', affected);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RETURN jsonb_build_object('error', SQLERRM, 'sqlstate', SQLSTATE);
END;
$function$

-- @statement
GRANT USAGE ON SCHEMA {{schema}} TO postgres, anon, authenticated, service_role

-- @statement
GRANT ALL ON ALL TABLES IN SCHEMA {{schema}} TO postgres, anon, authenticated, service_role

-- @statement
GRANT ALL ON ALL SEQUENCES IN SCHEMA {{schema}} TO postgres, anon, authenticated, service_role

-- @statement
GRANT ALL ON ALL ROUTINES IN SCHEMA {{schema}} TO postgres, anon, authenticated, service_role

-- @statement
ALTER DEFAULT PRIVILEGES IN SCHEMA {{schema}} GRANT ALL ON TABLES TO postgres, anon, authenticated, service_role

-- @statement
ALTER DEFAULT PRIVILEGES IN SCHEMA {{schema}} GRANT ALL ON SEQUENCES TO postgres, anon, authenticated, service_role

-- @statement
ALTER DEFAULT PRIVILEGES IN SCHEMA {{schema}} GRANT ALL ON ROUTINES TO postgres, anon, authenticated, service_role

-- @statement
DO $do$
DECLARE cur text;
BEGIN
    SELECT split_part(cfg, '=', 2) INTO cur
      FROM pg_roles r, unnest(r.rolconfig) AS cfg
     WHERE r.rolname = 'authenticator' AND cfg LIKE 'pgrst.db_schemas=%';
    IF cur IS NULL OR cur = '' THEN
        cur := 'public, storage, graphql_public';
    END IF;
    IF position('{{schema}}' IN cur) = 0 THEN
        EXECUTE format('ALTER ROLE authenticator SET pgrst.db_schemas = %L', cur || ', {{schema}}');
    END IF;
END $do$

-- @statement
NOTIFY pgrst, 'reload config'

-- @statement
NOTIFY pgrst, 'reload schema'
