-- ============================================================================
-- Migration: AI Worker — write-capable SQL RPC + audit log
--
-- ai_run_sql_write() is the AI Worker's escape hatch for data fixes the
-- dedicated tools (add_sale, delete_event, …) don't cover. It executes ONE
-- statement of any kind. A SELECT/WITH statement returns rows (500 cap, same
-- shape as ai_run_sql); WITH also covers DML+RETURNING via
--   WITH x AS (UPDATE ... RETURNING *) SELECT * FROM x.
-- Anything else executes and returns the affected row count.
--
-- DEFENSE LAYERS:
--   1. Only the admin-gated /api/worker/chat endpoint calls it (requireAdmin).
--   2. Node-side validator rejects statement chaining and requires an explicit
--      confirm flag for DDL/TRUNCATE.
--   3. statement_timeout aborts at 10s.
--   4. GRANT is service_role ONLY — unlike ai_run_sql, the authenticated role
--      can NOT call this via PostgREST, so a dashboard viewer's JWT is useless.
--
-- worker_audit_log records every write-tool call the worker makes (who, what,
-- result). RLS enabled with no policies: readable/writable only via the
-- service key.
--
-- Per-funnel: run once in each schema. For native, prefix with
--   SET search_path = native;
-- ============================================================================

CREATE OR REPLACE FUNCTION ai_run_sql_write(query text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout = '10s'
AS $$
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
$$;

REVOKE ALL ON FUNCTION ai_run_sql_write(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ai_run_sql_write(text) TO service_role;

CREATE TABLE IF NOT EXISTS worker_audit_log (
    id          BIGSERIAL PRIMARY KEY,
    user_email  TEXT,
    tool        TEXT NOT NULL,
    input       JSONB NOT NULL DEFAULT '{}',
    result      JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_worker_audit_log_created
    ON worker_audit_log (created_at DESC);

ALTER TABLE worker_audit_log ENABLE ROW LEVEL SECURITY;

-- ============================================================================
-- For the `native` funnel schema, run this again with:
--   SET search_path = native;
-- ============================================================================
