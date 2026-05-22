-- ============================================================================
-- Migration: ai_run_sql() — read-only SQL escape hatch for AI Insights
--
-- The AI Insights chat can call this function via the `run_sql` tool when
-- none of the higher-level tools (get_metrics, compare_periods, etc.) fit.
--
-- DEFENSE LAYERS:
--   1. Node-side validator (isReadOnlySQL in server.js) rejects anything that
--      isn't a single SELECT/WITH statement and bans DDL/DML keywords.
--   2. This function caps results at 500 rows.
--   3. statement_timeout aborts long-running queries at 5s.
--
-- Note: SECURITY DEFINER + service-role calls bypass RLS, so the Node-side
-- validator is the load-bearing guard. Don't expose this RPC to anonymous
-- users — it's only called from the authenticated chat endpoint.
--
-- Per-funnel: run once in each schema. For native, prefix with
--   SET search_path = native;
-- ============================================================================

CREATE OR REPLACE FUNCTION ai_run_sql(query text)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET statement_timeout = '5s'
AS $$
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
$$;

-- Allow the authenticated role (Supabase service key uses this) to call it.
-- If you use a different role for the API, grant to that role instead.
REVOKE ALL ON FUNCTION ai_run_sql(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ai_run_sql(text) TO authenticated, service_role;

-- ============================================================================
-- For the `native` funnel schema, run this version instead:
--   SET search_path = native;
--   <repeat the CREATE OR REPLACE FUNCTION + GRANT blocks above>
-- ============================================================================
