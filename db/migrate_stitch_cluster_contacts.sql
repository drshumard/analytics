-- ============================================================================
-- Migration: stitch_cluster_contacts() — merge-cluster rows in one round-trip
--
-- getStitchAliases() previously paged the ENTIRE tracking_contacts table at
-- 1000 rows/request (145k+ rows ≈ 146 sequential round-trips ≈ 30s) on every
-- cache refresh — the main cause of multi-minute dashboard loads. Only two
-- kinds of rows can ever influence an alias:
--   1. rows in a merge chain (child or root) — they form multi-email clusters;
--   2. never-merged rows carrying BOTH an email AND a phone — their phone can
--      bridge to a tagged registrant, aliasing their email to the registrant
--      (a singleton with only an email, or only a phone, can't yield an alias:
--      its cluster holds nothing to alias / nothing to match a registrant by).
-- This returns just those rows (~1/3 of the table) as a single jsonb payload —
-- one round-trip, immune to the PostgREST 1000-row page cap because jsonb is
-- a single value.
--
-- server.js falls back to the old full-table paging if this function is
-- absent (e.g. a schema that never ran this migration).
--
-- Per-funnel: only `public` has tracking_contacts today. If a funnel schema
-- ever gains tracking tables, re-run with: SET search_path = <schema>;
-- ============================================================================

CREATE OR REPLACE FUNCTION stitch_cluster_contacts()
RETURNS jsonb
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public
SET statement_timeout = '15s'
AS $$
    SELECT COALESCE(jsonb_agg(row_to_json(t)), '[]'::jsonb)
    FROM (
        SELECT contact_id, email, phone, merged_into
        FROM tracking_contacts
        WHERE merged_into IS NOT NULL
           OR contact_id IN (SELECT merged_into FROM tracking_contacts
                             WHERE merged_into IS NOT NULL)
           OR (email IS NOT NULL AND phone IS NOT NULL)
    ) t;
$$;

REVOKE ALL ON FUNCTION stitch_cluster_contacts() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION stitch_cluster_contacts() TO service_role;
