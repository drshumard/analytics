-- ============================================================================
-- Tracking + CRM — shumard.js identity-stitching tables
--
-- Ported from the `tether` engine (FastAPI/MongoDB) into the analytics Postgres
-- schema. These tables hold the data shumard.js collects on the client site:
-- a persistent tracked identity per browser, the full pageview journey, and
-- timestamped tag fires. The CRM tab joins these to the existing `events` table
-- (registrations/attended/replays/CTA/purchases) by email/phone at read time.
--
-- Run ONCE against the analytics funnel (public schema):
--   psql $DATABASE_URL -f db/migrate_tracking_crm.sql
--
-- To enable the native funnel later, re-run with the native schema first:
--   SET search_path = native;  (then paste the CREATE TABLE / CREATE INDEX block)
-- Every object is schema-qualified to `public` below; swap to `native.` to mirror.
-- ============================================================================


-- ─── tracking_contacts ──────────────────────────────────────────────────────
-- One row per tracked browser identity (contact_id minted client-side by
-- shumard.js). Identities are fused via the merged_into chain by the stitching
-- engine (session_id > email > shared IP). A non-null merged_into means this
-- row was absorbed into a parent and should be hidden from the CRM list.
CREATE TABLE IF NOT EXISTS public.tracking_contacts (
    contact_id       TEXT PRIMARY KEY,                 -- UUID minted by shumard.js
    session_id       TEXT,                             -- shared across parent+iframe in one tab
    client_ip        TEXT,
    user_agent       TEXT,
    name             TEXT,
    email            TEXT,
    phone            TEXT,
    first_name       TEXT,
    last_name        TEXT,
    attribution      JSONB  NOT NULL DEFAULT '{}',     -- utm_*, fbclid, fbc, fbp, gclid, ..., extra{}
    tags             TEXT[] NOT NULL DEFAULT '{}',     -- shumard.js ?tag= fires (set membership)
    merged_into      TEXT,                             -- parent contact_id once merged
    merged_children  TEXT[] NOT NULL DEFAULT '{}',     -- child contact_ids absorbed into this row
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tracking_contacts_email      ON public.tracking_contacts (LOWER(email));
CREATE INDEX IF NOT EXISTS idx_tracking_contacts_phone      ON public.tracking_contacts (phone);
CREATE INDEX IF NOT EXISTS idx_tracking_contacts_session    ON public.tracking_contacts (session_id);
CREATE INDEX IF NOT EXISTS idx_tracking_contacts_ip_created ON public.tracking_contacts (client_ip, created_at);
CREATE INDEX IF NOT EXISTS idx_tracking_contacts_merged     ON public.tracking_contacts (merged_into);
CREATE INDEX IF NOT EXISTS idx_tracking_contacts_updated    ON public.tracking_contacts (updated_at DESC);


-- ─── tracking_page_visits ───────────────────────────────────────────────────
-- Every pageview from shumard.js. On a merge, child visits are re-pointed to the
-- surviving parent's contact_id; original_contact_id preserves where they came
-- from (so a re-stitch can move them back).
CREATE TABLE IF NOT EXISTS public.tracking_page_visits (
    id                  TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    contact_id          TEXT NOT NULL,
    original_contact_id TEXT,
    session_id          TEXT,
    client_ip           TEXT,
    current_url         TEXT NOT NULL,
    referrer_url        TEXT,
    page_title          TEXT,
    attribution         JSONB NOT NULL DEFAULT '{}',
    timestamp           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tracking_visits_contact ON public.tracking_page_visits (contact_id, timestamp);


-- ─── tracking_tag_events ────────────────────────────────────────────────────
-- Timestamped tag fires (shumard.js loaded as shumard.js?tag=attended, etc.).
-- tracking_contacts.tags[] holds set-membership; this table is the timeline so
-- the CRM journey can place "saw cta / clicked cta / attended" moments in time.
CREATE TABLE IF NOT EXISTS public.tracking_tag_events (
    id                  TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    contact_id          TEXT NOT NULL,
    original_contact_id TEXT,                  -- set when reassigned during a stitch
    tag                 TEXT NOT NULL,
    current_url         TEXT,
    timestamp           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tracking_tag_events_contact ON public.tracking_tag_events (contact_id, timestamp);


-- Note: updated_at on tracking_contacts is set explicitly by the application on
-- every write (mirrors the tether engine), so no trigger is needed here.

-- Tell PostgREST to reload its schema cache so the new tables are queryable
-- without a project restart.
NOTIFY pgrst, 'reload schema';
