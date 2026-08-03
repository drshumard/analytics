-- ============================================================================
-- Migration: contact_demographics — cached patient-profile demographics
--
-- Backing store for the get_customer_avatar AI tool. Rows are fetched from the
-- CP4L1 patient-profile service (PATIENT_PROFILE_URL, X-API-Key auth), which
-- returns { gender, date_of_birth, age, city, state } keyed by the patient's
-- portal email. Demographics don't change, so found rows are cached forever;
-- found=false rows are retried after 7 days (patients onboard later).
-- matched_email records when an identity-linked alternate email produced the
-- match rather than the funnel email itself.
--
-- RLS enabled with no policies: only the service key (and the SECURITY DEFINER
-- ai_run_sql RPC, for AI joins) can read it.
--
-- Per-funnel: run once in each schema. For native, prefix with
--   SET search_path = native;
-- ============================================================================

CREATE TABLE IF NOT EXISTS contact_demographics (
    email          TEXT PRIMARY KEY,
    found          BOOLEAN NOT NULL DEFAULT FALSE,
    gender         TEXT,
    date_of_birth  TEXT,
    age            INTEGER,
    city           TEXT,
    state          TEXT,
    matched_email  TEXT,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE contact_demographics ENABLE ROW LEVEL SECURITY;

-- ============================================================================
-- For the `native` funnel schema, run this again with:
--   SET search_path = native;
-- ============================================================================
