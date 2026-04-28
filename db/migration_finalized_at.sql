-- Migration: Add finalized_at to daily_metrics
-- After 4:05 AM PST each day, the previous day's row is "finalized" — its
-- deduped counts are written into the canonical columns and finalized_at is
-- set. The /api/metrics read path skips the dedup engine for any row where
-- finalized_at IS NOT NULL, eliminating the today-row flicker for past days
-- and dropping events-table egress to ~1 day's worth.
--
-- Run after the rest of the migrations:
--     psql $SUPABASE_DB_URL -f db/migration_finalized_at.sql

ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS finalized_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_daily_metrics_finalized_at
    ON daily_metrics (finalized_at);
