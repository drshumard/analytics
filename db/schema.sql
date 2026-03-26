-- ============================================================================
-- Dr Shumard Analytics — Supabase PostgreSQL Schema
-- Run this in Supabase SQL Editor (or via psql)
-- ============================================================================

-- Daily metrics table (one row per day)
CREATE TABLE IF NOT EXISTS daily_metrics (
    id              BIGSERIAL PRIMARY KEY,
    date            DATE NOT NULL UNIQUE,
    day_of_week     TEXT NOT NULL,
    fb_spend        NUMERIC(12,2) NOT NULL DEFAULT 0,
    registrations   INTEGER NOT NULL DEFAULT 0,
    replays         INTEGER NOT NULL DEFAULT 0,
    viewedcta       INTEGER NOT NULL DEFAULT 0,
    clickedcta      INTEGER NOT NULL DEFAULT 0,
    purchases       INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Custom calculated metrics (user-defined formulas)
CREATE TABLE IF NOT EXISTS custom_metrics (
    id              TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    name            TEXT NOT NULL,
    formula         TEXT NOT NULL,
    format          TEXT NOT NULL DEFAULT 'number' CHECK (format IN ('number', 'percent', 'currency')),
    sort_order      INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- API keys for webhook authentication
CREATE TABLE IF NOT EXISTS api_keys (
    id              BIGSERIAL PRIMARY KEY,
    key_hash        TEXT NOT NULL UNIQUE,
    label           TEXT NOT NULL DEFAULT 'Zapier',
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at    TIMESTAMPTZ
);

-- Audit log for all incoming data
CREATE TABLE IF NOT EXISTS webhook_log (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT NOT NULL DEFAULT 'unknown',
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'received' CHECK (status IN ('received', 'processed', 'error')),
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for fast date lookups (most common query)
CREATE INDEX IF NOT EXISTS idx_daily_metrics_date ON daily_metrics (date DESC);

-- Index for webhook log queries
CREATE INDEX IF NOT EXISTS idx_webhook_log_created ON webhook_log (created_at DESC);

-- Auto-update the updated_at timestamp
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

-- Row Level Security (optional but recommended)
-- Enable if you want Supabase auth protecting direct DB access
-- ALTER TABLE daily_metrics ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE custom_metrics ENABLE ROW LEVEL SECURITY;

-- ============================================================================
-- Seed: Generate your first API key
-- Replace 'your-secret-key-here' with a real key, then hash it
-- In production, use the /api/setup endpoint to generate keys
-- ============================================================================

-- Individual events table (stores each incoming event with user details)
CREATE TABLE IF NOT EXISTS events (
    id           BIGSERIAL PRIMARY KEY,
    event_type   TEXT NOT NULL,
    name         TEXT,
    email        TEXT,
    phone        TEXT,
    metadata     JSONB DEFAULT '{}',
    event_time   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_type_time ON events (event_type, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_email ON events (email);

-- User roles for dashboard access control
CREATE TABLE IF NOT EXISTS user_roles (
    user_id    UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    role       TEXT NOT NULL DEFAULT 'viewer' CHECK (role IN ('admin', 'viewer')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

