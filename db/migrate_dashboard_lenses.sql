-- Migration: Create dashboard_lenses table
-- Run this in Supabase SQL Editor

CREATE TABLE IF NOT EXISTS dashboard_lenses (
    id          TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    name        TEXT NOT NULL,
    metrics     JSONB NOT NULL DEFAULT '[]',
    sort_order  INTEGER NOT NULL DEFAULT 0,
    created_by  UUID REFERENCES auth.users(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed a default "All Metrics" lens
INSERT INTO dashboard_lenses (id, name, metrics, sort_order)
VALUES (
    'default-all',
    'All Metrics',
    '["fb_spend","registrations","attended","replays","viewedcta","clickedcta","purchases"]',
    0
) ON CONFLICT (id) DO NOTHING;
