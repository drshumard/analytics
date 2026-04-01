-- Migration: Add overrides JSONB column to daily_metrics
-- Run this in Supabase SQL Editor
-- When admin manually edits a metric, the value is stored here and takes precedence
-- Example: { "registrations": 150, "purchases": 10 }

ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS overrides JSONB DEFAULT '{}';
