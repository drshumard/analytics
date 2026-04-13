-- ============================================================================
-- Migration: Add purchase source breakdown columns to daily_metrics
-- Run this in Supabase SQL Editor
-- ============================================================================

-- Add 5 new purchase source columns
ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS purchases_fb            INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS purchases_native        INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS purchases_youtube       INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS purchases_aibot         INTEGER NOT NULL DEFAULT 0;
ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS purchases_postwebinar   INTEGER NOT NULL DEFAULT 0;

-- Backfill: copy existing purchases count into purchases_fb (default source)
-- This ensures historical data shows up under "FB Purchases" instead of all zeros
UPDATE daily_metrics SET purchases_fb = purchases WHERE purchases > 0;

-- Add overrides column if not present (may already exist from earlier migration)
-- ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS overrides JSONB DEFAULT '{}';
