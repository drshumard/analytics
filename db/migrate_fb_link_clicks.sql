-- Migration: Add fb_link_clicks column to daily_metrics
-- This stores Facebook's inline_link_clicks metric (people who clicked the ad link to the registration page)
-- Displayed as "Total Registration Page Visited" in the dashboard

ALTER TABLE daily_metrics ADD COLUMN IF NOT EXISTS fb_link_clicks INTEGER NOT NULL DEFAULT 0;
