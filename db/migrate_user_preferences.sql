-- Migration: Add preferences JSONB column to user_roles
-- Run this in Supabase SQL Editor

ALTER TABLE user_roles
    ADD COLUMN IF NOT EXISTS preferences JSONB NOT NULL DEFAULT '{}';
