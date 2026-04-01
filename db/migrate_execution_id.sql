-- Migration: Add execution_id column to events table
-- Stores the n8n execution ID for easy traceability

ALTER TABLE events ADD COLUMN IF NOT EXISTS execution_id TEXT;
CREATE INDEX IF NOT EXISTS idx_events_execution_id ON events (execution_id);
