-- ============================================================================
-- Migration: Add ai_memory table for cross-chat memory in AI Insights
--
-- Stores durable facts the AI has been told to remember (campaigns, business
-- context, definitions, etc.). Auto-included in every chat's system prompt.
--
-- Per-funnel: run this in BOTH schemas. For native, prefix the table refs
-- with `native.` or `SET search_path = native;` first.
--
-- Run this in the Supabase SQL Editor.
-- ============================================================================

CREATE TABLE IF NOT EXISTS ai_memory (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    key         TEXT NOT NULL,
    value       TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, key)
);

CREATE INDEX IF NOT EXISTS idx_ai_memory_user
    ON ai_memory (user_id, updated_at DESC);

-- Reuse the existing update_updated_at() trigger function
CREATE TRIGGER trg_ai_memory_updated
    BEFORE UPDATE ON ai_memory
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

ALTER TABLE ai_memory ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can manage own memory"
    ON ai_memory
    FOR ALL
    USING (auth.uid() = user_id)
    WITH CHECK (auth.uid() = user_id);

-- ============================================================================
-- For the `native` funnel schema, run this version instead:
-- ============================================================================
--   SET search_path = native;
--   <repeat the CREATE TABLE / INDEX / TRIGGER / RLS / POLICY blocks above>
-- ============================================================================
