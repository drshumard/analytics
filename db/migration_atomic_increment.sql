-- Migration: Create atomic increment function for daily_metrics
-- Run this in Supabase SQL Editor
-- Prevents race conditions when multiple webhooks hit the same date/field concurrently

CREATE OR REPLACE FUNCTION increment_field(p_date DATE, p_field TEXT, p_amount INTEGER)
RETURNS VOID AS $$
BEGIN
    EXECUTE format(
        'UPDATE daily_metrics SET %I = %I + $1 WHERE date = $2',
        p_field, p_field
    ) USING p_amount, p_date;
END;
$$ LANGUAGE plpgsql;
