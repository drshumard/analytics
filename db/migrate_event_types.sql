-- Migration: per-funnel event-type config — makes the funnel's EVENT columns
-- customizable, not just purchase sources. The webinar stages become seed data
-- for the existing funnels instead of a hardcoded skeleton; new funnels define
-- their own events (e.g. the eboov funnel's stages).
-- Purely additive: current prod code never reads these; the config-driven code
-- ships on feat/custom-event-columns after review.
--
-- Applied via ai_run_sql_write (one statement per call).

CREATE TABLE IF NOT EXISTS public.event_types (
    column_name   TEXT PRIMARY KEY CHECK (column_name ~ '^[a-z][a-z0-9_]{1,28}$'),
    display_label TEXT NOT NULL,
    sort_order    INTEGER NOT NULL DEFAULT 0,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS native.event_types (
    column_name   TEXT PRIMARY KEY CHECK (column_name ~ '^[a-z][a-z0-9_]{1,28}$'),
    display_label TEXT NOT NULL,
    sort_order    INTEGER NOT NULL DEFAULT 0,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.event_types (column_name, display_label, sort_order) VALUES
    ('registrations', 'Registrations', 10),
    ('attended',      'Attended',      20),
    ('replays',       'Replays',       30),
    ('viewedcta',     'Viewed CTA',    40),
    ('clickedcta',    'Clicked CTA',   50)
ON CONFLICT (column_name) DO NOTHING;

INSERT INTO native.event_types (column_name, display_label, sort_order)
SELECT column_name, display_label, sort_order FROM public.event_types
ON CONFLICT (column_name) DO NOTHING;

-- Whether the funnel uses the webinar stay-time milestones (stayeduntil →
-- stayed_45/60/80). True for the existing webinar funnels; new funnels opt in.
ALTER TABLE public.funnels ADD COLUMN IF NOT EXISTS webinar_milestones BOOLEAN NOT NULL DEFAULT TRUE;

NOTIFY pgrst, 'reload schema';
