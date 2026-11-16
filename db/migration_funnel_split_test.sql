-- Per-funnel split-test switch (shows the A/B variant toggle + Split test menu).
-- The two funnels that already had the split-test UI stay on.
ALTER TABLE public.funnels ADD COLUMN IF NOT EXISTS split_test boolean NOT NULL DEFAULT false;
UPDATE public.funnels SET split_test = true WHERE key IN ('analytics', 'native');
