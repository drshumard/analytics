-- Per-funnel Facebook campaign scope. NULL/empty = the funnel counts its whole
-- ad account, minus campaigns claimed by other funnels on the same account.
ALTER TABLE public.funnels ADD COLUMN IF NOT EXISTS fb_campaign_ids text[];
