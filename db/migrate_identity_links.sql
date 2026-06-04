-- Manual identity links: an admin says "events under alias_email are the same person
-- as canonical_email" (a registrant). Used to attribute a sale that came in under a
-- different email/phone (or none) to the right registrant — so it counts in the
-- correct A/B variant. Deterministic; complements the automatic email/phone matching.
--
-- Multi-tenant: run for EACH funnel schema (analytics = public, native = native).

-- ── analytics funnel (public schema) ──
CREATE TABLE IF NOT EXISTS public.identity_links (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    alias_email     text NOT NULL,          -- lowercased; the "other" email (e.g. the purchase)
    canonical_email text NOT NULL,          -- lowercased; the registrant this maps to
    note            text,
    created_by      uuid,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (alias_email)
);

-- ── native funnel (native schema) ──
CREATE TABLE IF NOT EXISTS native.identity_links (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    alias_email     text NOT NULL,
    canonical_email text NOT NULL,
    note            text,
    created_by      uuid,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (alias_email)
);
