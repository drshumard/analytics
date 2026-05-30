-- ============================================================================
-- Crowded-IP guard. When an IP is tied to more than 3 distinct (non-merged)
-- tracking contacts it's a shared address (office NAT, mobile CGNAT, VPN) — the
-- app flags those contacts and skips IP-stitching them, so strangers behind one
-- IP are never fused. The CRM shows a red "SHARED IP" badge for flagged contacts.
--
-- Run ONCE against the analytics funnel (public schema), after the earlier
-- tracking + crm_people migrations. For native, recreate under `native.`.
-- ============================================================================

ALTER TABLE public.tracking_contacts
    ADD COLUMN IF NOT EXISTS flagged_shared_ip BOOLEAN NOT NULL DEFAULT false;

-- Recreate crm_people to expose the flag as is_shared_ip. We DROP first because
-- CREATE OR REPLACE VIEW can't insert a column in the middle of the column list
-- (only append at the end). Nothing depends on this view, so dropping is safe.
DROP VIEW IF EXISTS public.crm_people;
CREATE VIEW public.crm_people AS
WITH ev AS (
    SELECT
        lower(email)                                                          AS email_key,
        (array_agg(name  ORDER BY event_time DESC) FILTER (WHERE name  IS NOT NULL))[1] AS ev_name,
        (array_agg(phone ORDER BY event_time DESC) FILTER (WHERE phone IS NOT NULL))[1] AS ev_phone,
        bool_or(event_type = 'registrations') AS has_registration,
        bool_or(event_type = 'attended')      AS has_attended,
        bool_or(event_type = 'replays')       AS has_replay,
        bool_or(event_type = 'viewedcta')     AS has_viewedcta,
        bool_or(event_type = 'clickedcta')    AS has_clickedcta,
        bool_or(event_type = 'purchases')     AS has_purchase,
        min(event_time)                       AS first_event,
        max(event_time)                       AS last_event,
        count(*)                              AS event_count
    FROM public.events
    WHERE email IS NOT NULL AND btrim(email) <> ''
    GROUP BY lower(email)
),
tc AS (
    SELECT DISTINCT ON (lower(email))
        contact_id, lower(email) AS email_key, email, name, phone,
        first_name, last_name, attribution, tags, flagged_shared_ip, created_at, updated_at
    FROM public.tracking_contacts
    WHERE merged_into IS NULL AND email IS NOT NULL AND btrim(email) <> ''
    ORDER BY lower(email), updated_at DESC
)
SELECT
    coalesce(tc.email_key, ev.email_key)                       AS email_key,
    coalesce(tc.email, ev.email_key)                           AS email,
    tc.contact_id,
    (tc.contact_id IS NOT NULL)                                AS is_tracked,
    coalesce(tc.flagged_shared_ip, false)                      AS is_shared_ip,
    coalesce(tc.name, ev.ev_name)                              AS name,
    coalesce(tc.phone, ev.ev_phone)                            AS phone,
    tc.first_name,
    tc.last_name,
    coalesce(tc.attribution, '{}'::jsonb)                      AS attribution,
    coalesce(tc.tags, '{}'::text[])                            AS tags,
    coalesce(ev.has_registration, false)                       AS has_registration,
    coalesce(ev.has_attended, false)                           AS has_attended,
    coalesce(ev.has_replay, false)                             AS has_replay,
    coalesce(ev.has_viewedcta, false)                          AS has_viewedcta,
    coalesce(ev.has_clickedcta, false)                         AS has_clickedcta,
    coalesce(ev.has_purchase, false)                           AS has_purchase,
    coalesce(ev.event_count, 0)                                AS event_count,
    CASE
        WHEN coalesce(ev.has_purchase, false)     THEN 'purchase'
        WHEN coalesce(ev.has_clickedcta, false)   THEN 'clickedcta'
        WHEN coalesce(ev.has_viewedcta, false)    THEN 'viewedcta'
        WHEN coalesce(ev.has_replay, false)       THEN 'replay'
        WHEN coalesce(ev.has_attended, false)     THEN 'attended'
        WHEN coalesce(ev.has_registration, false) THEN 'registration'
        ELSE 'lead'
    END                                                        AS stage,
    least(tc.created_at, ev.first_event)                       AS first_seen,
    greatest(tc.updated_at, ev.last_event)                     AS last_activity
FROM tc
FULL OUTER JOIN ev ON tc.email_key = ev.email_key;

NOTIFY pgrst, 'reload schema';
