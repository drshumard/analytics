# Future Work / Revisit-Later

Deferred items and "revisit when X" notes so we don't lose the reasoning. None of
these are needed now — they're documented so a later decision starts from context,
not from scratch.

> Multi-tenant reminder: the **analytics** funnel = `public` schema, **native** =
> `native` schema. Anything schema-level (indexes, etc.) must be applied to **both**.

---

## 1. Events-table scaling & indexing

**Status:** not needed now. Revisit when the `events` table crosses **~500k rows**
or AI Insights queries start brushing the 5-second cap.

**Measured baseline (2026-06-03, `public` schema):**
- `events`: **33,591 rows**, spanning 2026-03-17 → 2026-06-03 (~78 days)
- Growth: **~430 events/day (~157k/year)**
- → **1M events is ~6 years out** at the current pace.

**Why growth mostly doesn't matter:**
- The **dashboard** variant/metrics path (`computeDedupFromEvents`) only ever fetches
  a **~90-day window** of events — it is date-bounded, not table-bounded. Performance
  stays flat as the table grows. (The A/B toggle is client-side, so that 90-day fetch
  happens once per load.)
- Only the **AI Insights** tools that scan the whole table grow with it —
  `get_variant_funnel` (registration→variant map) and `describe_journey_data`.
  Postgres seq-scans are cheap (~1M rows < ~100ms vs a 5s cap), so this is a
  multi-million-row concern, not a hundred-thousand-row one.

**Thresholds:**
| Events rows | Action |
|---|---|
| < ~500k | Do nothing. |
| ~1M+ | *Consider* the indexes below — only if AI queries actually slow. |
| multi-M + slow | Add them. |

**When the time comes — indexes to add (run in BOTH `public` and `native`):**
```sql
-- speeds get_variant_funnel's registration→variant map
CREATE INDEX CONCURRENTLY idx_events_variant_regs
  ON events ((metadata->>'variant'))
  WHERE event_type = 'registrations';

-- speeds event_type + time-window scans (funnel/dedup, AI tools)
CREATE INDEX CONCURRENTLY idx_events_type_time
  ON events (event_type, event_time);

-- speeds email joins used in journey/variant attribution
CREATE INDEX CONCURRENTLY idx_events_email_lower
  ON events (lower(email));
```
`CREATE INDEX CONCURRENTLY` is non-blocking and changes **no data** — still not a data
migration. At ~34k rows today it would be pure ceremony; deliberately skipped.

---

## 2. A/B split test — real per-variant spend allocation ("Option C")

**Status:** declined for now (2026-06-02). Conversion-rate A/B comparison is live;
**cost**-per-variant is not, because FB reports `fb_spend` / `fb_link_clicks` at the
account/day level with no A/B dimension. In a single-variant view those two fields
(and any custom metric using them) are hidden.

**To make cost-per-variant real later:**
1. The reg **page** exposes its assigned variant to `shumard.js` (e.g. `window.__st_variant`).
2. `shumard.js` attaches it in `buildPayload()` → stored on `tracking_page_visits`
   (column or in the `attribution` jsonb). *(This would be a real migration if a new
   column is chosen.)*
3. Allocate spend by each variant's **reg-page VISIT share**, not registration share.

**The trap (why visit-share, not registration-share):** allocating spend by
*registration* share forces cost-per-registration to be identical for A and B by
construction (it just re-derives the blended number) and under-charges the
worse-converting variant. The split happens *after* the ad click, so both variants
share the same upstream spend per **visitor** — visits are the correct denominator.

**Non-issues already reasoned through:**
- `shumard.js` is on all pages, but only **reg-page** visits feed the denominator;
  other untagged pages are correctly ignored.
- A person's variant comes from their **registration** (first-reg-wins, by email),
  not pageviews — untagged pages never confuse it.
- Ad-blocker undercount (see backlog `p1-adblocker-naming`) cancels in the *share*
  as long as it's variant-neutral; only absolute visit counts look low.

---

## 2b. A/B — variant attribution via identity stitch (+ IP / session_id)

**Status:** deferred until A/B tagging is live (only ~5 test registrations carry a
variant as of 2026-06-03, so there's nothing to validate against yet). Today variant
attribution joins purchases→registration by **email → phone** (Phase 13c). This item
extends it to the CRM identity graph so a purchase made under a *different* email still
inherits the registration's variant.

**Data-model reality (why it must go through the stitch):** the `events` table has
**no IP and no session_id** — those live only on `tracking_contacts` /
`tracking_page_visits`. So "match variant by IP/session" is impossible directly on
events; it must bridge through the tracker. And the stitch engine **already** fuses
contacts by `session_id`, `email`, and IP (15-min window, with the `flagged_shared_ip`
>3-contacts guard). So the `merged_into` clusters **already encode session + email + IP
linkage** — variant-via-stitch = just propagate the variant across a merge cluster.

**Sound design (build when live):**
1. Fetch the merge alias graph once per metrics load (`tracking_contacts`:
   email, phone, merged_into) and build clusters of alias emails/phones.
2. In `computeDedupFromEvents`, when a registration with variant seeds the
   email/phone→variant maps, also stamp **every alias** in its cluster. Then a purchase
   under an aliased email inherits the variant. Mirror in `get_variant_funnel` (recursive
   CTE over `merged_into`).

**⚠ The IP caveat (important — the user's "stored first-seen IP, unbounded" idea):**
a longer/unbounded IP match is **more** collision-prone, not less. Households, offices,
CGNAT, and mobile carriers share IPs; matching variant by a stored IP with no time bound
would fuse strangers and **corrupt A/B integrity** (the worse-converting variant could
absorb another person's purchase). So:
- **session_id = safe** durable key (per-browser random; low collision) — already used
  by the stitch; Safari ITP caps cookie life ~7d.
- **IP = risky** — only via the **existing guarded stitch** (15-min window +
  `flagged_shared_ip` skip). Do **NOT** add an unbounded raw-IP variant join.
- Net: rely on the stitch's `merged_into` graph (which already blends session + guarded
  IP). Don't widen IP matching just for variant attribution.

**Hard limit either way:** linkage only exists if shumard.js actually captured the alias
email as a contact and the stitch merged it. A purchase email the tracker never saw
can't be linked — only the upstream fix (same checkout email) closes that.

## 3. A/B — pending activation (not a code task)

The A/B pipeline for the **main (analytics)** funnel is fully built and needs **no
migration** — `variant` rides in the existing `events.metadata` JSONB. It is dormant
until the **main registration webhook starts sending `variant: "A"|"B"`** (Option A,
upstream assignment on the reg page / Stealth).

- Verified 2026-06-03: registrations carrying a `variant` key = **0** (not live yet).
- It is **forward-looking only**: anyone who first registered before tagging goes live
  stays in the `undetected` bucket permanently (first-registration-wins).
- Native funnel already carries `variant` and uses the same path.

---

*See also the `crm-tracking-port` memory for full phase history and the live data model.*
