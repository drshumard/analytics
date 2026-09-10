# Dr Shumard Analytics — Product & Architecture Document

> **Purpose of this file:** the complete map of the product — foundation, features,
> data flows, conventions, and operational rules — so a handover or a future
> working session can find the right place to make a change **without reading the
> codebase end-to-end**. Keep it updated when architecture or features change.
> Last full revision: **2026-09-10**.

---

## 1. What this product is

A multi-funnel marketing analytics platform + lightweight CRM, originally built for
Dr Shumard's Facebook-ads webinar funnel and since generalized so that **new funnels
(other businesses, other event models) are provisioned from the admin UI** —
schema, dashboard, webhook key, AI tools and all.

Per funnel it provides:

- **Daily metrics dashboard** — ad spend + funnel event counts per day, custom
  metrics (formula columns), saved column sets ("lenses"), summary cards with
  sparklines, A/B split-test view, board/list views, manual day editor (admin).
- **AI Insights chat** — a Claude-powered analyst with ~15 tools over the funnel's
  data (metrics, rollups, journeys, segments, live Facebook Ads API, GHL pipelines
  for the main funnel), with per-user durable memory.
- **AI Worker chat** (admin-only) — the "do-er": records missed sales/events with
  correct backdating, fixes daily numbers, links duplicate identities, audited in
  `worker_audit_log`.
- **Webhook ingestion** — n8n (or anything) POSTs events; identity-deduped,
  counted, and finalized nightly.
- **Facebook spend sync** — per-funnel ad account, every 30 min + a 4 AM final
  re-fetch of yesterday.
- Main (Shumard) funnel only: **CRM/people view, email report, first-party
  tracking (shumard.js), GHL pipeline joins, patient-avatar demographics**.

**Live funnels (2026-09):** `analytics` (Main FB-ads webinar funnel, schema
`public`), `native` (Taboola, schema `native`), `eboov` (client funnel, schema
`eboov`, custom events).

---

## 2. System architecture

```
 funnel pages ── shumard.js ──▶ /api/sg/*            ┐
 n8n / GHL ─── webhooks ─────▶ /api/metrics/increment ├─▶ Express monolith (server.js, ~7.6k lines)
 Meta Graph API ◀── spend sync + get_fb_ads ──────────┤        │
 Anthropic API ◀── AI Insights / AI Worker ───────────┘        ▼
                                                    Supabase Postgres (via PostgREST)
                                                      ├─ public  schema = analytics funnel (+ shared control tables)
                                                      ├─ native  schema = native funnel
                                                      └─ <key>   schema = each provisioned funnel
 React dashboard (frontend/App.jsx, single file ~5.7k lines, Vite build → public/)
```

- **One Express server** (`server.js`) serves API + the built frontend from
  `public/`. No ORM: `@supabase/supabase-js` clients, one per schema
  (`clientForSchema` / `clientFor(funnel)`).
- **Funnel resolution:** every request carries `X-Funnel` (default `analytics`).
  Webhooks resolve the funnel from their API key instead.
- **The DB is reached only through PostgREST** (Supabase REST). There is no direct
  pg connection from the app. Consequences in §10.

### 2.1 The config registry (the heart of multi-funnel)

Nothing funnel- or column-shaped is hardcoded anymore. Two config sources drive
**everything** (server maps, dedup, finalize, dashboard columns, editors, AI
prompts/tools):

| Table | Scope | Drives |
|---|---|---|
| `public.funnels` | one row per funnel: `key`, `schema_name`, `label`, `brand`, `brand_context`, `fb_ad_account_id`, `webinar_milestones`, `is_active` | funnel list, workspace switcher, per-funnel FB sync, AI branding |
| `<schema>.event_types` | `column_name` (= webhook `field` = events.event_type = daily_metrics column), `display_label`, `sort_order` | the funnel's event columns end-to-end |
| `<schema>.purchase_sources` | `source_label` (webhook `source` value) → `column_name` → `display_label` | purchase routing + purchase columns end-to-end |

Server loads this into `funnelConfig` (refreshed every 60 s + forced on admin
writes; hardcoded fallbacks if the registry is unreachable). Getters:
`allowedFunnels() funnelExists() brandFor() fbAccountFor() purchaseSources()
purchaseSourceMap() purchaseSubCols() eventTypes() eventCols() hasMilestones()
milestoneCols() hasStandardEvents()`. Frontend gets the same via
`/api/me/funnels` → `meta[]` and rebuilds `MK`/`COL_LABELS` in place
(`applyFunnelColumns`).

**Universal columns** every funnel has: `fb_spend`, `fb_link_clicks`,
`purchases` (+ per-source `purchases_*`), `overrides`, `variant_splits`,
`finalized_at`. **The webinar stages** (registrations/attended/replays/
viewedcta/clickedcta) and stay-time milestones (`stayed_45/60/80`,
`webinar_milestones` flag) are just the seeded preset for analytics/native —
custom funnels define their own events (e.g. eboov: `registered`).

`hasStandardEvents(funnel)` gates webinar-specific behavior: the hand-written AI
prompt stages/metrics, GHL + patient-avatar tools, webinar-day registration
dedup semantics. Custom funnels get generated prompt text and generic tools only.

---

## 3. Data model (per funnel schema)

| Table | Role |
|---|---|
| `daily_metrics` | one row per LA calendar day; denormalized counters per column; `overrides` jsonb (admin display overrides, sit ON TOP of automated data); `variant_splits` jsonb (persisted A/B splits at finalize); `finalized_at` |
| `events` | source of truth. One row per funnel action: `event_type`, `name/email/phone`, `metadata` jsonb, `event_time` (UTC), `execution_id` (n8n) |
| `event_types`, `purchase_sources` | column config (§2.1) |
| `custom_metrics` | formula columns (`clickedcta / viewedcta * 100` style; variables = any MK column or other custom metric) |
| `dashboard_lenses` | saved column sets; `default-all` is the seeded one |
| `user_roles` | `admin`/`viewer` per funnel (absent row = viewer) |
| `chat_conversations`, `ai_memory` | AI Insights history + per-user durable facts (`remember` tool) |
| `worker_audit_log` | every AI-worker write |
| `app_settings` | misc funnel settings (A/B test start, sales-page URL maps…) |
| `webhook_log` | FB-sync errors etc. |
| `identity_links` | alias email → canonical email (counts merged people correctly) |
| `contact_demographics` | cached patient-intake demographics (avatar tool) |

Main funnel (`public`) additionally: `tracking_contacts`, `tracking_page_visits`,
`tracking_tag_events` (shumard.js), `crm_people` view (one row per person joining
tracking + events), and shared control tables `api_keys` (webhook keys, sha256
`key_hash`, `funnel` column FK→funnels, optional `scopes`), `user_funnel_access`
(user × funnel, FK→funnels).

---

## 4. The metrics pipeline (how a number gets on the dashboard)

1. **Ingest** — `POST /api/metrics/increment` (auth: `X-API-Key`; the key's row
   determines the funnel). Body: `{ "field": <event or 'purchases' or
   'stayeduntil' or 'fb_spend'...>, "email", "name", "phone", ... }`.
   - Valid `field`s = the funnel's `event_types` + `purchases` (+ `stayeduntil`
     if milestones). Purchases route by `source` via `purchase_sources`
     (unknown source → `purchases_fb`). `'Post Webinar'` may be sent explicitly.
   - **Ingest dedup:** registrations = one per email per *webinar day*
     (`metadata.webinar_datetime_utc`); purchases = one per email per LA day
     (36 h lookback); everything else = 5-minute window (webhook-retry guard).
   - Event row is written FIRST, then the counter via `increment_field` RPC
     (atomic, column-dynamic).
2. **Display (recent days)** — the dashboard does NOT show raw counters: it counts
   **distinct people per LA day** from `events` (identity key: email → phone →
   name, lowercased, alias-merged via `identity_links` + tracking stitches).
   Events with no identity are stored but never displayed. Cached in-memory per
   funnel (`cache.byFunnel`, ~60 s metrics TTL; past days cached forever).
3. **Finalize (nightly 4:05 AM LA cron, every funnel)** — yesterday's dedup counts
   are written into the canonical `daily_metrics` columns + `variant_splits`,
   `finalized_at` set. Finalized days render from columns (durable even if events
   are pruned). Admin `overrides` always win at display time.
4. **Post-webinar auto-detect** (standard funnels): a Paid Ads / Sales A / Sales B
   purchase 12 h+ after the buyer's last attended/replay is re-routed to
   `purchases_postwebinar`. Explicit `source:"Post Webinar"` skips detection.
   Retargeting/Promo/AI-bot sources are never re-routed.

**Timezone rules:** the business runs on **America/Los_Angeles**; days bucket by
LA date. `events.event_time` is UTC. Live webhook events are stamped at arrival
(no timestamp field accepted) — timezone bugs can only enter via manual backfills
(the app convention reads zoneless datetimes as LA; **eboov's CRM exports are
UTC** — shift accordingly).

**A/B variants** (standard funnels): `metadata.variant` on registrations
(Stealth-stamped), inherited forward-only to downstream events via email/phone;
`app_settings` A/B start cutoff; dashboard All/A/B/Undetected toggle;
`get_variant_funnel` AI tool does the math correctly.

---

## 5. Facebook spend sync

`fb-sync.js` + crons in `server.js`. For **every registry funnel with
`fb_ad_account_id`**: account-level Graph insights (`spend`,
`inline_link_clicks`) written to that funnel's `daily_metrics`
(`fb_spend`, `fb_link_clicks`) — absolute overwrite, not increment.
- Every 30 min (today) + 4:00 AM LA (yesterday's settled numbers).
- `POST /api/refresh-date {date}` re-pulls any single date (funnel via
  `X-Funnel`) — the backfill tool.
- One system-user token (`FB_ACCESS_TOKEN` env) must have access to every linked
  ad account (same Business Manager). Account-level = *everything in that ad
  account counts*; keep one ad account per funnel.
- `get_fb_ads` AI tool: live campaign/adset/ad + creatives + Meta-pixel
  conversions + breakdowns, per the funnel's linked account.

---

## 6. Auth & access

- **Supabase Auth** (email+password, Google OAuth). Frontend uses supabase-js;
  server verifies JWTs (`requireAuth`) → funnel access via `user_funnel_access`
  → role via `<schema>.user_roles` (`requireAdmin`).
- **Password flows** (no reliable email delivery — Resend domain is
  `analytics.drshumard.com`, Microsoft recipients may junk it):
  - Login screen has **Forgot password?** (modal → reset email).
  - Recovery links open an in-app **set-a-new-password** screen.
  - Admin: **Password reset link** (account menu) mints a single-use ~1 h link to
    hand over directly — no email needed.
- **Funnel access UI** (account menu → *Funnel access*, admin): per-funnel user
  list with access checkbox + viewer/admin role; **invite by email** creates the
  auth user, grants access, returns a set-password link. Self-lockout guarded.
- **Webhook auth:** `X-API-Key` → env keys (`API_KEY`=analytics,
  `NATIVE_API_KEY`) or `public.api_keys` (sha256 hash, per-funnel, minted at
  provisioning / by hand).

---

## 7. Admin self-service (account menu)

| Item | What it does |
|---|---|
| **New funnel** | Full provisioning: name → auto-slug key; optional FB ad account (validated live via Graph API, shows account name); events = standard-webinar preset **or** custom list; purchase-source picker (+ new ones). Executes `db/funnel_template.sql` statement-by-statement via the `ai_run_sql_write` RPC, ALTERs in chosen event/source columns, seeds config + default lens, grants creator admin, mints a **once-shown webhook API key**. Failure auto-drops the schema. **One manual step:** add the schema to Supabase Dashboard → Data API → *Exposed schemas* (automatic if `SUPABASE_ACCESS_TOKEN` env is set — Management API). |
| **Columns** | Add event columns (`field` name + label) or purchase sources to the current funnel — DB column + config + lens updated; webhooks accept immediately. |
| **Funnel access** | §6. |
| **Password reset link** | §6. |
| Also: New entry / day editor, Edit Columns + lenses, custom metrics, A/B settings, finalize, cache clear. |

**AI Worker tools** (admin chat): `add_sale` / `add_event` (correct backdating +
counters + refinalize + cache), `set_metric_override`, `link_identity`,
`run_sql_write` (last resort), `finalize_day`, `clear_cache`, audit log.

---

## 8. AI Insights (per-funnel analyst)

- `POST /api/insights/chat` — Claude with tool loop (10 iters, ~110 s budget,
  prompt caching, tool results capped ~100 KB with truncation markers;
  `get_metrics` refuses >6-month ranges → rollups/SQL).
- Tools: `get_metrics(_rollup)`, `compare_periods`, `get_event_counts`,
  `run_sql` (read-only RPC, 500-row cap), `remember/forget`, journey suite
  (funnel/variant/contact/segment/email-report/sales-pages — standard funnels),
  `get_fb_ads`, `get_ghl_pipeline_status` + `get_customer_avatar`
  (Shumard-only, withheld on custom funnels), code execution, GHL MCP.
- System prompt is built per funnel from the registry (stages, key metrics,
  purchase sources, branding). Frontend renders ```stats / ```chart fences.

---

## 9. Infra & operations

- **Prod:** single server, SSH alias **`LWD`** (use `/usr/bin/ssh` — plain `ssh`
  is shadowed by a broken wrapper). App at `/var/www/analytics`, PM2 app name
  `analytics`, port **5401**, nginx in front, domain
  `https://analytics.drshumard.com`.
- **Deploy:** push `main` → `/usr/bin/ssh LWD "cd /var/www/analytics &&
  ./deploy.sh"` (pulls, builds frontend, zero-downtime PM2 reload). **Process
  rule: new features/architecture go on a branch, verified locally
  (`PORT=5402 node server.js` against the live DB is safe), demoed, deployed
  only on explicit approval. Small fixes may ship directly.**
- **Crons (LA):** :00/:30 FB sync per funnel · 4:00 yesterday's final FB numbers ·
  4:05 finalize per funnel. Registry refresh every 60 s.
- **Migrations:** `db/*.sql`, applied manually. Preferred executor: the
  `ai_run_sql_write` RPC over HTTPS, one statement per call (DO blocks fine;
  `ALTER ROLE authenticator` is NOT permitted). Direct pg alternative: host
  `db.oxsuecigbzljwvpggcws.supabase.co` (IPv6-only; works from LWD, sometimes
  not from the Mac; `.env DATABASE_URL` host is wrong — swap it).
- Frontend build artifacts in `public/` are git-tracked; deploy.sh rebuilds
  server-side anyway.

---

## 10. Operational rules & sharp edges (read before touching prod)

1. **NEVER `DROP SCHEMA` while it is listed in Supabase "Exposed schemas"** — it
   bricks the *entire* REST API (PGRST002 503s, including the SQL RPCs). Order:
   remove from Exposed schemas first (verify: `Accept-Profile: <schema>` returns
   PGRST106), then drop + delete `funnels` / `user_funnel_access` / `api_keys`
   rows. Emergency unbrick: recreate an empty stub schema via direct pg from LWD.
2. **Dashboard counts people, not webhook hits.** Raw `events` row counts can
   overstate (e.g. duplicate n8n senders until 2026-08-25 were absorbed by
   dedup); no-contact events never display; finalize reconciles counters nightly.
3. **PostgREST exposure** is required before a new funnel's dashboard works
   (config reads fall back to the `ai_run_sql` RPC meanwhile, so the switcher/
   columns already look right).
4. **Timezones:** LA everywhere for bucketing; manual imports must convert
   (eboov CRM = UTC).
5. `ai_run_sql` caps at 500 rows / 5 s; `ai_run_sql_write` 10 s, one statement.
6. Registration webhooks may carry `webinar_datetime_utc` (quirky text format,
   UTC) — events bucket to the *webinar's* LA day, not arrival day.
7. Purchase dedup blocks a genuine second same-day purchase by the same email —
   accepted tradeoff; `add_sale skip_dedup` for real doubles.
8. Env keys of note: `SUPABASE_URL/SERVICE_KEY/ANON_KEY`, `API_KEY`,
   `NATIVE_API_KEY`, `FB_ACCESS_TOKEN`, `FB_AD_ACCOUNT_ID` (legacy fallback for
   analytics), `ANTHROPIC_API_KEY`, `GHL_MCP_TOKEN/LOCATION_ID/PIPELINE`,
   optional `SUPABASE_ACCESS_TOKEN` (enables automatic schema exposure).

---

## 11. Common tasks — recipes

- **New funnel** → admin UI (§7). Nothing manual except the exposure click.
- **New purchase source / event column** → admin UI → Columns. No migrations.
- **Give someone access / onboard someone new** → admin UI → Funnel access.
- **Missed sale/event** → AI Worker chat (`add_sale` / `add_event`) — it
  backdates, updates counters, refinalizes, clears cache.
- **Historic import** (CSV/CRM dump) → insert `events` rows (mind timezone),
  set `daily_metrics` counters to distinct counts, clear cache. See
  session pattern: metadata `{"imported": "<label>"}` for traceability.
- **Backfill FB spend for a date** → `POST /api/refresh-date` with `X-Funnel`.
- **Wire n8n for a funnel** → HTTP Request node → POST
  `https://analytics.drshumard.com/api/metrics/increment`, headers
  `X-API-Key: <funnel key>` + JSON body `{"field": "<event>", "email": …,
  "name": …, "phone": …}`; purchases add `"source"`. Server stamps arrival time.
- **Remove a funnel** → follow §10.1 order exactly.

---

## 12. File map

| Path | What lives there |
|---|---|
| `server.js` (~7.6k) | everything server: config registry (~line 30-200), auth (~540-700), webhook ingest (~840-1100), dedup engine (~1560-1750), finalize (~1850-1980), dashboard API (~2100-2300), admin endpoints (reset-link, funnel-users/invite, provisioning, columns ~2500-2900), FB cron wiring (~2850-3000), insights metrics assembly (~3300-3700), AI tools + chat (~4300-5450), AI worker (~5450-6100), static serving + boot (~7300+) |
| `frontend/App.jsx` (~5.6k) | the whole dashboard: config (`applyFunnelColumns` ~line 280-360), api client (~60-260), auth screens + recovery (~1490-1700), main dashboard component, admin modals (NewFunnel/Columns/FunnelAccess/ResetLink ~2700-3100), day editor, CRM, insights chat UI |
| `fb-sync.js` | Graph insights fetch + per-schema write |
| `tracking/shumard.js` | first-party tracker (main site) |
| `hyros.js` | legacy import tooling |
| `db/` | schema references + migrations; `funnel_template.sql` = provisioning template (`-- @statement` separated, `{{schema}}` placeholder) |
| `APIs.md` | endpoint reference (webhook contract details) |
| `DEPLOYMENT.md` | server setup notes |
| `README.md` | short intro (predates registry era — this file is canonical) |

## 13. Recent architecture history (why things look this way)

- **2026-09-09/10:** funnels + event columns + purchase sources became
  registry-driven and admin-provisionable (previously hardcoded; webinar stages
  were the only shape). Funnel access UI added. eboov funnel launched (custom
  `registered` event, UTC-source CRM import, own FB ad account).
- **2026-08/09:** auth overhaul (recovery screen, forgot-password modal, admin
  reset links — email delivery unreliable), AI insights hardening (tool-result
  caps, range guards), mobile date-picker fixes, Retargeting/Promo sources,
  `get_fb_ads` full-hierarchy tool.
- **2026-08-25:** duplicate n8n sender era ended (see §10.2).
