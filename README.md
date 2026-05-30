# Dr Shumard Analytics

Production analytics **and CRM** for a Facebook‑ads webinar funnel. It combines:

- a **daily metrics dashboard** (FB spend + funnel counts, custom metrics, split‑test variants),
- a **CRM** with one record per person and their full journey (Hyros‑style),
- an **Email Report** for email‑click → purchase performance,
- an **AI Insights** chat analyst (Claude) that can query all of the above, and
- **shumard.js**, a first‑party tracking script that captures the on‑site journey and stitches identities.

---

## Architecture

```
  Client funnel pages ──shumard.js──▶ /api/track/*  ─┐
                                                     ├─▶ Express (server.js) ─▶ Supabase / Postgres
  Zapier / Make / Stealth ─webhooks─▶ /api/metrics/* ┘        │                  ├─ public  = "analytics" funnel
                                                              │                  └─ native  = "native"   funnel
                                              React dashboard ─┘  (CRM · Email Report · AI Insights · daily metrics)
```

**Multi‑funnel:** the `analytics` funnel lives in Postgres schema **`public`**, the `native` funnel in schema **`native`**. Every request resolves one funnel via the **`X-Funnel`** header (defaults to `analytics`); the server uses a schema‑scoped Supabase client per funnel.

**Two data sources, joined by email:**
1. **Webhooks** (Zapier / Make / Stealth) → `/api/metrics/*` → per‑person rows in `events` + denormalized counts in `daily_metrics`.
2. **shumard.js** on the funnel pages → `/api/track/*` → `tracking_*` tables (identities, pageviews, tag fires), stitched into one person.

The **CRM** joins these by email at read time (the `crm_people` view); **AI Insights** reads everything through tools. The webhook → `events` → `daily_metrics` pipeline is independent of tracking — tracking never writes to it.

---

## Funnel model

Stage order: **Lead → Registration → Attended → Replay → Saw CTA → Clicked CTA → Purchase.**

| Layer | Where | Notes |
|---|---|---|
| Counts | `daily_metrics` | `fb_spend`, `fb_link_clicks`, `registrations`, `attended`, `replays`, `viewedcta`, `clickedcta`, purchases (by source), `stayed_45/60/80` |
| Per‑person events | `events` | `event_type` ∈ `registrations`/`attended`/`replays`/`viewedcta`/`clickedcta`/`purchases`/`stayeduntil`; `name`/`email`/`phone`/`metadata` |
| Identities + journey | `tracking_contacts` / `tracking_page_visits` / `tracking_tag_events` | from shumard.js |
| CRM people | `crm_people` view | tracking ⋈ events, one row per email |

**Purchase sources** (set via `metadata.source` on the purchase webhook): `Paid Ads → purchases_fb`, `Native → purchases_native`, `Youtube → purchases_youtube`, `AI Bot → purchases_aibot`, `CPA Traffic → purchases_cpa`, and **Post Webinar** (auto‑detected: a Paid‑Ads buyer who attended 12h+ earlier → `purchases_postwebinar`).

---

## Quick start (development)

### 1. Supabase
Run, in the SQL editor:
- `db/schema.sql` — base `public` (analytics) tables
- `db/migrate_tracking_crm.sql` — tracking tables
- `db/migrate_crm_views.sql` — the `crm_people` view
- `db/migrate_shared_ip_flag.sql` — shared‑IP flag + view refresh
- `db/migrate_ai_run_sql.sql`, `db/migrate_ai_memory.sql` — AI Insights SQL tool + memory
- (the other `db/migrate_*.sql` for purchase sources, finalize, etc.)
- For the **native** funnel: `db/native_schema.sql`, then re‑run the tracking/crm/ai migrations under `SET search_path = native;`, and expose `native` under Supabase → Settings → API → Exposed schemas.

### 2. `.env`
```
PORT=5401
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_ANON_KEY=eyJ...
SUPABASE_SERVICE_KEY=eyJ...
API_KEY=<analytics webhook key>          # node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
NATIVE_API_KEY=<native webhook key>      # optional, only if using the native funnel
ANTHROPIC_API_KEY=sk-ant-...             # for AI Insights
CORS_ORIGINS=https://analytics.drshumard.com
TRACKING_PUBLIC_URL=https://analytics.drshumard.com   # baked into the served shumard.js
DATABASE_URL=postgres://...              # only for psql-based migrations (optional)
FB_ACCESS_TOKEN=... / FB_AD_ACCOUNT_ID=act_... / FB_API_VERSION=v21.0   # optional, see facebook.md
```

### 3. Run
```bash
npm install
npm run dev:api      # Express backend on $PORT (node --watch server.js)
npm run dev:ui       # Vite dev server (proxies /api → backend); open this for the dashboard
# or, production-style: npm run build && npm start   (serves the built public/ from Express)
```

### 4. Smoke‑test
```bash
API_KEY=your-key node scripts/test-webhook.js        # metrics webhooks
node scripts/test-tracking.js                         # tracking + identity stitching (server must be running)
```

---

## Tracking — shumard.js

Embed on **every funnel page** (it's served by Express, no build step):
```html
<script src="https://analytics.drshumard.com/shumard.js"></script>
```
- **Auto‑captures**: pageviews, attribution (UTMs, `fbclid`, `_fbc`/`_fbp`, etc.), and form fields (email/phone/name) → fires `lead` on field blur and `registration` on submit.
- **`?tag=` (optional)**: load as `shumard.js?tag=attended` (or `replay`, `saw-cta`, …) to mark a stage from the page itself. Funnel stages also come from your webhooks, so tags are supplementary.
- **Email links**: `…/checkout?he={{contact.email}}&el=<source>&htrafficsource=email` →
  - `he` identifies the contact immediately (so the click is tied to a known person, any device),
  - `el` → attribution `source`, `htrafficsource` → attribution `traffic_source`.
  - These are stripped from the address bar after capture (no email left in history / Referer).
- **Identity stitching** (strongest → weakest): **session_id** (shared across tab + iframes via postMessage) → **email** → **shared IP** (15‑minute window).
  - **Crowded‑IP guard:** if an IP has **>3** contacts (office NAT / mobile CGNAT / VPN), those contacts are flagged **SHARED IP** and *not* IP‑merged — strangers behind one IP are never fused. Email + session stitching still apply.
  - **Different user‑agents are never IP‑merged.**
- **Bot/scanner clicks** (Outlook SafeLinks, link‑preview bots, headless) are dropped server‑side.
- **Console is silent by default.** For troubleshooting: `localStorage.setItem('st_debug','1')` before load.

---

## CRM

One record per person (`crm_people`, keyed by email), merging tracking identities with webhook events.

- **List** — searchable, filterable by stage; columns: name, email, phone, stage, source, visits, last seen.
- **Contact modal** — **Journey** (one chronological timeline: pageviews + tags + funnel events), **Clicks** (on‑site page views; empty for untracked people), **Details** (identity, attribution, tags, raw events).
- **Badges:** `LEGACY` = event‑only person (registered before tracking; empty Clicks). `SHARED IP` = seen on a shared network; identity wasn't auto‑merged.

---

## Email Report

Email‑click performance, grouped by `el` source (where `traffic_source=email`):

| Metric | Meaning |
|---|---|
| Clicks | email‑link clicks |
| People | unique people who clicked |
| Buyers | clickers who **purchased after the click** (any time after; pass `?window=<days>` to cap) |
| Conversion | buyers ÷ people |

Counts only — there's no purchase **amount** in the data yet; send `amount` on the purchase webhook to add revenue later.

---

## AI Insights

A chat analyst (Claude `claude-sonnet-4-6`, tool‑use) at `POST /api/insights/chat`. It calls **tools** that aggregate **inside Postgres** and return small results (low egress — no vector store / RAG):

- Metrics: `get_metrics`, `get_metrics_rollup`, `compare_periods`, `get_event_counts`, `list_custom_metrics`
- Journey: `get_journey_funnel`, `get_contact_journey`, `get_journey_segment`, `describe_journey_data`
- Email: `get_email_report`
- Escape hatch: `run_sql` (guarded read‑only single `SELECT`/`WITH`, 500‑row cap, 5s timeout)
- Memory: `remember` / `forget` (per‑funnel `ai_memory`), plus Anthropic code execution for stats.

Conversations persist per user (`/api/insights/conversations`).

---

## API

Full reference in **[APIs.md](APIs.md)**. Summary:

- **Webhooks** (`X-API-Key`): `/api/metrics`, `/api/metrics/batch`, `/api/metrics/increment`, `/api/metrics/set`, `/api/fb-sync`.
- **Tracking** (public, permissive CORS): `GET /shumard.js`, `POST /api/track/{pageview,lead,registration,tag}`.
- **Dashboard** (Supabase JWT; `X-Funnel`): metrics, custom‑metrics, events, CRM (`/api/crm/*`), insights (`/api/insights/*`), lenses, admin.

---

## Custom metrics

Calculated columns defined in the dashboard. Variables: any `daily_metrics` field (`fb_spend`, `registrations`, `attended`, `replays`, `viewedcta`, `clickedcta`, `total_purchases`, `purchases_fb`, …). Operators: `+ - * / ( )`.

| Name | Formula | Format |
|---|---|---|
| CTA Click Rate | `clickedcta / viewedcta * 100` | Percent |
| Cost Per Registration | `fb_spend / registrations` | Currency |
| Attendance Rate | `attended / registrations * 100` | Percent |
| CPA | `fb_spend / total_purchases` | Currency |

---

## Webhook setup (Zapier / Make / Stealth)

- **Daily summary:** once/day → `POST /api/metrics` with the day's fields.
- **Real‑time:** per event → `POST /api/metrics/increment` `{ "field": "registrations", "count": 1, "email": "…", "name": "…" }` (purchases also send `"source"`).
- **Absolute set:** `POST /api/metrics/set` `{ "field": "fb_spend", "value": 312.75 }` (e.g. FB running totals).

All with header `X-API-Key: <API_KEY>` (or `NATIVE_API_KEY` for the native funnel). Registrations dedupe by email + webinar day; other events dedupe within a 5‑minute window.

---

## Deployment

Production runs under **PM2** behind **nginx**, deployed by `deploy.sh` (git pull → `npm ci` → `npm run build` → `pm2 reload`). Full steps + the production checklist are in **[DEPLOYMENT.md](DEPLOYMENT.md)**.

## Facebook ad‑spend sync

Automatic 30‑minute spend sync via a Facebook System User token — setup in **[facebook.md](facebook.md)**.

---

## Timezone

All dates are **America/Los_Angeles** (`TZ` is set in `ecosystem.config.cjs`). Webhook dates are normalized to LA time before storing.

---

## Security

- Webhook API keys compared with `crypto.timingSafeEqual`; dashboard + CRM + Insights require a Supabase JWT (`requireAuth`); writes require admin (`requireAdmin`).
- Rate limits: webhooks 300/min, dashboard 120/min, **tracking 1200/min** (per IP).
- Helmet headers; CORS locked to `CORS_ORIGINS` for the dashboard, permissive **only** on `/shumard.js` + `/api/track/*` (unauthenticated, no credentials).
- The read‑only SQL tool is double‑guarded (Node validator + `ai_run_sql` row cap / timeout).

---

## File structure

```
analytics/
├── server.js                  # Express API — webhooks, tracking, CRM, insights, FB sync
├── tracking/
│   └── shumard.js             # First-party tracking script (served at /shumard.js)
├── frontend/
│   ├── App.jsx                # React dashboard (Dashboard · CRM · Email Report · Insights · Activity Log · Query)
│   ├── main.jsx · index.html
├── public/                    # Built dashboard (generated by `npm run build`)
├── db/
│   ├── schema.sql · native_schema.sql
│   ├── migrate_tracking_crm.sql · migrate_crm_views.sql · migrate_shared_ip_flag.sql
│   ├── migrate_ai_run_sql.sql · migrate_ai_memory.sql · migrate_*.sql
├── scripts/
│   ├── test-webhook.js        # metrics webhook test
│   └── test-tracking.js       # tracking + stitching test
├── fb-sync.js                 # Facebook spend sync
├── deploy.sh · ecosystem.config.cjs · nginx-analytics.conf · Dockerfile · docker-compose.yml
└── README.md · APIs.md · DEPLOYMENT.md · facebook.md
```
