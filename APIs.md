# API Reference — analytics.drshumard.com

Base URL: `https://analytics.drshumard.com`

**Auth models**
- **Webhooks** → header `X-API-Key: <API_KEY>` (or `NATIVE_API_KEY`). The key selects the funnel.
- **Dashboard / CRM / Insights** → header `Authorization: Bearer <Supabase JWT>` + `X-Funnel: analytics|native` (defaults to `analytics`). `requireAuth` validates funnel access; writes also need admin (`requireAdmin`).
- **Tracking** (`/shumard.js`, `/api/sg/*`) → public, permissive CORS (unauthenticated, embedded on client sites).

---

## Webhook endpoints — `X-API-Key`

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/api/metrics` | Upsert a full day's metrics |
| `POST` | `/api/metrics/batch` | Upsert multiple days (`{ entries: [...] }`) |
| `POST` | `/api/metrics/increment` | Add to a field for a date (event‑driven, e.g. +1 registration) |
| `POST` | `/api/metrics/set` | Set a field to an absolute value (e.g. FB running spend total) |
| `POST` | `/api/fb-sync` | Force a Facebook spend sync now |

`increment` body: `{ "field": "registrations", "count": 1, "email": "...", "name": "...", "phone": "..." }`. Valid `field`: `fb_spend`, `fb_link_clicks`, `registrations`, `replays`, `viewedcta`, `clickedcta`, `purchases`, `attended`, `stayeduntil`. Purchases route by `metadata.source` (`Paid Ads`/`Native`/`Youtube`/`AI Bot`/`CPA Traffic`/`Sales A`/`Sales B`; Post‑Webinar auto‑detected). Registrations dedupe by email + webinar day; other events dedupe within 5 minutes. Each event is also written to the `events` table.

---

## Tracking endpoints — public (shumard.js)

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/shumard.js` | The tracking script. `?tag=<name>` bakes in an auto‑tag. `BACKEND_URL` = `TRACKING_PUBLIC_URL`. |
| `POST` | `/api/sg/pageview` | Log a pageview |
| `POST` | `/api/sg/lead` | Email/phone captured on a form field |
| `POST` | `/api/sg/registration` | A form was submitted |
| `POST` | `/api/sg/tag` | Apply a funnel tag to a contact |

Payload (pageview/lead/registration): `{ contact_id, session_id, current_url, referrer_url, page_title, attribution{}, user_agent, email?, phone?, name? }`. Attribution recognizes UTMs, `fbclid`, `_fbc`/`_fbp`, `gclid`, `ttclid`, plus `source` (from `el`) and `traffic_source` (from `htrafficsource`). Bot/scanner user‑agents return `{status:"ok",skipped:"bot"}` and write nothing. Rate limit: 1200/min per IP.

---

## CRM endpoints — Bearer + `X-Funnel`

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/api/crm/contacts` | People list (`?search=` `?stage=` `?limit=` `?offset=`). Returns `is_tracked`, `is_shared_ip`, `stage`, `visit_count`, … |
| `GET` | `/api/crm/contacts/:id` | One person's full journey — `:id` is an email or `contact_id`. Returns identity + chronological `timeline` (pageviews + tags + events) + `visits` + `events`. |
| `GET` | `/api/crm/stats` | Funnel counts for the CRM header |
| `GET` | `/api/crm/email-report` | Email‑click performance by source (`?window=<days>` optional attribution cap; default: any purchase after the click) |

Stages: `lead → registration → attended → replay → viewedcta → clickedcta → purchase`.

---

## AI Insights endpoints — Bearer + `X-Funnel`

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/api/insights/chat` | Chat with the analyst (`{ messages:[{role,content}] }`). Claude tool‑use; streams back markdown + optional `chart` blocks. |
| `GET` | `/api/insights/conversations` | List saved conversations |
| `GET` | `/api/insights/conversations/:id` | Get one conversation |
| `PUT` | `/api/insights/conversations/:id` | Save/update a conversation |
| `DELETE` | `/api/insights/conversations/:id` | Delete a conversation |

The model's tools: `get_metrics`, `get_metrics_rollup`, `compare_periods`, `get_event_counts`, `list_custom_metrics`, `get_journey_funnel`, `get_variant_funnel`, `get_contact_journey`, `get_journey_segment`, `describe_journey_data`, `get_email_report`, `get_sales_page_visits`, `run_sql` (read‑only), `remember`, `forget`.

---

## AI Tools API — `X-API-Key` (external AI apps)

Gives an external AI app the same tool set the built‑in analyst uses. The app runs its own LLM loop: fetch the definitions, pass them to its model as tools, POST each `tool_use` here, and feed the JSON back as the `tool_result`.

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/api/ai/tools` | Tool definitions (Anthropic `{name, description, input_schema}` format), filtered to what the key may call. Returns `{ funnel, tools }`. |
| `POST` | `/api/ai/tools/<tool_name>` | Execute one tool (`<tool_name>` = a tool from the list, e.g. `get_metrics`, `run_sql`). Body = the tool's input JSON. Returns `{ tool, funnel, result }`. |
| `GET` | `/api/ai/tools/<email>` | Shortcut: a contact's complete journey (identity, attribution, stage, chronological timeline of pageviews + tags + funnel events) — the `@` marks it as an email, no body needed. Same as running `get_contact_journey`; requires that tool in scope. Also works as `POST`. |

- **Auth**: `X-API-Key` — a key row in `public.api_keys`; the key selects the funnel. Mint a dedicated key per external app (see `db/migrate_api_key_scopes.sql` for the recipe).
- **Scoping — deny by default**: the key row's `scopes TEXT[]` must explicitly name the tools it can list/call (`403` outside scope). `NULL`/empty = no AI tools; `ARRAY['*']` = all. The env webhook keys (`API_KEY`, `NATIVE_API_KEY`) get **no** AI tools access — they stay push‑only. PII lives in `run_sql`, `get_contact_journey`, `get_journey_segment` — omit those from `scopes` for a PII‑free key.
- **Read‑only**: `remember`/`forget` are not exposed (`404`) — they personalize the in‑app chat per dashboard user.
- **Errors**: unknown tool → `404`; out of scope → `403`. Tool‑level failures (bad input, SQL rejected) return `200` with `result.error` — exactly what the built‑in analyst's model sees.
- **Limits**: 300 req/min per IP; `run_sql` is a single `SELECT`/`WITH`, capped at 500 rows / 5 s.

```
curl -s https://analytics.drshumard.com/api/ai/tools -H "X-API-Key: <key>"
curl -s https://analytics.drshumard.com/api/ai/tools/get_journey_funnel \
     -H "X-API-Key: <key>" -H "Content-Type: application/json" \
     -d '{"date_from":"2026-06-01","date_to":"2026-06-30"}'
# one person's full journey — look up by EMAIL:
curl -s https://analytics.drshumard.com/api/ai/tools/jane@example.com -H "X-API-Key: <key>"
```

---

## Dashboard endpoints — Bearer + `X-Funnel`

| Method | Endpoint | Purpose | Auth |
|---|---|---|---|
| `GET` | `/api/metrics?limit=&offset=&variant=` | Daily metrics (deduped) | Public* |
| `PUT` | `/api/metrics/:date` | Edit a day | Admin |
| `DELETE` | `/api/metrics/:date` | Delete a day | Admin |
| `GET` | `/api/activity?type=&limit=` | Activity log (per‑person events; named to avoid ad‑blocker keyword filters) | Public* |
| `GET` | `/api/custom-metrics` · `POST` · `PUT/:id` · `DELETE/:id` | Custom calculated metrics | Public* / Admin |
| `GET` | `/api/lenses` · `POST` · `PUT/:id` · `DELETE/:id` | Dashboard metric lenses | Auth |
| `GET` | `/api/me` · `/api/me/funnels` · `PUT /api/me/preferences` | Current user, allowed funnels, prefs | Auth |
| `POST` | `/api/refresh` · `/api/refresh-date` | Refresh FB spend | Public |
| `POST` | `/api/admin/finalize-date` · `/api/admin/finalize-past-days` | Freeze deduped counts | Admin |
| `POST` | `/api/admin/query` | Ad‑hoc query builder | Auth |
| `POST` | `/api/cache/clear` | Clear server caches | Auth |
| `GET` | `/api/webhook-log?limit=` | Webhook audit log | Public* |
| `GET` | `/api/health` · `/api/fb-sync/status` | Health / FB sync status | Public |

\* Read endpoints resolve the funnel from `X-Funnel` and don't require a JWT; CRM, Insights, and all writes do.

---

## Quick reference — webhooks for n8n / Zapier / Make

```
POST https://analytics.drshumard.com/api/metrics/increment    # +1 event (with email/name)
POST https://analytics.drshumard.com/api/metrics/set          # overwrite a field (e.g. fb_spend)
POST https://analytics.drshumard.com/api/metrics              # full-day upsert
```
Header on all: `X-API-Key: <API_KEY>`
