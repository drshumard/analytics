# Claude session guide — Dr Shumard Analytics

**Start with `PRODUCT.md`** — it is the canonical map of the whole product
(architecture, data model, pipelines, features, admin tools, file map with line
ranges, operational rules). Read the relevant section there instead of scanning
`server.js` (~7.6k lines) or `frontend/App.jsx` (~5.6k lines) end-to-end.
Update `PRODUCT.md` whenever you change architecture or ship a feature.

## Non-negotiables

- **New features / architectural changes:** build on a branch, verify locally
  (`PORT=5402 node server.js` runs safely against the live DB), demo to Raymond
  (screenshots via SendUserFile), deploy **only on his explicit go-ahead**.
  Small bug fixes to existing behavior may ship directly.
- **Deploy:** push `main`, then `/usr/bin/ssh LWD "cd /var/www/analytics &&
  ./deploy.sh"` (plain `ssh` is shadowed by a broken wrapper).
- **Never `DROP SCHEMA` while it's in Supabase "Exposed schemas"** — it 503s the
  entire REST API. Un-expose first. Full order + emergency recovery:
  `PRODUCT.md` §10.1.
- Dashboard numbers = **distinct people per LA day from `events`**, not raw
  counters. Don't "fix" counts by editing counters on recent days; finalize
  recounts nightly.
- All day-bucketing is **America/Los_Angeles**; `events.event_time` is UTC.
  Manual imports must convert (eboov CRM exports are UTC).

## Quick how-tos

- SQL/DDL against prod: `ai_run_sql_write` RPC via PostgREST (service key), one
  statement per call. Read-only: `ai_run_sql` (500 rows / 5 s).
- Mint an admin JWT for testing authed endpoints: GoTrue admin
  `generate_link` (magiclink) → `verify` with the hashed token (see memory:
  testing-authed-insights-endpoints).
- Funnels, event columns, purchase sources, user access: all self-service in the
  admin UI (account menu) — never hand-write column migrations again.
- Webhook contract, crons, env vars, file map: `PRODUCT.md` §4, §9, §12.
