# Deployment

Production runs on a VPS as a **PM2** process behind **nginx**, deployed from `main` by `deploy.sh`.

- App dir: `/var/www/analytics`
- PM2 app name: `analytics`
- Port: `5401` (nginx `analytics.drshumard.com` → `proxy_pass http://127.0.0.1:5401`)
- Deploy: `git reset --hard origin/main` → `npm ci` → `npm run build` → `pm2 reload` (zero‑downtime; `wait_ready`)

---

## One‑time setup

1. **Supabase** — run the SQL in `db/` (see README → Quick start). Apply, in order:
   `schema.sql`, `migrate_tracking_crm.sql`, `migrate_crm_views.sql`, `migrate_shared_ip_flag.sql`, `migrate_ai_run_sql.sql`, `migrate_ai_memory.sql`, plus the other `migrate_*.sql`. For the **native** funnel: `native_schema.sql` + re‑run the tracking/crm/ai migrations under `SET search_path = native;`, and expose `native` under Settings → API → Exposed schemas.
2. **`.env`** at `/var/www/analytics/.env` (it is **gitignored** — edit it on the server, it is not deployed):
   ```
   PORT=5401
   SUPABASE_URL= / SUPABASE_ANON_KEY= / SUPABASE_SERVICE_KEY=
   API_KEY=                 # analytics webhook key
   NATIVE_API_KEY=          # optional (native funnel)
   ANTHROPIC_API_KEY=       # AI Insights
   CORS_ORIGINS=https://analytics.drshumard.com
   TRACKING_PUBLIC_URL=https://analytics.drshumard.com
   FB_ACCESS_TOKEN= / FB_AD_ACCOUNT_ID=act_... / FB_API_VERSION=v21.0   # optional
   TZ=America/Los_Angeles
   ```
   > `TRACKING_PUBLIC_URL` is baked into the served `shumard.js` as its callback URL. It **must be `https://`** — the script loads on https funnel pages, and an `http://` callback is blocked as mixed content.
3. **nginx** — `nginx-analytics.conf` proxies `location /` to `127.0.0.1:5401` (so `/shumard.js`, `/api/sg/*`, the dashboard, and the API all reach the app). Ensure **TLS is enabled** for `analytics.drshumard.com` (e.g. certbot adds the `:443` block + redirects `:80`). The script must be served over https.
4. **PM2 boot:** `pm2 start ecosystem.config.cjs && pm2 save && pm2 startup`.

---

## Each release

```bash
# 1. Locally: commit + push to main (deploy pulls origin/main via reset --hard)
git add -A && git commit -m "…" && git push origin main

# 2. On the server:
cd /var/www/analytics && ./deploy.sh        # pull → npm ci → npm run build → pm2 reload
```

`npm run build` regenerates `public/` (the dashboard bundle) with `SUPABASE_URL`/`ANON_KEY` injected from `.env`. `tracking/shumard.js` ships via git (not gitignored) and is read by the server at startup.

> `deploy.sh` does `git reset --hard origin/main` — it **discards any local changes on the server**. Everything must be committed and pushed first.

---

## Database migrations

Migrations are plain SQL in `db/`. Apply them **before** deploying code that depends on them — via the Supabase SQL editor (note: it runs the whole script in one transaction, so a single failing statement rolls back the rest), or `psql "$DATABASE_URL" -f db/<file>.sql`. They're written idempotent (`CREATE … IF NOT EXISTS`, `CREATE OR REPLACE VIEW` / `DROP VIEW IF EXISTS`).

---

## Instrument the funnel

Add to every funnel page:
```html
<script src="https://analytics.drshumard.com/shumard.js"></script>
```
- It **must** be on the pages your post‑webinar emails link to (checkout/landing) so `he`/`el`/`htrafficsource` are captured (powers the Email Report).
- Email links: `…?he={{contact.email}}&el=<source>&htrafficsource=email`.
- Optional `?tag=attended|replay|saw-cta|…` to mark a stage from the page (funnel stages also arrive via webhooks).

The Zapier/Make/Stealth webhook integrations (→ `/api/metrics/*`) are unchanged and keep feeding registrations/attendance/CTA/purchases.

---

## Post‑deploy verification

```bash
curl -s https://analytics.drshumard.com/api/health
curl -s https://analytics.drshumard.com/shumard.js | head    # BACKEND_URL must be https://analytics.drshumard.com
```
Then in a browser: load a funnel page → DevTools Network shows `POST /api/sg/pageview → 200`, and the visitor appears in the dashboard **CRM** tab. Confirm **CRM**, **Email Report**, and **AI Insights** all load. `pm2 logs analytics` for server logs.

---

## Rollback

```bash
cd /var/www/analytics
git reset --hard <previous-good-sha>
npm ci && npm run build && pm2 reload ecosystem.config.cjs
```
The tracking/CRM additions are isolated (new tables + a view + new routes); reverting the code leaves the existing metrics dashboard and webhook pipeline exactly as before. Dropping the new tables/view is optional and harmless to the core funnel.
