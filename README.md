# Dr Shumard Analytics

Production analytics dashboard for tracking Facebook ad spend and patient acquisition funnel metrics.

---

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Zapier / Make   │────▶│  Express API     │────▶│  Supabase       │
│  (webhooks)      │     │  (your server)   │     │  (PostgreSQL)   │
└─────────────────┘     └──────┬───────────┘     └─────────────────┘
                               │
                        ┌──────▼───────────┐
                        │  React Dashboard  │
                        │  (served static)  │
                        └──────────────────┘
```

**Data flow:**
1. Zapier/Make fires a webhook to `POST /api/metrics` with the day's numbers
2. Express validates, authenticates (API key), and upserts into Supabase
3. Dashboard reads from `GET /api/metrics` and renders the table
4. Custom metrics are calculated client-side from formulas you define

---

## Quick Start

### 1. Set Up Supabase

1. Create a project at [supabase.com](https://supabase.com)
2. Go to **SQL Editor** and run the contents of `db/schema.sql`
3. Go to **Settings → API** and copy:
   - Project URL → `SUPABASE_URL`
   - `anon` public key → `SUPABASE_ANON_KEY`
   - `service_role` secret key → `SUPABASE_SERVICE_KEY`

### 2. Configure Environment

```bash
cp .env.example .env
```

Fill in your `.env`:
```
PORT=3000
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_ANON_KEY=eyJ...
SUPABASE_SERVICE_KEY=eyJ...
API_KEY=<generate with: node -e "console.log(require('crypto').randomBytes(32).toString('hex'))">
CORS_ORIGINS=https://yourdomain.com
```

### 3. Install & Run

```bash
npm install
npm start
```

Or with Docker:
```bash
docker-compose up -d
```

### 4. Test the API

```bash
API_KEY=your-key-here node scripts/test-webhook.js
```

---

## API Endpoints

### Webhook Endpoints (require `X-API-Key` header)

#### `POST /api/metrics` — Upsert daily entry
This is the main endpoint Zapier/Make will call.

```json
{
  "date": "03/14/2026",
  "fb_spend": 245.50,
  "registrations": 42,
  "replays": 18,
  "viewedcta": 35,
  "clickedcta": 12,
  "purchases": 6
}
```

**Date formats accepted:** `MM/DD/YYYY`, `YYYY-MM-DD`, or ISO strings.  
**Behavior:** If the date already exists, it **replaces** the values (upsert).

#### `POST /api/metrics/batch` — Bulk upsert
```json
{
  "entries": [
    { "date": "03/11/2026", "fb_spend": 180, "registrations": 31, ... },
    { "date": "03/12/2026", "fb_spend": 220, "registrations": 38, ... }
  ]
}
```

#### `POST /api/metrics/increment` — Add to today's count
For real-time event tracking (e.g., a new registration triggers a Zap).
```json
{
  "field": "registrations",
  "count": 1
}
```

Valid fields: `fb_spend`, `registrations`, `replays`, `viewedcta`, `clickedcta`, `purchases`


# New endpoint added. For your Facebook API calls, use:
`POST /api/metrics/set`
Header: `X-API-Key: your-key`

```json
{
  "field": "fb_spend",
  "value": 312.75
}


That replaces today's fb_spend with 312.75, regardless of wha was there before. Call it 50 times a day, it just overwrites.

You can also target a specific date if needed:
json{
  "field": "fb_spend",
  "value": 312.75,
  "date": "03/14/2026"
}
So now you have three patterns depending on the data source:

/api/metrics — full day upsert (all 6 fields at once, end-of-day summary from Zapier)
/api/metrics/set — overwrite a single field with an absolute value (Facebook API returning running totals)
/api/metrics/increment — add to a field (event-driven, e.g. "a new registration just happened, +1")


### Dashboard Endpoints (no auth required)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/metrics?limit=90&offset=0` | Fetch daily entries |
| `PUT` | `/api/metrics/:date` | Update a specific day |
| `DELETE` | `/api/metrics/:date` | Delete a day's entry |
| `GET` | `/api/custom-metrics` | List custom formulas |
| `POST` | `/api/custom-metrics` | Create custom metric |
| `PUT` | `/api/custom-metrics/:id` | Update custom metric |
| `DELETE` | `/api/custom-metrics/:id` | Delete custom metric |
| `GET` | `/api/webhook-log?limit=50` | View webhook audit log |
| `GET` | `/api/health` | Health check |

---

## Zapier Setup

### Option A: Daily Summary (run once per day)

1. **Trigger:** Schedule → Every Day at 11:55 PM PT
2. **Action:** Webhooks by Zapier → POST
   - URL: `https://yourdomain.com/api/metrics`
   - Headers: `X-API-Key: your-api-key`
   - Body:
   ```json
   {
     "date": "{{zap_meta_human_now}}",
     "fb_spend": "{{steps.facebook.spend}}",
     "registrations": "{{steps.count_regs.count}}",
     "replays": "{{steps.count_replays.count}}",
     "viewedcta": "{{steps.count_viewedcta.count}}",
     "clickedcta": "{{steps.count_clickedcta.count}}",
     "purchases": "{{steps.count_purchases.count}}"
   }
   ```

### Option B: Real-time Increments

1. **Trigger:** New registration in your CRM/form
2. **Action:** Webhooks → POST to `/api/metrics/increment`
   ```json
   { "field": "registrations", "count": 1 }
   ```

---

## Make (Integromat) Setup

1. Create a new Scenario
2. Add an **HTTP** module → Make a Request
   - URL: `https://yourdomain.com/api/metrics`
   - Method: POST
   - Headers: `X-API-Key: your-api-key`, `Content-Type: application/json`
   - Body type: Raw → JSON
   - Request content: map your Facebook Ads fields
3. Schedule: Once per day at 11:55 PM (Los Angeles timezone)

---

## Custom Metrics

Create calculated columns from the dashboard UI. Examples:

| Name | Formula | Format |
|------|---------|--------|
| CTA Click Rate | `clickedcta / viewedcta * 100` | Percent |
| Cost Per Registration | `fb_spend / registrations` | Currency |
| Cost Per Purchase | `fb_spend / purchases` | Currency |
| Conversion Rate | `purchases / registrations * 100` | Percent |
| Replay Rate | `replays / registrations * 100` | Percent |

**Available variables:** `fb_spend`, `registrations`, `replays`, `viewedcta`, `clickedcta`, `purchases`  
**Operators:** `+`, `-`, `*`, `/`, `(`, `)`

---

## Deployment (Self-Hosted)

### With Docker (recommended)
```bash
docker-compose up -d
```

### With systemd
```bash
# /etc/systemd/system/dr-shumard.service
[Unit]
Description=Dr Shumard Analytics
After=network.target

[Service]
Type=simple
User=deploy
WorkingDirectory=/opt/dr-shumard
ExecStart=/usr/bin/node server.js
Restart=always
RestartSec=5
EnvironmentFile=/opt/dr-shumard/.env

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable dr-shumard
sudo systemctl start dr-shumard
```

### Reverse Proxy (nginx)
```nginx
server {
    listen 443 ssl http2;
    server_name analytics.drshumard.com;

    ssl_certificate     /etc/letsencrypt/live/analytics.drshumard.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/analytics.drshumard.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

---

## File Structure

```
dr-shumard/
├── server.js              # Express API (all routes)
├── package.json           # Dependencies
├── .env.example           # Environment template
├── Dockerfile             # Container build
├── docker-compose.yml     # Container orchestration
├── db/
│   └── schema.sql         # PostgreSQL schema (run in Supabase)
├── scripts/
│   └── test-webhook.js    # API test script
├── public/                # Built React dashboard (place build output here)
│   └── index.html
└── README.md
```

---

## Security Notes

- API key is validated with `crypto.timingSafeEqual` (timing-attack resistant)
- Rate limiting: 60 req/min for webhooks, 120 req/min for dashboard
- Helmet.js security headers enabled
- All webhook payloads are logged to `webhook_log` for audit
- CORS locked to configured origins in production
- Consider adding Supabase Row Level Security if exposing the DB directly

---

## Timezone

All dates are in **America/Los_Angeles**. The server sets `TZ=America/Los_Angeles` and all date parsing/display uses this timezone. When Zapier/Make sends a date, the API normalises it to LA time before storing.
