# API Reference — analytics.drshumard.com

Base URL: `https://analytics.drshumard.com`

---

## Webhook Endpoints

All require header: `X-API-Key: <your API_KEY from .env>`

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/api/metrics` | Upsert a day's metrics |
| `POST` | `/api/metrics/batch` | Upsert multiple days at once |
| `POST` | `/api/metrics/increment` | Increment a field for a date (e.g. +1 registration) |
| `POST` | `/api/metrics/set` | Set a specific field value for a date |
| `POST` | `/api/fb-sync` | Trigger Facebook spend sync |

---

## Dashboard Endpoints

Public read access. Writes require Supabase JWT auth.

| Method | Endpoint | Purpose | Auth |
|---|---|---|---|
| `GET` | `/api/metrics` | Fetch metrics (`?limit=` `?offset=`) | Public |
| `PUT` | `/api/metrics/:date` | Update a day's entry | Admin |
| `DELETE` | `/api/metrics/:date` | Delete a day's entry | Admin |
| `GET` | `/api/custom-metrics` | List custom calculated metrics | Public |
| `POST` | `/api/custom-metrics` | Create a custom metric | Admin |
| `PUT` | `/api/custom-metrics/:id` | Update a custom metric | Admin |
| `DELETE` | `/api/custom-metrics/:id` | Delete a custom metric | Admin |
| `GET` | `/api/events` | List webhook events (`?type=` `?limit=`) | Public |
|  |  | Types: `registrations`, `replays`, `viewedcta`, `clickedcta`, `purchases`, `attended` |  |
| `GET` | `/api/webhook-log` | View webhook call log | Public |
| `POST` | `/api/refresh` | Refresh FB spend for today | Public |
| `POST` | `/api/refresh-date` | Refresh FB spend for a specific date | Public |
| `GET` | `/api/me` | Get current user info/role | Auth |
| `GET` | `/api/health` | Health check | Public |
| `GET` | `/api/fb-sync/status` | FB sync status | Public |

---

## Key Endpoints for n8n / Zapier

```
POST https://analytics.drshumard.com/api/metrics/increment
POST https://analytics.drshumard.com/api/metrics/set
POST https://analytics.drshumard.com/api/metrics
```

All with header: `X-API-Key: <your API_KEY>`
