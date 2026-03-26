# Facebook Ad Spend Sync — Setup Guide

This guide gets you a **non-expiring** access token for the Facebook Graph API so your dashboard pulls ad spend automatically every 30 minutes without ever needing to refresh a token.

---

## Why a System User Token?

| Token type | Lifespan | Your experience |
|---|---|---|
| User token (what n8n gave you) | 60 minutes | Breaks constantly |
| Long-lived user token | 60 days | Breaks every 2 months |
| **System User token** | **Never expires** | **Set it once, forget it** |

System User tokens are tied to your Business Manager, not your personal account. Facebook designed them for server-to-server integrations exactly like this.

---

## Step-by-Step Setup

### 1. Open Facebook Business Manager

Go to [business.facebook.com/settings](https://business.facebook.com/settings)

If you don't have a Business Manager yet, create one — it's free and takes 2 minutes.

### 2. Create a System User

1. In Business Settings, go to **Users → System Users** in the left sidebar
2. Click **Add**
3. Name it something like `Dr Shumard Analytics Bot`
4. Set the role to **Admin** (needed for full ads_read access)
5. Click **Create System User**

### 3. Assign the Ad Account

1. Click on the system user you just created
2. Click **Add Assets**
3. Select **Ad Accounts**
4. Find your ad account and check it
5. Toggle on **View performance** (this grants `ads_read`)
6. Click **Save Changes**

### 4. Generate the Token

1. Still on the system user page, click **Generate New Token**
2. Select the **App** — if you don't have one:
   - Go to [developers.facebook.com](https://developers.facebook.com)
   - Create a new app (type: Business)
   - Come back and select it here
3. In the permissions list, check **ads_read**
4. Click **Generate Token**
5. **Copy the token immediately** — you won't see it again

### 5. Find Your Ad Account ID

Your ad account ID looks like `act_123456789`. You can find it:
- In Business Settings → Accounts → Ad Accounts
- Or in Ads Manager, it's in the URL: `adsmanager.facebook.com/adsmanager/manage/campaigns?act=123456789`

Add the `act_` prefix: if the number is `123456789`, the ID is `act_123456789`.

### 6. Add to Your .env

```env
FB_ACCESS_TOKEN=EAAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
FB_AD_ACCOUNT_ID=act_123456789
FB_API_VERSION=v21.0
```

### 7. Test It

```bash
# Test the token manually
curl "https://graph.facebook.com/v21.0/act_123456789/insights?access_token=YOUR_TOKEN&fields=spend&date_preset=today"

# Test via your app
node fb-sync.js

# Or trigger via the API
curl -X POST http://localhost:3000/api/fb-sync \
  -H "X-API-Key: your-api-key"
```

### 8. Restart Your Server

```bash
npm start
```

You should see in the console:
```
📡 Facebook ad spend sync enabled (every 30 min)
```

---

## How It Works Once Running

1. Every 30 minutes (at :00 and :30), `node-cron` triggers the sync
2. `fb-sync.js` calls the Facebook Graph API: `GET /act_XXX/insights?fields=spend&time_range=today`
3. Facebook returns today's **cumulative spend** (e.g. $142.50)
4. The sync writes that value directly to Supabase, **replacing** (not adding to) the current fb_spend for today
5. The dashboard auto-refreshes every 30 seconds and picks up the new number

```
Every 30 min:
  Facebook API → "today's spend is $142.50"
       ↓
  Supabase daily_metrics → fb_spend = 142.50 (overwrite)
       ↓
  Dashboard picks it up on next 30s refresh
```

---

## Endpoints

| Method | URL | Auth | Description |
|---|---|---|---|
| `POST` | `/api/fb-sync` | X-API-Key | Force a sync right now |
| `GET` | `/api/fb-sync/status` | None | Check if sync is configured |

---

## Troubleshooting

**"FB sync skipped — not configured"**
→ `FB_ACCESS_TOKEN` or `FB_AD_ACCOUNT_ID` is missing from your `.env`

**"Facebook API error: Invalid OAuth access token"**
→ Token is wrong or was revoked. Regenerate it from the System User page.

**"Facebook API error: (#100) The parameter account_id is required"**
→ `FB_AD_ACCOUNT_ID` is missing the `act_` prefix. It should be `act_123456789`.

**Spend shows $0 even though ads are running**
→ Facebook sometimes delays reporting by 15-30 minutes. Also check that the system user has access to the correct ad account.

**"Facebook API error: (#17) User request limit reached"**
→ You're hitting the rate limit. 30-minute intervals should be well within limits, but if you have other tools also querying the same account, consider reducing frequency.

---

## Security Notes

- The System User token has **read-only** access to ad performance data
- It cannot modify ads, create campaigns, or access personal data
- The token is stored only in your `.env` file on your server
- It's never exposed to the frontend — the sync runs server-side only
- If compromised, revoke it instantly from Business Settings → System Users → the user → Generate New Token (this invalidates the old one)