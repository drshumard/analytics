#!/usr/bin/env bash
set -euo pipefail

# ── Dr Shumard Analytics — Zero-Downtime Deploy Script ──
# Pulls latest from GitHub, rebuilds frontend, and gracefully reloads the app.
# PM2 keeps the old process alive until the new one signals ready.
# Usage: ./deploy.sh [branch]
#   branch  — git branch to deploy (default: main)

BRANCH="${1:-main}"
APP_DIR="/var/www/analytics"
PM2_APP_NAME="analytics"
LOG_FILE="/var/log/analytics-deploy.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

log "═══ Starting deployment (branch: $BRANCH) ═══"

# 1. Navigate to app directory
cd "$APP_DIR" || { log "ERROR: $APP_DIR not found"; exit 1; }

# 2. Pull latest code
log "Pulling latest changes…"
git fetch origin
git reset --hard "origin/$BRANCH"
log "Checked out $(git log -1 --oneline)"

# 3. Install / update dependencies
log "Installing dependencies…"
npm ci --prefer-offline

# 4. Build the Vite frontend
log "Building frontend…"
npm run build

# 5. Zero-downtime reload via PM2
#    - 'reload' (not 'restart') keeps the old process alive until the new one is ready
#    - ecosystem.config.cjs has wait_ready: true, so PM2 waits for process.send('ready')
log "Reloading PM2 process (zero-downtime)…"
if pm2 describe "$PM2_APP_NAME" > /dev/null 2>&1; then
    pm2 reload ecosystem.config.cjs
else
    pm2 start ecosystem.config.cjs
fi
pm2 save

log "═══ Deployment complete (zero-downtime) ═══"
