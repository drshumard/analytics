#!/usr/bin/env bash
set -euo pipefail

# ── Dr Shumard Analytics — Deploy Script ──
# Pulls latest from GitHub, rebuilds frontend, and restarts the app.
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

# 5. Restart via PM2
log "Restarting PM2 process…"
if pm2 describe "$PM2_APP_NAME" > /dev/null 2>&1; then
    pm2 restart "$PM2_APP_NAME"
else
    pm2 start server.js --name "$PM2_APP_NAME"
fi
pm2 save

log "═══ Deployment complete ═══"
