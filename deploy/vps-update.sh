#!/usr/bin/env bash
# ============================================================
# Agente Marketplace AM — redeploy (puxa último main + restart)
# Pode ser executado pelo deploy user OU root.
# Uso:
#   cd /home/deploy/agente-marketplace-am
#   bash deploy/vps-update.sh
# ============================================================
set -euo pipefail

APP_NAME="agente-am"
BRANCH="main"
DEPLOY_USER="deploy"
PROJECT_DIR="/home/${DEPLOY_USER}/agente-marketplace-am"

log() { printf "\n\033[1;32m==> %s\033[0m\n" "$*"; }

# Se não estiver rodando como o deploy user, recomeça via sudo -u
if [[ "$(whoami)" != "${DEPLOY_USER}" ]]; then
    if command -v sudo >/dev/null 2>&1; then
        exec sudo -u "${DEPLOY_USER}" bash "$0" "$@"
    fi
fi

cd "${PROJECT_DIR}"

log "git pull origin ${BRANCH}"
git fetch origin
git reset --hard "origin/${BRANCH}"

log "npm install"
npm install --omit=dev --no-audit --no-fund

log "pm2 restart ${APP_NAME}"
if pm2 describe "${APP_NAME}" >/dev/null 2>&1; then
    pm2 restart "${APP_NAME}" --update-env
else
    pm2 start server.js --name "${APP_NAME}"
fi
pm2 save

log "✅ Redeploy concluído"
pm2 status
