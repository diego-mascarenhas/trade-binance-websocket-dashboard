#!/usr/bin/env bash
# Restart the fleet only when the hub is down or unhealthy.
#
# Setup (edit paths for your server):
#   chmod +x scripts/watchdog.sh
#
# Crontab — check every 5 minutes:
#   */5 * * * * /full/path/trade-binance-websocket-dashboard/scripts/watchdog.sh >> /full/path/trade-binance-websocket-dashboard/logs/watchdog.log 2>&1
#
# Optional env in .env:
#   HUB_PORT=8050
#   WATCHDOG_MIN_RESTART_SEC=600   # min gap between restarts (default 10 min)
#   WATCHDOG_HEALTH_URL=           # override health URL entirely
#
# Tip: pause trading before scheduled restarts (Telegram /stop) or use
# EXECUTE_ON_VALID_ENTRY=false if you also run a daily cron restart.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SCRIPT_DIR"

if [[ -f "$SCRIPT_DIR/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/.env"
    set +a
fi

HUB_PORT="${HUB_PORT:-8050}"
MIN_RESTART_SEC="${WATCHDOG_MIN_RESTART_SEC:-600}"
HEALTH_URL="${WATCHDOG_HEALTH_URL:-http://127.0.0.1:${HUB_PORT}/api/health}"
PID_FILE="${PID_FILE:-.run-all.pids}"
COOLDOWN_FILE="${SCRIPT_DIR}/logs/.watchdog-last-restart"
LOG_DIR="${SCRIPT_DIR}/logs"
RUN_ALL="${SCRIPT_DIR}/run-all.sh"

mkdir -p "$LOG_DIR"

log() {
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

hub_healthy() {
    if ! command -v curl >/dev/null 2>&1; then
        log "ERROR: curl not found"
        return 1
    fi
    local code
    code="$(curl -sf --max-time 8 -o /dev/null -w '%{http_code}' "$HEALTH_URL" 2>/dev/null || echo "000")"
    [[ "$code" == "200" ]]
}

in_cooldown() {
    [[ ! -f "$COOLDOWN_FILE" ]] && return 1
    local last now
    last="$(cat "$COOLDOWN_FILE" 2>/dev/null || echo 0)"
    now="$(date +%s)"
    (( now - last < MIN_RESTART_SEC ))
}

mark_restart() {
    date +%s >"$COOLDOWN_FILE"
}

count_live_pids() {
    [[ ! -f "$PID_FILE" ]] && echo 0 && return
    local pid alive=0
    while IFS= read -r pid; do
        [[ -z "$pid" ]] && continue
        if kill -0 "$pid" 2>/dev/null; then
            alive=$((alive + 1))
        fi
    done <"$PID_FILE"
    echo "$alive"
}

if hub_healthy; then
    log "OK hub healthy ($HEALTH_URL)"
    exit 0
fi

alive="$(count_live_pids)"
log "WARN hub unhealthy ($HEALTH_URL) — live pids in $PID_FILE: $alive"

if in_cooldown; then
    log "SKIP restart (cooldown ${MIN_RESTART_SEC}s — see $COOLDOWN_FILE)"
    exit 0
fi

if [[ ! -x "$RUN_ALL" ]]; then
    log "ERROR: run-all.sh not executable at $RUN_ALL"
    exit 1
fi

log "ACTION stopping fleet…"
"$RUN_ALL" stop || true
sleep 5

log "ACTION starting fleet…"
"$RUN_ALL" >>"${LOG_DIR}/watchdog-restart.log" 2>&1 &
mark_restart
log "DONE fleet restart triggered (see logs/watchdog-restart.log)"
