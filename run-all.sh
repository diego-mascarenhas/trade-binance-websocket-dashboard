#!/usr/bin/env bash
# Start hub page + one dashboard process per pair.
# Usage:
#   ./run-all.sh              # default pairs
#   ./run-all.sh stop         # stop hub + dashboards started by this script
#   PAIRS="DOGEUSDT:8051,BNBUSDT:8052" ./run-all.sh   # override + rewrite hub/pairs.json
#   Edit hub/pairs.json directly — used on start unless PAIRS= is set
#   HUB_PORT=8050 ./run-all.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

HUB_PORT="${HUB_PORT:-8050}"
PID_FILE="${PID_FILE:-.run-all.pids}"
HUB_DIR="$SCRIPT_DIR/hub"
PAIRS_FILE="$HUB_DIR/pairs.json"
VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"

DEFAULT_PAIRS=(
    "1000PEPEUSDT:8052"
    "1000SHIBUSDT:8057"
    "ADAUSDT:8051"
    "ATOMUSDT:8054"
    "AVAXUSDT:8055"
    "BNBUSDT:8056"
    "DOGEUSDT:8058"
    "DOTUSDT:8059"
    "ETCUSDT:8060"
    "ETHUSDT:8061"
    "LINKUSDT:8064"
    "LTCUSDT:8065"
    "NEARUSDT:8066"
    "OPUSDT:8067"
    "SOLUSDT:8069"
    "SUIUSDT:8070"
    "TRXUSDT:8071"
    "WIFUSDT:8053"
    "XRPUSDT:8072"
)

log() {
    printf '[run-all] %s\n' "$*"
}

stop_listening_ports() {
    local ports=("$HUB_PORT")
    if [[ -f "$PAIRS_FILE" && -x "$VENV_PYTHON" ]]; then
        while IFS= read -r port; do
            [[ -n "$port" ]] && ports+=("$port")
        done < <(
            "$VENV_PYTHON" -c "
import json
from pathlib import Path
rows = json.loads(Path('$PAIRS_FILE').read_text(encoding='utf-8'))
for row in rows:
    print(row.get('port', ''))
"
        )
    else
        for item in "${DEFAULT_PAIRS[@]}"; do
            ports+=("${item##*:}")
        done
    fi

    for port in "${ports[@]}"; do
        [[ -z "$port" ]] && continue
        if command -v fuser >/dev/null 2>&1; then
            fuser -k "${port}/tcp" 2>/dev/null || true
        elif command -v lsof >/dev/null 2>&1; then
            while IFS= read -r pid; do
                [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
            done < <(lsof -ti "tcp:${port}" -sTCP:LISTEN 2>/dev/null || true)
        fi
    done
}

stop_all() {
    if [[ ! -f "$PID_FILE" ]]; then
        log "No pid file ($PID_FILE). Cleaning listening ports anyway."
        stop_listening_ports
        return 0
    fi

    while IFS= read -r pid; do
        [[ -z "$pid" ]] && continue
        if kill -0 "$pid" 2>/dev/null; then
            log "Stopping pid $pid"
            kill "$pid" 2>/dev/null || true
        fi
    done < "$PID_FILE"

    sleep 1

    while IFS= read -r pid; do
        [[ -z "$pid" ]] && continue
        if kill -0 "$pid" 2>/dev/null; then
            kill -9 "$pid" 2>/dev/null || true
        fi
    done < "$PID_FILE"

    rm -f "$PID_FILE"
    stop_listening_ports

    if [[ -x "$VENV_PYTHON" && -f "$SCRIPT_DIR/telegram_notify.py" ]]; then
        "$VENV_PYTHON" -c "
import telegram_notify as t
t.set_fleet_running(False, updated_by='run-all')
" 2>/dev/null || true
    fi

    log "Stopped."
}

parse_pairs() {
    PAIR_LINES=()
    WRITE_PAIRS_JSON=0

    if [[ -n "${PAIRS:-}" ]]; then
        IFS=',' read -r -a _raw_pairs <<< "$PAIRS"
        for item in "${_raw_pairs[@]}"; do
            item="${item// /}"
            [[ -n "$item" ]] && PAIR_LINES+=("$item")
        done
        WRITE_PAIRS_JSON=1
    elif [[ -f "$PAIRS_FILE" ]] && [[ -x "$VENV_PYTHON" ]]; then
        while IFS= read -r line; do
            [[ -n "$line" ]] && PAIR_LINES+=("$line")
        done < <(
            "$VENV_PYTHON" -c "
import json
from pathlib import Path
rows = json.loads(Path('$PAIRS_FILE').read_text(encoding='utf-8'))
for row in rows:
    print(f\"{row['symbol']}:{row['port']}\")
"
        )
        if [[ ${#PAIR_LINES[@]} -gt 0 ]]; then
            log "Using hub/pairs.json (${#PAIR_LINES[@]} pairs)"
        fi
    fi

    if [[ ${#PAIR_LINES[@]} -eq 0 ]]; then
        PAIR_LINES=("${DEFAULT_PAIRS[@]}")
        WRITE_PAIRS_JSON=1
        log "Using built-in default pairs (${#PAIR_LINES[@]})"
    fi

    if [[ ${#PAIR_LINES[@]} -eq 0 ]]; then
        log "No pairs configured."
        exit 1
    fi
}

write_pairs_json() {
    mkdir -p "$HUB_DIR"
    {
        printf '[\n'
        local first=1
        for item in "${PAIR_LINES[@]}"; do
            local symbol="${item%%:*}"
            local port="${item##*:}"
            if [[ "$symbol" == "$port" || -z "$symbol" || -z "$port" ]]; then
                log "Invalid pair entry: $item (expected SYMBOL:PORT)"
                exit 1
            fi
            if [[ $first -eq 0 ]]; then
                printf ',\n'
            fi
            printf '  {"symbol": "%s", "port": %s}' "$symbol" "$port"
            first=0
        done
        printf '\n]\n'
    } > "$PAIRS_FILE"
}

start_hub() {
    if [[ ! -x "$VENV_PYTHON" ]]; then
        log "Missing venv. Run: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
        exit 1
    fi
    if [[ ! -f "$SCRIPT_DIR/hub_server.py" ]]; then
        log "hub_server.py not found in $SCRIPT_DIR"
        exit 1
    fi

    log "Hub http://0.0.0.0:${HUB_PORT}/ (analytics /analytics/)"
    mkdir -p "$SCRIPT_DIR/logs"
    HUB_PORT="$HUB_PORT" "$VENV_PYTHON" "$SCRIPT_DIR/hub_server.py" >>"$SCRIPT_DIR/logs/hub.log" 2>&1 &
    echo $! >> "$PID_FILE"
}

run_db_migrate() {
    if [[ ! -x "$VENV_PYTHON" ]]; then
        return 0
    fi
    if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
        return 0
    fi
    if ! grep -qE '^DB_ENABLED=(1|true|yes)' "$SCRIPT_DIR/.env" 2>/dev/null; then
        return 0
    fi
    if [[ ! -f "$SCRIPT_DIR/scripts/migrate_db.py" ]]; then
        return 0
    fi

    log "Checking MySQL migrations..."
    if ! "$VENV_PYTHON" "$SCRIPT_DIR/scripts/migrate_db.py"; then
        log "WARNING: DB migration failed — fleet continues (check MySQL and .env)"
    fi
}

start_dashboards() {
    if [[ ! -x "$VENV_PYTHON" ]]; then
        log "Missing venv. Run: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
        exit 1
    fi

    if [[ ! -f "$SCRIPT_DIR/app.py" ]]; then
        log "app.py not found in $SCRIPT_DIR"
        exit 1
    fi

    mkdir -p "$SCRIPT_DIR/logs"

    for item in "${PAIR_LINES[@]}"; do
        local symbol="${item%%:*}"
        local port="${item##*:}"
        log "Dashboard ${symbol} -> http://127.0.0.1:${port}/"
        TELEGRAM_COMMANDS_ENABLED=false "$VENV_PYTHON" "$SCRIPT_DIR/app.py" "$symbol" --port "$port" \
            >>"$SCRIPT_DIR/logs/${symbol}.log" 2>&1 &
        echo $! >> "$PID_FILE"
    done
}

start_telegram_fleet() {
    if [[ ! -x "$VENV_PYTHON" ]]; then
        return 0
    fi
    if [[ ! -f "$SCRIPT_DIR/telegram_fleet.py" ]]; then
        return 0
    fi
    if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
        log "Telegram fleet skipped (.env not found)"
        return 0
    fi
    if ! grep -q '^TELEGRAM_BOT_TOKEN=.\+' "$SCRIPT_DIR/.env" 2>/dev/null; then
        log "Telegram fleet skipped (TELEGRAM_BOT_TOKEN not set)"
        return 0
    fi

    log "Telegram fleet commands (/start /stop /status)"
    TELEGRAM_COMMANDS_ENABLED=true HUB_PORT="$HUB_PORT" "$VENV_PYTHON" "$SCRIPT_DIR/telegram_fleet.py" >/dev/null 2>&1 &
    echo $! >> "$PID_FILE"
}

if [[ "${1:-}" == "stop" ]]; then
    stop_all
    exit 0
fi

if [[ -f "$PID_FILE" ]]; then
    log "Pid file exists. Run './run-all.sh stop' first or remove $PID_FILE"
    exit 1
fi

parse_pairs
: > "$PID_FILE"
if [[ "${WRITE_PAIRS_JSON}" == "1" ]]; then
    write_pairs_json
fi
run_db_migrate
start_hub
start_dashboards
start_telegram_fleet

log "Started ${#PAIR_LINES[@]} dashboard(s) + hub."
log "Open hub: http://127.0.0.1:${HUB_PORT}/"
log "Analytics: http://127.0.0.1:${HUB_PORT}/analytics/"
log "Stop all: ./run-all.sh stop"

wait
