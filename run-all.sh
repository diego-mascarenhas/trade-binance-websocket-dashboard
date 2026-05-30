#!/usr/bin/env bash
# Start hub page + one dashboard process per pair.
# Usage:
#   ./run-all.sh              # default pairs
#   ./run-all.sh stop         # stop hub + dashboards started by this script
#   PAIRS="DOGEUSDT:8051,BNBUSDT:8052" ./run-all.sh
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
    "1000SHIBUSDT:8051"
    "ADAUSDT:8052"
    "ATOMUSDT:8053"
    "AVAXUSDT:8054"
    "BTCUSDT:8055"
    "DOGEUSDT:8056"
    "ETCUSDT:8057"
    "ETHUSDT:8058"
    "LINKUSDT:8059"
    "LTCUSDT:8060"
    "SOLUSDT:8061"
    "TRXUSDT:8062"
    "XRPUSDT:8063"
)

log() {
    printf '[run-all] %s\n' "$*"
}

stop_all() {
    if [[ ! -f "$PID_FILE" ]]; then
        log "No pid file ($PID_FILE). Nothing to stop."
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
    log "Stopped."
}

parse_pairs() {
    PAIR_LINES=()
    if [[ -n "${PAIRS:-}" ]]; then
        IFS=',' read -r -a _raw_pairs <<< "$PAIRS"
        for item in "${_raw_pairs[@]}"; do
            item="${item// /}"
            [[ -n "$item" ]] && PAIR_LINES+=("$item")
        done
    else
        PAIR_LINES=("${DEFAULT_PAIRS[@]}")
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
    if ! command -v python3 >/dev/null 2>&1; then
        log "python3 not found."
        exit 1
    fi

    log "Hub http://0.0.0.0:${HUB_PORT}/"
    python3 -m http.server "$HUB_PORT" --bind 0.0.0.0 --directory "$HUB_DIR" >/dev/null 2>&1 &
    echo $! >> "$PID_FILE"
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

    for item in "${PAIR_LINES[@]}"; do
        local symbol="${item%%:*}"
        local port="${item##*:}"
        log "Dashboard ${symbol} -> http://127.0.0.1:${port}/"
        "$VENV_PYTHON" "$SCRIPT_DIR/app.py" "$symbol" --port "$port" >/dev/null 2>&1 &
        echo $! >> "$PID_FILE"
    done
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
write_pairs_json
start_hub
start_dashboards

log "Started ${#PAIR_LINES[@]} dashboard(s) + hub."
log "Open hub: http://127.0.0.1:${HUB_PORT}/"
log "Stop all: ./run-all.sh stop"

wait
