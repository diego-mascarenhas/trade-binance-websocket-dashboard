#!/usr/bin/env python3
"""Single Telegram command listener for the whole run-all fleet."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
import urllib.error
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

import telegram_notify as telegram

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PAIRS_FILE = SCRIPT_DIR / "hub" / "pairs.json"
HUB_PORT = int(os.getenv("HUB_PORT", "8050"))
FLEET_STATUS_HOST = os.getenv("FLEET_STATUS_HOST", "127.0.0.1")
FLEET_STATUS_TIMEOUT = float(os.getenv("FLEET_STATUS_TIMEOUT", "2.5"))


def _load_pairs() -> list[dict]:
    if not PAIRS_FILE.exists():
        return []
    try:
        data = json.loads(PAIRS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read pairs.json: %s", exc)
        return []
    return data if isinstance(data, list) else []


def _fetch_pair_summary(port: int) -> dict | None:
    url = f"http://{FLEET_STATUS_HOST}:{port}/api/hub-summary"
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=FLEET_STATUS_TIMEOUT) as response:
            payload = json.loads(response.read().decode())
            return payload if isinstance(payload, dict) else None
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError):
        return None


def build_fleet_status() -> str:
    pairs = _load_pairs()
    lines = [
        f"Fleet · {len(pairs)} pairs · hub :{HUB_PORT}",
        f"Trading: {telegram.trading_state_label()}",
    ]

    trade_rows: list[str] = []
    position_rows: list[str] = []
    offline = 0

    for pair in pairs:
        symbol = str(pair.get("symbol", "—"))
        port = int(pair.get("port", 0))
        data = _fetch_pair_summary(port)
        if not data:
            offline += 1
            continue

        signal = data.get("signal", "NEUTRAL")
        confidence = int(data.get("confidence", 0))
        action = data.get("action", "WATCH")
        position = data.get("position") or "None"
        smc = data.get("smc_pattern") or data.get("smc_state_short") or "—"

        if data.get("position_open"):
            position_rows.append(f"{symbol}: {position}")
        if action == "TRADE":
            trade_rows.append(f"{symbol}: {signal} {confidence}% · SMC {smc}")

    if trade_rows:
        lines.append("TRADE:")
        lines.extend(trade_rows[:8])
    if position_rows:
        lines.append("Open:")
        lines.extend(position_rows[:8])
    if offline:
        lines.append(f"Offline: {offline}")

    if len(lines) == 2:
        lines.append("All pairs idle · no open positions")
    return "\n".join(lines)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    os.environ["TELEGRAM_COMMANDS_ENABLED"] = "true"

    pairs = _load_pairs()
    telegram.set_fleet_running(True, updated_by="run-all")
    telegram.notify_fleet_started(len(pairs), HUB_PORT)
    telegram.start_command_listener(build_fleet_status)

    stop = False

    def _handle_stop(signum: int | None = None, _frame=None) -> None:
        nonlocal stop
        if signum is not None:
            logger.info("Shutdown signal received (%s)", signum)
        stop = True

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    while not stop:
        time.sleep(1)

    telegram.shutdown_fleet()


if __name__ == "__main__":
    main()
