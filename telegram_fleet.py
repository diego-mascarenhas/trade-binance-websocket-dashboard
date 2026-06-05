#!/usr/bin/env python3
"""Single Telegram command listener for the whole run-all fleet."""

from __future__ import annotations

import html
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

import fapi_watch
import telegram_notify as telegram

logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
PAIRS_FILE = SCRIPT_DIR / "hub" / "pairs.json"
HUB_PORT = int(os.getenv("HUB_PORT", "8050"))
FLEET_STATUS_HOST = os.getenv("FLEET_STATUS_HOST", "127.0.0.1")
FLEET_STATUS_TIMEOUT = float(os.getenv("FLEET_STATUS_TIMEOUT", "2.5"))

SEP_SECTION = "────────"
SEP_SYMBOL = "· · ·"


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


def _esc(value: object) -> str:
    return html.escape(str(value), quote=False)


def _signal_emoji(signal: str) -> str:
    if signal == "LONG":
        return "🟢"
    if signal == "SHORT":
        return "🔴"
    return "⚪"


def _trading_emoji() -> str:
    label = telegram.trading_state_label()
    if label == "active":
        return "▶️"
    if label == "paused":
        return "⏸️"
    return "⏹️"


def _pnl_badge(pct: float | None) -> str:
    if pct is None:
        return ""
    return f"<b>{_esc(f'{pct:+.2f}%')}</b>"


def _section_header(emoji: str, title: str, count: int, *, leading_blank: bool = False) -> str:
    lead = "\n" if leading_blank else ""
    return f"{lead}{emoji} <b>{title}</b> ({count})\n{SEP_SECTION}"


def _join_symbol_blocks(blocks: list[str]) -> str:
    if not blocks:
        return ""
    return f"\n{SEP_SYMBOL}\n".join(blocks)


def _setup_block(data: dict) -> str:
    signal = data.get("signal", "NEUTRAL")
    confidence = int(data.get("confidence", 0))
    smc = data.get("smc_pattern") or data.get("smc_state_short") or "—"
    symbol = _esc(data.get("symbol", "—"))
    return (
        f"{_signal_emoji(signal)} <b>{symbol}</b>\n"
        f"   {_esc(signal)} <b>{confidence}%</b> · <i>{_esc(smc)}</i>"
    )


def _open_block(data: dict) -> str:
    symbol = _esc(data.get("symbol", "—"))
    pos = _esc(data.get("position") or "Open")
    pct = data.get("position_pnl_pct")
    pnl = _pnl_badge(pct)
    head = f"<b>{symbol}</b>  {pnl}" if pnl else f"<b>{symbol}</b>"
    return f"{head}\n   {pos}"


def build_fleet_status() -> str:
    pairs = _load_pairs()
    parts = [
        f"<b>Fleet</b> · {len(pairs)} pairs",
        f"{_trading_emoji()} Trading: <b>{_esc(telegram.trading_state_label())}</b>",
    ]
    try:
        import execution

        exp = execution.get_fleet_side_exposure()
        if exp.get("enabled"):
            parts.append(
                f"⚖️ L <b>{exp['long_total_usdt']:.0f}</b> / S <b>{exp['short_total_usdt']:.0f}</b> USDT "
                f"(imb {exp['imbalance_pct']:.0f}% · max +{exp['max_pct']:.0f}%)"
            )
    except Exception as exc:
        logger.debug("Fleet exposure unavailable: %s", exc)

    trade_blocks: list[str] = []
    watch_blocks: list[str] = []
    open_blocks: list[str] = []
    offline = 0

    for pair in pairs:
        port = int(pair.get("port", 0))
        data = _fetch_pair_summary(port)
        if not data:
            offline += 1
            continue

        action = data.get("action", "WATCH")
        if data.get("position_open"):
            open_blocks.append(_open_block(data))
        if action == "TRADE":
            trade_blocks.append(_setup_block(data))
        elif action == "WATCH":
            watch_blocks.append(_setup_block(data))

    has_content = False
    if trade_blocks:
        has_content = True
        parts.append(_section_header("🔥", "TRADE", len(trade_blocks), leading_blank=True))
        parts.append(_join_symbol_blocks(trade_blocks[:10]))
    if watch_blocks:
        has_content = True
        parts.append(_section_header("👀", "WATCH", len(watch_blocks), leading_blank=True))
        parts.append(_join_symbol_blocks(watch_blocks[:15]))
    if open_blocks:
        has_content = True
        parts.append(_section_header("💼", "OPEN", len(open_blocks), leading_blank=True))
        parts.append(_join_symbol_blocks(open_blocks[:10]))
    if offline:
        if has_content:
            parts.append("")
        parts.append(SEP_SECTION)
        parts.append(f"⚠️ Offline: <b>{offline}</b>")
    if not has_content:
        parts.append(f"\n{SEP_SECTION}\n<i>All pairs idle · no setups or positions</i>")

    return "\n".join(parts)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    os.environ["TELEGRAM_COMMANDS_ENABLED"] = "true"

    pairs = _load_pairs()
    telegram.set_fleet_running(True, updated_by="run-all")
    telegram.notify_fleet_started(len(pairs), HUB_PORT)
    telegram.start_command_listener(build_fleet_status)
    fapi_watch.start_background_watch()

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

    fapi_watch.stop_background_watch()
    telegram.shutdown_fleet()


if __name__ == "__main__":
    main()
