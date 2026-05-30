"""Telegram notifications — same style as trade-binance-websocket-order-blocks."""

from __future__ import annotations

import json
import logging
import os
import threading
import urllib.error
import urllib.request

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()


def is_configured() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def _send_sync(text: str) -> bool:
    if not is_configured():
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": text}).encode()
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status == 200
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        logger.warning("Telegram send failed: %s", exc)
        return False


def _send_async(text: str) -> None:
    if not is_configured():
        return
    thread = threading.Thread(target=_send_sync, args=(text,), daemon=True, name="telegram-send")
    thread.start()


def position_emoji(direction: str) -> str:
    match direction.upper():
        case "LONG":
            return "🍏"
        case "SHORT":
            return "🍎"
        case _:
            return "🤖"


def send_bot(message: str) -> None:
    _send_async(f"🤖 {message}")


def send_position(direction: str, message: str) -> None:
    _send_async(f"{position_emoji(direction)} {message}")


def send_tp(message: str) -> None:
    _send_async(f"🥳 {message}")


def send_sl(message: str) -> None:
    _send_async(f"😢 {message}")


def send_raw(message: str) -> None:
    _send_async(message)


def format_tp_label(tp: str, trailing: bool, callback_rate: float) -> str:
    if trailing:
        return f"TP: {tp} (trail {callback_rate}%)"
    return f"TP: {tp} (fixed)"


def notify_valid_entry(
    symbol: str,
    direction: str,
    entry: str,
    confidence: int,
    reasons: str,
    trend_bias: str,
    sl: str | None = None,
    tp1: str | None = None,
) -> None:
    lines = [
        f"{symbol.upper()} futures",
        f"Valid entry {direction} · {confidence}%",
        f"Entry: {entry} | HTF: {trend_bias}",
    ]
    if sl and tp1:
        lines.append(f"Plan SL: {sl} | TP1: {tp1}")
    if reasons:
        lines.append(reasons)
    send_position(direction, "\n".join(lines))


def notify_dry_run(symbol: str, direction: str, entry: str) -> None:
    send_bot(f"DRY-RUN: {symbol.upper()} {direction} | Entry {entry}")


def notify_live_open(
    symbol: str,
    direction: str,
    entry: str,
    sl: str,
    tp_label: str,
    vol_usdt: str,
) -> None:
    send_position(
        direction,
        f"{symbol.upper()} futures\n"
        f"LIMIT #OPEN {direction}\n"
        f"Entry: {entry} | {tp_label} | SL: {sl} | Vol: {vol_usdt} USDT",
    )


def notify_order_failed(symbol: str, direction: str) -> None:
    send_raw(f"❌ {symbol.upper()} futures — ORDER FAILED ({direction})")


def notify_started(
    symbol: str,
    execution_enabled: bool,
    execution_mode: str,
    dash_port: int,
    ui_enabled: bool,
    interval: str,
) -> None:
    if execution_enabled:
        mode_label = execution_mode.upper()
    else:
        mode_label = "OFF"
    ui_label = "on" if ui_enabled else "headless"
    send_bot(
        f"Dashboard started\n"
        f"Mode: {mode_label} | Symbol: {symbol.upper()} | Interval: {interval} | "
        f"Port: {dash_port} | UI: {ui_label}"
    )


def notify_stopped() -> None:
    send_bot("Dashboard stopped")
