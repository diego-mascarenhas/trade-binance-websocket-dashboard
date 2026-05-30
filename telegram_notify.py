"""Telegram notifications and /start /stop /status commands."""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_POLL_INTERVAL = float(os.getenv("TELEGRAM_POLL_INTERVAL", "2"))

_trading_paused = False
_pause_lock = threading.Lock()
_update_offset = 0
_listener_stop = threading.Event()
_polling_conflict_warned = False


def is_configured() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def is_trading_paused() -> bool:
    with _pause_lock:
        return _trading_paused


def set_trading_paused(paused: bool) -> None:
    global _trading_paused
    with _pause_lock:
        _trading_paused = paused


def trading_state_label() -> str:
    return "paused" if is_trading_paused() else "active"


def _send_sync(text: str, chat_id: str | None = None) -> bool:
    if not is_configured():
        return False
    target_chat = chat_id or TELEGRAM_CHAT_ID
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({"chat_id": target_chat, "text": text}).encode()
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
        f"Port: {dash_port} | UI: {ui_label}\n"
        f"Commands: /status · /stop · /start"
    )


def notify_stopped() -> None:
    send_bot("Dashboard stopped")


def _telegram_api_get(method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    query = urllib.parse.urlencode(params or {}, doseq=True)
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    if query:
        url = f"{url}?{query}"
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode())
    except urllib.error.HTTPError:
        raise
    if not payload.get("ok"):
        raise RuntimeError(payload.get("description", "Telegram API error"))
    return payload


def _http_error_detail(exc: urllib.error.HTTPError) -> str:
    try:
        body = json.loads(exc.read().decode())
        return str(body.get("description") or body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return exc.reason or str(exc)


def _prepare_command_polling() -> None:
    """Use long-polling: drop webhook if set (webhook and getUpdates cannot run together)."""
    try:
        _telegram_api_get("deleteWebhook", {"drop_pending_updates": "false"})
    except Exception as exc:
        logger.warning("Telegram deleteWebhook failed: %s", exc)


def _parse_command(text: str) -> str | None:
    if not text or not text.startswith("/"):
        return None
    command = text.split()[0].split("@")[0].lower()
    if command in ("/start", "/stop", "/status"):
        return command
    return None


def _authorized_chat(chat_id: Any) -> bool:
    return str(chat_id) == str(TELEGRAM_CHAT_ID)


def _handle_command(command: str, status_provider: Callable[[], str]) -> None:
    if command == "/stop":
        set_trading_paused(True)
        send_bot("Trading paused — no new orders will be sent")
        return
    if command == "/start":
        set_trading_paused(False)
        send_bot("Trading resumed — orders enabled again")
        return
    if command == "/status":
        send_bot(status_provider())
        return


def _command_loop(status_provider: Callable[[], str]) -> None:
    global _update_offset, _polling_conflict_warned
    logger.info("Telegram command listener started (/start /stop /status)")
    while not _listener_stop.is_set():
        try:
            params: dict[str, Any] = {
                "timeout": 0,
                "allowed_updates": json.dumps(["message"]),
            }
            if _update_offset:
                params["offset"] = _update_offset
            result = _telegram_api_get("getUpdates", params)
            for update in result.get("result", []):
                _update_offset = int(update["update_id"]) + 1
                message = update.get("message") or {}
                chat = message.get("chat") or {}
                if not _authorized_chat(chat.get("id")):
                    continue
                text = message.get("text") or ""
                command = _parse_command(text.strip())
                if command:
                    _handle_command(command, status_provider)
        except urllib.error.HTTPError as exc:
            if exc.code == 409:
                if not _polling_conflict_warned:
                    logger.warning(
                        "Telegram 409 Conflict: another process is already polling getUpdates "
                        "with this TELEGRAM_BOT_TOKEN (only one poller allowed). "
                        "sendMessage from other scripts is OK; /status /stop /start disabled here. %s",
                        _http_error_detail(exc),
                    )
                    _polling_conflict_warned = True
                if _listener_stop.wait(max(TELEGRAM_POLL_INTERVAL, 15)):
                    break
                continue
            logger.exception("Telegram command poll failed: %s", _http_error_detail(exc))
        except Exception:
            logger.exception("Telegram command poll failed")
        if _listener_stop.wait(TELEGRAM_POLL_INTERVAL):
            break
    logger.info("Telegram command listener stopped")


def start_command_listener(status_provider: Callable[[], str]) -> None:
    if not is_configured():
        return
    global _update_offset
    _prepare_command_polling()
    try:
        bootstrap = _telegram_api_get("getUpdates", {"offset": -1, "limit": 1})
        updates = bootstrap.get("result") or []
        if updates:
            _update_offset = int(updates[-1]["update_id"]) + 1
    except urllib.error.HTTPError as exc:
        if exc.code == 409:
            logger.warning(
                "Telegram commands unavailable: bot token already used for getUpdates elsewhere"
            )
        else:
            logger.warning("Telegram bootstrap getUpdates failed: %s", _http_error_detail(exc))
    except Exception:
        logger.warning("Telegram bootstrap getUpdates failed; old messages may replay")
    thread = threading.Thread(
        target=_command_loop,
        args=(status_provider,),
        daemon=True,
        name="telegram-commands",
    )
    thread.start()


def stop_command_listener() -> None:
    _listener_stop.set()
