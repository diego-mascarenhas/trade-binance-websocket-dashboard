"""Telegram notifications and fleet-wide /start /stop /status commands."""

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
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_POLL_INTERVAL = float(os.getenv("TELEGRAM_POLL_INTERVAL", "2"))
LOG_DIR = os.getenv("LOG_DIR", "logs")

_pause_lock = threading.Lock()
_update_offset = 0
_listener_stop = threading.Event()
_shutdown_notified = False
_shutdown_lock = threading.Lock()
_polling_conflict_warned = False


def _fleet_state_path() -> Path:
    path = Path(LOG_DIR)
    path.mkdir(parents=True, exist_ok=True)
    return path / "fleet.state"


def read_fleet_state() -> dict[str, Any]:
    default: dict[str, Any] = {
        "trading_paused": False,
        "fleet_running": False,
    }
    state_path = _fleet_state_path()
    if not state_path.exists():
        return dict(default)
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return dict(default)
        return {**default, **data}
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read fleet state: %s", exc)
        return dict(default)


def write_fleet_state(*, updated_by: str, **fields: Any) -> dict[str, Any]:
    with _pause_lock:
        state = read_fleet_state()
        state.update(fields)
        state["updated_at"] = time.time()
        state["updated_by"] = updated_by
        _fleet_state_path().write_text(json.dumps(state, indent=2), encoding="utf-8")
        return dict(state)


def is_trading_paused() -> bool:
    with _pause_lock:
        return bool(read_fleet_state().get("trading_paused"))


def is_fleet_running() -> bool:
    with _pause_lock:
        return bool(read_fleet_state().get("fleet_running"))


def set_trading_paused(paused: bool, *, updated_by: str = "telegram") -> None:
    write_fleet_state(trading_paused=paused, updated_by=updated_by)


def set_fleet_running(running: bool, *, updated_by: str = "run-all") -> None:
    write_fleet_state(fleet_running=running, updated_by=updated_by)


def trading_state_label() -> str:
    if not is_fleet_running():
        return "fleet stopped"
    return "paused" if is_trading_paused() else "active"


def is_configured() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def commands_enabled() -> bool:
    return os.getenv("TELEGRAM_COMMANDS_ENABLED", "").lower() in ("1", "true", "yes")


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


def send_bot_sync(message: str) -> bool:
    return _send_sync(f"🤖 {message}")


def send_position(direction: str, message: str) -> None:
    _send_async(f"{position_emoji(direction)} {message}")


def send_tp(message: str) -> None:
    _send_async(f"🥳 {message}")


def send_trailing(message: str) -> None:
    _send_async(f"🏄 {message}")


def send_sl(message: str) -> None:
    _send_async(f"😢 {message}")


def send_shield(message: str) -> None:
    _send_async(f"🛡️ {message}")


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
        lines.append(f"SL: {sl} | TP: {tp1}")
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


def notify_tp_exit(symbol: str, message: str, *, trailing: bool = False) -> None:
    body = f"{symbol.upper()} futures\n{message}"
    if trailing:
        send_trailing(body)
    else:
        send_tp(body)


def notify_sl_exit(symbol: str, message: str) -> None:
    send_sl(f"{symbol.upper()} futures\n{message}")


def notify_be_exit(symbol: str, message: str) -> None:
    send_shield(f"{symbol.upper()} futures\n{message}")


def notify_position_closed(symbol: str, message: str) -> None:
    send_bot(f"{symbol.upper()} futures\n{message}")


def notify_fleet_started(pair_count: int, hub_port: int) -> None:
    send_bot(
        f"Fleet started · {pair_count} pairs · hub :{hub_port}\n"
        f"Trading: {trading_state_label()}\n"
        f"Commands: /status · /stop · /start"
    )


def notify_started(
    symbol: str,
    execution_enabled: bool,
    execution_mode: str,
    dash_port: int,
    ui_enabled: bool,
    interval: str,
) -> None:
    if not os.getenv("TELEGRAM_NOTIFY_PAIR_START", "").lower() in ("1", "true", "yes"):
        return
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


def notify_fleet_stopped() -> None:
    global _shutdown_notified
    with _shutdown_lock:
        if _shutdown_notified:
            return
        _shutdown_notified = True
    if not send_bot_sync("Fleet stopped — all dashboards offline"):
        logger.warning("Telegram fleet stop notification was not delivered")


def notify_stopped(symbol: str | None = None) -> None:
    if symbol:
        return
    notify_fleet_stopped()


def shutdown_fleet() -> None:
    """Stop fleet command listener and notify once (run-all stop)."""
    set_fleet_running(False, updated_by="run-all")
    stop_command_listener()
    notify_fleet_stopped()


def shutdown(symbol: str | None = None) -> None:
    """Per-pair shutdown hook — fleet stop is handled by shutdown_fleet()."""
    if symbol:
        return
    shutdown_fleet()


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
        set_trading_paused(True, updated_by="telegram")
        send_bot("Fleet trading paused — no new orders on any pair")
        return
    if command == "/start":
        set_trading_paused(False, updated_by="telegram")
        send_bot("Fleet trading resumed — orders enabled on all pairs")
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
    if not is_configured() or not commands_enabled():
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
