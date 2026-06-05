"""Binance Futures REST health probe — pause fleet + Telegram on block."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import telegram_notify as telegram

logger = logging.getLogger(__name__)

FAPI_BASE = os.getenv("FAPI_BASE", "https://fapi.binance.com").rstrip("/")
PING_PATH = "/fapi/v1/ping"
TIMEOUT_SEC = max(3, int(os.getenv("FAPI_WATCH_TIMEOUT_SEC", "10")))
INTERVAL_SEC = max(60, int(os.getenv("FAPI_WATCH_INTERVAL_SEC", "900")))
COOLDOWN_SEC = max(60, int(os.getenv("FAPI_WATCH_COOLDOWN_SEC", "3600")))
LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
STATE_PATH = LOG_DIR / "fapi_watch.state"
ROOT = Path(__file__).resolve().parent

_watch_stop = threading.Event()
_watch_thread: threading.Thread | None = None


def is_enabled() -> bool:
    return os.getenv("FAPI_WATCH_ENABLED", "true").lower() in ("1", "true", "yes")


def _load_state() -> dict:
    default = {"status": "unknown", "last_http_code": None, "last_check_at": None}
    if not STATE_PATH.exists():
        return dict(default)
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return {**default, **data}
    except (OSError, json.JSONDecodeError):
        pass
    return dict(default)


def _save_state(state: dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    state["last_check_at"] = time.time()
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def probe() -> tuple[bool, int | None, str]:
    try:
        import execution

        if execution._fapi_in_backoff():
            until = execution._read_shared_fapi_backoff_until()
            return (
                False,
                429,
                f"Skipping probe — fleet REST backoff until {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(until))}",
            )
    except Exception:
        pass

    url = f"{FAPI_BASE}{PING_PATH}"
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SEC) as response:
            code = response.getcode()
            body = response.read().decode("utf-8", errors="replace").strip()
    except urllib.error.HTTPError as exc:
        code = exc.code
        body = exc.read().decode("utf-8", errors="replace").strip()
        try:
            import execution

            execution._apply_fapi_error_backoff(body)
        except Exception:
            pass
    except (urllib.error.URLError, TimeoutError) as exc:
        return False, None, str(exc)

    lower = body.lower()
    if code != 200:
        return False, code, body[:400] or f"HTTP {code}"
    if "cloudfront" in lower or "request blocked" in lower:
        return False, code, body[:400]
    if body in ("", "{}"):
        return True, code, body or "{}"
    try:
        json.loads(body)
        return True, code, body[:200]
    except json.JSONDecodeError:
        return False, code, body[:400]


def _cooldown_elapsed(state: dict, key: str) -> bool:
    last = state.get(key)
    if last is None:
        return True
    try:
        return time.time() - float(last) >= COOLDOWN_SEC
    except (TypeError, ValueError):
        return True


def _pause_trading() -> None:
    if not telegram.is_trading_paused():
        telegram.set_trading_paused(True, updated_by="fapi_watch")


def _notify_blocked(code: int | None, detail: str) -> bool:
    host = os.uname().nodename if hasattr(os, "uname") else "server"
    code_s = str(code) if code is not None else "n/a"
    snippet = detail.replace("\n", " ").strip()
    if len(snippet) > 120:
        snippet = snippet[:117] + "..."
    ban_hint = " (IP ban — wait or change VPS IP)" if code == 418 else ""
    text = (
        f"Binance Futures REST blocked on {host}\n"
        f"URL: {FAPI_BASE}{PING_PATH}\n"
        f"HTTP {code_s}{ban_hint} — trading paused (same as /stop)\n"
        f"{snippet}\n"
        "Resume manually with /start when curl ping returns 200."
    )
    return telegram.send_bot_sync(text)


def _notify_recovered() -> bool:
    host = os.uname().nodename if hasattr(os, "uname") else "server"
    text = (
        f"Binance Futures REST OK again on {host}\n"
        f"{FAPI_BASE}{PING_PATH} → HTTP 200\n"
        "Trading is still paused — send /start to resume."
    )
    return telegram.send_bot_sync(text)


def _kill_fleet() -> None:
    script = ROOT / "run-all.sh"
    if not script.is_file():
        logger.warning("run-all.sh not found — cannot stop fleet")
        return
    subprocess.run([str(script), "stop"], cwd=ROOT, check=False)


def run_check(*, dry_run: bool = False, kill_fleet: bool = False) -> int:
    """Single probe. Returns 0 if OK, 2 if blocked."""
    ok, code, detail = probe()
    state = _load_state()
    prev_status = state.get("status")
    state["last_http_code"] = code

    if not ok and detail.startswith("Skipping probe"):
        logger.debug("fapi_watch probe skipped: %s", detail)
        _save_state(state)
        return 0

    if ok:
        state["status"] = "ok"
        try:
            import execution

            execution.clear_fapi_backoff(reason="fapi_watch_ping_ok")
        except Exception:
            pass
        if prev_status == "blocked" and _cooldown_elapsed(state, "notified_recovered_at"):
            if not dry_run and telegram.is_configured():
                if _notify_recovered():
                    state["notified_recovered_at"] = time.time()
            logger.info("fapi_watch OK HTTP %s", code)
        else:
            logger.debug("fapi_watch OK HTTP %s", code)
        _save_state(state)
        return 0

    state["status"] = "blocked"
    logger.warning("fapi_watch BLOCKED HTTP %s: %s", code, detail[:200])

    if dry_run:
        _save_state(state)
        return 2

    _pause_trading()
    should_notify = prev_status != "blocked" or _cooldown_elapsed(state, "notified_blocked_at")
    if should_notify and telegram.is_configured():
        if _notify_blocked(code, detail):
            state["notified_blocked_at"] = time.time()
    elif not telegram.is_configured():
        logger.warning("Telegram not configured — cannot alert fapi block")

    if kill_fleet:
        _kill_fleet()

    _save_state(state)
    return 2


def _watch_loop() -> None:
    logger.info(
        "fapi_watch started (every %ss, notify cooldown %ss)",
        INTERVAL_SEC,
        COOLDOWN_SEC,
    )
    while not _watch_stop.wait(INTERVAL_SEC):
        try:
            run_check()
        except Exception:
            logger.exception("fapi_watch check failed")


def start_background_watch() -> threading.Thread | None:
    """Daemon thread; safe to call once from telegram_fleet."""
    global _watch_thread
    if not is_enabled():
        logger.info("fapi_watch disabled (FAPI_WATCH_ENABLED=false)")
        return None
    if _watch_thread is not None and _watch_thread.is_alive():
        return _watch_thread

    _watch_stop.clear()
    _watch_thread = threading.Thread(
        target=_watch_loop,
        name="fapi-watch",
        daemon=True,
    )
    _watch_thread.start()
    try:
        import execution

        if not execution._fapi_in_backoff():
            run_check()
    except Exception:
        run_check()
    return _watch_thread


def stop_background_watch() -> None:
    _watch_stop.set()
    if _watch_thread is not None:
        _watch_thread.join(timeout=INTERVAL_SEC + 5)
