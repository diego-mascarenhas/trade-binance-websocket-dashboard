"""Notify running dashboard processes after symbol_config changes."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
PAIRS_FILE = ROOT / "hub" / "pairs.json"


def load_fleet_symbols() -> list[str]:
    if not PAIRS_FILE.is_file():
        return []
    try:
        pairs = json.loads(PAIRS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read %s: %s", PAIRS_FILE, exc)
        return []
    symbols: list[str] = []
    for item in pairs:
        symbol = str(item.get("symbol", "")).strip().upper()
        if symbol:
            symbols.append(symbol)
    return symbols


def _load_pair_port(symbol: str) -> int | None:
    symbol = symbol.strip().upper()
    if not PAIRS_FILE.is_file():
        return None
    try:
        pairs = json.loads(PAIRS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read %s: %s", PAIRS_FILE, exc)
        return None

    for item in pairs:
        if str(item.get("symbol", "")).upper() == symbol:
            try:
                return int(item["port"])
            except (KeyError, TypeError, ValueError):
                return None
    return None


def notify_dashboard_reload(symbol: str, *, timeout: float = 5.0, retries: int = 2) -> dict:
    last_error: dict | None = None
    for attempt in range(max(1, retries)):
        result = _notify_dashboard_reload_once(symbol, timeout=timeout)
        if result.get("ok"):
            if attempt > 0:
                result["retried"] = attempt
            return result
        last_error = result
    return last_error or {"ok": False, "symbol": symbol.upper(), "error": "reload failed"}


def _notify_dashboard_reload_once(symbol: str, *, timeout: float) -> dict:
    port = _load_pair_port(symbol)
    if port is None:
        return {
            "ok": False,
            "symbol": symbol.upper(),
            "error": f"No dashboard port found for {symbol.upper()} in hub/pairs.json",
        }

    url = f"http://127.0.0.1:{port}/api/reload-config"
    request = urllib.request.Request(url, method="POST", data=b"{}")
    request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            payload = json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "symbol": symbol.upper(),
            "port": port,
            "error": f"HTTP {exc.code}: {detail[:200]}",
        }
    except urllib.error.URLError as exc:
        return {
            "ok": False,
            "symbol": symbol.upper(),
            "port": port,
            "error": f"Dashboard not reachable on port {port}: {exc.reason}",
        }
    except json.JSONDecodeError:
        payload = {}

    return {
        "ok": True,
        "symbol": symbol.upper(),
        "port": port,
        "reload": payload,
    }
