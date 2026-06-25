"""Notify running dashboard processes after symbol_config changes."""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
PAIRS_FILE = ROOT / "hub" / "pairs.json"
load_dotenv(ROOT / ".env")


def _parse_pairs_items(raw: str) -> list[tuple[str, int | None]]:
    items: list[tuple[str, int | None]] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            symbol_part, port_part = part.split(":", 1)
            symbol = symbol_part.strip().upper()
            try:
                port = int(port_part.strip())
            except (TypeError, ValueError):
                port = None
        else:
            symbol = part.upper()
            port = None
        if symbol:
            items.append((symbol, port))
    return items


def _pairs_from_env() -> list[tuple[str, int | None]]:
    raw = os.getenv("PAIRS", "").strip()
    if not raw:
        return []
    return _parse_pairs_items(raw)


def _pairs_from_json() -> list[tuple[str, int | None]]:
    if not PAIRS_FILE.is_file():
        return []
    try:
        pairs = json.loads(PAIRS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read %s: %s", PAIRS_FILE, exc)
        return []

    items: list[tuple[str, int | None]] = []
    for item in pairs:
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        try:
            port = int(item["port"])
        except (KeyError, TypeError, ValueError):
            port = None
        items.append((symbol, port))
    return items


def load_fleet_pairs() -> list[tuple[str, int | None]]:
    """Fleet symbol/port list — PAIRS in .env first, then hub/pairs.json."""
    items = _pairs_from_env()
    if items:
        return items
    return _pairs_from_json()


def load_fleet_symbols() -> list[str]:
    symbols: list[str] = []
    for symbol, _port in load_fleet_pairs():
        if symbol not in symbols:
            symbols.append(symbol)
    return symbols


def _load_pair_port(symbol: str) -> int | None:
    symbol = symbol.strip().upper()
    for pair_symbol, port in load_fleet_pairs():
        if pair_symbol == symbol:
            return port
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
            "error": f"No dashboard port found for {symbol.upper()} (check PAIRS or hub/pairs.json)",
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
