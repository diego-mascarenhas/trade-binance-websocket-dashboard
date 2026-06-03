"""Server-side proxy to per-pair dashboard APIs (browser cannot reach 8051+ on remote hosts)."""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
PAIRS_FILE = ROOT / "hub" / "pairs.json"
PROXY_HOST = os.getenv("HUB_PROXY_HOST", os.getenv("FLEET_STATUS_HOST", "127.0.0.1"))
PROXY_TIMEOUT = float(os.getenv("HUB_PROXY_TIMEOUT_SEC", os.getenv("FLEET_STATUS_TIMEOUT", "5")))


def load_pairs() -> list[dict[str, Any]]:
    if not PAIRS_FILE.exists():
        return []
    try:
        data = json.loads(PAIRS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read pairs.json: %s", exc)
        return []
    return data if isinstance(data, list) else []


def fetch_pair_summary(port: int, *, host: str | None = None) -> dict[str, Any] | None:
    target = host or PROXY_HOST
    url = f"http://{target}:{port}/api/hub-summary"
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=PROXY_TIMEOUT) as response:
            payload = json.loads(response.read().decode())
            return payload if isinstance(payload, dict) else None
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
        logger.debug("hub-summary %s:%s failed: %s", target, port, exc)
        return None


def fetch_all_summaries(pairs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    items = pairs if pairs is not None else load_pairs()
    if not items:
        return {"summaries": {}, "offline": [], "pairs": []}

    summaries: dict[str, Any] = {}
    offline: list[str] = []
    max_workers = min(16, max(len(items), 1))

    def _one(pair: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
        symbol = str(pair.get("symbol") or "").upper()
        try:
            port = int(pair.get("port"))
        except (TypeError, ValueError):
            return symbol, None
        return symbol, fetch_pair_summary(port)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_one, pair) for pair in items]
        for future in as_completed(futures):
            symbol, payload = future.result()
            if not symbol:
                continue
            if payload:
                summaries[symbol] = payload
            else:
                offline.append(symbol)

    offline.sort()
    return {
        "summaries": summaries,
        "offline": offline,
        "pairs": items,
        "proxy_host": PROXY_HOST,
    }
