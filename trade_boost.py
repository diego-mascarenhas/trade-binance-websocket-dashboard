"""One-shot per-direction trade boosts — relax indicator filters for the next valid entry."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from indicators import IndicatorFilterSettings

logger = logging.getLogger(__name__)

LOG_DIR = os.getenv("LOG_DIR", "logs")
BOOST_DIR = Path(LOG_DIR) / "trade_boost"
LOCK_SUFFIX = ".lock"

TRADE_BOOST_ENABLED = os.getenv("TRADE_BOOST_ENABLED", "true").lower() in ("1", "true", "yes")
TRADE_BOOST_TTL_SEC = int(os.getenv("TRADE_BOOST_TTL_SEC", "3600"))
TRADE_BOOST_ADX_DELTA = float(os.getenv("TRADE_BOOST_ADX_DELTA", "8"))
TRADE_BOOST_ADX_FLOOR = float(os.getenv("TRADE_BOOST_ADX_FLOOR", "8"))
TRADE_BOOST_ADX_USE_1M = os.getenv("TRADE_BOOST_ADX_USE_1M", "true").lower() in ("1", "true", "yes")
TRADE_BOOST_RSI_LONG_DELTA = float(os.getenv("TRADE_BOOST_RSI_LONG_DELTA", "10"))
TRADE_BOOST_RSI_SHORT_DELTA = float(os.getenv("TRADE_BOOST_RSI_SHORT_DELTA", "10"))
TRADE_BOOST_DISABLE_MACD = os.getenv("TRADE_BOOST_DISABLE_MACD", "true").lower() in ("1", "true", "yes")
TRADE_BOOST_RELAX_TREND = os.getenv("TRADE_BOOST_RELAX_TREND", "true").lower() in ("1", "true", "yes")
TRADE_BOOST_MIN_CONF_DELTA = float(os.getenv("TRADE_BOOST_MIN_CONF_DELTA", "15"))
TRADE_BOOST_SKIP_COOLDOWN = os.getenv("TRADE_BOOST_SKIP_COOLDOWN", "true").lower() in ("1", "true", "yes")
TRADE_BOOST_RETRY_SEC = float(os.getenv("TRADE_BOOST_RETRY_SEC", "3"))
# When armed: skip ADX/RSI/MACD filters entirely (fixes adx_low on HTF while 1m is strong)
TRADE_BOOST_BYPASS_INDICATORS = os.getenv("TRADE_BOOST_BYPASS_INDICATORS", "true").lower() in (
    "1",
    "true",
    "yes",
)
TRADE_BOOST_DISABLE_RSI = os.getenv("TRADE_BOOST_DISABLE_RSI", "true").lower() in ("1", "true", "yes")


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _normalize_direction(direction: str) -> str:
    value = direction.strip().upper()
    if value not in ("LONG", "SHORT"):
        raise ValueError(f"Invalid direction: {direction!r}")
    return value


def _boost_path(symbol: str) -> Path:
    return BOOST_DIR / f"{_normalize_symbol(symbol)}.json"


def _lock_path(symbol: str) -> Path:
    return BOOST_DIR / f"{_normalize_symbol(symbol)}{LOCK_SUFFIX}"


def _acquire_lock(symbol: str, timeout: float = 2.0) -> bool:
    BOOST_DIR.mkdir(parents=True, exist_ok=True)
    path = _lock_path(symbol)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            return True
        except FileExistsError:
            time.sleep(0.02)
    return False


def _release_lock(symbol: str) -> None:
    try:
        _lock_path(symbol).unlink(missing_ok=True)
    except OSError:
        pass


def _read_state(symbol: str) -> dict[str, Any]:
    path = _boost_path(symbol)
    if not path.is_file():
        return {"boosts": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("trade_boost read failed for %s: %s", symbol, exc)
        return {"boosts": {}}
    if not isinstance(payload, dict):
        return {"boosts": {}}
    payload.setdefault("boosts", {})
    return payload


def _write_state(symbol: str, payload: dict[str, Any]) -> None:
    BOOST_DIR.mkdir(parents=True, exist_ok=True)
    path = _boost_path(symbol)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)


def _purge_expired(payload: dict[str, Any]) -> bool:
    boosts = payload.get("boosts") or {}
    if not isinstance(boosts, dict):
        payload["boosts"] = {}
        return True
    changed = False
    now = time.time()
    for direction in list(boosts.keys()):
        entry = boosts.get(direction)
        if not isinstance(entry, dict):
            boosts.pop(direction, None)
            changed = True
            continue
        expires_at = entry.get("expires_at")
        if expires_at is not None and float(expires_at) <= now:
            boosts.pop(direction, None)
            changed = True
    return changed


@dataclass
class BoostApplyInfo:
    direction: str
    relaxed: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {"direction": self.direction, "relaxed": self.relaxed}


def _compute_relaxed(base: IndicatorFilterSettings, direction: str) -> dict[str, Any]:
    relaxed: dict[str, Any] = {}
    if TRADE_BOOST_BYPASS_INDICATORS:
        relaxed["bypass_indicators"] = True
        relaxed["adx_min_trend"] = TRADE_BOOST_ADX_FLOOR
        if TRADE_BOOST_ADX_USE_1M:
            relaxed["adx_use_htf"] = False
        if TRADE_BOOST_DISABLE_RSI and base.rsi_enabled:
            relaxed["rsi_enabled"] = False
        if TRADE_BOOST_DISABLE_MACD and base.macd_enabled:
            relaxed["macd_enabled"] = False
        if TRADE_BOOST_RELAX_TREND:
            relaxed["relax_trend_align"] = True
        return relaxed

    adx_min = max(TRADE_BOOST_ADX_FLOOR, base.adx_min_trend - TRADE_BOOST_ADX_DELTA)
    if adx_min < base.adx_min_trend:
        relaxed["adx_min_trend"] = adx_min
    if TRADE_BOOST_ADX_USE_1M and base.adx_use_htf:
        relaxed["adx_use_htf"] = False
    if direction == "LONG" and TRADE_BOOST_RSI_LONG_DELTA > 0:
        relaxed["rsi_long_max"] = base.rsi_long_max + TRADE_BOOST_RSI_LONG_DELTA
    if direction == "SHORT" and TRADE_BOOST_RSI_SHORT_DELTA > 0:
        relaxed["rsi_short_min"] = base.rsi_short_min - TRADE_BOOST_RSI_SHORT_DELTA
    if TRADE_BOOST_DISABLE_MACD and base.macd_enabled:
        relaxed["macd_enabled"] = False
    if TRADE_BOOST_RELAX_TREND:
        relaxed["relax_trend_align"] = True
    return relaxed


def _apply_relaxed(base: IndicatorFilterSettings, relaxed: dict[str, Any]) -> IndicatorFilterSettings:
    updates: dict[str, Any] = {}
    for key, value in relaxed.items():
        if key == "relax_trend_align":
            continue
        if hasattr(base, key):
            updates[key] = value
    return replace(base, **updates) if updates else base


def _remaining_sec(expires_at: Any) -> int | None:
    if expires_at is None:
        return None
    return max(0, int(float(expires_at) - time.time()))


def format_remaining(seconds: int | None) -> str:
    if seconds is None:
        return "—"
    if seconds <= 0:
        return "0:00"
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def _status_from_payload(
    symbol: str,
    payload: dict[str, Any],
    *,
    preview_base: IndicatorFilterSettings | None = None,
) -> dict[str, Any]:
    boosts = payload.get("boosts") or {}
    preview = preview_base or IndicatorFilterSettings()
    out: dict[str, Any] = {}
    for direction in ("LONG", "SHORT"):
        entry = boosts.get(direction)
        if not entry:
            continue
        relaxed = _compute_relaxed(preview, direction)
        remaining = _remaining_sec(entry.get("expires_at"))
        out[direction] = {
            "active": True,
            "created_at": entry.get("created_at"),
            "created_by": entry.get("created_by"),
            "expires_at": entry.get("expires_at"),
            "remaining_sec": remaining,
            "remaining_display": format_remaining(remaining),
            "relaxed": relaxed,
            "summary": _format_summary(relaxed),
        }
    return {
        "symbol": symbol,
        "enabled": TRADE_BOOST_ENABLED,
        "boosts": out,
        "ttl_sec": TRADE_BOOST_TTL_SEC,
    }


def get_status(
    symbol: str,
    *,
    preview_base: IndicatorFilterSettings | None = None,
) -> dict[str, Any]:
    symbol = _normalize_symbol(symbol)
    if not _acquire_lock(symbol):
        return {"symbol": symbol, "enabled": TRADE_BOOST_ENABLED, "boosts": {}, "error": "lock_timeout"}
    try:
        payload = _read_state(symbol)
        changed = _purge_expired(payload)
        if changed:
            _write_state(symbol, payload)
        return _status_from_payload(symbol, payload, preview_base=preview_base)
    finally:
        _release_lock(symbol)


def _format_summary(relaxed: dict[str, Any]) -> str:
    if relaxed.get("bypass_indicators"):
        parts = ["no ADX/RSI/MACD block"]
        if relaxed.get("adx_use_htf") is False:
            parts.append("ADX 1m")
        if relaxed.get("relax_trend_align"):
            parts.append("HTF trend OK")
        return " · ".join(parts)

    parts: list[str] = []
    if "adx_min_trend" in relaxed:
        parts.append(f"ADX≥{relaxed['adx_min_trend']}")
    if relaxed.get("adx_use_htf") is False:
        parts.append("ADX 1m")
    if relaxed.get("rsi_enabled") is False:
        parts.append("no RSI")
    if "rsi_long_max" in relaxed:
        parts.append(f"RSI long≤{relaxed['rsi_long_max']}")
    if "rsi_short_min" in relaxed:
        parts.append(f"RSI short≥{relaxed['rsi_short_min']}")
    if relaxed.get("macd_enabled") is False:
        parts.append("no MACD")
    if relaxed.get("relax_trend_align"):
        parts.append("HTF relax")
    return " · ".join(parts) if parts else "permissive filters"


def activate(
    symbol: str,
    direction: str,
    *,
    created_by: str = "ui",
    base_settings: IndicatorFilterSettings | None = None,
) -> dict[str, Any]:
    if not TRADE_BOOST_ENABLED:
        return {"ok": False, "error": "TRADE_BOOST_ENABLED=false"}

    symbol = _normalize_symbol(symbol)
    direction = _normalize_direction(direction)
    preview = base_settings or IndicatorFilterSettings()

    if not _acquire_lock(symbol):
        return {"ok": False, "error": "Could not acquire boost lock"}

    try:
        payload = _read_state(symbol)
        _purge_expired(payload)
        boosts = payload.setdefault("boosts", {})
        now = time.time()
        for other in ("LONG", "SHORT"):
            if other != direction:
                boosts.pop(other, None)
        boosts[direction] = {
            "created_at": _utc_now(),
            "created_by": created_by,
            "expires_at": now + TRADE_BOOST_TTL_SEC if TRADE_BOOST_TTL_SEC > 0 else None,
        }
        relaxed = _compute_relaxed(preview, direction)
        _write_state(symbol, payload)
        return {
            "ok": True,
            "symbol": symbol,
            "direction": direction,
            "message": f"Boost {direction} armed for next valid entry on {symbol}.",
            "summary": _format_summary(relaxed),
            "status": _status_from_payload(symbol, payload, preview_base=preview),
        }
    finally:
        _release_lock(symbol)


def deactivate(symbol: str, direction: str) -> dict[str, Any]:
    symbol = _normalize_symbol(symbol)
    direction = _normalize_direction(direction)

    if not _acquire_lock(symbol):
        return {"ok": False, "error": "Could not acquire boost lock"}

    try:
        payload = _read_state(symbol)
        boosts = payload.get("boosts") or {}
        removed = boosts.pop(direction, None) is not None
        _write_state(symbol, payload)
        return {
            "ok": True,
            "symbol": symbol,
            "direction": direction,
            "removed": removed,
            "message": f"Boost {direction} cleared for {symbol}." if removed else f"No active {direction} boost.",
            "status": _status_from_payload(symbol, payload),
        }
    finally:
        _release_lock(symbol)


def deactivate_all(symbol: str) -> dict[str, Any]:
    symbol = _normalize_symbol(symbol)
    if not _acquire_lock(symbol):
        return {"ok": False, "error": "Could not acquire boost lock"}

    try:
        payload = _read_state(symbol)
        boosts = payload.get("boosts") or {}
        removed = [d for d in ("LONG", "SHORT") if boosts.pop(d, None) is not None]
        _write_state(symbol, payload)
        return {
            "ok": True,
            "symbol": symbol,
            "removed": removed,
            "message": (
                f"Boost cleared ({', '.join(removed)})."
                if removed
                else "No active boost."
            ),
            "status": _status_from_payload(symbol, payload),
        }
    finally:
        _release_lock(symbol)


def _has_active_boost(payload: dict[str, Any], direction: str) -> bool:
    boosts = payload.get("boosts") or {}
    entry = boosts.get(direction)
    if not entry:
        return False
    expires_at = entry.get("expires_at")
    if expires_at is not None and float(expires_at) <= time.time():
        return False
    return True


def _read_active_boost(symbol: str, direction: str) -> bool:
    if not TRADE_BOOST_ENABLED:
        return False
    symbol = _normalize_symbol(symbol)
    direction = _normalize_direction(direction)
    if not _acquire_lock(symbol):
        return False
    try:
        payload = _read_state(symbol)
        changed = _purge_expired(payload)
        if changed:
            _write_state(symbol, payload)
        return _has_active_boost(payload, direction)
    finally:
        _release_lock(symbol)


def is_active(symbol: str, direction: str) -> bool:
    return _read_active_boost(symbol, direction)


def apply_indicator_settings(
    symbol: str,
    signal: str,
    base: IndicatorFilterSettings,
) -> tuple[IndicatorFilterSettings, BoostApplyInfo | None]:
    if signal not in ("LONG", "SHORT") or not _read_active_boost(symbol, signal):
        return base, None
    relaxed = _compute_relaxed(base, signal)
    settings = _apply_relaxed(base, relaxed)
    return settings, BoostApplyInfo(direction=signal, relaxed=relaxed)


def trend_align_relaxed(symbol: str, signal: str) -> bool:
    if signal not in ("LONG", "SHORT") or not TRADE_BOOST_RELAX_TREND:
        return False
    return _read_active_boost(symbol, signal)


def effective_min_confidence(base_min: int) -> int:
    if TRADE_BOOST_MIN_CONF_DELTA <= 0:
        return base_min
    return max(0, int(base_min - TRADE_BOOST_MIN_CONF_DELTA))


def min_confidence_for(symbol: str, direction: str, base_min: int) -> int:
    if _read_active_boost(symbol, direction):
        return effective_min_confidence(base_min)
    return base_min


def bypasses_indicators(symbol: str, direction: str) -> bool:
    if not TRADE_BOOST_BYPASS_INDICATORS:
        return False
    return _read_active_boost(symbol, direction)


def skips_signal_cooldown(symbol: str, direction: str) -> bool:
    return TRADE_BOOST_SKIP_COOLDOWN and _read_active_boost(symbol, direction)


def consume(symbol: str, direction: str) -> bool:
    """Remove boost after a valid entry is recorded for that direction."""
    symbol = _normalize_symbol(symbol)
    direction = _normalize_direction(direction)

    if not _acquire_lock(symbol):
        logger.warning("trade_boost consume lock failed for %s", symbol)
        return False

    try:
        payload = _read_state(symbol)
        boosts = payload.get("boosts") or {}
        if direction not in boosts:
            return False
        boosts.pop(direction, None)
        _write_state(symbol, payload)
        return True
    finally:
        _release_lock(symbol)
