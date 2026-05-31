"""Per-symbol config overrides from MySQL (optional) + config/market snapshots for audit."""

from __future__ import annotations

import logging
from typing import Any, Callable

import db_store

logger = logging.getLogger(__name__)

OVERRIDABLE_KEYS: dict[str, Callable[[Any], Any]] = {
    "MIN_CONFIDENCE": int,
    "HTF_INTERVAL": str,
    "INTERVAL": str,
    "REQUIRE_TREND_ALIGN": lambda v: str(v).lower() in ("1", "true", "yes"),
    "SIGNAL_DEBOUNCE_COUNT": int,
    "SIGNAL_COOLDOWN_SEC": int,
    "MIN_PATTERN_RANGE_PCT": float,
    "OB_WALL_RANGE_PCT": float,
    "INDICATOR_FILTERS_ENABLED": lambda v: str(v).lower() in ("1", "true", "yes"),
    "RSI_FILTER_ENABLED": lambda v: str(v).lower() in ("1", "true", "yes"),
    "RSI_LONG_MAX": float,
    "RSI_SHORT_MIN": float,
    "MACD_FILTER_ENABLED": lambda v: str(v).lower() in ("1", "true", "yes"),
    "ADX_FILTER_ENABLED": lambda v: str(v).lower() in ("1", "true", "yes"),
    "ADX_MIN_TREND": float,
    "ADX_USE_HTF": lambda v: str(v).lower() in ("1", "true", "yes"),
    "symbol_trading_enabled": lambda v: str(v).lower() in ("1", "true", "yes"),
}

_symbol_config_version: int = 0
_symbol_trading_enabled: bool = True
_cached_config_snapshot: dict[str, Any] = {}


def config_version() -> int:
    return _symbol_config_version


def symbol_trading_enabled() -> bool:
    return _symbol_trading_enabled


def get_config_snapshot() -> dict[str, Any]:
    return dict(_cached_config_snapshot)


def apply_db_overrides(module_globals: dict[str, Any], symbol: str) -> None:
    global _symbol_config_version, _symbol_trading_enabled, _cached_config_snapshot

    _cached_config_snapshot = build_config_snapshot(module_globals)

    if not db_store.is_enabled():
        return

    db_store.init()
    overrides, version = db_store.get_symbol_config(symbol)
    _symbol_config_version = version

    if not overrides:
        logger.info("%s: no DB config row — using .env defaults", symbol.upper())
        _cached_config_snapshot = build_config_snapshot(module_globals)
        return

    if "symbol_trading_enabled" in overrides:
        _symbol_trading_enabled = OVERRIDABLE_KEYS["symbol_trading_enabled"](
            overrides["symbol_trading_enabled"]
        )

    applied: list[str] = []
    for key, caster in OVERRIDABLE_KEYS.items():
        if key == "symbol_trading_enabled":
            continue
        if key not in overrides or key not in module_globals:
            continue
        try:
            module_globals[key] = caster(overrides[key])
            applied.append(key)
        except (TypeError, ValueError) as exc:
            logger.warning("Invalid DB override %s=%r for %s: %s", key, overrides[key], symbol, exc)

    _cached_config_snapshot = build_config_snapshot(module_globals)

    if applied:
        logger.info(
            "%s: applied DB config v%s overrides: %s",
            symbol.upper(),
            version,
            ", ".join(applied),
        )
    if not _symbol_trading_enabled:
        logger.info("%s: symbol_trading_enabled=false in DB", symbol.upper())

    import execution

    for key in ("POSITION_SIZE_USDT",):
        if key in overrides:
            try:
                execution.POSITION_SIZE_USDT = float(overrides[key])
                applied.append(f"execution.{key}")
            except (TypeError, ValueError) as exc:
                logger.warning("Invalid DB override %s for %s: %s", key, symbol, exc)

    _cached_config_snapshot = build_config_snapshot(module_globals)


def build_config_snapshot(module_globals: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "SYMBOL",
        "INTERVAL",
        "HTF_INTERVAL",
        "MIN_CONFIDENCE",
        "REQUIRE_TREND_ALIGN",
        "SIGNAL_DEBOUNCE_COUNT",
        "SIGNAL_COOLDOWN_SEC",
        "OB_WALL_RANGE_PCT",
        "INDICATOR_FILTERS_ENABLED",
        "RSI_FILTER_ENABLED",
        "RSI_LONG_MAX",
        "RSI_SHORT_MIN",
        "MACD_FILTER_ENABLED",
        "ADX_FILTER_ENABLED",
        "ADX_MIN_TREND",
        "ADX_USE_HTF",
        "ADX_PERIOD",
    ]
    snapshot = {key: module_globals.get(key) for key in keys if key in module_globals}
    snapshot["symbol_config_version"] = _symbol_config_version
    snapshot["symbol_trading_enabled"] = _symbol_trading_enabled
    snapshot["db_enabled"] = db_store.is_enabled()

    import execution

    snapshot["execution_enabled"] = execution.EXECUTION_ENABLED
    snapshot["execute_on_valid_entry"] = execution.EXECUTE_ON_VALID_ENTRY
    snapshot["execution_mode"] = execution.EXECUTION_MODE
    snapshot["position_size_usdt"] = execution.POSITION_SIZE_USDT
    return snapshot


def build_market_snapshot(
    metrics: dict[str, Any],
    *,
    trend_aligned: bool | None = None,
) -> dict[str, Any]:
    analysis = metrics.get("market_analysis") or {}
    smc = analysis.get("smc") or {}
    return {
        "signal": metrics.get("signal", "NEUTRAL"),
        "confidence": metrics.get("confidence"),
        "action": metrics.get("action"),
        "trend": analysis.get("htf_bias", "NEUTRAL"),
        "trend_aligned": trend_aligned,
        "smc_pattern": smc.get("pattern"),
        "smc_trend": smc.get("trend"),
        "ob_proximity": metrics.get("ob_proximity"),
        "ob_near": metrics.get("ob_near"),
        "price": metrics.get("price"),
        "change_24h": metrics.get("change_24h"),
        "rsi": analysis.get("rsi"),
        "macd_hist": analysis.get("macd_hist"),
        "adx": analysis.get("adx"),
        "htf_adx": analysis.get("htf_adx"),
    }
