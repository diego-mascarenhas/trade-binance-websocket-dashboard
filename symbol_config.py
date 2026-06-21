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

_EXEC_BOOL = lambda v: str(v).lower() in ("1", "true", "yes")

EXECUTION_OVERRIDABLE_KEYS: dict[str, Callable[[Any], Any]] = {
    "POSITION_SIZE_USDT": float,
    "OB_EXIT_ON_OPPOSITE": _EXEC_BOOL,
    "OB_EXIT_REQUIRE_OB_REASON": _EXEC_BOOL,
    "OB_EXIT_MIN_PROFIT_PCT": float,
    "SCALPER_MODE": _EXEC_BOOL,
    "CLOSE_ON_RSI": _EXEC_BOOL,
    "CLOSE_RSI_LONG_MIN": float,
    "CLOSE_RSI_SHORT_MAX": float,
    "CLOSE_RSI_MIN_PROFIT_PCT": float,
    "CLOSE_RSI_REQUIRE_PROFIT": _EXEC_BOOL,
    "TRAIL_SL_ENABLED": _EXEC_BOOL,
    "TRAIL_SL_FEE_PCT": float,
    "TRAIL_SL_CANDLE_OFFSET": int,
}


def all_overridable_keys() -> dict[str, Callable[[Any], Any]]:
    return {**OVERRIDABLE_KEYS, **EXECUTION_OVERRIDABLE_KEYS}

_symbol_config_version: int = 0
_symbol_trading_enabled: bool = True
_cached_config_snapshot: dict[str, Any] = {}
_env_defaults: dict[str, Any] = {}
_execution_env_defaults: dict[str, Any] = {}
_loaded_db_version: int = -1


def config_version() -> int:
    return _symbol_config_version


def symbol_trading_enabled() -> bool:
    return _symbol_trading_enabled


def get_config_snapshot() -> dict[str, Any]:
    return dict(_cached_config_snapshot)


def capture_env_defaults(module_globals: dict[str, Any]) -> None:
    """Store .env values once so DB reload can reset before re-applying overrides."""
    global _env_defaults, _execution_env_defaults
    if _env_defaults:
        return

    for key in OVERRIDABLE_KEYS:
        if key == "symbol_trading_enabled":
            continue
        if key in module_globals:
            _env_defaults[key] = module_globals[key]
    _env_defaults["symbol_trading_enabled"] = True

    import execution

    for key, caster in EXECUTION_OVERRIDABLE_KEYS.items():
        if hasattr(execution, key):
            _execution_env_defaults[key] = getattr(execution, key)


def _reset_module_globals(module_globals: dict[str, Any]) -> None:
    global _symbol_trading_enabled

    for key, value in _env_defaults.items():
        if key == "symbol_trading_enabled":
            _symbol_trading_enabled = bool(value)
        elif key in module_globals:
            module_globals[key] = value

    import execution

    for key, value in _execution_env_defaults.items():
        setattr(execution, key, value)


def _apply_overrides(module_globals: dict[str, Any], overrides: dict[str, Any]) -> list[str]:
    global _symbol_trading_enabled

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
            logger.warning("Invalid DB override %s=%r: %s", key, overrides[key], exc)

    import execution

    for key, caster in EXECUTION_OVERRIDABLE_KEYS.items():
        if key in overrides:
            try:
                setattr(execution, key, caster(overrides[key]))
                applied.append(f"execution.{key}")
            except (TypeError, ValueError) as exc:
                logger.warning("Invalid DB override %s: %s", key, exc)

    return applied


def reload_from_db(
    module_globals: dict[str, Any],
    symbol: str,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Reload symbol overrides from MySQL. Resets to .env defaults first."""
    global _symbol_config_version, _cached_config_snapshot, _loaded_db_version

    capture_env_defaults(module_globals)

    if not db_store.is_enabled():
        _cached_config_snapshot = build_config_snapshot(module_globals)
        return {"changed": False, "db_enabled": False}

    db_store.init()
    overrides, version, active = db_store.get_symbol_config_record(symbol)

    if not force and version == _loaded_db_version:
        return {"changed": False, "config_version": version, "db_enabled": True}

    previous_htf = module_globals.get("HTF_INTERVAL")
    previous_interval = module_globals.get("INTERVAL")

    _reset_module_globals(module_globals)
    applied: list[str] = []

    if active and overrides:
        applied = _apply_overrides(module_globals, overrides)
        logger.info(
            "%s: hot-reloaded DB config v%s overrides: %s",
            symbol.upper(),
            version,
            ", ".join(applied) if applied else "(empty)",
        )
    else:
        logger.info("%s: hot-reloaded DB config v%s — using .env defaults", symbol.upper(), version)

    _loaded_db_version = version
    _symbol_config_version = version if active else 0
    _cached_config_snapshot = build_config_snapshot(module_globals)

    needs_ws_reconnect = (
        module_globals.get("HTF_INTERVAL") != previous_htf
        or module_globals.get("INTERVAL") != previous_interval
    )

    return {
        "changed": True,
        "db_enabled": True,
        "config_version": version,
        "active": active,
        "applied": applied,
        "needs_ws_reconnect": needs_ws_reconnect,
    }


def apply_db_overrides(module_globals: dict[str, Any], symbol: str) -> None:
    reload_from_db(module_globals, symbol, force=True)


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
    snapshot["fleet_side_balance_max_pct"] = execution.FLEET_SIDE_BALANCE_MAX_PCT
    snapshot["trade_plan_execute_dca"] = execution.TRADE_PLAN_EXECUTE_DCA
    snapshot["trade_plan_dca_signal_driven"] = execution.TRADE_PLAN_DCA_SIGNAL_DRIVEN
    snapshot["trade_plan_dca_adverse_only"] = execution.TRADE_PLAN_DCA_ADVERSE_ONLY
    snapshot["trade_plan_auto_be"] = execution.TRADE_PLAN_AUTO_BE
    snapshot["trade_plan_partial_close_pct"] = execution.TRADE_PLAN_PARTIAL_CLOSE_PCT
    snapshot["ob_exit_on_opposite"] = execution.OB_EXIT_ON_OPPOSITE
    snapshot["ob_exit_min_profit_pct"] = execution.OB_EXIT_MIN_PROFIT_PCT
    snapshot["scalper_mode"] = execution.SCALPER_MODE
    snapshot["close_on_rsi"] = execution.CLOSE_ON_RSI
    snapshot["close_rsi_long_min"] = execution.CLOSE_RSI_LONG_MIN
    snapshot["close_rsi_short_max"] = execution.CLOSE_RSI_SHORT_MAX
    snapshot["close_rsi_require_profit"] = execution.CLOSE_RSI_REQUIRE_PROFIT
    snapshot["trail_sl_enabled"] = execution.TRAIL_SL_ENABLED
    snapshot["trail_sl_fee_pct"] = execution.TRAIL_SL_FEE_PCT
    snapshot["trail_sl_candle_offset"] = execution.TRAIL_SL_CANDLE_OFFSET
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
