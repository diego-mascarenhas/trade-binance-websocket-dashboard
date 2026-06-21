"""Apply / restore per-symbol config overrides from analytics suggestions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import db_store
import symbol_config

import dashboard_notify

UPDATED_BY = "deepseek_analytics"
CONFIG_PAGE_UPDATED_BY = "config_page"

CONFIG_KEY_LABELS: dict[str, str] = {
    "MIN_CONFIDENCE": "Confianza mínima (TRADE)",
    "HTF_INTERVAL": "Intervalo HTF",
    "INTERVAL": "Intervalo LTF",
    "REQUIRE_TREND_ALIGN": "Exigir alineación HTF",
    "SIGNAL_DEBOUNCE_COUNT": "Debounce de señal (ticks)",
    "SIGNAL_COOLDOWN_SEC": "Cooldown entre entradas (s)",
    "MIN_PATTERN_RANGE_PCT": "Rango mínimo patrón (%)",
    "OB_WALL_RANGE_PCT": "Rango muros OB (%)",
    "INDICATOR_FILTERS_ENABLED": "Filtros indicadores (master)",
    "RSI_FILTER_ENABLED": "Filtro RSI",
    "RSI_LONG_MAX": "RSI máx LONG",
    "RSI_SHORT_MIN": "RSI mín SHORT",
    "MACD_FILTER_ENABLED": "Filtro MACD",
    "ADX_FILTER_ENABLED": "Filtro ADX",
    "ADX_MIN_TREND": "ADX mínimo",
    "ADX_USE_HTF": "ADX en velas HTF",
    "symbol_trading_enabled": "Trading habilitado (par)",
    "OB_EXIT_ON_OPPOSITE": "Cerrar en OB contrario",
    "OB_EXIT_REQUIRE_OB_REASON": "OB exit solo con muro OB",
    "OB_EXIT_MIN_PROFIT_PCT": "Beneficio mínimo OB exit (%)",
    "SCALPER_MODE": "Modo scalper (gate de cierres)",
    "CLOSE_ON_RSI": "Cerrar por RSI extremo",
    "CLOSE_RSI_LONG_MIN": "RSI cierre LONG (sobrecompra)",
    "CLOSE_RSI_SHORT_MAX": "RSI cierre SHORT (sobreventa)",
    "CLOSE_RSI_MIN_PROFIT_PCT": "Beneficio mínimo RSI exit (%)",
    "CLOSE_RSI_REQUIRE_PROFIT": "RSI exit solo en ganancia",
    "TRAIL_SL_ENABLED": "Trailing SL por apertura de vela",
    "TRAIL_SL_FEE_PCT": "Comisión round-trip (%)",
    "TRAIL_SL_CANDLE_OFFSET": "Velas atrás para el ancla",
}

CONFIG_KEY_HELP: dict[str, str] = {
    "MIN_CONFIDENCE": "LONG/SHORT con confianza ≥ este valor = TRADE.",
    "REQUIRE_TREND_ALIGN": "LONG solo con HTF BULLISH; SHORT solo con BEARISH.",
    "SIGNAL_DEBOUNCE_COUNT": "Ticks OB consecutivos antes de señal estable (0 = inmediato).",
    "ADX_USE_HTF": "Si true, ADX usa velas HTF; si false, usa 1m.",
    "symbol_trading_enabled": "false desactiva entradas en ese par.",
    "OB_EXIT_ON_OPPOSITE": "Cierra en OB contrario. Forzado a ON con SCALPER_MODE; si no, gobierna este flag.",
    "OB_EXIT_REQUIRE_OB_REASON": "Solo cierra con OB: near support/resistance (no señales 24h).",
    "OB_EXIT_MIN_PROFIT_PCT": "0 = cierra siempre; >0 exige ese % de uPnL mínimo.",
    "SCALPER_MODE": "Interruptor maestro: si true, SIEMPRE cierra por RSI y por OB contrario.",
    "CLOSE_ON_RSI": "Cierra por RSI extremo. Forzado a ON con SCALPER_MODE; si no, gobierna este flag.",
    "CLOSE_RSI_LONG_MIN": "LONG: cierra cuando RSI ≥ este valor (sobrecompra, p.ej. 72).",
    "CLOSE_RSI_SHORT_MAX": "SHORT: cierra cuando RSI ≤ este valor (sobreventa, p.ej. 28).",
    "CLOSE_RSI_MIN_PROFIT_PCT": "0 = sin umbral %; >0 exige ese % de uPnL mínimo.",
    "CLOSE_RSI_REQUIRE_PROFIT": "true = solo cierra si la posición está en ganancia (uPnL > 0).",
    "TRAIL_SL_ENABLED": "Sube el SL pegado a la apertura de la última vela cerrada una vez en ganancia (solo scalper).",
    "TRAIL_SL_FEE_PCT": "Comisión ida+vuelta a cubrir antes de trailar y suelo de break-even (default 0.10%).",
    "TRAIL_SL_CANDLE_OFFSET": "1 = última vela cerrada; 2 = dos atrás (más colchón).",
}

# Suggested bounds for /config/ UI (more trades vs fewer/stricter).
CONFIG_KEY_PRESETS: dict[str, dict[str, Any]] = {
    "MIN_CONFIDENCE": {
        "permissive": 40,
        "conservative": 65,
        "permissive_hint": "más señales TRADE",
        "conservative_hint": "solo confianza alta",
    },
    "HTF_INTERVAL": {
        "permissive": "5m",
        "conservative": "1h",
        "permissive_hint": "HTF reactivo",
        "conservative_hint": "tendencia más estable",
    },
    "INTERVAL": {
        "permissive": "1m",
        "conservative": "5m",
        "permissive_hint": "OB en tiempo real",
        "conservative_hint": "menos ruido LTF",
    },
    "REQUIRE_TREND_ALIGN": {
        "permissive": False,
        "conservative": True,
        "permissive_hint": "entra contra HTF",
        "conservative_hint": "solo a favor de HTF",
    },
    "SIGNAL_DEBOUNCE_COUNT": {
        "permissive": 0,
        "conservative": 8,
        "permissive_hint": "cambio inmediato",
        "conservative_hint": "confirma varios ticks",
    },
    "SIGNAL_COOLDOWN_SEC": {
        "permissive": 60,
        "conservative": 300,
        "permissive_hint": "reentrada rápida",
        "conservative_hint": "evita repetir entrada",
    },
    "MIN_PATTERN_RANGE_PCT": {
        "permissive": 0.01,
        "conservative": 0.08,
        "permissive_hint": "patrones pequeños",
        "conservative_hint": "solo rangos amplios",
    },
    "OB_WALL_RANGE_PCT": {
        "permissive": 1.0,
        "conservative": 0.35,
        "permissive_hint": "zonas OB amplias",
        "conservative_hint": "muros muy cercanos al precio",
    },
    "INDICATOR_FILTERS_ENABLED": {
        "permissive": False,
        "conservative": True,
        "permissive_hint": "sin bloqueo indicadores",
        "conservative_hint": "master filtros ON",
    },
    "RSI_FILTER_ENABLED": {
        "permissive": False,
        "conservative": True,
        "permissive_hint": "ignora RSI",
        "conservative_hint": "bloquea extremos RSI",
    },
    "RSI_LONG_MAX": {
        "permissive": 85,
        "conservative": 65,
        "permissive_hint": "LONG con RSI alto",
        "conservative_hint": "no LONG sobrecomprado",
    },
    "RSI_SHORT_MIN": {
        "permissive": 15,
        "conservative": 35,
        "permissive_hint": "SHORT con RSI bajo",
        "conservative_hint": "no SHORT sobrevendido",
    },
    "MACD_FILTER_ENABLED": {
        "permissive": False,
        "conservative": True,
        "permissive_hint": "ignora MACD",
        "conservative_hint": "exige MACD a favor",
    },
    "ADX_FILTER_ENABLED": {
        "permissive": False,
        "conservative": True,
        "permissive_hint": "ignora ADX",
        "conservative_hint": "exige tendencia fuerte",
    },
    "ADX_MIN_TREND": {
        "permissive": 5,
        "conservative": 28,
        "permissive_hint": "mercado lateral OK",
        "conservative_hint": "solo ADX alto",
    },
    "ADX_USE_HTF": {
        "permissive": False,
        "conservative": True,
        "permissive_hint": "ADX en 1m",
        "conservative_hint": "ADX en HTF",
    },
    "symbol_trading_enabled": {
        "permissive": True,
        "conservative": False,
        "permissive_hint": "entradas activas",
        "conservative_hint": "sin nuevas entradas",
    },
}

# Fleet-wide presets for /config/ (load or apply in one click).
FLEET_PRESET_META: dict[str, dict[str, Any]] = {
    "permissive": {
        "label": "Permisivo",
        "hint": "Más señales: HTF off, conf 40, filtros relajados",
    },
    "balanced": {
        "label": "Equilibrado",
        "hint": "Conf 50, HTF align, filtros moderados (similar a .env.example)",
    },
    "conservative": {
        "label": "Conservador",
        "hint": "Pocas señales: conf 65, HTF estricto, ADX alto",
    },
    "scalp_aggressive": {
        "label": "Scalping agresivo",
        "hint": "1m/3m, debounce 0, cooldown 30s, sin HTF align ni filtros",
    },
    "scalp_moderate": {
        "label": "Scalping moderado",
        "hint": "1m/5m, debounce 2, cooldown 90s, filtros ligeros en 1m",
    },
    "env": {
        "label": "Default (.env)",
        "hint": "Elimina overrides MySQL; cada par usa su .env",
        "restore_only": True,
    },
}

BALANCED_FLEET_VALUES: dict[str, Any] = {
    "MIN_CONFIDENCE": 50,
    "HTF_INTERVAL": "15m",
    "INTERVAL": "1m",
    "REQUIRE_TREND_ALIGN": True,
    "SIGNAL_DEBOUNCE_COUNT": 5,
    "SIGNAL_COOLDOWN_SEC": 180,
    "MIN_PATTERN_RANGE_PCT": 0.02,
    "OB_WALL_RANGE_PCT": 0.6,
    "INDICATOR_FILTERS_ENABLED": True,
    "RSI_FILTER_ENABLED": True,
    "RSI_LONG_MAX": 70.0,
    "RSI_SHORT_MIN": 30.0,
    "MACD_FILTER_ENABLED": False,
    "ADX_FILTER_ENABLED": True,
    "ADX_MIN_TREND": 25.0,
    "ADX_USE_HTF": True,
    "symbol_trading_enabled": True,
}

SCALP_AGGRESSIVE_FLEET_VALUES: dict[str, Any] = {
    "MIN_CONFIDENCE": 40,
    "HTF_INTERVAL": "3m",
    "INTERVAL": "1m",
    "REQUIRE_TREND_ALIGN": False,
    "SIGNAL_DEBOUNCE_COUNT": 0,
    "SIGNAL_COOLDOWN_SEC": 30,
    "MIN_PATTERN_RANGE_PCT": 0.01,
    "OB_WALL_RANGE_PCT": 1.0,
    "INDICATOR_FILTERS_ENABLED": False,
    "RSI_FILTER_ENABLED": False,
    "RSI_LONG_MAX": 85.0,
    "RSI_SHORT_MIN": 15.0,
    "MACD_FILTER_ENABLED": False,
    "ADX_FILTER_ENABLED": False,
    "ADX_MIN_TREND": 5.0,
    "ADX_USE_HTF": False,
    "symbol_trading_enabled": True,
    "OB_EXIT_ON_OPPOSITE": True,
    "OB_EXIT_REQUIRE_OB_REASON": True,
    "OB_EXIT_MIN_PROFIT_PCT": 0.0,
    "SCALPER_MODE": True,
    "CLOSE_ON_RSI": True,
    "CLOSE_RSI_LONG_MIN": 72.0,
    "CLOSE_RSI_SHORT_MAX": 28.0,
    "CLOSE_RSI_MIN_PROFIT_PCT": 0.0,
    "CLOSE_RSI_REQUIRE_PROFIT": True,
    "TRAIL_SL_ENABLED": True,
    "TRAIL_SL_FEE_PCT": 0.10,
    "TRAIL_SL_CANDLE_OFFSET": 1,
}

SCALP_MODERATE_FLEET_VALUES: dict[str, Any] = {
    "MIN_CONFIDENCE": 45,
    "HTF_INTERVAL": "5m",
    "INTERVAL": "1m",
    "REQUIRE_TREND_ALIGN": False,
    "SIGNAL_DEBOUNCE_COUNT": 2,
    "SIGNAL_COOLDOWN_SEC": 90,
    "MIN_PATTERN_RANGE_PCT": 0.015,
    "OB_WALL_RANGE_PCT": 0.85,
    "INDICATOR_FILTERS_ENABLED": True,
    "RSI_FILTER_ENABLED": True,
    "RSI_LONG_MAX": 78.0,
    "RSI_SHORT_MIN": 22.0,
    "MACD_FILTER_ENABLED": False,
    "ADX_FILTER_ENABLED": True,
    "ADX_MIN_TREND": 12.0,
    "ADX_USE_HTF": False,
    "symbol_trading_enabled": True,
    "OB_EXIT_ON_OPPOSITE": True,
    "OB_EXIT_REQUIRE_OB_REASON": True,
    "OB_EXIT_MIN_PROFIT_PCT": 0.05,
    "SCALPER_MODE": True,
    "CLOSE_ON_RSI": True,
    "CLOSE_RSI_LONG_MIN": 74.0,
    "CLOSE_RSI_SHORT_MAX": 26.0,
    "CLOSE_RSI_MIN_PROFIT_PCT": 0.05,
    "CLOSE_RSI_REQUIRE_PROFIT": True,
    "TRAIL_SL_ENABLED": True,
    "TRAIL_SL_FEE_PCT": 0.10,
    "TRAIL_SL_CANDLE_OFFSET": 2,
}


def build_fleet_preset_values(preset_id: str) -> dict[str, Any] | None:
    """Build full override dict for a named fleet preset."""
    if preset_id == "env":
        return None
    if preset_id == "balanced":
        return dict(BALANCED_FLEET_VALUES)
    if preset_id == "scalp_aggressive":
        return dict(SCALP_AGGRESSIVE_FLEET_VALUES)
    if preset_id == "scalp_moderate":
        return dict(SCALP_MODERATE_FLEET_VALUES)
    if preset_id not in ("permissive", "conservative"):
        return None

    values: dict[str, Any] = {}
    for key in symbol_config.OVERRIDABLE_KEYS:
        preset = CONFIG_KEY_PRESETS.get(key, {})
        if preset_id in preset:
            values[key] = preset[preset_id]
    values.setdefault("symbol_trading_enabled", True)
    return values


def list_fleet_presets(*, env_defaults: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Metadata + values for /config/ preset picker."""
    env_defaults = env_defaults if env_defaults is not None else read_env_defaults()
    items: list[dict[str, Any]] = []
    for preset_id, meta in FLEET_PRESET_META.items():
        restore_only = bool(meta.get("restore_only"))
        if restore_only:
            values = dict(env_defaults)
        else:
            values = build_fleet_preset_values(preset_id) or {}
        items.append(
            {
                "id": preset_id,
                "label": meta.get("label", preset_id),
                "hint": meta.get("hint", ""),
                "restore_only": restore_only,
                "values": values,
            }
        )
    return items


_BOOL_KEYS = frozenset(
    {
        "REQUIRE_TREND_ALIGN",
        "INDICATOR_FILTERS_ENABLED",
        "RSI_FILTER_ENABLED",
        "MACD_FILTER_ENABLED",
        "ADX_FILTER_ENABLED",
        "ADX_USE_HTF",
        "symbol_trading_enabled",
        "OB_EXIT_ON_OPPOSITE",
        "OB_EXIT_REQUIRE_OB_REASON",
        "SCALPER_MODE",
        "CLOSE_ON_RSI",
        "CLOSE_RSI_REQUIRE_PROFIT",
        "TRAIL_SL_ENABLED",
    }
)

_INT_KEYS = frozenset(
    {"MIN_CONFIDENCE", "SIGNAL_DEBOUNCE_COUNT", "SIGNAL_COOLDOWN_SEC", "TRAIL_SL_CANDLE_OFFSET"}
)

_FLOAT_KEYS = frozenset(
    {
        "MIN_PATTERN_RANGE_PCT",
        "OB_WALL_RANGE_PCT",
        "RSI_LONG_MAX",
        "RSI_SHORT_MIN",
        "ADX_MIN_TREND",
        "OB_EXIT_MIN_PROFIT_PCT",
        "CLOSE_RSI_LONG_MIN",
        "CLOSE_RSI_SHORT_MAX",
        "CLOSE_RSI_MIN_PROFIT_PCT",
        "TRAIL_SL_FEE_PCT",
        "POSITION_SIZE_USDT",
    }
)
# (bool keys handled separately in _BOOL_KEYS)


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def validate_config_changes(changes: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    if not isinstance(changes, dict) or not changes:
        return {}, "config_changes must be a non-empty object"

    allowed = symbol_config.all_overridable_keys()
    validated: dict[str, Any] = {}
    for key, raw_value in changes.items():
        if key not in allowed:
            return {}, f"Key not allowed: {key}"
        caster = allowed[key]
        try:
            validated[key] = caster(raw_value)
        except (TypeError, ValueError):
            return {}, f"Invalid value for {key}: {raw_value!r}"

    return validated, None


def get_symbol_override(symbol: str) -> dict[str, Any]:
    if not db_store.is_enabled():
        return {"enabled": False, "error": "DB_ENABLED=false"}
    overrides, version = db_store.get_symbol_config(_normalize_symbol(symbol))
    return {
        "enabled": True,
        "symbol": _normalize_symbol(symbol),
        "config_version": version,
        "overrides": overrides,
        "using_defaults": not bool(overrides),
    }


def _key_field_type(key: str) -> str:
    if key in _BOOL_KEYS:
        return "bool"
    if key in _INT_KEYS:
        return "int"
    if key in _FLOAT_KEYS:
        return "float"
    return "string"


def read_env_defaults() -> dict[str, Any]:
    """Parse .env for overridable keys (hub has no app.py globals loaded)."""
    env_path = Path(__file__).resolve().parent / ".env"
    raw: dict[str, str] = {}
    if env_path.is_file():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, _, value = stripped.partition("=")
            raw[key.strip()] = value.strip().strip('"').strip("'")

    defaults: dict[str, Any] = {}
    for key, caster in symbol_config.all_overridable_keys().items():
        if key not in raw:
            if key == "symbol_trading_enabled":
                defaults[key] = True
            continue
        try:
            defaults[key] = caster(raw[key])
        except (TypeError, ValueError):
            defaults[key] = raw[key]
    return defaults


def get_config_editor_state() -> dict[str, Any]:
    env_defaults = read_env_defaults()
    fleet = dashboard_notify.load_fleet_symbols()
    fleet_status = get_fleet_overrides_status()
    fleet_wide = fleet_status.get("fleet_wide") or {}

    keys: list[dict[str, Any]] = []
    for key in symbol_config.all_overridable_keys():
        env_val = env_defaults.get(key)
        effective = fleet_wide.get(key, env_val)
        source = "mysql_fleet" if key in fleet_wide else "env"
        preset = CONFIG_KEY_PRESETS.get(key, {})
        keys.append(
            {
                "key": key,
                "type": _key_field_type(key),
                "label": CONFIG_KEY_LABELS.get(key, key),
                "help": CONFIG_KEY_HELP.get(key, ""),
                "env_default": env_val,
                "effective": effective,
                "source": source,
                "presets": {
                    "permissive": preset.get("permissive"),
                    "conservative": preset.get("conservative"),
                    "permissive_hint": preset.get("permissive_hint", ""),
                    "conservative_hint": preset.get("conservative_hint", ""),
                }
                if preset
                else None,
            }
        )

    return {
        "enabled": db_store.is_enabled(),
        "fleet_size": len(fleet),
        "fleet_symbols": fleet,
        "overrides": fleet_status,
        "keys": keys,
        "env_defaults": env_defaults,
        "fleet_presets": list_fleet_presets(env_defaults=env_defaults),
    }


def apply_fleet_config(
    config_changes: dict[str, Any],
    *,
    reason: str | None = None,
    updated_by: str = CONFIG_PAGE_UPDATED_BY,
) -> dict[str, Any]:
    if not db_store.is_enabled():
        return {"ok": False, "error": "DB_ENABLED=false"}

    validated, error = validate_config_changes(config_changes)
    if error:
        return {"ok": False, "error": error}

    fleet = dashboard_notify.load_fleet_symbols()
    if not fleet:
        return {"ok": False, "error": "No hay símbolos en hub/pairs.json"}

    detail = reason or "Config page fleet apply"
    results: list[dict[str, Any]] = []
    errors: list[str] = []

    for symbol in fleet:
        result = apply_config_changes(
            symbol,
            validated,
            reason=detail,
            updated_by=updated_by,
        )
        results.append(result)
        if not result.get("ok"):
            errors.append(f"{symbol}: {result.get('error', 'unknown')}")

    applied = sum(1 for result in results if result.get("ok"))
    message = f"Aplicado en {applied}/{len(fleet)} símbolo(s). Vuelve a .env con Restaurar."
    if errors:
        message = f"Aplicado en {applied}/{len(fleet)} símbolo(s). Fallos: {'; '.join(errors[:5])}"

    return {
        "ok": applied > 0 and not errors,
        "partial": applied > 0 and bool(errors),
        "applied_symbols": applied,
        "total_symbols": len(fleet),
        "errors": errors,
        "results": results,
        "applied": validated,
        "message": message,
    }


def apply_config_changes(
    symbol: str,
    config_changes: dict[str, Any],
    *,
    reason: str | None = None,
    updated_by: str = UPDATED_BY,
) -> dict[str, Any]:
    if not db_store.is_enabled():
        return {"ok": False, "error": "DB_ENABLED=false"}

    symbol = _normalize_symbol(symbol)
    validated, error = validate_config_changes(config_changes)
    if error:
        return {"ok": False, "error": error}

    current, _ = db_store.get_symbol_config(symbol)
    merged = dict(current)
    merged.update(validated)

    detail = reason or "Applied from analytics suggestion"
    version = db_store.upsert_symbol_config(
        symbol,
        merged,
        updated_by=updated_by,
        reason=detail,
    )
    if version is None:
        return {"ok": False, "error": "Failed to save symbol_config"}

    reload = dashboard_notify.notify_dashboard_reload(symbol)
    message = f"Saved {symbol} config v{version}."
    if reload.get("ok"):
        message += " Dashboard reloaded from DB."
    elif reload.get("error"):
        message += f" Reload failed: {reload['error']}. Is app.py updated on that pair's port?"

    return {
        "ok": True,
        "symbol": symbol,
        "config_version": version,
        "applied": validated,
        "overrides": merged,
        "restart_required": False,
        "reload": reload,
        "message": message,
    }


def _normalize_suggestion_symbol(symbol: Any) -> str | None:
    if symbol is None:
        return None
    value = str(symbol).strip().upper()
    return value or None


def plan_suggestions_apply(suggestions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Merge DeepSeek suggestions: fleet-wide first, then per-symbol overrides."""
    import dashboard_notify

    fleet = dashboard_notify.load_fleet_symbols()
    fleet_set = set(fleet)
    per_symbol: dict[str, dict[str, Any]] = {}
    global_changes: dict[str, Any] = {}

    for item in suggestions:
        if not isinstance(item, dict):
            continue
        raw_changes = item.get("config_changes") or {}
        if not isinstance(raw_changes, dict) or not raw_changes:
            continue
        validated, error = validate_config_changes(raw_changes)
        if error:
            continue

        symbol = _normalize_suggestion_symbol(item.get("symbol"))
        if symbol:
            bucket = per_symbol.setdefault(symbol, {})
            bucket.update(validated)
        else:
            global_changes.update(validated)

    plan: dict[str, dict[str, Any]] = {}
    targets = fleet or sorted(per_symbol.keys())
    for symbol in targets:
        merged = dict(global_changes)
        merged.update(per_symbol.get(symbol, {}))
        if merged:
            plan[symbol] = merged

    for symbol, changes in per_symbol.items():
        if symbol not in fleet_set and symbol not in plan:
            plan[symbol] = dict(changes)

    return plan


def plan_suggestions_restore(suggestions: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Keys to remove per symbol so dashboards fall back to .env defaults."""
    import dashboard_notify

    fleet = dashboard_notify.load_fleet_symbols()
    restore_map: dict[str, set[str]] = {symbol: set() for symbol in fleet}

    for item in suggestions:
        if not isinstance(item, dict):
            continue
        raw_changes = item.get("config_changes") or {}
        if not isinstance(raw_changes, dict) or not raw_changes:
            continue
        keys = [key for key in raw_changes if key in symbol_config.all_overridable_keys()]
        if not keys:
            continue

        symbol = _normalize_suggestion_symbol(item.get("symbol"))
        if symbol:
            restore_map.setdefault(symbol, set()).update(keys)
        else:
            for fleet_symbol in fleet:
                restore_map[fleet_symbol].update(keys)

    return {
        symbol: sorted(keys)
        for symbol, keys in restore_map.items()
        if keys
    }


def apply_suggestions_batch(
    suggestions: list[dict[str, Any]],
    *,
    reason: str | None = None,
) -> dict[str, Any]:
    if not db_store.is_enabled():
        return {"ok": False, "error": "DB_ENABLED=false"}

    plan = plan_suggestions_apply(suggestions)
    if not plan:
        return {"ok": False, "error": "No aplicable config_changes in suggestions"}

    detail = reason or "DeepSeek bulk apply"
    results: list[dict[str, Any]] = []
    errors: list[str] = []

    for symbol, changes in sorted(plan.items()):
        result = apply_config_changes(symbol, changes, reason=detail)
        results.append(result)
        if not result.get("ok"):
            errors.append(f"{symbol}: {result.get('error', 'unknown')}")

    applied = sum(1 for result in results if result.get("ok"))
    message = f"Aplicado en {applied}/{len(plan)} símbolo(s). Vuelve a .env con Restaurar."
    if errors:
        message = f"Aplicado en {applied}/{len(plan)} símbolo(s). Fallos: {'; '.join(errors[:5])}"

    return {
        "ok": applied > 0 and not errors,
        "partial": applied > 0 and bool(errors),
        "applied_symbols": applied,
        "total_symbols": len(plan),
        "errors": errors,
        "results": results,
        "message": message,
    }


def _format_config_value(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    return value


def get_fleet_overrides_status() -> dict[str, Any]:
    """Summarize active MySQL overrides for analytics UI."""
    if not db_store.is_enabled():
        return {"enabled": False, "active": False, "error": "DB_ENABLED=false"}

    fleet = dashboard_notify.load_fleet_symbols()
    rows = db_store.list_active_symbol_configs()
    fleet_set = set(fleet)
    symbol_overrides: dict[str, dict[str, Any]] = {}
    meta_by_symbol: dict[str, dict[str, Any]] = {}

    for row in rows:
        symbol = row.get("symbol")
        overrides = row.get("overrides") or {}
        if not symbol or not overrides:
            continue
        symbol_overrides[symbol] = overrides
        meta_by_symbol[symbol] = {
            "config_version": row.get("config_version"),
            "updated_at": row.get("updated_at"),
            "updated_by": row.get("updated_by"),
        }

    active_symbols = [symbol for symbol in fleet if symbol in symbol_overrides]
    extra_symbols = sorted(symbol for symbol in symbol_overrides if symbol not in fleet_set)

    if not active_symbols and not extra_symbols:
        return {
            "enabled": True,
            "active": False,
            "fleet_size": len(fleet),
            "symbols_with_overrides": 0,
        }

    scoped_symbols = active_symbols or extra_symbols
    all_keys: set[str] = set()
    for symbol in scoped_symbols:
        all_keys.update(symbol_overrides[symbol].keys())

    fleet_wide: dict[str, Any] = {}
    per_symbol: dict[str, dict[str, Any]] = {}
    for key in sorted(all_keys):
        values = {
            symbol: symbol_overrides[symbol][key]
            for symbol in scoped_symbols
            if key in symbol_overrides[symbol]
        }
        if not values:
            continue
        serialized = {symbol: json.dumps(value, sort_keys=True, default=str) for symbol, value in values.items()}
        if len(values) == len(scoped_symbols) and len(set(serialized.values())) == 1:
            fleet_wide[key] = _format_config_value(next(iter(values.values())))
        else:
            for symbol, value in values.items():
                per_symbol.setdefault(symbol, {})[key] = _format_config_value(value)

    latest_at = None
    latest_by = None
    for symbol in scoped_symbols:
        meta = meta_by_symbol.get(symbol) or {}
        updated_at = meta.get("updated_at")
        if updated_at and (latest_at is None or str(updated_at) > str(latest_at)):
            latest_at = updated_at
            latest_by = meta.get("updated_by")

    return {
        "enabled": True,
        "active": True,
        "fleet_size": len(fleet),
        "symbols_with_overrides": len(active_symbols),
        "extra_symbols": extra_symbols,
        "fleet_wide": fleet_wide,
        "per_symbol": per_symbol,
        "updated_at": latest_at,
        "updated_by": latest_by,
        "source": "mysql_symbol_config",
    }


def restore_fleet_to_env_defaults(*, reason: str | None = None) -> dict[str, Any]:
    if not db_store.is_enabled():
        return {"ok": False, "error": "DB_ENABLED=false"}

    fleet = dashboard_notify.load_fleet_symbols()
    rows = db_store.list_active_symbol_configs()
    active_symbols = {row["symbol"] for row in rows if row.get("symbol")}
    targets = sorted(symbol for symbol in fleet if symbol in active_symbols)
    if not targets:
        return {
            "ok": True,
            "restored_symbols": 0,
            "message": "No hay overrides activos — ya se usa .env.",
        }

    detail = reason or "Analytics restore all to .env defaults"
    results: list[dict[str, Any]] = []
    errors: list[str] = []

    for symbol in targets:
        if symbol not in fleet:
            continue
        version = db_store.deactivate_symbol_config(
            symbol,
            updated_by=UPDATED_BY,
            reason=detail,
        )
        if version is None:
            errors.append(f"{symbol}: deactivate failed")
            results.append({"ok": False, "symbol": symbol})
            continue
        reload = dashboard_notify.notify_dashboard_reload(symbol)
        results.append({"ok": True, "symbol": symbol, "config_version": version, "reload": reload})

    restored = sum(1 for result in results if result.get("ok"))
    message = f"Restaurado a .env en {restored}/{len(targets)} símbolo(s)."
    if errors:
        message = f"Restaurado en {restored}/{len(targets)} símbolo(s). Fallos: {'; '.join(errors[:5])}"

    return {
        "ok": restored > 0 and not errors,
        "partial": restored > 0 and bool(errors),
        "restored_symbols": restored,
        "total_symbols": len(targets),
        "errors": errors,
        "results": results,
        "message": message,
    }


def restore_suggestions_batch(
    suggestions: list[dict[str, Any]],
    *,
    reason: str | None = None,
) -> dict[str, Any]:
    if not db_store.is_enabled():
        return {"ok": False, "error": "DB_ENABLED=false"}

    plan = plan_suggestions_restore(suggestions)
    if not plan:
        return {"ok": False, "error": "No aplicable config_changes to restore"}

    detail = reason or "DeepSeek bulk restore"
    results: list[dict[str, Any]] = []
    errors: list[str] = []

    for symbol, keys in sorted(plan.items()):
        result = restore_config_keys(symbol, keys, reason=detail)
        results.append(result)
        if not result.get("ok"):
            errors.append(f"{symbol}: {result.get('error', 'unknown')}")

    restored = sum(1 for result in results if result.get("ok"))
    message = f"Restaurado a .env en {restored}/{len(plan)} símbolo(s)."
    if errors:
        message = f"Restaurado en {restored}/{len(plan)} símbolo(s). Fallos: {'; '.join(errors[:5])}"

    return {
        "ok": restored > 0 and not errors,
        "partial": restored > 0 and bool(errors),
        "restored_symbols": restored,
        "total_symbols": len(plan),
        "errors": errors,
        "results": results,
        "message": message,
    }


def restore_config_keys(
    symbol: str,
    config_keys: list[str],
    *,
    reason: str | None = None,
) -> dict[str, Any]:
    if not db_store.is_enabled():
        return {"ok": False, "error": "DB_ENABLED=false"}

    symbol = _normalize_symbol(symbol)
    if not config_keys:
        return {"ok": False, "error": "config_keys required"}

    allowed_keys: list[str] = []
    allowed = symbol_config.all_overridable_keys()
    for key in config_keys:
        if key not in allowed:
            return {"ok": False, "error": f"Key not allowed: {key}"}
        allowed_keys.append(key)

    current, _ = db_store.get_symbol_config(symbol)
    if not current:
        return {
            "ok": True,
            "symbol": symbol,
            "restored_keys": allowed_keys,
            "using_defaults": True,
            "message": f"{symbol} already uses .env defaults.",
        }

    remaining = {key: value for key, value in current.items() if key not in allowed_keys}
    detail = reason or f"Restored keys to .env defaults: {', '.join(allowed_keys)}"

    if not remaining:
        version = db_store.deactivate_symbol_config(
            symbol,
            updated_by=UPDATED_BY,
            reason=detail,
        )
        if version is None:
            return {"ok": False, "error": "Failed to restore defaults"}
        reload = dashboard_notify.notify_dashboard_reload(symbol)
        message = f"{symbol} restored to .env defaults (v{version})."
        if reload.get("ok"):
            message += " Dashboard reloaded from DB."
        elif reload.get("error"):
            message += f" Will auto-reload within ~15s ({reload['error']})."
        return {
            "ok": True,
            "symbol": symbol,
            "config_version": version,
            "restored_keys": allowed_keys,
            "using_defaults": True,
            "restart_required": False,
            "reload": reload,
            "message": message,
        }

    version = db_store.upsert_symbol_config(
        symbol,
        remaining,
        updated_by=UPDATED_BY,
        reason=detail,
    )
    if version is None:
        return {"ok": False, "error": "Failed to update symbol_config"}

    reload = dashboard_notify.notify_dashboard_reload(symbol)
    message = f"Removed {', '.join(allowed_keys)} from {symbol} overrides (v{version})."
    if reload.get("ok"):
        message += " Dashboard reloaded from DB."
    elif reload.get("error"):
        message += f" Reload failed: {reload['error']}. Is app.py updated on that pair's port?"

    return {
        "ok": True,
        "symbol": symbol,
        "config_version": version,
        "restored_keys": allowed_keys,
        "overrides": remaining,
        "using_defaults": False,
        "restart_required": False,
        "reload": reload,
        "message": message,
    }
