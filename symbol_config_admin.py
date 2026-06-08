"""Apply / restore per-symbol config overrides from analytics suggestions."""

from __future__ import annotations

import json
from typing import Any

import db_store
import symbol_config

import dashboard_notify

UPDATED_BY = "deepseek_analytics"


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def validate_config_changes(changes: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
    if not isinstance(changes, dict) or not changes:
        return {}, "config_changes must be a non-empty object"

    validated: dict[str, Any] = {}
    for key, raw_value in changes.items():
        if key not in symbol_config.OVERRIDABLE_KEYS:
            return {}, f"Key not allowed: {key}"
        caster = symbol_config.OVERRIDABLE_KEYS[key]
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


def apply_config_changes(
    symbol: str,
    config_changes: dict[str, Any],
    *,
    reason: str | None = None,
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
        updated_by=UPDATED_BY,
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
        keys = [key for key in raw_changes if key in symbol_config.OVERRIDABLE_KEYS]
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
    for key in config_keys:
        if key not in symbol_config.OVERRIDABLE_KEYS:
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
