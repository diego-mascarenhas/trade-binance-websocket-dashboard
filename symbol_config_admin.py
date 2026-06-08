"""Apply / restore per-symbol config overrides from analytics suggestions."""

from __future__ import annotations

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
