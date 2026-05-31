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
