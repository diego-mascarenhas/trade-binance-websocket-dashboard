#!/usr/bin/env python3
"""Hub + analytics on a single port (default HUB_PORT=8050)."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, Response, abort, jsonify, redirect, request, send_from_directory

import db_analytics
import db_store
import deepseek_advisor
import execution
import hub_proxy
import symbol_config_admin
import trade_boost

load_dotenv()

ROOT = Path(__file__).resolve().parent
HUB_DIR = ROOT / "hub"
ANALYTICS_DIR = ROOT / "analytics"
CONFIG_DIR = ROOT / "config"
HOST = os.getenv("HUB_HOST", os.getenv("ANALYTICS_HOST", "0.0.0.0"))
PORT = int(os.getenv("HUB_PORT", "8050"))
CONFIG_PAGE_TOKEN = os.getenv("CONFIG_PAGE_TOKEN", "").strip()

app = Flask(__name__)


def _cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


def _int_arg(name: str, default: int, *, minimum: int = 1, maximum: int = 365) -> int:
    try:
        value = int(request.args.get(name, default))
    except (TypeError, ValueError):
        return default
    return max(minimum, min(value, maximum))


def _optional_symbol() -> str | None:
    symbol = (request.args.get("symbol") or "").strip().upper()
    return symbol or None


def _config_page_token_ok() -> bool:
    if not CONFIG_PAGE_TOKEN:
        return True
    supplied = (
        request.args.get("token")
        or request.headers.get("X-Config-Token")
        or ""
    ).strip()
    return supplied == CONFIG_PAGE_TOKEN


def _config_page_guard() -> None:
    if not _config_page_token_ok():
        abort(403)


def _config_forbidden_html() -> str:
    return """<!DOCTYPE html>
<html lang="es"><head><meta charset="UTF-8"><title>Config</title>
<style>body{font-family:system-ui;background:#0f1117;color:#e8ecf1;padding:2rem;max-width:520px;margin:auto}
code{background:#1e293b;padding:2px 6px;border-radius:4px}</style></head><body>
<h1>Token requerido</h1>
<p>Define <code>CONFIG_PAGE_TOKEN</code> en <code>.env</code> y abre:</p>
<p><code>/config/?token=TU_TOKEN</code></p>
<p>Ejemplo: <code>http://127.0.0.1:8050/config/?token=…</code></p>
</body></html>"""


@app.route("/config")
def config_redirect():
    token = request.args.get("token")
    if token:
        return redirect(f"/config/?token={token}", code=302)
    return redirect("/config/", code=302)


@app.route("/config/")
def config_index():
    if not _config_page_token_ok():
        return _config_forbidden_html(), 403, {"Content-Type": "text/html; charset=utf-8"}
    return send_from_directory(CONFIG_DIR, "index.html")


@app.route("/config/<path:filename>")
def config_static(filename: str):
    if ".." in filename:
        abort(404)
    # JS/CSS load without token (APIs remain protected); avoids blank page when token is in ?query only.
    allowed = filename.endswith(".js") or filename.endswith(".css")
    if not allowed:
        _config_page_guard()
    return send_from_directory(CONFIG_DIR, filename)


@app.route("/api/config/editor", methods=["GET"])
def config_editor_state():
    _config_page_guard()
    return _cors(jsonify(symbol_config_admin.get_config_editor_state()))


@app.route("/api/config/apply-fleet", methods=["POST"])
def config_apply_fleet():
    _config_page_guard()
    payload = request.get_json(silent=True) or {}
    config_changes = payload.get("config_changes") or {}
    reason = payload.get("reason") or "Config page fleet apply"
    if not isinstance(config_changes, dict) or not config_changes:
        return _cors(jsonify({"ok": False, "error": "config_changes required"})), 400
    result = symbol_config_admin.apply_fleet_config(config_changes, reason=reason)
    status = 200 if result.get("ok") or result.get("partial") else 400
    return _cors(jsonify(result)), status


@app.route("/api/config/restore-fleet", methods=["POST"])
def config_restore_fleet():
    _config_page_guard()
    payload = request.get_json(silent=True) or {}
    reason = payload.get("reason") or "Config page restore to .env"
    result = symbol_config_admin.restore_fleet_to_env_defaults(reason=reason)
    status = 200 if result.get("ok") or result.get("partial") else 400
    return _cors(jsonify(result)), status


@app.route("/api/config/apply-preset", methods=["POST"])
def config_apply_preset():
    _config_page_guard()
    payload = request.get_json(silent=True) or {}
    preset_id = str(payload.get("preset_id") or "").strip()
    if not preset_id:
        return _cors(jsonify({"ok": False, "error": "preset_id required"})), 400

    meta = symbol_config_admin.FLEET_PRESET_META.get(preset_id)
    if not meta:
        return _cors(jsonify({"ok": False, "error": f"Unknown preset: {preset_id}"})), 400

    reason = payload.get("reason") or f"Config page preset: {preset_id}"
    if meta.get("restore_only"):
        result = symbol_config_admin.restore_fleet_to_env_defaults(reason=reason)
    else:
        values = symbol_config_admin.build_fleet_preset_values(preset_id)
        if not values:
            return _cors(jsonify({"ok": False, "error": f"Preset empty: {preset_id}"})), 400
        result = symbol_config_admin.apply_fleet_config(values, reason=reason)

    status = 200 if result.get("ok") or result.get("partial") else 400
    return _cors(jsonify(result)), status


@app.route("/analytics")
def analytics_redirect():
    return redirect("/analytics/", code=302)


@app.route("/analytics/")
def analytics_index():
    return send_from_directory(ANALYTICS_DIR, "index.html")


@app.route("/analytics/<path:filename>")
def analytics_static(filename: str):
    if ".." in filename:
        abort(404)
    return send_from_directory(ANALYTICS_DIR, filename)


@app.route("/api/hub-summaries")
def hub_summaries():
    """Aggregate hub-summary from each local dashboard port (for remote hub UI)."""
    payload = hub_proxy.fetch_all_summaries()
    payload["online_count"] = len(payload.get("summaries") or {})
    payload["offline_count"] = len(payload.get("offline") or [])
    return _cors(jsonify(payload))


@app.route("/api/fleet-exposure")
def fleet_exposure():
    return _cors(jsonify(execution.get_fleet_side_exposure()))


@app.route("/api/fleet-positions")
def fleet_positions():
    return _cors(jsonify(execution.get_fleet_open_positions_map()))


@app.route("/api/health")
def health():
    db_store.init()
    payload = {
        "ok": True,
        "db_enabled": db_store.is_enabled(),
        "db_ready": db_store.ensure_schema() if db_store.is_enabled() else False,
        "deepseek_enabled": deepseek_advisor.is_enabled(),
        "deepseek_configured": deepseek_advisor.is_configured(),
        "binance_keys_configured": execution.keys_configured(),
    }
    return _cors(jsonify(payload))


@app.route("/api/fapi-rest-metrics")
def fapi_rest_metrics():
    """Binance Futures REST usage (all processes, via execution.py)."""
    return _cors(jsonify(execution.get_fapi_rest_metrics()))


@app.route("/api/account-performance")
def account_performance():
    """Live wallet + realized PnL averages + projection to MILLION_GOAL_USDT."""
    days = _int_arg("days", execution.PNL_STATS_LOOKBACK_DAYS, minimum=0, maximum=3650)
    lookback = execution.PNL_STATS_LOOKBACK_DAYS if days == 0 else days
    payload = execution.get_performance_snapshot(_optional_symbol(), days=lookback)
    return _cors(jsonify(payload))


@app.route("/api/overview")
def overview():
    days = _int_arg("days", 7, minimum=0, maximum=365)
    days_filter = None if days == 0 else days
    return _cors(jsonify(db_analytics.get_overview(days_filter)))


@app.route("/api/series/hourly")
def series_hourly():
    days = _int_arg("days", 7)
    return _cors(jsonify(db_analytics.get_hourly_series(days, _optional_symbol())))


@app.route("/api/series/daily")
def series_daily():
    days = _int_arg("days", 30)
    return _cors(jsonify(db_analytics.get_daily_series(days, _optional_symbol())))


@app.route("/api/breakdown/<field>")
def breakdown(field: str):
    days = _int_arg("days", 7)
    return _cors(jsonify(db_analytics.get_breakdown(field, days=days, symbol=_optional_symbol())))


@app.route("/api/block-summary")
def block_summary():
    days = _int_arg("days", 7, minimum=0, maximum=365)
    days_filter = None if days == 0 else days
    return _cors(jsonify(db_analytics.get_block_summary(days_filter, _optional_symbol())))


@app.route("/api/recent")
def recent():
    limit = _int_arg("limit", 50, minimum=1, maximum=500)
    try:
        days = int(request.args.get("days", 7))
    except (TypeError, ValueError):
        days = 7
    days_filter = None if days <= 0 else max(1, min(days, 365))
    event_group = (request.args.get("event_group") or "").strip().lower() or None
    if event_group in ("all", ""):
        event_group = None
    return _cors(
        jsonify(
            db_analytics.get_recent_events(
                limit,
                _optional_symbol(),
                days=days_filter,
                event_group=event_group,
            )
        )
    )


@app.route("/api/features")
def features():
    days = _int_arg("days", 30)
    limit = _int_arg("limit", 200, minimum=1, maximum=5000)
    offset = _int_arg("offset", 0, minimum=0, maximum=500000)
    rows, total = db_analytics.get_ml_features(
        days=days,
        symbol=_optional_symbol(),
        limit=limit,
        offset=offset,
    )
    return _cors(jsonify({"rows": rows, "total": total, "columns": list(db_analytics.ML_FEATURE_KEYS)}))


@app.route("/api/suggestions")
def suggestions():
    days = _int_arg("days", 7, minimum=0, maximum=365)
    days_filter = None if days == 0 else days
    force = request.args.get("force", "").lower() in ("1", "true", "yes")
    result = deepseek_advisor.generate_suggestions(
        days=days_filter,
        symbol=_optional_symbol(),
        force=force,
    )
    status = 200 if not result.get("error") or result.get("suggestions") else 503
    return _cors(jsonify(result)), status


@app.route("/api/config-overrides", methods=["GET"])
def config_overrides_status():
    return _cors(jsonify(symbol_config_admin.get_fleet_overrides_status()))


@app.route("/api/config-overrides/restore", methods=["POST"])
def config_overrides_restore():
    payload = request.get_json(silent=True) or {}
    reason = payload.get("reason")
    result = symbol_config_admin.restore_fleet_to_env_defaults(reason=reason)
    status = 200 if result.get("ok") or result.get("partial") else 400
    return _cors(jsonify(result)), status


@app.route("/api/symbol-config/<symbol>", methods=["GET"])
def symbol_config_get(symbol: str):
    return _cors(jsonify(symbol_config_admin.get_symbol_override(symbol)))


@app.route("/api/symbol-config/apply", methods=["POST"])
def symbol_config_apply():
    payload = request.get_json(silent=True) or {}
    symbol = (payload.get("symbol") or "").strip()
    config_changes = payload.get("config_changes") or {}
    reason = payload.get("reason")
    if not symbol:
        return _cors(jsonify({"ok": False, "error": "symbol required"})), 400
    result = symbol_config_admin.apply_config_changes(symbol, config_changes, reason=reason)
    status = 200 if result.get("ok") else 400
    return _cors(jsonify(result)), status


@app.route("/api/suggestions/apply-all", methods=["POST"])
def suggestions_apply_all():
    payload = request.get_json(silent=True) or {}
    suggestions = payload.get("suggestions") or []
    reason = payload.get("reason")
    if not isinstance(suggestions, list) or not suggestions:
        return _cors(jsonify({"ok": False, "error": "suggestions required"})), 400
    result = symbol_config_admin.apply_suggestions_batch(suggestions, reason=reason)
    status = 200 if result.get("ok") or result.get("partial") else 400
    return _cors(jsonify(result)), status


@app.route("/api/suggestions/restore-all", methods=["POST"])
def suggestions_restore_all():
    payload = request.get_json(silent=True) or {}
    suggestions = payload.get("suggestions") or []
    reason = payload.get("reason")
    if not isinstance(suggestions, list) or not suggestions:
        return _cors(jsonify({"ok": False, "error": "suggestions required"})), 400
    result = symbol_config_admin.restore_suggestions_batch(suggestions, reason=reason)
    status = 200 if result.get("ok") or result.get("partial") else 400
    return _cors(jsonify(result)), status


@app.route("/api/symbol-config/restore", methods=["POST"])
def symbol_config_restore():
    payload = request.get_json(silent=True) or {}
    symbol = (payload.get("symbol") or "").strip()
    config_keys = payload.get("config_keys") or []
    reason = payload.get("reason")
    if not symbol:
        return _cors(jsonify({"ok": False, "error": "symbol required"})), 400
    result = symbol_config_admin.restore_config_keys(symbol, config_keys, reason=reason)
    status = 200 if result.get("ok") else 400
    return _cors(jsonify(result)), status


@app.route("/api/trade-boost/<symbol>", methods=["GET"])
def trade_boost_get(symbol: str):
    return _cors(jsonify(trade_boost.get_status(symbol)))


@app.route("/api/trade-boost/<symbol>/<direction>", methods=["POST"])
def trade_boost_set(symbol: str, direction: str):
    payload = request.get_json(silent=True) or {}
    action = (payload.get("action") or "activate").strip().lower()
    try:
        if action == "deactivate":
            result = trade_boost.deactivate(symbol, direction)
        else:
            result = trade_boost.activate(
                symbol,
                direction,
                created_by=payload.get("source") or "analytics",
            )
    except ValueError as exc:
        result = {"ok": False, "error": str(exc)}
    status = 200 if result.get("ok") else 400
    return _cors(jsonify(result)), status


@app.route("/api/trade-outcomes")
def trade_outcomes():
    days = _int_arg("days", 30)
    limit = _int_arg("limit", 200, minimum=1, maximum=5000)
    offset = _int_arg("offset", 0, minimum=0, maximum=500000)
    rows, total = db_analytics.get_ml_trade_outcomes(
        days=days,
        symbol=_optional_symbol(),
        limit=limit,
        offset=offset,
    )
    return _cors(
        jsonify(
            {
                "rows": rows,
                "total": total,
                "columns": list(db_analytics.ML_TRADE_OUTCOME_KEYS),
            }
        )
    )


@app.route("/api/export/features.csv")
def export_features_csv():
    days = _int_arg("days", 30)
    limit = _int_arg("limit", 50000, minimum=1, maximum=50000)
    rows, _ = db_analytics.get_ml_features(
        days=days,
        symbol=_optional_symbol(),
        limit=limit,
        offset=0,
    )
    csv_text = db_analytics.features_to_csv(rows)
    filename = f"decision_features_{days}d.csv"
    response = Response(csv_text, mimetype="text/csv")
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return _cors(response)


@app.route("/api/export/trade-outcomes.csv")
def export_trade_outcomes_csv():
    days = _int_arg("days", 30)
    limit = _int_arg("limit", 50000, minimum=1, maximum=50000)
    rows, _ = db_analytics.get_ml_trade_outcomes(
        days=days,
        symbol=_optional_symbol(),
        limit=limit,
        offset=0,
    )
    csv_text = db_analytics.trade_outcomes_to_csv(rows)
    filename = f"trade_outcomes_{days}d.csv"
    response = Response(csv_text, mimetype="text/csv")
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return _cors(response)


@app.route("/")
def hub_index():
    return send_from_directory(HUB_DIR, "index.html")


@app.route("/help")
@app.route("/help/")
def help_page():
    return send_from_directory(HUB_DIR, "help.html")


@app.route("/help/entradas")
@app.route("/help/entradas/")
def help_entradas_page():
    return send_from_directory(HUB_DIR, "help-entradas.html")


@app.route("/<path:filename>")
def hub_static(filename: str):
    if filename.startswith("api/") or filename.startswith("analytics/") or filename.startswith("config/"):
        abort(404)
    if ".." in filename:
        abort(404)
    target = HUB_DIR / filename
    if not target.is_file():
        abort(404)
    return send_from_directory(HUB_DIR, filename)


def main() -> None:
    if db_store.is_enabled():
        db_store.run_migrations()
    print(f"Hub http://{HOST}:{PORT}/")
    print(f"Help http://{HOST}:{PORT}/help")
    print(f"Help entradas http://{HOST}:{PORT}/help/entradas")
    print(f"Analytics http://{HOST}:{PORT}/analytics/")
    if CONFIG_PAGE_TOKEN:
        print(f"Config http://{HOST}:{PORT}/config/?token=<CONFIG_PAGE_TOKEN>")
    else:
        print(f"Config http://{HOST}:{PORT}/config/ (set CONFIG_PAGE_TOKEN in .env to protect)")
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
