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
HOST = os.getenv("HUB_HOST", os.getenv("ANALYTICS_HOST", "0.0.0.0"))
PORT = int(os.getenv("HUB_PORT", "8050"))

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
    return _cors(jsonify(db_analytics.get_recent_events(limit, _optional_symbol())))


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


@app.route("/<path:filename>")
def hub_static(filename: str):
    if filename.startswith("api/") or filename.startswith("analytics/"):
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
    print(f"Analytics http://{HOST}:{PORT}/analytics/")
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
