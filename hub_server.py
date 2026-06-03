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
import symbol_config_admin

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


@app.route("/api/fleet-exposure")
def fleet_exposure():
    return _cors(jsonify(execution.get_fleet_side_exposure()))


@app.route("/api/health")
def health():
    db_store.init()
    payload = {
        "ok": True,
        "db_enabled": db_store.is_enabled(),
        "db_ready": db_store.ensure_schema() if db_store.is_enabled() else False,
        "deepseek_enabled": deepseek_advisor.is_enabled(),
        "deepseek_configured": deepseek_advisor.is_configured(),
    }
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


@app.route("/")
def hub_index():
    return send_from_directory(HUB_DIR, "index.html")


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
    print(f"Analytics http://{HOST}:{PORT}/analytics/")
    app.run(host=HOST, port=PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
