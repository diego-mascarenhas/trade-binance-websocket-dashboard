"""Read-only analytics on decision_events and trade_outcomes (ML-ready exports)."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from typing import Any

import db_store

ML_TRADE_OUTCOME_KEYS = (
    "id",
    "symbol",
    "direction",
    "entry_decision_event_id",
    "entry_price",
    "exit_price",
    "exit_qty",
    "realized_pnl",
    "pnl_pct",
    "exit_type",
    "outcome",
    "sl_price",
    "tp_price",
    "tp_type",
    "be_applied",
    "dca_legs_placed",
    "entry_opened_at",
    "entry_signal",
    "entry_confidence",
    "entry_trend",
    "entry_trend_aligned",
    "entry_rsi",
    "entry_macd_hist",
    "entry_adx",
    "entry_htf_adx",
    "entry_smc_pattern",
    "entry_smc_trend",
    "entry_price_at_signal",
    "entry_change_24h",
    "closed_at",
)

ML_FEATURE_KEYS = (
    "symbol",
    "event_type",
    "outcome",
    "block_reason",
    "created_at",
    "signal",
    "confidence",
    "action",
    "trend",
    "trend_aligned",
    "rsi",
    "macd_hist",
    "adx",
    "htf_adx",
    "smc_pattern",
    "smc_trend",
    "change_24h",
    "price",
    "min_confidence",
    "htf_interval",
    "execution_mode",
    "execution_enabled",
    "execute_on_valid_entry",
    "config_version",
)


def _days_clause(days: int | None) -> tuple[str, list[Any]]:
    if days is None or days <= 0:
        return "", []
    return " AND created_at >= NOW() - INTERVAL %s DAY", [days]


def _parse_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str) and value:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _flatten_row(row: dict[str, Any]) -> dict[str, Any]:
    market = _parse_json(row.get("market_snapshot"))
    config = _parse_json(row.get("config_snapshot"))
    created = row.get("created_at")
    if isinstance(created, datetime):
        created = created.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "id": row.get("id"),
        "symbol": row.get("symbol"),
        "event_type": row.get("event_type"),
        "outcome": row.get("outcome"),
        "block_reason": row.get("block_reason"),
        "created_at": created,
        "signal": market.get("signal"),
        "confidence": market.get("confidence"),
        "action": market.get("action"),
        "trend": market.get("trend"),
        "trend_aligned": market.get("trend_aligned"),
        "rsi": market.get("rsi"),
        "macd_hist": market.get("macd_hist"),
        "adx": market.get("adx"),
        "htf_adx": market.get("htf_adx"),
        "smc_pattern": market.get("smc_pattern"),
        "smc_trend": market.get("smc_trend"),
        "change_24h": market.get("change_24h"),
        "price": market.get("price"),
        "min_confidence": config.get("MIN_CONFIDENCE"),
        "htf_interval": config.get("HTF_INTERVAL"),
        "execution_mode": config.get("execution_mode"),
        "execution_enabled": config.get("execution_enabled"),
        "execute_on_valid_entry": config.get("execute_on_valid_entry"),
        "config_version": row.get("config_version"),
    }


def is_available() -> bool:
    return db_store.is_enabled()


def get_overview(days: int | None = 7) -> dict[str, Any]:
    if not db_store.is_enabled():
        return {"enabled": False}

    where_extra, params = _days_clause(days)
    try:
        conn = db_store.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        COUNT(*) AS total_events,
                        MIN(created_at) AS first_event,
                        MAX(created_at) AS last_event,
                        COUNT(DISTINCT symbol) AS symbols_seen,
                        SUM(event_type = 'valid_entry') AS valid_entries,
                        SUM(event_type = 'valid_entry_blocked') AS valid_entry_blocked,
                        SUM(event_type = 'indicator_blocked') AS indicator_blocked,
                        SUM(event_type = 'order_skip') AS order_skips,
                        SUM(event_type = 'order_dry_run') AS order_dry_runs,
                        SUM(event_type = 'order_live_open') AS order_live_opens
                    FROM decision_events
                    WHERE 1=1{where_extra}
                    """,
                    params,
                )
                row = cursor.fetchone() or {}
        finally:
            conn.close()
    except Exception as exc:
        return {"enabled": True, "error": str(exc)}

    for key in ("first_event", "last_event"):
        value = row.get(key)
        if isinstance(value, datetime):
            row[key] = value.astimezone(timezone.utc).isoformat()

    numeric_keys = (
        "total_events",
        "symbols_seen",
        "valid_entries",
        "valid_entry_blocked",
        "indicator_blocked",
        "order_skips",
        "order_dry_runs",
        "order_live_opens",
    )
    for key in numeric_keys:
        row[key] = int(row.get(key) or 0)

    row["enabled"] = True
    row["days"] = days
    row.update(_trade_outcomes_summary(days))
    return row


def _trade_outcomes_summary(days: int | None) -> dict[str, Any]:
    if not db_store.is_enabled():
        return {
            "closed_trades": 0,
            "trade_wins": 0,
            "trade_losses": 0,
            "trade_breakeven": 0,
            "total_realized_pnl": 0.0,
            "avg_realized_pnl": None,
        }

    where_extra, params = _days_clause(days)
    try:
        conn = db_store.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        COUNT(*) AS closed_trades,
                        SUM(outcome = 'win') AS trade_wins,
                        SUM(outcome = 'loss') AS trade_losses,
                        SUM(outcome = 'breakeven') AS trade_breakeven,
                        COALESCE(SUM(realized_pnl), 0) AS total_realized_pnl,
                        AVG(realized_pnl) AS avg_realized_pnl
                    FROM trade_outcomes
                    WHERE 1=1{where_extra}
                    """,
                    params,
                )
                stats = cursor.fetchone() or {}
        finally:
            conn.close()
    except Exception:
        return {
            "closed_trades": 0,
            "trade_wins": 0,
            "trade_losses": 0,
            "trade_breakeven": 0,
            "total_realized_pnl": 0.0,
            "avg_realized_pnl": None,
        }

    avg_pnl = stats.get("avg_realized_pnl")
    return {
        "closed_trades": int(stats.get("closed_trades") or 0),
        "trade_wins": int(stats.get("trade_wins") or 0),
        "trade_losses": int(stats.get("trade_losses") or 0),
        "trade_breakeven": int(stats.get("trade_breakeven") or 0),
        "total_realized_pnl": float(stats.get("total_realized_pnl") or 0),
        "avg_realized_pnl": float(avg_pnl) if avg_pnl is not None else None,
    }


def _flatten_trade_outcome_row(row: dict[str, Any]) -> dict[str, Any]:
    created = row.get("created_at")
    if isinstance(created, datetime):
        created = created.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    entry_opened = row.get("entry_opened_at")
    if isinstance(entry_opened, datetime):
        entry_opened = entry_opened.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    entry_market = _parse_json(row.get("entry_market_snapshot"))

    return {
        "id": row.get("id"),
        "symbol": row.get("symbol"),
        "direction": row.get("direction"),
        "entry_decision_event_id": row.get("entry_decision_event_id"),
        "entry_price": row.get("entry_price"),
        "exit_price": row.get("exit_price"),
        "exit_qty": row.get("exit_qty"),
        "realized_pnl": row.get("realized_pnl"),
        "pnl_pct": row.get("pnl_pct"),
        "exit_type": row.get("exit_type"),
        "outcome": row.get("outcome"),
        "sl_price": row.get("sl_price"),
        "tp_price": row.get("tp_price"),
        "tp_type": row.get("tp_type"),
        "be_applied": bool(row.get("be_applied")),
        "dca_legs_placed": row.get("dca_legs_placed"),
        "entry_opened_at": entry_opened,
        "entry_signal": entry_market.get("signal"),
        "entry_confidence": entry_market.get("confidence"),
        "entry_trend": entry_market.get("trend"),
        "entry_trend_aligned": entry_market.get("trend_aligned"),
        "entry_rsi": entry_market.get("rsi"),
        "entry_macd_hist": entry_market.get("macd_hist"),
        "entry_adx": entry_market.get("adx"),
        "entry_htf_adx": entry_market.get("htf_adx"),
        "entry_smc_pattern": entry_market.get("smc_pattern"),
        "entry_smc_trend": entry_market.get("smc_trend"),
        "entry_price_at_signal": entry_market.get("price"),
        "entry_change_24h": entry_market.get("change_24h"),
        "closed_at": created,
    }


def get_ml_trade_outcomes(
    *,
    days: int | None = 30,
    symbol: str | None = None,
    limit: int = 5000,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    if not db_store.is_enabled():
        return [], 0

    limit = max(1, min(limit, 50000))
    offset = max(0, offset)
    where_sql, params = _where_parts(days, symbol)

    try:
        conn = db_store.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) AS total FROM trade_outcomes WHERE {where_sql}",
                    params,
                )
                total = int((cursor.fetchone() or {}).get("total") or 0)
                cursor.execute(
                    f"""
                    SELECT
                        id, symbol, direction, entry_price, exit_price, exit_qty,
                        realized_pnl, pnl_pct, exit_type, outcome,
                        sl_price, tp_price, tp_type, be_applied, dca_legs_placed,
                        entry_opened_at, entry_market_snapshot, entry_decision_event_id,
                        created_at
                    FROM trade_outcomes
                    WHERE {where_sql}
                    ORDER BY created_at ASC, id ASC
                    LIMIT %s OFFSET %s
                    """,
                    [*params, limit, offset],
                )
                rows = cursor.fetchall()
        finally:
            conn.close()
    except Exception:
        return [], 0

    return [_flatten_trade_outcome_row(row) for row in rows], total


def trade_outcomes_to_csv(rows: list[dict[str, Any]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(ML_TRADE_OUTCOME_KEYS), extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def get_hourly_series(days: int | None = 7, symbol: str | None = None) -> list[dict[str, Any]]:
    if not db_store.is_enabled():
        return []

    where_parts: list[str] = []
    params: list[Any] = []
    if days and days > 0:
        where_parts.append("created_at >= NOW() - INTERVAL %s DAY")
        params.append(days)
    if symbol:
        where_parts.append("symbol = %s")
        params.append(symbol.upper())
    where_sql = " AND ".join(where_parts) if where_parts else "1=1"

    try:
        conn = db_store.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        DATE_FORMAT(created_at, '%%Y-%%m-%%d %%H:00:00') AS bucket,
                        event_type,
                        COUNT(*) AS count
                    FROM decision_events
                    WHERE {where_sql}
                    GROUP BY bucket, event_type
                    ORDER BY bucket ASC, event_type ASC
                    """,
                    params,
                )
                rows = cursor.fetchall()
        finally:
            conn.close()
    except Exception:
        return []

    return [
        {
            "bucket": row["bucket"],
            "event_type": row["event_type"],
            "count": int(row["count"]),
        }
        for row in rows
    ]


def get_daily_series(days: int | None = 30, symbol: str | None = None) -> list[dict[str, Any]]:
    if not db_store.is_enabled():
        return []

    where_parts: list[str] = []
    params: list[Any] = []
    where_extra, day_params = _days_clause(days)
    if where_extra:
        where_parts.append(where_extra.lstrip(" AND "))
        params.extend(day_params)
    if symbol:
        where_parts.append("symbol = %s")
        params.append(symbol.upper())

    where_sql = " AND ".join(where_parts) if where_parts else "1=1"

    try:
        conn = db_store.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        DATE(created_at) AS bucket,
                        event_type,
                        COUNT(*) AS count
                    FROM decision_events
                    WHERE {where_sql}
                    GROUP BY bucket, event_type
                    ORDER BY bucket ASC, event_type ASC
                    """,
                    params,
                )
                rows = cursor.fetchall()
        finally:
            conn.close()
    except Exception:
        return []

    return [
        {
            "bucket": str(row["bucket"]),
            "event_type": row["event_type"],
            "count": int(row["count"]),
        }
        for row in rows
    ]


def get_breakdown(
    field: str,
    *,
    days: int | None = 7,
    symbol: str | None = None,
) -> list[dict[str, Any]]:
    allowed = {"event_type", "block_reason", "outcome", "symbol"}
    if field not in allowed:
        return []

    if not db_store.is_enabled():
        return []

    where_parts: list[str] = []
    params: list[Any] = []
    where_extra, day_params = _days_clause(days)
    if where_extra:
        where_parts.append(where_extra.lstrip(" AND "))
        params.extend(day_params)
    if symbol:
        where_parts.append("symbol = %s")
        params.append(symbol.upper())

    where_sql = " AND ".join(where_parts) if where_parts else "1=1"

    try:
        conn = db_store.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        COALESCE({field}, '(none)') AS label,
                        COUNT(*) AS count
                    FROM decision_events
                    WHERE {where_sql}
                    GROUP BY label
                    ORDER BY count DESC, label ASC
                    LIMIT 50
                    """,
                    params,
                )
                rows = cursor.fetchall()
        finally:
            conn.close()
    except Exception:
        return []

    return [{"label": row["label"], "count": int(row["count"])} for row in rows]


def get_recent_events(limit: int = 50, symbol: str | None = None) -> list[dict[str, Any]]:
    if not db_store.is_enabled():
        return []

    limit = max(1, min(limit, 500))
    params: list[Any] = []
    where_sql = "1=1"
    if symbol:
        where_sql = "symbol = %s"
        params.append(symbol.upper())

    params.append(limit)

    try:
        conn = db_store.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        id, symbol, event_type, outcome, block_reason,
                        config_version, created_at,
                        market_snapshot, config_snapshot
                    FROM decision_events
                    WHERE {where_sql}
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s
                    """,
                    params,
                )
                rows = cursor.fetchall()
        finally:
            conn.close()
    except Exception:
        return []

    return [_flatten_row(row) for row in rows]


def get_ml_features(
    *,
    days: int | None = 30,
    symbol: str | None = None,
    limit: int = 5000,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    if not db_store.is_enabled():
        return [], 0

    limit = max(1, min(limit, 50000))
    offset = max(0, offset)

    where_parts: list[str] = []
    params: list[Any] = []
    where_extra, day_params = _days_clause(days)
    if where_extra:
        where_parts.append(where_extra.lstrip(" AND "))
        params.extend(day_params)
    if symbol:
        where_parts.append("symbol = %s")
        params.append(symbol.upper())

    where_sql = " AND ".join(where_parts) if where_parts else "1=1"

    try:
        conn = db_store.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) AS total FROM decision_events WHERE {where_sql}",
                    params,
                )
                total = int((cursor.fetchone() or {}).get("total") or 0)

                cursor.execute(
                    f"""
                    SELECT
                        id, symbol, event_type, outcome, block_reason,
                        config_version, created_at,
                        market_snapshot, config_snapshot
                    FROM decision_events
                    WHERE {where_sql}
                    ORDER BY created_at ASC, id ASC
                    LIMIT %s OFFSET %s
                    """,
                    [*params, limit, offset],
                )
                rows = cursor.fetchall()
        finally:
            conn.close()
    except Exception:
        return [], 0

    return [_flatten_row(row) for row in rows], total


def features_to_csv(rows: list[dict[str, Any]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(ML_FEATURE_KEYS), extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue()


def _where_parts(days: int | None, symbol: str | None) -> tuple[str, list[Any]]:
    where_parts: list[str] = []
    params: list[Any] = []
    where_extra, day_params = _days_clause(days)
    if where_extra:
        where_parts.append(where_extra.lstrip(" AND "))
        params.extend(day_params)
    if symbol:
        where_parts.append("symbol = %s")
        params.append(symbol.upper())
    where_sql = " AND ".join(where_parts) if where_parts else "1=1"
    return where_sql, params


def get_block_indicator_stats(
    days: int | None = 7,
    symbol: str | None = None,
) -> list[dict[str, Any]]:
    """Average RSI/ADX at block time, grouped by block_reason."""
    if not db_store.is_enabled():
        return []

    where_sql, params = _where_parts(days, symbol)
    try:
        conn = db_store.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        COALESCE(block_reason, '(none)') AS block_reason,
                        COUNT(*) AS count,
                        AVG(CAST(JSON_UNQUOTE(JSON_EXTRACT(market_snapshot, '$.rsi')) AS DECIMAL(12,4))) AS avg_rsi,
                        AVG(CAST(JSON_UNQUOTE(JSON_EXTRACT(market_snapshot, '$.adx')) AS DECIMAL(12,4))) AS avg_adx,
                        AVG(CAST(JSON_UNQUOTE(JSON_EXTRACT(market_snapshot, '$.htf_adx')) AS DECIMAL(12,4))) AS avg_htf_adx,
                        AVG(CAST(JSON_UNQUOTE(JSON_EXTRACT(market_snapshot, '$.confidence')) AS DECIMAL(12,4))) AS avg_confidence
                    FROM decision_events
                    WHERE {where_sql}
                      AND block_reason IS NOT NULL
                    GROUP BY block_reason
                    ORDER BY count DESC
                    LIMIT 20
                    """,
                    params,
                )
                rows = cursor.fetchall()
        finally:
            conn.close()
    except Exception:
        return []

    result: list[dict[str, Any]] = []
    for row in rows:
        item = {"block_reason": row["block_reason"], "count": int(row["count"] or 0)}
        for key in ("avg_rsi", "avg_adx", "avg_htf_adx", "avg_confidence"):
            value = row.get(key)
            item[key] = float(value) if value is not None else None
        result.append(item)
    return result


def get_symbol_event_stats(days: int | None = 7) -> list[dict[str, Any]]:
    if not db_store.is_enabled():
        return []

    where_sql, params = _where_parts(days, None)
    try:
        conn = db_store.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        symbol,
                        COUNT(*) AS total,
                        SUM(event_type = 'valid_entry') AS valid_entries,
                        SUM(event_type = 'indicator_blocked') AS indicator_blocked,
                        SUM(event_type = 'valid_entry_blocked') AS valid_entry_blocked
                    FROM decision_events
                    WHERE {where_sql}
                    GROUP BY symbol
                    ORDER BY total DESC
                    LIMIT 20
                    """,
                    params,
                )
                rows = cursor.fetchall()
        finally:
            conn.close()
    except Exception:
        return []

    return [
        {
            "symbol": row["symbol"],
            "total": int(row["total"] or 0),
            "valid_entries": int(row["valid_entries"] or 0),
            "indicator_blocked": int(row["indicator_blocked"] or 0),
            "valid_entry_blocked": int(row["valid_entry_blocked"] or 0),
        }
        for row in rows
    ]


def get_latest_config_snapshot() -> dict[str, Any]:
    if not db_store.is_enabled():
        return {}

    try:
        conn = db_store.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT config_snapshot
                    FROM decision_events
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """
                )
                row = cursor.fetchone()
        finally:
            conn.close()
    except Exception:
        return {}

    if not row:
        return {}
    return _parse_json(row.get("config_snapshot"))


def build_suggestion_context(
    days: int | None = 7,
    symbol: str | None = None,
) -> dict[str, Any]:
    """Compact payload for DeepSeek config suggestions."""
    days_filter = None if days is not None and days <= 0 else days
    return {
        "range_days": days_filter,
        "symbol_filter": symbol.upper() if symbol else None,
        "overview": get_overview(days_filter),
        "event_types": get_breakdown("event_type", days=days_filter, symbol=symbol),
        "block_reasons": get_breakdown("block_reason", days=days_filter, symbol=symbol),
        "symbols": get_symbol_event_stats(days_filter),
        "block_indicator_stats": get_block_indicator_stats(days_filter, symbol),
        "recent_valid_entries": [
            row
            for row in get_recent_events(30, symbol)
            if row.get("event_type") == "valid_entry"
        ][:10],
        "recent_indicator_blocks": [
            row
            for row in get_recent_events(30, symbol)
            if row.get("event_type") == "indicator_blocked"
        ][:10],
        "active_config": get_latest_config_snapshot(),
        "trade_outcomes": _trade_outcomes_summary(days_filter),
        "overridable_keys": sorted(
            [
                "MIN_CONFIDENCE",
                "HTF_INTERVAL",
                "REQUIRE_TREND_ALIGN",
                "SIGNAL_COOLDOWN_SEC",
                "RSI_LONG_MAX",
                "RSI_SHORT_MIN",
                "ADX_MIN_TREND",
                "ADX_USE_HTF",
                "INDICATOR_FILTERS_ENABLED",
                "RSI_FILTER_ENABLED",
                "ADX_FILTER_ENABLED",
                "symbol_trading_enabled",
            ]
        ),
    }

