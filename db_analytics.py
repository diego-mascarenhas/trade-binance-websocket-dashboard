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
        "adx_min_trend": config.get("ADX_MIN_TREND"),
        "adx_use_htf": config.get("ADX_USE_HTF"),
        "rsi_long_max": config.get("RSI_LONG_MAX"),
        "rsi_short_min": config.get("RSI_SHORT_MIN"),
        "require_trend_align": config.get("REQUIRE_TREND_ALIGN"),
        "indicator_filters_enabled": config.get("INDICATOR_FILTERS_ENABLED"),
        "rsi_filter_enabled": config.get("RSI_FILTER_ENABLED"),
        "macd_filter_enabled": config.get("MACD_FILTER_ENABLED"),
        "adx_filter_enabled": config.get("ADX_FILTER_ENABLED"),
        "execution_mode": config.get("execution_mode"),
        "execution_enabled": config.get("execution_enabled"),
        "execute_on_valid_entry": config.get("execute_on_valid_entry"),
        "sl": market.get("sl"),
        "prev_sl": market.get("prev_sl"),
        "candle_open": market.get("candle_open"),
        "candle_close": market.get("candle_close"),
        "unrealized_pnl_pct": market.get("unrealized_pnl_pct"),
        "trail_target": market.get("target"),
        "trail_current": market.get("current"),
        "trail_min_pct": market.get("min_pct"),
        "pnl_pct": market.get("pnl_pct"),
        "candle_time": market.get("candle_time"),
        "trail_stage": market.get("stage"),
        "in_profit": market.get("in_profit"),
        "profit_gate_pct": market.get("profit_gate_pct"),
        "close_profit_pct": market.get("close_profit_pct"),
        "sl_anchor": market.get("sl_anchor"),
        "gate_source": market.get("gate_source") or market.get("profit_gate_source"),
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


def get_realized_pnl_daily_stats(
    days: int = 30,
    *,
    symbol: str | None = None,
) -> dict[str, Any]:
    """Realized PnL over a lookback window and calendar daily average (total / days)."""
    empty = {
        "closed_trades": 0,
        "total_realized_pnl": 0.0,
        "daily_avg_usdt": None,
        "source": "none",
    }
    if days <= 0:
        return empty

    symbol_filter = ""
    params: list[Any] = [days]
    if symbol:
        symbol_filter = " AND symbol = %s"
        params.append(symbol.upper())

    if db_store.is_enabled():
        try:
            conn = db_store.connect()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                        SELECT
                            COUNT(*) AS closed_trades,
                            COALESCE(SUM(realized_pnl), 0) AS total_realized_pnl
                        FROM trade_outcomes
                        WHERE created_at >= NOW() - INTERVAL %s DAY{symbol_filter}
                        """,
                        params,
                    )
                    stats = cursor.fetchone() or {}
            finally:
                conn.close()
        except Exception:
            return empty

        total = float(stats.get("total_realized_pnl") or 0)
        closed = int(stats.get("closed_trades") or 0)
        return {
            "closed_trades": closed,
            "total_realized_pnl": total,
            "daily_avg_usdt": total / days if closed else None,
            "source": "db",
        }

    return empty


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


def get_recent_events(
    limit: int = 50,
    symbol: str | None = None,
    *,
    days: int | None = None,
    event_group: str | None = None,
) -> list[dict[str, Any]]:
    if not db_store.is_enabled():
        return []

    limit = max(1, min(limit, 500))
    where_parts: list[str] = []
    params: list[Any] = []
    where_extra, day_params = _days_clause(days)
    if where_extra:
        where_parts.append(where_extra.lstrip(" AND "))
        params.extend(day_params)
    if symbol:
        where_parts.append("symbol = %s")
        params.append(symbol.upper())
    group = (event_group or "").strip().lower()
    if group == "trail":
        where_parts.append(
            "event_type IN ('trail_candle', 'trail_sl', 'trail_sl_skip', 'trail_candle_diag')"
        )
    elif group:
        where_parts.append("event_type = %s")
        params.append(group)

    where_sql = " AND ".join(where_parts) if where_parts else "1=1"
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


def _config_flag(value: Any, *, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("1", "true", "yes")


def _synthesize_block_reason(
    reason: str,
    *,
    count: int,
    pct: float,
    stats: dict[str, Any],
    config: dict[str, Any],
) -> str:
    avg_rsi = stats.get("avg_rsi")
    avg_adx = stats.get("avg_adx")
    avg_htf_adx = stats.get("avg_htf_adx")
    avg_conf = stats.get("avg_confidence")

    def fmt_num(value: Any, digits: int = 1) -> str:
        if value is None:
            return "—"
        try:
            return f"{float(value):.{digits}f}"
        except (TypeError, ValueError):
            return "—"

    if reason == "adx_low":
        use_htf = _config_flag(config.get("ADX_USE_HTF"), default=True)
        min_adx = config.get("ADX_MIN_TREND")
        source = f"HTF ({config.get('HTF_INTERVAL') or 'HTF'})" if use_htf else "1m"
        adx_val = avg_htf_adx if use_htf else avg_adx
        min_text = fmt_num(min_adx, 0) if min_adx is not None else "?"
        return (
            f"ADX {source} medio {fmt_num(adx_val)} < mín {min_text} "
            f"(ADX_USE_HTF={'true' if use_htf else 'false'})"
        )

    if reason == "htf_mismatch":
        return (
            f"Señal LONG/SHORT contra tendencia HTF "
            f"(REQUIRE_TREND_ALIGN={str(_config_flag(config.get('REQUIRE_TREND_ALIGN'), default=True)).lower()})"
        )

    if reason == "signal_cooldown":
        cooldown = config.get("SIGNAL_COOLDOWN_SEC")
        return f"Entrada repetida antes del cooldown ({cooldown or '?'}s)"

    if reason == "same_ob_level":
        return "Mismo OB que la última entrada — espera el siguiente nivel o cierre"

    if reason == "ob_dca_not_ready":
        return "Posición abierta: el precio aún no llegó al siguiente nivel DCA del OB"

    if reason == "rsi_overbought":
        max_rsi = config.get("RSI_LONG_MAX")
        return f"RSI medio {fmt_num(avg_rsi)} > máx LONG {max_rsi or '?'}"

    if reason == "rsi_oversold":
        min_rsi = config.get("RSI_SHORT_MIN")
        return f"RSI medio {fmt_num(avg_rsi)} < mín SHORT {min_rsi or '?'}"

    if reason == "macd_bearish":
        return "MACD histograma negativo en señales LONG"

    if reason == "macd_bullish":
        return "MACD histograma positivo en señales SHORT"

    if reason == "no_active_plan":
        return "Plan SL/TP inactivo — no hay niveles calculables"

    if reason == "symbol_disabled":
        return "Par deshabilitado (symbol_trading_enabled=false)"

    if reason == "missing_candle":
        return "Sin vela 1m (forming o cerrada) — no se registra valid_entry"

    if reason == "missing_entry":
        return "Precio de entrada no disponible en el cambio de señal"

    if reason == "low signal" or reason == "low_confidence":
        min_conf = config.get("MIN_CONFIDENCE")
        return f"Confianza media {fmt_num(avg_conf, 0)}% < mín {min_conf or '?'}%"

    if reason == "(none)":
        return "Eventos sin block_reason registrado"

    return f"{count} eventos ({pct:.1f}% del total bloqueado)"


def get_block_summary(
    days: int | None = 7,
    symbol: str | None = None,
) -> dict[str, Any]:
    """Human-readable synthesis of block_reason distribution in the selected range."""
    if not db_store.is_enabled():
        return {"enabled": False, "items": [], "total_blocked": 0}

    stats_rows = get_block_indicator_stats(days, symbol)
    config = get_latest_config_snapshot()

    try:
        import symbol_config_admin

        fleet_status = symbol_config_admin.get_fleet_overrides_status()
        fleet_wide = fleet_status.get("fleet_wide") or {}
        if fleet_wide:
            config = {**config, **fleet_wide}
    except Exception:
        fleet_status = {"active": False}

    activity = _get_block_activity_counts(days, symbol)

    if not stats_rows:
        return {
            "enabled": True,
            "total_blocked": 0,
            "items": [],
            "activity": activity,
            "fleet_overrides_active": bool(fleet_status.get("active")),
            "dedup_log_only": True,
            "config": _block_summary_config(config),
        }

    total = sum(row["count"] for row in stats_rows)
    items: list[dict[str, Any]] = []

    for row in stats_rows:
        reason = row["block_reason"]
        count = row["count"]
        pct = round(100 * count / total, 1) if total else 0.0
        items.append(
            {
                "block_reason": reason,
                "count": count,
                "pct": pct,
                "avg_rsi": row.get("avg_rsi"),
                "avg_adx": row.get("avg_adx"),
                "avg_htf_adx": row.get("avg_htf_adx"),
                "avg_confidence": row.get("avg_confidence"),
                "summary": _synthesize_block_reason(
                    reason,
                    count=count,
                    pct=pct,
                    stats=row,
                    config=config,
                ),
            }
        )

    return {
        "enabled": True,
        "total_blocked": total,
        "items": items,
        "activity": activity,
        "fleet_overrides_active": bool(fleet_status.get("active")),
        "dedup_log_only": True,
        "config": _block_summary_config(config),
    }


def _block_summary_config(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "ADX_MIN_TREND": config.get("ADX_MIN_TREND"),
        "ADX_USE_HTF": config.get("ADX_USE_HTF"),
        "ADX_FILTER_ENABLED": config.get("ADX_FILTER_ENABLED"),
        "HTF_INTERVAL": config.get("HTF_INTERVAL"),
        "REQUIRE_TREND_ALIGN": config.get("REQUIRE_TREND_ALIGN"),
        "SIGNAL_COOLDOWN_SEC": config.get("SIGNAL_COOLDOWN_SEC"),
        "MIN_CONFIDENCE": config.get("MIN_CONFIDENCE"),
    }


def _get_block_activity_counts(
    days: int | None,
    symbol: str | None,
) -> dict[str, int]:
    if not db_store.is_enabled():
        return {}

    where_sql, params = _where_parts(days, symbol)
    try:
        conn = db_store.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        SUM(event_type = 'valid_entry') AS valid_entries,
                        SUM(event_type = 'order_live_open') AS live_opens,
                        SUM(event_type = 'order_dry_run') AS dry_runs,
                        SUM(event_type = 'indicator_blocked') AS indicator_blocked,
                        SUM(event_type = 'valid_entry_blocked') AS valid_entry_blocked,
                        SUM(
                            COALESCE(
                                CAST(JSON_UNQUOTE(JSON_EXTRACT(market_snapshot, '$.dedup_suppressed')) AS UNSIGNED),
                                0
                            )
                        ) AS dedup_suppressed_logged
                    FROM decision_events
                    WHERE {where_sql}
                    """,
                    params,
                )
                row = cursor.fetchone() or {}
        finally:
            conn.close()
    except Exception:
        return {}

    return {
        key: int(row.get(key) or 0)
        for key in (
            "valid_entries",
            "live_opens",
            "dry_runs",
            "indicator_blocked",
            "valid_entry_blocked",
            "dedup_suppressed_logged",
        )
    }


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

