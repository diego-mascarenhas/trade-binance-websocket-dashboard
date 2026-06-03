"""Binance Futures order execution (dry-run or live), modeled on trade-binance-websocket-order-blocks."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, ROUND_DOWN
from typing import Any

import db_store
import symbol_config
import telegram_notify as telegram
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).lower() in ("1", "true", "yes")


EXECUTION_ENABLED = _env_bool("EXECUTION_ENABLED")
EXECUTE_ON_VALID_ENTRY = _env_bool("EXECUTE_ON_VALID_ENTRY")
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "dry").lower()
FAPI_BASE = os.getenv("FAPI_BASE", "https://fapi.binance.com").rstrip("/")
POSITION_SIZE_USDT = float(os.getenv("POSITION_SIZE_USDT", "25"))
POSITION_WALLET_PCT = float(os.getenv("POSITION_WALLET_PCT", "0"))
def _parse_leverage_env() -> tuple[str, int]:
    mode = os.getenv("LEVERAGE_MODE", "").strip().lower()
    raw = os.getenv("LEVERAGE", "4").strip().lower()
    if mode in ("max", "maximum", "true", "1", "yes", "on") or raw in ("max", "maximum"):
        fallback_raw = os.getenv("LEVERAGE_FALLBACK", "4")
        try:
            return "max", max(int(fallback_raw), 1)
        except ValueError:
            return "max", 4
    try:
        return "fixed", max(int(raw), 1)
    except ValueError:
        logger.warning("Invalid LEVERAGE=%r — using 4x", raw)
        return "fixed", 4


LEVERAGE_MODE, LEVERAGE = _parse_leverage_env()
REST_PLACE_SL_TP = _env_bool("REST_PLACE_SL_TP", "true")
REST_SL_TP_FILL_WAIT = int(os.getenv("REST_SL_TP_FILL_WAIT", "90"))
REST_SL_TP_POLL_INTERVAL = float(os.getenv("REST_SL_TP_POLL_INTERVAL", "2"))
TP_ORDER_TYPE = os.getenv("TP_ORDER_TYPE", "trailing").lower()
TP_TRAILING_CALLBACK_RATE = float(os.getenv("TP_TRAILING_CALLBACK_RATE", "0.5"))
EXECUTION_ORDER_COOLDOWN = int(os.getenv("EXECUTION_ORDER_COOLDOWN", "180"))
EXECUTION_MAINTENANCE_SEC = float(os.getenv("EXECUTION_MAINTENANCE_SEC", "15"))
ENTRY_LIMIT_TTL_SEC = int(os.getenv("ENTRY_LIMIT_TTL_SEC", "600"))
PROTECTION_RECONCILE_ENABLED = _env_bool("PROTECTION_RECONCILE_ENABLED", "true")
FLEET_SIDE_BALANCE_MAX_PCT = float(os.getenv("FLEET_SIDE_BALANCE_MAX_PCT", "20"))
FLEET_EXPOSURE_CACHE_SEC = float(os.getenv("FLEET_EXPOSURE_CACHE_SEC", "8"))
EXECUTION_BLOCK_IF_OPEN = _env_bool("EXECUTION_BLOCK_IF_OPEN", "true")
EXECUTION_POSITION_CACHE_SEC = float(os.getenv("EXECUTION_POSITION_CACHE_SEC", "5"))
BE_EXIT_PNL_MAX_USDT = float(os.getenv("BE_EXIT_PNL_MAX_USDT", "0.15"))
BE_EXIT_PRICE_PCT = float(os.getenv("BE_EXIT_PRICE_PCT", "0.12"))
LOG_DIR = os.getenv("LOG_DIR", "logs")
TRADE_PLAN_EXECUTE_DCA = _env_bool("TRADE_PLAN_EXECUTE_DCA", "true")
TRADE_PLAN_DCA_SIGNAL_DRIVEN = _env_bool("TRADE_PLAN_DCA_SIGNAL_DRIVEN", "true")

_status_lock = threading.Lock()
_maintenance_lock = threading.Lock()
_last_maintenance: dict[str, float] = {}
_fleet_exposure_lock = threading.Lock()
_fleet_exposure_cache: tuple[float, dict[str, Any]] | None = None
_hedge_mode_lock = threading.Lock()
_hedge_mode: bool | None = None
_last_execution_monotonic: float | None = None


def _execution_status_message() -> str:
    if not EXECUTION_ENABLED:
        return "Execution disabled"
    if not EXECUTE_ON_VALID_ENTRY:
        return f"Valid entries off · {EXECUTION_MODE} ready"
    return f"Auto on valid entry · {EXECUTION_MODE}"


_execution_status: dict[str, Any] = {
    "enabled": EXECUTION_ENABLED,
    "execute_on_valid_entry": EXECUTE_ON_VALID_ENTRY,
    "auto_execute": EXECUTION_ENABLED and EXECUTE_ON_VALID_ENTRY,
    "mode": EXECUTION_MODE,
    "message": _execution_status_message(),
    "last_event": None,
    "last_at": None,
    "last_order_id": None,
    "last_symbol": None,
    "last_direction": None,
}

_symbol_filters: dict[str, dict[str, Decimal]] = {}
_max_leverage_cache: dict[str, int] = {}
_filters_lock = threading.Lock()
_leverage_lock = threading.Lock()
_position_snapshot_lock = threading.Lock()
_position_snapshot_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_trade_context_lock = threading.Lock()
_trade_context: dict[str, dict[str, Any]] = {}


def _trade_context_path() -> str:
    return os.path.join(LOG_DIR, "trade_context.json")


def _load_trade_context_store() -> dict[str, dict[str, Any]]:
    global _trade_context
    path = _trade_context_path()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read trade_context.json: %s", exc)
        return {}


def _persist_trade_context_store() -> None:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(_trade_context_path(), "w", encoding="utf-8") as handle:
            json.dump(_trade_context, handle, indent=2, default=str)
    except OSError:
        logger.exception("Failed writing trade_context.json")


def _get_trade_context(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper()
    with _trade_context_lock:
        if not _trade_context:
            _trade_context.update(_load_trade_context_store())
        return dict(_trade_context.get(symbol, {}))


def _update_trade_context(symbol: str, **fields: Any) -> dict[str, Any]:
    symbol = symbol.upper()
    with _trade_context_lock:
        if not _trade_context:
            _trade_context.update(_load_trade_context_store())
        current = dict(_trade_context.get(symbol, {}))
        current.update(fields)
        _trade_context[symbol] = current
        _persist_trade_context_store()
        return dict(current)


def record_trade_context(
    symbol: str,
    direction: str,
    *,
    entry: str | float | None = None,
    sl: str | float | None = None,
    tp: str | float | None = None,
    tp_type: str | None = None,
    dca_legs_placed: int | None = None,
    dca_max_legs: int | None = None,
) -> None:
    fields: dict[str, Any] = {
        "direction": direction.upper(),
        "entry": str(entry) if entry is not None else None,
        "sl": str(sl) if sl is not None else None,
        "tp": str(tp) if tp is not None else None,
        "tp_type": (tp_type or TP_ORDER_TYPE).lower(),
        "was_open": False,
        "exit_notified": False,
    }
    if dca_legs_placed is not None:
        fields["dca_legs_placed"] = int(dca_legs_placed)
    if dca_max_legs is not None:
        fields["dca_max_legs"] = int(dca_max_legs)
    _update_trade_context(symbol, **fields)


def dca_legs_placed(symbol: str) -> int:
    try:
        return max(int(_get_trade_context(symbol).get("dca_legs_placed") or 0), 0)
    except (TypeError, ValueError):
        return 0


def reset_dca_state(symbol: str) -> None:
    _update_trade_context(symbol.upper(), dca_legs_placed=0, dca_max_legs=0)


def _sync_dca_leg_count(symbol: str) -> None:
    """Mark first leg as done once the initial limit has filled into a position."""
    symbol = symbol.upper()
    ctx = _get_trade_context(symbol)
    try:
        max_legs = int(ctx.get("dca_max_legs") or 0)
    except (TypeError, ValueError):
        max_legs = 0
    if max_legs <= 0:
        return
    if dca_legs_placed(symbol) > 0:
        return
    if not has_open_position(symbol):
        return
    pos_dir = get_open_position_direction(symbol)
    ctx_dir = str(ctx.get("direction") or "").upper()
    if pos_dir and ctx_dir and pos_dir != ctx_dir:
        return
    _update_trade_context(symbol, dca_legs_placed=1)


def get_open_position_direction(symbol: str) -> str | None:
    snapshot = _fetch_exchange_exposure(symbol.upper())
    if snapshot.get("open"):
        direction = snapshot.get("direction")
        return str(direction).upper() if direction in ("LONG", "SHORT") else None
    return None


def _format_realized_pnl(pnl: float) -> str:
    sign = "+" if pnl >= 0 else ""
    return f"{sign}{pnl:.2f} USDT"


def _fetch_last_realized_trade(symbol: str) -> dict[str, Any] | None:
    if not _keys_configured():
        return None
    try:
        resp = _fapi_request("GET", "/fapi/v1/userTrades", {"symbol": symbol.upper(), "limit": 30})
    except RuntimeError as exc:
        logger.warning("userTrades failed for %s: %s", symbol, exc)
        return None
    if not isinstance(resp, list):
        return None
    for row in reversed(resp):
        try:
            pnl = float(row.get("realizedPnl", 0) or 0)
        except (TypeError, ValueError):
            pnl = 0.0
        if pnl == 0:
            continue
        try:
            price = float(row.get("price", 0) or 0)
        except (TypeError, ValueError):
            price = 0.0
        if price <= 0:
            continue
        return {"price": price, "qty": row.get("qty"), "realized_pnl": pnl}
    return None


def _is_breakeven_exit(entry: float | None, exit_price: float, pnl: float) -> bool:
    if entry is None or entry <= 0:
        return False
    price_ok = abs(exit_price - entry) / entry * 100 <= BE_EXIT_PRICE_PCT
    pnl_ok = abs(pnl) <= BE_EXIT_PNL_MAX_USDT
    return price_ok and pnl_ok


def _maybe_notify_position_exit(symbol: str, snapshot: dict[str, Any]) -> None:
    if not _keys_configured() or not telegram.is_configured():
        return

    symbol = symbol.upper()
    is_active = bool(snapshot.get("open") or snapshot.get("pending"))
    ctx = _get_trade_context(symbol)

    if snapshot.get("open"):
        _update_trade_context(
            symbol,
            was_open=True,
            direction=snapshot.get("direction") or ctx.get("direction"),
            exit_notified=False,
        )
        return

    if is_active or not ctx.get("was_open") or ctx.get("exit_notified"):
        return

    direction = str(ctx.get("direction") or "LONG")
    sl = ctx.get("sl")
    tp = ctx.get("tp")
    tp_type = str(ctx.get("tp_type") or TP_ORDER_TYPE).lower()
    entry_raw = ctx.get("entry")
    entry = float(entry_raw) if entry_raw not in (None, "") else None

    trade = _fetch_last_realized_trade(symbol)
    if not trade:
        _update_trade_context(
            symbol,
            was_open=False,
            exit_notified=True,
            dca_legs_placed=0,
            dca_max_legs=0,
        )
        return

    exit_price = float(trade["price"])
    pnl = float(trade["realized_pnl"])
    pnl_label = _format_realized_pnl(pnl)

    _update_trade_context(
        symbol,
        was_open=False,
        exit_notified=True,
        dca_legs_placed=0,
        dca_max_legs=0,
    )
    _append_orders_log(
        "position_closed",
        symbol=symbol,
        direction=direction,
        exit_price=exit_price,
        realized_pnl=pnl,
        sl=sl,
        tp=tp,
        tp_type=tp_type,
    )

    if _is_breakeven_exit(entry, exit_price, pnl):
        msg = f"BREAK_EVEN #BE @ {exit_price} | PnL: {pnl_label}"
        if entry is not None:
            msg = f"BREAK_EVEN #BE @ {exit_price} (entry {entry}) | PnL: {pnl_label}"
        telegram.notify_be_exit(symbol, msg)
        return

    if pnl > 0:
        if tp_type == "trailing":
            msg = (
                f"TRAILING_TP @ {exit_price} (activate {tp}, trail {TP_TRAILING_CALLBACK_RATE}%)"
                f" | PnL: {pnl_label}"
            )
            telegram.notify_tp_exit(symbol, msg, trailing=True)
        else:
            msg = f"TAKE_PROFIT #TP @ {exit_price} | PnL: {pnl_label}"
            if tp:
                msg = f"TAKE_PROFIT #TP @ {exit_price} (TP {tp}) | PnL: {pnl_label}"
            telegram.notify_tp_exit(symbol, msg)
        return

    if pnl < 0:
        msg = f"STOP_MARKET #SL @ {exit_price} | PnL: {pnl_label}"
        if sl:
            msg = f"STOP_MARKET #SL @ {exit_price} (SL {sl}) | PnL: {pnl_label}"
        telegram.notify_sl_exit(symbol, msg)
        return

    telegram.notify_position_closed(symbol, f"#CLOSED {direction} @ {exit_price} | PnL: {pnl_label}")


def get_execution_status() -> dict[str, Any]:
    with _status_lock:
        status = dict(_execution_status)
    status["execute_on_valid_entry"] = EXECUTE_ON_VALID_ENTRY
    status["auto_execute"] = EXECUTION_ENABLED and EXECUTE_ON_VALID_ENTRY
    if status.get("message") in (None, "", "Execution disabled", "Ready (dry)", "Ready (live)"):
        status["message"] = _execution_status_message()
    return status


def _set_status(**fields: Any) -> None:
    with _status_lock:
        _execution_status.update(fields)


def _append_orders_log(event: str, **fields: Any) -> None:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        record = {
            "ts": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            "event": event,
            **fields,
        }
        path = os.path.join(LOG_DIR, "orders.log")
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, default=str) + "\n")
    except OSError:
        logger.exception("Failed writing orders.log")


def _keys_configured() -> bool:
    return bool(os.getenv("BINANCE_API_KEY")) and bool(os.getenv("BINANCE_SECRET_KEY"))


def _position_mode_from_env() -> bool | None:
    raw = os.getenv("BINANCE_POSITION_MODE", "").strip().lower()
    if not raw:
        return None
    if raw in ("hedge", "dual", "hedged"):
        return True
    if raw in ("oneway", "one-way", "single"):
        return False
    logger.warning("Invalid BINANCE_POSITION_MODE=%r — detecting via API", raw)
    return None


def is_hedge_mode() -> bool:
    """True when Binance Futures account uses dual-side (hedge) position mode."""
    global _hedge_mode
    with _hedge_mode_lock:
        if _hedge_mode is not None:
            return _hedge_mode

        env_mode = _position_mode_from_env()
        if env_mode is not None:
            _hedge_mode = env_mode
            logger.info("Position mode from env: %s", "hedge" if _hedge_mode else "one-way")
            return _hedge_mode

        if not _keys_configured():
            _hedge_mode = False
            return False

        try:
            resp = _fapi_request("GET", "/fapi/v1/positionSide/dual", {})
            _hedge_mode = bool(resp.get("dualSidePosition"))
            logger.info("Position mode detected: %s", "hedge" if _hedge_mode else "one-way")
        except RuntimeError as exc:
            logger.warning("Could not detect position mode (%s); assuming one-way", exc)
            _hedge_mode = False
        return _hedge_mode


def _apply_position_params(params: dict[str, Any], direction: str, *, reduce_only: bool = False) -> dict[str, Any]:
    out = dict(params)
    if is_hedge_mode():
        out["positionSide"] = "LONG" if direction == "LONG" else "SHORT"
    elif reduce_only:
        out["reduceOnly"] = "true"
    return out


def _position_amt(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(0)


def _get_position_risk(symbol: str) -> list[dict[str, Any]]:
    if not _keys_configured():
        return []
    try:
        resp = _fapi_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol.upper()})
        return resp if isinstance(resp, list) else []
    except RuntimeError as exc:
        logger.warning("positionRisk failed for %s: %s", symbol, exc)
        return []


def has_open_position(symbol: str) -> bool:
    """True when Binance reports a non-zero futures position on symbol."""
    if not _keys_configured():
        return False
    symbol = symbol.upper()
    for row in _get_position_risk(symbol):
        if row.get("symbol") != symbol:
            continue
        if _position_amt(row.get("positionAmt", "0")).copy_abs() > Decimal("0"):
            return True
    return False


def _parse_position_row(row: dict[str, Any]) -> dict[str, Any] | None:
    amt = _position_amt(row.get("positionAmt", "0"))
    if amt.copy_abs() <= Decimal("0"):
        return None

    pos_side = (row.get("positionSide") or "BOTH").upper()
    if is_hedge_mode() and pos_side in ("LONG", "SHORT"):
        direction = pos_side
    elif amt > 0:
        direction = "LONG"
    else:
        direction = "SHORT"

    qty_decimal = amt.copy_abs()
    qty = format(qty_decimal.normalize(), "f")
    entry = float(row.get("entryPrice", 0) or 0)
    mark = float(row.get("markPrice", 0) or 0)
    upnl = float(row.get("unRealizedProfit", 0) or 0)
    initial_margin = float(row.get("initialMargin", 0) or row.get("isolatedMargin", 0) or 0)

    if entry > 0:
        volume_usdt = float(qty_decimal) * entry
    elif mark > 0:
        volume_usdt = float(qty_decimal) * mark
    else:
        volume_usdt = None

    return {
        "open": True,
        "pending": False,
        "direction": direction,
        "qty": qty,
        "entry": entry if entry > 0 else None,
        "volume_usdt": round(volume_usdt, 2) if volume_usdt is not None else None,
        "unrealized_pnl": round(upnl, 2),
        "initial_margin": round(initial_margin, 2) if initial_margin > 0 else None,
        "mark_price": mark if mark > 0 else None,
    }


def _attach_unrealized_pnl_pct(snapshot: dict[str, Any]) -> None:
    pnl = snapshot.get("unrealized_pnl")
    if pnl is None:
        snapshot["unrealized_pnl_pct"] = None
        return
    margin = snapshot.get("initial_margin")
    volume = snapshot.get("volume_usdt")
    if margin and margin > 0:
        snapshot["unrealized_pnl_pct"] = round(float(pnl) / float(margin) * 100, 2)
    elif volume and volume > 0:
        snapshot["unrealized_pnl_pct"] = round(float(pnl) / float(volume) * 100, 2)
    else:
        entry = snapshot.get("entry")
        mark = snapshot.get("mark_price")
        direction = snapshot.get("direction")
        if entry and mark and entry > 0 and direction in ("LONG", "SHORT"):
            move = (float(mark) - float(entry)) / float(entry) * 100
            if direction == "SHORT":
                move = -move
            snapshot["unrealized_pnl_pct"] = round(move, 2)
        else:
            snapshot["unrealized_pnl_pct"] = None


def _pending_entry_snapshot(symbol: str) -> dict[str, Any] | None:
    for order in _get_open_orders(symbol):
        if not _is_entry_limit_order(order):
            continue
        side = order.get("side")
        direction = "LONG" if side == "BUY" else "SHORT"
        price = float(order.get("price", 0) or 0)
        qty_raw = order.get("origQty") or order.get("quantity") or "0"
        qty_decimal = _position_amt(qty_raw)
        if qty_decimal <= Decimal("0") or price <= 0:
            continue
        volume_usdt = float(qty_decimal) * price
        return {
            "open": False,
            "pending": True,
            "direction": direction,
            "qty": format(qty_decimal.normalize(), "f"),
            "entry": price,
            "volume_usdt": round(volume_usdt, 2),
            "unrealized_pnl": None,
            "mark_price": None,
            "order_id": order.get("orderId"),
        }
    return None


def _fetch_exchange_exposure(symbol: str) -> dict[str, Any]:
    empty: dict[str, Any] = {
        "open": False,
        "pending": False,
        "direction": None,
        "qty": None,
        "entry": None,
        "volume_usdt": None,
        "unrealized_pnl": None,
        "mark_price": None,
    }
    if not _keys_configured():
        empty["source"] = "no_keys"
        return empty

    symbol = symbol.upper()
    legs: list[dict[str, Any]] = []
    for row in _get_position_risk(symbol):
        if row.get("symbol") != symbol:
            continue
        parsed = _parse_position_row(row)
        if parsed:
            legs.append(parsed)

    if len(legs) == 1:
        snapshot = dict(legs[0])
        snapshot["source"] = "binance"
        _attach_unrealized_pnl_pct(snapshot)
        return snapshot

    if len(legs) > 1:
        total_vol = sum(leg.get("volume_usdt") or 0 for leg in legs)
        total_pnl = sum(leg.get("unrealized_pnl") or 0 for leg in legs)
        total_margin = sum(leg.get("initial_margin") or 0 for leg in legs)
        directions = "+".join(leg["direction"] for leg in legs)
        snapshot = {
            "open": True,
            "pending": False,
            "direction": directions,
            "qty": ", ".join(f"{leg['direction']} {leg['qty']}" for leg in legs),
            "entry": legs[0].get("entry"),
            "volume_usdt": round(total_vol, 2) if total_vol else None,
            "unrealized_pnl": round(total_pnl, 2),
            "initial_margin": round(total_margin, 2) if total_margin else None,
            "mark_price": legs[0].get("mark_price"),
            "legs": legs,
            "source": "binance",
        }
        _attach_unrealized_pnl_pct(snapshot)
        return snapshot

    pending = _pending_entry_snapshot(symbol)
    if pending:
        pending["source"] = "binance"
        return pending

    empty["source"] = "binance"
    return empty


def get_exchange_exposure(symbol: str) -> dict[str, Any]:
    """Open position or pending entry LIMIT on Binance Futures (cached for dashboard)."""
    symbol = symbol.upper()
    if not _keys_configured():
        return {
            "open": False,
            "pending": False,
            "direction": None,
            "qty": None,
            "entry": None,
            "volume_usdt": None,
            "unrealized_pnl": None,
            "mark_price": None,
            "source": "no_keys",
        }

    now = time.monotonic()
    with _position_snapshot_lock:
        cached = _position_snapshot_cache.get(symbol)
        if cached and now - cached[0] < EXECUTION_POSITION_CACHE_SEC:
            return dict(cached[1])

    snapshot = _fetch_exchange_exposure(symbol)
    _maybe_notify_position_exit(symbol, snapshot)
    if not EXECUTION_ENABLED:
        snapshot = dict(snapshot)
        if snapshot.get("source") == "binance":
            snapshot["source"] = "monitoring"
    snapshot = dict(snapshot)
    primary_dir = _primary_position_direction(snapshot.get("direction"))
    if snapshot.get("open") and primary_dir:
        snapshot.update(get_position_protection(symbol, primary_dir))
    else:
        snapshot["has_sl"] = False
        snapshot["has_tp"] = False
        snapshot["sl_count"] = 0
        snapshot["tp_count"] = 0
        snapshot["tp_kind"] = None
        snapshot["trailing_active"] = False
        snapshot["trailing_pending"] = False
        snapshot["protection_ok"] = False
    with _position_snapshot_lock:
        _position_snapshot_cache[symbol] = (now, snapshot)
    return dict(snapshot)


def _get_open_orders(symbol: str) -> list[dict[str, Any]]:
    if not _keys_configured():
        return []
    try:
        resp = _fapi_request("GET", "/fapi/v1/openOrders", {"symbol": symbol.upper()})
        return resp if isinstance(resp, list) else []
    except RuntimeError as exc:
        logger.warning("openOrders failed for %s: %s", symbol, exc)
        return []


def _get_open_algo_orders(symbol: str) -> list[dict[str, Any]]:
    if not _keys_configured():
        return []
    try:
        resp = _fapi_request(
            "GET",
            "/fapi/v1/openAlgoOrders",
            {"symbol": symbol.upper(), "algoType": "CONDITIONAL"},
        )
        return resp if isinstance(resp, list) else []
    except RuntimeError as exc:
        logger.warning("openAlgoOrders failed for %s: %s", symbol, exc)
        return []


def _algo_order_role(order: dict[str, Any], position_direction: str) -> str | None:
    """Classify an open algo order as stop-loss or take-profit for a position side."""
    position_direction = position_direction.upper()
    order_type = str(order.get("orderType") or order.get("type") or "").upper()
    side = str(order.get("side") or "").upper()
    status = str(order.get("algoStatus") or order.get("status") or "").upper()
    if status in ("CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FINISHED"):
        return None

    if position_direction == "LONG":
        if side == "SELL" and order_type == "STOP_MARKET":
            return "sl"
        if side == "SELL" and order_type in (
            "TAKE_PROFIT_MARKET",
            "TRAILING_STOP_MARKET",
            "TAKE_PROFIT",
        ):
            return "tp"
    elif position_direction == "SHORT":
        if side == "BUY" and order_type == "STOP_MARKET":
            return "sl"
        if side == "BUY" and order_type in (
            "TAKE_PROFIT_MARKET",
            "TRAILING_STOP_MARKET",
            "TAKE_PROFIT",
        ):
            return "tp"
    return None


def _primary_position_direction(direction: str | None) -> str | None:
    if not direction:
        return None
    text = str(direction).upper()
    if "+" in text:
        return text.split("+", 1)[0].strip() or None
    return text


def _trailing_tp_is_active(order: dict[str, Any]) -> bool:
    """True when Binance reports the trailing stop as triggered / working."""
    try:
        trigger_time = int(order.get("triggerTime") or 0)
    except (TypeError, ValueError):
        trigger_time = 0
    status = str(order.get("algoStatus") or order.get("status") or "").upper()
    if trigger_time > 0:
        return True
    return status in ("TRIGGERED", "WORKING", "FILLED")


def get_position_protection(symbol: str, direction: str | None) -> dict[str, Any]:
    """Whether open conditional orders cover SL and TP for the current position side."""
    primary = _primary_position_direction(direction)
    result: dict[str, Any] = {
        "has_sl": False,
        "has_tp": False,
        "sl_count": 0,
        "tp_count": 0,
        "tp_kind": None,
        "trailing_active": False,
        "trailing_pending": False,
        "protection_ok": False,
    }
    if not primary or not _keys_configured():
        return result

    want_ps = primary if is_hedge_mode() else None
    for order in _get_open_algo_orders(symbol):
        if want_ps:
            pos_side = str(order.get("positionSide") or "").upper()
            if pos_side and pos_side not in (want_ps, "BOTH"):
                continue
        role = _algo_order_role(order, primary)
        if role == "sl":
            result["has_sl"] = True
            result["sl_count"] += 1
        elif role == "tp":
            result["has_tp"] = True
            result["tp_count"] += 1
            order_type = str(order.get("orderType") or order.get("type") or "").upper()
            if order_type == "TRAILING_STOP_MARKET":
                result["tp_kind"] = "trailing"
                if _trailing_tp_is_active(order):
                    result["trailing_active"] = True
                else:
                    result["trailing_pending"] = True
            elif result["tp_kind"] != "trailing":
                result["tp_kind"] = "fixed"

    result["protection_ok"] = bool(
        result["has_sl"]
        and (
            (result["tp_kind"] == "trailing" and result["trailing_active"])
            or (result["has_tp"] and result["tp_kind"] != "trailing")
        )
    )
    return result


def _invalidate_position_cache(symbol: str) -> None:
    symbol = symbol.upper()
    with _position_snapshot_lock:
        _position_snapshot_cache.pop(symbol, None)


def _position_qty_string(symbol: str, direction: str) -> str | None:
    direction = direction.upper()
    for row in _get_position_risk(symbol):
        if row.get("symbol") != symbol.upper():
            continue
        parsed = _parse_position_row(row)
        if not parsed or parsed.get("direction") != direction:
            continue
        qty_raw = parsed.get("qty")
        if qty_raw is None:
            continue
        try:
            return round_qty(symbol, float(qty_raw))
        except (TypeError, ValueError):
            continue
    return None


def _cancel_limit_order(symbol: str, order_id: int) -> dict[str, Any]:
    return _fapi_request(
        "DELETE",
        "/fapi/v1/order",
        {"symbol": symbol.upper(), "orderId": order_id},
    )


def cancel_stale_entry_limits(symbol: str) -> int:
    """Cancel unfilled entry LIMIT orders older than ENTRY_LIMIT_TTL_SEC (0 = disabled)."""
    if ENTRY_LIMIT_TTL_SEC <= 0 or not _keys_configured():
        return 0

    symbol = symbol.upper()
    now_ms = int(time.time() * 1000)
    cancelled = 0
    for order in _get_open_orders(symbol):
        if not _is_entry_limit_order(order):
            continue
        order_id = order.get("orderId")
        if order_id is None:
            continue
        updated_ms = int(order.get("updateTime") or order.get("time") or 0)
        if updated_ms <= 0 or now_ms - updated_ms < ENTRY_LIMIT_TTL_SEC * 1000:
            continue
        try:
            _cancel_limit_order(symbol, int(order_id))
            age_sec = int((now_ms - updated_ms) / 1000)
            _append_orders_log(
                "entry_limit_expired",
                symbol=symbol,
                orderId=order_id,
                age_sec=age_sec,
                ttl_sec=ENTRY_LIMIT_TTL_SEC,
            )
            logger.info(
                "%s: cancelled stale entry limit %s (age %ss)",
                symbol,
                order_id,
                age_sec,
            )
            cancelled += 1
        except RuntimeError as exc:
            logger.warning("%s: failed to cancel stale limit %s: %s", symbol, order_id, exc)
            _append_orders_log(
                "entry_limit_cancel_failed",
                symbol=symbol,
                orderId=order_id,
                error=str(exc),
            )
    if cancelled:
        _invalidate_position_cache(symbol)
    return cancelled


def _place_protection_legs(
    symbol: str,
    direction: str,
    sl: float,
    tp: float,
    qty: str,
    *,
    need_sl: bool,
    need_tp: bool,
) -> dict[str, bool]:
    """Place missing SL/TP on an open position (no entry wait)."""
    result = {"sl": not need_sl, "tp": not need_tp}
    if not REST_PLACE_SL_TP:
        return result

    sl_price = round_price(symbol, sl)
    tp_price = round_price(symbol, tp)

    if need_sl:
        try:
            sl_resp = _place_stop_loss(symbol, direction, sl_price, qty)
            _append_orders_log("sl_reconciled", symbol=symbol, response=sl_resp)
            result["sl"] = True
        except RuntimeError as exc:
            logger.error("SL reconcile failed for %s: %s", symbol, exc)
            _append_orders_log("sl_reconcile_failed", symbol=symbol, error=str(exc))
            telegram.notify_sl_tp_failed(symbol, direction, "SL (reconcile)", str(exc))

    if need_tp:
        try:
            placed, skipped = _place_tp_for_position(
                symbol,
                direction,
                tp_price,
                qty,
                log_suffix="_reconciled",
            )
            if placed:
                result["tp"] = True
            elif not skipped:
                result["tp"] = False
        except RuntimeError as exc:
            logger.error("TP reconcile failed for %s: %s", symbol, exc)
            _append_orders_log("tp_reconcile_failed", symbol=symbol, error=str(exc))
            telegram.notify_sl_tp_failed(symbol, direction, "TP (reconcile)", str(exc))

    return result


def reconcile_position_protection(
    symbol: str,
    *,
    direction: str | None = None,
    sl: float | None = None,
    tp: float | None = None,
) -> dict[str, Any]:
    """Ensure open position has SL/TP algo orders; place any missing legs."""
    symbol = symbol.upper()
    summary: dict[str, Any] = {
        "symbol": symbol,
        "reconciled": False,
        "placed_sl": False,
        "placed_tp": False,
    }
    if not EXECUTION_ENABLED or EXECUTION_MODE != "live" or not _keys_configured():
        return summary
    if not PROTECTION_RECONCILE_ENABLED or not REST_PLACE_SL_TP:
        return summary
    if not has_open_position(symbol):
        return summary

    snapshot = _fetch_exchange_exposure(symbol)
    if not snapshot.get("open"):
        return summary

    pos_dir = _primary_position_direction(snapshot.get("direction")) or _primary_position_direction(
        direction
    )
    ctx = _get_trade_context(symbol)
    if not pos_dir:
        pos_dir = _primary_position_direction(ctx.get("direction")) or _primary_position_direction(direction)
    if not pos_dir:
        logger.warning("%s: cannot reconcile protection without position direction", symbol)
        return summary

    sl_val = sl
    if sl_val is None or sl_val <= 0:
        raw_sl = ctx.get("sl")
        try:
            sl_val = float(raw_sl) if raw_sl not in (None, "") else None
        except (TypeError, ValueError):
            sl_val = None

    tp_val = tp
    if tp_val is None or tp_val <= 0:
        raw_tp = ctx.get("tp")
        try:
            tp_val = float(raw_tp) if raw_tp not in (None, "") else None
        except (TypeError, ValueError):
            tp_val = None

    protection = get_position_protection(symbol, pos_dir)
    summary.update(protection)

    qty = _position_qty_string(symbol, pos_dir)
    if not qty:
        logger.warning("%s: cannot reconcile protection — no position qty", symbol)
        return summary

    if (protection["has_sl"] or protection["has_tp"]) and not _protection_qty_covers_position(
        symbol, pos_dir, qty
    ):
        _cancel_symbol_protection_orders(symbol, pos_dir)
        _invalidate_position_cache(symbol)
        protection = get_position_protection(symbol, pos_dir)
        summary.update(protection)

    need_sl = not protection["has_sl"]
    need_tp = not protection["has_tp"]
    if not need_sl and not need_tp:
        return summary

    if need_sl and (sl_val is None or sl_val <= 0):
        logger.warning("%s: missing SL on exchange but no sl price in context/plan", symbol)
        need_sl = False
    if need_tp and (tp_val is None or tp_val <= 0):
        logger.warning("%s: missing TP on exchange but no tp price in context/plan", symbol)
        need_tp = False
    if not need_sl and not need_tp:
        return summary

    placed = _place_protection_legs(
        symbol,
        pos_dir,
        float(sl_val or 0),
        float(tp_val or 0),
        qty,
        need_sl=need_sl,
        need_tp=need_tp,
    )
    summary["reconciled"] = placed["sl"] or placed["tp"]
    summary["placed_sl"] = need_sl and placed["sl"]
    summary["placed_tp"] = need_tp and placed["tp"]
    if summary["reconciled"]:
        _invalidate_position_cache(symbol)
        parts = []
        if summary["placed_sl"]:
            parts.append("SL")
        if summary["placed_tp"]:
            parts.append("TP")
        _set_status(
            message=f"Protection reconciled · {' + '.join(parts)} · {symbol}",
            last_event="protection_reconciled",
        )
    return summary


def run_execution_maintenance(
    symbol: str,
    *,
    direction: str | None = None,
    sl: float | None = None,
    tp: float | None = None,
) -> None:
    """Periodic: cancel stale entry limits; repair missing SL/TP on open positions."""
    if not EXECUTION_ENABLED or not _keys_configured() or EXECUTION_MODE != "live":
        return

    symbol = symbol.upper()
    now = time.monotonic()
    with _maintenance_lock:
        last = _last_maintenance.get(symbol, 0.0)
        if now - last < EXECUTION_MAINTENANCE_SEC:
            return
        _last_maintenance[symbol] = now

    cancel_stale_entry_limits(symbol)
    _sync_dca_leg_count(symbol)
    reconcile_position_protection(symbol, direction=direction, sl=sl, tp=tp)


def _is_entry_limit_order(order: dict[str, Any]) -> bool:
    if order.get("type") not in ("LIMIT", "LIMIT_MAKER"):
        return False
    if order.get("reduceOnly") in (True, "true", "True"):
        return False
    return True


def has_open_limit_same_side(symbol: str, direction: str) -> bool:
    direction = direction.upper()
    side = "BUY" if direction == "LONG" else "SELL"
    want_ps = direction if is_hedge_mode() else None
    for order in _get_open_orders(symbol):
        if not _is_entry_limit_order(order):
            continue
        if order.get("side") != side:
            continue
        if want_ps:
            pos_side = (order.get("positionSide") or "").upper()
            if pos_side and pos_side not in (want_ps, "BOTH"):
                continue
        return True
    return False


def has_conflicting_entry_limits(symbol: str) -> bool:
    return has_open_limit_same_side(symbol, "LONG") and has_open_limit_same_side(symbol, "SHORT")


def has_limit_at_price(symbol: str, direction: str, entry: float) -> bool:
    direction = direction.upper()
    side = "BUY" if direction == "LONG" else "SELL"
    want_ps = direction if is_hedge_mode() else None
    entry_str = round_price(symbol, entry)
    for order in _get_open_orders(symbol):
        if not _is_entry_limit_order(order):
            continue
        if order.get("side") != side:
            continue
        if want_ps:
            pos_side = (order.get("positionSide") or "").upper()
            if pos_side and pos_side not in (want_ps, "BOTH"):
                continue
        price_str = round_price(symbol, float(order.get("price", 0)))
        if price_str == entry_str:
            return True
    return False


def _get_all_position_risk() -> list[dict[str, Any]]:
    if not _keys_configured():
        return []
    try:
        resp = _fapi_request("GET", "/fapi/v2/positionRisk", {})
        return resp if isinstance(resp, list) else []
    except RuntimeError as exc:
        logger.warning("positionRisk (all symbols) failed: %s", exc)
        return []


def _get_all_open_orders() -> list[dict[str, Any]]:
    if not _keys_configured():
        return []
    try:
        resp = _fapi_request("GET", "/fapi/v1/openOrders", {})
        return resp if isinstance(resp, list) else []
    except RuntimeError as exc:
        logger.warning("openOrders (all symbols) failed: %s", exc)
        return []


def _pending_entry_notional(order: dict[str, Any]) -> float:
    if not _is_entry_limit_order(order):
        return 0.0
    try:
        price = float(order.get("price", 0) or 0)
        qty = float(order.get("origQty") or order.get("quantity") or 0)
    except (TypeError, ValueError):
        return 0.0
    if price <= 0 or qty <= 0:
        return 0.0
    return price * qty


def get_fleet_side_exposure(*, force: bool = False) -> dict[str, Any]:
    """Aggregate LONG/SHORT notional (open positions + pending entry limits)."""
    global _fleet_exposure_cache

    empty = {
        "enabled": FLEET_SIDE_BALANCE_MAX_PCT > 0,
        "max_pct": FLEET_SIDE_BALANCE_MAX_PCT,
        "long_positions_usdt": 0.0,
        "short_positions_usdt": 0.0,
        "long_pending_usdt": 0.0,
        "short_pending_usdt": 0.0,
        "long_total_usdt": 0.0,
        "short_total_usdt": 0.0,
        "imbalance_pct": 0.0,
    }
    if not _keys_configured():
        return empty

    now = time.monotonic()
    with _fleet_exposure_lock:
        if (
            not force
            and _fleet_exposure_cache is not None
            and now - _fleet_exposure_cache[0] < FLEET_EXPOSURE_CACHE_SEC
        ):
            return dict(_fleet_exposure_cache[1])

    long_pos = short_pos = long_pending = short_pending = 0.0

    for row in _get_all_position_risk():
        parsed = _parse_position_row(row)
        if not parsed:
            continue
        vol = float(parsed.get("volume_usdt") or 0)
        if parsed.get("direction") == "LONG":
            long_pos += vol
        else:
            short_pos += vol

    for order in _get_all_open_orders():
        notional = _pending_entry_notional(order)
        if notional <= 0:
            continue
        if order.get("side") == "BUY":
            long_pending += notional
        else:
            short_pending += notional

    long_total = long_pos + long_pending
    short_total = short_pos + short_pending
    smaller = min(long_total, short_total)
    larger = max(long_total, short_total)
    if smaller > 0:
        imbalance_pct = round((larger - smaller) / smaller * 100, 1)
    elif larger > 0:
        imbalance_pct = 100.0
    else:
        imbalance_pct = 0.0

    result = {
        "enabled": FLEET_SIDE_BALANCE_MAX_PCT > 0,
        "max_pct": FLEET_SIDE_BALANCE_MAX_PCT,
        "long_positions_usdt": round(long_pos, 2),
        "short_positions_usdt": round(short_pos, 2),
        "long_pending_usdt": round(long_pending, 2),
        "short_pending_usdt": round(short_pending, 2),
        "long_total_usdt": round(long_total, 2),
        "short_total_usdt": round(short_total, 2),
        "imbalance_pct": imbalance_pct,
    }
    with _fleet_exposure_lock:
        _fleet_exposure_cache = (now, result)
    return dict(result)


def fleet_side_balance_blocks(
    direction: str,
    additional_usdt: float,
) -> tuple[bool, str]:
    """
    Block when the larger side would exceed the smaller by more than FLEET_SIDE_BALANCE_MAX_PCT.
    Example: max 20% → LONG total may be at most SHORT_total * 1.20.
    If the opposite side is zero, new entries on the empty side are allowed.
    """
    if FLEET_SIDE_BALANCE_MAX_PCT <= 0 or not _keys_configured() or not EXECUTION_ENABLED:
        return False, ""

    direction = direction.upper()
    try:
        add_usdt = max(float(additional_usdt), 0.0)
    except (TypeError, ValueError):
        add_usdt = 0.0

    exposure = get_fleet_side_exposure()
    long_total = float(exposure["long_total_usdt"])
    short_total = float(exposure["short_total_usdt"])
    max_mult = 1.0 + FLEET_SIDE_BALANCE_MAX_PCT / 100.0

    if direction == "LONG":
        if short_total <= 0:
            return False, ""
        if long_total + add_usdt > short_total * max_mult:
            return True, "fleet_long_imbalance"
    elif direction == "SHORT":
        if long_total <= 0:
            return False, ""
        if short_total + add_usdt > long_total * max_mult:
            return True, "fleet_short_imbalance"
    return False, ""


def estimate_order_notional_usdt(size_pct: float = 100.0) -> float:
    return _calculate_notional_usdt() * (max(float(size_pct), 0.0) / 100.0)


def can_place_dca_add(
    symbol: str,
    direction: str,
    entry: float,
    *,
    size_pct: float,
    max_legs: int,
) -> tuple[bool, str]:
    """Add one DCA leg on a new valid entry while position is open (same side)."""
    symbol = symbol.upper()
    direction = direction.upper()
    if entry <= 0:
        return False, "invalid_entry"

    placed = dca_legs_placed(symbol)
    if placed >= max_legs:
        return False, "dca_max_legs"

    pos_dir = get_open_position_direction(symbol)
    if not pos_dir:
        return False, "dca_no_position"
    if pos_dir != direction:
        return False, "dca_direction_mismatch"

    blocked, balance_reason = fleet_side_balance_blocks(
        direction,
        estimate_order_notional_usdt(size_pct),
    )
    if blocked:
        return False, balance_reason

    if not EXECUTION_BLOCK_IF_OPEN or not _keys_configured():
        return True, ""

    if has_conflicting_entry_limits(symbol):
        return False, "conflicting_limits"
    opposite = "SHORT" if direction == "LONG" else "LONG"
    if has_open_limit_same_side(symbol, opposite):
        return False, "pending_opposite_limit"
    if has_limit_at_price(symbol, direction, entry):
        return False, "duplicate_limit_price"
    return True, ""


def can_place_dca_bundle(
    symbol: str,
    direction: str,
    legs: list[dict[str, Any]],
) -> tuple[bool, str]:
    """Allow multiple entry limits at different prices (DCA stack) before any fill."""
    symbol = symbol.upper()
    direction = direction.upper()
    if not legs:
        return False, "invalid_dca_leg"

    total_pct = 0.0
    for leg in legs:
        try:
            total_pct += float(leg.get("size_pct", 0))
        except (TypeError, ValueError):
            return False, "invalid_dca_leg"

    blocked, balance_reason = fleet_side_balance_blocks(
        direction,
        estimate_order_notional_usdt(total_pct),
    )
    if blocked:
        return False, balance_reason

    if not EXECUTION_BLOCK_IF_OPEN or not _keys_configured():
        return True, ""

    if has_open_position(symbol):
        return False, "open_position"
    if has_conflicting_entry_limits(symbol):
        return False, "conflicting_limits"
    opposite = "SHORT" if direction == "LONG" else "LONG"
    if has_open_limit_same_side(symbol, opposite):
        return False, "pending_opposite_limit"

    seen_prices: set[str] = set()
    for leg in legs:
        try:
            price = float(leg.get("price", 0))
        except (TypeError, ValueError):
            return False, "invalid_dca_leg"
        if price <= 0:
            return False, "invalid_dca_leg"
        price_key = round_price(symbol, price)
        if price_key in seen_prices:
            return False, "duplicate_dca_price"
        seen_prices.add(price_key)
        if has_limit_at_price(symbol, direction, price):
            return False, "duplicate_limit_price"
    return True, ""


def can_place_new_order(
    symbol: str,
    direction: str,
    entry: float,
    *,
    size_pct: float = 100.0,
) -> tuple[bool, str]:
    """Mirror trade-binance-websocket-order-blocks can_place_new_order."""
    symbol = symbol.upper()
    direction = direction.upper()

    blocked, balance_reason = fleet_side_balance_blocks(
        direction,
        estimate_order_notional_usdt(size_pct),
    )
    if blocked:
        return False, balance_reason

    if not EXECUTION_BLOCK_IF_OPEN or not _keys_configured():
        return True, ""

    if has_open_position(symbol):
        return False, "open_position"
    if has_conflicting_entry_limits(symbol):
        return False, "conflicting_limits"
    opposite = "SHORT" if direction == "LONG" else "LONG"
    if has_open_limit_same_side(symbol, opposite):
        return False, "pending_opposite_limit"
    if has_open_limit_same_side(symbol, direction):
        return False, "pending_entry_limit"
    if has_limit_at_price(symbol, direction, entry):
        return False, "duplicate_limit_price"
    return True, ""


def _log_execution_decision(
    symbol: str,
    event_type: str,
    *,
    block_reason: str | None = None,
    outcome: str = "skipped",
    market_snapshot: dict[str, Any] | None = None,
) -> None:
    db_store.log_decision_event(
        symbol,
        event_type,
        outcome=outcome,
        block_reason=block_reason,
        config_snapshot=symbol_config.get_config_snapshot(),
        market_snapshot=market_snapshot,
        config_version=symbol_config.config_version(),
    )


def _log_skip_order(symbol: str, signal: str, reason: str, *, entry: float | None = None) -> None:
    event = {
        "open_position": "skip_open_position",
        "pending_entry_limit": "skip_pending_limit",
        "pending_opposite_limit": "skip_pending_limit",
        "conflicting_limits": "skip_conflicting_limits",
        "duplicate_limit_price": "skip_duplicate_limit",
        "fleet_long_imbalance": "skip_fleet_long_imbalance",
        "fleet_short_imbalance": "skip_fleet_short_imbalance",
        "invalid_dca_leg": "skip_invalid_dca",
        "duplicate_dca_price": "skip_duplicate_dca",
        "dca_max_legs": "skip_dca_max",
        "dca_no_position": "skip_dca_no_position",
        "dca_direction_mismatch": "skip_dca_mismatch",
    }.get(reason, "skip_order_guard")
    fields: dict[str, Any] = {"symbol": symbol.upper(), "signal": signal, "reason": reason}
    if entry is not None:
        fields["entry"] = entry
    _append_orders_log(event, **fields)
    logger.warning("%s: blocked new %s order (%s)", symbol.upper(), signal, reason)
    _log_execution_decision(
        symbol,
        event,
        block_reason=reason,
        market_snapshot={"signal": signal, "entry": entry},
    )


def _sign_query(params: dict[str, Any]) -> str:
    secret = os.getenv("BINANCE_SECRET_KEY", "")
    query = urllib.parse.urlencode(params)
    signature = hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()
    return f"{query}&signature={signature}"


def _fapi_public_get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    query = urllib.parse.urlencode(params or {})
    url = f"{FAPI_BASE}{path}"
    if query:
        url = f"{url}?{query}"
    request = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = response.read().decode()
            return json.loads(payload) if payload else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()
        raise RuntimeError(detail or str(exc)) from exc


def _fapi_request(method: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000
    body = _sign_query(params)
    url = f"{FAPI_BASE}{path}"
    headers = {"X-MBX-APIKEY": os.getenv("BINANCE_API_KEY", "")}
    if method == "GET":
        url = f"{url}?{body}"
        data = None
    else:
        data = body.encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = response.read().decode()
            return json.loads(payload) if payload else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode()
        raise RuntimeError(detail or str(exc)) from exc


def _load_symbol_filters(symbol: str) -> dict[str, Decimal]:
    symbol = symbol.upper()
    with _filters_lock:
        if symbol in _symbol_filters:
            return _symbol_filters[symbol]

    info = _fapi_public_get("/fapi/v1/exchangeInfo", {})
    filters: dict[str, Decimal] = {
        "tick_size": Decimal("0.01"),
        "step_size": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "min_notional": Decimal("5"),
    }
    for item in info.get("symbols", []):
        if item.get("symbol") != symbol:
            continue
        for filt in item.get("filters", []):
            if filt.get("filterType") == "PRICE_FILTER":
                filters["tick_size"] = Decimal(str(filt.get("tickSize", "0.01")))
            elif filt.get("filterType") == "LOT_SIZE":
                filters["step_size"] = Decimal(str(filt.get("stepSize", "0.001")))
                filters["min_qty"] = Decimal(str(filt.get("minQty", "0.001")))
            elif filt.get("filterType") == "MIN_NOTIONAL":
                filters["min_notional"] = Decimal(str(filt.get("notional", "5")))
        break

    with _filters_lock:
        _symbol_filters[symbol] = filters
    return filters


def round_price(symbol: str, value: float) -> str:
    filt = _load_symbol_filters(symbol)
    tick = filt["tick_size"]
    quantized = Decimal(str(value)).quantize(tick, rounding=ROUND_DOWN)
    return format(quantized, "f")


def _get_mark_price(symbol: str) -> float | None:
    snapshot = _fetch_exchange_exposure(symbol.upper())
    mark = snapshot.get("mark_price")
    if mark is not None and float(mark) > 0:
        return float(mark)
    return None


def _tp_would_trigger_immediately(direction: str, tp: float, mark: float) -> bool:
    """True when TP trigger/activation would fire at current mark (Binance -2021)."""
    direction = direction.upper()
    if direction == "LONG":
        return mark >= tp
    return mark <= tp


def _is_immediate_trigger_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "-2021" in str(exc) or "immediately trigger" in text


def _place_tp_for_position(
    symbol: str,
    direction: str,
    tp_price: str,
    qty: str,
    *,
    log_suffix: str = "",
) -> tuple[bool, bool]:
    """Place TP if valid vs mark. Returns (placed, skipped_immediate)."""
    symbol = symbol.upper()
    direction = direction.upper()
    try:
        tp_val = float(tp_price)
    except (TypeError, ValueError):
        tp_val = 0.0

    mark = _get_mark_price(symbol)
    if mark is not None and tp_val > 0 and _tp_would_trigger_immediately(direction, tp_val, mark):
        _append_orders_log(
            "tp_skip_immediate",
            symbol=symbol,
            direction=direction,
            tp=tp_price,
            mark=mark,
            context=log_suffix or "protection",
        )
        logger.info(
            "%s: skip TP%s — mark %.8g already at/through target %.8g (%s)",
            symbol,
            f" ({log_suffix})" if log_suffix else "",
            mark,
            tp_val,
            direction,
        )
        return False, True

    try:
        if TP_ORDER_TYPE == "trailing":
            tp_resp = _place_trailing_tp(symbol, direction, tp_price, qty)
            event = f"tp_trailing{log_suffix}"
        else:
            tp_resp = _place_take_profit_fixed(symbol, direction, tp_price, qty)
            event = f"tp_fixed{log_suffix}"
        _append_orders_log(event, symbol=symbol, response=tp_resp, tp=tp_price, mark=mark)
        return True, False
    except RuntimeError as exc:
        if _is_immediate_trigger_error(exc):
            _append_orders_log(
                "tp_skip_immediate",
                symbol=symbol,
                direction=direction,
                tp=tp_price,
                mark=mark,
                error=str(exc),
                context=log_suffix or "protection",
            )
            logger.info("%s: TP skipped (immediate trigger): %s", symbol, exc)
            return False, True
        raise


def round_qty(symbol: str, value: float) -> str:
    filt = _load_symbol_filters(symbol)
    step = filt["step_size"]
    quantized = Decimal(str(value)).quantize(step, rounding=ROUND_DOWN)
    return format(quantized, "f")


def _calculate_notional_usdt() -> float:
    if POSITION_WALLET_PCT > 0 and _keys_configured():
        try:
            account = _fapi_request("GET", "/fapi/v2/account", {})
            wallet = float(account.get("totalWalletBalance", 0) or 0)
            if wallet > 0:
                return wallet * POSITION_WALLET_PCT / 100.0
        except RuntimeError:
            logger.warning("Could not read wallet balance; using POSITION_SIZE_USDT")
    return POSITION_SIZE_USDT


def _calculate_quantity(symbol: str, entry_price: float, size_pct: float = 100.0) -> str:
    notional = _calculate_notional_usdt() * (size_pct / 100.0)
    if entry_price <= 0:
        raise ValueError("Invalid entry price for quantity")
    qty = notional / entry_price
    filt = _load_symbol_filters(symbol)
    qty_str = round_qty(symbol, qty)
    if Decimal(qty_str) < filt["min_qty"]:
        qty_str = format(filt["min_qty"], "f")
    if Decimal(qty_str) * Decimal(str(entry_price)) < filt["min_notional"]:
        raise ValueError(f"Order notional below minimum for {symbol}")
    return qty_str


def _get_symbol_max_leverage(symbol: str) -> int | None:
    symbol = symbol.upper()
    with _leverage_lock:
        if symbol in _max_leverage_cache:
            return _max_leverage_cache[symbol]

    if not _keys_configured():
        return None

    try:
        resp = _fapi_request("GET", "/fapi/v1/leverageBracket", {"symbol": symbol})
    except RuntimeError:
        logger.warning("Could not fetch leverage brackets for %s", symbol)
        return None

    brackets: list[dict[str, Any]] = []
    if isinstance(resp, list):
        for item in resp:
            if item.get("symbol") == symbol:
                brackets = item.get("brackets") or []
                break
    elif isinstance(resp, dict):
        brackets = resp.get("brackets") or []

    max_lev = 0
    for bracket in brackets:
        try:
            max_lev = max(max_lev, int(bracket.get("initialLeverage", 0)))
        except (TypeError, ValueError):
            continue

    if max_lev <= 0:
        return None

    with _leverage_lock:
        _max_leverage_cache[symbol] = max_lev
    return max_lev


def resolve_leverage_for_symbol(symbol: str) -> int:
    if LEVERAGE_MODE == "max":
        max_lev = _get_symbol_max_leverage(symbol)
        if max_lev:
            return max_lev
    return LEVERAGE


def _set_leverage(symbol: str) -> int:
    lev = resolve_leverage_for_symbol(symbol)
    if EXECUTION_MODE != "live":
        return lev
    _fapi_request("POST", "/fapi/v1/leverage", {"symbol": symbol.upper(), "leverage": lev})
    return lev


def _place_limit_entry(
    symbol: str,
    direction: str,
    price: str,
    quantity: str,
    *,
    leg_index: int | None = None,
) -> dict[str, Any]:
    side = "BUY" if direction == "LONG" else "SELL"
    client_id = f"dash_{int(time.time())}"[:36]
    if leg_index is not None:
        client_id = f"dc{leg_index}_{int(time.time())}"[:36]
    params = _apply_position_params(
        {
            "symbol": symbol.upper(),
            "side": side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": quantity,
            "price": price,
            "newClientOrderId": client_id,
        },
        direction,
    )
    return _fapi_request("POST", "/fapi/v1/order", params)


def _cancel_algo_order(symbol: str, algo_id: int) -> None:
    _fapi_request(
        "DELETE",
        "/fapi/v1/algoOrder",
        {"symbol": symbol.upper(), "algoId": int(algo_id)},
    )


def _cancel_symbol_protection_orders(symbol: str, direction: str) -> None:
    """Cancel open SL/TP algo orders so they can be re-placed with updated qty."""
    primary = _primary_position_direction(direction)
    if not primary:
        return
    want_ps = primary if is_hedge_mode() else None
    for order in _get_open_algo_orders(symbol):
        if want_ps:
            pos_side = str(order.get("positionSide") or "").upper()
            if pos_side and pos_side not in (want_ps, "BOTH"):
                continue
        if _algo_order_role(order, primary) not in ("sl", "tp"):
            continue
        algo_id = order.get("algoId")
        if algo_id is None:
            continue
        try:
            _cancel_algo_order(symbol, int(algo_id))
            _append_orders_log(
                "protection_cancelled",
                symbol=symbol,
                algoId=algo_id,
                role=_algo_order_role(order, primary),
            )
        except RuntimeError as exc:
            logger.warning("%s: cancel protection algo %s failed: %s", symbol, algo_id, exc)


def _protection_qty_covers_position(symbol: str, direction: str, position_qty: str) -> bool:
    """True when existing SL/TP algo qty is >= open position qty."""
    primary = _primary_position_direction(direction)
    if not primary:
        return True
    try:
        pos_qty = Decimal(position_qty)
    except Exception:
        return True
    if pos_qty <= 0:
        return True

    want_ps = primary if is_hedge_mode() else None
    sl_qty = tp_qty = Decimal(0)
    for order in _get_open_algo_orders(symbol):
        if want_ps:
            pos_side = str(order.get("positionSide") or "").upper()
            if pos_side and pos_side not in (want_ps, "BOTH"):
                continue
        role = _algo_order_role(order, primary)
        try:
            order_qty = Decimal(str(order.get("quantity") or "0"))
        except Exception:
            order_qty = Decimal(0)
        if role == "sl" and order_qty > sl_qty:
            sl_qty = order_qty
        elif role == "tp" and order_qty > tp_qty:
            tp_qty = order_qty

    if sl_qty > 0 and sl_qty < pos_qty:
        return False
    if tp_qty > 0 and tp_qty < pos_qty:
        return False
    return True


def _wait_limit_fill(symbol: str, order_id: int, timeout_sec: int) -> str | None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        order = _fapi_request("GET", "/fapi/v1/order", {"symbol": symbol.upper(), "orderId": order_id})
        status = order.get("status")
        if status == "FILLED":
            return str(order.get("executedQty", ""))
        if status in ("CANCELED", "REJECTED", "EXPIRED"):
            return None
        time.sleep(REST_SL_TP_POLL_INTERVAL)
    return None


def _place_stop_loss(symbol: str, direction: str, stop_price: str, quantity: str) -> dict[str, Any]:
    side = "SELL" if direction == "LONG" else "BUY"
    params = _apply_position_params(
        {
            "symbol": symbol.upper(),
            "algoType": "CONDITIONAL",
            "side": side,
            "type": "STOP_MARKET",
            "triggerPrice": stop_price,
            "quantity": quantity,
            "workingType": "CONTRACT_PRICE",
        },
        direction,
        reduce_only=True,
    )
    return _fapi_request("POST", "/fapi/v1/algoOrder", params)


def _place_take_profit_fixed(symbol: str, direction: str, tp_price: str, quantity: str) -> dict[str, Any]:
    side = "SELL" if direction == "LONG" else "BUY"
    params = _apply_position_params(
        {
            "symbol": symbol.upper(),
            "algoType": "CONDITIONAL",
            "side": side,
            "type": "TAKE_PROFIT_MARKET",
            "triggerPrice": tp_price,
            "quantity": quantity,
            "workingType": "CONTRACT_PRICE",
        },
        direction,
        reduce_only=True,
    )
    return _fapi_request("POST", "/fapi/v1/algoOrder", params)


def _place_trailing_tp(symbol: str, direction: str, activation_price: str, quantity: str) -> dict[str, Any]:
    side = "SELL" if direction == "LONG" else "BUY"
    params = _apply_position_params(
        {
            "symbol": symbol.upper(),
            "algoType": "CONDITIONAL",
            "side": side,
            "type": "TRAILING_STOP_MARKET",
            "activatePrice": activation_price,
            "callbackRate": TP_TRAILING_CALLBACK_RATE,
            "quantity": quantity,
            "workingType": "CONTRACT_PRICE",
        },
        direction,
        reduce_only=True,
    )
    return _fapi_request("POST", "/fapi/v1/algoOrder", params)


def _place_sl_tp_after_fill(
    symbol: str,
    direction: str,
    sl: float,
    tp: float,
    quantity: str,
    order_id: int | None,
) -> dict[str, bool]:
    """Place SL then TP after entry fill. Never raises — logs and notifies on partial failure."""
    result = {"sl": False, "tp": False}
    if not REST_PLACE_SL_TP:
        return result

    fill_qty = quantity
    if order_id is not None:
        waited = _wait_limit_fill(symbol, order_id, REST_SL_TP_FILL_WAIT)
        if not waited:
            msg = f"Entry not filled in {REST_SL_TP_FILL_WAIT}s — SL/TP skipped"
            _set_status(message=msg, last_event="skip_sl_tp")
            _append_orders_log("skip_sl_tp", symbol=symbol, reason="entry_not_filled", orderId=order_id)
            telegram.notify_sl_tp_failed(
                symbol,
                direction,
                "SL/TP",
                f"Limit order {order_id} not FILLED within {REST_SL_TP_FILL_WAIT}s",
            )
            return result
        fill_qty = waited

    sl_price = round_price(symbol, sl)
    tp_price = round_price(symbol, tp)
    qty = round_qty(symbol, float(fill_qty))

    try:
        sl_resp = _place_stop_loss(symbol, direction, sl_price, qty)
        _append_orders_log("sl_placed", symbol=symbol, response=sl_resp)
        result["sl"] = True
    except RuntimeError as exc:
        logger.error("SL placement failed for %s: %s", symbol, exc)
        _append_orders_log("sl_failed", symbol=symbol, sl=sl_price, qty=qty, error=str(exc))
        telegram.notify_sl_tp_failed(symbol, direction, "SL", str(exc))

    try:
        placed, skipped = _place_tp_for_position(symbol, direction, tp_price, qty, log_suffix="_placed")
        if placed:
            result["tp"] = True
        elif skipped:
            _append_orders_log(
                "tp_skipped_after_fill",
                symbol=symbol,
                direction=direction,
                tp=tp_price,
                tp_type=TP_ORDER_TYPE,
                qty=qty,
                reason="mark_through_target",
            )
    except RuntimeError as exc:
        logger.error("TP placement failed for %s: %s", symbol, exc)
        _append_orders_log(
            "tp_failed",
            symbol=symbol,
            tp=tp_price,
            tp_type=TP_ORDER_TYPE,
            qty=qty,
            error=str(exc),
        )
        telegram.notify_sl_tp_failed(symbol, direction, "TP", str(exc))

    if result["sl"] and result["tp"]:
        _set_status(message=f"SL/TP placed · qty {qty}", last_event="sl_tp_placed")
    elif result["sl"] or result["tp"]:
        missing = "TP" if result["sl"] else "SL"
        _set_status(
            message=f"Partial protection · {missing} missing · qty {qty}",
            last_event="sl_tp_partial",
        )
    else:
        _set_status(message=f"SL/TP failed · qty {qty}", last_event="sl_tp_failed")
    return result


def _execute_open(
    symbol: str,
    direction: str,
    entry: float,
    sl: float,
    tp: float,
    size_pct: float,
    reasons: str,
    plan_fingerprint: str,
    *,
    leg_index: int = 0,
    dca_max_legs: int = 0,
) -> None:
    global _last_execution_monotonic

    symbol = symbol.upper()
    allowed, block_reason = can_place_new_order(symbol, direction, entry, size_pct=size_pct)
    if not allowed:
        _log_skip_order(symbol, direction, block_reason, entry=entry)
        _set_status(message=f"Blocked: {block_reason}", last_event=block_reason)
        return

    price_str = round_price(symbol, entry)
    try:
        qty = _calculate_quantity(symbol, entry, size_pct)
    except ValueError as exc:
        _set_status(message=str(exc), last_event="error")
        _append_orders_log("error", symbol=symbol, error=str(exc))
        return

    payload = {
        "symbol": symbol,
        "direction": direction,
        "entry": price_str,
        "sl": round_price(symbol, sl),
        "tp": round_price(symbol, tp),
        "qty": qty,
        "size_pct": size_pct,
        "mode": EXECUTION_MODE,
        "leverage_mode": LEVERAGE_MODE,
        "leverage": resolve_leverage_for_symbol(symbol),
        "reasons": reasons,
        "plan": plan_fingerprint,
    }

    if EXECUTION_MODE == "dry":
        _append_orders_log("dry_run_open", **payload)
        if REST_PLACE_SL_TP:
            _append_orders_log(
                "dry_run_sl",
                symbol=symbol,
                trigger=payload["sl"],
                qty=qty,
                type="STOP_MARKET",
            )
            tp_type = "TRAILING_STOP_MARKET" if TP_ORDER_TYPE == "trailing" else "TAKE_PROFIT_MARKET"
            _append_orders_log(
                "dry_run_tp",
                symbol=symbol,
                trigger=payload["tp"],
                qty=qty,
                type=tp_type,
                callbackRate=TP_TRAILING_CALLBACK_RATE if TP_ORDER_TYPE == "trailing" else None,
            )
        _set_status(
            message=f"DRY-RUN {direction} {symbol} entry {price_str} qty {qty}",
            last_event="dry_run_open",
            last_at=time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            last_symbol=symbol,
            last_direction=direction,
            last_order_id=None,
        )
        _last_execution_monotonic = time.monotonic()
        if dca_max_legs > 0:
            record_trade_context(
                symbol,
                direction,
                entry=price_str,
                sl=payload["sl"],
                tp=payload["tp"],
                tp_type=TP_ORDER_TYPE,
                dca_legs_placed=0,
                dca_max_legs=dca_max_legs,
            )
        telegram.notify_dry_run(symbol, direction, price_str)
        _log_execution_decision(
            symbol,
            "order_dry_run",
            outcome="dry_run",
            market_snapshot={"signal": direction, "entry": price_str, "leg": leg_index},
        )
        return

    if not _keys_configured():
        _set_status(message="Live mode requires BINANCE_API_KEY and BINANCE_SECRET_KEY", last_event="error")
        _append_orders_log("error", symbol=symbol, error="missing_api_keys")
        return

    try:
        applied_lev = _set_leverage(symbol)
        payload["leverage"] = applied_lev
        payload["hedge_mode"] = is_hedge_mode()
        response = _place_limit_entry(symbol, direction, price_str, qty, leg_index=leg_index if dca_max_legs else None)
        order_id = response.get("orderId")
        _append_orders_log("live_open", orderId=order_id, **payload)
        _set_status(
            message=f"LIVE {direction} {symbol} orderId {order_id}",
            last_event="live_open",
            last_at=time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
            last_symbol=symbol,
            last_direction=direction,
            last_order_id=order_id,
        )
        _last_execution_monotonic = time.monotonic()
        vol_usdt = f"{float(qty) * float(price_str):.2f}"
        tp_label = telegram.format_tp_label(
            payload["tp"],
            TP_ORDER_TYPE == "trailing",
            TP_TRAILING_CALLBACK_RATE,
        )
        telegram.notify_live_open(symbol, direction, price_str, payload["sl"], tp_label, vol_usdt)
        record_trade_context(
            symbol,
            direction,
            entry=price_str,
            sl=payload["sl"],
            tp=payload["tp"],
            tp_type=TP_ORDER_TYPE,
            dca_legs_placed=0 if dca_max_legs else None,
            dca_max_legs=dca_max_legs if dca_max_legs else None,
        )
        if dca_max_legs > 0:
            _sync_dca_leg_count(symbol)
        _log_execution_decision(
            symbol,
            "order_live_open",
            outcome="live",
            market_snapshot={"signal": direction, "entry": price_str, "orderId": order_id, "leg": leg_index},
        )
        if order_id is not None:
            _place_sl_tp_after_fill(symbol, direction, sl, tp, qty, int(order_id))
    except RuntimeError as exc:
        logger.error("Order failed for %s: %s", symbol, exc)
        _append_orders_log("live_open_failed", symbol=symbol, error=str(exc), **payload)
        _set_status(message=f"Order failed: {exc}", last_event="error")
        telegram.notify_order_failed(symbol, direction)


def _execute_signal_dca_add(
    symbol: str,
    direction: str,
    entry: float,
    sl: float,
    tp: float,
    size_pct: float,
    leg_index: int,
    max_legs: int,
    reasons: str,
    plan_fingerprint: str,
) -> None:
    """Place one add-on limit when a new valid entry fires with an open position."""
    global _last_execution_monotonic

    symbol = symbol.upper()
    direction = direction.upper()
    allowed, block_reason = can_place_dca_add(
        symbol,
        direction,
        entry,
        size_pct=size_pct,
        max_legs=max_legs,
    )
    if not allowed:
        _log_skip_order(symbol, direction, block_reason, entry=entry)
        _set_status(message=f"DCA add blocked: {block_reason}", last_event=block_reason)
        return

    price_str = round_price(symbol, entry)
    try:
        qty = _calculate_quantity(symbol, entry, size_pct)
    except ValueError as exc:
        _set_status(message=str(exc), last_event="error")
        _append_orders_log("error", symbol=symbol, error=str(exc), leg=leg_index)
        return

    sl_price = round_price(symbol, sl)
    tp_price = round_price(symbol, tp)

    if EXECUTION_MODE == "dry":
        _append_orders_log(
            "dry_run_dca_add",
            symbol=symbol,
            direction=direction,
            leg=leg_index,
            entry=price_str,
            qty=qty,
            size_pct=size_pct,
            plan=plan_fingerprint,
        )
        record_trade_context(
            symbol,
            direction,
            sl=sl_price,
            tp=tp_price,
            tp_type=TP_ORDER_TYPE,
            dca_legs_placed=leg_index + 1,
            dca_max_legs=max_legs,
        )
        _set_status(
            message=f"DRY-RUN DCA add {direction} {symbol} leg {leg_index} @ {price_str}",
            last_event="dry_run_dca_add",
            last_symbol=symbol,
            last_direction=direction,
        )
        _last_execution_monotonic = time.monotonic()
        telegram.notify_dry_run(symbol, direction, f"DCA {leg_index} @ {price_str}")
        _log_execution_decision(
            symbol,
            "order_dry_run_dca_add",
            outcome="dry_run",
            market_snapshot={"signal": direction, "leg": leg_index, "entry": price_str},
        )
        return

    if not _keys_configured():
        _set_status(message="Live mode requires BINANCE_API_KEY and BINANCE_SECRET_KEY", last_event="error")
        return

    try:
        response = _place_limit_entry(symbol, direction, price_str, qty, leg_index=leg_index)
        order_id = response.get("orderId")
        _append_orders_log(
            "live_dca_add",
            symbol=symbol,
            direction=direction,
            leg=leg_index,
            orderId=order_id,
            entry=price_str,
            qty=qty,
            size_pct=size_pct,
            plan=plan_fingerprint,
        )
        record_trade_context(
            symbol,
            direction,
            sl=sl_price,
            tp=tp_price,
            tp_type=TP_ORDER_TYPE,
            dca_legs_placed=leg_index + 1,
            dca_max_legs=max_legs,
        )
        _last_execution_monotonic = time.monotonic()
        vol_usdt = f"{float(qty) * float(price_str):.2f}"
        tp_label = "Trailing TP" if TP_ORDER_TYPE == "trailing" else "TP"
        telegram.notify_live_open(
            symbol,
            direction,
            f"DCA {leg_index} @ {price_str}",
            sl_price,
            tp_label,
            vol_usdt,
        )
        _set_status(
            message=f"LIVE DCA add {direction} {symbol} leg {leg_index} · {order_id}",
            last_event="live_dca_add",
            last_symbol=symbol,
            last_direction=direction,
            last_order_id=order_id,
        )
        _log_execution_decision(
            symbol,
            "order_live_dca_add",
            outcome="live",
            market_snapshot={"signal": direction, "leg": leg_index, "orderId": order_id},
        )
        reconcile_position_protection(symbol, direction=direction, sl=sl, tp=tp)
    except RuntimeError as exc:
        logger.error("DCA add failed for %s leg %s: %s", symbol, leg_index, exc)
        _append_orders_log("live_dca_add_failed", symbol=symbol, leg=leg_index, error=str(exc))
        _set_status(message=f"DCA add failed: {exc}", last_event="error")
        telegram.notify_order_failed(symbol, direction)


def _execute_open_dca(
    symbol: str,
    direction: str,
    sl: float,
    tp: float,
    legs: list[dict[str, Any]],
    reasons: str,
    plan_fingerprint: str,
    avg_entry: float,
) -> None:
    """Place one LIMIT per trade-plan leg; SL/TP via reconcile when position exists."""
    global _last_execution_monotonic

    symbol = symbol.upper()
    direction = direction.upper()
    sl_price = round_price(symbol, sl)
    tp_price = round_price(symbol, tp)

    if EXECUTION_MODE == "dry":
        leg_rows: list[dict[str, Any]] = []
        for index, leg in enumerate(legs):
            price = float(leg["price"])
            size_pct = float(leg["size_pct"])
            price_str = round_price(symbol, price)
            try:
                qty = _calculate_quantity(symbol, price, size_pct)
            except ValueError as exc:
                _set_status(message=str(exc), last_event="error")
                _append_orders_log("error", symbol=symbol, error=str(exc))
                return
            leg_rows.append(
                {
                    "leg": index,
                    "label": leg.get("label"),
                    "price": price_str,
                    "qty": qty,
                    "size_pct": size_pct,
                }
            )
            _append_orders_log(
                "dry_run_dca_leg",
                symbol=symbol,
                leg=index,
                direction=direction,
                entry=price_str,
                qty=qty,
                size_pct=size_pct,
            )
        _append_orders_log(
            "dry_run_dca_bundle",
            symbol=symbol,
            direction=direction,
            legs=leg_rows,
            sl=sl_price,
            tp=tp_price,
            plan=plan_fingerprint,
        )
        if REST_PLACE_SL_TP:
            total_qty = leg_rows[-1]["qty"] if leg_rows else "0"
            _append_orders_log(
                "dry_run_sl",
                symbol=symbol,
                trigger=sl_price,
                qty=total_qty,
                note="SL/TP on reconcile when filled (DCA)",
                type="STOP_MARKET",
            )
        _set_status(
            message=f"DRY-RUN DCA {direction} {symbol} · {len(leg_rows)} limits",
            last_event="dry_run_dca",
            last_symbol=symbol,
            last_direction=direction,
        )
        _last_execution_monotonic = time.monotonic()
        record_trade_context(
            symbol,
            direction,
            entry=round_price(symbol, avg_entry),
            sl=sl_price,
            tp=tp_price,
            tp_type=TP_ORDER_TYPE,
        )
        _log_execution_decision(
            symbol,
            "order_dry_run_dca",
            outcome="dry_run",
            market_snapshot={"signal": direction, "legs": len(leg_rows)},
        )
        telegram.notify_dry_run(
            symbol,
            direction,
            f"{round_price(symbol, avg_entry)} · {len(leg_rows)} limits (DCA)",
        )
        return

    if not _keys_configured():
        _set_status(message="Live mode requires BINANCE_API_KEY and BINANCE_SECRET_KEY", last_event="error")
        return

    try:
        applied_lev = _set_leverage(symbol)
    except RuntimeError as exc:
        _set_status(message=f"Leverage failed: {exc}", last_event="error")
        return

    placed: list[dict[str, Any]] = []
    for index, leg in enumerate(legs):
        price = float(leg["price"])
        size_pct = float(leg["size_pct"])
        price_str = round_price(symbol, price)
        try:
            qty = _calculate_quantity(symbol, price, size_pct)
        except ValueError as exc:
            _set_status(message=str(exc), last_event="error")
            _append_orders_log("error", symbol=symbol, error=str(exc), leg=index)
            return
        try:
            response = _place_limit_entry(symbol, direction, price_str, qty, leg_index=index)
            order_id = response.get("orderId")
            row = {
                "leg": index,
                "label": leg.get("label"),
                "orderId": order_id,
                "price": price_str,
                "qty": qty,
                "size_pct": size_pct,
            }
            placed.append(row)
            _append_orders_log("live_dca_leg", **row, symbol=symbol, direction=direction)
        except RuntimeError as exc:
            logger.error("DCA leg %s failed for %s: %s", index, symbol, exc)
            _append_orders_log("live_dca_leg_failed", symbol=symbol, leg=index, error=str(exc))
            _set_status(message=f"DCA leg {index} failed: {exc}", last_event="error")
            telegram.notify_order_failed(symbol, direction)
            return

    _append_orders_log(
        "live_dca_bundle",
        symbol=symbol,
        direction=direction,
        leverage=applied_lev,
        sl=sl_price,
        tp=tp_price,
        legs=placed,
        plan=plan_fingerprint,
    )
    _last_execution_monotonic = time.monotonic()
    record_trade_context(
        symbol,
        direction,
        entry=round_price(symbol, avg_entry),
        sl=sl_price,
        tp=tp_price,
        tp_type=TP_ORDER_TYPE,
    )
    _set_status(
        message=f"LIVE DCA {direction} {symbol} · {len(placed)} limits · SL/TP on fill",
        last_event="live_dca",
        last_at=time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        last_symbol=symbol,
        last_direction=direction,
    )
    _log_execution_decision(
        symbol,
        "order_live_dca",
        outcome="live",
        market_snapshot={"signal": direction, "legs": len(placed)},
    )
    total_pct = sum(float(leg["size_pct"]) for leg in placed)
    vol_usdt = f"{estimate_order_notional_usdt(total_pct):.2f}"
    tp_label = "Trailing TP" if TP_ORDER_TYPE == "trailing" else "TP"
    telegram.notify_live_open(
        symbol,
        direction,
        f"{round_price(symbol, avg_entry)} · {len(placed)} limits (DCA)",
        sl_price,
        tp_label,
        vol_usdt,
    )
    reconcile_position_protection(symbol, direction=direction, sl=sl, tp=tp)


def try_execute_valid_entry(
    symbol: str,
    signal: str,
    entry: float | None,
    trade_plan: dict | None,
    reasons: str,
    trend_bias: str,
) -> None:
    """Fire-and-forget execution when dashboard records a valid entry."""
    if not EXECUTE_ON_VALID_ENTRY:
        _append_orders_log(
            "skip_valid_entry_auto",
            symbol=symbol.upper(),
            signal=signal,
            execute_on_valid_entry=False,
        )
        _log_execution_decision(symbol, "order_skip", block_reason="execute_off")
        return
    if not EXECUTION_ENABLED or telegram.is_trading_paused():
        _append_orders_log(
            "skip_disabled",
            symbol=symbol.upper(),
            signal=signal,
            execution_enabled=EXECUTION_ENABLED,
            trading_paused=telegram.is_trading_paused(),
        )
        reason = "fleet_paused" if telegram.is_trading_paused() else "execution_disabled"
        _log_execution_decision(symbol, "order_skip", block_reason=reason, market_snapshot={"signal": signal})
        return
    if entry is None or not trade_plan or not trade_plan.get("active"):
        _append_orders_log(
            "skip_no_plan",
            symbol=symbol.upper(),
            signal=signal,
            entry=entry,
            has_trade_plan=trade_plan is not None,
            trade_plan_active=bool(trade_plan and trade_plan.get("active")),
        )
        _log_execution_decision(symbol, "order_skip", block_reason="no_plan", market_snapshot={"signal": signal})
        return

    global _last_execution_monotonic
    now = time.monotonic()
    if (
        _last_execution_monotonic is not None
        and now - _last_execution_monotonic < EXECUTION_ORDER_COOLDOWN
    ):
        _append_orders_log("skip_cooldown", symbol=symbol.upper(), signal=signal)
        _log_execution_decision(symbol, "order_skip", block_reason="execution_cooldown", market_snapshot={"signal": signal})
        return

    sl = float(trade_plan.get("sl", 0))
    tp = float(trade_plan.get("tp1", 0))
    if sl <= 0 or tp <= 0:
        _append_orders_log("skip_invalid_sl_tp", symbol=symbol.upper(), signal=signal, sl=sl, tp=tp)
        _log_execution_decision(symbol, "order_skip", block_reason="invalid_sl_tp", market_snapshot={"signal": signal})
        return

    legs = trade_plan.get("legs") or []
    use_dca = TRADE_PLAN_EXECUTE_DCA and len(legs) > 1
    avg_entry = float(trade_plan.get("avg_entry") or entry or 0)

    if use_dca and TRADE_PLAN_DCA_SIGNAL_DRIVEN:
        _sync_dca_leg_count(symbol)
        placed = dca_legs_placed(symbol)
        if has_open_position(symbol):
            leg_index = placed
        else:
            leg_index = 0
        if leg_index >= len(legs):
            _append_orders_log(
                "skip_dca_max",
                symbol=symbol.upper(),
                signal=signal,
                legs_placed=placed,
                legs_total=len(legs),
            )
            _log_execution_decision(
                symbol,
                "order_skip",
                block_reason="dca_max_legs",
                market_snapshot={"signal": signal, "legs_placed": placed},
            )
            return
        leg = legs[leg_index]
        entry_price = float(entry)
        size_pct = float(leg["size_pct"])
        fingerprint = f"{signal}|{sl:.2f}|{tp:.2f}|leg{leg_index}|{entry_price:.4f}"
        if leg_index == 0:
            allowed, block_reason = can_place_new_order(
                symbol, signal, entry_price, size_pct=size_pct
            )
            if not allowed:
                _log_skip_order(symbol, signal, block_reason, entry=entry_price)
                return
            thread = threading.Thread(
                target=_execute_open,
                kwargs={
                    "symbol": symbol,
                    "direction": signal,
                    "entry": entry_price,
                    "sl": sl,
                    "tp": tp,
                    "size_pct": size_pct,
                    "reasons": reasons,
                    "plan_fingerprint": fingerprint,
                    "leg_index": 0,
                    "dca_max_legs": len(legs),
                },
                daemon=True,
                name=f"exec-{symbol}-{signal}-leg0",
            )
        else:
            allowed, block_reason = can_place_dca_add(
                symbol,
                signal,
                entry_price,
                size_pct=size_pct,
                max_legs=len(legs),
            )
            if not allowed:
                _log_skip_order(symbol, signal, block_reason, entry=entry_price)
                return
            thread = threading.Thread(
                target=_execute_signal_dca_add,
                args=(
                    symbol,
                    signal,
                    entry_price,
                    sl,
                    tp,
                    size_pct,
                    leg_index,
                    len(legs),
                    reasons,
                    fingerprint,
                ),
                daemon=True,
                name=f"exec-dca-add-{symbol}-{signal}-leg{leg_index}",
            )
    elif use_dca:
        leg_prices = "|".join(f"{float(leg['price']):.4f}" for leg in legs) if legs else ""
        fingerprint = f"{signal}|{sl:.2f}|{tp:.2f}|{leg_prices}"
        allowed, block_reason = can_place_dca_bundle(symbol, signal, legs)
        if not allowed:
            _log_skip_order(symbol, signal, block_reason, entry=avg_entry)
            return
        thread = threading.Thread(
            target=_execute_open_dca,
            args=(symbol, signal, sl, tp, legs, reasons, fingerprint, avg_entry),
            daemon=True,
            name=f"exec-dca-{symbol}-{signal}",
        )
    else:
        size_pct = float(legs[0]["size_pct"]) if legs else float(trade_plan.get("partial_close_pct", 50))
        entry_price = float(legs[0]["price"]) if legs else float(entry)
        fingerprint = f"{signal}|{sl:.2f}|{tp:.2f}|{entry_price:.4f}"
        allowed, block_reason = can_place_new_order(symbol, signal, entry_price, size_pct=size_pct)
        if not allowed:
            _log_skip_order(symbol, signal, block_reason, entry=entry_price)
            return
        thread = threading.Thread(
            target=_execute_open,
            args=(symbol, signal, entry_price, sl, tp, size_pct, reasons, fingerprint),
            daemon=True,
            name=f"exec-{symbol}-{signal}",
        )
    thread.start()
