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

from dotenv import load_dotenv

load_dotenv()

import telegram_notify as telegram

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
EXECUTION_BLOCK_IF_OPEN = _env_bool("EXECUTION_BLOCK_IF_OPEN", "true")
EXECUTION_POSITION_CACHE_SEC = float(os.getenv("EXECUTION_POSITION_CACHE_SEC", "5"))
BE_EXIT_PNL_MAX_USDT = float(os.getenv("BE_EXIT_PNL_MAX_USDT", "0.15"))
BE_EXIT_PRICE_PCT = float(os.getenv("BE_EXIT_PRICE_PCT", "0.12"))
LOG_DIR = os.getenv("LOG_DIR", "logs")

_status_lock = threading.Lock()
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
) -> None:
    _update_trade_context(
        symbol,
        direction=direction.upper(),
        entry=str(entry) if entry is not None else None,
        sl=str(sl) if sl is not None else None,
        tp=str(tp) if tp is not None else None,
        tp_type=(tp_type or TP_ORDER_TYPE).lower(),
        was_open=False,
        exit_notified=False,
    )


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
        _update_trade_context(symbol, was_open=False, exit_notified=True)
        return

    exit_price = float(trade["price"])
    pnl = float(trade["realized_pnl"])
    pnl_label = _format_realized_pnl(pnl)

    _update_trade_context(symbol, was_open=False, exit_notified=True)
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


def can_place_new_order(symbol: str, direction: str, entry: float) -> tuple[bool, str]:
    """Mirror trade-binance-websocket-order-blocks can_place_new_order."""
    if not EXECUTION_BLOCK_IF_OPEN or not _keys_configured():
        return True, ""

    symbol = symbol.upper()
    direction = direction.upper()

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


def _log_skip_order(symbol: str, signal: str, reason: str, *, entry: float | None = None) -> None:
    event = {
        "open_position": "skip_open_position",
        "pending_entry_limit": "skip_pending_limit",
        "pending_opposite_limit": "skip_pending_limit",
        "conflicting_limits": "skip_conflicting_limits",
        "duplicate_limit_price": "skip_duplicate_limit",
    }.get(reason, "skip_order_guard")
    fields: dict[str, Any] = {"symbol": symbol.upper(), "signal": signal, "reason": reason}
    if entry is not None:
        fields["entry"] = entry
    _append_orders_log(event, **fields)
    logger.warning("%s: blocked new %s order (%s)", symbol.upper(), signal, reason)


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


def _place_limit_entry(symbol: str, direction: str, price: str, quantity: str) -> dict[str, Any]:
    side = "BUY" if direction == "LONG" else "SELL"
    params = _apply_position_params(
        {
            "symbol": symbol.upper(),
            "side": side,
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": quantity,
            "price": price,
            "newClientOrderId": f"dash_{int(time.time())}"[:36],
        },
        direction,
    )
    return _fapi_request("POST", "/fapi/v1/order", params)


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
) -> None:
    if not REST_PLACE_SL_TP:
        return

    fill_qty = quantity
    if order_id is not None:
        waited = _wait_limit_fill(symbol, order_id, REST_SL_TP_FILL_WAIT)
        if not waited:
            _set_status(message=f"Entry not filled in {REST_SL_TP_FILL_WAIT}s — SL/TP skipped")
            _append_orders_log("skip_sl_tp", symbol=symbol, reason="entry_not_filled", orderId=order_id)
            return
        fill_qty = waited

    sl_price = round_price(symbol, sl)
    tp_price = round_price(symbol, tp)
    qty = round_qty(symbol, float(fill_qty))

    sl_resp = _place_stop_loss(symbol, direction, sl_price, qty)
    _append_orders_log("sl_placed", symbol=symbol, response=sl_resp)

    if TP_ORDER_TYPE == "trailing":
        tp_resp = _place_trailing_tp(symbol, direction, tp_price, qty)
        _append_orders_log("tp_trailing_placed", symbol=symbol, response=tp_resp)
    else:
        tp_resp = _place_take_profit_fixed(symbol, direction, tp_price, qty)
        _append_orders_log("tp_fixed_placed", symbol=symbol, response=tp_resp)

    _set_status(message=f"SL/TP placed · qty {qty}")


def _execute_open(
    symbol: str,
    direction: str,
    entry: float,
    sl: float,
    tp: float,
    size_pct: float,
    reasons: str,
    plan_fingerprint: str,
) -> None:
    global _last_execution_monotonic

    symbol = symbol.upper()
    allowed, block_reason = can_place_new_order(symbol, direction, entry)
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
        telegram.notify_dry_run(symbol, direction, price_str)
        return

    if not _keys_configured():
        _set_status(message="Live mode requires BINANCE_API_KEY and BINANCE_SECRET_KEY", last_event="error")
        _append_orders_log("error", symbol=symbol, error="missing_api_keys")
        return

    try:
        applied_lev = _set_leverage(symbol)
        payload["leverage"] = applied_lev
        payload["hedge_mode"] = is_hedge_mode()
        response = _place_limit_entry(symbol, direction, price_str, qty)
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
        )
        if order_id is not None:
            _place_sl_tp_after_fill(symbol, direction, sl, tp, qty, int(order_id))
    except RuntimeError as exc:
        logger.error("Order failed for %s: %s", symbol, exc)
        _append_orders_log("live_open_failed", symbol=symbol, error=str(exc), **payload)
        _set_status(message=f"Order failed: {exc}", last_event="error")
        telegram.notify_order_failed(symbol, direction)


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
        return
    if not EXECUTION_ENABLED or telegram.is_trading_paused():
        _append_orders_log(
            "skip_disabled",
            symbol=symbol.upper(),
            signal=signal,
            execution_enabled=EXECUTION_ENABLED,
            trading_paused=telegram.is_trading_paused(),
        )
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
        return

    global _last_execution_monotonic
    now = time.monotonic()
    if (
        _last_execution_monotonic is not None
        and now - _last_execution_monotonic < EXECUTION_ORDER_COOLDOWN
    ):
        _append_orders_log("skip_cooldown", symbol=symbol.upper(), signal=signal)
        return

    sl = float(trade_plan.get("sl", 0))
    tp = float(trade_plan.get("tp1", 0))
    if sl <= 0 or tp <= 0:
        _append_orders_log("skip_invalid_sl_tp", symbol=symbol.upper(), signal=signal, sl=sl, tp=tp)
        return

    legs = trade_plan.get("legs") or []
    size_pct = float(legs[0]["size_pct"]) if legs else float(trade_plan.get("partial_close_pct", 50))
    entry_price = float(legs[0]["price"]) if legs else float(entry)
    fingerprint = f"{signal}|{entry_price:.2f}|{sl:.2f}|{tp:.2f}"

    allowed, block_reason = can_place_new_order(symbol, signal, entry_price)
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
