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
LOG_DIR = os.getenv("LOG_DIR", "logs")

_status_lock = threading.Lock()
_hedge_mode_lock = threading.Lock()
_hedge_mode: bool | None = None
_last_execution_monotonic: float | None = None
_execution_status: dict[str, Any] = {
    "enabled": EXECUTION_ENABLED,
    "mode": EXECUTION_MODE,
    "message": "Execution disabled" if not EXECUTION_ENABLED else f"Ready ({EXECUTION_MODE})",
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


def get_execution_status() -> dict[str, Any]:
    with _status_lock:
        return dict(_execution_status)


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

    thread = threading.Thread(
        target=_execute_open,
        args=(symbol, signal, entry_price, sl, tp, size_pct, reasons, fingerprint),
        daemon=True,
        name=f"exec-{symbol}-{signal}",
    )
    thread.start()
