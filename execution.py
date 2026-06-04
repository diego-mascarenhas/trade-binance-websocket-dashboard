"""Binance Futures order execution (dry-run or live), modeled on trade-binance-websocket-order-blocks."""

from __future__ import annotations

import calendar
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
from decimal import Decimal, ROUND_DOWN, ROUND_UP
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
ENTRY_FIRST_LEG_MARKET = _env_bool("ENTRY_FIRST_LEG_MARKET", "true")
PROTECTION_RECONCILE_ENABLED = _env_bool("PROTECTION_RECONCILE_ENABLED", "true")
FLEET_SIDE_BALANCE_MAX_PCT = float(os.getenv("FLEET_SIDE_BALANCE_MAX_PCT", "20"))
FLEET_EXPOSURE_CACHE_SEC = float(os.getenv("FLEET_EXPOSURE_CACHE_SEC", "8"))
EXECUTION_BLOCK_IF_OPEN = _env_bool("EXECUTION_BLOCK_IF_OPEN", "true")
EXECUTION_POSITION_CACHE_SEC = float(os.getenv("EXECUTION_POSITION_CACHE_SEC", "5"))
ACCOUNT_SNAPSHOT_CACHE_SEC = float(os.getenv("ACCOUNT_SNAPSHOT_CACHE_SEC", "15"))
PNL_STATS_LOOKBACK_DAYS = max(1, int(os.getenv("PNL_STATS_LOOKBACK_DAYS", "30")))
MILLION_GOAL_USDT = float(os.getenv("MILLION_GOAL_USDT", "1000000"))
BE_EXIT_PNL_MAX_USDT = float(os.getenv("BE_EXIT_PNL_MAX_USDT", "0.15"))
BE_EXIT_PRICE_PCT = float(os.getenv("BE_EXIT_PRICE_PCT", "0.12"))
EXIT_NOTIFY_MAX_AGE_SEC = max(60, int(os.getenv("EXIT_NOTIFY_MAX_AGE_SEC", "900")))
LOG_DIR = os.getenv("LOG_DIR", "logs")
TRADE_PLAN_EXECUTE_DCA = _env_bool("TRADE_PLAN_EXECUTE_DCA", "true")
TRADE_PLAN_DCA_SIGNAL_DRIVEN = _env_bool("TRADE_PLAN_DCA_SIGNAL_DRIVEN", "true")
TRADE_PLAN_DCA_ADVERSE_ONLY = _env_bool("TRADE_PLAN_DCA_ADVERSE_ONLY", "true")
DCA_FAVORABLE_PNL_MAX_USDT = float(os.getenv("DCA_FAVORABLE_PNL_MAX_USDT", "0"))
TRADE_PLAN_TP1_RR = float(os.getenv("TRADE_PLAN_TP1_RR", "1.0"))
TRADE_PLAN_SL_MIN_DISTANCE_PCT = float(os.getenv("TRADE_PLAN_SL_MIN_DISTANCE_PCT", "1.0"))
TRADE_PLAN_AUTO_BE = _env_bool("TRADE_PLAN_AUTO_BE", "true")
TRADE_PLAN_PARTIAL_CLOSE_PCT = float(os.getenv("TRADE_PLAN_PARTIAL_CLOSE_PCT", "70"))
TRADE_PLAN_BE_BUFFER_PCT = float(os.getenv("TRADE_PLAN_BE_BUFFER_PCT", "0.05"))
PARTIAL_CLOSE_DETECT_TOLERANCE_PCT = float(os.getenv("PARTIAL_CLOSE_DETECT_TOLERANCE_PCT", "8"))
BE_TRIGGER_PARTIAL = _env_bool("BE_TRIGGER_PARTIAL", "true")
BE_TRIGGER_SIGNAL = _env_bool("BE_TRIGGER_SIGNAL", "true")
BE_MIN_PROFIT_PCT = float(os.getenv("BE_MIN_PROFIT_PCT", "0.25"))
BE_MIN_PROFIT_USDT = float(os.getenv("BE_MIN_PROFIT_USDT", "0"))
BE_ON_HTF_NEUTRAL = _env_bool("BE_ON_HTF_NEUTRAL", "true")
BE_ON_HTF_AGAINST = _env_bool("BE_ON_HTF_AGAINST", "true")
BE_ON_SMC_RANGING = _env_bool("BE_ON_SMC_RANGING", "true")
BE_ON_RSI_EXIT = _env_bool("BE_ON_RSI_EXIT", "true")
BE_ON_EMA_FLIP = _env_bool("BE_ON_EMA_FLIP", "false")
BE_RSI_LONG_EXIT_MAX = float(os.getenv("BE_RSI_LONG_EXIT_MAX", os.getenv("RSI_LONG_MAX", "70")))
BE_RSI_SHORT_EXIT_MIN = float(os.getenv("BE_RSI_SHORT_EXIT_MIN", os.getenv("RSI_SHORT_MIN", "30")))
BE_USE_SPREAD = _env_bool("BE_USE_SPREAD", "true")
BE_SPREAD_MULTIPLIER = float(os.getenv("BE_SPREAD_MULTIPLIER", "1.0"))
BE_LOCK_CURRENT_PROFIT = _env_bool("BE_LOCK_CURRENT_PROFIT", "true")
TP_REPRICE_TOLERANCE_PCT = float(os.getenv("TP_REPRICE_TOLERANCE_PCT", "0.5"))
SL_REPRICE_TOLERANCE_PCT = float(os.getenv("SL_REPRICE_TOLERANCE_PCT", "0.35"))

_status_lock = threading.Lock()
_maintenance_lock = threading.Lock()
_last_maintenance: dict[str, float] = {}
_fleet_exposure_lock = threading.Lock()
_fleet_exposure_cache: tuple[float, dict[str, Any]] | None = None
_fleet_positions_lock = threading.Lock()
_fleet_positions_cache: tuple[float, dict[str, Any]] | None = None
_last_position_api_error: str | None = None
_hedge_mode_lock = threading.Lock()
_hedge_mode: bool | None = None
_last_execution_monotonic: float | None = None
_execution_inflight_lock = threading.Lock()
_execution_inflight_symbol: str | None = None


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
_account_snapshot_lock = threading.Lock()
_account_snapshot_cache: tuple[float, dict[str, Any]] | None = None
_baseline_lock = threading.Lock()
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


def stage_valid_entry_snapshot(symbol: str, market_snapshot: dict[str, Any]) -> None:
    """Store RSI/ADX/confidence from the latest valid_entry until the position opens."""
    if not market_snapshot:
        return
    _update_trade_context(
        symbol.upper(),
        pending_entry_market_snapshot=dict(market_snapshot),
        pending_entry_at=time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    )


def _resolve_entry_link(symbol: str, ctx: dict[str, Any]) -> tuple[dict[str, Any] | None, int | None]:
    snapshot = ctx.get("entry_market_snapshot")
    if isinstance(snapshot, dict) and snapshot:
        market = snapshot
    else:
        pending = ctx.get("pending_entry_market_snapshot")
        market = pending if isinstance(pending, dict) and pending else None

    event_id = ctx.get("entry_decision_event_id")
    try:
        event_id = int(event_id) if event_id is not None else None
    except (TypeError, ValueError):
        event_id = None

    if market and event_id:
        return market, event_id

    if not db_store.is_enabled():
        return market, event_id

    row = db_store.fetch_last_valid_entry(
        symbol,
        before=ctx.get("entry_opened_at") or ctx.get("pending_entry_at"),
    )
    if not row:
        return market, event_id

    if not market:
        db_market = row.get("market_snapshot")
        market = db_market if isinstance(db_market, dict) and db_market else None
    if not event_id:
        event_id = row.get("id")
    return market, event_id


def _bind_entry_snapshot_for_open_position(symbol: str) -> None:
    """Attach pending valid_entry features to the trade (first open only)."""
    symbol = symbol.upper()
    ctx = _get_trade_context(symbol)
    if ctx.get("entry_market_snapshot"):
        return

    pending = ctx.get("pending_entry_market_snapshot")
    fields: dict[str, Any] = {}
    if isinstance(pending, dict) and pending:
        fields["entry_market_snapshot"] = dict(pending)

    if db_store.is_enabled():
        row = db_store.fetch_last_valid_entry(
            symbol,
            before=ctx.get("entry_opened_at") or ctx.get("pending_entry_at"),
        )
        if row:
            if "entry_market_snapshot" not in fields:
                db_market = row.get("market_snapshot")
                if isinstance(db_market, dict) and db_market:
                    fields["entry_market_snapshot"] = db_market
            if row.get("id"):
                fields["entry_decision_event_id"] = int(row["id"])

    if fields:
        _update_trade_context(symbol, **fields)


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
    existing = _get_trade_context(symbol)
    if not existing.get("entry_opened_at"):
        fields["entry_opened_at"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    _update_trade_context(symbol, **fields)
    _bind_entry_snapshot_for_open_position(symbol)


def dca_legs_placed(symbol: str) -> int:
    try:
        return max(int(_get_trade_context(symbol).get("dca_legs_placed") or 0), 0)
    except (TypeError, ValueError):
        return 0


def reset_dca_state(symbol: str) -> None:
    _update_trade_context(
        symbol.upper(),
        dca_legs_placed=0,
        dca_max_legs=0,
        be_applied=False,
        position_peak_qty=None,
    )


def _fetch_book_spread(symbol: str) -> float | None:
    """Best ask − best bid from Binance bookTicker (live spread)."""
    try:
        data = _fapi_public_get("/fapi/v1/ticker/bookTicker", {"symbol": symbol.upper()})
        bid = float(data.get("bidPrice") or 0)
        ask = float(data.get("askPrice") or 0)
        if bid > 0 and ask > 0 and ask >= bid:
            return ask - bid
    except (RuntimeError, TypeError, ValueError) as exc:
        logger.debug("%s: bookTicker spread unavailable: %s", symbol, exc)
    return None


def _spread_offset(spread_abs: float | None) -> float:
    if BE_USE_SPREAD and spread_abs is not None and spread_abs > 0:
        return spread_abs * BE_SPREAD_MULTIPLIER
    return 0.0


def _resolve_lock_profit_pct(snapshot: dict[str, Any]) -> float:
    """% gain to lock when moving SL (min floor + optional full unrealized)."""
    lock = max(0.0, BE_MIN_PROFIT_PCT)
    try:
        unrealized_pct = float(snapshot.get("unrealized_pnl_pct") or 0)
    except (TypeError, ValueError):
        unrealized_pct = 0.0
    if BE_LOCK_CURRENT_PROFIT and unrealized_pct > lock:
        lock = unrealized_pct
    return lock


def profit_protect_sl_price(
    direction: str,
    entry: float,
    *,
    lock_profit_pct: float,
    spread_abs: float | None = None,
) -> float:
    """
    Stop that locks at least lock_profit_pct unrealized gain.

    SHORT in profit: SL below entry (stop buy when price rallies to lock level).
    LONG in profit: SL above entry (stop sell when price dips to lock level).
    """
    direction = direction.upper()
    if entry <= 0:
        return entry

    spread_part = _spread_offset(spread_abs)
    pct_offset = entry * (TRADE_PLAN_BE_BUFFER_PCT / 100)
    lock = max(0.0, float(lock_profit_pct))

    if lock <= 0:
        if direction == "LONG":
            return entry - spread_part - pct_offset
        if direction == "SHORT":
            return entry + spread_part + pct_offset
        return entry

    if direction == "SHORT":
        return entry * (1 - lock / 100) - spread_part - pct_offset
    if direction == "LONG":
        return entry * (1 + lock / 100) - spread_part - pct_offset
    return entry


def breakeven_sl_price(
    direction: str,
    entry: float,
    *,
    spread_abs: float | None = None,
) -> float:
    """Legacy entry-level BE (0% lock). Prefer profit_protect_sl_price when in profit."""
    return profit_protect_sl_price(
        direction,
        entry,
        lock_profit_pct=0.0,
        spread_abs=spread_abs,
    )


def round_price_for_profit_sl(symbol: str, direction: str, value: float) -> str:
    """Round protective stop — favor locking more profit (DOWN)."""
    filt = _load_symbol_filters(symbol)
    tick = filt["tick_size"]
    quantized = Decimal(str(value)).quantize(tick, rounding=ROUND_DOWN)
    return format(quantized, "f")


def _position_qty_decimal(snapshot: dict[str, Any]) -> Decimal:
    try:
        return Decimal(str(snapshot.get("qty") or "0"))
    except Exception:
        return Decimal(0)


def _update_position_peak_qty(symbol: str, snapshot: dict[str, Any]) -> tuple[Decimal, Decimal]:
    """Track max position size to detect partial closes (TP1)."""
    current = _position_qty_decimal(snapshot)
    if current <= 0:
        return Decimal(0), current
    ctx = _get_trade_context(symbol)
    try:
        peak = Decimal(str(ctx.get("position_peak_qty") or "0"))
    except Exception:
        peak = Decimal(0)
    if current > peak:
        peak = current
        _update_trade_context(
            symbol,
            position_peak_qty=format(peak.normalize(), "f"),
        )
    return peak, current


def _be_meets_min_profit(snapshot: dict[str, Any]) -> bool:
    """Require minimum unrealized gain before signal-based BE (not used for partial-close BE)."""
    try:
        pnl_pct = snapshot.get("unrealized_pnl_pct")
        if pnl_pct is not None and float(pnl_pct) < BE_MIN_PROFIT_PCT:
            return False
    except (TypeError, ValueError):
        return False

    if BE_MIN_PROFIT_USDT > 0:
        try:
            pnl_usdt = snapshot.get("unrealized_pnl")
            if pnl_usdt is None or float(pnl_usdt) < BE_MIN_PROFIT_USDT:
                return False
        except (TypeError, ValueError):
            return False

    try:
        if snapshot.get("unrealized_pnl") is not None and float(snapshot["unrealized_pnl"]) <= 0:
            return False
    except (TypeError, ValueError):
        pass
    return True


def _be_signal_trigger_reason(
    direction: str,
    market_analysis: dict[str, Any] | None,
) -> str | None:
    """Why signal-based BE fired (any enabled rule). None if no rule matched."""
    if not market_analysis:
        return None

    direction = direction.upper()
    htf = str(market_analysis.get("htf_bias") or "NEUTRAL").upper()
    smc = market_analysis.get("smc") if isinstance(market_analysis.get("smc"), dict) else {}
    smc_trend = str(smc.get("trend") or "RANGING").upper()
    rsi = market_analysis.get("rsi")
    ema_cross = str(market_analysis.get("ema_cross") or "")
    hits: list[str] = []

    if direction == "LONG":
        if BE_ON_HTF_NEUTRAL and htf == "NEUTRAL":
            hits.append("htf_neutral")
        if BE_ON_HTF_AGAINST and htf == "BEARISH":
            hits.append("htf_bearish")
        if BE_ON_SMC_RANGING and smc_trend == "RANGING":
            hits.append("smc_ranging")
        if BE_ON_RSI_EXIT and rsi is not None:
            try:
                if float(rsi) < BE_RSI_LONG_EXIT_MAX:
                    hits.append(f"rsi_{float(rsi):.0f}")
            except (TypeError, ValueError):
                pass
        if BE_ON_EMA_FLIP and ema_cross == "Bear cross":
            hits.append("ema_bear_cross")
    elif direction == "SHORT":
        if BE_ON_HTF_NEUTRAL and htf == "NEUTRAL":
            hits.append("htf_neutral")
        if BE_ON_HTF_AGAINST and htf == "BULLISH":
            hits.append("htf_bullish")
        if BE_ON_SMC_RANGING and smc_trend == "RANGING":
            hits.append("smc_ranging")
        if BE_ON_RSI_EXIT and rsi is not None:
            try:
                if float(rsi) > BE_RSI_SHORT_EXIT_MIN:
                    hits.append(f"rsi_{float(rsi):.0f}")
            except (TypeError, ValueError):
                pass
        if BE_ON_EMA_FLIP and ema_cross == "Bull cross":
            hits.append("ema_bull_cross")

    return ",".join(hits) if hits else None


def _be_partial_close_trigger(
    symbol: str,
    snapshot: dict[str, Any],
) -> tuple[bool, float, float, float]:
    """True when ~TP1 partial size has been closed (legacy BE path)."""
    peak, current = _update_position_peak_qty(symbol, snapshot)
    if peak <= 0 or current <= 0:
        return False, 0.0, peak, current

    closed_pct = float((peak - current) / peak * 100)
    trigger_threshold = max(
        TRADE_PLAN_PARTIAL_CLOSE_PCT - PARTIAL_CLOSE_DETECT_TOLERANCE_PCT,
        50.0,
    )
    if closed_pct < trigger_threshold:
        return False, closed_pct, peak, current
    return True, closed_pct, peak, current


def _apply_breakeven_sl(
    symbol: str,
    snapshot: dict[str, Any],
    *,
    trigger: str,
    trigger_detail: str,
    closed_pct: float,
    peak: Decimal,
    current: Decimal,
) -> bool:
    symbol = symbol.upper()
    ctx = _get_trade_context(symbol)
    direction = _primary_position_direction(snapshot.get("direction")) or _primary_position_direction(
        ctx.get("direction")
    )
    if direction not in ("LONG", "SHORT"):
        return False

    entry_raw = snapshot.get("entry") or ctx.get("entry")
    try:
        entry = float(entry_raw) if entry_raw not in (None, "") else 0.0
    except (TypeError, ValueError):
        entry = 0.0
    if entry <= 0:
        logger.warning("%s: cannot apply BE — missing entry", symbol)
        return False

    pos_dir = direction
    qty = _position_qty_string(symbol, pos_dir)
    if not qty:
        return False

    _cancel_symbol_sl_orders(symbol, pos_dir)
    spread_abs = _fetch_book_spread(symbol)
    lock_pct = _resolve_lock_profit_pct(snapshot)
    be_raw = profit_protect_sl_price(
        direction,
        entry,
        lock_profit_pct=lock_pct,
        spread_abs=spread_abs,
    )
    be_sl = _ensure_sl_behind_mark(symbol, direction, be_raw)
    sl_price = round_price_for_profit_sl(symbol, direction, be_sl)
    placed, skipped = _place_sl_for_position(symbol, pos_dir, sl_price, qty, log_suffix="_be")
    if not placed:
        if skipped:
            logger.info("%s: BE SL skipped (mark through stop)", symbol)
        else:
            logger.error("%s: BE SL placement failed", symbol)
            telegram.notify_sl_tp_failed(symbol, pos_dir, "BE SL", "placement failed")
        return False

    pnl_pct = snapshot.get("unrealized_pnl_pct")
    _append_orders_log(
        "be_sl_applied",
        symbol=symbol,
        direction=pos_dir,
        entry=entry,
        sl=sl_price,
        qty=qty,
        trigger=trigger,
        trigger_detail=trigger_detail,
        closed_pct=round(closed_pct, 2),
        peak_qty=str(peak),
        unrealized_pnl_pct=pnl_pct,
        spread_abs=spread_abs,
        be_raw=be_raw,
        lock_profit_pct=round(lock_pct, 4),
    )

    _update_trade_context(
        symbol,
        be_applied=True,
        sl=sl_price,
        entry=round_price(symbol, entry),
        direction=pos_dir,
    )
    _invalidate_position_cache(symbol)
    runner_pct = max(0.0, float(current / peak * 100)) if peak > 0 else 100.0
    if telegram.is_configured():
        telegram.notify_be_sl_applied(
            symbol,
            pos_dir,
            sl_price,
            format_price_human(entry),
            runner_pct,
            closed_pct,
            trigger=trigger,
            trigger_detail=trigger_detail,
            profit_pct=pnl_pct,
            spread_abs=spread_abs,
            lock_profit_pct=lock_pct,
        )
    _set_status(
        message=f"Profit lock SL ({trigger}) · {symbol} {pos_dir} @ {sl_price} · lock {lock_pct:.2f}%",
        last_event="be_sl_applied",
    )
    return True


def _maybe_apply_breakeven_sl(
    symbol: str,
    snapshot: dict[str, Any],
    market_analysis: dict[str, Any] | None = None,
) -> bool:
    """
    Move SL to break-even when:
    - signal: HTF neutral/against, SMC ranging, RSI exit, etc. + min profit (BE_TRIGGER_SIGNAL)
    - partial: ~TRADE_PLAN_PARTIAL_CLOSE_PCT of qty closed after TP1 (BE_TRIGGER_PARTIAL)
    """
    if not TRADE_PLAN_AUTO_BE or not REST_PLACE_SL_TP:
        return False
    if not snapshot.get("open"):
        return False

    symbol = symbol.upper()
    ctx = _get_trade_context(symbol)
    if ctx.get("be_applied") in (True, "true", "1", 1):
        return False

    direction = _primary_position_direction(snapshot.get("direction")) or _primary_position_direction(
        ctx.get("direction")
    )
    if direction not in ("LONG", "SHORT"):
        return False

    _attach_unrealized_pnl_pct(snapshot)
    peak, current = _update_position_peak_qty(symbol, snapshot)
    closed_pct = float((peak - current) / peak * 100) if peak > 0 else 0.0

    if BE_TRIGGER_SIGNAL and market_analysis and _be_meets_min_profit(snapshot):
        signal_reason = _be_signal_trigger_reason(direction, market_analysis)
        if signal_reason:
            return _apply_breakeven_sl(
                symbol,
                snapshot,
                trigger="signal",
                trigger_detail=signal_reason,
                closed_pct=closed_pct,
                peak=peak,
                current=current,
            )

    if BE_TRIGGER_PARTIAL:
        partial_ok, partial_closed, peak, current = _be_partial_close_trigger(symbol, snapshot)
        if partial_ok:
            return _apply_breakeven_sl(
                symbol,
                snapshot,
                trigger="partial_close",
                trigger_detail=f"closed_{partial_closed:.0f}pct",
                closed_pct=partial_closed,
                peak=peak,
                current=current,
            )

    return False


def format_price_human(price: float) -> str:
    if price >= 1000:
        return f"{price:,.4f}"
    if price >= 1:
        return f"{price:.4f}"
    return f"{price:.6f}"


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


def _entry_opened_at_ms(ctx: dict[str, Any]) -> int | None:
    raw = ctx.get("entry_opened_at")
    if not raw:
        return None
    try:
        opened = time.strptime(str(raw).strip(), "%Y-%m-%d %H:%M:%S UTC")
        return int(calendar.timegm(opened) * 1000)
    except ValueError:
        return None


def _fetch_last_realized_trade(
    symbol: str,
    *,
    since_ms: int | None = None,
    max_age_sec: int | None = None,
    exclude_trade_id: Any = None,
) -> dict[str, Any] | None:
    if not _keys_configured():
        return None
    max_age_sec = EXIT_NOTIFY_MAX_AGE_SEC if max_age_sec is None else max_age_sec
    cutoff_ms = int(time.time() * 1000) - max_age_sec * 1000
    try:
        resp = _fapi_request("GET", "/fapi/v1/userTrades", {"symbol": symbol.upper(), "limit": 30})
    except RuntimeError as exc:
        logger.warning("userTrades failed for %s: %s", symbol, exc)
        return None
    if not isinstance(resp, list):
        return None
    for row in reversed(resp):
        try:
            trade_ms = int(row.get("time") or 0)
        except (TypeError, ValueError):
            trade_ms = 0
        if trade_ms <= 0 or trade_ms < cutoff_ms:
            continue
        if since_ms is not None and trade_ms < since_ms:
            continue
        trade_id = row.get("id")
        if exclude_trade_id is not None and trade_id is not None:
            if str(trade_id) == str(exclude_trade_id):
                continue
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
        return {
            "price": price,
            "qty": row.get("qty"),
            "realized_pnl": pnl,
            "trade_id": trade_id,
            "time_ms": trade_ms,
        }
    return None


def _is_breakeven_exit(entry: float | None, exit_price: float, pnl: float) -> bool:
    if entry is None or entry <= 0:
        return False
    price_ok = abs(exit_price - entry) / entry * 100 <= BE_EXIT_PRICE_PCT
    pnl_ok = abs(pnl) <= BE_EXIT_PNL_MAX_USDT
    return price_ok and pnl_ok


def _classify_closed_trade(
    entry: float | None,
    exit_price: float,
    pnl: float,
    tp_type: str,
) -> tuple[str, str]:
    if _is_breakeven_exit(entry, exit_price, pnl):
        return "breakeven", "breakeven"
    if pnl > 0:
        if tp_type == "trailing":
            return "trailing_tp", "win"
        return "tp", "win"
    if pnl < 0:
        return "sl", "loss"
    return "flat", "breakeven"


def _compute_pnl_pct(entry: float | None, exit_qty: Any, pnl: float) -> float | None:
    if entry is None or entry <= 0:
        return None
    try:
        qty = float(exit_qty or 0)
    except (TypeError, ValueError):
        qty = 0.0
    if qty <= 0:
        return None
    notional = entry * qty
    if notional <= 0:
        return None
    return round(pnl / notional * 100, 4)


def _persist_closed_trade_outcome(
    symbol: str,
    *,
    direction: str,
    entry: float | None,
    exit_price: float,
    exit_qty: Any,
    pnl: float,
    exit_type: str,
    outcome: str,
    sl: Any,
    tp: Any,
    tp_type: str,
    ctx: dict[str, Any],
) -> None:
    try:
        dca_legs = int(ctx.get("dca_legs_placed") or 0)
    except (TypeError, ValueError):
        dca_legs = 0
    entry_market, entry_event_id = _resolve_entry_link(symbol, ctx)
    db_store.log_trade_outcome(
        symbol,
        direction=direction,
        entry_price=entry,
        exit_price=exit_price,
        exit_qty=exit_qty,
        realized_pnl=pnl,
        pnl_pct=_compute_pnl_pct(entry, exit_qty, pnl),
        exit_type=exit_type,
        outcome=outcome,
        sl_price=sl,
        tp_price=tp,
        tp_type=tp_type,
        be_applied=bool(ctx.get("be_applied")),
        dca_legs_placed=dca_legs or None,
        entry_opened_at=ctx.get("entry_opened_at"),
        entry_market_snapshot=entry_market,
        entry_decision_event_id=entry_event_id,
        config_snapshot=symbol_config.get_config_snapshot(),
        trade_context=dict(ctx),
    )


def _maybe_notify_position_exit(symbol: str, snapshot: dict[str, Any]) -> None:
    if not _keys_configured():
        return

    symbol = symbol.upper()
    is_active = bool(snapshot.get("open") or snapshot.get("pending"))
    ctx = _get_trade_context(symbol)

    if snapshot.get("open"):
        open_fields: dict[str, Any] = {
            "was_open": True,
            "direction": snapshot.get("direction") or ctx.get("direction"),
            "exit_notified": False,
            "last_exit_trade_id": None,
        }
        if not ctx.get("entry_opened_at"):
            open_fields["entry_opened_at"] = time.strftime(
                "%Y-%m-%d %H:%M:%S UTC",
                time.gmtime(),
            )
        _update_trade_context(symbol, **open_fields)
        _bind_entry_snapshot_for_open_position(symbol)
        return

    if is_active or not ctx.get("was_open") or ctx.get("exit_notified"):
        return

    direction = str(ctx.get("direction") or "LONG")
    sl = ctx.get("sl")
    tp = ctx.get("tp")
    tp_type = str(ctx.get("tp_type") or TP_ORDER_TYPE).lower()
    entry_raw = ctx.get("entry")
    entry = float(entry_raw) if entry_raw not in (None, "") else None

    trade = _fetch_last_realized_trade(
        symbol,
        since_ms=_entry_opened_at_ms(ctx),
        exclude_trade_id=ctx.get("last_exit_trade_id"),
    )
    if not trade:
        logger.info(
            "%s: position flat but no recent realized fill (was_open stale or already notified)",
            symbol,
        )
        _update_trade_context(
            symbol,
            was_open=False,
            exit_notified=True,
            dca_legs_placed=0,
            dca_max_legs=0,
            be_applied=False,
            position_peak_qty=None,
        )
        return

    exit_price = float(trade["price"])
    pnl = float(trade["realized_pnl"])
    exit_qty = trade.get("qty")
    pnl_label = _format_realized_pnl(pnl)
    exit_type, outcome = _classify_closed_trade(entry, exit_price, pnl, tp_type)

    _persist_closed_trade_outcome(
        symbol,
        direction=direction,
        entry=entry,
        exit_price=exit_price,
        exit_qty=exit_qty,
        pnl=pnl,
        exit_type=exit_type,
        outcome=outcome,
        sl=sl,
        tp=tp,
        tp_type=tp_type,
        ctx=ctx,
    )

    _update_trade_context(
        symbol,
        was_open=False,
        exit_notified=True,
        last_exit_trade_id=trade.get("trade_id"),
        dca_legs_placed=0,
        dca_max_legs=0,
        be_applied=False,
        position_peak_qty=None,
        pending_entry_market_snapshot=None,
        pending_entry_at=None,
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
        exit_type=exit_type,
        outcome=outcome,
    )

    if not telegram.is_configured():
        return

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


def _notify_order_failed(symbol: str, direction: str, exc: Exception) -> None:
    telegram.notify_order_failed(symbol, direction, str(exc))


def _reserve_execution_attempt(symbol: str) -> bool:
    """One live order attempt at a time; cooldown starts when attempt is reserved."""
    global _last_execution_monotonic, _execution_inflight_symbol

    symbol = symbol.upper()
    now = time.monotonic()
    with _execution_inflight_lock:
        if _execution_inflight_symbol:
            return False
        if (
            _last_execution_monotonic is not None
            and now - _last_execution_monotonic < EXECUTION_ORDER_COOLDOWN
        ):
            return False
        _execution_inflight_symbol = symbol
        _last_execution_monotonic = now
    return True


def _release_execution_attempt(symbol: str) -> None:
    global _execution_inflight_symbol

    symbol = symbol.upper()
    with _execution_inflight_lock:
        if _execution_inflight_symbol == symbol:
            _execution_inflight_symbol = None


def _run_execution_thread(target, *args, **kwargs) -> None:
    symbol = (args[0] if args else kwargs.get("symbol", "")).upper()
    thread_name = kwargs.pop("thread_name", f"exec-{symbol}")

    if not _reserve_execution_attempt(symbol):
        _append_orders_log(
            "skip_inflight_or_cooldown",
            symbol=symbol,
            signal=kwargs.get("direction") or (args[1] if len(args) > 1 else None),
        )
        _log_execution_decision(
            symbol,
            "order_skip",
            block_reason="execution_inflight_or_cooldown",
            market_snapshot={"signal": kwargs.get("direction") or (args[1] if len(args) > 1 else None)},
        )
        return

    def runner() -> None:
        try:
            target(*args, **kwargs)
        finally:
            _release_execution_attempt(symbol)

    thread = threading.Thread(target=runner, daemon=True, name=thread_name)
    thread.start()


def get_execution_status() -> dict[str, Any]:
    with _status_lock:
        status = dict(_execution_status)
    status["execute_on_valid_entry"] = EXECUTE_ON_VALID_ENTRY
    status["auto_execute"] = EXECUTION_ENABLED and EXECUTE_ON_VALID_ENTRY
    if status.get("message") in (None, "", "Execution disabled", "Ready (dry)", "Ready (live)"):
        status["message"] = _execution_status_message()
    return status


def _optional_env_float(name: str) -> float | None:
    raw = os.getenv(name, "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid %s=%r — ignored", name, raw)
        return None


def get_account_wallet_snapshot(force: bool = False) -> dict[str, Any]:
    """Cached Binance Futures wallet totals (shared across all pairs)."""
    global _account_snapshot_cache
    if not _keys_configured():
        return {
            "configured": False,
            "wallet_usdt": None,
            "available_usdt": None,
            "unrealized_usdt": None,
            "margin_balance_usdt": None,
        }

    now = time.monotonic()
    with _account_snapshot_lock:
        if (
            not force
            and _account_snapshot_cache
            and now - _account_snapshot_cache[0] < ACCOUNT_SNAPSHOT_CACHE_SEC
        ):
            return dict(_account_snapshot_cache[1])

    snapshot = {
        "configured": True,
        "wallet_usdt": None,
        "available_usdt": None,
        "unrealized_usdt": None,
        "margin_balance_usdt": None,
    }
    try:
        account = _fapi_request("GET", "/fapi/v2/account", {})
        snapshot["wallet_usdt"] = float(account.get("totalWalletBalance", 0) or 0)
        snapshot["available_usdt"] = float(account.get("availableBalance", 0) or 0)
        snapshot["unrealized_usdt"] = float(account.get("totalUnrealizedProfit", 0) or 0)
        snapshot["margin_balance_usdt"] = float(account.get("totalMarginBalance", 0) or 0)
    except RuntimeError as exc:
        logger.warning("Account snapshot failed: %s", exc)

    with _account_snapshot_lock:
        _account_snapshot_cache = (now, snapshot)
    return dict(snapshot)


def _realized_pnl_from_orders_log(days: int, symbol: str | None = None) -> dict[str, Any]:
    """Fallback when DB is off: sum position_closed events from orders.log."""
    path = os.path.join(LOG_DIR, "orders.log")
    if not os.path.isfile(path):
        return {
            "closed_trades": 0,
            "total_realized_pnl": 0.0,
            "daily_avg_usdt": None,
            "source": "none",
        }

    cutoff = time.time() - days * 86400
    total = 0.0
    closed = 0
    sym = symbol.upper() if symbol else None
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if record.get("event") != "position_closed":
                    continue
                if sym and str(record.get("symbol", "")).upper() != sym:
                    continue
                ts_raw = str(record.get("ts") or "")
                try:
                    ts_struct = time.strptime(ts_raw.replace(" UTC", ""), "%Y-%m-%d %H:%M:%S")
                    ts_epoch = time.mktime(ts_struct)
                except ValueError:
                    continue
                if ts_epoch < cutoff:
                    continue
                pnl = record.get("realized_pnl")
                if pnl is None:
                    continue
                total += float(pnl)
                closed += 1
    except OSError:
        logger.exception("Failed reading orders.log for PnL stats")

    return {
        "closed_trades": closed,
        "total_realized_pnl": total,
        "daily_avg_usdt": total / days if closed else None,
        "source": "orders_log" if closed else "none",
    }


def _resolve_realized_pnl_stats(days: int, symbol: str | None = None) -> dict[str, Any]:
    try:
        import db_store
        import db_analytics

        if db_store.is_enabled():
            return db_analytics.get_realized_pnl_daily_stats(days, symbol=symbol)
    except Exception:
        logger.exception("DB realized PnL stats failed")

    return _realized_pnl_from_orders_log(days, symbol=symbol)


def _account_baseline_path() -> str:
    return os.path.join(LOG_DIR, "account_baseline.json")


def _load_account_baseline_file() -> dict[str, Any]:
    path = _account_baseline_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        logger.warning("Could not read account_baseline.json: %s", exc)
        return {}


def _save_account_baseline_file(payload: dict[str, Any]) -> None:
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(_account_baseline_path(), "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _resolve_account_baseline(
    wallet_usdt: float | None,
    unrealized_usdt: float | None,
) -> tuple[float | None, str, str | None]:
    """Baseline for account ROI. Env override, else persisted file, else auto on first snapshot."""
    env_baseline = _optional_env_float("ACCOUNT_BASELINE_USDT")
    if env_baseline is not None and env_baseline > 0:
        return env_baseline, "env", None

    if wallet_usdt is None or wallet_usdt <= 0:
        return None, "none", None

    with _baseline_lock:
        stored = _load_account_baseline_file()
        stored_baseline = stored.get("baseline_usdt")
        if stored_baseline is not None:
            try:
                baseline = float(stored_baseline)
            except (TypeError, ValueError):
                baseline = 0.0
            if baseline > 0:
                return baseline, str(stored.get("source") or "file"), stored.get("set_at")

        all_time = _resolve_realized_pnl_stats(3650, symbol=None)
        realized = float(all_time.get("total_realized_pnl") or 0)
        unrealized = float(unrealized_usdt or 0)
        if realized != 0 or unrealized != 0:
            estimated = wallet_usdt - realized - unrealized
            if estimated > 0:
                baseline = estimated
                source = "auto_estimated"
            else:
                baseline = wallet_usdt
                source = "auto_first_snapshot"
        else:
            baseline = wallet_usdt
            source = "auto_first_snapshot"

        set_at = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        try:
            _save_account_baseline_file(
                {
                    "baseline_usdt": baseline,
                    "set_at": set_at,
                    "source": source,
                    "wallet_usdt_at_set": wallet_usdt,
                }
            )
        except OSError:
            logger.exception("Failed writing account_baseline.json")

        return baseline, source, set_at


def get_performance_snapshot(
    symbol: str | None = None,
    *,
    days: int | None = None,
) -> dict[str, Any]:
    """Wallet, realized PnL averages, ROI baseline, and projection to MILLION_GOAL_USDT."""
    wallet = get_account_wallet_snapshot()
    lookback = PNL_STATS_LOOKBACK_DAYS if days is None else max(1, int(days))
    account_stats = _resolve_realized_pnl_stats(lookback, symbol=None)
    pair_stats = _resolve_realized_pnl_stats(lookback, symbol=symbol) if symbol else None

    wallet_usdt = wallet.get("wallet_usdt")
    daily_avg = account_stats.get("daily_avg_usdt")
    goal = MILLION_GOAL_USDT
    remaining = None
    days_to_goal = None
    if wallet_usdt is not None:
        remaining = max(0.0, goal - wallet_usdt)
        if daily_avg and daily_avg > 0 and remaining > 0:
            days_to_goal = remaining / daily_avg
        elif remaining <= 0:
            days_to_goal = 0.0

    baseline, baseline_source, baseline_set_at = _resolve_account_baseline(
        wallet_usdt,
        wallet.get("unrealized_usdt"),
    )
    account_roi_pct = None
    if baseline and baseline > 0 and wallet_usdt is not None:
        account_roi_pct = (wallet_usdt - baseline) / baseline * 100.0

    return {
        **wallet,
        "lookback_days": lookback,
        "million_goal_usdt": goal,
        "million_remaining_usdt": remaining,
        "million_days_at_avg": days_to_goal,
        "account_roi_pct": account_roi_pct,
        "account_baseline_usdt": baseline,
        "account_baseline_source": baseline_source,
        "account_baseline_set_at": baseline_set_at,
        "realized_total_usdt": account_stats.get("total_realized_pnl"),
        "daily_avg_usdt": daily_avg,
        "closed_trades": account_stats.get("closed_trades", 0),
        "stats_source": account_stats.get("source", "none"),
        "pair_realized_total_usdt": pair_stats.get("total_realized_pnl") if pair_stats else None,
        "pair_daily_avg_usdt": pair_stats.get("daily_avg_usdt") if pair_stats else None,
        "pair_closed_trades": pair_stats.get("closed_trades", 0) if pair_stats else 0,
    }


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

    sl_price = round_price_for_sl(symbol, direction, sl)
    tp_price = round_price(symbol, tp)

    if need_sl:
        sl_adj = _ensure_sl_behind_mark(symbol, direction, sl)
        sl_price = round_price_for_sl(symbol, direction, sl_adj)
        placed, skipped = _place_sl_for_position(
            symbol,
            direction,
            sl_price,
            qty,
            log_suffix="_reconciled",
        )
        if placed:
            result["sl"] = True
        elif skipped:
            result["sl"] = True
        else:
            logger.error("SL reconcile failed for %s", symbol)
            _append_orders_log("sl_reconcile_failed", symbol=symbol, error="placement_failed")
            telegram.notify_sl_tp_failed(symbol, direction, "SL (reconcile)", "placement failed")

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

    position_entry = snapshot.get("entry")
    try:
        entry_f = float(position_entry) if position_entry not in (None, "") else None
    except (TypeError, ValueError):
        entry_f = None

    sl_changed = False
    if entry_f is not None and entry_f > 0:
        if ctx.get("be_applied") in (True, "true", "1", 1):
            _attach_unrealized_pnl_pct(snapshot)
            lock_pct = _resolve_lock_profit_pct(snapshot)
            spread_abs = _fetch_book_spread(symbol)
            be_sl = profit_protect_sl_price(
                pos_dir,
                entry_f,
                lock_profit_pct=lock_pct,
                spread_abs=spread_abs,
            )
            if sl_val is None or abs(float(sl_val or 0) - be_sl) > 1e-12:
                sl_val = be_sl
                sl_changed = True
        else:
            sl_val, sl_changed = resolve_sl_for_open_position(
                pos_dir,
                entry_f,
                float(sl_val) if sl_val is not None and sl_val > 0 else None,
            )

    tp_val, tp_recalc = _resolve_tp_for_open_position(
        symbol,
        pos_dir,
        position_entry=entry_f,
        sl_val=float(sl_val) if sl_val is not None and sl_val > 0 else None,
        tp_val=float(tp_val) if tp_val is not None and tp_val > 0 else None,
    )
    if entry_f is not None and (tp_recalc or sl_changed) and tp_val is not None and sl_val is not None:
        _persist_recalculated_protection(
            symbol,
            pos_dir,
            position_entry=entry_f,
            sl_val=float(sl_val),
            tp_val=float(tp_val),
            reason="protection_recalculated",
        )

    if sl_val is not None and sl_val > 0:
        sl_val = _ensure_sl_behind_mark(symbol, pos_dir, float(sl_val))

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

    if (
        not need_tp
        and tp_val is not None
        and tp_val > 0
        and _tp_order_needs_refresh(symbol, pos_dir, float(tp_val))
    ):
        _cancel_symbol_tp_orders(symbol, pos_dir)
        _invalidate_position_cache(symbol)
        protection = get_position_protection(symbol, pos_dir)
        summary.update(protection)
        need_tp = True
        summary["tp_repriced"] = True

    if (
        not need_sl
        and sl_val is not None
        and sl_val > 0
        and entry_f is not None
        and not ctx.get("be_applied")
        and _sl_order_needs_refresh(symbol, pos_dir, float(sl_val), entry_f)
    ):
        _cancel_symbol_sl_orders(symbol, pos_dir)
        _invalidate_position_cache(symbol)
        protection = get_position_protection(symbol, pos_dir)
        summary.update(protection)
        need_sl = True
        summary["sl_repriced"] = True

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
    market_analysis: dict[str, Any] | None = None,
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
    snapshot = _fetch_exchange_exposure(symbol)
    if snapshot.get("open"):
        _maybe_apply_breakeven_sl(symbol, snapshot, market_analysis)
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
    global _last_position_api_error
    if not _keys_configured():
        _last_position_api_error = "no_api_keys"
        return []
    try:
        resp = _fapi_request("GET", "/fapi/v2/positionRisk", {})
        _last_position_api_error = None
        return resp if isinstance(resp, list) else []
    except RuntimeError as exc:
        _last_position_api_error = str(exc)
        logger.warning("positionRisk (all symbols) failed: %s", exc)
        return []


def keys_configured() -> bool:
    return _keys_configured()


def format_protection_display(pos: dict[str, Any]) -> str:
    if pos.get("open"):
        sl_mark = "✓" if pos.get("has_sl") else "✗"
        if pos.get("tp_kind") == "trailing":
            if pos.get("trailing_active"):
                tp_part = "TP ✓ trail"
            elif pos.get("trailing_pending") or pos.get("has_tp"):
                tp_part = "TP ○ trail"
            else:
                tp_part = "TP ✗"
        elif pos.get("has_tp"):
            tp_part = "TP ✓"
        else:
            tp_part = "TP ✗"
        return f"SL {sl_mark} · {tp_part}"
    if pos.get("pending"):
        ttl = ENTRY_LIMIT_TTL_SEC
        if ttl > 0:
            return f"Pending (TTL {ttl}s)"
        return "Pending"
    return "—"


def get_fleet_open_positions_map(*, force: bool = False) -> dict[str, Any]:
    """All open futures positions in one Binance call (for hub fleet overlay)."""
    global _fleet_positions_cache

    payload: dict[str, Any] = {
        "keys_configured": _keys_configured(),
        "api_error": _last_position_api_error,
        "positions": {},
    }
    if not _keys_configured():
        payload["api_error"] = "no_api_keys"
        return payload

    now = time.monotonic()
    with _fleet_positions_lock:
        if (
            not force
            and _fleet_positions_cache is not None
            and now - _fleet_positions_cache[0] < EXECUTION_POSITION_CACHE_SEC
        ):
            return dict(_fleet_positions_cache[1])

    positions: dict[str, dict[str, Any]] = {}
    for row in _get_all_position_risk():
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        parsed = _parse_position_row(row)
        if not parsed:
            continue
        snap = dict(parsed)
        _attach_unrealized_pnl_pct(snap)
        snap["source"] = "binance"
        primary = _primary_position_direction(snap.get("direction"))
        if primary:
            snap.update(get_position_protection(symbol, primary))
        snap["protection_display"] = format_protection_display(snap)
        positions[symbol] = snap

    payload["api_error"] = _last_position_api_error
    payload["positions"] = positions
    with _fleet_positions_lock:
        _fleet_positions_cache = (now, dict(payload))
    return dict(payload)


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
        "keys_configured": _keys_configured(),
        "api_error": _last_position_api_error,
        "long_positions_usdt": 0.0,
        "short_positions_usdt": 0.0,
        "long_pending_usdt": 0.0,
        "short_pending_usdt": 0.0,
        "long_total_usdt": 0.0,
        "short_total_usdt": 0.0,
        "imbalance_pct": 0.0,
    }
    if not _keys_configured():
        empty["api_error"] = "no_api_keys"
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
        "keys_configured": True,
        "api_error": _last_position_api_error,
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


def _position_adverse_for_dca(
    symbol: str,
    direction: str,
    add_entry: float,
) -> tuple[bool, str]:
    """
    DCA adds only when the open position is against us (underwater / averaging into loss).
    SHORT: add only above position entry; LONG: add only below position entry.
    """
    symbol = symbol.upper()
    direction = direction.upper()
    if add_entry <= 0:
        return False, "invalid_entry"

    snapshot = _fetch_exchange_exposure(symbol)
    if not snapshot.get("open"):
        return False, "dca_no_position"

    pos_entry_raw = snapshot.get("entry")
    try:
        pos_entry = float(pos_entry_raw) if pos_entry_raw not in (None, "") else None
    except (TypeError, ValueError):
        pos_entry = None

    mark_raw = snapshot.get("mark_price")
    try:
        mark = float(mark_raw) if mark_raw not in (None, "") else None
    except (TypeError, ValueError):
        mark = None

    upnl_raw = snapshot.get("unrealized_pnl")
    if upnl_raw is not None:
        try:
            upnl = float(upnl_raw)
            if upnl > DCA_FAVORABLE_PNL_MAX_USDT:
                return False, "dca_favorable_pnl"
        except (TypeError, ValueError):
            pass

    if pos_entry is not None and pos_entry > 0:
        if direction == "LONG" and add_entry >= pos_entry:
            return False, "dca_add_with_trend"
        if direction == "SHORT" and add_entry <= pos_entry:
            return False, "dca_add_with_trend"

    if mark is not None and mark > 0 and pos_entry is not None and pos_entry > 0:
        if direction == "LONG" and mark >= pos_entry:
            return False, "dca_position_favorable"
        if direction == "SHORT" and mark <= pos_entry:
            return False, "dca_position_favorable"

    return True, ""


def resolve_dca_leg_entry_price(
    leg_index: int,
    leg: dict[str, Any],
    signal_entry: float,
) -> float:
    """Leg 0 = signal entry; DCA2/DCA3+ use planned adverse levels from the trade plan."""
    if leg_index <= 0:
        return signal_entry
    try:
        plan_price = float(leg.get("price") or 0)
    except (TypeError, ValueError):
        plan_price = 0.0
    return plan_price if plan_price > 0 else signal_entry


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

    if TRADE_PLAN_DCA_ADVERSE_ONLY:
        adverse, adverse_reason = _position_adverse_for_dca(symbol, direction, entry)
        if not adverse:
            return False, adverse_reason or "dca_favorable"

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
        "dca_favorable_pnl": "skip_dca_favorable",
        "dca_add_with_trend": "skip_dca_favorable",
        "dca_position_favorable": "skip_dca_favorable",
        "dca_favorable": "skip_dca_favorable",
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


def round_price_for_sl(symbol: str, direction: str, value: float) -> str:
    """Round SL so it does not move closer to entry (SHORT → up, LONG → down)."""
    filt = _load_symbol_filters(symbol)
    tick = filt["tick_size"]
    rounding = ROUND_UP if direction.upper() == "SHORT" else ROUND_DOWN
    quantized = Decimal(str(value)).quantize(tick, rounding=rounding)
    return format(quantized, "f")


def _get_mark_price(symbol: str) -> float | None:
    snapshot = _fetch_exchange_exposure(symbol.upper())
    mark = snapshot.get("mark_price")
    if mark is not None and float(mark) > 0:
        return float(mark)
    return None


def _sl_distance_pct(direction: str, entry: float, sl: float) -> float:
    direction = direction.upper()
    if entry <= 0:
        return 0.0
    if direction == "LONG":
        return max(0.0, (entry - sl) / entry * 100)
    if direction == "SHORT":
        return max(0.0, (sl - entry) / entry * 100)
    return 0.0


def _sl_too_close_to_entry(direction: str, entry: float, sl: float) -> bool:
    if entry <= 0 or sl <= 0:
        return True
    return _sl_distance_pct(direction, entry, sl) < TRADE_PLAN_SL_MIN_DISTANCE_PCT


def resolve_sl_for_open_position(
    direction: str,
    entry: float,
    sl_val: float | None,
) -> tuple[float | None, bool]:
    """Keep structural SL from plan but never closer than TRADE_PLAN_SL_MIN_DISTANCE_PCT to entry."""
    if entry <= 0:
        return sl_val, False
    direction = direction.upper()
    min_pct = TRADE_PLAN_SL_MIN_DISTANCE_PCT / 100
    plan_sl = float(sl_val) if sl_val is not None and sl_val > 0 else None
    if direction == "LONG":
        min_dist_sl = entry * (1 - min_pct)
        if plan_sl is None:
            resolved = min_dist_sl
        else:
            resolved = min(plan_sl, min_dist_sl)
    elif direction == "SHORT":
        min_dist_sl = entry * (1 + min_pct)
        if plan_sl is None:
            resolved = min_dist_sl
        else:
            resolved = max(plan_sl, min_dist_sl)
    else:
        return sl_val, False
    changed = plan_sl is None or abs(resolved - plan_sl) > 1e-12
    return resolved, changed


def recalculate_tp1_from_position(direction: str, entry: float, sl: float) -> float | None:
    """TP1 from actual position entry and plan SL using TRADE_PLAN_TP1_RR."""
    direction = direction.upper()
    try:
        entry_f = float(entry)
        sl_f = float(sl)
    except (TypeError, ValueError):
        return None
    if entry_f <= 0:
        return None
    if direction == "LONG":
        risk = entry_f - sl_f
        if risk <= 0:
            return None
        return entry_f + risk * TRADE_PLAN_TP1_RR
    if direction == "SHORT":
        risk = sl_f - entry_f
        if risk <= 0:
            return None
        return entry_f - risk * TRADE_PLAN_TP1_RR
    return None


def _ensure_tp_ahead_of_mark(symbol: str, direction: str, tp: float) -> float:
    """Nudge TP so Binance will accept it (mark not already through the target)."""
    mark = _get_mark_price(symbol)
    if mark is None or mark <= 0:
        return tp
    filt = _load_symbol_filters(symbol)
    tick = float(filt["tick_size"])
    cushion = tick * 2
    direction = direction.upper()
    if direction == "LONG" and mark >= tp:
        return mark + cushion
    if direction == "SHORT" and mark <= tp:
        return max(mark - cushion, tick)
    return tp


def _resolve_tp_for_open_position(
    symbol: str,
    direction: str,
    *,
    position_entry: float | None,
    sl_val: float | None,
    tp_val: float | None,
) -> tuple[float | None, bool]:
    """Return (tp price, True if recalculated from position entry)."""
    if position_entry is None or position_entry <= 0:
        return tp_val, False
    if sl_val is None or sl_val <= 0:
        return tp_val, False
    recalc = recalculate_tp1_from_position(direction, position_entry, sl_val)
    if recalc is None or recalc <= 0:
        return tp_val, False
    adjusted = _ensure_tp_ahead_of_mark(symbol, direction, recalc)
    changed = tp_val is None or abs(adjusted - float(tp_val)) > 1e-12
    return adjusted, changed


def _tp_order_needs_refresh(symbol: str, direction: str, target_tp: float) -> bool:
    """True when open TP algo is stale vs target or would trigger immediately."""
    primary = _primary_position_direction(direction)
    if not primary or target_tp <= 0:
        return False
    mark = _get_mark_price(symbol)
    want_ps = primary if is_hedge_mode() else None
    found_tp = False
    for order in _get_open_algo_orders(symbol):
        if want_ps:
            pos_side = str(order.get("positionSide") or "").upper()
            if pos_side and pos_side not in (want_ps, "BOTH"):
                continue
        if _algo_order_role(order, primary) != "tp":
            continue
        found_tp = True
        raw_trigger = order.get("activatePrice") or order.get("triggerPrice")
        try:
            trigger = float(raw_trigger or 0)
        except (TypeError, ValueError):
            trigger = 0.0
        if trigger > 0 and mark is not None and _tp_would_trigger_immediately(primary, trigger, mark):
            return True
        if trigger > 0 and abs(trigger - target_tp) / target_tp * 100 > TP_REPRICE_TOLERANCE_PCT:
            return True
    return False


def _get_sl_trigger_from_orders(symbol: str, direction: str) -> float | None:
    primary = _primary_position_direction(direction)
    if not primary:
        return None
    want_ps = primary if is_hedge_mode() else None
    for order in _get_open_algo_orders(symbol):
        if want_ps:
            pos_side = str(order.get("positionSide") or "").upper()
            if pos_side and pos_side not in (want_ps, "BOTH"):
                continue
        if _algo_order_role(order, primary) != "sl":
            continue
        try:
            trigger = float(order.get("triggerPrice") or 0)
        except (TypeError, ValueError):
            trigger = 0.0
        if trigger > 0:
            return trigger
    return None


def _sl_order_needs_refresh(
    symbol: str,
    direction: str,
    target_sl: float,
    position_entry: float,
) -> bool:
    if target_sl <= 0 or position_entry <= 0:
        return False
    if _sl_too_close_to_entry(direction, position_entry, target_sl):
        return True
    trigger = _get_sl_trigger_from_orders(symbol, direction)
    if trigger is None or trigger <= 0:
        return False
    mark = _get_mark_price(symbol)
    if mark is not None and _sl_would_trigger_immediately(direction, trigger, mark):
        return True
    if _sl_too_close_to_entry(direction, position_entry, trigger):
        return True
    if abs(trigger - target_sl) / target_sl * 100 > SL_REPRICE_TOLERANCE_PCT:
        return True
    return False


def _cancel_symbol_sl_orders(symbol: str, direction: str) -> None:
    primary = _primary_position_direction(direction)
    if not primary:
        return
    want_ps = primary if is_hedge_mode() else None
    for order in _get_open_algo_orders(symbol):
        if want_ps:
            pos_side = str(order.get("positionSide") or "").upper()
            if pos_side and pos_side not in (want_ps, "BOTH"):
                continue
        if _algo_order_role(order, primary) != "sl":
            continue
        algo_id = order.get("algoId")
        if algo_id is None:
            continue
        try:
            _cancel_algo_order(symbol, int(algo_id))
            _append_orders_log(
                "sl_cancelled_reprice",
                symbol=symbol,
                algoId=algo_id,
                direction=primary,
            )
        except RuntimeError as exc:
            logger.warning("%s: cancel SL algo %s failed: %s", symbol, algo_id, exc)


def _cancel_symbol_tp_orders(symbol: str, direction: str) -> None:
    primary = _primary_position_direction(direction)
    if not primary:
        return
    want_ps = primary if is_hedge_mode() else None
    for order in _get_open_algo_orders(symbol):
        if want_ps:
            pos_side = str(order.get("positionSide") or "").upper()
            if pos_side and pos_side not in (want_ps, "BOTH"):
                continue
        if _algo_order_role(order, primary) != "tp":
            continue
        algo_id = order.get("algoId")
        if algo_id is None:
            continue
        try:
            _cancel_algo_order(symbol, int(algo_id))
            _append_orders_log(
                "tp_cancelled_reprice",
                symbol=symbol,
                algoId=algo_id,
                direction=primary,
            )
        except RuntimeError as exc:
            logger.warning("%s: cancel TP algo %s failed: %s", symbol, algo_id, exc)


def _persist_recalculated_protection(
    symbol: str,
    direction: str,
    *,
    position_entry: float,
    sl_val: float,
    tp_val: float,
    reason: str = "protection_recalculated",
) -> None:
    direction = direction.upper()
    _update_trade_context(
        symbol,
        direction=direction,
        entry=round_price(symbol, position_entry),
        sl=round_price_for_sl(symbol, direction, sl_val),
        tp=round_price(symbol, tp_val),
    )
    _append_orders_log(
        reason,
        symbol=symbol,
        direction=direction,
        entry=position_entry,
        sl=sl_val,
        tp=tp_val,
        rr=TRADE_PLAN_TP1_RR,
        sl_min_distance_pct=TRADE_PLAN_SL_MIN_DISTANCE_PCT,
    )


def _persist_recalculated_tp(
    symbol: str,
    direction: str,
    *,
    position_entry: float,
    sl_val: float,
    tp_val: float,
) -> None:
    _persist_recalculated_protection(
        symbol,
        direction,
        position_entry=position_entry,
        sl_val=sl_val,
        tp_val=tp_val,
        reason="tp_recalculated",
    )


def _sl_would_trigger_immediately(direction: str, sl: float, mark: float) -> bool:
    """True when SL trigger would fire at current mark (Binance -2021)."""
    direction = direction.upper()
    if direction == "LONG":
        return mark <= sl
    return mark >= sl


def _ensure_sl_behind_mark(symbol: str, direction: str, sl: float) -> float:
    """Nudge SL so Binance accepts it (mark not already through the stop)."""
    mark = _get_mark_price(symbol)
    if mark is None or mark <= 0:
        return sl
    filt = _load_symbol_filters(symbol)
    tick = float(filt["tick_size"])
    cushion = tick * 2
    direction = direction.upper()
    if direction == "LONG" and _sl_would_trigger_immediately(direction, sl, mark):
        return max(mark - cushion, tick)
    if direction == "SHORT" and _sl_would_trigger_immediately(direction, sl, mark):
        return mark + cushion
    return sl


def _place_sl_for_position(
    symbol: str,
    direction: str,
    sl_price: str,
    qty: str,
    *,
    log_suffix: str = "",
) -> tuple[bool, bool]:
    """Place SL if valid vs mark. Returns (placed, skipped_immediate)."""
    symbol = symbol.upper()
    direction = direction.upper()
    try:
        sl_val = float(sl_price)
    except (TypeError, ValueError):
        sl_val = 0.0

    mark = _get_mark_price(symbol)
    sl_use = sl_val
    if mark is not None and sl_val > 0 and _sl_would_trigger_immediately(direction, sl_val, mark):
        original = sl_val
        sl_use = _ensure_sl_behind_mark(symbol, direction, sl_val)
        if abs(sl_use - original) > 1e-12:
            sl_price = round_price_for_sl(symbol, direction, sl_use)
            sl_val = float(sl_price)
            _append_orders_log(
                "sl_repriced_for_mark",
                symbol=symbol,
                direction=direction,
                original=original,
                sl=sl_price,
                mark=mark,
                context=log_suffix or "protection",
            )

    if mark is not None and sl_val > 0 and _sl_would_trigger_immediately(direction, sl_val, mark):
        _append_orders_log(
            "sl_skip_immediate",
            symbol=symbol,
            direction=direction,
            sl=sl_price,
            mark=mark,
            context=log_suffix or "protection",
        )
        logger.info(
            "%s: skip SL%s — mark %.8g already at/through stop %.8g (%s)",
            symbol,
            f" ({log_suffix})" if log_suffix else "",
            mark,
            sl_val,
            direction,
        )
        return False, True

    try:
        sl_resp = _place_stop_loss(symbol, direction, sl_price, qty)
        event = f"sl{log_suffix}" if log_suffix else "sl_placed"
        _append_orders_log(event, symbol=symbol, response=sl_resp, sl=sl_price, mark=mark)
        return True, False
    except RuntimeError as exc:
        if _is_immediate_trigger_error(exc):
            retry_sl = round_price_for_sl(
                symbol,
                direction,
                _ensure_sl_behind_mark(symbol, direction, sl_val),
            )
            if retry_sl != sl_price:
                try:
                    mark = _get_mark_price(symbol)
                    retry_val = float(retry_sl)
                    if mark is None or not _sl_would_trigger_immediately(direction, retry_val, mark):
                        sl_resp = _place_stop_loss(symbol, direction, retry_sl, qty)
                        _append_orders_log(
                            f"sl{log_suffix or '_placed'}",
                            symbol=symbol,
                            response=sl_resp,
                            sl=retry_sl,
                            mark=mark,
                            repriced=True,
                        )
                        return True, False
                except RuntimeError as retry_exc:
                    if not _is_immediate_trigger_error(retry_exc):
                        raise
            _append_orders_log(
                "sl_skip_immediate",
                symbol=symbol,
                direction=direction,
                sl=sl_price,
                mark=mark,
                error=str(exc),
                context=log_suffix or "protection",
            )
            logger.info("%s: SL skipped (immediate trigger): %s", symbol, exc)
            return False, True
        raise


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


def _use_market_for_first_leg(leg_index: int) -> bool:
    return ENTRY_FIRST_LEG_MARKET and leg_index == 0


def _entry_price_for_quantity(symbol: str, planned_entry: float, *, use_market: bool) -> float:
    if not use_market:
        return planned_entry
    mark = _get_mark_price(symbol)
    if mark is not None and mark > 0:
        return float(mark)
    return planned_entry


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


def _place_market_entry(
    symbol: str,
    direction: str,
    quantity: str,
    *,
    leg_index: int | None = None,
) -> dict[str, Any]:
    side = "BUY" if direction == "LONG" else "SELL"
    client_id = f"mkt_{int(time.time())}"[:36]
    if leg_index is not None:
        client_id = f"mk{leg_index}_{int(time.time())}"[:36]
    params = _apply_position_params(
        {
            "symbol": symbol.upper(),
            "side": side,
            "type": "MARKET",
            "quantity": quantity,
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

    sl_price = round_price_for_sl(symbol, direction, sl)
    qty = round_qty(symbol, float(fill_qty))

    fill_entry = None
    if order_id is not None:
        try:
            order = _fapi_request("GET", "/fapi/v1/order", {"symbol": symbol.upper(), "orderId": order_id})
            avg_price = float(order.get("avgPrice") or 0)
            if avg_price > 0:
                fill_entry = avg_price
        except (RuntimeError, TypeError, ValueError):
            pass
    if fill_entry is None:
        pos_snap = _fetch_exchange_exposure(symbol)
        if pos_snap.get("open") and pos_snap.get("entry"):
            try:
                fill_entry = float(pos_snap["entry"])
            except (TypeError, ValueError):
                fill_entry = None
    tp_use = tp
    if fill_entry and sl > 0:
        sl_resolved, _ = resolve_sl_for_open_position(direction, fill_entry, sl)
        if sl_resolved and sl_resolved > 0:
            sl = float(sl_resolved)
            sl_price = round_price_for_sl(symbol, direction, sl)
        recalc = recalculate_tp1_from_position(direction, fill_entry, sl)
        if recalc and recalc > 0:
            tp_use = _ensure_tp_ahead_of_mark(symbol, direction, recalc)
            _persist_recalculated_protection(
                symbol,
                direction,
                position_entry=fill_entry,
                sl_val=sl,
                tp_val=tp_use,
                reason="protection_after_fill",
            )
    tp_price = round_price(symbol, tp_use)

    placed_sl, skipped_sl = _place_sl_for_position(symbol, direction, sl_price, qty, log_suffix="_placed")
    if placed_sl:
        result["sl"] = True
    elif skipped_sl:
        _append_orders_log(
            "sl_skipped_after_fill",
            symbol=symbol,
            direction=direction,
            sl=sl_price,
            qty=qty,
            reason="mark_through_stop",
        )
    else:
        logger.error("SL placement failed for %s", symbol)
        telegram.notify_sl_tp_failed(symbol, direction, "SL", "reconcile failed")

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

    use_market = _use_market_for_first_leg(leg_index)
    qty_entry = _entry_price_for_quantity(symbol, entry, use_market=use_market)
    price_str = round_price(symbol, entry)
    try:
        qty = _calculate_quantity(symbol, qty_entry, size_pct)
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
        "entry_type": "MARKET" if use_market else "LIMIT",
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
            message=(
                f"DRY-RUN {direction} {symbol} "
                f"{'MARKET' if use_market else 'LIMIT'} entry {price_str} qty {qty}"
            ),
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
        if use_market:
            response = _place_market_entry(
                symbol,
                direction,
                qty,
                leg_index=leg_index if dca_max_legs else None,
            )
        else:
            response = _place_limit_entry(
                symbol,
                direction,
                price_str,
                qty,
                leg_index=leg_index if dca_max_legs else None,
            )
        order_id = response.get("orderId")
        _append_orders_log("live_open", orderId=order_id, **payload)
        _set_status(
            message=(
                f"LIVE {direction} {symbol} "
                f"{'MARKET' if use_market else 'LIMIT'} orderId {order_id}"
            ),
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
        telegram.notify_live_open(
            symbol,
            direction,
            price_str if not use_market else f"~{round_price(symbol, qty_entry)} (market)",
            payload["sl"],
            tp_label,
            vol_usdt,
            entry_order_type="MARKET" if use_market else "LIMIT",
        )
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
        _update_trade_context(symbol, be_applied=False, position_peak_qty=None)
        if dca_max_legs > 0:
            _sync_dca_leg_count(symbol)
        _log_execution_decision(
            symbol,
            "order_live_open",
            outcome="live",
            market_snapshot={
                "signal": direction,
                "entry": price_str,
                "entry_type": payload["entry_type"],
                "orderId": order_id,
                "leg": leg_index,
            },
        )
        if order_id is not None and REST_PLACE_SL_TP:
            _place_sl_tp_after_fill(symbol, direction, sl, tp, qty, int(order_id))
    except RuntimeError as exc:
        logger.error("Order failed for %s: %s", symbol, exc)
        _append_orders_log("live_open_failed", symbol=symbol, error=str(exc), **payload)
        _set_status(message=f"Order failed: {exc}", last_event="error")
        _notify_order_failed(symbol, direction, exc)


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
        _notify_order_failed(symbol, direction, exc)


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
        use_market = _use_market_for_first_leg(index)
        qty_price = _entry_price_for_quantity(symbol, price, use_market=use_market)
        price_str = round_price(symbol, price)
        try:
            qty = _calculate_quantity(symbol, qty_price, size_pct)
        except ValueError as exc:
            _set_status(message=str(exc), last_event="error")
            _append_orders_log("error", symbol=symbol, error=str(exc), leg=index)
            return
        try:
            if use_market:
                response = _place_market_entry(symbol, direction, qty, leg_index=index)
            else:
                response = _place_limit_entry(symbol, direction, price_str, qty, leg_index=index)
            order_id = response.get("orderId")
            row = {
                "leg": index,
                "label": leg.get("label"),
                "orderId": order_id,
                "price": price_str,
                "qty": qty,
                "size_pct": size_pct,
                "entry_type": "MARKET" if use_market else "LIMIT",
            }
            placed.append(row)
            _append_orders_log("live_dca_leg", **row, symbol=symbol, direction=direction)
            if use_market and order_id is not None and REST_PLACE_SL_TP:
                _place_sl_tp_after_fill(symbol, direction, sl, tp, qty, int(order_id))
        except RuntimeError as exc:
            logger.error("DCA leg %s failed for %s: %s", index, symbol, exc)
            _append_orders_log("live_dca_leg_failed", symbol=symbol, leg=index, error=str(exc))
            _set_status(message=f"DCA leg {index} failed: {exc}", last_event="error")
            _notify_order_failed(symbol, direction, exc)
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
    entry_summary = "market+limits" if any(r.get("entry_type") == "MARKET" for r in placed) else "limits"
    _set_status(
        message=f"LIVE DCA {direction} {symbol} · {len(placed)} legs ({entry_summary}) · SL/TP on fill",
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
    first_type = placed[0].get("entry_type", "LIMIT") if placed else "LIMIT"
    telegram.notify_live_open(
        symbol,
        direction,
        f"{round_price(symbol, avg_entry)} · {len(placed)} legs (DCA)",
        sl_price,
        tp_label,
        vol_usdt,
        entry_order_type=first_type,
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
        entry_price = resolve_dca_leg_entry_price(leg_index, leg, float(entry))
        size_pct = float(leg["size_pct"])
        fingerprint = f"{signal}|{sl:.2f}|{tp:.2f}|leg{leg_index}|{entry_price:.4f}"
        if leg_index == 0:
            allowed, block_reason = can_place_new_order(
                symbol, signal, entry_price, size_pct=size_pct
            )
            if not allowed:
                _log_skip_order(symbol, signal, block_reason, entry=entry_price)
                return
            _run_execution_thread(
                _execute_open,
                symbol=symbol,
                direction=signal,
                entry=entry_price,
                sl=sl,
                tp=tp,
                size_pct=size_pct,
                reasons=reasons,
                plan_fingerprint=fingerprint,
                leg_index=0,
                dca_max_legs=len(legs),
                thread_name=f"exec-{symbol}-{signal}-leg0",
            )
            return
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
            _run_execution_thread(
                _execute_signal_dca_add,
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
                thread_name=f"exec-dca-add-{symbol}-{signal}-leg{leg_index}",
            )
            return
    elif use_dca:
        bundle_legs = legs
        if TRADE_PLAN_DCA_ADVERSE_ONLY:
            bundle_legs = legs[:1]
        leg_prices = "|".join(f"{float(leg['price']):.4f}" for leg in bundle_legs) if bundle_legs else ""
        fingerprint = f"{signal}|{sl:.2f}|{tp:.2f}|{leg_prices}"
        allowed, block_reason = can_place_dca_bundle(symbol, signal, bundle_legs)
        if not allowed:
            _log_skip_order(symbol, signal, block_reason, entry=avg_entry)
            return
        _run_execution_thread(
            _execute_open_dca,
            symbol,
            signal,
            sl,
            tp,
            bundle_legs,
            reasons,
            fingerprint,
            avg_entry,
            thread_name=f"exec-dca-{symbol}-{signal}",
        )
        return
    else:
        size_pct = float(legs[0]["size_pct"]) if legs else float(trade_plan.get("partial_close_pct", 50))
        entry_price = float(legs[0]["price"]) if legs else float(entry)
        fingerprint = f"{signal}|{sl:.2f}|{tp:.2f}|{entry_price:.4f}"
        allowed, block_reason = can_place_new_order(symbol, signal, entry_price, size_pct=size_pct)
        if not allowed:
            _log_skip_order(symbol, signal, block_reason, entry=entry_price)
            return
        _run_execution_thread(
            _execute_open,
            symbol,
            signal,
            entry_price,
            sl,
            tp,
            size_pct,
            reasons,
            fingerprint,
            thread_name=f"exec-{symbol}-{signal}",
        )
