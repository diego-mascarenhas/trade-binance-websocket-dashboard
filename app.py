import argparse
import asyncio
import atexit
import json
import logging
import os
import random
import re
import signal
import socket
import time
from collections import deque
from threading import Event, Lock, Thread

import aiohttp
import pandas as pd
import plotly.graph_objects as go
import websockets
from dash import Dash, Input, Output, dcc, html
from dotenv import load_dotenv
from flask import jsonify
from plotly.subplots import make_subplots

load_dotenv()

import execution
import symbol_config
import telegram_notify as telegram
from indicators import IndicatorFilterSettings, evaluate_indicator_filters, compute_adx
import db_store

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def normalize_symbol(value: str) -> str:
    return value.strip().lower().replace("/", "").replace("-", "")


def parse_cli_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Binance WebSocket Dashboard — live OHLC, order book, signals.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python app.py\n"
            "  python app.py BNBUSDT\n"
            "  python app.py etcusdt --port 8051\n"
            "  python app.py --port 8051 ETCUSDT"
        ),
    )
    parser.add_argument(
        "symbol",
        nargs="?",
        metavar="SYMBOL",
        help="Trading pair (e.g. BTCUSDT). Overrides SYMBOL in .env",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        metavar="PORT",
        help="Dash HTTP port. Overrides DASH_PORT in .env (default: 8050)",
    )
    return parser.parse_args(argv)


def resolve_dash_port(cli_port: int | None) -> int:
    env_port = int(os.getenv("DASH_PORT", "8050"))
    if cli_port is not None:
        if cli_port != env_port:
            logger.info("Port %s (CLI overrides .env DASH_PORT=%s)", cli_port, env_port)
        return cli_port
    return env_port


def is_port_available(host: str, port: int) -> bool:
    bind_host = "" if host in ("", "0.0.0.0") else host
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((bind_host, port))
            return True
        except OSError:
            return False


_cli_args = parse_cli_args()
_env_symbol = normalize_symbol(os.getenv("SYMBOL", "btcusdt"))
SYMBOL = normalize_symbol(_cli_args.symbol) if _cli_args.symbol else _env_symbol
if _cli_args.symbol:
    logger.info("Symbol %s (CLI overrides .env SYMBOL=%s)", SYMBOL.upper(), _env_symbol.upper())
INTERVAL = os.getenv("INTERVAL", "1m")
DEPTH_LEVELS = int(os.getenv("DEPTH_LEVELS", "20"))
DEPTH_CHART_PADDING_PCT = float(os.getenv("DEPTH_CHART_PADDING_PCT", "0.1"))
MAX_CANDLES = int(os.getenv("MAX_CANDLES", "200"))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "50"))
MIN_PATTERN_RANGE_PCT = float(os.getenv("MIN_PATTERN_RANGE_PCT", "0.02"))
OB_WALL_RANGE_PCT = float(os.getenv("OB_WALL_RANGE_PCT", "0.6"))
CANDLE_CHART_LOOKBACK = int(os.getenv("CANDLE_CHART_LOOKBACK", "90"))
SIGNAL_ZONE_LONG_ENTER = float(os.getenv("SIGNAL_ZONE_LONG_ENTER", "20"))
SIGNAL_ZONE_LONG_EXIT = float(os.getenv("SIGNAL_ZONE_LONG_EXIT", "35"))
SIGNAL_ZONE_SHORT_ENTER = float(os.getenv("SIGNAL_ZONE_SHORT_ENTER", "80"))
SIGNAL_ZONE_SHORT_EXIT = float(os.getenv("SIGNAL_ZONE_SHORT_EXIT", "65"))
SIGNAL_DEBOUNCE_COUNT = int(os.getenv("SIGNAL_DEBOUNCE_COUNT", "5"))
MAX_ENTRY_MARKERS = int(os.getenv("MAX_ENTRY_MARKERS", "50"))
HTF_INTERVAL = os.getenv("HTF_INTERVAL", "15m")
HTF_CANDLES = int(os.getenv("HTF_CANDLES", "120"))
REQUIRE_TREND_ALIGN = os.getenv("REQUIRE_TREND_ALIGN", "true").lower() in ("1", "true", "yes")
SIGNAL_COOLDOWN_SEC = int(os.getenv("SIGNAL_COOLDOWN_SEC", "180"))
EMA_FAST = int(os.getenv("EMA_FAST", "9"))
EMA_SLOW = int(os.getenv("EMA_SLOW", "21"))
HTF_EMA_TREND = int(os.getenv("HTF_EMA_TREND", "50"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
MACD_FAST = int(os.getenv("MACD_FAST", "12"))
MACD_SLOW = int(os.getenv("MACD_SLOW", "26"))
MACD_SIGNAL = int(os.getenv("MACD_SIGNAL", "9"))
SWING_LOOKBACK = int(os.getenv("SWING_LOOKBACK", "20"))
SMC_SWING_LEFT = int(os.getenv("SMC_SWING_LEFT", "2"))
SMC_SWING_RIGHT = int(os.getenv("SMC_SWING_RIGHT", "2"))
SMC_INT_SWING_LEFT = int(os.getenv("SMC_INT_SWING_LEFT", "1"))
SMC_INT_SWING_RIGHT = int(os.getenv("SMC_INT_SWING_RIGHT", "1"))
SMC_BREAK_LOOKBACK_HTF = int(os.getenv("SMC_BREAK_LOOKBACK_HTF", "3"))
SMC_BREAK_LOOKBACK_LTF = int(os.getenv("SMC_BREAK_LOOKBACK_LTF", "12"))
SMC_EQ_TOLERANCE_PCT = float(os.getenv("SMC_EQ_TOLERANCE_PCT", "0.08"))
SMC_PD_DISCOUNT_MAX = float(os.getenv("SMC_PD_DISCOUNT_MAX", "38"))
SMC_PD_PREMIUM_MIN = float(os.getenv("SMC_PD_PREMIUM_MIN", "62"))
TRADE_PLAN_DCA_STEPS = int(os.getenv("TRADE_PLAN_DCA_STEPS", "2"))
TRADE_PLAN_DCA_STEP_PCT = float(os.getenv("TRADE_PLAN_DCA_STEP_PCT", "2.5"))
TRADE_PLAN_SL_BUFFER_PCT = float(os.getenv("TRADE_PLAN_SL_BUFFER_PCT", "0.25"))
TRADE_PLAN_TP1_RR = float(os.getenv("TRADE_PLAN_TP1_RR", "1.0"))
TRADE_PLAN_TP2_RR = float(os.getenv("TRADE_PLAN_TP2_RR", "2.0"))
TRADE_PLAN_PARTIAL_CLOSE_PCT = float(os.getenv("TRADE_PLAN_PARTIAL_CLOSE_PCT", "70"))
TRADE_PLAN_TRAIL_PCT = float(os.getenv("TRADE_PLAN_TRAIL_PCT", "0.25"))
TRADE_PLAN_INITIAL_SIZE_PCT = float(os.getenv("TRADE_PLAN_INITIAL_SIZE_PCT", "50"))
TRADE_PLAN_EXECUTE_DCA = os.getenv("TRADE_PLAN_EXECUTE_DCA", "true").lower() in (
    "1",
    "true",
    "yes",
)
LOG_DIR = os.getenv("LOG_DIR", "logs")
DB_CONFIG_POLL_SEC = int(os.getenv("DB_CONFIG_POLL_SEC", "0"))

INDICATOR_FILTERS_ENABLED = os.getenv("INDICATOR_FILTERS_ENABLED", "true").lower() in (
    "1",
    "true",
    "yes",
)
RSI_FILTER_ENABLED = os.getenv("RSI_FILTER_ENABLED", "true").lower() in ("1", "true", "yes")
RSI_LONG_MAX = float(os.getenv("RSI_LONG_MAX", "70"))
RSI_SHORT_MIN = float(os.getenv("RSI_SHORT_MIN", "30"))
MACD_FILTER_ENABLED = os.getenv("MACD_FILTER_ENABLED", "false").lower() in ("1", "true", "yes")
ADX_PERIOD = int(os.getenv("ADX_PERIOD", "14"))
ADX_FILTER_ENABLED = os.getenv("ADX_FILTER_ENABLED", "true").lower() in ("1", "true", "yes")
ADX_MIN_TREND = float(os.getenv("ADX_MIN_TREND", "25"))
ADX_USE_HTF = os.getenv("ADX_USE_HTF", "true").lower() in ("1", "true", "yes")

DASH_HOST = os.getenv("DASH_HOST", "0.0.0.0")
DASH_PORT = resolve_dash_port(_cli_args.port)

symbol_config.capture_env_defaults(globals())
symbol_config.apply_db_overrides(globals(), SYMBOL)

ws_force_reconnect = Event()

REST_BASE = "https://api.binance.com"
WS_BASE = "wss://stream.binance.com:9443"

state_lock = Lock()
candles: deque = deque(maxlen=MAX_CANDLES)
htf_candles: deque = deque(maxlen=HTF_CANDLES)
forming_candle: dict | None = None
htf_forming_candle: dict | None = None
last_valid_entry_monotonic: float | None = None
orderbook: dict = {"bids": [], "asks": []}
analysis_orderbook: dict = {"bids": [], "asks": []}
latest_pattern = "None"
latest_price: float | None = None
spread: float | None = None
spread_pct: float | None = None
volume_delta: float | None = None
bid_volume: float = 0.0
ask_volume: float = 0.0
ws_status = "connecting"
support: float | None = None
resistance: float | None = None
ob_support_qty: float = 0.0
ob_resistance_qty: float = 0.0
signal_dir = "NEUTRAL"
stable_signal_dir = "NEUTRAL"
pending_signal = "NEUTRAL"
pending_signal_count = 0
signal_confidence = 0
signal_reasons = ""
signal_entry: float | None = None
zone_position_pct: float | None = None
change_24h: float | None = None
valid_entries: deque = deque(maxlen=MAX_ENTRY_MARKERS)
active_trade_plan: dict | None = None
active_trade_plan_created_at: str | None = None


def utc_now_str() -> str:
    return pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S UTC")


def append_event_log(log_name: str, event: str, **fields) -> None:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        record = {"ts": utc_now_str(), "event": event, "symbol": SYMBOL.upper(), **fields}
        path = os.path.join(LOG_DIR, f"{log_name}.log")
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, default=str) + "\n")
    except OSError:
        logger.exception("Failed writing %s log", log_name)


def log_error(event: str, **fields) -> None:
    append_event_log("errors", event, **fields)
    logger.error("%s %s", event, fields)


def indicator_filter_settings() -> IndicatorFilterSettings:
    return IndicatorFilterSettings(
        enabled=INDICATOR_FILTERS_ENABLED,
        rsi_enabled=RSI_FILTER_ENABLED,
        rsi_long_max=RSI_LONG_MAX,
        rsi_short_min=RSI_SHORT_MIN,
        macd_enabled=MACD_FILTER_ENABLED,
        adx_enabled=ADX_FILTER_ENABLED,
        adx_min_trend=ADX_MIN_TREND,
        adx_use_htf=ADX_USE_HTF,
    )


def log_decision_event(
    event_type: str,
    *,
    outcome: str | None = None,
    block_reason: str | None = None,
    market_snapshot: dict | None = None,
) -> None:
    db_store.log_decision_event(
        SYMBOL,
        event_type,
        outcome=outcome,
        block_reason=block_reason,
        config_snapshot=symbol_config.get_config_snapshot(),
        market_snapshot=market_snapshot,
        config_version=symbol_config.config_version(),
    )


def trade_plan_fingerprint(plan: dict) -> str:
    return "|".join(
        f"{plan.get('signal')}:{round(float(plan[key]), 2)}"
        for key in ("avg_entry", "sl", "tp1", "tp2")
        if plan.get(key) is not None
    )


def trade_plan_log_payload(plan: dict) -> dict:
    keys = (
        "signal",
        "entry",
        "avg_entry",
        "sl",
        "tp1",
        "tp2",
        "partial_close_pct",
        "runner_pct",
        "trail_pct",
        "risk_pct",
        "reward_tp1_pct",
    )
    return {key: plan[key] for key in keys if key in plan}


def resolve_trade_plan(current_plan: dict, live_signal: str) -> dict:
    global active_trade_plan, active_trade_plan_created_at

    if current_plan.get("active"):
        fingerprint = trade_plan_fingerprint(current_plan)
        stored_fingerprint = trade_plan_fingerprint(active_trade_plan) if active_trade_plan else None
        if stored_fingerprint != fingerprint:
            active_trade_plan = dict(current_plan)
            active_trade_plan_created_at = utc_now_str()
            payload = trade_plan_log_payload(active_trade_plan)
            append_event_log("plans", "plan_suggested", fingerprint=fingerprint, **payload)
            append_event_log("trades", "plan_opened", fingerprint=fingerprint, **payload)

    if active_trade_plan:
        display = dict(active_trade_plan)
        display["active"] = True
        display["created_at"] = active_trade_plan_created_at
        display["persisted"] = True
        display["live_signal"] = live_signal
        display["live_match"] = live_signal == active_trade_plan.get("signal") and current_plan.get(
            "active", False
        )
        if display["live_match"]:
            display["status_note"] = "Matches live TRADE signal"
        else:
            display["status_note"] = f"Held until next plan · live signal: {live_signal}"
        return display

    return current_plan


def _immediate_bullish_run(prior_rows: pd.DataFrame | None, lookback: int = 4) -> bool:
    if prior_rows is None or len(prior_rows) < 3:
        return False
    tail = prior_rows.tail(lookback)
    closes = [float(value) for value in tail["c"].tolist()]
    rises = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i - 1])
    return rises >= 2 and closes[-1] >= closes[0]


def _immediate_bearish_run(prior_rows: pd.DataFrame | None, lookback: int = 4) -> bool:
    if prior_rows is None or len(prior_rows) < 3:
        return False
    tail = prior_rows.tail(lookback)
    closes = [float(value) for value in tail["c"].tolist()]
    falls = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i - 1])
    return falls >= 2 and closes[-1] <= closes[0]


def _at_swing_high(row: pd.Series, prior_rows: pd.DataFrame | None, lookback: int = 10) -> bool:
    if prior_rows is None or len(prior_rows) < 3:
        return False
    window = prior_rows.tail(lookback - 1)
    prior_high = float(window["h"].max())
    return float(row["h"]) >= prior_high * 0.9995


def _at_swing_low(row: pd.Series, prior_rows: pd.DataFrame | None, lookback: int = 10) -> bool:
    if prior_rows is None or len(prior_rows) < 3:
        return False
    window = prior_rows.tail(lookback - 1)
    prior_low = float(window["l"].min())
    return float(row["l"]) <= prior_low * 1.0005


def _near_support(row: pd.Series, support_level: float | None, resistance_level: float | None) -> bool:
    if not support_level or not resistance_level or resistance_level <= support_level:
        return False
    position = (float(row["c"]) - support_level) * 100 / (resistance_level - support_level)
    return position <= 30


def _near_resistance(row: pd.Series, support_level: float | None, resistance_level: float | None) -> bool:
    if not support_level or not resistance_level or resistance_level <= support_level:
        return False
    position = (float(row["c"]) - support_level) * 100 / (resistance_level - support_level)
    return position >= 70


def detect_pattern(
    row: pd.Series,
    prior_rows: pd.DataFrame | None = None,
    support_level: float | None = None,
    resistance_level: float | None = None,
) -> str | None:
    body = abs(float(row["c"]) - float(row["o"]))
    rng = max(float(row["h"]) - float(row["l"]), 1e-12)
    upper = float(row["h"]) - max(float(row["o"]), float(row["c"]))
    lower = min(float(row["o"]), float(row["c"])) - float(row["l"])
    close = float(row["c"])

    if close <= 0 or (rng / close) * 100 < MIN_PATTERN_RANGE_PCT:
        return None

    body_pct = body / rng
    upper_pct = upper / rng
    lower_pct = lower / rng
    close_pos = (close - float(row["l"])) / rng

    is_hammer_shape = body_pct <= 0.28 and lower_pct >= 0.62 and upper_pct <= 0.12
    is_star_shape = body_pct <= 0.28 and upper_pct >= 0.62 and lower_pct <= 0.12

    hammer_context = _immediate_bearish_run(prior_rows) or _near_support(
        row, support_level, resistance_level
    )
    star_context = _immediate_bullish_run(prior_rows) or _near_resistance(
        row, support_level, resistance_level
    )

    if (
        is_hammer_shape
        and close_pos >= 0.58
        and hammer_context
        and _at_swing_low(row, prior_rows)
    ):
        return "Hammer"

    if (
        is_star_shape
        and close_pos <= 0.42
        and float(row["c"]) <= float(row["o"])
        and star_context
        and _at_swing_high(row, prior_rows)
    ):
        return "Shooting Star"

    return None


def pattern_signal_match(pattern: str, signal_dir: str) -> bool:
    if pattern == "Hammer":
        return signal_dir == "LONG"
    if pattern == "Shooting Star":
        return signal_dir == "SHORT"
    return False


def confirmed_pattern(
    pattern: str | None,
    signal_dir: str,
    confidence: int,
    min_confidence: int = MIN_CONFIDENCE,
) -> str | None:
    if not pattern or not pattern_signal_match(pattern, signal_dir):
        return None
    if confidence < min_confidence:
        return None
    return pattern


def pattern_marker_label(pattern: str, signal_dir: str, confidence: int) -> str:
    return f"{pattern} · {signal_dir} {confidence}%"


def compute_ema(values: pd.Series, period: int) -> pd.Series:
    return values.ewm(span=period, adjust=False).mean()


def compute_rsi(closes: pd.Series, period: int = RSI_PERIOD) -> float | None:
    if len(closes) < period + 1:
        return None
    delta = closes.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, 1e-12)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


def compute_macd_values(
    closes: pd.Series,
    fast: int = MACD_FAST,
    slow: int = MACD_SLOW,
    signal_period: int = MACD_SIGNAL,
) -> tuple[float | None, float | None, float | None]:
    if len(closes) < slow + signal_period:
        return None, None, None
    ema_fast = compute_ema(closes, fast)
    ema_slow = compute_ema(closes, slow)
    macd_line = ema_fast - ema_slow
    signal_line = compute_ema(macd_line, signal_period)
    histogram = macd_line - signal_line
    return float(macd_line.iloc[-1]), float(signal_line.iloc[-1]), float(histogram.iloc[-1])


def detect_latest_fvg(closed_df: pd.DataFrame) -> dict | None:
    if len(closed_df) < 3:
        return None
    for index in range(len(closed_df) - 1, 1, -1):
        first = closed_df.iloc[index - 2]
        third = closed_df.iloc[index]
        if float(first["h"]) < float(third["l"]):
            gap_low = float(first["h"])
            gap_high = float(third["l"])
            return {
                "type": "BULL",
                "low": gap_low,
                "high": gap_high,
                "label": f"Bull {gap_low:,.2f}–{gap_high:,.2f}",
            }
        if float(first["l"]) > float(third["h"]):
            gap_low = float(third["h"])
            gap_high = float(first["l"])
            return {
                "type": "BEAR",
                "low": gap_low,
                "high": gap_high,
                "label": f"Bear {gap_low:,.2f}–{gap_high:,.2f}",
            }
    return None


def liquidity_label(closed_df: pd.DataFrame, price: float | None, lookback: int = SWING_LOOKBACK) -> str:
    if closed_df.empty or price is None or len(closed_df) < 3:
        return "—"
    window = closed_df.tail(lookback)
    swing_high = float(window["h"].max())
    swing_low = float(window["l"].min())
    tolerance = price * 0.00035
    if abs(price - swing_high) <= tolerance:
        return "At swing high"
    if abs(price - swing_low) <= tolerance:
        return "At swing low"
    last = closed_df.iloc[-1]
    prev = closed_df.iloc[-2]
    if float(last["l"]) < float(prev["l"]) and float(last["c"]) > float(prev["l"]):
        return "Sweep low"
    if float(last["h"]) > float(prev["h"]) and float(last["c"]) < float(prev["h"]):
        return "Sweep high"
    return "Mid range"


def find_fractal_swings(
    closed_df: pd.DataFrame,
    left: int = SMC_SWING_LEFT,
    right: int = SMC_SWING_RIGHT,
) -> list[dict]:
    """Alternating swing highs/lows (fractal pivots) on OHLC data."""
    if len(closed_df) < left + right + 1:
        return []

    highs = closed_df["h"].astype(float).values
    lows = closed_df["l"].astype(float).values
    raw: list[dict] = []

    for index in range(left, len(closed_df) - right):
        high_window = highs[index - left : index + right + 1]
        if highs[index] >= max(high_window):
            raw.append({"index": index, "type": "high", "price": float(highs[index])})
        low_window = lows[index - left : index + right + 1]
        if lows[index] <= min(low_window):
            raw.append({"index": index, "type": "low", "price": float(lows[index])})

    raw.sort(key=lambda item: item["index"])
    merged: list[dict] = []
    for swing in raw:
        if not merged:
            merged.append(swing)
            continue
        if merged[-1]["type"] != swing["type"]:
            merged.append(swing)
            continue
        if swing["type"] == "high" and swing["price"] >= merged[-1]["price"]:
            merged[-1] = swing
        elif swing["type"] == "low" and swing["price"] <= merged[-1]["price"]:
            merged[-1] = swing

    return merged


def _swing_sequence_label(highs: list[float], lows: list[float]) -> str:
    parts: list[str] = []
    if len(highs) >= 2:
        parts.append("HH" if highs[-1] > highs[-2] else "LH")
    if len(lows) >= 2:
        parts.append("HL" if lows[-1] > lows[-2] else "LL")
    return " · ".join(parts) if parts else "—"


def _structure_trend(highs: list[float], lows: list[float]) -> str:
    if len(highs) < 2 or len(lows) < 2:
        return "RANGING"
    hh = highs[-1] > highs[-2]
    hl = lows[-1] > lows[-2]
    lh = highs[-1] < highs[-2]
    ll = lows[-1] < lows[-2]
    if hh and hl:
        return "BULLISH"
    if lh and ll:
        return "BEARISH"
    return "RANGING"


def _pd_zone_from_pct(pct: float | None) -> str:
    if pct is None:
        return "—"
    if pct <= SMC_PD_DISCOUNT_MAX:
        return "Discount"
    if pct >= SMC_PD_PREMIUM_MIN:
        return "Premium"
    return "Equilibrium"


def _detect_recent_structure_event(
    closed_df: pd.DataFrame,
    swings: list[dict],
    trend: str,
    *,
    lookback: int = 3,
    prefix: str = "",
) -> tuple[str, str, str]:
    """Return (pattern, bias, pattern_type) e.g. ('BOS', 'bull', 'bos_bull')."""
    if len(closed_df) < 2 or len(swings) < 2:
        return "—", "neutral", "none"

    swing_highs = [sw for sw in swings if sw["type"] == "high"]
    swing_lows = [sw for sw in swings if sw["type"] == "low"]
    if not swing_highs or not swing_lows:
        return "—", "neutral", "none"

    start = max(1, len(closed_df) - lookback)
    for index in range(len(closed_df) - 1, start - 1, -1):
        last_close = float(closed_df.iloc[index]["c"])
        prev_close = float(closed_df.iloc[index - 1]["c"])
        relevant_highs = [sw for sw in swing_highs if sw["index"] < index]
        relevant_lows = [sw for sw in swing_lows if sw["index"] < index]
        if not relevant_highs or not relevant_lows:
            continue

        last_high = relevant_highs[-1]["price"]
        last_low = relevant_lows[-1]["price"]
        broke_high = last_close > last_high and prev_close <= last_high
        broke_low = last_close < last_low and prev_close >= last_low

        if broke_high:
            if trend in ("BULLISH", "RANGING"):
                return f"{prefix}BOS", "bull", "bos_bull"
            return f"{prefix}CHoCH", "bull", "choch_bull"
        if broke_low:
            if trend in ("BEARISH", "RANGING"):
                return f"{prefix}BOS", "bear", "bos_bear"
            return f"{prefix}CHoCH", "bear", "choch_bear"

    return "—", "neutral", "none"


def _detect_equal_liquidity(swings: list[dict], price: float) -> tuple[str, str, str]:
    """Equal highs (EQH) or equal lows (EQL) liquidity pools."""
    highs = [sw for sw in swings if sw["type"] == "high"]
    lows = [sw for sw in swings if sw["type"] == "low"]
    tolerance = price * SMC_EQ_TOLERANCE_PCT / 100

    if len(highs) >= 2:
        level = (highs[-2]["price"] + highs[-1]["price"]) / 2
        if abs(highs[-2]["price"] - highs[-1]["price"]) <= tolerance:
            if abs(price - level) <= tolerance * 2:
                return "EQH", "bear", "eqh"

    if len(lows) >= 2:
        level = (lows[-2]["price"] + lows[-1]["price"]) / 2
        if abs(lows[-2]["price"] - lows[-1]["price"]) <= tolerance:
            if abs(price - level) <= tolerance * 2:
                return "EQL", "bull", "eql"

    return "—", "neutral", "none"


def _pd_zone_pattern_fallback(pd_zone: str) -> tuple[str, str, str]:
    mapping = {
        "Discount": ("Disc", "bull", "pd_discount"),
        "Premium": ("Prem", "bear", "pd_premium"),
        "Equilibrium": ("Eq", "neutral", "pd_equilibrium"),
    }
    return mapping.get(pd_zone, ("—", "neutral", "none"))


def _resolve_smc_pattern(
    htf_df: pd.DataFrame,
    htf_swings: list[dict],
    htf_trend: str,
    ltf_df: pd.DataFrame | None,
    price: float,
    liquidity: str,
    pd_zone: str,
) -> tuple[str, str, str]:
    pattern, bias, ptype = _detect_recent_structure_event(
        htf_df,
        htf_swings,
        htf_trend,
        lookback=SMC_BREAK_LOOKBACK_HTF,
    )
    if pattern != "—":
        return pattern, bias, ptype

    if ltf_df is not None and not ltf_df.empty:
        ltf_swings = find_fractal_swings(
            ltf_df,
            left=SMC_INT_SWING_LEFT,
            right=SMC_INT_SWING_RIGHT,
        )
        ltf_highs = [sw["price"] for sw in ltf_swings if sw["type"] == "high"]
        ltf_lows = [sw["price"] for sw in ltf_swings if sw["type"] == "low"]
        ltf_trend = _structure_trend(ltf_highs, ltf_lows)
        pattern, bias, ptype = _detect_recent_structure_event(
            ltf_df,
            ltf_swings,
            ltf_trend,
            lookback=SMC_BREAK_LOOKBACK_LTF,
            prefix="i",
        )
        if pattern != "—":
            return pattern, bias, ptype

    pattern, bias, ptype = _detect_equal_liquidity(htf_swings, price)
    if pattern != "—":
        return pattern, bias, ptype

    if liquidity == "Sweep high":
        return "EQH", "bear", "sweep_high"
    if liquidity == "Sweep low":
        return "EQL", "bull", "sweep_low"
    if liquidity == "At swing high":
        return "EQH", "bear", "at_swing_high"
    if liquidity == "At swing low":
        return "EQL", "bull", "at_swing_low"

    return _pd_zone_pattern_fallback(pd_zone)


def _build_smc_state(
    trend: str,
    pd_zone: str,
    pattern: str,
    liquidity: str,
    ob_zone_pct: float | None,
) -> tuple[str, str]:
    """Human-readable SMC phase + short hint."""
    state = f"{pattern} · {pd_zone}" if pd_zone != "—" else pattern

    hints: list[str] = []
    if pattern.startswith("BOS"):
        hints.append("Structure continuation")
    elif pattern.startswith("CHoCH") or pattern.startswith("iCHoCH"):
        hints.append("Potential reversal")
    elif pattern.startswith("iBOS"):
        hints.append("Internal continuation")
    elif pattern == "EQH":
        hints.append("Sell-side liquidity · equal highs")
    elif pattern == "EQL":
        hints.append("Buy-side liquidity · equal lows")
    if liquidity in ("Sweep low", "At swing low") and pd_zone == "Discount":
        hints.append("Liquidity grab below")
    elif liquidity in ("Sweep high", "At swing high") and pd_zone == "Premium":
        hints.append("Liquidity grab above")
    if ob_zone_pct is not None:
        if ob_zone_pct <= SIGNAL_ZONE_LONG_ENTER:
            hints.append("At OB support")
        elif ob_zone_pct >= SIGNAL_ZONE_SHORT_ENTER:
            hints.append("At OB resistance")

    return state, " · ".join(hints) if hints else "—"


def format_smc_hub_label(smc: dict) -> str:
    """Hub cards: SMC pattern code only (BOS, CHoCH, EQH, …)."""
    pattern = smc.get("pattern", "—")
    return pattern if pattern not in (None, "—") else "—"


def compute_smc_structure(
    htf_closed_df: pd.DataFrame,
    price: float | None,
    liquidity: str,
    ltf_closed_df: pd.DataFrame | None = None,
    support: float | None = None,
    resistance: float | None = None,
    ob_zone_pct: float | None = None,
) -> dict:
    empty = {
        "trend": "RANGING",
        "sequence": "—",
        "pattern": "—",
        "pattern_bias": "neutral",
        "pattern_type": "none",
        "last_event": "—",
        "last_event_type": "none",
        "pd_zone": "—",
        "pd_pct": None,
        "state": "—",
        "state_hint": "—",
    }
    if htf_closed_df.empty or price is None:
        return empty

    swings = find_fractal_swings(htf_closed_df)
    swing_highs = [sw["price"] for sw in swings if sw["type"] == "high"]
    swing_lows = [sw["price"] for sw in swings if sw["type"] == "low"]
    trend = _structure_trend(swing_highs, swing_lows)
    sequence = _swing_sequence_label(swing_highs, swing_lows)

    range_low = swing_lows[-1] if swing_lows else None
    range_high = swing_highs[-1] if swing_highs else None
    if (
        support
        and resistance
        and resistance > support
        and (range_low is None or range_high is None or range_high <= range_low)
    ):
        range_low = support
        range_high = resistance

    pd_pct = None
    if range_low is not None and range_high is not None and range_high > range_low:
        pd_pct = (price - range_low) * 100 / (range_high - range_low)
    pd_zone = _pd_zone_from_pct(pd_pct)

    pattern, pattern_bias, pattern_type = _resolve_smc_pattern(
        htf_closed_df,
        swings,
        trend,
        ltf_closed_df,
        price,
        liquidity,
        pd_zone,
    )
    last_event = pattern if pattern != "—" else "—"
    last_event_type = pattern_type
    state, state_hint = _build_smc_state(trend, pd_zone, pattern, liquidity, ob_zone_pct)

    return {
        "trend": trend,
        "sequence": sequence,
        "pattern": pattern,
        "pattern_bias": pattern_bias,
        "pattern_type": pattern_type,
        "last_event": last_event,
        "last_event_type": last_event_type,
        "pd_zone": pd_zone,
        "pd_pct": pd_pct,
        "state": state,
        "state_hint": state_hint,
    }


def pd_zone_value_class(pd_zone: str) -> str:
    if pd_zone == "Discount":
        return "kv-value smc-discount"
    if pd_zone == "Premium":
        return "kv-value smc-premium"
    if pd_zone == "Equilibrium":
        return "kv-value smc-equilibrium"
    return "kv-value"


def structure_event_badge_class(event_type: str) -> str:
    if event_type in ("bos_bull", "choch_bull", "eql", "sweep_low", "at_swing_low", "pd_discount"):
        return "badge badge-bull"
    if event_type in ("bos_bear", "choch_bear", "eqh", "sweep_high", "at_swing_high", "pd_premium"):
        return "badge badge-bear"
    return "badge badge-neutral"


def compute_htf_bias(htf_closed_df: pd.DataFrame) -> tuple[str, float | None, float | None, float | None]:
    if len(htf_closed_df) < HTF_EMA_TREND + 2:
        return "NEUTRAL", None, None, None
    closes = htf_closed_df["c"].astype(float)
    ema_fast = float(compute_ema(closes, EMA_FAST).iloc[-1])
    ema_slow = float(compute_ema(closes, EMA_SLOW).iloc[-1])
    ema_trend = float(compute_ema(closes, HTF_EMA_TREND).iloc[-1])
    close = float(closes.iloc[-1])
    if close > ema_trend and ema_fast > ema_slow:
        return "BULLISH", ema_fast, ema_slow, ema_trend
    if close < ema_trend and ema_fast < ema_slow:
        return "BEARISH", ema_fast, ema_slow, ema_trend
    return "NEUTRAL", ema_fast, ema_slow, ema_trend


def signal_aligned_with_trend(signal: str, trend_bias: str) -> bool:
    if not REQUIRE_TREND_ALIGN:
        return True
    if trend_bias == "NEUTRAL":
        return False
    if signal == "LONG" and trend_bias == "BULLISH":
        return True
    if signal == "SHORT" and trend_bias == "BEARISH":
        return True
    return False


def compute_market_analysis(
    closed_rows: list[dict],
    htf_closed_rows: list[dict],
    price: float | None,
    *,
    support: float | None = None,
    resistance: float | None = None,
    ob_zone_pct: float | None = None,
) -> dict:
    analysis = {
        "htf_interval": HTF_INTERVAL,
        "htf_bias": "NEUTRAL",
        "htf_ema_fast": None,
        "htf_ema_slow": None,
        "htf_ema_trend": None,
        "ema_fast": None,
        "ema_slow": None,
        "ema_cross": "—",
        "rsi": None,
        "adx": None,
        "htf_adx": None,
        "macd": None,
        "macd_signal": None,
        "macd_hist": None,
        "fvg": None,
        "fvg_label": "—",
        "fvg_interval": HTF_INTERVAL,
        "liquidity": "—",
        "liquidity_interval": INTERVAL,
        "smc": {
            "trend": "RANGING",
            "sequence": "—",
            "last_event": "—",
            "last_event_type": "none",
            "pd_zone": "—",
            "pd_pct": None,
            "state": "—",
            "state_hint": "—",
        },
    }
    if not closed_rows:
        return analysis

    closed_df = pd.DataFrame(closed_rows)
    closes = closed_df["c"].astype(float)

    if len(closes) >= EMA_SLOW + 2:
        analysis["ema_fast"] = float(compute_ema(closes, EMA_FAST).iloc[-1])
        analysis["ema_slow"] = float(compute_ema(closes, EMA_SLOW).iloc[-1])
        if analysis["ema_fast"] > analysis["ema_slow"]:
            analysis["ema_cross"] = "Bull cross"
        elif analysis["ema_fast"] < analysis["ema_slow"]:
            analysis["ema_cross"] = "Bear cross"
        else:
            analysis["ema_cross"] = "Flat"

    analysis["rsi"] = compute_rsi(closes, RSI_PERIOD)
    analysis["adx"] = compute_adx(closed_df, ADX_PERIOD)
    macd, macd_signal, macd_hist = compute_macd_values(closes)
    analysis["macd"] = macd
    analysis["macd_signal"] = macd_signal
    analysis["macd_hist"] = macd_hist

    analysis["liquidity"] = liquidity_label(closed_df, price)

    if htf_closed_rows:
        htf_df = pd.DataFrame(htf_closed_rows)
        htf_bias, htf_fast, htf_slow, htf_trend = compute_htf_bias(htf_df)
        analysis["htf_bias"] = htf_bias
        analysis["htf_ema_fast"] = htf_fast
        analysis["htf_ema_slow"] = htf_slow
        analysis["htf_ema_trend"] = htf_trend

        analysis["htf_adx"] = compute_adx(htf_df, ADX_PERIOD)

        fvg = detect_latest_fvg(htf_df)
        if fvg:
            analysis["fvg"] = fvg
            analysis["fvg_label"] = fvg["label"]
            analysis["fvg_interval"] = HTF_INTERVAL

        analysis["smc"] = compute_smc_structure(
            htf_df,
            price,
            analysis["liquidity"],
            ltf_closed_df=closed_df if not closed_df.empty else None,
            support=support,
            resistance=resistance,
            ob_zone_pct=ob_zone_pct,
        )

    return analysis


def trade_plan_for_position_reconcile(
    exposure: dict,
    *,
    support: float | None,
    resistance: float | None,
    price: float | None,
    market_analysis: dict | None,
) -> dict:
    """Build SL/TP from an open exchange position (for protection reconcile on WATCH)."""
    raw_dir = exposure.get("direction")
    if not raw_dir:
        return {"active": False}
    direction = str(raw_dir).split("+", 1)[0].strip().upper()
    entry = exposure.get("entry")
    if direction not in ("LONG", "SHORT") or entry is None or float(entry) <= 0:
        return {"active": False}
    return compute_trade_plan(
        direction,
        max(int(MIN_CONFIDENCE), 50),
        float(entry),
        support,
        resistance,
        price,
        market_analysis,
        min_confidence=0,
    )


def compute_trade_plan(
    signal: str,
    confidence: int,
    entry: float | None,
    support: float | None,
    resistance: float | None,
    price: float | None,
    market_analysis: dict | None,
    min_confidence: int = MIN_CONFIDENCE,
) -> dict:
    inactive = {
        "active": False,
        "summary": "Wait for a TRADE signal (LONG/SHORT with minimum confidence).",
    }
    if not is_tradable_signal(signal, confidence, min_confidence) or entry is None or entry <= 0:
        return inactive

    analysis = market_analysis or {}
    fvg = analysis.get("fvg") or {}
    is_long = signal == "LONG"
    step = TRADE_PLAN_DCA_STEP_PCT / 100
    buffer = TRADE_PLAN_SL_BUFFER_PCT / 100

    dca_prices: list[float] = [float(entry)]
    for step_index in range(1, TRADE_PLAN_DCA_STEPS + 1):
        offset = step * step_index
        dca_price = entry * (1 - offset) if is_long else entry * (1 + offset)
        if is_long and support:
            structure = float(support) * (1 - buffer)
            if structure < entry:
                dca_price = min(dca_price, structure)
        elif not is_long and resistance:
            structure = float(resistance) * (1 + buffer)
            if structure > entry:
                dca_price = max(dca_price, structure)
        dca_prices.append(dca_price)

    if is_long and fvg.get("type") == "BULL" and len(dca_prices) > 1:
        fvg_mid = (float(fvg["low"]) + float(fvg["high"])) / 2
        if fvg_mid < entry and fvg_mid < dca_prices[1]:
            dca_prices[1] = fvg_mid
    elif not is_long and fvg.get("type") == "BEAR" and len(dca_prices) > 1:
        fvg_mid = (float(fvg["low"]) + float(fvg["high"])) / 2
        if fvg_mid > entry and fvg_mid > dca_prices[1]:
            dca_prices[1] = fvg_mid

    total_slots = len(dca_prices)
    remaining_size = max(0.0, 100.0 - TRADE_PLAN_INITIAL_SIZE_PCT)
    dca_size = remaining_size / max(total_slots - 1, 1) if total_slots > 1 else 0.0
    legs: list[dict] = [
        {
            "label": f"Entry · {TRADE_PLAN_INITIAL_SIZE_PCT:.0f}%",
            "price": dca_prices[0],
            "size_pct": TRADE_PLAN_INITIAL_SIZE_PCT,
        }
    ]
    for index, dca_price in enumerate(dca_prices[1:], start=1):
        legs.append(
            {
                "label": f"DCA {index + 1} · {dca_size:.0f}% · if position adverse",
                "price": dca_price,
                "size_pct": dca_size,
                "trigger": "valid_entry_adverse",
            }
        )

    avg_entry = sum(leg["price"] * leg["size_pct"] for leg in legs) / 100.0

    if is_long:
        structure_sl = float(support) * (1 - buffer) if support else entry * (1 - step * (TRADE_PLAN_DCA_STEPS + 1))
        sl = min(structure_sl, min(leg["price"] for leg in legs) * (1 - buffer))
        risk = avg_entry - sl
        tp1 = avg_entry + risk * TRADE_PLAN_TP1_RR
        tp2 = avg_entry + risk * TRADE_PLAN_TP2_RR
        if resistance and tp2 > float(resistance) * 0.998:
            tp2 = float(resistance) * 0.998
    else:
        structure_sl = float(resistance) * (1 + buffer) if resistance else entry * (1 + step * (TRADE_PLAN_DCA_STEPS + 1))
        sl = max(structure_sl, max(leg["price"] for leg in legs) * (1 + buffer))
        risk = sl - avg_entry
        tp1 = avg_entry - risk * TRADE_PLAN_TP1_RR
        tp2 = avg_entry - risk * TRADE_PLAN_TP2_RR
        if support and tp2 < float(support) * 1.002:
            tp2 = float(support) * 1.002

    if risk <= 0:
        return inactive

    runner_pct = 100.0 - TRADE_PLAN_PARTIAL_CLOSE_PCT
    risk_pct = abs(avg_entry - sl) / avg_entry * 100
    reward_tp1_pct = abs(tp1 - avg_entry) / avg_entry * 100

    return {
        "active": True,
        "signal": signal,
        "summary": (
            f"Suggested {signal} plan · DCA if position adverse · partial {TRADE_PLAN_PARTIAL_CLOSE_PCT:.0f}% at TP1"
            if TRADE_PLAN_EXECUTE_DCA and len(legs) > 1
            else f"Suggested {signal} plan · partial {TRADE_PLAN_PARTIAL_CLOSE_PCT:.0f}% at TP1"
        ),
        "entry": entry,
        "avg_entry": avg_entry,
        "legs": legs,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "partial_close_pct": TRADE_PLAN_PARTIAL_CLOSE_PCT,
        "runner_pct": runner_pct,
        "breakeven_price": avg_entry,
        "breakeven_note": (
            f"At ~{TRADE_PLAN_PARTIAL_CLOSE_PCT:.0f}% closed → auto SL to BE "
            f"({format_price(avg_entry)}) on runner {runner_pct:.0f}%"
            if execution.TRADE_PLAN_AUTO_BE
            else (
                f"At TP1: close {TRADE_PLAN_PARTIAL_CLOSE_PCT:.0f}% → SL to BE "
                f"({format_price(avg_entry)}) on runner {runner_pct:.0f}% (auto BE off)"
            )
        ),
        "trail_pct": TRADE_PLAN_TRAIL_PCT,
        "trail_note": (
            f"Trail remaining {runner_pct:.0f}% at {TRADE_PLAN_TRAIL_PCT:.2f}% "
            f"from {'peak' if is_long else 'trough'}"
        ),
        "risk_pct": risk_pct,
        "reward_tp1_pct": reward_tp1_pct,
        "rr_tp1": TRADE_PLAN_TP1_RR,
        "rr_tp2": TRADE_PLAN_TP2_RR,
    }


def format_price(price: float | None) -> str:
    if price is None:
        return "—"
    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:.4f}"
    return f"{price:.6f}"


def price_label(price: float) -> str:
    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 100:
        return f"{price:.3f}"
    if price >= 1:
        return f"{price:.4f}"
    return f"{price:.6f}"


def price_tick_format(price: float | None) -> str:
    if price is None or price <= 0:
        return ".4f"
    if price >= 1000:
        return ",.2f"
    if price >= 100:
        return ".3f"
    if price >= 1:
        return ".4f"
    return ".6f"


def change_24h_class(change_24h_pct: float | None) -> str:
    if change_24h_pct is None:
        return "price-change-neutral"
    if change_24h_pct > 0:
        return "price-change-up"
    if change_24h_pct < 0:
        return "price-change-down"
    return "price-change-neutral"


def kline_row(k: dict) -> dict:
    return {
        "t": pd.to_datetime(k["t"], unit="ms"),
        "o": float(k["o"]),
        "h": float(k["h"]),
        "l": float(k["l"]),
        "c": float(k["c"]),
        "v": float(k["v"]),
        "x": bool(k["x"]),
    }


def apply_depth_update(data: dict, bids: dict[float, float], asks: dict[float, float]) -> None:
    for p, q in data.get("b", []):
        price, qty = float(p), float(q)
        if qty == 0:
            bids.pop(price, None)
        else:
            bids[price] = qty

    for p, q in data.get("a", []):
        price, qty = float(p), float(q)
        if qty == 0:
            asks.pop(price, None)
        else:
            asks[price] = qty


def snapshot_to_levels(snapshot: dict) -> tuple[list[list[float]], list[list[float]]]:
    bids = [[float(p), float(q)] for p, q in snapshot["bids"]]
    asks = [[float(p), float(q)] for p, q in snapshot["asks"]]
    return bids, asks


def map_to_levels(
    bid_map: dict[float, float],
    ask_map: dict[float, float],
    limit: int | None = None,
) -> tuple[list[list[float]], list[list[float]]]:
    bids = sorted(bid_map.items(), key=lambda item: item[0], reverse=True)
    asks = sorted(ask_map.items(), key=lambda item: item[0])
    if limit is not None:
        bids = bids[:limit]
        asks = asks[:limit]
    return [[price, qty] for price, qty in bids], [[price, qty] for price, qty in asks]


def trim_orderbook(bids: dict[float, float], asks: dict[float, float]) -> tuple[list[list[float]], list[list[float]]]:
    return map_to_levels(bids, asks, DEPTH_LEVELS)


def filter_levels_near_mid(
    bids: list[list[float]],
    asks: list[list[float]],
    mid: float,
    range_pct: float = OB_WALL_RANGE_PCT,
) -> tuple[list[list[float]], list[list[float]]]:
    if mid <= 0:
        return bids, asks
    lower = mid * (1 - range_pct / 100)
    upper = mid * (1 + range_pct / 100)
    near_bids = [level for level in bids if lower <= level[0] <= upper]
    near_asks = [level for level in asks if lower <= level[0] <= upper]
    if not near_bids:
        near_bids = bids[: max(DEPTH_LEVELS, 1)]
    if not near_asks:
        near_asks = asks[: max(DEPTH_LEVELS, 1)]
    return near_bids, near_asks


def is_tradable_signal(signal: str, confidence: int, min_confidence: int = MIN_CONFIDENCE) -> bool:
    return signal in ("LONG", "SHORT") and confidence >= min_confidence


def zone_signal_with_hysteresis(position: float, stable_signal: str) -> str:
    if stable_signal == "LONG":
        return "NEUTRAL" if position > SIGNAL_ZONE_LONG_EXIT else "LONG"
    if stable_signal == "SHORT":
        return "NEUTRAL" if position < SIGNAL_ZONE_SHORT_EXIT else "SHORT"
    if position < SIGNAL_ZONE_LONG_ENTER:
        return "LONG"
    if position > SIGNAL_ZONE_SHORT_ENTER:
        return "SHORT"
    return "NEUTRAL"


def _decision_market_snapshot(
    signal: str,
    confidence: int,
    trend_bias: str,
    market_analysis: dict | None,
) -> dict:
    metrics = {
        "signal": signal,
        "confidence": confidence,
        "action": "TRADE" if is_tradable_signal(signal, confidence) else "WATCH",
        "market_analysis": market_analysis or {},
    }
    aligned = (
        signal_aligned_with_trend(signal, trend_bias)
        if signal in ("LONG", "SHORT")
        else None
    )
    return symbol_config.build_market_snapshot(metrics, trend_aligned=aligned)


def record_valid_entry(
    signal: str,
    entry: float | None,
    confidence: int,
    reasons: str,
    candle_time,
    trend_bias: str,
    trade_plan: dict | None = None,
    market_analysis: dict | None = None,
) -> None:
    global last_valid_entry_monotonic

    market = _decision_market_snapshot(signal, confidence, trend_bias, market_analysis)

    if not symbol_config.symbol_trading_enabled():
        log_decision_event(
            "valid_entry_blocked",
            outcome="blocked",
            block_reason="symbol_disabled",
            market_snapshot=market,
        )
        return

    if not is_tradable_signal(signal, confidence) or entry is None or candle_time is None:
        return

    if not signal_aligned_with_trend(signal, trend_bias):
        log_decision_event(
            "valid_entry_blocked",
            outcome="blocked",
            block_reason="htf_mismatch",
            market_snapshot=market,
        )
        return

    now = time.monotonic()
    if (
        last_valid_entry_monotonic is not None
        and now - last_valid_entry_monotonic < SIGNAL_COOLDOWN_SEC
    ):
        log_decision_event(
            "valid_entry_blocked",
            outcome="blocked",
            block_reason="signal_cooldown",
            market_snapshot=market,
        )
        return

    if valid_entries:
        last = valid_entries[-1]
        if last["t"] == candle_time and last["signal"] == signal:
            last["entry"] = entry
            last["confidence"] = confidence
            last["reasons"] = reasons
            return

    valid_entries.append(
        {
            "t": candle_time,
            "entry": entry,
            "signal": signal,
            "confidence": confidence,
            "reasons": reasons,
        }
    )
    last_valid_entry_monotonic = now
    append_event_log(
        "trades",
        "valid_entry",
        signal=signal,
        entry=entry,
        confidence=confidence,
        reasons=reasons,
        candle_time=str(candle_time),
        trend_bias=trend_bias,
    )
    log_decision_event(
        "valid_entry",
        outcome="recorded",
        market_snapshot=market,
    )
    execution.stage_valid_entry_snapshot(SYMBOL, market)
    sl_val = trade_plan.get("sl") if trade_plan else None
    tp1_val = trade_plan.get("tp1") if trade_plan else None
    telegram.notify_valid_entry(
        SYMBOL,
        signal,
        format_price(entry),
        confidence,
        reasons,
        trend_bias,
        format_price(sl_val) if sl_val else None,
        format_price(tp1_val) if tp1_val else None,
    )
    if trade_plan and trade_plan.get("active"):
        legs = trade_plan.get("legs") or []
        use_dca = TRADE_PLAN_EXECUTE_DCA and len(legs) > 1
        if use_dca and execution.TRADE_PLAN_DCA_SIGNAL_DRIVEN:
            execution._sync_dca_leg_count(SYMBOL)
            placed = execution.dca_legs_placed(SYMBOL)
            if execution.has_open_position(SYMBOL):
                leg_index = placed
            else:
                leg_index = 0
            if leg_index >= len(legs):
                log_decision_event(
                    "valid_entry_blocked",
                    outcome="blocked",
                    block_reason="dca_max_legs",
                    market_snapshot=market,
                )
                return
            leg = legs[leg_index]
            dca_entry = execution.resolve_dca_leg_entry_price(leg_index, leg, float(entry))
            if leg_index >= 1 and execution.TRADE_PLAN_DCA_ADVERSE_ONLY:
                allowed, block_reason = execution.can_place_dca_add(
                    SYMBOL,
                    signal,
                    dca_entry,
                    size_pct=float(leg["size_pct"]),
                    max_legs=len(legs),
                )
                if not allowed:
                    log_decision_event(
                        "valid_entry_blocked",
                        outcome="blocked",
                        block_reason=block_reason,
                        market_snapshot=market,
                    )
                    return
            size_pct = float(leg["size_pct"])
        elif use_dca:
            size_pct = sum(float(leg["size_pct"]) for leg in legs)
        else:
            size_pct = float(legs[0]["size_pct"]) if legs else float(
                trade_plan.get("partial_close_pct", 50)
            )
        blocked, balance_reason = execution.fleet_side_balance_blocks(
            signal,
            execution.estimate_order_notional_usdt(size_pct),
        )
        if blocked:
            log_decision_event(
                "valid_entry_blocked",
                outcome="blocked",
                block_reason=balance_reason,
                market_snapshot=market,
            )
            return
    execution.try_execute_valid_entry(
        SYMBOL,
        signal,
        entry,
        trade_plan,
        reasons,
        trend_bias,
    )


def apply_signal_debounce(
    candidate: str,
    confidence: int,
    reasons: str,
    entry: float | None,
    candle_time,
    trend_bias: str,
    trade_plan: dict | None = None,
    market_analysis: dict | None = None,
) -> None:
    global stable_signal_dir, pending_signal, pending_signal_count
    global signal_dir, signal_confidence, signal_reasons, signal_entry

    if candidate == pending_signal:
        pending_signal_count += 1
    else:
        pending_signal = candidate
        pending_signal_count = 1

    if pending_signal_count >= SIGNAL_DEBOUNCE_COUNT and candidate != stable_signal_dir:
        append_event_log(
            "signals",
            "signal_stable",
            from_signal=stable_signal_dir,
            to_signal=candidate,
            confidence=confidence,
            reasons=reasons,
            entry=entry,
            trend_bias=trend_bias,
        )
        if is_tradable_signal(candidate, confidence):
            effective_plan = trade_plan
            analysis = market_analysis
            if not effective_plan or not effective_plan.get("active"):
                if analysis is None:
                    closed_rows = [row for row in candles if row.get("x")]
                    htf_closed_rows = [row for row in htf_candles if row.get("x")]
                    price = latest_price or entry
                    analysis = compute_market_analysis(
                        closed_rows,
                        htf_closed_rows,
                        price,
                        support=support,
                        resistance=resistance,
                        ob_zone_pct=zone_position_pct,
                    )
                effective_plan = compute_trade_plan(
                    candidate,
                    confidence,
                    entry,
                    support,
                    resistance,
                    latest_price or entry,
                    analysis,
                    MIN_CONFIDENCE,
                )
                if not effective_plan.get("active"):
                    effective_plan = None
                    log_decision_event(
                        "valid_entry_blocked",
                        outcome="blocked",
                        block_reason="no_active_plan",
                        market_snapshot=_decision_market_snapshot(
                            candidate, confidence, trend_bias, analysis
                        ),
                    )

            if analysis is None:
                closed_rows = [row for row in candles if row.get("x")]
                htf_closed_rows = [row for row in htf_candles if row.get("x")]
                analysis = compute_market_analysis(
                    closed_rows,
                    htf_closed_rows,
                    latest_price or entry,
                    support=support,
                    resistance=resistance,
                    ob_zone_pct=zone_position_pct,
                )

            filter_result = evaluate_indicator_filters(
                candidate,
                confidence,
                analysis,
                indicator_filter_settings(),
            )
            if not filter_result.allowed:
                log_decision_event(
                    "indicator_blocked",
                    outcome="blocked",
                    block_reason=filter_result.block_reason,
                    market_snapshot=_decision_market_snapshot(
                        candidate, confidence, trend_bias, analysis
                    ),
                )
            elif effective_plan and effective_plan.get("active"):
                entry_reasons = reasons
                if filter_result.notes:
                    entry_reasons = f"{reasons} · {filter_result.notes}".strip(" · ")
                record_valid_entry(
                    candidate,
                    entry,
                    filter_result.confidence,
                    entry_reasons,
                    candle_time,
                    trend_bias,
                    effective_plan,
                    analysis,
                )
        stable_signal_dir = candidate

    signal_dir = stable_signal_dir
    if pending_signal_count >= SIGNAL_DEBOUNCE_COUNT:
        signal_confidence = confidence
        signal_reasons = reasons
        signal_entry = entry


def analyze_order_book(
    bids: list[list[float]], asks: list[list[float]]
) -> tuple[float, float, float, float, float, float]:
    best_bid = bids[0][0] if bids else 0.0
    best_ask = asks[0][0] if asks else 0.0
    support_wall = max(bids, key=lambda level: level[1]) if bids else [0.0, 0.0]
    resistance_wall = max(asks, key=lambda level: level[1]) if asks else [0.0, 0.0]
    return (
        support_wall[0],
        resistance_wall[0],
        best_bid,
        best_ask,
        support_wall[1],
        resistance_wall[1],
    )


def determine_signal(
    current_price: float | None,
    support_level: float,
    resistance_level: float,
    best_bid: float,
    best_ask: float,
    change_24h_pct: float | None,
    stable_signal: str = "NEUTRAL",
) -> tuple[str, int, str, float | None]:
    signal = "NEUTRAL"
    confidence = 0
    reasons = ""
    entry = current_price

    if (
        current_price
        and current_price > 0
        and support_level > 0
        and resistance_level > 0
    ):
        price_range = resistance_level - support_level
        if price_range > 0:
            position = (current_price - support_level) * 100 / price_range
            zone_signal = zone_signal_with_hysteresis(position, stable_signal)

            if zone_signal == "LONG":
                signal = "LONG"
                confidence = 65
                entry = support_level * 1.001
                if best_bid > 0 and entry > best_bid:
                    entry = best_bid
                reasons = "OB: near support"
                if change_24h_pct is not None and change_24h_pct < -3:
                    confidence += 15
                    reasons += " + reversal"

            elif zone_signal == "SHORT":
                signal = "SHORT"
                confidence = 65
                entry = resistance_level * 0.999
                if best_ask > 0 and entry < best_ask:
                    entry = best_ask
                reasons = "OB: near resistance"
                if change_24h_pct is not None and change_24h_pct > 3:
                    confidence += 15
                    reasons += " + reversal"

    if signal == "NEUTRAL" and current_price and current_price > 0 and change_24h_pct is not None:
        if change_24h_pct > 5:
            signal = "SHORT"
            confidence = 50
            entry = current_price * 0.998
            reasons = f"24h extreme gain (+{change_24h_pct:.2f}%)"
        elif change_24h_pct < -5:
            signal = "LONG"
            confidence = 50
            entry = current_price * 1.002
            reasons = f"24h extreme loss ({change_24h_pct:.2f}%)"
        elif change_24h_pct > 2:
            signal = "LONG"
            confidence = 40
            entry = current_price * 1.001
            reasons = f"uptrend (+{change_24h_pct:.2f}%)"
        elif change_24h_pct < -2:
            signal = "SHORT"
            confidence = 40
            entry = current_price * 0.999
            reasons = f"downtrend ({change_24h_pct:.2f}%)"

    return signal, confidence, reasons, entry


def update_trading_signal(
    bids: list[list[float]],
    asks: list[list[float]],
    current_price: float | None,
    change_24h_pct: float | None,
) -> None:
    global support, resistance, zone_position_pct, ob_support_qty, ob_resistance_qty

    if not bids or not asks:
        return

    mid = current_price
    if mid is None:
        mid = (bids[0][0] + asks[0][0]) / 2
    bids, asks = filter_levels_near_mid(bids, asks, mid)

    support_level, resistance_level, best_bid, best_ask, support_qty, resistance_qty = analyze_order_book(
        bids, asks
    )
    candidate, confidence, reasons, entry = determine_signal(
        current_price,
        support_level,
        resistance_level,
        best_bid,
        best_ask,
        change_24h_pct,
        stable_signal_dir,
    )

    support = support_level
    resistance = resistance_level
    ob_support_qty = support_qty
    ob_resistance_qty = resistance_qty

    if (
        current_price
        and support_level > 0
        and resistance_level > support_level
    ):
        zone_position_pct = (current_price - support_level) * 100 / (resistance_level - support_level)
    else:
        zone_position_pct = None

    candle_time = forming_candle["t"] if forming_candle else None
    closed_rows = [row for row in candles if row.get("x")]
    htf_closed_rows = [row for row in htf_candles if row.get("x")]
    analysis = compute_market_analysis(
        closed_rows,
        htf_closed_rows,
        current_price,
        support=support_level,
        resistance=resistance_level,
        ob_zone_pct=zone_position_pct,
    )
    current_plan = compute_trade_plan(
        candidate if candidate in ("LONG", "SHORT") else stable_signal_dir,
        confidence if pending_signal_count >= SIGNAL_DEBOUNCE_COUNT - 1 else signal_confidence,
        entry if entry is not None else signal_entry,
        support,
        resistance,
        current_price,
        analysis,
        MIN_CONFIDENCE,
    )
    apply_signal_debounce(
        candidate,
        confidence,
        reasons,
        entry,
        candle_time,
        analysis.get("htf_bias", "NEUTRAL"),
        current_plan if current_plan.get("active") else None,
        analysis,
    )


def update_metrics(
    display_bids: list[list[float]],
    display_asks: list[list[float]],
    analysis_bids: list[list[float]] | None = None,
    analysis_asks: list[list[float]] | None = None,
) -> None:
    global spread, spread_pct, volume_delta, bid_volume, ask_volume, latest_price

    ob_bids = analysis_bids or display_bids
    ob_asks = analysis_asks or display_asks

    bid_volume = sum(q for _, q in display_bids)
    ask_volume = sum(q for _, q in display_asks)
    volume_delta = bid_volume - ask_volume

    if display_bids and display_asks:
        best_bid = display_bids[0][0]
        best_ask = display_asks[0][0]
        spread = best_ask - best_bid
        mid = (best_bid + best_ask) / 2
        spread_pct = (spread / mid) * 100 if mid else None
        latest_price = latest_price or (display_bids[0][0] + display_asks[0][0]) / 2

    update_trading_signal(ob_bids, ob_asks, latest_price, change_24h)


def sync_orderbook_state(bid_map: dict[float, float], ask_map: dict[float, float]) -> None:
    global orderbook, analysis_orderbook

    display_bids, display_asks = map_to_levels(bid_map, ask_map, DEPTH_LEVELS)
    full_bids, full_asks = map_to_levels(bid_map, ask_map)
    orderbook = {"bids": display_bids, "asks": display_asks}
    analysis_orderbook = {"bids": full_bids, "asks": full_asks}
    update_metrics(display_bids, display_asks, full_bids, full_asks)


async def fetch_depth_snapshot(session: aiohttp.ClientSession) -> dict:
    url = f"{REST_BASE}/api/v3/depth"
    params = {"symbol": SYMBOL.upper(), "limit": 1000}
    async with session.get(url, params=params, timeout=10) as resp:
        resp.raise_for_status()
        return await resp.json()


async def fetch_24h_ticker(session: aiohttp.ClientSession) -> float | None:
    url = f"{REST_BASE}/api/v3/ticker/24hr"
    params = {"symbol": SYMBOL.upper()}
    async with session.get(url, params=params, timeout=10) as resp:
        resp.raise_for_status()
        data = await resp.json()
    try:
        return float(data["priceChangePercent"])
    except (KeyError, TypeError, ValueError):
        return None


async def fetch_historical_klines(
    session: aiohttp.ClientSession,
    interval: str = INTERVAL,
    limit: int | None = None,
) -> list[dict]:
    url = f"{REST_BASE}/api/v3/klines"
    candle_limit = limit if limit is not None else min(MAX_CANDLES, 500)
    params = {
        "symbol": SYMBOL.upper(),
        "interval": interval,
        "limit": candle_limit,
    }
    async with session.get(url, params=params, timeout=10) as resp:
        resp.raise_for_status()
        rows = await resp.json()

    return [
        {
            "t": pd.to_datetime(row[0], unit="ms"),
            "o": float(row[1]),
            "h": float(row[2]),
            "l": float(row[3]),
            "c": float(row[4]),
            "v": float(row[5]),
            "x": True,
        }
        for row in rows
    ]


async def sync_orderbook(session: aiohttp.ClientSession, depth_buffer: list[dict]) -> tuple[dict[float, float], dict[float, float], int]:
    snapshot = await fetch_depth_snapshot(session)
    last_update_id = snapshot["lastUpdateId"]

    bid_map = {float(p): float(q) for p, q in snapshot["bids"]}
    ask_map = {float(p): float(q) for p, q in snapshot["asks"]}

    valid_events: list[dict] = []
    for event in depth_buffer:
        if event["u"] <= last_update_id:
            continue
        if event["U"] <= last_update_id + 1 <= event["u"]:
            valid_events.append(event)
            break

    if not valid_events:
        return bid_map, ask_map, last_update_id

    start_idx = depth_buffer.index(valid_events[0])
    for event in depth_buffer[start_idx:]:
        if event["u"] < last_update_id:
            continue
        apply_depth_update(event, bid_map, ask_map)
        last_update_id = event["u"]

    return bid_map, ask_map, last_update_id


def reload_symbol_config_from_db(*, force: bool = False) -> dict:
    result = symbol_config.reload_from_db(globals(), SYMBOL, force=force)
    if result.get("needs_ws_reconnect"):
        ws_force_reconnect.set()
        logger.info("%s: HTF/LTF interval changed — reconnecting WebSocket", SYMBOL.upper())
    return result


def config_poll_loop() -> None:
    while True:
        time.sleep(max(5, DB_CONFIG_POLL_SEC))
        if not db_store.is_enabled():
            continue
        try:
            result = reload_symbol_config_from_db()
            if result.get("changed"):
                logger.info(
                    "%s: polled DB config v%s (%s)",
                    SYMBOL.upper(),
                    result.get("config_version"),
                    ", ".join(result.get("applied") or []) or "defaults",
                )
        except Exception as exc:
            logger.warning("%s: DB config poll failed: %s", SYMBOL.upper(), exc)


def _websocket_reconnect_delay(exc: Exception, attempt: int) -> float:
    """Backoff + honor Binance -1003 IP ban window."""
    message = str(exc)
    lower = message.lower()
    if "-1003" in message or "too many requests" in lower or "banned until" in lower:
        match = re.search(r"banned until (\d+)", message)
        if match:
            until_s = int(match.group(1)) / 1000.0
            return max(60.0, until_s - time.time() + 5.0)
        return min(600.0, 90.0 * (2 ** min(attempt, 3)))

    base = min(120.0, 3.0 * (2 ** min(attempt, 6)))
    return base + random.uniform(0.0, min(15.0, base * 0.25))


async def ws_loop() -> None:
    global forming_candle, htf_forming_candle, latest_price, orderbook, ws_status, change_24h

    depth_buffer: list[dict] = []
    last_htf_interval = HTF_INTERVAL
    reconnect_attempt = 0

    stagger_s = (hash(SYMBOL.upper()) % 30) + random.uniform(0.0, 2.0)
    logger.info("%s: WebSocket stagger %.1fs (reduces REST burst on fleet start)", SYMBOL.upper(), stagger_s)
    await asyncio.sleep(stagger_s)

    async with aiohttp.ClientSession() as session:
        history = await fetch_historical_klines(session, INTERVAL, min(MAX_CANDLES, 500))
        htf_history = await fetch_historical_klines(session, HTF_INTERVAL, min(HTF_CANDLES, 500))
        initial_change = await fetch_24h_ticker(session)
        with state_lock:
            candles.extend(history)
            htf_candles.extend(htf_history)
            if initial_change is not None:
                change_24h = initial_change

        while True:
            current_ltf = INTERVAL
            current_htf = HTF_INTERVAL
            stream_url = (
                f"{WS_BASE}/stream?streams="
                f"{SYMBOL}@kline_{current_ltf}/{SYMBOL}@kline_{current_htf}/"
                f"{SYMBOL}@depth@100ms/{SYMBOL}@miniTicker"
            )

            if current_htf != last_htf_interval:
                try:
                    htf_history = await fetch_historical_klines(
                        session, current_htf, min(HTF_CANDLES, 500)
                    )
                    with state_lock:
                        htf_candles.clear()
                        htf_forming_candle = None
                        htf_candles.extend(htf_history)
                    last_htf_interval = current_htf
                    logger.info("%s: reloaded HTF klines for %s", SYMBOL.upper(), current_htf)
                except Exception as exc:
                    logger.warning("%s: HTF history reload failed: %s", SYMBOL.upper(), exc)

            try:
                ws_force_reconnect.clear()
                ws_status = "connecting"
                depth_buffer.clear()

                async with websockets.connect(
                    stream_url,
                    ping_interval=30,
                    ping_timeout=60,
                    close_timeout=10,
                ) as ws:
                    logger.info("WebSocket connected for %s", SYMBOL.upper())
                    ws_status = "buffering depth"

                    while len(depth_buffer) < 3:
                        msg = json.loads(await ws.recv())
                        data = msg.get("data", {})
                        if "U" in data and "u" in data:
                            depth_buffer.append(data)

                    bid_map, ask_map, last_update_id = await sync_orderbook(session, depth_buffer)

                    with state_lock:
                        sync_orderbook_state(bid_map, ask_map)

                    ws_status = "live"
                    reconnect_attempt = 0
                    logger.info("Order book synced at updateId=%s", last_update_id)

                    async for raw in ws:
                        if ws_force_reconnect.is_set():
                            logger.info("%s: config change — reconnecting WebSocket", SYMBOL.upper())
                            break

                        msg = json.loads(raw)
                        data = msg.get("data", {})

                        if "k" in data:
                            k = data["k"]
                            row = kline_row(k)
                            interval = k.get("i", current_ltf)

                            with state_lock:
                                if interval == current_htf:
                                    htf_forming_candle = row
                                    if row["x"]:
                                        if htf_candles and htf_candles[-1]["t"] == row["t"]:
                                            htf_candles[-1] = row
                                        else:
                                            htf_candles.append(row)
                                elif interval == current_ltf:
                                    latest_price = row["c"]
                                    forming_candle = row
                                    if row["x"]:
                                        candles.append(row)
                                    if orderbook.get("bids") and orderbook.get("asks"):
                                        update_metrics(
                                            orderbook["bids"],
                                            orderbook["asks"],
                                            analysis_orderbook.get("bids"),
                                            analysis_orderbook.get("asks"),
                                        )

                        elif data.get("e") == "24hrMiniTicker" or msg.get("stream", "").endswith("@miniTicker"):
                            pct_raw = data.get("P")
                            if pct_raw is not None:
                                try:
                                    with state_lock:
                                        change_24h = float(pct_raw)
                                        if orderbook.get("bids") and orderbook.get("asks"):
                                            update_metrics(
                                                orderbook["bids"],
                                                orderbook["asks"],
                                                analysis_orderbook.get("bids"),
                                                analysis_orderbook.get("asks"),
                                            )
                                except (TypeError, ValueError):
                                    log_error("mini_ticker_parse_error", raw=pct_raw)
                                    logger.warning("Invalid miniTicker change pct: %r", pct_raw)
                            else:
                                logger.debug("miniTicker event without P field: %s", data)

                        elif "U" in data and "u" in data:
                            if data["u"] <= last_update_id:
                                continue
                            if not (data["U"] <= last_update_id + 1 <= data["u"]):
                                log_error("orderbook_desync", last_update_id=last_update_id, event=data)
                                logger.warning("Order book desync detected, resyncing...")
                                break

                            apply_depth_update(data, bid_map, ask_map)
                            last_update_id = data["u"]

                            with state_lock:
                                sync_orderbook_state(bid_map, ask_map)

            except Exception as exc:
                delay = _websocket_reconnect_delay(exc, reconnect_attempt)
                reconnect_attempt += 1
                log_error(
                    "websocket_loop_error",
                    error=str(exc),
                    reconnect_in_sec=round(delay, 1),
                    attempt=reconnect_attempt,
                )
                logger.warning(
                    "%s: WebSocket error (%s), reconnect in %.0fs (attempt %s)",
                    SYMBOL.upper(),
                    exc,
                    delay,
                    reconnect_attempt,
                )
                ws_status = "reconnecting"
                await asyncio.sleep(delay)


def resolve_confirmed_pattern(closed_rows: list[dict]) -> str | None:
    if not closed_rows:
        return None
    prior = pd.DataFrame(closed_rows[:-1]) if len(closed_rows) > 1 else None
    last_closed = pd.Series(closed_rows[-1])
    raw_pattern = detect_pattern(
        last_closed,
        prior_rows=prior,
        support_level=support,
        resistance_level=resistance,
    )
    return confirmed_pattern(raw_pattern, signal_dir, signal_confidence)


def get_candles_df() -> pd.DataFrame:
    global latest_pattern
    with state_lock:
        rows = list(candles)
        if forming_candle and (not rows or forming_candle["t"] != rows[-1]["t"]):
            rows = rows + [forming_candle.copy()]
        elif forming_candle and rows and forming_candle["t"] == rows[-1]["t"]:
            rows = rows[:-1] + [forming_candle.copy()]

        closed_rows = [row for row in candles if row.get("x")]
        htf_closed_rows = [row for row in htf_candles if row.get("x")]
        confirmed = resolve_confirmed_pattern(closed_rows)
        latest_pattern = confirmed or "None"
        market_analysis = compute_market_analysis(
            closed_rows,
            htf_closed_rows,
            latest_price,
            support=support,
            resistance=resistance,
            ob_zone_pct=zone_position_pct,
        )

        ob = {
            "bids": list(orderbook.get("bids", [])),
            "asks": list(orderbook.get("asks", [])),
        }
        best_bid = ob["bids"][0][0] if ob["bids"] else None
        best_ask = ob["asks"][0][0] if ob["asks"] else None
        metrics = {
            "pattern": latest_pattern,
            "price": latest_price,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": spread,
            "spread_pct": spread_pct,
            "volume_delta": volume_delta,
            "bid_volume": bid_volume,
            "ask_volume": ask_volume,
            "status": ws_status,
            "signal": signal_dir,
            "confidence": signal_confidence,
            "signal_reasons": signal_reasons,
            "signal_entry": signal_entry,
            "support": support,
            "resistance": resistance,
            "support_qty": ob_support_qty,
            "resistance_qty": ob_resistance_qty,
            "zone_position_pct": zone_position_pct,
            "change_24h": change_24h,
            "min_confidence": MIN_CONFIDENCE,
            "valid_entries": list(valid_entries),
            "pending_signal": pending_signal,
            "pending_signal_count": pending_signal_count,
            "signal_debounce_count": SIGNAL_DEBOUNCE_COUNT,
            "market_analysis": market_analysis,
            "require_trend_align": REQUIRE_TREND_ALIGN,
            "signal_cooldown_sec": SIGNAL_COOLDOWN_SEC,
            "trade_plan": (
                _resolved_trade_plan := resolve_trade_plan(
                    compute_trade_plan(
                        signal_dir,
                        signal_confidence,
                        signal_entry,
                        support,
                        resistance,
                        latest_price,
                        market_analysis,
                        MIN_CONFIDENCE,
                    ),
                    signal_dir,
                )
            ),
            "log_dir": LOG_DIR,
        }
        exposure = execution.get_exchange_exposure(SYMBOL)
        maintain_plan = _resolved_trade_plan
        if not maintain_plan.get("active") and exposure.get("open"):
            maintain_plan = trade_plan_for_position_reconcile(
                exposure,
                support=support,
                resistance=resistance,
                price=latest_price,
                market_analysis=market_analysis,
            )
        if maintain_plan.get("active"):
            execution.run_execution_maintenance(
                SYMBOL,
                direction=maintain_plan.get("signal"),
                sl=float(maintain_plan["sl"]) if maintain_plan.get("sl") else None,
                tp=float(maintain_plan["tp1"]) if maintain_plan.get("tp1") else None,
            )
        else:
            execution.run_execution_maintenance(SYMBOL)
        metrics["execution"] = {
            **execution.get_execution_status(),
            "position": exposure,
        }

    return pd.DataFrame(rows), ob, metrics


def depth_category_labels(bids: list[list[float]], asks: list[list[float]]) -> list[str]:
    prices = sorted({price for price, _ in bids} | {price for price, _ in asks})
    return [price_label(price) for price in prices]


def depth_chart_x_range(
    bids: list[list[float]],
    asks: list[list[float]],
    *,
    padding_pct: float = DEPTH_CHART_PADDING_PCT,
) -> tuple[float, float] | None:
    """Symmetric quantity axis so 0 stays centered (← bids | asks →)."""
    max_bid = max((qty for _, qty in bids), default=0.0)
    max_ask = max((qty for _, qty in asks), default=0.0)
    peak = max(max_bid, max_ask)
    if peak <= 0:
        return None
    pad = max(padding_pct, 0.0)
    limit = peak * (1 + pad)
    return (-limit, limit)


def side_bar_colors(
    levels: list[list[float]],
    wall_price: float | None,
    base_color: str,
    wall_color: str,
) -> list[str]:
    wall_label = price_label(wall_price) if wall_price else None
    return [wall_color if price_label(price) == wall_label else base_color for price, _ in levels]


def add_order_block_overlays(
    fig: go.Figure,
    metrics: dict,
    row: int = 1,
    col: int = 1,
) -> None:
    support = metrics.get("support")
    resistance = metrics.get("resistance")
    if not support or not resistance or resistance <= support:
        return

    current_price = metrics.get("price")
    if current_price:
        max_dist = current_price * (OB_WALL_RANGE_PCT / 100) * 1.5
        if abs(float(support) - current_price) > max_dist or abs(float(resistance) - current_price) > max_dist:
            return

    fig.add_hline(
        y=support,
        line_color="rgba(0,193,118,0.95)",
        line_width=2,
        row=row,
        col=col,
    )
    fig.add_hline(
        y=resistance,
        line_color="rgba(255,77,79,0.95)",
        line_width=2,
        row=row,
        col=col,
    )

    if current_price:
        fig.add_hline(
            y=current_price,
            line_dash="dash",
            line_color="rgba(255,193,7,0.85)",
            line_width=1,
            row=row,
            col=col,
        )


def chart_use_line_mode(df: pd.DataFrame) -> bool:
    """Use close line chart when 1m candles are too flat for readable candlesticks (e.g. ETC)."""
    lookback = min(len(df), 60)
    if lookback < 10:
        return False
    recent = df.tail(lookback)
    ref = float(recent["c"].median()) or float(recent["c"].iloc[-1]) or 1.0
    avg_body = float((recent["c"] - recent["o"]).abs().mean())
    avg_range = float((recent["h"] - recent["l"]).mean())
    return avg_body < ref * 0.0001 or avg_range < ref * 0.00012


def add_price_chart(fig: go.Figure, df: pd.DataFrame, row: int = 1, col: int = 1) -> str:
    """Add candlesticks or close line depending on pair volatility. Returns chart mode label."""
    if chart_use_line_mode(df):
        closes = df["c"].astype(float)
        prev = closes.shift(1).fillna(closes.iloc[0])
        marker_colors = [
            "#00c176" if c >= p else "#ff4d4f"
            for c, p in zip(closes, prev, strict=False)
        ]
        hovers = [
            f"O {format_price(float(r.o))} H {format_price(float(r.h))} "
            f"L {format_price(float(r.l))} C {format_price(float(r.c))}"
            for r in df.itertuples()
        ]
        fig.add_trace(
            go.Scatter(
                x=df["t"],
                y=closes,
                mode="lines+markers",
                name=f"{SYMBOL.upper()} close",
                line=dict(color="rgba(88,166,255,0.85)", width=1.5),
                marker=dict(size=4, color=marker_colors, line=dict(width=0)),
                text=hovers,
                hoverinfo="text+x",
            ),
            row=row,
            col=col,
        )
        return "line"

    fig.add_trace(
        go.Candlestick(
            x=df["t"],
            open=df["o"],
            high=df["h"],
            low=df["l"],
            close=df["c"],
            increasing=dict(
                line=dict(color="#00c176", width=1),
                fillcolor="rgba(0,193,118,0.9)",
            ),
            decreasing=dict(
                line=dict(color="#ff4d4f", width=1),
                fillcolor="rgba(255,77,79,0.9)",
            ),
            whiskerwidth=0.4,
            name=SYMBOL.upper(),
        ),
        row=row,
        col=col,
    )
    return "candles"


def candle_chart_y_range(df: pd.DataFrame, metrics: dict) -> tuple[float, float]:
    lookback = min(len(df), max(CANDLE_CHART_LOOKBACK, 30))
    recent = df.tail(lookback)
    bar_ranges = (recent["h"] - recent["l"]).astype(float)
    closes = recent["c"].astype(float)
    mid = metrics.get("price") or float(closes.iloc[-1])

    y_min = float(min(closes.quantile(0.04), recent["l"].quantile(0.04)))
    y_max = float(max(closes.quantile(0.96), recent["h"].quantile(0.96)))
    span = max(y_max - y_min, 1e-12)

    typical_range = float(bar_ranges.quantile(0.75)) if len(bar_ranges) >= 5 else float(bar_ranges.mean())
    min_span = max(typical_range * 8, mid * 0.0018, 0.003)
    if span < min_span:
        center = mid if mid else (y_min + y_max) / 2
        half = min_span / 2
        y_min = center - half
        y_max = center + half
        span = min_span

    max_extend = span * 0.1

    def maybe_extend(value: float | None) -> None:
        nonlocal y_min, y_max, span
        if value is None:
            return
        v = float(value)
        if v < y_min and y_min - v <= max_extend:
            y_min = v
            span = y_max - y_min
        elif v > y_max and v - y_max <= max_extend:
            y_max = v
            span = y_max - y_min

    for entry in metrics.get("valid_entries") or []:
        maybe_extend(entry.get("entry"))

    active_entry = metrics.get("signal_entry")
    signal = metrics.get("signal", "NEUTRAL")
    confidence = metrics.get("confidence", 0)
    if active_entry is not None and is_tradable_signal(signal, confidence, metrics.get("min_confidence", MIN_CONFIDENCE)):
        maybe_extend(active_entry)

    plan = metrics.get("trade_plan") or {}
    if plan.get("active"):
        for key in ("sl", "tp1", "tp2", "avg_entry"):
            maybe_extend(plan.get(key))
        for leg in plan.get("legs") or []:
            maybe_extend(leg.get("price"))

    padding = max(span * 0.05, mid * 0.00015)
    return y_min - padding, y_max + padding


def add_valid_entry_markers(
    fig: go.Figure,
    metrics: dict,
    row: int = 1,
    col: int = 1,
) -> None:
    entries = metrics.get("valid_entries") or []
    long_entries = [entry for entry in entries if entry["signal"] == "LONG"]
    short_entries = [entry for entry in entries if entry["signal"] == "SHORT"]

    if long_entries:
        fig.add_trace(
            go.Scatter(
                x=[entry["t"] for entry in long_entries],
                y=[entry["entry"] for entry in long_entries],
                mode="markers+text",
                name="Valid LONG",
                text=[f"L {format_price(entry['entry'])}" for entry in long_entries],
                textposition="bottom center",
                textfont=dict(size=10, color="#00c176"),
                marker=dict(size=11, color="#00c176", symbol="triangle-up", line=dict(width=1, color="#ffffff")),
                hovertemplate=(
                    "Valid LONG entry<br>Price: %{y}<br>Conf: %{customdata[0]}%<br>%{customdata[1]}<extra></extra>"
                ),
                customdata=[
                    [entry["confidence"], entry["reasons"]] for entry in long_entries
                ],
            ),
            row=row,
            col=col,
        )

    if short_entries:
        fig.add_trace(
            go.Scatter(
                x=[entry["t"] for entry in short_entries],
                y=[entry["entry"] for entry in short_entries],
                mode="markers+text",
                name="Valid SHORT",
                text=[f"S {format_price(entry['entry'])}" for entry in short_entries],
                textposition="top center",
                textfont=dict(size=10, color="#ff4d4f"),
                marker=dict(size=11, color="#ff4d4f", symbol="triangle-down", line=dict(width=1, color="#ffffff")),
                hovertemplate=(
                    "Valid SHORT entry<br>Price: %{y}<br>Conf: %{customdata[0]}%<br>%{customdata[1]}<extra></extra>"
                ),
                customdata=[
                    [entry["confidence"], entry["reasons"]] for entry in short_entries
                ],
            ),
            row=row,
            col=col,
        )

    signal = metrics.get("signal", "NEUTRAL")
    confidence = metrics.get("confidence", 0)
    min_conf = metrics.get("min_confidence", MIN_CONFIDENCE)
    entry_price = metrics.get("signal_entry")
    if is_tradable_signal(signal, confidence, min_conf) and entry_price:
        line_color = "rgba(0,193,118,0.55)" if signal == "LONG" else "rgba(255,77,79,0.55)"
        fig.add_hline(
            y=entry_price,
            line_dash="dot",
            line_color=line_color,
            line_width=1.5,
            row=row,
            col=col,
        )


def add_trade_plan_overlays(
    fig: go.Figure,
    metrics: dict,
    row: int = 1,
    col: int = 1,
) -> None:
    plan = metrics.get("trade_plan") or {}
    if not plan.get("active"):
        return

    fig.add_hline(
        y=plan["sl"],
        line_dash="dash",
        line_color="rgba(255,77,79,0.75)",
        line_width=1.5,
        row=row,
        col=col,
    )
    fig.add_hline(
        y=plan["avg_entry"],
        line_dash="dashdot",
        line_color="rgba(88,166,255,0.6)",
        line_width=1,
        row=row,
        col=col,
    )
    fig.add_hline(
        y=plan["tp1"],
        line_dash="dot",
        line_color="rgba(0,193,118,0.7)",
        line_width=1.5,
        row=row,
        col=col,
    )
    fig.add_hline(
        y=plan["tp2"],
        line_dash="dot",
        line_color="rgba(0,193,118,0.45)",
        line_width=1,
        row=row,
        col=col,
    )


def build_figure() -> go.Figure:
    df, ob, metrics = get_candles_df()
    price_chart_mode = (
        "line" if not df.empty and chart_use_line_mode(df) else "candles"
    )
    price_subtitle = (
        "Price (line) + Order Blocks"
        if price_chart_mode == "line"
        else "OHLC + Order Blocks"
    )

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.1,
        row_heights=[0.72, 0.28],
        subplot_titles=(price_subtitle, "Order Book Depth"),
    )

    if not df.empty:
        add_price_chart(fig, df, row=1, col=1)

        closed_df = df[df["x"]] if "x" in df.columns else df.iloc[:-1]
        pattern_x: list = []
        pattern_y: list = []
        pattern_text: list = []
        pattern_hover: list = []
        if not closed_df.empty:
            confirmed = metrics.get("pattern")
            if confirmed and confirmed != "None":
                row = closed_df.iloc[-1]
                marker_label = pattern_marker_label(
                    confirmed,
                    metrics.get("signal", "NEUTRAL"),
                    metrics.get("confidence", 0),
                )
                pattern_x.append(row["t"])
                pattern_y.append(row["c"])
                pattern_text.append(marker_label)
                pattern_hover.append(marker_label)

        if pattern_x:
            fig.add_trace(
                go.Scatter(
                    x=pattern_x,
                    y=pattern_y,
                    mode="markers+text",
                    text=pattern_text,
                    textposition="top center",
                    marker=dict(size=10, color="gold", symbol="diamond"),
                    showlegend=False,
                    hovertemplate="%{customdata}<br>Close: %{y}<extra></extra>",
                    customdata=pattern_hover,
                ),
                row=1,
                col=1,
            )

        add_order_block_overlays(fig, metrics, row=1, col=1)
        add_valid_entry_markers(fig, metrics, row=1, col=1)
        add_trade_plan_overlays(fig, metrics, row=1, col=1)
        y_min, y_max = candle_chart_y_range(df, metrics)
        tick_fmt = price_tick_format(metrics.get("price"))
        mid_price = metrics.get("price") or (y_min + y_max) / 2
        y_dtick = max((y_max - y_min) / 8, float(mid_price) * 0.0005)
        fig.update_yaxes(range=[y_min, y_max], tickformat=tick_fmt, dtick=y_dtick, row=1, col=1)

    bids = ob["bids"]
    asks = ob["asks"]
    depth_labels = depth_category_labels(bids, asks)

    if bids:
        fig.add_trace(
            go.Bar(
                x=[-qty for _, qty in bids],
                y=[price_label(price) for price, _ in bids],
                orientation="h",
                name="Bids",
                marker=dict(
                    color=side_bar_colors(
                        bids,
                        metrics.get("support"),
                        "rgba(0,193,118,0.45)",
                        "rgba(0,193,118,0.95)",
                    )
                ),
                hovertemplate="Bid<br>Price: %{y}<br>Qty: %{customdata}<extra></extra>",
                customdata=[qty for _, qty in bids],
            ),
            row=2,
            col=1,
        )

    if asks:
        fig.add_trace(
            go.Bar(
                x=[qty for _, qty in asks],
                y=[price_label(price) for price, _ in asks],
                orientation="h",
                name="Asks",
                marker=dict(
                    color=side_bar_colors(
                        asks,
                        metrics.get("resistance"),
                        "rgba(255,77,79,0.45)",
                        "rgba(255,77,79,0.95)",
                    )
                ),
                hovertemplate="Ask<br>Price: %{y}<br>Qty: %{customdata}<extra></extra>",
                customdata=[qty for _, qty in asks],
            ),
            row=2,
            col=1,
        )

    price_text = format_price(metrics.get("price"))
    signal_text = metrics.get("signal", "NEUTRAL")
    confidence_text = f"{metrics.get('confidence', 0)}%"

    fig.update_layout(
        title=dict(
            text=f"{SYMBOL.upper()} · {INTERVAL} · {signal_text} {confidence_text} · {price_text}",
            x=0.01,
            xanchor="left",
            font=dict(size=15),
        ),
        template="plotly_dark",
        height=900,
        barmode="overlay",
        xaxis_rangeslider_visible=False,
        legend_orientation="h",
        legend=dict(y=1.02, x=0, orientation="h"),
        margin=dict(l=20, r=20, t=56, b=20),
    )

    fig.update_xaxes(type="date", rangeslider_visible=False, tickformat="%H:%M", row=1, col=1)
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_xaxes(
        title_text="Quantity (← Bids | Asks →)",
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor="rgba(255,255,255,0.25)",
        row=2,
        col=1,
    )
    depth_x_range = depth_chart_x_range(bids, asks)
    if depth_x_range:
        fig.update_xaxes(range=list(depth_x_range), row=2, col=1)
    if depth_labels:
        fig.update_yaxes(
            title_text="Price level",
            type="category",
            categoryorder="array",
            categoryarray=depth_labels,
            row=2,
            col=1,
        )
    else:
        fig.update_yaxes(title_text="Price level", row=2, col=1)

    return fig


def trend_badge_class(bias: str) -> str:
    if bias == "BULLISH":
        return "badge badge-bull"
    if bias == "BEARISH":
        return "badge badge-bear"
    return "badge badge-neutral"


def rsi_badge_class(rsi: float | None) -> str:
    if rsi is None:
        return "badge badge-neutral"
    if rsi >= 70:
        return "badge badge-bear"
    if rsi <= 30:
        return "badge badge-bull"
    return "badge badge-neutral"


def macd_badge_class(histogram: float | None) -> str:
    if histogram is None:
        return "badge badge-neutral"
    if histogram > 0:
        return "badge badge-bull"
    if histogram < 0:
        return "badge badge-bear"
    return "badge badge-neutral"


def adx_badge_class(adx: float | None) -> str:
    if adx is None:
        return "badge badge-neutral"
    if adx >= ADX_MIN_TREND:
        return "badge badge-bull"
    return "badge badge-neutral"


def panel_section(title: str) -> html.Div:
    return html.Div(title, className="panel-section-title")


def kv_row(
    label: str,
    value,
    *,
    strong: bool = False,
    badge_class: str | None = None,
    value_class: str | None = None,
    hint: str | None = None,
) -> html.Div:
    if badge_class:
        value_node = html.Span(str(value), className=badge_class)
    elif value_class:
        value_node = html.Span(str(value), className=value_class)
    elif strong:
        value_node = html.Strong(str(value), className="kv-value")
    else:
        value_node = html.Span(str(value), className="kv-value")

    children = [
        html.Span(label, className="panel-label kv-label"),
        html.Div(
            [value_node, html.Span(hint, className="panel-hint")] if hint else [value_node],
            className="kv-value-wrap",
        ),
    ]
    return html.Div(children, className="kv-row")


def format_ob_proximity(metrics: dict) -> tuple[str, str]:
    """Which OB wall is nearest: support/resistance (0–100% between walls)."""
    zone = metrics.get("zone_position_pct")
    if zone is None:
        return "—", "none"

    zone = float(zone)
    if zone <= SIGNAL_ZONE_LONG_ENTER:
        return f"Support · {zone:.0f}%", "support"
    if zone >= SIGNAL_ZONE_SHORT_ENTER:
        return f"Resistance · {zone:.0f}%", "resistance"
    if zone < 50:
        return f"→ Support · {zone:.0f}%", "support-side"
    if zone > 50:
        return f"→ Resistance · {zone:.0f}%", "resistance-side"
    return f"Mid · {zone:.0f}%", "mid"


def ob_proximity_value_class(ob_near: str) -> str:
    if ob_near == "support":
        return "kv-value ob-near-support"
    if ob_near == "resistance":
        return "kv-value ob-near-resistance"
    if ob_near == "support-side":
        return "kv-value ob-near-support-side"
    if ob_near == "resistance-side":
        return "kv-value ob-near-resistance-side"
    return "kv-value"


def build_ob_zone_kv_row(metrics: dict) -> html.Div:
    ob_label, ob_near = format_ob_proximity(metrics)
    return kv_row("OB zone", ob_label, value_class=ob_proximity_value_class(ob_near))


def build_pattern_panel_children(metrics: dict) -> list:
    analysis = metrics.get("market_analysis") or {}

    children: list = [
        panel_section("Order block"),
        build_ob_zone_kv_row(metrics),
    ]
    support_wall = metrics.get("support")
    resistance_wall = metrics.get("resistance")
    if support_wall:
        children.append(
            kv_row(
                "Support wall",
                format_price(support_wall),
                strong=True,
                hint=(
                    f"qty {metrics['support_qty']:.4f}"
                    if metrics.get("support_qty") is not None
                    else None
                ),
            )
        )
    if resistance_wall:
        children.append(
            kv_row(
                "Resistance wall",
                format_price(resistance_wall),
                strong=True,
                hint=(
                    f"qty {metrics['resistance_qty']:.4f}"
                    if metrics.get("resistance_qty") is not None
                    else None
                ),
            )
        )

    children.extend(
        [
            panel_section("Pattern"),
            kv_row(
                "Confirmed",
                metrics["pattern"],
                badge_class=pattern_badge_class(metrics["pattern"]),
            ),
        ]
    )

    if metrics["pattern"] == "None":
        children.append(
            html.P(
                f"Hammer → LONG · Star → SHORT · min {metrics.get('min_confidence', MIN_CONFIDENCE)}%",
                className="panel-hint panel-footnote",
            )
        )
    else:
        children.append(
            kv_row(
                "Aligned signal",
                f"{metrics.get('signal')} {metrics.get('confidence')}%",
                badge_class=confidence_badge_class(
                    metrics.get("confidence", 0),
                    metrics.get("min_confidence", MIN_CONFIDENCE),
                ),
            )
        )

    htf_interval = analysis.get("htf_interval", HTF_INTERVAL)
    children.append(panel_section(f"Trend · {htf_interval}"))
    children.append(
        kv_row(
            "Bias",
            analysis.get("htf_bias", "NEUTRAL"),
            badge_class=trend_badge_class(analysis.get("htf_bias", "NEUTRAL")),
        )
    )
    htf_adx = analysis.get("htf_adx")
    htf_adx_text = f"{htf_adx:.1f}" if htf_adx is not None else "—"
    htf_adx_hint = (
        f"min {ADX_MIN_TREND} · HTF filter"
        if INDICATOR_FILTERS_ENABLED and ADX_FILTER_ENABLED and ADX_USE_HTF
        else None
    )
    children.append(
        kv_row(
            f"ADX ({ADX_PERIOD})",
            htf_adx_text,
            badge_class=adx_badge_class(htf_adx),
            hint=htf_adx_hint,
        )
    )

    children.append(panel_section(f"Indicators · {INTERVAL}"))
    ema_fast = analysis.get("ema_fast")
    ema_slow = analysis.get("ema_slow")
    ema_text = (
        f"{format_price(ema_fast)} / {format_price(ema_slow)}"
        if ema_fast is not None and ema_slow is not None
        else "—"
    )
    children.append(
        kv_row(
            f"EMA {EMA_FAST}/{EMA_SLOW}",
            ema_text,
            strong=True,
            hint=analysis.get("ema_cross"),
        )
    )

    rsi = analysis.get("rsi")
    rsi_text = f"{rsi:.1f}" if rsi is not None else "—"
    children.append(kv_row(f"RSI ({RSI_PERIOD})", rsi_text, badge_class=rsi_badge_class(rsi)))

    adx = analysis.get("adx")
    adx_text = f"{adx:.1f}" if adx is not None else "—"
    adx_hint = (
        f"min {ADX_MIN_TREND} · LTF"
        if INDICATOR_FILTERS_ENABLED and ADX_FILTER_ENABLED and not ADX_USE_HTF
        else (f"min {ADX_MIN_TREND} · LTF" if adx is not None else None)
    )
    children.append(
        kv_row(f"ADX ({ADX_PERIOD})", adx_text, badge_class=adx_badge_class(adx), hint=adx_hint)
    )

    macd = analysis.get("macd")
    macd_hist = analysis.get("macd_hist")
    if macd is not None and macd_hist is not None:
        macd_text = f"{macd:.2f} (hist {macd_hist:+.2f})"
    else:
        macd_text = "—"
    children.append(kv_row("MACD", macd_text, badge_class=macd_badge_class(macd_hist)))

    children.append(panel_section(f"Structure · {HTF_INTERVAL}"))
    smc = analysis.get("smc") or {}
    smc_trend = smc.get("trend", "RANGING")
    children.append(
        kv_row(
            "Market structure",
            smc_trend,
            badge_class=trend_badge_class(smc_trend),
            hint=smc.get("sequence"),
        )
    )
    smc_event = smc.get("pattern", "—")
    smc_event_type = smc.get("pattern_type", "none")
    children.append(
        kv_row(
            "Pattern",
            smc_event,
            badge_class=structure_event_badge_class(smc_event_type),
            hint=smc.get("sequence"),
        )
    )
    pd_zone = smc.get("pd_zone", "—")
    pd_pct = smc.get("pd_pct")
    pd_text = f"{pd_zone} · {pd_pct:.0f}%" if pd_pct is not None and pd_zone != "—" else pd_zone
    children.append(kv_row("P/D zone", pd_text, value_class=pd_zone_value_class(pd_zone)))
    children.append(
        kv_row(
            "SMC detail",
            smc.get("state", "—"),
            strong=True,
            hint=smc.get("state_hint"),
        )
    )

    fvg = analysis.get("fvg")
    fvg_label = analysis.get("fvg_label", "—")
    fvg_interval = analysis.get("fvg_interval", HTF_INTERVAL)
    fvg_class = (
        trend_badge_class("BULLISH")
        if fvg and fvg.get("type") == "BULL"
        else trend_badge_class("BEARISH")
        if fvg
        else "badge badge-neutral"
    )
    children.append(
        kv_row(f"FVG ({fvg_interval})", fvg_label, badge_class=fvg_class if fvg else "badge badge-neutral")
    )
    liquidity_interval = analysis.get("liquidity_interval", INTERVAL)
    children.append(kv_row(f"Liquidity ({liquidity_interval})", analysis.get("liquidity", "—"), strong=True))

    if metrics.get("require_trend_align") or INDICATOR_FILTERS_ENABLED:
        filter_bits = []
        if metrics.get("require_trend_align"):
            filter_bits.append("HTF trend")
        if INDICATOR_FILTERS_ENABLED:
            filter_bits.append("RSI/ADX filters")
        filter_bits.append(f"{metrics.get('signal_cooldown_sec', SIGNAL_COOLDOWN_SEC)}s cooldown")
        children.append(
            html.P(
                f"Valid entries: {' + '.join(filter_bits)}",
                className="panel-hint panel-footnote",
            )
        )

    return children


def build_signal_panel_children(metrics: dict) -> list:
    signal = metrics.get("signal", "NEUTRAL")
    confidence = metrics.get("confidence", 0)
    min_conf = metrics.get("min_confidence", MIN_CONFIDENCE)
    action_label = "TRADE" if is_tradable_signal(signal, confidence, min_conf) else "WATCH"
    pending = metrics.get("pending_signal", "NEUTRAL")
    pending_count = metrics.get("pending_signal_count", 0)
    debounce_target = metrics.get("signal_debounce_count", SIGNAL_DEBOUNCE_COUNT)
    pending_text = (
        f"Confirming {pending} ({pending_count}/{debounce_target})"
        if pending != signal and pending_count > 0
        else None
    )
    entry_text = (
        format_price(metrics.get("signal_entry"))
        if metrics.get("signal_entry") is not None
        else "—"
    )
    support_text = format_price(metrics.get("support")) if metrics.get("support") else "—"
    resistance_text = format_price(metrics.get("resistance")) if metrics.get("resistance") else "—"
    change_text = (
        f"{metrics['change_24h']:+.2f}%"
        if metrics.get("change_24h") is not None
        else "—"
    )
    reasons = metrics.get("signal_reasons") or "No active setup"
    valid_count = len(metrics.get("valid_entries") or [])

    children: list = [
        panel_section("Signal"),
        html.Div(
            [
                html.Span(signal, className=signal_badge_class(signal)),
                html.Span(action_label, className=confidence_badge_class(confidence, min_conf)),
            ],
            className="badge-row",
        ),
        kv_row("Confidence", f"{confidence}%", strong=True, hint=f"min {min_conf}%"),
        kv_row("Entry", entry_text, strong=True),
        panel_section("Order block zone"),
        build_ob_zone_kv_row(metrics),
        kv_row("Support", support_text, strong=True),
        kv_row("Resistance", resistance_text, strong=True),
        kv_row("24h change", change_text, strong=True),
        html.P(reasons, className="signal-reasons"),
    ]
    if pending_text:
        children.append(html.P(pending_text, className="signal-pending"))
    children.append(html.P(f"Valid entries on chart: {valid_count}", className="panel-hint panel-footnote"))
    return children


def build_metrics_panel_children(metrics: dict) -> list:
    spread_text = f"{metrics['spread']:.4f}" if metrics.get("spread") is not None else "—"
    spread_pct_text = f"{metrics['spread_pct']:.4f}%" if metrics.get("spread_pct") is not None else "—"
    delta_text = (
        f"{metrics['volume_delta']:+.4f}" if metrics.get("volume_delta") is not None else "—"
    )

    return [
        panel_section("Order book"),
        kv_row("Spread", spread_text, strong=True),
        kv_row("Spread %", spread_pct_text, strong=True),
        kv_row("Bid volume", f"{metrics.get('bid_volume', 0):.4f}", strong=True),
        kv_row("Ask volume", f"{metrics.get('ask_volume', 0):.4f}", strong=True),
        kv_row("Volume delta", delta_text, strong=True),
        panel_section("Connection"),
        kv_row("WebSocket", metrics.get("status", "—"), strong=True),
    ]


def execution_badge_class(mode: str, enabled: bool) -> str:
    if not enabled:
        return "badge badge-neutral"
    if mode == "live":
        return "badge badge-trade"
    return "badge badge-watch"


def build_execution_panel_section(metrics: dict) -> list:
    ex = metrics.get("execution") or {}
    enabled = ex.get("enabled", False)
    auto_execute = ex.get("auto_execute", False)
    mode = (ex.get("mode") or "dry").upper()
    pos = ex.get("position") or {}
    badge_label = "Off"
    if enabled and auto_execute:
        badge_label = f"{mode} · Auto"
    elif enabled:
        badge_label = f"{mode} · Manual"

    children: list = [
        panel_section("Execution"),
        html.Div(
            [
                html.Span(
                    badge_label,
                    className=execution_badge_class(ex.get("mode", "dry"), enabled and auto_execute),
                ),
            ],
            className="badge-row",
        ),
    ]

    if pos.get("open"):
        vol_text = (
            f"{pos['volume_usdt']:.2f} USDT"
            if pos.get("volume_usdt") is not None
            else "—"
        )
        entry_text = format_price(pos.get("entry")) if pos.get("entry") is not None else "—"
        pnl = pos.get("unrealized_pnl")
        pnl_pct = pos.get("unrealized_pnl_pct")
        pnl_text = f"{pnl:+.2f} USDT" if pnl is not None else "—"
        pnl_pct_text = f"{pnl_pct:+.2f}%" if pnl_pct is not None else "—"
        pnl_class = "kv-value"
        if pnl_pct is not None:
            if pnl_pct > 0:
                pnl_class = "kv-value pnl-up"
            elif pnl_pct < 0:
                pnl_class = "kv-value pnl-down"
        prot_class = "kv-value"
        if pos.get("protection_ok") or pos.get("trailing_active"):
            prot_class = "kv-value pnl-up"
        elif pos.get("has_sl") or pos.get("has_tp") or pos.get("trailing_pending"):
            prot_class = "kv-value change-down"
        children.extend(
            [
                kv_row("Position", f"{pos.get('direction', '—')} · {vol_text}", strong=True),
                kv_row("Entry", entry_text, strong=True),
                kv_row("PnL", f"{pnl_text} · {pnl_pct_text}", value_class=pnl_class),
                kv_row("Size", pos.get("qty", "—"), strong=True),
                kv_row("Protection", format_protection_display(pos), value_class=prot_class),
            ]
        )
    elif pos.get("pending"):
        vol_text = (
            f"{pos['volume_usdt']:.2f} USDT"
            if pos.get("volume_usdt") is not None
            else "—"
        )
        entry_text = format_price(pos.get("entry")) if pos.get("entry") is not None else "—"
        children.extend(
            [
                kv_row("Pending", f"{pos.get('direction', '—')} · {vol_text}", strong=True),
                kv_row("Limit", entry_text, strong=True, hint=f"qty {pos.get('qty', '—')}"),
            ]
        )
    elif enabled and pos.get("source") == "binance":
        children.append(kv_row("Position", "None", strong=True))

    children.append(kv_row("Status", ex.get("message", "—"), strong=True))
    if telegram.is_trading_paused():
        children.append(kv_row("Telegram", "Paused", strong=True))
    if ex.get("last_order_id"):
        last_bits = [
            ex.get("last_direction"),
            ex.get("last_symbol"),
            str(ex.get("last_order_id")),
        ]
        children.append(
            kv_row(
                "Last order",
                " · ".join(bit for bit in last_bits if bit),
                hint=ex.get("last_at"),
            )
        )
    return children


def build_trade_plan_panel_children(metrics: dict) -> list:
    plan = metrics.get("trade_plan") or {}
    ob_row = build_ob_zone_kv_row(metrics)

    if not plan.get("active"):
        return [
            ob_row,
            panel_section("Trade plan"),
            kv_row("Plan", plan.get("summary", "No active plan."), strong=True),
            *build_execution_panel_section(metrics),
        ]

    status_class = "badge badge-trade" if plan.get("live_match") else "badge badge-watch"
    status_label = "Live" if plan.get("live_match") else "Held"

    plan_details: list = [
        panel_section("Entry"),
    ]
    for leg in plan.get("legs") or []:
        plan_details.append(kv_row(leg["label"], format_price(leg["price"]), strong=True))
    plan_details.append(kv_row("Avg entry", format_price(plan["avg_entry"]), strong=True))

    plan_details.append(panel_section("Targets"))
    plan_details.extend(
        [
            kv_row(
                "Stop loss",
                format_price(plan["sl"]),
                strong=True,
                hint=f"risk {plan['risk_pct']:.2f}%",
            ),
            kv_row(
                "TP1",
                format_price(plan["tp1"]),
                strong=True,
                hint=f"+{plan['reward_tp1_pct']:.2f}% · {plan['partial_close_pct']:.0f}%",
            ),
            kv_row(
                "TP2",
                format_price(plan["tp2"]),
                strong=True,
                hint=f"RR {plan['rr_tp2']:.1f}",
            ),
            kv_row(
                "Break even",
                format_price(plan["breakeven_price"]),
                strong=True,
                hint=f"{plan['runner_pct']:.0f}% runner",
            ),
            kv_row(
                "Trail",
                f"{plan['trail_pct']:.2f}%",
                strong=True,
                hint=f"from {'peak' if plan['signal'] == 'LONG' else 'trough'}",
            ),
        ]
    )

    children: list = [
        ob_row,
        panel_section("Trade plan"),
        html.Div(
            [
                html.Span(plan["signal"], className=signal_badge_class(plan["signal"])),
                html.Span(status_label, className=status_class),
            ],
            className="badge-row",
        ),
        kv_row(
            "Status",
            plan.get("status_note", plan.get("summary", "—")),
            strong=True,
            hint=plan.get("created_at"),
        ),
        html.Div(plan_details, className="trade-plan-grid"),
        *build_execution_panel_section(metrics),
    ]
    return children


def pattern_badge_class(pattern: str) -> str:
    if pattern == "Hammer":
        return "badge badge-bull"
    if pattern == "Shooting Star":
        return "badge badge-bear"
    return "badge badge-neutral"


def signal_badge_class(signal: str) -> str:
    if signal == "LONG":
        return "badge badge-bull"
    if signal == "SHORT":
        return "badge badge-bear"
    return "badge badge-neutral"


def confidence_badge_class(confidence: int, min_confidence: int) -> str:
    if confidence >= min_confidence and confidence > 0:
        return "badge badge-trade"
    if confidence > 0:
        return "badge badge-watch"
    return "badge badge-neutral"


app = Dash(__name__, update_title=False)
app.title = f"Binance Live | {SYMBOL.upper()}"

app.layout = html.Div(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.H2("Binance Live Dashboard", className="title"),
                        html.P(
                            f"Streaming {SYMBOL.upper()} · interval {INTERVAL} · depth {DEPTH_LEVELS} levels · "
                            f"min confidence {MIN_CONFIDENCE}%",
                            className="subtitle",
                        ),
                    ],
                    className="header-copy",
                ),
                html.Div(id="price-header", className="price-header"),
            ],
            className="header",
        ),
        html.Div(
            [
                html.Div(id="pattern-panel", className="panel panel-pattern"),
                html.Div(id="signal-panel", className="panel panel-signal"),
                html.Div(id="metrics-panel", className="panel panel-metrics"),
                html.Div(id="trade-plan-panel", className="panel panel-trade-plan"),
            ],
            className="panels",
        ),
        dcc.Graph(id="live-chart", config={"displayModeBar": True}),
        dcc.Interval(id="interval", interval=1500, n_intervals=0),
    ],
    className="app-shell",
)


@app.server.route("/api/hub-summary")
def hub_summary_route():
    response = jsonify(build_hub_summary())
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


@app.server.route("/api/reload-config", methods=["POST"])
def reload_config_route():
    result = reload_symbol_config_from_db(force=True)
    response = jsonify({"ok": True, "symbol": SYMBOL.upper(), **result})
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


@app.server.route("/api/reconcile-protection", methods=["POST"])
def reconcile_protection_route():
    """Force SL/TP reconcile for this symbol (live + keys required)."""
    _, _, metrics = get_candles_df()
    exposure = (metrics.get("execution") or {}).get("position") or {}
    plan = metrics.get("trade_plan") or {}
    if not plan.get("active") and exposure.get("open"):
        plan = trade_plan_for_position_reconcile(
            exposure,
            support=metrics.get("support"),
            resistance=metrics.get("resistance"),
            price=metrics.get("price"),
            market_analysis=metrics.get("market_analysis"),
        )
    result = execution.reconcile_position_protection(
        SYMBOL,
        direction=plan.get("signal") if plan.get("active") else exposure.get("direction"),
        sl=float(plan["sl"]) if plan.get("active") and plan.get("sl") else None,
        tp=float(plan["tp1"]) if plan.get("active") and plan.get("tp1") else None,
    )
    execution._invalidate_position_cache(SYMBOL)
    response = jsonify({"ok": True, "symbol": SYMBOL.upper(), **result})
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


@app.callback(
    Output("live-chart", "figure"),
    Output("price-header", "children"),
    Output("pattern-panel", "children"),
    Output("signal-panel", "children"),
    Output("metrics-panel", "children"),
    Output("trade-plan-panel", "children"),
    Input("interval", "n_intervals"),
)
def update_dashboard(_: int):
    _, _, metrics = get_candles_df()
    figure = build_figure()

    change_text = (
        f"{metrics['change_24h']:+.2f}%"
        if metrics.get("change_24h") is not None
        else "—"
    )
    ob_label, ob_near = format_ob_proximity(metrics)
    price_header_children = [
        html.Span(SYMBOL.upper(), className="price-symbol"),
        html.Strong(format_price(metrics.get("price")), className="price-value"),
        html.Span(change_text, className=change_24h_class(metrics.get("change_24h"))),
        html.Div(
            [
                html.Span("OB zone", className="price-side-label"),
                html.Span(
                    ob_label,
                    className=f"price-ob {ob_proximity_value_class(ob_near).replace('kv-value ', '')}",
                ),
            ],
            className="price-ob-row",
        ),
        html.Div(
            [
                html.Span("Bid", className="price-side-label"),
                html.Strong(format_price(metrics.get("best_bid")), className="price-bid"),
                html.Span("Ask", className="price-side-label"),
                html.Strong(format_price(metrics.get("best_ask")), className="price-ask"),
            ],
            className="price-book",
        ),
    ]

    pattern_children = build_pattern_panel_children(metrics)
    signal_children = build_signal_panel_children(metrics)
    metrics_children = build_metrics_panel_children(metrics)
    trade_plan_children = build_trade_plan_panel_children(metrics)

    return (
        figure,
        price_header_children,
        pattern_children,
        signal_children,
        metrics_children,
        trade_plan_children,
    )


def build_telegram_status() -> str:
    with state_lock:
        price = latest_price
        signal = signal_dir
        confidence = signal_confidence
        ws = ws_status
        change = change_24h
    ex = execution.get_execution_status()
    exec_enabled = ex.get("enabled", False)
    auto_execute = ex.get("auto_execute", False)
    exec_mode = ex.get("mode", "dry").upper() if exec_enabled else "OFF"
    if telegram.is_trading_paused():
        trading = "fleet paused (/start to resume)"
    elif not exec_enabled:
        trading = "off (EXECUTION_ENABLED=false)"
    elif not auto_execute:
        trading = "valid entries off (EXECUTE_ON_VALID_ENTRY=false)"
    else:
        trading = f"auto · {exec_mode.lower()}"
    price_text = format_price(price) if price else "—"
    change_text = f"{change:+.2f}%" if change is not None else "—"
    lines = [
        f"{SYMBOL.upper()} · {INTERVAL}",
        f"Signal: {signal} {confidence}% · Price {price_text} ({change_text})",
        f"WebSocket: {ws}",
        f"Execution: {exec_mode} · {trading}",
    ]
    if ex.get("message"):
        lines.append(f"Last: {ex['message']}")
    return "\n".join(lines)


def format_protection_display(pos: dict) -> str:
    return execution.format_protection_display(pos)


def format_position_label(pos: dict) -> tuple[str, float | None, float | None]:
    """Human-readable position + optional PnL USDT and ROE %."""
    if pos.get("open"):
        vol = pos.get("volume_usdt")
        vol_text = f"{vol:.2f} USDT" if vol is not None else ""
        label = f"{pos.get('direction', '—')} · {vol_text}".strip(" · ")
        pnl = pos.get("unrealized_pnl")
        pnl_pct = pos.get("unrealized_pnl_pct")
        return label, pnl, pnl_pct
    if pos.get("pending"):
        return f"Pending {pos.get('direction', '—')}", None, None
    return "None", None, None


def build_hub_summary() -> dict:
    """Compact snapshot for the multi-pair hub cards."""
    _, _, metrics = get_candles_df()
    signal = metrics.get("signal", "NEUTRAL")
    confidence = int(metrics.get("confidence", 0))
    min_conf = int(metrics.get("min_confidence", MIN_CONFIDENCE))
    analysis = metrics.get("market_analysis") or {}
    ex = metrics.get("execution") or {}
    pos = ex.get("position") or {}
    if pos.get("open"):
        primary = str(pos.get("direction") or "").split("+", 1)[0].strip().upper()
        if primary in ("LONG", "SHORT") and not pos.get("has_sl") and not pos.get("has_tp"):
            pos = {**pos, **execution.get_position_protection(SYMBOL, primary)}
        pos = {**pos, "protection_display": execution.format_protection_display(pos)}
    position, position_pnl, position_pnl_pct = format_position_label(pos)

    change = metrics.get("change_24h")
    ob_proximity, ob_near = format_ob_proximity(metrics)
    smc = analysis.get("smc") or {}
    trend = analysis.get("htf_bias", "NEUTRAL")
    trend_aligned = None
    if signal in ("LONG", "SHORT"):
        trend_aligned = signal_aligned_with_trend(signal, trend)
    return {
        "symbol": SYMBOL.upper(),
        "interval": INTERVAL,
        "price": metrics.get("price"),
        "price_display": format_price(metrics.get("price")),
        "change_24h": change,
        "change_display": f"{change:+.2f}%" if change is not None else "—",
        "signal": signal,
        "confidence": confidence,
        "min_confidence": min_conf,
        "action": "TRADE" if is_tradable_signal(signal, confidence, min_conf) else "WATCH",
        "trend": trend,
        "smc_state": smc.get("state", "—"),
        "smc_pattern": smc.get("pattern", "—"),
        "smc_pattern_bias": smc.get("pattern_bias", "neutral"),
        "smc_state_short": format_smc_hub_label(smc),
        "smc_trend": smc.get("trend", "RANGING"),
        "smc_pd_zone": smc.get("pd_zone", "—"),
        "ob_proximity": ob_proximity,
        "ob_near": ob_near,
        "zone_position_pct": metrics.get("zone_position_pct"),
        "ws_status": metrics.get("status", "—"),
        "position": position,
        "position_open": bool(pos.get("open")),
        "position_source": pos.get("source"),
        "keys_configured": execution.keys_configured(),
        "position_direction": (
            str(pos.get("direction") or "").split("+", 1)[0].strip().upper()
            if pos.get("open")
            else None
        ),
        "position_pending": bool(pos.get("pending")),
        "position_pnl": position_pnl,
        "position_pnl_pct": position_pnl_pct,
        "has_sl": bool(pos.get("has_sl")),
        "has_tp": bool(pos.get("has_tp")),
        "tp_kind": pos.get("tp_kind"),
        "trailing_active": bool(pos.get("trailing_active")),
        "trailing_pending": bool(pos.get("trailing_pending")),
        "protection_ok": bool(pos.get("protection_ok")),
        "protection_display": format_protection_display(pos),
        "trend_aligned": trend_aligned,
        "require_trend_align": REQUIRE_TREND_ALIGN,
        "trading_paused": telegram.is_trading_paused(),
        "execution_enabled": bool(ex.get("enabled")),
        "execution_auto": bool(ex.get("auto_execute")),
        "execution_mode": ex.get("mode", "dry"),
        "db_enabled": db_store.is_enabled(),
        "indicator_filters_enabled": INDICATOR_FILTERS_ENABLED,
        "rsi": analysis.get("rsi"),
        "adx": analysis.get("htf_adx") if ADX_USE_HTF else analysis.get("adx"),
        "config_version": symbol_config.config_version(),
    }


def start_ws() -> None:
    asyncio.run(ws_loop())


def run_server() -> None:
    ui_enabled = is_port_available(DASH_HOST, DASH_PORT)
    telegram.notify_started(
        SYMBOL,
        execution.EXECUTION_ENABLED,
        execution.EXECUTION_MODE,
        DASH_PORT,
        ui_enabled,
        INTERVAL,
    )
    if execution.EXECUTION_ENABLED and execution.EXECUTION_MODE == "live":
        mode = "hedge" if execution.is_hedge_mode() else "one-way"
        logger.info("Execution live · Binance position mode: %s", mode)

    def _request_shutdown(signum: int | None = None, _frame=None) -> None:
        if signum is not None:
            logger.info("Shutdown signal received (%s)", signum)
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)
    atexit.register(lambda: None)

    try:
        if ui_enabled:
            app.run(debug=False, host=DASH_HOST, port=DASH_PORT, use_reloader=False)
        else:
            logger.warning(
                "Port %s already in use — skipping Dash UI; websocket and execution continue",
                DASH_PORT,
            )
            while True:
                time.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Shutting down")


if __name__ == "__main__":
    Thread(target=start_ws, daemon=True).start()
    if DB_CONFIG_POLL_SEC > 0:
        Thread(target=config_poll_loop, daemon=True, name=f"config-poll-{SYMBOL}").start()
    run_server()
