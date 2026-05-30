import asyncio
import json
import logging
import os
import time
from collections import deque
from threading import Lock, Thread

import aiohttp
import pandas as pd
import plotly.graph_objects as go
import websockets
from dash import Dash, Input, Output, dcc, html
from dotenv import load_dotenv
from plotly.subplots import make_subplots

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

SYMBOL = os.getenv("SYMBOL", "btcusdt").lower()
INTERVAL = os.getenv("INTERVAL", "1m")
DEPTH_LEVELS = int(os.getenv("DEPTH_LEVELS", "20"))
MAX_CANDLES = int(os.getenv("MAX_CANDLES", "200"))
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "50"))
MIN_PATTERN_RANGE_PCT = float(os.getenv("MIN_PATTERN_RANGE_PCT", "0.02"))
OB_WALL_RANGE_PCT = float(os.getenv("OB_WALL_RANGE_PCT", "0.6"))
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
DASH_HOST = os.getenv("DASH_HOST", "0.0.0.0")
DASH_PORT = int(os.getenv("DASH_PORT", "8050"))

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
    if not REQUIRE_TREND_ALIGN or trend_bias == "NEUTRAL":
        return True
    if signal == "LONG" and trend_bias == "BULLISH":
        return True
    if signal == "SHORT" and trend_bias == "BEARISH":
        return True
    return False


def compute_market_analysis(
    closed_rows: list[dict],
    htf_closed_rows: list[dict],
    price: float | None,
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
        "macd": None,
        "macd_signal": None,
        "macd_hist": None,
        "fvg": None,
        "fvg_label": "—",
        "liquidity": "—",
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
    macd, macd_signal, macd_hist = compute_macd_values(closes)
    analysis["macd"] = macd
    analysis["macd_signal"] = macd_signal
    analysis["macd_hist"] = macd_hist

    fvg = detect_latest_fvg(closed_df)
    if fvg:
        analysis["fvg"] = fvg
        analysis["fvg_label"] = fvg["label"]

    analysis["liquidity"] = liquidity_label(closed_df, price)

    if htf_closed_rows:
        htf_bias, htf_fast, htf_slow, htf_trend = compute_htf_bias(pd.DataFrame(htf_closed_rows))
        analysis["htf_bias"] = htf_bias
        analysis["htf_ema_fast"] = htf_fast
        analysis["htf_ema_slow"] = htf_slow
        analysis["htf_ema_trend"] = htf_trend

    return analysis


def format_price(price: float | None) -> str:
    if price is None:
        return "—"
    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:.4f}"
    return f"{price:.6f}"


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


def record_valid_entry(
    signal: str,
    entry: float | None,
    confidence: int,
    reasons: str,
    candle_time,
    trend_bias: str,
) -> None:
    global last_valid_entry_monotonic

    if not is_tradable_signal(signal, confidence) or entry is None or candle_time is None:
        return
    if not signal_aligned_with_trend(signal, trend_bias):
        return

    now = time.monotonic()
    if (
        last_valid_entry_monotonic is not None
        and now - last_valid_entry_monotonic < SIGNAL_COOLDOWN_SEC
    ):
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


def apply_signal_debounce(
    candidate: str,
    confidence: int,
    reasons: str,
    entry: float | None,
    candle_time,
    trend_bias: str,
) -> None:
    global stable_signal_dir, pending_signal, pending_signal_count
    global signal_dir, signal_confidence, signal_reasons, signal_entry

    if candidate == pending_signal:
        pending_signal_count += 1
    else:
        pending_signal = candidate
        pending_signal_count = 1

    if pending_signal_count >= SIGNAL_DEBOUNCE_COUNT and candidate != stable_signal_dir:
        if is_tradable_signal(candidate, confidence):
            record_valid_entry(candidate, entry, confidence, reasons, candle_time, trend_bias)
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
    analysis = compute_market_analysis(closed_rows, htf_closed_rows, current_price)
    apply_signal_debounce(
        candidate,
        confidence,
        reasons,
        entry,
        candle_time,
        analysis.get("htf_bias", "NEUTRAL"),
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


async def ws_loop() -> None:
    global forming_candle, htf_forming_candle, latest_price, orderbook, ws_status, change_24h

    depth_buffer: list[dict] = []
    stream_url = (
        f"{WS_BASE}/stream?streams="
        f"{SYMBOL}@kline_{INTERVAL}/{SYMBOL}@kline_{HTF_INTERVAL}/"
        f"{SYMBOL}@depth@100ms/{SYMBOL}@miniTicker"
    )

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
            try:
                ws_status = "connecting"
                depth_buffer.clear()

                async with websockets.connect(stream_url, ping_interval=20, ping_timeout=20) as ws:
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
                    logger.info("Order book synced at updateId=%s", last_update_id)

                    async for raw in ws:
                        msg = json.loads(raw)
                        data = msg.get("data", {})

                        if "k" in data:
                            k = data["k"]
                            row = kline_row(k)
                            interval = k.get("i", INTERVAL)

                            with state_lock:
                                if interval == HTF_INTERVAL:
                                    htf_forming_candle = row
                                    if row["x"]:
                                        if htf_candles and htf_candles[-1]["t"] == row["t"]:
                                            htf_candles[-1] = row
                                        else:
                                            htf_candles.append(row)
                                elif interval == INTERVAL:
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
                                    logger.warning("Invalid miniTicker change pct: %r", pct_raw)
                            else:
                                logger.debug("miniTicker event without P field: %s", data)

                        elif "U" in data and "u" in data:
                            if data["u"] <= last_update_id:
                                continue
                            if not (data["U"] <= last_update_id + 1 <= data["u"]):
                                logger.warning("Order book desync detected, resyncing...")
                                break

                            apply_depth_update(data, bid_map, ask_map)
                            last_update_id = data["u"]

                            with state_lock:
                                sync_orderbook_state(bid_map, ask_map)

            except Exception:
                logger.exception("WebSocket loop error, reconnecting in 3s")
                ws_status = "reconnecting"
                await asyncio.sleep(3)


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
        market_analysis = compute_market_analysis(closed_rows, htf_closed_rows, latest_price)

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
        }

    return pd.DataFrame(rows), ob, metrics


def depth_category_labels(bids: list[list[float]], asks: list[list[float]]) -> list[str]:
    prices = sorted({price for price, _ in bids} | {price for price, _ in asks})
    return [f"{price:.2f}" for price in prices]


def side_bar_colors(
    levels: list[list[float]],
    wall_price: float | None,
    base_color: str,
    wall_color: str,
) -> list[str]:
    wall_label = f"{wall_price:.2f}" if wall_price else None
    return [wall_color if f"{price:.2f}" == wall_label else base_color for price, _ in levels]


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

    price_range = resistance - support
    spread = metrics.get("spread") or 0
    wall_band = max(price_range * 0.012, spread * 2, support * 0.00005)
    support_qty = metrics.get("support_qty", 0)
    resistance_qty = metrics.get("resistance_qty", 0)

    fig.add_hrect(
        y0=support,
        y1=support + price_range * 0.25,
        fillcolor="rgba(0,193,118,0.07)",
        line_width=0,
        row=row,
        col=col,
    )
    fig.add_hrect(
        y0=resistance - price_range * 0.25,
        y1=resistance,
        fillcolor="rgba(255,77,79,0.07)",
        line_width=0,
        row=row,
        col=col,
    )
    fig.add_hrect(
        y0=support - wall_band,
        y1=support + wall_band,
        fillcolor="rgba(0,193,118,0.24)",
        line_width=0,
        row=row,
        col=col,
    )
    fig.add_hrect(
        y0=resistance - wall_band,
        y1=resistance + wall_band,
        fillcolor="rgba(255,77,79,0.24)",
        line_width=0,
        row=row,
        col=col,
    )

    fig.add_hline(
        y=support,
        line_color="rgba(0,193,118,0.95)",
        line_width=2,
        annotation_text=f"OB Support · {format_price(support)} · qty {support_qty:.4f}",
        annotation_position="right",
        row=row,
        col=col,
    )
    fig.add_hline(
        y=resistance,
        line_color="rgba(255,77,79,0.95)",
        line_width=2,
        annotation_text=f"OB Resistance · {format_price(resistance)} · qty {resistance_qty:.4f}",
        annotation_position="right",
        row=row,
        col=col,
    )

    current_price = metrics.get("price")
    if current_price:
        fig.add_hline(
            y=current_price,
            line_dash="dash",
            line_color="rgba(255,193,7,0.85)",
            line_width=1,
            row=row,
            col=col,
        )


def candle_chart_y_range(df: pd.DataFrame, metrics: dict) -> tuple[float, float]:
    y_min = float(df["l"].min())
    y_max = float(df["h"].max())
    mid = metrics.get("price") or (y_min + y_max) / 2
    max_dist = mid * (OB_WALL_RANGE_PCT / 100) * 1.5

    for key in ("support", "resistance"):
        value = metrics.get(key)
        if value is not None and abs(float(value) - mid) <= max_dist:
            y_min = min(y_min, float(value))
            y_max = max(y_max, float(value))

    for entry in metrics.get("valid_entries") or []:
        y_min = min(y_min, float(entry["entry"]))
        y_max = max(y_max, float(entry["entry"]))

    active_entry = metrics.get("signal_entry")
    signal = metrics.get("signal", "NEUTRAL")
    confidence = metrics.get("confidence", 0)
    if active_entry is not None and is_tradable_signal(signal, confidence, metrics.get("min_confidence", MIN_CONFIDENCE)):
        y_min = min(y_min, float(active_entry))
        y_max = max(y_max, float(active_entry))

    padding = max((y_max - y_min) * 0.06, mid * 0.0005)
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
            annotation_text=f"Active {signal} entry · {format_price(entry_price)}",
            annotation_position="right",
            row=row,
            col=col,
        )


def build_figure() -> go.Figure:
    df, ob, metrics = get_candles_df()

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.1,
        row_heights=[0.72, 0.28],
        subplot_titles=("OHLC + Order Blocks", "Order Book Depth"),
    )

    if not df.empty:
        fig.add_trace(
            go.Candlestick(
                x=df["t"],
                open=df["o"],
                high=df["h"],
                low=df["l"],
                close=df["c"],
                increasing=dict(
                    line=dict(color="#00c176", width=0.8),
                    fillcolor="#00c176",
                ),
                decreasing=dict(
                    line=dict(color="#ff4d4f", width=0.8),
                    fillcolor="#ff4d4f",
                ),
                whiskerwidth=0.2,
                name=SYMBOL.upper(),
            ),
            row=1,
            col=1,
        )

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
        y_min, y_max = candle_chart_y_range(df, metrics)
        fig.update_yaxes(range=[y_min, y_max], row=1, col=1)

    bids = ob["bids"]
    asks = ob["asks"]
    depth_labels = depth_category_labels(bids, asks)

    if bids:
        fig.add_trace(
            go.Bar(
                x=[-qty for _, qty in bids],
                y=[f"{price:.2f}" for price, _ in bids],
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
                y=[f"{price:.2f}" for price, _ in asks],
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
    spread_text = f"{metrics['spread']:.4f}" if metrics["spread"] is not None else "—"
    spread_pct_text = f"{metrics['spread_pct']:.4f}%" if metrics["spread_pct"] is not None else "—"
    delta_text = f"{metrics['volume_delta']:.4f}" if metrics["volume_delta"] is not None else "—"
    signal_text = metrics.get("signal", "NEUTRAL")
    confidence_text = f"{metrics.get('confidence', 0)}%"
    change_text = (
        f"{metrics['change_24h']:+.2f}%"
        if metrics.get("change_24h") is not None
        else "—"
    )

    fig.update_layout(
        title=(
            f"Live Binance {SYMBOL.upper()} ({INTERVAL}) | "
            f"Price: {price_text} | Signal: {signal_text} {confidence_text} | "
            f"24h: {change_text} | Status: {metrics['status']}"
        ),
        template="plotly_dark",
        height=900,
        barmode="overlay",
        xaxis_rangeslider_visible=False,
        legend_orientation="h",
        margin=dict(l=20, r=20, t=80, b=20),
        annotations=[
            dict(
                text=(
                    f"Spread: {spread_text} ({spread_pct_text}) | "
                    f"Bid vol: {metrics['bid_volume']:.4f} | "
                    f"Ask vol: {metrics['ask_volume']:.4f} | "
                    f"Delta: {delta_text} | "
                    f"Zone: {metrics['zone_position_pct']:.1f}%"
                    if metrics.get("zone_position_pct") is not None
                    else (
                        f"Spread: {spread_text} ({spread_pct_text}) | "
                        f"Bid vol: {metrics['bid_volume']:.4f} | "
                        f"Ask vol: {metrics['ask_volume']:.4f} | "
                        f"Delta: {delta_text}"
                    )
                ),
                xref="paper",
                yref="paper",
                x=0,
                y=1.08,
                showarrow=False,
                font=dict(size=12, color="#cccccc"),
            )
        ],
    )

    fig.update_xaxes(type="date", rangeslider_visible=False, row=1, col=1)
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_xaxes(
        title_text="Quantity (← Bids | Asks →)",
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor="rgba(255,255,255,0.25)",
        row=2,
        col=1,
    )
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


def build_pattern_panel_children(metrics: dict) -> list:
    analysis = metrics.get("market_analysis") or {}
    children: list = [
        html.Div(
            [
                html.Span("Confirmed pattern", className="panel-label"),
                html.Span(metrics["pattern"], className=pattern_badge_class(metrics["pattern"])),
            ],
            className="analysis-row",
        ),
    ]

    if metrics["pattern"] == "None":
        children.append(
            html.P(
                f"Hammer→LONG · Star→SHORT · conf≥{metrics.get('min_confidence', MIN_CONFIDENCE)}%",
                className="panel-hint",
            )
        )
    else:
        children.append(
            html.Div(
                [
                    html.Span(
                        f"{metrics.get('signal')} {metrics.get('confidence')}%",
                        className=confidence_badge_class(
                            metrics.get("confidence", 0),
                            metrics.get("min_confidence", MIN_CONFIDENCE),
                        ),
                    ),
                ],
                className="analysis-row",
            )
        )

    children.append(html.Div(className="analysis-divider"))
    children.append(
        html.Span(f"Market context · {INTERVAL}", className="panel-label analysis-heading")
    )

    htf_interval = analysis.get("htf_interval", HTF_INTERVAL)
    htf_bias = analysis.get("htf_bias", "NEUTRAL")
    children.append(
        html.Div(
            [
                html.Span(f"HTF ({htf_interval})", className="panel-label"),
                html.Span(htf_bias, className=trend_badge_class(htf_bias)),
            ],
            className="analysis-row",
        )
    )

    ema_fast = analysis.get("ema_fast")
    ema_slow = analysis.get("ema_slow")
    ema_text = (
        f"{format_price(ema_fast)} / {format_price(ema_slow)}"
        if ema_fast is not None and ema_slow is not None
        else "—"
    )
    children.append(
        html.Div(
            [
                html.Span(f"EMA {EMA_FAST}/{EMA_SLOW}", className="panel-label"),
                html.Strong(ema_text, className="analysis-value"),
                html.Span(analysis.get("ema_cross", "—"), className="panel-hint"),
            ],
            className="analysis-row",
        )
    )

    rsi = analysis.get("rsi")
    rsi_text = f"{rsi:.1f}" if rsi is not None else "—"
    children.append(
        html.Div(
            [
                html.Span(f"RSI ({RSI_PERIOD})", className="panel-label"),
                html.Span(rsi_text, className=rsi_badge_class(rsi)),
            ],
            className="analysis-row",
        )
    )

    macd = analysis.get("macd")
    macd_hist = analysis.get("macd_hist")
    if macd is not None and macd_hist is not None:
        macd_text = f"{macd:.2f} · hist {macd_hist:+.2f}"
    else:
        macd_text = "—"
    children.append(
        html.Div(
            [
                html.Span("MACD", className="panel-label"),
                html.Span(macd_text, className=macd_badge_class(macd_hist)),
            ],
            className="analysis-row",
        )
    )

    fvg = analysis.get("fvg")
    fvg_label = analysis.get("fvg_label", "—")
    fvg_class = trend_badge_class("BULLISH" if fvg and fvg.get("type") == "BULL" else "BEARISH" if fvg else "NEUTRAL")
    children.append(
        html.Div(
            [
                html.Span("FVG", className="panel-label"),
                html.Span(fvg_label, className=fvg_class if fvg else "badge badge-neutral"),
            ],
            className="analysis-row",
        )
    )

    children.append(
        html.Div(
            [
                html.Span("Liquidity", className="panel-label"),
                html.Strong(analysis.get("liquidity", "—"), className="analysis-value"),
            ],
            className="analysis-row",
        )
    )

    if metrics.get("require_trend_align"):
        children.append(
            html.P(
                f"Valid entries need HTF trend · cooldown {metrics.get('signal_cooldown_sec', SIGNAL_COOLDOWN_SEC)}s",
                className="panel-hint",
            )
        )

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


app = Dash(__name__)
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
            ],
            className="panels",
        ),
        dcc.Graph(id="live-chart", config={"displayModeBar": True}),
        dcc.Interval(id="interval", interval=1500, n_intervals=0),
    ],
    className="app-shell",
)


@app.callback(
    Output("live-chart", "figure"),
    Output("price-header", "children"),
    Output("pattern-panel", "children"),
    Output("signal-panel", "children"),
    Output("metrics-panel", "children"),
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
    price_header_children = [
        html.Span(SYMBOL.upper(), className="price-symbol"),
        html.Strong(format_price(metrics.get("price")), className="price-value"),
        html.Span(change_text, className=change_24h_class(metrics.get("change_24h"))),
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
        f"{metrics['signal_entry']:.2f}"
        if metrics.get("signal_entry") is not None
        else "—"
    )
    support_text = f"{metrics['support']:.2f}" if metrics.get("support") else "—"
    resistance_text = f"{metrics['resistance']:.2f}" if metrics.get("resistance") else "—"
    zone_text = (
        f"{metrics['zone_position_pct']:.1f}%"
        if metrics.get("zone_position_pct") is not None
        else "—"
    )
    change_text = (
        f"{metrics['change_24h']:+.2f}%"
        if metrics.get("change_24h") is not None
        else "—"
    )
    reasons = metrics.get("signal_reasons") or "No active setup"

    signal_children = [
        html.Div(
            [
                html.Span("Signal", className="panel-label"),
                html.Span(signal, className=signal_badge_class(signal)),
                html.Span(action_label, className=confidence_badge_class(confidence, min_conf)),
            ],
            className="signal-row",
        ),
        html.Div(
            [
                html.Span("Confidence", className="panel-label"),
                html.Strong(f"{confidence}%"),
                html.Span(f"need {min_conf}%", className="panel-hint"),
            ],
            className="signal-row",
        ),
        html.Div(
            [
                html.Span("Entry", className="panel-label"),
                html.Strong(entry_text),
            ],
            className="signal-row",
        ),
        html.Div(
            [
                html.Span("Support", className="panel-label"),
                html.Strong(support_text),
                html.Span("Resistance", className="panel-label"),
                html.Strong(resistance_text),
                html.Span("Zone", className="panel-label"),
                html.Strong(zone_text),
                html.Span("24h", className="panel-label"),
                html.Strong(change_text),
            ],
            className="signal-row",
        ),
        html.P(reasons, className="signal-reasons"),
    ]
    if pending_text:
        signal_children.append(html.P(pending_text, className="signal-pending"))
    valid_count = len(metrics.get("valid_entries") or [])
    signal_children.append(
        html.P(f"Valid entries on chart: {valid_count}", className="signal-pending")
    )

    metrics_children = [
        html.Div(
            [
                html.Span("Spread", className="metric-label"),
                html.Strong(
                    f"{metrics['spread']:.4f}" if metrics["spread"] is not None else "—"
                ),
            ],
            className="metric",
        ),
        html.Div(
            [
                html.Span("Spread %", className="metric-label"),
                html.Strong(
                    f"{metrics['spread_pct']:.4f}%" if metrics["spread_pct"] is not None else "—"
                ),
            ],
            className="metric",
        ),
        html.Div(
            [
                html.Span("Bid volume", className="metric-label"),
                html.Strong(f"{metrics['bid_volume']:.4f}"),
            ],
            className="metric",
        ),
        html.Div(
            [
                html.Span("Ask volume", className="metric-label"),
                html.Strong(f"{metrics['ask_volume']:.4f}"),
            ],
            className="metric",
        ),
        html.Div(
            [
                html.Span("Volume delta", className="metric-label"),
                html.Strong(
                    f"{metrics['volume_delta']:+.4f}"
                    if metrics["volume_delta"] is not None
                    else "—"
                ),
            ],
            className="metric",
        ),
        html.Div(
            [
                html.Span("WS status", className="metric-label"),
                html.Strong(metrics["status"]),
            ],
            className="metric",
        ),
    ]

    return figure, price_header_children, pattern_children, signal_children, metrics_children


def start_ws() -> None:
    asyncio.run(ws_loop())


if __name__ == "__main__":
    Thread(target=start_ws, daemon=True).start()
    app.run(debug=False, host=DASH_HOST, port=DASH_PORT)
