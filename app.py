import asyncio
import json
import logging
import os
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
DASH_HOST = os.getenv("DASH_HOST", "0.0.0.0")
DASH_PORT = int(os.getenv("DASH_PORT", "8050"))

REST_BASE = "https://api.binance.com"
WS_BASE = "wss://stream.binance.com:9443"

state_lock = Lock()
candles: deque = deque(maxlen=MAX_CANDLES)
forming_candle: dict | None = None
orderbook: dict = {"bids": [], "asks": []}
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
signal_dir = "NEUTRAL"
signal_confidence = 0
signal_reasons = ""
signal_entry: float | None = None
zone_position_pct: float | None = None
change_24h: float | None = None


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


def trim_orderbook(bids: dict[float, float], asks: dict[float, float]) -> tuple[list[list[float]], list[list[float]]]:
    top_bids = sorted(bids.items(), key=lambda x: x[0], reverse=True)[:DEPTH_LEVELS]
    top_asks = sorted(asks.items(), key=lambda x: x[0])[:DEPTH_LEVELS]
    return [[p, q] for p, q in top_bids], [[p, q] for p, q in top_asks]


def analyze_order_book(
    bids: list[list[float]], asks: list[list[float]]
) -> tuple[float, float, float, float]:
    best_bid = bids[0][0] if bids else 0.0
    best_ask = asks[0][0] if asks else 0.0
    support_level = max(bids, key=lambda level: level[1])[0] if bids else 0.0
    resistance_level = max(asks, key=lambda level: level[1])[0] if asks else 0.0
    return support_level, resistance_level, best_bid, best_ask


def determine_signal(
    current_price: float | None,
    support_level: float,
    resistance_level: float,
    best_bid: float,
    best_ask: float,
    change_24h_pct: float | None,
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

            if position < 25:
                signal = "LONG"
                confidence = 65
                entry = support_level * 1.001
                if best_bid > 0 and entry > best_bid:
                    entry = best_bid
                reasons = "OB: near support"
                if change_24h_pct is not None and change_24h_pct < -3:
                    confidence += 15
                    reasons += " + reversal"

            elif position > 75:
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
    global support, resistance, signal_dir, signal_confidence, signal_reasons
    global signal_entry, zone_position_pct

    if not bids or not asks:
        return

    support_level, resistance_level, best_bid, best_ask = analyze_order_book(bids, asks)
    signal, confidence, reasons, entry = determine_signal(
        current_price,
        support_level,
        resistance_level,
        best_bid,
        best_ask,
        change_24h_pct,
    )

    support = support_level
    resistance = resistance_level
    signal_dir = signal
    signal_confidence = confidence
    signal_reasons = reasons
    signal_entry = entry

    if (
        current_price
        and support_level > 0
        and resistance_level > support_level
    ):
        zone_position_pct = (current_price - support_level) * 100 / (resistance_level - support_level)
    else:
        zone_position_pct = None


def update_metrics(bids: list[list[float]], asks: list[list[float]]) -> None:
    global spread, spread_pct, volume_delta, bid_volume, ask_volume, latest_price

    bid_volume = sum(q for _, q in bids)
    ask_volume = sum(q for _, q in asks)
    volume_delta = bid_volume - ask_volume

    if bids and asks:
        best_bid = bids[0][0]
        best_ask = asks[0][0]
        spread = best_ask - best_bid
        mid = (best_bid + best_ask) / 2
        spread_pct = (spread / mid) * 100 if mid else None
        latest_price = latest_price or (bids[0][0] + asks[0][0]) / 2

    update_trading_signal(bids, asks, latest_price, change_24h)


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


async def fetch_historical_klines(session: aiohttp.ClientSession) -> list[dict]:
    url = f"{REST_BASE}/api/v3/klines"
    params = {
        "symbol": SYMBOL.upper(),
        "interval": INTERVAL,
        "limit": min(MAX_CANDLES, 500),
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
    global forming_candle, latest_price, orderbook, ws_status, change_24h

    depth_buffer: list[dict] = []
    stream_url = (
        f"{WS_BASE}/stream?streams="
        f"{SYMBOL}@kline_{INTERVAL}/{SYMBOL}@depth@100ms/{SYMBOL}@miniTicker"
    )

    async with aiohttp.ClientSession() as session:
        history = await fetch_historical_klines(session)
        initial_change = await fetch_24h_ticker(session)
        with state_lock:
            candles.extend(history)
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
                    bids, asks = trim_orderbook(bid_map, ask_map)

                    with state_lock:
                        orderbook = {"bids": bids, "asks": asks}
                        update_metrics(bids, asks)

                    ws_status = "live"
                    logger.info("Order book synced at updateId=%s", last_update_id)

                    async for raw in ws:
                        msg = json.loads(raw)
                        data = msg.get("data", {})

                        if "k" in data:
                            k = data["k"]
                            row = kline_row(k)
                            latest_price = row["c"]

                            with state_lock:
                                forming_candle = row
                                if row["x"]:
                                    candles.append(row)
                                if orderbook.get("bids") and orderbook.get("asks"):
                                    update_metrics(orderbook["bids"], orderbook["asks"])

                        elif data.get("e") == "24hrMiniTicker" or msg.get("stream", "").endswith("@miniTicker"):
                            pct_raw = data.get("P")
                            if pct_raw is not None:
                                try:
                                    with state_lock:
                                        change_24h = float(pct_raw)
                                        if orderbook.get("bids") and orderbook.get("asks"):
                                            update_metrics(orderbook["bids"], orderbook["asks"])
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
                            bids, asks = trim_orderbook(bid_map, ask_map)

                            with state_lock:
                                orderbook = {"bids": bids, "asks": asks}
                                update_metrics(bids, asks)

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
        confirmed = resolve_confirmed_pattern(closed_rows)
        latest_pattern = confirmed or "None"

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
            "zone_position_pct": zone_position_pct,
            "change_24h": change_24h,
            "min_confidence": MIN_CONFIDENCE,
        }

    return pd.DataFrame(rows), ob, metrics


def build_depth_bar_data(
    bids: list[list[float]], asks: list[list[float]]
) -> tuple[list[str], list[float | None], list[float | None]]:
    bid_map = {price: qty for price, qty in bids}
    ask_map = {price: qty for price, qty in asks}
    all_prices = sorted(set(bid_map) | set(ask_map))
    labels = [f"{price:.2f}" for price in all_prices]
    bid_values = [-bid_map[price] if price in bid_map else None for price in all_prices]
    ask_values = [ask_map[price] if price in ask_map else None for price in all_prices]
    return labels, bid_values, ask_values


def build_figure() -> go.Figure:
    df, ob, metrics = get_candles_df()

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.1,
        row_heights=[0.72, 0.28],
        subplot_titles=("OHLC + Confirmed Patterns", "Order Book Depth"),
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

        if metrics.get("support"):
            fig.add_hline(
                y=metrics["support"],
                line_dash="dot",
                line_color="rgba(0,193,118,0.85)",
                line_width=1,
                annotation_text="Support",
                annotation_position="right",
                row=1,
                col=1,
            )

        if metrics.get("resistance"):
            fig.add_hline(
                y=metrics["resistance"],
                line_dash="dot",
                line_color="rgba(255,77,79,0.85)",
                line_width=1,
                annotation_text="Resistance",
                annotation_position="right",
                row=1,
                col=1,
            )

    bids = ob["bids"]
    asks = ob["asks"]
    depth_labels, depth_bids, depth_asks = build_depth_bar_data(bids, asks)

    if depth_labels:
        fig.add_trace(
            go.Bar(
                x=depth_bids,
                y=depth_labels,
                orientation="h",
                name="Bids",
                marker_color="rgba(0,193,118,0.65)",
                hovertemplate="Bid<br>Price: %{y}<br>Qty: %{customdata}<extra></extra>",
                customdata=[abs(v) if v is not None else 0 for v in depth_bids],
            ),
            row=2,
            col=1,
        )

        fig.add_trace(
            go.Bar(
                x=depth_asks,
                y=depth_labels,
                orientation="h",
                name="Asks",
                marker_color="rgba(255,77,79,0.65)",
                hovertemplate="Ask<br>Price: %{y}<br>Qty: %{customdata}<extra></extra>",
                customdata=[v if v is not None else 0 for v in depth_asks],
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
                html.Div(id="pattern-panel", className="panel"),
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

    pattern_children = [
        html.Span("Confirmed pattern", className="panel-label"),
        html.Span(
            metrics["pattern"],
            className=pattern_badge_class(metrics["pattern"]),
        ),
    ]
    if metrics["pattern"] == "None":
        pattern_children.append(
            html.Span(
                f"Hammer→LONG · Star→SHORT · conf≥{metrics.get('min_confidence', MIN_CONFIDENCE)}%",
                className="panel-hint",
            )
        )
    else:
        pattern_children.append(
            html.Span(
                f"{metrics.get('signal')} {metrics.get('confidence')}%",
                className=confidence_badge_class(
                    metrics.get("confidence", 0),
                    metrics.get("min_confidence", MIN_CONFIDENCE),
                ),
            )
        )

    signal = metrics.get("signal", "NEUTRAL")
    confidence = metrics.get("confidence", 0)
    min_conf = metrics.get("min_confidence", MIN_CONFIDENCE)
    action_label = "TRADE" if signal != "NEUTRAL" and confidence >= min_conf else "WATCH"
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

    metrics_children = [
        html.Div(
            [
                html.Span("Current price", className="metric-label"),
                html.Strong(format_price(metrics.get("price")), className="metric-price"),
            ],
            className="metric metric-price-block",
        ),
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
