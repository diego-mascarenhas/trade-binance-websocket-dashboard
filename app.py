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


def detect_pattern(row: pd.Series) -> str | None:
    body = abs(row["c"] - row["o"])
    rng = max(row["h"] - row["l"], 1e-12)
    upper = row["h"] - max(row["o"], row["c"])
    lower = min(row["o"], row["c"]) - row["l"]

    body_pct = body / rng
    upper_pct = upper / rng
    lower_pct = lower / rng

    if body_pct < 0.35 and lower_pct > 0.55 and upper_pct < 0.2:
        return "Hammer"
    if body_pct < 0.35 and upper_pct > 0.55 and lower_pct < 0.2:
        return "Shooting Star"
    return None


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


async def fetch_depth_snapshot(session: aiohttp.ClientSession) -> dict:
    url = f"{REST_BASE}/api/v3/depth"
    params = {"symbol": SYMBOL.upper(), "limit": 1000}
    async with session.get(url, params=params, timeout=10) as resp:
        resp.raise_for_status()
        return await resp.json()


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
    global forming_candle, latest_pattern, latest_price, orderbook, ws_status

    depth_buffer: list[dict] = []
    stream_url = f"{WS_BASE}/stream?streams={SYMBOL}@kline_{INTERVAL}/{SYMBOL}@depth@100ms"

    async with aiohttp.ClientSession() as session:
        history = await fetch_historical_klines(session)
        with state_lock:
            candles.extend(history)

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
                                    latest_pattern = detect_pattern(pd.Series(row)) or "None"

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


def get_candles_df() -> pd.DataFrame:
    with state_lock:
        rows = list(candles)
        if forming_candle and (not rows or forming_candle["t"] != rows[-1]["t"]):
            rows = rows + [forming_candle.copy()]
        elif forming_candle and rows and forming_candle["t"] == rows[-1]["t"]:
            rows = rows[:-1] + [forming_candle.copy()]

        ob = {
            "bids": list(orderbook.get("bids", [])),
            "asks": list(orderbook.get("asks", [])),
        }
        metrics = {
            "pattern": latest_pattern,
            "price": latest_price,
            "spread": spread,
            "spread_pct": spread_pct,
            "volume_delta": volume_delta,
            "bid_volume": bid_volume,
            "ask_volume": ask_volume,
            "status": ws_status,
        }

    return pd.DataFrame(rows), ob, metrics


def build_figure() -> go.Figure:
    df, ob, metrics = get_candles_df()

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.1,
        row_heights=[0.72, 0.28],
        subplot_titles=("OHLC + Patterns", "Order Book Depth"),
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
        for _, row in closed_df.iterrows():
            pattern = detect_pattern(row)
            if pattern:
                pattern_x.append(row["t"])
                pattern_y.append(row["c"])
                pattern_text.append(pattern)
                pattern_hover.append(pattern)

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

    bids = ob["bids"]
    asks = ob["asks"]

    if bids:
        fig.add_trace(
            go.Bar(
                x=[q for _, q in bids],
                y=[f"{p:.2f}" for p, _ in bids],
                orientation="h",
                name="Bids",
                marker_color="rgba(0,193,118,0.65)",
            ),
            row=2,
            col=1,
        )

    if asks:
        fig.add_trace(
            go.Bar(
                x=[-q for _, q in asks],
                y=[f"{p:.2f}" for p, _ in asks],
                orientation="h",
                name="Asks",
                marker_color="rgba(255,77,79,0.65)",
            ),
            row=2,
            col=1,
        )

    price_text = f"{metrics['price']:.2f}" if metrics["price"] else "—"
    spread_text = f"{metrics['spread']:.4f}" if metrics["spread"] is not None else "—"
    spread_pct_text = f"{metrics['spread_pct']:.4f}%" if metrics["spread_pct"] is not None else "—"
    delta_text = f"{metrics['volume_delta']:.4f}" if metrics["volume_delta"] is not None else "—"

    fig.update_layout(
        title=(
            f"Live Binance {SYMBOL.upper()} ({INTERVAL}) | "
            f"Price: {price_text} | Pattern: {metrics['pattern']} | Status: {metrics['status']}"
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
                    f"Delta: {delta_text}"
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
    fig.update_xaxes(title_text="Quantity", row=2, col=1)
    fig.update_yaxes(title_text="Price level", row=2, col=1)

    return fig


def pattern_badge_class(pattern: str) -> str:
    if pattern == "Hammer":
        return "badge badge-bull"
    if pattern == "Shooting Star":
        return "badge badge-bear"
    return "badge badge-neutral"


app = Dash(__name__)
app.title = f"Binance Live | {SYMBOL.upper()}"

app.layout = html.Div(
    [
        html.Div(
            [
                html.H2("Binance Live Dashboard", className="title"),
                html.P(
                    f"Streaming {SYMBOL.upper()} · interval {INTERVAL} · depth {DEPTH_LEVELS} levels",
                    className="subtitle",
                ),
            ],
            className="header",
        ),
        html.Div(
            [
                html.Div(id="pattern-panel", className="panel"),
                html.Div(id="metrics-panel", className="panel"),
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
    Output("pattern-panel", "children"),
    Output("metrics-panel", "children"),
    Input("interval", "n_intervals"),
)
def update_dashboard(_: int):
    _, _, metrics = get_candles_df()
    figure = build_figure()

    pattern_children = [
        html.Span("Last closed pattern", className="panel-label"),
        html.Span(metrics["pattern"], className=pattern_badge_class(metrics["pattern"])),
    ]

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

    return figure, pattern_children, metrics_children


def start_ws() -> None:
    asyncio.run(ws_loop())


if __name__ == "__main__":
    Thread(target=start_ws, daemon=True).start()
    app.run(debug=False, host=DASH_HOST, port=DASH_PORT)
