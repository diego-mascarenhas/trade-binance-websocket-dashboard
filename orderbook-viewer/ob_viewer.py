#!/usr/bin/env python3
"""Standalone Binance Futures USDT-M order book (OB) viewer.

Self-contained: reuses the depth-sync pattern from the main dashboard
(REST snapshot + @depth@100ms WebSocket deltas) but has no dependency on
the rest of the project. Renders a live order book in the terminal.

Usage:
    python ob_viewer.py                 # BTCUSDT, 15 levels
    python ob_viewer.py ETHUSDT
    python ob_viewer.py MORPHOUSDT --levels 20 --limit 500

Only needs: aiohttp, websockets
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
import time

import aiohttp
import websockets

FAPI_BASE = "https://fapi.binance.com"
FSTREAM_WS_BASE = "wss://fstream.binance.com"

# ANSI colors
GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"
CLEAR = "\033[2J\033[H"


# --- Depth sync helpers (mirrors app.py) ---------------------------------

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


def _depth_update_follows(last_update_id: int, data: dict) -> bool:
    """True when `data` continues the local book (Binance Futures `pu` chain)."""
    final_id = data.get("u")
    if final_id is None or int(final_id) <= last_update_id:
        return False
    pu = data.get("pu")
    if pu is not None:
        try:
            return int(pu) == last_update_id
        except (TypeError, ValueError):
            pass
    first_id = data.get("U")
    if first_id is None:
        return False
    return int(first_id) <= last_update_id + 1 <= int(final_id)


def _apply_buffered_depth_updates(
    depth_buffer: list[dict],
    last_update_id: int,
    bid_map: dict[float, float],
    ask_map: dict[float, float],
) -> int:
    start_idx: int | None = None
    for index, event in enumerate(depth_buffer):
        if _depth_update_follows(last_update_id, event):
            start_idx = index
            break
    if start_idx is None:
        return last_update_id
    for event in depth_buffer[start_idx:]:
        if int(event["u"]) <= last_update_id:
            continue
        if not _depth_update_follows(last_update_id, event):
            break
        apply_depth_update(event, bid_map, ask_map)
        last_update_id = int(event["u"])
    return last_update_id


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
    return [[p, q] for p, q in bids], [[p, q] for p, q in asks]


# --- REST snapshot -------------------------------------------------------

async def fetch_depth_snapshot(session: aiohttp.ClientSession, symbol: str, limit: int) -> dict:
    url = f"{FAPI_BASE}/fapi/v1/depth"
    params = {"symbol": symbol.upper(), "limit": limit}
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        resp.raise_for_status()
        return await resp.json()


# --- Rendering -----------------------------------------------------------

def price_fmt(price: float) -> str:
    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:,.4f}"
    return f"{price:.7f}"


def render(symbol: str, bids: list[list[float]], asks: list[list[float]], levels: int) -> str:
    bids = bids[:levels]
    asks = asks[:levels]
    best_bid = bids[0][0] if bids else 0.0
    best_ask = asks[0][0] if asks else 0.0
    mid = (best_bid + best_ask) / 2 if best_bid and best_ask else 0.0
    spread = best_ask - best_bid if best_bid and best_ask else 0.0
    spread_pct = (spread / mid * 100) if mid else 0.0

    # Walls (largest resting size within shown levels)
    support = max(bids, key=lambda x: x[1]) if bids else [0.0, 0.0]
    resistance = max(asks, key=lambda x: x[1]) if asks else [0.0, 0.0]

    max_qty = max([q for _, q in bids + asks], default=1.0) or 1.0
    bar_w = 22

    def bar(qty: float) -> str:
        filled = int(round(qty / max_qty * bar_w))
        return "█" * filled + " " * (bar_w - filled)

    lines: list[str] = []
    lines.append(CLEAR)
    lines.append(f"{BOLD}{CYAN}Order Book · {symbol.upper()} · Binance Futures USDT-M{RESET}")
    lines.append(
        f"{DIM}mid {price_fmt(mid)}   spread {price_fmt(spread)} "
        f"({spread_pct:.3f}%)   {time.strftime('%H:%M:%S')}{RESET}"
    )
    lines.append("")
    lines.append(f"{DIM}{'PRICE':>14}  {'SIZE':>14}  {'DEPTH':<{bar_w}}{RESET}")

    # Asks: farthest on top, best ask at the bottom (closest to mid)
    for price, qty in reversed(asks):
        lines.append(f"{RED}{price_fmt(price):>14}  {qty:>14,.4f}  {bar(qty)}{RESET}")

    lines.append(f"{BOLD}{'':>14}  {'──── mid ' + price_fmt(mid) + ' ────':^{18 + bar_w}}{RESET}")

    for price, qty in bids:
        lines.append(f"{GREEN}{price_fmt(price):>14}  {qty:>14,.4f}  {bar(qty)}{RESET}")

    lines.append("")
    lines.append(
        f"{GREEN}support wall {price_fmt(support[0])} ({support[1]:,.2f}){RESET}   "
        f"{RED}resistance wall {price_fmt(resistance[0])} ({resistance[1]:,.2f}){RESET}"
    )
    lines.append(f"{DIM}Ctrl+C to exit{RESET}")
    return "\n".join(lines)


# --- Main loop -----------------------------------------------------------

async def run(symbol: str, levels: int, limit: int) -> None:
    stream_url = f"{FSTREAM_WS_BASE}/stream?streams={symbol.lower()}@depth@100ms"

    async with aiohttp.ClientSession() as session:
        while True:  # reconnect loop
            try:
                async with websockets.connect(
                    stream_url, ping_interval=20, ping_timeout=20
                ) as ws:
                    bid_map, ask_map, last_update_id = await _bootstrap(
                        session, ws, symbol, limit
                    )
                    last_render = 0.0
                    while True:
                        raw = await ws.recv()
                        data = json.loads(raw).get("data", {})
                        if "u" not in data:
                            continue
                        if _depth_update_follows(last_update_id, data):
                            apply_depth_update(data, bid_map, ask_map)
                            last_update_id = int(data["u"])
                        elif int(data["u"]) <= last_update_id:
                            continue  # stale, ignore
                        else:
                            # Gap detected -> REST resync (same connection)
                            snap = await fetch_depth_snapshot(session, symbol, limit)
                            last_update_id = int(snap["lastUpdateId"])
                            bid_map = {float(p): float(q) for p, q in snap["bids"]}
                            ask_map = {float(p): float(q) for p, q in snap["asks"]}

                        now = time.monotonic()
                        if now - last_render >= 0.4:
                            bids, asks = map_to_levels(bid_map, ask_map, levels)
                            sys.stdout.write(render(symbol, bids, asks, levels))
                            sys.stdout.flush()
                            last_render = now
            except (websockets.ConnectionClosed, asyncio.TimeoutError, aiohttp.ClientError) as exc:
                print(f"\n{RED}Connection issue: {exc}. Reconnecting in 2s...{RESET}")
                await asyncio.sleep(2)


async def _bootstrap(
    session: aiohttp.ClientSession, ws, symbol: str, limit: int
) -> tuple[dict[float, float], dict[float, float], int]:
    """Buffer depth events while fetching the REST snapshot, then bridge."""
    depth_buffer: list[dict] = []
    collecting = True

    async def collect() -> None:
        while collecting:
            raw = await ws.recv()
            data = json.loads(raw).get("data", {})
            if "U" in data and "u" in data:
                depth_buffer.append(data)

    collector = asyncio.create_task(collect())
    try:
        await asyncio.sleep(0.05)
        snap = await fetch_depth_snapshot(session, symbol, limit)
    finally:
        collecting = False
        collector.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await collector

    last_update_id = int(snap["lastUpdateId"])
    bid_map = {float(p): float(q) for p, q in snap["bids"]}
    ask_map = {float(p): float(q) for p, q in snap["asks"]}
    last_update_id = _apply_buffered_depth_updates(depth_buffer, last_update_id, bid_map, ask_map)
    return bid_map, ask_map, last_update_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live Binance Futures order book viewer")
    parser.add_argument("symbol", nargs="?", default="BTCUSDT", help="Symbol, e.g. ETHUSDT")
    parser.add_argument("--levels", type=int, default=15, help="Levels to display per side")
    parser.add_argument("--limit", type=int, default=500, help="REST snapshot depth limit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run(args.symbol, args.levels, args.limit))
    except KeyboardInterrupt:
        print(f"\n{RESET}Bye.")


if __name__ == "__main__":
    main()
