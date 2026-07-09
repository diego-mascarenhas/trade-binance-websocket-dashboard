#!/usr/bin/env python3
"""Standalone DCA grid generator (3Commas / Binance-DCA style).

Builds an entry (Base Order) plus N safety orders (DCA) where:
  - each DCA price steps away geometrically  -> step scale
  - each DCA size grows geometrically (martingale) -> volume scale (compensation)
  - the take-profit keeps the SAME relationship to the position:
    TP is always a fixed % above (long) / below (short) the running average price.

So as each DCA fills, the average moves and the TP recomputes to hold the same
target %, exactly like the "Running Order Detail" screens.

Self-contained: only the Python standard library. No API keys.

Usage:
    python dca_grid.py MORPHOUSDT                 # fetch live price as base entry
    python dca_grid.py MORPHOUSDT --price 2.1515  # fixed entry, reproduces the sample
    python dca_grid.py ETHUSDT --direction short --tp 0.6
    python dca_grid.py --price 100 --so-count 8 --base-size 100 --so-size 100
"""

from __future__ import annotations

import argparse
import json
import urllib.request

FAPI_BASE = "https://fapi.binance.com"

GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"


def fetch_price(symbol: str) -> float:
    url = f"{FAPI_BASE}/fapi/v1/ticker/price?symbol={symbol.upper()}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        data = json.loads(resp.read().decode())
    return float(data["price"])


def price_fmt(price: float) -> str:
    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:.4f}"
    return f"{price:.7f}"


def qty_fmt(qty: float) -> str:
    if qty >= 1000:
        return f"{qty:,.1f}"
    if qty >= 1:
        return f"{qty:,.2f}"
    return f"{qty:.4f}"


def build_grid(
    entry: float,
    direction: str,
    so_count: int,
    deviation: float,
    step_scale: float,
    volume_scale: float,
    base_size: float,
    so_size: float,
    tp_pct: float,
) -> list[dict]:
    """Return the ordered list of legs with running average and TP for each fill."""
    is_long = direction == "long"

    orders: list[dict] = [
        {"name": "Base Order", "price": entry, "size_usdt": base_size, "qty": base_size / entry}
    ]

    for n in range(1, so_count + 1):
        if step_scale == 1:
            cum_dev = deviation * n
        else:
            cum_dev = deviation * (step_scale**n - 1) / (step_scale - 1)
        price = entry * (1 - cum_dev / 100) if is_long else entry * (1 + cum_dev / 100)
        size = so_size * (volume_scale ** (n - 1))
        orders.append(
            {"name": f"DCA #{n}", "price": price, "size_usdt": size, "qty": size / price}
        )

    cum_qty = 0.0
    cum_usdt = 0.0
    for o in orders:
        cum_qty += o["qty"]
        cum_usdt += o["size_usdt"]
        avg = cum_usdt / cum_qty
        o["cum_usdt"] = cum_usdt
        o["cum_qty"] = cum_qty
        o["avg"] = avg
        o["tp"] = avg * (1 + tp_pct / 100) if is_long else avg * (1 - tp_pct / 100)
        o["delta_pct"] = (o["price"] / entry - 1) * 100
    return orders


def approx_liquidation(
    avg: float, total_qty: float, total_notional: float, leverage: float, is_long: bool, mmr: float
) -> float:
    """Isolated-margin approximation for the FULLY filled grid.

    Note: real exchange liq uses cross/wallet balance and per-tier maintenance,
    so this is only an estimate.
    """
    margin = total_notional / leverage
    if is_long:
        return (total_qty * avg - margin) / (total_qty * (1 - mmr))
    return (total_qty * avg + margin) / (total_qty * (1 + mmr))


def render(symbol: str, args: argparse.Namespace, orders: list[dict], entry: float) -> str:
    is_long = args.direction == "long"
    dir_color = GREEN if is_long else RED
    tp_sign = "+" if is_long else "-"

    lines: list[str] = []
    lines.append(
        f"{BOLD}{CYAN}DCA Grid · {symbol} · "
        f"{dir_color}{args.direction.upper()} {args.leverage:g}x{CYAN}{RESET}"
    )
    lines.append(
        f"{DIM}base entry {price_fmt(entry)}  ·  {args.so_count} DCA  ·  "
        f"dev {args.deviation:g}%  step×{args.step_scale:g}  vol×{args.volume_scale:g}  ·  "
        f"TP {tp_sign}{args.tp:g}% from avg{RESET}"
    )
    lines.append("")

    header = (
        f"{'ORDER':<10} {'QTY':>12} {'PRICE':>13} {'Δ ENTRY':>9} "
        f"{'SIZE USDT':>11} {'POS USDT':>11} {'AVG':>13} {'TP PRICE':>13}"
    )
    lines.append(f"{DIM}{header}{RESET}")
    lines.append(f"{DIM}{'─' * len(header)}{RESET}")

    for o in orders:
        is_base = o["name"] == "Base Order"
        row_color = "" if is_base else (GREEN if is_long else RED)
        lines.append(
            f"{row_color}{o['name']:<10} {qty_fmt(o['qty']):>12} "
            f"{price_fmt(o['price']):>13} {o['delta_pct']:>+8.2f}% "
            f"{o['size_usdt']:>11,.2f} {o['cum_usdt']:>11,.2f} "
            f"{price_fmt(o['avg']):>13} {price_fmt(o['tp']):>13}{RESET}"
        )

    last = orders[-1]
    total_qty = last["cum_qty"]
    total_notional = last["cum_usdt"]
    margin = total_notional / args.leverage
    liq = approx_liquidation(
        last["avg"], total_qty, total_notional, args.leverage, is_long, args.mmr / 100
    )

    lines.append("")
    lines.append(
        f"{BOLD}Full grid{RESET}  qty {qty_fmt(total_qty)}  ·  "
        f"notional {total_notional:,.2f} USDT  ·  margin@{args.leverage:g}x {margin:,.2f} USDT"
    )
    lines.append(
        f"{BOLD}Full-fill avg{RESET} {price_fmt(last['avg'])}  ·  "
        f"{BOLD}Full-fill TP{RESET} {price_fmt(last['tp'])} "
        f"({tp_sign}{args.tp:g}%)"
    )
    lines.append(
        f"{RED}Approx. liquidation (isolated, full grid, mmr {args.mmr:g}%): "
        f"{price_fmt(liq)}{RESET}"
    )
    lines.append(
        f"{DIM}Note: TP stays {tp_sign}{args.tp:g}% from the running average on every fill "
        f"(same relationship). Liquidation is an isolated-margin estimate; the exchange "
        f"uses cross/wallet balance.{RESET}"
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DCA grid generator (base order + N safety orders)")
    p.add_argument("symbol", nargs="?", default=None, help="Symbol, e.g. MORPHOUSDT (fetches live price)")
    p.add_argument("--price", type=float, default=None, help="Base entry price (overrides live fetch)")
    p.add_argument("--direction", choices=["long", "short"], default="long")
    p.add_argument("--so-count", type=int, default=8, help="Number of DCA / safety orders")
    p.add_argument("--deviation", type=float, default=0.3, help="First DCA price deviation %%")
    p.add_argument("--step-scale", type=float, default=1.6, help="Price step multiplier per DCA")
    p.add_argument("--volume-scale", type=float, default=1.3, help="Size multiplier per DCA (compensation)")
    p.add_argument("--base-size", type=float, default=71.64, help="Base order size in USDT")
    p.add_argument("--so-size", type=float, default=58.99, help="First safety order size in USDT")
    p.add_argument("--tp", type=float, default=0.5, help="Take-profit %% from average")
    p.add_argument("--leverage", type=float, default=10.0)
    p.add_argument("--mmr", type=float, default=0.5, help="Maintenance margin rate %% (liq estimate)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.price is not None:
        entry = args.price
        symbol = (args.symbol or "MANUAL").upper()
    elif args.symbol:
        symbol = args.symbol.upper()
        try:
            entry = fetch_price(symbol)
        except Exception as exc:
            print(f"{RED}Could not fetch price for {symbol}: {exc}{RESET}")
            print("Pass a fixed price with --price, e.g. --price 2.1515")
            return
    else:
        print(f"{RED}Provide a symbol (e.g. MORPHOUSDT) or --price.{RESET}")
        return

    if entry <= 0 or args.so_count < 1:
        print(f"{RED}Invalid entry price or so-count.{RESET}")
        return

    orders = build_grid(
        entry,
        args.direction,
        args.so_count,
        args.deviation,
        args.step_scale,
        args.volume_scale,
        args.base_size,
        args.so_size,
        args.tp,
    )
    print(render(symbol, args, orders, entry))


if __name__ == "__main__":
    main()
