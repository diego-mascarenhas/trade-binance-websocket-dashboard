#!/usr/bin/env python3
"""DCA grid anchored to REAL order book levels (not a geometric derivation).

Instead of deriving DCA prices from a step formula, this reads the live order
book and places each DCA on an actual wall going *backwards* from entry:
  - LONG  -> significant BID walls below the entry price
  - SHORT -> significant ASK walls above the entry price

Sizes still compensate so the take-profit keeps the SAME relationship to the
position (TP is a fixed % above/below the running average price).

Self-contained: Python standard library only. No API keys.

Usage:
    python ob_dca_grid.py MORPHOUSDT                 # live book + live entry
    python ob_dca_grid.py MORPHOUSDT --price 2.0816  # fixed entry
    python ob_dca_grid.py ETHUSDT --direction short
    python ob_dca_grid.py MORPHOUSDT --size-mode wall --so-count 8
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import ROUND_DOWN, ROUND_UP, Decimal

FAPI_BASE = os.getenv("FAPI_BASE", "https://fapi.binance.com").rstrip("/")

GREEN = "\033[32m"
RED = "\033[31m"
CYAN = "\033[36m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"


def fetch_depth(symbol: str, limit: int) -> dict:
    url = f"{FAPI_BASE}/fapi/v1/depth?symbol={symbol.upper()}&limit={limit}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read().decode())


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


def select_walls(
    levels: list[list[float]],
    entry: float,
    is_long: bool,
    count: int,
    min_gap_pct: float,
    min_dist_pct: float,
    max_range_pct: float,
) -> list[tuple[float, float, float]]:
    """Pick the most significant walls (by resting size) going backwards from entry.

    Greedy by liquidity, enforcing a minimum price gap so picks are spread out.
    Returns [(price, wall_qty, dist_pct), ...] sorted by distance from entry.
    """
    cands: list[tuple[float, float, float]] = []
    for price, qty in levels:
        if is_long and price >= entry:
            continue
        if not is_long and price <= entry:
            continue
        dist = abs(price - entry) / entry * 100
        if dist < min_dist_pct:
            continue
        if max_range_pct > 0 and dist > max_range_pct:
            continue
        cands.append((price, qty, dist))

    cands.sort(key=lambda x: x[1], reverse=True)  # biggest walls first
    picked: list[tuple[float, float, float]] = []
    for price, qty, dist in cands:
        if all(abs(price - p) / entry * 100 >= min_gap_pct for p, _, _ in picked):
            picked.append((price, qty, dist))
        if len(picked) >= count:
            break

    picked.sort(key=lambda x: x[2])  # nearest to entry = DCA #1
    return picked


def build_grid(
    entry: float,
    is_long: bool,
    walls: list[tuple[float, float, float]],
    base_size: float,
    tp_pct: float,
    size_mode: str,
    comp_factor: float,
    so_size: float,
    volume_scale: float,
) -> list[dict]:
    orders: list[dict] = [
        {
            "name": "Base Order",
            "price": entry,
            "wall_qty": None,
            "size_usdt": base_size,
            "qty": base_size / entry,
        }
    ]

    prev_dist = 0.0
    for i, (price, wall_qty, dist) in enumerate(walls, start=1):
        if size_mode == "comp":
            # Distance compensation: farther wall -> bigger add (keeps TP relationship).
            band = max(dist - prev_dist, 0.0)
            size = base_size * band * comp_factor
        elif size_mode == "wall":
            size = None  # filled in after we know total wall liquidity
        elif size_mode == "scale":
            size = so_size * (volume_scale ** (i - 1))
        else:  # flat
            size = so_size
        orders.append(
            {
                "name": f"DCA #{i}",
                "price": price,
                "wall_qty": wall_qty,
                "size_usdt": size,
                "dist": dist,
            }
        )
        prev_dist = dist

    if size_mode == "wall":
        total_wall = sum(w[1] for w in walls) or 1.0
        # Distribute a budget proportional to each wall's liquidity.
        budget = base_size * len(walls)  # default budget = base_size per DCA on average
        for o in orders[1:]:
            o["size_usdt"] = budget * (o["wall_qty"] / total_wall)

    for o in orders[1:]:
        o["qty"] = o["size_usdt"] / o["price"]

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


def render(symbol: str, args: argparse.Namespace, orders: list[dict], entry: float, found: int) -> str:
    is_long = args.direction == "long"
    dir_color = GREEN if is_long else RED
    tp_sign = "+" if is_long else "-"
    side = "BID walls below" if is_long else "ASK walls above"

    lines: list[str] = []
    lines.append(
        f"{BOLD}{CYAN}OB DCA Grid · {symbol} · "
        f"{dir_color}{args.direction.upper()} {args.leverage:g}x{CYAN}{RESET}"
    )
    lines.append(
        f"{DIM}entry {price_fmt(entry)}  ·  {found} DCA on {side}  ·  "
        f"size-mode {args.size_mode}  ·  TP {tp_sign}{args.tp:g}% from avg  ·  "
        f"depth limit {args.limit}, min-gap {args.min_gap:g}%{RESET}"
    )
    lines.append("")

    header = (
        f"{'ORDER':<10} {'QTY':>12} {'PRICE':>13} {'Δ ENTRY':>9} "
        f"{'WALL':>12} {'SIZE USDT':>11} {'POS USDT':>11} {'AVG':>13} {'TP PRICE':>13}"
    )
    lines.append(f"{DIM}{header}{RESET}")
    lines.append(f"{DIM}{'─' * len(header)}{RESET}")

    for o in orders:
        is_base = o["name"] == "Base Order"
        row_color = "" if is_base else (GREEN if is_long else RED)
        wall = "—" if o["wall_qty"] is None else qty_fmt(o["wall_qty"])
        lines.append(
            f"{row_color}{o['name']:<10} {qty_fmt(o['qty']):>12} "
            f"{price_fmt(o['price']):>13} {o['delta_pct']:>+8.2f}% "
            f"{wall:>12} {o['size_usdt']:>11,.2f} {o['cum_usdt']:>11,.2f} "
            f"{price_fmt(o['avg']):>13} {price_fmt(o['tp']):>13}{RESET}"
        )

    last = orders[-1]
    margin = last["cum_usdt"] / args.leverage
    lines.append("")
    lines.append(
        f"{BOLD}Full grid{RESET}  qty {qty_fmt(last['cum_qty'])}  ·  "
        f"notional {last['cum_usdt']:,.2f} USDT  ·  margin@{args.leverage:g}x {margin:,.2f} USDT"
    )
    lines.append(
        f"{BOLD}Full-fill avg{RESET} {price_fmt(last['avg'])}  ·  "
        f"{BOLD}Full-fill TP{RESET} {price_fmt(last['tp'])} ({tp_sign}{args.tp:g}%)"
    )
    lines.append(
        f"{DIM}Each DCA sits on a real order-book wall (WALL column = resting size there). "
        f"TP stays {tp_sign}{args.tp:g}% from the running average on every fill.{RESET}"
    )
    if found < args.so_count:
        lines.append(
            f"{RED}Only {found}/{args.so_count} qualifying walls found in the fetched depth. "
            f"Try a higher --limit or a smaller --min-gap / larger --max-range.{RESET}"
        )
    return "\n".join(lines)


# --- Execution (Binance Futures LIMIT orders) ----------------------------

def load_keys(env_file: str | None) -> tuple[str, str]:
    """Read API keys from environment, falling back to an .env file."""
    api = os.getenv("BINANCE_API_KEY", "")
    sec = os.getenv("BINANCE_SECRET_KEY", "")
    if api and sec:
        return api, sec
    path = env_file or os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                v = v.strip().strip('"').strip("'")
                if k.strip() == "BINANCE_API_KEY" and not api:
                    api = v
                elif k.strip() == "BINANCE_SECRET_KEY" and not sec:
                    sec = v
    return api, sec


def _public_get(path: str, params: dict) -> dict:
    url = f"{FAPI_BASE}{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read().decode())


def _signed_request(method: str, path: str, params: dict, api: str, sec: str, recv_window: int) -> dict:
    params = dict(params)
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = recv_window
    query = urllib.parse.urlencode(params)
    sig = hmac.new(sec.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"{FAPI_BASE}{path}?{query}&signature={sig}"
    req = urllib.request.Request(url, method=method, headers={"X-MBX-APIKEY": api})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode()
        raise RuntimeError(f"HTTP {exc.code}: {body}") from None


def load_symbol_filters(symbol: str) -> dict[str, Decimal]:
    info = _public_get("/fapi/v1/exchangeInfo", {})
    filt = {
        "tick_size": Decimal("0.01"),
        "step_size": Decimal("0.001"),
        "min_qty": Decimal("0.001"),
        "min_notional": Decimal("5"),
    }
    for item in info.get("symbols", []):
        if item.get("symbol") != symbol.upper():
            continue
        for f in item.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                filt["tick_size"] = Decimal(str(f.get("tickSize", "0.01")))
            elif f.get("filterType") == "LOT_SIZE":
                filt["step_size"] = Decimal(str(f.get("stepSize", "0.001")))
                filt["min_qty"] = Decimal(str(f.get("minQty", "0.001")))
            elif f.get("filterType") == "MIN_NOTIONAL":
                filt["min_notional"] = Decimal(str(f.get("notional", "5")))
        break
    return filt


def _dec_places(step: Decimal) -> int:
    exp = step.normalize().as_tuple().exponent
    return max(0, -exp)


def _round_to(value: float, step: Decimal, rounding: str) -> Decimal:
    return (Decimal(str(value)) / step).to_integral_value(rounding=rounding) * step


def prepare_orders(orders: list[dict], symbol: str, is_long: bool, filt: dict[str, Decimal]) -> list[dict]:
    """Round price/qty to exchange precision and enforce min qty / min notional."""
    tick = filt["tick_size"]
    step = filt["step_size"]
    price_dp = _dec_places(tick)
    qty_dp = _dec_places(step)
    price_round = ROUND_DOWN if is_long else ROUND_UP  # entry limits do not cross away

    prepared: list[dict] = []
    for o in orders:
        price_d = _round_to(o["price"], tick, price_round)
        qty_d = _round_to(o["qty"], step, ROUND_DOWN)
        if qty_d < filt["min_qty"]:
            qty_d = filt["min_qty"]
        # Bump quantity up to satisfy min notional
        while qty_d * price_d < filt["min_notional"]:
            qty_d += step
        prepared.append(
            {
                "name": o["name"],
                "price": f"{price_d:.{price_dp}f}",
                "quantity": f"{qty_d:.{qty_dp}f}",
                "notional": float(price_d * qty_d),
            }
        )
    return prepared


def place_orders(
    symbol: str,
    is_long: bool,
    prepared: list[dict],
    args: argparse.Namespace,
) -> None:
    side = "BUY" if is_long else "SELL"
    api, sec = load_keys(args.env_file)
    if not api or not sec:
        print(f"{RED}No API keys found (env or .env). Cannot execute.{RESET}")
        return

    # Position mode
    hedge = False
    if args.position_mode == "hedge":
        hedge = True
    elif args.position_mode == "oneway":
        hedge = False
    else:
        try:
            resp = _signed_request("GET", "/fapi/v1/positionSide/dual", {}, api, sec, args.recv_window)
            hedge = bool(resp.get("dualSidePosition"))
        except Exception as exc:
            print(f"{RED}Could not detect position mode ({exc}); assuming one-way.{RESET}")

    if args.set_leverage:
        try:
            _signed_request(
                "POST", "/fapi/v1/leverage",
                {"symbol": symbol.upper(), "leverage": int(args.set_leverage)},
                api, sec, args.recv_window,
            )
            print(f"{DIM}Leverage set to {args.set_leverage}x{RESET}")
        except Exception as exc:
            print(f"{RED}Set leverage failed: {exc}{RESET}")

    print(f"\n{BOLD}Placing {len(prepared)} {side} LIMIT orders on {symbol.upper()} "
          f"({'hedge' if hedge else 'one-way'} mode)...{RESET}")
    placed = 0
    for o in prepared:
        params = {
            "symbol": symbol.upper(),
            "side": side,
            "type": "LIMIT",
            "timeInForce": args.tif,
            "quantity": o["quantity"],
            "price": o["price"],
        }
        if hedge:
            params["positionSide"] = "LONG" if is_long else "SHORT"
        try:
            resp = _signed_request("POST", "/fapi/v1/order", params, api, sec, args.recv_window)
            print(f"{GREEN}✓ {o['name']:<10} {side} {o['quantity']} @ {o['price']}  "
                  f"orderId={resp.get('orderId')}{RESET}")
            placed += 1
        except Exception as exc:
            print(f"{RED}✗ {o['name']:<10} {side} {o['quantity']} @ {o['price']}  → {exc}{RESET}")
    print(f"{BOLD}Placed {placed}/{len(prepared)} orders.{RESET}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DCA grid anchored to real order-book walls")
    p.add_argument("symbol", help="Symbol, e.g. MORPHOUSDT")
    p.add_argument("--price", type=float, default=None, help="Entry price (default: live mid)")
    p.add_argument("--direction", choices=["long", "short"], default="long")
    p.add_argument("--so-count", type=int, default=8, help="Number of DCA orders (walls to place)")
    p.add_argument("--limit", type=int, default=1000, help="Order book depth to fetch (5..1000)")
    p.add_argument("--min-gap", type=float, default=0.8, help="Min %% spacing between chosen walls")
    p.add_argument("--min-dist", type=float, default=0.1, help="Min %% distance of first wall from entry")
    p.add_argument("--max-range", type=float, default=0.0, help="Only walls within this %% of entry (0=off)")
    p.add_argument(
        "--size-mode",
        choices=["comp", "wall", "scale", "flat"],
        default="comp",
        help="comp=distance compensation, wall=∝ wall liquidity, scale=geometric, flat=equal",
    )
    p.add_argument("--base-size", type=float, default=71.64, help="Base order size in USDT")
    p.add_argument("--comp-factor", type=float, default=1.0, help="USDT per %% band per base size (comp mode)")
    p.add_argument("--so-size", type=float, default=58.99, help="First/each DCA size (scale/flat modes)")
    p.add_argument("--volume-scale", type=float, default=1.3, help="Size multiplier per DCA (scale mode)")
    p.add_argument("--tp", type=float, default=0.5, help="Take-profit %% from average")
    p.add_argument("--leverage", type=float, default=10.0)
    # Execution (LIMIT orders on Binance Futures)
    p.add_argument("--execute", action="store_true", help="ACTUALLY place the LIMIT orders (else dry-run)")
    p.add_argument("--tif", choices=["GTC", "GTX", "IOC", "FOK"], default="GTC", help="Time in force")
    p.add_argument("--position-mode", choices=["auto", "hedge", "oneway"], default="auto")
    p.add_argument("--set-leverage", type=int, default=0, help="Set leverage before placing (0=skip)")
    p.add_argument("--recv-window", type=int, default=5000)
    p.add_argument("--env-file", default=None, help="Path to .env with API keys (default: project root)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    is_long = args.direction == "long"

    try:
        depth = fetch_depth(args.symbol, args.limit)
    except Exception as exc:
        print(f"{RED}Could not fetch depth for {args.symbol.upper()}: {exc}{RESET}")
        return

    bids = [[float(p), float(q)] for p, q in depth["bids"]]
    asks = [[float(p), float(q)] for p, q in depth["asks"]]
    if not bids or not asks:
        print(f"{RED}Empty order book.{RESET}")
        return

    best_bid, best_ask = bids[0][0], asks[0][0]
    entry = args.price if args.price is not None else (best_bid + best_ask) / 2

    levels = bids if is_long else asks
    walls = select_walls(
        levels, entry, is_long, args.so_count, args.min_gap, args.min_dist, args.max_range
    )
    if not walls:
        print(f"{RED}No qualifying walls found. Adjust --min-gap/--min-dist/--max-range/--limit.{RESET}")
        return

    orders = build_grid(
        entry,
        is_long,
        walls,
        args.base_size,
        args.tp,
        args.size_mode,
        args.comp_factor,
        args.so_size,
        args.volume_scale,
    )
    print(render(args.symbol.upper(), args, orders, entry, len(walls)))

    # Prepare orders with exchange precision (price/qty rounding, min notional)
    try:
        filt = load_symbol_filters(args.symbol)
    except Exception as exc:
        print(f"{RED}Could not load symbol filters: {exc}{RESET}")
        return
    prepared = prepare_orders(orders, args.symbol, is_long, filt)

    side = "BUY" if is_long else "SELL"
    print(f"\n{BOLD}{CYAN}Orders to send ({side} LIMIT, rounded to exchange precision):{RESET}")
    for o in prepared:
        print(f"  {o['name']:<10} {side} {o['quantity']:>14} @ {o['price']:>13}  "
              f"(~{o['notional']:.2f} USDT)")

    if args.execute:
        place_orders(args.symbol, is_long, prepared, args)
    else:
        print(f"\n{DIM}DRY-RUN — no orders sent. Re-run with --execute to place them for real.{RESET}")


if __name__ == "__main__":
    main()
