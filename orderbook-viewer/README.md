# Order Book (OB) Viewer

Standalone live order book viewer for Binance Futures USDT-M. Self-contained,
no dependency on the rest of the project. Reuses the same depth-sync pattern
as the main dashboard (REST snapshot + `@depth@100ms` WebSocket deltas).

No API keys required (public market data only).

## Install

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Or reuse the project's existing `.venv`:

```bash
source ../.venv/bin/activate
```

## Run

```bash
python ob_viewer.py                       # BTCUSDT, 15 levels
python ob_viewer.py ETHUSDT
python ob_viewer.py MORPHOUSDT --levels 20
python ob_viewer.py TLMUSDT --levels 25 --limit 1000
```

Press `Ctrl+C` to exit.

## What it shows

- Live bids (green) / asks (red) with depth bars
- Mid price and spread (absolute + %)
- Largest support / resistance walls within the shown levels
