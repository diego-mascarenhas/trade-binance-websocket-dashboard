# DCA Grid Generators

Two standalone generators for a **base order + N safety orders (DCA)** grid, in
the same style as the "Running Order Detail" screens.

Self-contained: **Python standard library only**, no pip installs, no API keys.

- **`dca_grid.py`** — DCA prices *derived* from a geometric step formula.
- **`ob_dca_grid.py`** — DCA prices *anchored to real order-book walls* (reads
  the live book and places each DCA on an actual level going backwards).

Both keep the same TP relationship: TP is always a fixed % above (long) / below
(short) the running average, recomputed on every fill.

---

## `ob_dca_grid.py` — anchored to the order book

Reads the live order book and puts each DCA on a **real wall** going backwards
from entry: BID walls below (long) or ASK walls above (short). The `WALL` column
shows the resting size at that level.

```bash
python3 ob_dca_grid.py MORPHOUSDT                  # live book + live entry
python3 ob_dca_grid.py MORPHOUSDT --price 2.0816   # fixed entry
python3 ob_dca_grid.py ETHUSDT --direction short
python3 ob_dca_grid.py MORPHOUSDT --size-mode wall # size ∝ wall liquidity
```

Wall selection: the most significant walls by resting size, spread out with a
minimum gap (`--min-gap`), within the fetched depth (`--limit`, up to 1000).

| Flag | Default | Meaning |
|------|---------|---------|
| `--price` | live mid | Entry price |
| `--direction` | `long` | `long` (bid walls) / `short` (ask walls) |
| `--so-count` | `8` | Number of DCA (walls) to place |
| `--limit` | `1000` | Order book depth to fetch (5..1000) |
| `--min-gap` | `0.8` | Min % spacing between chosen walls |
| `--min-dist` | `0.1` | Min % distance of first wall from entry |
| `--max-range` | `0` | Only walls within this % of entry (0 = off) |
| `--size-mode` | `comp` | `comp` (distance compensation), `wall` (∝ wall size), `scale` (geometric), `flat` (equal) |
| `--base-size` | `71.64` | Base order size (USDT) |
| `--comp-factor` | `1.0` | USDT per % band per base size (comp mode) |
| `--tp` | `0.5` | Take-profit % from average |
| `--leverage` | `10` | For margin/notional summary |

> Depth from the REST snapshot reaches only as far as the book is populated
> (often ~20-30% for alts). If fewer than `--so-count` walls qualify, raise
> `--limit` or lower `--min-gap`.

---

## `dca_grid.py` — geometric derivation

## The idea

- Each DCA price steps away geometrically from the entry → **step scale**.
- Each DCA size grows geometrically (martingale) → **volume scale** (this is the
  *compensation*).
- The take-profit always sits a fixed % above (long) / below (short) the
  **running average price**. As each DCA fills, the average moves and the TP
  recomputes to hold the *same relationship*.

## Run

```bash
python3 dca_grid.py MORPHOUSDT                 # fetch live price as base entry
python3 dca_grid.py MORPHOUSDT --price 2.1515  # fixed entry (reproduces the sample)
python3 dca_grid.py ETHUSDT --direction short --tp 0.6
python3 dca_grid.py --price 100 --base-size 100 --so-size 100   # generic
```

## Options

| Flag | Default | Meaning |
|------|---------|---------|
| `symbol` | – | Symbol; fetches live price if `--price` omitted |
| `--price` | live | Base entry price |
| `--direction` | `long` | `long` or `short` |
| `--so-count` | `8` | Number of DCA / safety orders |
| `--deviation` | `0.3` | First DCA price deviation (%) |
| `--step-scale` | `1.6` | Price step multiplier per DCA |
| `--volume-scale` | `1.3` | Size multiplier per DCA (compensation) |
| `--base-size` | `71.64` | Base order size (USDT) |
| `--so-size` | `58.99` | First safety order size (USDT) |
| `--tp` | `0.5` | Take-profit % from the average |
| `--leverage` | `10` | Leverage (for the liquidation estimate) |
| `--mmr` | `0.5` | Maintenance margin rate % (liquidation estimate) |

The defaults reproduce the MORPHOUSDT sample. At DCA #5 they yield avg ≈ 2.1014
and TP ≈ 2.1119, matching the on-exchange screen.

## Notes

- The TP column shows the take-profit **after each fill** — it always stays the
  configured % from the running average (the "same relationship to TP").
- Liquidation is an **isolated-margin estimate** for the fully-filled grid; the
  exchange computes it from cross/wallet balance and per-tier maintenance, so it
  will differ.
