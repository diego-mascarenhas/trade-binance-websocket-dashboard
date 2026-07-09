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

### Trailing TP on the opposite order book (profit-guaranteed)

The TP is a `TRAILING_STOP_MARKET` (Binance algo order) whose activation is
snapped to a **wall on the opposite side** of the book:

- SHORT → BUY trailing, activation on a **bid/support** wall below the average.
- LONG → SELL trailing, activation on an **ask/resistance** wall above the average.

The activation is clamped so the **worst-case** trailing exit (after the full
callback retrace) is still green:

```
SHORT (BUY):  activation * (1 + callback%) <= avg * (1 - fee_buffer%)
LONG  (SELL): activation * (1 - callback%) >= avg * (1 + fee_buffer%)
```

If no wall is deep enough to satisfy that, the activation is clamped to the
profit floor (so it never sits at a loss).

The TP is **automatic**. When you place the grid with `--execute`, the script
then enters a loop that manages the trailing TP for you (no extra flag needed):

```bash
# Place grid AND auto-manage the TP (default)
python3 ob_dca_grid.py SOLUSDT --direction short --execute

# Place grid only, no TP management
python3 ob_dca_grid.py SOLUSDT --direction short --execute --no-tp

# Attach the automatic TP to an ALREADY-placed grid (no new grid orders)
python3 ob_dca_grid.py SOLUSDT --direction short --tp-only --execute
```

The manager polls the live position (`positionRisk` → real average), recomputes
the activation from the current opposite-OB walls, and cancels/replaces the
reduce-only trailing TP whenever the position size or target changes. It keeps
running (waiting when there is no position yet). Stop it with `Ctrl+C`
(the last TP order stays in place).

| Flag | Default | Meaning |
|------|---------|---------|
| `--no-tp` | off | Do NOT auto-manage the TP after placing the grid |
| `--tp-only` | off | Skip the grid; only auto-manage the TP for the position |
| `--tp-callback` | `0.2` | Callback rate % (0.1..10) |
| `--tp-fee-buffer` | `0.12` | Extra profit margin % (fees+buffer) to stay green |
| `--tp-wall-min-mult` | `3` | Min wall size vs median book qty to count as a wall |
| `--tp-wall-pick` | `nearest` | `nearest` or `strongest` opposite wall |
| `--tp-poll-sec` | `5` | Position/TP re-sync interval |

> The TP uses the **live position average**, not the current price. When only the
> base is filled the average is near entry, so the activation sits close; as DCAs
> fill and the average moves, the loop moves the TP with it — always keeping the
> callback in profit.

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
