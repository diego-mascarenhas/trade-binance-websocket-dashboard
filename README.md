# trade-binance-websocket-dashboard

**trade-binance-websocket-dashboard** is a **Binance Spot** live dashboard that streams **klines** and the **order book** over WebSocket, renders OHLC candles with pattern markers (Hammer, Shooting Star), and shows depth, spread, and volume metrics in a **Plotly Dash** UI.

Optional **order execution** sends **Binance Futures** LIMIT entries (with SL/TP algos) when a **valid chart entry** is confirmed — same REST flow as [trade-binance-websocket-order-blocks](https://github.com/idoneo/trade-binance-websocket-order-blocks). Use `EXECUTION_MODE=dry` to log only, or `live` with API keys.

## Requirements

* macOS or Linux with **Python 3.10+**
* pip: `python3 -m pip install --upgrade pip`
* Network access to Binance REST and WebSocket APIs (no API keys required for public streams)

## Installation

Use a **virtual environment** (`.venv/`). Project dependencies (`pandas`, `dash`, etc.) are installed **inside** that folder, not in the system Python.

| Command | Result |
| -------- | ------ |
| `python3 app.py` (no venv) | Often fails with `ModuleNotFoundError: No module named 'pandas'` |
| `source .venv/bin/activate` then `python app.py` | Correct |

```bash
git clone <your-repo> trade-binance-websocket-dashboard
cd trade-binance-websocket-dashboard
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env
# Edit .env if needed (`.env` is gitignored; see `.env.example` for all defaults)
```

Check the venv is active — the prompt should show `(.venv)` and:

```bash
which python
# .../trade-binance-websocket-dashboard/.venv/bin/python
```

## Usage

The **only entry script** in this project is `app.py` (project root). Styles live in `assets/`.

```bash
source .venv/bin/activate
python app.py

# Single symbol (overrides SYMBOL in .env — same as trade-binance-websocket-order-blocks)
python app.py BNBUSDT
python app.py etcusdt --port 8051

# If the port is already in use, the web UI is skipped (no error); websocket + execution keep running
python app.py ETCUSDT --port 8050
```

Open the dashboard at `http://127.0.0.1:8050` (or the host/port from `.env` / `--port`).

### Main variables (`.env`)

| Variable       | Description                          | Default   |
| -------------- | ------------------------------------ | --------- |
| `SYMBOL`       | Trading pair (lowercase) — default when no CLI arg | `btcusdt` |
| `INTERVAL`     | Kline interval (e.g. `1m`, `5m`)     | `1m`      |
| `DEPTH_LEVELS` | Order book levels shown in the chart | `20`      |
| `MAX_CANDLES`  | Max candles kept in memory           | `200`     |
| `MIN_CONFIDENCE` | Minimum confidence to highlight a **TRADE** setup (same logic as order-blocks bot) | `50` |
| `DASH_HOST`    | Dash bind address                    | `0.0.0.0` |
| `DASH_PORT`    | Dash HTTP port — default when no `--port` | `8050`    |
| `EXECUTION_ENABLED` | Enable Binance Futures execution module | `false` |
| `EXECUTE_ON_VALID_ENTRY` | Auto-send orders when a valid entry is confirmed | `false` |
| `EXECUTION_MODE` | `dry` (log only) or `live` (Binance Futures REST) | `dry` |
| `BINANCE_API_KEY` / `BINANCE_SECRET_KEY` | Required for `live` mode | — |
| `POSITION_SIZE_USDT` | Notional per entry (Futures) | `25` |
| `LEVERAGE_MODE` | `max` = max per symbol via API, `fixed` = use `LEVERAGE` | `fixed` |
| `LEVERAGE` | Fixed leverage, or fallback when `LEVERAGE_MODE=max` / API fails | `4` |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | Optional Telegram alerts + commands `/status` `/stop` `/start` | — |

Telegram commands (same chat as `TELEGRAM_CHAT_ID` only). With `./run-all.sh`, **one** fleet listener handles commands for all pairs (avoids 409 Conflict):

- `/status` — fleet overview: TRADE setups, open positions, offline count
- `/stop` — pause trading on **all** pairs (shared `logs/fleet.state`)
- `/start` — resume fleet trading

Individual dashboards only send alerts (valid entry, positions, dry-run/live). They do not poll Telegram commands.

Single-pair mode: set `TELEGRAM_COMMANDS_ENABLED=true` on `app.py` to restore per-process commands.

**Note:** Several scripts may **send** alerts with the same `TELEGRAM_BOT_TOKEN` (e.g. this dashboard + order-blocks bot). That is fine. Only **one** process may **poll** `getUpdates` for commands; if you see `409 Conflict`, stop the other poller or use a separate bot token for commands.

The app loads historical klines via REST, then keeps the order book in sync using Binance’s depth snapshot + incremental updates. It also streams `@miniTicker` for 24h change and computes **order-block signal + confidence** (support/resistance walls, zone position, and fallback 24h rules). With `EXECUTE_ON_VALID_ENTRY=true` and `EXECUTION_ENABLED=true`, confirmed valid entries can open **Futures** positions (dry-run or live). The UI refreshes every 1.5s.

## Ubuntu / VPS (e.g. Forge, Sleipnir)

**First time** on the server:

```bash
cd ~/scripts/trade-binance-websocket-dashboard   # or your deploy path

sudo apt update
sudo apt install -y python3 python3-venv python3-pip

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

cp .env.example .env
nano .env   # TELEGRAM, BINANCE keys, EXECUTION_*, etc.
```

**Every time** you start the bot (SSH session):

```bash
cd ~/scripts/trade-binance-websocket-dashboard
source .venv/bin/activate
python app.py DOGEUSDT --port 8051
```

Use `python` from the venv, **not** bare `python3 app.py` from the system.

### Keep running after SSH disconnect

```bash
source .venv/bin/activate
screen -S dashboard
python app.py DOGEUSDT --port 8051
# Detach: Ctrl+A, then D
# Reattach later: screen -r dashboard
```

Headless (no browser needed): the bot still streams data and can place orders; open the UI only if you need charts (`http://your-server-ip:8051`).

### Telegram on the VPS

Alerts use outbound HTTPS to `api.telegram.org`. Test from the server:

```bash
curl -s --max-time 5 "https://api.telegram.org"
```

If that times out, fix firewall/DNS — **trades on Binance still work**; only Telegram alerts/commands are affected.

## Hosting

The dashboard can run on any machine with Python (local dev, VPS, or dedicated server). If you need hosting, you can rent a VPS at **REVISION ALPHA**.

## Project layout

```
trade-binance-websocket-dashboard/
├── app.py               # Entry script (run this)
├── execution.py         # Binance Futures dry/live order execution
├── telegram_notify.py   # Telegram alerts (optional)
├── assets/              # Dash static assets (custom.css)
├── .env.example
├── requirements.txt
└── README.md
```

## License

**trade-binance-websocket-dashboard** is licensed under **AGPL-3.0**.

If you deploy or modify this software (including running the dashboard on a server), you must comply with AGPL-3.0 (source availability, license notices, and documenting changes) and:

1. Notify the project maintainers: hola@idoneo.dev
2. Share modifications or enhancements via the same contact

## Contact

Questions, support, or license-related notices: hola@idoneo.dev

## Security

Do not commit `.env` or secrets to the repository. Public Spot streams need no keys; **live execution** requires Futures API keys with trade permissions. Report security issues to hola@idoneo.dev.

## Disclaimer

Trading software carries risk of capital loss. With `EXECUTION_MODE=live`, this app can place real orders on Binance Futures. Use dry-run first, understand the risks, and treat all signals as informational — not financial advice.
