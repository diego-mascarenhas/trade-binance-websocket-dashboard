# trade-binance-websocket-dashboard

**trade-binance-websocket-dashboard** is a **Binance Spot** live dashboard that streams **klines** and the **order book** over WebSocket, renders OHLC candles with pattern markers (Hammer, Shooting Star), and shows depth, spread, and volume metrics in a **Plotly Dash** UI.

## Requirements

* macOS or Linux with **Python 3.10+**
* pip: `python3 -m pip install --upgrade pip`
* Network access to Binance REST and WebSocket APIs (no API keys required for public streams)

## Installation

```bash
git clone <your-repo> trade-binance-websocket-dashboard
cd trade-binance-websocket-dashboard
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env if needed (`.env` is gitignored; see `.env.example` for all defaults)
```

## Usage

The **only entry script** in this project is `app.py` (project root). Styles live in `assets/`.

```bash
source .venv/bin/activate
python app.py
```

Open the dashboard at `http://127.0.0.1:8050` (or the host/port set in `.env`).

### Main variables (`.env`)

| Variable       | Description                          | Default   |
| -------------- | ------------------------------------ | --------- |
| `SYMBOL`       | Trading pair (lowercase)             | `btcusdt` |
| `INTERVAL`     | Kline interval (e.g. `1m`, `5m`)     | `1m`      |
| `DEPTH_LEVELS` | Order book levels shown in the chart | `20`      |
| `MAX_CANDLES`  | Max candles kept in memory           | `200`     |
| `DASH_HOST`    | Dash bind address                    | `0.0.0.0` |
| `DASH_PORT`    | Dash HTTP port                       | `8050`    |

The app loads historical klines via REST, then keeps the order book in sync using Binance’s depth snapshot + incremental updates. The UI refreshes every 1.5s.

## Hosting

The dashboard can run on any machine with Python (local dev, VPS, or dedicated server). If you need hosting, you can rent a VPS at **REVISION ALPHA**.

## Project layout

```
trade-binance-websocket-dashboard/
├── app.py               # Entry script (run this)
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

Do not commit `.env` or secrets to the repository. This project uses **public** Binance market data only; no trading keys are required. Report security issues to hola@idoneo.dev.

## Disclaimer

Trading software carries risk of capital loss. This dashboard is for monitoring and analysis only; it does not place orders. Use at your own risk; this is not financial advice.
