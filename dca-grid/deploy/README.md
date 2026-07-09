# Deploy on Ubuntu (systemd)

`ob_dca_grid.py` uses only the Python standard library — no pip installs, no venv.
The trailing-TP manager runs as a `systemd` service per symbol (auto-restart,
survives reboots). Placement of the grid is a one-shot command you run once.

## 1. Copy the script

```bash
sudo mkdir -p /opt/dca-grid /var/log/dca-grid
sudo cp ob_dca_grid.py /opt/dca-grid/
# create a user to run it (optional but recommended)
sudo useradd -r -s /usr/sbin/nologin trader 2>/dev/null || true
sudo chown -R trader:trader /opt/dca-grid /var/log/dca-grid
```

Ubuntu 20.04+ ships Python 3.8+, which is enough (the script uses
`from __future__ import annotations`). Check with `python3 --version`.

## 2. API keys

Put your Binance Futures keys in `/etc/dca-grid.env` (systemd reads it as env
vars — the script picks up `BINANCE_API_KEY` / `BINANCE_SECRET_KEY` from the
environment automatically):

```bash
sudo tee /etc/dca-grid.env >/dev/null <<'EOF'
BINANCE_API_KEY=your_key
BINANCE_SECRET_KEY=your_secret
FAPI_BASE=https://fapi.binance.com
EOF
sudo chmod 600 /etc/dca-grid.env
```

## 3. Install the service template

```bash
sudo cp deploy/dca-tp@.service /etc/systemd/system/
sudo systemctl daemon-reload
```

## 4. Place a grid (one-shot, per symbol)

Run once per symbol you want to trade. This places the LIMIT grid (auto-direction,
entry = 5% of wallet, max leverage) and exits without starting a manager:

```bash
sudo -u trader BINANCE_API_KEY=... BINANCE_SECRET_KEY=... \
  python3 /opt/dca-grid/ob_dca_grid.py SOLUSDT --max-range 12 --execute --no-tp
```

(or `set -a; . /etc/dca-grid.env; set +a` first so the keys are in your shell.)

## 5. Start the TP manager service (per symbol)

```bash
sudo systemctl enable --now dca-tp@SOLUSDT
sudo systemctl enable --now dca-tp@EIGENUSDT
```

The manager auto-detects the open side, keeps one reduce-only trailing TP synced
to the position, and removes any foreign SL (from other scripts). It is safe to
restart at any time (idempotent).

## Operate

```bash
systemctl status dca-tp@SOLUSDT
journalctl -u dca-tp@SOLUSDT -f          # or tail -f /var/log/dca-grid/SOLUSDT.log
sudo systemctl restart dca-tp@SOLUSDT
sudo systemctl disable --now dca-tp@SOLUSDT
```

## Fully autonomous option (recommended for a server): supervisor

Instead of the separate "place once + manage" flow, a single service per symbol
can **re-arm the grid whenever the account is flat** and manage the trailing TP
the rest of the time — no manual placement needed:

```bash
sudo cp deploy/dca-super@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now dca-super@ADAUSDT
sudo systemctl enable --now dca-super@EIGENUSDT
```

Each supervisor loop:
- If a position is open → keep the trailing TP synced (and remove foreign SLs).
- If flat with resting orders → wait (grid armed).
- If flat with no orders → re-arm the grid (auto-direction, 5% wallet, max leverage).

Use either the `dca-tp@` (manage only) **or** the `dca-super@` (autonomous) service
per symbol — not both.

## Notes

- The trailing TP lives on Binance and trails on its own; the service only
  (re)places it when the position size changes (a DCA fills) and cleans foreign SLs.
- Placement (step 4) is separate from the manager (step 5) on purpose: a service
  that restarts should never re-place orders. Re-run step 4 manually when you want
  a fresh grid after a position fully closes.
- To keep the wallet-% sizing and max-leverage behavior, no extra flags are needed.
