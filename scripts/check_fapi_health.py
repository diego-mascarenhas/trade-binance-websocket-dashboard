#!/usr/bin/env python3
"""CLI wrapper for fapi_watch (also runs in background via telegram_fleet.py)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import fapi_watch


def main() -> int:
    parser = argparse.ArgumentParser(description="Watch fapi.binance.com and pause fleet on block.")
    parser.add_argument(
        "--kill-fleet",
        action="store_true",
        help="Also run ./run-all.sh stop (kills hub + all dashboards)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check only; do not pause trading or send Telegram",
    )
    args = parser.parse_args()
    code = fapi_watch.run_check(dry_run=args.dry_run, kill_fleet=args.kill_fleet)
    if code == 0:
        print("[fapi_watch] OK")
    else:
        print("[fapi_watch] BLOCKED", file=sys.stderr)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
