#!/usr/bin/env python3
"""Apply pending MySQL migrations from db/migrations/."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import db_store


def main() -> int:
    parser = argparse.ArgumentParser(description="Run MySQL schema migrations")
    parser.add_argument(
        "--status",
        action="store_true",
        help="List applied and pending migrations (no changes)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit 1 if pending migrations exist (no changes)",
    )
    args = parser.parse_args()

    if not db_store.is_enabled():
        print("DB_ENABLED is false — migrations skipped")
        return 0

    if args.status or args.check:
        status = db_store.migration_status()
        if status.error:
            print(status.error)
            return 1
        for version in status.applied:
            print(f"applied  {version}")
        for version in status.pending:
            print(f"pending  {version}")
        if args.check and status.pending:
            return 1
        if args.status:
            if not status.applied and not status.pending:
                print("No migration files found")
            elif not status.pending:
                print("Schema up to date")
        return 0

    result = db_store.run_migrations()
    if result.error:
        print(result.error)
        return 1
    if result.applied:
        for version in result.applied:
            print(f"Applied {version}")
        print(f"Schema ready on {db_store.DB_HOST}/{db_store.DB_NAME}")
    elif result.up_to_date:
        print(f"Schema up to date ({db_store.DB_HOST}/{db_store.DB_NAME})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
