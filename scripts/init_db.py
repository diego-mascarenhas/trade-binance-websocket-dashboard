#!/usr/bin/env python3
"""Apply pending MySQL migrations (wrapper for scripts/migrate_db.py)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import db_store


def main() -> int:
    if not db_store.is_enabled():
        print("Set DB_ENABLED=true in .env before running init_db.py")
        return 1
    result = db_store.run_migrations()
    if result.error:
        print(f"Migration failed: {result.error}")
        return 1
    for version in result.applied:
        print(f"Applied {version}")
    print(f"Schema ready on {db_store.DB_HOST}/{db_store.DB_NAME}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
