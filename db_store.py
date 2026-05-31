"""Optional MySQL persistence for symbol config and decision audit (DeepSeek-ready).

When DB_ENABLED=false (default), every public function is a no-op — the bot runs
from .env only with zero MySQL dependency at runtime.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent
MIGRATIONS_DIR = ROOT_DIR / "db" / "migrations"
_MIGRATION_FILE_RE = re.compile(r"^(\d{3})_.+\.sql$")

DB_ENABLED = os.getenv("DB_ENABLED", "").lower() in ("1", "true", "yes")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "deepseek")
DB_USER = os.getenv("DB_USER", "deepseek")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_CONNECT_TIMEOUT = float(os.getenv("DB_CONNECT_TIMEOUT", "3"))

_write_lock = threading.Lock()
_schema_ready = False
_schema_lock = threading.Lock()


@dataclass
class MigrationStatus:
    applied: list[str] = field(default_factory=list)
    pending: list[str] = field(default_factory=list)
    error: str | None = None


@dataclass
class MigrationResult:
    applied: list[str] = field(default_factory=list)
    up_to_date: bool = False
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


def is_enabled() -> bool:
    return DB_ENABLED


def _credentials_configured() -> bool:
    return bool(DB_USER and DB_NAME)


def connect():
    """Public connection helper for read-only analytics modules."""
    return _connect()


def _connect():
    import pymysql

    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        connect_timeout=DB_CONNECT_TIMEOUT,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, default=str, separators=(",", ":"))


def _parse_json_field(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str) and value:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    for chunk in sql.split(";"):
        statement = chunk.strip()
        if statement:
            statements.append(statement)
    return statements


def _migration_version(path: Path) -> str | None:
    match = _MIGRATION_FILE_RE.match(path.name)
    if not match:
        return None
    return path.stem


def _list_migration_files() -> list[Path]:
    if not MIGRATIONS_DIR.is_dir():
        return []
    files = [path for path in MIGRATIONS_DIR.glob("*.sql") if _migration_version(path)]
    return sorted(files, key=lambda path: path.name)


def _ensure_migrations_table(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version VARCHAR(64) NOT NULL PRIMARY KEY,
                description VARCHAR(255) NULL,
                applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )


def _get_applied_versions(conn) -> set[str]:
    with conn.cursor() as cursor:
        cursor.execute("SELECT version FROM schema_migrations ORDER BY version")
        rows = cursor.fetchall()
    return {row["version"] for row in rows}


def _migration_description(path: Path, sql: str) -> str | None:
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            text = stripped.lstrip("-").strip()
            if text and not text.lower().startswith("migration:"):
                return text[:255]
    return path.stem.replace("_", " ")


def migration_status() -> MigrationStatus:
    if not DB_ENABLED:
        return MigrationStatus()
    if not _credentials_configured():
        return MigrationStatus(error="DB_USER/DB_NAME missing in .env")

    files = _list_migration_files()
    if not files:
        return MigrationStatus(error=f"No migration files in {MIGRATIONS_DIR}")

    try:
        conn = _connect()
        try:
            _ensure_migrations_table(conn)
            applied_set = _get_applied_versions(conn)
        finally:
            conn.close()
    except Exception as exc:
        return MigrationStatus(error=f"MySQL connection failed: {exc}")

    applied = [version for path in files if (version := _migration_version(path)) in applied_set]
    pending = [version for path in files if (version := _migration_version(path)) not in applied_set]
    return MigrationStatus(applied=applied, pending=pending)


def run_migrations() -> MigrationResult:
    """Apply pending SQL files from db/migrations/. Idempotent per version."""
    global _schema_ready

    if not DB_ENABLED:
        return MigrationResult(up_to_date=True)
    if not _credentials_configured():
        return MigrationResult(error="DB_ENABLED=true but DB_USER/DB_NAME missing")

    files = _list_migration_files()
    if not files:
        return MigrationResult(error=f"No migration files in {MIGRATIONS_DIR}")

    try:
        conn = _connect()
        try:
            _ensure_migrations_table(conn)
            applied_set = _get_applied_versions(conn)
            applied_now: list[str] = []

            for path in files:
                version = _migration_version(path)
                if not version or version in applied_set:
                    continue

                sql = path.read_text(encoding="utf-8")
                description = _migration_description(path, sql)
                statements = _split_sql_statements(sql)
                if not statements:
                    continue

                with conn.cursor() as cursor:
                    for statement in statements:
                        cursor.execute(statement)
                    cursor.execute(
                        """
                        INSERT INTO schema_migrations (version, description)
                        VALUES (%s, %s)
                        """,
                        (version, description),
                    )

                applied_set.add(version)
                applied_now.append(version)
                logger.info("Applied migration %s", version)

            with _schema_lock:
                _schema_ready = True

            if applied_now:
                logger.info(
                    "MySQL migrations ready (%s@%s/%s)",
                    DB_USER,
                    DB_HOST,
                    DB_NAME,
                )
                return MigrationResult(applied=applied_now)

            return MigrationResult(up_to_date=True)
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("MySQL migration failed — continuing without DB: %s", exc)
        return MigrationResult(error=str(exc))


def ensure_schema() -> bool:
    """Ensure DB schema is current. Returns False when DB disabled or unreachable."""
    global _schema_ready
    if not DB_ENABLED:
        return False
    if _schema_ready:
        return True
    result = run_migrations()
    return result.ok and (result.up_to_date or bool(result.applied))


def init() -> bool:
    if not DB_ENABLED:
        return False
    return ensure_schema()


def config_hash(config_snapshot: dict[str, Any]) -> str:
    encoded = _json_dumps(config_snapshot).encode()
    return hashlib.sha256(encoded).hexdigest()[:16]


def get_symbol_config(symbol: str) -> tuple[dict[str, Any], int]:
    if not DB_ENABLED or not _credentials_configured():
        return {}, 0
    if not ensure_schema():
        return {}, 0

    symbol = symbol.upper()
    try:
        conn = _connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT config_json, config_version, active
                    FROM symbol_config
                    WHERE symbol = %s
                    LIMIT 1
                    """,
                    (symbol,),
                )
                row = cursor.fetchone()
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("MySQL get_symbol_config failed for %s: %s", symbol, exc)
        return {}, 0

    if not row or not row.get("active"):
        return {}, 0

    return _parse_json_field(row.get("config_json")), int(row.get("config_version") or 0)


def upsert_symbol_config(
    symbol: str,
    config_json: dict[str, Any],
    *,
    updated_by: str = "manual",
    reason: str | None = None,
) -> int | None:
    if not DB_ENABLED or not _credentials_configured():
        return None
    if not ensure_schema():
        return None

    symbol = symbol.upper()
    with _write_lock:
        try:
            conn = _connect()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT config_version FROM symbol_config WHERE symbol = %s LIMIT 1",
                        (symbol,),
                    )
                    existing = cursor.fetchone()
                    previous_version = int(existing["config_version"]) if existing else None
                    new_version = (previous_version or 0) + 1

                    cursor.execute(
                        """
                        INSERT INTO symbol_config (symbol, config_json, config_version, active, updated_by)
                        VALUES (%s, %s, %s, 1, %s)
                        ON DUPLICATE KEY UPDATE
                            config_json = VALUES(config_json),
                            config_version = VALUES(config_version),
                            active = VALUES(active),
                            updated_by = VALUES(updated_by)
                        """,
                        (symbol, _json_dumps(config_json), new_version, updated_by),
                    )
                    cursor.execute(
                        """
                        INSERT INTO symbol_config_history
                            (symbol, config_json, config_version, previous_version, updated_by, reason)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            symbol,
                            _json_dumps(config_json),
                            new_version,
                            previous_version,
                            updated_by,
                            reason,
                        ),
                    )
            finally:
                conn.close()
            return new_version
        except Exception as exc:
            logger.warning("MySQL upsert_symbol_config failed for %s: %s", symbol, exc)
            return None


def deactivate_symbol_config(
    symbol: str,
    *,
    updated_by: str = "manual",
    reason: str | None = None,
) -> int | None:
    """Disable DB overrides for a symbol (falls back to .env on next process start)."""
    if not DB_ENABLED or not _credentials_configured():
        return None
    if not ensure_schema():
        return None

    symbol = symbol.upper()
    with _write_lock:
        try:
            conn = _connect()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT config_json, config_version
                        FROM symbol_config
                        WHERE symbol = %s
                        LIMIT 1
                        """,
                        (symbol,),
                    )
                    existing = cursor.fetchone()
                    if not existing:
                        return 0

                    previous_version = int(existing["config_version"])
                    new_version = previous_version + 1

                    cursor.execute(
                        """
                        UPDATE symbol_config
                        SET active = 0,
                            config_version = %s,
                            updated_by = %s
                        WHERE symbol = %s
                        """,
                        (new_version, updated_by, symbol),
                    )
                    cursor.execute(
                        """
                        INSERT INTO symbol_config_history
                            (symbol, config_json, config_version, previous_version, updated_by, reason)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            symbol,
                            _json_dumps({}),
                            new_version,
                            previous_version,
                            updated_by,
                            reason or "restored to .env defaults",
                        ),
                    )
            finally:
                conn.close()
            return new_version
        except Exception as exc:
            logger.warning("MySQL deactivate_symbol_config failed for %s: %s", symbol, exc)
            return None


def log_decision_event(
    symbol: str,
    event_type: str,
    *,
    outcome: str | None = None,
    block_reason: str | None = None,
    config_snapshot: dict[str, Any] | None = None,
    market_snapshot: dict[str, Any] | None = None,
    config_version: int | None = None,
) -> None:
    if not DB_ENABLED:
        return

    thread = threading.Thread(
        target=_log_decision_event_sync,
        kwargs={
            "symbol": symbol.upper(),
            "event_type": event_type,
            "outcome": outcome,
            "block_reason": block_reason,
            "config_snapshot": config_snapshot or {},
            "market_snapshot": market_snapshot,
            "config_version": config_version,
        },
        daemon=True,
        name=f"db-decision-{symbol}-{event_type}",
    )
    thread.start()


def _log_decision_event_sync(
    symbol: str,
    event_type: str,
    *,
    outcome: str | None = None,
    block_reason: str | None = None,
    config_snapshot: dict[str, Any] | None = None,
    market_snapshot: dict[str, Any] | None = None,
    config_version: int | None = None,
) -> None:
    if not _credentials_configured() or not ensure_schema():
        return
    try:
        conn = _connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO decision_events
                        (symbol, event_type, outcome, block_reason, config_snapshot, market_snapshot, config_version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        symbol.upper(),
                        event_type,
                        outcome,
                        block_reason,
                        _json_dumps(config_snapshot or {}),
                        _json_dumps(market_snapshot) if market_snapshot is not None else None,
                        config_version,
                    ),
                )
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("MySQL log_decision_event failed for %s/%s: %s", symbol, event_type, exc)
