"""SQLite connection factory and transaction management.

The service deliberately uses explicit ``BEGIN IMMEDIATE`` transactions:
writers take the reserved lock up front, which is what makes durable job
claiming and atomic ledger effects safe across multiple worker connections.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from app.config import get_settings

_SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    """ISO-8601 UTC timestamp with fixed precision (lexicographically sortable)."""
    return utc_now().isoformat(timespec="microseconds")


def get_connection() -> sqlite3.Connection:
    """Open a configured connection. One connection per unit of work."""
    settings = get_settings()
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(settings.db_path, isolation_level=None, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Create tables if they do not exist (idempotent)."""
    conn = get_connection()
    try:
        conn.executescript(_SCHEMA_PATH.read_text(encoding="utf-8"))
    finally:
        conn.close()


@contextmanager
def immediate_tx(conn: sqlite3.Connection):
    """Run a block inside an explicit ``BEGIN IMMEDIATE`` transaction.

    Commits on success, rolls back on any exception, then re-raises.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")
