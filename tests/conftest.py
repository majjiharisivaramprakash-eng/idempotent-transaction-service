"""Shared test fixtures.

Every test gets a fresh file-based SQLite database (WAL behaves the same as
production) and a TestClient with the embedded worker disabled, so tests
drive processing explicitly and stay deterministic.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("TXN_DB_PATH", str(db_file))
    monkeypatch.setenv("TXN_WORKER_MODE", "off")
    monkeypatch.setenv("TXN_MAX_ATTEMPTS", "3")
    monkeypatch.setenv("TXN_BACKOFF_BASE_SECONDS", "0")
    # Reset any chaos hooks leaked between tests.
    from app.services.transaction_service import TransactionService

    TransactionService._chaos_hook = None

    from app.db.database import init_db

    init_db()
    yield str(db_file)


@pytest.fixture()
def client():
    from app.main import create_app

    with TestClient(create_app()) as c:
        yield c


def make_request(**overrides) -> dict:
    payload = {
        "request_id": "req-0001",
        "transaction_id": "txn-0001",
        "account_id": "ACC-1001",
        "transaction_type": "CREDIT",
        "amount_minor": 500000,
        "currency": "INR",
        "sequence_number": 1,
    }
    payload.update(overrides)
    return payload


@pytest.fixture()
def process():
    """Drain the queue synchronously; returns the number of jobs attempted."""
    from app.services.transaction_service import TransactionService

    def _run(max_jobs: int = 100) -> int:
        return TransactionService().process_pending(max_jobs=max_jobs)

    return _run


@pytest.fixture()
def fetch_transaction():
    from app.db.database import get_connection
    from app.db.repository import TransactionRepository

    def _get(transaction_id: str) -> dict | None:
        conn = get_connection()
        try:
            return TransactionRepository(conn).get_transaction(transaction_id)
        finally:
            conn.close()

    return _get


@pytest.fixture()
def ledger_count():
    from app.db.database import get_connection
    from app.db.repository import TransactionRepository

    def _count(transaction_id: str) -> int:
        conn = get_connection()
        try:
            return TransactionRepository(conn).ledger_count_for_transaction(transaction_id)
        finally:
            conn.close()

    return _count


@pytest.fixture()
def fetch_account():
    from app.db.database import get_connection
    from app.db.repository import TransactionRepository

    def _get(account_id: str) -> dict | None:
        conn = get_connection()
        try:
            return TransactionRepository(conn).get_account(account_id)
        finally:
            conn.close()

    return _get
