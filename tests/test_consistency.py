"""Atomicity, concurrent workers and crash recovery."""
from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app.db.database import get_connection, immediate_tx
from app.db.repository import TransactionRepository
from tests.conftest import make_request


def test_mid_update_failure_rolls_back_entire_financial_effect(client, process,
                                                               fetch_transaction,
                                                               fetch_account, ledger_count):
    """A failure after the ledger insert must roll back ledger, balance,
    sequence and status together — nothing partial may survive."""
    from app.services.transaction_service import TransactionService

    def fail_after_ledger(repo, txn):
        raise RuntimeError("database failure mid-update")

    TransactionService._chaos_hook = fail_after_ledger
    client.post("/transactions", json=make_request())
    process(max_jobs=1)  # one attempt, ends in RETRY_PENDING after rollback

    txn = fetch_transaction("txn-0001")
    assert txn["status"] == "RETRY_PENDING"
    assert txn["attempt_count"] == 1
    assert ledger_count("txn-0001") == 0            # ledger insert rolled back
    account = fetch_account("ACC-1001")
    assert account is None or account["balance_minor"] == 0  # balance untouched
    assert account is None or account["last_sequence"] == 0  # sequence untouched

    TransactionService._chaos_hook = None
    process()
    assert fetch_transaction("txn-0001")["status"] == "PROCESSED"
    assert ledger_count("txn-0001") == 1            # exactly one effect overall


def test_two_workers_cannot_claim_the_same_job(client):
    """A claimed job is invisible to every other worker."""
    client.post("/transactions", json=make_request())

    conn_a = get_connection()
    with immediate_tx(conn_a):
        claimed_by_a = TransactionRepository(conn_a).try_claim("txn-0001")
    conn_a.close()

    conn_b = get_connection()
    with immediate_tx(conn_b):
        claimed_by_b = TransactionRepository(conn_b).try_claim("txn-0001")
    conn_b.close()

    assert claimed_by_a is not None
    assert claimed_by_b is None  # second worker sees nothing to claim


def test_ledger_unique_constraint_blocks_double_application(client, process, ledger_count):
    """Even if a duplicate effect were attempted directly, the schema forbids it."""
    client.post("/transactions", json=make_request())
    process()
    assert ledger_count("txn-0001") == 1

    conn = get_connection()
    try:
        with immediate_tx(conn):
            repo = TransactionRepository(conn)
            with pytest.raises(sqlite3.IntegrityError):
                repo.insert_ledger_entry("txn-0001", "ACC-1001", 500000)
    finally:
        conn.close()
    assert ledger_count("txn-0001") == 1


def test_redelivery_after_crash_before_ack_has_no_second_effect(client, process,
                                                                 fetch_transaction,
                                                                 ledger_count,
                                                                 fetch_account):
    """Simulate: effect committed, worker crashed before acknowledging.

    The same job is delivered again (forced back to RECEIVED). The ordering
    layer detects the existing ledger entry and marks it PROCESSED without
    producing any new financial effect.
    """
    client.post("/transactions", json=make_request())
    process()
    assert fetch_transaction("txn-0001")["status"] == "PROCESSED"

    conn = get_connection()
    with immediate_tx(conn):
        conn.execute(
            "UPDATE transactions SET status = 'RECEIVED' WHERE transaction_id = 'txn-0001'"
        )
    conn.close()

    process()
    assert fetch_transaction("txn-0001")["status"] == "PROCESSED"
    assert ledger_count("txn-0001") == 1
    assert fetch_account("ACC-1001")["balance_minor"] == 500000


def test_worker_restart_recovers_persisted_jobs(isolated_db):
    """Pending work lives in the database, so a fresh process resumes it."""
    from app.main import create_app
    from app.services.transaction_service import TransactionService

    with TestClient(create_app()) as c1:
        resp = c1.post("/transactions", json=make_request())
        assert resp.status_code == 202
        # "crash": first client shuts down without processing.

    # A brand-new worker instance (simulating a restarted process):
    assert TransactionService().process_pending() >= 1

    with TestClient(create_app()) as c2:
        txn = c2.get("/transactions/txn-0001").json()
        assert txn["status"] == "PROCESSED"
        metrics = c2.get("/metrics/summary").json()
        assert metrics["counts_by_status"].get("PROCESSED") == 1
