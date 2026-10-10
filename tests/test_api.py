"""Endpoint contracts and operational reporting."""
from __future__ import annotations

from tests.conftest import make_request


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "database": "up"}


def test_single_post_accepts_and_queues_without_processing(client, fetch_transaction):
    resp = client.post("/transactions", json=make_request())
    assert resp.status_code == 202
    body = resp.json()
    assert body["accepted"] is True
    assert body["status"] == "RECEIVED"
    # Acceptance means persisted, not finished.
    assert fetch_transaction("txn-0001")["status"] == "RECEIVED"


def test_get_transaction_status_and_attempts(client, process):
    client.post("/transactions", json=make_request())
    process()
    resp = client.get("/transactions/txn-0001")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "PROCESSED"
    assert body["attempt_count"] == 0
    assert body["last_error"] is None


def test_get_unknown_transaction_returns_404(client):
    assert client.get("/transactions/nope-not-here").status_code == 404


def test_get_account_after_processing(client, process):
    client.post("/transactions", json=make_request())
    process()
    resp = client.get("/accounts/ACC-1001")
    assert resp.status_code == 200
    body = resp.json()
    assert body["balance_minor"] == 500000
    assert body["last_sequence"] == 1


def test_get_unknown_account_returns_404(client):
    assert client.get("/accounts/ACC-NONE").status_code == 404


def test_metrics_summary_counts_statuses_and_ledger(client, process):
    client.post("/transactions", json=make_request(request_id="req-m-1",
                                                   transaction_id="txn-m-1"))
    client.post("/transactions", json=make_request(request_id="req-m-2",
                                                   transaction_id="txn-m-2",
                                                   transaction_type="DEBIT",
                                                   amount_minor=200000,
                                                   sequence_number=2))
    client.post("/transactions", json=make_request(request_id="req-m-3",
                                                   transaction_id="txn-m-3",
                                                   sequence_number=9))
    process()

    body = client.get("/metrics/summary").json()
    assert body["counts_by_status"]["PROCESSED"] == 2
    assert body["counts_by_status"]["PENDING_ORDER"] == 1
    assert body["total_transactions"] == 3
    assert body["ledger"]["entries"] == 2
    assert body["ledger"]["total_credit_minor"] == 500000
    assert body["ledger"]["total_debit_minor"] == 200000
    assert body["ledger"]["net_minor"] == 300000


def test_batch_reports_per_record_outcomes(client):
    batch = {"transactions": [
        make_request(request_id="req-b-001", transaction_id="txn-b-001"),
        make_request(request_id="req-b-001", transaction_id="txn-b-001"),          # replay
        make_request(request_id="req-b-002", transaction_id="txn-b-001"),          # business dup
        make_request(request_id="req-b-001", transaction_id="txn-b-001",
                     amount_minor=123),                                            # conflict
        make_request(request_id="req-b-003", transaction_id="txn-b-003",
                     amount_minor=-1),                                              # invalid
    ]}
    body = client.post("/transactions/batch", json=batch).json()
    assert body["counts"]["accepted"] == 1
    assert body["counts"]["replayed"] == 1
    assert body["counts"]["duplicate"] == 1
    assert body["counts"]["conflict"] == 1
    assert body["counts"]["invalid"] == 1
