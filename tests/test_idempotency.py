"""Request-level and business-level idempotency guarantees."""
from __future__ import annotations

from tests.conftest import make_request


def test_same_request_submitted_twice_produces_one_financial_effect(client, process,
                                                                    fetch_transaction,
                                                                    ledger_count,
                                                                    fetch_account):
    resp1 = client.post("/transactions", json=make_request())
    assert resp1.status_code == 202

    resp2 = client.post("/transactions", json=make_request())
    assert resp2.status_code == 200
    assert resp2.json()["replayed"] is True

    process()

    txn = fetch_transaction("txn-0001")
    assert txn["status"] == "PROCESSED"
    assert ledger_count("txn-0001") == 1
    assert fetch_account("ACC-1001")["balance_minor"] == 500000


def test_same_key_different_payload_is_conflict(client, process, ledger_count, fetch_transaction):
    resp1 = client.post("/transactions", json=make_request())
    assert resp1.status_code == 202

    resp2 = client.post("/transactions", json=make_request(amount_minor=700000))
    assert resp2.status_code == 409
    assert resp2.json()["error"] == "IDEMPOTENCY_KEY_REUSED"

    process()

    # Only the original instruction ever produced an effect.
    assert ledger_count("txn-0001") == 1
    assert fetch_transaction("txn-0001")["amount_minor"] == 500000
    # The conflicting submission never persisted a new job.
    assert fetch_transaction("txn-0001")["request_id"] == "req-0001"


def test_same_transaction_id_different_request_key_no_second_effect(client, process,
                                                                    ledger_count,
                                                                    fetch_transaction):
    resp1 = client.post("/transactions", json=make_request())
    assert resp1.status_code == 202
    process()
    assert fetch_transaction("txn-0001")["status"] == "PROCESSED"

    # Duplicate *delivery* of the same business transaction under a new request key.
    resp2 = client.post("/transactions", json=make_request(request_id="req-9999"))
    assert resp2.status_code == 200
    assert resp2.json()["duplicate"] is True

    process()
    assert ledger_count("txn-0001") == 1


def test_duplicate_delivery_does_not_overwrite_processed_status(client, process,
                                                                 fetch_transaction):
    client.post("/transactions", json=make_request())
    process()
    resp = client.post("/transactions", json=make_request(request_id="req-8888"))
    assert resp.status_code == 200
    # The replayed status is the original transaction's live status.
    assert fetch_transaction("txn-0001")["status"] == "PROCESSED"


def test_replay_after_processing_returns_live_status(client, process):
    client.post("/transactions", json=make_request())
    process()
    resp = client.post("/transactions", json=make_request())
    assert resp.status_code == 200
    assert resp.json()["status"] == "PROCESSED"
