"""Per-account ordering: gaps block, successors recover deterministically."""
from __future__ import annotations

from tests.conftest import make_request


def test_sequence_2_before_sequence_1_stays_pending(client, process, fetch_transaction,
                                                    fetch_account):
    resp = client.post(
        "/transactions",
        json=make_request(request_id="req-0002", transaction_id="txn-0002",
                          amount_minor=250000, sequence_number=2),
    )
    assert resp.status_code == 202

    process()

    txn = fetch_transaction("txn-0002")
    assert txn["status"] == "PENDING_ORDER"
    assert fetch_account("ACC-1001") is None or fetch_account("ACC-1001")["balance_minor"] == 0


def test_sequence_1_arrives_then_both_apply_in_order(client, process, fetch_transaction,
                                                     fetch_account, ledger_count):
    client.post(
        "/transactions",
        json=make_request(request_id="req-0002", transaction_id="txn-0002",
                          amount_minor=250000, sequence_number=2),
    )
    process()
    assert fetch_transaction("txn-0002")["status"] == "PENDING_ORDER"

    client.post("/transactions", json=make_request())  # seq 1
    process()

    assert fetch_transaction("txn-0001")["status"] == "PROCESSED"
    assert fetch_transaction("txn-0002")["status"] == "PROCESSED"
    account = fetch_account("ACC-1001")
    assert account["balance_minor"] == 750000
    assert account["last_sequence"] == 2
    assert ledger_count("txn-0001") == 1
    assert ledger_count("txn-0002") == 1


def test_gap_never_fills_transaction_remains_observable(client, process, fetch_transaction):
    # Sequence 5 arrives while 1..4 are unknown: it must wait indefinitely.
    client.post(
        "/transactions",
        json=make_request(request_id="req-0005", transaction_id="txn-0005",
                          sequence_number=5),
    )
    process()
    assert fetch_transaction("txn-0005")["status"] == "PENDING_ORDER"


def test_sequence_conflict_fails_with_diagnostic(client, process, fetch_transaction,
                                                 ledger_count):
    # txn-0001 applies as sequence 1...
    client.post("/transactions", json=make_request())
    process()
    # ...a different transaction claiming the same consumed slot is a conflict.
    client.post(
        "/transactions",
        json=make_request(request_id="req-0077", transaction_id="txn-0077",
                          sequence_number=1, amount_minor=100),
    )
    process()

    txn = fetch_transaction("txn-0077")
    assert txn["status"] == "FAILED"
    assert "SEQUENCE_CONFLICT" in txn["last_error"]
    assert ledger_count("txn-0077") == 0
