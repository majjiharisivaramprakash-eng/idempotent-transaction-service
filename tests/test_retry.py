"""Bounded retries for transient faults; fast-fail for business faults."""
from __future__ import annotations

from tests.conftest import make_request

TRANSIENT_AMOUNT = 1111  # configured fault sentinel: fails first attempt only
PERMANENT_AMOUNT = 2222  # configured fault sentinel: fails every attempt


def test_transient_failure_retries_then_applies_exactly_once(client, process,
                                                              fetch_transaction,
                                                              ledger_count):
    client.post(
        "/transactions",
        json=make_request(amount_minor=TRANSIENT_AMOUNT),
    )
    process()

    txn = fetch_transaction("txn-0001")
    assert txn["status"] == "PROCESSED"
    assert txn["attempt_count"] == 1  # one failed attempt, then success
    assert ledger_count("txn-0001") == 1


def test_retry_exhaustion_marks_failed_and_observable(client, process, fetch_transaction,
                                                       ledger_count):
    client.post(
        "/transactions",
        json=make_request(amount_minor=PERMANENT_AMOUNT),
    )
    process()

    txn = fetch_transaction("txn-0001")
    assert txn["status"] == "FAILED"
    assert "RETRY_EXHAUSTED" in txn["last_error"]
    assert txn["attempt_count"] == 3  # TXN_MAX_ATTEMPTS
    assert ledger_count("txn-0001") == 0


def test_exhausted_job_is_manually_recoverable(client, process, fetch_transaction):
    client.post(
        "/transactions",
        json=make_request(amount_minor=PERMANENT_AMOUNT),
    )
    process()
    assert fetch_transaction("txn-0001")["status"] == "FAILED"

    resp = client.post("/transactions/txn-0001/retry")
    assert resp.status_code == 202

    txn = fetch_transaction("txn-0001")
    assert txn["status"] == "RECEIVED"
    assert txn["attempt_count"] == 0  # fresh budget on explicit operator action


def test_retry_endpoint_rejects_non_failed_transactions(client, process):
    client.post("/transactions", json=make_request())
    process()
    resp = client.post("/transactions/txn-0001/retry")
    assert resp.status_code == 409
    assert resp.json()["error"] == "NOT_RETRYABLE"


def test_retry_endpoint_unknown_transaction(client):
    resp = client.post("/transactions/txn-does-not-exist/retry")
    assert resp.status_code == 404


def test_insufficient_funds_fails_fast_without_retrying(client, process, fetch_transaction,
                                                        ledger_count):
    # Fund the account: 5000.00
    client.post("/transactions", json=make_request())
    # Withdraw 1000.00 (valid), then attempt a debit far above the balance.
    client.post(
        "/transactions",
        json=make_request(request_id="req-0002", transaction_id="txn-0002",
                          transaction_type="DEBIT", amount_minor=100000,
                          sequence_number=2),
    )
    client.post(
        "/transactions",
        json=make_request(request_id="req-0003", transaction_id="txn-0003",
                          transaction_type="DEBIT", amount_minor=999999999,
                          sequence_number=3),
    )
    process()

    overdraft = fetch_transaction("txn-0003")
    assert overdraft["status"] == "FAILED"
    assert overdraft["last_error"] == "INSUFFICIENT_FUNDS"
    # Deterministic business failure: exactly one attempt, no retry churn.
    assert overdraft["attempt_count"] == 1
    assert ledger_count("txn-0003") == 0
    # The valid debit before it still applied.
    assert fetch_transaction("txn-0002")["status"] == "PROCESSED"


def test_manual_retry_recovers_exhausted_job(client, process, fetch_transaction, ledger_count):
    """Operator recovery path: infra outage exhausts the budget, then a
    manual retry applies the job exactly once."""
    from app.services.transaction_service import TransactionService

    def simulated_outage(repo, txn):
        raise RuntimeError("simulated infrastructure outage")

    TransactionService._chaos_hook = simulated_outage
    client.post("/transactions", json=make_request())
    process()

    txn = fetch_transaction("txn-0001")
    assert txn["status"] == "FAILED"
    assert "RETRY_EXHAUSTED" in txn["last_error"]
    assert ledger_count("txn-0001") == 0  # every failed attempt rolled back

    TransactionService._chaos_hook = None  # outage resolved
    resp = client.post("/transactions/txn-0001/retry")
    assert resp.status_code == 202
    process()

    assert fetch_transaction("txn-0001")["status"] == "PROCESSED"
    assert ledger_count("txn-0001") == 1
