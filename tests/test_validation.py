"""Input validation and batch isolation of invalid records."""
from __future__ import annotations

import pytest

from tests.conftest import make_request


@pytest.mark.parametrize(
    "override, bad_field",
    [
        ({"amount_minor": -500}, "amount_minor"),
        ({"amount_minor": 0}, "amount_minor"),
        ({"currency": "USD"}, "currency"),
        ({"transaction_type": "TRANSFER"}, "transaction_type"),
        ({"sequence_number": 0}, "sequence_number"),
        ({"sequence_number": -3}, "sequence_number"),
        ({"request_id": "ab"}, "request_id"),            # too short
        ({"transaction_id": "bad id with spaces"}, "transaction_id"),
        ({"account_id": "x" * 40}, "account_id"),        # too long
    ],
)
def test_invalid_single_transactions_are_rejected(client, override, bad_field):
    resp = client.post("/transactions", json=make_request(**override))
    assert resp.status_code == 422
    errors = resp.json()["detail"]
    assert any(bad_field in ".".join(str(loc) for loc in err["loc"]) for err in errors)


def test_batch_continues_past_invalid_records(client, process, fetch_transaction):
    batch = {
        "transactions": [
            make_request(request_id="req-v-001", transaction_id="txn-v-001"),
            make_request(request_id="req-v-002", transaction_id="txn-v-002",
                         amount_minor=-1),                       # invalid
            make_request(request_id="req-v-003", transaction_id="txn-v-003",
                         currency="EUR"),                        # invalid
            make_request(request_id="req-v-004", transaction_id="txn-v-004",
                         sequence_number=2),                     # valid
        ]
    }
    resp = client.post("/transactions/batch", json=batch)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 4
    assert body["counts"]["accepted"] == 2
    assert body["counts"]["invalid"] == 2

    # Invalid records never reach the queue.
    assert fetch_transaction("txn-v-002") is None
    assert fetch_transaction("txn-v-003") is None

    process()
    assert fetch_transaction("txn-v-001")["status"] == "PROCESSED"
    assert fetch_transaction("txn-v-004")["status"] == "PROCESSED"


def test_batch_invalid_result_carries_field_errors(client):
    resp = client.post(
        "/transactions/batch",
        json={"transactions": [make_request(amount_minor=0)]},
    )
    assert resp.status_code == 200
    result = resp.json()["results"][0]
    assert result["outcome"] == "invalid"
    assert any(e["field"] == "amount_minor" for e in result["errors"])
