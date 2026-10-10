"""Submit the sample batch and print the final processing outcome.

Usage (API running with an embedded worker):
    uvicorn app.main:app
    python scripts/run_batch.py [--base-url http://127.0.0.1:8000]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import httpx

SETTLED = {"PROCESSED", "FAILED", "PENDING_ORDER"}
TERMINAL_IN_QUEUE = {"RECEIVED", "PROCESSING", "RETRY_PENDING"}


def wait_for_settled(client: httpx.Client, transaction_ids: list[str], timeout: float) -> dict:
    """Poll transaction statuses until every job reaches a settled state."""
    deadline = time.monotonic() + timeout
    while True:
        statuses = {tid: client.get(f"/transactions/{tid}").json()["status"]
                    for tid in transaction_ids}
        if all(s in SETTLED for s in statuses.values()):
            return statuses
        if time.monotonic() > deadline:
            return statuses  # report whatever state we are in
        time.sleep(0.5)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--timeout", type=float, default=45.0,
                        help="max seconds to wait for the worker to settle the queue")
    args = parser.parse_args()

    sample_path = Path(__file__).resolve().parent.parent / "sample_data" / "transactions.json"
    payload = json.loads(sample_path.read_text(encoding="utf-8"))
    # Strip documentation-only keys before posting.
    body = {"transactions": payload["transactions"]}

    with httpx.Client(base_url=args.base_url, timeout=10.0) as client:
        health = client.get("/health")
        if health.status_code != 200:
            print("Service is not reachable. Start it with: uvicorn app.main:app", file=sys.stderr)
            return 1

        print(f"Submitting {len(body['transactions'])} sample records "
              f"(duplicates, conflicts, out-of-order, invalid and fault-triggering)...\n")
        resp = client.post("/transactions/batch", json=body)
        batch = resp.json()
        print(f"Ingestion outcomes: {json.dumps(batch['counts'])}\n")

        tracked: dict[str, str] = {}
        for result in batch["results"]:
            tid = result.get("transaction_id")
            if tid and result["outcome"] != "invalid":
                tracked.setdefault(tid, result["outcome"])

        statuses = wait_for_settled(client, list(tracked), args.timeout)

        header = f"{'transaction_id':<14} {'account':<10} {'seq':>4} {'type':<7} " \
                 f"{'amount':>10} {'status':<14} {'attempts':>8}  last_error"
        print(header)
        print("-" * len(header))
        for tid in sorted(tracked):
            txn = client.get(f"/transactions/{tid}").json()
            print(f"{tid:<14} {txn['account_id']:<10} {txn['sequence_number']:>4} "
                  f"{txn['transaction_type']:<7} {txn['amount_minor']:>10} "
                  f"{txn['status']:<14} {txn['attempt_count']:>8}  "
                  f"{txn['last_error'] or ''}")

        print()
        for account_id in ("ACC-1001", "ACC-2002", "ACC-4004"):
            acc = client.get(f"/accounts/{account_id}")
            if acc.status_code == 200:
                data = acc.json()
                print(f"Account {data['account_id']}: balance={data['balance_minor']} paise "
                      f"last_sequence={data['last_sequence']}")

        print("\nMetrics summary:")
        metrics = client.get("/metrics/summary").json()
        print(json.dumps(metrics, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
