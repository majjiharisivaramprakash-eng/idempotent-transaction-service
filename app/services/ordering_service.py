"""Per-account ordering.

Only the next expected sequence for an account may be applied. A job that
arrives early is *persisted*, not dropped: it waits as ``PENDING_ORDER`` and
is re-queued deterministically when its predecessor applies.
"""
from __future__ import annotations

from app.db.repository import TransactionRepository

APPLY = "apply"
PENDING_ORDER = "pending_order"
ALREADY_APPLIED = "already_applied"
CONFLICT = "conflict"


class OrderingService:
    def decide(self, repo: TransactionRepository, txn: dict) -> str:
        account = repo.get_account(txn["account_id"])
        last_sequence = account["last_sequence"] if account else 0
        seq = txn["sequence_number"]

        if seq == last_sequence + 1:
            return APPLY
        if seq > last_sequence + 1:
            return PENDING_ORDER
        # seq <= last_sequence: the slot was consumed. Either this very
        # transaction already applied (idempotent redelivery) or its slot was
        # taken by a different transaction (genuine sequence conflict).
        if repo.has_ledger_entry(txn["transaction_id"]):
            return ALREADY_APPLIED
        return CONFLICT
