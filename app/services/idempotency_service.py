"""Request-level idempotency.

Two distinct concepts, deliberately separated:

1. Request idempotency — the same ``request_id`` (the idempotency key)
   submitted again. Same payload hash -> replay the recorded outcome.
   Different payload hash -> conflict (never silently re-processed).
2. Business idempotency — the same ``transaction_id`` arriving under a
   *different* request. It must never produce a second financial effect.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.db.repository import TransactionRepository

KEY_NEW = "new"
KEY_REPLAY = "replay"
KEY_CONFLICT = "conflict"


@dataclass(frozen=True)
class KeyCheck:
    outcome: str                       # KEY_NEW | KEY_REPLAY | KEY_CONFLICT
    record: Optional[dict] = None      # existing idempotency_keys row, if any


class IdempotencyService:
    def __init__(self, repo: TransactionRepository):
        self.repo = repo

    def check_request_key(self, request_id: str, payload_hash: str) -> KeyCheck:
        record = self.repo.get_idempotency_key(request_id)
        if record is None:
            return KeyCheck(KEY_NEW)
        if record["payload_hash"] == payload_hash:
            return KeyCheck(KEY_REPLAY, record)
        return KeyCheck(KEY_CONFLICT, record)

    def record(self, request_id: str, payload_hash: str, transaction_id: str, result_status: str) -> None:
        self.repo.save_idempotency_key(request_id, payload_hash, transaction_id, result_status)
