"""Transaction orchestration: ingestion pipeline and processing pipeline.

Ingestion rules (request layer), in order:
  1. input validation (schema layer, see app.models.transaction)
  2. request idempotency: replay vs conflict
  3. business uniqueness: never accept a second instruction for an
     already-known transaction_id
  4. persist as RECEIVED and acknowledge (202) — processing is asynchronous

Processing rules (worker layer), in order:
  5. durable claim (BEGIN IMMEDIATE + guarded UPDATE)
  6. ordering check -> apply / defer as PENDING_ORDER / conflict
  7. atomic apply: ledger insert + balance + sequence + status in ONE tx
  8. bounded retries for transient faults; deterministic business failures
     are failed fast, never retried
"""
from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from app.config import get_settings
from app.db.database import get_connection, immediate_tx, now_iso
from app.db.repository import TransactionRepository
from app.models.transaction import Status, TransactionRequest, TransactionType
from app.services.idempotency_service import (
    KEY_CONFLICT,
    KEY_NEW,
    KEY_REPLAY,
    IdempotencyService,
)
from app.services.ordering_service import (
    ALREADY_APPLIED,
    APPLY,
    CONFLICT,
    PENDING_ORDER,
    OrderingService,
)
from app.services.retry_service import (
    ERROR_INSUFFICIENT_FUNDS,
    ERROR_RETRY_EXHAUSTED,
    ERROR_SEQUENCE_CONFLICT,
    ERROR_TRANSIENT,
    RetryService,
)

logger = logging.getLogger("txn.service")


def _log(event: str, **fields: object) -> None:
    """Structured JSON log line, with sensitive fields kept minimal."""
    payload = {"ts": now_iso(), "event": event, **fields}
    logger.info(json.dumps(payload, default=str))


class TransientFaultError(Exception):
    """Injected/retryable failure."""


class InsufficientFundsError(Exception):
    """Deterministic business failure — must never be retried."""


@dataclass
class IngestResult:
    kind: str  # accepted | replayed | conflict | duplicate
    http_status: int
    body: dict = field(default_factory=dict)


class TransactionService:
    def __init__(self) -> None:
        self.ordering = OrderingService()
        self.retry = RetryService()

    # ------------------------------------------------------------------ #
    # ingestion
    # ------------------------------------------------------------------ #
    def ingest(self, req: TransactionRequest) -> IngestResult:
        conn = get_connection()
        try:
            repo = TransactionRepository(conn)
            idempotency = IdempotencyService(repo)
            payload_hash = req.payload_hash()

            check = idempotency.check_request_key(req.request_id, payload_hash)
            if check.outcome == KEY_REPLAY:
                txn = repo.get_transaction(req.transaction_id) or {}
                _log("ingest.replay", request_id=req.request_id,
                     transaction_id=req.transaction_id)
                return IngestResult(
                    kind="replayed",
                    http_status=200,
                    body={
                        "replayed": True,
                        "request_id": req.request_id,
                        "transaction_id": req.transaction_id,
                        "status": txn.get("status", check.record["result_status"]),
                        "message": "Request already accepted; recorded outcome replayed. "
                                   "No new transaction was created.",
                    },
                )
            if check.outcome == KEY_CONFLICT:
                _log("ingest.conflict", request_id=req.request_id,
                     transaction_id=req.transaction_id)
                return IngestResult(
                    kind="conflict",
                    http_status=409,
                    body={
                        "accepted": False,
                        "request_id": req.request_id,
                        "error": "IDEMPOTENCY_KEY_REUSED",
                        "message": "This request key was already used with a different payload. "
                                   "Submit it unchanged or use a new request key.",
                    },
                )

            # Business-level uniqueness: same transaction_id under another request.
            existing = repo.get_transaction(req.transaction_id)
            if existing is not None:
                idempotency.record(req.request_id, payload_hash,
                                   existing["transaction_id"], "DUPLICATE")
                _log("ingest.business_duplicate", request_id=req.request_id,
                     transaction_id=req.transaction_id)
                return IngestResult(
                    kind="duplicate",
                    http_status=200,
                    body={
                        "duplicate": True,
                        "request_id": req.request_id,
                        "transaction_id": existing["transaction_id"],
                        "status": existing["status"],
                        "message": "Business transaction already recorded; no second financial effect.",
                    },
                )

            try:
                repo.insert_transaction(req)
                idempotency.record(req.request_id, payload_hash,
                                   req.transaction_id, Status.RECEIVED.value)
            except sqlite3.IntegrityError:
                # Lost a race against an identical/concurrent submission:
                # re-read and classify rather than erroring out.
                txn = repo.get_transaction(req.transaction_id)
                rec = repo.get_idempotency_key(req.request_id)
                if rec is not None and rec["payload_hash"] == payload_hash:
                    return IngestResult(
                        kind="replayed", http_status=200,
                        body={"replayed": True, "request_id": req.request_id,
                              "transaction_id": req.transaction_id,
                              "status": (txn or {}).get("status"),
                              "message": "Concurrent identical submission deduplicated."},
                    )
                if txn is not None:
                    return IngestResult(
                        kind="duplicate", http_status=200,
                        body={"duplicate": True, "request_id": req.request_id,
                              "transaction_id": txn["transaction_id"],
                              "status": txn["status"],
                              "message": "Business transaction already recorded."},
                    )
                raise

            _log("ingest.accepted", request_id=req.request_id,
                 transaction_id=req.transaction_id,
                 account=_mask(req.account_id), sequence=req.sequence_number)
            return IngestResult(
                kind="accepted",
                http_status=202,
                body={
                    "accepted": True,
                    "request_id": req.request_id,
                    "transaction_id": req.transaction_id,
                    "status": Status.RECEIVED.value,
                    "message": "Transaction persisted and queued for processing.",
                },
            )
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    # processing (worker entry points)
    # ------------------------------------------------------------------ #
    def process_next(self) -> bool:
        """Claim and process one job. Returns False when the queue is idle."""
        conn = get_connection()
        try:
            repo = TransactionRepository(conn)
            for candidate in repo.list_claimable():
                with immediate_tx(conn):
                    txn = repo.try_claim(candidate["transaction_id"])
                if txn is None:
                    continue  # another worker owns it
                self._process_claimed(repo, txn)
                return True
            return False
        finally:
            conn.close()

    def process_pending(self, max_jobs: int = 1000) -> int:
        """Drain the queue synchronously. Used by tests and scripts."""
        processed = 0
        while processed < max_jobs and self.process_next():
            processed += 1
        return processed

    def _process_claimed(self, repo: TransactionRepository, txn: dict) -> None:
        transaction_id = txn["transaction_id"]
        try:
            self._simulate_fault(txn)
            self._order_and_apply(repo, txn)
        except InsufficientFundsError:
            self._fail_business(repo, txn, ERROR_INSUFFICIENT_FUNDS)
            _log("process.business_failure", transaction_id=transaction_id,
                 error=ERROR_INSUFFICIENT_FUNDS)
        except (TransientFaultError, sqlite3.Error):
            self._handle_transient(repo, txn, ERROR_TRANSIENT)
            _log("process.transient_failure", transaction_id=transaction_id,
                 attempt=txn["attempt_count"] + 1)
        except Exception:  # defensive: unknown faults are treated as transient
            logger.exception("unexpected processing failure")
            self._handle_transient(repo, txn, ERROR_TRANSIENT)
        self._sync_idempotency_result(repo, txn)

    def _order_and_apply(self, repo: TransactionRepository, txn: dict) -> None:
        applied_sequence: Optional[int] = None
        with immediate_tx(repo.conn):
            repo.ensure_account(txn["account_id"])
            decision = self.ordering.decide(repo, txn)
            if decision == APPLY:
                applied_sequence = self._apply_effects(repo, txn)
            elif decision == PENDING_ORDER:
                repo.set_status(txn["transaction_id"], Status.PENDING_ORDER)
                _log("process.pending_order", transaction_id=txn["transaction_id"],
                     sequence=txn["sequence_number"])
            elif decision == ALREADY_APPLIED:
                repo.set_status(txn["transaction_id"], Status.PROCESSED)
                applied_sequence = txn["sequence_number"]
            elif decision == CONFLICT:
                repo.set_status(
                    txn["transaction_id"], Status.FAILED,
                    last_error=f"{ERROR_SEQUENCE_CONFLICT}: sequence "
                               f"{txn['sequence_number']} already consumed for account",
                )
                _log("process.sequence_conflict", transaction_id=txn["transaction_id"])

        if applied_sequence is not None:
            # Deterministic ordering recovery: the immediate successor, if it
            # is waiting, becomes claimable right now.
            with immediate_tx(repo.conn):
                requeued = repo.requeue_successors(txn["account_id"], applied_sequence + 1)
            if requeued:
                _log("process.successor_requeued",
                     account=_mask(txn["account_id"]), sequence=applied_sequence + 1)

    def _apply_effects(self, repo: TransactionRepository, txn: dict) -> int:
        """Atomically apply the financial effect. Called inside immediate_tx."""
        signed = txn["amount_minor"]
        if txn["transaction_type"] == TransactionType.DEBIT.value:
            signed = -signed
            account = repo.get_account(txn["account_id"])
            if account is None or account["balance_minor"] + signed < 0:
                raise InsufficientFundsError()

        repo.insert_ledger_entry(txn["transaction_id"], txn["account_id"], signed)

        # Test/chaos hook: raises after the ledger insert so the rollback of
        # the whole financial update (ledger + balance + sequence + status)
        # can be verified end-to-end.
        if self._chaos_hook is not None:
            self._chaos_hook(repo, txn)

        repo.apply_account_effect(txn["account_id"], signed, txn["sequence_number"])
        repo.set_status(txn["transaction_id"], Status.PROCESSED, last_error=None)
        _log("process.applied", transaction_id=txn["transaction_id"],
             account=_mask(txn["account_id"]), signed_amount=signed,
             sequence=txn["sequence_number"])
        return txn["sequence_number"]

    # ------------------------------------------------------------------ #
    # fault injection & retry bookkeeping
    # ------------------------------------------------------------------ #
    _chaos_hook = None  # class-level hook, set by tests

    def _simulate_fault(self, txn: dict) -> None:
        settings = get_settings()
        if txn["amount_minor"] == settings.fault_permanent_amount:
            raise TransientFaultError("simulated permanent transient fault")
        if txn["amount_minor"] == settings.fault_transient_amount and txn["attempt_count"] < 1:
            raise TransientFaultError("simulated transient fault (first attempt only)")

    def _handle_transient(self, repo: TransactionRepository, txn: dict, code: str) -> None:
        attempts = txn["attempt_count"] + 1
        if self.retry.should_retry(attempts):
            repo.set_status(
                txn["transaction_id"], Status.RETRY_PENDING,
                last_error=code,
                next_retry_at=self.retry.next_retry_at(attempts),
                attempt_count=attempts,
            )
        else:
            repo.set_status(
                txn["transaction_id"], Status.FAILED,
                last_error=f"{ERROR_RETRY_EXHAUSTED} after {attempts} attempts ({code})",
                attempt_count=attempts, next_retry_at=None,
            )
            _log("process.retry_exhausted", transaction_id=txn["transaction_id"],
                 attempts=attempts)

    def _fail_business(self, repo: TransactionRepository, txn: dict, code: str) -> None:
        repo.set_status(
            txn["transaction_id"], Status.FAILED, last_error=code,
            attempt_count=txn["attempt_count"] + 1, next_retry_at=None,
        )

    def _sync_idempotency_result(self, repo: TransactionRepository, txn: dict) -> None:
        """Keep the recorded outcome fresh for future replays."""
        fresh = repo.get_transaction(txn["transaction_id"])
        if fresh is not None:
            repo.update_idempotency_result(txn["request_id"], fresh["status"])


def _mask(account_id: str) -> str:
    """Truncated account identifier for logs and diagnostics."""
    if len(account_id) <= 4:
        return "****"
    return account_id[:4] + "***"
