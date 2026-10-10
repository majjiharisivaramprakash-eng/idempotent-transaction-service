"""HTTP endpoints.

Handlers deliberately contain no business rules: they parse input, call the
service layer and map results to status codes. A successful ingestion means
the request was *accepted and persisted*, not that it has finished processing.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError

from app.db.database import get_connection
from app.db.repository import TransactionRepository
from app.models.transaction import TransactionRequest
from app.services.retry_service import RetryService
from app.services.transaction_service import TransactionService

router = APIRouter()


def get_service() -> TransactionService:
    return TransactionService()


@router.get("/health")
def health() -> dict:
    conn = get_connection()
    try:
        conn.execute("SELECT 1").fetchone()
        return {"status": "ok", "database": "up"}
    finally:
        conn.close()


@router.post("/transactions")
def create_transaction(
    req: TransactionRequest, service: TransactionService = Depends(get_service)
) -> JSONResponse:
    result = service.ingest(req)
    return JSONResponse(status_code=result.http_status, content=result.body)


class BatchRequest(BaseModel):
    """Raw items: per-record validation happens inside the endpoint so one
    invalid record can never abort the rest of the batch."""

    transactions: list[dict[str, Any]]


@router.post("/transactions/batch")
def create_batch(
    payload: BatchRequest, service: TransactionService = Depends(get_service)
) -> dict:
    counts = {"accepted": 0, "replayed": 0, "duplicate": 0, "conflict": 0, "invalid": 0}
    results: list[dict] = []
    for index, item in enumerate(payload.transactions):
        try:
            req = TransactionRequest(**item)
        except ValidationError as exc:
            counts["invalid"] += 1
            results.append({
                "index": index,
                "request_id": item.get("request_id"),
                "outcome": "invalid",
                "errors": [
                    {"field": ".".join(str(p) for p in err["loc"]), "message": err["msg"]}
                    for err in exc.errors()
                ],
            })
            continue
        outcome = service.ingest(req)
        counts[outcome.kind] = counts.get(outcome.kind, 0) + 1
        entry = {"index": index, "outcome": outcome.kind}
        entry.update(outcome.body)
        results.append(entry)
    return {
        "total": len(payload.transactions),
        "counts": counts,
        "results": results,
        "message": "Accepted records are queued; poll GET /transactions/{id} for final status.",
    }


@router.get("/transactions/{transaction_id}")
def get_transaction(transaction_id: str) -> JSONResponse:
    conn = get_connection()
    try:
        txn = TransactionRepository(conn).get_transaction(transaction_id)
    finally:
        conn.close()
    if txn is None:
        return JSONResponse(status_code=404, content={"error": "TRANSACTION_NOT_FOUND"})
    return JSONResponse(
        status_code=200,
        content={
            "transaction_id": txn["transaction_id"],
            "request_id": txn["request_id"],
            "account_id": txn["account_id"],
            "transaction_type": txn["transaction_type"],
            "amount_minor": txn["amount_minor"],
            "currency": txn["currency"],
            "sequence_number": txn["sequence_number"],
            "status": txn["status"],
            "attempt_count": txn["attempt_count"],
            "last_error": txn["last_error"],
            "next_retry_at": txn["next_retry_at"],
            "created_at": txn["created_at"],
            "updated_at": txn["updated_at"],
        },
    )


@router.post("/transactions/{transaction_id}/retry")
def retry_transaction(
    transaction_id: str, service: TransactionService = Depends(get_service)
) -> JSONResponse:
    """Operator-initiated recovery for terminal FAILED jobs."""
    conn = get_connection()
    try:
        repo = TransactionRepository(conn)
        txn = repo.get_transaction(transaction_id)
        if txn is None:
            return JSONResponse(status_code=404, content={"error": "TRANSACTION_NOT_FOUND"})
        if not RetryService().is_manually_retryable(txn):
            return JSONResponse(
                status_code=409,
                content={
                    "error": "NOT_RETRYABLE",
                    "status": txn["status"],
                    "message": "Only FAILED transactions can be retried manually.",
                },
            )
        repo.reset_for_manual_retry(transaction_id)
    finally:
        conn.close()
    return JSONResponse(
        status_code=202,
        content={
            "transaction_id": transaction_id,
            "status": "RECEIVED",
            "message": "Transaction re-queued with a fresh attempt budget.",
        },
    )


@router.get("/accounts/{account_id}")
def get_account(account_id: str) -> JSONResponse:
    conn = get_connection()
    try:
        account = TransactionRepository(conn).get_account(account_id)
    finally:
        conn.close()
    if account is None:
        return JSONResponse(status_code=404, content={"error": "ACCOUNT_NOT_FOUND"})
    return JSONResponse(
        status_code=200,
        content={
            "account_id": account["account_id"],
            "balance_minor": account["balance_minor"],
            "currency": "INR",
            "last_sequence": account["last_sequence"],
        },
    )


@router.get("/metrics/summary")
def metrics_summary() -> dict:
    conn = get_connection()
    try:
        repo = TransactionRepository(conn)
        counts = repo.status_counts()
        return {
            "counts_by_status": counts,
            "total_transactions": sum(counts.values()),
            "processed_after_retry": repo.processed_after_retry_count(),
            "retry_outcomes": {
                "retry_pending": counts.get("RETRY_PENDING", 0),
                "failed": counts.get("FAILED", 0),
            },
            "ledger": repo.ledger_totals(),
        }
    finally:
        conn.close()
