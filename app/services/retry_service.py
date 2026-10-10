"""Bounded retry policy for transient failures.

Deterministic business failures (e.g. insufficient funds) are NOT retried —
retrying them cannot change the outcome. Only transient faults consume the
attempt budget; when it is exhausted the job becomes FAILED and stays fully
observable for manual recovery via the retry endpoint.
"""
from __future__ import annotations

from datetime import timedelta

from app.config import get_settings
from app.db.database import utc_now

# Sanitized, machine-readable failure codes stored in ``last_error``.
ERROR_TRANSIENT = "TRANSIENT_FAILURE"
ERROR_RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
ERROR_INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS"
ERROR_SEQUENCE_CONFLICT = "SEQUENCE_CONFLICT"


class RetryService:
    def should_retry(self, attempts_used: int) -> bool:
        return attempts_used < get_settings().max_attempts

    def next_retry_at(self, attempts_used: int) -> str:
        """Exponential backoff: base * 2^(attempt-1) seconds from now."""
        base = get_settings().backoff_base_seconds
        delay = base * (2 ** max(attempts_used - 1, 0))
        return (utc_now() + timedelta(seconds=delay)).isoformat(timespec="microseconds")

    def is_manually_retryable(self, txn: dict) -> bool:
        """Only terminal FAILED jobs can be re-queued by an operator."""
        return txn["status"] == "FAILED"

    def max_attempts(self) -> int:
        return get_settings().max_attempts
