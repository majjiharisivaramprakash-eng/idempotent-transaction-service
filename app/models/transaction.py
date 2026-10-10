"""Request schemas, field constraints and transaction statuses.

Validation rules live here so the API layer and the batch script share one
definition of a well-formed transaction. Business rules (idempotency,
ordering, retries) are deliberately NOT part of this module.
"""
from __future__ import annotations

import hashlib
import json
from enum import Enum

from pydantic import BaseModel, Field, field_validator

# Identifiers: alphanumeric start, then letters/digits/underscore/hyphen.
ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{5,63}$"
ACCOUNT_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{2,31}$"

SUPPORTED_CURRENCIES = {"INR"}


class TransactionType(str, Enum):
    CREDIT = "CREDIT"
    DEBIT = "DEBIT"


class Status(str, Enum):
    """Lifecycle states for a transaction job.

    RECEIVED      -> persisted, waiting for the worker
    PROCESSING    -> claimed by a worker
    PENDING_ORDER -> valid, but an earlier account sequence is missing
    RETRY_PENDING -> transient failure, scheduled for another attempt
    PROCESSED     -> ledger effect applied atomically
    FAILED        -> terminal failure, observable and manually recoverable
    """

    RECEIVED = "RECEIVED"
    PROCESSING = "PROCESSING"
    PENDING_ORDER = "PENDING_ORDER"
    RETRY_PENDING = "RETRY_PENDING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


class TransactionRequest(BaseModel):
    """A single incoming transaction instruction."""

    request_id: str = Field(
        ...,
        pattern=ID_PATTERN,
        description="Unique incoming request key; doubles as the idempotency key.",
    )
    transaction_id: str = Field(
        ...,
        pattern=ID_PATTERN,
        description="Unique business transaction identifier.",
    )
    account_id: str = Field(..., pattern=ACCOUNT_ID_PATTERN)
    transaction_type: TransactionType
    amount_minor: int = Field(
        ...,
        gt=0,
        le=10**12,
        description="Positive amount in minor units (e.g. paise).",
    )
    currency: str = Field(default="INR")
    sequence_number: int = Field(
        ...,
        ge=1,
        le=10**9,
        description="Account-level ordering sequence, starting at 1.",
    )

    @field_validator("currency")
    @classmethod
    def _currency_supported(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in SUPPORTED_CURRENCIES:
            raise ValueError(
                f"unsupported currency '{value}'; supported: {sorted(SUPPORTED_CURRENCIES)}"
            )
        return normalized

    @field_validator("transaction_type", mode="before")
    @classmethod
    def _normalize_type(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value

    def business_payload(self) -> dict:
        """Canonical payload used for idempotency-hash comparison.

        ``request_id`` is excluded: it *is* the idempotency key, so the hash
        must capture everything else about the business instruction.
        """
        return {
            "transaction_id": self.transaction_id,
            "account_id": self.account_id,
            "transaction_type": self.transaction_type.value,
            "amount_minor": self.amount_minor,
            "currency": self.currency,
            "sequence_number": self.sequence_number,
        }

    def payload_hash(self) -> str:
        canonical = json.dumps(self.business_payload(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
