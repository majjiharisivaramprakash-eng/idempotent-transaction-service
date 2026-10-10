"""Persistence layer.

The repository only reads and writes rows. It never decides whether a
transaction is valid, ordered or retry-eligible — that belongs to the
service layer.
"""
from __future__ import annotations

import sqlite3
from typing import Optional

from app.db.database import now_iso
from app.models.transaction import Status, TransactionRequest


def _row_to_dict(row: Optional[sqlite3.Row]) -> Optional[dict]:
    return dict(row) if row is not None else None


class TransactionRepository:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    # ------------------------------------------------------------------ #
    # transactions
    # ------------------------------------------------------------------ #
    def insert_transaction(self, req: TransactionRequest) -> dict:
        ts = now_iso()
        self.conn.execute(
            """
            INSERT INTO transactions (
                transaction_id, request_id, account_id, transaction_type,
                amount_minor, currency, sequence_number, status,
                attempt_count, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
            """,
            (
                req.transaction_id,
                req.request_id,
                req.account_id,
                req.transaction_type.value,
                req.amount_minor,
                req.currency,
                req.sequence_number,
                Status.RECEIVED.value,
                ts,
                ts,
            ),
        )
        return self.get_transaction(req.transaction_id)  # type: ignore[return-value]

    def get_transaction(self, transaction_id: str) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM transactions WHERE transaction_id = ?", (transaction_id,)
        ).fetchone()
        return _row_to_dict(row)

    def list_claimable(self, limit: int = 10) -> list[dict]:
        """Jobs eligible for a processing attempt right now."""
        rows = self.conn.execute(
            """
            SELECT transaction_id FROM transactions
            WHERE status = 'RECEIVED'
               OR (status = 'RETRY_PENDING'
                   AND (next_retry_at IS NULL OR next_retry_at <= ?))
            ORDER BY created_at
            LIMIT ?
            """,
            (now_iso(), limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def try_claim(self, transaction_id: str) -> Optional[dict]:
        """Atomically move a claimable job to PROCESSING.

        Must be called inside an ``immediate_tx`` block. Returns the claimed
        row, or ``None`` if another worker already owns the job (rowcount 0).
        """
        cur = self.conn.execute(
            """
            UPDATE transactions
               SET status = 'PROCESSING', updated_at = ?
             WHERE transaction_id = ?
               AND (status = 'RECEIVED'
                    OR (status = 'RETRY_PENDING'
                        AND (next_retry_at IS NULL OR next_retry_at <= ?)))
            """,
            (now_iso(), transaction_id, now_iso()),
        )
        if cur.rowcount != 1:
            return None
        return self.get_transaction(transaction_id)

    def set_status(
        self,
        transaction_id: str,
        status: Status,
        last_error: Optional[str] = None,
        next_retry_at: Optional[str] = None,
        attempt_count: Optional[int] = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE transactions
               SET status = ?, last_error = ?, next_retry_at = ?,
                   attempt_count = COALESCE(?, attempt_count), updated_at = ?
             WHERE transaction_id = ?
            """,
            (status.value, last_error, next_retry_at, attempt_count, now_iso(), transaction_id),
        )

    def reset_for_manual_retry(self, transaction_id: str) -> None:
        """Operator-initiated retry: fresh attempt budget, back to the queue."""
        self.conn.execute(
            """
            UPDATE transactions
               SET status = 'RECEIVED', attempt_count = 0, last_error = NULL,
                   next_retry_at = NULL, updated_at = ?
             WHERE transaction_id = ?
            """,
            (now_iso(), transaction_id),
        )

    def requeue_successors(self, account_id: str, sequence_number: int) -> int:
        """Flip a PENDING_ORDER job to RECEIVED once its predecessor applied."""
        cur = self.conn.execute(
            """
            UPDATE transactions
               SET status = 'RECEIVED', updated_at = ?
             WHERE account_id = ? AND sequence_number = ? AND status = 'PENDING_ORDER'
            """,
            (now_iso(), account_id, sequence_number),
        )
        return cur.rowcount

    def status_counts(self) -> dict:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) AS n FROM transactions GROUP BY status"
        ).fetchall()
        return {r["status"]: r["n"] for r in rows}

    def processed_after_retry_count(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM transactions WHERE status = 'PROCESSED' AND attempt_count > 0"
        ).fetchone()
        return row["n"]

    # ------------------------------------------------------------------ #
    # accounts
    # ------------------------------------------------------------------ #
    def ensure_account(self, account_id: str) -> None:
        ts = now_iso()
        self.conn.execute(
            """
            INSERT INTO accounts (account_id, balance_minor, last_sequence, created_at, updated_at)
            VALUES (?, 0, 0, ?, ?)
            ON CONFLICT(account_id) DO NOTHING
            """,
            (account_id, ts, ts),
        )

    def get_account(self, account_id: str) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM accounts WHERE account_id = ?", (account_id,)
        ).fetchone()
        return _row_to_dict(row)

    def apply_account_effect(self, account_id: str, signed_amount: int, sequence_number: int) -> None:
        cur = self.conn.execute(
            """
            UPDATE accounts
               SET balance_minor = balance_minor + ?,
                   last_sequence = ?,
                   updated_at = ?
             WHERE account_id = ? AND last_sequence = ?
            """,
            (signed_amount, sequence_number, now_iso(), account_id, sequence_number - 1),
        )
        if cur.rowcount != 1:
            raise sqlite3.IntegrityError(
                f"account sequence advanced concurrently for {account_id}"
            )

    # ------------------------------------------------------------------ #
    # ledger
    # ------------------------------------------------------------------ #
    def insert_ledger_entry(self, transaction_id: str, account_id: str, signed_amount: int) -> None:
        self.conn.execute(
            """
            INSERT INTO ledger_entries (transaction_id, account_id, amount_minor, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (transaction_id, account_id, signed_amount, now_iso()),
        )

    def has_ledger_entry(self, transaction_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM ledger_entries WHERE transaction_id = ?", (transaction_id,)
        ).fetchone()
        return row is not None

    def ledger_count_for_transaction(self, transaction_id: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM ledger_entries WHERE transaction_id = ?",
            (transaction_id,),
        ).fetchone()
        return row["n"]

    def ledger_totals(self) -> dict:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS entries,
                   COALESCE(SUM(CASE WHEN amount_minor > 0 THEN amount_minor ELSE 0 END), 0) AS credits,
                   COALESCE(SUM(CASE WHEN amount_minor < 0 THEN -amount_minor ELSE 0 END), 0) AS debits
              FROM ledger_entries
            """
        ).fetchone()
        return {
            "entries": row["entries"],
            "total_credit_minor": row["credits"],
            "total_debit_minor": row["debits"],
            "net_minor": row["credits"] - row["debits"],
        }

    # ------------------------------------------------------------------ #
    # idempotency keys
    # ------------------------------------------------------------------ #
    def get_idempotency_key(self, key: str) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM idempotency_keys WHERE idempotency_key = ?", (key,)
        ).fetchone()
        return _row_to_dict(row)

    def save_idempotency_key(
        self, key: str, payload_hash: str, transaction_id: str, result_status: str
    ) -> None:
        ts = now_iso()
        self.conn.execute(
            """
            INSERT INTO idempotency_keys (idempotency_key, payload_hash, transaction_id,
                                          result_status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(idempotency_key)
            DO UPDATE SET result_status = excluded.result_status, updated_at = excluded.updated_at
            """,
            (key, payload_hash, transaction_id, result_status, ts, ts),
        )

    def update_idempotency_result(self, key: str, result_status: str) -> None:
        self.conn.execute(
            "UPDATE idempotency_keys SET result_status = ?, updated_at = ? WHERE idempotency_key = ?",
            (result_status, now_iso(), key),
        )
