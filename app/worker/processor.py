"""Durable background worker.

Polls the SQLite job table, claims work atomically (BEGIN IMMEDIATE +
guarded status transition) and runs the processing pipeline. Jobs survive
process restarts because the queue *is* the database.

Run embedded (default, inside the API process) or standalone:

    python -m app.worker.processor
"""
from __future__ import annotations

import asyncio
import logging
import signal
import sys

from app.config import get_settings
from app.db.database import init_db
from app.services.transaction_service import TransactionService, _log

logger = logging.getLogger("txn.worker")


class TransactionWorker:
    def __init__(self, service: TransactionService | None = None):
        self.service = service or TransactionService()
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        settings = get_settings()
        interval = settings.poll_interval_seconds
        _log("worker.started", poll_interval=interval, pid=_pid())
        while not self._stop.is_set():
            try:
                # Keep the event loop responsive: DB work is blocking.
                worked = await asyncio.to_thread(self.service.process_next)
            except Exception:
                logger.exception("worker poll iteration failed")
                worked = False
            if not worked:
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass
        _log("worker.stopped", pid=_pid())


def _pid() -> int:
    import os

    return os.getpid()


async def _main_async() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    init_db()
    worker = TransactionWorker()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.stop)
        except NotImplementedError:
            pass  # Windows: KeyboardInterrupt below

    _log("worker.booting", mode="external", pid=_pid())
    await worker.run()


def main() -> None:
    try:
        asyncio.run(_main_async())
    except KeyboardInterrupt:
        _log("worker.interrupted", pid=_pid())
        sys.exit(0)


if __name__ == "__main__":
    main()
