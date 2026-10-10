"""FastAPI application and lifecycle.

By default (TXN_WORKER_MODE=embedded) the background worker runs inside the
API process so the whole system starts with one command. For a production
topology set TXN_WORKER_MODE=off on the API and run the worker as a separate
process: python -m app.worker.processor
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.config import WORKER_MODE_EMBEDDED, get_settings
from app.db.database import init_db
from app.worker.processor import TransactionWorker

logger = logging.getLogger("txn.main")


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logging.basicConfig(level=logging.INFO, format="%(message)s")
        init_db()
        settings = get_settings()
        worker_task: asyncio.Task | None = None
        worker: TransactionWorker | None = None
        if settings.worker_mode == WORKER_MODE_EMBEDDED:
            worker = TransactionWorker()
            worker_task = asyncio.create_task(worker.run())
        app.state.worker = worker
        app.state.worker_task = worker_task
        try:
            yield
        finally:
            if worker is not None:
                worker.stop()
            if worker_task is not None:
                await asyncio.wait_for(worker_task, timeout=5)

    app = FastAPI(
        title="Idempotent Transaction Service",
        version="1.0.0",
        description="Durable, idempotent, order-aware transaction processing engine.",
        lifespan=lifespan,
    )
    app.include_router(router)
    return app


app = create_app()
