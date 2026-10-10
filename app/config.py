"""Application configuration.

Settings are read from environment variables (optionally populated from a
local ``.env`` file) on every call, which keeps the configuration trivially
overridable in tests without module reloading.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

WORKER_MODE_EMBEDDED = "embedded"
WORKER_MODE_EXTERNAL = "external"
WORKER_MODE_OFF = "off"


@dataclass(frozen=True)
class Settings:
    db_path: str
    worker_mode: str
    poll_interval_seconds: float
    max_attempts: int
    backoff_base_seconds: float
    fault_transient_amount: int
    fault_permanent_amount: int


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def get_settings() -> Settings:
    """Build settings from the current environment."""
    db_path = os.getenv("TXN_DB_PATH") or str(BASE_DIR / "txn.db")
    if not Path(db_path).is_absolute():
        db_path = str(BASE_DIR / db_path)
    return Settings(
        db_path=db_path,
        worker_mode=os.getenv("TXN_WORKER_MODE", WORKER_MODE_EMBEDDED).lower(),
        poll_interval_seconds=_float_env("TXN_POLL_INTERVAL_SECONDS", 1.0),
        max_attempts=_int_env("TXN_MAX_ATTEMPTS", 3),
        backoff_base_seconds=_float_env("TXN_BACKOFF_BASE_SECONDS", 1.0),
        fault_transient_amount=_int_env("TXN_FAULT_TRANSIENT_AMOUNT", 1111),
        fault_permanent_amount=_int_env("TXN_FAULT_PERMANENT_AMOUNT", 2222),
    )
