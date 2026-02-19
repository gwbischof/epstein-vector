"""Write-capable Postgres pool for ingestion endpoints."""

from __future__ import annotations

import logging
import os

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

INGEST_DATABASE_URL = os.environ.get("INGEST_DATABASE_URL", "")

_pool: ConnectionPool | None = None


def get_ingest_pool() -> ConnectionPool:
    global _pool
    if not INGEST_DATABASE_URL:
        raise RuntimeError("INGEST_DATABASE_URL not set — ingestion endpoints disabled")
    if _pool is None:
        # Mask password for logging
        masked = INGEST_DATABASE_URL.split("@")[-1] if "@" in INGEST_DATABASE_URL else INGEST_DATABASE_URL
        logger.info(f"Creating ingest pool: ...@{masked}")
        _pool = ConnectionPool(
            INGEST_DATABASE_URL,
            min_size=0,
            max_size=10,
            kwargs={"row_factory": dict_row},
        )
    return _pool


def close_ingest_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
