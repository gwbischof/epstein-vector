"""Write-capable Postgres pool for ingestion endpoints."""

from __future__ import annotations

import os

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

INGEST_DATABASE_URL = os.environ.get("INGEST_DATABASE_URL", "")

_pool: ConnectionPool | None = None


def get_ingest_pool() -> ConnectionPool:
    global _pool
    if not INGEST_DATABASE_URL:
        raise RuntimeError("INGEST_DATABASE_URL not set — ingestion endpoints disabled")
    if _pool is None:
        _pool = ConnectionPool(
            INGEST_DATABASE_URL,
            min_size=2,
            max_size=10,
            kwargs={"row_factory": dict_row},
        )
    return _pool


def close_ingest_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
