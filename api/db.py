"""Postgres/pgvector connection pool."""

from __future__ import annotations

import os

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://epstein_reader:epstein@localhost:5432/epstein")

_pool: ConnectionPool | None = None


def _configure_conn(conn):
    conn.execute("SET hnsw.ef_search = 100")
    conn.commit()


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            DATABASE_URL,
            min_size=2,
            max_size=10,
            kwargs={"row_factory": dict_row},
            configure=_configure_conn,
        )
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
