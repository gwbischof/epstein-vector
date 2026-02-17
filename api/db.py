"""Postgres/pgvector connection pool."""

from __future__ import annotations

import os

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://epstein:epstein@localhost:5432/epstein")

_conn: psycopg.Connection | None = None


def get_conn() -> psycopg.Connection:
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    return _conn


def close_conn() -> None:
    global _conn
    if _conn is not None and not _conn.closed:
        _conn.close()
        _conn = None
