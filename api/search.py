"""Search endpoint: embed query on CPU, search pgvector."""

from __future__ import annotations

import logging

from pydantic import BaseModel, Field

from api.db import get_conn

logger = logging.getLogger(__name__)

# Model loaded at startup by main.py
_model = None


def set_model(model):
    global _model
    _model = model


class SearchRequest(BaseModel):
    query: str
    limit: int = Field(default=20, ge=1, le=100)
    dataset: int | None = None


class ChunkResult(BaseModel):
    efta_id: str
    dataset: int | None = None
    chunk_index: int
    total_chunks: int
    text: str
    score: float


class SearchResponse(BaseModel):
    query: str
    results: list[ChunkResult]


def search(req: SearchRequest) -> SearchResponse:
    """Embed query and search pgvector."""
    if _model is None:
        raise RuntimeError("Embedding model not loaded")

    # Embed query
    embeddings = _model.encode(
        [req.query],
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    query_vec = embeddings[0].tolist()
    vec_str = "[" + ",".join(str(v) for v in query_vec) + "]"

    conn = get_conn()

    if req.dataset is not None:
        sql = """
            SELECT c.efta_id, d.dataset, c.chunk_index, c.total_chunks, c.text,
                   1 - (c.embedding <=> %s::halfvec) AS score
            FROM chunks c
            JOIN documents d ON d.efta_id = c.efta_id
            WHERE d.dataset = %s
            ORDER BY c.embedding <=> %s::halfvec
            LIMIT %s
        """
        params = (vec_str, req.dataset, vec_str, req.limit)
    else:
        sql = """
            SELECT c.efta_id, d.dataset, c.chunk_index, c.total_chunks, c.text,
                   1 - (c.embedding <=> %s::halfvec) AS score
            FROM chunks c
            JOIN documents d ON d.efta_id = c.efta_id
            ORDER BY c.embedding <=> %s::halfvec
            LIMIT %s
        """
        params = (vec_str, vec_str, req.limit)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    results = [
        ChunkResult(
            efta_id=row["efta_id"],
            dataset=row["dataset"],
            chunk_index=row["chunk_index"],
            total_chunks=row["total_chunks"],
            text=row["text"],
            score=float(row["score"]),
        )
        for row in rows
    ]

    return SearchResponse(query=req.query, results=results)
