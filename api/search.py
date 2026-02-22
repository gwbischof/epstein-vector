"""Search endpoints: vector (semantic) and full-text keyword search."""

from __future__ import annotations

import logging
import re

from pydantic import BaseModel, Field

from api.db import get_pool

logger = logging.getLogger(__name__)

# Model loaded at startup by main.py
_model = None


def set_model(model):
    global _model
    _model = model


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


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

    pool = get_pool()

    sql = """
        SELECT c.efta_id, d.dataset, c.chunk_index, c.total_chunks, c.text,
               1 - (c.embedding <=> %s::halfvec) AS score
        FROM chunks c
        JOIN documents d ON d.efta_id = c.efta_id
        ORDER BY c.embedding <=> %s::halfvec
        LIMIT %s OFFSET %s
    """
    params = (vec_str, vec_str, req.limit, req.offset)

    with pool.connection() as conn:
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


class TextSearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


class TextResult(BaseModel):
    efta_id: str
    dataset: int | None = None
    word_count: int
    rank: float
    headline: str


class TextSearchResponse(BaseModel):
    query: str
    total: int | None = None
    results: list[TextResult]


def _has_wildcards(query: str) -> bool:
    """Check if the query contains wildcard characters."""
    return "*" in query


def _build_wildcard_tsquery(query: str) -> str:
    """Build a raw tsquery string with :* prefix matching for wildcard terms.

    Supports: word* (prefix), OR between terms, - for NOT.
    Terms without * are matched exactly. All terms are ANDed unless OR is used.
    """
    tokens = query.split()
    parts: list[str] = []
    op = "&"

    for token in tokens:
        upper = token.upper()
        if upper == "OR":
            op = "|"
            continue
        if upper == "AND":
            op = "&"
            continue

        negate = token.startswith("-")
        if negate:
            token = token[1:]

        # Strip wildcard, sanitize to alphanumeric
        is_prefix = token.endswith("*")
        word = re.sub(r"[^\w]", "", token.rstrip("*"))
        if not word:
            continue

        term = f"'{word}':*" if is_prefix else f"'{word}'"
        if negate:
            term = f"!{term}"

        if parts:
            parts.append(op)
        parts.append(term)
        op = "&"  # reset to AND after each term

    return " ".join(parts) if parts else "''"


def text_search(req: TextSearchRequest) -> TextSearchResponse:
    """Full-text search over documents."""
    pool = get_pool()

    use_wildcard = _has_wildcards(req.query)

    # Choose tsquery function:
    # - Wildcards: to_tsquery with manually built prefix expressions
    # - Normal: websearch_to_tsquery for AND, OR, NOT, "exact phrases"
    if use_wildcard:
        tsquery_expr = "to_tsquery('english', %s)"
        query_param = _build_wildcard_tsquery(req.query)
    else:
        tsquery_expr = "websearch_to_tsquery('english', %s)"
        query_param = req.query

    sql = f"""
        SELECT efta_id, dataset, word_count,
               ts_rank(tsv, {tsquery_expr}) AS rank,
               ts_headline('english', text, {tsquery_expr},
                           'MaxWords=60, MinWords=20, MaxFragments=3') AS headline
        FROM documents
        WHERE tsv @@ {tsquery_expr}
        ORDER BY rank DESC
        LIMIT %s OFFSET %s
    """
    params = (query_param, query_param, query_param, req.limit, req.offset)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    results = [
        TextResult(
            efta_id=row["efta_id"],
            dataset=row["dataset"],
            word_count=row["word_count"],
            rank=float(row["rank"]),
            headline=row["headline"],
        )
        for row in rows
    ]

    return TextSearchResponse(query=req.query, results=results)


class FuzzySearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)
    exclude_exact: bool = Field(default=False, description="Exclude documents that match via keyword search")


class FuzzyResult(BaseModel):
    efta_id: str
    dataset: int | None = None
    chunk_index: int
    total_chunks: int
    text: str
    similarity: float


class FuzzySearchResponse(BaseModel):
    query: str
    total: int | None = None
    results: list[FuzzyResult]


def ensure_pg_trgm():
    """Try to create pg_trgm extension and trigram index. Non-fatal if it fails."""
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_chunks_trgm "
                    "ON chunks USING gin (text gin_trgm_ops)"
                )
            conn.commit()
    except Exception as e:
        logger.warning("pg_trgm setup skipped (fuzzy search will be unavailable): %s", e)


def fuzzy_search(req: FuzzySearchRequest) -> FuzzySearchResponse:
    """Fuzzy trigram search over chunks — typo-tolerant matching."""
    pool = get_pool()

    # Optional: exclude docs that keyword search already finds
    exclude_clause = ""
    exclude_params: tuple = ()
    if req.exclude_exact:
        exclude_clause = " AND NOT d.tsv @@ websearch_to_tsquery('english', %s)"
        exclude_params = (req.query,)

    sql = f"""
        SELECT c.efta_id, d.dataset, c.chunk_index, c.total_chunks, c.text,
               word_similarity(%s, c.text) AS sim
        FROM chunks c
        JOIN documents d ON d.efta_id = c.efta_id
        WHERE %s <%% c.text{exclude_clause}
        ORDER BY sim DESC
        LIMIT %s OFFSET %s
    """
    params = (req.query, req.query) + exclude_params + (req.limit, req.offset)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    results = [
        FuzzyResult(
            efta_id=row["efta_id"],
            dataset=row["dataset"],
            chunk_index=row["chunk_index"],
            total_chunks=row["total_chunks"],
            text=row["text"],
            similarity=float(row["sim"]),
        )
        for row in rows
    ]

    return FuzzySearchResponse(query=req.query, results=results)


class SimilarRequest(BaseModel):
    efta_id: str = Field(..., min_length=1)
    chunk_index: int = Field(default=0, ge=0)
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


def similar(req: SimilarRequest) -> SearchResponse:
    """Find chunks similar to an existing chunk by reusing its embedding."""
    pool = get_pool()

    # Fetch the source chunk's embedding
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT embedding FROM chunks WHERE efta_id = %s AND chunk_index = %s",
                (req.efta_id, req.chunk_index),
            )
            row = cur.fetchone()

    if row is None:
        return SearchResponse(query=f"Similar to {req.efta_id}", results=[])

    vec_str = str(row["embedding"])

    # Find nearest neighbors, excluding the source chunk itself
    sql = """
        SELECT c.efta_id, d.dataset, c.chunk_index, c.total_chunks, c.text,
               1 - (c.embedding <=> %s::halfvec) AS score
        FROM chunks c
        JOIN documents d ON d.efta_id = c.efta_id
        WHERE NOT (c.efta_id = %s AND c.chunk_index = %s)
        ORDER BY c.embedding <=> %s::halfvec
        LIMIT %s OFFSET %s
    """
    params = (vec_str, req.efta_id, req.chunk_index, vec_str, req.limit, req.offset)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    results = [
        ChunkResult(
            efta_id=r["efta_id"],
            dataset=r["dataset"],
            chunk_index=r["chunk_index"],
            total_chunks=r["total_chunks"],
            text=r["text"],
            score=float(r["score"]),
        )
        for r in rows
    ]

    return SearchResponse(query=f"Similar to {req.efta_id}", results=results)
