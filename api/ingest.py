"""Ingestion endpoints: accept pre-embedded documents from GPU workers."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from api.ingest_db import get_ingest_pool

logger = logging.getLogger(__name__)

router = APIRouter()

EMBEDDING_DIM = 1024
MAX_TSV_BYTES = 750_000


# --- Request / Response models ---


class DocumentRecord(BaseModel):
    efta_id: str
    dataset: int
    url: str = ""
    pages: int = 0
    word_count: int = 0
    text: str = ""


class ChunkRecord(BaseModel):
    efta_id: str
    chunk_index: int
    total_chunks: int
    text: str
    embedding: list[float]  # 1024-dim


class IngestRequest(BaseModel):
    documents: list[DocumentRecord]
    chunks: list[ChunkRecord]


class IngestResponse(BaseModel):
    inserted_documents: int
    inserted_chunks: int


class DoneResponse(BaseModel):
    efta_ids: list[str] = []
    count: int = 0
    # For single-doc check mode
    efta_id: str | None = None
    done: bool | None = None


# --- Endpoints ---


@router.get("/ingest/done")
def ingest_done(dataset: int | None = None, efta_id: str | None = None) -> DoneResponse:
    """Check which documents are already embedded.

    Modes:
    - ?dataset=9           → full list of done efta_ids for that dataset
    - ?efta_id=EFTA001234  → check if single doc is done
    - ?dataset=9&efta_id=X → check if doc is done within dataset
    """
    try:
        pool = get_ingest_pool()
    except Exception as e:
        logger.error(f"Ingest pool error: {e}")
        raise HTTPException(status_code=503, detail=str(e))

    # Single-doc check
    if efta_id is not None:
        if dataset is not None:
            sql = """
                SELECT 1 FROM chunks c
                JOIN documents d ON d.efta_id = c.efta_id
                WHERE c.efta_id = %s AND d.dataset = %s
                LIMIT 1
            """
            params = (efta_id, dataset)
        else:
            sql = "SELECT 1 FROM chunks WHERE efta_id = %s LIMIT 1"
            params = (efta_id,)

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                found = cur.fetchone() is not None

        return DoneResponse(efta_id=efta_id, done=found, count=1 if found else 0)

    # Full list mode — requires dataset
    if dataset is None:
        raise HTTPException(status_code=400, detail="Provide dataset or efta_id parameter")

    sql = """
        SELECT DISTINCT c.efta_id
        FROM chunks c
        JOIN documents d ON d.efta_id = c.efta_id
        WHERE d.dataset = %s
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (dataset,))
            ids = [row["efta_id"] for row in cur.fetchall()]

    return DoneResponse(efta_ids=ids, count=len(ids))


@router.post("/ingest")
def ingest(req: IngestRequest) -> IngestResponse:
    """Accept batch of documents + chunks with pre-computed embeddings."""
    # Validate embedding dimensions
    for i, chunk in enumerate(req.chunks):
        if len(chunk.embedding) != EMBEDDING_DIM:
            raise HTTPException(
                status_code=422,
                detail=f"Chunk {i} ({chunk.efta_id}:{chunk.chunk_index}) has {len(chunk.embedding)}-dim embedding, expected {EMBEDDING_DIM}",
            )

    pool = get_ingest_pool()

    with pool.connection() as conn:
        with conn.cursor() as cur:
            # Insert documents first (chunks have FK to documents)
            doc_values = []
            for doc in req.documents:
                text = doc.text
                encoded = text.encode("utf-8")
                if len(encoded) > MAX_TSV_BYTES:
                    text = encoded[:MAX_TSV_BYTES].decode("utf-8", errors="ignore")
                doc_values.append((
                    doc.efta_id,
                    doc.dataset,
                    doc.url,
                    doc.pages,
                    doc.word_count,
                    text,
                ))

            if doc_values:
                cur.executemany(
                    """
                    INSERT INTO documents (efta_id, dataset, url, pages, word_count, text)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (efta_id) DO NOTHING
                    """,
                    doc_values,
                )

            # Insert chunks with embeddings
            chunk_values = []
            for chunk in req.chunks:
                vec_str = "[" + ",".join(str(v) for v in chunk.embedding) + "]"
                chunk_values.append((
                    chunk.efta_id,
                    chunk.chunk_index,
                    chunk.total_chunks,
                    chunk.text,
                    vec_str,
                ))

            if chunk_values:
                cur.executemany(
                    """
                    INSERT INTO chunks (efta_id, chunk_index, total_chunks, text, embedding)
                    VALUES (%s, %s, %s, %s, %s::halfvec)
                    ON CONFLICT (efta_id, chunk_index) DO NOTHING
                    """,
                    chunk_values,
                )

        conn.commit()

    logger.info(f"Ingested {len(req.documents)} docs, {len(req.chunks)} chunks")
    return IngestResponse(
        inserted_documents=len(req.documents),
        inserted_chunks=len(req.chunks),
    )
