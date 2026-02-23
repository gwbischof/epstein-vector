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
    version: int = 1


class ChunkRecord(BaseModel):
    efta_id: str
    chunk_index: int
    total_chunks: int
    text: str
    embedding: list[float] | None = None  # 1024-dim, None for sentinel chunks
    version: int = 1


class IngestRequest(BaseModel):
    documents: list[DocumentRecord]
    chunks: list[ChunkRecord]
    overwrite: bool = False


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
    pool = get_ingest_pool()

    # Single-doc check
    if efta_id is not None:
        if dataset is not None:
            sql = """
                SELECT 1 FROM documents
                WHERE efta_id = %s AND dataset = %s
                LIMIT 1
            """
            params = (efta_id, dataset)
        else:
            sql = "SELECT 1 FROM documents WHERE efta_id = %s LIMIT 1"
            params = (efta_id,)

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                found = cur.fetchone() is not None

        return DoneResponse(efta_id=efta_id, done=found, count=1 if found else 0)

    # Full list mode — requires dataset
    if dataset is None:
        raise HTTPException(status_code=400, detail="Provide dataset or efta_id parameter")

    sql = "SELECT efta_id FROM documents WHERE dataset = %s"
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (dataset,))
            ids = [row["efta_id"] for row in cur.fetchall()]

    return DoneResponse(efta_ids=ids, count=len(ids))


@router.get("/ingest/stats")
def ingest_stats(dataset: int | None = None):
    """Return total embedded document and chunk counts, optionally filtered by dataset."""
    pool = get_ingest_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            if dataset is not None:
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT c.efta_id) AS docs, COUNT(*) AS chunks
                    FROM chunks c JOIN documents d ON d.efta_id = c.efta_id
                    WHERE d.dataset = %s
                    """,
                    (dataset,),
                )
            else:
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT efta_id) AS docs, COUNT(*) AS chunks
                    FROM chunks
                    """,
                )
            row = cur.fetchone()
    return {"dataset": dataset, "documents": row["docs"], "chunks": row["chunks"]}


@router.post("/ingest")
def ingest(req: IngestRequest) -> IngestResponse:
    """Accept batch of documents + chunks with pre-computed embeddings."""
    # Validate embedding dimensions (skip sentinel chunks with None embedding)
    for i, chunk in enumerate(req.chunks):
        if chunk.embedding is not None and len(chunk.embedding) != EMBEDDING_DIM:
            raise HTTPException(
                status_code=422,
                detail=f"Chunk {i} ({chunk.efta_id}:{chunk.chunk_index}) has {len(chunk.embedding)}-dim embedding, expected {EMBEDDING_DIM}",
            )

    pool = get_ingest_pool()

    DB_BATCH = 500  # max rows per executemany to limit memory/transaction size

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
                    doc.version,
                ))

            if req.overwrite:
                doc_sql = """
                    INSERT INTO documents (efta_id, dataset, url, pages, word_count, text, version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (efta_id) DO UPDATE SET
                        dataset=EXCLUDED.dataset, url=EXCLUDED.url, pages=EXCLUDED.pages,
                        word_count=EXCLUDED.word_count, text=EXCLUDED.text, version=EXCLUDED.version
                """
            else:
                doc_sql = """
                    INSERT INTO documents (efta_id, dataset, url, pages, word_count, text, version)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (efta_id) DO NOTHING
                """

            for i in range(0, len(doc_values), DB_BATCH):
                cur.executemany(doc_sql, doc_values[i : i + DB_BATCH])

            # When overwriting, delete old chunks for all efta_ids in this batch
            if req.overwrite and req.chunks:
                efta_ids = list({c.efta_id for c in req.chunks})
                cur.execute("DELETE FROM chunks WHERE efta_id = ANY(%s)", (efta_ids,))

            # Insert chunks with embeddings in batches
            chunk_values = []
            for chunk in req.chunks:
                if chunk.embedding is not None:
                    vec_str = "[" + ",".join(str(v) for v in chunk.embedding) + "]"
                else:
                    vec_str = None  # NULL in Postgres — skipped by HNSW index
                chunk_values.append((
                    chunk.efta_id,
                    chunk.chunk_index,
                    chunk.total_chunks,
                    chunk.text,
                    vec_str,
                    chunk.version,
                ))

            for i in range(0, len(chunk_values), DB_BATCH):
                cur.executemany(
                    """
                    INSERT INTO chunks (efta_id, chunk_index, total_chunks, text, embedding, version)
                    VALUES (%s, %s, %s, %s, %s::halfvec, %s)
                    ON CONFLICT (efta_id, chunk_index) DO NOTHING
                    """,
                    chunk_values[i : i + DB_BATCH],
                )

        conn.commit()

    logger.info(f"Ingested {len(req.documents)} docs, {len(req.chunks)} chunks (overwrite={req.overwrite})")
    return IngestResponse(
        inserted_documents=len(req.documents),
        inserted_chunks=len(req.chunks),
    )


# --- Chunk status endpoint ---


class ChunkStatusRequest(BaseModel):
    efta_ids: list[str] = Field(..., max_length=50)


class ChunkStatusItem(BaseModel):
    efta_id: str
    has_doc: bool
    doc_word_count: int
    doc_version: int
    chunk_count: int
    embedded_count: int
    chunk_version: int


@router.post("/ingest/chunk_status")
def ingest_chunk_status(req: ChunkStatusRequest) -> list[ChunkStatusItem]:
    """Return per-document chunk status for a batch of efta_ids."""
    pool = get_ingest_pool()
    sql = """
        SELECT
            e.efta_id,
            d.efta_id IS NOT NULL AS has_doc,
            COALESCE(d.word_count, 0) AS doc_word_count,
            COALESCE(d.version, 0) AS doc_version,
            COUNT(c.efta_id) AS chunk_count,
            COUNT(c.embedding) AS embedded_count,
            COALESCE(MIN(c.version), 0) AS chunk_version
        FROM unnest(%s::text[]) AS e(efta_id)
        LEFT JOIN documents d ON d.efta_id = e.efta_id
        LEFT JOIN chunks c ON c.efta_id = e.efta_id
        GROUP BY e.efta_id, d.efta_id, d.word_count, d.version
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (req.efta_ids,))
            rows = cur.fetchall()
    return [ChunkStatusItem(**row) for row in rows]
