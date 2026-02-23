"""Ingestion endpoints: accept pre-embedded documents from GPU workers."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query
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


class DocumentUpsertRequest(BaseModel):
    documents: list[DocumentRecord]


class DocumentUpsertResponse(BaseModel):
    upserted: int


class PendingResponse(BaseModel):
    efta_ids: list[str]
    count: int


class DocumentFetchRequest(BaseModel):
    efta_ids: list[str] = Field(..., max_length=50)


class DocumentFetchRecord(BaseModel):
    efta_id: str
    dataset: int
    url: str
    pages: int
    word_count: int
    text: str
    version: int


class ChunksRequest(BaseModel):
    chunks: list[ChunkRecord]
    overwrite: bool = False


class ChunksResponse(BaseModel):
    inserted_chunks: int


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


# --- Endpoints ---


@router.post("/ingest/documents")
def ingest_documents(req: DocumentUpsertRequest) -> DocumentUpsertResponse:
    """Upsert document rows with version-gated overwrites.

    Documents are only updated if the incoming version is strictly greater
    than the existing version. Same or lower versions are no-ops.
    This makes the endpoint fully idempotent — safe to re-run any time.
    """
    pool = get_ingest_pool()

    DB_BATCH = 500

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

    sql = """
        INSERT INTO documents (efta_id, dataset, url, pages, word_count, text, version)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (efta_id) DO UPDATE SET
            dataset=EXCLUDED.dataset, url=EXCLUDED.url, pages=EXCLUDED.pages,
            word_count=EXCLUDED.word_count, text=EXCLUDED.text, version=EXCLUDED.version
        WHERE EXCLUDED.version > documents.version
    """

    with pool.connection() as conn:
        with conn.cursor() as cur:
            for i in range(0, len(doc_values), DB_BATCH):
                cur.executemany(sql, doc_values[i : i + DB_BATCH])
        conn.commit()

    logger.info(f"Upserted {len(req.documents)} docs (version-gated)")
    return DocumentUpsertResponse(upserted=len(req.documents))


@router.get("/ingest/documents/pending")
def ingest_documents_pending(
    dataset: int = Query(...),
    status: str = Query("pending", pattern="^(pending|all)$"),
    limit: int = Query(10000, ge=1, le=100000),
    offset: int = Query(0, ge=0),
) -> PendingResponse:
    """Return efta_ids for a dataset filtered by chunking status.

    - status=pending (default): docs with zero chunks
    - status=all: all docs in the dataset
    """
    pool = get_ingest_pool()

    if status == "all":
        sql = """
            SELECT efta_id FROM documents
            WHERE dataset = %s
            ORDER BY efta_id
            LIMIT %s OFFSET %s
        """
        params = (dataset, limit, offset)
    else:
        sql = """
            SELECT d.efta_id
            FROM documents d
            LEFT JOIN chunks c ON c.efta_id = d.efta_id
            WHERE d.dataset = %s
            GROUP BY d.efta_id
            HAVING COUNT(c.efta_id) = 0
            ORDER BY d.efta_id
            LIMIT %s OFFSET %s
        """
        params = (dataset, limit, offset)

    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            ids = [row["efta_id"] for row in cur.fetchall()]

    return PendingResponse(efta_ids=ids, count=len(ids))


@router.post("/ingest/documents/fetch")
def ingest_documents_fetch(req: DocumentFetchRequest) -> list[DocumentFetchRecord]:
    """Return full document records by efta_id list (max 50)."""
    pool = get_ingest_pool()

    sql = """
        SELECT efta_id, dataset, url, pages, word_count, text, version
        FROM documents
        WHERE efta_id = ANY(%s)
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (req.efta_ids,))
            rows = cur.fetchall()

    return [DocumentFetchRecord(**row) for row in rows]


@router.post("/ingest/chunks")
def ingest_chunks(req: ChunksRequest) -> ChunksResponse:
    """Accept pre-embedded chunks without document metadata.

    Same chunk insertion logic as the old POST /ingest — validates embeddings,
    casts to halfvec, ON CONFLICT DO NOTHING (or deletes+reinserts if overwrite=True).
    """
    for i, chunk in enumerate(req.chunks):
        if chunk.embedding is not None and len(chunk.embedding) != EMBEDDING_DIM:
            raise HTTPException(
                status_code=422,
                detail=f"Chunk {i} ({chunk.efta_id}:{chunk.chunk_index}) has {len(chunk.embedding)}-dim embedding, expected {EMBEDDING_DIM}",
            )

    pool = get_ingest_pool()

    DB_BATCH = 500

    with pool.connection() as conn:
        with conn.cursor() as cur:
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
                    vec_str = None
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

    logger.info(f"Ingested {len(req.chunks)} chunks (overwrite={req.overwrite})")
    return ChunksResponse(inserted_chunks=len(req.chunks))


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
