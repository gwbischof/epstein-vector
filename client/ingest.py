"""Orchestrator: download → chunk → embed → insert into Postgres.

Usage:
    python -m client.ingest                     # All datasets
    python -m client.ingest --datasets 1 2 3    # Specific datasets
    python -m client.ingest --db-url postgresql://user:pass@host:5432/db
"""

from __future__ import annotations

import argparse
import logging
import threading
from pathlib import Path
from queue import Queue

import psycopg
import psycopg.sql
from tqdm import tqdm

from client.chunk import Chunk, chunk_document, chunk_documents
from client.download import download_jsonl, load_jsonl
from client.embed import BATCH_SIZE, EmbeddingClient

logger = logging.getLogger(__name__)

DEFAULT_DB_URL = "postgresql://epstein:epstein@localhost:5432/epstein"
EMBED_SERVER = "http://localhost:8200"

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS documents (
    efta_id TEXT PRIMARY KEY,
    dataset INT,
    url TEXT,
    pages INT,
    word_count INT,
    text TEXT,
    tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(text, ''))) STORED
);

CREATE TABLE IF NOT EXISTS chunks (
    id SERIAL PRIMARY KEY,
    efta_id TEXT REFERENCES documents(efta_id),
    chunk_index INT,
    total_chunks INT,
    text TEXT,
    embedding halfvec(1024),
    UNIQUE (efta_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS chunks_embedding_idx ON chunks
    USING hnsw (embedding halfvec_cosine_ops);

CREATE INDEX IF NOT EXISTS chunks_efta_idx ON chunks (efta_id);

CREATE INDEX IF NOT EXISTS documents_tsv_idx ON documents USING gin (tsv);

CREATE INDEX IF NOT EXISTS idx_chunks_trgm ON chunks USING gin (text gin_trgm_ops);

"""

READER_GRANTS_SQL = """
GRANT CONNECT ON DATABASE epstein TO epstein_reader;
GRANT USAGE ON SCHEMA public TO epstein_reader;
GRANT SELECT ON documents, chunks TO epstein_reader;
"""


def ensure_schema(conn: psycopg.Connection, reader_password: str | None = None) -> None:
    """Create tables, indexes, and read-only user if they don't exist."""
    conn.execute(SCHEMA_SQL)
    if reader_password:
        # CREATE ROLE can't use query params, but password is operator-supplied (not user input)
        cur = conn.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = 'epstein_reader'"
        )
        if cur.fetchone() is None:
            conn.execute(
                psycopg.sql.SQL("CREATE ROLE epstein_reader LOGIN PASSWORD {}").format(
                    psycopg.sql.Literal(reader_password)
                )
            )
        conn.execute(READER_GRANTS_SQL)
    conn.commit()
    logger.info("Schema ready")


MAX_TSV_BYTES = 1_000_000  # PostgreSQL tsvector limit is 1048575 bytes


def insert_documents(conn: psycopg.Connection, docs: list[dict], batch_size: int = 500) -> int:
    """Insert document metadata in batches, skipping duplicates."""
    inserted = 0
    with conn.cursor() as cur:
        for i in tqdm(range(0, len(docs), batch_size), desc="Inserting docs", unit="batch"):
            batch = docs[i : i + batch_size]
            values = []
            for doc in batch:
                text = doc.get("text", "")
                if len(text.encode("utf-8")) > MAX_TSV_BYTES:
                    text = text[:MAX_TSV_BYTES]
                values.append((
                    doc.get("efta_id") or doc.get("efta"),
                    int(doc.get("dataset", 0)),
                    doc.get("url", ""),
                    doc.get("pages", 0),
                    doc.get("word_count", 0),
                    text,
                ))
            cur.executemany(
                """
                INSERT INTO documents (efta_id, dataset, url, pages, word_count, text)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (efta_id) DO NOTHING
                """,
                values,
            )
            inserted += len(values)
            conn.commit()
    return inserted


def insert_chunks(
    conn: psycopg.Connection,
    chunks: list[Chunk],
    embeddings: list[list[float]],
    batch_size: int = 500,
) -> int:
    """Insert chunks with embeddings into Postgres."""
    inserted = 0
    with conn.cursor() as cur:
        for i in tqdm(range(0, len(chunks), batch_size), desc="Inserting"):
            batch_chunks = chunks[i : i + batch_size]
            batch_embeds = embeddings[i : i + batch_size]
            values = []
            for chunk, emb in zip(batch_chunks, batch_embeds):
                vec_str = "[" + ",".join(str(v) for v in emb) + "]"
                values.append((
                    chunk.efta_id,
                    chunk.chunk_index,
                    chunk.total_chunks,
                    chunk.text,
                    vec_str,
                ))
            cur.executemany(
                """
                INSERT INTO chunks (efta_id, chunk_index, total_chunks, text, embedding)
                VALUES (%s, %s, %s, %s, %s::halfvec)
                ON CONFLICT (efta_id, chunk_index) DO NOTHING
                """,
                values,
            )
            inserted += len(values)
        conn.commit()
    return inserted


def get_embedded_efta_ids(conn: psycopg.Connection) -> set[str]:
    """Get set of efta_ids that already have chunks in the database."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT efta_id FROM chunks")
        return {row[0] for row in cur.fetchall()}


def _embed_worker(
    chunk_queue: Queue,
    db_url: str,
    embed_client: EmbeddingClient,
    result: dict,
):
    """Worker thread: pull chunk batches from queue, embed, insert into DB."""
    try:
        with psycopg.connect(db_url) as conn:
            while True:
                item = chunk_queue.get()
                if item is None:  # poison pill
                    break
                chunks: list[Chunk] = item
                texts = [c.text for c in chunks]
                embeddings = embed_client.embed_batch(texts)
                count = insert_chunks(conn, chunks, embeddings)
                result["chunks"] += count
                logger.info(f"Embedded + inserted {len(chunks)} chunks (total: {result['chunks']})")
    except Exception as e:
        result["error"] = e
        logger.error(f"Embed worker failed: {e}")


def _insert_one_doc(conn: psycopg.Connection, doc: dict) -> None:
    """Insert a single document row (needed before its chunks can be inserted)."""
    text = doc.get("text", "")
    if len(text.encode("utf-8")) > MAX_TSV_BYTES:
        text = text[:MAX_TSV_BYTES]
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO documents (efta_id, dataset, url, pages, word_count, text)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (efta_id) DO NOTHING
            """,
            (
                doc.get("efta_id") or doc.get("efta"),
                int(doc.get("dataset", 0)),
                doc.get("url", ""),
                doc.get("pages", 0),
                doc.get("word_count", 0),
                text,
            ),
        )
    conn.commit()


def ingest_dataset(
    dataset: int,
    db_url: str,
    conn: psycopg.Connection,
    embed_client: EmbeddingClient,
    data_dir: Path,
    done_ids: set[str],
) -> dict:
    """Full pipeline for a single dataset.

    For each doc: insert row → chunk → accumulate. Once a batch of chunks
    reaches BATCH_SIZE, a worker thread embeds and inserts them while the
    main thread continues inserting + chunking the next docs.
    """
    logger.info(f"=== Dataset {dataset} ===")

    # Download
    jsonl_path = download_jsonl(dataset, data_dir)
    docs = load_jsonl(jsonl_path)
    logger.info(f"Loaded {len(docs)} documents")

    # Filter to docs needing embedding
    new_docs = [d for d in docs if (d.get("efta_id") or d.get("efta")) not in done_ids]
    logger.info(f"{len(new_docs)} docs to embed ({len(docs) - len(new_docs)} already done)")

    if not new_docs:
        return {"dataset": dataset, "docs": len(docs), "new": 0, "chunks": 0}

    # Pipeline: main thread inserts+chunks, worker thread embeds+inserts chunks
    chunk_queue: Queue = Queue(maxsize=4)
    result: dict = {"chunks": 0}

    worker = threading.Thread(
        target=_embed_worker,
        args=(chunk_queue, db_url, embed_client, result),
        daemon=True,
    )
    worker.start()

    batch: list[Chunk] = []
    skipped = 0
    for i, doc in enumerate(new_docs):
        # Check if worker died
        if "error" in result:
            raise RuntimeError(f"Embed worker failed: {result['error']}") from result["error"]

        # Insert this doc's row so FK constraint is satisfied
        _insert_one_doc(conn, doc)

        # Chunk immediately
        doc_chunks = chunk_document(doc)
        if not doc_chunks:
            skipped += 1
            continue

        batch.extend(doc_chunks)

        # When batch is full, send to embed worker
        while len(batch) >= BATCH_SIZE:
            chunk_queue.put(batch[:BATCH_SIZE])
            batch = batch[BATCH_SIZE:]

        if (i + 1) % 1000 == 0:
            logger.info(f"Inserted + chunked {i + 1}/{len(new_docs)} docs")

    # Send remaining chunks
    if batch:
        chunk_queue.put(batch)

    # Signal worker to stop and wait
    chunk_queue.put(None)
    worker.join()

    if "error" in result:
        raise RuntimeError(f"Embed worker failed: {result['error']}") from result["error"]

    logger.info(f"Skipped {skipped} docs (no extractable text)")

    # Track completed ids
    for d in new_docs:
        done_ids.add(d.get("efta_id") or d.get("efta"))

    logger.info(f"Dataset {dataset} complete: {len(new_docs)} new docs, {result['chunks']} chunks")
    return {"dataset": dataset, "docs": len(docs), "new": len(new_docs), "chunks": result["chunks"]}


def main():
    parser = argparse.ArgumentParser(description="Ingest Epstein documents into pgvector")
    parser.add_argument("--datasets", type=int, nargs="+", default=list(range(1, 13)))
    parser.add_argument("--db-url", default=DEFAULT_DB_URL)
    parser.add_argument("--embed-server", default=EMBED_SERVER)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--reader-password", default=None, help="Password for read-only epstein_reader role")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    embed_client = EmbeddingClient(args.embed_server)
    logger.info(f"Embed server health: {embed_client.health()}")

    with psycopg.connect(args.db_url) as conn:
        ensure_schema(conn, reader_password=args.reader_password)
        done_ids = get_embedded_efta_ids(conn)
        logger.info(f"Found {len(done_ids)} already-embedded documents")
        results = []
        for ds in args.datasets:
            result = ingest_dataset(ds, args.db_url, conn, embed_client, args.data_dir, done_ids)
            results.append(result)
            logger.info(f"Dataset {ds}: {result}")

    logger.info("=== Done ===")
    for r in results:
        logger.info(f"  Dataset {r['dataset']}: {r['docs']} docs, {r['chunks']} chunks")


if __name__ == "__main__":
    main()
