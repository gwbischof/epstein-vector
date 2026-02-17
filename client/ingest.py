"""Orchestrator: download → chunk → embed → insert into Postgres.

Usage:
    python -m client.ingest                     # All datasets
    python -m client.ingest --datasets 1 2 3    # Specific datasets
    python -m client.ingest --db-url postgresql://user:pass@host:5432/db
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import psycopg
import psycopg.sql
from tqdm import tqdm

from client.chunk import Chunk, chunk_documents
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
    text TEXT
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
        conn.execute(
            "SELECT 1 FROM pg_roles WHERE rolname = 'epstein_reader'"
        )
        if conn.fetchone() is None:
            conn.execute(
                psycopg.sql.SQL("CREATE ROLE epstein_reader LOGIN PASSWORD {}").format(
                    psycopg.sql.Literal(reader_password)
                )
            )
        conn.execute(READER_GRANTS_SQL)
    conn.commit()
    logger.info("Schema ready")


def insert_documents(conn: psycopg.Connection, docs: list[dict]) -> int:
    """Insert document metadata, skipping duplicates."""
    inserted = 0
    with conn.cursor() as cur:
        for doc in docs:
            cur.execute(
                """
                INSERT INTO documents (efta_id, dataset, url, pages, word_count, text)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (efta_id) DO NOTHING
                """,
                (
                    doc.get("efta_id"),
                    doc.get("dataset"),
                    doc.get("url", ""),
                    doc.get("pages", 0),
                    doc.get("word_count", 0),
                    doc.get("text", ""),
                ),
            )
            inserted += cur.rowcount
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


def ingest_dataset(
    dataset: int,
    conn: psycopg.Connection,
    embed_client: EmbeddingClient,
    data_dir: Path,
    done_ids: set[str],
) -> dict:
    """Full pipeline for a single dataset."""
    logger.info(f"=== Dataset {dataset} ===")

    # Download
    jsonl_path = download_jsonl(dataset, data_dir)
    docs = load_jsonl(jsonl_path)
    logger.info(f"Loaded {len(docs)} documents")

    # Insert document metadata
    doc_count = insert_documents(conn, docs)
    logger.info(f"Inserted {doc_count} new documents")

    # Filter out already-embedded docs
    new_docs = [d for d in docs if d.get("efta_id") not in done_ids]
    logger.info(f"Skipping {len(docs) - len(new_docs)} already-embedded docs, {len(new_docs)} remaining")

    # Chunk
    chunks = chunk_documents(new_docs)
    if not chunks:
        logger.info("No chunks to embed")
        return {"dataset": dataset, "docs": len(docs), "new": len(new_docs), "chunks": 0}

    # Embed
    texts = [c.text for c in chunks]
    embeddings = embed_client.embed_all(texts, batch_size=BATCH_SIZE)

    # Insert
    chunk_count = insert_chunks(conn, chunks, embeddings)
    logger.info(f"Inserted {chunk_count} chunks")

    # Track newly inserted ids
    for d in new_docs:
        done_ids.add(d.get("efta_id"))

    return {"dataset": dataset, "docs": len(docs), "new": len(new_docs), "chunks": chunk_count}


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
            result = ingest_dataset(ds, conn, embed_client, args.data_dir, done_ids)
            results.append(result)
            logger.info(f"Dataset {ds}: {result}")

    logger.info("=== Done ===")
    for r in results:
        logger.info(f"  Dataset {r['dataset']}: {r['docs']} docs, {r['chunks']} chunks")


if __name__ == "__main__":
    main()
