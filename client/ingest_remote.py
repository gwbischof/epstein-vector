"""Self-contained GPU ingestion worker.

Downloads JSONL, chunks docs, embeds on local GPU, and POSTs results
to the remote /ingest API. No database credentials needed.

Usage:
    python -m client.ingest_remote

Environment variables:
    API_URL      - Base URL of the vector API (default: https://vector.korroni.cloud)
    API_KEY      - API key for authentication
    DATASETS     - Comma-separated dataset numbers (default: 1,2,3,...,12)
    CUDA_DEVICES - Comma-separated GPU indices (default: 0). Use "cpu" for CPU.
    DATA_DIR     - Directory for downloaded JSONL files (default: data)
"""

from __future__ import annotations

import logging
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

from client.chunk import Chunk, chunk_document
from client.download import download_jsonl, load_jsonl
from server.models.bge import BGEModel

logger = logging.getLogger(__name__)

# Config from env
API_URL = os.environ.get("API_URL", "https://vector.korroni.cloud").rstrip("/")
API_KEY = os.environ.get("API_KEY", "")
DATASETS = os.environ.get("DATASETS", "1,2,3,4,5,6,7,8,9,10,11,12")
CUDA_DEVICES = os.environ.get("CUDA_DEVICES", "0")
DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
BATCH_SIZE = 50


def get_done_ids(dataset: int, session: requests.Session) -> set[str]:
    """Fetch set of already-embedded efta_ids for a dataset."""
    url = f"{API_URL}/ingest/done"
    headers = {}
    if API_KEY:
        headers["X-API-Key"] = API_KEY

    for attempt in range(3):
        try:
            resp = session.get(url, params={"dataset": dataset}, headers=headers, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            ids = set(data.get("efta_ids", []))
            logger.info(f"Dataset {dataset}: {len(ids)} docs already done")
            return ids
        except requests.RequestException as e:
            if attempt < 2:
                wait = 2 ** attempt
                logger.warning(f"Failed to fetch done IDs (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def post_batch(
    documents: list[dict],
    chunks: list[Chunk],
    embeddings: list[list[float]],
    session: requests.Session,
) -> dict:
    """POST a batch of documents + chunks with embeddings to the API."""
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-Key"] = API_KEY

    payload = {
        "documents": [
            {
                "efta_id": d.get("efta_id") or d.get("efta", ""),
                "dataset": int(d.get("dataset", 0)),
                "url": d.get("url", ""),
                "pages": d.get("pages", 0),
                "word_count": d.get("word_count", 0),
                "text": d.get("text", ""),
            }
            for d in documents
        ],
        "chunks": [
            {
                "efta_id": c.efta_id,
                "chunk_index": c.chunk_index,
                "total_chunks": c.total_chunks,
                "text": c.text,
                "embedding": emb,
            }
            for c, emb in zip(chunks, embeddings)
        ],
    }

    for attempt in range(3):
        try:
            resp = session.post(f"{API_URL}/ingest", json=payload, headers=headers, timeout=300)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < 2:
                wait = 2 ** (attempt + 1)
                logger.warning(f"POST failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def load_models(devices: list[str]) -> list[BGEModel]:
    """Load BGE model on each specified device."""
    models = []
    for device in devices:
        m = BGEModel()
        m.load(device)
        models.append(m)
    return models


def embed_chunks(models: list[BGEModel], chunks: list[Chunk]) -> list[list[float]]:
    """Embed chunk texts using available models. Multi-GPU interleave if multiple."""
    texts = [c.text for c in chunks]

    if len(models) == 1:
        return models[0].encode(texts)

    # Multi-GPU: split texts across models, run in parallel
    n = len(models)
    splits = [texts[i::n] for i in range(n)]

    with ThreadPoolExecutor(max_workers=n) as executor:
        futures = [executor.submit(m.encode, s) for m, s in zip(models, splits)]
        results = [f.result() for f in futures]

    # Interleave back to original order
    embeddings: list[list[float]] = [[] for _ in texts]
    for gpu_idx, gpu_results in enumerate(results):
        for j, emb in enumerate(gpu_results):
            original_idx = j * n + gpu_idx
            embeddings[original_idx] = emb

    return embeddings


def ingest_dataset(
    dataset: int,
    models: list[BGEModel],
    session: requests.Session,
) -> dict:
    """Full pipeline for a single dataset."""
    logger.info(f"=== Dataset {dataset} ===")

    # Download JSONL
    jsonl_path = download_jsonl(dataset, DATA_DIR)
    docs = load_jsonl(jsonl_path)
    logger.info(f"Loaded {len(docs)} documents")

    # Check what's already done
    done_ids = get_done_ids(dataset, session)

    # Filter to new docs and shuffle so concurrent workers diverge
    new_docs = [d for d in docs if (d.get("efta_id") or d.get("efta")) not in done_ids]
    random.shuffle(new_docs)
    logger.info(f"{len(new_docs)} docs to process ({len(docs) - len(new_docs)} already done)")

    if not new_docs:
        return {"dataset": dataset, "total": len(docs), "new": 0, "chunks": 0}

    # Process in batches
    total_chunks = 0
    for batch_start in range(0, len(new_docs), BATCH_SIZE):
        batch_docs = new_docs[batch_start : batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(new_docs) + BATCH_SIZE - 1) // BATCH_SIZE

        # Chunk all docs in this batch
        all_chunks: list[Chunk] = []
        chunkable_docs: list[dict] = []
        for doc in batch_docs:
            doc_chunks = chunk_document(doc)
            if doc_chunks:
                all_chunks.extend(doc_chunks)
                chunkable_docs.append(doc)

        if not all_chunks:
            logger.info(f"Batch {batch_num}/{total_batches}: no chunkable docs, skipping")
            continue

        # Embed all chunks on local GPU(s)
        logger.info(f"Batch {batch_num}/{total_batches}: embedding {len(all_chunks)} chunks from {len(chunkable_docs)} docs...")
        embeddings = embed_chunks(models, all_chunks)

        # POST to API — send all docs in batch (including unchunkable) for document row insertion
        logger.info(f"Batch {batch_num}/{total_batches}: uploading...")
        result = post_batch(batch_docs, all_chunks, embeddings, session)
        total_chunks += len(all_chunks)
        logger.info(
            f"Batch {batch_num}/{total_batches}: done — "
            f"{result.get('inserted_documents', 0)} docs, {result.get('inserted_chunks', 0)} chunks"
        )

    logger.info(f"Dataset {dataset} complete: {len(new_docs)} new docs, {total_chunks} chunks")
    return {"dataset": dataset, "total": len(docs), "new": len(new_docs), "chunks": total_chunks}


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    # Parse config
    datasets = [int(x.strip()) for x in DATASETS.split(",") if x.strip()]
    if CUDA_DEVICES.lower() == "cpu":
        devices = ["cpu"]
    else:
        devices = [f"cuda:{x.strip()}" for x in CUDA_DEVICES.split(",") if x.strip()]

    logger.info(f"API: {API_URL}")
    logger.info(f"Datasets: {datasets}")
    logger.info(f"Devices: {devices}")
    logger.info(f"Batch size: {BATCH_SIZE}")

    # Load models
    models = load_models(devices)

    # Create HTTP session
    session = requests.Session()

    # Process each dataset
    results = []
    for ds in datasets:
        result = ingest_dataset(ds, models, session)
        results.append(result)

    logger.info("=== Done ===")
    for r in results:
        logger.info(f"  Dataset {r['dataset']}: {r['total']} total, {r['new']} new, {r['chunks']} chunks")


if __name__ == "__main__":
    main()
