"""Self-contained GPU ingestion worker.

Downloads JSONL, chunks docs, embeds on local GPU, and POSTs results
to the remote /ingest API. No database credentials needed.

Usage:
    python -m client.ingest_remote
    python -m client.ingest_remote --check   # verify and fix existing data first

Environment variables:
    API_URL      - Base URL of the vector API (default: https://vector.korroni.cloud)
    API_KEY      - API key for authentication
    DATASETS     - Comma-separated dataset numbers (default: 1,2,3,...,12)
    CUDA_DEVICES - Comma-separated GPU indices (default: 0). Use "cpu" for CPU.
    DATA_DIR     - Directory for downloaded JSONL files (default: data)
"""

from __future__ import annotations

import argparse
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


def get_chunk_status(efta_ids: list[str], session: requests.Session) -> dict[str, dict]:
    """Fetch chunk status for a batch of efta_ids (max 50)."""
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-Key"] = API_KEY
    resp = session.post(
        f"{API_URL}/ingest/chunk_status",
        json={"efta_ids": efta_ids},
        headers=headers, timeout=60,
    )
    resp.raise_for_status()
    return {item["efta_id"]: item for item in resp.json()}


def post_batch(
    documents: list[dict],
    embeddable: list[Chunk],
    embeddings: list[list[float]],
    sentinels: list[Chunk],
    session: requests.Session,
    overwrite: bool = False,
) -> dict:
    """POST a batch of documents + chunks with embeddings to the API."""
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-Key"] = API_KEY

    chunk_payloads = []
    for c, emb in zip(embeddable, embeddings):
        chunk_payloads.append({
            "efta_id": c.efta_id,
            "chunk_index": c.chunk_index,
            "total_chunks": c.total_chunks,
            "text": c.text,
            "embedding": emb,
            "version": c.version,
        })
    for c in sentinels:
        chunk_payloads.append({
            "efta_id": c.efta_id,
            "chunk_index": c.chunk_index,
            "total_chunks": c.total_chunks,
            "text": c.text,
            "embedding": None,
            "version": c.version,
        })

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
        "chunks": chunk_payloads,
        "overwrite": overwrite,
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


def _process_and_post(
    batch_docs: list[dict],
    all_chunks: list[Chunk],
    models: list[BGEModel],
    session: requests.Session,
    overwrite: bool = False,
    label: str = "",
) -> int:
    """Split chunks into embeddable/sentinels, embed, and POST. Returns chunk count."""
    embeddable = [c for c in all_chunks if not c.skip_embedding]
    sentinels = [c for c in all_chunks if c.skip_embedding]

    # Embed embeddable chunks on GPU
    embeddings = embed_chunks(models, embeddable) if embeddable else []

    # POST to API in sub-batches to avoid overwhelming the server
    MAX_CHUNKS_PER_POST = 500
    total_chunks = len(all_chunks)

    if total_chunks <= MAX_CHUNKS_PER_POST:
        result = post_batch(batch_docs, embeddable, embeddings, sentinels, session, overwrite=overwrite)
        logger.info(
            f"{label}: done — "
            f"{result.get('inserted_documents', 0)} docs, {result.get('inserted_chunks', 0)} chunks "
            f"({len(sentinels)} sentinels)"
        )
    else:
        # Split into smaller uploads; need to split embeddable and sentinels separately
        inserted_docs = 0
        inserted_chunks = 0
        # Combine for sub-batching: embeddable first (paired with embeddings), then sentinels
        emb_idx = 0
        sent_idx = 0
        sub_num = 0
        sub_total = (total_chunks + MAX_CHUNKS_PER_POST - 1) // MAX_CHUNKS_PER_POST

        remaining_embeddable = list(embeddable)
        remaining_embeddings = list(embeddings)
        remaining_sentinels = list(sentinels)

        while remaining_embeddable or remaining_sentinels:
            sub_num += 1
            budget = MAX_CHUNKS_PER_POST
            # Take from embeddable first
            sub_emb = remaining_embeddable[:budget]
            sub_emb_vecs = remaining_embeddings[:budget]
            remaining_embeddable = remaining_embeddable[budget:]
            remaining_embeddings = remaining_embeddings[budget:]
            budget -= len(sub_emb)
            # Fill rest with sentinels
            sub_sent = remaining_sentinels[:budget]
            remaining_sentinels = remaining_sentinels[budget:]

            sub_docs = batch_docs if sub_num == 1 else []
            logger.info(f"{label}: sub-batch {sub_num}/{sub_total} ({len(sub_emb) + len(sub_sent)} chunks)...")
            result = post_batch(sub_docs, sub_emb, sub_emb_vecs, sub_sent, session, overwrite=overwrite)
            inserted_docs += result.get('inserted_documents', 0)
            inserted_chunks += result.get('inserted_chunks', 0)

        logger.info(f"{label}: done — {inserted_docs} docs, {inserted_chunks} chunks ({len(sentinels)} sentinels)")

    return total_chunks


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

    initial_new = len(new_docs)

    # Process in batches, re-checking done list periodically
    total_chunks = 0
    batch_num = 0
    while new_docs:
        batch_docs = new_docs[:BATCH_SIZE]
        new_docs = new_docs[BATCH_SIZE:]
        batch_num += 1
        remaining = len(new_docs)

        # Chunk all docs in this batch
        all_chunks: list[Chunk] = []
        for doc in batch_docs:
            doc_chunks = chunk_document(doc)
            all_chunks.extend(doc_chunks)

        # Embed and POST
        logger.info(f"Batch {batch_num}: processing {len(all_chunks)} chunks from {len(batch_docs)} docs ({remaining} remaining)...")
        total_chunks += _process_and_post(
            batch_docs, all_chunks, models, session,
            label=f"Batch {batch_num}",
        )

        # Re-check done list every 10 batches to skip work other workers completed
        if batch_num % 10 == 0 and new_docs:
            done_ids = get_done_ids(dataset, session)
            before = len(new_docs)
            new_docs = [d for d in new_docs if (d.get("efta_id") or d.get("efta")) not in done_ids]
            skipped = before - len(new_docs)
            if skipped:
                logger.info(f"Refreshed done list: skipped {skipped} docs completed by other workers")

    logger.info(f"Dataset {dataset} complete: {total_chunks} chunks embedded")
    return {"dataset": dataset, "total": len(docs), "new": initial_new, "chunks": total_chunks}


def check_dataset(
    dataset: int,
    models: list[BGEModel],
    session: requests.Session,
) -> None:
    """Verify and fix existing data for a dataset.

    Iterates through ALL docs in the JSONL, comparing expected vs actual state.
    Fixes any discrepancies, then returns so normal ingestion can run after.
    """
    logger.info(f"=== Check Dataset {dataset} ===")

    jsonl_path = download_jsonl(dataset, DATA_DIR)
    docs = load_jsonl(jsonl_path)
    logger.info(f"Loaded {len(docs)} documents for checking")

    # Counters
    stats = {
        "ok": 0,
        "missing_doc": 0,
        "missing_chunks": 0,
        "wrong_chunk_count": 0,
        "missing_embeddings": 0,
        "stale_metadata": 0,
        "version_mismatch": 0,
    }

    to_fix: list[tuple[dict, list[Chunk], str]] = []  # (doc_dict, expected_chunks, reason)

    # Check in batches of 50
    for batch_start in range(0, len(docs), BATCH_SIZE):
        batch_docs = docs[batch_start : batch_start + BATCH_SIZE]
        efta_ids = [(d.get("efta_id") or d.get("efta", "")) for d in batch_docs]

        # Get expected chunks for each doc
        expected: dict[str, list[Chunk]] = {}
        for doc in batch_docs:
            eid = doc.get("efta_id") or doc.get("efta", "")
            expected[eid] = chunk_document(doc)

        # Get actual status from API
        try:
            actual = get_chunk_status(efta_ids, session)
        except requests.RequestException as e:
            logger.warning(f"Failed to get chunk status for batch at {batch_start}: {e}")
            continue

        # Compare
        for doc in batch_docs:
            eid = doc.get("efta_id") or doc.get("efta", "")
            exp_chunks = expected[eid]
            exp_chunk_count = len(exp_chunks)
            exp_embedded = sum(1 for c in exp_chunks if not c.skip_embedding)
            act = actual.get(eid, {})

            if not act or not act.get("has_doc", False):
                stats["missing_doc"] += 1
                to_fix.append((doc, exp_chunks, "missing_doc"))
            elif act.get("chunk_count", 0) == 0:
                stats["missing_chunks"] += 1
                to_fix.append((doc, exp_chunks, "missing_chunks"))
            elif act.get("chunk_count", 0) != exp_chunk_count:
                stats["wrong_chunk_count"] += 1
                to_fix.append((doc, exp_chunks, "wrong_chunk_count"))
            elif act.get("embedded_count", 0) < exp_embedded:
                stats["missing_embeddings"] += 1
                to_fix.append((doc, exp_chunks, "missing_embeddings"))
            elif act.get("doc_word_count", 0) != doc.get("word_count", 0):
                stats["stale_metadata"] += 1
                to_fix.append((doc, exp_chunks, "stale_metadata"))
            elif act.get("doc_version", 0) < 1 or act.get("chunk_version", 0) < 1:
                stats["version_mismatch"] += 1
                to_fix.append((doc, exp_chunks, "version_mismatch"))
            else:
                stats["ok"] += 1

        if (batch_start // BATCH_SIZE) % 100 == 0:
            checked = batch_start + len(batch_docs)
            logger.info(f"Checked {checked}/{len(docs)} docs ({len(to_fix)} to fix)")

    logger.info(f"Dataset {dataset}: {len(docs)} docs checked")
    for reason, count in stats.items():
        logger.info(f"  {reason:25s}: {count:>8,}")

    if not to_fix:
        logger.info("No fixes needed")
        return

    logger.info(f"Fixing {len(to_fix)} docs...")

    # Process fixes in batches
    fixed = 0
    for fix_start in range(0, len(to_fix), BATCH_SIZE):
        fix_batch = to_fix[fix_start : fix_start + BATCH_SIZE]
        batch_docs_dicts = [item[0] for item in fix_batch]
        all_chunks: list[Chunk] = []
        for _, chunks, _ in fix_batch:
            all_chunks.extend(chunks)

        _process_and_post(
            batch_docs_dicts, all_chunks, models, session,
            overwrite=True,
            label=f"Fix batch {fix_start // BATCH_SIZE + 1}",
        )
        fixed += len(fix_batch)
        logger.info(f"Fixed {fixed}/{len(to_fix)} docs")

    logger.info(f"Dataset {dataset} check complete: fixed {len(to_fix)} docs")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(description="GPU ingestion worker")
    parser.add_argument("--check", action="store_true",
                        help="Verify and fix existing data before ingestion")
    args = parser.parse_args()

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
    if args.check:
        logger.info("Check mode: will verify and fix existing data first")

    # Load models
    models = load_models(devices)

    # Create HTTP session
    session = requests.Session()

    # Check mode: verify and fix existing data
    if args.check:
        for ds in datasets:
            check_dataset(ds, models, session)

    # Normal ingestion always runs after
    results = []
    for ds in datasets:
        result = ingest_dataset(ds, models, session)
        results.append(result)

    logger.info("=== Done ===")
    for r in results:
        logger.info(f"  Dataset {r['dataset']}: {r['total']} total, {r['new']} new, {r['chunks']} chunks")


if __name__ == "__main__":
    main()
