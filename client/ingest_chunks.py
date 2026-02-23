"""GPU chunk worker: fetches docs from API, chunks, embeds, uploads chunks.

Reads documents from the database (via API) instead of JSONL files.
No download step needed — doc rows must already exist (loaded by ingest_docs.py).

Usage:
    python -m client.ingest_chunks
    python -m client.ingest_chunks --check         # verify and fix existing data first
    python -m client.ingest_chunks --super-check              # compare text_hash, stamp or re-chunk
    python -m client.ingest_chunks --super-check --dry-run   # report what would change

Environment variables:
    API_URL      - Base URL of the vector API (default: https://vector.korroni.cloud)
    API_KEY      - API key for authentication
    DATASETS     - Comma-separated dataset numbers (default: 1,2,3,...,12)
    CUDA_DEVICES - Comma-separated GPU indices (default: 0). Use "cpu" for CPU.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from client.chunk import Chunk, chunk_document
from server.models.bge import BGEModel

logger = logging.getLogger(__name__)

# Config from env
API_URL = os.environ.get("API_URL", "https://vector.korroni.cloud").rstrip("/")
API_KEY = os.environ.get("API_KEY", "")
DATASETS = os.environ.get("DATASETS", "1,2,3,4,5,6,7,8,9,10,11,12")
CUDA_DEVICES = os.environ.get("CUDA_DEVICES", "0")
BATCH_SIZE = 50


def _headers() -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if API_KEY:
        headers["X-API-Key"] = API_KEY
    return headers


def get_pending_ids(dataset: int, session: requests.Session) -> list[str]:
    """Fetch efta_ids that need chunking (docs with zero chunks)."""
    all_ids: list[str] = []
    offset = 0
    limit = 10000

    while True:
        for attempt in range(3):
            try:
                resp = session.get(
                    f"{API_URL}/ingest/documents/pending",
                    params={"dataset": dataset, "status": "pending", "limit": limit, "offset": offset},
                    headers=_headers(),
                    timeout=120,
                )
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.RequestException as e:
                if attempt < 2:
                    wait = 2 ** attempt
                    logger.warning(f"Failed to fetch pending IDs (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise

        batch_ids = data.get("efta_ids", [])
        all_ids.extend(batch_ids)

        if len(batch_ids) < limit:
            break
        offset += limit

    logger.info(f"Dataset {dataset}: {len(all_ids)} docs pending")
    return all_ids


def get_all_ids(dataset: int, session: requests.Session) -> list[str]:
    """Fetch all efta_ids for a dataset (for --check mode)."""
    all_ids: list[str] = []
    offset = 0
    limit = 10000

    while True:
        resp = session.get(
            f"{API_URL}/ingest/documents/pending",
            params={"dataset": dataset, "status": "all", "limit": limit, "offset": offset},
            headers=_headers(),
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        batch_ids = data.get("efta_ids", [])
        all_ids.extend(batch_ids)

        if len(batch_ids) < limit:
            break
        offset += limit

    logger.info(f"Dataset {dataset}: {len(all_ids)} total docs")
    return all_ids


def fetch_documents(efta_ids: list[str], session: requests.Session) -> list[dict]:
    """Fetch full doc records from API, reconstruct 'extracted' field."""
    resp = session.post(
        f"{API_URL}/ingest/documents/fetch",
        json={"efta_ids": efta_ids},
        headers=_headers(),
        timeout=60,
    )
    resp.raise_for_status()
    rows = resp.json()

    # Reconstruct the dict format chunk_document() expects, including
    # the 'extracted' field inferred from word_count >= 5
    docs = []
    for row in rows:
        docs.append({
            "efta_id": row["efta_id"],
            "dataset": row["dataset"],
            "url": row.get("url", ""),
            "pages": row.get("pages", 0),
            "word_count": row.get("word_count", 0),
            "text": row.get("text", ""),
            "extracted": row.get("word_count", 0) >= 5,
            "text_hash": row.get("text_hash", ""),
        })
    return docs


def post_chunks(
    embeddable: list[Chunk],
    embeddings: list[list[float]],
    sentinels: list[Chunk],
    session: requests.Session,
    overwrite: bool = False,
    doc_text_hashes: dict[str, str] | None = None,
) -> dict:
    """POST chunks only to /ingest/chunks."""
    hashes = doc_text_hashes or {}
    chunk_payloads = []
    for c, emb in zip(embeddable, embeddings):
        payload = {
            "efta_id": c.efta_id,
            "chunk_index": c.chunk_index,
            "total_chunks": c.total_chunks,
            "text": c.text,
            "embedding": emb,
            "version": c.version,
        }
        if c.efta_id in hashes:
            payload["doc_text_hash"] = hashes[c.efta_id]
        chunk_payloads.append(payload)
    for c in sentinels:
        payload = {
            "efta_id": c.efta_id,
            "chunk_index": c.chunk_index,
            "total_chunks": c.total_chunks,
            "text": c.text,
            "embedding": None,
            "version": c.version,
        }
        if c.efta_id in hashes:
            payload["doc_text_hash"] = hashes[c.efta_id]
        chunk_payloads.append(payload)

    payload = {"chunks": chunk_payloads, "overwrite": overwrite}

    for attempt in range(3):
        try:
            resp = session.post(
                f"{API_URL}/ingest/chunks",
                json=payload,
                headers=_headers(),
                timeout=300,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < 2:
                wait = 2 ** (attempt + 1)
                logger.warning(f"POST failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def get_chunk_status(efta_ids: list[str], session: requests.Session) -> dict[str, dict]:
    """Fetch chunk status for a batch of efta_ids (max 50)."""
    resp = session.post(
        f"{API_URL}/ingest/chunk_status",
        json={"efta_ids": efta_ids},
        headers=_headers(),
        timeout=60,
    )
    resp.raise_for_status()
    return {item["efta_id"]: item for item in resp.json()}


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
    all_chunks: list[Chunk],
    models: list[BGEModel],
    session: requests.Session,
    overwrite: bool = False,
    label: str = "",
    doc_text_hashes: dict[str, str] | None = None,
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
        result = post_chunks(embeddable, embeddings, sentinels, session, overwrite=overwrite, doc_text_hashes=doc_text_hashes)
        logger.info(
            f"{label}: done — "
            f"{result.get('inserted_chunks', 0)} chunks "
            f"({len(sentinels)} sentinels)"
        )
    else:
        inserted_chunks = 0
        remaining_embeddable = list(embeddable)
        remaining_embeddings = list(embeddings)
        remaining_sentinels = list(sentinels)
        sub_num = 0
        sub_total = (total_chunks + MAX_CHUNKS_PER_POST - 1) // MAX_CHUNKS_PER_POST

        while remaining_embeddable or remaining_sentinels:
            sub_num += 1
            budget = MAX_CHUNKS_PER_POST
            sub_emb = remaining_embeddable[:budget]
            sub_emb_vecs = remaining_embeddings[:budget]
            remaining_embeddable = remaining_embeddable[budget:]
            remaining_embeddings = remaining_embeddings[budget:]
            budget -= len(sub_emb)
            sub_sent = remaining_sentinels[:budget]
            remaining_sentinels = remaining_sentinels[budget:]

            logger.info(f"{label}: sub-batch {sub_num}/{sub_total} ({len(sub_emb) + len(sub_sent)} chunks)...")
            result = post_chunks(sub_emb, sub_emb_vecs, sub_sent, session, overwrite=overwrite, doc_text_hashes=doc_text_hashes)
            inserted_chunks += result.get('inserted_chunks', 0)

        logger.info(f"{label}: done — {inserted_chunks} chunks ({len(sentinels)} sentinels)")

    return total_chunks


def ingest_dataset(
    dataset: int,
    models: list[BGEModel],
    session: requests.Session,
) -> dict:
    """Full pipeline for a single dataset."""
    logger.info(f"=== Dataset {dataset} ===")

    # Get pending doc IDs from the database
    pending_ids = get_pending_ids(dataset, session)

    if not pending_ids:
        logger.info("No pending docs")
        return {"dataset": dataset, "pending": 0, "chunks": 0}

    # Shuffle for multi-worker divergence
    random.shuffle(pending_ids)
    initial_pending = len(pending_ids)
    logger.info(f"{initial_pending} docs to process")

    total_chunks = 0
    batch_num = 0

    while pending_ids:
        batch_ids = pending_ids[:BATCH_SIZE]
        pending_ids = pending_ids[BATCH_SIZE:]
        batch_num += 1
        remaining = len(pending_ids)

        # Fetch full doc records from API
        docs = fetch_documents(batch_ids, session)

        # Build doc_text_hashes map
        doc_text_hashes = {d["efta_id"]: d["text_hash"] for d in docs if d.get("text_hash")}

        # Chunk all docs in this batch
        all_chunks: list[Chunk] = []
        for doc in docs:
            doc_chunks = chunk_document(doc)
            all_chunks.extend(doc_chunks)

        # Embed and POST
        logger.info(f"Batch {batch_num}: processing {len(all_chunks)} chunks from {len(docs)} docs ({remaining} remaining)...")
        total_chunks += _process_and_post(
            all_chunks, models, session,
            label=f"Batch {batch_num}",
            doc_text_hashes=doc_text_hashes,
        )

        # Re-fetch pending list every 10 batches to skip work done by other workers
        if batch_num % 10 == 0 and pending_ids:
            fresh_ids = set(get_pending_ids(dataset, session))
            before = len(pending_ids)
            pending_ids = [eid for eid in pending_ids if eid in fresh_ids]
            skipped = before - len(pending_ids)
            if skipped:
                logger.info(f"Refreshed pending list: skipped {skipped} docs completed by other workers")

    logger.info(f"Dataset {dataset} complete: {total_chunks} chunks embedded")
    return {"dataset": dataset, "pending": initial_pending, "chunks": total_chunks}


def check_dataset(
    dataset: int,
    models: list[BGEModel],
    session: requests.Session,
) -> None:
    """Verify and fix existing data for a dataset.

    Fetches ALL docs from the database, compares expected vs actual chunks.
    """
    logger.info(f"=== Check Dataset {dataset} ===")

    all_ids = get_all_ids(dataset, session)
    logger.info(f"{len(all_ids)} documents to check")

    stats = {
        "ok": 0,
        "missing_chunks": 0,
        "wrong_chunk_count": 0,
        "missing_embeddings": 0,
        "stale_metadata": 0,
        "version_mismatch": 0,
        "hash_mismatch": 0,
    }

    to_fix: list[tuple[dict, list[Chunk], str]] = []

    # Check in batches of 50
    for batch_start in range(0, len(all_ids), BATCH_SIZE):
        batch_ids = all_ids[batch_start : batch_start + BATCH_SIZE]

        # Fetch full docs from API
        docs = fetch_documents(batch_ids, session)
        docs_by_id = {d["efta_id"]: d for d in docs}

        # Get expected chunks for each doc
        expected: dict[str, list[Chunk]] = {}
        for doc in docs:
            expected[doc["efta_id"]] = chunk_document(doc)

        # Get actual status from API
        try:
            actual = get_chunk_status(batch_ids, session)
        except requests.RequestException as e:
            logger.warning(f"Failed to get chunk status for batch at {batch_start}: {e}")
            continue

        # Compare
        for eid in batch_ids:
            doc = docs_by_id.get(eid)
            if doc is None:
                continue

            exp_chunks = expected.get(eid, [])
            exp_chunk_count = len(exp_chunks)
            exp_embedded = sum(1 for c in exp_chunks if not c.skip_embedding)
            act = actual.get(eid, {})

            if act.get("chunk_count", 0) == 0:
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
            elif act.get("chunk_text_hash") and act.get("doc_text_hash") and \
                 act.get("chunk_text_hash") != act.get("doc_text_hash"):
                stats["hash_mismatch"] += 1
                to_fix.append((doc, exp_chunks, "hash_mismatch"))
            else:
                stats["ok"] += 1

        if (batch_start // BATCH_SIZE) % 100 == 0:
            checked = batch_start + len(batch_ids)
            logger.info(f"Checked {checked}/{len(all_ids)} docs ({len(to_fix)} to fix)")

    logger.info(f"Dataset {dataset}: {len(all_ids)} docs checked")
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
        all_chunks: list[Chunk] = []
        batch_hashes: dict[str, str] = {}
        for doc, chunks, _ in fix_batch:
            all_chunks.extend(chunks)
            if doc.get("text_hash"):
                batch_hashes[doc["efta_id"]] = doc["text_hash"]

        _process_and_post(
            all_chunks, models, session,
            overwrite=True,
            label=f"Fix batch {fix_start // BATCH_SIZE + 1}",
            doc_text_hashes=batch_hashes,
        )
        fixed += len(fix_batch)
        logger.info(f"Fixed {fixed}/{len(to_fix)} docs")

    logger.info(f"Dataset {dataset} check complete: fixed {len(to_fix)} docs")


def fetch_chunks(efta_ids: list[str], session: requests.Session) -> dict[str, list[dict]]:
    """Fetch chunk texts grouped by efta_id from the API."""
    resp = session.post(
        f"{API_URL}/ingest/chunks/fetch",
        json={"efta_ids": efta_ids},
        headers=_headers(),
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def stamp_hashes(stamps: list[dict], session: requests.Session) -> dict:
    """Stamp doc_text_hash on existing chunks without re-uploading."""
    resp = session.post(
        f"{API_URL}/ingest/chunks/stamp_hash",
        json={"stamps": stamps},
        headers=_headers(),
        timeout=120,
    )
    resp.raise_for_status()
    return resp.json()


def super_check_dataset(
    dataset: int,
    models: list[BGEModel],
    session: requests.Session,
    dry_run: bool = False,
) -> None:
    """Compare text_hash between documents and chunks, fix mismatches.

    For chunks missing doc_text_hash (NULL), fetches actual chunk texts and
    compares against expected. Stamps matching chunks, re-chunks mismatches.
    """
    mode = "DRY RUN" if dry_run else "LIVE"
    logger.info(f"=== Super-Check Dataset {dataset} ({mode}) ===")

    all_ids = get_all_ids(dataset, session)
    logger.info(f"{len(all_ids)} documents to super-check")

    stats = {
        "already_synced": 0,
        "stamped": 0,
        "rechunked": 0,
        "missing_chunks": 0,
    }

    for batch_start in range(0, len(all_ids), BATCH_SIZE):
        batch_ids = all_ids[batch_start : batch_start + BATCH_SIZE]

        # Fetch docs (with text_hash) and chunk status in parallel
        docs = fetch_documents(batch_ids, session)
        docs_by_id = {d["efta_id"]: d for d in docs}

        try:
            actual = get_chunk_status(batch_ids, session)
        except requests.RequestException as e:
            logger.warning(f"Failed to get chunk status for batch at {batch_start}: {e}")
            continue

        # Categorize docs
        to_stamp: list[dict] = []  # chunks match, just need hash stamped
        to_rechunk: list[tuple[dict, list[Chunk]]] = []  # chunks stale, need re-embed
        to_fetch_and_compare: list[str] = []  # need to compare actual vs expected

        for eid in batch_ids:
            doc = docs_by_id.get(eid)
            if doc is None:
                continue

            act = actual.get(eid, {})
            doc_hash = doc.get("text_hash")
            chunk_hash = act.get("chunk_text_hash")
            chunk_count = act.get("chunk_count", 0)

            if chunk_count == 0:
                # No chunks at all — need full re-chunk
                exp_chunks = chunk_document(doc)
                to_rechunk.append((doc, exp_chunks))
                stats["missing_chunks"] += 1
            elif chunk_hash and doc_hash and chunk_hash == doc_hash:
                # Already synced
                stats["already_synced"] += 1
            elif chunk_hash is None and chunk_count > 0:
                # Chunks exist but no hash — need to compare texts
                to_fetch_and_compare.append(eid)
            elif chunk_hash and doc_hash and chunk_hash != doc_hash:
                # Hash mismatch — chunks are stale
                exp_chunks = chunk_document(doc)
                to_rechunk.append((doc, exp_chunks))
                stats["rechunked"] += 1

        # Fetch actual chunk texts for comparison (batches of 50)
        if to_fetch_and_compare:
            chunk_data = fetch_chunks(to_fetch_and_compare, session)

            for eid in to_fetch_and_compare:
                doc = docs_by_id.get(eid)
                if doc is None:
                    continue

                # Get expected chunks
                exp_chunks = chunk_document(doc)
                exp_texts = [c.text for c in exp_chunks]

                # Get actual chunk texts
                actual_chunks = chunk_data.get(eid, [])
                actual_texts = [c["text"] for c in sorted(actual_chunks, key=lambda x: x["chunk_index"])]

                if exp_texts == actual_texts:
                    # Chunks match — just stamp the hash
                    doc_hash = doc.get("text_hash")
                    if doc_hash:
                        to_stamp.append({"efta_id": eid, "doc_text_hash": doc_hash})
                    stats["stamped"] += 1
                else:
                    # Chunks differ — re-chunk and re-embed
                    to_rechunk.append((doc, exp_chunks))
                    stats["rechunked"] += 1

        # Stamp matching chunks
        if to_stamp:
            if dry_run:
                logger.info(f"[DRY RUN] Would stamp {len(to_stamp)} docs: {[s['efta_id'] for s in to_stamp[:5]]}{'...' if len(to_stamp) > 5 else ''}")
            else:
                try:
                    stamp_hashes(to_stamp, session)
                    logger.info(f"Stamped {len(to_stamp)} docs' chunks")
                except requests.RequestException as e:
                    logger.warning(f"Failed to stamp hashes: {e}")

        # Re-chunk and re-embed mismatches
        if to_rechunk:
            rechunk_ids = [doc["efta_id"] for doc, _ in to_rechunk]
            if dry_run:
                for doc, chunks in to_rechunk:
                    logger.info(f"[DRY RUN] Would re-chunk {doc['efta_id']} (word_count={doc.get('word_count', 0)}, {len(chunks)} chunks)")
            else:
                all_chunks: list[Chunk] = []
                batch_hashes: dict[str, str] = {}
                for doc, chunks in to_rechunk:
                    all_chunks.extend(chunks)
                    if doc.get("text_hash"):
                        batch_hashes[doc["efta_id"]] = doc["text_hash"]

                _process_and_post(
                    all_chunks, models, session,
                    overwrite=True,
                    label=f"Super-check fix batch {batch_start // BATCH_SIZE + 1}",
                    doc_text_hashes=batch_hashes,
                )

        if (batch_start // BATCH_SIZE) % 100 == 0:
            checked = batch_start + len(batch_ids)
            logger.info(
                f"Super-checked {checked}/{len(all_ids)} docs — "
                f"synced={stats['already_synced']}, stamped={stats['stamped']}, "
                f"rechunked={stats['rechunked']}, missing={stats['missing_chunks']}"
            )

    logger.info(f"Dataset {dataset} super-check complete:")
    for reason, count in stats.items():
        logger.info(f"  {reason:25s}: {count:>8,}")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(description="GPU chunk worker")
    parser.add_argument("--check", action="store_true",
                        help="Verify and fix existing data before ingestion")
    parser.add_argument("--super-check", action="store_true",
                        help="Compare text_hash between docs and chunks, stamp or re-chunk as needed")
    parser.add_argument("--dry-run", action="store_true",
                        help="With --super-check, report what would change without modifying anything")
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
    if args.super_check:
        if args.dry_run:
            logger.info("Super-check DRY RUN: will report changes without modifying anything")
        else:
            logger.info("Super-check mode: will compare text_hash and stamp/re-chunk")
    if args.check:
        logger.info("Check mode: will verify and fix existing data first")

    # Load models
    models = load_models(devices)

    # Create HTTP session
    session = requests.Session()

    # Super-check mode: compare text_hash and stamp/re-chunk
    if args.super_check:
        for ds in datasets:
            super_check_dataset(ds, models, session, dry_run=args.dry_run)
        logger.info("=== Super-check complete ===")
        return

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
        logger.info(f"  Dataset {r['dataset']}: {r['pending']} pending, {r['chunks']} chunks")


if __name__ == "__main__":
    main()
