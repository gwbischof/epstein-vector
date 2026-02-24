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
import queue
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import requests

from client.chunk import Chunk, MIN_WORD_COUNT, chunk_document
from server.models.bge import BGEModel

logger = logging.getLogger(__name__)

# Config from env
API_URL = os.environ.get("API_URL", "https://vector.korroni.cloud").rstrip("/")
API_KEY = os.environ.get("API_KEY", "")
DATASETS = os.environ.get("DATASETS", "1,2,3,4,5,6,7,8,9,10,11,12")
CUDA_DEVICES = os.environ.get("CUDA_DEVICES", "0")
BATCH_SIZE = 50
CHECKER_BATCH_SIZE = int(os.environ.get("CHECKER_BATCH_SIZE", "200"))
TARGET_EMBED_CHUNKS = int(os.environ.get("TARGET_EMBED_CHUNKS", "500"))


@dataclass
class RechunkWorkItem:
    chunks: list[Chunk]
    doc_text_hashes: dict[str, str]
    batch_label: str


class PipelineStats:
    """Thread-safe counter for super-check statistics."""

    def __init__(self):
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {
            "already_synced": 0,
            "stamped": 0,
            "rechunked": 0,
            "missing_chunks": 0,
            "missing_embeddings": 0,
        }

    def inc(self, key: str, n: int = 1) -> None:
        with self._lock:
            self._counts[key] += n

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)


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
            sub_overwrite = overwrite and sub_num == 1
            result = post_chunks(sub_emb, sub_emb_vecs, sub_sent, session, overwrite=sub_overwrite, doc_text_hashes=doc_text_hashes)
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
        "missing_embeddings": 0,
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
                logger.info(f"  FLAGGED {eid}: missing_chunks (word_count={doc.get('word_count', 0)})")
            elif act.get("embedded_count", 0) < exp_embedded:
                stats["missing_embeddings"] += 1
                to_fix.append((doc, exp_chunks, "missing_embeddings"))
                logger.info(f"  FLAGGED {eid}: missing_embeddings (expected={exp_embedded}, actual={act.get('embedded_count', 0)}, chunks={act.get('chunk_count', 0)})")
            elif act.get("chunk_text_hash") and act.get("doc_text_hash") and \
                 act.get("chunk_text_hash") != act.get("doc_text_hash"):
                stats["hash_mismatch"] += 1
                to_fix.append((doc, exp_chunks, "hash_mismatch"))
                logger.info(f"  FLAGGED {eid}: hash_mismatch (doc={act.get('doc_text_hash')[:8]}, chunk={act.get('chunk_text_hash')[:8]})")
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


def _checker_thread(
    all_ids: list[str],
    work_queue: queue.Queue | None,
    stats: PipelineStats,
    shutdown: threading.Event,
    dry_run: bool,
    dataset: int,
) -> None:
    """Producer: checks docs in batches, stamps matching, enqueues rechunks."""
    session = requests.Session()
    try:
        for batch_start in range(0, len(all_ids), CHECKER_BATCH_SIZE):
            if shutdown.is_set():
                logger.info("Checker: shutdown requested, stopping")
                return

            big_batch_ids = all_ids[batch_start : batch_start + CHECKER_BATCH_SIZE]
            to_stamp: list[dict] = []
            rechunk_docs: list[tuple[dict, list[Chunk]]] = []

            # Sub-batch API calls at BATCH_SIZE (server limit)
            for sub_start in range(0, len(big_batch_ids), BATCH_SIZE):
                if shutdown.is_set():
                    return

                batch_ids = big_batch_ids[sub_start : sub_start + BATCH_SIZE]

                docs = fetch_documents(batch_ids, session)
                docs_by_id = {d["efta_id"]: d for d in docs}

                try:
                    actual = get_chunk_status(batch_ids, session)
                except requests.RequestException as e:
                    logger.warning(f"Failed to get chunk status for batch at {batch_start + sub_start}: {e}")
                    continue

                to_fetch_and_compare: list[str] = []

                for eid in batch_ids:
                    doc = docs_by_id.get(eid)
                    if doc is None:
                        continue

                    act = actual.get(eid, {})
                    doc_hash = doc.get("text_hash")
                    chunk_hash = act.get("chunk_text_hash")
                    chunk_count = act.get("chunk_count", 0)

                    if chunk_count == 0:
                        exp_chunks = chunk_document(doc)
                        rechunk_docs.append((doc, exp_chunks))
                        stats.inc("missing_chunks")
                    elif chunk_hash and doc_hash and chunk_hash == doc_hash:
                        # Hash matches, but verify all chunks/embeddings were uploaded
                        exp_chunks = chunk_document(doc)
                        exp_embedded = sum(1 for c in exp_chunks if not c.skip_embedding)
                        if act.get("embedded_count", 0) < exp_embedded:
                            rechunk_docs.append((doc, exp_chunks))
                            stats.inc("missing_embeddings")
                        else:
                            stats.inc("already_synced")
                    elif chunk_hash is None and chunk_count > 0:
                        to_fetch_and_compare.append(eid)
                    elif chunk_hash and doc_hash and chunk_hash != doc_hash:
                        exp_chunks = chunk_document(doc)
                        rechunk_docs.append((doc, exp_chunks))
                        stats.inc("rechunked")

                # Fetch and compare chunk texts
                if to_fetch_and_compare:
                    chunk_data = fetch_chunks(to_fetch_and_compare, session)

                    for eid in to_fetch_and_compare:
                        doc = docs_by_id.get(eid)
                        if doc is None:
                            continue

                        exp_chunks = chunk_document(doc)
                        exp_texts = [c.text for c in exp_chunks]

                        actual_chunks = chunk_data.get(eid, [])
                        actual_texts = [c["text"] for c in sorted(actual_chunks, key=lambda x: x["chunk_index"])]

                        if exp_texts == actual_texts:
                            # Texts match, but verify all embeddings were uploaded
                            act = actual.get(eid, {})
                            exp_embedded = sum(1 for c in exp_chunks if not c.skip_embedding)
                            if act.get("embedded_count", 0) < exp_embedded:
                                rechunk_docs.append((doc, exp_chunks))
                                stats.inc("missing_embeddings")
                            else:
                                doc_hash = doc.get("text_hash")
                                if doc_hash:
                                    to_stamp.append({"efta_id": eid, "doc_text_hash": doc_hash})
                                stats.inc("stamped")
                        else:
                            rechunk_docs.append((doc, exp_chunks))
                            stats.inc("rechunked")

            # Stamp matching chunks
            if to_stamp:
                if dry_run:
                    pass  # counted in stats, reported in final summary
                else:
                    try:
                        stamp_hashes(to_stamp, session)
                        logger.info(f"Stamped {len(to_stamp)} docs' chunks")
                    except requests.RequestException as e:
                        logger.warning(f"Failed to stamp hashes: {e}")

            # Enqueue or log rechunk work
            if rechunk_docs:
                if dry_run:
                    logger.info(f"[DRY RUN] Would re-chunk {len(rechunk_docs)} docs ({sum(len(c) for _, c in rechunk_docs)} chunks)")
                elif work_queue is not None:
                    all_chunks: list[Chunk] = []
                    batch_hashes: dict[str, str] = {}
                    for doc, chunks in rechunk_docs:
                        all_chunks.extend(chunks)
                        if doc.get("text_hash"):
                            batch_hashes[doc["efta_id"]] = doc["text_hash"]
                    item = RechunkWorkItem(
                        chunks=all_chunks,
                        doc_text_hashes=batch_hashes,
                        batch_label=f"Super-check batch {batch_start // CHECKER_BATCH_SIZE + 1}",
                    )
                    while not shutdown.is_set():
                        try:
                            work_queue.put(item, timeout=1.0)
                            break
                        except queue.Full:
                            continue

            # Progress log
            checked = batch_start + len(big_batch_ids)
            snap = stats.snapshot()
            if dry_run:
                up_to_date = snap['already_synced']
                need_work = snap['stamped'] + snap['rechunked'] + snap['missing_chunks'] + snap['missing_embeddings']
                logger.info(
                    f"Dry run: {checked}/{len(all_ids)} docs — "
                    f"{up_to_date} up-to-date, {need_work} need work"
                )
            else:
                logger.info(
                    f"Checker: {checked}/{len(all_ids)} docs — "
                    f"synced={snap['already_synced']}, stamped={snap['stamped']}, "
                    f"rechunked={snap['rechunked']}, missing={snap['missing_chunks']}, "
                    f"missing_emb={snap['missing_embeddings']}"
                )
    except Exception:
        logger.exception("Checker thread crashed")
        shutdown.set()
        raise
    finally:
        if work_queue is not None:
            work_queue.put(None)


def _embedder_thread(
    work_queue: queue.Queue,
    models: list[BGEModel],
    shutdown: threading.Event,
) -> None:
    """Consumer: pulls rechunk items from queue, embeds, and posts."""
    session = requests.Session()
    accumulated_chunks: list[Chunk] = []
    accumulated_hashes: dict[str, str] = {}
    batch_num = 0

    try:
        while True:
            try:
                item = work_queue.get(timeout=1.0)
            except queue.Empty:
                if shutdown.is_set():
                    logger.warning("Embedder: shutdown requested, stopping")
                    return
                continue

            if item is None:
                break

            accumulated_chunks.extend(item.chunks)
            accumulated_hashes.update(item.doc_text_hashes)

            # Split at doc boundaries — never split a doc's chunks across batches
            while len(accumulated_chunks) >= TARGET_EMBED_CHUNKS:
                # Find a cut point that doesn't split a doc
                cut = TARGET_EMBED_CHUNKS
                if cut < len(accumulated_chunks):
                    # Walk forward to include all chunks of the last doc in the batch
                    last_eid = accumulated_chunks[cut - 1].efta_id
                    while cut < len(accumulated_chunks) and accumulated_chunks[cut].efta_id == last_eid:
                        cut += 1

                batch_num += 1
                batch = accumulated_chunks[:cut]
                accumulated_chunks = accumulated_chunks[cut:]
                batch_eids = {c.efta_id for c in batch}
                batch_hashes = {k: v for k, v in accumulated_hashes.items() if k in batch_eids}
                _process_and_post(
                    batch, models, session,
                    overwrite=True,
                    label=f"Embedder batch {batch_num}",
                    doc_text_hashes=batch_hashes,
                )

        # Flush remaining chunks
        if accumulated_chunks:
            batch_num += 1
            batch_eids = {c.efta_id for c in accumulated_chunks}
            batch_hashes = {k: v for k, v in accumulated_hashes.items() if k in batch_eids}
            _process_and_post(
                accumulated_chunks, models, session,
                overwrite=True,
                label=f"Embedder batch {batch_num} (final)",
                doc_text_hashes=batch_hashes,
            )
    except Exception:
        logger.exception("Embedder thread crashed")
        shutdown.set()
        raise


def super_check_dataset(
    dataset: int,
    models: list[BGEModel],
    session: requests.Session,
    dry_run: bool = False,
) -> None:
    """Compare text_hash between documents and chunks, fix mismatches.

    Uses a producer/consumer pipeline:
    - Checker thread: fetches docs, compares hashes, stamps matching, enqueues rechunks
    - Embedder thread: pulls rechunk items, embeds, posts

    For --dry-run, the checker runs inline on the main thread with no embedder.
    Each thread creates its own requests.Session (not thread-safe to share).
    """
    mode = "DRY RUN" if dry_run else "LIVE"
    logger.info(f"=== Super-Check Dataset {dataset} ({mode}) ===")

    all_ids = get_all_ids(dataset, session)
    logger.info(f"{len(all_ids)} documents to super-check")

    stats = PipelineStats()
    shutdown = threading.Event()

    if dry_run:
        # Run checker inline on main thread — no embedder, no queue
        _checker_thread(all_ids, None, stats, shutdown, True, dataset)
    else:
        work_queue: queue.Queue[RechunkWorkItem | None] = queue.Queue(maxsize=20)
        checker_error: list[BaseException | None] = [None]
        embedder_error: list[BaseException | None] = [None]

        def checker_wrapper():
            try:
                _checker_thread(all_ids, work_queue, stats, shutdown, False, dataset)
            except Exception as e:
                checker_error[0] = e

        def embedder_wrapper():
            try:
                _embedder_thread(work_queue, models, shutdown)
            except Exception as e:
                embedder_error[0] = e

        checker = threading.Thread(target=checker_wrapper, name="checker")
        embedder = threading.Thread(target=embedder_wrapper, name="embedder")

        checker.start()
        embedder.start()

        checker.join()
        embedder.join()

        if checker_error[0]:
            raise checker_error[0]
        if embedder_error[0]:
            raise embedder_error[0]

    snap = stats.snapshot()
    up_to_date = snap['already_synced']
    chunks_ok = snap['stamped']
    to_reembed = snap['rechunked'] + snap['missing_chunks'] + snap['missing_embeddings']
    total = up_to_date + chunks_ok + to_reembed
    if dry_run:
        logger.info(
            f"Dataset {dataset} dry run — {total:,} docs: "
            f"{up_to_date:,} good, {chunks_ok:,} chunks ok but need hash written, {to_reembed:,} need re-embed"
        )
    else:
        logger.info(
            f"Dataset {dataset} super-check complete — {total:,} docs: "
            f"{up_to_date:,} good, {chunks_ok:,} hashes written, {to_reembed:,} re-embedded"
        )


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
