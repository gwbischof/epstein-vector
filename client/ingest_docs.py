"""CPU-only doc loader: streams JSONL and bulk-loads document rows.

No GPU dependencies. Reads JSONL line by line to avoid loading
multi-GB files into memory, and POSTs batches of 500 docs to the API.
Version-gated upsert on the server side means this is fully idempotent.

Usage:
    python -m client.ingest_docs API_KEY
    python -m client.ingest_docs API_KEY --datasets 9,10
    python -m client.ingest_docs API_KEY --version 2
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time

import requests

from client.download import DATASETS, download_jsonl

logger = logging.getLogger(__name__)

API_URL = "https://vector.korroni.cloud"
DATA_DIR = "data"
BATCH_SIZE = 500


def stream_jsonl(path):
    """Yield dicts from a JSONL file one line at a time."""
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def post_documents(batch: list[dict], api_key: str, session: requests.Session) -> int:
    """POST a batch of document records to the API. Returns count sent."""
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key

    payload = {"documents": batch}

    for attempt in range(3):
        try:
            resp = session.post(
                f"{API_URL}/ingest/documents",
                json=payload,
                headers=headers,
                timeout=120,
            )
            resp.raise_for_status()
            return len(batch)
        except requests.RequestException as e:
            if attempt < 2:
                wait = 2 ** (attempt + 1)
                logger.warning(f"POST failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def ingest_dataset(dataset: int, api_key: str, version: int, session: requests.Session) -> dict:
    """Stream a single dataset's JSONL and upload doc rows in batches."""
    from pathlib import Path

    logger.info(f"=== Dataset {dataset} ===")

    jsonl_path = download_jsonl(dataset, Path(DATA_DIR))

    batch: list[dict] = []
    total = 0
    posted = 0

    for doc in stream_jsonl(jsonl_path):
        efta_id = doc.get("efta_id") or doc.get("efta", "")
        record = {
            "efta_id": efta_id,
            "dataset": int(doc.get("dataset", dataset)),
            "url": doc.get("url", ""),
            "pages": doc.get("pages", 0),
            "word_count": doc.get("word_count", 0),
            "text": doc.get("text", ""),
            "version": version,
        }
        batch.append(record)
        total += 1

        if len(batch) >= BATCH_SIZE:
            posted += post_documents(batch, api_key, session)
            if posted % 5000 == 0:
                logger.info(f"  {posted} docs posted...")
            batch = []

    # Flush remaining
    if batch:
        posted += post_documents(batch, api_key, session)

    logger.info(f"Dataset {dataset}: {total} docs streamed, {posted} posted")
    return {"dataset": dataset, "total": total, "posted": posted}


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(description="CPU doc loader — streams JSONL into documents table")
    parser.add_argument("api_key", help="Ingest API key")
    parser.add_argument("--datasets", default=None,
                        help="Comma-separated dataset numbers (default: all)")
    parser.add_argument("--version", type=int, default=1,
                        help="Document version (default: 1)")
    args = parser.parse_args()

    if args.datasets:
        datasets = [int(x.strip()) for x in args.datasets.split(",") if x.strip()]
    else:
        datasets = list(DATASETS)

    logger.info(f"API: {API_URL}")
    logger.info(f"Datasets: {datasets}")
    logger.info(f"Version: {args.version}")

    session = requests.Session()

    results = []
    for ds in datasets:
        result = ingest_dataset(ds, args.api_key, args.version, session)
        results.append(result)

    logger.info("=== Done ===")
    for r in results:
        logger.info(f"  Dataset {r['dataset']}: {r['total']} total, {r['posted']} posted")


if __name__ == "__main__":
    main()
