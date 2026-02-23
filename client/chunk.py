"""Text chunking for Epstein documents.

Short docs (< 200 words) are embedded whole.
Long docs are split with RecursiveCharacterTextSplitter at ~200 words with 30 word overlap.
Each chunk gets a contextual prefix: [EFTA_ID | Dataset N]
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from langchain_text_splitters import RecursiveCharacterTextSplitter

logger = logging.getLogger(__name__)

# ~200 words ≈ 1000 chars, ~30 words ≈ 150 chars
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 150
MIN_WORD_COUNT = 5
MIN_ALPHA_RATIO = 0.5  # reject garbage OCR: at least 50% of non-whitespace chars must be letters

splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
    length_function=len,
    separators=["\n\n", "\n", ". ", " ", ""],
)


@dataclass
class Chunk:
    efta_id: str
    dataset: int
    chunk_index: int
    total_chunks: int
    text: str
    skip_embedding: bool = False
    version: int = 1


def _sentinel(efta_id: str, dataset: int, text: str) -> list[Chunk]:
    """Return a single sentinel chunk for docs that can't be embedded."""
    prefix = f"[{efta_id} | Dataset {dataset}] "
    body = text.strip() if text.strip() else "(no text)"
    return [Chunk(
        efta_id=efta_id,
        dataset=dataset,
        chunk_index=0,
        total_chunks=1,
        text=prefix + body,
        skip_embedding=True,
    )]


def chunk_document(doc: dict) -> list[Chunk]:
    """Chunk a single document dict from the JSONL.

    Expected fields: efta_id, dataset, text, word_count, extracted.
    Always returns at least one Chunk. Documents that fail quality checks
    get a sentinel chunk with skip_embedding=True (stored with NULL embedding).
    """
    efta_id = doc.get("efta_id") or doc.get("efta", "")
    dataset = int(doc.get("dataset", 0))
    text = doc.get("text", "")
    word_count = doc.get("word_count", 0)
    extracted = doc.get("extracted", True)

    # Non-extracted or very short docs → sentinel
    if not extracted or word_count < MIN_WORD_COUNT or not text.strip():
        return _sentinel(efta_id, dataset, text)

    # Garbage OCR → sentinel
    non_ws = [c for c in text if not c.isspace()]
    if non_ws:
        alpha_ratio = sum(1 for c in non_ws if c.isalpha()) / len(non_ws)
        if alpha_ratio < MIN_ALPHA_RATIO:
            logger.debug(f"Skipping {efta_id}: alpha_ratio={alpha_ratio:.2f}")
            return _sentinel(efta_id, dataset, text)

    prefix = f"[{efta_id} | Dataset {dataset}] "

    # Short docs: embed whole
    if word_count < 200:
        return [Chunk(
            efta_id=efta_id,
            dataset=dataset,
            chunk_index=0,
            total_chunks=1,
            text=prefix + text,
        )]

    # Long docs: split
    splits = splitter.split_text(text)
    total = len(splits)
    return [
        Chunk(
            efta_id=efta_id,
            dataset=dataset,
            chunk_index=i,
            total_chunks=total,
            text=prefix + s,
        )
        for i, s in enumerate(splits)
    ]


def chunk_documents(docs: list[dict]) -> list[Chunk]:
    """Chunk a list of documents."""
    chunks = []
    sentinels = 0
    for doc in docs:
        result = chunk_document(doc)
        chunks.extend(result)
        if result and result[0].skip_embedding:
            sentinels += 1
    logger.info(f"Chunked {len(docs)} docs → {len(chunks)} chunks ({sentinels} sentinels)")
    return chunks
