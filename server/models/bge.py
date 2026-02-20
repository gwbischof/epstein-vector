"""BGE-large-en-v1.5 embedding model wrapper."""

from __future__ import annotations

import logging

import torch
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

MODEL_NAME = "BAAI/bge-large-en-v1.5"
MODEL_REVISION = "d4aa6901d3a41ba39fb536a557fa166f842b0e09"
EMBEDDING_DIM = 1024
MAX_SEQ_LENGTH = 512


class BGEModel:
    def __init__(self):
        self.model: SentenceTransformer | None = None
        self.device: str | None = None

    def load(self, device: str = "cuda:0") -> None:
        logger.info(f"Loading {MODEL_NAME} on {device}...")
        self.model = SentenceTransformer(MODEL_NAME, revision=MODEL_REVISION, device=device)
        self.device = device
        logger.info(f"Loaded {MODEL_NAME} on {device}")

    def encode(self, texts: list[str]) -> list[list[float]]:
        if self.model is None:
            raise RuntimeError("Model not loaded. Call load() first.")
        embeddings = self.model.encode(
            texts,
            normalize_embeddings=True,
            show_progress_bar=False,
            batch_size=64,
        )
        return embeddings.tolist()
