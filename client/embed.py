"""Client that sends text chunks to BentoML embedding server."""

from __future__ import annotations

import logging
import time

import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

DEFAULT_SERVER = "http://localhost:8200"
BATCH_SIZE = 1024


class EmbeddingClient:
    def __init__(self, server_url: str = DEFAULT_SERVER):
        self.server_url = server_url.rstrip("/")
        self.session = requests.Session()

    def embed_batch(self, texts: list[str], retries: int = 3) -> list[list[float]]:
        """Send a batch of texts to the embedding server."""
        for attempt in range(retries):
            try:
                resp = self.session.post(
                    f"{self.server_url}/embed",
                    json={"texts": texts},
                    timeout=120,
                )
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as e:
                if attempt < retries - 1:
                    wait = 2 ** attempt
                    logger.warning(f"Embed failed (attempt {attempt + 1}): {e}. Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise

    def embed_all(
        self,
        texts: list[str],
        batch_size: int = BATCH_SIZE,
    ) -> list[list[float]]:
        """Embed all texts in batches with progress bar."""
        all_embeddings = []
        for i in tqdm(range(0, len(texts), batch_size), desc="Embedding"):
            batch = texts[i : i + batch_size]
            embeddings = self.embed_batch(batch)
            all_embeddings.extend(embeddings)
        return all_embeddings

    def health(self) -> dict:
        resp = self.session.post(f"{self.server_url}/health", timeout=10)
        resp.raise_for_status()
        return resp.json()
