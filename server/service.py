"""BentoML embedding service with dual-GPU parallelism.

Runs on Windows machine with 2x GTX 1080Ti, port 8200.
Each batch is split across both GPUs and run in parallel.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import bentoml

from server.models.bge import BGEModel

logger = logging.getLogger(__name__)


@bentoml.service(
    name="embedding_service",
    traffic={"timeout": 300},
    resources={"gpu": 2, "gpu_type": "nvidia-gtx-1080-ti"},
)
class EmbeddingService:
    def __init__(self):
        self.models: list[BGEModel] = []
        for device in ("cuda:0", "cuda:1"):
            m = BGEModel()
            m.load(device)
            self.models.append(m)
        self._pool = ThreadPoolExecutor(max_workers=len(self.models))
        logger.info(f"Embedding service ready with {len(self.models)} GPUs")

    @bentoml.api
    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts, split across all GPUs in parallel."""
        n_gpus = len(self.models)
        if n_gpus == 1 or len(texts) < 2:
            return self.models[0].encode(texts)

        # Split texts into N chunks, one per GPU
        chunks = [texts[i::n_gpus] for i in range(n_gpus)]
        logger.info(f"Embedding {len(texts)} texts split across {n_gpus} GPUs ({[len(c) for c in chunks]})")

        futures = [
            self._pool.submit(model.encode, chunk)
            for model, chunk in zip(self.models, chunks)
        ]
        results = [f.result() for f in futures]

        # Interleave results back into original order
        merged = [None] * len(texts)
        for gpu_idx, gpu_results in enumerate(results):
            for j, emb in enumerate(gpu_results):
                merged[gpu_idx + j * n_gpus] = emb

        return merged

    @bentoml.api
    def health(self) -> dict:
        return {
            "status": "ok",
            "gpus": len(self.models),
            "model": "bge-large-en-v1.5",
        }
