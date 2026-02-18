"""BentoML embedding service with dual-GPU round-robin.

Runs on Windows machine with 2x GTX 1080Ti, port 8200.
"""

from __future__ import annotations

import itertools
import logging

import bentoml

from server.models.bge import BGEModel

logger = logging.getLogger(__name__)


@bentoml.service(
    name="embedding_service",
    traffic={"timeout": 300},
    resources={"gpu": 1, "gpu_type": "nvidia-gtx-1080-ti"},
)
class EmbeddingService:
    def __init__(self):
        self.models: list[BGEModel] = []
        m = BGEModel()
        m.load("cuda:0")
        self.models.append(m)
        self._gpu_cycle = itertools.cycle(range(1))
        logger.info("Embedding service ready with 1 GPU")

    @bentoml.api
    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts. Round-robins between GPUs."""
        gpu_idx = next(self._gpu_cycle)
        model = self.models[gpu_idx]
        logger.info(f"Embedding {len(texts)} texts on cuda:{gpu_idx}")
        return model.encode(texts)

    @bentoml.api
    def health(self) -> dict:
        return {
            "status": "ok",
            "gpus": len(self.models),
            "model": "bge-large-en-v1.5",
        }
