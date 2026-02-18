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
    resources={"gpu": 2, "gpu_type": "nvidia-gtx-1080-ti"},
)
class EmbeddingService:
    def __init__(self):
        self.models: list[BGEModel] = []
        for device in ("cuda:0", "cuda:1"):
            m = BGEModel()
            m.load(device)
            self.models.append(m)
        self._gpu_cycle = itertools.cycle(range(len(self.models)))
        logger.info(f"Embedding service ready with {len(self.models)} GPUs")

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
