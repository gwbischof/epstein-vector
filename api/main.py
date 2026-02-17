"""FastAPI app for semantic search over Epstein documents."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.security import APIKeyHeader

from api import search as search_module
from api.db import close_conn

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("API_KEY", "")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(key: str | None = Security(api_key_header)):
    if not API_KEY:
        return  # No key configured = no auth
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load embedding model on CPU at startup
    logger.info("Loading embedding model on CPU...")
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer("BAAI/bge-large-en-v1.5", device="cpu")
    search_module.set_model(model)
    logger.info("Embedding model loaded")
    yield
    close_conn()


app = FastAPI(
    title="Epstein Vector Search",
    description="Semantic search over DOJ Epstein Library documents",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/search", dependencies=[Depends(verify_api_key)])
def search_endpoint(req: search_module.SearchRequest) -> search_module.SearchResponse:
    return search_module.search(req)
