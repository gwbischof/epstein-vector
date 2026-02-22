"""FastAPI app for semantic search over Epstein documents."""

from __future__ import annotations

import hmac
import logging
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.security import APIKeyHeader

from api import search as search_module
from api.db import close_pool
from api.ingest import router as ingest_router
from api.ingest_db import close_ingest_pool

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("API_KEY", "")
INGEST_API_KEY = os.environ.get("INGEST_API_KEY", "")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(key: str | None = Security(api_key_header)):
    if not API_KEY:
        return  # No key configured = no auth
    if key is None or not hmac.compare_digest(key, API_KEY):
        raise HTTPException(status_code=401, detail="Invalid API key")


def verify_ingest_api_key(key: str | None = Security(api_key_header)):
    if not INGEST_API_KEY:
        return  # No key configured = no auth
    if key is None or not hmac.compare_digest(key, INGEST_API_KEY):
        raise HTTPException(status_code=401, detail="Invalid API key")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load embedding model on CPU at startup
    logger.info("Loading embedding model on CPU...")
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer("BAAI/bge-large-en-v1.5", revision="d4aa6901d3a41ba39fb536a557fa166f842b0e09", device="cpu")
    search_module.set_model(model)
    logger.info("Embedding model loaded")
    search_module.ensure_pg_trgm()
    logger.info("pg_trgm extension and index ensured")
    yield
    close_pool()
    close_ingest_pool()


app = FastAPI(
    title="Epstein Vector Search",
    description="Semantic search over DOJ Epstein Library documents",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


app.include_router(ingest_router, dependencies=[Depends(verify_ingest_api_key)])


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/vector_search", dependencies=[Depends(verify_api_key)])
def vector_search_endpoint(req: search_module.SearchRequest) -> search_module.SearchResponse:
    return search_module.search(req)


@app.post("/text_search", dependencies=[Depends(verify_api_key)])
def text_search_endpoint(req: search_module.TextSearchRequest) -> search_module.TextSearchResponse:
    return search_module.text_search(req)


@app.post("/fuzzy_search", dependencies=[Depends(verify_api_key)])
def fuzzy_search_endpoint(req: search_module.FuzzySearchRequest) -> search_module.FuzzySearchResponse:
    return search_module.fuzzy_search(req)


@app.post("/text_search/count", dependencies=[Depends(verify_api_key)])
def text_search_count_endpoint(req: search_module.CountRequest) -> search_module.CountResponse:
    return search_module.text_search_count(req)


@app.post("/fuzzy_search/count", dependencies=[Depends(verify_api_key)])
def fuzzy_search_count_endpoint(req: search_module.CountRequest) -> search_module.CountResponse:
    return search_module.fuzzy_search_count(req)


@app.post("/similarity_search", dependencies=[Depends(verify_api_key)])
def similarity_search_endpoint(req: search_module.SimilarRequest) -> search_module.SearchResponse:
    return search_module.similar(req)
