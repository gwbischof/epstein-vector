# epstein-vector

Semantic search over ~1M DOJ Epstein Library documents using pgvector.

## Architecture

```
Bulk Job (one-time):
  Download JSONL → Chunk text → Embed on Windows 1080Ti → Insert into Postgres

Production:
  User query → FastAPI on VPS → embed query on CPU → pgvector search → return results
```

## Setup

### API (production)

```bash
# Start Postgres + API
docker compose up -d

# Or deploy to korroni.cloud (via GitHub Actions on push to main)
```

### Bulk Ingestion

```bash
# Install client deps
pip install '.[client]'

# Start embedding server on Windows GPU machine
# (see server/ README)

# Run ingestion pipeline
python -m client.ingest --datasets 1 2 3
```

### Embedding Server (Windows GPU)

```bash
pip install '.[server]'
bentoml serve server.service:EmbeddingService --port 8200
```

## API

### POST /search

```bash
curl -X POST https://vector.korroni.cloud/search \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "flight logs", "limit": 10}'
```

### GET /health

```bash
curl https://vector.korroni.cloud/health
```

## Stack

- **Embedding model**: bge-large-en-v1.5 (1024 dims)
- **Vector DB**: Postgres + pgvector (halfvec for 50% storage)
- **API**: FastAPI + uvicorn
- **GPU server**: BentoML on 2x GTX 1080Ti
- **Deployment**: Docker + Traefik on korroni.cloud
