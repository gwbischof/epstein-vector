# epstein-vector

Semantic search over ~1M DOJ Epstein Library documents using pgvector.

## Architecture

```
Bulk Job (one-time):
  Download JSONL → Chunk text → Embed on GPU → Insert into Postgres

Production:
  User query → FastAPI → embed query on CPU → pgvector search → return results
```

## Setup

### Embedding Server (GPU)

```bash
uv sync --extra server
bentoml serve server.service:EmbeddingService --port 8200
```

Loads bge-large-en-v1.5 on both GPUs and round-robins batches between them. Edit `server/service.py` to adjust GPU count.

### Bulk Ingestion

Ingestion populates the database. It downloads the JSONL source files, chunks each document into ~200 word pieces, sends them to the GPU server for embedding, and inserts the vectors into Postgres. Ingestion is resumable — it skips documents that already have embeddings in the database, so you can safely re-run after a crash or interruption.

```bash
uv sync --extra client

# Ingest specific datasets
python -m client.ingest --datasets 1 2 3

# All 12 datasets
python -m client.ingest

# Custom embed server / DB
python -m client.ingest \
  --embed-server http://192.168.1.100:8200 \
  --db-url postgresql://epstein:password@localhost:5432/epstein
```

**Remote ingestion via SSH tunnel** — if Postgres is on a remote server (not exposed publicly), tunnel through SSH and point the ingest at localhost:

```bash
# Terminal 1: open tunnel to remote Postgres
ssh -L 5433:epstein-vector-pg:5432 user@yourserver

# Terminal 2: run ingestion through the tunnel
python -m client.ingest \
  --db-url postgresql://epstein:password@localhost:5433/epstein \
  --embed-server http://gpu-machine:8200 \
  --reader-password 'reader-password-here'
```

The `--reader-password` flag creates a read-only `epstein_reader` Postgres role that the API uses. This ensures the public-facing API can only `SELECT`, never modify data.

### API

```bash
# Start Postgres + API
docker compose up -d

# Or run API directly
uv sync --extra api
DATABASE_URL=postgresql://epstein:epstein@localhost:5432/epstein uvicorn api.main:app --reload
```

Set `API_KEY` env var to require authentication. If unset, all endpoints are open.

## API Reference

### POST /vector_search

Semantic search — finds documents by meaning.

```bash
curl -X POST http://localhost:8000/vector_search \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "flight logs", "limit": 10, "dataset": 9}'
```

Response:

```json
{
  "query": "flight logs",
  "results": [
    {
      "efta_id": "EFTA00123456",
      "dataset": 9,
      "chunk_index": 0,
      "total_chunks": 3,
      "text": "[EFTA00123456 | Dataset 9] ...",
      "score": 0.87
    }
  ]
}
```

Parameters:
- `query` (required): search text
- `limit` (optional, default 20, max 100): number of results
- `offset` (optional, default 0): skip results for pagination
- `dataset` (optional): filter to specific dataset number

### POST /text_search

Full-text keyword search — finds exact word matches.

```bash
curl -X POST http://localhost:8000/text_search \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "Maxwell flight", "limit": 10, "dataset": 9}'
```

Response:

```json
{
  "query": "Maxwell flight",
  "results": [
    {
      "efta_id": "EFTA00123456",
      "dataset": 9,
      "word_count": 450,
      "rank": 0.075,
      "headline": "...traveled with <b>Maxwell</b> on a <b>flight</b> to..."
    }
  ]
}
```

Keyword search supports several query syntaxes:

| Syntax | Example | Behavior |
|---|---|---|
| Plain terms | `Maxwell flight` | AND — both words must appear |
| Exact phrase | `"wire transfer"` | Phrase match |
| OR | `Maxwell OR Brunel` | Either term |
| NOT | `island -vacation` | Exclude a term |
| Wildcard | `maxw*` | Prefix match |

Parameters:
- `query` (required): search text
- `limit` (optional, default 20, max 100): number of results
- `offset` (optional, default 0): skip results for pagination
- `dataset` (optional): filter to specific dataset number

### POST /fuzzy_search

Fuzzy trigram search — typo-tolerant matching for OCR errors and misspellings.

```bash
curl -X POST http://localhost:8000/fuzzy_search \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "Maxwel", "limit": 10}'
```

Response:

```json
{
  "query": "Maxwel",
  "results": [
    {
      "efta_id": "EFTA00123456",
      "dataset": 9,
      "word_count": 450,
      "similarity": 0.42,
      "headline": "...traveled with <b>Maxwell</b> on a flight to..."
    }
  ]
}
```

Parameters:
- `query` (required): search text
- `limit` (optional, default 20, max 100): number of results
- `offset` (optional, default 0): skip results for pagination
- `dataset` (optional): filter to specific dataset number
- `exclude_exact` (optional, default false): exclude documents that keyword search already matches, showing only fuzzy-only results

### POST /similarity_search

Find documents similar to a given chunk — uses the existing embedding without re-encoding.

```bash
curl -X POST http://localhost:8000/similarity_search \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"efta_id": "EFTA00123456", "chunk_index": 0, "limit": 10}'
```

Parameters:
- `efta_id` (required): source document ID
- `chunk_index` (optional, default 0): which chunk to use as the query vector
- `limit` (optional, default 20, max 100): number of results
- `offset` (optional, default 0): skip results for pagination
- `dataset` (optional): filter to specific dataset number

### GET /health

```bash
curl http://localhost:8000/health
```

## Web Frontend

A search UI at `web/` — Next.js with semantic + keyword search, infinite scroll, similarity search, and links to DOJ source PDFs.

```bash
cd web
npm install
npm run dev
```

The frontend calls the API directly from the browser (same domain via Traefik path routing). Users enter an API key which is stored in localStorage.

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | Postgres connection string | `postgresql://epstein_reader:epstein@localhost:5432/epstein` |
| `API_KEY` | API authentication key (optional) | *(none — no auth)* |

## Deployment

The included GitHub Actions workflow (`.github/workflows/deploy.yml`) builds a Docker image, pushes to GHCR, and deploys via SSH. It expects a Traefik reverse proxy for TLS.

Required secrets in a `deployment` environment:

| Secret | Description |
|---|---|
| `DEPLOY_HOST` | VPS hostname |
| `DEPLOY_USER` | SSH user |
| `DEPLOY_SSH_KEY` | SSH private key |
| `DOCKER_NETWORK` | Docker network (for Traefik) |
| `POSTGRES_PASSWORD` | Postgres admin password (for ingestion) |
| `READER_PASSWORD` | Postgres read-only password (used by API) |
| `API_KEY` | API authentication key |

## Backup & Restore

```bash
# Dump (SQL)
docker exec epstein-vector-pg pg_dump -U epstein epstein > backup.sql

# Restore (SQL)
docker exec -i epstein-vector-pg psql -U epstein epstein < backup.sql

# Dump (binary — faster for large DBs with vectors)
docker exec epstein-vector-pg pg_dump -U epstein -Fc epstein > backup.dump

# Restore (binary)
docker exec -i epstein-vector-pg pg_restore -U epstein -d epstein --clean < backup.dump
```

For remote servers, prefix with `ssh user@host`:

```bash
ssh deploy@yourserver "docker exec epstein-vector-pg pg_dump -U epstein -Fc epstein" > backup.dump
```

## Stack

- **Embedding model**: bge-large-en-v1.5 (1024 dims, 512 token limit)
- **Vector DB**: Postgres 17 + pgvector (halfvec for 50% storage savings)
- **Chunking**: ~200 words per chunk, 30 word overlap, contextual prefix, OCR quality filter (alpha ratio >50%)
- **Web UI**: Next.js 15, Tailwind v4, Framer Motion
- **API**: FastAPI + uvicorn
- **GPU server**: BentoML (tested on 2x GTX 1080Ti)
