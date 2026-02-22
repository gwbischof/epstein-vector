# epstein-vector

Semantic search over ~1M DOJ Epstein Library documents using pgvector.

## Architecture

```
Ingestion (one-time, distributed across contributor GPUs):
  Download JSONL → Chunk text → Embed on GPU → POST to API → Insert into Postgres

Search (production):
  User query → FastAPI → Embed on CPU → pgvector search → Return results
```

## API

```bash
# Start with Docker
docker compose up -d

# Or run directly
uv sync --extra api
DATABASE_URL=postgresql://epstein:epstein@localhost:5432/epstein uvicorn api.main:app --reload
```

Set `API_KEY` to require authentication. If unset, all endpoints are open.

### Endpoints

All search endpoints accept `POST` with JSON body and require `X-API-Key` header.

**Common parameters** (all endpoints):
- `limit` (default 20, max 100): number of results
- `offset` (default 0): skip results for pagination
- `dataset` (optional): filter to specific dataset number

#### POST /vector_search

Semantic search — finds documents by meaning.

```bash
curl -X POST http://localhost:8000/vector_search \
  -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"query": "flight logs", "limit": 10, "dataset": 9}'
```

#### POST /text_search

Full-text keyword search — finds exact word matches.

```bash
curl -X POST http://localhost:8000/text_search \
  -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"query": "Maxwell flight", "limit": 10}'
```

Query syntax:

| Syntax | Example | Behavior |
|---|---|---|
| Plain terms | `Maxwell flight` | AND — both words must appear |
| Exact phrase | `"wire transfer"` | Phrase match |
| OR | `Maxwell OR Brunel` | Either term |
| NOT | `island -vacation` | Exclude a term |
| Wildcard | `maxw*` | Prefix match |

#### POST /fuzzy_search

Trigram search — typo-tolerant matching for OCR errors and misspellings.

```bash
curl -X POST http://localhost:8000/fuzzy_search \
  -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"query": "Maxwel", "limit": 10}'
```

Additional parameter: `exclude_exact` (default false) — exclude documents that keyword search already matches.

#### POST /similarity_search

Find documents similar to a given chunk — uses the existing embedding without re-encoding.

```bash
curl -X POST http://localhost:8000/similarity_search \
  -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"efta_id": "EFTA00123456", "chunk_index": 0, "limit": 10}'
```

Additional parameters: `efta_id` (required), `chunk_index` (default 0).

#### GET /ingest/stats

Embedded document and chunk counts. Requires `INGEST_API_KEY`.

```bash
curl http://localhost:8000/ingest/stats -H "X-API-Key: $INGEST_API_KEY"
curl "http://localhost:8000/ingest/stats?dataset=9" -H "X-API-Key: $INGEST_API_KEY"
```

#### GET /health

```bash
curl http://localhost:8000/health
```

## Distributed GPU Ingestion

Contributors run a Docker container on their own GPU to embed documents. The container downloads source data, embeds locally, and uploads results to the API — no database credentials needed.

```
Container (your GPU)                 Server (vector.korroni.cloud)
1. Download JSONL locally
2. GET /ingest/done?dataset=9  ───>  Returns set of completed efta_ids
3. Skip done docs
4. Chunk + embed (local GPU)
5. POST /ingest               ───>  Inserts docs + chunks into Postgres
6. Repeat until done
```

### Build

```bash
docker build -f Dockerfile.ingest -t epstein-ingest .
```

First build downloads PyTorch + CUDA (~2GB) and model weights (~1.3GB) — expect 5-10 minutes. Rebuilds after code changes are fast (cached layers).

**Blackwell GPUs (RTX 50 series)** need PyTorch nightly with CUDA 12.8:

```bash
docker build -f Dockerfile.ingest -t epstein-ingest --build-arg TORCH_INDEX=nightly/cu128 .
```

The default (`cu118`) works for Pascal, Turing, and Ampere GPUs (GTX 10xx, RTX 20xx/30xx/40xx).

### Run

```bash
docker run --gpus all \
  -e API_URL="https://vector.korroni.cloud" \
  -e API_KEY="your-ingest-api-key" \
  -e DATASETS="9" \
  epstein-ingest
```

To persist downloaded JSONL files between runs (avoids re-downloading), mount a volume:

```bash
docker run --gpus all \
  -e API_URL="https://vector.korroni.cloud" \
  -e API_KEY="your-ingest-api-key" \
  -e DATASETS="9" \
  -v /path/to/storage:/app/data \
  epstein-ingest
```

### Multi-GPU

`--gpus all` makes GPUs visible to the container. `CUDA_DEVICES` controls which ones the worker uses.

```bash
# Both GPUs in one container
docker run --gpus all -e CUDA_DEVICES="0,1" -e API_KEY="key" -e DATASETS="9" epstein-ingest

# Or separate containers per GPU
docker run --gpus all -e CUDA_DEVICES="0" -e API_KEY="key" -e DATASETS="9" epstein-ingest
docker run --gpus all -e CUDA_DEVICES="1" -e API_KEY="key" -e DATASETS="9" epstein-ingest
```

### Resumability

The worker checks `/ingest/done` before processing each dataset and skips already-embedded documents. You can kill and restart at any time. Multiple workers on the same dataset are safe — `ON CONFLICT DO NOTHING` handles overlap.

### Container environment variables

| Variable | Description | Default |
|---|---|---|
| `API_URL` | Vector API base URL | `https://vector.korroni.cloud` |
| `API_KEY` | Ingest API key (get from maintainer) | *(required)* |
| `DATASETS` | Comma-separated dataset numbers | `1,2,3,4,5,6,7,8,9,10,11,12` |
| `CUDA_DEVICES` | GPU indices, or `cpu` | `0` |

## Web Frontend

A search UI at `web/` — Next.js with semantic + keyword search, infinite scroll, similarity search, and links to DOJ source PDFs.

```bash
cd web && npm install && npm run dev
```

The frontend calls the API directly from the browser (same domain via Traefik path routing). Users enter an API key which is stored in localStorage.

## Deployment

The GitHub Actions workflow (`.github/workflows/deploy.yml`) builds a Docker image, pushes to GHCR, and deploys via SSH. It expects a Traefik reverse proxy for TLS.

### Server environment variables

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | Postgres connection string (read-only) | `postgresql://epstein_reader:...@localhost:5432/epstein` |
| `INGEST_DATABASE_URL` | Postgres connection string (write) | *(none — ingestion disabled)* |
| `API_KEY` | API key for search endpoints | *(none — no auth)* |
| `INGEST_API_KEY` | API key for ingestion endpoints | *(none — no auth)* |

### Required GitHub secrets (`deployment` environment)

| Secret | Description |
|---|---|
| `DEPLOY_HOST` | VPS hostname |
| `DEPLOY_USER` | SSH user |
| `DEPLOY_SSH_KEY` | SSH private key |
| `DOCKER_NETWORK` | Docker network (for Traefik) |
| `POSTGRES_PASSWORD` | Postgres admin password |
| `READER_PASSWORD` | Postgres read-only password |
| `API_KEY` | Search API key |
| `INGEST_API_KEY` | Ingestion API key |

## Backup & Restore

Automated backups run every 3 days at 4am UTC via cron, saving compressed dumps to `/mnt/nas/backups/epstein-vector/`. The 3 most recent backups are kept.

```bash
# Manual backup (run on the server)
docker exec epstein-vector-pg pg_dump -U epstein -Fc epstein | gzip > epstein-backup.dump.gz

# Restore
gunzip -c epstein-backup.dump.gz | docker exec -i epstein-vector-pg pg_restore -U epstein -d epstein --clean
```

## Stack

- **Embedding model**: bge-large-en-v1.5 (1024 dims, 512 token limit)
- **Vector DB**: Postgres 17 + pgvector (halfvec for 50% storage savings)
- **Chunking**: ~200 words per chunk, 30 word overlap, contextual prefix, OCR quality filter (alpha ratio >50%)
- **Web UI**: Next.js 15, Tailwind v4, Framer Motion
- **API**: FastAPI + uvicorn (4 workers)
- **GPU ingestion**: Distributed Docker containers (tested on GTX 1080 Ti, RTX 3070, RTX PRO 6000)
