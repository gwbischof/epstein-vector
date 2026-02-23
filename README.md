# epstein-vector

Semantic search over ~1M DOJ Epstein Library documents using pgvector.

## Architecture

```
Ingestion (two-stage):
  Stage 1 (CPU): Stream JSONL → POST doc rows to API → Version-gated upsert into Postgres
  Stage 2 (GPU): Fetch pending docs from API → Chunk text → Embed on GPU → POST chunks to API

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

Full-text keyword search — finds exact word matches. Returns chunk-level results with `chunk_index` and `total_chunks`.

```bash
curl -X POST http://localhost:8000/text_search \
  -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"query": "Maxwell flight", "limit": 10}'
```

Response includes: `efta_id`, `dataset`, `chunk_index`, `total_chunks`, `word_count`, `rank`, `headline`.

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

#### POST /ingest/documents

Upsert document rows with version-gated overwrites. Documents are only updated if the incoming `version` is strictly greater than the existing version — same or lower version is a no-op. This makes the endpoint fully idempotent. Requires `INGEST_API_KEY`.

```bash
curl -X POST http://localhost:8000/ingest/documents \
  -H "X-API-Key: $INGEST_API_KEY" -H "Content-Type: application/json" \
  -d '{"documents": [{"efta_id": "EFTA00123456", "dataset": 9, "word_count": 186, "version": 1}]}'
```

#### GET /ingest/documents/pending

Returns efta_ids for a dataset filtered by chunking status. Requires `INGEST_API_KEY`.

```bash
# Docs with zero chunks (default)
curl "http://localhost:8000/ingest/documents/pending?dataset=9" -H "X-API-Key: $INGEST_API_KEY"

# All docs in dataset
curl "http://localhost:8000/ingest/documents/pending?dataset=9&status=all" -H "X-API-Key: $INGEST_API_KEY"
```

Parameters: `dataset` (required), `status` (`pending` or `all`, default `pending`), `limit` (default 10000), `offset` (default 0).

Response: `{"efta_ids": [...], "count": 12345}`

#### POST /ingest/documents/fetch

Returns full document records by efta_id list (max 50). Used by the GPU worker to get text for chunking. Requires `INGEST_API_KEY`.

```bash
curl -X POST http://localhost:8000/ingest/documents/fetch \
  -H "X-API-Key: $INGEST_API_KEY" -H "Content-Type: application/json" \
  -d '{"efta_ids": ["EFTA00123456", "EFTA00123457"]}'
```

Response: `[{"efta_id": "...", "dataset": 9, "url": "...", "pages": 5, "word_count": 186, "text": "...", "version": 1}]`

#### POST /ingest/chunks

Accept pre-embedded chunks without document metadata. Validates embedding dimensions, casts to halfvec, uses ON CONFLICT DO NOTHING (or deletes+reinserts with `overwrite: true`). Requires `INGEST_API_KEY`.

```bash
curl -X POST http://localhost:8000/ingest/chunks \
  -H "X-API-Key: $INGEST_API_KEY" -H "Content-Type: application/json" \
  -d '{"chunks": [{"efta_id": "EFTA00123456", "chunk_index": 0, "total_chunks": 1, "text": "...", "embedding": [...]}]}'
```

Response: `{"inserted_chunks": 1}`

#### POST /ingest/chunk_status

Check per-document chunk status for a batch of efta_ids (max 50). Returns whether the document row exists, chunk counts, embedding counts, and version info. Requires `INGEST_API_KEY`.

```bash
curl -X POST http://localhost:8000/ingest/chunk_status \
  -H "X-API-Key: $INGEST_API_KEY" -H "Content-Type: application/json" \
  -d '{"efta_ids": ["EFTA00123456", "EFTA00123457"]}'
```

Response: `[{"efta_id": "...", "has_doc": true, "doc_word_count": 186, "doc_version": 1, "chunk_count": 3, "embedded_count": 3, "chunk_version": 1}]`

#### GET /health

```bash
curl http://localhost:8000/health
```

## How Ingestion Works

Ingestion is split into two independent stages:

### Stage 1: Document Loading (CPU)

`ingest_docs.py` streams JSONL files line by line and bulk-loads document rows into the `documents` table via `POST /ingest/documents`. No GPU needed.

```bash
python -m client.ingest_docs API_KEY                   # all datasets
python -m client.ingest_docs API_KEY --datasets 9,10   # specific datasets
python -m client.ingest_docs API_KEY --version 2       # set version (for OCR v2)
```

Documents are version-gated: only updated if the incoming version is strictly greater than the existing version. This makes it safe to re-run at any time — Postgres handles dedup.

Data sizes: ~3.6 GB total, 1.2M docs across 12 datasets. The big three are set_9 (463k docs, 1.7 GB), set_10 (496k docs, 1.4 GB), and set_11 (226k docs, 420 MB). JSONL is streamed line by line to avoid loading multi-GB files into memory.

### Stage 2: Chunk Embedding (GPU)

`ingest_chunks.py` fetches pending documents (those with zero chunks) from the API, chunks the text, embeds on GPU, and uploads chunks.

```bash
python -m client.ingest_chunks
python -m client.ingest_chunks --check   # verify and fix existing data first
```

The GPU worker never downloads JSONL — it reads documents from the database via the API. This means documents must be loaded first (Stage 1).

### Pipeline

```
Stage 1 (CPU):
  JSONL file ──stream──> POST /ingest/documents ──> documents table

Stage 2 (GPU):
  GET /ingest/documents/pending ──> fetch doc text ──> chunk ──> embed on GPU ──> POST /ingest/chunks ──> chunks table
```

**Chunking** — Each document is split into ~200-word chunks (~1000 chars) with 30-word overlap using `RecursiveCharacterTextSplitter`. Each chunk gets a contextual prefix: `[EFTA00123456 | Dataset 9]`.

  - **Short docs** (< 200 words): embedded as a single chunk
  - **Long docs**: split into multiple chunks with overlap for context continuity
  - **Sentinel chunks**: documents that fail quality checks (< 5 words, or < 50% alphabetic characters) get a single chunk with `skip_embedding=True`. The document text is still stored in the chunk so it appears in text search and fuzzy search via the `tsv` column — it just doesn't get a vector embedding, so it won't appear in vector/similarity search.

**Extracted field inference** — The `extracted` field exists in JSONL but not in the `documents` table. It has a perfect 1:1 correlation with `word_count >= 5`. The GPU worker infers `extracted = (word_count >= 5)` when reconstructing docs for chunking.

**Embedding** — Embeddable chunks are encoded with bge-large-en-v1.5 (1024 dims) on the local GPU. Multi-GPU is supported — chunks are interleaved across devices and run in parallel.

### Database Schema

```
documents                          chunks
├── efta_id (PK)                   ├── efta_id (FK → documents)
├── dataset                        ├── chunk_index
├── url                            ├── total_chunks
├── pages                          ├── text
├── word_count                     ├── embedding (halfvec(1024), nullable)
├── text                           ├── tsv (tsvector, auto-generated)
└── version                        └── version
                                   PK: (efta_id, chunk_index)
```

- `embedding` is `halfvec(1024)` — half-precision vectors for 50% storage savings with HNSW index
- `tsv` is auto-populated by a trigger from `text` using `to_tsvector('english', text)`, with a GIN index for full-text search
- Sentinel chunks have `embedding = NULL` — excluded from vector/similarity search by `WHERE embedding IS NOT NULL`, but included in text/fuzzy search via `tsv`

## Distributed GPU Ingestion

Contributors run a Docker container on their own GPU to embed documents. The container fetches pending docs from the API, embeds locally, and uploads chunks — no database credentials or JSONL downloads needed.

```
Container (your GPU)                 Server (vector.korroni.cloud)
1. GET /documents/pending     ───>  Returns efta_ids needing chunks
2. POST /documents/fetch      ───>  Returns full doc records with text
3. Chunk + embed (local GPU)
4. POST /ingest/chunks        ───>  Inserts chunks into Postgres
5. Repeat until done
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

No data volume mount needed — the GPU worker reads documents from the API, not from local JSONL files.

### Multi-GPU

`--gpus all` makes GPUs visible to the container. `CUDA_DEVICES` controls which ones the worker uses.

```bash
# Both GPUs in one container
docker run --gpus all -e CUDA_DEVICES="0,1" -e API_KEY="key" -e DATASETS="9" epstein-ingest

# Or separate containers per GPU
docker run --gpus all -e CUDA_DEVICES="0" -e API_KEY="key" -e DATASETS="9" epstein-ingest
docker run --gpus all -e CUDA_DEVICES="1" -e API_KEY="key" -e DATASETS="9" epstein-ingest
```

### Check mode

Run with `--check` to verify and fix existing data before normal ingestion. This is useful after code changes (e.g. updated chunking logic or a new schema migration) where existing data might not match the expected state.

```bash
docker run --gpus all \
  -e API_URL="https://vector.korroni.cloud" \
  -e API_KEY="your-ingest-api-key" \
  -e DATASETS="9" \
  epstein-ingest python -m client.ingest_chunks --check
```

How it works:

1. Fetches all doc IDs for the dataset via `GET /ingest/documents/pending?status=all`
2. For each batch of 50 IDs, fetches full docs via `POST /ingest/documents/fetch`
3. Runs `chunk_document()` locally to compute the expected chunks
4. Queries the actual state from the API via `POST /ingest/chunk_status`
5. Compares expected vs actual and flags any mismatches:

| Check | Condition | Meaning |
|-------|-----------|---------|
| `missing_chunks` | Doc exists but has 0 chunks | Chunks were lost or never inserted |
| `wrong_chunk_count` | Chunk count doesn't match expected | Chunking logic changed |
| `missing_embeddings` | Fewer embeddings than expected | Some embeddable chunks have NULL embedding |
| `stale_metadata` | `word_count` doesn't match | Document metadata is outdated |
| `version_mismatch` | `doc_version` or `chunk_version` < 1 | Pre-migration data without version column |

6. Re-embeds and uploads all flagged docs with `overwrite=True`
7. After check completes, normal ingestion runs to pick up any remaining pending docs

### Resumability

The worker fetches pending doc IDs (docs with zero chunks) before processing each dataset. Every 10 batches it re-fetches the pending list to skip docs completed by other concurrent workers. You can kill and restart at any time. Multiple workers on the same dataset are safe — `ON CONFLICT DO NOTHING` handles overlap.

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
