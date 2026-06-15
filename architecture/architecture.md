# Architecture: Batch Inference Engine

## Overview

A FastAPI backend that ingests batches of AI prompts, processes them in parallel via a bounded `ThreadPoolExecutor` against a mock inference API, handles 429 rate limits with exponential backoff, and serves results. Uses an event-sourced durable orchestrator backed by SQLite. Deploys to DigitalOcean App Platform with GitHub Actions CI/CD.

## System Diagram

``` 
                                     ┌────────────────────────┐
  Client                             │     server.py :8080    │
  ──────                             │                        │
  POST /  {"prompts": [...]} ───────►│  ingest_batch()        │
  POST /from-file {"path": "..."}───►│  ingest_from_file()    │
                                     │    │                   │
                                     │    ├─ batch_id = uuid  │
                                     │    └─ spawn daemon     │
                                     │       thread ──────────┤
                                     │                        │
                                     │  run_orchestrator()    │
                                     │                        │
                                     │  ThreadPoolExecutor    │
                                     │  max_workers=POOL_SIZE │
                                     │  ┌───────┐            │
                                     │  │worker1│──┐         │
                                     │  ├───────┤  │         │     ┌──────────────────────┐
                                     │  │worker2│  │         │     │ mock_inference :8081 │
                                     │  ├───────┤  │  POST   │     │                      │
                                     │  │  ...  │──┼─────────┼────►│ POST /infer          │
                                     │  ├───────┤  │         │     │   70% → 200 OK       │
                                     │  │workerN│  │         │     │   30% → 429          │
                                     │  └───────┘  │         │     └──────────────────────┘
                                     │             │         │
                                     │  retry loop │         │
                                     │  ┌─────────┐│         │
                                     │  │on 429:  ││         │
                                     │  │ sleep 2^n│◄────────┘
                                     │  │ retry    │
                                     │  └─────────┘
                                     │       │
                                     │  checkpoint via DurableStore
                                     │       │
                                     │       ▼
                                     │  ┌───────────────────┐
                                     │  │  orchestrator.db  │
                                     │  │  (SQLite)         │
                                     │  │                   │
                                     │  │  orchestrations   │
                                     │  │  orchestration_   │
                                     │  │    events         │
                                     │  └───────────────────┘
                                     │
  GET /status/{batch_id} ◄───────────┤  store.get_status()
  GET /results/{batch_id} ◄──────────┤  store.get_result()
                                     └────────────────────────┘

                         ┌─────────────────────────┐
                         │     GitHub Actions       │
                         │  test.yml  deploy.yml    │
                         └─────────────────────────┘
```

## Code Structure

```
backend/
  server.py              # FastAPI server — ingestion, kicks off orchestrator
  durable.py             # Durable orchestrator framework — event sourcing, ThreadPoolExecutor, SQLite store
  mock_inference.py      # Mock AI inference API (configurable fail rate, latency)

  scripts/
    start.sh             # App Platform entrypoint: starts mock + server
  tests/
    test_unit.py         # Unit tests: DurableStore CRUD, replay, TTL, failures
    test_service.py      # Integration tests: full POST/GET flow against live servers
  view_db.py             # Interactive SQLite database browser
  sample_prompts.json    # Sample prompts for /from-file testing
  .github/workflows/
    test.yml             # CI: install deps + run pytest on push
    deploy.yml           # CD: deploy to DO App Platform on push
  .do/
    app.yaml             # DO App Platform deployment spec
```

## Components

### 1. `server.py` — API Layer (FastAPI)

| Method | Path | Role |
|---|---|---|
| `GET /` | Health check | Returns `{"status": "ok"}` for App Platform liveness probes |
| `POST /` | Batch ingestion | Accepts `{"prompts": [...]}`, starts orchestrator in background thread, returns `{"batch_id": "...", "status": "accepted", "count": N}` |
| `POST /from-file` | File ingestion | Accepts `{"path": "/some/file.json"}`, reads JSON array, starts orchestrator |
| `GET /status/{id}` | Progress query | Reads from SQLite: `{batch_id, status, total, completed, created_at, updated_at}` |
| `GET /results/{id}` | Result retrieval | Returns completed output from SQLite, or error if not yet done |

### 2. `durable.py` — Durable Orchestrator Framework

Core framework implementing event-sourced orchestration, inspired by Azure Durable Functions.

#### `DurableStore`

Manages all SQLite operations via thread-local connections:
- `create_orchestration()` — inserts row (status=running)
- `complete_orchestration()` — sets status=completed, writes output JSON
- `fail_orchestration()` — sets status=failed with error
- `get_event()` / `save_event()` — checkpoint/read individual activity results
- `get_status()` — returns progress (counts events to derive completed)
- `get_result()` — returns final output if completed
- `cleanup_old()` — TTL-based deletion of stale orchestrations

#### `run_orchestrator()`

The orchestrator function fans out work across a bounded `ThreadPoolExecutor`:

1. Inserts orchestrations row (status=running)
2. Runs TTL cleanup if `TTL_DAYS > 0`
3. Submits each `(prompt_index, prompt)` to the thread pool (size = `POOL_SIZE`)
4. Each worker independently:
   - Checks DB for existing event at `(orchestration_id, prompt_index)` → skip if found (replay)
   - Calls mock inference API with exponential backoff retry
   - Saves successful result to `orchestration_events`
5. When all workers complete, sorts results by `prompt_index` (preserves input order)
6. Writes final output to `orchestrations` table (status=completed)

#### Replay Semantics

If the process crashes mid-batch and restarts with the same `batch_id`:
- Workers for already-saved prompt indices skip inference and return saved results
- Workers for unsaved indices call the API normally
- The `UNIQUE(orchestration_id, prompt_index)` constraint prevents duplicate events

### 3. `mock_inference.py` — Mock Inference API

A separate FastAPI server (port 8081) that simulates an external AI service:

| Endpoint | Method(s) | Purpose |
|---|---|---|
| `/` | GET | Health check |
| `/infer` | POST | Process a prompt; returns 429 with probability `fail_rate` (default 0.3) |
| `/config` | GET, POST | Get/set `fail_rate`, `latency_ms`, `response_template` at runtime |

Env vars: `MOCK_FAIL_RATE` (0.3), `MOCK_LATENCY_MS` (0), `MOCK_RESPONSE_TEMPLATE` (`"AI says: {reversed}"`).

### 4. SQLite Database (`orchestrator.db`)

```
orchestrations
├── id              TEXT PRIMARY KEY
├── status          TEXT               (pending | running | completed | failed)
├── input           TEXT               (JSON array of prompts)
├── output          TEXT               (JSON array of results, set on completion)
├── created_at      TEXT
└── updated_at      TEXT

orchestration_events
├── id              INTEGER PK
├── orchestration_id TEXT
├── prompt_index    INTEGER            (position in original prompt array)
├── activity_input  TEXT               (the prompt text)
├── activity_output TEXT               (the inference result)
└── created_at      TEXT

UNIQUE(orchestration_id, prompt_index) ← prevents duplicate work on replay
```

### 5. `scripts/start.sh`

For App Platform deployment: starts both `mock_inference` (port 8081) and `server` (port `$PORT`) in one container.

## Data Flow

```
1. Client sends POST / {prompts: [...]}
2. server.py generates batch_id, inserts orchestrations row (status=running)
3. Background thread calls run_orchestrator()
4. Orchestrator fans out to ThreadPoolExecutor (pool_size = POOL_SIZE)
5. Each worker independently:
   a. Check events table for (batch_id, prompt_index) — replay fast path
   b. POST /infer to mock_inference.py
   c. On 429: exponential backoff (up to 5 retries)
   d. Save result to orchestration_events (checkpoint)
6. All workers done → sort results by prompt_index → write orchestrations (status=completed)
7. Client polls GET /status/{id} (reads event count from DB)
8. Client calls GET /results/{id} (reads output JSON from DB)
```

## Concurrency Model

### Bounded Thread Pool

`run_orchestrator()` uses `concurrent.futures.ThreadPoolExecutor` with `max_workers` set to `POOL_SIZE` (default: **5**).

- All prompts are submitted to the pool at once
- Workers complete in any order — results sorted by `prompt_index` before writing final output
- The bounded pool prevents unbounded thread creation and protects system memory
- On HTTP 429 errors, only the affected worker waits and retries — other workers continue unaffected

### Retry Strategy

Exponential backoff per worker: `delay = 2^attempt` seconds

| Attempt | Delay |
|---|---|
| 0 | 1s |
| 1 | 2s |
| 2 | 4s |
| 3 | 8s |
| 4 | 16s |

After 5 failed attempts, the prompt is marked `FAILED after max retries` but never silently dropped.

### TTL Cleanup

Set `TTL_DAYS` to a positive number (e.g. `7`) to automatically delete orchestrations and their events older than that many days. Cleanup runs on each new batch ingestion. Set to `0` (default) to disable.

## Environment Variables

| Variable | Default | Used By | Description |
|---|---|---|---|
| `INFERENCE_URL` | `http://localhost:8081/infer` | `durable.py` | Inference API endpoint |
| `DURABLE_DB_PATH` | `orchestrator.db` | `durable.py` | SQLite database path |
| `POOL_SIZE` | `5` | `durable.py` | Bounded worker thread pool size |
| `TTL_DAYS` | `0` (never) | `durable.py` | Auto-delete old orchestrations |
| `MOCK_FAIL_RATE` | `0.3` | `mock_inference.py` | Probability of returning 429 |
| `MOCK_LATENCY_MS` | `0` | `mock_inference.py` | Artificial response delay |
| `MOCK_RESPONSE_TEMPLATE` | `AI says: {reversed}` | `mock_inference.py` | Response format string |

## Testing

### Unit Tests (`tests/test_unit.py`)

5 tests against `DurableStore` using temp SQLite files — no servers needed:
- Orchestration lifecycle (create → events → complete)
- Replay event skip (saved events are returned, unsaved return None)
- Missing orchestration lookups
- TTL cleanup
- Failure state handling

```bash
python -m pytest tests/
```

### Integration Tests (`tests/test_service.py`)

3 tests requiring running servers:
- Parallel execution with 10 prompts
- Two-batch replay verification
- TTL cleanup after completion

```bash
python tests/test_service.py
```

## CI/CD

- **`.github/workflows/test.yml`** — installs deps, runs `python -m pytest tests/` on every push and PR to `main`
- **`.github/workflows/deploy.yml`** — deploys to DigitalOcean App Platform via `digitalocean/app_action/deploy@v2` on push to `main`

## Deployment

```bash
doctl apps create --spec .do/app.yaml
```

Or connect the GitHub repo in the [App Platform dashboard](https://cloud.digitalocean.com/apps). App Platform auto-detects Python, installs dependencies, and runs `bash scripts/start.sh`.
