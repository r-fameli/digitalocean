# Batch Inference Engine

Backend service that ingests batches of AI prompts, processes them against a mock inference endpoint with retry logic, and aggregates results. Uses an event-sourced durable orchestrator that survives restarts.

## Architecture

This project implements a **durable orchestrator pattern** (inspired by Azure Durable Functions) with SQLite as the backing store:

```
POST /  →  Orchestrator (fan-out)  →  Activity: call /infer  →  Activity: call /infer  →  ...
                │                           │                           │
                └── checkpoint ─────────────┴── checkpoint ─────────────┴── checkpoint  →  SQLite
```

- **Orchestrator** — receives a batch of prompts, fans out to activity calls, checkpoints after each one
- **Activity** — calls the inference API with exponential backoff retry on 429s
- **Event sourcing** — each completed activity is recorded in `orchestration_events`. If the process crashes, the orchestrator replays from history and skips already-completed work
- **Status API** — queries real-time progress from the database (survives restarts)

## Setup

```bash
pip install -r requirements.txt
```

## Running Locally

Start the mock inference API:

```bash
uvicorn mock_inference:app --host 0.0.0.0 --port 8081
```

Start the server (in a second terminal):

```bash
uvicorn server:app --host 0.0.0.0 --port 8080
```

## Usage

### Ingest a batch via API

```bash
curl -X POST http://localhost:8080/ \
  -H "Content-Type: application/json" \
  -d '{"prompts": ["What is the capital of France?", "Explain quantum computing."]}'
# → {"batch_id":"a1b2c3d4","status":"accepted","count":2}
```

### Ingest from a JSON file

```bash
curl -X POST http://localhost:8080/from-file \
  -H "Content-Type: application/json" \
  -d '{"path": "/workspaces/backend/sample_prompts.json"}'
```

### Check batch progress

```bash
curl http://localhost:8080/status/a1b2c3d4
# → {"batch_id":"a1b2c3d4","completed":2,"total":2,"status":"completed"}
```

### Retrieve results

```bash
curl http://localhost:8080/results/a1b2c3d4
```

## Concurrency Model

The orchestrator uses a **bounded thread pool** (`ThreadPoolExecutor`) to process prompts in parallel. Each worker independently handles one prompt at a time, including its retry/backoff logic. The pool size is controlled by `POOL_SIZE` (default: 5).

- All prompts are submitted to the pool at once
- Workers complete in any order — results are sorted by original array index before writing the final output
- The bounded pool prevents unbounded thread creation and protects system resources
- On HTTP 429 errors, only the affected worker waits and retries — other workers continue unaffected

### Retry / Backoff

On HTTP 429 (Too Many Requests) or connection errors, each worker retries with **exponential backoff**: wait `2^attempt` seconds, up to 5 attempts. Prompts that exhaust retries are marked `FAILED after max retries` but never silently dropped.

### Replay / Crash Recovery

Each completed activity writes a row to `orchestration_events` with `UNIQUE(orchestration_id, prompt_index)`. If an orchestration is restarted (e.g. after a crash), workers check the events table before calling the inference API — already-completed prompts are skipped and their saved results are returned immediately.

### TTL Cleanup

Set `TTL_DAYS` to a positive number (e.g. `7`) to automatically delete orchestrations and their events older than that many days. Cleanup runs on each new batch ingestion.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `INFERENCE_URL` | `http://localhost:8081/infer` | Inference API endpoint |
| `DURABLE_DB_PATH` | `orchestrator.db` | SQLite database for orchestration state |
| `POOL_SIZE` | `5` | Bounded worker thread pool size |
| `TTL_DAYS` | `0` | Auto-delete orchestrations older than N days (0 = never) |

## API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Health check |
| `POST` | `/` | Ingest batch (`{"prompts": [...]}`) |
| `POST` | `/from-file` | Ingest from JSON file (`{"path": "..."}`) |
| `GET` | `/status/{batch_id}` | Real-time progress (completed/total/status) |
| `GET` | `/results/{batch_id}` | Retrieve completed results |

## Running Tests

### Unit tests (no servers needed)

```bash
python -m pytest tests/
```

### Integration tests (with server and mock inference running)

```bash
python tests/test_service.py
```

Tests verify: batch acceptance, result retrieval, parallel execution, checkpoint persistence, and TTL cleanup.

## CI/CD

- **`.github/workflows/test.yml`** — runs `pytest` on every push and PR to `main`
- **`.github/workflows/deploy.yml`** — deploys to DigitalOcean App Platform on push to `main`

## Deploy to DigitalOcean

```bash
doctl apps create --spec .do/app.yaml
```

Or connect your GitHub repo in the [App Platform dashboard](https://cloud.digitalocean.com/apps).
