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
  -d '{"path": "/workspaces/sample_prompts.json"}'
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

The orchestrator processes prompts sequentially, but each activity call runs independently with its own retry logic. The `Sequential` execution is by design — it ensures deterministic replay. For parallelism, the orchestrator can be extended to fan out with `executor.map()` while still checkpointing each result to the events table.

### Retry / Backoff

On HTTP 429 (Too Many Requests) or connection errors, activities retry with **exponential backoff**: wait `initial_delay * backoff_multiplier^attempt` seconds, up to `max_retries` (default: 5). Prompts that exhaust retries are marked as failed but never silently dropped.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `INFERENCE_URL` | `http://localhost:8081/infer` | Inference API endpoint |
| `DURABLE_DB_PATH` | `orchestrator.db` | SQLite database for orchestration state |

## API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Health check |
| `POST` | `/` | Ingest batch (`{"prompts": [...]}`) |
| `POST` | `/from-file` | Ingest from JSON file (`{"path": "..."}`) |
| `GET` | `/status/{batch_id}` | Real-time progress (completed/total/status) |
| `GET` | `/results/{batch_id}` | Retrieve completed results |

## Running Tests

With the server and mock inference running:

```bash
python test_service.py
```

Tests verify: batch acceptance, result retrieval, checkpoint persistence, and crash-recovery readiness.

## CI/CD

- **`.github/workflows/test.yml`** — runs `pytest` on every push and PR to `main`
- **`.github/workflows/deploy.yml`** — deploys to DigitalOcean App Platform on push to `main`

## Deploy to DigitalOcean

```bash
doctl apps create --spec .do/app.yaml
```

Or connect your GitHub repo in the [App Platform dashboard](https://cloud.digitalocean.com/apps).
