# Batch Inference Engine

Backend service that ingests batches of AI prompts, processes them in parallel against a mock inference endpoint with retry logic, and aggregates results. Uses an event-sourced durable orchestrator backed by SQLite.

See [architecture/architecture.md](architecture/architecture.md) for the full design, system diagram, and data flow.

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

## API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Health check |
| `POST` | `/` | Ingest batch (`{"prompts": [...]}`) |
| `POST` | `/from-file` | Ingest from JSON file (`{"path": "..."}`) |
| `GET` | `/status/{batch_id}` | Real-time progress (completed/total/status) |
| `GET` | `/results/{batch_id}` | Retrieve completed results |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `INFERENCE_URL` | `http://localhost:8081/infer` | Inference API endpoint |
| `DURABLE_DB_PATH` | `orchestrator.db` | SQLite database path |
| `POOL_SIZE` | `5` | Bounded worker thread pool size |
| `TTL_DAYS` | `0` | Auto-delete orchestrations older than N days (0 = never) |

## Running Tests

### Unit tests (no servers needed)

```bash
python -m pytest tests/
```

### Integration tests (with server and mock inference running)

```bash
python tests/test_service.py
```

## CI/CD

- **`.github/workflows/test.yml`** — runs `pytest` on every push and PR to `main`
- **`.github/workflows/deploy.yml`** — deploys to DigitalOcean App Platform on push to `main`

## Deploy to DigitalOcean

```bash
doctl apps create --spec .do/app.yaml
```

Or connect your GitHub repo in the [App Platform dashboard](https://cloud.digitalocean.com/apps).
