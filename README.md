# Batch Inference Engine

Backend service that ingests batches of AI prompts, processes them concurrently against a mock inference endpoint with retry logic, and aggregates results.

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

The server uses a **bounded thread pool** (`concurrent.futures.ThreadPoolExecutor`) to process prompts in parallel. The pool size is controlled by the `POOL_SIZE` environment variable (default: 5).

- Each worker calls the inference API for one prompt at a time
- `executor.map()` submits all prompts and preserves input order in results
- The pool is bounded — at most N HTTP requests to the inference API run concurrently
- This prevents unbounded thread creation and protects system memory

### Retry / Backoff

On HTTP 429 (Too Many Requests) or connection errors, workers retry with **exponential backoff**: wait `2^attempt` seconds, up to `MAX_RETRIES` (default: 5). Prompts that exhaust retries are marked as failed but never silently dropped.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `INFERENCE_URL` | `http://localhost:8081/infer` | Inference API endpoint |
| `MAX_RETRIES` | `5` | Max retry attempts on 429/errors |
| `POOL_SIZE` | `5` | Bounded worker pool size |
| `OUTPUT_DIR` | `/workspaces/output` | Directory for persisted result files |

## API Reference

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Health check |
| `POST` | `/` | Ingest batch (`{"prompts": [...]}`) |
| `POST` | `/from-file` | Ingest from JSON file (`{"path": "..."}`) |
| `GET` | `/status/{batch_id}` | Real-time progress (completed/total/status) |
| `GET` | `/results/{batch_id}` | Retrieve results (memory first, disk fallback) |

## Running Tests

No external services needed — all tests use mocks:

```bash
python test_service.py
```

## CI/CD

GitHub Actions runs tests on every push to `main` (`.github/workflows/ci.yml`).

## Deploy to DigitalOcean

```bash
doctl apps create --spec .do/app.yaml
```

Or connect your GitHub repo in the [App Platform dashboard](https://cloud.digitalocean.com/apps).
