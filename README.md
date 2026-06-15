# Batch Inference Engine

A backend service that ingests a batch of AI prompts, processes them concurrently against a mock inference endpoint with retry logic, and returns results.

## Setup

```bash
pip install -r requirements.txt
```

## Local Development

Start the mock inference API:

```bash
uvicorn mock_inference:app --host 0.0.0.0 --port 8081
```

Start the server (in another terminal):

```bash
uvicorn server:app --host 0.0.0.0 --port 8080
```

## Run Tests

With the server and mock inference running:

```bash
python test_service.py
```

## Deploy to DigitalOcean App Platform

This project includes a `.do/app.yaml` app spec. Deploy via:

```bash
doctl apps create --spec .do/app.yaml
```

Or connect your GitHub repo in the [App Platform dashboard](https://cloud.digitalocean.com/apps). App Platform will auto-detect Python, install from `requirements.txt`, and run the server. The `PORT` environment variable is injected automatically by App Platform.

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `INFERENCE_URL` | `http://localhost:8081/infer` | URL of the inference endpoint |
| `MAX_RETRIES` | `5` | Max retry attempts on 429 errors |
