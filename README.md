# Batch Inference Engine

A backend service that ingests a batch of AI prompts, processes them in the background, and returns results.

## Setup

```bash
pip install fastapi uvicorn requests
```

## Run the Server

```bash
uvicorn server:app --host 0.0.0.0 --port 8080
```

## Run Tests

With the server running in another terminal:

```bash
python test_service.py
```
