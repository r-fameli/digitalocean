import json
import os
import uuid
import threading
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from durable import DurableStore, run_orchestrator

app = FastAPI()

INFERENCE_URL = os.environ.get("INFERENCE_URL", "http://localhost:8081/infer")
POOL_SIZE = int(os.environ.get("POOL_SIZE", "5"))
store = DurableStore()


class BatchInput(BaseModel):
    prompts: list[str]


class FileInput(BaseModel):
    path: str


@app.get("/")
def health_check():
    return {"status": "ok"}


@app.post("/")
def ingest_batch(batch: BatchInput):
    batch_id = str(uuid.uuid4())[:8]
    store.create_orchestration(batch_id, batch.prompts)
    threading.Thread(
        target=run_orchestrator,
        args=(batch_id, batch.prompts, INFERENCE_URL),
        daemon=True,
    ).start()
    return {"batch_id": batch_id, "status": "accepted", "count": len(batch.prompts)}


@app.post("/from-file")
def ingest_from_file(body: FileInput):
    path = body.path
    if not os.path.isfile(path):
        raise HTTPException(status_code=400, detail=f"File not found: {path}")
    try:
        with open(path, "r") as f:
            prompts = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON file: {e}")
    if not isinstance(prompts, list) or not all(isinstance(p, str) for p in prompts):
        raise HTTPException(status_code=400, detail="File must contain a JSON array of strings")
    batch_id = str(uuid.uuid4())[:8]
    store.create_orchestration(batch_id, prompts)
    threading.Thread(
        target=run_orchestrator,
        args=(batch_id, prompts, INFERENCE_URL),
        daemon=True,
    ).start()
    return {"batch_id": batch_id, "status": "accepted", "count": len(prompts)}


@app.get("/status/{batch_id}")
def get_status(batch_id: str):
    status = store.get_status(batch_id)
    if status is None:
        return {"error": "not found"}
    return status


@app.get("/results/{batch_id}")
def get_results(batch_id: str):
    data = store.get_result(batch_id)
    if data is None:
        return {"error": "not found or still processing"}
    return data
