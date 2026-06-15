import json
import os
import uuid
import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

INFERENCE_URL = os.environ.get("INFERENCE_URL", "http://localhost:8081/infer")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
POOL_SIZE = int(os.environ.get("POOL_SIZE", "5"))
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/workspaces/output")
results = {}
status_map = {}
status_lock = threading.Lock()


class BatchInput(BaseModel):
    prompts: list[str]


class FileInput(BaseModel):
    path: str


@app.get("/")
def health_check():
    return {"status": "ok"}


def infer_with_retry(prompt: str) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(INFERENCE_URL, json={"prompt": prompt}, timeout=5)
            if resp.status_code == 429:
                wait = 2 ** attempt
                print(f"      429 on '{prompt[:30]}...' - retry in {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            return resp.json()
        except requests.RequestException as e:
            wait = 2 ** attempt
            print(f"      Error on '{prompt[:30]}...' - {e}, retry in {wait}s")
            time.sleep(wait)
    return {"prompt": prompt, "response": "FAILED after max retries"}


def persist_results(batch_id: str, output: list[dict]):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, f"{batch_id}.json")
    tmp = filepath + ".tmp"
    with open(tmp, "w") as f:
        json.dump(output, f, indent=2)
    os.replace(tmp, filepath)
    print(f"[{batch_id}] Results written to {filepath}")


def process_batch(batch_id: str, prompts: list[str]):
    total = len(prompts)
    print(f"[{batch_id}] Processing {total} prompts with pool size {POOL_SIZE}...")

    with status_lock:
        status_map[batch_id] = {"completed": 0, "total": total, "status": "in_progress"}

    def worker(prompt: str):
        result = infer_with_retry(prompt)
        with status_lock:
            status_map[batch_id]["completed"] += 1
        return result

    with ThreadPoolExecutor(max_workers=POOL_SIZE) as executor:
        output = list(executor.map(worker, prompts))

    results[batch_id] = output
    with status_lock:
        status_map[batch_id]["status"] = "completed"
    persist_results(batch_id, output)
    print(f"[{batch_id}] Done.")


@app.post("/")
def ingest_batch(batch: BatchInput):
    batch_id = str(uuid.uuid4())[:8]
    threading.Thread(target=process_batch, args=(batch_id, batch.prompts), daemon=True).start()
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
    threading.Thread(target=process_batch, args=(batch_id, prompts), daemon=True).start()
    return {"batch_id": batch_id, "status": "accepted", "count": len(prompts)}


@app.get("/status/{batch_id}")
def get_status(batch_id: str):
    with status_lock:
        status = status_map.get(batch_id)
    if status is None:
        return {"error": "not found"}
    return {
        "batch_id": batch_id,
        "completed": status["completed"],
        "total": status["total"],
        "status": status["status"],
    }


@app.get("/results/{batch_id}")
def get_results(batch_id: str):
    data = results.get(batch_id)
    if data is None:
        filepath = os.path.join(OUTPUT_DIR, f"{batch_id}.json")
        if os.path.isfile(filepath):
            with open(filepath, "r") as f:
                return json.load(f)
        return {"error": "not found"}
    return data
