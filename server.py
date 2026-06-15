import os
import uuid
import time
import threading
import requests
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

INFERENCE_URL = os.environ.get("INFERENCE_URL", "http://localhost:8081/infer")
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))
POOL_SIZE = int(os.environ.get("POOL_SIZE", "5"))
results = {}
status_map = {}
status_lock = threading.Lock()


class BatchInput(BaseModel):
    prompts: list[str]


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


def process_batch(batch_id: str, prompts: list[str]):
    total = len(prompts)
    print(f"[{batch_id}] Processing {total} prompts with pool size {POOL_SIZE}...")

    with status_lock:
        status_map[batch_id] = {"completed": 0, "total": total}

    def worker(prompt: str):
        result = infer_with_retry(prompt)
        with status_lock:
            status_map[batch_id]["completed"] += 1
        return result

    with ThreadPoolExecutor(max_workers=POOL_SIZE) as executor:
        output = list(executor.map(worker, prompts))

    results[batch_id] = output
    print(f"[{batch_id}] Done.")


@app.post("/")
def ingest_batch(batch: BatchInput):
    batch_id = str(uuid.uuid4())[:8]
    threading.Thread(target=process_batch, args=(batch_id, batch.prompts), daemon=True).start()
    return {"batch_id": batch_id, "status": "accepted", "count": len(batch.prompts)}


@app.get("/status/{batch_id}")
def get_status(batch_id: str):
    with status_lock:
        status = status_map.get(batch_id)
    if status is None:
        return {"error": "not found"}
    return {"batch_id": batch_id, "completed": status["completed"], "total": status["total"]}


@app.get("/results/{batch_id}")
def get_results(batch_id: str):
    data = results.get(batch_id)
    if data is None:
        return {"error": "not found"}
    return data
