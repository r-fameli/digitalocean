import uuid
import time
import threading
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

results = {}


class BatchInput(BaseModel):
    prompts: list[str]


def process_batch(batch_id: str, prompts: list[str]):
    print(f"[{batch_id}] Processing {len(prompts)} prompts...")
    output = []
    for prompt in prompts:
        time.sleep(0.01)
        output.append({"prompt": prompt, "response": f"echo: {prompt}"})
    results[batch_id] = output
    print(f"[{batch_id}] Done.")


@app.post("/")
def ingest_batch(batch: BatchInput):
    batch_id = str(uuid.uuid4())[:8]
    threading.Thread(target=process_batch, args=(batch_id, batch.prompts), daemon=True).start()
    return {"batch_id": batch_id, "status": "accepted", "count": len(batch.prompts)}


@app.get("/results/{batch_id}")
def get_results(batch_id: str):
    data = results.get(batch_id)
    if data is None:
        return {"error": "not found"}
    return data
