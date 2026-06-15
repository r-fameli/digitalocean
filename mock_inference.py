import os
import time
import random
from fastapi import FastAPI, Query
from pydantic import BaseModel

app = FastAPI()

config = {
    "fail_rate": float(os.environ.get("MOCK_FAIL_RATE", "0.3")),
    "latency_ms": int(os.environ.get("MOCK_LATENCY_MS", "0")),
    "response_template": os.environ.get("MOCK_RESPONSE_TEMPLATE", "AI says: {reversed}"),
}


class InferenceRequest(BaseModel):
    prompt: str


@app.get("/")
def health_check():
    return {"status": "ok"}


@app.post("/infer")
def infer(req: InferenceRequest, fail_rate: float = Query(default=None, ge=0, le=1)):
    rate = fail_rate if fail_rate is not None else config["fail_rate"]
    if config["latency_ms"]:
        time.sleep(config["latency_ms"] / 1000)
    if random.random() < rate:
        return {"error": "Too Many Requests"}, 429
    return {
        "prompt": req.prompt,
        "response": config["response_template"].format(prompt=req.prompt, reversed=req.prompt[::-1]),
        "tokens_used": random.randint(10, 100),
    }


@app.get("/config")
def get_config():
    return config


@app.post("/config")
def set_config(fail_rate: float | None = None, latency_ms: int | None = None, response_template: str | None = None):
    if fail_rate is not None:
        config["fail_rate"] = fail_rate
    if latency_ms is not None:
        config["latency_ms"] = latency_ms
    if response_template is not None:
        config["response_template"] = response_template
    return config
