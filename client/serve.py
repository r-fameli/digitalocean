import os
import requests
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response

BACKEND_URL = os.environ.get("BACKEND_URL", "http://127.0.0.1:8080")
CLIENT_DIR = os.path.dirname(os.path.abspath(__file__))
executor = ThreadPoolExecutor(max_workers=10)

app = FastAPI()


@app.get("/", response_class=HTMLResponse)
async def index():
    with open(os.path.join(CLIENT_DIR, "index.html"), "r") as f:
        return f.read()


def _proxy_sync(method: str, url: str, headers: dict, body: bytes):
    resp = requests.request(
        method=method,
        url=url,
        headers=headers,
        data=body,
        timeout=30,
    )
    return resp.status_code, dict(resp.headers), resp.content


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(path: str, request: Request):
    url = f"{BACKEND_URL}/{path}"
    body = await request.body()
    headers = {k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length")}

    loop = __import__("asyncio").get_running_loop()
    status_code, resp_headers, content = await loop.run_in_executor(
        executor,
        _proxy_sync,
        request.method,
        url,
        headers,
        body,
    )
    return Response(
        content=content,
        status_code=status_code,
        headers=resp_headers,
    )
