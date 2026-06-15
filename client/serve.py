import os
import httpx2 as httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response

BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8080")
CLIENT_DIR = os.path.dirname(os.path.abspath(__file__))

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
async def index():
    with open(os.path.join(CLIENT_DIR, "index.html"), "r") as f:
        return f.read()

@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(path: str, request: Request):
    url = f"{BACKEND_URL}/{path}"
    body = await request.body()
    headers = dict(request.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)

    async with httpx.AsyncClient() as client:
        resp = await client.request(
            method=request.method,
            url=url,
            headers=headers,
            content=body,
            timeout=30.0,
        )
    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=dict(resp.headers),
    )
