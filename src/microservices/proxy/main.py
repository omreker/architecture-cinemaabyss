import os
import random
import requests
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import Response

app = FastAPI(title="CinemaAbyss Proxy Service")


def migrate_movies_service():
    if not os.getenv("GRADUAL_MIGRATION"):
        return False
    return random.randint(1, 100) <= int(os.getenv("MOVIES_MIGRATION_PERCENT"))


def proxy_request(request: Request, target_url: str, body: bytes):
    url = f"{target_url}{request.url.path}"
    if request.url.query:
        url += f"?{request.url.query}"

    headers = {k: v for k, v in request.headers.items() if k.lower() != 'host'}

    response = requests.request(
        method=request.method,
        url=url,
        headers=headers,
        data=body
    )

    return Response(content=response.content, status_code=response.status_code, headers=response.headers)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def gateway(full_path: str, request: Request):

    path = f"/{full_path}"

    body = await request.body()

    if path.startswith("/api/events"):
        print(f"Proxy: {path} -> Events Service")
        return proxy_request(request, os.getenv("EVENTS_SERVICE_URL"), body)

    if path.startswith("/api/movies"):
        if migrate_movies_service():
            print(f"Proxy: {path} -> Movies Service (Migration ON)")
            return proxy_request(request, os.getenv("MOVIES_SERVICE_URL"), body)
        else:
            print(f"Proxy: {path} -> Monolith")
            return proxy_request(request, os.getenv("MONOLITH_URL"), body)

    return proxy_request(request, os.getenv("MONOLITH_URL"), body)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
