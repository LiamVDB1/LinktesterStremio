from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from time import monotonic
from typing import Any

import anyio
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse

from app.config import Settings
from app.http_client import build_async_client
from app.manifest import build_manifest
from app.ranking import reorder_streams
from app.schemas import HealthResponse, StreamsResponse


def create_app(
    *, settings: Settings | None = None, client: httpx.AsyncClient | None = None
) -> FastAPI:
    settings = settings or Settings()
    started = monotonic()
    logger = logging.getLogger("app")

    app = FastAPI(default_response_class=ORJSONResponse)
    app.state.settings = settings
    app.state.sem = anyio.Semaphore(settings.max_concurrency)
    if client is not None:
        app.state.client = client

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        close_client = False
        if not hasattr(app.state, "client"):
            app.state.client = build_async_client(settings)
            close_client = True
        try:
            yield
        finally:
            if close_client:
                await app.state.client.aclose()

    app.router.lifespan_context = lifespan
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def _log_requests(request, call_next):  # type: ignore[no-untyped-def]
        t0 = monotonic()
        response = None
        try:
            response = await call_next(request)
            return response
        finally:
            dt_ms = (monotonic() - t0) * 1000
            logger.info(
                "request %s %s %s %.1fms",
                request.method,
                request.url.path,
                getattr(response, "status_code", None),
                dt_ms,
            )

    @app.get("/healthz", response_model=HealthResponse)
    async def healthz() -> HealthResponse:
        return HealthResponse(status="ok", uptime_s=monotonic() - started)

    @app.get("/manifest.json")
    async def manifest() -> dict[str, Any]:
        return build_manifest(settings)

    @app.get("/stream/{type}/{id}.json", response_model=StreamsResponse)
    async def stream(type: str, id: str) -> StreamsResponse:
        upstream = str(settings.upstream_base_url).rstrip("/")

        from urllib.parse import quote

        encoded_id = quote(id, safe="")
        url = f"{upstream}/stream/{type}/{encoded_id}.json"
        resp: httpx.Response | None = None
        for attempt in range(2):
            try:
                with anyio.fail_after(settings.upstream_timeout_ms / 1000):
                    resp = await app.state.client.get(
                        url, timeout=settings.upstream_timeout_ms / 1000
                    )
            except (TimeoutError, httpx.RequestError):
                if attempt == 0:
                    continue
                raise HTTPException(status_code=502, detail="upstream_error") from None
            if resp.status_code >= 500 and attempt == 0:
                continue
            break

        if resp is None or resp.status_code != 200:
            raise HTTPException(
                status_code=502, detail=f"upstream_status:{getattr(resp, 'status_code', None)}"
            )

        data = resp.json()
        streams = data.get("streams") if isinstance(data, dict) else None
        if not isinstance(streams, list):
            return StreamsResponse(streams=[])

        ranked = await reorder_streams(
            streams=streams,
            client=app.state.client,
            settings=settings,
            sem=app.state.sem,
        )
        return StreamsResponse(streams=ranked)

    return app


__all__ = ["create_app"]
