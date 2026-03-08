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

from app.cache import TTLCache
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
    app.state.stream_cache = TTLCache[list[dict[str, Any]]](
        ttl_s=settings.stream_cache_ttl_s,
        max_entries=settings.stream_cache_max_entries,
    )
    app.state.probe_cache = TTLCache(
        ttl_s=settings.probe_cache_ttl_s,
        max_entries=settings.probe_cache_max_entries,
    )
    app.state.meta_cache = TTLCache(
        ttl_s=settings.meta_cache_ttl_s,
        max_entries=settings.meta_cache_max_entries,
    )
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
            logger.debug(
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
        t0 = monotonic()
        upstream = str(settings.upstream_base_url).rstrip("/")

        from urllib.parse import quote

        encoded_id = quote(id, safe="")
        url = f"{upstream}/stream/{type}/{encoded_id}.json"
        cache_key = f"{type}:{id}"
        cached_streams = app.state.stream_cache.get(cache_key)
        if cached_streams is not None:
            return StreamsResponse(streams=[dict(stream) for stream in cached_streams])
        resp: httpx.Response | None = None
        t_up = monotonic()
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
        upstream_ms = (monotonic() - t_up) * 1000

        if resp is None or resp.status_code != 200:
            raise HTTPException(
                status_code=502, detail=f"upstream_status:{getattr(resp, 'status_code', None)}"
            )

        data = resp.json()
        streams = data.get("streams") if isinstance(data, dict) else None
        if not isinstance(streams, list):
            return StreamsResponse(streams=[])

        ranked, stats = await reorder_streams(
            streams=streams,
            client=app.state.client,
            settings=settings,
            sem=app.state.sem,
            probe_cache=app.state.probe_cache,
            meta_cache=app.state.meta_cache,
        )
        app.state.stream_cache.set(cache_key, [dict(stream) for stream in ranked])
        total_ms = (monotonic() - t0) * 1000
        logger.debug(
            "stream pipeline total=%.1fms upstream=%.1fms rank=%.1fms p1=%.1fms p2=%.1fms p3=%.1fms streams=%d probed=%d kept=%d p2=%d ok=%d p3=%d fail=%d",
            total_ms,
            upstream_ms,
            stats.total_ms,
            stats.p1_ms,
            stats.p2_ms,
            stats.p3_ms,
            stats.streams_in,
            stats.probed,
            stats.kept,
            stats.p2_attempted,
            stats.p2_ok,
            stats.p3_attempted,
            stats.p3_fail,
        )
        if not ranked:
            logger.warning(
                "stream pipeline produced no streams upstream=%.1fms total=%.1fms streams_in=%d candidates=%d probed=%d kept=%d",
                upstream_ms,
                total_ms,
                stats.streams_in,
                stats.candidate_urls,
                stats.probed,
                stats.kept,
            )
        return StreamsResponse(streams=ranked)

    return app


__all__ = ["create_app"]
