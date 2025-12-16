from __future__ import annotations

import httpx

from app.config import Settings


def build_async_client(settings: Settings) -> httpx.AsyncClient:
    headers = {"User-Agent": settings.user_agent, "Accept": "*/*"}
    limits = httpx.Limits(max_keepalive_connections=50, max_connections=200)
    timeout = httpx.Timeout(connect=2.0, read=2.0, write=2.0, pool=2.0)
    return httpx.AsyncClient(
        headers=headers,
        limits=limits,
        timeout=timeout,
        follow_redirects=True,
        max_redirects=settings.max_redirects,
    )
