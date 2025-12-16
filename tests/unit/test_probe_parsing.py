from __future__ import annotations

import anyio
import httpx
import pytest
import respx

from app.config import Settings
from app.probe import range_probe_1kb
from tests.ebml_builder import build_mini_mkv_bytes


@pytest.mark.asyncio
async def test_range_probe_accepts_206_and_mkv_magic() -> None:
    settings = Settings(UPSTREAM_BASE_URL="http://upstream.invalid")
    sem = anyio.Semaphore(10)
    mkv = build_mini_mkv_bytes()[:2048]

    with respx.mock(assert_all_called=True) as rsps:
        rsps.get("https://example.test/video").respond(
            206,
            headers={"Content-Range": "bytes 0-1024/9999", "Accept-Ranges": "bytes"},
            content=mkv,
        )
        async with httpx.AsyncClient() as client:
            r = await range_probe_1kb(
                url="https://example.test/video", client=client, settings=settings, sem=sem
            )
    assert r.ok
    assert r.magic_mkv
    assert r.seekable
    assert r.total_size == 9999


@pytest.mark.asyncio
async def test_range_probe_accepts_200_when_seekable() -> None:
    settings = Settings(UPSTREAM_BASE_URL="http://upstream.invalid")
    sem = anyio.Semaphore(10)
    mkv = build_mini_mkv_bytes()[:2048]

    with respx.mock(assert_all_called=True) as rsps:
        rsps.get("https://example.test/video200").respond(
            200,
            headers={"Accept-Ranges": "bytes", "Content-Length": "9999"},
            content=mkv,
        )
        async with httpx.AsyncClient() as client:
            r = await range_probe_1kb(
                url="https://example.test/video200", client=client, settings=settings, sem=sem
            )
    assert r.ok
    assert r.seekable
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_range_probe_drops_htmlish() -> None:
    settings = Settings(UPSTREAM_BASE_URL="http://upstream.invalid")
    sem = anyio.Semaphore(10)

    with respx.mock(assert_all_called=True) as rsps:
        rsps.get("https://example.test/html").respond(
            206,
            headers={
                "Content-Range": "bytes 0-100/200",
                "Accept-Ranges": "bytes",
                "Content-Type": "text/html",
            },
            text="<!doctype html><html>nope</html>",
        )
        async with httpx.AsyncClient() as client:
            r = await range_probe_1kb(
                url="https://example.test/html", client=client, settings=settings, sem=sem
            )
    assert r.hard_fail
    assert r.error == "htmlish"


@pytest.mark.asyncio
async def test_range_probe_hard_fails_when_not_seekable_and_required() -> None:
    settings = Settings(UPSTREAM_BASE_URL="http://upstream.invalid", REQUIRE_SEEKABLE="true")
    sem = anyio.Semaphore(10)
    mkv = build_mini_mkv_bytes()[:2048]

    with respx.mock(assert_all_called=True) as rsps:
        rsps.get("https://example.test/norange").respond(200, headers={}, content=mkv)
        async with httpx.AsyncClient() as client:
            r = await range_probe_1kb(
                url="https://example.test/norange", client=client, settings=settings, sem=sem
            )
    assert r.hard_fail
    assert r.error == "not_seekable"
