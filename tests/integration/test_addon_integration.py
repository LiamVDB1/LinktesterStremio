from __future__ import annotations

from typing import Any

import anyio
import httpx
import pytest
from fastapi import FastAPI, Header, Response

from app.config import Settings
from app.main import create_app
from tests.ebml_builder import MiniMkvSpec, build_mini_mkv_bytes, build_mkv_with_delayed_tracks


def range_response(blob: bytes, range_header: str | None) -> Response:
    r = range_header
    if r and r.startswith("bytes="):
        s, e = r.removeprefix("bytes=").split("-", 1)
        start = int(s)
        end = min(int(e), len(blob) - 1)
        part = blob[start : end + 1]
        return Response(
            content=part,
            media_type="video/x-matroska",
            status_code=206,
            headers={"Content-Range": f"bytes {start}-{end}/{len(blob)}", "Accept-Ranges": "bytes"},
        )
    return Response(
        content=blob,
        media_type="video/x-matroska",
        status_code=200,
        headers={"Accept-Ranges": "bytes", "Content-Length": str(len(blob))},
    )


def _upstream_app() -> FastAPI:
    app = FastAPI()
    good_bytes = build_mini_mkv_bytes()
    html_bytes = b"<!doctype html><html>nope</html>"

    @app.get("/stream/{type}/{id}.json")
    async def upstream_stream(type: str, id: str) -> dict[str, Any]:
        return {
            "streams": [
                {"name": "HTML", "url": "http://upstream/video/html"},
                {"name": "SLOW", "url": "http://upstream/video/slow"},
                {"name": "GOOD", "url": "http://upstream/video/good"},
            ]
        }

    @app.get("/video/html")
    async def video_html() -> Response:
        return Response(
            content=html_bytes,
            media_type="text/html",
            status_code=206,
            headers={"Accept-Ranges": "bytes"},
        )

    @app.get("/video/slow")
    async def video_slow() -> Response:
        await anyio.sleep(1.0)
        return Response(
            content=good_bytes[:2048],
            media_type="video/x-matroska",
            status_code=206,
            headers={"Content-Range": "bytes 0-1024/9999", "Accept-Ranges": "bytes"},
        )

    @app.get("/video/good")
    async def video_good(
        range_header: str | None = Header(default=None, alias="Range"),
    ) -> Response:
        return range_response(good_bytes, range_header)

    return app


@pytest.mark.asyncio
async def test_addon_ranks_good_stream_first() -> None:
    upstream = _upstream_app()
    upstream_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=upstream), base_url="http://upstream"
    )
    settings = Settings(
        UPSTREAM_BASE_URL="http://upstream",
        TOP_K_PHASE1="3",
        TOP_M_PHASE2="1",
        T_PROBE_TOTAL_MS="200",
        T_TTFB_MS="100",
        REQUIRE_SEEKABLE="true",
    )
    addon = create_app(settings=settings, client=upstream_client)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=addon), base_url="http://addon"
    ) as client:
        r = await client.get("/stream/movie/tt123.json")
    assert r.status_code == 200
    data = r.json()
    assert data["streams"][0]["url"] == "http://upstream/video/good"
    assert "MKV" in (data["streams"][0].get("title") or "")

    await upstream_client.aclose()


@pytest.mark.asyncio
async def test_addon_prefers_preferred_audio_lang() -> None:
    upstream = FastAPI()
    good_en = build_mini_mkv_bytes(MiniMkvSpec(audio_lang="eng", audio_lang_bcp47="en"))
    good_nl = build_mini_mkv_bytes(MiniMkvSpec(audio_lang="nld", audio_lang_bcp47="nl"))

    @upstream.get("/stream/{type}/{id}.json")
    async def upstream_stream(type: str, id: str) -> dict[str, Any]:
        return {
            "streams": [
                {"name": "GOOD_EN", "url": "http://upstream/video/en"},
                {"name": "GOOD_NL", "url": "http://upstream/video/nl"},
            ]
        }

    @upstream.get("/video/en")
    async def video_en(range_header: str | None = Header(default=None, alias="Range")) -> Response:
        return range_response(good_en, range_header)

    @upstream.get("/video/nl")
    async def video_nl(range_header: str | None = Header(default=None, alias="Range")) -> Response:
        return range_response(good_nl, range_header)

    upstream_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=upstream), base_url="http://upstream"
    )
    settings = Settings(
        UPSTREAM_BASE_URL="http://upstream",
        TOP_K_PHASE1="2",
        TOP_M_PHASE2="2",
        PREFERRED_AUDIO_LANGS="nl,en",
        T_PROBE_TOTAL_MS="500",
        T_TTFB_MS="200",
        REQUIRE_SEEKABLE="true",
    )
    addon = create_app(settings=settings, client=upstream_client)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=addon), base_url="http://addon"
    ) as client:
        r = await client.get("/stream/movie/tt123.json")
    assert r.status_code == 200
    data = r.json()
    assert data["streams"][0]["url"] == "http://upstream/video/nl"

    await upstream_client.aclose()


@pytest.mark.asyncio
async def test_phase2_incremental_fetch_extracts_late_tracks_and_labels() -> None:
    upstream = FastAPI()
    blob = build_mkv_with_delayed_tracks(
        tracks_start_at=300 * 1024,
        spec=MiniMkvSpec(height=720, audio_lang="eng", audio_lang_bcp47="en", audio_channels=6),
    )

    @upstream.get("/stream/{type}/{id}.json")
    async def upstream_stream(type: str, id: str) -> dict[str, Any]:
        return {"streams": [{"name": "LATE", "url": "http://upstream/video/late"}]}

    @upstream.get("/video/late")
    async def video_late(
        range_header: str | None = Header(default=None, alias="Range"),
    ) -> Response:
        return range_response(blob, range_header)

    upstream_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=upstream), base_url="http://upstream"
    )
    settings = Settings(
        UPSTREAM_BASE_URL="http://upstream",
        TOP_K_PHASE1="1",
        TOP_M_PHASE2="1",
        TOP_P_WEIRD="0",
        MKV_META_CHUNK_BYTES="262144",
        MKV_META_MAX_BYTES="2097152",
        T_PROBE_TOTAL_MS="800",
        T_TTFB_MS="300",
        REQUIRE_SEEKABLE="true",
    )
    addon = create_app(settings=settings, client=upstream_client)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=addon), base_url="http://addon"
    ) as c:
        r = await c.get("/stream/movie/tt123.json")
    assert r.status_code == 200
    title = r.json()["streams"][0].get("title") or ""
    assert "P2✓" in title
    assert "P3-" in title
    assert "720p" in title
    assert "EN" in title
    assert "HEVC" in title
    assert "5.1" in title

    await upstream_client.aclose()


@pytest.mark.asyncio
async def test_labels_show_p2_skipped_and_p3_disabled() -> None:
    upstream = FastAPI()
    fast_blob = build_mini_mkv_bytes(MiniMkvSpec(audio_lang="eng", audio_lang_bcp47="en"))
    slow_blob = build_mini_mkv_bytes(MiniMkvSpec(audio_lang="ita", audio_lang_bcp47="it"))

    @upstream.get("/stream/{type}/{id}.json")
    async def upstream_stream(type: str, id: str) -> dict[str, Any]:
        return {
            "streams": [
                {"name": "FAST 1080p", "url": "http://upstream/video/fast"},
                {"name": "SLOW 1080p", "url": "http://upstream/video/slow2"},
            ]
        }

    @upstream.get("/video/fast")
    async def video_fast(
        range_header: str | None = Header(default=None, alias="Range"),
    ) -> Response:
        return range_response(fast_blob, range_header)

    @upstream.get("/video/slow2")
    async def video_slow2(
        range_header: str | None = Header(default=None, alias="Range"),
    ) -> Response:
        await anyio.sleep(0.15)
        return range_response(slow_blob, range_header)

    upstream_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=upstream), base_url="http://upstream"
    )
    settings = Settings(
        UPSTREAM_BASE_URL="http://upstream",
        TOP_K_PHASE1="2",
        TOP_M_PHASE2="1",
        TOP_P_WEIRD="0",
        T_PROBE_TOTAL_MS="800",
        T_TTFB_MS="400",
        REQUIRE_SEEKABLE="true",
    )
    addon = create_app(settings=settings, client=upstream_client)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=addon), base_url="http://addon"
    ) as c:
        r = await c.get("/stream/movie/tt123.json")
    assert r.status_code == 200
    streams = r.json()["streams"]
    by_url = {s["url"]: s for s in streams}
    assert "P3-" in (by_url["http://upstream/video/fast"].get("title") or "")
    assert "P3-" in (by_url["http://upstream/video/slow2"].get("title") or "")
    assert "P2✓" in (by_url["http://upstream/video/fast"].get("title") or "")
    assert "P2-" in (by_url["http://upstream/video/slow2"].get("title") or "")

    await upstream_client.aclose()


@pytest.mark.asyncio
async def test_stream_endpoint_caches_ranked_results() -> None:
    upstream = FastAPI()
    blob = build_mini_mkv_bytes()
    counters = {"streams": 0, "video": 0}

    @upstream.get("/stream/{type}/{id}.json")
    async def upstream_stream(type: str, id: str) -> dict[str, Any]:
        counters["streams"] += 1
        return {"streams": [{"name": "GOOD", "url": "http://upstream/video/good"}]}

    @upstream.get("/video/good")
    async def video_good(
        range_header: str | None = Header(default=None, alias="Range"),
    ) -> Response:
        counters["video"] += 1
        return range_response(blob, range_header)

    upstream_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=upstream), base_url="http://upstream"
    )
    settings = Settings(
        UPSTREAM_BASE_URL="http://upstream",
        TOP_M_PHASE2="0",
        TOP_P_WEIRD="0",
        STREAM_CACHE_TTL_S="60",
        PROBE_CACHE_TTL_S="60",
        META_CACHE_TTL_S="60",
    )
    addon = create_app(settings=settings, client=upstream_client)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=addon), base_url="http://addon"
    ) as client:
        first = await client.get("/stream/movie/tt123.json")
        second = await client.get("/stream/movie/tt123.json")

    assert first.status_code == 200
    assert second.status_code == 200
    assert counters["streams"] == 1
    assert counters["video"] == 1

    await upstream_client.aclose()


@pytest.mark.asyncio
async def test_streams_without_name_are_normalized_for_stremio() -> None:
    upstream = FastAPI()
    blob = build_mini_mkv_bytes()

    @upstream.get("/stream/{type}/{id}.json")
    async def upstream_stream(type: str, id: str) -> dict[str, Any]:
        return {
            "streams": [
                {
                    "title": "AIO 1080p HEVC 4.2 GB",
                    "url": "http://upstream/video/good-no-name",
                }
            ]
        }

    @upstream.get("/video/good-no-name")
    async def video_good(
        range_header: str | None = Header(default=None, alias="Range"),
    ) -> Response:
        return range_response(blob, range_header)

    upstream_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=upstream), base_url="http://upstream"
    )
    settings = Settings(
        UPSTREAM_BASE_URL="http://upstream",
        TOP_K_PHASE1="1",
        TOP_M_PHASE2="1",
        T_PROBE_TOTAL_MS="800",
        T_TTFB_MS="300",
        REQUIRE_SEEKABLE="true",
    )
    addon = create_app(settings=settings, client=upstream_client)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=addon), base_url="http://addon"
    ) as client:
        response = await client.get("/stream/movie/tt123.json")

    assert response.status_code == 200
    stream = response.json()["streams"][0]
    assert stream["name"]
    assert stream["behaviorHints"]["notWebReady"] is True

    await upstream_client.aclose()


@pytest.mark.asyncio
async def test_stream_endpoint_returns_only_top_three_fhd_and_uhd_results() -> None:
    upstream = FastAPI()
    blob = build_mini_mkv_bytes()
    stream_defs = [
        ("fhd-a", "🚀 FHD", "🎥 BluRay 🎞️ HEVC | 📦 4.4 GB 📊 5.2 Mbps"),
        ("fhd-b", "🚀 FHD", "🎥 BluRay 🎞️ HEVC | 📦 4.8 GB 📊 5.8 Mbps"),
        ("fhd-c", "🚀 FHD", "🎥 BluRay 🎞️ AVC | 📦 7.2 GB 📊 8.6 Mbps"),
        ("fhd-d", "🚀 FHD", "🎥 BluRay REMUX 🎞️ AVC | 📦 33.2 GB 📊 39.2 Mbps"),
        ("uhd-a", "✨ 4K", "🎥 BluRay 🎞️ HEVC | 📦 18 GB 📊 19 Mbps"),
        ("uhd-b", "✨ 4K", "🎥 BluRay 🎞️ HEVC | 📦 21 GB 📊 22 Mbps"),
        ("uhd-c", "✨ 4K", "🎥 BluRay REMUX 📺 HDR | DV 🎞️ HEVC | 📦 36.6 GB 📊 76.2 Mbps"),
        ("uhd-d", "✨ 4K", "🎥 WEB-DL 📺 HDR 🎞️ HEVC | 📦 14 GB 📊 15 Mbps"),
        ("hd-a", "📺 HD", "🎥 WEB-DL 🎞️ AVC | 📦 1.5 GB 📊 3.5 Mbps"),
    ]

    @upstream.get("/stream/{type}/{id}.json")
    async def upstream_stream(type: str, id: str) -> dict[str, Any]:
        return {
            "streams": [
                {"name": name, "description": description, "url": f"http://upstream/video/{slug}"}
                for slug, name, description in stream_defs
            ]
        }

    @upstream.get("/video/{slug}")
    async def video(
        slug: str, range_header: str | None = Header(default=None, alias="Range")
    ) -> Response:
        return range_response(blob, range_header)

    upstream_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=upstream), base_url="http://upstream"
    )
    settings = Settings(
        UPSTREAM_BASE_URL="http://upstream",
        TOP_K_PHASE1="9",
        TOP_M_PHASE2="0",
        TOP_P_WEIRD="0",
    )
    addon = create_app(settings=settings, client=upstream_client)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=addon), base_url="http://addon"
    ) as client:
        response = await client.get("/stream/movie/tt123.json")

    assert response.status_code == 200
    streams = response.json()["streams"]
    assert len(streams) == 6
    titles = [stream.get("title") or "" for stream in streams]
    assert sum("FHD #" in title for title in titles) == 3
    assert sum("4K #" in title for title in titles) == 3
    assert all(not title.startswith("HD #") for title in titles)

    await upstream_client.aclose()
