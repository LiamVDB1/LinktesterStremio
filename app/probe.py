from __future__ import annotations

import re
from dataclasses import dataclass

import anyio
import httpx

from app.config import Settings
from app.mkv import EBML_MAGIC

_RE_CONTENT_RANGE = re.compile(r"^bytes\s+(\d+)-(\d+)/(\d+|\*)$", re.IGNORECASE)


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    hard_fail: bool
    url: str
    status_code: int | None
    seekable: bool
    content_type: str | None
    total_size: int | None
    ttfb_ms: float | None
    dl_1kb_ms: float | None
    magic_mkv: bool
    error: str | None = None
    headers: dict[str, str] | None = None


def _is_htmlish(content_type: str | None, prefix: bytes) -> bool:
    if content_type:
        ct = content_type.lower()
        if (
            ct.startswith("text/html")
            or ct.startswith("text/plain")
            or ct.startswith("application/xhtml")
        ):
            return True
    sniff = prefix.lstrip()[:64].lower()
    return (
        sniff.startswith(b"<html") or sniff.startswith(b"<!doctype") or sniff.startswith(b"<head")
    )


def _parse_total_size(headers: httpx.Headers) -> int | None:
    cr = headers.get("Content-Range")
    if cr:
        m = _RE_CONTENT_RANGE.match(cr.strip())
        if m:
            total = m.group(3)
            if total.isdigit():
                return int(total)
    cl = headers.get("Content-Length")
    if cl and cl.isdigit():
        return int(cl)
    return None


def _is_seekable(headers: httpx.Headers) -> bool:
    ar = headers.get("Accept-Ranges", "")
    if "bytes" in ar.lower():
        return True
    cr = headers.get("Content-Range")
    return bool(cr and cr.lower().startswith("bytes"))


async def range_probe_1kb(
    *,
    url: str,
    client: httpx.AsyncClient,
    settings: Settings,
    sem: anyio.Semaphore,
) -> ProbeResult:
    for attempt in range(2):
        result = await _range_probe_1kb_once(url=url, client=client, settings=settings, sem=sem)
        if result.ok or result.hard_fail:
            return result
        if attempt == 0 and (result.error or "").startswith(("request_error")):
            continue
        return ProbeResult(
            ok=False,
            hard_fail=True,
            url=result.url,
            status_code=result.status_code,
            seekable=result.seekable,
            content_type=result.content_type,
            total_size=result.total_size,
            ttfb_ms=result.ttfb_ms,
            dl_1kb_ms=result.dl_1kb_ms,
            magic_mkv=result.magic_mkv,
            error=result.error or "retry_exhausted",
            headers=result.headers,
        )
    return ProbeResult(
        ok=False,
        hard_fail=True,
        url=url,
        status_code=None,
        seekable=False,
        content_type=None,
        total_size=None,
        ttfb_ms=None,
        dl_1kb_ms=None,
        magic_mkv=False,
        error="retry_exhausted",
    )


async def _range_probe_1kb_once(
    *,
    url: str,
    client: httpx.AsyncClient,
    settings: Settings,
    sem: anyio.Semaphore,
) -> ProbeResult:
    async with sem:
        headers = {"Range": "bytes=0-1024"}
        try:
            with anyio.fail_after(settings.t_probe_total_ms / 1000):
                start = anyio.current_time()
                async with client.stream("GET", url, headers=headers) as resp:
                    status = resp.status_code
                    content_type = resp.headers.get("Content-Type")
                    seekable = _is_seekable(resp.headers)
                    total_size = _parse_total_size(resp.headers)

                    if status not in (200, 206):
                        hard = status < 500
                        return ProbeResult(
                            ok=False,
                            hard_fail=hard,
                            url=url,
                            status_code=status,
                            seekable=seekable,
                            content_type=content_type,
                            total_size=total_size,
                            ttfb_ms=None,
                            dl_1kb_ms=None,
                            magic_mkv=False,
                            error=f"bad_status:{status}",
                            headers=dict(resp.headers),
                        )

                    body = bytearray()
                    first_byte_t: float | None = None
                    aiter = resp.aiter_bytes()

                    with anyio.fail_after(settings.t_ttfb_ms / 1000):
                        while True:
                            try:
                                chunk = await aiter.__anext__()
                            except StopAsyncIteration:
                                chunk = b""
                            if chunk:
                                body.extend(chunk)
                                first_byte_t = anyio.current_time()
                                break

                    if first_byte_t is None:
                        return ProbeResult(
                            ok=False,
                            hard_fail=False,
                            url=url,
                            status_code=status,
                            seekable=seekable,
                            content_type=content_type,
                            total_size=total_size,
                            ttfb_ms=None,
                            dl_1kb_ms=None,
                            magic_mkv=False,
                            error="no_body",
                            headers=dict(resp.headers),
                        )

                    async for chunk in aiter:
                        if not chunk:
                            continue
                        body.extend(chunk)
                        if len(body) >= 1024:
                            break

                    done = anyio.current_time()
                    ttfb_ms = (first_byte_t - start) * 1000
                    dl_ms = (done - start) * 1000

                    prefix = bytes(body[:256])
                    if _is_htmlish(content_type, prefix):
                        return ProbeResult(
                            ok=False,
                            hard_fail=True,
                            url=url,
                            status_code=status,
                            seekable=seekable,
                            content_type=content_type,
                            total_size=total_size,
                            ttfb_ms=ttfb_ms,
                            dl_1kb_ms=dl_ms,
                            magic_mkv=False,
                            error="htmlish",
                            headers=dict(resp.headers),
                        )

                    magic_mkv = bytes(body[:4]) == EBML_MAGIC
                    hard_fail = settings.require_seekable and not seekable
                    ok = not hard_fail
                    return ProbeResult(
                        ok=ok,
                        hard_fail=hard_fail,
                        url=url,
                        status_code=status,
                        seekable=seekable,
                        content_type=content_type,
                        total_size=total_size,
                        ttfb_ms=ttfb_ms,
                        dl_1kb_ms=dl_ms,
                        magic_mkv=magic_mkv,
                        error=None if ok else "not_seekable",
                        headers=dict(resp.headers),
                    )
        except TimeoutError:
            return ProbeResult(
                ok=False,
                hard_fail=False,
                url=url,
                status_code=None,
                seekable=False,
                content_type=None,
                total_size=None,
                ttfb_ms=None,
                dl_1kb_ms=None,
                magic_mkv=False,
                error="timeout",
            )
        except httpx.RequestError as e:
            return ProbeResult(
                ok=False,
                hard_fail=False,
                url=url,
                status_code=None,
                seekable=False,
                content_type=None,
                total_size=None,
                ttfb_ms=None,
                dl_1kb_ms=None,
                magic_mkv=False,
                error=f"request_error:{type(e).__name__}",
            )


async def range_probe_at_offset(
    *,
    url: str,
    offset: int,
    size: int,
    client: httpx.AsyncClient,
    settings: Settings,
    sem: anyio.Semaphore,
) -> tuple[bool, str | None]:
    async with sem:
        headers = {"Range": f"bytes={offset}-{offset + size - 1}"}
        try:
            with anyio.fail_after(settings.t_probe_total_ms / 1000):
                async with client.stream("GET", url, headers=headers) as resp:
                    if resp.status_code not in (200, 206):
                        return False, f"bad_status:{resp.status_code}"
                    with anyio.fail_after(settings.t_ttfb_ms / 1000):
                        async for chunk in resp.aiter_bytes():
                            if chunk:
                                return True, None
                    return False, "no_body"
        except TimeoutError:
            return False, "timeout"
        except httpx.RequestError as e:
            return False, f"request_error:{type(e).__name__}"


async def fetch_prefix_bytes(
    *,
    url: str,
    size: int,
    client: httpx.AsyncClient,
    settings: Settings,
    sem: anyio.Semaphore,
) -> bytes | None:
    async with sem:
        headers = {"Range": f"bytes=0-{size - 1}"}
        try:
            with anyio.fail_after(settings.t_probe_total_ms / 1000):
                async with client.stream("GET", url, headers=headers) as resp:
                    if resp.status_code not in (200, 206):
                        return None
                    buf = bytearray()
                    aiter = resp.aiter_bytes()
                    with anyio.fail_after(settings.t_ttfb_ms / 1000):
                        while len(buf) == 0:
                            try:
                                chunk = await aiter.__anext__()
                            except StopAsyncIteration:
                                chunk = b""
                            buf.extend(chunk)
                            if not chunk:
                                break
                    async for chunk in aiter:
                        if not chunk:
                            continue
                        buf.extend(chunk)
                        if len(buf) >= size:
                            break
                    return bytes(buf[:size])
        except Exception:
            return None


async def fetch_range_bytes(
    *,
    url: str,
    start: int,
    length: int,
    client: httpx.AsyncClient,
    settings: Settings,
    sem: anyio.Semaphore,
) -> tuple[bytes | None, int | None]:
    async with sem:
        end = start + length - 1
        headers = {"Range": f"bytes={start}-{end}"}
        try:
            with anyio.fail_after(settings.t_probe_total_ms / 1000):
                async with client.stream("GET", url, headers=headers) as resp:
                    status = resp.status_code
                    if status not in (200, 206):
                        return None, status
                    buf = bytearray()
                    aiter = resp.aiter_bytes()
                    with anyio.fail_after(settings.t_ttfb_ms / 1000):
                        while len(buf) == 0:
                            try:
                                chunk = await aiter.__anext__()
                            except StopAsyncIteration:
                                chunk = b""
                            buf.extend(chunk)
                            if not chunk:
                                break
                    async for chunk in aiter:
                        if not chunk:
                            continue
                        buf.extend(chunk)
                        if len(buf) >= length:
                            break
                    return bytes(buf[:length]), status
        except Exception:
            return None, None
