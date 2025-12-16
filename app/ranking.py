from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import anyio
import httpx

from app.async_utils import gather
from app.config import Settings
from app.mkv import EbmlParseError, MkvMetadata, parse_mkv_metadata
from app.probe import (
    ProbeResult,
    fetch_prefix_bytes,
    fetch_range_bytes,
    range_probe_1kb,
    range_probe_at_offset,
)


@dataclass(frozen=True)
class ScoreBreakdown:
    score: float
    reasons: list[str]


def _normalize_lang(lang: str | None) -> str | None:
    if not lang:
        return None
    lang_code = lang.strip().lower()
    # Common ISO639-2 -> ISO639-1 mappings for preferred langs.
    return {"nld": "nl", "dut": "nl", "eng": "en", "fre": "fr", "fra": "fr"}.get(
        lang_code, lang_code[:2]
    )


def _pick_primary_audio_langs(meta: MkvMetadata) -> list[str]:
    langs: list[str] = []
    for a in meta.audio_tracks:
        lang_code = _normalize_lang(a.lang_bcp47 or a.lang)
        if lang_code and lang_code not in langs:
            langs.append(lang_code)
    return langs


def _best_video_height(meta: MkvMetadata) -> int | None:
    heights = [t.height for t in meta.video_tracks if t.height]
    return max(heights) if heights else None


def _best_video_codec(meta: MkvMetadata) -> str | None:
    codecs = [t.codec_id for t in meta.video_tracks if t.codec_id]
    return codecs[0] if codecs else None


def _best_audio_channels(meta: MkvMetadata) -> int | None:
    chans = [t.channels for t in meta.audio_tracks if t.channels]
    return max(chans) if chans else None


def _codec_hint(codec_id: str | None) -> str | None:
    if not codec_id:
        return None
    c = codec_id.lower()
    if "hevc" in c or "hvc" in c:
        return "HEVC"
    if "avc" in c or "h264" in c:
        return "AVC"
    if "vp9" in c:
        return "VP9"
    if "av1" in c:
        return "AV1"
    return None


def score_from_probe(
    *,
    probe: ProbeResult | None,
    meta: MkvMetadata | None,
    upstream_index: int,
    settings: Settings,
    name_hint: str | None = None,
    weird_failed: bool = False,
) -> ScoreBreakdown:
    reasons: list[str] = []
    score = 0.0

    score += (settings.top_k_phase1 - upstream_index) * 0.01

    if probe is None:
        return ScoreBreakdown(score=-1000.0 + score, reasons=["unprobed"])

    if probe.hard_fail:
        return ScoreBreakdown(score=-10_000.0, reasons=[f"hard_fail:{probe.error or 'unknown'}"])

    if probe.status_code == 206:
        score += 10.0
        reasons.append("206")
    elif probe.status_code == 200:
        score += 2.0
        reasons.append("200")

    if probe.seekable:
        score += 8.0
        reasons.append("seekable")
    else:
        score -= 10.0
        reasons.append("not_seekable")

    if probe.magic_mkv:
        score += 6.0
        reasons.append("mkv_magic")
    else:
        score -= 3.0

    if probe.content_type:
        ct = probe.content_type.lower()
        if ct.startswith("video/") or "octet-stream" in ct:
            score += 1.0
        else:
            score -= 1.0

    if probe.ttfb_ms is not None:
        score += max(0.0, 30.0 - probe.ttfb_ms / 10.0)
        reasons.append(f"ttfb:{probe.ttfb_ms:.0f}ms")

    if probe.dl_1kb_ms is not None:
        score += max(0.0, 20.0 - probe.dl_1kb_ms / 20.0)
        reasons.append(f"1kb:{probe.dl_1kb_ms:.0f}ms")

    if meta:
        langs = _pick_primary_audio_langs(meta)
        if langs:
            reasons.append("aud:" + "/".join(langs[:2]))
        for i, pref in enumerate(settings.preferred_audio_lang_list):
            if pref in langs:
                score += 10.0 - i
                reasons.append(f"pref_aud:{pref}")
                break
        if any(a.default for a in meta.audio_tracks):
            score += 1.0
        if any(a.original for a in meta.audio_tracks):
            score += 1.0

        height = _best_video_height(meta)
        if height:
            reasons.append(f"{height}p")
            if settings.min_video_height and height < settings.min_video_height:
                score -= 8.0
                reasons.append("below_min_height")
            else:
                score += min(12.0, height / 200.0)

        codec_id = _best_video_codec(meta)
        codec_hint = _codec_hint(codec_id)
        if codec_hint:
            reasons.append(codec_hint)
        if codec_hint and settings.prefer_codec_list:
            for i, pref in enumerate(settings.prefer_codec_list):
                if pref in (codec_hint or "").lower():
                    score += 6.0 - i
                    reasons.append(f"pref_codec:{pref}")
                    break

    if settings.prefer_hdr and name_hint:
        nh = name_hint.lower()
        if "hdr" in nh or "dolby vision" in nh or " dv " in f" {nh} ":
            score += 2.0
            reasons.append("hdr_hint")

    if weird_failed:
        score -= 15.0
        reasons.append("weird_fail")

    return ScoreBreakdown(score=score, reasons=reasons)


def _format_label(probe: ProbeResult | None, meta: MkvMetadata | None) -> str:
    parts: list[str] = []
    if probe and probe.magic_mkv:
        parts.append("MKV")
    if probe and probe.ttfb_ms is not None:
        parts.append(f"{probe.ttfb_ms:.0f}ms")
    if meta:
        height = _best_video_height(meta)
        if height:
            parts.append(f"{height}p")
        langs = _pick_primary_audio_langs(meta)
        if langs:
            parts.append("/".join([lang_code.upper() for lang_code in langs[:2]]))
        codec_hint = _codec_hint(_best_video_codec(meta))
        if codec_hint:
            parts.append(codec_hint)
    return " • ".join(parts[:5])


def _format_channels(ch: int | None) -> str | None:
    if not ch:
        return None
    if ch == 1:
        return "1.0"
    if ch == 2:
        return "2.0"
    if ch == 6:
        return "5.1"
    if ch == 8:
        return "7.1"
    return f"{ch}ch"


def _looks_like_mkv_url(url: str) -> bool:
    base = url.split("?", 1)[0].lower()
    return base.endswith((".mkv", ".webm"))


async def _extract_mkv_metadata_incremental(
    *,
    url: str,
    client: httpx.AsyncClient,
    settings: Settings,
    sem: anyio.Semaphore,
) -> MkvMetadata | None:
    max_bytes = settings.mkv_meta_max_bytes
    chunk_bytes = settings.mkv_meta_chunk_bytes

    buf = bytearray()
    offset = 0
    while offset < max_bytes:
        want = min(chunk_bytes, max_bytes - offset)
        chunk, status = await fetch_range_bytes(
            url=url, start=offset, length=want, client=client, settings=settings, sem=sem
        )
        if not chunk:
            return None

        # Server ignored range for non-zero offset; fall back to single bounded prefix read.
        if offset > 0 and status == 200:
            prefix = await fetch_prefix_bytes(
                url=url, size=max_bytes, client=client, settings=settings, sem=sem
            )
            if not prefix:
                return None
            try:
                meta = parse_mkv_metadata(prefix)
            except EbmlParseError:
                return None
            return meta if (meta.audio_tracks or meta.video_tracks) else None

        buf.extend(chunk)
        offset += len(chunk)
        try:
            meta = parse_mkv_metadata(bytes(buf))
        except EbmlParseError:
            continue
        if meta.audio_tracks or meta.video_tracks:
            return meta

    return None


async def reorder_streams(
    *,
    streams: list[dict[str, Any]],
    client: httpx.AsyncClient,
    settings: Settings,
    sem: anyio.Semaphore,
) -> list[dict[str, Any]]:
    # Select top-K URL streams to probe; everything else stays in upstream order after those.
    url_indices: list[int] = []
    for i, s in enumerate(streams):
        url = s.get("url")
        if isinstance(url, str) and url.startswith(("http://", "https://")):
            url_indices.append(i)
        if len(url_indices) >= settings.top_k_phase1:
            break

    probes: dict[int, ProbeResult] = {}
    if url_indices:
        results = await gather(
            *[
                range_probe_1kb(url=streams[i]["url"], client=client, settings=settings, sem=sem)
                for i in url_indices
            ]
        )
        probes = {idx: res for idx, res in zip(url_indices, results, strict=True)}

    # Drop hard fails only among probed candidates; keep unprobed streams.
    kept_top = [i for i in url_indices if not probes[i].hard_fail]

    # Phase 2: parse metadata for top-M of the phase1 (best latency first for cheaper MKV parsing).
    top_for_phase2 = sorted(
        kept_top,
        key=lambda i: (
            probes[i].ttfb_ms if probes[i].ttfb_ms is not None else 1e9,
            probes[i].dl_1kb_ms if probes[i].dl_1kb_ms is not None else 1e9,
        ),
    )[: settings.top_m_phase2]

    metas: dict[int, MkvMetadata] = {}
    p2_status: dict[int, str] = {i: "-" for i in kept_top}
    top_for_phase2_mkvish: list[int] = []
    for idx in top_for_phase2:
        pr = probes.get(idx)
        url = streams[idx].get("url")
        if not isinstance(url, str):
            continue
        if _looks_like_mkv_url(url) or bool(pr and pr.magic_mkv):
            top_for_phase2_mkvish.append(idx)

    if top_for_phase2_mkvish:
        meta_results = await gather(
            *[
                _extract_mkv_metadata_incremental(
                    url=streams[i]["url"],
                    client=client,
                    settings=settings,
                    sem=sem,
                )
                for i in top_for_phase2_mkvish
            ]
        )
        for idx, meta in zip(top_for_phase2_mkvish, meta_results, strict=True):
            p2_status[idx] = "×"
            if meta and (meta.audio_tracks or meta.video_tracks):
                metas[idx] = meta
                p2_status[idx] = "✓"

    # Phase 3: weirdness probes (top-P by combined score).
    weird_failed: set[int] = set()
    p3_status: dict[int, str] = {i: "-" for i in kept_top}
    scored: list[tuple[int, ScoreBreakdown]] = []
    for i in kept_top:
        hint = (
            (streams[i].get("title") or streams[i].get("name") or "")
            if isinstance(streams[i], dict)
            else ""
        )
        scored.append(
            (
                i,
                score_from_probe(
                    probe=probes.get(i),
                    meta=metas.get(i),
                    upstream_index=i,
                    settings=settings,
                    name_hint=str(hint),
                ),
            )
        )
    scored.sort(key=lambda t: (-t[1].score, t[0]))

    # Phase 3: weirdness probes (top-P by combined score) — PARALLEL
    weird_failed: set[int] = set()
    p3_status: dict[int, str] = {i: "-" for i in kept_top}

    weird_checked = [idx for idx, _sb in scored[: settings.top_p_weird]]

    async def _check_weird(idx: int) -> tuple[int, str, bool]:
        pr = probes.get(idx)
        if not pr or not pr.total_size or pr.total_size < 10_000:
            return idx, "-", False  # skipped

        total = pr.total_size
        offsets = [int(total * 0.1), int(total * 0.9)]

        # Run both offset probes concurrently
        (ok1, _), (ok2, _) = await gather(
            range_probe_at_offset(
                url=streams[idx]["url"],
                offset=offsets[0],
                size=settings.weird_probe_bytes,
                client=client,
                settings=settings,
                sem=sem,
            ),
            range_probe_at_offset(
                url=streams[idx]["url"],
                offset=offsets[1],
                size=settings.weird_probe_bytes,
                client=client,
                settings=settings,
                sem=sem,
            ),
        )

        passed = bool(ok1 and ok2)
        return idx, ("✓" if passed else "×"), (not passed)

    if weird_checked:
        results = await gather(*[_check_weird(idx) for idx in weird_checked])
        for idx, status, failed in results:
            if status != "-":
                p3_status[idx] = status
            if failed:
                weird_failed.add(idx)

    # Final rank among probed top-K.
    final_scored: list[tuple[int, ScoreBreakdown]] = []
    for i in kept_top:
        hint = streams[i].get("title") or streams[i].get("name") or ""
        final_scored.append(
            (
                i,
                score_from_probe(
                    probe=probes.get(i),
                    meta=metas.get(i),
                    upstream_index=i,
                    settings=settings,
                    name_hint=str(hint),
                    weird_failed=i in weird_failed,
                ),
            )
        )
    final_scored.sort(key=lambda t: (-t[1].score, t[0]))

    # Build output: ranked candidates + remaining streams in upstream order (excluding dropped hard fails).
    dropped = set(url_indices) - set(kept_top)
    remaining = [s for j, s in enumerate(streams) if j not in dropped and j not in url_indices]
    ranked_streams: list[dict[str, Any]] = []

    for i, _sb in final_scored:
        s = dict(streams[i])
        label = _format_label_with_phases(
            probe=probes.get(i),
            meta=metas.get(i),
            p2=p2_status.get(i, "-"),
            p3=p3_status.get(i, "-"),
        )
        if label:
            s["title"] = label if not s.get("title") else f"{label} — {s.get('title')}"
        s.setdefault("description", "")
        ranked_streams.append(s)

    ranked_streams.extend(remaining)
    return ranked_streams


def _format_label_with_phases(
    *,
    probe: ProbeResult | None,
    meta: MkvMetadata | None,
    p2: str,
    p3: str,
) -> str:
    parts: list[str] = []
    if probe and probe.magic_mkv:
        parts.append("MKV")
    flags = f"P2{p2} P3{p3}"
    parts.append(flags)
    if probe and probe.ttfb_ms is not None:
        parts.append(f"{probe.ttfb_ms:.0f}ms")

    if meta and p2 == "✓":
        height = _best_video_height(meta)
        if height:
            parts.append(f"{height}p")
        langs = _pick_primary_audio_langs(meta)
        if langs:
            parts.append("/".join([lang_code.upper() for lang_code in langs[:2]]))
        codec_hint = _codec_hint(_best_video_codec(meta))
        if codec_hint:
            parts.append(codec_hint)
        ch = _format_channels(_best_audio_channels(meta))
        if ch:
            parts.append(ch)

    return " • ".join(parts[:7])
