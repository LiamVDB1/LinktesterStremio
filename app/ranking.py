from __future__ import annotations

import logging
from dataclasses import dataclass
from time import monotonic
from typing import Any

import anyio
import httpx

from app.async_utils import gather
from app.cache import TTLCache
from app.config import Settings
from app.mkv import EbmlParseError, MkvMetadata, parse_mkv_metadata
from app.probe import (
    ProbeResult,
    fetch_prefix_bytes,
    fetch_range_bytes,
    range_probe_1kb,
    range_probe_at_offset,
)
from app.stream_profile import (
    PeerGroupStats,
    StreamProfile,
    build_peer_group_stats,
    build_stream_profile,
    classify_resolution_tier,
    score_profile_centrality,
    score_quality_fit,
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
    profile: StreamProfile | None = None,
    peer_stats: PeerGroupStats | None = None,
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

    if profile and not (meta and meta.video_tracks) and profile.resolution:
        reasons.append(f"{profile.resolution}p_hint")
        if settings.min_video_height and profile.resolution < settings.min_video_height:
            score -= 6.0
            reasons.append("below_min_height_hint")
        else:
            score += min(8.0, profile.resolution / 240.0)

    if (
        profile
        and not (meta and meta.video_tracks)
        and profile.codec
        and settings.prefer_codec_list
    ):
        reasons.append(profile.codec.upper())
        for i, pref in enumerate(settings.prefer_codec_list):
            if pref == profile.codec:
                score += 4.0 - i
                reasons.append(f"pref_codec_hint:{pref}")
                break

    hdr_hint = False
    if profile and (profile.hdr or profile.dolby_vision):
        hdr_hint = True
    elif name_hint:
        nh = name_hint.lower()
        hdr_hint = "hdr" in nh or "dolby vision" in nh or " dv " in f" {nh} "
    if settings.prefer_hdr and hdr_hint:
        score += 2.0
        reasons.append("hdr_hint")

    size_score, size_reasons = score_profile_centrality(profile, peer_stats)
    if size_score:
        score += size_score
        reasons.extend(size_reasons)

    fit_score, fit_reasons = score_quality_fit(profile)
    if fit_score:
        score += fit_score
        reasons.extend(fit_reasons)

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


@dataclass(frozen=True)
class RankStats:
    total_ms: float
    p1_ms: float
    p2_ms: float
    p3_ms: float
    streams_in: int
    probed: int
    kept: int
    p2_attempted: int
    p2_ok: int
    p3_attempted: int
    p3_fail: int


def _ms_since(t0: float) -> float:
    return (monotonic() - t0) * 1000.0


async def _extract_mkv_metadata_incremental(
    *,
    url: str,
    client: httpx.AsyncClient,
    settings: Settings,
    sem: anyio.Semaphore,
    probe: ProbeResult | None,
) -> MkvMetadata | None:
    logger = logging.getLogger("uvicorn.error")
    max_bytes = settings.mkv_meta_max_bytes
    chunk_bytes = settings.mkv_meta_chunk_bytes

    base_ttfb = int(probe.ttfb_ms) if (probe and probe.ttfb_ms is not None) else settings.t_ttfb_ms
    meta_ttfb_ms = min(2000, max(settings.t_ttfb_ms, base_ttfb + 600))
    meta_total_ms = min(8000, max(settings.t_probe_total_ms, meta_ttfb_ms + 2500))

    buf = bytearray()
    offset = 0
    chunks = 0
    last_err: str | None = None
    while offset < max_bytes:
        want = min(chunk_bytes, max_bytes - offset)
        chunk, status, range_ok = await fetch_range_bytes(
            url=url,
            start=offset,
            length=want,
            client=client,
            settings=settings,
            sem=sem,
            total_timeout_ms=meta_total_ms,
            ttfb_timeout_ms=meta_ttfb_ms,
        )
        if chunk is None:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "mkv_meta failed no_bytes offset=%d ttfb_ms=%d total_ms=%d url=%s",
                    offset,
                    meta_ttfb_ms,
                    meta_total_ms,
                    url[:20] + "..." + url[-20:],
                )
            return None

        if chunk == b"" and not range_ok:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "mkv_meta range_mismatch offset=%d url=%s", offset, url[:20] + "..." + url[-20:]
                )
            # Treat as range ignored/misbehaving; fall back to a single bounded prefix request.
            prefix = await fetch_prefix_bytes(
                url=url,
                size=max_bytes,
                client=client,
                settings=settings,
                sem=sem,
                total_timeout_ms=meta_total_ms,
                ttfb_timeout_ms=meta_ttfb_ms,
            )
            if not prefix:
                return None
            try:
                meta = parse_mkv_metadata(prefix)
            except EbmlParseError:
                return None
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "mkv_meta ok fallback bytes=%d a=%d v=%d url=%s",
                    len(prefix),
                    len(meta.audio_tracks),
                    len(meta.video_tracks),
                    url[:20] + "..." + url[-20:],
                )
            return meta if (meta.audio_tracks or meta.video_tracks) else None

        # Server ignored range for non-zero offset; fall back to single bounded prefix read.
        if offset > 0 and status == 200:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "mkv_meta range_ignored falling_back url=%s", url[:20] + "..." + url[-20:]
                )
            prefix = await fetch_prefix_bytes(
                url=url,
                size=max_bytes,
                client=client,
                settings=settings,
                sem=sem,
                total_timeout_ms=meta_total_ms,
                ttfb_timeout_ms=meta_ttfb_ms,
            )
            if not prefix:
                return None
            try:
                meta = parse_mkv_metadata(prefix)
            except EbmlParseError:
                return None
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "mkv_meta ok fallback bytes=%d a=%d v=%d url=%s",
                    len(prefix),
                    len(meta.audio_tracks),
                    len(meta.video_tracks),
                    url[:20] + "..." + url[-20:],
                )
            return meta if (meta.audio_tracks or meta.video_tracks) else None

        buf.extend(chunk)
        offset += len(chunk)
        chunks += 1
        try:
            meta = parse_mkv_metadata(bytes(buf))
        except EbmlParseError as e:
            last_err = str(e)
            continue
        if meta.audio_tracks or meta.video_tracks:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "mkv_meta ok bytes=%d chunks=%d a=%d v=%d url=%s",
                    len(buf),
                    chunks,
                    len(meta.audio_tracks),
                    len(meta.video_tracks),
                    url[:20] + "..." + url[-20:],
                )
            return meta

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "mkv_meta failed cap bytes=%d chunks=%d last=%s url=%s",
            len(buf),
            chunks,
            last_err,
            url[:20] + "..." + url[-20:],
        )
    return None


async def reorder_streams(
    *,
    streams: list[dict[str, Any]],
    client: httpx.AsyncClient,
    settings: Settings,
    sem: anyio.Semaphore,
    probe_cache: TTLCache[ProbeResult] | None = None,
    meta_cache: TTLCache[MkvMetadata] | None = None,
) -> tuple[list[dict[str, Any]], RankStats]:
    logger = logging.getLogger("uvicorn.error")
    t_total = monotonic()
    candidate_indices: list[int] = []
    text_only_profiles: dict[int, StreamProfile] = {}
    for i, s in enumerate(streams):
        url = s.get("url")
        if isinstance(url, str) and url.startswith(("http://", "https://")):
            candidate_indices.append(i)
            text_only_profiles[i] = build_stream_profile(stream=s, probe=None, meta=None)

    prefilter_stats = build_peer_group_stats(list(text_only_profiles.values()))
    candidate_scores: list[tuple[int, float]] = []
    for idx in candidate_indices:
        profile = text_only_profiles[idx]
        score = 0.0
        score += max(0.0, 2.0 - (idx * 0.01))
        if profile.resolution:
            if settings.min_video_height and profile.resolution < settings.min_video_height:
                score -= 3.0
            else:
                score += min(6.0, profile.resolution / 360.0)
        if profile.codec:
            for pref_idx, pref in enumerate(settings.prefer_codec_list):
                if pref == profile.codec:
                    score += 4.0 - pref_idx
                    break
        if settings.prefer_hdr and (profile.hdr or profile.dolby_vision):
            score += 1.5
        centrality_score, _ = score_profile_centrality(profile, prefilter_stats)
        score += centrality_score * 0.5
        fit_score, _ = score_quality_fit(profile)
        score += fit_score
        candidate_scores.append((idx, score))
    candidate_scores.sort(key=lambda item: (-item[1], item[0]))
    url_indices = [idx for idx, _score in candidate_scores[: settings.top_k_phase1]]

    probes: dict[int, ProbeResult] = {}
    t_p1 = monotonic()
    uncached_probe_indices: list[int] = []
    for idx in url_indices:
        url = streams[idx]["url"]
        cached_probe = probe_cache.get(url) if probe_cache else None
        if cached_probe is not None:
            probes[idx] = cached_probe
        else:
            uncached_probe_indices.append(idx)
    if uncached_probe_indices:
        results = await gather(
            *[
                range_probe_1kb(
                    url=streams[i]["url"],
                    client=client,
                    settings=settings,
                    sem=sem,
                )
                for i in uncached_probe_indices
            ]
        )
        for idx, result in zip(uncached_probe_indices, results, strict=True):
            probes[idx] = result
            if probe_cache:
                probe_cache.set(streams[idx]["url"], result)
    p1_ms = _ms_since(t_p1)

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

    t_p2 = monotonic()
    metas: dict[int, MkvMetadata] = {}
    p2_status: dict[int, str] = {i: "-" for i in kept_top}
    uncached_meta_indices: list[int] = []
    for idx in top_for_phase2:
        url = streams[idx].get("url")
        if not isinstance(url, str):
            continue
        cached_meta = meta_cache.get(url) if meta_cache else None
        if cached_meta is not None:
            metas[idx] = cached_meta
            p2_status[idx] = "✓"
        else:
            uncached_meta_indices.append(idx)

    if uncached_meta_indices:
        meta_results = await gather(
            *[
                _extract_mkv_metadata_incremental(
                    url=streams[i]["url"],
                    client=client,
                    settings=settings,
                    sem=sem,
                    probe=probes.get(i),
                )
                for i in uncached_meta_indices
            ]
        )
        for idx, meta in zip(uncached_meta_indices, meta_results, strict=True):
            p2_status[idx] = "×"
            if meta and (meta.audio_tracks or meta.video_tracks or meta.duration_s):
                metas[idx] = meta
                p2_status[idx] = "✓"
                if meta_cache:
                    meta_cache.set(streams[idx]["url"], meta)
    p2_ms = _ms_since(t_p2)

    all_profiles: dict[int, StreamProfile] = {
        idx: build_stream_profile(stream=streams[idx], probe=probes.get(idx), meta=metas.get(idx))
        for idx in candidate_indices
    }
    peer_stats = build_peer_group_stats(list(all_profiles.values()))

    # Phase 3: weirdness probes (top-P by combined score).
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
                    profile=all_profiles.get(i),
                    peer_stats=peer_stats,
                ),
            )
        )
    scored.sort(key=lambda t: (-t[1].score, t[0]))

    t_p3 = monotonic()
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
    p3_ms = _ms_since(t_p3)

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
                    profile=all_profiles.get(i),
                    peer_stats=peer_stats,
                ),
            )
        )
    final_scored.sort(key=lambda t: (-t[1].score, t[0]))

    ranked_streams: list[dict[str, Any]] = []
    display_indices, display_positions = _select_display_indices(final_scored, all_profiles)

    for i in display_indices:
        s = dict(streams[i])
        label = _format_label_with_phases(
            probe=probes.get(i),
            meta=metas.get(i),
            p2=p2_status.get(i, "-"),
            p3=p3_status.get(i, "-"),
            tier=classify_resolution_tier(all_profiles.get(i)),
            tier_rank=display_positions.get(i),
        )
        if label:
            s["title"] = label if not s.get("title") else f"{label} — {s.get('title')}"
        s.setdefault("description", "")
        ranked_streams.append(s)
    total_ms = _ms_since(t_total)
    p2_attempted = sum(1 for v in p2_status.values() if v != "-")
    p2_ok = sum(1 for v in p2_status.values() if v == "✓")
    p3_attempted = sum(1 for v in p3_status.values() if v != "-")
    p3_fail = sum(1 for v in p3_status.values() if v == "×")
    stats = RankStats(
        total_ms=total_ms,
        p1_ms=p1_ms,
        p2_ms=p2_ms,
        p3_ms=p3_ms,
        streams_in=len(streams),
        probed=len(url_indices),
        kept=len(kept_top),
        p2_attempted=p2_attempted,
        p2_ok=p2_ok,
        p3_attempted=p3_attempted,
        p3_fail=p3_fail,
    )

    if logger.isEnabledFor(logging.DEBUG):
        for idx, sb in final_scored[: min(3, len(final_scored))]:
            url = streams[idx].get("url")
            logger.debug(
                "rank top score=%.1f idx=%d p2=%s p3=%s reasons=%s url=%s",
                sb.score,
                idx,
                p2_status.get(idx, "-"),
                p3_status.get(idx, "-"),
                ",".join(sb.reasons[:10]),
                url[:20] + "..." + url[-20:],
            )

    return ranked_streams, stats


def _format_label_with_phases(
    *,
    probe: ProbeResult | None,
    meta: MkvMetadata | None,
    p2: str,
    p3: str,
    tier: str | None,
    tier_rank: int | None,
) -> str:
    parts: list[str] = []
    if tier and tier_rank:
        parts.append(f"{_tier_display_name(tier)} #{tier_rank}")
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


def _select_display_indices(
    final_scored: list[tuple[int, ScoreBreakdown]],
    profiles: dict[int, StreamProfile],
) -> tuple[list[int], dict[int, int]]:
    grouped: dict[str, list[int]] = {"fhd": [], "uhd": []}
    for idx, _sb in final_scored:
        tier = classify_resolution_tier(profiles.get(idx))
        if tier in grouped and len(grouped[tier]) < 3:
            grouped[tier].append(idx)

    selected = grouped["fhd"] + grouped["uhd"]
    if not selected:
        selected = [idx for idx, _sb in final_scored[:3]]

    positions: dict[int, int] = {}
    for tier in ("fhd", "uhd"):
        for pos, idx in enumerate(grouped[tier], start=1):
            positions[idx] = pos
    return selected, positions


def _tier_display_name(tier: str) -> str:
    if tier == "uhd":
        return "4K"
    return "FHD"
