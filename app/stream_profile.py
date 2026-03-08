from __future__ import annotations

import math
import re
from dataclasses import dataclass
from statistics import median
from typing import Any

from app.mkv import MkvMetadata
from app.probe import ProbeResult

_IGNORE_TEXT_KEYS = {"url", "externalurl", "proxyheaders", "headers"}
_RESOLUTION_PATTERNS: tuple[tuple[re.Pattern[str], int], ...] = (
    (re.compile(r"\b(?:2160p|4k|uhd)\b", re.IGNORECASE), 2160),
    (re.compile(r"\b(?:1080p|fhd)\b", re.IGNORECASE), 1080),
    (re.compile(r"\b720p\b", re.IGNORECASE), 720),
    (re.compile(r"\b480p\b", re.IGNORECASE), 480),
)
_SIZE_PATTERN = re.compile(r"(\d+(?:[.,]\d+)?)\s*(gib|gb|mib|mb)\b", re.IGNORECASE)
_BITRATE_PATTERN = re.compile(r"(\d+(?:[.,]\d+)?)\s*mbps\b", re.IGNORECASE)
_HOUR_MIN_PATTERN = re.compile(
    r"(?:(\d+)\s*h(?:ours?)?)?\s*(\d+)\s*m(?:in(?:utes?)?)?\b", re.IGNORECASE
)
_MIN_ONLY_PATTERN = re.compile(r"\b(\d{2,3})\s*min\b", re.IGNORECASE)


@dataclass(frozen=True)
class StreamTextHints:
    resolution: int | None = None
    codec: str | None = None
    source: str | None = None
    hdr: bool = False
    dolby_vision: bool = False
    size_bytes: int | None = None
    duration_s: float | None = None
    bitrate_mbps: float | None = None


@dataclass(frozen=True)
class StreamProfile:
    resolution: int | None = None
    codec: str | None = None
    source: str | None = None
    hdr: bool = False
    dolby_vision: bool = False
    size_bytes: int | None = None
    duration_s: float | None = None
    bitrate_mbps_hint: float | None = None

    @property
    def bitrate_mbps(self) -> float | None:
        if self.bitrate_mbps_hint:
            return self.bitrate_mbps_hint
        if not self.size_bytes or not self.duration_s or self.duration_s <= 0:
            return None
        return (self.size_bytes * 8) / self.duration_s / 1_000_000


@dataclass(frozen=True)
class PeerGroupStats:
    full_size_medians: dict[tuple[int | None, str | None, str | None], float]
    codec_size_medians: dict[tuple[int | None, str | None], float]
    resolution_size_medians: dict[int | None, float]
    full_bitrate_medians: dict[tuple[int | None, str | None, str | None], float]
    codec_bitrate_medians: dict[tuple[int | None, str | None], float]
    resolution_bitrate_medians: dict[int | None, float]


def collect_stream_text(stream: dict[str, Any]) -> str:
    parts: list[str] = []

    def _walk(value: Any, *, key: str | None = None) -> None:
        if key and key.lower() in _IGNORE_TEXT_KEYS:
            return
        if isinstance(value, str):
            if value.strip():
                parts.append(value.strip())
            return
        if isinstance(value, dict):
            for child_key, child_value in value.items():
                _walk(child_value, key=str(child_key))
            return
        if isinstance(value, list):
            for item in value:
                _walk(item, key=key)

    _walk(stream)
    return " ".join(parts)


def parse_stream_text(text: str) -> StreamTextHints:
    normalized = f" {text} "
    resolution = next(
        (value for pattern, value in _RESOLUTION_PATTERNS if pattern.search(normalized)),
        None,
    )
    codec = _detect_codec(normalized)
    source = _detect_source(normalized)
    hdr = bool(re.search(r"\b(?:hdr10\+|hdr10|hdr)\b", normalized, re.IGNORECASE))
    dolby_vision = bool(re.search(r"\b(?:dolby[\s.-]*vision|dv)\b", normalized, re.IGNORECASE))
    size_bytes = _detect_size_bytes(normalized)
    duration_s = _detect_duration_seconds(normalized)
    bitrate_mbps = _detect_bitrate_mbps(normalized)
    return StreamTextHints(
        resolution=resolution,
        codec=codec,
        source=source,
        hdr=hdr,
        dolby_vision=dolby_vision,
        size_bytes=size_bytes,
        duration_s=duration_s,
        bitrate_mbps=bitrate_mbps,
    )


def build_stream_profile(
    *,
    stream: dict[str, Any],
    probe: ProbeResult | None,
    meta: MkvMetadata | None,
) -> StreamProfile:
    hints = parse_stream_text(collect_stream_text(stream))
    codec = _normalize_codec(meta.video_tracks[0].codec_id if meta and meta.video_tracks else None)
    if codec is None:
        codec = hints.codec
    resolution = None
    if meta and meta.video_tracks:
        heights = [track.height for track in meta.video_tracks if track.height]
        resolution = max(heights) if heights else None
    if hints.resolution and (resolution is None or hints.resolution > resolution):
        resolution = hints.resolution
    size_bytes = probe.total_size if probe and probe.total_size else hints.size_bytes
    duration_s = meta.duration_s if meta and meta.duration_s else hints.duration_s
    return StreamProfile(
        resolution=resolution,
        codec=codec,
        source=hints.source,
        hdr=hints.hdr,
        dolby_vision=hints.dolby_vision,
        size_bytes=size_bytes,
        duration_s=duration_s,
        bitrate_mbps_hint=hints.bitrate_mbps,
    )


def classify_resolution_tier(profile: StreamProfile | None) -> str | None:
    if profile is None or profile.resolution is None:
        return None
    if profile.resolution >= 1800:
        return "uhd"
    if profile.resolution >= 900:
        return "fhd"
    return None


def score_quality_fit(profile: StreamProfile | None) -> tuple[float, list[str]]:
    if profile is None:
        return 0.0, []

    tier = classify_resolution_tier(profile)
    reasons: list[str] = []
    score = 0.0

    if tier == "fhd" and profile.source == "remux":
        score -= 12.0
        reasons.append("remux_penalty:fhd")
    elif tier == "uhd" and profile.source == "remux":
        score -= 4.0
        reasons.append("remux_penalty:uhd")

    bitrate = profile.bitrate_mbps
    if bitrate is None:
        return score, reasons

    target = _bitrate_target_band(tier=tier, codec=profile.codec)
    if target is None:
        return score, reasons

    low, high = target
    if low <= bitrate <= high:
        score += 6.0
    else:
        midpoint = (low + high) / 2
        width = max(1.0, (high - low) / 2)
        deviation = abs(bitrate - midpoint) / width
        score += max(-12.0, 6.0 - (deviation * 6.0))
    reasons.append(f"bitrate_band:{bitrate:.1f}/{low:.0f}-{high:.0f}")
    return score, reasons


def build_peer_group_stats(profiles: list[StreamProfile]) -> PeerGroupStats:
    full_sizes: dict[tuple[int | None, str | None, str | None], list[float]] = {}
    codec_sizes: dict[tuple[int | None, str | None], list[float]] = {}
    resolution_sizes: dict[int | None, list[float]] = {}
    full_bitrates: dict[tuple[int | None, str | None, str | None], list[float]] = {}
    codec_bitrates: dict[tuple[int | None, str | None], list[float]] = {}
    resolution_bitrates: dict[int | None, list[float]] = {}

    for profile in profiles:
        full_key = (profile.resolution, profile.codec, profile.source)
        codec_key = (profile.resolution, profile.codec)
        res_key = profile.resolution
        if profile.size_bytes:
            full_sizes.setdefault(full_key, []).append(float(profile.size_bytes))
            codec_sizes.setdefault(codec_key, []).append(float(profile.size_bytes))
            resolution_sizes.setdefault(res_key, []).append(float(profile.size_bytes))
        bitrate = profile.bitrate_mbps
        if bitrate:
            full_bitrates.setdefault(full_key, []).append(bitrate)
            codec_bitrates.setdefault(codec_key, []).append(bitrate)
            resolution_bitrates.setdefault(res_key, []).append(bitrate)

    return PeerGroupStats(
        full_size_medians={
            key: median(values) for key, values in full_sizes.items() if len(values) >= 3
        },
        codec_size_medians={
            key: median(values) for key, values in codec_sizes.items() if len(values) >= 3
        },
        resolution_size_medians={
            key: median(values) for key, values in resolution_sizes.items() if len(values) >= 3
        },
        full_bitrate_medians={
            key: median(values) for key, values in full_bitrates.items() if len(values) >= 3
        },
        codec_bitrate_medians={
            key: median(values) for key, values in codec_bitrates.items() if len(values) >= 3
        },
        resolution_bitrate_medians={
            key: median(values) for key, values in resolution_bitrates.items() if len(values) >= 3
        },
    )


def score_profile_centrality(
    profile: StreamProfile | None, stats: PeerGroupStats | None
) -> tuple[float, list[str]]:
    if profile is None or stats is None:
        return 0.0, []

    full_key = (profile.resolution, profile.codec, profile.source)
    codec_key = (profile.resolution, profile.codec)
    res_key = profile.resolution

    bitrate = profile.bitrate_mbps
    if bitrate:
        median_value = (
            stats.full_bitrate_medians.get(full_key)
            or stats.codec_bitrate_medians.get(codec_key)
            or stats.resolution_bitrate_medians.get(res_key)
        )
        if median_value:
            score = _centrality_score(bitrate, median_value)
            return score, [f"bitrate_center:{bitrate:.1f}/{median_value:.1f}"]

    if profile.size_bytes:
        median_value = (
            stats.full_size_medians.get(full_key)
            or stats.codec_size_medians.get(codec_key)
            or stats.resolution_size_medians.get(res_key)
        )
        if median_value:
            size_gb = profile.size_bytes / (1024**3)
            median_gb = median_value / (1024**3)
            score = _centrality_score(float(profile.size_bytes), median_value)
            return score, [f"size_center:{size_gb:.1f}/{median_gb:.1f}GB"]

    return 0.0, []


def _centrality_score(value: float, median_value: float) -> float:
    if value <= 0 or median_value <= 0:
        return 0.0
    deviation = abs(math.log(value / median_value))
    return max(-10.0, 10.0 - (deviation * 10.0))


def _detect_codec(text: str) -> str | None:
    if re.search(r"\b(?:x265|h\.?265|hevc)\b", text, re.IGNORECASE):
        return "hevc"
    if re.search(r"\bav1\b", text, re.IGNORECASE):
        return "av1"
    if re.search(r"\b(?:x264|h\.?264|avc)\b", text, re.IGNORECASE):
        return "avc"
    return None


def _detect_source(text: str) -> str | None:
    if re.search(r"\bremux\b", text, re.IGNORECASE):
        return "remux"
    if re.search(r"\bblu[\s.-]?ray\b", text, re.IGNORECASE):
        return "bluray"
    if re.search(r"\bweb[\s.-]?dl\b", text, re.IGNORECASE):
        return "web-dl"
    if re.search(r"\bweb[\s.-]?rip\b", text, re.IGNORECASE):
        return "webrip"
    return None


def _detect_size_bytes(text: str) -> int | None:
    match = _SIZE_PATTERN.search(text)
    if not match:
        return None
    value = float(match.group(1).replace(",", "."))
    unit = match.group(2).lower()
    if unit in {"gb", "gib"}:
        return int(value * 1024**3)
    if unit in {"mb", "mib"}:
        return int(value * 1024**2)
    return None


def _detect_duration_seconds(text: str) -> float | None:
    match = _HOUR_MIN_PATTERN.search(text)
    if match:
        hours = int(match.group(1) or "0")
        minutes = int(match.group(2))
        if hours or minutes:
            return float((hours * 3600) + (minutes * 60))
    match = _MIN_ONLY_PATTERN.search(text)
    if match:
        return float(int(match.group(1)) * 60)
    return None


def _detect_bitrate_mbps(text: str) -> float | None:
    match = _BITRATE_PATTERN.search(text)
    if not match:
        return None
    return float(match.group(1).replace(",", "."))


def _normalize_codec(codec_id: str | None) -> str | None:
    if not codec_id:
        return None
    lowered = codec_id.lower()
    if "hevc" in lowered or "hvc" in lowered:
        return "hevc"
    if "av1" in lowered:
        return "av1"
    if "avc" in lowered or "h264" in lowered:
        return "avc"
    return None


def _bitrate_target_band(*, tier: str | None, codec: str | None) -> tuple[float, float] | None:
    if tier == "fhd":
        if codec == "hevc":
            return 4.0, 10.0
        if codec == "av1":
            return 4.0, 9.0
        return 7.0, 16.0
    if tier == "uhd":
        if codec == "av1":
            return 10.0, 22.0
        if codec == "hevc":
            return 14.0, 32.0
        return 20.0, 40.0
    return None
