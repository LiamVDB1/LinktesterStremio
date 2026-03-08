from __future__ import annotations

from app.config import Settings
from app.mkv import AudioTrack, MkvMetadata, VideoTrack
from app.probe import ProbeResult
from app.ranking import score_from_probe
from app.stream_profile import StreamProfile, build_peer_group_stats


def test_scoring_prefers_preferred_audio_language() -> None:
    settings = Settings(UPSTREAM_BASE_URL="http://upstream.invalid", PREFERRED_AUDIO_LANGS="nl,en")
    base_probe = ProbeResult(
        ok=True,
        hard_fail=False,
        url="http://x",
        status_code=206,
        seekable=True,
        content_type="video/x-matroska",
        total_size=100_000,
        ttfb_ms=50.0,
        dl_1kb_ms=70.0,
        magic_mkv=True,
    )
    meta_nl = MkvMetadata(
        video_tracks=[VideoTrack(codec_id="V_MPEGH/ISO/HEVC", height=1080)],
        audio_tracks=[
            AudioTrack(
                codec_id="A_AAC",
                lang="nld",
                lang_bcp47="nl",
                channels=2,
                default=True,
                original=False,
            )
        ],
        duration_s=5400,
    )
    meta_und = MkvMetadata(
        video_tracks=[VideoTrack(codec_id="V_MPEGH/ISO/HEVC", height=1080)],
        audio_tracks=[
            AudioTrack(
                codec_id="A_AAC",
                lang="und",
                lang_bcp47=None,
                channels=2,
                default=True,
                original=False,
            )
        ],
        duration_s=5400,
    )

    s1 = score_from_probe(probe=base_probe, meta=meta_nl, upstream_index=0, settings=settings)
    s2 = score_from_probe(probe=base_probe, meta=meta_und, upstream_index=0, settings=settings)
    assert s1.score > s2.score


def test_scoring_prefers_stream_near_size_median() -> None:
    settings = Settings(UPSTREAM_BASE_URL="http://upstream.invalid")
    base_probe = ProbeResult(
        ok=True,
        hard_fail=False,
        url="http://x",
        status_code=206,
        seekable=True,
        content_type="video/x-matroska",
        total_size=4 * 1024**3,
        ttfb_ms=50.0,
        dl_1kb_ms=70.0,
        magic_mkv=True,
    )
    meta = MkvMetadata(
        video_tracks=[VideoTrack(codec_id="V_MPEGH/ISO/HEVC", height=1080)],
        audio_tracks=[],
        duration_s=7200,
    )
    peer_stats = build_peer_group_stats(
        [
            StreamProfile(resolution=1080, codec="hevc", size_bytes=3 * 1024**3, duration_s=7200),
            StreamProfile(resolution=1080, codec="hevc", size_bytes=4 * 1024**3, duration_s=7200),
            StreamProfile(resolution=1080, codec="hevc", size_bytes=18 * 1024**3, duration_s=7200),
        ]
    )

    centered = score_from_probe(
        probe=base_probe,
        meta=meta,
        upstream_index=0,
        settings=settings,
        peer_stats=peer_stats,
    )
    huge = score_from_probe(
        probe=ProbeResult(
            ok=True,
            hard_fail=False,
            url="http://y",
            status_code=206,
            seekable=True,
            content_type="video/x-matroska",
            total_size=18 * 1024**3,
            ttfb_ms=50.0,
            dl_1kb_ms=70.0,
            magic_mkv=True,
        ),
        meta=meta,
        upstream_index=1,
        settings=settings,
        peer_stats=peer_stats,
    )

    assert centered.score > huge.score


def test_scoring_penalizes_fhd_remux_outlier() -> None:
    settings = Settings(UPSTREAM_BASE_URL="http://upstream.invalid")
    peer_stats = build_peer_group_stats(
        [
            StreamProfile(resolution=1080, codec="hevc", source="bluray", bitrate_mbps_hint=5.0),
            StreamProfile(resolution=1080, codec="hevc", source="bluray", bitrate_mbps_hint=6.0),
            StreamProfile(resolution=1080, codec="avc", source="remux", bitrate_mbps_hint=38.0),
        ]
    )

    common_probe = dict(
        ok=True,
        hard_fail=False,
        status_code=206,
        seekable=True,
        content_type="video/x-matroska",
        ttfb_ms=250.0,
        dl_1kb_ms=300.0,
        magic_mkv=True,
    )
    lean = score_from_probe(
        probe=ProbeResult(url="http://lean", total_size=4 * 1024**3, **common_probe),
        meta=None,
        upstream_index=0,
        settings=settings,
        profile=StreamProfile(
            resolution=1080, codec="hevc", source="bluray", bitrate_mbps_hint=5.2
        ),
        peer_stats=peer_stats,
    )
    remux = score_from_probe(
        probe=ProbeResult(url="http://remux", total_size=33 * 1024**3, **common_probe),
        meta=None,
        upstream_index=1,
        settings=settings,
        profile=StreamProfile(resolution=1080, codec="avc", source="remux", bitrate_mbps_hint=39.5),
        peer_stats=peer_stats,
    )

    assert lean.score > remux.score
