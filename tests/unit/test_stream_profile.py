from __future__ import annotations

from app.stream_profile import (
    StreamProfile,
    build_peer_group_stats,
    classify_resolution_tier,
    collect_stream_text,
    parse_stream_text,
    score_profile_centrality,
    score_quality_fit,
)


def test_collect_stream_text_includes_user_visible_fields_and_behavior_hints() -> None:
    stream = {
        "name": "AIO",
        "title": "The.Show.S01E01.1080p.WEB-DL.x265.HDR.4.8 GB",
        "description": "Dual Audio NL/EN",
        "url": "https://example.test/stream",
        "behaviorHints": {"bingeGroup": "1080p HEVC HDR"},
    }

    text = collect_stream_text(stream)

    assert "1080p.WEB-DL.x265.HDR.4.8 GB" in text
    assert "Dual Audio NL/EN" in text
    assert "1080p HEVC HDR" in text
    assert "https://example.test/stream" not in text


def test_parse_stream_text_extracts_quality_and_size_hints() -> None:
    hints = parse_stream_text("Movie.2024.2160p.NF.WEB-DL.DV.HDR10+.HEVC.17.6 GB 18.4 Mbps")

    assert hints.resolution == 2160
    assert hints.codec == "hevc"
    assert hints.source == "web-dl"
    assert hints.hdr is True
    assert hints.dolby_vision is True
    assert hints.size_bytes is not None
    assert hints.bitrate_mbps == 18.4
    assert round(hints.size_bytes / (1024**3), 1) == 17.6


def test_peer_group_centrality_prefers_reasonable_middle_size() -> None:
    profiles = [
        StreamProfile(resolution=1080, codec="hevc", size_bytes=2 * 1024**3),
        StreamProfile(resolution=1080, codec="hevc", size_bytes=4 * 1024**3),
        StreamProfile(resolution=1080, codec="hevc", size_bytes=20 * 1024**3),
    ]

    stats = build_peer_group_stats(profiles)

    small_score, _ = score_profile_centrality(profiles[0], stats)
    middle_score, reasons = score_profile_centrality(profiles[1], stats)
    huge_score, _ = score_profile_centrality(profiles[2], stats)

    assert middle_score > small_score
    assert middle_score > huge_score
    assert any("size_center" in reason for reason in reasons)


def test_peer_group_uses_bitrate_when_duration_is_known() -> None:
    profiles = [
        StreamProfile(
            resolution=2160,
            codec="hevc",
            size_bytes=18 * 1024**3,
            duration_s=2 * 3600,
        ),
        StreamProfile(
            resolution=2160,
            codec="hevc",
            size_bytes=20 * 1024**3,
            duration_s=2 * 3600,
        ),
        StreamProfile(
            resolution=2160,
            codec="hevc",
            size_bytes=42 * 1024**3,
            duration_s=2 * 3600,
        ),
    ]

    stats = build_peer_group_stats(profiles)

    center_score, reasons = score_profile_centrality(profiles[1], stats)
    huge_score, _ = score_profile_centrality(profiles[2], stats)

    assert center_score > huge_score
    assert any("bitrate_center" in reason for reason in reasons)


def test_quality_fit_prefers_reasonable_fhd_hevc_over_fhd_remux() -> None:
    lean_hevc = StreamProfile(
        resolution=1080,
        codec="hevc",
        source="bluray",
        bitrate_mbps_hint=5.2,
    )
    heavy_remux = StreamProfile(
        resolution=1080,
        codec="avc",
        source="remux",
        bitrate_mbps_hint=39.5,
    )

    lean_score, lean_reasons = score_quality_fit(lean_hevc)
    heavy_score, heavy_reasons = score_quality_fit(heavy_remux)

    assert lean_score > heavy_score
    assert any("bitrate_band" in reason for reason in lean_reasons)
    assert any("remux_penalty" in reason for reason in heavy_reasons)


def test_resolution_tier_classifies_only_fhd_and_4k_buckets() -> None:
    assert classify_resolution_tier(StreamProfile(resolution=1080)) == "fhd"
    assert classify_resolution_tier(StreamProfile(resolution=2160)) == "uhd"
    assert classify_resolution_tier(StreamProfile(resolution=720)) is None
