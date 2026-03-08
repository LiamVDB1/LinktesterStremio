from __future__ import annotations

import pytest

from app.mkv import EbmlParseError, parse_mkv_metadata
from tests.ebml_builder import MiniMkvSpec, build_mini_mkv_bytes


def test_parse_mkv_metadata_extracts_tracks() -> None:
    b = build_mini_mkv_bytes(MiniMkvSpec(height=720, video_codec="V_MPEGH/ISO/HEVC"))
    meta = parse_mkv_metadata(b)
    assert meta.video_tracks and meta.video_tracks[0].height == 720
    assert meta.audio_tracks and (meta.audio_tracks[0].lang in ("nld", "nl"))
    assert meta.duration_s is not None
    assert round(meta.duration_s) == 5400


def test_parse_mkv_metadata_rejects_non_mkv() -> None:
    with pytest.raises(EbmlParseError):
        parse_mkv_metadata(b"not-mkv")
