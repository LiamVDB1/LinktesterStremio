from __future__ import annotations

from app.config import Settings
from app.mkv import AudioTrack, MkvMetadata, VideoTrack
from app.probe import ProbeResult
from app.ranking import score_from_probe


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
    )

    s1 = score_from_probe(probe=base_probe, meta=meta_nl, upstream_index=0, settings=settings)
    s2 = score_from_probe(probe=base_probe, meta=meta_und, upstream_index=0, settings=settings)
    assert s1.score > s2.score
