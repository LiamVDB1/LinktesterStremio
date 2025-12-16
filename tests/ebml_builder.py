from __future__ import annotations

from dataclasses import dataclass


def _encode_size(size: int) -> bytes:
    if size < 0:
        raise ValueError("size must be >= 0")
    for length in range(1, 9):
        maxv = (1 << (7 * length)) - 1
        if size <= maxv:
            break
    else:
        raise ValueError("size too large")
    marker = 1 << (8 - length)
    encoded = size.to_bytes(length, "big")
    first = (encoded[0] & ((1 << (8 - length)) - 1)) | marker
    return bytes([first]) + encoded[1:]


def _elm(id_bytes: bytes, payload: bytes) -> bytes:
    return id_bytes + _encode_size(len(payload)) + payload


def _u(i: int, nbytes: int | None = None) -> bytes:
    if i < 0:
        raise ValueError("uint must be >= 0")
    if nbytes is None:
        nbytes = max(1, (i.bit_length() + 7) // 8)
    return i.to_bytes(nbytes, "big")


def _s(s: str) -> bytes:
    return s.encode("utf-8")


ID_EBML = bytes.fromhex("1A45DFA3")
ID_SEGMENT = bytes.fromhex("18538067")
ID_INFO = bytes.fromhex("1549A966")
ID_TRACKS = bytes.fromhex("1654AE6B")
ID_TRACK_ENTRY = bytes.fromhex("AE")
ID_TRACK_TYPE = bytes.fromhex("83")
ID_FLAG_DEFAULT = bytes.fromhex("88")
ID_LANGUAGE = bytes.fromhex("22B59C")
ID_LANGUAGE_BCP47 = bytes.fromhex("22B59D")
ID_CODEC_ID = bytes.fromhex("86")
ID_VIDEO = bytes.fromhex("E0")
ID_AUDIO = bytes.fromhex("E1")
ID_PIXEL_HEIGHT = bytes.fromhex("BA")
ID_CHANNELS = bytes.fromhex("9F")
ID_VOID = bytes.fromhex("EC")


@dataclass(frozen=True)
class MiniMkvSpec:
    height: int = 1080
    video_codec: str = "V_MPEGH/ISO/HEVC"
    audio_lang: str = "nld"
    audio_lang_bcp47: str = "nl"
    audio_codec: str = "A_AAC"
    audio_channels: int = 2


def build_mini_mkv_bytes(spec: MiniMkvSpec | None = None) -> bytes:
    spec = spec or MiniMkvSpec()
    # EBML header: minimal, payload empty; enough for magic bytes and Element structure.
    ebml_header = _elm(ID_EBML, b"")

    info = _elm(ID_INFO, b"")

    video = _elm(ID_VIDEO, _elm(ID_PIXEL_HEIGHT, _u(spec.height)))
    video_entry = _elm(
        ID_TRACK_ENTRY,
        b"".join(
            [
                _elm(ID_TRACK_TYPE, _u(1)),
                _elm(ID_CODEC_ID, _s(spec.video_codec)),
                video,
            ]
        ),
    )

    audio = _elm(ID_AUDIO, _elm(ID_CHANNELS, _u(spec.audio_channels)))
    audio_entry = _elm(
        ID_TRACK_ENTRY,
        b"".join(
            [
                _elm(ID_TRACK_TYPE, _u(2)),
                _elm(ID_FLAG_DEFAULT, _u(1)),
                _elm(ID_LANGUAGE, _s(spec.audio_lang)),
                _elm(ID_LANGUAGE_BCP47, _s(spec.audio_lang_bcp47)),
                _elm(ID_CODEC_ID, _s(spec.audio_codec)),
                audio,
            ]
        ),
    )

    tracks = _elm(ID_TRACKS, video_entry + audio_entry)
    segment_payload = info + tracks
    segment = _elm(ID_SEGMENT, segment_payload)

    # Add padding to simulate file size.
    return ebml_header + segment + (b"\x00" * 8192)


def build_mkv_with_delayed_tracks(
    *, tracks_start_at: int, spec: MiniMkvSpec | None = None
) -> bytes:
    spec = spec or MiniMkvSpec()
    ebml_header = _elm(ID_EBML, b"")

    info = _elm(ID_INFO, b"")

    video = _elm(ID_VIDEO, _elm(ID_PIXEL_HEIGHT, _u(spec.height)))
    video_entry = _elm(
        ID_TRACK_ENTRY,
        b"".join([_elm(ID_TRACK_TYPE, _u(1)), _elm(ID_CODEC_ID, _s(spec.video_codec)), video]),
    )

    audio = _elm(ID_AUDIO, _elm(ID_CHANNELS, _u(spec.audio_channels)))
    audio_entry = _elm(
        ID_TRACK_ENTRY,
        b"".join(
            [
                _elm(ID_TRACK_TYPE, _u(2)),
                _elm(ID_FLAG_DEFAULT, _u(1)),
                _elm(ID_LANGUAGE, _s(spec.audio_lang)),
                _elm(ID_LANGUAGE_BCP47, _s(spec.audio_lang_bcp47)),
                _elm(ID_CODEC_ID, _s(spec.audio_codec)),
                audio,
            ]
        ),
    )

    tracks = _elm(ID_TRACKS, video_entry + audio_entry)

    void_payload_len = 0
    while True:
        void = _elm(ID_VOID, b"\x00" * void_payload_len)
        segment_payload = info + void + tracks
        segment_header = ID_SEGMENT + _encode_size(len(segment_payload))
        actual_tracks_start = len(ebml_header) + len(segment_header) + len(info) + len(void)
        if actual_tracks_start >= tracks_start_at:
            segment = segment_header + segment_payload
            return ebml_header + segment + (b"\x00" * 8192)
        void_payload_len += tracks_start_at - actual_tracks_start
