from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

EBML_MAGIC = b"\x1a\x45\xdf\xa3"

# IDs (bytes) we care about.
ID_EBML = bytes.fromhex("1A45DFA3")
ID_SEGMENT = bytes.fromhex("18538067")
ID_INFO = bytes.fromhex("1549A966")
ID_TRACKS = bytes.fromhex("1654AE6B")
ID_TRACK_ENTRY = bytes.fromhex("AE")
ID_TRACK_TYPE = bytes.fromhex("83")
ID_FLAG_DEFAULT = bytes.fromhex("88")
ID_FLAG_ORIGINAL = bytes.fromhex("55AE")
ID_LANGUAGE = bytes.fromhex("22B59C")
ID_LANGUAGE_BCP47 = bytes.fromhex("22B59D")
ID_CODEC_ID = bytes.fromhex("86")
ID_VIDEO = bytes.fromhex("E0")
ID_AUDIO = bytes.fromhex("E1")
ID_PIXEL_HEIGHT = bytes.fromhex("BA")
ID_CHANNELS = bytes.fromhex("9F")


@dataclass(frozen=True)
class VideoTrack:
    codec_id: str | None
    height: int | None


@dataclass(frozen=True)
class AudioTrack:
    codec_id: str | None
    lang: str | None
    lang_bcp47: str | None
    channels: int | None
    default: bool
    original: bool


@dataclass(frozen=True)
class MkvMetadata:
    video_tracks: list[VideoTrack]
    audio_tracks: list[AudioTrack]


class EbmlParseError(RuntimeError):
    pass


def _read_vint(data: bytes, pos: int, *, is_id: bool) -> tuple[int, int]:
    if pos >= len(data):
        raise EbmlParseError("unexpected EOF")
    first = data[pos]
    mask = 0x80
    length = 1
    while length <= 8 and (first & mask) == 0:
        mask >>= 1
        length += 1
    if length > 8:
        raise EbmlParseError("invalid vint")
    if pos + length > len(data):
        raise EbmlParseError("truncated vint")
    raw = data[pos : pos + length]
    if is_id:
        value = int.from_bytes(raw, "big")
    else:
        value = raw[0] & (mask - 1)
        for b in raw[1:]:
            value = (value << 8) | b
        # Unknown size (all 1s) is allowed; treat as -1 and let callers decide.
        if value == (1 << (7 * length)) - 1:
            value = -1
    return value, pos + length


def _iter_elements(data: bytes, start: int, end: int) -> Iterable[tuple[int, int, int]]:
    pos = start
    while pos < end:
        element_id, pos2 = _read_vint(data, pos, is_id=True)
        size, pos3 = _read_vint(data, pos2, is_id=False)
        payload_start = pos3
        payload_end = end if size < 0 else payload_start + size
        if payload_end > end:
            return
        yield element_id, payload_start, payload_end
        if size < 0:
            return
        pos = payload_end


def _decode_str(b: bytes) -> str:
    try:
        return b.decode("utf-8", "ignore").strip("\x00").strip()
    except Exception:
        return ""


def _decode_uint(b: bytes) -> int:
    if not b:
        return 0
    return int.from_bytes(b, "big", signed=False)


def parse_mkv_metadata(prefix_bytes: bytes) -> MkvMetadata:
    if not prefix_bytes.startswith(EBML_MAGIC):
        raise EbmlParseError("missing EBML magic")

    video_tracks: list[VideoTrack] = []
    audio_tracks: list[AudioTrack] = []

    # Find Segment, then walk its children to find Tracks.
    segment_payload: tuple[int, int] | None = None
    for element_id, p0, p1 in _iter_elements(prefix_bytes, 0, len(prefix_bytes)):
        if element_id == int.from_bytes(ID_SEGMENT, "big"):
            segment_payload = (p0, p1)
            break

    if not segment_payload:
        raise EbmlParseError("missing Segment")

    seg0, seg1 = segment_payload
    for element_id, p0, p1 in _iter_elements(prefix_bytes, seg0, seg1):
        if element_id != int.from_bytes(ID_TRACKS, "big"):
            continue
        _parse_tracks(prefix_bytes[p0:p1], video_tracks, audio_tracks)
        break

    return MkvMetadata(video_tracks=video_tracks, audio_tracks=audio_tracks)


def _parse_tracks(
    tracks_bytes: bytes, video_tracks: list[VideoTrack], audio_tracks: list[AudioTrack]
) -> None:
    # tracks_bytes includes only payload; parse its children directly.
    for element_id, p0, p1 in _iter_elements(tracks_bytes, 0, len(tracks_bytes)):
        if element_id != int.from_bytes(ID_TRACK_ENTRY, "big"):
            continue
        _parse_track_entry(tracks_bytes[p0:p1], video_tracks, audio_tracks)


def _parse_track_entry(
    entry: bytes, video_tracks: list[VideoTrack], audio_tracks: list[AudioTrack]
) -> None:
    track_type: int | None = None
    codec_id: str | None = None
    lang: str | None = None
    lang_bcp47: str | None = None
    flag_default = False
    flag_original = False
    height: int | None = None
    channels: int | None = None

    for element_id, p0, p1 in _iter_elements(entry, 0, len(entry)):
        payload = entry[p0:p1]
        if element_id == int.from_bytes(ID_TRACK_TYPE, "big"):
            track_type = _decode_uint(payload)
        elif element_id == int.from_bytes(ID_CODEC_ID, "big"):
            codec_id = _decode_str(payload)
        elif element_id == int.from_bytes(ID_LANGUAGE, "big"):
            lang = _decode_str(payload)
        elif element_id == int.from_bytes(ID_LANGUAGE_BCP47, "big"):
            lang_bcp47 = _decode_str(payload)
        elif element_id == int.from_bytes(ID_FLAG_DEFAULT, "big"):
            flag_default = _decode_uint(payload) == 1
        elif element_id == int.from_bytes(ID_FLAG_ORIGINAL, "big"):
            flag_original = _decode_uint(payload) == 1
        elif element_id == int.from_bytes(ID_VIDEO, "big"):
            height = _parse_video(payload)
        elif element_id == int.from_bytes(ID_AUDIO, "big"):
            channels = _parse_audio(payload)

    if track_type == 1:
        video_tracks.append(VideoTrack(codec_id=codec_id, height=height))
    elif track_type == 2:
        audio_tracks.append(
            AudioTrack(
                codec_id=codec_id,
                lang=lang,
                lang_bcp47=lang_bcp47,
                channels=channels,
                default=flag_default,
                original=flag_original,
            )
        )


def _parse_video(video_bytes: bytes) -> int | None:
    for element_id, p0, p1 in _iter_elements(video_bytes, 0, len(video_bytes)):
        if element_id == int.from_bytes(ID_PIXEL_HEIGHT, "big"):
            return _decode_uint(video_bytes[p0:p1])
    return None


def _parse_audio(audio_bytes: bytes) -> int | None:
    for element_id, p0, p1 in _iter_elements(audio_bytes, 0, len(audio_bytes)):
        if element_id == int.from_bytes(ID_CHANNELS, "big"):
            return _decode_uint(audio_bytes[p0:p1])
    return None
