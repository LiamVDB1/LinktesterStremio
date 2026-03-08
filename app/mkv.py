from __future__ import annotations

import logging
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

# SeekHead related IDs
ID_SEEK_HEAD = bytes.fromhex("114D9B74")
ID_SEEK = bytes.fromhex("4DBB")
ID_SEEK_ID = bytes.fromhex("53AB")
ID_SEEK_POSITION = bytes.fromhex("53AC")
ID_DURATION = bytes.fromhex("4489")
ID_TIMECODE_SCALE = bytes.fromhex("2AD7B1")


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
    duration_s: float | None = None


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
            # Partial buffer: yield a truncated payload and stop (common for Segment).
            yield element_id, payload_start, end
            return
        yield element_id, payload_start, payload_end
        if size < 0:
            # Unknown-size element consumes remainder of buffer; cannot safely continue.
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


def _decode_float(b: bytes) -> float | None:
    if len(b) == 4:
        import struct

        return struct.unpack(">f", b)[0]
    if len(b) == 8:
        import struct

        return struct.unpack(">d", b)[0]
    return None


def _read_element_at(data: bytes, pos: int, *, end: int) -> tuple[int, int, int]:
    """
    Read a single EBML element header at absolute position `pos` in `data`.
    Returns (element_id_int, payload_start, payload_end).
    Raises EbmlParseError if truncated.
    """
    if pos >= end:
        raise EbmlParseError("element_at: pos beyond end")

    element_id, pos2 = _read_vint(data, pos, is_id=True)
    size, pos3 = _read_vint(data, pos2, is_id=False)
    payload_start = pos3
    payload_end = end if size < 0 else payload_start + size

    if payload_end > end:
        raise EbmlParseError("element_at: truncated payload")
    return element_id, payload_start, payload_end


def _seekhead_tracks_offset(seekhead_payload: bytes) -> int | None:
    """
    Parse SeekHead payload (not including header) and return SeekPosition for Tracks (relative to Segment data start),
    or None if not present.
    """
    tracks_id_int = int.from_bytes(ID_TRACKS, "big")
    for element_id, p0, p1 in _iter_elements(seekhead_payload, 0, len(seekhead_payload)):
        if element_id != int.from_bytes(ID_SEEK, "big"):
            continue
        seek_entry = seekhead_payload[p0:p1]
        seek_id_int: int | None = None
        seek_pos: int | None = None
        for sid, q0, q1 in _iter_elements(seek_entry, 0, len(seek_entry)):
            payload = seek_entry[q0:q1]
            if sid == int.from_bytes(ID_SEEK_ID, "big"):
                # payload is the raw element ID bytes (vint-form); interpret as int and compare.
                seek_id_int = int.from_bytes(payload, "big")
            elif sid == int.from_bytes(ID_SEEK_POSITION, "big"):
                seek_pos = _decode_uint(payload)
        if seek_id_int == tracks_id_int and seek_pos is not None:
            return seek_pos
    return None


def parse_mkv_metadata(prefix_bytes: bytes) -> MkvMetadata:
    logger = logging.getLogger("uvicorn.error")
    if not prefix_bytes.startswith(EBML_MAGIC):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("mkv parse fail missing_magic bytes=%d", len(prefix_bytes))
        raise EbmlParseError("missing EBML magic")

    video_tracks: list[VideoTrack] = []
    audio_tracks: list[AudioTrack] = []
    duration_s: float | None = None

    # Find Segment.
    segment_payload: tuple[int, int] | None = None
    for element_id, p0, p1 in _iter_elements(prefix_bytes, 0, len(prefix_bytes)):
        if element_id == int.from_bytes(ID_SEGMENT, "big"):
            segment_payload = (p0, p1)
            break

    if not segment_payload:
        raise EbmlParseError("missing Segment")

    seg0, seg1 = segment_payload  # seg0 is Segment DATA start (payload start)
    segment_data_start = seg0

    # Pass 1: scan top-level elements inside Segment until buffer ends.
    # We try:
    #  - if we hit Tracks normally: parse and return
    #  - if we see SeekHead: remember tracks offset
    tracks_seek_offset: int | None = None

    for element_id, p0, p1 in _iter_elements(prefix_bytes, seg0, seg1):
        if element_id == int.from_bytes(ID_SEEK_HEAD, "big"):
            # Parse SeekHead payload and remember tracks position if present.
            seekhead_payload = prefix_bytes[p0:p1]
            tracks_seek_offset = _seekhead_tracks_offset(seekhead_payload)
            if logger.isEnabledFor(logging.DEBUG) and tracks_seek_offset is not None:
                logger.debug(
                    "mkv seekhead tracks_offset=%d seg_data_start=%d bytes=%d",
                    tracks_seek_offset,
                    segment_data_start,
                    len(prefix_bytes),
                )
        elif element_id == int.from_bytes(ID_INFO, "big"):
            duration_s = _parse_info_duration(prefix_bytes[p0:p1])
        elif element_id == int.from_bytes(ID_TRACKS, "big"):
            _parse_tracks(prefix_bytes[p0:p1], video_tracks, audio_tracks)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "mkv tracks sequential a=%d v=%d bytes=%d",
                    len(audio_tracks),
                    len(video_tracks),
                    len(prefix_bytes),
                )
            return MkvMetadata(
                video_tracks=video_tracks,
                audio_tracks=audio_tracks,
                duration_s=duration_s,
            )

    # If we didn't find Tracks by sequential scan, but SeekHead told us where Tracks is,
    # try to jump directly to that absolute offset.
    if tracks_seek_offset is not None:
        abs_pos = segment_data_start + tracks_seek_offset
        if abs_pos >= len(prefix_bytes):
            # We know where Tracks should be, but we haven't fetched far enough yet.
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "mkv tracks seek_offset_beyond_bytes abs=%d bytes=%d",
                    abs_pos,
                    len(prefix_bytes),
                )
            raise EbmlParseError("Tracks offset beyond available bytes")

        # Read element at that offset (absolute in file buffer).
        try:
            eid, p0, p1 = _read_element_at(prefix_bytes, abs_pos, end=len(prefix_bytes))
        except EbmlParseError as e:
            # Not enough bytes to parse full Tracks element yet.
            raise EbmlParseError(f"Tracks at seek offset not yet parseable: {e}") from e

        if eid != int.from_bytes(ID_TRACKS, "big"):
            # SeekHead pointed somewhere unexpected (corrupt or uncommon layout).
            # Fall back to "not found yet" behavior.
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("mkv tracks seekhead_mismatch eid=%#x abs=%d", eid, abs_pos)
            raise EbmlParseError("SeekHead Tracks pointer did not land on Tracks")

        _parse_tracks(prefix_bytes[p0:p1], video_tracks, audio_tracks)
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "mkv tracks seek_jump a=%d v=%d abs=%d bytes=%d",
                len(audio_tracks),
                len(video_tracks),
                abs_pos,
                len(prefix_bytes),
            )
        return MkvMetadata(
            video_tracks=video_tracks,
            audio_tracks=audio_tracks,
            duration_s=duration_s,
        )

    # No Tracks found yet in this buffer (and no usable SeekHead pointer).
    # Return empty metadata (caller can keep fetching more and retry).
    return MkvMetadata(video_tracks=video_tracks, audio_tracks=audio_tracks, duration_s=duration_s)


def _parse_tracks(
    tracks_bytes: bytes, video_tracks: list[VideoTrack], audio_tracks: list[AudioTrack]
) -> None:
    # tracks_bytes includes full Tracks element PAYLOAD (because caller sliced payload already).
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


def _parse_info_duration(info_bytes: bytes) -> float | None:
    timecode_scale = 1_000_000
    duration_raw: float | None = None
    for element_id, p0, p1 in _iter_elements(info_bytes, 0, len(info_bytes)):
        payload = info_bytes[p0:p1]
        if element_id == int.from_bytes(ID_TIMECODE_SCALE, "big"):
            timecode_scale = _decode_uint(payload) or timecode_scale
        elif element_id == int.from_bytes(ID_DURATION, "big"):
            duration_raw = _decode_float(payload)
    if duration_raw is None:
        return None
    return (duration_raw * timecode_scale) / 1_000_000_000
