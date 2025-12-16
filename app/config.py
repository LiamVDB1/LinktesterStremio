from __future__ import annotations

from pydantic import AnyHttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    upstream_base_url: AnyHttpUrl = Field(alias="UPSTREAM_BASE_URL")

    top_k_phase1: int = Field(default=10, alias="TOP_K_PHASE1", ge=1, le=50)
    top_m_phase2: int = Field(default=5, alias="TOP_M_PHASE2", ge=0, le=10)
    top_p_weird: int = Field(default=2, alias="TOP_P_WEIRD", ge=0, le=5)

    t_probe_total_ms: int = Field(default=1200, alias="T_PROBE_TOTAL_MS", ge=50, le=10_000)
    t_ttfb_ms: int = Field(default=250, alias="T_TTFB_MS", ge=50, le=10_000)

    max_concurrency: int = Field(default=12, alias="MAX_CONCURRENCY", ge=1, le=200)

    preferred_audio_langs: str = Field(default="nl,en", alias="PREFERRED_AUDIO_LANGS")
    preferred_sub_langs: str = Field(default="nl,en", alias="PREFERRED_SUB_LANGS")
    require_seekable: bool = Field(default=True, alias="REQUIRE_SEEKABLE")
    prefer_hdr: bool = Field(default=False, alias="PREFER_HDR")
    prefer_codec: str = Field(default="hevc,avc", alias="PREFER_CODEC")
    min_video_height: int | None = Field(default=None, alias="MIN_VIDEO_HEIGHT", ge=1)

    max_redirects: int = Field(default=2, alias="MAX_REDIRECTS", ge=0, le=10)
    user_agent: str = Field(default="StremioLinkRanker/1.0", alias="USER_AGENT")

    upstream_timeout_ms: int = Field(default=5000, alias="UPSTREAM_TIMEOUT_MS", ge=100, le=120_000)

    mkv_meta_max_bytes: int = Field(default=2_097_152, alias="MKV_META_MAX_BYTES", ge=65_536)
    mkv_meta_chunk_bytes: int = Field(default=262_144, alias="MKV_META_CHUNK_BYTES", ge=16_384)
    weird_probe_bytes: int = Field(default=512, alias="WEIRD_PROBE_BYTES", ge=64, le=8192)

    @property
    def preferred_audio_lang_list(self) -> list[str]:
        return [x.strip().lower() for x in self.preferred_audio_langs.split(",") if x.strip()]

    @property
    def preferred_sub_lang_list(self) -> list[str]:
        return [x.strip().lower() for x in self.preferred_sub_langs.split(",") if x.strip()]

    @property
    def prefer_codec_list(self) -> list[str]:
        return [x.strip().lower() for x in self.prefer_codec.split(",") if x.strip()]
