from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class StreamsResponse(BaseModel):
    streams: list[dict[str, Any]] = Field(default_factory=list)


class HealthResponse(BaseModel):
    status: str
    uptime_s: float
