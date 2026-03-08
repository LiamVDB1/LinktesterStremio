from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from time import monotonic
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass
class _CacheEntry(Generic[T]):
    value: T
    expires_at: float


class TTLCache(Generic[T]):
    def __init__(self, *, ttl_s: float, max_entries: int) -> None:
        self.ttl_s = max(0.0, ttl_s)
        self.max_entries = max(1, max_entries)
        self._data: OrderedDict[str, _CacheEntry[T]] = OrderedDict()

    def get(self, key: str) -> T | None:
        now = monotonic()
        entry = self._data.get(key)
        if entry is None:
            return None
        if entry.expires_at <= now:
            self._data.pop(key, None)
            return None
        self._data.move_to_end(key)
        return entry.value

    def set(self, key: str, value: T) -> None:
        if self.ttl_s <= 0:
            return
        now = monotonic()
        self._purge(now)
        self._data[key] = _CacheEntry(value=value, expires_at=now + self.ttl_s)
        self._data.move_to_end(key)
        while len(self._data) > self.max_entries:
            self._data.popitem(last=False)

    def _purge(self, now: float) -> None:
        expired = [key for key, entry in self._data.items() if entry.expires_at <= now]
        for key in expired:
            self._data.pop(key, None)
