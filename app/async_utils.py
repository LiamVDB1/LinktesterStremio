from __future__ import annotations

from collections.abc import Awaitable
from typing import cast

import anyio


async def gather[T](*aws: Awaitable[T]) -> list[T]:
    results: list[object] = [None] * len(aws)

    async def _run(i: int, aw: Awaitable[T]) -> None:
        results[i] = await aw

    async with anyio.create_task_group() as tg:
        for i, aw in enumerate(aws):
            tg.start_soon(_run, i, aw)

    return cast(list[T], results)
