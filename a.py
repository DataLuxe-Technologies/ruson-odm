import asyncio
import inspect
from typing import Any, Awaitable, Callable


def batata(a: Callable[[], Awaitable[Any] | Any]) -> None:
    t = a()
    if isinstance(t, Awaitable):
        return asyncio.run(t)
    return t


async def first() -> int:
    return 10


def second() -> int:
    return 20


print(batata(first))
print(batata(second))
