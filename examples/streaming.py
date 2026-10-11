from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated

from grpclette import Field, gRPClette

app = gRPClette(service="Streaming", package="example")


@dataclass(frozen=True)
class Greeting:
    name: Annotated[str, Field(number=1)]


@dataclass(frozen=True)
class Reply:
    message: Annotated[str, Field(number=1)]


@dataclass(frozen=True)
class Summary:
    count: Annotated[int, Field(number=1)]


@app.rpc
async def say_hello(request: Greeting) -> Reply:
    return Reply(message=f"Hello, {request.name}!")


@app.rpc
async def watch_greetings(request: Greeting) -> AsyncIterator[Reply]:
    for index in range(3):
        yield Reply(message=f"Hello, {request.name}! ({index + 1})")


@app.rpc
async def count_greetings(requests: AsyncIterator[Greeting]) -> Summary:
    count = 0
    async for _ in requests:
        count += 1
    return Summary(count=count)


@app.rpc
async def chat(requests: AsyncIterator[Greeting]) -> AsyncIterator[Reply]:
    async for request in requests:
        yield Reply(message=f"Hello, {request.name}!")
