from __future__ import annotations

from typing import Any

import grpc
import pytest


@pytest.mark.anyio
async def test_context_lifespan_state_cached_dependencies_and_cleanup(
    client: tuple[Any, Any], events: list[str]
) -> None:
    messages, stub = client
    call = stub.Context(messages.EchoInput(text="world"), metadata=(("authorization", "Bearer test"),))
    assert (await call).text == "hello:world"
    assert ("x-header", "hello") in tuple(await call.initial_metadata())
    assert ("x-trailer", "bye") in tuple(await call.trailing_metadata())
    assert events == ["acquire", "release"]


@pytest.mark.anyio
async def test_dependency_failure_is_an_rpc_status(client: tuple[Any, Any], events: list[str]) -> None:
    messages, stub = client
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await stub.Context(messages.EchoInput(text="hello"))
    assert error.value.code() == grpc.StatusCode.UNAUTHENTICATED
    assert events == []
