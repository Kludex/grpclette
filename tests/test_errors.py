from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import anyio
import grpc
import pytest


@pytest.mark.anyio
async def test_request_validation(client: tuple[Any, Any]) -> None:
    messages, stub = client
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await stub.Echo(messages.EchoInput(text=""))
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert error.value.details() == "Invalid request."


@pytest.mark.anyio
async def test_streaming_request_validation(client: tuple[Any, Any]) -> None:
    messages, stub = client

    async def requests() -> AsyncIterator[Any]:
        yield messages.EchoInput(text="")

    with pytest.raises(grpc.aio.AioRpcError) as error:
        await stub.Upload(requests())
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT


@pytest.mark.anyio
@pytest.mark.parametrize("method", ["Broken", "InvalidResponse", "NoneResponse"])
async def test_response_failures_are_sanitized(client: tuple[Any, Any], method: str) -> None:
    messages, stub = client
    call = {"Broken": stub.Broken, "InvalidResponse": stub.InvalidResponse, "NoneResponse": stub.NoneResponse}[method]
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await call(messages.EchoInput(text="hello"))
    assert error.value.code() == grpc.StatusCode.INTERNAL
    assert "secret" not in (error.value.details() or "")


@pytest.mark.anyio
@pytest.mark.parametrize("invalid_message", [False, True])
async def test_wrong_protobuf_responses(client: tuple[Any, Any], invalid_message: bool) -> None:
    _, stub = client
    from google.protobuf.timestamp_pb2 import Timestamp

    call = stub.InvalidProtobuf if invalid_message else stub.WrongProtobuf
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await call(Timestamp(seconds=123))
    assert error.value.code() == grpc.StatusCode.INTERNAL


@pytest.mark.anyio
async def test_stream_failure_preserves_status(client: tuple[Any, Any]) -> None:
    messages, stub = client
    call = stub.FailingStream(messages.EchoInput(text="hello"))
    assert (await call.read()).text == "hello"
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await call.read()
    assert error.value.code() == grpc.StatusCode.NOT_FOUND


@pytest.mark.anyio
async def test_deadline_stops_handler(client: tuple[Any, Any], cancelled: anyio.Event) -> None:
    messages, stub = client
    with pytest.raises(grpc.aio.AioRpcError) as error:
        await stub.Wait(messages.EchoInput(text="hello"), timeout=0.1)
    assert error.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    with anyio.fail_after(1):
        await cancelled.wait()


@pytest.mark.anyio
async def test_disconnect_closes_stream(client: tuple[Any, Any], cancelled: anyio.Event) -> None:
    messages, stub = client
    call = stub.WaitingStream(messages.EchoInput(text="hello"))
    assert (await call.read()).text == "hello"
    assert call.cancel()
    with anyio.fail_after(1):
        await cancelled.wait()
