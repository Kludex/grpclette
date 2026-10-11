from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import anyio
import grpc
import pytest
from google.protobuf.empty_pb2 import Empty


@pytest.mark.anyio
async def test_unary_mapping_and_compression(client: tuple[Any, Any]) -> None:
    messages, stub = client
    call = stub.Echo(messages.EchoInput(text="hello"), compression=grpc.Compression.Gzip)
    assert (await call).text == "hello"
    assert await call.code() == grpc.StatusCode.OK


@pytest.mark.anyio
async def test_server_stream(client: tuple[Any, Any]) -> None:
    messages, stub = client
    assert [item.text async for item in stub.Watch(messages.EchoInput(text="hello"))] == ["hello0", "hello1"]


@pytest.mark.anyio
async def test_client_stream(client: tuple[Any, Any]) -> None:
    messages, stub = client

    async def requests() -> AsyncIterator[Any]:
        yield messages.EchoInput(text="one")
        yield messages.EchoInput(text="two")

    assert (await stub.Upload(requests())).count == 2


@pytest.mark.anyio
async def test_interleaved_bidirectional_stream(client: tuple[Any, Any]) -> None:
    messages, stub = client
    first_response = anyio.Event()

    async def requests() -> AsyncIterator[Any]:
        yield messages.EchoInput(text="one")
        await first_response.wait()
        yield messages.EchoInput(text="two")

    call = stub.Talk(requests(), timeout=3)
    assert (await call.read()).text == "one"
    first_response.set()
    assert (await call.read()).text == "two"
    assert await call.code() == grpc.StatusCode.OK


@pytest.mark.anyio
async def test_bytes_large_messages_int64_optional_and_dataclasses(client: tuple[Any, Any]) -> None:
    messages, stub = client
    payload = bytes(range(256)) * 2048
    request = messages.Blob(data=payload, number=2**63 - 1, labels=["one", "two"])
    reply = await stub.Blob(request, compression=grpc.Compression.Gzip)
    assert reply.data == payload
    assert reply.number == 2**63 - 1
    assert list(reply.labels) == ["one", "two"]
    assert not reply.HasField("optional")
    request.optional = 0
    assert (await stub.Blob(request)).HasField("optional")


@pytest.mark.anyio
async def test_nested_maps_and_repeated_messages(client: tuple[Any, Any]) -> None:
    messages, stub = client
    nested = messages.Blob(data=b"\x00\xff", number=123)
    request = messages.Envelope(child=nested, children=[nested], lookup={"one": nested})
    reply = await stub.Envelope(request)
    assert reply == request


@pytest.mark.anyio
async def test_python_models_and_mapping_requests(client: tuple[Any, Any]) -> None:
    messages, stub = client
    assert (await stub.PydanticEcho(messages.EchoInput(text="hello"))).text == "hello"
    assert (await stub.MappingRequest(messages.EchoResult(text="hello"))).text == "hello"
    assert (await stub.ProtobufResult(messages.EchoInput(text="hello"))).text == "hello"
    assert (await stub.AliasEcho(messages.AliasedEcho(text="hello"))).text == "hello"


@pytest.mark.anyio
async def test_protobuf_input_and_output(client: tuple[Any, Any]) -> None:
    _, stub = client
    from google.protobuf.timestamp_pb2 import Timestamp

    request = Timestamp(seconds=123, nanos=456)
    assert await stub.Timestamp(request) == request
    assert await stub.ProtobufMapping(request) == request


@pytest.mark.anyio
async def test_nested_protobuf_messages_preserve_python_types(client: tuple[Any, Any]) -> None:
    messages, stub = client
    from google.protobuf.timestamp_pb2 import Timestamp

    stamp = Timestamp(seconds=123, nanos=456)
    envelope = messages.ProtobufEnvelope(stamp=stamp, stamps=[stamp], lookup={"first": stamp})
    assert await stub.ProtobufEnvelope(envelope) == envelope
    envelope.optional.CopyFrom(stamp)
    assert await stub.ProtobufEnvelope(envelope) == envelope
    assert (await stub.ProtobufRecord(messages.ProtobufRecord(stamp=stamp))).stamp == stamp
    assert (await stub.ProtobufDictionary(messages.ProtobufMapping(stamp=stamp))).stamp == stamp


@pytest.mark.anyio
async def test_no_request_and_no_response(client: tuple[Any, Any]) -> None:
    _, stub = client
    assert await stub.Empty(Empty()) == Empty()
