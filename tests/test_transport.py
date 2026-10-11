from __future__ import annotations

import asyncio
import gzip
import struct
from collections.abc import AsyncIterator
from typing import cast

import anyio
import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.method import IdempotencyLevel, MethodInfo
from connectrpc.request import RequestContext
from connectrpc.server import Endpoint
from google.protobuf.message import Message
from google.protobuf.wrappers_pb2 import StringValue
from starlette.types import Message as ASGIMessage, Scope

from grpclette.transport import RPCTransport

pytestmark = pytest.mark.anyio


def method() -> MethodInfo[Message, Message]:
    return MethodInfo(
        name="Echo",
        service_name="test.Service",
        input=StringValue,
        output=StringValue,
        idempotency_level=IdempotencyLevel.UNKNOWN,
    )


def scope(headers: list[tuple[bytes, bytes]] | None = None) -> Scope:
    return {
        "type": "http",
        "http_version": "2",
        "method": "POST",
        "scheme": "http",
        "path": "/test.Service/Echo",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"content-type", b"application/grpc+proto"), *(headers or [])],
        "extensions": {"http.response.trailers": {}},
    }


def frame(message: Message, *, compressed: bool = False) -> bytes:
    data = message.SerializeToString()
    if compressed:
        data = gzip.compress(data)
    return struct.pack(">BI", int(compressed), len(data)) + data


async def echo(request: Message, context: RequestContext[Message, Message]) -> Message:
    context.response_headers["x-header"] = "header"
    context.response_trailers["x-trailer"] = "trailer"
    return request


async def invoke(
    transport: RPCTransport,
    request_scope: Scope,
    chunks: list[bytes] | None = None,
) -> list[ASGIMessage]:
    chunks = list(chunks if chunks is not None else [frame(StringValue(value="hello"))])
    sent: list[ASGIMessage] = []

    async def receive() -> ASGIMessage:
        if chunks:
            return {"type": "http.request", "body": chunks.pop(0), "more_body": bool(chunks)}
        await anyio.sleep_forever()
        raise AssertionError("unreachable")  # pragma: no cover - sleep_forever never returns

    async def send(message: ASGIMessage) -> None:
        sent.append(message)
        await anyio.sleep(0)

    with anyio.fail_after(2):
        await transport(request_scope, receive, send)
    return sent


def status(sent: list[ASGIMessage]) -> bytes:
    trailers = [message for message in sent if message["type"] == "http.response.trailers"]
    assert len(trailers) == 1
    assert trailers[0]["more_trailers"] is False
    return cast(bytes, dict(trailers[0]["headers"])[b"grpc-status"])


@pytest.mark.parametrize("content_type", [b"application/grpc", b"application/grpc+proto"])
async def test_unary_chunking_metadata_and_mount(content_type: bytes) -> None:
    transport = RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), echo)}, 1024)
    request_scope = scope()
    request_scope["headers"] = [(b"content-type", content_type)]
    request_scope["root_path"] = "/rpc"
    request_scope["path"] = "/rpc/test.Service/Echo"
    data = frame(StringValue(value="hello"))
    sent = await invoke(transport, request_scope, [data[:2], data[2:6], data[6:], b""])
    assert sent[0]["status"] == 200
    assert sent[0]["trailers"] is True
    assert dict(sent[0]["headers"])[b"x-header"] == b"header"
    assert b"".join(event.get("body", b"") for event in sent) == data
    assert dict(sent[-1]["headers"])[b"x-trailer"] == b"trailer"
    assert status(sent) == b"0"


@pytest.mark.parametrize(
    "change,expected",
    [
        ({"headers": []}, 415),
        ({"headers": [(b"content-type", b"application/grpc-web+proto")]}, 415),
        ({"headers": [(b"content-type", b"application/json")]}, 415),
        ({"headers": [(b"content-type", b"application/grpc+json")]}, 415),
        ({"headers": [(b"content-type", b"application/grpc; charset=utf-8")]}, 415),
        ({"headers": [(b"content-type", b"application/grpc")] * 2}, 415),
        ({"method": "GET"}, 405),
        ({"http_version": "1.1"}, 505),
        ({"extensions": {}}, 505),
    ],
)
async def test_reject_unsupported_request(change: Scope, expected: int) -> None:
    sent = await invoke(RPCTransport({}, 1024), {**scope(), **change})
    assert [event["type"] for event in sent] == ["http.response.start", "http.response.body"]
    assert sent[0]["status"] == expected
    assert dict(sent[0]["headers"]) == ({b"allow": b"POST"} if expected == 405 else {})
    assert sent[1]["body"] == b""


async def test_unknown_method_is_grpc_unimplemented_without_reading_body() -> None:
    sent = await invoke(RPCTransport({}, 1024), scope(), [])
    assert sent[0]["status"] == 200
    assert sent[0]["trailers"] is True
    assert status(sent) == b"12"


@pytest.mark.parametrize(
    "value",
    [
        b"",
        b"1",
        b"1s",
        b"-1S",
        b"+1S",
        b"1.5S",
        b" 1S",
        b"1 S",
        b"1S ",
        b"123456789n",
        b"000000001n",
        "١S".encode(),
        b"\xffS",
        b"1S\n",
    ],
)
async def test_invalid_timeout_is_grpc_invalid_argument(value: bytes) -> None:
    sent = await invoke(RPCTransport({}, 1024), scope([(b"grpc-timeout", value)]), [])
    assert status(sent) == b"3"


async def test_duplicate_timeout_is_invalid() -> None:
    sent = await invoke(RPCTransport({}, 1024), scope([(b"grpc-timeout", b"1S")] * 2), [])
    assert status(sent) == b"3"


@pytest.mark.parametrize(
    "value,milliseconds",
    [
        (b"1H", 3600000),
        (b"1M", 60000),
        (b"1S", 1000),
        (b"1000m", 1000),
        (b"99999999u", 99999),
        (b"99999999n", 99),
        (b"00000001S", 1000),
    ],
)
async def test_timeout_context(value: bytes, milliseconds: int) -> None:
    async def handler(request: Message, context: RequestContext[Message, Message]) -> Message:
        remaining = context.timeout_ms
        assert remaining is not None and 0 < remaining <= milliseconds
        return request

    sent = await invoke(
        RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), handler)}, 1024),
        scope([(b"grpc-timeout", value)]),
    )
    assert status(sent) == b"0"


async def test_gzip_request_and_response() -> None:
    transport = RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), echo)}, 1024)
    message = StringValue(value="hello" * 20)
    sent = await invoke(
        transport,
        scope([(b"grpc-encoding", b"gzip"), (b"grpc-accept-encoding", b"gzip")]),
        [frame(message, compressed=True)],
    )
    data = b"".join(event.get("body", b"") for event in sent)
    assert data[0] == 1
    assert int.from_bytes(data[1:5], "big") == len(data) - 5
    assert StringValue.FromString(gzip.decompress(data[5:])) == message
    assert dict(sent[0]["headers"])[b"grpc-encoding"] == b"gzip"
    assert status(sent) == b"0"


@pytest.mark.parametrize("compressed,limit", [(False, 64), (True, 64), (True, 8)])
async def test_wire_and_decompressed_limits(compressed: bool, limit: int) -> None:
    data = frame(StringValue(value="x" * 512), compressed=compressed)
    headers = [(b"grpc-encoding", b"gzip")] if compressed else []
    sent = await invoke(
        RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), echo)}, limit), scope(headers), [data]
    )
    assert status(sent) == b"8"


@pytest.mark.parametrize(
    "data",
    [
        *(b"\x00" * size for size in range(1, 5)),
        *(frame(StringValue(value="hello"))[:size] for size in range(5, 13)),
        b"\x00" * 5,
        *(b"\x00" * 5 + b"\x00" * size for size in range(1, 5)),
        b"\x00" * 5 + frame(StringValue(value="hello"))[:-1],
    ],
)
@pytest.mark.parametrize("chunking", ["whole", "bytes", "empty_eof"])
async def test_unary_framing_validation_precedes_dispatch(data: bytes, chunking: str) -> None:
    dispatched = False

    async def handler(request: Message, context: RequestContext[Message, Message]) -> Message:
        nonlocal dispatched
        dispatched = True
        return request

    chunks = [data] if chunking == "whole" else [data, b""]
    if chunking == "bytes":
        chunks = [data[index : index + 1] for index in range(len(data))]
    sent = await invoke(RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), handler)}, 1024), scope(), chunks)
    valid = data in (b"\x00" * 5, frame(StringValue(value="hello")))
    assert status(sent) == (b"0" if valid else b"3")
    assert dispatched == valid


@pytest.mark.parametrize("flag", [0, *range(2, 256)])
@pytest.mark.parametrize("split", [False, True])
async def test_reserved_flags_never_dispatch_unary(flag: int, split: bool) -> None:
    dispatched = False

    async def handler(request: Message, context: RequestContext[Message, Message]) -> Message:
        nonlocal dispatched
        dispatched = True
        return request

    data = bytes([flag]) + b"\x00" * 4
    chunks = [data[:1], data[1:]] if split else [data]
    sent = await invoke(RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), handler)}, 1024), scope(), chunks)
    assert status(sent) == (b"0" if flag == 0 else b"3")
    assert dispatched == (flag == 0)


async def test_unexpected_receive_event_is_engine_error() -> None:
    async def receive() -> ASGIMessage:
        return {"type": "unexpected.event"}

    sent: list[ASGIMessage] = []

    async def send(event: ASGIMessage) -> None:
        sent.append(event)

    with anyio.fail_after(2):
        await RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), echo)}, 1024)(scope(), receive, send)
    assert status(sent) == b"2"


@pytest.mark.parametrize("kind", ["client", "bidi"])
@pytest.mark.parametrize("tail", [b"\x00", b"\x00\x00\x00\x00\x02\x00", b"\x02\x00\x00\x00\x00"])
@pytest.mark.parametrize("split", [False, True])
async def test_malformed_stream_is_not_successful_eof(kind: str, tail: bytes, split: bool) -> None:
    consumed: list[Message] = []
    cleaned = anyio.Event()

    async def client(requests: AsyncIterator[Message], context: RequestContext[Message, Message]) -> Message:
        try:
            async for request in requests:
                consumed.append(request)
        finally:
            await anyio.sleep(0)
            cleaned.set()
        return StringValue()  # pragma: no cover - malformed framing raises before EOF

    async def bidi(
        requests: AsyncIterator[Message], context: RequestContext[Message, Message]
    ) -> AsyncIterator[Message]:
        yield await client(requests, context)  # pragma: no branch - client always raises

    endpoint = Endpoint.client_stream(method(), client) if kind == "client" else Endpoint.bidi_stream(method(), bidi)
    data = frame(StringValue(value="first"))
    chunks = [data, tail] if split else [data + tail]
    sent = await invoke(RPCTransport({"/test.Service/Echo": endpoint}, 1024), scope(), chunks)
    assert status(sent) == b"3"
    assert consumed == ([StringValue(value="first")] if split else [])
    assert cleaned.is_set()


async def test_unsupported_compression() -> None:
    sent = await invoke(
        RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), echo)}, 1024),
        scope([(b"grpc-encoding", b"unknown")]),
    )
    assert status(sent) == b"12"


async def test_application_error_is_serialized() -> None:
    async def handler(request: Message, context: RequestContext[Message, Message]) -> Message:
        raise ConnectError(Code.PERMISSION_DENIED, "Not allowed")

    sent = await invoke(RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), handler)}, 1024), scope())
    assert status(sent) == b"7"
    assert dict(sent[-1]["headers"])[b"grpc-message"] == b"Not%20allowed"


@pytest.mark.parametrize("kind", ["unary", "client", "server", "bidi"])
@pytest.mark.parametrize("disconnect", [False, True])
async def test_cancel_sleep_and_cleanup(kind: str, disconnect: bool) -> None:
    entered = anyio.Event()
    cleaned = anyio.Event()
    sent: list[ASGIMessage] = []

    async def unary(request: Message, context: RequestContext[Message, Message]) -> Message:
        entered.set()
        try:
            await anyio.sleep_forever()
        finally:
            await anyio.sleep(0)
            cleaned.set()
        raise AssertionError("unreachable")  # pragma: no cover - sleep_forever never returns

    async def client(requests: AsyncIterator[Message], context: RequestContext[Message, Message]) -> Message:
        return await unary(await anext(requests), context)

    async def server(request: Message, context: RequestContext[Message, Message]) -> AsyncIterator[Message]:
        try:
            yield request
            await unary(request, context)
        finally:
            await anyio.sleep(0)
            cleaned.set()

    async def bidi(
        requests: AsyncIterator[Message], context: RequestContext[Message, Message]
    ) -> AsyncIterator[Message]:
        async for response in server(await anext(requests), context):
            yield response

    endpoints = {
        "unary": Endpoint.unary(method(), unary),
        "client": Endpoint.client_stream(method(), client),
        "server": Endpoint.server_stream(method(), server),
        "bidi": Endpoint.bidi_stream(method(), bidi),
    }
    transport = RPCTransport({"/test.Service/Echo": endpoints[kind]}, 1024)
    delivered = False

    async def receive() -> ASGIMessage:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": frame(StringValue(value="hello")), "more_body": False}
        await entered.wait()
        if disconnect:
            return {"type": "http.disconnect"}
        await anyio.sleep_forever()
        raise AssertionError("unreachable")  # pragma: no cover - sleep_forever never returns

    async def send(event: ASGIMessage) -> None:
        sent.append(event)

    with anyio.fail_after(2):
        await transport(scope([] if disconnect else [(b"grpc-timeout", b"30m")]), receive, send)
    assert entered.is_set()
    assert cleaned.is_set()
    if not disconnect:
        assert status(sent) == b"4"
        assert sum(event["type"] == "http.response.start" for event in sent) == 1
        assert sum(event["type"] == "http.response.body" and not event.get("more_body", False) for event in sent) == 1


@pytest.mark.parametrize("kind", ["client", "bidi", "unary", "server"])
@pytest.mark.parametrize("disconnect", [False, True])
async def test_cancel_while_waiting_for_input(kind: str, disconnect: bool) -> None:
    cleaned = anyio.Event()

    async def unary(request: Message, context: RequestContext[Message, Message]) -> Message:
        pytest.fail("Incomplete unary request")  # pragma: no cover - dispatch requires a complete request

    async def client(requests: AsyncIterator[Message], context: RequestContext[Message, Message]) -> Message:
        try:
            await anext(requests)
        finally:
            cleaned.set()
        pytest.fail("Request stream not cancelled")  # pragma: no cover - no request was provided

    async def bidi(
        requests: AsyncIterator[Message], context: RequestContext[Message, Message]
    ) -> AsyncIterator[Message]:
        yield await client(requests, context)

    async def server(request: Message, context: RequestContext[Message, Message]) -> AsyncIterator[Message]:
        yield await unary(request, context)  # pragma: no cover - dispatch requires a complete request

    endpoints = {
        "unary": Endpoint.unary(method(), unary),
        "client": Endpoint.client_stream(method(), client),
        "server": Endpoint.server_stream(method(), server),
        "bidi": Endpoint.bidi_stream(method(), bidi),
    }
    transport = RPCTransport({"/test.Service/Echo": endpoints[kind]}, 1024)
    sent: list[ASGIMessage] = []

    async def receive() -> ASGIMessage:
        await anyio.sleep(0.01)
        if disconnect:
            return {"type": "http.disconnect"}
        await anyio.sleep_forever()
        raise AssertionError("unreachable")  # pragma: no cover - sleep_forever never returns

    async def send(event: ASGIMessage) -> None:
        sent.append(event)

    with anyio.fail_after(2):
        await transport(scope([] if disconnect else [(b"grpc-timeout", b"30m")]), receive, send)
    assert cleaned.is_set() == (kind in ("client", "bidi"))
    if not disconnect:
        assert status(sent) == b"4"


async def test_interleaved_stream_preserves_order_and_backpressure() -> None:
    consumed = received = 0
    cleaned = anyio.Event()
    release = anyio.Event()
    first_sent = anyio.Event()
    in_receive = False
    sent: list[ASGIMessage] = []

    async def handler(
        requests: AsyncIterator[Message], context: RequestContext[Message, Message]
    ) -> AsyncIterator[Message]:
        nonlocal consumed
        try:
            async for request in requests:
                consumed += 1
                yield request
                if consumed == 1:
                    await release.wait()
        finally:
            cleaned.set()

    async def receive() -> ASGIMessage:
        nonlocal received, in_receive
        assert not in_receive
        in_receive = True
        try:
            await anyio.sleep(0)
            if received == 20:
                await anyio.sleep_forever()
            received += 1
            return {"type": "http.request", "body": frame(StringValue(value=str(received))), "more_body": received < 20}
        finally:
            in_receive = False

    async def send(event: ASGIMessage) -> None:
        sent.append(event)
        if event["type"] == "http.response.body" and event.get("body"):
            first_sent.set()

    transport = RPCTransport({"/test.Service/Echo": Endpoint.bidi_stream(method(), handler)}, 1024)
    with anyio.fail_after(2):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(transport, scope(), receive, send)
            await first_sent.wait()
            await anyio.sleep(0.01)
            assert consumed == 1
            assert received == 2
            release.set()
    bodies = [event["body"] for event in sent if event["type"] == "http.response.body" and event["body"]]
    assert bodies == [frame(StringValue(value=str(number))) for number in range(1, 21)]
    assert status(sent) == b"0"
    assert cleaned.is_set()


@pytest.mark.parametrize("deadline", [False, True])
async def test_unread_stream_input_delays_disconnect_but_not_deadline(deadline: bool) -> None:
    entered = anyio.Event()
    second_received = anyio.Event()
    release = anyio.Event()
    cleaned = anyio.Event()
    received = 0
    sent: list[ASGIMessage] = []

    async def handler(requests: AsyncIterator[Message], context: RequestContext[Message, Message]) -> Message:
        first = await anext(requests)
        entered.set()
        try:
            await release.wait()
        finally:
            await anyio.sleep(0)
            cleaned.set()
        return first

    async def receive() -> ASGIMessage:
        nonlocal received
        received += 1
        if received == 1:
            return {"type": "http.request", "body": frame(StringValue(value="first")), "more_body": True}
        await entered.wait()
        if received == 2:
            second_received.set()
            return {"type": "http.request", "body": frame(StringValue(value="unread")), "more_body": True}
        return {"type": "http.disconnect"}  # pragma: no cover - unread input blocks the receive pump

    async def send(event: ASGIMessage) -> None:
        sent.append(event)

    transport = RPCTransport({"/test.Service/Echo": Endpoint.client_stream(method(), handler)}, 1024)
    with anyio.fail_after(2):
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(transport, scope([(b"grpc-timeout", b"100m")] if deadline else []), receive, send)
            await second_received.wait()
            await anyio.sleep(0.01)
            assert received == 2
            assert not cleaned.is_set()
            if not deadline:
                release.set()
    assert cleaned.is_set()
    assert status(sent) == (b"4" if deadline else b"0")
    assert received == 2


@pytest.mark.parametrize("delay", [0, 0.01])
async def test_disconnect_during_accepted_final_send(delay: float) -> None:
    returned = False
    final_accepted = anyio.Event()
    disconnected = anyio.Event()
    sent: list[ASGIMessage] = []
    delivered = False

    async def receive() -> ASGIMessage:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": frame(StringValue(value="hello")), "more_body": False}
        await final_accepted.wait()
        disconnected.set()
        return {"type": "http.disconnect"}

    async def send(event: ASGIMessage) -> None:
        nonlocal returned
        sent.append(event)
        if event["type"] == "http.response.trailers":
            final_accepted.set()
            await disconnected.wait()
            if delay:
                await anyio.sleep(delay)
            returned = True

    transport = RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), echo)}, 1024)
    with anyio.fail_after(2):
        await transport(scope(), receive, send)
    assert disconnected.is_set()
    assert status(sent) == b"0"
    assert returned == (delay == 0)


@pytest.mark.parametrize("disconnect", [False, True])
async def test_deadline_during_blocked_final_send(disconnect: bool) -> None:
    final_started = anyio.Event()
    delivered = False
    blocked = False
    sent: list[ASGIMessage] = []

    async def receive() -> ASGIMessage:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": frame(StringValue(value="hello")), "more_body": False}
        await final_started.wait()
        if disconnect:
            return {"type": "http.disconnect"}
        await anyio.sleep_forever()
        raise AssertionError("unreachable")  # pragma: no cover - sleep_forever never returns

    async def send(event: ASGIMessage) -> None:
        nonlocal blocked
        if event["type"] == "http.response.trailers" and not blocked:
            blocked = True
            final_started.set()
            await anyio.sleep_forever()
        sent.append(event)

    transport = RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), echo)}, 1024)
    with anyio.fail_after(2):
        await transport(scope([(b"grpc-timeout", b"30m")]), receive, send)
    assert blocked
    if disconnect:
        assert not any(event["type"] == "http.response.trailers" for event in sent)
    else:
        assert status(sent) == b"4"
    assert sum(event["type"] == "http.response.start" for event in sent) == 1
    assert sum(event["type"] == "http.response.body" and not event.get("more_body", False) for event in sent) == 1


async def test_disconnect_cancels_blocked_final_send_without_deadline() -> None:
    final_started = anyio.Event()
    send_cancelled = anyio.Event()
    delivered = False
    sent: list[ASGIMessage] = []

    async def receive() -> ASGIMessage:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": frame(StringValue(value="hello")), "more_body": False}
        await final_started.wait()
        return {"type": "http.disconnect"}

    async def send(event: ASGIMessage) -> None:
        if event["type"] == "http.response.trailers":
            final_started.set()
            try:
                await anyio.sleep_forever()
            finally:
                await anyio.sleep(0)
                send_cancelled.set()
        sent.append(event)

    transport = RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), echo)}, 1024)
    with anyio.fail_after(1):
        await transport(scope(), receive, send)
    assert send_cancelled.is_set()
    assert not any(event["type"] == "http.response.trailers" for event in sent)


@pytest.mark.parametrize("value", [b"0n", b"0H", b"1n", b"1u"])
async def test_already_expired_deadline(value: bytes) -> None:
    async def handler(request: Message, context: RequestContext[Message, Message]) -> Message:
        pytest.fail("Expired RPC dispatched")  # pragma: no cover - expiry prevents dispatch

    sent = await invoke(
        RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), handler)}, 1024),
        scope([(b"grpc-timeout", value)]),
        [],
    )
    assert status(sent) == b"4"


@pytest.mark.parametrize("boundary", ["receive", "send"])
async def test_io_failure_cancels_resources(boundary: str) -> None:
    entered = anyio.Event()
    cleaned = anyio.Event()
    delivered = False

    async def handler(request: Message, context: RequestContext[Message, Message]) -> AsyncIterator[Message]:
        try:
            entered.set()
            yield request
            await anyio.sleep_forever()
        finally:
            await anyio.sleep(0)
            cleaned.set()

    async def receive() -> ASGIMessage:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": frame(StringValue(value="hello")), "more_body": False}
        await entered.wait()
        if boundary == "receive":
            raise OSError("Peer closed")
        await anyio.sleep_forever()
        raise AssertionError("unreachable")  # pragma: no cover - sleep_forever never returns

    async def send(event: ASGIMessage) -> None:
        if boundary == "send":
            raise OSError("Peer closed")

    with anyio.fail_after(2):
        await RPCTransport({"/test.Service/Echo": Endpoint.server_stream(method(), handler)}, 1024)(
            scope(), receive, send
        )
    assert cleaned.is_set()


async def test_deadline_survives_cleanup_io_failure() -> None:
    async def handler(request: Message, context: RequestContext[Message, Message]) -> Message:
        try:
            await anyio.sleep_forever()
        finally:
            raise OSError("Cleanup failed")

    sent = await invoke(
        RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), handler)}, 1024),
        scope([(b"grpc-timeout", b"30m")]),
    )
    assert status(sent) == b"4"


async def test_disconnect_does_not_cancel_cleanup_twice_at_deadline() -> None:
    entered = anyio.Event()
    cleaned = anyio.Event()
    delivered = False
    sent: list[ASGIMessage] = []

    async def handler(request: Message, context: RequestContext[Message, Message]) -> Message:
        entered.set()
        try:
            await anyio.sleep_forever()
        finally:
            await anyio.sleep(0.06)
            cleaned.set()
        raise AssertionError("unreachable")  # pragma: no cover - sleep_forever never returns

    async def receive() -> ASGIMessage:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": frame(StringValue(value="hello")), "more_body": False}
        await entered.wait()
        return {"type": "http.disconnect"}

    async def send(event: ASGIMessage) -> None:
        sent.append(event)  # pragma: no cover - disconnected responses are suppressed

    with anyio.fail_after(2):
        await RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), handler)}, 1024)(
            scope([(b"grpc-timeout", b"30m")]), receive, send
        )
    assert cleaned.is_set()
    assert sent == []


async def test_external_handler_cancellation_is_not_silent_success() -> None:
    async def handler(request: Message, context: RequestContext[Message, Message]) -> Message:
        raise asyncio.CancelledError

    with pytest.raises(ExceptionGroup) as caught:
        await invoke(RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), handler)}, 1024), scope())
    assert len(caught.value.exceptions) == 1
    error = caught.value.exceptions[0]
    assert isinstance(error, ConnectError)
    assert error.code == Code.CANCELED


async def test_deadline_response_handles_closed_send() -> None:
    cancelled = anyio.Event()

    async def handler(request: Message, context: RequestContext[Message, Message]) -> Message:
        try:
            await anyio.sleep_forever()
        finally:
            cancelled.set()
        raise AssertionError("unreachable")  # pragma: no cover - sleep_forever never returns

    delivered = False

    async def receive() -> ASGIMessage:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": frame(StringValue(value="hello")), "more_body": False}
        await anyio.sleep_forever()
        raise AssertionError("unreachable")  # pragma: no cover - sleep_forever never returns

    async def send(event: ASGIMessage) -> None:
        raise OSError("Peer closed")

    with anyio.fail_after(2):
        await RPCTransport({"/test.Service/Echo": Endpoint.unary(method(), handler)}, 1024)(
            scope([(b"grpc-timeout", b"30m")]), receive, send
        )
    assert cancelled.is_set()


async def test_constructor_and_non_http_scope_validation() -> None:
    with pytest.raises(ValueError, match="positive"):
        RPCTransport({}, 0)
    with pytest.raises(RuntimeError, match="HTTP"):
        await invoke(RPCTransport({}, 1024), {"type": "websocket"})
