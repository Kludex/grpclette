from __future__ import annotations

import struct

import pytest
from connectrpc.method import IdempotencyLevel, MethodInfo
from connectrpc.request import RequestContext
from connectrpc.server import Endpoint
from google.protobuf.message import Message
from google.protobuf.wrappers_pb2 import StringValue
from starlette.types import Message as ASGIMessage

from grpclette.transport import RPCTransport


@pytest.mark.anyio
async def test_compressed_flag_requires_encoding_header() -> None:
    invoked = False
    delivered = False
    sent: list[ASGIMessage] = []

    async def handler(request: Message, context: RequestContext[Message, Message]) -> Message:
        nonlocal invoked
        invoked = True  # pragma: no cover - Invalid framing must prevent handler dispatch.
        return request  # pragma: no cover - Invalid framing must prevent handler dispatch.

    async def receive() -> ASGIMessage:
        nonlocal delivered
        assert not delivered
        delivered = True
        body = StringValue(value="hello").SerializeToString()
        return {"type": "http.request", "body": struct.pack("!BI", 1, len(body)) + body}

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    method: MethodInfo[Message, Message] = MethodInfo(
        name="Echo",
        service_name="example.Service",
        input=StringValue,
        output=StringValue,
        idempotency_level=IdempotencyLevel.UNKNOWN,
    )
    await RPCTransport({"/example.Service/Echo": Endpoint.unary(method, handler)}, 1024)(
        {
            "type": "http",
            "path": "/example.Service/Echo",
            "root_path": "",
            "method": "POST",
            "http_version": "2",
            "headers": [(b"content-type", b"application/grpc")],
            "extensions": {"http.response.trailers": {}},
        },
        receive,
        send,
    )
    assert not invoked
    assert dict(sent[-1]["headers"])[b"grpc-status"] == b"3"
