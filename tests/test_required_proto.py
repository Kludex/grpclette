from __future__ import annotations

import struct

import anyio
import pytest
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.message import Message
from starlette.types import Message as ASGIMessage

from grpclette import gRPClette


@pytest.mark.anyio
async def test_required_protobuf_fields_are_validated_before_dispatch() -> None:
    definition = descriptor_pb2.FileDescriptorProto(name="required.proto", package="required", syntax="proto2")
    message = definition.message_type.add(name="Request")
    message.field.add(
        name="text",
        number=1,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_REQUIRED,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    )
    pool = descriptor_pool.DescriptorPool()
    pool.AddSerializedFile(definition.SerializeToString())
    request_type = message_factory.GetMessageClass(pool.FindMessageTypeByName("required.Request"))
    app = gRPClette()
    delivered = False
    sent: list[ASGIMessage] = []

    async def required(request: Message) -> Message:
        pytest.fail("Invalid request dispatched")  # pragma: no cover - Missing required fields prevent dispatch.

    required.__annotations__ = {"request": request_type, "return": request_type}
    app.rpc(required)

    async def receive() -> ASGIMessage:
        nonlocal delivered
        if delivered:
            await anyio.sleep_forever()
            raise AssertionError("unreachable")  # pragma: no cover - Receive waits for transport cancellation.
        delivered = True
        return {"type": "http.request", "body": struct.pack("!BI", 0, 0)}

    async def send(event: ASGIMessage) -> None:
        sent.append(event)

    await app(
        {
            "type": "http",
            "method": "POST",
            "path": "/grpclette.Service/Required",
            "root_path": "",
            "http_version": "2",
            "headers": [(b"content-type", b"application/grpc")],
            "extensions": {"http.response.trailers": {}},
        },
        receive,
        send,
    )
    assert dict(sent[-1]["headers"])[b"grpc-status"] == b"3"
