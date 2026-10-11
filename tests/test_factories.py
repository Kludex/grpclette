from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from types import GenericAlias
from typing import Any, Self

import anyio
import pytest
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.message import Message
from starlette.types import Message as ASGIMessage, Scope
from typing_extensions import TypedDict

from grpclette import gRPClette


def dataclass_app() -> gRPClette:
    app = gRPClette()

    @dataclass
    class Child:
        text: str

    @dataclass
    class Request:
        child: Child
        children: list[Child]
        lookup: dict[str, Child]

    @dataclass
    class Reply:
        child: Child
        children: list[Child]
        lookup: dict[str, Child]

    @app.rpc
    async def echo(request: Request) -> Reply:
        assert isinstance(request, Request)
        assert isinstance(request.child, Child)
        assert isinstance(request.children[0], Child)
        assert isinstance(request.lookup["first"], Child)
        return Reply(request.child, request.children, request.lookup)

    return app


def dictionary_app() -> gRPClette:
    app = gRPClette()

    class Child(TypedDict):
        text: str

    class Request(TypedDict):
        child: Child
        children: list[Child]
        lookup: dict[str, Child]

    class Reply(TypedDict):
        child: Child
        children: list[Child]
        lookup: dict[str, Child]

    @app.rpc
    async def echo(request: Request) -> Reply:
        assert request["child"]["text"] == "child"
        assert request["children"][0]["text"] == "repeated"
        assert request["lookup"]["first"]["text"] == "mapped"
        return Reply(child=request["child"], children=request["children"], lookup=request["lookup"])

    return app


async def invoke(app: gRPClette, payload: bytes) -> list[ASGIMessage]:
    scope: Scope = {
        "type": "http",
        "http_version": "2",
        "method": "POST",
        "scheme": "http",
        "path": "/grpclette.Service/Echo",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"content-type", b"application/grpc+proto")],
        "extensions": {"http.response.trailers": {}},
    }
    delivered = False
    sent: list[ASGIMessage] = []

    async def receive() -> ASGIMessage:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": b"\x00" + len(payload).to_bytes(4, "big") + payload}
        await anyio.sleep_forever()
        raise AssertionError("unreachable")  # pragma: no cover - sleep_forever never returns.

    async def send(message: ASGIMessage) -> None:
        sent.append(message)
        await anyio.sleep(0)

    with anyio.fail_after(2):
        await app(scope, receive, send)
    return sent


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["dataclass", "typeddict"])
async def test_nested_factory_models_roundtrip_through_asgi(kind: str) -> None:
    app = dataclass_app() if kind == "dataclass" else dictionary_app()
    descriptor = app.descriptor()
    assert {message.name for message in descriptor.message_type} == {"Child", "Request", "Reply"}
    assert descriptor.dependency == []
    pool = descriptor_pool.DescriptorPool()
    pool.AddSerializedFile(descriptor.SerializeToString())
    request_type = message_factory.GetMessageClass(pool.FindMessageTypeByName("grpclette.Request"))
    reply_type = message_factory.GetMessageClass(pool.FindMessageTypeByName("grpclette.Reply"))
    request = request_type(
        child={"text": "child"}, children=[{"text": "repeated"}], lookup={"first": {"text": "mapped"}}
    )
    sent = await invoke(app, request.SerializeToString())
    assert sent[0]["status"] == 200
    assert dict(sent[-1]["headers"])[b"grpc-status"] == b"0"
    body = b"".join(event.get("body", b"") for event in sent)
    assert body[:5] == b"\x00" + (len(body) - 5).to_bytes(4, "big")
    reply = reply_type.FromString(body[5:])
    assert reply == reply_type.FromString(request.SerializeToString())


def test_unsupported_self_fields_do_not_change_exports() -> None:
    app = gRPClette()

    @dataclass
    class Node:
        text: str
        child: Self | None = None

    async def echo(request: Node) -> Node:
        return request  # pragma: no cover - Unsupported field annotations prevent registration.

    original = app.proto()
    with pytest.raises(TypeError, match="Unsupported message annotation"):
        app.rpc(echo)
    assert app.proto() == original
    assert app.descriptor().message_type == []


@pytest.mark.anyio
@pytest.mark.parametrize("streaming", [False, True])
async def test_missing_required_protobuf_fields_are_invalid_argument(streaming: bool) -> None:
    file = descriptor_pb2.FileDescriptorProto(name="legacy/required.proto", package="legacy", syntax="proto2")
    model = file.message_type.add(name="RequiredRequest")
    model.field.add(
        name="text",
        number=1,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_REQUIRED,
    )
    pool = descriptor_pool.DescriptorPool()
    pool.AddSerializedFile(file.SerializeToString())
    request_type = message_factory.GetMessageClass(pool.FindMessageTypeByName("legacy.RequiredRequest"))
    dispatched = False

    async def unary(request: Message) -> None:
        nonlocal dispatched
        dispatched = True  # pragma: no cover - Missing required fields must prevent dispatch.

    async def client(request: AsyncIterator[Message]) -> None:
        nonlocal dispatched
        await anext(request)
        dispatched = True  # pragma: no cover - Invalid messages cannot be yielded to the handler.

    handler: Callable[..., Any] = client if streaming else unary
    handler.__name__ = "echo"
    handler.__annotations__ = {
        "request": GenericAlias(AsyncIterator, request_type) if streaming else request_type,
        "return": type(None),
    }
    app = gRPClette()
    app.rpc(handler)
    sent = await invoke(app, b"")
    assert dict(sent[-1]["headers"])[b"grpc-status"] == b"3"
    assert not dispatched
