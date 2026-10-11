from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, make_dataclass
from typing import Annotated, Any

import anyio
import pytest
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import BaseModel
from starlette.testclient import TestClient
from starlette.types import Message as ASGIMessage, Scope

from grpclette import Depends, RPCContext, gRPClette


class Input(BaseModel):
    text: str


@pytest.mark.parametrize(
    "kwargs",
    [
        {"service": "a.b"},
        {"service": ""},
        {"package": ""},
        {"package": "a..b"},
        {"max_message_bytes": 0},
        {"max_message_bytes": -1},
    ],
)
def test_invalid_configuration(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        gRPClette(**kwargs)


def test_decorator_preserves_callable_and_infers_name() -> None:
    app = gRPClette(service="Users", package="example.v1")

    async def get_user(request: Input) -> Input:
        return request  # pragma: no cover - This test verifies registration, not handler execution.

    assert app.rpc(get_user) is get_user
    assert "rpc GetUser" in app.proto()
    assert app.descriptor().service[0].name == "Users"
    with pytest.raises(ValueError, match="already registered"):
        app.rpc(get_user)


def test_registration_is_frozen_after_startup() -> None:
    app = gRPClette()

    async def echo(request: Input) -> Input:
        return request  # pragma: no cover - Registration is rejected before execution.

    with TestClient(app):
        with pytest.raises(RuntimeError, match="before the application starts"):
            app.rpc(echo)


@pytest.mark.parametrize(
    "scenario",
    [
        "sync",
        "private",
        "unannotated",
        "missing-return",
        "two-requests",
        "scalar",
        "positional",
        "variadic",
        "incorrect-stream",
        "sync-dependency",
        "invalid-dependency",
        "cycle",
    ],
)
def test_invalid_registration(scenario: str) -> None:
    async def echo(request: Input) -> Input:
        return request  # pragma: no cover - Invalid registrations never invoke the handler.

    handler: Callable[..., Any] = echo
    if scenario == "sync":

        def sync(request: Input) -> Input:
            return request  # pragma: no cover - Sync handlers are rejected.

        handler = sync
    elif scenario == "private":
        handler.__name__ = "_echo"
    elif scenario == "unannotated":
        handler.__annotations__.pop("request")
    elif scenario == "missing-return":
        handler.__annotations__.pop("return")
    elif scenario == "two-requests":

        async def two(first: Input, second: Input) -> Input:
            return first  # pragma: no cover - Multiple request parameters are rejected.

        handler = two
    elif scenario == "scalar":

        async def scalar(request: int) -> Input:
            return Input(text=str(request))  # pragma: no cover - Scalar messages are unsupported.

        handler = scalar
    elif scenario == "positional":

        async def positional(request: Input, /) -> Input:
            return request  # pragma: no cover - Positional-only parameters are rejected.

        handler = positional
    elif scenario == "variadic":

        async def variadic(*request: Input) -> Input:
            return request[0]  # pragma: no cover - Variadic parameters are rejected.

        handler = variadic
    elif scenario == "incorrect-stream":

        async def incorrect(request: Input) -> AsyncIterator[Input]:
            return iter(())  # type: ignore[return-value]  # pragma: no cover - Annotation is rejected.

        handler = incorrect
    elif scenario == "sync-dependency":

        def dependency() -> str:
            return "no"  # pragma: no cover - Sync dependencies are rejected.

        handler.__defaults__ = (Depends(dependency),)
    elif scenario == "invalid-dependency":

        async def invalid_dependency(request: str) -> str:
            return request  # pragma: no cover - Dependency request types are rejected.

        handler.__defaults__ = (Depends(invalid_dependency),)
    else:
        handler.__defaults__ = (Depends(handler),)
    with pytest.raises((TypeError, ValueError)):
        gRPClette().rpc(handler)


def test_context_is_not_a_request_parameter() -> None:
    app = gRPClette()

    @app.rpc
    async def ping(context: RPCContext) -> None:
        return None  # pragma: no cover - This test checks schema inference.

    assert "google.protobuf.Empty" in app.proto()


def test_rejected_response_does_not_change_exports_and_allows_type_name_retry() -> None:
    app = gRPClette()

    @app.rpc
    async def echo(request: Input) -> Input:
        return request  # pragma: no cover - This test verifies registration and exports.

    original_proto = app.proto()
    original_descriptor = app.descriptor().SerializeToString()

    @dataclass
    class Candidate:
        stamp: Timestamp

    async def rejected(request: Candidate) -> int:
        return 1  # pragma: no cover - Unsupported responses prevent registration.

    with pytest.raises(TypeError, match="Unsupported message annotation"):
        app.rpc(rejected)
    assert app.proto() == original_proto
    assert app.descriptor().SerializeToString() == original_descriptor

    replacement = make_dataclass("Candidate", [("text", str)])

    async def accepted(request: Any) -> Any:
        return request  # pragma: no cover - This test verifies a same-name registration retry.

    accepted.__annotations__ = {"request": replacement, "return": replacement}
    app.rpc(accepted)
    exported = app.descriptor()
    assert {message.name for message in exported.message_type} == {"Candidate", "Input"}
    assert exported.dependency == []
    assert {method.name for method in exported.service[0].method} == {"Accepted", "Echo"}
    assert "string text = 1;" in app.proto()
    assert "stamp" not in app.proto()


def test_duplicate_registration_precedes_model_compilation() -> None:
    app = gRPClette()

    @app.rpc
    async def echo(request: Input) -> Input:
        return request  # pragma: no cover - This test verifies duplicate registration rejection.

    @dataclass
    class Unregistered:
        stamp: Timestamp

    async def duplicate(request: Unregistered) -> int:
        return 1  # pragma: no cover - Duplicate registration prevents execution.

    duplicate.__name__ = "echo"
    original = app.proto()
    with pytest.raises(ValueError, match="already registered"):
        app.rpc(duplicate)
    assert app.proto() == original
    assert app.descriptor().dependency == []


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["unary", "client", "server", "bidi"])
@pytest.mark.parametrize("cancel", ["task", "scope"])
async def test_outer_cancellation_awaits_dependency_and_generator_cleanup(kind: str, cancel: str) -> None:
    app = gRPClette()
    entered = anyio.Event()
    cleaned = anyio.Event()
    generator_cleaned = anyio.Event()
    delivered = False
    events: list[str] = []

    async def resource() -> AsyncIterator[None]:
        try:
            yield None
        finally:
            await anyio.sleep(0)
            events.append("dependency")
            cleaned.set()

    async def unary(request: Input, resource: Annotated[None, Depends(resource)]) -> None:
        entered.set()
        await anyio.sleep_forever()

    async def client(request: AsyncIterator[Input], resource: Annotated[None, Depends(resource)]) -> None:
        await anext(request)
        entered.set()
        await anyio.sleep_forever()

    async def server(request: Input, resource: Annotated[None, Depends(resource)]) -> AsyncIterator[Input]:
        try:
            yield request
            pytest.fail("Blocked send must not resume the handler")  # pragma: no cover - Send remains blocked.
        finally:
            await anyio.sleep(0)
            events.append("generator")
            generator_cleaned.set()

    async def bidi(request: AsyncIterator[Input], resource: Annotated[None, Depends(resource)]) -> AsyncIterator[Input]:
        try:
            yield await anext(request)
            pytest.fail("Blocked send must not resume the handler")  # pragma: no cover - Send remains blocked.
        finally:
            await anyio.sleep(0)
            events.append("generator")
            generator_cleaned.set()

    handlers: dict[str, Callable[..., Any]] = {"unary": unary, "client": client, "server": server, "bidi": bidi}
    app.rpc(handlers[kind])
    scope: Scope = {
        "type": "http",
        "http_version": "2",
        "method": "POST",
        "scheme": "http",
        "path": f"/grpclette.Service/{kind.title()}",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"content-type", b"application/grpc+proto")],
        "extensions": {"http.response.trailers": {}},
    }

    async def receive() -> ASGIMessage:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": b"\x00" * 5, "more_body": False}
        await anyio.sleep_forever()
        raise AssertionError("unreachable")  # pragma: no cover - sleep_forever never returns.

    async def send(message: ASGIMessage) -> None:
        if message["type"] == "http.response.body" and message.get("body"):
            entered.set()
            await anyio.sleep_forever()

    with pytest.raises(ExceptionGroup) as caught, anyio.fail_after(2):
        if cancel == "task":
            task = asyncio.create_task(app(scope, receive, send))
            await entered.wait()
            task.cancel()
            await task
        else:
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(app, scope, receive, send)
                await entered.wait()
                tasks.cancel_scope.cancel()
    assert caught.value.split(ConnectError)[1] is None
    assert caught.value.subgroup(lambda exc: isinstance(exc, ConnectError) and exc.code != Code.CANCELED) is None
    assert cleaned.is_set()
    assert generator_cleaned.is_set() == (kind in ("server", "bidi"))
    assert events == (["generator", "dependency"] if kind in ("server", "bidi") else ["dependency"])
