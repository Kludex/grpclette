from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, cast

import anyio
from google.protobuf.empty_pb2 import Empty
from google.protobuf.timestamp_pb2 import Timestamp
from starlette.applications import Starlette

from grpclette import Code, Depends, RPCContext, RPCError, gRPClette
from tests.models import (
    AliasedEcho,
    Blob,
    EchoInput,
    EchoResult,
    Envelope,
    LifespanState,
    ProtobufEnvelope,
    ProtobufMapping,
    ProtobufRecord,
    Summary,
)


@asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[LifespanState]:
    yield {"prefix": "hello"}


def create_app(events: list[str], cancelled: anyio.Event) -> gRPClette:
    app = gRPClette(service="TestService", package="testing", lifespan=lifespan)

    @app.rpc
    async def echo(request: EchoInput) -> EchoResult:
        return {"text": request.text}

    @app.rpc
    async def watch(request: EchoInput) -> AsyncIterator[EchoResult]:
        yield {"text": request.text + "0"}
        yield {"text": request.text + "1"}

    @app.rpc
    async def upload(requests: AsyncIterator[EchoInput]) -> Summary:
        count = 0
        async for _ in requests:
            count += 1
        return Summary(count=count)

    @app.rpc
    async def talk(requests: AsyncIterator[EchoInput]) -> AsyncIterator[EchoResult]:
        async for request in requests:
            yield {"text": request.text}

    @app.rpc
    async def blob(request: Blob) -> Blob:
        return request

    @app.rpc
    async def envelope(request: Envelope) -> Envelope:
        return request

    @app.rpc
    async def protobuf_envelope(request: ProtobufEnvelope) -> ProtobufEnvelope:
        assert type(request.stamp) is Timestamp
        return request

    @app.rpc
    async def protobuf_record(request: ProtobufRecord) -> ProtobufRecord:
        assert type(request) is ProtobufRecord
        assert type(request.stamp) is Timestamp
        return request

    @app.rpc
    async def protobuf_dictionary(request: ProtobufMapping) -> ProtobufMapping:
        assert type(request["stamp"]) is Timestamp
        return request

    @app.rpc
    async def alias_echo(request: AliasedEcho) -> AliasedEcho:
        return cast(AliasedEcho, {"text": request.text})

    @app.rpc
    async def pydantic_echo(request: EchoInput) -> EchoInput:
        return request

    @app.rpc
    async def mapping_request(request: Annotated[EchoResult, "request model"]) -> EchoResult:
        return request

    @app.rpc
    async def timestamp(request: Timestamp) -> Timestamp:
        return Timestamp(seconds=request.seconds, nanos=request.nanos)

    @app.rpc
    async def protobuf_mapping(request: Timestamp) -> Timestamp:
        return cast(Timestamp, {"seconds": request.seconds, "nanos": request.nanos})

    @app.rpc
    async def wrong_protobuf(request: Timestamp) -> Timestamp:
        return cast(Timestamp, Empty())

    @app.rpc
    async def invalid_protobuf(request: Timestamp) -> Timestamp:
        return cast(Timestamp, "not a message")

    @app.rpc
    async def protobuf_result(request: EchoInput, context: RPCContext) -> EchoResult:
        return cast(EchoResult, context.method.output(text=request.text))

    @app.rpc
    async def empty() -> None:
        return None

    async def prefix(context: RPCContext) -> str:
        return cast(LifespanState, context.state)["prefix"]

    async def authenticate(context: RPCContext, prefix: Annotated[str, Depends(prefix)]) -> AsyncIterator[str]:
        if context.request_headers.get("authorization") != "Bearer test":
            raise RPCError(Code.UNAUTHENTICATED, "Authentication required.")
        events.append("acquire")
        try:
            yield prefix
        finally:
            events.append("release")

    @app.rpc
    async def context(
        request: EchoInput,
        context: RPCContext,
        first: Annotated[str, "cached resource", Depends(authenticate)],
        second: str = Depends(authenticate),  # type: ignore[assignment]
    ) -> EchoResult:
        assert first == second
        context.response_headers["x-header"] = "hello"
        context.response_trailers["x-trailer"] = "bye"
        assert context.method.name == "Context"
        assert context.client_address is not None
        return {"text": first + ":" + request.text}

    @app.rpc
    async def broken(request: EchoInput) -> EchoResult:
        raise RuntimeError("secret application details")

    @app.rpc
    async def invalid_response(request: EchoInput) -> EchoResult:
        return cast(EchoResult, {"wrong": "value"})

    @app.rpc
    async def none_response(request: EchoInput) -> EchoResult:
        return cast(EchoResult, None)

    @app.rpc
    async def failing_stream(request: EchoInput) -> AsyncIterator[EchoResult]:
        yield {"text": request.text}
        raise RPCError(Code.NOT_FOUND, "The next item was not found.")

    @app.rpc
    async def wait(request: EchoInput, context: RPCContext) -> EchoResult:
        assert context.timeout_ms is not None
        try:
            await anyio.sleep_forever()
        finally:
            cancelled.set()
        raise AssertionError("unreachable")  # pragma: no cover - sleep_forever exits only by cancellation.

    @app.rpc
    async def waiting_stream(request: EchoInput) -> AsyncIterator[EchoResult]:
        try:
            yield {"text": request.text}
            await anyio.sleep_forever()
        finally:
            cancelled.set()

    return app
