from __future__ import annotations

import inspect
import re
from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Callable
from contextlib import AsyncExitStack, aclosing
from dataclasses import dataclass
from typing import Any, cast, get_args, get_origin, get_type_hints

import anyio
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.method import IdempotencyLevel, MethodInfo
from connectrpc.request import RequestContext
from connectrpc.server import Endpoint
from google.protobuf.empty_pb2 import Empty
from google.protobuf.message import DecodeError, Message
from pydantic import ValidationError

from grpclette.context import RPC_STATE, RPCContext
from grpclette.dependencies import arguments, dependency_marker, parameter_type, validate_parameters
from grpclette.errors import handler_errors
from grpclette.messages import MessageAdapter
from grpclette.schema import MethodDefinition, Schema


@dataclass(frozen=True)
class RPCMethod:
    handler: Callable[..., Any]
    definition: MethodDefinition
    request: MessageAdapter
    response: MessageAdapter
    namespace: dict[str, Any]

    @classmethod
    def create(cls, handler: Callable[..., Any], schema: Schema, namespace: dict[str, Any]) -> RPCMethod:
        if not (inspect.iscoroutinefunction(handler) or inspect.isasyncgenfunction(handler)):
            raise ValueError("RPC handlers must be async functions or async generators.")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", handler.__name__):
            raise ValueError("RPC names must start with a letter and contain ASCII letters, digits, or underscores.")
        hints = get_type_hints(handler, localns=namespace, include_extras=True)
        if "return" not in hints:
            raise ValueError("RPC handlers require a return annotation.")
        request_annotations = [
            hints.get(name)
            for name, parameter in inspect.signature(handler).parameters.items()
            if dependency_marker(hints.get(name), parameter.default) is None
            and parameter_type(hints.get(name)) is not RPCContext
        ]
        if len(request_annotations) > 1 or (request_annotations and request_annotations[0] is None):
            raise ValueError("RPC handlers accept one typed request plus optional context and dependencies.")
        request_annotation = request_annotations[0] if request_annotations else type(None)
        validate_parameters(handler, request_annotation, namespace)
        request_type, client_streaming = stream_item(request_annotation)
        response_type, server_streaming = stream_item(hints["return"])
        if inspect.isasyncgenfunction(handler) != server_streaming:
            raise ValueError("Streaming response annotations require an async-generator handler.")
        request_message = schema.message(Empty if request_type is type(None) else request_type, namespace=namespace)
        response_message = schema.message(Empty if response_type is type(None) else response_type, namespace=namespace)
        name = "".join(part[:1].upper() + part[1:] for part in handler.__name__.split("_"))
        definition = MethodDefinition(name, request_message, response_message, client_streaming, server_streaming)
        return cls(
            handler,
            definition,
            MessageAdapter.create(request_type, request_message, namespace=namespace),
            MessageAdapter.create(response_type, response_message, namespace=namespace),
            namespace,
        )

    def endpoint(self, service_name: str) -> Endpoint[Message, Message]:
        info = MethodInfo(
            name=self.definition.name,
            service_name=service_name,
            input=self.definition.input,
            output=self.definition.output,
            idempotency_level=IdempotencyLevel.UNKNOWN,
        )
        if self.definition.server_streaming:
            if self.definition.client_streaming:
                return Endpoint.bidi_stream(info, self.stream)
            return Endpoint.server_stream(info, self.stream)
        if self.definition.client_streaming:
            return Endpoint.client_stream(info, self.unary)
        return Endpoint.unary(info, self.unary)

    async def unary(
        self, request: Message | AsyncIterator[Message], context: RequestContext[Message, Message]
    ) -> Message:
        async with handler_errors(self.definition.name):
            stack = AsyncExitStack()
            try:
                incoming = self.incoming(request)
                if self.definition.client_streaming:
                    incoming = await stack.enter_async_context(aclosing(incoming))
                kwargs = await arguments(
                    self.handler, incoming, RPCContext(context, RPC_STATE.get()), stack, {}, self.namespace
                )
                result = await self.handler(**kwargs)
                return self.response.encode(result)
            finally:
                with anyio.CancelScope(shield=True):
                    await stack.aclose()

    async def stream(
        self, request: Message | AsyncIterator[Message], context: RequestContext[Message, Message]
    ) -> AsyncIterator[Message]:
        async with handler_errors(self.definition.name):
            stack = AsyncExitStack()
            try:
                incoming = self.incoming(request)
                if self.definition.client_streaming:
                    incoming = await stack.enter_async_context(aclosing(incoming))
                kwargs = await arguments(
                    self.handler, incoming, RPCContext(context, RPC_STATE.get()), stack, {}, self.namespace
                )
                output = await stack.enter_async_context(aclosing(self.handler(**kwargs)))
                async for result in output:
                    yield self.response.encode(result)
            finally:
                with anyio.CancelScope(shield=True):
                    await stack.aclose()

    def incoming(self, request: Message | AsyncIterator[Message]) -> Any:
        if self.definition.client_streaming:
            return self.decode_stream(cast(AsyncIterator[Message], request))
        try:
            return self.request.decode(cast(Message, request))
        except (DecodeError, ValidationError) as exc:
            raise ConnectError(Code.INVALID_ARGUMENT, "Invalid request.") from exc

    async def decode_stream(self, requests: AsyncIterator[Message]) -> AsyncGenerator[Any, None]:
        async for request in requests:
            try:
                yield self.request.decode(request)
            except (DecodeError, ValidationError) as exc:
                raise ConnectError(Code.INVALID_ARGUMENT, "Invalid request.") from exc


def stream_item(annotation: Any) -> tuple[Any, bool]:
    annotation = parameter_type(annotation)
    if get_origin(annotation) in (AsyncIterator, AsyncIterable, AsyncGenerator):
        return get_args(annotation)[0], True
    return annotation, False
