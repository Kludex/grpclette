from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator, Callable
from typing import Any, cast

import anyio
from connectrpc.code import Code
from connectrpc.compat import google_protobuf_codecs
from connectrpc.errors import ConnectError
from connectrpc.method import IdempotencyLevel, MethodInfo
from connectrpc.request import RequestContext
from connectrpc.server import ConnectASGIApplication, Endpoint
from google.protobuf.message import Message
from starlette.types import ASGIApp, Message as ASGIMessage, Receive, Scope, Send

from grpclette.context import RPC_DEADLINE
from grpclette.framing import FrameValidator


class RPCTransport:
    def __init__(self, endpoints: dict[str, Endpoint[Message, Message]], max_message_bytes: int) -> None:
        if max_message_bytes <= 0:
            raise ValueError("max_message_bytes must be positive.")
        self._endpoints = endpoints
        self._application = cast(
            ASGIApp,
            _Application(
                service=None,
                endpoints=lambda _: cast(Any, endpoints),
                read_max_bytes=max_message_bytes,
                codecs=google_protobuf_codecs(),
            ),
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            raise RuntimeError("RPCTransport only supports HTTP scopes.")
        headers = scope.get("headers", [])
        content_types = [value for key, value in headers if key.lower() == b"content-type"]
        status = 0
        if content_types not in ([b"application/grpc"], [b"application/grpc+proto"]):
            status = 415
        elif scope["method"] != "POST":
            status = 405
        elif scope.get("http_version") != "2" or "http.response.trailers" not in scope.get("extensions", {}):
            status = 505
        if status:
            await send(
                {
                    "type": "http.response.start",
                    "status": status,
                    "headers": [(b"allow", b"POST")] if status == 405 else [],
                }
            )
            await send({"type": "http.response.body", "body": b""})
            return

        timeout_headers = [value for key, value in headers if key.lower() == b"grpc-timeout"]
        timeout: float | None = None
        if timeout_headers:
            if len(timeout_headers) != 1 or not re.fullmatch(rb"[0-9]{1,8}[HMSmun]", timeout_headers[0]):
                await _error_response(Code.INVALID_ARGUMENT, scope, receive, send)
                return
            value = timeout_headers[0]
            timeout = int(value[:-1]) * {b"H": 3600, b"M": 60, b"S": 1, b"m": 1e-3, b"u": 1e-6, b"n": 1e-9}[value[-1:]]
        deadline = anyio.current_time() + timeout if timeout is not None else None
        path = scope["path"]
        if path not in self._endpoints and path.removeprefix(scope.get("root_path", "")) not in self._endpoints:
            await _error_response(Code.UNIMPLEMENTED, scope, receive, send)
            return
        if deadline is not None and deadline <= anyio.current_time():
            await _error_response(Code.DEADLINE_EXCEEDED, scope, receive, send)
            return

        # ponytail: one unread chunk; out-of-band server signals are needed behind backpressured input.
        incoming, outgoing = anyio.create_memory_object_stream[ASGIMessage | ConnectError](0)
        framing = FrameValidator(compressed=any(key.lower() == b"grpc-encoding" for key, _ in headers))
        finished = anyio.Event()
        aborted: Code | None = None
        started = ended_body = completed = final_send = False

        async def guarded_send(message: ASGIMessage) -> None:
            nonlocal started, ended_body, completed, final_send
            if aborted is not None:
                return
            final_send = message["type"] == "http.response.trailers" and not message.get("more_trailers", False)
            await send(message)
            if message["type"] == "http.response.start":
                started = True
            elif message["type"] == "http.response.body" and not message.get("more_body", False):
                ended_body = True
            elif message["type"] == "http.response.trailers" and not message.get("more_trailers", False):
                completed = True

        async def guarded_receive() -> ASGIMessage:
            message = await outgoing.receive()
            if isinstance(message, ConnectError):
                raise message
            return message

        async def run(task_status: anyio.abc.TaskStatus[Callable[[], bool]]) -> None:
            nonlocal aborted
            # Connect's server is asyncio-only; edge cancellation lets async cleanup finish.
            task_status.started(cast(asyncio.Task[None], asyncio.current_task()).cancel)
            token = RPC_DEADLINE.set(deadline)
            try:
                await self._application(scope, guarded_receive, guarded_send)
            except ConnectError:
                if aborted is None:
                    raise
            except OSError:
                if aborted is None:
                    aborted = Code.CANCELED
            finally:
                RPC_DEADLINE.reset(token)
                finished.set()

        async def monitor(cancel: Callable[[], bool]) -> None:
            nonlocal aborted
            while True:
                try:
                    message = await receive()
                except OSError:
                    message = {"type": "http.disconnect"}
                if message["type"] == "http.disconnect":
                    # Let an accepted final send return before cancelling an unfinished send.
                    if final_send:
                        await anyio.sleep(0)
                    if not completed and aborted is None:
                        aborted = Code.CANCELED
                        cancel()
                    return
                if message["type"] == "http.request":
                    try:
                        framing(message.get("body", b""), final=not message.get("more_body", False))
                    except ConnectError as error:
                        await incoming.send(error)
                        return
                await incoming.send(message)

        async def expire(cancel: Callable[[], bool]) -> None:
            nonlocal aborted
            assert deadline is not None
            await anyio.sleep(max(0, deadline - anyio.current_time()))
            if not completed and aborted is None:
                aborted = Code.DEADLINE_EXCEEDED
                cancel()

        async with incoming, outgoing, anyio.create_task_group() as tasks:
            cancel = await tasks.start(run)
            tasks.start_soon(monitor, cancel)
            if deadline is not None:
                tasks.start_soon(expire, cancel)
            await finished.wait()
            tasks.cancel_scope.cancel()
        if aborted == Code.DEADLINE_EXCEEDED:

            async def finish(message: ASGIMessage) -> None:
                if message["type"] == "http.response.start" and started:
                    return
                if message["type"] == "http.response.body" and ended_body:
                    return
                await send(message)

            try:
                await _error_response(aborted, scope, receive, finish)
            except OSError:
                return


class _Application(ConnectASGIApplication[None]):
    path = "/"


async def _error_response(code: Code, scope: Scope, receive: Receive, send: Send) -> None:
    async def reject(request: AsyncIterator[Message], context: RequestContext[Message, Message]) -> Message:
        raise ConnectError(code, code.name.replace("_", " ").capitalize())

    endpoint = Endpoint.client_stream(
        MethodInfo(
            name="Reject",
            service_name="grpclette.Transport",
            input=Message,
            output=Message,
            idempotency_level=IdempotencyLevel.UNKNOWN,
        ),
        reject,
    )
    application = cast(
        ASGIApp,
        _Application(
            service=None,
            endpoints=lambda _: {"/": endpoint},
            codecs=google_protobuf_codecs(),
        ),
    )
    await application(
        {**scope, "path": "/", "root_path": "", "headers": [(b"content-type", b"application/grpc+proto")]},
        receive,
        send,
    )
