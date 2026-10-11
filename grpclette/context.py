from __future__ import annotations

from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

import anyio
from connectrpc.method import MethodInfo
from connectrpc.request import Headers, RequestContext
from google.protobuf.message import Message

RPC_STATE: ContextVar[Mapping[str, Any]] = ContextVar("grpclette_state", default={})
RPC_DEADLINE: ContextVar[float | None] = ContextVar("grpclette_deadline", default=None)


@dataclass(frozen=True)
class RPCContext:
    context: RequestContext[Message, Message]
    state: Mapping[str, Any]

    @property
    def method(self) -> MethodInfo[Message, Message]:
        return self.context.method

    @property
    def request_headers(self) -> Headers:
        return self.context.request_headers

    @property
    def response_headers(self) -> Headers:
        return self.context.response_headers

    @property
    def response_trailers(self) -> Headers:
        return self.context.response_trailers

    @property
    def timeout_ms(self) -> float | None:
        deadline = RPC_DEADLINE.get()
        return self.context.timeout_ms if deadline is None else (deadline - anyio.current_time()) * 1000

    @property
    def client_address(self) -> str | None:
        return self.context.client_address
