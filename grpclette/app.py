from __future__ import annotations

import inspect
import re
from collections.abc import Callable, Sequence
from typing import Any, TypeVar

from google.protobuf.descriptor_pb2 import FileDescriptorProto
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.routing import Mount
from starlette.types import Lifespan, Receive, Scope, Send

from grpclette.context import RPC_STATE
from grpclette.methods import RPCMethod
from grpclette.schema import Schema
from grpclette.transport import RPCTransport

F = TypeVar("F", bound=Callable[..., Any])


class gRPClette:
    def __init__(
        self,
        *,
        service: str = "Service",
        package: str = "grpclette",
        middleware: Sequence[Middleware] = (),
        lifespan: Lifespan[Starlette] | None = None,
        max_message_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", service):
            raise ValueError("Service names must be protobuf identifiers.")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*", package):
            raise ValueError("Package names must be dot-separated protobuf identifiers.")
        if max_message_bytes <= 0:
            raise ValueError("max_message_bytes must be positive.")
        self.service = service
        self.package = package
        self.max_message_bytes = max_message_bytes
        self.starlette = Starlette(middleware=middleware, lifespan=lifespan)
        self.schema = Schema(package)
        self._methods: dict[str, RPCMethod] = {}
        self._started = False

    def rpc(self, handler: F) -> F:
        if self._started:
            raise RuntimeError("Register RPC handlers before the application starts.")
        name = "".join(part[:1].upper() + part[1:] for part in handler.__name__.split("_"))
        if name in self._methods:
            raise ValueError(f"RPC {name!r} is already registered.")
        frame = inspect.currentframe()
        namespace = {} if frame is None or frame.f_back is None else frame.f_back.f_locals.copy()
        del frame
        schema = self.schema.clone()
        method = RPCMethod.create(handler, schema, namespace)
        self.schema = schema
        self._methods[name] = method
        return handler

    def proto(self) -> str:
        return self.schema.proto([method.definition for method in self._methods.values()], self.service)

    def descriptor(self) -> FileDescriptorProto:
        return self.schema.descriptor([method.definition for method in self._methods.values()], self.service)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if not self._started:
            service_name = f"{self.package}.{self.service}"
            endpoints = {
                f"/{service_name}/{name}": method.endpoint(service_name) for name, method in self._methods.items()
            }
            self.starlette.routes.append(Mount("/", app=RPCTransport(endpoints, self.max_message_bytes)))
            self._started = True
        token = RPC_STATE.set(scope.get("state", {}))
        try:
            await self.starlette(scope, receive, send)
        finally:
            RPC_STATE.reset(token)
