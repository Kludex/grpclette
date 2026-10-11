from __future__ import annotations

import inspect
from collections.abc import Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Any, get_args, get_origin, get_type_hints

from grpclette.context import RPCContext


@dataclass(frozen=True)
class Depends:
    dependency: Callable[..., Any]


def validate_parameters(
    handler: Callable[..., Any],
    request_type: Any,
    namespace: dict[str, Any],
    seen: tuple[Callable[..., Any], ...] = (),
) -> None:
    if handler in seen:
        raise ValueError("RPC dependencies contain a cycle.")
    hints = get_type_hints(handler, localns=namespace, include_extras=True)
    for name, parameter in inspect.signature(handler).parameters.items():
        if parameter.kind in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD, parameter.POSITIONAL_ONLY):
            raise ValueError("RPC handlers and dependencies require named parameters.")
        annotation = hints.get(name)
        dependency = dependency_marker(annotation, parameter.default)
        if dependency is not None:
            if not (inspect.iscoroutinefunction(dependency) or inspect.isasyncgenfunction(dependency)):
                raise ValueError("RPC dependencies must be async functions or async generators.")
            validate_parameters(dependency, request_type, namespace, (*seen, handler))
        elif (
            parameter_type(annotation) != parameter_type(request_type) and parameter_type(annotation) is not RPCContext
        ):
            raise ValueError(f"Parameter {name!r} must be the request, RPCContext, or an async dependency.")


async def arguments(
    handler: Callable[..., Any],
    request: Any,
    context: RPCContext,
    stack: AsyncExitStack,
    cache: dict[Callable[..., Any], Any],
    namespace: dict[str, Any],
) -> dict[str, Any]:
    hints = get_type_hints(handler, localns=namespace, include_extras=True)
    values: dict[str, Any] = {}
    for name, parameter in inspect.signature(handler).parameters.items():
        annotation = hints.get(name)
        dependency = dependency_marker(annotation, parameter.default)
        if dependency is not None:
            if dependency not in cache:
                kwargs = await arguments(dependency, request, context, stack, cache, namespace)
                if inspect.isasyncgenfunction(dependency):
                    cache[dependency] = await stack.enter_async_context(asynccontextmanager(dependency)(**kwargs))
                else:
                    cache[dependency] = await dependency(**kwargs)
            values[name] = cache[dependency]
        elif parameter_type(annotation) is RPCContext:
            values[name] = context
        else:
            values[name] = request
    return values


def parameter_type(annotation: Any) -> Any:
    return get_args(annotation)[0] if get_origin(annotation) is Annotated else annotation


def dependency_marker(annotation: Any, default: Any) -> Callable[..., Any] | None:
    if isinstance(default, Depends):
        return default.dependency
    if get_origin(annotation) is Annotated:
        for marker in get_args(annotation)[1:]:
            if isinstance(marker, Depends):
                return marker.dependency
    return None
