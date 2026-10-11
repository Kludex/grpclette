from __future__ import annotations

import importlib
import importlib.resources
import socket
import sys
from collections.abc import AsyncIterator, Iterator
from typing import Any

import anyio
import grpc.aio
import pytest
import uvicorn
from grpc_tools import protoc  # type: ignore[import-untyped]
from starlette.types import ASGIApp

from grpclette import gRPClette
from tests.service import create_app


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def events() -> list[str]:
    return []


@pytest.fixture
def cancelled() -> anyio.Event:
    return anyio.Event()


@pytest.fixture
def app(events: list[str], cancelled: anyio.Event) -> gRPClette:
    return create_app(events, cancelled)


@pytest.fixture(scope="session")
def generated(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[Any, Any]]:
    path = tmp_path_factory.mktemp("grpc_client")
    schema = create_app([], anyio.Event()).proto()
    (path / "integration.proto").write_text(schema)
    includes = importlib.resources.files("grpc_tools").joinpath("_proto")
    assert (
        protoc.main(
            [
                "protoc",
                f"-I{path}",
                f"-I{includes}",
                f"--python_out={path}",
                f"--grpc_python_out={path}",
                str(path / "integration.proto"),
            ]
        )
        == 0
    )
    sys.path.insert(0, str(path))
    try:
        messages = importlib.import_module("integration_pb2")
        services = importlib.import_module("integration_pb2_grpc")
        yield messages, services
    finally:
        sys.path.remove(str(path))
        del sys.modules["integration_pb2"]
        del sys.modules["integration_pb2_grpc"]


@pytest.fixture
def application(app: gRPClette) -> ASGIApp:
    return app


@pytest.fixture
async def client(application: ASGIApp, generated: tuple[Any, Any]) -> AsyncIterator[tuple[Any, Any]]:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        server = uvicorn.Server(
            uvicorn.Config(
                application,
                http="zttp",
                http2=True,
                lifespan="on",
                log_config=None,
                log_level="error",
                timeout_graceful_shutdown=1,
            )
        )
        stopped = anyio.Event()

        async def run() -> None:
            try:
                await server.serve(sockets=[listener])
            finally:
                stopped.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(run)
            try:
                with anyio.fail_after(10):
                    while not server.started:
                        await anyio.sleep(0.001)
                async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                    yield generated[0], generated[1].TestServiceStub(channel)
            finally:
                server.should_exit = True
                with anyio.CancelScope(shield=True):  # pragma: no branch - Suppression would require aborting pytest.
                    await stopped.wait()
