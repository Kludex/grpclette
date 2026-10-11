from __future__ import annotations

from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.types import ASGIApp

from grpclette import gRPClette
from tests.service import lifespan


@pytest.fixture
def application(app: gRPClette) -> ASGIApp:
    return Starlette(routes=[Mount("/testing.TestService", app=app)], lifespan=lifespan)


@pytest.mark.anyio
async def test_service_mounted_under_its_grpc_path(client: tuple[Any, Any]) -> None:
    messages, stub = client
    assert (await stub.Echo(messages.EchoInput(text="mounted"))).text == "mounted"
    reply = await stub.Context(messages.EchoInput(text="mounted"), metadata=(("authorization", "Bearer test"),))
    assert reply.text == "hello:mounted"
