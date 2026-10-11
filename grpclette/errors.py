from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from connectrpc.code import Code
from connectrpc.errors import ConnectError
from pydantic import ValidationError


@asynccontextmanager
async def handler_errors() -> AsyncIterator[None]:
    try:
        yield
    except ConnectError:
        raise
    except ValidationError as exc:
        raise ConnectError(Code.INTERNAL, "The handler returned an invalid response.") from exc
    except Exception as exc:
        raise ConnectError(Code.INTERNAL, "Internal server error.") from exc
