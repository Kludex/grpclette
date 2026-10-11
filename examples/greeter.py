from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, Field as PydanticField
from typing_extensions import TypedDict

from grpclette import Field, gRPClette

app = gRPClette(service="Greeter", package="example")


class HelloRequest(BaseModel):
    name: Annotated[str, Field(number=1)] = PydanticField(min_length=1)


class HelloReply(TypedDict):
    message: Annotated[str, Field(number=1)]


@app.rpc
async def say_hello(request: HelloRequest) -> HelloReply:
    return {"message": f"Hello, {request.name}!"}
