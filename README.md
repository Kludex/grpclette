# gRPClette

gRPClette is a framework for building gRPC services in Python, based on standard Python type hints.

You write async functions. gRPClette generates the `.proto` schema for you. ✨

The key features are:

- **Code first**: Your Python types are the schema. No handwritten `.proto` files, no generated server code.
- **Standard clients**: Export the schema and generate clients with the usual gRPC tools, in any language.
- **Typed**: Use Pydantic models, dataclasses, `TypedDict`, or generated protobuf messages.
- **Streaming**: Unary, server streaming, client streaming, and bidirectional RPCs, all with `async` functions.
- **Dependencies**: `Depends`, lifespan state, and per-RPC context. If you know FastAPI, you already know them.
- **ASGI**: Built on Starlette. You can mount it and wrap it with pure ASGI middleware.

> **Under development**
> gRPClette serves native gRPC over HTTP/2. It does not serve gRPC-Web or the Connect protocol. The HTTP/2 support in Uvicorn is still experimental, so evaluate it before deploying.

## Installation

```bash
pip install "grpclette[server]"
```

The `server` extra installs Uvicorn with HTTP/2 support. You need Python 3.11 or newer.

## Example

### Create it

Create a file `main.py` with:

```python
from pydantic import BaseModel

from grpclette import gRPClette

app = gRPClette(service="Greeter", package="example")


class HelloRequest(BaseModel):
    name: str


class HelloReply(BaseModel):
    message: str


@app.rpc
async def say_hello(request: HelloRequest) -> HelloReply:
    return HelloReply(message=f"Hello, {request.name}!")
```

### Run it

```bash
grpclette run main:app
```

That's it. 🚀 You have a gRPC service at `127.0.0.1:50051` with a `SayHello` method.

### Check it

Export the schema:

```bash
grpclette proto main:app --output greeter.proto
```

```proto
syntax = "proto3";

package example;

message HelloReply {
  string message = 1;
}

message HelloRequest {
  string name = 1;
}

service Greeter {
  rpc SayHello (.example.HelloRequest) returns (.example.HelloReply);
}
```

Generate a standard Python client from it:

```bash
pip install grpcio-tools
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. greeter.proto
```

Create a file `client.py` with:

```python
import grpc

import greeter_pb2
import greeter_pb2_grpc

with grpc.insecure_channel("127.0.0.1:50051") as channel:
    stub = greeter_pb2_grpc.GreeterStub(channel)
    reply = stub.SayHello(greeter_pb2.HelloRequest(name="Ada"))
    print(reply.message)
```

Run it while the server is running:

```bash
$ python client.py
Hello, Ada!
```

The client doesn't need gRPClette. It's plain gRPC. 😎

## Example upgrade

Now add a server streaming RPC. Use an async generator and annotate the return as `AsyncIterator`:

```python
from collections.abc import AsyncIterator
from typing import Annotated

from pydantic import BaseModel

from grpclette import Field, gRPClette

app = gRPClette(service="Greeter", package="example")


class HelloRequest(BaseModel):
    name: Annotated[str, Field(number=1)]


class HelloReply(BaseModel):
    message: Annotated[str, Field(number=1)]


@app.rpc
async def say_hello(request: HelloRequest) -> HelloReply:
    return HelloReply(message=f"Hello, {request.name}!")


@app.rpc
async def watch_greetings(request: HelloRequest) -> AsyncIterator[HelloReply]:
    for count in range(3):
        yield HelloReply(message=f"Hello, {request.name}! ({count + 1})")
```

`watch_greetings` becomes `WatchGreetings` in the schema. For client streaming, annotate the request as `AsyncIterator[HelloRequest]` and read it with `async for`.

`Field(number=N)` pins the protobuf field number. Clients read values by that number, not by name.

> **🚨 Pin field numbers before you ship**
> Without `Field`, gRPClette numbers fields in declaration order. Reordering or removing a field then changes the numbers, and old clients silently read the wrong values. Pin them, and never reuse a removed number.

## Recap

You declare the types of your requests and responses once, with standard Python types:

```python
class HelloRequest(BaseModel):
    name: str
```

And you get:

- Validation of every request, with `INVALID_ARGUMENT` errors for bad input.
- Conversion between protobuf messages and your Python types.
- A `.proto` schema, to generate clients in any language.

Check the [examples](examples) for all four RPC shapes. 🎉

## License

This project is licensed under the terms of the MIT license.
