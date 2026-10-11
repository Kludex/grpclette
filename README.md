# gRPClette

gRPClette is a code-first Python gRPC framework built on Starlette and the Connect for Python engine. You write typed async handlers. gRPClette generates the protobuf schema and adapts protobuf messages to your Python models.

This project is under development. It serves native gRPC over HTTP/2, not HTTP/1.1, gRPC-Web, or the Connect protocol.

## Installation

You need Python 3.11 or newer. From a checkout, install the optional server packages to run the examples:

```bash
uv sync --extra server
```

The `server` extra installs Uvicorn and `zttp`, its HTTP/2 implementation. Schema export does not need either package. For a core-only environment, omit development dependencies when you sync and run:

```bash
uv sync --no-dev
uv run --no-dev grpclette proto examples.greeter:app
```

> **Experimental HTTP/2 server**
> Native HTTP/2 support in Uvicorn 0.54 is experimental. The CLI selects `http="zttp"` and `http2=True`. A default HTTP/1.1 Uvicorn configuration cannot serve native gRPC. Evaluate the server stack before deploying it.

## Unary service

Save this complete service as `hello.py`. A unary RPC accepts one message and returns one message.

```python
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
```

You use `@app.rpc` without arguments. gRPClette converts `say_hello` to `SayHello`, so the gRPC method path is `/example.Greeter/SayHello`. The request annotation defines the input message. The return annotation defines the output message, even when you return a mapping.

Start the service from the directory containing `hello.py`:

```bash
uv run grpclette run hello:app --host 127.0.0.1 --port 50051
```

The CLI requires a `gRPClette` instance at `module:app`. It defaults to `127.0.0.1:50051`. You can also run the checked-in equivalent with `uv run grpclette run examples.greeter:app`.

## Export schemas and generate clients

Export the schema before generating standard gRPC client stubs. Clients do not need gRPClette.

```bash
uv run grpclette proto hello:app --output greeter.proto
uv run --with grpcio-tools python -m grpc_tools.protoc \
  -I . --python_out=. --grpc_python_out=. greeter.proto
```

This creates `greeter_pb2.py` for messages and `greeter_pb2_grpc.py` for the client stub. Without `--output`, `grpclette proto hello:app` writes the schema to stdout. If your schema imports another `.proto` file, make that file available on the compiler's include path.

Save this client as `client.py` and keep the server running in another terminal:

```python
import sys

import grpc

import greeter_pb2
import greeter_pb2_grpc

with grpc.insecure_channel("127.0.0.1:50051") as channel:
    stub = greeter_pb2_grpc.GreeterStub(channel)
    response = stub.SayHello(greeter_pb2.HelloRequest(name="Ada"), timeout=5)
    sys.stdout.write(response.message + "\n")
```

Run the client with the standard Python gRPC package:

```bash
uv run --with grpcio python client.py
```

The client prints `Hello, Ada!`. The insecure channel is appropriate for this local example. These CLI options do not configure TLS.

You can also access the schema through the public Python API:

```python
from hello import app

schema: str = app.proto()
descriptor = app.descriptor()
assert descriptor.package == "example"
assert "SayHello" in schema
```

`app.descriptor()` returns a protobuf `FileDescriptorProto`. Both exports describe the registered handlers. Register your handlers before the application starts.

## Streaming RPCs

The complete [streaming example](examples/streaming.py) defines all four RPC shapes. An `AsyncIterator[T]` annotation marks a stream of messages of type `T`.

```bash
uv run grpclette proto examples.streaming:app --output streaming.proto
uv run --with grpcio-tools python -m grpc_tools.protoc \
  -I . --python_out=. --grpc_python_out=. streaming.proto
uv run grpclette run examples.streaming:app --port 50052
```

| RPC | Request annotation | Return annotation | Handler |
| --- | --- | --- | --- |
| `SayHello` | `Greeting` | `Reply` | Async function |
| `WatchGreetings` | `Greeting` | `AsyncIterator[Reply]` | Async generator |
| `CountGreetings` | `AsyncIterator[Greeting]` | `Summary` | Async function |
| `Chat` | `AsyncIterator[Greeting]` | `AsyncIterator[Reply]` | Async generator |

Save this client as `streaming_client.py`:

```python
import grpc

import streaming_pb2 as messages
import streaming_pb2_grpc as services

with grpc.insecure_channel("127.0.0.1:50052") as channel:
    stub = services.StreamingStub(channel)
    request = messages.Greeting(name="Ada")
    assert stub.SayHello(request, timeout=5).message == "Hello, Ada!"
    assert len(list(stub.WatchGreetings(request, timeout=5))) == 3
    assert stub.CountGreetings(iter([request, request]), timeout=5).count == 2
    assert len(list(stub.Chat(iter([request, request]), timeout=5))) == 2
```

Run it with `uv run --with grpcio python streaming_client.py`. Server-streaming and bidirectional handlers yield responses instead of returning a list. Client-streaming handlers consume requests with `async for`. Dependencies remain active until the RPC finishes, including streaming responses.

## Message types

You can use Pydantic `BaseModel`, `typing_extensions.TypedDict`, standard dataclasses, or generated protobuf message classes for request and response annotations. Save this dataclass service as `blobs.py`:

```python
from dataclasses import dataclass
from typing import Annotated

from grpclette import Field, gRPClette

app = gRPClette(service="Blobs", package="example")


@dataclass(frozen=True)
class Blob:
    data: Annotated[bytes, Field(number=1)]
    sequence: Annotated[int, Field(number=2)]
    label: Annotated[str | None, Field(number=3)] = None


@app.rpc
async def echo(request: Blob) -> Blob:
    return request
```

You can inspect it with `uv run grpclette proto blobs:app`. Typed model and mapping responses are validated against the declared response type. Generated protobuf message annotations keep their existing protobuf descriptors and field numbers.

| Python field | Protobuf field |
| --- | --- |
| `str` | `string` |
| `bool` | `bool` |
| `int` | `int64` |
| `float` | `double` |
| `bytes` | `bytes` |
| Supported model | Message |
| `list[T]` | `repeated T` |
| `dict[K, V]` | `map<K, V>` |
| `T \| None` | Scalar presence or message presence |

Python integers must fit the signed 64-bit protobuf range. Binary fields stay `bytes`; you do not base64-encode them yourself. Map keys must be `str`, `bool`, or `int`. Optional containers, nested containers, arbitrary unions, Python enum fields, and `Self` annotations are not supported by the model-first schema. Use the concrete model name for recursive fields.

Omitted non-optional scalar fields use protobuf defaults, such as `0` or `""`, not a custom Python default. Use an optional scalar when you need to distinguish an omitted field from its zero value. A handler with no request parameter or a `None` return uses `google.protobuf.Empty` on that side of the RPC.

## Schema compatibility

Field numbers identify values on the protobuf wire. The examples use `Annotated[T, Field(number=N)]` to keep those numbers stable when you edit a model.

> **Keep automatically numbered fields append-only**
> Without `Field`, gRPClette assigns tags in declaration order. Do not remove, insert between, or reorder existing fields. Append new fields instead. Removing or reordering fields can silently make old clients read the wrong values.

Explicit tags let you reorder declarations without changing the wire schema. Never reuse a removed field's number for a different meaning. Keep a record of retired numbers and compare exported schemas before releasing a change. Model-first export does not manage protobuf `reserved` declarations for you. Changing a field type, message name, service, package, or RPC name also requires a compatibility review.

## Context, dependencies, and lifespan

You can inject `RPCContext` and async dependencies without adding fields to the request message. Save this service as `context_service.py`:

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, cast

from pydantic import BaseModel
from starlette.applications import Starlette
from typing_extensions import TypedDict

from grpclette import Code, Depends, Field, RPCContext, RPCError, gRPClette


class HelloRequest(BaseModel):
    name: Annotated[str, Field(number=1)]


class HelloReply(TypedDict):
    message: Annotated[str, Field(number=1)]


class State(TypedDict):
    prefix: str


@asynccontextmanager
async def lifespan(application: Starlette) -> AsyncIterator[State]:
    yield {"prefix": "Hello"}


async def greeting_prefix(request: HelloRequest, context: RPCContext) -> str:
    if not request.name.strip():
        raise RPCError(Code.INVALID_ARGUMENT, "A non-blank name is required.")
    return cast(State, context.state)["prefix"]


app = gRPClette(service="Greeter", package="example", lifespan=lifespan)


@app.rpc
async def say_hello(
    request: HelloRequest,
    context: RPCContext,
    prefix: Annotated[str, Depends(greeting_prefix)],
) -> HelloReply:
    context.response_headers["x-service"] = "greeter"
    context.response_trailers["x-finished"] = "true"
    return {"message": f"{prefix}, {request.name}!"}
```

Run it with `uv run grpclette run context_service:app`. Lifespan yields state for each RPC through `context.state`. Use it for shared resources instead of `app.state`.

`RPCContext` provides `request_headers`, `response_headers`, `response_trailers`, `timeout_ms`, `client_address`, `state`, and `method`. Metadata uses string-valued wire headers. Binary `-bin` metadata is base64-encoded, not decoded to `bytes`. Use `Headers.add()` and `Headers.getall()` for repeated metadata. `timeout_ms` is the remaining client timeout, or `None` when no deadline was sent. `method` describes the RPC, including its name and service.

`Depends` is a frozen dataclass. You can place it in `Annotated` or use it as a parameter default. Dependencies must be async functions or async generators. Repeated use of the same dependency is cached within one RPC. An async generator yields its value once and releases resources in `finally` when the RPC ends or is cancelled.

`RPCError` is an alias for Connect's `ConnectError`. Raise it with `Code` and a safe client-facing message. Request validation errors become `INVALID_ARGUMENT`. Unexpected handler errors and invalid responses become `INTERNAL` without exposing exception details.

## ASGI integration and limits

`gRPClette` is an ASGI application. ASGI is the interface between Python web applications and servers. Its constructor accepts `service`, `package`, `middleware`, `lifespan`, and `max_message_bytes`. The incoming message limit defaults to 4,194,304 bytes to bound memory use. The Connect engine requires an asyncio backend; Trio is not supported.

> **Use pure ASGI middleware**
> Pass Starlette `Middleware` instances wrapping pure ASGI middleware. `BaseHTTPMiddleware` is incompatible with the gRPC response trailers required by this framework. Your server must support HTTP/2 and the ASGI `http.response.trailers` extension.

> **Avoid legacy gzip middleware on gRPC paths**
> Published Starlette 1.7.0 does not forward response trailers through `GZipMiddleware`. The change is merged in [#3565](https://github.com/Kludex/starlette/pull/3565), but you need a release containing it before wrapping gRPC paths with that middleware. gRPClette already negotiates gRPC per-message compression through its protocol engine.

> **Compose mounted lifespans explicitly**
> Starlette does not automatically run the lifespan of a mounted application. If you mount a lifespan-dependent gRPClette service, compose its lifecycle with the parent and yield its state from the parent lifespan.

> **Bound handlers that stop consuming streamed input**
> The receive relay preserves backpressure and does not buffer an entire request stream. A disconnect can remain behind an unread body chunk until your handler resumes consumption. A supplied deadline is monitored independently. Without a deadline, a handler that stops reading must finish or enforce its own bound; prompt disconnect detection is not guaranteed in this case.

The Connect engine handles gRPC framing and serialization. Using that engine does not enable the Connect protocol or gRPC-Web endpoints in gRPClette.
