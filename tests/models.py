from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated

from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import BaseModel, ConfigDict, Field as PydanticField
from typing_extensions import TypedDict

from grpclette import Field


class EchoInput(BaseModel):
    text: Annotated[str, Field(1)] = PydanticField(min_length=1)


class AliasedEcho(BaseModel):
    text: Annotated[str, Field(1)] = PydanticField(alias="content")


class EchoResult(TypedDict):
    text: Annotated[str, Field(1)]


@dataclass(frozen=True)
class Summary:
    count: Annotated[int, Field(1)]


@dataclass(frozen=True)
class Blob:
    data: Annotated[bytes, Field(1)]
    number: Annotated[int, Field(2)]
    optional: Annotated[int | None, Field(3)] = None
    labels: Annotated[list[str], Field(4)] = field(default_factory=list)


@dataclass(frozen=True)
class Envelope:
    child: Blob
    children: list[Blob]
    lookup: dict[str, Blob]


class ProtobufEnvelope(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    stamp: Annotated[Timestamp, Field(1)]
    stamps: Annotated[list[Timestamp], Field(2)]
    lookup: Annotated[dict[str, Timestamp], Field(3)]
    optional: Annotated[Timestamp | None, Field(4)] = None


@dataclass(frozen=True)
class ProtobufRecord:
    stamp: Annotated[Timestamp, Field(1)]


class ProtobufMapping(TypedDict):
    stamp: Annotated[Timestamp, Field(1)]


class LifespanState(TypedDict):
    prefix: str
