from __future__ import annotations

import types
from collections.abc import Mapping
from dataclasses import asdict, dataclass, is_dataclass
from typing import Annotated, Any, NotRequired, Required, Union, cast, get_args, get_origin, get_type_hints

from google.protobuf.message import DecodeError, Message
from pydantic import BaseModel, ConfigDict, TypeAdapter
from typing_extensions import is_typeddict


@dataclass(frozen=True)
class MessageAdapter:
    annotation: Any
    message: type[Message]
    adapter: TypeAdapter[Any] | None
    namespace: dict[str, Any]

    @classmethod
    def create(cls, annotation: Any, message: type[Message], namespace: dict[str, Any] | None = None) -> MessageAdapter:
        namespace = namespace or {}
        if isinstance(annotation, type) and issubclass(annotation, Message):
            return cls(annotation, message, None, namespace)
        if is_dataclass(annotation) or is_typeddict(annotation):
            # Pydantic permits adapter config around nullable records, not on the record itself.
            adapter = TypeAdapter(annotation | None, config=ConfigDict(arbitrary_types_allowed=True))
        else:
            adapter = TypeAdapter(annotation)
        adapter.rebuild(force=True, _types_namespace=namespace)
        return cls(annotation, message, adapter, namespace)

    def decode(self, message: Message) -> Any:
        if not message.IsInitialized():
            raise DecodeError("The protobuf request is missing required fields.")
        if self.annotation is type(None):
            return None
        if self.adapter is None:
            return self.annotation.FromString(message.SerializeToString())
        return self.adapter.validate_python(message_values(message, self.annotation, self.namespace), by_name=True)

    def encode(self, value: Any) -> Message:
        if isinstance(value, Message):
            if value.DESCRIPTOR.full_name != self.message.DESCRIPTOR.full_name:
                raise ValueError(f"Expected protobuf message {self.message.DESCRIPTOR.full_name}.")
            if self.adapter is None:
                return self.message.FromString(value.SerializeToString())
            value = message_values(value, self.annotation, self.namespace)
        if value is None and self.annotation is not type(None):
            raise ValueError("A nonempty response annotation cannot return None.")
        if self.adapter is not None:
            value = self.adapter.validate_python(value, by_name=True)
        payload = {} if value is None else model_values(value)
        if not isinstance(payload, dict):
            raise ValueError("RPC responses must be a model, mapping, dataclass, or protobuf message.")
        return self.message(**payload)


def message_values(message: Message, annotation: Any = None, namespace: dict[str, Any] | None = None) -> dict[str, Any]:
    hints = get_type_hints(annotation, localns=namespace, include_extras=True) if annotation is not None else {}
    values: dict[str, Any] = {}
    present = {field.name: value for field, value in message.ListFields()}
    for field in message.DESCRIPTOR.fields:
        value = present.get(field.name, field.default_value)
        target = unwrap(hints.get(field.name))
        args = get_args(target)
        if field.is_repeated:
            if field.message_type is not None and field.message_type.GetOptions().map_entry:
                values[field.name] = {
                    key: field_value(item, args[1] if args else None, namespace)
                    for key, item in cast(Mapping[Any, Any], present.get(field.name, {})).items()
                }
            else:
                values[field.name] = [
                    field_value(item, args[0] if args else None, namespace) for item in present.get(field.name, [])
                ]
        elif field.has_presence and field.name not in present:
            values[field.name] = None
        else:
            values[field.name] = field_value(value, target, namespace)
    return values


def field_value(value: Any, annotation: Any, namespace: dict[str, Any] | None) -> Any:
    annotation = unwrap(annotation)
    if isinstance(value, Message):
        if isinstance(annotation, type) and issubclass(annotation, Message):
            return annotation.FromString(value.SerializeToString())
        return message_values(value, annotation, namespace)
    return value


def unwrap(annotation: Any) -> Any:
    while get_origin(annotation) in (Annotated, Required, NotRequired, Union, types.UnionType):
        if get_origin(annotation) in (Union, types.UnionType):
            annotation = next(arg for arg in get_args(annotation) if arg is not type(None))
        else:
            annotation = get_args(annotation)[0]
    return annotation


def model_values(value: Any) -> Any:
    if isinstance(value, Message):
        value = message_values(value)
    elif isinstance(value, BaseModel):
        value = value.model_dump(mode="python", by_alias=False)
    elif is_dataclass(value) and not isinstance(value, type):
        value = asdict(value)
    if isinstance(value, Mapping):
        return {key: model_values(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [model_values(item) for item in value]
    return value
