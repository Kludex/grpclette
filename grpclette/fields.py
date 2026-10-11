from __future__ import annotations

import re
import types
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any, NotRequired, Required, Union, get_args, get_origin

from google.protobuf import descriptor_pb2


@dataclass(frozen=True)
class Field:
    number: int

    def __post_init__(self) -> None:
        if type(self.number) is not int or not 1 <= self.number <= 536870911 or 19000 <= self.number <= 19999:
            raise ValueError("Field numbers must be integers in 1..536870911, excluding 19000..19999")


_SCALARS = {
    str: descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    bool: descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
    bytes: descriptor_pb2.FieldDescriptorProto.TYPE_BYTES,
    int: descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
    float: descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
}


def _identifier(name: str) -> None:
    if not isinstance(name, str) or re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name) is None:
        raise ValueError(f"Invalid protobuf identifier: {name!r}")


def _unwrap(annotation: Any) -> tuple[Any, int | None, bool]:
    number = None
    optional = False
    while True:
        origin = get_origin(annotation)
        args = get_args(annotation)
        if origin is Annotated:
            for metadata in args[1:]:
                if isinstance(metadata, Field):
                    if number is not None:
                        raise ValueError("A field can only have one Field number")
                    number = metadata.number
            annotation = args[0]
        elif origin in (Required, NotRequired):
            annotation = args[0]
        elif origin in (Union, types.UnionType):
            if len(args) != 2 or type(None) not in args or optional:
                raise TypeError(f"Only optional unions are supported: {annotation!r}")
            optional = True
            annotation = next(arg for arg in args if arg is not type(None))
        else:
            return annotation, number, optional


def _populate(
    message: descriptor_pb2.DescriptorProto,
    name: str,
    annotation: Any,
    number: int,
    resolve: Callable[[Any], str],
    names: set[str],
    scope: str,
) -> None:
    annotation, _, optional = _unwrap(annotation)
    field = message.field.add(name=name, number=number)
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in (list, dict):
        if optional:
            raise TypeError("Optional lists and maps are not supported by protobuf")
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
        if origin is list:
            if len(args) != 1:
                raise TypeError("Lists require an element annotation")
            annotation, tag, nullable = _unwrap(args[0])
            if tag is not None or nullable or get_origin(annotation) in (list, dict):
                raise TypeError("List elements cannot be optional, tagged, or containers")
        else:
            if len(args) != 2 or args[0] not in (str, bool, int):
                raise TypeError("Maps require str, bool, or int keys and a value annotation")
            value, tag, nullable = _unwrap(args[1])
            if tag is not None or nullable or get_origin(value) in (list, dict):
                raise TypeError("Map values cannot be optional, tagged, or containers")
            entry_name = "".join(part[:1].upper() + part[1:] for part in name.split("_")) + "Entry"
            if entry_name in names:
                raise ValueError(f"Duplicate map entry name: {entry_name}")
            names.add(entry_name)
            entry = message.nested_type.add(name=entry_name)
            entry.options.map_entry = True
            entry_scope = f"{scope}.{entry_name}"
            _populate(entry, "key", args[0], 1, resolve, {"key", "value"}, entry_scope)
            _populate(entry, "value", value, 2, resolve, {"key", "value"}, entry_scope)
            field.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
            field.type_name = entry_scope
            return
    else:
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    if isinstance(annotation, type) and annotation in _SCALARS:
        field.type = _SCALARS[annotation]
        if optional:
            oneof_name = f"_{name}"
            while oneof_name in names:
                oneof_name = "_" + oneof_name
            names.add(oneof_name)
            field.oneof_index = len(message.oneof_decl)
            message.oneof_decl.add(name=oneof_name)
            field.proto3_optional = True
    else:
        field.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
        field.type_name = resolve(annotation)
