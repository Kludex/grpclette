from __future__ import annotations

import json
from dataclasses import dataclass

from google.protobuf import descriptor_pb2, descriptor_pool
from google.protobuf.descriptor import FileDescriptor
from google.protobuf.message import Message


@dataclass(frozen=True)
class MethodDefinition:
    name: str
    input: type[Message]
    output: type[Message]
    client_streaming: bool
    server_streaming: bool


def _render_file(file: descriptor_pb2.FileDescriptorProto) -> str:
    lines = ['syntax = "proto3";', "", f"package {file.package};", ""]
    lines.extend(f"import {json.dumps(name)};" for name in file.dependency)
    if file.dependency:
        lines.append("")
    scalars = {
        descriptor_pb2.FieldDescriptorProto.TYPE_STRING: "string",
        descriptor_pb2.FieldDescriptorProto.TYPE_BOOL: "bool",
        descriptor_pb2.FieldDescriptorProto.TYPE_BYTES: "bytes",
        descriptor_pb2.FieldDescriptorProto.TYPE_INT64: "int64",
        descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE: "double",
    }
    for message in file.message_type:
        lines.append(f"message {message.name} {{")
        entries = {f".{file.package}.{message.name}.{nested.name}": nested for nested in message.nested_type}
        for field in message.field:
            entry = entries.get(field.type_name)
            if entry is not None and entry.options.map_entry:
                key, value = entry.field
                field_type = f"map<{scalars[key.type]}, {scalars.get(value.type, value.type_name)}>"
                label = ""
            else:
                field_type = scalars.get(field.type, field.type_name)
                if field.proto3_optional:
                    label = "optional "
                elif field.label == descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED:
                    label = "repeated "
                else:
                    label = ""
            lines.append(f"  {label}{field_type} {field.name} = {field.number};")
        lines.extend(["}", ""])
    for service in file.service:
        lines.append(f"service {service.name} {{")
        for method in service.method:
            request = ("stream " if method.client_streaming else "") + method.input_type
            response = ("stream " if method.server_streaming else "") + method.output_type
            lines.append(f"  rpc {method.name} ({request}) returns ({response});")
        lines.extend(["}", ""])
    return "\n".join(lines)


def _import_file(files: dict[str, FileDescriptor], file: FileDescriptor) -> None:
    if file.name in files and files[file.name].serialized_pb != file.serialized_pb:
        raise ValueError(f"Conflicting protobuf import: {file.name}")
    files[file.name] = file


def _add_dependency(pool: descriptor_pool.DescriptorPool, file: FileDescriptor) -> None:
    try:
        existing = pool.FindFileByName(file.name)
    except KeyError:
        for dependency in file.dependencies:
            _add_dependency(pool, dependency)
        pool.AddSerializedFile(file.serialized_pb)
    else:
        if existing.serialized_pb != file.serialized_pb:
            raise ValueError(f"Conflicting protobuf import: {file.name}")
