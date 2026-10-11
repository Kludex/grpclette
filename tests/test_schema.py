from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass, make_dataclass
from pathlib import Path
from typing import Annotated, Any, NotRequired, Required, cast

import pytest
from google.protobuf import api_pb2, descriptor_pb2, descriptor_pool, json_format, message_factory, timestamp_pb2
from google.protobuf.message import Message
from grpc_tools import protoc  # type: ignore[import-untyped]
from pydantic import BaseModel
from typing_extensions import TypedDict

from grpclette.fields import Field
from grpclette.schema import MethodDefinition, Schema


@dataclass
class Child:
    name: Annotated[str, Field(7)]


class Scalars(BaseModel):
    text: str
    flag: bool
    data: bytes
    integer: int
    decimal: float
    present: int | None
    optional_text: str | None
    optional_flag: bool | None
    optional_data: bytes | None
    optional_decimal: Annotated[float | None, Field(30)]
    _private: int = 0


class Envelope(TypedDict):
    child: Required[Child]
    note: NotRequired[str]
    nullable: NotRequired[int | None]


@dataclass
class Payload:
    scalars: Scalars
    children: list[Child]
    labels: list[str]
    numbers: list[int]
    by_name: dict[str, Child]
    toggles: dict[bool, int]
    measures: dict[int, float]
    child: Child | None
    timestamp: timestamp_pb2.Timestamp
    api: api_pb2.Api
    envelope: Envelope


@dataclass
class Recursive:
    child: Recursive | None
    children: list[Recursive]
    by_name: dict[str, Recursive]


def test_roundtrip_models_presence_maps_and_imports() -> None:
    schema = Schema("example.v1")
    child_type = schema.message(Child)
    scalars_type = schema.message(Scalars)
    payload_type = schema.message(Payload)
    assert schema.message(Payload) is payload_type
    assert schema.message(Child) is child_type
    assert schema.message(timestamp_pb2.Timestamp) is timestamp_pb2.Timestamp
    scalars = scalars_type()
    assert not scalars.HasField("present")
    json_format.ParseDict({"present": "0", "optionalFlag": False, "optionalText": "", "optionalData": ""}, scalars)
    assert scalars.HasField("present")
    assert scalars.HasField("optional_flag")
    assert scalars.HasField("optional_text")
    assert scalars.HasField("optional_data")
    scalars.ClearField("present")
    assert not scalars.HasField("present")
    payload = payload_type()
    assert not payload.HasField("child")
    data: dict[str, Any] = {
        "scalars": {
            "text": "hello",
            "flag": True,
            "data": "YWJj",
            "integer": "9223372036854775807",
            "decimal": 1.25,
            "present": "0",
            "optionalDecimal": 0.0,
        },
        "children": [{"name": "one"}],
        "labels": ["a", "b"],
        "numbers": ["1", "-2"],
        "byName": {"first": {"name": "nested"}},
        "toggles": {"true": "3", "false": "4"},
        "measures": {"1": 2.5},
        "child": {"name": "optional"},
        "timestamp": "2026-01-01T00:00:00Z",
        "api": {"name": "api", "sourceContext": {"fileName": "example.proto"}},
        "envelope": {"child": {"name": "typed"}, "note": "ok", "nullable": "0"},
    }
    json_format.ParseDict(data, payload)
    restored = payload_type.FromString(payload.SerializeToString())
    assert json_format.MessageToDict(restored) == data
    assert restored.HasField("child")
    assert child_type.DESCRIPTOR.fields_by_name["name"].number == 7
    assert scalars_type.DESCRIPTOR.fields_by_name["integer"].type == descriptor_pb2.FieldDescriptorProto.TYPE_INT64
    assert scalars_type.DESCRIPTOR.fields_by_name["decimal"].type == descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE
    assert "_private" not in scalars_type.DESCRIPTOR.fields_by_name


def test_recursive_messages_and_shared_nested_classes() -> None:
    schema = Schema("recursive")
    recursive = schema.message(Recursive)
    value = recursive()
    data = {"child": {"children": [{"child": {}}]}, "children": [{}], "byName": {"key": {}}}
    json_format.ParseDict(data, value)
    assert json_format.MessageToDict(recursive.FromString(value.SerializeToString())) == data
    child = schema.message(Child)
    first = schema.message(make_dataclass("First", [("child", Child)]))
    second = schema.message(make_dataclass("Second", [("child", Child)]))
    assert first(child=child(name="one")).SerializeToString() == second(child=child(name="one")).SerializeToString()


def test_deterministic_export_compiles(tmp_path: Path) -> None:
    schema = Schema("example.v1")
    payload = schema.message(Payload)
    recursive = schema.message(Recursive)
    methods = [
        MethodDefinition("ZUnary", payload, payload, False, False),
        MethodDefinition("Bidi", recursive, recursive, True, True),
        MethodDefinition("Client", payload, recursive, True, False),
        MethodDefinition("Server", recursive, payload, False, True),
    ]
    exported = schema.descriptor(methods, "Example")
    assert exported.name == "example/v1/Example.proto"
    assert list(exported.dependency) == ["google/protobuf/api.proto", "google/protobuf/timestamp.proto"]
    assert [method.name for method in exported.service[0].method] == ["Bidi", "Client", "Server", "ZUnary"]
    text = schema.proto(methods, "Example")
    assert text == schema.proto(list(reversed(methods)), "Example")
    assert "optional int64 present = 6;" in text
    assert "map<string, .example.v1.Child> by_name = 5;" in text
    assert ".google.protobuf.Timestamp timestamp = 9;" in text
    exported.message_type.clear()
    assert schema.proto(methods, "Example") == text
    source = tmp_path / "example/v1/Example.proto"
    source.parent.mkdir(parents=True)
    source.write_text(text)
    compiled = tmp_path / "schema.pb"
    include = Path(protoc.__file__).parent / "_proto"
    assert (
        protoc.main(
            [
                "protoc",
                f"-I{tmp_path}",
                f"-I{include}",
                f"--descriptor_set_out={compiled}",
                "--include_imports",
                str(source),
            ]
        )
        == 0
    )
    files = descriptor_pb2.FileDescriptorSet.FromString(compiled.read_bytes())
    pool = descriptor_pool.DescriptorPool()
    for file in files.file:
        pool.AddSerializedFile(file.SerializeToString())
    compiled_type = message_factory.GetMessageClass(pool.FindMessageTypeByName("example.v1.Payload"))
    value = payload()
    json_format.ParseDict({"byName": {"key": {"name": "ok"}}, "scalars": {"present": "0"}}, value)
    assert json_format.MessageToDict(compiled_type.FromString(value.SerializeToString())) == {
        "byName": {"key": {"name": "ok"}},
        "scalars": {"present": "0"},
    }


def test_import_only_service_and_empty_message(tmp_path: Path) -> None:
    schema = Schema("imported")
    method = MethodDefinition("Echo", timestamp_pb2.Timestamp, api_pb2.Api, False, False)
    descriptor = schema.descriptor([method], "Service")
    assert descriptor.service[0].method[0].input_type == ".google.protobuf.Timestamp"
    assert len(descriptor.message_type) == 0
    assert list(descriptor.dependency) == ["google/protobuf/api.proto", "google/protobuf/timestamp.proto"]
    source = tmp_path / "Service.proto"
    source.write_text(schema.proto([method], "Service"))
    assert (
        protoc.main(
            [
                "protoc",
                f"-I{tmp_path}",
                f"-I{Path(protoc.__file__).parent / '_proto'}",
                f"--descriptor_set_out={tmp_path / 'service.pb'}",
                str(source),
            ]
        )
        == 0
    )
    empty = schema.message(make_dataclass("Empty", []))
    assert empty().SerializeToString() == b""
    assert schema.proto([], "EmptyService").endswith("service EmptyService {\n}\n")


def test_explicit_tags_survive_reordering_and_fields_are_frozen() -> None:
    original = make_dataclass("Stable", [("first", Annotated[str, Field(10)]), ("last", Annotated[int, Field(20)])])
    reordered = make_dataclass("Stable", [("last", Annotated[int, Field(20)]), ("first", Annotated[str, Field(10)])])
    first = Schema("stable").message(original)
    second = Schema("stable").message(reordered)
    value = first(first="same", last=42)
    assert json_format.MessageToDict(second.FromString(value.SerializeToString())) == {"first": "same", "last": "42"}
    maximum = Schema("tags").message(make_dataclass("Max", [("value", Annotated[int, Field(536870911)])]))
    assert maximum.DESCRIPTOR.fields_by_name["value"].number == 536870911
    with pytest.raises(FrozenInstanceError):
        setattr(Field(1), "number", 2)
    with pytest.raises(FrozenInstanceError):
        setattr(MethodDefinition("Echo", first, first, False, False), "name", "Changed")


@pytest.mark.parametrize("number", [0, -1, 19000, 19999, 536870912, True, 1.5, "1"])
def test_invalid_field_numbers(number: Any) -> None:
    with pytest.raises(ValueError, match="Field numbers"):
        Field(number)


@pytest.mark.parametrize("package", ["", ".demo", "demo.", "demo..v1", "bad-package", "123demo", None])
def test_invalid_packages(package: Any) -> None:
    with pytest.raises(ValueError):
        Schema(package)


@pytest.mark.parametrize(
    "annotation",
    [
        Any,
        int,
        Message,
        object,
        type("Fake", (Message,), {}),
        list[int],
        [],
        None,
    ],
)
def test_invalid_message_annotations(annotation: Any) -> None:
    with pytest.raises(TypeError, match="Unsupported message annotation|require a protobuf descriptor"):
        Schema("bad").message(annotation)


@pytest.mark.parametrize(
    "annotation",
    [
        Any,
        object,
        None,
        str | int,
        str | int | None,
        list,
        cast(Any, list)[()],
        list[int | None],
        list[Annotated[int, Field(1)]],
        list[list[int]],
        list[int] | None,
        dict,
        dict[float, str],
        dict[str, int | None],
        dict[str, Annotated[int, Field(2)]],
        dict[str, list[int]],
        dict[str, int] | None,
        tuple[str, ...],
    ],
)
def test_unsupported_field_annotations_fail_during_registration(annotation: Any) -> None:
    schema = Schema("bad")
    with pytest.raises(TypeError):
        schema.message(make_dataclass("Broken", [("value", annotation)]))
    valid = schema.message(make_dataclass("Broken", [("value", int)]))
    assert valid.DESCRIPTOR.full_name == "bad.Broken"


@pytest.mark.parametrize(
    "annotations",
    [
        [("first", Annotated[int, Field(1)]), ("second", Annotated[str, Field(1)])],
        [("first", Annotated[int, Field(2)]), ("second", str)],
        [("first", Annotated[int, Field(1), Field(2)])],
        [("x_y", dict[str, int]), ("x__y", dict[str, int])],
        [("values", dict[str, int]), ("ValuesEntry", int)],
        [("foo_bar", str), ("fooBar", str)],
    ],
)
def test_conflicting_fields(annotations: list[tuple[str, Any]]) -> None:
    with pytest.raises(ValueError):
        Schema("bad").message(make_dataclass("Broken", annotations))


def test_invalid_names_forward_references_and_duplicate_types() -> None:
    schema = Schema("bad")
    with pytest.raises(ValueError, match="identifier"):
        schema.message(make_dataclass("BadName", [("non_ascii_é", str)]))
    invalid = make_dataclass("Valid", [])
    invalid.__name__ = "invalid-name"
    with pytest.raises(ValueError, match="identifier"):
        schema.message(invalid)
    for annotation in ("MissingModel", "invalid["):
        with pytest.raises(TypeError, match="Cannot resolve annotations"):
            schema.message(make_dataclass("Unresolved", [("value", annotation)]))
    original = make_dataclass("Duplicate", [("value", str)])
    compiled = schema.message(original)
    with pytest.raises(ValueError, match="Duplicate protobuf type name"):
        schema.message(make_dataclass("Duplicate", [("value", int)]))
    assert schema.message(original) is compiled
    first = make_dataclass("Nested", [])
    second = make_dataclass("Nested", [])
    with pytest.raises(ValueError, match="Duplicate protobuf type name"):
        schema.message(make_dataclass("Parent", [("first", first), ("second", second)]))


def test_optional_metadata_wrappers_and_oneof_collisions() -> None:
    model = make_dataclass(
        "Wrappers",
        [
            ("value", Annotated[int, Field(9)] | None),
            ("_value", Annotated[str, "ignored metadata"]),
            ("_entries", dict[str, bytes]),
        ],
    )
    schema = Schema("wrappers")
    message = schema.message(model)
    assert message.DESCRIPTOR.fields_by_name["value"].number == 9
    value = message(value=0)
    assert value.HasField("value")
    assert "map<string, bytes> _entries = 3;" in schema.proto([], "Service")


def _imported(files: list[descriptor_pb2.FileDescriptorProto], full_name: str) -> type[Message]:
    pool = descriptor_pool.DescriptorPool()
    for file in files:
        pool.AddSerializedFile(file.SerializeToString())
    return message_factory.GetMessageClass(pool.FindMessageTypeByName(full_name))


def test_conflicting_imports_and_imported_symbols() -> None:
    common = descriptor_pb2.FileDescriptorProto(name="outside.proto", package="outside", syntax="proto3")
    common.message_type.add(name="Shared").field.add(name="value", number=1, type=9)
    changed = descriptor_pb2.FileDescriptorProto.FromString(common.SerializeToString())
    changed.message_type[0].field[0].type = descriptor_pb2.FieldDescriptorProto.TYPE_INT64
    original = _imported([common], "outside.Shared")
    conflicting = _imported([changed], "outside.Shared")
    schema = Schema("local")
    assert schema.message(original) is original
    with pytest.raises(ValueError, match="Conflicting protobuf import"):
        schema.message(conflicting)
    with pytest.raises(ValueError, match="Conflicting protobuf import"):
        schema.descriptor([MethodDefinition("Echo", conflicting, original, False, False)], "Service")
    first_file = descriptor_pb2.FileDescriptorProto(
        name="first.proto", package="outside", syntax="proto3", dependency=["outside.proto"]
    )
    first_file.message_type.add(name="First").field.add(name="nested", number=1, type=11, type_name=".outside.Shared")
    second_file = descriptor_pb2.FileDescriptorProto(
        name="second.proto", package="outside", syntax="proto3", dependency=["outside.proto"]
    )
    second_file.message_type.add(name="Second").field.add(name="nested", number=1, type=11, type_name=".outside.Shared")
    first = _imported([common, first_file], "outside.First")
    second = _imported([changed, second_file], "outside.Second")
    with pytest.raises(ValueError, match="Conflicting protobuf import"):
        Schema("local").message(make_dataclass("UsesBoth", [("first", first), ("second", second)]))
    collision = Schema("outside")
    collision.message(make_dataclass("Shared", [("value", str)]))
    with pytest.raises(ValueError, match="Invalid protobuf import"):
        collision.message(original)
    reverse = Schema("outside")
    reverse.message(original)
    with pytest.raises(ValueError, match="Invalid protobuf schema"):
        reverse.message(make_dataclass("Shared", [("value", str)]))


def test_imported_type_with_same_short_name_as_map_entry(tmp_path: Path) -> None:
    file = descriptor_pb2.FileDescriptorProto(name="imported.proto", package="outside", syntax="proto3")
    file.message_type.add(name="ValuesEntry").field.add(name="value", number=1, type=9)
    imported = _imported([file], "outside.ValuesEntry")
    schema = Schema("local")
    schema.message(make_dataclass("Example", [("values", dict[str, int]), ("other", imported)]))
    text = schema.proto([], "Service")
    assert ".outside.ValuesEntry other = 2;" in text
    assert "map<string, int64> values = 1;" in text
    (tmp_path / "imported.proto").write_text(
        'syntax = "proto3"; package outside; message ValuesEntry {string value = 1;}'
    )
    source = tmp_path / "Service.proto"
    source.write_text(text)
    assert protoc.main(["protoc", f"-I{tmp_path}", f"--descriptor_set_out={tmp_path / 'schema.pb'}", str(source)]) == 0


def test_invalid_services_and_methods() -> None:
    schema = Schema("bad")
    message = schema.message(Child)
    method = MethodDefinition("Echo", message, message, False, False)
    with pytest.raises(ValueError, match="identifier"):
        schema.descriptor([method], "bad-service")
    with pytest.raises(ValueError, match="identifier"):
        schema.descriptor([MethodDefinition("bad-method", message, message, False, False)], "Service")
    with pytest.raises(ValueError, match="Duplicate protobuf method name"):
        schema.descriptor([method, method], "Service")
    for annotation in (int, Message, None, type("Fake", (Message,), {})):
        with pytest.raises(TypeError, match="concrete protobuf Message"):
            schema.descriptor(
                [MethodDefinition("Echo", cast(type[Message], annotation), message, False, False)], "Service"
            )
    with pytest.raises(TypeError, match="Streaming flags"):
        schema.descriptor([MethodDefinition("Echo", message, message, cast(bool, 1), False)], "Service")
    with pytest.raises(ValueError, match="Invalid protobuf service schema"):
        schema.descriptor([method], "Child")
