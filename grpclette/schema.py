from __future__ import annotations

from dataclasses import fields, is_dataclass
from typing import Any, cast, get_type_hints

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.descriptor import FileDescriptor
from google.protobuf.message import Message
from pydantic import BaseModel
from typing_extensions import is_typeddict

from grpclette.fields import Field, _identifier, _populate, _unwrap
from grpclette.proto import MethodDefinition as MethodDefinition, _add_dependency, _import_file, _render_file


class Schema:
    """Compile Python models into protobuf messages.

    Automatic tags follow declaration order and are not stable on reorder.
    Use Annotated metadata with Field(number) for stable wire tags.
    """

    def __init__(self, package: str) -> None:
        if not isinstance(package, str) or not package:
            raise ValueError("A nonempty protobuf package is required")
        for part in package.split("."):
            _identifier(part)
        self.package = package
        self._pool = descriptor_pool.DescriptorPool()
        self._messages: dict[str, descriptor_pb2.DescriptorProto] = {}
        self._annotations: dict[type[Any], str] = {}
        self._classes: dict[type[Any], type[Message]] = {}
        self._imports: dict[str, FileDescriptor] = {}

    def clone(self) -> Schema:
        schema = Schema(self.package)
        schema._messages = self._messages.copy()
        schema._annotations = self._annotations.copy()
        schema._classes = self._classes.copy()
        schema._imports = self._imports.copy()
        for message in self._classes.values():
            _add_dependency(schema._pool, cast(FileDescriptor, message.DESCRIPTOR.file))
        for imported in self._imports.values():
            _add_dependency(schema._pool, imported)
        return schema

    def message(self, annotation: Any, *, namespace: dict[str, Any] | None = None) -> type[Message]:
        if isinstance(annotation, type) and annotation in self._classes:
            return self._classes[annotation]
        pending: dict[str, descriptor_pb2.DescriptorProto] = {}
        annotations = self._annotations.copy()
        imports = self._imports.copy()
        dependencies = self._imports.copy()

        def resolve(model: Any) -> str:
            if not isinstance(model, type):
                raise TypeError(f"Unsupported message annotation: {model!r}")
            if issubclass(model, Message) and model is not Message:
                if model.DESCRIPTOR is None:
                    raise TypeError("Message annotations require a protobuf descriptor")
                imported = cast(FileDescriptor, model.DESCRIPTOR.file)
                _import_file(imports, imported)
                dependencies[imported.name] = imported
                return "." + model.DESCRIPTOR.full_name
            if model in annotations:
                if model in self._classes:
                    compiled = cast(FileDescriptor, self._classes[model].DESCRIPTOR.file)
                    dependencies[compiled.name] = compiled
                return "." + self.package + "." + annotations[model]
            if issubclass(model, BaseModel):
                names = list(model.model_fields)
            elif is_dataclass(model):
                names = [field.name for field in fields(model)]
            elif is_typeddict(model):
                names = list(model.__annotations__)
            else:
                raise TypeError(f"Unsupported message annotation: {model!r}")
            name = model.__name__
            _identifier(name)
            if name in self._messages or name in pending:
                raise ValueError(f"Duplicate protobuf type name: {name}")
            definition = descriptor_pb2.DescriptorProto(name=name)
            pending[name] = definition
            annotations[model] = name
            try:
                hints = get_type_hints(model, localns={**(namespace or {}), name: model}, include_extras=True)
            except (NameError, TypeError, SyntaxError) as exc:
                raise TypeError(f"Cannot resolve annotations for {name}: {exc}") from exc
            numbers: set[int] = set()
            symbols = set(names)
            for position, field_name in enumerate(names, 1):
                _identifier(field_name)
                _, explicit, _ = _unwrap(hints[field_name])
                # ponytail: declaration-order tags, explicit Field numbers for evolving schemas.
                number = Field(position if explicit is None else explicit).number
                if number in numbers:
                    raise ValueError(f"Duplicate protobuf field number {number} in {name}")
                numbers.add(number)
                _populate(
                    definition, field_name, hints[field_name], number, resolve, symbols, f".{self.package}.{name}"
                )
            return f".{self.package}.{name}"

        resolve(annotation)
        try:
            for dependency in dependencies.values():
                _add_dependency(self._pool, dependency)
        except TypeError as exc:
            raise ValueError(f"Invalid protobuf import: {exc}") from exc
        if pending:
            file = descriptor_pb2.FileDescriptorProto(
                name=f"{self.package.replace('.', '/')}/_schema_{len(self._messages)}.proto",
                package=self.package,
                syntax="proto3",
                dependency=sorted(dependencies),
            )
            file.message_type.extend(pending[name] for name in sorted(pending))
            try:
                self._pool.AddSerializedFile(file.SerializeToString())
            except TypeError as exc:
                raise ValueError(f"Invalid protobuf schema: {exc}") from exc
            for model in annotations.keys() - self._annotations.keys():
                self._classes[model] = message_factory.GetMessageClass(
                    self._pool.FindMessageTypeByName(f"{self.package}.{annotations[model]}")
                )
            self._messages.update(pending)
            self._annotations = annotations
        else:
            self._classes[annotation] = cast(type[Message], annotation)
        self._imports = imports
        return self._classes[annotation]

    def descriptor(self, methods: list[MethodDefinition], service: str) -> descriptor_pb2.FileDescriptorProto:
        _identifier(service)
        file = descriptor_pb2.FileDescriptorProto(
            name=f"{self.package.replace('.', '/')}/{service}.proto", package=self.package, syntax="proto3"
        )
        file.message_type.extend(self._messages[name] for name in sorted(self._messages))
        definition = file.service.add(name=service)
        imports = self._imports.copy()
        names: set[str] = set()
        for method in sorted(methods, key=lambda item: item.name):
            _identifier(method.name)
            if method.name in names:
                raise ValueError(f"Duplicate protobuf method name: {method.name}")
            names.add(method.name)
            for message in (method.input, method.output):
                if not isinstance(message, type) or not issubclass(message, Message) or message.DESCRIPTOR is None:
                    raise TypeError("Method inputs and outputs must be concrete protobuf Message classes")
                if message not in self._classes.values():
                    imported = cast(FileDescriptor, message.DESCRIPTOR.file)
                    _import_file(imports, imported)
            if type(method.client_streaming) is not bool or type(method.server_streaming) is not bool:
                raise TypeError("Streaming flags must be bool values")
            definition.method.add(
                name=method.name,
                input_type="." + method.input.DESCRIPTOR.full_name,
                output_type="." + method.output.DESCRIPTOR.full_name,
                client_streaming=method.client_streaming,
                server_streaming=method.server_streaming,
            )
        file.dependency.extend(sorted(imports))
        pool = descriptor_pool.DescriptorPool()
        for imported in imports.values():
            _add_dependency(pool, imported)
        try:
            pool.AddSerializedFile(file.SerializeToString())
        except TypeError as exc:
            raise ValueError(f"Invalid protobuf service schema: {exc}") from exc
        return file

    def proto(self, methods: list[MethodDefinition], service: str) -> str:
        return _render_file(self.descriptor(methods, service))
