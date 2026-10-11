from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path

from grpclette import gRPClette


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="grpclette", description="Export and serve code-first gRPC applications.")
    commands = parser.add_subparsers(dest="command", required=True)
    proto = commands.add_parser("proto", help="Export a protobuf schema.")
    proto.add_argument("app", help="Application import path, such as examples.greeter:app.")
    proto.add_argument("--output", type=Path, help="Write the schema to a file instead of stdout.")
    run = commands.add_parser("run", help="Serve native gRPC over HTTP/2.")
    run.add_argument("app", help="Application import path, such as examples.greeter:app.")
    run.add_argument("--host", default="127.0.0.1")
    run.add_argument("--port", type=int, default=50051)
    args = parser.parse_args(argv)

    module_name, separator, attribute = args.app.partition(":")
    if not module_name or module_name.startswith(".") or not separator or not attribute or ":" in attribute:
        parser.error("Use an application import path in the form module:app.")
    sys.path.insert(0, str(Path.cwd()))
    try:
        module = importlib.import_module(module_name)
    except (ImportError, ValueError) as exc:
        parser.error(f"Could not import {module_name!r}: {exc}")
    try:
        application = vars(module)[attribute]
    except KeyError:
        parser.error(f"Module {module_name!r} has no application named {attribute!r}.")
    if not isinstance(application, gRPClette):
        parser.error(f"{args.app!r} must be a gRPClette instance.")

    if args.command == "proto":
        schema = application.proto()
        if args.output is None:
            sys.stdout.write(schema)
        else:
            try:
                args.output.write_text(schema, encoding="utf-8")
            except OSError as exc:
                parser.error(f"Could not write {args.output}: {exc}")
        return

    if not 0 <= args.port <= 65535:
        parser.error("--port must be between 0 and 65535.")
    try:
        import uvicorn

        importlib.import_module("zttp")
    except ImportError as exc:
        parser.error(f"Serving requires the optional server packages. Install 'grpclette[server]'. {exc}")
    uvicorn.run(application, host=args.host, port=args.port, http="zttp", http2=True)


if __name__ == "__main__":
    main()
