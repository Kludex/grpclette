from __future__ import annotations

import os
import signal
import site
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from examples.greeter import app as greeter
from examples.streaming import app as streaming

ROOT = Path(__file__).resolve().parents[1]


def cli(*args: str, cwd: Path = ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "grpclette.cli", *args],
        cwd=cwd,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )


@pytest.mark.parametrize("module, schema", [("greeter", greeter.proto()), ("streaming", streaming.proto())])
def test_proto_stdout_and_file(module: str, schema: str, tmp_path: Path) -> None:
    result = cli("proto", f"examples.{module}:app")
    assert result.returncode == 0, result.stderr
    assert result.stdout == schema
    assert result.stderr == ""
    output = tmp_path / "service.proto"
    result = cli("proto", f"examples.{module}:app", "--output", str(output))
    assert result.returncode == 0, result.stderr
    assert result.stdout == result.stderr == ""
    assert output.read_text(encoding="utf-8") == schema


@pytest.mark.parametrize(
    "reference",
    ["examples.greeter", ":app", "examples.greeter:", "examples.greeter:app:extra", ".examples.greeter:app"],
)
def test_invalid_import_path(reference: str) -> None:
    result = cli("proto", reference)
    assert result.returncode == 2
    assert "module:app" in result.stderr


@pytest.mark.parametrize(
    "reference, source, expected",
    [
        ("missing_grpclette_example:app", None, "Could not import 'missing_grpclette_example'"),
        ("broken:app", "import missing_grpclette_dependency\n", "No module named 'missing_grpclette_dependency'"),
        ("broken:app", "raise ValueError('invalid configuration')\n", "invalid configuration"),
        ("broken:app", "other = object()\n", "has no application named 'app'"),
        ("broken:app", "app = object()\n", "must be a gRPClette instance"),
    ],
)
def test_bad_application(reference: str, source: str | None, expected: str, tmp_path: Path) -> None:
    if source is not None:
        (tmp_path / "broken.py").write_text(source, encoding="utf-8")
    result = cli("proto", reference, cwd=tmp_path)
    assert result.returncode == 2
    assert expected in result.stderr
    assert "Traceback" not in result.stderr


def test_output_error(tmp_path: Path) -> None:
    result = cli("proto", "examples.greeter:app", "--output", str(tmp_path))
    assert result.returncode == 2
    assert "Could not write" in result.stderr


@pytest.mark.parametrize("port", ["-1", "65536"])
def test_invalid_port(port: str) -> None:
    result = cli("run", "examples.greeter:app", "--port", port)
    assert result.returncode == 2
    assert "--port must be between 0 and 65535" in result.stderr


@pytest.mark.parametrize("missing", ["uvicorn", "zttp"])
def test_optional_server_import_boundary(missing: str, tmp_path: Path) -> None:
    dependencies = tmp_path / "dependencies"
    dependencies.mkdir()
    for directory in site.getsitepackages():
        for package in Path(directory).glob("*"):
            if package.name != missing and not (dependencies / package.name).exists():
                (dependencies / package.name).symlink_to(package, target_is_directory=package.is_dir())
    script = (
        "import sys; "
        f"sys.path[:] = [path for path in sys.path if path not in {site.getsitepackages()!r}]; "
        f"sys.path[:0] = [{str(dependencies)!r}, {str(ROOT)!r}]; "
        "from grpclette.cli import main; "
        "main(sys.argv[1:])"
    )
    for command in ("proto", "run"):
        result = subprocess.run(
            [sys.executable, "-c", script, command, "examples.greeter:app"],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        if command == "proto":
            assert result.returncode == 0, result.stderr
            assert result.stdout == greeter.proto()
        else:
            assert result.returncode == 2
            assert "grpclette[server]" in result.stderr
            assert missing in result.stderr
            assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    "module, calls",
    [
        (
            "greeter",
            "stub = services.GreeterStub(channel)\n"
            "assert stub.SayHello(messages.HelloRequest(name='Ada'), timeout=5).message == 'Hello, Ada!'\n",
        ),
        (
            "streaming",
            "stub = services.StreamingStub(channel)\n"
            "request = messages.Greeting(name='Ada')\n"
            "assert stub.SayHello(request, timeout=5).message == 'Hello, Ada!'\n"
            "assert [reply.message for reply in stub.WatchGreetings(request, timeout=5)] == "
            "['Hello, Ada! (1)', 'Hello, Ada! (2)', 'Hello, Ada! (3)']\n"
            "assert stub.CountGreetings(iter([request, request]), timeout=5).count == 2\n"
            "assert [reply.message for reply in stub.Chat(iter([request, request]), timeout=5)] == "
            "['Hello, Ada!', 'Hello, Ada!']\n",
        ),
    ],
)
def test_run_examples_with_standard_grpc_stubs(module: str, calls: str, tmp_path: Path) -> None:
    schema = tmp_path / f"{module}.proto"
    result = cli("proto", f"examples.{module}:app", "--output", str(schema))
    assert result.returncode == 0, result.stderr
    subprocess.run(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{tmp_path}",
            f"--python_out={tmp_path}",
            f"--grpc_python_out={tmp_path}",
            str(schema),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    script = (
        f"import grpc\nimport {module}_pb2 as messages\nimport {module}_pb2_grpc as services\n"
        f"channel = grpc.insecure_channel('127.0.0.1:{port}')\n"
        "grpc.channel_ready_future(channel).result(timeout=10)\n" + calls + "channel.close()\n"
    )
    log_path = tmp_path / "server.log"
    with log_path.open("w", encoding="utf-8") as log:
        with subprocess.Popen(
            [
                sys.executable,
                "-m",
                "grpclette.cli",
                "run",
                f"examples.{module}:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            cwd=ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
        ) as server:
            try:
                result = subprocess.run(
                    [sys.executable, "-c", script],
                    cwd=tmp_path,
                    capture_output=True,
                    text=True,
                    timeout=20,
                    check=False,
                )
                assert result.returncode == 0, result.stderr + log_path.read_text(encoding="utf-8")
            finally:
                server.send_signal(signal.SIGINT)
                server.wait(timeout=10)
    assert server.returncode == 0, log_path.read_text(encoding="utf-8")
