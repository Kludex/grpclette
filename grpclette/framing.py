from __future__ import annotations

from connectrpc.code import Code
from connectrpc.errors import ConnectError


class FrameValidator:
    def __init__(self, *, compressed: bool = False) -> None:
        self._compressed = compressed
        self._header = bytearray()
        self._remaining = 0

    def __call__(self, body: bytes, *, final: bool) -> None:
        offset = 0
        while offset < len(body):
            if self._remaining:
                consumed = min(self._remaining, len(body) - offset)
                self._remaining -= consumed
                offset += consumed
                continue
            consumed = min(5 - len(self._header), len(body) - offset)
            self._header.extend(body[offset : offset + consumed])
            offset += consumed
            if self._header[0] not in (0, 1):
                raise ConnectError(Code.INVALID_ARGUMENT, "Invalid gRPC frame flags")
            if self._header[0] == 1 and not self._compressed:
                raise ConnectError(Code.INVALID_ARGUMENT, "Compressed frames require grpc-encoding")
            if len(self._header) == 5:
                self._remaining = int.from_bytes(self._header[1:], "big")
                self._header.clear()
        if final and (self._header or self._remaining):
            raise ConnectError(Code.INVALID_ARGUMENT, "Truncated gRPC frame")
