#!/usr/bin/env python3
"""Small bounded QEMU Machine Protocol client shared by host test tools."""

from __future__ import annotations

import json
import socket
import time
from pathlib import Path
from typing import Any


MAX_QMP_MESSAGE_BYTES = 1024 * 1024
MAX_PENDING_EVENTS = 256


class QmpError(RuntimeError):
    pass


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise QmpError(f"duplicate QMP JSON member {key!r}")
        result[key] = value
    return result


class QmpClient:
    """Synchronous QMP client with bounded messages and queued events."""

    def __init__(self, path: Path, timeout: float):
        if timeout <= 0:
            raise ValueError("QMP timeout must be positive")
        self._timeout = timeout
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._socket.settimeout(timeout)
        self._socket.connect(str(path))
        self._stream = self._socket.makefile("rwb", buffering=0)
        self._events: list[dict[str, Any]] = []
        self._discarded_events = 0
        self._next_id = 1
        try:
            deadline = time.monotonic() + timeout
            greeting = self._read_message(deadline)
            if "QMP" not in greeting:
                raise QmpError(f"invalid QMP greeting: {greeting!r}")
            self.execute("qmp_capabilities", deadline=deadline)
        except Exception:
            self.close()
            raise

    def __enter__(self) -> QmpClient:
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()

    def close(self) -> None:
        stream = getattr(self, "_stream", None)
        sock = getattr(self, "_socket", None)
        self._stream = None
        self._socket = None
        if stream is not None:
            stream.close()
        if sock is not None:
            sock.close()

    def _read_message(self, deadline: float | None = None) -> dict[str, Any]:
        if self._stream is None:
            raise QmpError("QMP connection is closed")
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise QmpError("QMP operation timed out")
            assert self._socket is not None
            self._socket.settimeout(remaining)
        try:
            raw = self._stream.readline(MAX_QMP_MESSAGE_BYTES + 1)
        except socket.timeout as exc:
            raise QmpError("QMP operation timed out") from exc
        if not raw:
            raise QmpError("QMP connection closed")
        if len(raw) > MAX_QMP_MESSAGE_BYTES or not raw.endswith(b"\n"):
            raise QmpError("QMP message exceeds the configured bound")
        try:
            message = json.loads(raw, object_pairs_hook=_object_without_duplicates)
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
            raise QmpError(f"invalid QMP JSON: {exc}") from exc
        if not isinstance(message, dict):
            raise QmpError(f"invalid QMP message: {message!r}")
        return message

    def _queue_event(self, message: dict[str, Any]) -> None:
        if len(self._events) >= MAX_PENDING_EVENTS:
            self._events.pop(0)
            self._discarded_events += 1
        self._events.append(message)

    def execute(
        self,
        command: str,
        arguments: dict[str, Any] | None = None,
        *,
        deadline: float | None = None,
    ) -> dict[str, Any]:
        if self._stream is None:
            raise QmpError("QMP connection is closed")
        if deadline is None:
            deadline = time.monotonic() + self._timeout
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise QmpError("QMP operation timed out")
        assert self._socket is not None
        self._socket.settimeout(remaining)
        request_id = self._next_id
        self._next_id += 1
        request: dict[str, Any] = {"execute": command, "id": request_id}
        if arguments:
            request["arguments"] = arguments
        encoded = json.dumps(request, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > MAX_QMP_MESSAGE_BYTES:
            raise QmpError("QMP request exceeds the configured bound")
        try:
            self._stream.write(encoded + b"\r\n")
        except socket.timeout as exc:
            raise QmpError("QMP operation timed out") from exc

        while True:
            response = self._read_message(deadline)
            if response.get("id") != request_id:
                if "event" in response:
                    self._queue_event(response)
                continue
            if "error" in response:
                raise QmpError(f"{command} failed: {response['error']!r}")
            return response

    def drain_events(self) -> tuple[list[dict[str, Any]], int]:
        events = self._events
        discarded = self._discarded_events
        self._events = []
        self._discarded_events = 0
        return events, discarded

    def wait_for_event(
        self,
        event_name: str,
        deadline: float,
        predicate=lambda _message: True,
    ) -> dict[str, Any]:
        while time.monotonic() < deadline:
            message = self._events.pop(0) if self._events else self._read_message(deadline)
            if message.get("event") == event_name and predicate(message):
                return message
        raise QmpError(f"timed out waiting for QMP event {event_name}")

    def wait_for_device_deleted(self, device_id: str, deadline: float) -> None:
        self.wait_for_event(
            "DEVICE_DELETED",
            deadline,
            lambda message: message.get("data", {}).get("device") == device_id
            or message.get("data", {}).get("path") == device_id,
        )
