#!/usr/bin/env python3
"""Repeatedly hot-add and remove QEMU's CDC-compatible ``usb-net`` device.

The VM must have ``vm.usb_hotplug.enabled`` in its node specification.  This
script talks only to the resulting rootless QMP Unix socket; topology setup and
root privileges are not required once WOS was launched with ``--no-setup``.
"""

from __future__ import annotations

import argparse
import json
import socket
import time
from pathlib import Path
from typing import Any


class QmpError(RuntimeError):
    pass


class QmpClient:
    def __init__(self, path: Path, timeout: float):
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._socket.settimeout(timeout)
        self._socket.connect(str(path))
        self._stream = self._socket.makefile("rwb", buffering=0)
        self._events: list[dict[str, Any]] = []
        greeting = self._read_message()
        if "QMP" not in greeting:
            raise QmpError(f"invalid QMP greeting: {greeting!r}")
        self._next_id = 1
        self.execute("qmp_capabilities")

    def close(self) -> None:
        self._stream.close()
        self._socket.close()

    def _read_message(self) -> dict[str, Any]:
        raw = self._stream.readline()
        if not raw:
            raise QmpError("QMP connection closed")
        message = json.loads(raw)
        if not isinstance(message, dict):
            raise QmpError(f"invalid QMP message: {message!r}")
        return message

    def execute(self, command: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        request_id = self._next_id
        self._next_id += 1
        request: dict[str, Any] = {"execute": command, "id": request_id}
        if arguments:
            request["arguments"] = arguments
        self._stream.write(json.dumps(request, separators=(",", ":")).encode() + b"\r\n")

        while True:
            response = self._read_message()
            if response.get("id") != request_id:
                if "event" in response:
                    self._events.append(response)
                continue
            if "error" in response:
                raise QmpError(f"{command} failed: {response['error']!r}")
            return response

    def wait_for_device_deleted(self, device_id: str, deadline: float) -> None:
        while time.monotonic() < deadline:
            message = self._events.pop(0) if self._events else self._read_message()
            if message.get("event") != "DEVICE_DELETED":
                continue
            data = message.get("data", {})
            if data.get("device") == device_id or data.get("path") == device_id:
                return
        raise QmpError(f"timed out waiting for DEVICE_DELETED for {device_id}")


def add_usb_net(client: QmpClient, device_id: str, netdev_id: str, controller_id: str, mac: str) -> None:
    client.execute(
        "device_add",
        {
            "driver": "usb-net",
            "id": device_id,
            "bus": f"{controller_id}.0",
            "netdev": netdev_id,
            "mac": mac,
        },
    )


def run(args: argparse.Namespace) -> None:
    client = QmpClient(args.socket, args.timeout)
    try:
        client.execute("netdev_add", {"type": "user", "id": args.netdev_id})
        try:
            for cycle in range(args.cycles):
                add_usb_net(client, args.device_id, args.netdev_id, args.controller_id, args.mac)
                print(f"cycle {cycle + 1}/{args.cycles}: attached {args.device_id}", flush=True)
                time.sleep(args.attached_seconds)
                client.execute("device_del", {"id": args.device_id})
                client.wait_for_device_deleted(args.device_id, time.monotonic() + args.timeout)
                print(f"cycle {cycle + 1}/{args.cycles}: detached {args.device_id}", flush=True)
                time.sleep(args.detached_seconds)
        finally:
            client.execute("netdev_del", {"id": args.netdev_id})
    finally:
        client.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", type=Path, default=Path("ktest-data/qmp-vm0.sock"))
    parser.add_argument("--cycles", type=int, default=32)
    parser.add_argument("--attached-seconds", type=float, default=1.0)
    parser.add_argument("--detached-seconds", type=float, default=0.25)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--controller-id", default="xhci")
    parser.add_argument("--device-id", default="usbnet0")
    parser.add_argument("--netdev-id", default="usbhot")
    # QEMU's CDC descriptors advertise this address; use the same address for
    # the user-mode backend so bidirectional traffic reaches the guest.
    parser.add_argument("--mac", default="02:05:25:a4:a2:00")
    args = parser.parse_args()
    if args.cycles <= 0 or args.timeout <= 0 or args.attached_seconds < 0 or args.detached_seconds < 0:
        parser.error("cycles/timeout must be positive and dwell times must be non-negative")
    return args


if __name__ == "__main__":
    run(parse_args())
