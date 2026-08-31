#!/usr/bin/env python3
"""Repeatedly hot-add and remove QEMU's CDC-compatible ``usb-net`` device.

The VM must have ``vm.usb_hotplug.enabled`` in its node specification.  This
script talks only to the resulting rootless QMP Unix socket; topology setup and
root privileges are not required once WOS was launched with ``--no-setup``.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

CLUSTER_DIR = Path(__file__).resolve().parents[1] / "cluster"
if str(CLUSTER_DIR) not in sys.path:
    sys.path.insert(0, str(CLUSTER_DIR))

from qmp import QmpClient, QmpError  # noqa: E402, F401


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
