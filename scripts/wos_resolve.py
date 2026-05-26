#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
VM_LOG_RE = re.compile(r"serial-vm(?P<vm>\d+)\.log$")
IP_RE = re.compile(r"eth0 configured with IP (?P<ip>\d+\.\d+\.\d+\.\d+)")
HOST_RE = re.compile(r"hostname='(?P<hostname>[^']+)'")


def is_ipv4(value: str) -> bool:
    parts = value.split(".")
    if len(parts) != 4:
        return False
    try:
        return all(0 <= int(part) <= 255 for part in parts)
    except ValueError:
        return False


def collect_nodes() -> list[dict[str, str]]:
    nodes: list[dict[str, str]] = []
    for path in sorted(ROOT.glob("serial-vm*.log")):
        match = VM_LOG_RE.search(path.name)
        if not match:
            continue

        vm_name = f"vm{match.group('vm')}"
        text = path.read_text(encoding="utf-8", errors="ignore")

        ip_match = None
        for ip_match in IP_RE.finditer(text):
            pass

        host_match = None
        for host_match in HOST_RE.finditer(text):
            pass

        node = {"vm": vm_name}
        if ip_match is not None:
            node["ip"] = ip_match.group("ip")
        if host_match is not None:
            node["hostname"] = host_match.group("hostname")
        nodes.append(node)

    return nodes


def resolve_target(spec: str) -> str:
    if is_ipv4(spec):
        return spec

    for node in collect_nodes():
        if spec == node.get("vm"):
            return node.get("ip", spec)
        if spec == node.get("hostname"):
            return node.get("ip", spec)

    return spec


def resolve_path(path: str) -> str:
    resolved = path
    for node in collect_nodes():
        vm_name = node.get("vm")
        hostname = node.get("hostname")
        if not vm_name or not hostname:
            continue
        resolved = resolved.replace(f"/wki/{vm_name}/", f"/wki/{hostname}/")
        if resolved == f"/wki/{vm_name}":
            resolved = f"/wki/{hostname}"
    return resolved


def main() -> int:
    parser = argparse.ArgumentParser(description="Resolve WOS benchmark node aliases.")
    parser.add_argument("kind", choices=["target", "path"])
    parser.add_argument("value")
    args = parser.parse_args()

    if args.kind == "target":
        print(resolve_target(args.value))
    else:
        print(resolve_path(args.value))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
