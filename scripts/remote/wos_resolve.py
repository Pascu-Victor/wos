#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
VM_LOG_RE = re.compile(r"serial-vm(?P<vm>\d+)\.log$")
IP_RE = re.compile(r"eth0 configured(?: with IP |: ip=)(?P<ip>\d+\.\d+\.\d+\.\d+)")
HOST_RE = re.compile(r"hostname='(?P<hostname>[^']+)'")
MAX_DISCOVERY_LOGS = 128
MAX_LOG_TAIL_BYTES = 2 * 1024 * 1024


def is_ipv4(value: str) -> bool:
    parts = value.split(".")
    if len(parts) != 4:
        return False
    try:
        return all(0 <= int(part) <= 255 for part in parts)
    except ValueError:
        return False


def _bounded_log_tail(path: Path) -> str:
    size = path.stat().st_size
    with path.open("rb") as stream:
        if size > MAX_LOG_TAIL_BYTES:
            stream.seek(size - MAX_LOG_TAIL_BYTES)
            stream.readline()  # Discard the bounded tail's partial first line.
        data = stream.read(MAX_LOG_TAIL_BYTES)
    return data.decode("utf-8", errors="ignore")


def _latest_complete_observation(text: str) -> tuple[str, str] | None:
    hosts = list(HOST_RE.finditer(text))
    if not hosts:
        return None
    complete: list[tuple[int, str, str]] = []
    for index, host_match in enumerate(hosts):
        end = hosts[index + 1].start() if index + 1 < len(hosts) else len(text)
        ip_matches = list(IP_RE.finditer(text, host_match.end(), end))
        if ip_matches:
            complete.append(
                (ip_matches[-1].end(), host_match.group("hostname"), ip_matches[-1].group("ip"))
            )
    if not complete:
        return None
    _position, hostname, ip = max(complete, key=lambda item: item[0])
    return hostname, ip


def collect_nodes() -> list[dict[str, str]]:
    nodes: list[dict[str, str]] = []
    candidates = {
        *sorted(ROOT.glob("serial-vm*.log")),
        *sorted((ROOT / "ktest-data").glob("serial-vm*.log")),
        *sorted((ROOT / "wki-chaos-data").glob("*serial-vm*.log")),
    }
    log_paths = sorted(
        candidates,
        key=lambda path: (-path.stat().st_mtime_ns, path.as_posix()),
    )[:MAX_DISCOVERY_LOGS]
    for path in log_paths:
        match = VM_LOG_RE.search(path.name)
        if not match:
            continue
        try:
            observation = _latest_complete_observation(_bounded_log_tail(path))
        except OSError:
            continue
        if observation is None:
            continue
        hostname, ip = observation
        if is_ipv4(ip):
            nodes.append({"hostname": hostname, "ip": ip, "vm": f"vm{match.group('vm')}"})

    return nodes


def resolve_target(spec: str) -> str:
    if is_ipv4(spec):
        return spec

    for node in collect_nodes():
        vm_name = node.get("vm")
        hostname = node.get("hostname")
        hostname_fqdn = f"{hostname}.wos" if hostname else None

        if spec == vm_name:
            return node.get("ip", spec)
        if spec == hostname or spec == hostname_fqdn:
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
