#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


KVM_THREAD_RE = re.compile(r"CPU (\d+)/KVM")


@dataclass(frozen=True)
class QemuSpec:
    label: str
    pid: int
    source: str


@dataclass(frozen=True)
class VcpuThread:
    vcpu: int
    tid: int
    comm: str


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def read_cmdline(pid: int) -> list[str]:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return []
    return [field.decode("utf-8", errors="replace") for field in raw.split(b"\0") if field]


def read_schedstat(tid: int) -> tuple[int, int, int]:
    fields = read_text(Path(f"/proc/{tid}/schedstat")).split()
    if len(fields) < 3:
        raise RuntimeError(f"/proc/{tid}/schedstat had fewer than three fields")
    return int(fields[0]), int(fields[1]), int(fields[2])


def read_psr(tid: int) -> int:
    text = read_text(Path(f"/proc/{tid}/stat"))
    tail = text.rsplit(")", 1)[1].split()
    return int(tail[36])


def kvm_threads(pid: int) -> list[VcpuThread]:
    out: list[VcpuThread] = []
    task_root = Path(f"/proc/{pid}/task")
    for task_path in task_root.iterdir():
        if not task_path.name.isdigit():
            continue
        tid = int(task_path.name)
        try:
            comm = read_text(task_path / "comm").strip()
        except OSError:
            continue
        match = KVM_THREAD_RE.fullmatch(comm)
        if match:
            out.append(VcpuThread(vcpu=int(match.group(1)), tid=tid, comm=comm))
    return sorted(out, key=lambda item: item.vcpu)


def process_is_qemu(pid: int, argv: list[str]) -> bool:
    if not argv:
        return False
    basename = os.path.basename(argv[0])
    if basename.startswith("qemu-system-"):
        return True
    try:
        comm = read_text(Path(f"/proc/{pid}/comm")).strip()
    except OSError:
        return False
    return comm.startswith("qemu-system-")


def discover_vm_pid(vm_index: int) -> QemuSpec:
    markers = (
        f"serial-vm{vm_index}.log",
        f"qemu-vm{vm_index}.log",
        f"qemu-vm{vm_index}-cpu",
        f"disk-vm{vm_index}.qcow2",
        f"mountfs-vm{vm_index}.qcow2",
    )
    matches: list[tuple[int, list[str]]] = []
    for proc_path in Path("/proc").iterdir():
        if not proc_path.name.isdigit():
            continue
        pid = int(proc_path.name)
        argv = read_cmdline(pid)
        if not process_is_qemu(pid, argv):
            continue
        joined = "\0".join(argv)
        if any(marker in joined for marker in markers):
            matches.append((pid, argv))

    if not matches:
        raise RuntimeError(
            f"could not auto-discover qemu pid for vm{vm_index}; "
            f"pass --qemu-vm vm{vm_index}:<pid> explicitly"
        )
    if len(matches) > 1:
        rendered = ", ".join(str(pid) for pid, _ in matches)
        raise RuntimeError(
            f"auto-discovery found multiple qemu pids for vm{vm_index}: {rendered}; "
            "pass --qemu-vm explicitly"
        )
    pid, _ = matches[0]
    return QemuSpec(label=f"vm{vm_index}", pid=pid, source="auto")


def parse_qemu_vm(raw: str) -> QemuSpec:
    if ":" not in raw:
        raise argparse.ArgumentTypeError("expected VM:PID, for example vm0:12345 or 0:12345")
    raw_label, raw_pid = raw.split(":", 1)
    if raw_label.startswith("vm"):
        label = raw_label
    elif raw_label.isdigit():
        label = f"vm{raw_label}"
    else:
        label = raw_label
    if not raw_pid.isdigit() or int(raw_pid) <= 0:
        raise argparse.ArgumentTypeError("PID must be a positive integer")
    return QemuSpec(label=label, pid=int(raw_pid), source="explicit")


def snapshot(specs: list[QemuSpec], threads: dict[str, list[VcpuThread]]) -> tuple[int, dict[str, Any]]:
    now = time.monotonic_ns()
    rows: dict[str, Any] = {}
    for spec in specs:
        vm_rows: dict[str, Any] = {}
        for thread in threads[spec.label]:
            try:
                vm_rows[str(thread.vcpu)] = {
                    "vcpu": thread.vcpu,
                    "tid": thread.tid,
                    "comm": thread.comm,
                    "psr": read_psr(thread.tid),
                    "schedstat": read_schedstat(thread.tid),
                }
            except OSError:
                continue
        rows[spec.label] = vm_rows
    return now, rows


def run_payload(args: argparse.Namespace, command: list[str]) -> dict[str, Any]:
    if command:
        started = datetime.now(timezone.utc).isoformat()
        result = subprocess.run(command, check=False, text=True, capture_output=True)
        finished = datetime.now(timezone.utc).isoformat()
        return {
            "mode": "command",
            "command": command,
            "started": started,
            "finished": finished,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

    started = datetime.now(timezone.utc).isoformat()
    time.sleep(args.duration)
    finished = datetime.now(timezone.utc).isoformat()
    return {
        "mode": "duration",
        "duration_seconds": args.duration,
        "started": started,
        "finished": finished,
        "returncode": 0,
        "stdout": "",
        "stderr": "",
    }


def build_result(
    specs: list[QemuSpec],
    threads: dict[str, list[VcpuThread]],
    before_time: int,
    before: dict[str, Any],
    after_time: int,
    after: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    qemu_rows: list[dict[str, Any]] = []
    max_wait_ms = 0.0
    max_wait_thread: dict[str, Any] | None = None
    max_run_ms = 0.0
    min_nonzero_run_ms: float | None = None

    for spec in specs:
        argv = read_cmdline(spec.pid)
        vm_threads: list[dict[str, Any]] = []
        for thread in threads[spec.label]:
            before_row = before.get(spec.label, {}).get(str(thread.vcpu))
            after_row = after.get(spec.label, {}).get(str(thread.vcpu))
            if not before_row or not after_row:
                vm_threads.append(
                    {
                        "vcpu": thread.vcpu,
                        "tid": thread.tid,
                        "comm": thread.comm,
                        "missing": True,
                    }
                )
                continue

            before_sched = before_row["schedstat"]
            after_sched = after_row["schedstat"]
            run_ms = (after_sched[0] - before_sched[0]) / 1_000_000.0
            wait_ms = (after_sched[1] - before_sched[1]) / 1_000_000.0
            slices = after_sched[2] - before_sched[2]
            row = {
                "vcpu": thread.vcpu,
                "tid": thread.tid,
                "comm": thread.comm,
                "psr_before": before_row["psr"],
                "psr_after": after_row["psr"],
                "run_ms": run_ms,
                "wait_ms": wait_ms,
                "slices": slices,
                "schedstat_before": before_sched,
                "schedstat_after": after_sched,
            }
            vm_threads.append(row)
            if wait_ms > max_wait_ms:
                max_wait_ms = wait_ms
                max_wait_thread = {"vm": spec.label, **row}
            max_run_ms = max(max_run_ms, run_ms)
            if run_ms > 0:
                min_nonzero_run_ms = run_ms if min_nonzero_run_ms is None else min(min_nonzero_run_ms, run_ms)

        qemu_rows.append(
            {
                "label": spec.label,
                "pid": spec.pid,
                "source": spec.source,
                "cmdline": argv,
                "vcpu_threads": vm_threads,
            }
        )

    wall_ms = (after_time - before_time) / 1_000_000.0
    run_imbalance = None
    if min_nonzero_run_ms and min_nonzero_run_ms > 0:
        run_imbalance = max_run_ms / min_nonzero_run_ms

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "wall_ms": wall_ms,
        "payload": payload,
        "qemu": qemu_rows,
        "summary": {
            "qemu_count": len(qemu_rows),
            "vcpu_count": sum(len(item["vcpu_threads"]) for item in qemu_rows),
            "max_wait_ms": max_wait_ms,
            "max_wait_thread": max_wait_thread,
            "run_imbalance_max_over_min_nonzero": run_imbalance,
        },
    }


def render_text_report(result: dict[str, Any]) -> str:
    lines = [
        "schedstat probe",
        f"timestamp: {result['timestamp']}",
        f"wall_ms: {result['wall_ms']:.2f}",
        f"payload_mode: {result['payload']['mode']}",
    ]
    command = result["payload"].get("command")
    if command:
        lines.append(f"command: {shlex.join(command)}")
    else:
        lines.append(f"duration_seconds: {result['payload'].get('duration_seconds')}")
    lines.append(f"payload_returncode: {result['payload']['returncode']}")
    lines.append("")
    for qemu in result["qemu"]:
        lines.append(f"{qemu['label']} pid={qemu['pid']} source={qemu['source']}")
        for thread in qemu["vcpu_threads"]:
            if thread.get("missing"):
                lines.append(f"  vcpu={thread['vcpu']} tid={thread['tid']} missing_after_snapshot=true")
                continue
            lines.append(
                "  "
                f"vcpu={thread['vcpu']} tid={thread['tid']} "
                f"psr={thread['psr_before']}->{thread['psr_after']} "
                f"run={thread['run_ms']:.2f}ms wait={thread['wait_ms']:.2f}ms "
                f"slices={thread['slices']}"
            )
    lines.append("")
    summary = result["summary"]
    lines.append(
        "summary: "
        f"qemu={summary['qemu_count']} vcpus={summary['vcpu_count']} "
        f"max_wait={summary['max_wait_ms']:.2f}ms "
        f"run_imbalance={summary['run_imbalance_max_over_min_nonzero']}"
    )
    if result["payload"].get("stdout"):
        lines.extend(["", "=== payload stdout ===", result["payload"]["stdout"].rstrip()])
    if result["payload"].get("stderr"):
        lines.extend(["", "=== payload stderr ===", result["payload"]["stderr"].rstrip()])
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Measure host Linux schedstat deltas for QEMU KVM vCPU threads around a command or duration."
    )
    parser.add_argument(
        "--qemu-vm",
        action="append",
        type=parse_qemu_vm,
        default=[],
        metavar="VM:PID",
        help="QEMU process to sample, for example vm0:12345 or 0:12345. Repeat for multiple VMs.",
    )
    parser.add_argument(
        "--auto-discover-vm",
        action="append",
        type=int,
        default=[],
        metavar="INDEX",
        help="Explicitly auto-discover the QEMU pid for WOS vm INDEX from host /proc command lines.",
    )
    parser.add_argument("--duration", type=float, default=0.0, help="Sample passively for this many seconds.")
    parser.add_argument("--output-json", type=Path, help="Write the full machine-readable result here.")
    parser.add_argument("--output-text", type=Path, help="Write a compact text report here.")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="Command to run after a literal --.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    command = args.command
    if command and command[0] == "--":
        command = command[1:]

    if args.duration < 0:
        parser.error("--duration must be nonnegative")
    if command and args.duration > 0:
        parser.error("pass either a command or --duration, not both")
    if not command and args.duration <= 0:
        parser.error("pass a command after -- or a positive --duration")

    specs: list[QemuSpec] = list(args.qemu_vm)
    seen_labels = {spec.label for spec in specs}
    for vm_index in args.auto_discover_vm:
        label = f"vm{vm_index}"
        if label in seen_labels:
            continue
        spec = discover_vm_pid(vm_index)
        specs.append(spec)
        seen_labels.add(label)

    if not specs:
        parser.error("at least one --qemu-vm or --auto-discover-vm is required")

    threads: dict[str, list[VcpuThread]] = {}
    for spec in specs:
        vm_threads = kvm_threads(spec.pid)
        if not vm_threads:
            raise RuntimeError(f"no KVM vCPU threads found under {spec.label} pid {spec.pid}")
        threads[spec.label] = vm_threads

    before_time, before = snapshot(specs, threads)
    payload = run_payload(args, command)
    after_time, after = snapshot(specs, threads)
    result = build_result(specs, threads, before_time, before, after_time, after, payload)
    report = render_text_report(result)

    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    if args.output_text:
        args.output_text.parent.mkdir(parents=True, exist_ok=True)
        args.output_text.write_text(report, encoding="utf-8")
    print(report, end="")
    return int(payload["returncode"])


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"schedstat_probe: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
