#!/usr/bin/env python3
"""Run the exact four-node WOS physical-memory regression sequence."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import validate_wos_memory_balance as balance


ROOT = Path(__file__).resolve().parents[2]
CAPTURE = ROOT / "scripts" / "bench" / "capture_wos_memory_balance.sh"
SELFHOST = ROOT / "scripts" / "bench" / "run_wos_selfhost_repeatability.sh"
WOS_SSH = ROOT / "scripts" / "remote" / "wos_ssh.sh"
WOS_CLUSTER = ROOT / "bin" / "wos-cluster"
DEFAULT_CONFIG = ROOT / "configs" / "cluster_selfhost_4_host32.json"
DEFAULT_SYSTEMS = ["wos-0", "wos-1", "wos-2", "wos-3"]
DEFAULT_SERIAL_LOGS = [ROOT / f"serial-vm{index}.log" for index in range(4)]
BUILD_RUNS = 3
MAX_ACCOUNTING_OVERHEAD_PERCENT = 2.0
PACKET_POOL_PRESSURE_BUFFERS = 256
PRESSURE_RECLAIM_OWNERS = (
    "user_file_cache",
    "buffer_cache_data",
    "buffer_cache_metadata",
    "xfs_inode_metadata",
    "network_packet",
)
RECLAIM_TARGETS = (
    ("buffer_cache", "total_bytes", 0),
    ("packet_pool", "capacity", "baseline_capacity"),
    ("xfs_inode", "idle_inodes", 0),
    ("file_mmap_cache", "pages", 0),
)
SAFE_GUEST_WORKDIR = re.compile(r"/root/wos-selfhost-[A-Za-z0-9._-]+")
SERIAL_BOOT_ID = re.compile(r"WOS version=\S+ boot_id=([0-9a-fA-F]+)")


class RegressionError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def append_progress(path: Path, phase: str, status: str, detail: str = "") -> None:
    with path.open("a") as output:
        output.write(json.dumps({"timestamp_utc": utc_now(), "phase": phase, "status": status, "detail": detail}, sort_keys=True) + "\n")


def run_command(
    command: list[str],
    *,
    timeout: float | None = None,
    stdout_path: Path | None = None,
    cwd: Path = ROOT,
) -> subprocess.CompletedProcess[str]:
    if stdout_path is None:
        result = subprocess.run(command, cwd=cwd, text=True, capture_output=True, timeout=timeout, check=False)
    else:
        with stdout_path.open("w") as output:
            result = subprocess.run(command, cwd=cwd, text=True, stdout=output, stderr=subprocess.STDOUT, timeout=timeout, check=False)
    if result.returncode != 0:
        detail = result.stderr.strip() if stdout_path is None else f"see {stdout_path}"
        raise RegressionError(f"command failed ({result.returncode}): {' '.join(command)}: {detail}")
    return result


def repository_state(expected_commit: str) -> dict[str, Any]:
    commit_result = run_command(["git", "rev-parse", "HEAD"])
    commit = commit_result.stdout.strip()
    if not commit.startswith(expected_commit.lower()) and not expected_commit.lower().startswith(commit):
        raise RegressionError(f"--expected-commit {expected_commit} does not identify current HEAD {commit}")
    status_result = run_command(["git", "status", "--porcelain=v1", "--untracked-files=all"])
    if status_result.stdout:
        raise RegressionError("acceptance soak requires a clean committed worktree")
    return {"commit": commit, "worktree": "clean"}


def validate_config(path: Path, systems: list[str]) -> dict[str, Any]:
    try:
        config = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise RegressionError(f"failed to read cluster config {path}: {exc}") from exc
    zones = config.get("zones")
    if not isinstance(zones, list):
        raise RegressionError("cluster config has no zones array")
    node_zones = [zone for zone in zones if isinstance(zone, dict) and isinstance(zone.get("nodes"), int)]
    if not node_zones or any(zone["nodes"] != len(systems) for zone in node_zones):
        raise RegressionError(f"cluster config must define exactly {len(systems)} nodes in every network zone")
    if len(systems) != 4:
        raise RegressionError("acceptance regression requires exactly four WOS systems")
    global_zones = [zone for zone in zones if isinstance(zone, dict) and zone.get("id") == "GLOBAL"]
    if len(global_zones) != 1:
        raise RegressionError("cluster config must contain one GLOBAL zone")
    global_vm = global_zones[0].get("vm", {})
    if global_vm.get("memory") != "16384M" or global_vm.get("cpus") != 7:
        raise RegressionError("worker VM defaults must remain 16 GiB and 7 vCPUs")
    launcher_overrides = [
        node
        for zone in node_zones
        for node in zone.get("nodes_config", [])
        if isinstance(node, dict) and node.get("id") == 0
    ]
    if not launcher_overrides or any(node.get("vm", {}).get("memory") != "32768M" for node in launcher_overrides):
        raise RegressionError("VM0 must retain the host32 32 GiB memory override")
    return config


def parse_accepted_wall_times(path: Path) -> list[int]:
    if not path.is_file():
        raise RegressionError(f"overhead baseline table does not exist: {path}")
    with path.open(newline="") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    accepted: list[int] = []
    for row in rows:
        if row.get("accepted") != "1":
            continue
        value = row.get("wall_ms", "")
        if not value.isdecimal() or int(value) == 0:
            raise RegressionError(f"{path}: accepted row has invalid wall_ms={value!r}")
        accepted.append(int(value))
    if not accepted:
        raise RegressionError(f"{path}: no accepted baseline runs")
    return accepted


def overhead_result(baseline_wall_ms: list[int], measured_wall_ms: list[int]) -> dict[str, Any]:
    if not measured_wall_ms or any(value <= 0 for value in measured_wall_ms):
        raise RegressionError("measured self-host wall times are incomplete")
    baseline_median = float(statistics.median(baseline_wall_ms))
    measured_median = float(statistics.median(measured_wall_ms))
    overhead_percent = ((measured_median - baseline_median) / baseline_median) * 100.0
    if overhead_percent > MAX_ACCOUNTING_OVERHEAD_PERCENT:
        raise RegressionError(
            f"normal accounting overhead is {overhead_percent:.3f}%, limit is {MAX_ACCOUNTING_OVERHEAD_PERCENT:.3f}%"
        )
    return {
        "baseline_wall_ms": baseline_wall_ms,
        "measured_wall_ms": measured_wall_ms,
        "baseline_median_ms": baseline_median,
        "measured_median_ms": measured_median,
        "overhead_percent": overhead_percent,
        "limit_percent": MAX_ACCOUNTING_OVERHEAD_PERCENT,
        "status": "pass",
    }


def parse_reclaim_rows(memacc_path: Path) -> dict[str, dict[str, str]]:
    sections = balance.parse_sections(memacc_path.read_text(), str(memacc_path))
    rows: dict[str, dict[str, str]] = {}
    for target, _, _ in RECLAIM_TARGETS:
        section_name = f"reclaim/{target}"
        section = sections.get(section_name, [])
        matches = [values for record, values in section if record == "reclaim" and values.get("name") == target]
        if len(matches) != 1:
            raise RegressionError(f"{memacc_path}: expected one {section_name} reclaim row")
        rows[target] = matches[0]
    return rows


def validate_reclaim(
    pressure_snapshot: dict[str, Any],
    after_snapshot: dict[str, Any],
    endpoint_before_dir: Path,
    after_dir: Path,
    systems: list[str],
) -> dict[str, Any]:
    owner_results: dict[str, dict[str, int | str]] = {}
    for owner_name in PRESSURE_RECLAIM_OWNERS:
        before_pages = 0
        after_pages = 0
        for host in systems:
            before_owners = {row["name"]: row for row in pressure_snapshot["nodes"][host]["owners"]}
            after_owners = {row["name"]: row for row in after_snapshot["nodes"][host]["owners"]}
            if owner_name not in before_owners or owner_name not in after_owners:
                raise RegressionError(f"reclaim evidence is missing physical owner {owner_name}")
            if before_owners[owner_name]["reclaimability"] != "pressure_reclaim":
                raise RegressionError(f"{owner_name} is not exported as pressure_reclaim")
            before_pages += int(before_owners[owner_name]["pages"])
            after_pages += int(after_owners[owner_name]["pages"])
        if before_pages == 0:
            raise RegressionError(f"reclaim workload did not populate {owner_name}")
        if after_pages >= before_pages:
            raise RegressionError(f"{owner_name} did not release pages under controlled reclaim ({before_pages} -> {after_pages})")
        owner_results[owner_name] = {
            "before_pages": before_pages,
            "after_pages": after_pages,
            "released_pages": before_pages - after_pages,
            "status": "pass",
        }

    endpoint_results: dict[str, dict[str, Any]] = {}
    for target, metric, floor_spec in RECLAIM_TARGETS:
        before_total = 0
        after_total = 0
        floor_total = 0
        for host in systems:
            before_rows = parse_reclaim_rows(endpoint_before_dir / f"{host}-memacc-all.txt")
            after_rows = parse_reclaim_rows(after_dir / f"{host}-memacc-all.txt")
            before_row = before_rows[target]
            after_row = after_rows[target]
            before_total += balance.required_u64(before_row, metric, f"{host}.{target}.before")
            after_total += balance.required_u64(after_row, metric, f"{host}.{target}.after")
            if isinstance(floor_spec, str):
                floor_total += balance.required_u64(before_row, floor_spec, f"{host}.{target}.floor")
            else:
                floor_total += floor_spec
        if after_total > before_total:
            raise RegressionError(f"{target} reclaim metric grew ({before_total} -> {after_total})")
        if before_total > floor_total and after_total >= before_total:
            raise RegressionError(f"{target} reclaim endpoint released nothing above its floor")
        endpoint_results[target] = {
            "metric": metric,
            "before": before_total,
            "after": after_total,
            "floor": floor_total,
            "released": before_total - after_total,
            "status": "pass",
        }
    return {"owners": owner_results, "endpoints": endpoint_results, "status": "pass"}


def capture_checkpoint(output_dir: Path, label: str, systems: list[str], quiescent: bool, timeout: float) -> dict[str, Any]:
    command = [
        str(CAPTURE),
        "--systems",
        ",".join(systems),
        "--output-dir",
        str(output_dir),
        "--label",
        label,
    ]
    if quiescent:
        command.append("--require-quiescent")
    run_command(command, timeout=timeout)
    return json.loads((output_dir / label / "snapshot.json").read_text())


def wait_for_hosts(systems: list[str], timeout: float, progress_path: Path) -> None:
    deadline = time.monotonic() + timeout
    pending = set(systems)
    while pending and time.monotonic() < deadline:
        for host in list(pending):
            try:
                result = subprocess.run(
                    [str(WOS_SSH), host, "/usr/bin/hostname"],
                    cwd=ROOT,
                    text=True,
                    capture_output=True,
                    timeout=10,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                continue
            if result.returncode == 0 and result.stdout.strip() == host:
                pending.remove(host)
                append_progress(progress_path, "cluster_reachability", "node_ready", host)
        if pending:
            time.sleep(2)
    if pending:
        raise RegressionError(f"cluster nodes did not become reachable: {','.join(sorted(pending))}")


def read_serial_boot_ids(
    serial_logs: list[Path],
    systems: list[str],
    minimum_mtime: float | None = None,
) -> dict[str, str]:
    boot_ids: dict[str, str] = {}
    for host, path in zip(systems, serial_logs, strict=True):
        try:
            stat = path.stat()
            text = path.read_text(errors="replace")
        except OSError as exc:
            raise RegressionError(f"cannot read {host} serial log {path}: {exc}") from exc
        if minimum_mtime is not None and stat.st_mtime < minimum_mtime - 1.0:
            raise RegressionError(f"{host} serial log was not written by the fresh cluster launch")
        matches = SERIAL_BOOT_ID.findall(text)
        if not matches:
            raise RegressionError(f"{host} serial log contains no WOS boot ID")
        boot_ids[host] = matches[-1].lower()
    return boot_ids


def require_unchanged_boot_ids(
    serial_logs: list[Path],
    systems: list[str],
    expected: dict[str, str],
    context: str,
) -> None:
    current = read_serial_boot_ids(serial_logs, systems)
    if current != expected:
        raise RegressionError(f"{context}: cluster boot IDs changed: expected={expected} current={current}")


def start_cluster(config: Path, log_path: Path) -> tuple[subprocess.Popen[str], Any]:
    output = log_path.open("w")
    process = subprocess.Popen(
        [str(WOS_CLUSTER), "--config", str(config), "--launch", "--no-setup"],
        cwd=ROOT,
        text=True,
        stdout=output,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    return process, output


def stop_cluster(process: subprocess.Popen[str] | None, output: Any | None) -> None:
    if process is not None and process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
    if output is not None:
        output.close()


def selfhost_command(args: argparse.Namespace, output_dir: Path, run_number: int) -> list[str]:
    command = [
        str(SELFHOST),
        "--runs",
        "1",
        "--host",
        args.submitter,
        "--jobs",
        str(args.jobs),
        "--workdir",
        args.workdir,
        "--output-dir",
        str(output_dir / f"selfhost-build-{run_number}"),
        "--serial-log",
        str(args.serial_logs[0]),
        "--distributed-hosts",
        ",".join(args.systems),
        "--distributed-serial-logs",
        ",".join(str(path) for path in args.serial_logs),
        "--expected-commit",
        args.expected_commit,
        "--repo",
        args.repo,
        "--run-timeout-seconds",
        str(args.build_timeout_seconds),
    ]
    if args.mirror_file is not None:
        command.extend(["--mirror-file", str(args.mirror_file)])
    if args.distdir is not None:
        command.extend(["--distdir", args.distdir])
    return command


def read_selfhost_wall_ms(path: Path, expected_submitter_boot_id: str) -> int:
    with (path / "runs.tsv").open(newline="") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if len(rows) != 1 or rows[0].get("accepted") != "1" or not rows[0].get("wall_ms", "").isdecimal():
        raise RegressionError(f"{path}: self-host run was not accepted")
    summary = json.loads((path / "summary.json").read_text())
    if summary.get("pass") != 1 or summary.get("same_boot") != 1 or summary.get("accepted_runs") != 1:
        raise RegressionError(f"{path}: self-host summary did not prove one accepted unchanged-boot run")
    if summary.get("boot_id", "").lower() != expected_submitter_boot_id:
        raise RegressionError(f"{path}: self-host summary boot ID does not match the acceptance boot")
    return int(rows[0]["wall_ms"])


def delete_guest_worktree(args: argparse.Namespace) -> None:
    if SAFE_GUEST_WORKDIR.fullmatch(args.workdir) is None or args.workdir in {"/root", "/root/."}:
        raise RegressionError(f"unsafe guest workdir for cleanup: {args.workdir}")
    command = (
        f"test -d '{args.workdir}' && test ! -L '{args.workdir}' && "
        f"rm -rf -- '{args.workdir}' && /usr/bin/sync"
    )
    run_command([str(WOS_SSH), args.submitter, command], timeout=args.command_timeout_seconds)


def sync_all_nodes(args: argparse.Namespace) -> None:
    for host in args.systems:
        run_command([str(WOS_SSH), host, "/usr/bin/sync"], timeout=args.command_timeout_seconds)


def run_pressure(args: argparse.Namespace, output_dir: Path) -> subprocess.Popen[str]:
    output = (output_dir / "controlled-pressure.log").open("w")
    process = subprocess.Popen(
        [
            str(WOS_SSH),
            args.submitter,
            "/usr/bin/testprog",
            "memory-pressure",
            "--bytes",
            args.pressure_bytes,
            "--hold-seconds",
            str(args.pressure_hold_seconds),
        ],
        cwd=ROOT,
        text=True,
        stdout=output,
        stderr=subprocess.STDOUT,
    )
    setattr(process, "_wos_output", output)
    deadline = time.monotonic() + args.pressure_ready_timeout_seconds
    log_path = output_dir / "controlled-pressure.log"
    while time.monotonic() < deadline:
        output.flush()
        if "memory_pressure_ready" in log_path.read_text():
            return process
        if process.poll() is not None:
            output.close()
            raise RegressionError(f"controlled pressure exited before readiness with status {process.returncode}")
        time.sleep(1)
    process.terminate()
    process.wait(timeout=10)
    output.close()
    raise RegressionError("controlled pressure did not report readiness")


def finish_pressure(process: subprocess.Popen[str], timeout: float) -> None:
    try:
        returncode = process.wait(timeout=timeout)
    finally:
        output = getattr(process, "_wos_output", None)
        if output is not None:
            output.close()
    if returncode != 0:
        raise RegressionError(f"controlled pressure exited with status {returncode}")


def run_reclaim(args: argparse.Namespace, output_dir: Path) -> None:
    reclaim_dir = output_dir / "reclaim-commands"
    reclaim_dir.mkdir()
    for host in args.systems:
        run_command([str(WOS_SSH), host, "/usr/bin/sync"], timeout=args.command_timeout_seconds)
        for target, _, _ in RECLAIM_TARGETS:
            run_command(
                [str(WOS_SSH), host, "/usr/bin/memacc", "reclaim", target, "all"],
                timeout=args.command_timeout_seconds,
                stdout_path=reclaim_dir / f"{host}-{target}.txt",
            )


def populate_packet_pool_for_pressure(args: argparse.Namespace, output_dir: Path) -> dict[str, Any]:
    populate_dir = output_dir / "packet-pool-pressure-populate"
    populate_dir.mkdir()
    nodes: dict[str, dict[str, int | str]] = {}
    for host in args.systems:
        path = populate_dir / f"{host}.txt"
        run_command(
            [
                str(WOS_SSH),
                host,
                "/usr/bin/memacc",
                "reclaim",
                "packet_pool",
                f"grow={PACKET_POOL_PRESSURE_BUFFERS}",
            ],
            timeout=args.command_timeout_seconds,
            stdout_path=path,
        )
        values = parse_packet_pool_reclaim_command(path.read_text(), str(path))
        if values["after"] < values["before"] + PACKET_POOL_PRESSURE_BUFFERS:
            raise RegressionError(
                f"{host}: packet-pool pressure stimulus did not add {PACKET_POOL_PRESSURE_BUFFERS} buffers "
                f"({values['before']} -> {values['after']})"
            )
        nodes[host] = {
            "before": values["before"],
            "after": values["after"],
            "added": values["after"] - values["before"],
            "baseline": values["baseline"],
            "status": "pass",
        }
    return {"requested_buffers_per_node": PACKET_POOL_PRESSURE_BUFFERS, "nodes": nodes, "status": "pass"}


def parse_packet_pool_reclaim_command(text: str, context: str) -> dict[str, int]:
    lines = [line.strip() for line in text.splitlines() if line.strip().startswith("packet_pool ")]
    if len(lines) != 1:
        raise RegressionError(f"{context}: expected one packet_pool reclaim result")
    values: dict[str, int] = {}
    for key, raw_value in re.findall(r"(?:^|\s)([a-z_]+)=([0-9]+)(?=\s|$)", lines[0]):
        if key in values:
            raise RegressionError(f"{context}: duplicate packet_pool field {key}")
        values[key] = int(raw_value)
    for key in ("before", "after", "baseline", "draining", "draining_free"):
        if key not in values:
            raise RegressionError(f"{context}: packet_pool result is missing {key}")
    if values["after"] < values["baseline"]:
        raise RegressionError(f"{context}: packet_pool capacity fell below its permanent baseline")
    if values["draining_free"] > values["draining"]:
        raise RegressionError(f"{context}: packet_pool draining free count exceeds draining capacity")
    return values


def packet_pool_reclaim_complete(values: dict[str, int]) -> bool:
    return values["after"] == values["baseline"] and values["draining"] == 0


def finish_packet_pool_reclaim(args: argparse.Namespace, output_dir: Path, started: float) -> dict[str, Any]:
    reclaim_dir = output_dir / "reclaim-commands"
    deadline = started + args.post_reclaim_quiescence_seconds
    pending = set(args.systems)
    attempts = {host: 1 for host in args.systems}
    last: dict[str, dict[str, int]] = {}

    for host in args.systems:
        path = reclaim_dir / f"{host}-packet_pool.txt"
        last[host] = parse_packet_pool_reclaim_command(path.read_text(), str(path))
        if packet_pool_reclaim_complete(last[host]):
            pending.remove(host)

    while pending:
        if time.monotonic() >= deadline:
            states = ", ".join(
                f"{host}:after={last[host]['after']},baseline={last[host]['baseline']},"
                f"draining={last[host]['draining']},draining_free={last[host]['draining_free']}"
                for host in sorted(pending)
            )
            raise RegressionError(f"packet_pool reclaim did not finish within {args.post_reclaim_quiescence_seconds}s ({states})")
        for host in sorted(pending):
            attempts[host] += 1
            path = reclaim_dir / f"{host}-packet_pool-attempt-{attempts[host]}.txt"
            run_command(
                [str(WOS_SSH), host, "/usr/bin/memacc", "reclaim", "packet_pool", "all"],
                timeout=args.command_timeout_seconds,
                stdout_path=path,
            )
            last[host] = parse_packet_pool_reclaim_command(path.read_text(), str(path))
            if packet_pool_reclaim_complete(last[host]):
                pending.remove(host)
        if pending:
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))

    remaining = deadline - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)
    return {
        "attempts": attempts,
        "final": last,
        "bound_seconds": args.post_reclaim_quiescence_seconds,
        "status": "pass",
    }


def inventory(output_dir: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for path in sorted(output_dir.rglob("*")):
        if path.is_file() and path.name != "manifest.json":
            result[str(path.relative_to(output_dir))] = sha256_file(path)
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--systems", type=balance.parse_systems, default=DEFAULT_SYSTEMS)
    parser.add_argument("--submitter", default="wos-0")
    parser.add_argument("--serial-logs", type=lambda value: [Path(item) for item in value.split(",")], default=DEFAULT_SERIAL_LOGS)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--overhead-baseline-runs-tsv", required=True, type=Path)
    parser.add_argument("--repo", default="https://github.com/Pascu-Victor/wos.git")
    parser.add_argument("--mirror-file", type=Path)
    parser.add_argument("--distdir")
    parser.add_argument("--workdir", default="/root/wos-selfhost-memory-balance")
    parser.add_argument("--jobs", type=int, default=36)
    parser.add_argument("--boot-timeout-seconds", type=int, default=300)
    parser.add_argument("--boot-settle-seconds", type=int, default=30)
    parser.add_argument("--build-timeout-seconds", type=int, default=7200)
    parser.add_argument("--command-timeout-seconds", type=int, default=300)
    parser.add_argument("--quiescence-seconds", type=int, default=300)
    parser.add_argument("--post-reclaim-quiescence-seconds", type=int, default=30)
    parser.add_argument("--pressure-bytes", default="24g")
    parser.add_argument("--pressure-hold-seconds", type=int, default=180)
    parser.add_argument("--pressure-ready-timeout-seconds", type=int, default=60)
    parser.add_argument("--use-running-cluster", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if args.submitter not in args.systems:
        parser.error("--submitter must be one of --systems")
    if len(args.serial_logs) != len(args.systems):
        parser.error("--serial-logs must contain one path per system")
    if not re.fullmatch(r"[0-9a-fA-F]{7,40}", args.expected_commit):
        parser.error("--expected-commit must be a 7-40 digit hexadecimal commit")
    for name in (
        "jobs",
        "boot_timeout_seconds",
        "build_timeout_seconds",
        "command_timeout_seconds",
        "quiescence_seconds",
        "post_reclaim_quiescence_seconds",
        "pressure_hold_seconds",
        "pressure_ready_timeout_seconds",
    ):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.boot_settle_seconds < 0:
        parser.error("--boot-settle-seconds must be nonnegative")
    if SAFE_GUEST_WORKDIR.fullmatch(args.workdir) is None:
        parser.error("--workdir must be a normalized /root/wos-selfhost-* scratch path")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    args.config = args.config.resolve()
    args.overhead_baseline_runs_tsv = args.overhead_baseline_runs_tsv.resolve()
    args.serial_logs = [path.resolve() for path in args.serial_logs]
    config = validate_config(args.config, args.systems)
    baseline_wall_ms = parse_accepted_wall_times(args.overhead_baseline_runs_tsv)

    if args.output_dir is None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        args.output_dir = ROOT / "benchmarks" / "results" / f"wos-memory-balance-regression-{stamp}"
    else:
        args.output_dir = args.output_dir.resolve()
    if args.output_dir.exists():
        print(f"error: output directory already exists: {args.output_dir}", file=sys.stderr)
        return 2
    if args.dry_run:
        print(
            json.dumps(
                {
                    "build_runs": BUILD_RUNS,
                    "checkpoints": [
                        "clean-boot",
                        "after-successful-build-1",
                        "after-successful-build-2",
                        "after-successful-build-3",
                        "after-build-tree-delete",
                        "after-five-minute-quiescence",
                        "during-controlled-pressure",
                        "before-explicit-reclaim",
                        "after-controlled-pressure-reclaim",
                    ],
                    "config": str(args.config),
                    "systems": args.systems,
                    "use_running_cluster": args.use_running_cluster,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    source_state = repository_state(args.expected_commit)
    args.expected_commit = source_state["commit"]
    args.output_dir.mkdir(parents=True)
    progress_path = args.output_dir / "progress.jsonl"
    config_copy = args.output_dir / "cluster-config.json"
    shutil.copy2(args.config, config_copy)
    manifest: dict[str, Any] = {
        "schema": 1,
        "started_utc": utc_now(),
        "status": "running",
        "config": {
            "path": str(args.config),
            "sha256": sha256_file(args.config),
            "snapshot": config,
        },
        "systems": args.systems,
        "submitter": args.submitter,
        "jobs": args.jobs,
        "repository": args.repo,
        "mirror_file": str(args.mirror_file.resolve()) if args.mirror_file is not None else None,
        "distdir": args.distdir,
        "expected_commit": args.expected_commit,
        "source": source_state,
        "build_runs": BUILD_RUNS,
        "same_boot_required": True,
        "quiescence_seconds": args.quiescence_seconds,
        "post_reclaim_quiescence_seconds": args.post_reclaim_quiescence_seconds,
        "pressure": {"bytes": args.pressure_bytes, "hold_seconds": args.pressure_hold_seconds},
        "overhead_baseline": {
            "runs_tsv": str(args.overhead_baseline_runs_tsv),
            "sha256": sha256_file(args.overhead_baseline_runs_tsv),
        },
        "checkpoints": [],
        "failures": [],
    }
    write_json(args.output_dir / "manifest.json", manifest)

    cluster_process: subprocess.Popen[str] | None = None
    cluster_output: Any | None = None
    pressure_process: subprocess.Popen[str] | None = None
    cluster_launch_epoch: float | None = None
    try:
        if not args.use_running_cluster:
            append_progress(progress_path, "cluster", "start", str(args.config))
            cluster_launch_epoch = time.time()
            cluster_process, cluster_output = start_cluster(args.config, args.output_dir / "cluster.log")
        else:
            append_progress(progress_path, "cluster", "using_running_cluster")
        wait_for_hosts(args.systems, args.boot_timeout_seconds, progress_path)
        if cluster_process is not None and cluster_process.poll() is not None:
            raise RegressionError(f"fresh cluster launcher exited with status {cluster_process.returncode}")
        if args.boot_settle_seconds:
            time.sleep(args.boot_settle_seconds)
        initial_boot_ids = read_serial_boot_ids(args.serial_logs, args.systems, cluster_launch_epoch)
        manifest["boot_ids"] = initial_boot_ids
        write_json(args.output_dir / "manifest.json", manifest)

        def checkpoint(label: str, quiescent: bool) -> dict[str, Any]:
            append_progress(progress_path, label, "start")
            snapshot = capture_checkpoint(args.output_dir, label, args.systems, quiescent, args.command_timeout_seconds)
            require_unchanged_boot_ids(args.serial_logs, args.systems, initial_boot_ids, label)
            manifest["checkpoints"].append(
                {
                    "label": label,
                    "timestamp_utc": snapshot["timestamp_utc"],
                    "require_quiescent": quiescent,
                    "snapshot_json": f"{label}/snapshot.json",
                    "status": "pass",
                }
            )
            write_json(args.output_dir / "manifest.json", manifest)
            append_progress(progress_path, label, "pass")
            return snapshot

        checkpoint("clean-boot", True)
        measured_wall_ms: list[int] = []
        for run_number in range(1, BUILD_RUNS + 1):
            phase = f"selfhost-build-{run_number}"
            append_progress(progress_path, phase, "start")
            result_dir = args.output_dir / phase
            run_command(
                selfhost_command(args, args.output_dir, run_number),
                timeout=args.build_timeout_seconds + 600,
                stdout_path=args.output_dir / f"{phase}.log",
            )
            measured_wall_ms.append(read_selfhost_wall_ms(result_dir, initial_boot_ids[args.submitter]))
            require_unchanged_boot_ids(args.serial_logs, args.systems, initial_boot_ids, phase)
            append_progress(progress_path, phase, "pass", f"wall_ms={measured_wall_ms[-1]}")
            checkpoint(f"after-successful-build-{run_number}", True)

        manifest["accounting_overhead"] = overhead_result(baseline_wall_ms, measured_wall_ms)
        write_json(args.output_dir / "manifest.json", manifest)

        append_progress(progress_path, "build-tree-delete", "start", args.workdir)
        delete_guest_worktree(args)
        sync_all_nodes(args)
        append_progress(progress_path, "build-tree-delete", "pass")
        checkpoint("after-build-tree-delete", True)

        append_progress(progress_path, "five-minute-quiescence", "start", f"seconds={args.quiescence_seconds}")
        time.sleep(args.quiescence_seconds)
        checkpoint("after-five-minute-quiescence", True)

        append_progress(progress_path, "controlled-pressure", "start", args.pressure_bytes)
        pressure_process = run_pressure(args, args.output_dir)
        append_progress(progress_path, "packet-pool-pressure-populate", "start", f"buffers={PACKET_POOL_PRESSURE_BUFFERS}")
        manifest["packet_pool_pressure_population"] = populate_packet_pool_for_pressure(args, args.output_dir)
        write_json(args.output_dir / "manifest.json", manifest)
        append_progress(progress_path, "packet-pool-pressure-populate", "pass")
        pressure_snapshot = checkpoint("during-controlled-pressure", False)
        finish_pressure(pressure_process, args.pressure_hold_seconds + args.command_timeout_seconds)
        pressure_process = None
        append_progress(progress_path, "controlled-pressure", "released")

        before_reclaim = checkpoint("before-explicit-reclaim", True)
        append_progress(progress_path, "explicit-reclaim", "start")
        reclaim_started = time.monotonic()
        run_reclaim(args, args.output_dir)
        manifest["packet_pool_reclaim_completion"] = finish_packet_pool_reclaim(args, args.output_dir, reclaim_started)
        write_json(args.output_dir / "manifest.json", manifest)
        append_progress(progress_path, "explicit-reclaim", "commands_pass_and_packet_pool_drained")
        after_reclaim = checkpoint("after-controlled-pressure-reclaim", True)
        manifest["reclaim"] = validate_reclaim(
            pressure_snapshot,
            after_reclaim,
            args.output_dir / "before-explicit-reclaim",
            args.output_dir / "after-controlled-pressure-reclaim",
            args.systems,
        )
        manifest["completed_utc"] = utc_now()
        manifest["status"] = "pass"
        append_progress(progress_path, "regression", "pass")
        manifest["evidence_sha256"] = inventory(args.output_dir)
        write_json(args.output_dir / "manifest.json", manifest)
        print(f"status=pass evidence={args.output_dir}")
        return 0
    except (OSError, ValueError, json.JSONDecodeError, subprocess.TimeoutExpired, RegressionError, balance.ValidationError) as exc:
        manifest["completed_utc"] = utc_now()
        manifest["status"] = "fail"
        manifest["failures"].append({"classification": type(exc).__name__, "message": str(exc)})
        append_progress(progress_path, "regression", "fail", str(exc))
        manifest["evidence_sha256"] = inventory(args.output_dir)
        write_json(args.output_dir / "manifest.json", manifest)
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        if pressure_process is not None and pressure_process.poll() is None:
            pressure_process.terminate()
            try:
                pressure_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pressure_process.kill()
            output = getattr(pressure_process, "_wos_output", None)
            if output is not None:
                output.close()
        stop_cluster(cluster_process, cluster_output)


if __name__ == "__main__":
    raise SystemExit(main())
