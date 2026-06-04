#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
BENCH_SCRIPTS = ROOT / "scripts" / "bench"
REMOTE_SCRIPTS = ROOT / "scripts" / "remote"


@dataclass(frozen=True)
class RenderCase:
    name: str
    width: int
    height: int
    spp: int
    wos_scene: str | None
    linux_scene: str | None


@dataclass
class HostKvmTraceRun:
    tool: str
    step_name: str
    command: list[str]
    data_path: Path
    stdout_path: Path
    stderr_path: Path
    metadata_path: Path
    process: subprocess.Popen[str] | None = None
    stdout_handle: Any | None = None
    stderr_handle: Any | None = None
    start_monotonic: float = 0.0
    status: str = "created"
    error: str | None = None
    returncode: int | None = None
    stopped_by: str | None = None


@dataclass
class HostKvmTraceSession:
    step_name: str
    runs: list[HostKvmTraceRun]
    completed: list[dict[str, Any]]


def run_command(
    args: list[str],
    *,
    cwd: Path = ROOT,
    input_text: str | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            args,
            cwd=cwd,
            check=False,
            text=True,
            capture_output=True,
            input=input_text,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        message = [f"command timed out after {timeout:.1f}s: {shlex.join(args)}"]
        if exc.stdout:
            message.append("stdout:")
            message.append(str(exc.stdout).strip())
        else:
            message.append("stdout: (empty)")
        if exc.stderr:
            message.append("stderr:")
            message.append(str(exc.stderr).strip())
        else:
            message.append("stderr: (empty)")
        raise RuntimeError("\n".join(message)) from exc
    if result.returncode != 0:
        message = [f"command failed with exit code {result.returncode}: {shlex.join(args)}"]
        if result.stdout.strip():
            message.append("stdout:")
            message.append(result.stdout.strip())
        else:
            message.append("stdout: (empty)")
        if result.stderr.strip():
            message.append("stderr:")
            message.append(result.stderr.strip())
        else:
            message.append("stderr: (empty)")
        raise RuntimeError("\n".join(message))
    return result


def wos_remote_command(host: str, command: str, *, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
    return run_command([str(REMOTE_SCRIPTS / "wos_ssh.sh"), host, command], timeout=timeout)


def wos_benchmark_pids(host: str, *, timeout: float) -> list[int]:
    result = wos_remote_command(host, "ps aux 2>/dev/null || ps", timeout=timeout)
    pids: list[int] = []
    for line in result.stdout.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("PID "):
            continue
        fields = stripped.split(maxsplit=4)
        if len(fields) < 5 or not fields[0].isdigit():
            continue
        command = fields[4].split("\0", 1)[0].split()[0]
        basename = os.path.basename(command)
        if basename in {"renderbench", "testprog"}:
            pids.append(int(fields[0]))
    return pids


def kill_wos_pids(host: str, pids: list[int], signal: str, *, timeout: float) -> None:
    if not pids:
        return
    command = "kill -" + signal + " " + " ".join(str(pid) for pid in pids) + " 2>/dev/null || true"
    wos_remote_command(host, command, timeout=timeout)


def prepare_wos_hosts(args: argparse.Namespace, hosts: list[str]) -> None:
    if args.skip_wos_cleanup:
        return

    failures: list[str] = []
    for host in hosts:
        try:
            wos_remote_command(host, "hostname >/dev/null", timeout=args.wos_preflight_timeout)
        except RuntimeError as exc:
            failures.append(f"{host}: unreachable ({exc})")
            continue

        pids = wos_benchmark_pids(host, timeout=args.wos_preflight_timeout)
        if not pids:
            continue

        print(f"[suite] cleanup {host}: stale benchmark pids {','.join(str(pid) for pid in pids)}")
        kill_wos_pids(host, pids, "TERM", timeout=args.wos_preflight_timeout)
        remaining = wos_benchmark_pids(host, timeout=args.wos_preflight_timeout)
        remaining = [pid for pid in remaining if pid in pids]
        if remaining:
            kill_wos_pids(host, remaining, "KILL", timeout=args.wos_preflight_timeout)
            remaining = wos_benchmark_pids(host, timeout=args.wos_preflight_timeout)
            remaining = [pid for pid in remaining if pid in pids]
        if remaining:
            failures.append(f"{host}: stale benchmark pids did not exit: {','.join(str(pid) for pid in remaining)}")

    if failures:
        raise RuntimeError("WOS preflight failed:\n" + "\n".join(failures))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def relpath(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def step_log_text(result: subprocess.CompletedProcess[str]) -> str:
    return (
        "=== stdout ===\n"
        + (result.stdout if result.stdout else "")
        + "\n=== stderr ===\n"
        + (result.stderr if result.stderr else "")
    )


def fetch_remote_file(fetcher: Path, host: str, remote_path: str, local_path: Path) -> None:
    run_command([str(fetcher), host, remote_path, str(local_path)])


def parse_mandel_report(text: str) -> dict[str, Any]:
    line = ""
    for candidate in text.splitlines():
        stripped = candidate.strip()
        if stripped.startswith("name="):
            line = stripped
    if not line:
        raise RuntimeError("mandelbench report.txt did not contain a summary line")

    out: dict[str, Any] = {"summary_line": line}
    for token in line.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        if value.replace(".", "", 1).isdigit():
            if "." in value:
                out[key] = float(value)
            else:
                out[key] = int(value)
        else:
            out[key] = value
    return out


def render_cases(args: argparse.Namespace) -> list[RenderCase]:
    return [
        RenderCase(
            name="default-scene",
            width=args.default_render_width,
            height=args.default_render_height,
            spp=args.default_render_spp,
            wos_scene=None,
            linux_scene=None,
        ),
        RenderCase(
            name="duck",
            width=args.duck_render_width,
            height=args.duck_render_height,
            spp=args.duck_render_spp,
            wos_scene=args.wos_duck_scene,
            linux_scene=args.linux_duck_scene,
        ),
    ]


def render_placements(raw: str) -> list[str]:
    placements = [item.strip() for item in raw.split(",") if item.strip()]
    valid = {"node-threads", "process-per-core"}
    invalid = [item for item in placements if item not in valid]
    if invalid:
        raise ValueError(f"invalid render placements: {', '.join(invalid)}")
    if not placements:
        raise ValueError("at least one render placement is required")
    return placements


def parse_schedstat_qemu_pid(raw: str) -> str:
    if ":" not in raw:
        raise argparse.ArgumentTypeError("expected VM:PID, for example vm0:12345 or 0:12345")
    raw_label, raw_pid = raw.split(":", 1)
    if not raw_label:
        raise argparse.ArgumentTypeError("VM label cannot be empty")
    if not raw_pid.isdigit() or int(raw_pid) <= 0:
        raise argparse.ArgumentTypeError("PID must be a positive integer")
    return raw


def parse_host_kvm_trace_modes(raw: str) -> list[str]:
    normalized = raw.strip().lower()
    if normalized in {"", "none", "off", "false", "0"}:
        return []
    if normalized == "both":
        return ["perf", "trace-cmd"]

    modes: list[str] = []
    aliases = {"trace": "trace-cmd", "tracecmd": "trace-cmd"}
    valid = {"perf", "trace-cmd"}
    for item in normalized.split(","):
        mode = aliases.get(item.strip(), item.strip())
        if not mode:
            continue
        if mode not in valid:
            raise ValueError(f"invalid host KVM trace mode: {item}")
        if mode not in modes:
            modes.append(mode)
    return modes


def split_host_tool_command(raw: str) -> list[str]:
    command = shlex.split(raw)
    if not command:
        raise ValueError("host trace command must not be empty")
    return command


def command_exists(command: list[str]) -> bool:
    executable = command[0]
    if os.sep in executable:
        return os.access(executable, os.X_OK)
    return shutil.which(executable) is not None


def repeated_trace_event_args(raw: str) -> list[str]:
    args: list[str] = []
    for event in raw.split(","):
        event = event.strip()
        if event:
            args += ["-e", event]
    return args


class HostKvmTracer:
    def __init__(self, args: argparse.Namespace, suite_dir: Path) -> None:
        self.modes: list[str] = args.host_kvm_trace_modes
        self.trace_dir = suite_dir / "host-kvm-tracing"
        self.perf_command = split_host_tool_command(args.host_kvm_perf_cmd)
        self.trace_command = split_host_tool_command(args.host_kvm_trace_cmd)
        self.perf_events = args.host_kvm_perf_events
        self.trace_events = args.host_kvm_trace_events
        self.trace_buffer_kb = args.host_kvm_trace_buffer_kb
        self.startup_grace_seconds = args.host_kvm_trace_startup_grace
        self.stop_timeout_seconds = args.host_kvm_trace_stop_timeout
        self.max_seconds = args.host_kvm_trace_max_seconds
        self.records: list[dict[str, Any]] = []

    @property
    def enabled(self) -> bool:
        return bool(self.modes)

    def config_summary(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "modes": self.modes,
            "perf_command": self.perf_command,
            "perf_events": self.perf_events,
            "trace_command": self.trace_command,
            "trace_events": self.trace_events,
            "trace_buffer_kb": self.trace_buffer_kb,
            "max_seconds": self.max_seconds,
        }

    def start(self, step_name: str) -> HostKvmTraceSession | None:
        if not self.enabled:
            return None

        step_dir = self.trace_dir / step_name
        step_dir.mkdir(parents=True, exist_ok=True)
        session = HostKvmTraceSession(step_name=step_name, runs=[], completed=[])
        for mode in self.modes:
            run = self._create_run(mode, step_name, step_dir)
            if not command_exists(self._tool_command(mode)):
                run.status = "skipped"
                run.error = f"tool not found or not executable: {self._tool_command(mode)[0]}"
                session.completed.append(self._finalize(run))
                continue

            self._start_run(run)
            if run.process is None:
                session.completed.append(self._finalize(run))
            else:
                session.runs.append(run)
        return session

    def stop(self, session: HostKvmTraceSession | None) -> list[dict[str, Any]]:
        if session is None:
            return []

        entries = list(session.completed)
        for run in session.runs:
            self._stop_run(run)
            entries.append(self._finalize(run))
        return entries

    def summary_step(self) -> dict[str, Any]:
        return {
            "name": "host-kvm-tracing",
            "ok": True,
            "host": "localhost",
            "config": self.config_summary(),
            "records": self.records,
            "artifacts": [
                artifact
                for record in self.records
                for artifact in record.get("artifacts", [])
            ],
        }

    def _tool_command(self, mode: str) -> list[str]:
        if mode == "perf":
            return self.perf_command
        if mode == "trace-cmd":
            return self.trace_command
        raise ValueError(f"unknown host KVM trace mode: {mode}")

    def _create_run(self, mode: str, step_name: str, step_dir: Path) -> HostKvmTraceRun:
        if mode == "perf":
            data_path = step_dir / "perf-kvm.data"
            command = [
                *self.perf_command,
                "record",
                "-a",
                "-g",
                "-e",
                self.perf_events,
                "-o",
                str(data_path),
                "--",
                "sleep",
                str(self.max_seconds),
            ]
        elif mode == "trace-cmd":
            data_path = step_dir / "trace-kvm.dat"
            command = [
                *self.trace_command,
                "record",
                "-b",
                str(self.trace_buffer_kb),
                *repeated_trace_event_args(self.trace_events),
                "-o",
                str(data_path),
            ]
        else:
            raise ValueError(f"unknown host KVM trace mode: {mode}")

        return HostKvmTraceRun(
            tool=mode,
            step_name=step_name,
            command=command,
            data_path=data_path,
            stdout_path=step_dir / f"{mode}.stdout.log",
            stderr_path=step_dir / f"{mode}.stderr.log",
            metadata_path=step_dir / f"{mode}.json",
        )

    def _start_run(self, run: HostKvmTraceRun) -> None:
        run.stdout_handle = run.stdout_path.open("w", encoding="utf-8")
        run.stderr_handle = run.stderr_path.open("w", encoding="utf-8")
        run.start_monotonic = time.monotonic()
        try:
            run.process = subprocess.Popen(
                run.command,
                cwd=ROOT,
                stdout=run.stdout_handle,
                stderr=run.stderr_handle,
                text=True,
                start_new_session=True,
            )
        except OSError as exc:
            run.status = "skipped"
            run.error = str(exc)
            self._close_handles(run)
            return

        time.sleep(self.startup_grace_seconds)
        run.returncode = run.process.poll()
        if run.returncode is not None:
            run.status = "unavailable"
            run.error = (
                "recorder exited during startup; check stderr for permissions, "
                "missing tracepoints, or tracefs/perf_event restrictions"
            )
            self._close_handles(run)
            run.process = None
            return

        run.status = "recording"

    def _stop_run(self, run: HostKvmTraceRun) -> None:
        if run.process is None:
            return

        try:
            if run.process.poll() is None:
                os.killpg(run.process.pid, signal.SIGINT)
                run.returncode = run.process.wait(timeout=self.stop_timeout_seconds)
                run.stopped_by = "SIGINT"
            else:
                run.returncode = run.process.returncode
                run.stopped_by = "already-exited"
        except ProcessLookupError:
            run.returncode = run.process.poll()
            run.stopped_by = "already-exited"
        except subprocess.TimeoutExpired:
            os.killpg(run.process.pid, signal.SIGTERM)
            try:
                run.returncode = run.process.wait(timeout=self.stop_timeout_seconds)
                run.stopped_by = "SIGTERM"
            except subprocess.TimeoutExpired:
                os.killpg(run.process.pid, signal.SIGKILL)
                run.returncode = run.process.wait(timeout=self.stop_timeout_seconds)
                run.stopped_by = "SIGKILL"
        finally:
            self._close_handles(run)

        if run.data_path.exists() and run.data_path.stat().st_size > 0:
            run.status = "recorded"
        else:
            run.status = "no-data"
            if run.error is None:
                run.error = "recorder stopped without producing a non-empty data file"

    def _close_handles(self, run: HostKvmTraceRun) -> None:
        for handle_name in ("stdout_handle", "stderr_handle"):
            handle = getattr(run, handle_name)
            if handle is not None:
                handle.close()
                setattr(run, handle_name, None)

    def _finalize(self, run: HostKvmTraceRun) -> dict[str, Any]:
        duration = None
        if run.start_monotonic > 0.0:
            duration = time.monotonic() - run.start_monotonic

        for log_path in (run.stdout_path, run.stderr_path):
            if not log_path.exists():
                write_text(log_path, "")

        artifacts = [
            relpath(run.metadata_path),
            relpath(run.stdout_path),
            relpath(run.stderr_path),
        ]
        data_size = 0
        if run.data_path.exists():
            data_size = run.data_path.stat().st_size
            artifacts.append(relpath(run.data_path))

        entry: dict[str, Any] = {
            "tool": run.tool,
            "step": run.step_name,
            "status": run.status,
            "command": run.command,
            "data_file": relpath(run.data_path),
            "data_bytes": data_size,
            "stdout_file": relpath(run.stdout_path),
            "stderr_file": relpath(run.stderr_path),
            "metadata_file": relpath(run.metadata_path),
            "returncode": run.returncode,
            "stopped_by": run.stopped_by,
            "duration_seconds": duration,
            "artifacts": artifacts,
        }
        if run.error:
            entry["error"] = run.error
        write_json(run.metadata_path, entry)
        self.records.append(entry)
        return entry


def append_step(manifest: dict[str, Any], suite_dir: Path, step: dict[str, Any]) -> None:
    manifest["steps"].append(step)
    write_json(suite_dir / "manifest.json", manifest)


def run_host_schedstat_probe(
    args: argparse.Namespace,
    suite_dir: Path,
    wos_launcher: str,
) -> dict[str, Any]:
    step_name = "host-schedstat-wos-perf"
    step_dir = suite_dir / step_name
    step_dir.mkdir(parents=True, exist_ok=True)
    result_path = step_dir / "result.json"
    report_path = step_dir / "report.txt"

    command = [
        sys.executable,
        str(BENCH_SCRIPTS / "schedstat_probe.py"),
        "--output-json",
        str(result_path),
        "--output-text",
        str(report_path),
    ]
    for qemu_pid in args.schedstat_qemu_pid:
        command += ["--qemu-vm", qemu_pid]
    if args.schedstat_auto_discover_qemu:
        for vm_index in range(args.num_vms):
            command += ["--auto-discover-vm", str(vm_index)]

    if args.schedstat_duration > 0:
        command += ["--duration", str(args.schedstat_duration)]
    elif args.schedstat_command:
        command += ["--", *shlex.split(args.schedstat_command)]
    else:
        command += [
            "--",
            str(REMOTE_SCRIPTS / "wos_ssh.sh"),
            wos_launcher,
            "/usr/bin/testprog",
            "perf",
            "--verbose",
        ]

    result = run_command(command)
    write_text(step_dir / "command.log", step_log_text(result))

    summary = json.loads(result_path.read_text(encoding="utf-8"))
    return {
        "name": step_name,
        "ok": True,
        "kind": "host-schedstat",
        "os": "host",
        "target_os": "wos",
        "command": command,
        "result_file": relpath(result_path),
        "artifacts": [
            relpath(result_path),
            relpath(report_path),
            relpath(step_dir / "command.log"),
        ],
        "summary": summary.get("summary", {}),
    }


def run_wos_mandelbench(
    args: argparse.Namespace,
    suite_dir: Path,
    suite_remote_root: str,
    host: str,
    wos_hosts: list[str],
) -> dict[str, Any]:
    prepare_wos_hosts(args, wos_hosts)

    step_name = "wos-mandelbench"
    step_dir = suite_dir / step_name
    step_dir.mkdir(parents=True, exist_ok=True)
    remote_work_dir = f"{suite_remote_root}/{step_name}"
    total_workers = args.mandel_threads * args.num_vms
    remote_command = " ".join(
        [
            "mkdir -p",
            shlex.quote(remote_work_dir),
            "&& cd",
            shlex.quote(remote_work_dir),
            "&& /usr/bin/testprog mandelbench",
            "--width",
            shlex.quote(str(args.mandel_width)),
            "--height",
            shlex.quote(str(args.mandel_height)),
            "--max-iter",
            shlex.quote(str(args.mandel_max_iter)),
            "--threads",
            shlex.quote(str(total_workers)),
            "--repeat",
            shlex.quote(str(args.mandel_repeat)),
            "--nodes",
            shlex.quote(",".join(wos_hosts)),
        ]
    )
    result = run_command([str(REMOTE_SCRIPTS / "wos_ssh.sh"), host, remote_command])
    write_text(step_dir / "command.log", step_log_text(result))

    report_local = step_dir / "report.txt"
    fetch_remote_file(
        REMOTE_SCRIPTS / "wos_sftp_get.sh",
        host,
        f"{remote_work_dir}/report.txt",
        report_local,
    )
    summary = parse_mandel_report(report_local.read_text(encoding="utf-8"))
    summary.update(
        {
            "benchmark": "mandelbench",
            "os": "wos",
            "host": host,
            "hosts": wos_hosts,
            "width": args.mandel_width,
            "height": args.mandel_height,
            "max_iteration": args.mandel_max_iter,
            "threads": total_workers,
            "threads_per_node": args.mandel_threads,
            "repeat": args.mandel_repeat,
        }
    )
    write_json(step_dir / "result.json", summary)

    artifacts: list[str] = [relpath(report_local), relpath(step_dir / "result.json")]
    for index in range(args.mandel_repeat):
        image_name = f"process_{index:02d}.png"
        local_image = step_dir / image_name
        fetch_remote_file(
            REMOTE_SCRIPTS / "wos_sftp_get.sh",
            host,
            f"{remote_work_dir}/{image_name}",
            local_image,
        )
        artifacts.append(relpath(local_image))

    return {
        "name": step_name,
        "ok": True,
        "os": "wos",
        "host": host,
        "hosts": wos_hosts,
        "command": [str(REMOTE_SCRIPTS / "wos_ssh.sh"), host, remote_command],
        "result_file": relpath(step_dir / "result.json"),
        "artifacts": artifacts,
    }


def run_linux_mandelbench(
    args: argparse.Namespace,
    suite_dir: Path,
    suite_remote_root: str,
    linux_hosts: list[str],
    launcher: str,
) -> dict[str, Any]:
    step_name = "linux-mandelbench"
    step_dir = suite_dir / step_name
    step_dir.mkdir(parents=True, exist_ok=True)
    output_path = step_dir / "result.json"
    remote_work_dir = f"{suite_remote_root}/{step_name}"
    total_workers = args.mandel_threads * len(linux_hosts)

    command = [
        str(BENCH_SCRIPTS / "run_linux_mpi_benchmark.sh"),
        "--launcher",
        launcher,
        "--hosts",
        ",".join(linux_hosts),
        "--benchmark",
        "mandel",
        "--router-ip",
        args.router_ip,
        "--host-ip",
        args.host_ip,
        "--width",
        str(args.mandel_width),
        "--height",
        str(args.mandel_height),
        "--max-iter",
        str(args.mandel_max_iter),
        "--repeat",
        str(args.mandel_repeat),
        "--threads",
        str(args.mandel_threads),
        "--np",
        str(total_workers),
        "--mandel-output-root",
        remote_work_dir,
        "--output",
        str(output_path),
    ]
    if args.linux_use_host_binary:
        command += ["--use-host-binary"]

    result = run_command(command)
    write_text(step_dir / "command.log", step_log_text(result))

    report_local = step_dir / "report.txt"
    fetch_remote_file(
        REMOTE_SCRIPTS / "linux_sftp_get.sh",
        launcher,
        f"{remote_work_dir}/report.txt",
        report_local,
    )
    summary = parse_mandel_report(report_local.read_text(encoding="utf-8"))
    summary.update(
        {
            "benchmark": "mandelbench",
            "os": "linux",
            "host": launcher,
            "hosts": linux_hosts,
            "width": args.mandel_width,
            "height": args.mandel_height,
            "max_iteration": args.mandel_max_iter,
            "threads": total_workers,
            "threads_per_node": args.mandel_threads,
            "repeat": args.mandel_repeat,
        }
    )
    write_json(step_dir / "result.json", summary)

    artifacts: list[str] = [relpath(report_local), relpath(step_dir / "result.json")]
    for index in range(args.mandel_repeat):
        image_name = f"process_{index:02d}.png"
        local_image = step_dir / image_name
        fetch_remote_file(
            REMOTE_SCRIPTS / "linux_sftp_get.sh",
            launcher,
            f"{remote_work_dir}/{image_name}",
            local_image,
        )
        artifacts.append(relpath(local_image))

    return {
        "name": step_name,
        "ok": True,
        "os": "linux",
        "host": launcher,
        "hosts": linux_hosts,
        "command": command,
        "result_file": relpath(step_dir / "result.json"),
        "artifacts": artifacts,
    }


def run_wos_renderbench(
    args: argparse.Namespace,
    suite_dir: Path,
    suite_remote_root: str,
    host: str,
    wos_hosts: list[str],
    case: RenderCase,
    placement: str,
) -> dict[str, Any]:
    prepare_wos_hosts(args, wos_hosts)

    step_name = f"wos-render-{case.name}-{placement}"
    step_dir = suite_dir / step_name
    step_dir.mkdir(parents=True, exist_ok=True)
    remote_output_root = f"{suite_remote_root}/renderbench"
    run_id = f"{case.name}-{placement}"

    command = [
        "/usr/bin/renderbench",
        "--backend",
        "ipc",
        "--placement",
        placement,
        "--width",
        str(case.width),
        "--height",
        str(case.height),
        "--spp",
        str(case.spp),
        "--max-depth",
        str(args.render_max_depth),
        "--tile-size",
        str(args.render_tile_size),
        "--output-root",
        remote_output_root,
        "--run-id",
        run_id,
    ]
    if case.wos_scene:
        command += ["--scene", case.wos_scene]
    if args.wos_render_threads is not None:
        command += ["--threads", str(args.wos_render_threads)]
    if args.render_debug_constant_tile_us > 0:
        command += ["--debug-constant-tile-us", str(args.render_debug_constant_tile_us)]
    if args.render_debug_node_thread_batch_size > 0:
        command += ["--debug-node-thread-batch-size", str(args.render_debug_node_thread_batch_size)]
    if args.wos_coordinator_reserve_cpus is not None:
        command += ["--coordinator-reserve-cpus", str(args.wos_coordinator_reserve_cpus)]
    if args.wos_coordinator_skip_local_worker:
        command += ["--coordinator-skip-local-worker"]

    result = run_command([str(REMOTE_SCRIPTS / "wos_ssh.sh"), host, *command])
    write_text(step_dir / "command.log", step_log_text(result))

    remote_run_dir = f"{remote_output_root}/{run_id}"
    metrics_local = step_dir / "metrics.json"
    status_local = step_dir / "status.json"
    frame_local = step_dir / "frame_000.png"
    fetch_remote_file(
        REMOTE_SCRIPTS / "wos_sftp_get.sh",
        host,
        f"{remote_run_dir}/metrics.json",
        metrics_local,
    )
    fetch_remote_file(
        REMOTE_SCRIPTS / "wos_sftp_get.sh",
        host,
        f"{remote_run_dir}/status.json",
        status_local,
    )
    fetch_remote_file(
        REMOTE_SCRIPTS / "wos_sftp_get.sh",
        host,
        f"{remote_run_dir}/frame_000.png",
        frame_local,
    )

    metrics = json.loads(metrics_local.read_text(encoding="utf-8"))
    status = json.loads(status_local.read_text(encoding="utf-8"))
    merged = {**status, **metrics}
    merged.update(
        {
            "benchmark": "renderbench",
            "os": "wos",
            "host": host,
            "scene": case.name,
            "scene_path": case.wos_scene,
        }
    )
    write_json(step_dir / "result.json", merged)

    return {
        "name": step_name,
        "ok": True,
        "os": "wos",
        "host": host,
        "command": [str(REMOTE_SCRIPTS / "wos_ssh.sh"), host, *command],
        "result_file": relpath(step_dir / "result.json"),
        "artifacts": [
            relpath(metrics_local),
            relpath(status_local),
            relpath(frame_local),
            relpath(step_dir / "result.json"),
        ],
    }


def run_linux_renderbench(
    args: argparse.Namespace,
    suite_dir: Path,
    linux_hosts: list[str],
    launcher: str,
    case: RenderCase,
    placement: str,
) -> dict[str, Any]:
    step_name = f"linux-render-{case.name}-{placement}"
    step_dir = suite_dir / step_name
    step_dir.mkdir(parents=True, exist_ok=True)
    output_path = step_dir / "result.json"
    run_id = f"{case.name}-{placement}"
    remote_output_root = f"/var/lib/wos-bench/results/tracebench/{args.remote_suite_name}/{step_name}"

    command = [
        str(BENCH_SCRIPTS / "run_linux_mpi_benchmark.sh"),
        "--launcher",
        launcher,
        "--hosts",
        ",".join(linux_hosts),
        "--benchmark",
        "render",
        "--router-ip",
        args.router_ip,
        "--host-ip",
        args.host_ip,
        "--width",
        str(case.width),
        "--height",
        str(case.height),
        "--spp",
        str(case.spp),
        "--max-depth",
        str(args.render_max_depth),
        "--tile-size",
        str(args.render_tile_size),
        "--placement",
        placement,
        "--render-output-root",
        remote_output_root,
        "--run-id",
        run_id,
        "--output",
        str(output_path),
    ]
    if case.linux_scene:
        command += ["--scene", case.linux_scene]
    if args.linux_render_threads is not None:
        command += ["--threads", str(args.linux_render_threads)]
    if args.render_debug_constant_tile_us > 0:
        command += ["--debug-constant-tile-us", str(args.render_debug_constant_tile_us)]
    if args.linux_use_host_binary:
        command += ["--use-host-binary"]

    result = run_command(command)
    write_text(step_dir / "command.log", step_log_text(result))
    artifacts = [relpath(output_path), relpath(step_dir / "command.log")]
    frame_path = step_dir / "result-artifacts" / "frame_000.png"
    if frame_path.exists():
        artifacts.append(relpath(frame_path))

    return {
        "name": step_name,
        "ok": True,
        "os": "linux",
        "host": launcher,
        "hosts": linux_hosts,
        "command": command,
        "result_file": relpath(output_path),
        "artifacts": artifacts,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the configured WOS/Linux benchmark suite across already-running VMs."
    )
    parser.add_argument("--num-vms", type=int, required=True)
    parser.add_argument(
        "--os",
        choices=["both", "wos", "linux"],
        default="both",
        help="Choose whether to run both sides of the suite or only one OS.",
    )
    parser.add_argument("--wos-launcher-index", type=int, default=0)
    parser.add_argument("--linux-launcher-index", type=int, default=0)
    parser.add_argument(
        "--results-dir", default="benchmarks/results/cross-os"
    )
    parser.add_argument("--router-ip", default="10.10.0.1")
    parser.add_argument("--host-ip", default="10.10.0.100")
    parser.add_argument("--linux-user", default="user")
    parser.add_argument("--mandel-width", type=int, default=2000)
    parser.add_argument("--mandel-height", type=int, default=2000)
    parser.add_argument("--mandel-max-iter", type=int, default=5000)
    parser.add_argument("--mandel-threads", type=int, default=8, help="Mandelbench worker processes/ranks per node.")
    parser.add_argument("--mandel-repeat", type=int, default=5)
    parser.add_argument(
        "--render-placements",
        default="node-threads,process-per-core",
        help="Comma-separated placements to run for renderbench.",
    )
    parser.add_argument("--default-render-width", type=int, default=1000)
    parser.add_argument("--default-render-height", type=int, default=1000)
    parser.add_argument("--default-render-spp", type=int, default=500)
    parser.add_argument("--duck-render-width", type=int, default=4000)
    parser.add_argument("--duck-render-height", type=int, default=4000)
    parser.add_argument("--duck-render-spp", type=int, default=1600)
    parser.add_argument("--render-max-depth", type=int, default=6)
    parser.add_argument("--render-tile-size", type=int, default=32)
    parser.add_argument(
        "--render-debug-constant-tile-us",
        type=int,
        default=0,
        help="Use renderbench's constant-rate synthetic tile mode with this per-tile delay in microseconds.",
    )
    parser.add_argument(
        "--render-debug-node-thread-batch-size",
        type=int,
        default=0,
        help="Override renderbench's persistent node-thread batch size on WOS; 0 keeps auto sizing.",
    )
    parser.add_argument("--wos-duck-scene", default="/srv/Duck.glb")
    parser.add_argument("--linux-duck-scene", default="configs/drive/srv/Duck.glb")
    parser.add_argument("--wos-render-threads", type=int)
    parser.add_argument(
        "--wos-coordinator-reserve-cpus",
        type=int,
        help="Reserve this many local launcher CPUs from WOS node-thread render workers; omit for renderbench default.",
    )
    parser.add_argument(
        "--wos-coordinator-skip-local-worker",
        action="store_true",
        help="Do not run a WOS node-thread render worker on the local launcher host when remote workers exist.",
    )
    parser.add_argument("--linux-render-threads", type=int)
    parser.add_argument("--linux-use-host-binary", action="store_true")
    parser.add_argument(
        "--host-kvm-trace",
        default="none",
        metavar="{none,perf,trace-cmd,both}",
        help=(
            "Opt-in host-side KVM tracing around each benchmark step. "
            "Use 'perf', 'trace-cmd', 'both', or a comma-separated list."
        ),
    )
    parser.add_argument(
        "--host-kvm-perf-cmd",
        default="perf",
        help="Host perf command or wrapper used for --host-kvm-trace=perf.",
    )
    parser.add_argument(
        "--host-kvm-perf-events",
        default="kvm:*",
        help="Host perf event selector for KVM tracing.",
    )
    parser.add_argument(
        "--host-kvm-trace-cmd",
        default="trace-cmd",
        help="Host trace-cmd command or wrapper used for --host-kvm-trace=trace-cmd.",
    )
    parser.add_argument(
        "--host-kvm-trace-events",
        default="kvm",
        help="Comma-separated trace-cmd event systems/events; default matches 'trace-cmd record -e kvm'.",
    )
    parser.add_argument(
        "--host-kvm-trace-buffer-kb",
        type=int,
        default=20000,
        help="trace-cmd per-CPU buffer size in KiB; default matches '-b 20000'.",
    )
    parser.add_argument(
        "--host-kvm-trace-startup-grace",
        type=float,
        default=0.5,
        help="Seconds to wait for host recorders to fail fast before running a benchmark step.",
    )
    parser.add_argument(
        "--host-kvm-trace-stop-timeout",
        type=float,
        default=10.0,
        help="Seconds to wait for host recorders to flush after each benchmark step.",
    )
    parser.add_argument(
        "--host-kvm-trace-max-seconds",
        type=int,
        default=86400,
        help="Safety duration for perf's traced sleep command.",
    )
    parser.add_argument(
        "--schedstat-probe",
        action="store_true",
        help=(
            "Add an opt-in host-side schedstat diagnostic step for QEMU KVM vCPU threads. "
            "Requires --schedstat-qemu-pid or --schedstat-auto-discover-qemu."
        ),
    )
    parser.add_argument(
        "--schedstat-qemu-pid",
        action="append",
        type=parse_schedstat_qemu_pid,
        default=[],
        metavar="VM:PID",
        help="QEMU process to sample for --schedstat-probe, for example vm0:12345 or 0:12345. Repeat per VM.",
    )
    parser.add_argument(
        "--schedstat-auto-discover-qemu",
        action="store_true",
        help="Explicitly discover WOS QEMU PIDs for vm0..vmN from host /proc command lines.",
    )
    parser.add_argument(
        "--schedstat-command",
        help=(
            "Host command to run inside the schedstat step. Defaults to running "
            "'/usr/bin/testprog perf --verbose' on the WOS launcher."
        ),
    )
    parser.add_argument(
        "--schedstat-duration",
        type=float,
        default=0.0,
        help="Instead of running a command, sample QEMU vCPU schedstat for this many seconds.",
    )
    parser.add_argument(
        "--skip-wos-cleanup",
        action="store_true",
        help="Do not preflight WOS hosts or kill stale WOS benchmark processes before WOS steps.",
    )
    parser.add_argument(
        "--wos-preflight-timeout",
        type=float,
        default=8.0,
        help="Seconds to wait for each WOS preflight or cleanup SSH command.",
    )
    parser.add_argument("--skip-mandelbench", action="store_true")
    parser.add_argument("--skip-renderbench", action="store_true")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.num_vms <= 0:
        parser.error("--num-vms must be greater than zero")
    if not 0 <= args.wos_launcher_index < args.num_vms:
        parser.error("--wos-launcher-index must be within the VM count")
    if not 0 <= args.linux_launcher_index < args.num_vms:
        parser.error("--linux-launcher-index must be within the VM count")
    if args.render_debug_constant_tile_us < 0:
        parser.error("--render-debug-constant-tile-us must be nonnegative")
    if args.render_debug_node_thread_batch_size < 0:
        parser.error("--render-debug-node-thread-batch-size must be nonnegative")
    if args.wos_coordinator_reserve_cpus is not None and args.wos_coordinator_reserve_cpus < 0:
        parser.error("--wos-coordinator-reserve-cpus must be nonnegative")
    if args.host_kvm_trace_buffer_kb <= 0:
        parser.error("--host-kvm-trace-buffer-kb must be greater than zero")
    if args.host_kvm_trace_startup_grace < 0:
        parser.error("--host-kvm-trace-startup-grace must be nonnegative")
    if args.host_kvm_trace_stop_timeout <= 0:
        parser.error("--host-kvm-trace-stop-timeout must be greater than zero")
    if args.host_kvm_trace_max_seconds <= 0:
        parser.error("--host-kvm-trace-max-seconds must be greater than zero")
    if args.schedstat_duration < 0:
        parser.error("--schedstat-duration must be nonnegative")
    if args.schedstat_probe:
        if not args.schedstat_qemu_pid and not args.schedstat_auto_discover_qemu:
            parser.error("--schedstat-probe requires --schedstat-qemu-pid or --schedstat-auto-discover-qemu")
        if args.schedstat_command and args.schedstat_duration > 0:
            parser.error("--schedstat-command and --schedstat-duration are mutually exclusive")
        if args.os == "linux" and not args.schedstat_command and args.schedstat_duration <= 0:
            parser.error("--schedstat-probe with --os linux requires --schedstat-command or --schedstat-duration")

    try:
        placements = render_placements(args.render_placements)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        args.host_kvm_trace_modes = parse_host_kvm_trace_modes(args.host_kvm_trace)
        split_host_tool_command(args.host_kvm_perf_cmd)
        split_host_tool_command(args.host_kvm_trace_cmd)
    except ValueError as exc:
        parser.error(str(exc))

    linux_duck_scene = ROOT / args.linux_duck_scene
    if not args.skip_renderbench and not linux_duck_scene.is_file():
        parser.error(f"Linux Duck scene not found: {linux_duck_scene}")

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    args.remote_suite_name = f"cross-os-suite-{timestamp}"
    suite_dir = ROOT / args.results_dir / args.remote_suite_name
    suite_dir.mkdir(parents=True, exist_ok=True)
    suite_remote_root = f"/tmp/{args.remote_suite_name}"

    wos_hosts = [f"wos-{index}.wos" for index in range(args.num_vms)]
    linux_hosts = [f"wos-ubuntu-vm{index}.wos" for index in range(args.num_vms)]
    wos_launcher = wos_hosts[args.wos_launcher_index]
    linux_launcher = linux_hosts[args.linux_launcher_index]

    manifest: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "suite_name": args.remote_suite_name,
        "os_selection": args.os,
        "num_vms": args.num_vms,
        "wos_hosts": wos_hosts,
        "linux_hosts": linux_hosts,
        "wos_launcher": wos_launcher,
        "linux_launcher": linux_launcher,
        "steps": [],
    }
    host_kvm_tracer = HostKvmTracer(args, suite_dir)
    manifest["host_kvm_trace"] = host_kvm_tracer.config_summary()
    write_json(suite_dir / "manifest.json", manifest)

    failures = 0

    def attach_host_kvm_trace(step: dict[str, Any], trace_entries: list[dict[str, Any]]) -> None:
        if not trace_entries:
            return
        step["host_kvm_trace"] = trace_entries
        artifacts = step.setdefault("artifacts", [])
        for entry in trace_entries:
            artifacts.extend(entry.get("artifacts", []))

    def run_step(name: str, func: Any, *, host_kvm_trace: bool = True) -> None:
        nonlocal failures
        print(f"[suite] {name}")
        trace_session = host_kvm_tracer.start(name) if host_kvm_trace else None
        try:
            step = func()
        except Exception as exc:  # noqa: BLE001
            failures += 1
            trace_entries = host_kvm_tracer.stop(trace_session)
            error_path = suite_dir / name / "error.txt"
            write_text(error_path, f"{exc}\n")
            failed_step = {
                "name": name,
                "ok": False,
                "error": str(exc),
                "error_file": relpath(error_path),
            }
            attach_host_kvm_trace(failed_step, trace_entries)
            append_step(
                manifest,
                suite_dir,
                failed_step,
            )
            print(f"[suite] {name} failed")
            return

        trace_entries = host_kvm_tracer.stop(trace_session)
        attach_host_kvm_trace(step, trace_entries)
        append_step(manifest, suite_dir, step)
        print(f"[suite] {name} complete")

    run_wos = args.os in {"both", "wos"}
    run_linux = args.os in {"both", "linux"}

    if args.schedstat_probe:
        run_step(
            "host-schedstat-wos-perf",
            lambda: run_host_schedstat_probe(args, suite_dir, wos_launcher),
            host_kvm_trace=False,
        )

    if not args.skip_mandelbench:
        if run_wos:
            run_step(
                "wos-mandelbench",
                lambda: run_wos_mandelbench(args, suite_dir, suite_remote_root, wos_launcher, wos_hosts),
            )
        if run_linux:
            run_step(
                "linux-mandelbench",
                lambda: run_linux_mandelbench(args, suite_dir, suite_remote_root, linux_hosts, linux_launcher),
            )

    if not args.skip_renderbench:
        for case in render_cases(args):
            for placement in placements:
                if run_wos:
                    run_step(
                        f"wos-render-{case.name}-{placement}",
                        lambda case=case, placement=placement: run_wos_renderbench(
                            args,
                            suite_dir,
                            suite_remote_root,
                            wos_launcher,
                            wos_hosts,
                            case,
                            placement,
                        ),
                    )
                if run_linux:
                    run_step(
                        f"linux-render-{case.name}-{placement}",
                        lambda case=case, placement=placement: run_linux_renderbench(
                            args,
                            suite_dir,
                            linux_hosts,
                            linux_launcher,
                            case,
                            placement,
                        ),
                    )

    if host_kvm_tracer.enabled:
        append_step(manifest, suite_dir, host_kvm_tracer.summary_step())

    print(str(suite_dir / "manifest.json"))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
