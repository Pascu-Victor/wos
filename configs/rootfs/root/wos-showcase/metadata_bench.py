#!/usr/bin/env python3
"""Run fixed-total metadata work concurrently on every requested WOS host."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import math
import os
import re
import select
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Callable, Sequence

BENCHMARKS = {
    "create": "wos_vfsbench_create",
    "rename": "wos_vfsbench_rename",
}
READY_RECORD = "metadata-worker-ready-v1"
DONE_RECORD_PREFIX = "metadata-worker-done-v1"
MAX_TOTAL_WORKERS = 32
WORKER_SHELL = f"""\
set -eu
/usr/bin/wosid
exec /usr/bin/testprog vfsbench-metadata-worker \
    --create-path "$1" --rename-path "$2" --iterations "$3" --workers "$4"
"""
WOSID_PATTERN = re.compile(
    r"spawner=(?P<spawner>\S+) host=(?P<runner>\S+) "
    r"pid=(?P<pid>[0-9]+) remote_pid=(?P<remote_pid>[0-9]+)"
)
HOSTNAME_PATTERN = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*"
)
WKI_HOSTNAME_MAX_BYTES = 63
TERMINATE_GRACE_SECONDS = 1.0
KILL_GRACE_SECONDS = 1.0
GRACEFUL_SHUTDOWN_FRACTION = 0.1
GRACEFUL_SHUTDOWN_MAX_SECONDS = 10.0
POLL_SECONDS = 0.01
LOCAL_RUNTIME_PATHS = ("/usr", "/bin", "/lib", "/lib64", "/libexec", "/share")


class BenchmarkError(RuntimeError):
    """A metadata worker or its evidence violated the benchmark contract."""


@dataclass(frozen=True)
class Config:
    operations: tuple[str, ...]
    hosts: tuple[str, ...]
    launcher: str
    path: str
    total_work_units: int
    timeout_seconds: float
    log_dir: Path


@dataclass(frozen=True)
class Job:
    index: int
    host: str
    path: str
    work_units: int
    workers: int
    log_path: Path

    def worker_path(self, operation: str) -> str:
        return f"{self.path}.{operation}.worker-{self.index}"


@dataclass(frozen=True)
class WorkerEvidence:
    spawner_host: str
    runner_host: str
    process_pid: int
    remote_pid: int


@dataclass
class WorkerRuntime:
    job: Job
    process: subprocess.Popen[bytes]
    log: BinaryIO
    pending_output: bytearray
    lines: list[str]
    line_cursor: int = 0
    stdout_closed: bool = False
    ready: bool = False
    identity: WorkerEvidence | None = None


CommandFactory = Callable[[Job], Sequence[str]]


def normalize_hostname(hostname: str) -> str:
    return hostname[:-4] if hostname.endswith(".wos") else hostname


def require_safe_hostname(hostname: str, description: str) -> None:
    if (
        not hostname
        or len(hostname.encode("ascii", errors="ignore")) != len(hostname)
        or len(hostname) > WKI_HOSTNAME_MAX_BYTES
        or HOSTNAME_PATTERN.fullmatch(hostname) is None
    ):
        raise BenchmarkError(f"{description} is not a safe WOS hostname")


def parse_hosts(raw_hosts: str, launcher: str) -> tuple[str, ...]:
    if not raw_hosts:
        raise BenchmarkError("--hosts must name one to four WOS hosts")
    hosts = tuple(part.strip() for part in raw_hosts.split(","))
    if not 1 <= len(hosts) <= 4 or any(not host for host in hosts):
        raise BenchmarkError("--hosts must contain one to four non-empty hostnames")
    for host in hosts:
        require_safe_hostname(host, f"host {host!r}")

    normalized_hosts = tuple(normalize_hostname(host) for host in hosts)
    for host in normalized_hosts:
        require_safe_hostname(host, f"normalized host {host!r}")
    if len(set(normalized_hosts)) != len(normalized_hosts):
        raise BenchmarkError("--hosts contains duplicate hostnames")

    launcher = launcher.strip()
    require_safe_hostname(launcher, "--launcher")
    normalized_launcher = normalize_hostname(launcher)
    require_safe_hostname(normalized_launcher, "normalized --launcher")
    if normalized_hosts.count(normalized_launcher) != 1:
        raise BenchmarkError("--launcher must appear exactly once in --hosts")
    return hosts


def make_jobs(config: Config) -> list[Job]:
    canonical_hosts = sorted(config.hosts, key=normalize_hostname)
    host_count = len(canonical_hosts)
    if config.total_work_units < host_count:
        raise BenchmarkError(
            "--total-work-units must assign at least one operation to every host"
        )
    total_workers = min(MAX_TOTAL_WORKERS, config.total_work_units)
    quotient, remainder = divmod(config.total_work_units, host_count)
    worker_quotient, worker_remainder = divmod(total_workers, host_count)
    jobs = []
    for index, host in enumerate(canonical_hosts):
        work_units = quotient + (1 if index < remainder else 0)
        workers = worker_quotient + (1 if index < worker_remainder else 0)
        if not 1 <= workers <= work_units:
            raise BenchmarkError(
                f"internal worker partition for {host} exceeds its canonical work"
            )
        jobs.append(
            Job(
                index=index,
                host=host,
                path=config.path,
                work_units=work_units,
                workers=workers,
                log_path=config.log_dir / f"metadata-worker-{index}.log",
            )
        )
    return jobs


def worker_command(job: Job) -> Sequence[str]:
    return (
        "forward",
        "+/tmp",
        *(f"-{path}" for path in LOCAL_RUNTIME_PATHS),
        "--",
        "on",
        job.host,
        "sh",
        "-c",
        WORKER_SHELL,
        "metadata-worker",
        job.worker_path("create"),
        job.worker_path("rename"),
        str(job.work_units),
        str(job.workers),
    )


class SignalGuard:
    def __init__(self) -> None:
        self.received: int | None = None
        self.previous: dict[int, object] = {}

    def _handle(self, signum: int, _frame: object) -> None:
        self.received = signum

    def __enter__(self) -> "SignalGuard":
        for signum in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
            self.previous[signum] = signal.getsignal(signum)
            signal.signal(signum, self._handle)
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        for signum, previous in self.previous.items():
            signal.signal(signum, previous)


def running(
    processes: Sequence[subprocess.Popen[bytes]],
) -> list[subprocess.Popen[bytes]]:
    return [process for process in processes if process.poll() is None]


def wait_until_stopped(
    processes: Sequence[subprocess.Popen[bytes]], deadline: float
) -> None:
    while running(processes) and time.monotonic() < deadline:
        time.sleep(POLL_SECONDS)


def terminate_and_reap(processes: Sequence[subprocess.Popen[bytes]]) -> None:
    for process in running(processes):
        try:
            process.terminate()
        except ProcessLookupError:
            pass
    wait_until_stopped(processes, time.monotonic() + TERMINATE_GRACE_SECONDS)

    for process in running(processes):
        try:
            process.kill()
        except ProcessLookupError:
            pass
    wait_until_stopped(processes, time.monotonic() + KILL_GRACE_SECONDS)

    survivors = running(processes)
    for process in processes:
        if process.poll() is not None:
            process.wait()
    if survivors:
        raise BenchmarkError(
            "failed to reap worker processes: "
            + ", ".join(str(process.pid) for process in survivors)
        )


def graceful_shutdown_seconds(timeout_seconds: float) -> float:
    return min(
        GRACEFUL_SHUTDOWN_MAX_SECONDS,
        max(TERMINATE_GRACE_SECONDS, timeout_seconds * GRACEFUL_SHUTDOWN_FRACTION),
    )


def append_worker_output(runtime: WorkerRuntime, chunk: bytes) -> None:
    runtime.log.write(chunk)
    runtime.log.flush()
    runtime.pending_output.extend(chunk)
    while True:
        newline = runtime.pending_output.find(b"\n")
        if newline < 0:
            return
        raw_line = bytes(runtime.pending_output[:newline])
        del runtime.pending_output[: newline + 1]
        try:
            runtime.lines.append(raw_line.rstrip(b"\r").decode("utf-8"))
        except UnicodeDecodeError as error:
            raise BenchmarkError(
                f"{runtime.job.host} emitted non-UTF-8 worker output"
            ) from error


def finish_worker_output(runtime: WorkerRuntime) -> None:
    if runtime.pending_output:
        try:
            runtime.lines.append(
                bytes(runtime.pending_output).rstrip(b"\r").decode("utf-8")
            )
        except UnicodeDecodeError as error:
            raise BenchmarkError(
                f"{runtime.job.host} emitted non-UTF-8 worker output"
            ) from error
        runtime.pending_output.clear()
    runtime.stdout_closed = True


def pump_worker_output(
    runtimes: Sequence[WorkerRuntime], wait_seconds: float
) -> None:
    by_fd = {
        runtime.process.stdout.fileno(): runtime
        for runtime in runtimes
        if not runtime.stdout_closed and runtime.process.stdout is not None
    }
    if not by_fd:
        if wait_seconds > 0.0:
            time.sleep(wait_seconds)
        return
    try:
        readable, _, _ = select.select(tuple(by_fd), (), (), wait_seconds)
    except OSError as error:
        raise BenchmarkError(f"cannot poll metadata worker output: {error}") from error
    for fd in readable:
        runtime = by_fd[fd]
        try:
            chunk = os.read(fd, 65536)
        except OSError as error:
            raise BenchmarkError(
                f"cannot read {runtime.job.host} worker output: {error}"
            ) from error
        if chunk:
            append_worker_output(runtime, chunk)
        else:
            finish_worker_output(runtime)


def new_worker_lines(runtime: WorkerRuntime) -> list[str]:
    lines = runtime.lines[runtime.line_cursor :]
    runtime.line_cursor = len(runtime.lines)
    return lines


def parse_worker_identity(job: Job, launcher: str, line: str) -> WorkerEvidence:
    identity = WOSID_PATTERN.fullmatch(line.strip())
    if identity is None:
        raise BenchmarkError(f"{job.host} emitted a malformed wosid record")
    spawner = identity.group("spawner")
    runner = identity.group("runner")
    process_pid = int(identity.group("pid"))
    remote_pid = int(identity.group("remote_pid"))
    if process_pid <= 0:
        raise BenchmarkError(f"{job.host} reported an invalid process id")
    if normalize_hostname(spawner) != normalize_hostname(launcher):
        raise BenchmarkError(
            f"{job.host} reported launcher {spawner!r}, expected {launcher!r}"
        )
    if normalize_hostname(runner) != normalize_hostname(job.host):
        raise BenchmarkError(
            f"{job.host} reported runner {runner!r}; strict placement was not honored"
        )
    is_remote = normalize_hostname(job.host) != normalize_hostname(launcher)
    if is_remote and remote_pid <= 0:
        raise BenchmarkError(f"{job.host} did not report a remote execution id")
    if not is_remote and remote_pid != 0:
        raise BenchmarkError(
            f"{job.host} reported remote execution id {remote_pid} for local work"
        )
    return WorkerEvidence(
        spawner_host=spawner,
        runner_host=runner,
        process_pid=process_pid,
        remote_pid=remote_pid,
    )


def launch_workers(
    jobs: Sequence[Job], command_factory: CommandFactory, signal_guard: SignalGuard
) -> list[WorkerRuntime]:
    runtimes: list[WorkerRuntime] = []
    try:
        for job in jobs:
            if signal_guard.received is not None:
                raise BenchmarkError(
                    f"received signal {signal_guard.received} while launching workers"
                )
            worker_log = job.log_path.open("wb")
            try:
                process = subprocess.Popen(
                    command_factory(job),
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    bufsize=0,
                )
            except BaseException:
                worker_log.close()
                raise
            if process.stdin is None or process.stdout is None:
                worker_log.close()
                terminate_and_reap([process])
                raise BenchmarkError(f"cannot create control pipes for {job.host}")
            runtimes.append(
                WorkerRuntime(
                    job=job,
                    process=process,
                    log=worker_log,
                    pending_output=bytearray(),
                    lines=[],
                )
            )
    except BaseException:
        if runtimes:
            terminate_and_reap([runtime.process for runtime in runtimes])
            close_worker_pool(runtimes)
        raise
    return runtimes


def wait_for_ready_workers(
    runtimes: Sequence[WorkerRuntime],
    launcher: str,
    timeout_seconds: float,
    signal_guard: SignalGuard,
) -> None:
    deadline_ns = time.monotonic_ns() + int(timeout_seconds * 1_000_000_000)
    while not all(runtime.ready for runtime in runtimes):
        if signal_guard.received is not None:
            raise BenchmarkError(
                f"received signal {signal_guard.received} while placing workers"
            )
        now_ns = time.monotonic_ns()
        if now_ns >= deadline_ns:
            raise BenchmarkError(
                f"metadata workers did not become ready within {timeout_seconds:g} seconds"
            )
        pump_worker_output(
            runtimes,
            min(POLL_SECONDS, (deadline_ns - now_ns) / 1_000_000_000.0),
        )
        for runtime in runtimes:
            for line in new_worker_lines(runtime):
                stripped = line.strip()
                if stripped.startswith("spawner="):
                    if runtime.identity is not None:
                        raise BenchmarkError(
                            f"{runtime.job.host} emitted more than one wosid record"
                        )
                    runtime.identity = parse_worker_identity(
                        runtime.job, launcher, stripped
                    )
                elif stripped == READY_RECORD:
                    if runtime.ready:
                        raise BenchmarkError(
                            f"{runtime.job.host} emitted more than one ready record"
                        )
                    if runtime.identity is None:
                        raise BenchmarkError(
                            f"{runtime.job.host} became ready before proving its identity"
                        )
                    runtime.ready = True
                elif stripped.startswith("{") or stripped.startswith(
                    DONE_RECORD_PREFIX
                ):
                    raise BenchmarkError(
                        f"{runtime.job.host} emitted phase output before becoming ready"
                    )
            return_code = runtime.process.poll()
            if return_code is not None and not runtime.ready:
                raise BenchmarkError(
                    f"{runtime.job.host} exited before ready with rc={return_code}"
                )


def parse_phase_payload(job: Job, operation: str, line: str) -> dict[str, object]:
    try:
        payload = json.loads(line)
    except json.JSONDecodeError as error:
        raise BenchmarkError(f"{job.host} emitted malformed JSON: {error}") from error
    if not isinstance(payload, dict):
        raise BenchmarkError(f"{job.host} worker JSON is not an object")
    if payload.get("benchmark") != BENCHMARKS[operation]:
        raise BenchmarkError(f"{job.host} reported the wrong benchmark")
    if payload.get("path") != job.worker_path(operation):
        raise BenchmarkError(f"{job.host} reported the wrong worker path")
    iterations = payload.get("iterations")
    if (
        isinstance(iterations, bool)
        or not isinstance(iterations, int)
        or iterations != job.work_units
    ):
        raise BenchmarkError(f"{job.host} reported the wrong iteration count")
    workers = payload.get("workers")
    if (
        isinstance(workers, bool)
        or not isinstance(workers, int)
        or workers != job.workers
    ):
        raise BenchmarkError(f"{job.host} reported the wrong worker count")
    positive_finite(payload.get("elapsed_seconds"), f"{job.host} elapsed_seconds")
    return payload


def run_phase(
    runtimes: Sequence[WorkerRuntime],
    operation: str,
    timeout_seconds: float,
    signal_guard: SignalGuard,
) -> int:
    for runtime in runtimes:
        return_code = runtime.process.poll()
        if return_code is not None:
            raise BenchmarkError(
                f"{runtime.job.host} worker pool exited before {operation} with rc={return_code}"
            )

    # This is the reported phase boundary: it precedes every trigger write and
    # follows worker launch, strict placement evidence, and the ready handshake.
    started_ns = time.monotonic_ns()
    trigger = f"{operation}\n".encode("ascii")
    for runtime in runtimes:
        if signal_guard.received is not None:
            raise BenchmarkError(
                f"received signal {signal_guard.received} while starting {operation}"
            )
        try:
            written = runtime.process.stdin.write(trigger)
            runtime.process.stdin.flush()
        except (BrokenPipeError, OSError) as error:
            raise BenchmarkError(
                f"cannot start {operation} on {runtime.job.host}: {error}"
            ) from error
        if written != len(trigger):
            raise BenchmarkError(
                f"short control write while starting {operation} on {runtime.job.host}"
            )

    deadline_ns = started_ns + int(timeout_seconds * 1_000_000_000)
    phase_payloads: dict[int, dict[str, object]] = {}
    completed: set[int] = set()
    expected_done = f"{DONE_RECORD_PREFIX} {operation}"
    while len(completed) != len(runtimes):
        if signal_guard.received is not None:
            raise BenchmarkError(
                f"received signal {signal_guard.received} while waiting for {operation}"
            )
        now_ns = time.monotonic_ns()
        if now_ns >= deadline_ns:
            raise BenchmarkError(
                f"metadata {operation} phase exceeded {timeout_seconds:g} seconds"
            )
        pump_worker_output(
            runtimes,
            min(POLL_SECONDS, (deadline_ns - now_ns) / 1_000_000_000.0),
        )
        for runtime in runtimes:
            for line in new_worker_lines(runtime):
                stripped = line.strip()
                if stripped.startswith("{"):
                    if runtime.job.index in phase_payloads:
                        raise BenchmarkError(
                            f"{runtime.job.host} emitted more than one {operation} JSON record"
                        )
                    phase_payloads[runtime.job.index] = parse_phase_payload(
                        runtime.job, operation, stripped
                    )
                elif stripped.startswith(DONE_RECORD_PREFIX):
                    if stripped != expected_done:
                        raise BenchmarkError(
                            f"{runtime.job.host} emitted an unexpected completion record"
                        )
                    if runtime.job.index not in phase_payloads:
                        raise BenchmarkError(
                            f"{runtime.job.host} completed {operation} without valid JSON"
                        )
                    if runtime.job.index in completed:
                        raise BenchmarkError(
                            f"{runtime.job.host} completed {operation} more than once"
                        )
                    completed.add(runtime.job.index)
                elif stripped.startswith("spawner=") or stripped == READY_RECORD:
                    raise BenchmarkError(
                        f"{runtime.job.host} repeated worker placement evidence"
                    )
            return_code = runtime.process.poll()
            if return_code is not None and runtime.job.index not in completed:
                raise BenchmarkError(
                    f"{runtime.job.host} exited during {operation} with rc={return_code}"
                )

    elapsed_ns = time.monotonic_ns() - started_ns
    if elapsed_ns <= 0:
        raise BenchmarkError(f"{operation} monotonic phase timer did not advance")
    return elapsed_ns


def stop_worker_pool(
    runtimes: Sequence[WorkerRuntime],
    timeout_seconds: float,
    signal_guard: SignalGuard,
) -> None:
    for runtime in runtimes:
        try:
            runtime.process.stdin.close()
        except (BrokenPipeError, OSError):
            pass

    shutdown_seconds = graceful_shutdown_seconds(timeout_seconds)
    deadline = time.monotonic() + shutdown_seconds
    while running([runtime.process for runtime in runtimes]):
        if signal_guard.received is not None:
            raise BenchmarkError(
                f"received signal {signal_guard.received} while stopping workers"
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            raise BenchmarkError(
                "metadata worker pool did not stop within "
                f"{shutdown_seconds:g} seconds after control EOF"
            )
        pump_worker_output(runtimes, min(POLL_SECONDS, remaining))

    pump_worker_output(runtimes, 0.0)
    failed = []
    for runtime in runtimes:
        return_code = runtime.process.wait()
        if return_code != 0:
            failed.append(f"{runtime.job.host} rc={return_code}")
    if failed:
        raise BenchmarkError("metadata workers failed: " + ", ".join(failed))


def close_worker_pool(runtimes: Sequence[WorkerRuntime]) -> None:
    for runtime in runtimes:
        for stream in (runtime.process.stdin, runtime.process.stdout):
            if stream is not None and not stream.closed:
                stream.close()
        runtime.log.close()


def positive_finite(value: object, description: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BenchmarkError(f"{description} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result <= 0.0:
        raise BenchmarkError(f"{description} must be positive and finite")
    return result


def execute_benchmarks(
    config: Config, command_factory: CommandFactory = worker_command
) -> tuple[list[dict[str, object]], list[Job]]:
    config.log_dir.mkdir(parents=True, exist_ok=True)
    jobs = make_jobs(config)
    runtimes: list[WorkerRuntime] = []
    elapsed_by_operation: dict[str, int] = {}
    with SignalGuard() as signal_guard:
        try:
            runtimes = launch_workers(jobs, command_factory, signal_guard)
            wait_for_ready_workers(
                runtimes, config.launcher, config.timeout_seconds, signal_guard
            )
            for operation in config.operations:
                elapsed_by_operation[operation] = run_phase(
                    runtimes, operation, config.timeout_seconds, signal_guard
                )
            stop_worker_pool(runtimes, config.timeout_seconds, signal_guard)
        except BaseException:
            if runtimes:
                terminate_and_reap([runtime.process for runtime in runtimes])
            raise
        finally:
            close_worker_pool(runtimes)

    participants = []
    normalized_launcher = normalize_hostname(config.launcher)
    for runtime in runtimes:
        job = runtime.job
        worker_evidence = runtime.identity
        if worker_evidence is None:
            raise BenchmarkError(f"{job.host} has no validated worker identity")
        participants.append(
            {
                "host": job.host,
                "spawner_host": worker_evidence.spawner_host,
                "runner_host": worker_evidence.runner_host,
                "remote_pid": worker_evidence.remote_pid,
                "transport": (
                    "local"
                    if normalize_hostname(job.host) == normalized_launcher
                    else "wki"
                ),
                "work_units": job.work_units,
                "workers": job.workers,
            }
        )

    payloads = []
    for operation in config.operations:
        payloads.append(
            {
                "benchmark": BENCHMARKS[operation],
                "path": config.path,
                "iterations": config.total_work_units,
                "elapsed_seconds": elapsed_by_operation[operation]
                / 1_000_000_000.0,
                "total_work_units": config.total_work_units,
                "total_workers": sum(job.workers for job in jobs),
                "placement": "local-baseline" if len(jobs) == 1 else "strict-on",
                "wki_route": "host-path",
                "launcher_host": config.launcher,
                "participants": participants,
            }
        )
    return payloads, jobs


def emit_worker_diagnostics(operation: str, jobs: Sequence[Job]) -> None:
    for job in jobs:
        try:
            lines = job.log_path.read_text(
                encoding="utf-8", errors="replace"
            ).splitlines()
        except OSError as error:
            print(
                f"[metadata {operation} {job.host}] cannot read worker log: {error}",
                file=sys.stderr,
            )
            continue
        for line in lines:
            print(f"[metadata {operation} {job.host}] {line}", file=sys.stderr)


def positive_timeout(raw_value: str) -> float:
    try:
        value = float(raw_value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a number") from error
    if not math.isfinite(value) or value <= 0.0:
        raise argparse.ArgumentTypeError("must be positive and finite")
    return value


def parse_config(argv: Sequence[str]) -> Config:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--operation", required=True, action="append", choices=tuple(BENCHMARKS)
    )
    parser.add_argument("--hosts", required=True)
    parser.add_argument("--launcher", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--total-work-units", required=True, type=int)
    parser.add_argument("--timeout-seconds", required=True, type=positive_timeout)
    parser.add_argument("--log-dir", required=True, type=Path)
    arguments = parser.parse_args(argv)

    hosts = parse_hosts(arguments.hosts, arguments.launcher)
    operations = tuple(arguments.operation)
    if len(set(operations)) != len(operations):
        raise BenchmarkError("--operation must not name the same phase twice")
    if arguments.total_work_units <= 0:
        raise BenchmarkError("--total-work-units must be positive")
    if not arguments.path.startswith("/tmp/"):
        raise BenchmarkError("--path must be below /tmp for the HOST-routed benchmark")
    return Config(
        operations=operations,
        hosts=hosts,
        launcher=arguments.launcher.strip(),
        path=arguments.path,
        total_work_units=arguments.total_work_units,
        timeout_seconds=arguments.timeout_seconds,
        log_dir=arguments.log_dir,
    )


def fake_worker_main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--host", required=True)
    parser.add_argument("--launcher", required=True)
    parser.add_argument("--create-path", required=True)
    parser.add_argument("--rename-path", required=True)
    parser.add_argument("--iterations", required=True, type=int)
    parser.add_argument("--workers", required=True, type=int)
    parser.add_argument("--ready-sleep-seconds", required=True, type=float)
    parser.add_argument("--phase-sleep-seconds", required=True, type=float)
    parser.add_argument("--shutdown-sleep-seconds", type=float, default=0.0)
    parser.add_argument("--exit-code", type=int, default=0)
    parser.add_argument("--malformed-json", action="store_true")
    arguments = parser.parse_args(argv)
    time.sleep(arguments.ready_sleep_seconds)
    remote_pid = (
        0
        if normalize_hostname(arguments.host) == normalize_hostname(arguments.launcher)
        else 1
    )
    print(
        f"spawner={arguments.launcher} host={arguments.host} "
        f"pid=1 remote_pid={remote_pid}",
        flush=True,
    )
    print(READY_RECORD, flush=True)
    paths = {
        "create": arguments.create_path,
        "rename": arguments.rename_path,
    }
    for raw_operation in sys.stdin:
        operation = raw_operation.strip()
        if operation not in BENCHMARKS:
            return 64
        time.sleep(arguments.phase_sleep_seconds)
        if arguments.exit_code != 0:
            return arguments.exit_code
        if arguments.malformed_json:
            print("{malformed-json", flush=True)
        else:
            print(
                json.dumps(
                    {
                        "benchmark": BENCHMARKS[operation],
                        "path": paths[operation],
                        "iterations": arguments.iterations,
                        "workers": arguments.workers,
                        "elapsed_seconds": max(
                            arguments.phase_sleep_seconds, 0.000001
                        ),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                flush=True,
            )
        print(f"{DONE_RECORD_PREFIX} {operation}", flush=True)
    time.sleep(arguments.shutdown_sleep_seconds)
    return 0


def assert_worker_shell_execs_worker(directory: Path) -> None:
    fake_wosid = directory / "fake-wosid"
    fake_testprog = directory / "fake-testprog"
    fake_wosid.write_text(
        "#!/bin/sh\n"
        "printf 'spawner=wos-0 host=wos-0 pid=%s remote_pid=0\\n' \"$$\"\n",
        encoding="utf-8",
    )
    fake_testprog.write_text(
        "#!/bin/sh\n"
        "[ \"$1\" = vfsbench-metadata-worker ] || exit 64\n"
        "shift\n"
        "create_path=\n"
        "while [ \"$#\" -gt 0 ]; do\n"
        "    case \"$1\" in\n"
        "        --create-path) create_path=$2; shift 2 ;;\n"
        "        --rename-path|--iterations|--workers) shift 2 ;;\n"
        "        *) exit 64 ;;\n"
        "    esac\n"
        "done\n"
        "[ -n \"$create_path\" ] || exit 64\n"
        "printf '%s\\n' \"$$\" > \"$create_path.exec-pid\"\n"
        f"printf '%s\\n' '{READY_RECORD}'\n"
        "while IFS= read -r operation; do\n"
        "    printf 'metadata-worker-done-v1 %s\\n' \"$operation\"\n"
        "done\n",
        encoding="utf-8",
    )
    fake_wosid.chmod(0o700)
    fake_testprog.chmod(0o700)
    test_shell = WORKER_SHELL.replace("/usr/bin/wosid", str(fake_wosid)).replace(
        "/usr/bin/testprog", str(fake_testprog)
    )
    worker_path = directory / "shell-exec-worker"
    exec_pid_path = Path(f"{worker_path}.exec-pid")
    process = subprocess.Popen(
        (
            "sh",
            "-c",
            test_shell,
            "metadata-worker",
            str(worker_path),
            f"{worker_path}.rename",
            "1",
            "1",
        ),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        if process.stdout is None or process.stdin is None:
            raise AssertionError("worker shell test lacks control pipes")
        identity_line = process.stdout.readline().strip()
        ready_line = process.stdout.readline().strip()
        assert WOSID_PATTERN.fullmatch(identity_line) is not None
        assert ready_line == READY_RECORD
        deadline = time.monotonic() + 2.0
        while not exec_pid_path.exists() and time.monotonic() < deadline:
            if process.poll() is not None:
                break
            time.sleep(POLL_SECONDS)
        assert exec_pid_path.exists(), "worker shell did not exec its testprog worker"
        exec_pid = int(exec_pid_path.read_text(encoding="ascii").strip())
        assert exec_pid == process.pid
        process.stdin.write("create\n")
        process.stdin.flush()
        assert process.stdout.readline().strip() == f"{DONE_RECORD_PREFIX} create"
        process.terminate()
        process.wait(timeout=2.0)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=2.0)


def run_self_test() -> int:
    assert parse_hosts("wos-0,wos-1.wos,wos-2", "wos-0.wos") == (
        "wos-0",
        "wos-1.wos",
        "wos-2",
    )
    for invalid in (
        "",
        "wos-0,",
        "wos-0,wos-0.wos",
        "a,b,c,d,e",
        "wos-0;reboot",
        "../wos-0",
    ):
        try:
            parse_hosts(invalid, "wos-0")
        except BenchmarkError:
            pass
        else:
            raise AssertionError(f"accepted invalid hosts: {invalid!r}")

    with tempfile.TemporaryDirectory(prefix="metadata-bench-self-test-") as directory:
        assert graceful_shutdown_seconds(0.25) == TERMINATE_GRACE_SECONDS
        assert graceful_shutdown_seconds(20.0) == 2.0
        assert graceful_shutdown_seconds(120.0) == GRACEFUL_SHUTDOWN_MAX_SECONDS
        assert_worker_shell_execs_worker(Path(directory))

        config = Config(
            operations=("create", "rename"),
            hosts=("wos-0", "wos-1", "wos-2"),
            launcher="wos-0",
            path="/tmp/wos-showcase-vfsbench",
            total_work_units=10,
            timeout_seconds=2.0,
            log_dir=Path(directory),
        )

        launch_count = 0

        def fake_command(
            job: Job,
            ready_sleep_seconds: float = 0.20,
            phase_sleep_seconds: float = 0.03,
            shutdown_sleep_seconds: float = 0.0,
            runner_host: str | None = None,
            launcher: str | None = None,
            exit_code: int = 0,
            malformed_json: bool = False,
            reported_workers: int | None = None,
        ) -> Sequence[str]:
            nonlocal launch_count
            launch_count += 1
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--fake-worker",
                "--host",
                runner_host or job.host,
                "--launcher",
                launcher or config.launcher,
                "--create-path",
                job.worker_path("create"),
                "--rename-path",
                job.worker_path("rename"),
                "--iterations",
                str(job.work_units),
                "--workers",
                str(job.workers if reported_workers is None else reported_workers),
                "--ready-sleep-seconds",
                str(ready_sleep_seconds),
                "--phase-sleep-seconds",
                str(phase_sleep_seconds),
                "--shutdown-sleep-seconds",
                str(shutdown_sleep_seconds),
                "--exit-code",
                str(exit_code),
            ]
            if malformed_json:
                command.append("--malformed-json")
            return command

        payloads, jobs = execute_benchmarks(config, fake_command)
        assert launch_count == len(config.hosts)
        assert [job.work_units for job in jobs] == [4, 3, 3]
        assert [job.workers for job in jobs] == [4, 3, 3]
        assert [payload["benchmark"] for payload in payloads] == [
            BENCHMARKS["create"],
            BENCHMARKS["rename"],
        ]
        assert all(payload["total_work_units"] == 10 for payload in payloads)
        assert all(payload["total_workers"] == 10 for payload in payloads)
        assert all(payload["placement"] == "strict-on" for payload in payloads)
        assert [
            participant["remote_pid"]
            for participant in payloads[0]["participants"]
        ] == [0, 1, 1]
        assert all(
            participant["spawner_host"] == "wos-0"
            for participant in payloads[0]["participants"]
        )
        assert [
            participant["transport"] for participant in payloads[0]["participants"]
        ] == [
            "local",
            "wki",
            "wki",
        ]
        for payload in payloads:
            elapsed_seconds = float(payload["elapsed_seconds"])
            assert 0.015 <= elapsed_seconds < 0.15, elapsed_seconds
        assert worker_command(jobs[0])[:14] == (
            "forward",
            "+/tmp",
            "-/usr",
            "-/bin",
            "-/lib",
            "-/lib64",
            "-/libexec",
            "-/share",
            "--",
            "on",
            "wos-0",
            "sh",
            "-c",
            WORKER_SHELL,
        )

        expected_worker_splits = {
            1: [32],
            2: [16, 16],
            3: [11, 11, 10],
            4: [8, 8, 8, 8],
        }
        for total_work_units in (32, 256, 1000):
            for host_count, expected_workers in expected_worker_splits.items():
                split_config = Config(
                    operations=("create",),
                    hosts=tuple(f"wos-{index}" for index in range(host_count)),
                    launcher="wos-0",
                    path=config.path,
                    total_work_units=total_work_units,
                    timeout_seconds=2.0,
                    log_dir=Path(directory),
                )
                split_jobs = make_jobs(split_config)
                assert [job.workers for job in split_jobs] == expected_workers
                assert sum(job.workers for job in split_jobs) == MAX_TOTAL_WORKERS
                assert all(job.workers <= job.work_units for job in split_jobs)

        rotated_config = Config(
            operations=("create",),
            hosts=("wos-2.wos", "wos-0.wos", "wos-1.wos"),
            launcher="wos-2.wos",
            path=config.path,
            total_work_units=10,
            timeout_seconds=2.0,
            log_dir=Path(directory),
        )

        def rotated_command(job: Job) -> Sequence[str]:
            return fake_command(
                job,
                ready_sleep_seconds=0.001,
                phase_sleep_seconds=0.001,
                launcher=rotated_config.launcher,
            )

        rotated_payloads, rotated_jobs = execute_benchmarks(
            rotated_config, rotated_command
        )
        rotated_payload = rotated_payloads[0]
        assert [job.host for job in rotated_jobs] == [
            "wos-0.wos",
            "wos-1.wos",
            "wos-2.wos",
        ]
        assert [job.work_units for job in rotated_jobs] == [4, 3, 3]
        assert [job.workers for job in rotated_jobs] == [4, 3, 3]
        assert rotated_payload["launcher_host"] == "wos-2.wos"
        assert [
            participant["transport"] for participant in rotated_payload["participants"]
        ] == ["wki", "wki", "local"]

        single_config = Config(
            operations=("create",),
            hosts=("wos-0",),
            launcher="wos-0",
            path=config.path,
            total_work_units=1,
            timeout_seconds=2.0,
            log_dir=Path(directory),
        )

        def fast_command(job: Job) -> Sequence[str]:
            return fake_command(
                job, ready_sleep_seconds=0.001, phase_sleep_seconds=0.001
            )

        single_payloads, _ = execute_benchmarks(single_config, fast_command)
        single_payload = single_payloads[0]
        assert single_payload["placement"] == "local-baseline"
        assert single_payload["participants"][0]["remote_pid"] == 0

        slow_shutdown_config = Config(
            operations=("create",),
            hosts=single_config.hosts,
            launcher=single_config.launcher,
            path=single_config.path,
            total_work_units=single_config.total_work_units,
            timeout_seconds=20.0,
            log_dir=Path(directory),
        )

        def slow_shutdown_command(job: Job) -> Sequence[str]:
            return fake_command(
                job,
                ready_sleep_seconds=0.001,
                phase_sleep_seconds=0.001,
                shutdown_sleep_seconds=1.2,
            )

        shutdown_started = time.monotonic()
        slow_shutdown_payloads, _ = execute_benchmarks(
            slow_shutdown_config, slow_shutdown_command
        )
        shutdown_elapsed = time.monotonic() - shutdown_started
        assert 1.0 <= shutdown_elapsed < 2.5, shutdown_elapsed
        assert float(slow_shutdown_payloads[0]["elapsed_seconds"]) < 0.15

        def wrong_runner_command(job: Job) -> Sequence[str]:
            return fake_command(
                job,
                ready_sleep_seconds=0.001,
                phase_sleep_seconds=0.001,
                runner_host="unexpected-runner",
            )

        try:
            execute_benchmarks(single_config, wrong_runner_command)
        except BenchmarkError as error:
            assert "strict placement was not honored" in str(error)
        else:
            raise AssertionError("accepted the wrong runner identity")

        def failing_command(job: Job) -> Sequence[str]:
            return fake_command(
                job,
                ready_sleep_seconds=0.001,
                phase_sleep_seconds=0.001,
                exit_code=7,
            )

        try:
            execute_benchmarks(single_config, failing_command)
        except BenchmarkError as error:
            assert "rc=7" in str(error)
        else:
            raise AssertionError("accepted a nonzero worker exit")

        def wrong_worker_count_command(job: Job) -> Sequence[str]:
            return fake_command(
                job,
                ready_sleep_seconds=0.001,
                phase_sleep_seconds=0.001,
                reported_workers=job.workers + 1,
            )

        try:
            execute_benchmarks(single_config, wrong_worker_count_command)
        except BenchmarkError as error:
            assert "wrong worker count" in str(error)
        else:
            raise AssertionError("accepted an incorrect worker-count report")
        diagnostics = io.StringIO()
        with contextlib.redirect_stderr(diagnostics):
            emit_worker_diagnostics("create", make_jobs(single_config))
        assert not any(
            line.lstrip().startswith("{")
            for line in diagnostics.getvalue().splitlines()
        )

        def malformed_command(job: Job) -> Sequence[str]:
            return fake_command(
                job,
                ready_sleep_seconds=0.001,
                phase_sleep_seconds=0.001,
                malformed_json=True,
            )

        try:
            execute_benchmarks(single_config, malformed_command)
        except BenchmarkError as error:
            assert "malformed JSON" in str(error)
        else:
            raise AssertionError("accepted malformed worker JSON")

        timeout_config = Config(
            operations=("rename",),
            hosts=("wos-0",),
            launcher="wos-0",
            path=config.path,
            total_work_units=1,
            timeout_seconds=0.25,
            log_dir=Path(directory),
        )

        def slow_command(job: Job) -> Sequence[str]:
            return fake_command(
                job, ready_sleep_seconds=0.001, phase_sleep_seconds=0.8
            )

        try:
            execute_benchmarks(timeout_config, slow_command)
        except BenchmarkError as error:
            assert "exceeded" in str(error)
        else:
            raise AssertionError("timeout did not stop the worker")

        def never_ready_command(job: Job) -> Sequence[str]:
            return fake_command(
                job, ready_sleep_seconds=0.8, phase_sleep_seconds=0.001
            )

        try:
            execute_benchmarks(timeout_config, never_ready_command)
        except BenchmarkError as error:
            assert "did not become ready" in str(error)
        else:
            raise AssertionError("ready timeout did not stop the worker")

    print("metadata_bench self-test: PASS")
    return 0


def main(argv: Sequence[str]) -> int:
    if argv and argv[0] == "--fake-worker":
        return fake_worker_main(argv[1:])
    if list(argv) == ["--self-test"]:
        return run_self_test()

    jobs: list[Job] = []
    operations = "unknown"
    try:
        config = parse_config(argv)
        operations = ",".join(config.operations)
        jobs = make_jobs(config)
        payloads, jobs = execute_benchmarks(config)
    except BenchmarkError as error:
        emit_worker_diagnostics(operations, jobs)
        print(f"metadata-bench: error: {error}", file=sys.stderr)
        return 1
    except OSError as error:
        emit_worker_diagnostics(operations, jobs)
        print(f"metadata-bench: operating-system error: {error}", file=sys.stderr)
        return 1

    for payload in payloads:
        print(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
