#!/usr/bin/env python3
"""Run fixed-total metadata work concurrently on every requested WOS host."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import math
import re
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
WORKER_SHELL = """\
set -eu
/usr/bin/wosid
exec /usr/bin/testprog "$1" --path "$2" --iterations "$3"
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
POLL_SECONDS = 0.01
LOCAL_RUNTIME_PATHS = ("/usr", "/bin", "/lib", "/lib64", "/libexec", "/share")


class BenchmarkError(RuntimeError):
    """A metadata worker or its evidence violated the benchmark contract."""


@dataclass(frozen=True)
class Config:
    operation: str
    hosts: tuple[str, ...]
    launcher: str
    path: str
    total_work_units: int
    timeout_seconds: float
    log_dir: Path


@dataclass(frozen=True)
class Job:
    index: int
    operation: str
    host: str
    worker_path: str
    work_units: int
    log_path: Path


@dataclass(frozen=True)
class WorkerEvidence:
    spawner_host: str
    runner_host: str
    remote_pid: int
    elapsed_seconds: float


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
    quotient, remainder = divmod(config.total_work_units, host_count)
    jobs = []
    for index, host in enumerate(canonical_hosts):
        work_units = quotient + (1 if index < remainder else 0)
        jobs.append(
            Job(
                index=index,
                operation=config.operation,
                host=host,
                worker_path=f"{config.path}.{config.operation}.worker-{index}",
                work_units=work_units,
                log_path=config.log_dir / f"{config.operation}-worker-{index}.log",
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
        f"vfsbench-{job.operation}",
        job.worker_path,
        str(job.work_units),
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


def launch_and_wait(
    jobs: Sequence[Job],
    timeout_seconds: float,
    command_factory: CommandFactory,
) -> tuple[int, list[int]]:
    commands = [command_factory(job) for job in jobs]
    worker_logs: list[BinaryIO] = []
    try:
        for job in jobs:
            worker_logs.append(job.log_path.open("wb"))
    except BaseException:
        for worker_log in worker_logs:
            worker_log.close()
        raise

    processes: list[subprocess.Popen[bytes]] = []
    started_ns = time.monotonic_ns()
    try:
        with SignalGuard() as signal_guard:
            try:
                for command, worker_log in zip(commands, worker_logs):
                    processes.append(
                        subprocess.Popen(
                            command,
                            stdout=worker_log,
                            stderr=subprocess.STDOUT,
                        )
                    )
                    if signal_guard.received is not None:
                        raise BenchmarkError(
                            f"received signal {signal_guard.received} while launching workers"
                        )

                deadline_ns = started_ns + int(timeout_seconds * 1_000_000_000)
                while running(processes):
                    if signal_guard.received is not None:
                        raise BenchmarkError(
                            f"received signal {signal_guard.received} while waiting for workers"
                        )
                    if time.monotonic_ns() >= deadline_ns:
                        raise BenchmarkError(
                            f"metadata workers exceeded {timeout_seconds:g} seconds"
                        )
                    time.sleep(POLL_SECONDS)

                return_codes = [process.wait() for process in processes]
                elapsed_ns = time.monotonic_ns() - started_ns
            except BaseException:
                terminate_and_reap(processes)
                raise
    finally:
        for worker_log in worker_logs:
            worker_log.close()
    if elapsed_ns <= 0:
        raise BenchmarkError("outer monotonic timer did not advance")
    return elapsed_ns, return_codes


def positive_finite(value: object, description: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BenchmarkError(f"{description} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result <= 0.0:
        raise BenchmarkError(f"{description} must be positive and finite")
    return result


def parse_worker_log(job: Job, launcher: str) -> WorkerEvidence:
    try:
        lines = job.log_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise BenchmarkError(f"cannot read {job.log_path}: {error}") from error

    identity_lines = [
        line.strip() for line in lines if line.strip().startswith("spawner=")
    ]
    if len(identity_lines) != 1:
        raise BenchmarkError(
            f"{job.host} emitted {len(identity_lines)} wosid records; expected one"
        )
    identity = WOSID_PATTERN.fullmatch(identity_lines[0])
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

    json_lines = [line.strip() for line in lines if line.strip().startswith("{")]
    if len(json_lines) != 1:
        raise BenchmarkError(
            f"{job.host} emitted {len(json_lines)} JSON records; expected one"
        )
    try:
        payload = json.loads(json_lines[0])
    except json.JSONDecodeError as error:
        raise BenchmarkError(f"{job.host} emitted malformed JSON: {error}") from error
    if not isinstance(payload, dict):
        raise BenchmarkError(f"{job.host} worker JSON is not an object")
    if payload.get("benchmark") != BENCHMARKS[job.operation]:
        raise BenchmarkError(f"{job.host} reported the wrong benchmark")
    if payload.get("path") != job.worker_path:
        raise BenchmarkError(f"{job.host} reported the wrong worker path")
    iterations = payload.get("iterations")
    if (
        isinstance(iterations, bool)
        or not isinstance(iterations, int)
        or iterations != job.work_units
    ):
        raise BenchmarkError(f"{job.host} reported the wrong iteration count")
    elapsed_seconds = positive_finite(
        payload.get("elapsed_seconds"), f"{job.host} elapsed_seconds"
    )
    return WorkerEvidence(
        spawner_host=spawner,
        runner_host=runner,
        remote_pid=remote_pid,
        elapsed_seconds=elapsed_seconds,
    )


def execute_benchmark(
    config: Config, command_factory: CommandFactory = worker_command
) -> tuple[dict[str, object], list[Job]]:
    config.log_dir.mkdir(parents=True, exist_ok=True)
    jobs = make_jobs(config)
    elapsed_ns, return_codes = launch_and_wait(
        jobs, config.timeout_seconds, command_factory
    )
    failed = [
        f"{job.host} rc={return_code}"
        for job, return_code in zip(jobs, return_codes)
        if return_code != 0
    ]
    if failed:
        raise BenchmarkError("metadata workers failed: " + ", ".join(failed))

    evidence = [parse_worker_log(job, config.launcher) for job in jobs]
    participants = []
    normalized_launcher = normalize_hostname(config.launcher)
    for job, worker_evidence in zip(jobs, evidence):
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
            }
        )

    payload: dict[str, object] = {
        "benchmark": BENCHMARKS[config.operation],
        "path": config.path,
        "iterations": config.total_work_units,
        "elapsed_seconds": elapsed_ns / 1_000_000_000.0,
        "total_work_units": config.total_work_units,
        "placement": "local-baseline" if len(jobs) == 1 else "strict-on",
        "wki_route": "host-path",
        "launcher_host": config.launcher,
        "participants": participants,
    }
    return payload, jobs


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
    parser.add_argument("--operation", required=True, choices=tuple(BENCHMARKS))
    parser.add_argument("--hosts", required=True)
    parser.add_argument("--launcher", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--total-work-units", required=True, type=int)
    parser.add_argument("--timeout-seconds", required=True, type=positive_timeout)
    parser.add_argument("--log-dir", required=True, type=Path)
    arguments = parser.parse_args(argv)

    hosts = parse_hosts(arguments.hosts, arguments.launcher)
    if arguments.total_work_units <= 0:
        raise BenchmarkError("--total-work-units must be positive")
    if not arguments.path.startswith("/tmp/"):
        raise BenchmarkError("--path must be below /tmp for the HOST-routed benchmark")
    return Config(
        operation=arguments.operation,
        hosts=hosts,
        launcher=arguments.launcher.strip(),
        path=arguments.path,
        total_work_units=arguments.total_work_units,
        timeout_seconds=arguments.timeout_seconds,
        log_dir=arguments.log_dir,
    )


def fake_worker_main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--operation", required=True, choices=tuple(BENCHMARKS))
    parser.add_argument("--host", required=True)
    parser.add_argument("--launcher", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--iterations", required=True, type=int)
    parser.add_argument("--sleep-seconds", required=True, type=float)
    parser.add_argument("--exit-code", type=int, default=0)
    parser.add_argument("--malformed-json", action="store_true")
    arguments = parser.parse_args(argv)
    time.sleep(arguments.sleep_seconds)
    remote_pid = (
        0
        if normalize_hostname(arguments.host) == normalize_hostname(arguments.launcher)
        else 1
    )
    print(
        f"spawner={arguments.launcher} host={arguments.host} "
        f"pid=1 remote_pid={remote_pid}"
    )
    if arguments.malformed_json:
        print("{malformed-json")
    else:
        print(
            json.dumps(
                {
                    "benchmark": BENCHMARKS[arguments.operation],
                    "path": arguments.path,
                    "iterations": arguments.iterations,
                    "elapsed_seconds": max(arguments.sleep_seconds, 0.000001),
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    return arguments.exit_code


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
        config = Config(
            operation="create",
            hosts=("wos-0", "wos-1", "wos-2"),
            launcher="wos-0",
            path="/tmp/wos-showcase-vfsbench",
            total_work_units=10,
            timeout_seconds=2.0,
            log_dir=Path(directory),
        )

        def fake_command(
            job: Job,
            sleep_seconds: float = 0.12,
            runner_host: str | None = None,
            launcher: str | None = None,
            exit_code: int = 0,
            malformed_json: bool = False,
        ) -> Sequence[str]:
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "--fake-worker",
                "--operation",
                job.operation,
                "--host",
                runner_host or job.host,
                "--launcher",
                launcher or config.launcher,
                "--path",
                job.worker_path,
                "--iterations",
                str(job.work_units),
                "--sleep-seconds",
                str(sleep_seconds),
                "--exit-code",
                str(exit_code),
            ]
            if malformed_json:
                command.append("--malformed-json")
            return command

        payload, jobs = execute_benchmark(config, fake_command)
        assert [job.work_units for job in jobs] == [4, 3, 3]
        assert payload["total_work_units"] == 10
        assert payload["placement"] == "strict-on"
        assert [
            participant["remote_pid"] for participant in payload["participants"]
        ] == [0, 1, 1]
        assert all(
            participant["spawner_host"] == "wos-0"
            for participant in payload["participants"]
        )
        assert [
            participant["transport"] for participant in payload["participants"]
        ] == [
            "local",
            "wki",
            "wki",
        ]
        elapsed_seconds = float(payload["elapsed_seconds"])
        assert 0.08 <= elapsed_seconds < 0.30, elapsed_seconds
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

        rotated_config = Config(
            operation="create",
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
                sleep_seconds=0.001,
                launcher=rotated_config.launcher,
            )

        rotated_payload, rotated_jobs = execute_benchmark(
            rotated_config, rotated_command
        )
        assert [job.host for job in rotated_jobs] == [
            "wos-0.wos",
            "wos-1.wos",
            "wos-2.wos",
        ]
        assert [job.work_units for job in rotated_jobs] == [4, 3, 3]
        assert rotated_payload["launcher_host"] == "wos-2.wos"
        assert [
            participant["transport"] for participant in rotated_payload["participants"]
        ] == ["wki", "wki", "local"]

        single_config = Config(
            operation="create",
            hosts=("wos-0",),
            launcher="wos-0",
            path=config.path,
            total_work_units=1,
            timeout_seconds=2.0,
            log_dir=Path(directory),
        )

        def fast_command(job: Job) -> Sequence[str]:
            return fake_command(job, sleep_seconds=0.001)

        single_payload, _ = execute_benchmark(single_config, fast_command)
        assert single_payload["placement"] == "local-baseline"
        assert single_payload["participants"][0]["remote_pid"] == 0

        def wrong_runner_command(job: Job) -> Sequence[str]:
            return fake_command(
                job, sleep_seconds=0.001, runner_host="unexpected-runner"
            )

        try:
            execute_benchmark(single_config, wrong_runner_command)
        except BenchmarkError as error:
            assert "strict placement was not honored" in str(error)
        else:
            raise AssertionError("accepted the wrong runner identity")

        def failing_command(job: Job) -> Sequence[str]:
            return fake_command(job, sleep_seconds=0.001, exit_code=7)

        try:
            execute_benchmark(single_config, failing_command)
        except BenchmarkError as error:
            assert "rc=7" in str(error)
        else:
            raise AssertionError("accepted a nonzero worker exit")
        diagnostics = io.StringIO()
        with contextlib.redirect_stderr(diagnostics):
            emit_worker_diagnostics("create", make_jobs(single_config))
        assert not any(
            line.lstrip().startswith("{")
            for line in diagnostics.getvalue().splitlines()
        )

        def malformed_command(job: Job) -> Sequence[str]:
            return fake_command(job, sleep_seconds=0.001, malformed_json=True)

        try:
            execute_benchmark(single_config, malformed_command)
        except BenchmarkError as error:
            assert "malformed JSON" in str(error)
        else:
            raise AssertionError("accepted malformed worker JSON")

        timeout_config = Config(
            operation="rename",
            hosts=("wos-0",),
            launcher="wos-0",
            path=config.path,
            total_work_units=1,
            timeout_seconds=0.03,
            log_dir=Path(directory),
        )

        def slow_command(job: Job) -> Sequence[str]:
            return fake_command(job, sleep_seconds=0.5)

        try:
            execute_benchmark(timeout_config, slow_command)
        except BenchmarkError as error:
            assert "exceeded" in str(error)
        else:
            raise AssertionError("timeout did not stop the worker")

    print("metadata_bench self-test: PASS")
    return 0


def main(argv: Sequence[str]) -> int:
    if argv and argv[0] == "--fake-worker":
        return fake_worker_main(argv[1:])
    if list(argv) == ["--self-test"]:
        return run_self_test()

    jobs: list[Job] = []
    operation = "unknown"
    try:
        config = parse_config(argv)
        operation = config.operation
        jobs = make_jobs(config)
        payload, jobs = execute_benchmark(config)
    except BenchmarkError as error:
        emit_worker_diagnostics(operation, jobs)
        print(f"metadata-bench: error: {error}", file=sys.stderr)
        return 1
    except OSError as error:
        emit_worker_diagnostics(operation, jobs)
        print(f"metadata-bench: operating-system error: {error}", file=sys.stderr)
        return 1

    print(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
