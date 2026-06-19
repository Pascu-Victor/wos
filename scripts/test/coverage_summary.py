#!/usr/bin/env python3
"""Combine WOS line coverage and runtime manifest scores."""

import argparse
import dataclasses
import http.client
import json
import math
import os
import re
import signal
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import runtime_test_audit  # noqa: E402


VM_ALIAS_RE = re.compile(r"^vm(?P<node>\d+)$")
NETWORK_STARTUP_FAILURE_RE = re.compile(
    r"(?:no DHCP offer received|network startup failed|netd exited before eth0 IPv4 configuration)",
    re.IGNORECASE,
)
DEFAULT_HOST_TIMEOUT_SECONDS = 1800
DEFAULT_KTEST_TIMEOUT_SECONDS = 3600
DEFAULT_KTEST_COV_TIMEOUT_SECONDS = 900
DEFAULT_CLUSTER_READY_TIMEOUT_SECONDS = 300
DEFAULT_USERLAND_SSH_TIMEOUT_SECONDS = 300
DEFAULT_USERLAND_BUILD_TIMEOUT_SECONDS = 3600
DEFAULT_USERLAND_SYNC_TIMEOUT_SECONDS = 300
DEFAULT_USERLAND_TIMEOUT_SECONDS = 7200
DEFAULT_USERLAND_SHUTDOWN_TIMEOUT_SECONDS = 300
DEFAULT_USERLAND_PROFILE_FETCH_TIMEOUT_SECONDS = 300
DEFAULT_USERLAND_PROFILE_FILE_FETCH_TIMEOUT_SECONDS = 30
DEFAULT_USERLAND_NETBENCH_CASE_TIMEOUT_SECONDS = 120
DEFAULT_USERLAND_COVERAGE_RENDER_WIDTH = 640
DEFAULT_USERLAND_COVERAGE_RENDER_HEIGHT = 360
DEFAULT_USERLAND_COVERAGE_RENDER_SPP = 4
DEFAULT_USERLAND_COVERAGE_RENDER_MAX_DEPTH = 4
DEFAULT_USERLAND_WRAPPER_PROBE_TIMEOUT_SECONDS = 30
DEFAULT_USERLAND_COVERAGE_BUILD_DIR = "build-userland-coverage"
DEFAULT_WOS_USERLAND_PROFILE_DIR = "/tmp/wos-userland-coverage"
USERLAND_WRAPPER_TIMEOUT_GRACE_SECONDS = 15
SSH_PROBE_TIMEOUT_SECONDS = 10
MAX_USERLAND_VMS = 4
RUN_CONFIG_TIMEOUT_BOUNDS = {
    "host_timeout": DEFAULT_HOST_TIMEOUT_SECONDS,
    "ktest_timeout": DEFAULT_KTEST_TIMEOUT_SECONDS,
    "ktest_cov_timeout": DEFAULT_KTEST_COV_TIMEOUT_SECONDS,
    "cluster_ready_timeout": DEFAULT_CLUSTER_READY_TIMEOUT_SECONDS,
    "ssh_probe_timeout": SSH_PROBE_TIMEOUT_SECONDS,
    "userland_ssh_timeout": DEFAULT_USERLAND_SSH_TIMEOUT_SECONDS,
    "userland_build_timeout": DEFAULT_USERLAND_BUILD_TIMEOUT_SECONDS,
    "userland_sync_timeout": DEFAULT_USERLAND_SYNC_TIMEOUT_SECONDS,
    "userland_timeout": DEFAULT_USERLAND_TIMEOUT_SECONDS,
    "userland_profile_fetch_timeout": DEFAULT_USERLAND_PROFILE_FETCH_TIMEOUT_SECONDS,
    "userland_profile_file_fetch_timeout": DEFAULT_USERLAND_PROFILE_FILE_FETCH_TIMEOUT_SECONDS,
    "userland_wrapper_timeout_grace": USERLAND_WRAPPER_TIMEOUT_GRACE_SECONDS,
    "userland_wrapper_probe_timeout": DEFAULT_USERLAND_WRAPPER_PROBE_TIMEOUT_SECONDS,
    "userland_shutdown_timeout": DEFAULT_USERLAND_SHUTDOWN_TIMEOUT_SECONDS,
    "userland_netbench_case_timeout": DEFAULT_USERLAND_NETBENCH_CASE_TIMEOUT_SECONDS,
}
USERLAND_COVERAGE_SUITE_ENV_DEFAULTS = (
    ("WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS", DEFAULT_USERLAND_NETBENCH_CASE_TIMEOUT_SECONDS),
    ("WOS_SUITE_RENDER_WIDTH", DEFAULT_USERLAND_COVERAGE_RENDER_WIDTH),
    ("WOS_SUITE_RENDER_HEIGHT", DEFAULT_USERLAND_COVERAGE_RENDER_HEIGHT),
    ("WOS_SUITE_RENDER_SPP", DEFAULT_USERLAND_COVERAGE_RENDER_SPP),
    ("WOS_SUITE_RENDER_MAX_DEPTH", DEFAULT_USERLAND_COVERAGE_RENDER_MAX_DEPTH),
)
WOS_USERLAND_COVERAGE_OBJECTS = (
    ("init", "modules/init/init"),
    ("testprog", "modules/testprog/testprog"),
    ("testd", "modules/testd/testd"),
    ("netd", "modules/netd/netd"),
    ("httpd", "modules/httpd/httpd"),
    ("debugserver", "modules/debugserver/debugserver"),
    ("perf", "modules/perf/perf"),
    ("top", "modules/top/top"),
    ("memacc", "modules/memacc/memacc"),
    ("journal", "modules/journal/journal"),
    ("journal_lib", "modules/journal/libjournal.so"),
    ("wkictl", "modules/wkictl/wkictl"),
    ("powerctl", "modules/powerctl/powerctl"),
    ("renderbench", "modules/renderbench/renderbench"),
    ("strace", "modules/strace/strace"),
    ("sftpserver", "modules/sftpserver/sftp-server"),
)


class CoverageInputError(RuntimeError):
    pass


PROGRESS_STREAM = sys.stdout


def set_progress_stream(stream) -> None:
    global PROGRESS_STREAM  # noqa: PLW0603 - CLI mode controls process-wide progress output
    PROGRESS_STREAM = stream


def progress_print(*args, **kwargs) -> None:
    kwargs.setdefault("file", PROGRESS_STREAM)
    kwargs.setdefault("flush", True)
    print(*args, **kwargs)


@dataclasses.dataclass(frozen=True)
class CoverageScore:
    name: str
    hit: int
    total: int
    unit: str
    detail: str = ""

    @property
    def percent(self) -> float:
        return (100.0 * self.hit / self.total) if self.total else 0.0


@dataclasses.dataclass(frozen=True)
class LcovInput:
    name: str
    path: Path
    lines: dict[str, dict[int, int]]


@dataclasses.dataclass(frozen=True)
class AutoRunArtifacts:
    host_lcov: Path | None = None
    kcov_lcov: Path | None = None
    userland_log: Path | None = None
    cluster_log: Path | None = None
    userland_lcov: Path | None = None


@dataclasses.dataclass(frozen=True)
class LogRange:
    path: Path
    start_offset: int
    end_offset: int


@dataclasses.dataclass(frozen=True)
class AutoRunIteration:
    index: int
    results_dir: Path
    artifacts: AutoRunArtifacts
    elapsed_seconds: float
    external_log_ranges: list[LogRange] = dataclasses.field(default_factory=list)
    run_config: dict[str, object] = dataclasses.field(default_factory=dict)
    runtime_delta_percent: float = 0.0


@dataclasses.dataclass(frozen=True)
class ArtifactHealthPattern:
    name: str
    regex: re.Pattern[str]


@dataclasses.dataclass(frozen=True)
class ArtifactHealthIssue:
    path: Path
    line_number: int
    pattern: str
    line: str


@dataclasses.dataclass(frozen=True)
class VerifiedRunAllResults:
    results_dir: Path
    repeat: int
    elapsed_seconds: list[float]
    runtime_delta_percent: float


@dataclasses.dataclass(frozen=True)
class ResumeRunAllState:
    iterations: list[AutoRunIteration]
    artifacts: AutoRunArtifacts | None = None


ARTIFACT_HEALTH_PATTERNS = [
    ArtifactHealthPattern("WOS error log", re.compile(r"^\[[0-9.]+\]\s+error\b", re.IGNORECASE)),
    ArtifactHealthPattern("WOS critical log", re.compile(r"^\[[0-9.]+\]\s+critical\b", re.IGNORECASE)),
    ArtifactHealthPattern("tool error line", re.compile(r"^(?:ERROR|FAILED):\s+")),
    ArtifactHealthPattern("top-level error line", re.compile(r"^ERROR\b")),
    ArtifactHealthPattern("top-level failed line", re.compile(r"^FAILED\b")),
    ArtifactHealthPattern("suite failed case line", re.compile(r"^FAIL\s+\S+")),
    ArtifactHealthPattern("kernel panic", re.compile(r"\b(?:kernel\s+)?panic\b", re.IGNORECASE)),
    ArtifactHealthPattern("kernel oops", re.compile(r"\boops\b", re.IGNORECASE)),
    ArtifactHealthPattern("kernel lockup", re.compile(r"\b(?:soft|hard)?\s*lockup\b", re.IGNORECASE)),
    ArtifactHealthPattern("deadlock or hung task", re.compile(r"\b(?:deadlock|hung task|task blocked for more than)\b", re.IGNORECASE)),
    ArtifactHealthPattern("kernel bug", re.compile(r"\b(?:kernel\s+)?BUG:")),
    ArtifactHealthPattern("triple fault", re.compile(r"\btriple fault\b", re.IGNORECASE)),
    ArtifactHealthPattern("general protection fault", re.compile(r"\bgeneral protection fault\b", re.IGNORECASE)),
    ArtifactHealthPattern("fatal page fault", re.compile(r"\b(?:fatal\s+)?page fault\b", re.IGNORECASE)),
    ArtifactHealthPattern("segmentation fault", re.compile(r"\b(?:segmentation fault|segfault|core dumped)\b", re.IGNORECASE)),
    ArtifactHealthPattern("assertion failure", re.compile(r"\b(?:assertion failed|assert\(.*\)\s+failed)\b", re.IGNORECASE)),
    ArtifactHealthPattern("undefined behavior sanitizer", re.compile(r"\b(?:UBSAN|UndefinedBehaviorSanitizer)\b")),
    ArtifactHealthPattern("sanitizer runtime error", re.compile(r"\bruntime error:", re.IGNORECASE)),
    ArtifactHealthPattern("undefined behavior marker", re.compile(r"\bundefined behaviou?r\b", re.IGNORECASE)),
    ArtifactHealthPattern("address sanitizer", re.compile(r"\b(?:KASAN|AddressSanitizer|LeakSanitizer|ThreadSanitizer)\b")),
    ArtifactHealthPattern("signal crash", re.compile(r"\b(?:SIGSEGV|SIGABRT|SIGBUS|SIGILL|fatal signal|terminated by signal)\b", re.IGNORECASE)),
    ArtifactHealthPattern("qemu abort", re.compile(r"\b(?:qemu: fatal|Aborted)\b", re.IGNORECASE)),
    ArtifactHealthPattern("tool timeout", re.compile(r"^TIMEOUT after \d+(?:\.\d+)?s\b")),
    ArtifactHealthPattern("nonzero command exit", re.compile(r"^EXIT status [1-9][0-9]*\b")),
    ArtifactHealthPattern("suite timeout", re.compile(r"\bTIMEOUT\s+\S+\s+after\s+\d+s\b")),
    ArtifactHealthPattern("suite failed cases", re.compile(r"\bFAIL=([1-9][0-9]*)\b")),
    ArtifactHealthPattern("testd failed checks", re.compile(r"\[TESTD\].*\bDONE:\s+\d+\s+passed,\s+([1-9][0-9]*)\s+failed\b")),
]
ARTIFACT_HEALTH_IGNORE_PATTERNS = [
    re.compile(r"^\s*--\s+\[WOS\]\s+KCOV panic recent-PC trace enabled\s*$", re.IGNORECASE),
]
RUN_ALL_REQUIRED_ARTIFACTS = ("host_lcov", "kcov_lcov", "userland_log", "cluster_log", "userland_lcov")
REQUIRED_FULL_EXTERNAL_LOG_LABELS = (
    "userland/serial-vm0.log",
    "userland/qemu-vm0.log",
    "userland/serial-vm1.log",
    "userland/qemu-vm1.log",
    "userland/serial-vm2.log",
    "userland/qemu-vm2.log",
    "userland/serial-vm3.log",
    "userland/qemu-vm3.log",
    "ktest/serial-vm0.log",
    "ktest/qemu-vm0.log",
)


def normalize_source_path(path: str, root: Path = ROOT) -> str:
    source = Path(path)
    if not source.is_absolute():
        source = root / source
    return str(source.resolve(strict=False))


def display_command(cmd: list[str]) -> str:
    return " ".join(shlex_quote(part) for part in cmd)


def shlex_quote(text: str) -> str:
    return "'" + text.replace("'", "'\"'\"'") + "'" if re.search(r"\s|['\"\\$`!#&;|<>*?(){}[\]]", text) else text


def shell_quote(text: str) -> str:
    return "'" + text.replace("'", "'\"'\"'") + "'"


def run_checked(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int | None = None,
    cwd: Path = ROOT,
    log_path: Path | None = None,
) -> None:
    progress_print(f"$ {display_command(cmd)}")
    log_file = None
    stdout = sys.stderr if PROGRESS_STREAM is sys.stderr else None
    stderr = sys.stderr if PROGRESS_STREAM is sys.stderr else None
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = log_path.open("w", encoding="utf-8")
        print(f"$ {display_command(cmd)}", file=log_file, flush=True)
        stdout = log_file
        stderr = subprocess.STDOUT
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            env=env,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        try:
            returncode = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            signal_process_group(proc, signal.SIGKILL)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            if log_file is not None:
                print(f"TIMEOUT after {timeout}s", file=log_file, flush=True)
            raise subprocess.TimeoutExpired(cmd, timeout) from exc
        if returncode != 0:
            if log_file is not None:
                print(f"EXIT status {returncode}", file=log_file, flush=True)
            raise subprocess.CalledProcessError(returncode, cmd)
    finally:
        if log_file is not None:
            log_file.close()


def run_checked_capture(
    cmd: list[str],
    *,
    timeout: int | None = None,
    cwd: Path = ROOT,
    log_path: Path | None = None,
) -> str:
    progress_print(f"$ {display_command(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        check=False,
    )
    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        exit_text = "" if result.returncode == 0 else f"EXIT status {result.returncode}\n"
        log_path.write_text(
            f"$ {display_command(cmd)}\n"
            f"{result.stdout}"
            f"{result.stderr}"
            f"{exit_text}",
            encoding="utf-8",
        )
    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd, output=result.stdout, stderr=result.stderr)
    return result.stdout


def run_checked_stdout_to_file(
    cmd: list[str],
    stdout_path: Path,
    log_path: Path,
    *,
    timeout: int | None = None,
    cwd: Path = ROOT,
) -> None:
    progress_print(f"$ {display_command(cmd)} > {stdout_path}")
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with stdout_path.open("w") as output, log_path.open("w", encoding="utf-8") as log:
        print(f"$ {display_command(cmd)} > {stdout_path}", file=log, flush=True)
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=output,
            stderr=log,
            start_new_session=True,
        )
        try:
            returncode = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            signal_process_group(proc, signal.SIGKILL)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            print(f"TIMEOUT after {timeout}s", file=log, flush=True)
            raise subprocess.TimeoutExpired(cmd, timeout) from exc
        if returncode != 0:
            print(f"EXIT status {returncode}", file=log, flush=True)
            raise subprocess.CalledProcessError(returncode, cmd)


def signal_process_group(proc: subprocess.Popen, signum: int) -> None:
    try:
        os.killpg(proc.pid, signum)
        return
    except (AttributeError, ProcessLookupError, OSError):
        pass
    try:
        if signum == signal.SIGTERM:
            proc.terminate()
        else:
            proc.kill()
    except OSError:
        pass


def default_jobs() -> int:
    return max(1, os.cpu_count() or 1)


def find_host_compiler() -> tuple[Path, Path, str]:
    candidates = [Path("/usr/bin/clang")]
    path_clang = shutil.which("clang")
    if path_clang:
        candidates.append(Path(path_clang))
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            resolved = candidate.resolve()
        except OSError:
            continue
        if str(resolved).startswith(str((ROOT / "toolchain").resolve(strict=False))):
            continue
        clangxx = resolved.with_name("clang++")
        if clangxx.exists():
            return resolved, clangxx, ""

    clang = ROOT / "toolchain" / "host" / "bin" / "clang"
    clangxx = ROOT / "toolchain" / "host" / "bin" / "clang++"
    if clang.exists() and clangxx.exists():
        return clang, clangxx, "--target=x86_64-pc-linux-gnu --sysroot=/"
    raise CoverageInputError("no suitable host clang found for host gtest coverage")


def find_llvm_tool(name: str, compiler: Path | None = None) -> Path:
    candidates = []
    if compiler is not None:
        candidates.append(compiler.parent / name)
    which = shutil.which(name)
    if which:
        candidates.append(Path(which))
    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            return candidate
    raise CoverageInputError(f"{name} not found; install LLVM tools or put them on PATH")


def host_unit_test_targets(cmake_path: Path = ROOT / "tests" / "host" / "CMakeLists.txt") -> list[str]:
    source = cmake_path.read_text()
    targets = re.findall(r"\bwos_add_unit_test\(\s*([A-Za-z_][A-Za-z0-9_]*)\b", source)
    if not targets:
        raise CoverageInputError(f"{cmake_path}: no host gtest targets found")
    return sorted(dict.fromkeys(targets))


def executable_targets(build_dir: Path, targets: list[str]) -> list[Path]:
    binaries: list[Path] = []
    for target in targets:
        for candidate in (build_dir / "host" / target, build_dir / target):
            if candidate.exists() and os.access(candidate, os.X_OK):
                binaries.append(candidate)
                break
        else:
            raise CoverageInputError(f"host gtest binary for target {target!r} was not built under {build_dir}")
    return binaries


def coverage_ignore_regex(root: Path, build_dir: Path) -> str:
    ignored = [
        root / "tests",
        root / "build",
        root / "build-ktest",
        root / "ktest-data",
        build_dir,
    ]
    parts = [re.escape(str(path.resolve(strict=False))) for path in ignored]
    parts.extend([r".*/_deps/.*", r"/usr/.*"])
    return "|".join(parts)


def run_host_gtest_coverage(results_dir: Path, build_dir: Path, jobs: int, timeout: int | None) -> Path:
    progress_print("\n== Host gtest coverage ==")
    clang, clangxx, host_extra_flags = find_host_compiler()
    llvm_profdata = find_llvm_tool("llvm-profdata", clang)
    llvm_cov = find_llvm_tool("llvm-cov", clang)
    targets = host_unit_test_targets()
    profile_dir = results_dir / "host-profraw"
    profile_dir.mkdir(parents=True, exist_ok=True)
    for profile in profile_dir.glob("*.profraw"):
        profile.unlink()

    coverage_flags = " ".join(part for part in [host_extra_flags, "-fprofile-instr-generate -fcoverage-mapping"] if part)
    env = os.environ.copy()
    env.update(
        {
            "CC": str(clang),
            "CXX": str(clangxx),
            "CFLAGS": "",
            "CXXFLAGS": "",
            "LDFLAGS": "",
        }
    )
    configure_cmd = [
        "cmake",
        "-B",
        str(build_dir),
        "-S",
        str(ROOT / "tests"),
        f"-DCMAKE_C_COMPILER={clang}",
        f"-DCMAKE_CXX_COMPILER={clangxx}",
        f"-DCMAKE_C_FLAGS={coverage_flags}",
        f"-DCMAKE_CXX_FLAGS={coverage_flags}",
        f"-DCMAKE_EXE_LINKER_FLAGS={coverage_flags}",
        "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON",
    ]
    run_checked(configure_cmd, env=env, timeout=timeout, log_path=results_dir / "host-configure.log")
    run_checked(
        ["cmake", "--build", str(build_dir), "--target", *targets, "-j", str(jobs)],
        timeout=timeout,
        log_path=results_dir / "host-build.log",
    )

    ctest_env = env.copy()
    ctest_env["LLVM_PROFILE_FILE"] = str(profile_dir / "%p-%m.profraw")
    run_checked(
        ["ctest", "--test-dir", str(build_dir), "-L", "unit", "--output-on-failure", "-j", str(jobs)],
        env=ctest_env,
        timeout=timeout,
        log_path=results_dir / "host-ctest.log",
    )

    profraws = sorted(profile_dir.glob("*.profraw"))
    if not profraws:
        raise CoverageInputError("host gtest run produced no .profraw files")
    profdata = results_dir / "host-gtest.profdata"
    run_checked(
        [str(llvm_profdata), "merge", "-sparse", "-o", str(profdata), *map(str, profraws)],
        timeout=timeout,
        log_path=results_dir / "host-profdata.log",
    )

    binaries = executable_targets(build_dir, targets)
    lcov_path = results_dir / "host-gtest.info"
    export_cmd = [
        str(llvm_cov),
        "export",
        "-format=lcov",
        "-instr-profile",
        str(profdata),
        str(binaries[0]),
    ]
    for binary in binaries[1:]:
        export_cmd.extend(["-object", str(binary)])
    export_cmd.extend(["-ignore-filename-regex", coverage_ignore_regex(ROOT, build_dir)])
    run_checked_stdout_to_file(export_cmd, lcov_path, results_dir / "host-export.log", timeout=timeout)
    parse_lcov(lcov_path)
    return lcov_path


def run_ktest_kcov(args: argparse.Namespace, results_dir: Path) -> Path:
    progress_print("\n== KTEST KCOV coverage ==")
    cmd = [str(ROOT / "bin" / "wos-ktest")]
    if not args.ktest_setup:
        cmd.append("--no-setup")
    if args.ktest_fast:
        cmd.append("--fast")
    if args.ktest_no_build:
        cmd.append("--no-build")
    if args.ktest_no_package:
        cmd.append("--no-package")
    if args.ktest_reset_sysroot:
        cmd.append("--reset-sysroot")
    if args.ktest_tcg is not None:
        cmd.append("--tcg" if args.ktest_tcg == "" else f"--tcg={args.ktest_tcg}")
    cmd.extend(args.ktest_arg)
    try:
        run_checked(cmd, timeout=args.ktest_timeout, log_path=results_dir / "ktest-run.log")
    finally:
        if args.ktest_setup and not args.ktest_keep_topology:
            try:
                run_checked(
                    [str(ROOT / "bin" / "wos-ktest"), "--teardown"],
                    timeout=300,
                    log_path=results_dir / "ktest-teardown.log",
                )
            except Exception as exc:  # noqa: BLE001 - teardown diagnostics should not mask the KTEST result
                print(f"WARNING: failed to tear down KTEST topology: {exc}", file=sys.stderr)

    lcov_path = results_dir / "kcov.info"
    cov_cmd = [str(ROOT / "bin" / "wos-ktest-cov"), "--lcov", str(lcov_path), *args.ktest_cov_arg]
    run_checked(cov_cmd, timeout=args.ktest_cov_timeout, log_path=results_dir / "ktest-cov.log")
    parse_lcov(lcov_path)
    return lcov_path


def pump_process_output(
    proc: subprocess.Popen[str],
    log_path: Path,
    ready_event: threading.Event,
    command_line: str | None = None,
) -> None:
    with log_path.open("w") as log:
        if command_line is not None:
            log.write(f"$ {command_line}\n")
            log.flush()
        assert proc.stdout is not None
        for line in proc.stdout:
            progress_print(line, end="")
            log.write(line)
            log.flush()
            if "Press Ctrl+C to stop all VMs" in line:
                ready_event.set()


def file_tail(path: Path, max_lines: int = 40) -> str:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return ""
    return "\n".join(lines[-max_lines:])


def cluster_log_suffix(log_path: Path | None) -> str:
    if log_path is None:
        return ""
    tail = file_tail(log_path)
    return f"\ncluster log tail:\n{tail}" if tail else ""


def wait_for_cluster_ready(
    proc: subprocess.Popen[str],
    ready_event: threading.Event,
    timeout: int,
    log_path: Path | None = None,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ready_event.is_set():
            return
        rc = proc.poll()
        if rc is not None:
            raise CoverageInputError(
                f"wos-cluster exited before launch completed (status {rc})"
                f"{cluster_log_suffix(log_path)}"
            )
        time.sleep(0.25)
    raise CoverageInputError(
        f"timed out waiting {timeout}s for wos-cluster launch"
        f"{cluster_log_suffix(log_path)}"
    )


def serial_log_for_host(host: str) -> Path | None:
    match = VM_ALIAS_RE.fullmatch(host)
    if match is None:
        return None
    return ROOT / f"serial-vm{match.group('node')}.log"


def network_startup_failure_for_host(host: str) -> str | None:
    serial_log = serial_log_for_host(host)
    if serial_log is None or not serial_log.exists():
        return None
    try:
        text = serial_log.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    match = NETWORK_STARTUP_FAILURE_RE.search(text)
    if match is None:
        return None
    return (
        f"{host} did not finish network startup ({match.group(0)} in {serial_log}); "
        "the preconfigured --no-setup topology is not providing DHCP/host resolution"
    )


def wait_for_wos_ssh(host: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    ssh_cmd = [str(ROOT / "scripts" / "remote" / "wos_ssh.sh"), host, "true"]
    while time.monotonic() < deadline:
        remaining = max(0.1, deadline - time.monotonic())
        probe_timeout = min(SSH_PROBE_TIMEOUT_SECONDS, remaining)
        try:
            result = subprocess.run(
                ssh_cmd,
                cwd=ROOT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=probe_timeout,
            )
        except subprocess.TimeoutExpired:
            result = None
        if result is not None and result.returncode == 0:
            return
        network_failure = network_startup_failure_for_host(host)
        if network_failure is not None:
            raise CoverageInputError(network_failure)
        time.sleep(2.0)
    raise CoverageInputError(f"timed out waiting {timeout}s for SSH to {host}")


def wait_for_process_exit(
    proc: subprocess.Popen[str] | None,
    timeout: int,
    name: str,
    log_path: Path | None = None,
) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        stop_error = ""
        try:
            stop_cluster(proc)
        except CoverageInputError as stop_exc:
            stop_error = f"\nforced stop failed: {stop_exc}"
        raise CoverageInputError(
            f"timed out waiting {timeout}s for {name} to exit after guest shutdown"
            f"{cluster_log_suffix(log_path)}"
            f"{stop_error}"
        ) from exc


def stop_cluster(proc: subprocess.Popen[str] | None, timeout: int = 20) -> None:
    if proc is None or proc.poll() is not None:
        return
    signal_process_group(proc, signal.SIGTERM)
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        signal_process_group(proc, signal.SIGKILL)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise CoverageInputError(f"timed out waiting {timeout}s for cluster process group to stop") from exc


def env_assignment_value(assignments: list[str], name: str) -> str | None:
    prefix = f"{name}="
    for value in assignments:
        if value.startswith(prefix):
            return value[len(prefix) :]
    return None


def suite_shutdown_exits_vm(value: str | None) -> bool:
    return value is not None and value.lower() in {"shutdown", "poweroff"}


def cluster_node_hosts(config_path_text: str) -> list[str]:
    config_path = Path(config_path_text)
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    try:
        config = json.loads(config_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise CoverageInputError(f"{config_path}: unable to read cluster config for shutdown: {exc}") from exc

    node_ids: set[int] = set()
    for zone in config.get("zones", []):
        if zone.get("id") == "GLOBAL":
            continue
        try:
            count = int(zone.get("nodes", 0))
        except (TypeError, ValueError):
            count = 0
        node_ids.update(range(max(0, count)))
        for node_cfg in zone.get("nodes_config", []):
            try:
                node_ids.add(int(node_cfg.get("id")))
            except (TypeError, ValueError):
                continue
    return [f"vm{node_id}" for node_id in sorted(node_ids)]


def validate_userland_cluster_host_limit(hosts: list[str], context: str) -> None:
    invalid_hosts: list[str] = []
    for host in hosts:
        match = VM_ALIAS_RE.fullmatch(host)
        if match is None or int(match.group("node")) >= MAX_USERLAND_VMS:
            invalid_hosts.append(host)
    if len(hosts) > MAX_USERLAND_VMS or invalid_hosts:
        raise CoverageInputError(
            f"{context}: userland --run-all topology must use at most vm0..vm{MAX_USERLAND_VMS - 1}; "
            f"got {', '.join(hosts) if hosts else '<none>'}"
        )


def ordered_shutdown_hosts(hosts: list[str], primary_host: str) -> list[str]:
    ordered = [host for host in hosts if host != primary_host]
    if primary_host in hosts:
        ordered.append(primary_host)
    return ordered


def request_guest_poweroff(host: str, timeout: int) -> None:
    cmd = [str(ROOT / "scripts" / "remote" / "wos_ssh.sh"), host, "/sbin/poweroff -f"]
    try:
        run_checked(cmd, timeout=timeout)
    except subprocess.CalledProcessError as exc:
        if exc.returncode == 255:
            return
        print(f"WARNING: failed to request poweroff on {host}: {exc}", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001 - cluster wait timeout reports hard shutdown failures
        print(f"WARNING: failed to request poweroff on {host}: {exc}", file=sys.stderr)


def request_userland_cluster_poweroff(args: argparse.Namespace) -> None:
    hosts = cluster_node_hosts(args.userland_cluster_config)
    validate_userland_cluster_host_limit(hosts, args.userland_cluster_config)
    if not hosts:
        print("WARNING: no WOS nodes found in cluster config for guest poweroff", file=sys.stderr)
        return
    timeout = max(1, min(60, args.userland_shutdown_timeout))
    for host in ordered_shutdown_hosts(hosts, args.userland_host):
        request_guest_poweroff(host, timeout)


def append_userland_env_if_absent(cmd: list[str], user_env: list[str], key: str, value: str | int) -> None:
    if env_assignment_value(user_env, key) is None:
        cmd.extend(["--env", f"{key}={value}"])


def append_userland_coverage_suite_env_defaults(cmd: list[str], user_env: list[str]) -> None:
    for key, value in USERLAND_COVERAGE_SUITE_ENV_DEFAULTS:
        append_userland_env_if_absent(cmd, user_env, key, value)


def userland_lcov_enabled(args: argparse.Namespace) -> bool:
    return not bool(getattr(args, "userland_no_lcov", False))


def wos_clang() -> Path:
    clang = ROOT / "toolchain" / "host" / "bin" / "clang"
    if not clang.exists():
        raise CoverageInputError(f"WOS clang not found: {clang}")
    return clang


def wos_runtime_dir() -> Path:
    clang = wos_clang()
    output = run_checked_capture(
        [str(clang), "-print-runtime-dir"],
        timeout=30,
        log_path=None,
    ).strip()
    if not output:
        raise CoverageInputError("WOS clang did not report a runtime directory")
    return Path(output)


def ensure_wos_profile_runtime() -> None:
    profile_runtime = wos_runtime_dir() / "libclang_rt.profile.a"
    if profile_runtime.exists():
        return
    raise CoverageInputError(
        "WOS userspace LCOV requires LLVM's WOS profile runtime, but it is missing: "
        f"{profile_runtime}. Rebuild the WOS toolchain with tools/wos-toolchain.sh; "
        "the script now enables COMPILER_RT_BUILD_PROFILE."
    )


def wos_userland_coverage_build_dir(args: argparse.Namespace) -> Path:
    build_dir = Path(getattr(args, "userland_coverage_build_dir", DEFAULT_USERLAND_COVERAGE_BUILD_DIR))
    if not build_dir.is_absolute():
        build_dir = ROOT / build_dir
    return build_dir


def build_wos_userland_coverage(args: argparse.Namespace, results_dir: Path) -> None:
    ensure_wos_profile_runtime()
    env = os.environ.copy()
    existing_args = env.get("WOS_CMAKE_ARGS", "").strip()
    coverage_args = f"-DWOS_USERSPACE_COVERAGE=ON"
    env["WOS_CMAKE_ARGS"] = " ".join(part for part in (existing_args, coverage_args) if part)
    env["WOS_BUILD_DIR"] = str(wos_userland_coverage_build_dir(args))
    env["WOS_BUILD_TARGET"] = "wos_full"
    run_checked(
        [str(ROOT / "scripts" / "dev" / "build_wos.sh")],
        env=env,
        timeout=args.userland_build_timeout,
        log_path=results_dir / "userland-build.log",
    )


def wos_userland_coverage_objects(build_dir: Path) -> list[Path]:
    objects: list[Path] = []
    missing: list[str] = []
    for label, relative_path in WOS_USERLAND_COVERAGE_OBJECTS:
        path = build_dir / relative_path
        if path.exists() and path.is_file():
            objects.append(path)
        else:
            missing.append(f"{label}={path}")
    if missing:
        shown = ", ".join(missing[:8])
        more = "" if len(missing) <= 8 else f", +{len(missing) - 8} more"
        raise CoverageInputError(
            "WOS userland coverage build is missing expected ELF object(s): "
            f"{shown}{more}"
        )
    return objects


def wos_userland_coverage_ignore_regex(build_dir: Path) -> str:
    ignored = [
        ROOT / "toolchain",
        ROOT / "build",
        ROOT / "build-ktest",
        build_dir,
        ROOT / "ktest-data",
    ]
    parts = [re.escape(str(path.resolve(strict=False))) for path in ignored]
    parts.extend([r"/usr/.*", r".*/_deps/.*"])
    return "|".join(parts)


def remote_userland_profile_dir(results_dir: Path) -> str:
    _ = results_dir  # keep the call-site shape while using the boot-time profile directory
    return DEFAULT_WOS_USERLAND_PROFILE_DIR


def prepare_remote_userland_profile_dir(args: argparse.Namespace, remote_dir: str, results_dir: Path) -> None:
    quoted_dir = shell_quote(remote_dir)
    for host in cluster_node_hosts(args.userland_cluster_config):
        run_checked(
            [
                str(ROOT / "scripts" / "remote" / "wos_ssh.sh"),
                host,
                f"mkdir -p {quoted_dir}",
            ],
            timeout=getattr(args, "userland_profile_fetch_timeout", DEFAULT_USERLAND_PROFILE_FETCH_TIMEOUT_SECONDS),
            log_path=results_dir / f"userland-profraw-prepare-{host}.log",
        )


def userland_profile_file_pattern(remote_dir: str) -> str:
    return f"{remote_dir}/%m-%p.profraw%c"


def userland_profile_file_fetch_timeout(args: argparse.Namespace) -> int:
    overall_timeout = getattr(args, "userland_profile_fetch_timeout", DEFAULT_USERLAND_PROFILE_FETCH_TIMEOUT_SECONDS)
    file_timeout = getattr(args, "userland_profile_file_fetch_timeout", DEFAULT_USERLAND_PROFILE_FILE_FETCH_TIMEOUT_SECONDS)
    return max(1, min(overall_timeout, file_timeout))


def warn_userland_profile_fetch(results_dir: Path, message: str) -> None:
    print(f"WARNING: {message}", file=sys.stderr)
    warning_log = results_dir / "userland-profraw-fetch-warnings.log"
    warning_log.parent.mkdir(parents=True, exist_ok=True)
    with warning_log.open("a", encoding="utf-8") as output:
        print(message, file=output)


def resolve_wos_http_target(host: str, results_dir: Path) -> str:
    output = run_checked_capture(
        [sys.executable, str(ROOT / "scripts" / "remote" / "wos_resolve.py"), "target", host],
        timeout=SSH_PROBE_TIMEOUT_SECONDS,
        log_path=results_dir / f"userland-profraw-resolve-{host}.log",
    )
    target = output.strip()
    return target if target else host


def userland_profile_http_url(target: str, remote_profile: str) -> str:
    quoted_path = urllib.parse.quote(remote_profile, safe="/")
    return f"http://{target}{quoted_path}"


def fetch_http_to_file(url: str, local_path: Path, timeout: int, log_path: Path) -> None:
    progress_print(f"$ HTTP GET {url} > {local_path}")
    local_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = local_path.with_name(f"{local_path.name}.part")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(url, headers={"Connection": "close"})
    bytes_written = 0
    with log_path.open("w", encoding="utf-8") as log:
        print(f"$ HTTP GET {url} > {local_path}", file=log, flush=True)
        try:
            with opener.open(request, timeout=timeout) as response:
                status = getattr(response, "status", 200)
                reason = getattr(response, "reason", "OK")
                print(f"HTTP {status} {reason}", file=log)
                for key, value in response.headers.items():
                    print(f"{key}: {value}", file=log)
                with tmp_path.open("wb") as output:
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        output.write(chunk)
                        bytes_written += len(chunk)
                print(f"bytes: {bytes_written}", file=log, flush=True)
        except Exception as exc:
            print(f"ERROR: {exc}", file=log, flush=True)
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            raise
    tmp_path.replace(local_path)


def fetch_remote_userland_profraws(args: argparse.Namespace, remote_dir: str, results_dir: Path) -> list[Path]:
    quoted_dir = shell_quote(remote_dir)
    remote_snapshot_dir = f"{remote_dir.rstrip('/')}/.fetch"
    quoted_snapshot_dir = shell_quote(remote_snapshot_dir)
    list_command = (
        f"mkdir -p {quoted_snapshot_dir} && "
        f"for old in {quoted_snapshot_dir}/*.profraw; do [ -f \"$old\" ] && rm -f \"$old\"; done; "
        f"for f in {quoted_dir}/*.profraw; do "
        "[ -f \"$f\" ] || continue; "
        "b=${f##*/}; "
        f"dst={quoted_snapshot_dir}/\"$b\"; "
        "cp \"$f\" \"$dst\" && printf '%s\\n' \"$dst\"; "
        "done"
    )
    remote_profiles_by_host: list[tuple[str, str]] = []
    for host in cluster_node_hosts(args.userland_cluster_config):
        try:
            listing = run_checked_capture(
                [str(ROOT / "scripts" / "remote" / "wos_ssh.sh"), host, list_command],
                timeout=getattr(args, "userland_profile_fetch_timeout", DEFAULT_USERLAND_PROFILE_FETCH_TIMEOUT_SECONDS),
                log_path=results_dir / f"userland-profraw-list-{host}.log",
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            warn_userland_profile_fetch(results_dir, f"skipping WOS profile list from {host}: {exc}")
            continue
        remote_profiles_by_host.extend((host, line.strip()) for line in listing.splitlines() if line.strip())
    if not remote_profiles_by_host:
        raise CoverageInputError(
            f"WOS userland run produced no LLVM profile files in {remote_dir}; "
            "the userspace binaries were not built with WOS_USERSPACE_COVERAGE=ON or did not exit cleanly"
        )

    local_dir = results_dir / "userland-profraw"
    local_dir.mkdir(parents=True, exist_ok=True)
    local_profiles: list[Path] = []
    file_timeout = userland_profile_file_fetch_timeout(args)
    http_targets: dict[str, str] = {}
    for host, _remote_profile in remote_profiles_by_host:
        if host in http_targets:
            continue
        try:
            http_targets[host] = resolve_wos_http_target(host, results_dir)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            warn_userland_profile_fetch(results_dir, f"skipping WOS profile HTTP target resolution for {host}: {exc}")
    for index, (host, remote_profile) in enumerate(remote_profiles_by_host, start=1):
        target = http_targets.get(host)
        if target is None:
            continue
        remote_url = userland_profile_http_url(target, remote_profile)
        local_profile = local_dir / f"{index:04d}-{host}-{Path(remote_profile).name}"
        try:
            fetch_http_to_file(
                remote_url,
                local_profile,
                timeout=file_timeout,
                log_path=results_dir / f"userland-profraw-fetch-{index:04d}.log",
            )
        except (OSError, TimeoutError, urllib.error.URLError, http.client.HTTPException) as exc:
            try:
                local_profile.unlink()
            except FileNotFoundError:
                pass
            warn_userland_profile_fetch(results_dir, f"skipping WOS profile HTTP fetch from {host}:{remote_profile}: {exc}")
            continue
        if not local_profile.exists() or local_profile.stat().st_size == 0:
            warn_userland_profile_fetch(results_dir, f"skipping empty WOS profile HTTP fetch from {host}:{remote_profile}")
            continue
        local_profiles.append(local_profile)
    if not local_profiles:
        raise CoverageInputError(
            "WOS userland run produced profile file names, but none could be fetched; "
            f"see {results_dir / 'userland-profraw-fetch-warnings.log'}"
        )
    return local_profiles


def export_wos_userland_lcov(args: argparse.Namespace, results_dir: Path, profraws: list[Path]) -> Path:
    clang = wos_clang()
    llvm_profdata = find_llvm_tool("llvm-profdata", clang)
    llvm_cov = find_llvm_tool("llvm-cov", clang)
    build_dir = wos_userland_coverage_build_dir(args)
    objects = wos_userland_coverage_objects(build_dir)

    profdata = results_dir / "wos-userland.profdata"
    run_checked(
        [str(llvm_profdata), "merge", "-sparse", "-o", str(profdata), *map(str, profraws)],
        timeout=getattr(args, "userland_profile_fetch_timeout", DEFAULT_USERLAND_PROFILE_FETCH_TIMEOUT_SECONDS),
        log_path=results_dir / "userland-profdata.log",
    )

    lcov_path = results_dir / "wos-userland.info"
    export_cmd = [
        str(llvm_cov),
        "export",
        "-format=lcov",
        "-instr-profile",
        str(profdata),
        str(objects[0]),
    ]
    for obj in objects[1:]:
        export_cmd.extend(["-object", str(obj)])
    export_cmd.extend(["-ignore-filename-regex", wos_userland_coverage_ignore_regex(build_dir)])
    run_checked_stdout_to_file(
        export_cmd,
        lcov_path,
        results_dir / "userland-lcov-export.log",
        timeout=getattr(args, "userland_profile_fetch_timeout", DEFAULT_USERLAND_PROFILE_FETCH_TIMEOUT_SECONDS),
    )
    parse_lcov(lcov_path)
    return lcov_path


def sync_userland_cluster_rootfs(args: argparse.Namespace, results_dir: Path) -> None:
    sync_rootfs = bool(getattr(args, "userland_sync_rootfs", False)) and not bool(args.userland_no_sync)
    if not sync_rootfs:
        return
    env = None
    if userland_lcov_enabled(args):
        env = os.environ.copy()
        env["WOS_BUILD_DIR"] = str(wos_userland_coverage_build_dir(args))
    cmd = [
        str(ROOT / "bin" / "wos-cluster"),
        "--sync",
        "--config",
        args.userland_cluster_config,
        "--sync-timeout",
        str(args.userland_sync_timeout),
    ]
    if args.userland_sync_filter:
        cmd.extend(["--filter", args.userland_sync_filter])
    command_timeout = args.userland_sync_timeout if args.userland_sync_timeout > 0 else None
    run_checked(cmd, env=env, timeout=command_timeout, log_path=results_dir / "userland-sync.log")


def run_userland_suite_coverage(args: argparse.Namespace, results_dir: Path) -> tuple[Path, Path | None, Path | None]:
    progress_print("\n== WOS userland suite coverage ==")
    cluster_hosts = cluster_node_hosts(args.userland_cluster_config)
    validate_userland_cluster_host_limit(
        cluster_hosts,
        args.userland_cluster_config,
    )
    collect_lcov = userland_lcov_enabled(args)
    if collect_lcov and args.userland_existing_cluster:
        raise CoverageInputError(
            "WOS userland LCOV requires an auto-launched cluster so boot services inherit "
            "the coverage profile directory from init; pass --userland-no-lcov for an existing cluster"
        )
    requested_shutdown = env_assignment_value(args.userland_env, "WOS_SUITE_SHUTDOWN")
    if collect_lcov and suite_shutdown_exits_vm(requested_shutdown):
        raise CoverageInputError(
            "WOS userland LCOV needs the guest alive for profile fetch; "
            "remove WOS_SUITE_SHUTDOWN or pass --userland-no-lcov for a manifest-only run"
        )
    remote_profile_dir = remote_userland_profile_dir(results_dir) if collect_lcov else None
    cluster_proc: subprocess.Popen[str] | None = None
    cluster_log: Path | None = None
    pump_thread: threading.Thread | None = None
    try:
        if not args.userland_existing_cluster:
            if not args.userland_no_build:
                if collect_lcov:
                    build_wos_userland_coverage(args, results_dir)
                else:
                    run_checked(
                        [str(ROOT / "scripts" / "dev" / "build_wos.sh")],
                        timeout=args.userland_build_timeout,
                        log_path=results_dir / "userland-build.log",
                    )

            cluster_log = results_dir / "cluster-launch.log"
            launch_cmd = [
                str(ROOT / "bin" / "wos-cluster"),
                "--launch",
                "--no-setup",
                "--config",
                args.userland_cluster_config,
            ]
            if args.userland_tcg is not None:
                launch_cmd.append("--tcg" if args.userland_tcg == "" else f"--tcg={args.userland_tcg}")
            launch_display = display_command(launch_cmd)
            progress_print(f"$ {launch_display}")
            launch_env = os.environ.copy()
            launch_env["PYTHONUNBUFFERED"] = "1"
            ready_event = threading.Event()
            cluster_proc = subprocess.Popen(
                launch_cmd,
                cwd=ROOT,
                env=launch_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            pump_thread = threading.Thread(
                target=pump_process_output,
                args=(cluster_proc, cluster_log, ready_event, launch_display),
                daemon=True,
            )
            pump_thread.start()
            wait_for_cluster_ready(cluster_proc, ready_event, args.cluster_ready_timeout, cluster_log)
            for host in (cluster_hosts if collect_lcov else [args.userland_host]):
                wait_for_wos_ssh(host, args.userland_ssh_timeout)

        sync_userland_cluster_rootfs(args, results_dir)
        if collect_lcov and remote_profile_dir is not None:
            prepare_remote_userland_profile_dir(args, remote_profile_dir, results_dir)

        userland_log = results_dir / "userland-suite.log"
        cmd = [
            str(ROOT / "bin" / "wos-userland-suite"),
            args.userland_host,
            "--no-sync",
            "--scale",
            args.userland_scale,
            "--output",
            str(userland_log),
            "--probe-timeout",
            str(DEFAULT_USERLAND_WRAPPER_PROBE_TIMEOUT_SECONDS),
            "--timeout",
            str(args.userland_timeout),
        ]
        auto_shutdown = not args.userland_existing_cluster and requested_shutdown is None
        guest_suite_shutdown = auto_shutdown and not collect_lcov
        expect_guest_exit = guest_suite_shutdown or suite_shutdown_exits_vm(requested_shutdown)
        if guest_suite_shutdown:
            cmd.extend(["--env", "WOS_SUITE_SHUTDOWN=poweroff"])
        if collect_lcov and remote_profile_dir is not None:
            append_userland_env_if_absent(
                cmd,
                args.userland_env,
                "LLVM_PROFILE_FILE",
                userland_profile_file_pattern(remote_profile_dir),
            )
            append_userland_coverage_suite_env_defaults(cmd, args.userland_env)
        else:
            append_userland_env_if_absent(
                cmd,
                args.userland_env,
                "WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS",
                DEFAULT_USERLAND_NETBENCH_CASE_TIMEOUT_SECONDS,
            )
        for value in args.userland_env:
            cmd.extend(["--env", value])
        cmd.extend(args.userland_arg)
        suite_error: Exception | None = None
        try:
            run_checked(cmd, timeout=args.userland_timeout + USERLAND_WRAPPER_TIMEOUT_GRACE_SECONDS)
        except Exception as exc:  # noqa: BLE001 - wait for guest shutdown before preserving original failure
            suite_error = exc

        userland_lcov = None
        if suite_error is None and collect_lcov and remote_profile_dir is not None:
            profraws = fetch_remote_userland_profraws(args, remote_profile_dir, results_dir)
            userland_lcov = export_wos_userland_lcov(args, results_dir, profraws)

        if not args.userland_existing_cluster and (expect_guest_exit or auto_shutdown):
            try:
                request_userland_cluster_poweroff(args)
                wait_for_process_exit(cluster_proc, args.userland_shutdown_timeout, "wos-cluster", cluster_log)
            except CoverageInputError as exc:
                if suite_error is None:
                    raise
                print(f"WARNING: {exc}", file=sys.stderr)
        if suite_error is not None:
            raise suite_error
        scores_from_runtime_log(userland_log, args.allow_userland_skips)
        return userland_log, cluster_log, userland_lcov
    finally:
        if not args.userland_existing_cluster:
            stop_cluster(cluster_proc)
            if pump_thread is not None:
                pump_thread.join(timeout=2)


def run_all_coverage_once(args: argparse.Namespace, results_dir: Path) -> AutoRunArtifacts:
    results_dir.mkdir(parents=True, exist_ok=True)

    host_lcov = None
    kcov_lcov = None
    userland_log = None
    cluster_log = None
    userland_lcov = None
    if not args.skip_host:
        host_lcov = run_host_gtest_coverage(results_dir, Path(args.host_build_dir), args.jobs, args.host_timeout)
    if not args.skip_ktest:
        kcov_lcov = run_ktest_kcov(args, results_dir)
    if not args.skip_userland:
        userland_result = run_userland_suite_coverage(args, results_dir)
        if len(userland_result) == 2:
            userland_log, cluster_log = userland_result
            userland_lcov = existing_file(results_dir / "wos-userland.info")
        else:
            userland_log, cluster_log, userland_lcov = userland_result
        if userland_lcov_enabled(args) and userland_lcov is None:
            raise CoverageInputError("WOS userland LCOV artifact was not produced")
    return AutoRunArtifacts(
        host_lcov=host_lcov,
        kcov_lcov=kcov_lcov,
        userland_log=userland_log,
        cluster_log=cluster_log,
        userland_lcov=userland_lcov,
    )


def run_all_iteration_dir(base_results_dir: Path, index: int, repeat: int) -> Path:
    if repeat == 1:
        return base_results_dir
    return base_results_dir / f"run-{index:04d}"


def manifest_path_value(raw_value: object) -> Path | None:
    return Path(raw_value) if isinstance(raw_value, str) else None


def artifacts_from_manifest_data(data: dict[str, object]) -> AutoRunArtifacts:
    artifacts = data.get("artifacts")
    if not isinstance(artifacts, dict):
        return AutoRunArtifacts()
    return AutoRunArtifacts(
        host_lcov=manifest_path_value(artifacts.get("host_lcov")),
        kcov_lcov=manifest_path_value(artifacts.get("kcov_lcov")),
        userland_log=manifest_path_value(artifacts.get("userland_log")),
        cluster_log=manifest_path_value(artifacts.get("cluster_log")),
        userland_lcov=manifest_path_value(artifacts.get("userland_lcov")),
    )


def log_ranges_from_manifest_data(data: dict[str, object]) -> list[LogRange]:
    raw_ranges = data.get("external_log_ranges")
    if not isinstance(raw_ranges, list):
        return []
    ranges: list[LogRange] = []
    for raw_range in raw_ranges:
        if not isinstance(raw_range, dict):
            continue
        path = raw_range.get("path")
        start_offset = raw_range.get("start_offset")
        end_offset = raw_range.get("end_offset")
        if isinstance(path, str) and isinstance(start_offset, int) and isinstance(end_offset, int):
            ranges.append(LogRange(Path(path), start_offset, end_offset))
    return ranges


def iteration_from_manifest(path: Path, data: dict[str, object]) -> AutoRunIteration:
    return AutoRunIteration(
        index=manifest_int(data, path, "index"),
        results_dir=path.parent,
        artifacts=artifacts_from_manifest_data(data),
        elapsed_seconds=manifest_float(data, path, "elapsed_seconds"),
        external_log_ranges=log_ranges_from_manifest_data(data),
        run_config=data.get("run_config") if isinstance(data.get("run_config"), dict) else {},
        runtime_delta_percent=manifest_float(data, path, "runtime_delta_percent"),
    )


def runtime_delta_percent(iterations: list[AutoRunIteration]) -> float:
    if len(iterations) < 2:
        return 0.0
    fastest = min(iteration.elapsed_seconds for iteration in iterations)
    slowest = max(iteration.elapsed_seconds for iteration in iterations)
    if fastest <= 0.0:
        return 0.0 if slowest <= 0.0 else float("inf")
    return 100.0 * (slowest - fastest) / fastest


def validate_run_all_runtime_delta(args: argparse.Namespace, iterations: list[AutoRunIteration]) -> None:
    if len(iterations) < 2:
        return
    delta = runtime_delta_percent(iterations)
    if delta <= args.max_run_time_delta_pct + 1e-9:
        return
    fastest = min(iterations, key=lambda iteration: iteration.elapsed_seconds)
    slowest = max(iterations, key=lambda iteration: iteration.elapsed_seconds)
    raise CoverageInputError(
        "run-all runtime delta "
        f"{delta:.2f}% exceeds {args.max_run_time_delta_pct:.2f}% "
        f"(fastest run {fastest.index}: {fastest.elapsed_seconds:.3f}s; "
        f"slowest run {slowest.index}: {slowest.elapsed_seconds:.3f}s)"
    )


def existing_file(path: Path) -> Path | None:
    return path if path.exists() and path.is_file() else None


def discover_run_all_artifacts(results_dir: Path, artifacts: AutoRunArtifacts | None = None) -> AutoRunArtifacts:
    return AutoRunArtifacts(
        host_lcov=(
            artifacts.host_lcov
            if artifacts is not None and artifacts.host_lcov is not None
            else existing_file(results_dir / "host-gtest.info")
        ),
        kcov_lcov=(
            artifacts.kcov_lcov
            if artifacts is not None and artifacts.kcov_lcov is not None
            else existing_file(results_dir / "kcov.info")
        ),
        userland_log=(
            artifacts.userland_log
            if artifacts is not None and artifacts.userland_log is not None
            else existing_file(results_dir / "userland-suite.log")
        ),
        cluster_log=(
            artifacts.cluster_log
            if artifacts is not None and artifacts.cluster_log is not None
            else existing_file(results_dir / "cluster-launch.log")
        ),
        userland_lcov=(
            artifacts.userland_lcov
            if artifacts is not None and artifacts.userland_lcov is not None
            else existing_file(results_dir / "wos-userland.info")
        ),
    )


def unique_existing_paths(paths: list[Path]) -> list[Path]:
    seen: set[Path] = set()
    unique: list[Path] = []
    for path in paths:
        resolved = path.resolve(strict=False)
        if resolved in seen or not path.exists() or not path.is_file():
            continue
        seen.add(resolved)
        unique.append(path)
    return unique


def qemu_log_paths_for_host(host: str) -> list[Path]:
    match = VM_ALIAS_RE.fullmatch(host)
    if match is None:
        return []
    node = match.group("node")
    return [
        ROOT / f"qemu-vm{node}.log",
        *sorted(ROOT.glob(f"qemu-vm{node}-cpu*.log")),
    ]


def qemu_primary_log_path_for_host(host: str) -> Path | None:
    match = VM_ALIAS_RE.fullmatch(host)
    if match is None:
        return None
    return ROOT / f"qemu-vm{match.group('node')}.log"


def external_log_range_may_be_empty(path: Path) -> bool:
    return re.fullmatch(r"qemu-vm\d+(?:-cpu\d+)?\.log", path.name) is not None


def expected_external_health_log_paths(args: argparse.Namespace) -> list[Path]:
    paths: list[Path] = []
    if not args.skip_userland:
        hosts = cluster_node_hosts(args.userland_cluster_config)
        validate_userland_cluster_host_limit(hosts, args.userland_cluster_config)
        for host in hosts:
            serial_log = serial_log_for_host(host)
            if serial_log is not None:
                paths.append(serial_log)
            if args.userland_tcg is None:
                qemu_log = qemu_primary_log_path_for_host(host)
                if qemu_log is not None:
                    paths.append(qemu_log)

    if not args.skip_ktest:
        paths.append(ROOT / "ktest-data" / "serial-vm0.log")
        if args.ktest_tcg is None:
            paths.append(ROOT / "ktest-data" / "qemu-vm0.log")

    seen: set[Path] = set()
    unique: list[Path] = []
    for path in paths:
        resolved = path.resolve(strict=False)
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(path)
    return unique


def external_health_log_paths(args: argparse.Namespace) -> list[Path]:
    paths: list[Path] = []
    if not args.skip_userland:
        hosts = cluster_node_hosts(args.userland_cluster_config)
        validate_userland_cluster_host_limit(hosts, args.userland_cluster_config)
        for host in hosts:
            serial_log = serial_log_for_host(host)
            if serial_log is not None:
                paths.append(serial_log)
            paths.extend(qemu_log_paths_for_host(host))

    if not args.skip_ktest:
        paths.extend(
            [
                ROOT / "ktest-data" / "serial-vm0.log",
                ROOT / "ktest-data" / "qemu-vm0.log",
                *sorted((ROOT / "ktest-data").glob("qemu-vm0-cpu*.log")),
            ]
        )

    return unique_existing_paths(paths)


def snapshot_log_offsets(paths: list[Path]) -> dict[Path, int]:
    offsets: dict[Path, int] = {}
    for path in paths:
        try:
            offsets[path.resolve(strict=False)] = path.stat().st_size
        except OSError:
            continue
    return offsets


def capture_log_ranges(paths: list[Path], start_offsets: dict[Path, int]) -> list[LogRange]:
    ranges: list[LogRange] = []
    for path in paths:
        resolved = path.resolve(strict=False)
        try:
            end_offset = path.stat().st_size
        except OSError:
            continue
        start_offset = start_offsets.get(resolved, 0)
        if start_offset < 0 or start_offset > end_offset:
            start_offset = 0
        ranges.append(LogRange(path=path, start_offset=start_offset, end_offset=end_offset))
    return ranges


def missing_expected_external_logs(args: argparse.Namespace, ranges: list[LogRange]) -> list[Path]:
    present = {item.path.resolve(strict=False) for item in ranges}
    return [
        path
        for path in expected_external_health_log_paths(args)
        if path.resolve(strict=False) not in present
    ]


def empty_expected_external_log_ranges(args: argparse.Namespace, ranges: list[LogRange]) -> list[Path]:
    range_by_path = {item.path.resolve(strict=False): item for item in ranges}
    empty: list[Path] = []
    for path in expected_external_health_log_paths(args):
        log_range = range_by_path.get(path.resolve(strict=False))
        if (
            log_range is not None
            and log_range.end_offset <= log_range.start_offset
            and not external_log_range_may_be_empty(path)
        ):
            empty.append(path)
    return empty


def validate_required_external_log_ranges(args: argparse.Namespace, ranges: list[LogRange]) -> None:
    missing = missing_expected_external_logs(args, ranges)
    if missing:
        shown = ", ".join(str(path) for path in missing[:8])
        more = "" if len(missing) <= 8 else f", +{len(missing) - 8} more"
        raise CoverageInputError(
            "missing required external log range(s) for full run-all proof: "
            f"{shown}{more}"
        )

    empty = empty_expected_external_log_ranges(args, ranges)
    if empty:
        shown = ", ".join(str(path) for path in empty[:8])
        more = "" if len(empty) <= 8 else f", +{len(empty) - 8} more"
        raise CoverageInputError(
            "required external log range(s) captured no current-run bytes: "
            f"{shown}{more}"
        )


def run_all_health_log_paths(args: argparse.Namespace, artifacts: AutoRunArtifacts, results_dir: Path) -> list[Path]:
    paths: list[Path] = []
    if artifacts.userland_log is not None:
        paths.append(artifacts.userland_log)
    if artifacts.cluster_log is not None:
        paths.append(artifacts.cluster_log)
    paths.extend(sorted(results_dir.glob("*.log")))
    paths.extend(external_health_log_paths(args))
    return unique_existing_paths(paths)


def count_newlines_before_offset(path: Path, offset: int) -> int:
    remaining = max(0, offset)
    count = 0
    with path.open("rb") as input_file:
        while remaining > 0:
            chunk = input_file.read(min(1024 * 1024, remaining))
            if not chunk:
                break
            count += chunk.count(b"\n")
            remaining -= len(chunk)
    return count


def line_start_before_offset(path: Path, offset: int) -> int:
    if offset <= 0:
        return 0
    position = offset
    with path.open("rb") as input_file:
        while position > 0:
            chunk_size = min(1024 * 1024, position)
            position -= chunk_size
            input_file.seek(position)
            chunk = input_file.read(chunk_size)
            newline = chunk.rfind(b"\n")
            if newline >= 0:
                return position + newline + 1
    return 0


def scan_artifact_health(path: Path, start_offset: int = 0, end_offset: int | None = None) -> list[ArtifactHealthIssue]:
    issues: list[ArtifactHealthIssue] = []
    try:
        file_size = path.stat().st_size
        if start_offset < 0 or start_offset > file_size:
            start_offset = 0
        if end_offset is None or end_offset > file_size:
            end_offset = file_size
        if end_offset < start_offset:
            end_offset = start_offset
        scan_start = line_start_before_offset(path, start_offset) if start_offset > 0 else 0
        line_base = count_newlines_before_offset(path, scan_start) if scan_start > 0 else 0
        remaining = end_offset - scan_start
        with path.open("rb") as input_file:
            if scan_start > 0:
                input_file.seek(scan_start)
            line_start = scan_start
            for line_number, raw_line in enumerate(input_file, start=1):
                if remaining <= 0:
                    break
                if len(raw_line) > remaining:
                    raw_line = raw_line[:remaining]
                remaining -= len(raw_line)
                line_end = line_start + len(raw_line)
                if line_end <= start_offset:
                    line_start = line_end
                    continue
                line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                prefix_chars = 0
                if line_start < start_offset:
                    prefix_bytes = raw_line[: start_offset - line_start]
                    prefix_chars = len(prefix_bytes.decode("utf-8", errors="replace"))
                if any(pattern.search(line) for pattern in ARTIFACT_HEALTH_IGNORE_PATTERNS):
                    line_start = line_end
                    continue
                for pattern in ARTIFACT_HEALTH_PATTERNS:
                    if any(match.end() > prefix_chars for match in pattern.regex.finditer(line)):
                        issues.append(ArtifactHealthIssue(path, line_base + line_number, pattern.name, line))
                        break
                line_start = line_end
    except OSError as exc:
        raise CoverageInputError(f"{path}: unable to scan run artifact health: {exc}") from exc
    return issues


def validate_run_all_artifact_health(
    args: argparse.Namespace,
    artifacts: AutoRunArtifacts,
    results_dir: Path,
    external_log_ranges: list[LogRange] | None = None,
) -> None:
    issues: list[ArtifactHealthIssue] = []
    range_by_path = {
        item.path.resolve(strict=False): item
        for item in (external_log_ranges or [])
    }
    for path in run_all_health_log_paths(args, artifacts, results_dir):
        start_offset = 0
        end_offset = None
        log_range = range_by_path.get(path.resolve(strict=False))
        if log_range is not None:
            start_offset = log_range.start_offset
            end_offset = log_range.end_offset
        issues.extend(scan_artifact_health(path, start_offset=start_offset, end_offset=end_offset))
    if not issues:
        return

    lines = ["run-all artifact health scan found failure marker(s):"]
    for issue in issues[:20]:
        lines.append(f"  - {issue.path}:{issue.line_number}: {issue.pattern}: {issue.line}")
    if len(issues) > 20:
        lines.append(f"  ... {len(issues) - 20} more issue(s)")
    raise CoverageInputError("\n".join(lines))


def load_json_file(path: Path) -> dict[str, object]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CoverageInputError(f"{path}: unable to read JSON manifest: {exc}") from exc
    if not isinstance(data, dict):
        raise CoverageInputError(f"{path}: JSON manifest must be an object")
    return data


def manifest_paths_for_results(results_dir: Path) -> list[Path]:
    single = results_dir / "run-all-iteration.json"
    repeated = sorted(results_dir.glob("run-*/run-all-iteration.json"))
    if single.exists():
        if repeated:
            shown = ", ".join(str(path) for path in repeated[:5])
            more = "" if len(repeated) <= 5 else f", +{len(repeated) - 5} more"
            raise CoverageInputError(
                f"{results_dir}: mixed single-run and repeated run-all manifests found "
                f"({single}; {shown}{more})"
            )
        return [single]
    return repeated


def existing_run_all_state_paths(results_dir: Path) -> list[Path]:
    paths: list[Path] = []
    single = results_dir / "run-all-iteration.json"
    if single.exists():
        paths.append(single)
    paths.extend(
        path
        for path in sorted(results_dir.iterdir()) if path.is_dir() and re.fullmatch(r"run-\d{4}", path.name)
    )
    return paths


def require_clean_run_all_results_dir(args: argparse.Namespace, results_dir: Path) -> None:
    if args.allow_existing_results:
        return
    existing = existing_run_all_state_paths(results_dir)
    if not existing:
        return
    shown = ", ".join(str(path) for path in existing[:5])
    more = "" if len(existing) <= 5 else f", +{len(existing) - 5} more"
    raise CoverageInputError(
        f"{results_dir}: existing run-all state found ({shown}{more}); "
        "choose a fresh --results-dir or pass --allow-existing-results to reuse it"
    )


def resume_run_all_state(args: argparse.Namespace, results_dir: Path) -> ResumeRunAllState:
    existing = existing_run_all_state_paths(results_dir)
    if not existing:
        return ResumeRunAllState(iterations=[])

    single = results_dir / "run-all-iteration.json"
    if single.exists():
        raise CoverageInputError(f"{results_dir}: --resume-run-all requires repeated run-NNNN manifests, found {single}")

    run_dirs = [
        path
        for path in sorted(results_dir.iterdir())
        if path.is_dir() and re.fullmatch(r"run-\d{4}", path.name)
    ]
    missing_manifests = [path for path in run_dirs if not (path / "run-all-iteration.json").exists()]
    if missing_manifests:
        shown = ", ".join(str(path) for path in missing_manifests[:5])
        more = "" if len(missing_manifests) <= 5 else f", +{len(missing_manifests) - 5} more"
        raise CoverageInputError(f"{results_dir}: cannot resume with incomplete run directory/directories: {shown}{more}")

    verify_args = argparse.Namespace(
        verify_run_all_results=str(results_dir),
        verify_repeat=args.repeat,
        max_run_time_delta_pct=args.max_run_time_delta_pct,
        allow_userland_skips=args.allow_userland_skips,
    )
    verified = verify_run_all_results(verify_args, allow_partial=True)
    manifest_paths = manifest_paths_for_results(results_dir)
    manifests = [(path, load_json_file(path)) for path in manifest_paths]
    iterations = [iteration_from_manifest(path, data) for path, data in manifests]
    if not iterations:
        return ResumeRunAllState(iterations=[])
    progress_print(
        f"== resuming run-all at iteration {len(iterations) + 1}/{args.repeat} "
        f"after verifying {len(iterations)} completed iteration(s); "
        f"current runtime delta {verified.runtime_delta_percent:.3f}% =="
    )
    return ResumeRunAllState(iterations=iterations, artifacts=iterations[-1].artifacts)


def manifest_int(data: dict[str, object], path: Path, key: str) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise CoverageInputError(f"{path}: manifest field {key!r} must be an integer")
    return value


def manifest_float(data: dict[str, object], path: Path, key: str) -> float:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise CoverageInputError(f"{path}: manifest field {key!r} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise CoverageInputError(f"{path}: manifest field {key!r} must be finite")
    return result


def verify_manifest_results_dir(data: dict[str, object], path: Path, problems: list[str]) -> None:
    value = data.get("results_dir")
    if not isinstance(value, str) or not value:
        problems.append(f"{path}: manifest field 'results_dir' must be a non-empty string")
        return
    recorded = Path(value).resolve(strict=False)
    expected = path.parent.resolve(strict=False)
    if recorded != expected:
        problems.append(
            f"{path}: manifest results_dir must match manifest directory {expected}: {value}"
        )


def verify_manifest_artifact_paths(
    data: dict[str, object],
    path: Path,
    problems: list[str],
) -> dict[str, Path]:
    artifacts = data.get("artifacts")
    if not isinstance(artifacts, dict):
        problems.append(f"{path}: artifacts must be an object with complete run-all outputs")
        return {}

    resolved: dict[str, Path] = {}
    manifest_dir = path.parent.resolve(strict=False)
    for artifact_name, artifact_path in artifacts.items():
        if artifact_path is None:
            if artifact_name in RUN_ALL_REQUIRED_ARTIFACTS:
                problems.append(f"{path}: required artifact {artifact_name!r} is null")
            continue
        if not isinstance(artifact_path, str):
            problems.append(f"{path}: artifact {artifact_name!r} path must be a string or null")
            continue
        candidate = Path(artifact_path)
        if not candidate.exists():
            problems.append(f"{path}: artifact {artifact_name!r} is missing: {artifact_path}")
            continue
        resolved_candidate = candidate.resolve(strict=False)
        if not resolved_candidate.is_relative_to(manifest_dir):
            problems.append(
                f"{path}: artifact {artifact_name!r} must be inside manifest result directory "
                f"{manifest_dir}: {artifact_path}"
            )
            continue
        resolved[artifact_name] = candidate

    for artifact_name in RUN_ALL_REQUIRED_ARTIFACTS:
        if artifact_name not in resolved:
            problems.append(f"{path}: missing required artifact {artifact_name!r}")
    return resolved


def verify_run_all_core_artifacts(
    manifest_path: Path,
    artifact_paths: dict[str, Path],
    problems: list[str],
    *,
    allow_userland_skips: bool,
) -> None:
    for artifact_name in ("host_lcov", "kcov_lcov", "userland_lcov"):
        artifact_path = artifact_paths.get(artifact_name)
        if artifact_path is None:
            continue
        try:
            parse_lcov(artifact_path)
        except CoverageInputError as exc:
            problems.append(f"{manifest_path}: artifact {artifact_name!r} failed LCOV parse: {exc}")

    userland_log = artifact_paths.get("userland_log")
    if userland_log is None:
        return
    try:
        scores = scores_from_runtime_log(userland_log, allow_userland_skips)
    except CoverageInputError as exc:
        problems.append(f"{manifest_path}: artifact 'userland_log' failed runtime audit: {exc}")
        return

    score_names = {score.name for score in scores}
    for score_name in ("testd", "userland"):
        if score_name not in score_names:
            problems.append(f"{manifest_path}: artifact 'userland_log' is missing {score_name} runtime results")
    for score in scores:
        if score.hit != score.total:
            problems.append(
                f"{manifest_path}: artifact 'userland_log' has incomplete {score.name} results: "
                f"{score.hit}/{score.total}"
            )

    cluster_log = artifact_paths.get("cluster_log")
    if cluster_log is not None:
        try:
            cluster_text = cluster_log.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            problems.append(f"{manifest_path}: artifact 'cluster_log' could not be read: {exc}")
        else:
            launch_lines = [line for line in cluster_text.splitlines() if line.startswith("$ ") and "wos-cluster" in line]
            if not any("--launch" in line and "--no-setup" in line for line in launch_lines):
                problems.append(
                    f"{manifest_path}: artifact 'cluster_log' must record "
                    "wos-cluster --launch --no-setup"
                )


def verify_manifest_external_log_ranges(
    data: dict[str, object],
    path: Path,
    problems: list[str],
) -> list[LogRange]:
    raw_ranges = data.get("external_log_ranges")
    if not isinstance(raw_ranges, list):
        problems.append(f"{path}: external_log_ranges must be a list")
        return []

    ranges: list[LogRange] = []
    seen_paths: set[Path] = set()
    for index, raw_range in enumerate(raw_ranges, start=1):
        label = f"{path}: external_log_ranges[{index}]"
        if not isinstance(raw_range, dict):
            problems.append(f"{label} must be an object")
            continue
        path_value = raw_range.get("path")
        start_value = raw_range.get("start_offset")
        end_value = raw_range.get("end_offset")
        if not isinstance(path_value, str):
            problems.append(f"{label}.path must be a string")
            continue
        if not isinstance(start_value, int) or not isinstance(end_value, int):
            problems.append(f"{label} offsets must be integers")
            continue
        if start_value < 0 or end_value < start_value:
            problems.append(f"{label} has invalid byte range {start_value}..{end_value}")
            continue
        log_path = Path(path_value)
        if not log_path.exists():
            problems.append(f"{label} log is missing: {path_value}")
            continue
        resolved_log_path = log_path.resolve(strict=False)
        if resolved_log_path in seen_paths:
            problems.append(f"{label}.path duplicates an earlier external log range: {path_value}")
            continue
        seen_paths.add(resolved_log_path)
        ranges.append(LogRange(path=log_path, start_offset=start_value, end_offset=end_value))
    return ranges


def full_external_log_label(path: Path) -> str | None:
    name = path.name
    parent = path.parent.name
    if parent == "ktest-data" and name in {"serial-vm0.log", "qemu-vm0.log"}:
        return f"ktest/{name}"
    if re.fullmatch(r"(?:serial|qemu)-vm[0-3]\.log", name):
        return f"userland/{name}"
    return None


def required_full_external_log_paths() -> dict[str, Path]:
    return {
        "userland/serial-vm0.log": ROOT / "serial-vm0.log",
        "userland/qemu-vm0.log": ROOT / "qemu-vm0.log",
        "userland/serial-vm1.log": ROOT / "serial-vm1.log",
        "userland/qemu-vm1.log": ROOT / "qemu-vm1.log",
        "userland/serial-vm2.log": ROOT / "serial-vm2.log",
        "userland/qemu-vm2.log": ROOT / "qemu-vm2.log",
        "userland/serial-vm3.log": ROOT / "serial-vm3.log",
        "userland/qemu-vm3.log": ROOT / "qemu-vm3.log",
        "ktest/serial-vm0.log": ROOT / "ktest-data" / "serial-vm0.log",
        "ktest/qemu-vm0.log": ROOT / "ktest-data" / "qemu-vm0.log",
    }


def missing_required_full_external_log_labels(paths: list[Path]) -> list[str]:
    present = {
        label
        for path in paths
        if (label := full_external_log_label(path)) is not None
    }
    return [label for label in REQUIRED_FULL_EXTERNAL_LOG_LABELS if label not in present]


def verify_required_full_external_log_paths(
    manifest_path: Path,
    paths: list[Path],
    problems: list[str],
) -> None:
    required_paths = required_full_external_log_paths()
    for path in paths:
        label = full_external_log_label(path)
        if label is None:
            continue
        expected_path = required_paths.get(label)
        if expected_path is None:
            continue
        if path.resolve(strict=False) != expected_path.resolve(strict=False):
            problems.append(
                f"{manifest_path}: run_config.expected_external_logs entry for {label} "
                f"must be {expected_path}: {path}"
            )


def verify_no_extra_userland_external_logs(
    manifest_path: Path,
    paths: list[Path],
    problems: list[str],
) -> None:
    extra_logs: list[str] = []
    for path in paths:
        match = re.fullmatch(r"(?:serial|qemu)-vm(?P<node>\d+)\.log", path.name)
        if match is not None and int(match.group("node")) >= MAX_USERLAND_VMS:
            extra_logs.append(str(path))
    if extra_logs:
        problems.append(
            f"{manifest_path}: run_config.expected_external_logs contains unsupported userland VM log(s) "
            f"outside vm0..vm{MAX_USERLAND_VMS - 1}: {', '.join(extra_logs)}"
        )


def verify_run_config_timeout_bound(
    run_config: dict[str, object],
    path: Path,
    problems: list[str],
    key: str,
    default_bound: int | float,
) -> None:
    value = run_config.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        problems.append(f"{path}: run_config.{key} must be numeric")
        return
    value_float = float(value)
    if not math.isfinite(value_float):
        problems.append(f"{path}: run_config.{key} must be finite")
    elif value_float <= 0.0:
        problems.append(f"{path}: run_config.{key} must be positive")
    elif value_float > float(default_bound) + 1e-9:
        problems.append(
            f"{path}: run_config.{key}={value_float:.2f} exceeds default bound {float(default_bound):.2f}"
        )


def verify_run_config_exact_number(
    run_config: dict[str, object],
    path: Path,
    problems: list[str],
    key: str,
    expected_value: int | float,
) -> None:
    value = run_config.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        problems.append(f"{path}: run_config.{key} must be numeric")
        return
    value_float = float(value)
    if not math.isfinite(value_float):
        problems.append(f"{path}: run_config.{key} must be finite")
    elif abs(value_float - float(expected_value)) > 1e-9:
        problems.append(
            f"{path}: run_config.{key}={value_float:.2f} must equal default {float(expected_value):.2f}"
        )


def verify_manifest_run_config(
    data: dict[str, object],
    path: Path,
    problems: list[str],
    *,
    max_run_time_delta_pct: float,
) -> list[Path]:
    run_config = data.get("run_config")
    if not isinstance(run_config, dict):
        problems.append(f"{path}: run_config must be an object proving a full --run-all mode")
        return []

    for key in (
        "skip_host",
        "skip_ktest",
        "skip_userland",
        "ktest_fast",
        "ktest_no_build",
        "ktest_no_package",
        "ktest_reset_sysroot",
        "ktest_keep_topology",
        "userland_existing_cluster",
        "userland_no_build",
        "userland_no_lcov",
        "allow_userland_skips",
        "userland_sync_rootfs",
    ):
        if run_config.get(key) is not False:
            problems.append(f"{path}: run_config.{key} must be false for full run-all verification")
    for key in ("ktest_no_setup", "userland_launch_no_setup"):
        if run_config.get(key) is not True:
            problems.append(f"{path}: run_config.{key} must be true for no-setup verification")
    if run_config.get("userland_lcov") is not True:
        problems.append(f"{path}: run_config.userland_lcov must be true for full run-all verification")
    if run_config.get("userland_auto_shutdown") is not True:
        problems.append(f"{path}: run_config.userland_auto_shutdown must be true for full run-all verification")
    if run_config.get("userland_shutdown_action") != "poweroff":
        problems.append(f"{path}: run_config.userland_shutdown_action must be 'poweroff' for full run-all verification")
    if run_config.get("userland_scale") != "full":
        problems.append(f"{path}: run_config.userland_scale must be 'full' for full run-all verification")
    if run_config.get("userland_host") != "vm0":
        problems.append(f"{path}: run_config.userland_host must be 'vm0' for full run-all verification")
    if not isinstance(run_config.get("userland_coverage_build_dir"), str) or not run_config.get("userland_coverage_build_dir"):
        problems.append(f"{path}: run_config.userland_coverage_build_dir must be a non-empty string")

    for key in ("ktest_tcg", "userland_tcg", "userland_sync_filter"):
        if key not in run_config or run_config.get(key) is not None:
            problems.append(f"{path}: run_config.{key} must be null for default full run-all verification")

    for key in ("ktest_arg", "ktest_cov_arg", "userland_env", "userland_arg"):
        value = run_config.get(key)
        if not isinstance(value, list) or value:
            problems.append(f"{path}: run_config.{key} must be an empty list for default full run-all verification")

    for key, default_bound in RUN_CONFIG_TIMEOUT_BOUNDS.items():
        verify_run_config_timeout_bound(run_config, path, problems, key, default_bound)

    for key, expected_value in (
        ("userland_coverage_render_width", DEFAULT_USERLAND_COVERAGE_RENDER_WIDTH),
        ("userland_coverage_render_height", DEFAULT_USERLAND_COVERAGE_RENDER_HEIGHT),
        ("userland_coverage_render_spp", DEFAULT_USERLAND_COVERAGE_RENDER_SPP),
        ("userland_coverage_render_max_depth", DEFAULT_USERLAND_COVERAGE_RENDER_MAX_DEPTH),
    ):
        verify_run_config_exact_number(run_config, path, problems, key, expected_value)

    cluster_config = run_config.get("userland_cluster_config")
    if not isinstance(cluster_config, str) or not cluster_config:
        problems.append(f"{path}: run_config.userland_cluster_config must be a non-empty string")
    else:
        cluster_config_path = Path(cluster_config)
        if not cluster_config_path.is_absolute():
            cluster_config_path = ROOT / cluster_config_path
        expected_cluster_config = (ROOT / "configs" / "cluster.json").resolve(strict=False)
        if cluster_config_path.resolve(strict=False) != expected_cluster_config:
            problems.append(
                f"{path}: run_config.userland_cluster_config must resolve to "
                f"{expected_cluster_config}: {cluster_config}"
            )

    configured_delta = run_config.get("max_run_time_delta_pct")
    if isinstance(configured_delta, bool) or not isinstance(configured_delta, (int, float)):
        problems.append(f"{path}: run_config.max_run_time_delta_pct must be numeric")
    else:
        configured_delta_float = float(configured_delta)
        if not math.isfinite(configured_delta_float):
            problems.append(f"{path}: run_config.max_run_time_delta_pct must be finite")
        elif configured_delta_float < 0.0:
            problems.append(f"{path}: run_config.max_run_time_delta_pct must be nonnegative")
        elif configured_delta_float > max_run_time_delta_pct + 1e-9:
            problems.append(
                f"{path}: run_config.max_run_time_delta_pct={configured_delta_float:.2f} "
                f"is looser than verification threshold {max_run_time_delta_pct:.2f}"
            )

    raw_expected_logs = run_config.get("expected_external_logs")
    expected_logs: list[Path] = []
    if not isinstance(raw_expected_logs, list):
        problems.append(f"{path}: run_config.expected_external_logs must be a non-empty list")
    else:
        seen_expected_logs: set[Path] = set()
        for index, raw_log in enumerate(raw_expected_logs, start=1):
            if not isinstance(raw_log, str):
                problems.append(f"{path}: run_config.expected_external_logs[{index}] must be a string")
                continue
            expected_log = Path(raw_log)
            resolved_expected_log = expected_log.resolve(strict=False)
            if resolved_expected_log in seen_expected_logs:
                problems.append(f"{path}: run_config.expected_external_logs[{index}] duplicates an earlier log: {raw_log}")
                continue
            seen_expected_logs.add(resolved_expected_log)
            expected_logs.append(expected_log)
        if not expected_logs:
            problems.append(f"{path}: run_config.expected_external_logs must be non-empty")

    missing_log_labels = missing_required_full_external_log_labels(expected_logs)
    if missing_log_labels:
        shown = ", ".join(missing_log_labels[:8])
        more = "" if len(missing_log_labels) <= 8 else f", +{len(missing_log_labels) - 8} more"
        problems.append(
            f"{path}: run_config.expected_external_logs missing required full-run log(s): "
            f"{shown}{more}"
        )
    verify_required_full_external_log_paths(path, expected_logs, problems)
    verify_no_extra_userland_external_logs(path, expected_logs, problems)

    return expected_logs


def verify_expected_external_log_ranges(
    manifest_path: Path,
    expected_logs: list[Path],
    ranges: list[LogRange],
    problems: list[str],
) -> None:
    range_by_path = {item.path.resolve(strict=False): item for item in ranges}
    for expected in expected_logs:
        log_range = range_by_path.get(expected.resolve(strict=False))
        if log_range is None:
            problems.append(f"{manifest_path}: missing external_log_ranges entry for required log {expected}")
        elif log_range.end_offset <= log_range.start_offset and not external_log_range_may_be_empty(expected):
            problems.append(
                f"{manifest_path}: external_log_ranges entry for required log {expected} "
                f"captured no current-run bytes"
            )


def verify_run_all_results(args: argparse.Namespace, *, allow_partial: bool = False) -> VerifiedRunAllResults:
    results_dir = Path(args.verify_run_all_results)
    if not results_dir.is_absolute():
        results_dir = ROOT / results_dir
    if not results_dir.exists() or not results_dir.is_dir():
        raise CoverageInputError(f"{results_dir}: verify results directory does not exist")

    manifest_paths = manifest_paths_for_results(results_dir)
    if not manifest_paths:
        raise CoverageInputError(f"{results_dir}: no run-all iteration manifests found")

    manifests: list[tuple[Path, dict[str, object]]] = [(path, load_json_file(path)) for path in manifest_paths]
    indexes: list[int] = []
    manifest_records: list[tuple[int, float, float, Path]] = []
    repeat_values: set[int] = set()
    problems: list[str] = []
    health_issues: list[ArtifactHealthIssue] = []

    for path, data in manifests:
        index = manifest_int(data, path, "index")
        repeat = manifest_int(data, path, "repeat")
        elapsed = manifest_float(data, path, "elapsed_seconds")
        recorded_delta = manifest_float(data, path, "runtime_delta_percent")
        status = data.get("status")
        indexes.append(index)
        repeat_values.add(repeat)
        manifest_records.append((index, elapsed, recorded_delta, path))
        verify_manifest_results_dir(data, path, problems)
        if path.parent != results_dir and path.parent.name != f"run-{index:04d}":
            problems.append(
                f"{path}: repeated run manifest path must be run-{index:04d}/run-all-iteration.json "
                f"for index {index}"
            )
        if status != "pass":
            problems.append(f"{path}: status={status!r}; error={data.get('error', '')!r}")
        if elapsed <= 0.0:
            problems.append(f"{path}: elapsed_seconds must be positive, got {elapsed}")
        if recorded_delta < 0.0:
            problems.append(f"{path}: runtime_delta_percent must be nonnegative, got {recorded_delta}")
        expected_external_logs = verify_manifest_run_config(
            data,
            path,
            problems,
            max_run_time_delta_pct=args.max_run_time_delta_pct,
        )

        artifact_paths = verify_manifest_artifact_paths(data, path, problems)
        verify_run_all_core_artifacts(
            path,
            artifact_paths,
            problems,
            allow_userland_skips=args.allow_userland_skips,
        )
        external_log_ranges = verify_manifest_external_log_ranges(data, path, problems)
        verify_expected_external_log_ranges(path, expected_external_logs, external_log_ranges, problems)
        for log_range in external_log_ranges:
            health_issues.extend(
                scan_artifact_health(
                    log_range.path,
                    start_offset=log_range.start_offset,
                    end_offset=log_range.end_offset,
                )
            )

    if len(repeat_values) != 1:
        problems.append(f"manifest repeat fields disagree: {sorted(repeat_values)}")
    manifest_repeat = next(iter(repeat_values)) if repeat_values else len(manifests)
    expected_repeat = args.verify_repeat if args.verify_repeat is not None else manifest_repeat
    if allow_partial:
        if len(manifests) > expected_repeat:
            problems.append(f"expected at most {expected_repeat} iteration manifest(s), found {len(manifests)}")
    elif len(manifests) != expected_repeat:
        problems.append(f"expected {expected_repeat} iteration manifest(s), found {len(manifests)}")
    if manifest_repeat != expected_repeat:
        problems.append(f"manifest repeat={manifest_repeat}, expected {expected_repeat}")

    expected_indexes = list(range(1, len(manifests) + 1))
    if sorted(indexes) != expected_indexes:
        problems.append(f"iteration indexes are not contiguous 1..{len(manifests)}: {sorted(indexes)}")

    synthetic_iterations: list[AutoRunIteration] = []
    for index, elapsed, recorded_delta, path in sorted(manifest_records, key=lambda record: record[0]):
        synthetic_iterations.append(
            AutoRunIteration(
                index=index,
                results_dir=path.parent,
                artifacts=AutoRunArtifacts(),
                elapsed_seconds=elapsed,
            )
        )
        expected_delta = runtime_delta_percent(synthetic_iterations)
        if abs(recorded_delta - expected_delta) > 1e-6:
            problems.append(
                f"{path}: runtime_delta_percent {recorded_delta:.6f}% does not match "
                f"cumulative elapsed delta {expected_delta:.6f}%"
            )
    delta = runtime_delta_percent(synthetic_iterations)
    if delta > args.max_run_time_delta_pct + 1e-9:
        problems.append(
            f"run-all runtime delta {delta:.2f}% exceeds {args.max_run_time_delta_pct:.2f}%"
        )

    for log_path in sorted(results_dir.glob("**/*.log")):
        health_issues.extend(scan_artifact_health(log_path))
    for issue in health_issues[:20]:
        problems.append(f"{issue.path}:{issue.line_number}: {issue.pattern}: {issue.line}")
    if len(health_issues) > 20:
        problems.append(f"... {len(health_issues) - 20} more health issue(s)")

    if problems:
        raise CoverageInputError("run-all results verification failed:\n  - " + "\n  - ".join(problems))

    return VerifiedRunAllResults(
        results_dir=results_dir,
        repeat=len(manifests),
        elapsed_seconds=[iteration.elapsed_seconds for iteration in synthetic_iterations],
        runtime_delta_percent=delta,
    )


def run_all_coverage(args: argparse.Namespace) -> tuple[AutoRunArtifacts, list[AutoRunIteration]]:
    results_dir = Path(args.results_dir)
    if not results_dir.is_absolute():
        results_dir = ROOT / results_dir
    results_dir.mkdir(parents=True, exist_ok=True)
    resume_state = ResumeRunAllState(iterations=[])
    if args.resume_run_all:
        resume_state = resume_run_all_state(args, results_dir)
    else:
        require_clean_run_all_results_dir(args, results_dir)

    iterations: list[AutoRunIteration] = list(resume_state.iterations)
    for index in range(len(iterations) + 1, args.repeat + 1):
        iteration_dir = run_all_iteration_dir(results_dir, index, args.repeat)
        if args.repeat > 1:
            progress_print(f"\n== run-all iteration {index}/{args.repeat} ==")
        start_time = time.monotonic()
        manifest_path = iteration_dir / "run-all-iteration.json"
        artifacts: AutoRunArtifacts | None = None
        external_log_ranges: list[LogRange] = []
        external_log_offsets = snapshot_log_offsets(external_health_log_paths(args))
        try:
            artifacts = run_all_coverage_once(args, iteration_dir)
            external_log_ranges = capture_log_ranges(external_health_log_paths(args), external_log_offsets)
            validate_required_external_log_ranges(args, external_log_ranges)
            validate_run_all_artifact_health(args, artifacts, iteration_dir, external_log_ranges)
            elapsed = time.monotonic() - start_time
            run_config = run_all_config_to_json(args)
            iteration = AutoRunIteration(
                index=index,
                results_dir=iteration_dir,
                artifacts=artifacts,
                elapsed_seconds=elapsed,
                external_log_ranges=external_log_ranges,
                run_config=run_config,
            )
            current_delta = runtime_delta_percent([*iterations, iteration])
            iteration = dataclasses.replace(
                iteration,
                runtime_delta_percent=current_delta if iterations else 0.0,
            )
            iterations.append(iteration)
            write_iteration_manifest(
                manifest_path,
                index=index,
                repeat=args.repeat,
                results_dir=iteration_dir,
                status="pass",
                elapsed_seconds=elapsed,
                artifacts=artifacts,
                external_log_ranges=external_log_ranges,
                run_config=run_config,
                runtime_delta_percent_value=iteration.runtime_delta_percent,
            )
            if args.repeat > 1:
                progress_print(f"== run-all iteration {index}/{args.repeat} completed in {elapsed:.3f}s ==")
                validate_run_all_runtime_delta(args, iterations)
        except Exception as exc:
            elapsed = time.monotonic() - start_time
            artifacts = discover_run_all_artifacts(iteration_dir, artifacts)
            external_log_ranges = capture_log_ranges(external_health_log_paths(args), external_log_offsets)
            write_iteration_manifest(
                manifest_path,
                index=index,
                repeat=args.repeat,
                results_dir=iteration_dir,
                status="fail",
                elapsed_seconds=elapsed,
                artifacts=artifacts,
                external_log_ranges=external_log_ranges,
                run_config=run_all_config_to_json(args),
                error=str(exc),
                runtime_delta_percent_value=runtime_delta_percent(iterations) if iterations else 0.0,
            )
            raise

    validate_run_all_runtime_delta(args, iterations)
    if resume_state.artifacts is not None and len(iterations) == len(resume_state.iterations):
        return resume_state.artifacts, iterations
    return iterations[-1].artifacts, iterations


def parse_lcov(path: Path) -> dict[str, dict[int, int]]:
    lines_by_file: dict[str, dict[int, int]] = {}
    current_file: str | None = None
    saw_record = False

    try:
        raw_lines = path.read_text(errors="replace").splitlines()
    except OSError as exc:
        raise CoverageInputError(f"{path}: unable to read LCOV file: {exc}") from exc

    for raw_line in raw_lines:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("SF:"):
            current_file = normalize_source_path(line[3:])
            lines_by_file.setdefault(current_file, {})
            saw_record = True
            continue
        if line.startswith("DA:"):
            if current_file is None:
                raise CoverageInputError(f"{path}: DA entry appeared before an SF entry")
            payload = line[3:].split(",")
            if len(payload) < 2:
                raise CoverageInputError(f"{path}: malformed DA entry {line!r}")
            try:
                source_line = int(payload[0])
                hit_count = int(payload[1])
            except ValueError as exc:
                raise CoverageInputError(f"{path}: malformed DA entry {line!r}") from exc
            if source_line <= 0:
                raise CoverageInputError(f"{path}: DA entry has invalid source line {source_line}")
            existing = lines_by_file[current_file].get(source_line, 0)
            lines_by_file[current_file][source_line] = max(existing, hit_count)
            continue
        if line == "end_of_record":
            current_file = None

    if not saw_record:
        raise CoverageInputError(f"{path}: no LCOV source-file records found")
    if not any(lines for lines in lines_by_file.values()):
        raise CoverageInputError(f"{path}: no LCOV DA line records found")
    return lines_by_file


def merge_lcov_lines(inputs: list[LcovInput]) -> dict[str, dict[int, int]]:
    merged: dict[str, dict[int, int]] = {}
    for input_file in inputs:
        for source, lines in input_file.lines.items():
            dest = merged.setdefault(source, {})
            for line, hit_count in lines.items():
                dest[line] = max(dest.get(line, 0), hit_count)
    return merged


def score_lcov_lines(name: str, lines_by_file: dict[str, dict[int, int]], detail: str = "") -> CoverageScore:
    total = sum(len(lines) for lines in lines_by_file.values())
    hit = sum(1 for lines in lines_by_file.values() for hit_count in lines.values() if hit_count > 0)
    return CoverageScore(name=name, hit=hit, total=total, unit="lines", detail=detail)


def parse_named_path(text: str) -> tuple[str, Path]:
    if "=" not in text:
        raise CoverageInputError(f"expected NAME=PATH, got {text!r}")
    name, path = text.split("=", 1)
    name = name.strip()
    if not name:
        raise CoverageInputError(f"coverage input name cannot be empty in {text!r}")
    if not path:
        raise CoverageInputError(f"coverage input path cannot be empty in {text!r}")
    return name, Path(path)


def parse_ratio(text: str) -> tuple[int, int]:
    if "/" not in text:
        raise CoverageInputError(f"expected HIT/TOTAL, got {text!r}")
    hit_text, total_text = text.split("/", 1)
    try:
        hit = int(hit_text)
        total = int(total_text)
    except ValueError as exc:
        raise CoverageInputError(f"expected integer HIT/TOTAL, got {text!r}") from exc
    if hit < 0 or total <= 0 or hit > total:
        raise CoverageInputError(f"invalid coverage ratio {text!r}")
    return hit, total


def parse_named_score(text: str) -> CoverageScore:
    if "=" not in text:
        raise CoverageInputError(f"expected NAME=HIT/TOTAL, got {text!r}")
    name, ratio = text.split("=", 1)
    name = name.strip()
    if not name:
        raise CoverageInputError(f"coverage score name cannot be empty in {text!r}")
    hit, total = parse_ratio(ratio)
    return CoverageScore(name=name, hit=hit, total=total, unit="units", detail="explicit score")


def uniquify_name(name: str, used: set[str]) -> str:
    if name not in used:
        used.add(name)
        return name
    index = 2
    while f"{name}[{index}]" in used:
        index += 1
    unique = f"{name}[{index}]"
    used.add(unique)
    return unique


def add_lcov_input(inputs: list[LcovInput], used_names: set[str], name: str, path: Path) -> None:
    unique_name = uniquify_name(name, used_names)
    inputs.append(LcovInput(unique_name, path, parse_lcov(path)))


def has_userland_markers(text: str) -> bool:
    return any(
        runtime_test_audit.USERLAND_RUN_START_RE.search(line)
        or runtime_test_audit.USERLAND_SUMMARY_START_RE.search(line)
        for line in text.splitlines()
    )


def testd_score_from_text(name: str, text: str, detail: str) -> CoverageScore:
    result = runtime_test_audit.audit_testd_text(text)
    total = len(runtime_test_audit.testd_expected_pass_labels())
    return CoverageScore(name=name, hit=result.passed, total=total, unit="checks", detail=detail)


def userland_score_from_text(name: str, text: str, detail: str, allow_skips: bool) -> CoverageScore:
    result = runtime_test_audit.audit_userland_text(text, allow_skips=allow_skips)
    total = len(runtime_test_audit.userland_expected_cases())
    return CoverageScore(name=name, hit=result.passed, total=total, unit="cases", detail=detail)


def read_runtime_log(path: Path) -> str:
    try:
        return path.read_text(errors="replace")
    except OSError as exc:
        raise CoverageInputError(f"{path}: unable to read runtime log: {exc}") from exc


def scores_from_runtime_log(path: Path, allow_userland_skips: bool) -> list[CoverageScore]:
    text = read_runtime_log(path)
    scores: list[CoverageScore] = []
    detail = str(path)
    if "[TESTD]" in text:
        scores.append(testd_score_from_text("testd", text, detail))
    if has_userland_markers(text):
        scores.append(userland_score_from_text("userland", text, detail, allow_userland_skips))
    if not scores:
        raise CoverageInputError(f"{path}: no TESTD or WOS userland suite markers found")
    return scores


def combined_score(scores: list[CoverageScore]) -> CoverageScore:
    total = sum(score.total for score in scores)
    if total <= 0:
        raise CoverageInputError("no coverage inputs were provided")
    hit = sum(score.hit for score in scores)
    return CoverageScore(name="combined", hit=hit, total=total, unit="units")


def format_score(score: CoverageScore) -> str:
    detail = f"  {score.detail}" if score.detail else ""
    return f"{score.name}: {score.hit}/{score.total} {score.unit} ({score.percent:.1f}%){detail}"


def score_to_json(score: CoverageScore) -> dict[str, object]:
    return {
        "name": score.name,
        "hit": score.hit,
        "total": score.total,
        "unit": score.unit,
        "percent": score.percent,
        "detail": score.detail,
    }


def artifacts_to_json(artifacts: AutoRunArtifacts | None) -> dict[str, str | None] | None:
    if artifacts is None:
        return None
    return {
        "host_lcov": str(artifacts.host_lcov) if artifacts.host_lcov is not None else None,
        "kcov_lcov": str(artifacts.kcov_lcov) if artifacts.kcov_lcov is not None else None,
        "userland_log": str(artifacts.userland_log) if artifacts.userland_log is not None else None,
        "cluster_log": str(artifacts.cluster_log) if artifacts.cluster_log is not None else None,
        "userland_lcov": str(artifacts.userland_lcov) if artifacts.userland_lcov is not None else None,
    }


def log_ranges_to_json(log_ranges: list[LogRange] | None) -> list[dict[str, object]] | None:
    if log_ranges is None:
        return None
    return [
        {
            "path": str(item.path),
            "start_offset": item.start_offset,
            "end_offset": item.end_offset,
        }
        for item in log_ranges
    ]


def full_run_all_config_json() -> dict[str, object]:
    return {
        "skip_host": False,
        "skip_ktest": False,
        "skip_userland": False,
        "ktest_fast": False,
        "ktest_no_build": False,
        "ktest_no_package": False,
        "ktest_reset_sysroot": False,
        "ktest_keep_topology": False,
        "ktest_tcg": None,
        "ktest_arg": [],
        "ktest_cov_arg": [],
        "ktest_no_setup": True,
        "userland_existing_cluster": False,
        "userland_no_build": False,
        "userland_no_lcov": False,
        "userland_lcov": True,
        "userland_coverage_build_dir": DEFAULT_USERLAND_COVERAGE_BUILD_DIR,
        "userland_launch_no_setup": True,
        "userland_auto_shutdown": True,
        "userland_shutdown_action": "poweroff",
        "userland_tcg": None,
        "allow_userland_skips": False,
        "userland_no_sync": False,
        "userland_sync_rootfs": False,
        "userland_sync_filter": None,
        "userland_env": [],
        "userland_arg": [],
        "max_run_time_delta_pct": 2.0,
        "host_timeout": DEFAULT_HOST_TIMEOUT_SECONDS,
        "ktest_timeout": DEFAULT_KTEST_TIMEOUT_SECONDS,
        "ktest_cov_timeout": DEFAULT_KTEST_COV_TIMEOUT_SECONDS,
        "cluster_ready_timeout": DEFAULT_CLUSTER_READY_TIMEOUT_SECONDS,
        "ssh_probe_timeout": SSH_PROBE_TIMEOUT_SECONDS,
        "userland_ssh_timeout": DEFAULT_USERLAND_SSH_TIMEOUT_SECONDS,
        "userland_build_timeout": DEFAULT_USERLAND_BUILD_TIMEOUT_SECONDS,
        "userland_sync_timeout": DEFAULT_USERLAND_SYNC_TIMEOUT_SECONDS,
        "userland_timeout": DEFAULT_USERLAND_TIMEOUT_SECONDS,
        "userland_profile_fetch_timeout": DEFAULT_USERLAND_PROFILE_FETCH_TIMEOUT_SECONDS,
        "userland_profile_file_fetch_timeout": DEFAULT_USERLAND_PROFILE_FILE_FETCH_TIMEOUT_SECONDS,
        "userland_wrapper_timeout_grace": USERLAND_WRAPPER_TIMEOUT_GRACE_SECONDS,
        "userland_wrapper_probe_timeout": DEFAULT_USERLAND_WRAPPER_PROBE_TIMEOUT_SECONDS,
        "userland_shutdown_timeout": DEFAULT_USERLAND_SHUTDOWN_TIMEOUT_SECONDS,
        "userland_netbench_case_timeout": DEFAULT_USERLAND_NETBENCH_CASE_TIMEOUT_SECONDS,
        "userland_coverage_render_width": DEFAULT_USERLAND_COVERAGE_RENDER_WIDTH,
        "userland_coverage_render_height": DEFAULT_USERLAND_COVERAGE_RENDER_HEIGHT,
        "userland_coverage_render_spp": DEFAULT_USERLAND_COVERAGE_RENDER_SPP,
        "userland_coverage_render_max_depth": DEFAULT_USERLAND_COVERAGE_RENDER_MAX_DEPTH,
        "expected_external_logs": [],
        "userland_host": "vm0",
        "userland_scale": "full",
        "userland_cluster_config": "configs/cluster.json",
    }


def run_all_config_to_json(args: argparse.Namespace) -> dict[str, object]:
    requested_shutdown = env_assignment_value(args.userland_env, "WOS_SUITE_SHUTDOWN")
    userland_auto_shutdown = not bool(args.userland_existing_cluster) and requested_shutdown is None
    userland_sync_rootfs = bool(getattr(args, "userland_sync_rootfs", False)) and not bool(args.userland_no_sync)
    return {
        "skip_host": bool(args.skip_host),
        "skip_ktest": bool(args.skip_ktest),
        "skip_userland": bool(args.skip_userland),
        "ktest_fast": bool(args.ktest_fast),
        "ktest_no_build": bool(args.ktest_no_build),
        "ktest_no_package": bool(args.ktest_no_package),
        "ktest_reset_sysroot": bool(args.ktest_reset_sysroot),
        "ktest_keep_topology": bool(args.ktest_keep_topology),
        "ktest_tcg": args.ktest_tcg,
        "ktest_arg": list(args.ktest_arg),
        "ktest_cov_arg": list(args.ktest_cov_arg),
        "ktest_no_setup": not bool(args.ktest_setup),
        "userland_existing_cluster": bool(args.userland_existing_cluster),
        "userland_no_build": bool(args.userland_no_build),
        "userland_no_lcov": bool(getattr(args, "userland_no_lcov", False)),
        "userland_lcov": userland_lcov_enabled(args),
        "userland_coverage_build_dir": str(getattr(args, "userland_coverage_build_dir", DEFAULT_USERLAND_COVERAGE_BUILD_DIR)),
        "userland_launch_no_setup": not bool(args.userland_existing_cluster),
        "userland_auto_shutdown": userland_auto_shutdown,
        "userland_shutdown_action": "poweroff" if userland_auto_shutdown else requested_shutdown,
        "userland_tcg": args.userland_tcg,
        "allow_userland_skips": bool(args.allow_userland_skips),
        "userland_no_sync": bool(args.userland_no_sync),
        "userland_sync_rootfs": userland_sync_rootfs,
        "userland_sync_filter": args.userland_sync_filter,
        "userland_env": list(args.userland_env),
        "userland_arg": list(args.userland_arg),
        "max_run_time_delta_pct": float(args.max_run_time_delta_pct),
        "host_timeout": args.host_timeout,
        "ktest_timeout": args.ktest_timeout,
        "ktest_cov_timeout": args.ktest_cov_timeout,
        "cluster_ready_timeout": args.cluster_ready_timeout,
        "ssh_probe_timeout": SSH_PROBE_TIMEOUT_SECONDS,
        "userland_ssh_timeout": args.userland_ssh_timeout,
        "userland_build_timeout": args.userland_build_timeout,
        "userland_sync_timeout": args.userland_sync_timeout,
        "userland_timeout": args.userland_timeout,
        "userland_profile_fetch_timeout": getattr(args, "userland_profile_fetch_timeout", DEFAULT_USERLAND_PROFILE_FETCH_TIMEOUT_SECONDS),
        "userland_profile_file_fetch_timeout": getattr(
            args,
            "userland_profile_file_fetch_timeout",
            DEFAULT_USERLAND_PROFILE_FILE_FETCH_TIMEOUT_SECONDS,
        ),
        "userland_wrapper_timeout_grace": USERLAND_WRAPPER_TIMEOUT_GRACE_SECONDS,
        "userland_wrapper_probe_timeout": DEFAULT_USERLAND_WRAPPER_PROBE_TIMEOUT_SECONDS,
        "userland_shutdown_timeout": args.userland_shutdown_timeout,
        "userland_netbench_case_timeout": DEFAULT_USERLAND_NETBENCH_CASE_TIMEOUT_SECONDS,
        "userland_coverage_render_width": DEFAULT_USERLAND_COVERAGE_RENDER_WIDTH,
        "userland_coverage_render_height": DEFAULT_USERLAND_COVERAGE_RENDER_HEIGHT,
        "userland_coverage_render_spp": DEFAULT_USERLAND_COVERAGE_RENDER_SPP,
        "userland_coverage_render_max_depth": DEFAULT_USERLAND_COVERAGE_RENDER_MAX_DEPTH,
        "expected_external_logs": [str(path) for path in expected_external_health_log_paths(args)],
        "userland_host": args.userland_host,
        "userland_scale": args.userland_scale,
        "userland_cluster_config": args.userland_cluster_config,
    }


def write_iteration_manifest(
    path: Path,
    *,
    index: int,
    repeat: int,
    results_dir: Path,
    status: str,
    elapsed_seconds: float,
    artifacts: AutoRunArtifacts | None = None,
    external_log_ranges: list[LogRange] | None = None,
    run_config: dict[str, object] | None = None,
    error: str | None = None,
    runtime_delta_percent_value: float | None = None,
) -> None:
    data: dict[str, object] = {
        "index": index,
        "repeat": repeat,
        "status": status,
        "results_dir": str(results_dir),
        "elapsed_seconds": elapsed_seconds,
        "artifacts": artifacts_to_json(artifacts),
    }
    if external_log_ranges is not None:
        data["external_log_ranges"] = log_ranges_to_json(external_log_ranges)
    if run_config is not None:
        data["run_config"] = run_config
    if error is not None:
        data["error"] = error
    if runtime_delta_percent_value is not None:
        data["runtime_delta_percent"] = runtime_delta_percent_value
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def run_iterations_to_json(iterations: list[AutoRunIteration]) -> list[dict[str, object]]:
    return [
        {
            "index": iteration.index,
            "results_dir": str(iteration.results_dir),
            "elapsed_seconds": iteration.elapsed_seconds,
            "artifacts": artifacts_to_json(iteration.artifacts),
            "external_log_ranges": log_ranges_to_json(iteration.external_log_ranges),
            "run_config": iteration.run_config,
            "runtime_delta_percent": iteration.runtime_delta_percent,
        }
        for iteration in iterations
    ]


def print_run_iterations(iterations: list[AutoRunIteration]) -> None:
    if len(iterations) < 2:
        return
    delta = runtime_delta_percent(iterations)
    print("Run-all iterations:")
    for iteration in iterations:
        print(f"  run {iteration.index}: {iteration.elapsed_seconds:.3f}s  {iteration.results_dir}")
    print(f"  fastest/slowest delta: {delta:.2f}%")
    print()


def print_auto_artifacts(artifacts: AutoRunArtifacts | None) -> None:
    if artifacts is None:
        return
    print("Auto-run artifacts:")
    for label, path in (
        ("host LCOV", artifacts.host_lcov),
        ("KCOV LCOV", artifacts.kcov_lcov),
        ("WOS userland LCOV", artifacts.userland_lcov),
        ("userland log", artifacts.userland_log),
        ("cluster log", artifacts.cluster_log),
    ):
        if path is not None:
            print(f"  {label}: {path}")
    print()


def print_text_report(
    artifacts: AutoRunArtifacts | None,
    iterations: list[AutoRunIteration],
    lcov_inputs: list[LcovInput],
    lcov_input_scores: list[CoverageScore],
    merged_lcov_score: CoverageScore | None,
    runtime_scores: list[CoverageScore],
    explicit_scores: list[CoverageScore],
    combined: CoverageScore,
) -> None:
    print_run_iterations(iterations)
    print_auto_artifacts(artifacts)
    if lcov_inputs:
        print("LCOV line coverage:")
        for score in lcov_input_scores:
            print(f"  {format_score(score)}")
        if merged_lcov_score is not None:
            print(f"  {format_score(merged_lcov_score)}  merged line universe")
        print()

    if runtime_scores:
        print("Runtime manifest coverage:")
        for score in runtime_scores:
            print(f"  {format_score(score)}")
        print()

    if explicit_scores:
        print("Explicit coverage scores:")
        for score in explicit_scores:
            print(f"  {format_score(score)}")
        print()

    print(f"Combined WOS test coverage: {combined.hit}/{combined.total} units ({combined.percent:.1f}%)")


def coverage_threshold_arg(text: str) -> float:
    try:
        value = float(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{text!r} is not a number") from exc
    if not math.isfinite(value):
        raise argparse.ArgumentTypeError("coverage threshold must be finite")
    if value < 0.0 or value > 100.0:
        raise argparse.ArgumentTypeError("coverage threshold must be between 0 and 100")
    return value


def nonnegative_float_arg(text: str) -> float:
    try:
        value = float(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{text!r} is not a number") from exc
    if not math.isfinite(value):
        raise argparse.ArgumentTypeError("value must be finite")
    if value < 0.0:
        raise argparse.ArgumentTypeError("value must be nonnegative")
    return value


def positive_int_arg(text: str) -> int:
    try:
        value = int(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{text!r} is not an integer") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Merge KCOV LCOV, host gtest LCOV, and WOS runtime coverage scores",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  bin/wos-coverage-summary --run-all\n"
            "  bin/wos-coverage-summary --kcov-lcov ktest.info "
            "--host-lcov host.info --runtime-log userland.log"
        ),
    )
    parser.add_argument(
        "--run-all",
        action="store_true",
        help="run host gtests with coverage, KTEST/KCOV, and the WOS userland suite before aggregating",
    )
    parser.add_argument("--results-dir", default="test-results/coverage", help="artifact directory for --run-all")
    parser.add_argument(
        "--allow-existing-results",
        action="store_true",
        help="with --run-all, allow an existing run-all manifest or run-NNNN directory in --results-dir",
    )
    parser.add_argument(
        "--resume-run-all",
        action="store_true",
        help="with --run-all --repeat, verify and skip completed leading run-NNNN iterations in --results-dir",
    )
    parser.add_argument("--repeat", type=positive_int_arg, default=1, help="number of --run-all iterations to execute")
    parser.add_argument(
        "--max-run-time-delta-pct",
        type=nonnegative_float_arg,
        default=2.0,
        help="with --repeat > 1, fail if fastest/slowest --run-all wall time differs by more than this percent",
    )
    parser.add_argument("--jobs", type=int, default=default_jobs(), help="parallel build/test jobs for --run-all host coverage")
    parser.add_argument("--skip-host", action="store_true", help="with --run-all, skip host gtest coverage")
    parser.add_argument("--skip-ktest", action="store_true", help="with --run-all, skip KTEST/KCOV coverage")
    parser.add_argument("--skip-userland", action="store_true", help="with --run-all, skip WOS userland suite coverage")
    parser.add_argument("--host-build-dir", default="/tmp/wos-tests-coverage", help="host coverage CMake build directory")
    parser.add_argument(
        "--host-timeout",
        type=positive_int_arg,
        default=DEFAULT_HOST_TIMEOUT_SECONDS,
        help=f"timeout in seconds for each host coverage command; default {DEFAULT_HOST_TIMEOUT_SECONDS}",
    )
    parser.add_argument("--ktest-timeout", type=positive_int_arg, default=DEFAULT_KTEST_TIMEOUT_SECONDS, help="timeout in seconds for bin/wos-ktest")
    parser.add_argument("--ktest-cov-timeout", type=positive_int_arg, default=DEFAULT_KTEST_COV_TIMEOUT_SECONDS, help="timeout in seconds for bin/wos-ktest-cov")
    parser.add_argument("--ktest-fast", action="store_true", help="pass --fast to bin/wos-ktest")
    parser.add_argument("--ktest-no-build", action="store_true", help="pass --no-build to bin/wos-ktest")
    parser.add_argument("--ktest-no-package", action="store_true", help="pass --no-package to bin/wos-ktest")
    parser.add_argument("--ktest-reset-sysroot", action="store_true", help="pass --reset-sysroot to bin/wos-ktest")
    parser.add_argument("--ktest-tcg", nargs="?", const="", default=None, metavar="LEVEL", help="pass --tcg[=LEVEL] to bin/wos-ktest")
    parser.add_argument("--ktest-setup", action="store_true", help="run privileged KTEST topology setup/teardown instead of bin/wos-ktest --no-setup")
    parser.add_argument("--ktest-keep-topology", action="store_true", help="with --ktest-setup, do not run bin/wos-ktest --teardown after KCOV collection")
    parser.add_argument("--ktest-arg", action="append", default=[], help="extra argument for bin/wos-ktest; may be repeated")
    parser.add_argument("--ktest-cov-arg", action="append", default=[], help="extra argument for bin/wos-ktest-cov; may be repeated")
    parser.add_argument("--userland-host", default="vm0", help="host/IP/alias for bin/wos-userland-suite")
    parser.add_argument("--userland-scale", default="full", choices=("quick", "full", "stress"), help="WOS userland suite scale")
    parser.add_argument("--userland-existing-cluster", action="store_true", help="use an already running WOS cluster")
    parser.add_argument("--userland-no-build", action="store_true", help="with --run-all, skip Build WOS before launching userland cluster")
    parser.add_argument("--userland-cluster-config", default="configs/cluster.json", help="cluster config for the userland suite run")
    parser.add_argument("--userland-tcg", nargs="?", const="", default=None, metavar="LEVEL", help="launch userland cluster with --tcg[=LEVEL]")
    parser.add_argument("--cluster-ready-timeout", type=positive_int_arg, default=DEFAULT_CLUSTER_READY_TIMEOUT_SECONDS, help="seconds to wait for wos-cluster launch")
    parser.add_argument("--userland-ssh-timeout", type=positive_int_arg, default=DEFAULT_USERLAND_SSH_TIMEOUT_SECONDS, help="seconds to wait for SSH readiness")
    parser.add_argument(
        "--userland-build-timeout",
        type=positive_int_arg,
        default=DEFAULT_USERLAND_BUILD_TIMEOUT_SECONDS,
        help=f"timeout in seconds for Build WOS before userland run; default {DEFAULT_USERLAND_BUILD_TIMEOUT_SECONDS}",
    )
    parser.add_argument(
        "--userland-no-lcov",
        action="store_true",
        help="with --run-all, skip LLVM LCOV export for WOS userspace binaries",
    )
    parser.add_argument(
        "--userland-coverage-build-dir",
        default=DEFAULT_USERLAND_COVERAGE_BUILD_DIR,
        help=f"WOS CMake build directory used for userspace LCOV; default {DEFAULT_USERLAND_COVERAGE_BUILD_DIR}",
    )
    parser.add_argument(
        "--userland-profile-fetch-timeout",
        type=positive_int_arg,
        default=DEFAULT_USERLAND_PROFILE_FETCH_TIMEOUT_SECONDS,
        help=f"seconds to fetch/merge WOS userspace profile data; default {DEFAULT_USERLAND_PROFILE_FETCH_TIMEOUT_SECONDS}",
    )
    parser.add_argument(
        "--userland-profile-file-fetch-timeout",
        type=positive_int_arg,
        default=DEFAULT_USERLAND_PROFILE_FILE_FETCH_TIMEOUT_SECONDS,
        help=(
            "seconds to wait for each individual WOS userspace profile HTTP transfer; "
            f"default {DEFAULT_USERLAND_PROFILE_FILE_FETCH_TIMEOUT_SECONDS}"
        ),
    )
    parser.add_argument(
        "--userland-sync-rootfs",
        action="store_true",
        help="with --run-all, opt into live wos-cluster --sync before the userland suite",
    )
    parser.add_argument(
        "--userland-no-sync",
        action="store_true",
        help="compatibility no-op; live rootfs sync is disabled unless --userland-sync-rootfs is set",
    )
    parser.add_argument("--userland-sync-timeout", type=int, default=DEFAULT_USERLAND_SYNC_TIMEOUT_SECONDS, help="seconds to wait for live rootfs sync before the userland suite; 0 disables the timeout")
    parser.add_argument("--userland-sync-filter", help="optional wos-cluster --sync filter for the pre-suite rootfs sync")
    parser.add_argument("--userland-timeout", type=positive_int_arg, default=DEFAULT_USERLAND_TIMEOUT_SECONDS, help="timeout in seconds for bin/wos-userland-suite")
    parser.add_argument("--userland-shutdown-timeout", type=positive_int_arg, default=DEFAULT_USERLAND_SHUTDOWN_TIMEOUT_SECONDS, help="seconds to wait for an auto-launched WOS cluster to exit after guest shutdown")
    parser.add_argument("--userland-env", action="append", default=[], metavar="KEY=VALUE", help="environment assignment for remote userland suite")
    parser.add_argument("--userland-arg", action="append", default=[], help="extra argument for bin/wos-userland-suite; may be repeated")
    parser.add_argument("--kcov-lcov", action="append", default=[], metavar="FILE", help="KCOV LCOV .info file")
    parser.add_argument("--host-lcov", action="append", default=[], metavar="FILE", help="host gtest LCOV .info file")
    parser.add_argument("--lcov", action="append", default=[], metavar="NAME=FILE", help="additional named LCOV input")
    parser.add_argument("--runtime-log", action="append", default=[], metavar="FILE", help="log containing TESTD and/or userland suite output")
    parser.add_argument("--testd-log", action="append", default=[], metavar="FILE", help="log containing TESTD output")
    parser.add_argument("--userland-log", action="append", default=[], metavar="FILE", help="log containing WOS userland suite output")
    parser.add_argument(
        "--allow-userland-skips",
        action="store_true",
        help="Count userland SKIP rows as uncovered cases instead of rejecting the log",
    )
    parser.add_argument("--host-score", metavar="HIT/TOTAL", help="host gtest score when no LCOV file is available")
    parser.add_argument("--testd-score", metavar="HIT/TOTAL", help="explicit TESTD score when no log is available")
    parser.add_argument("--userland-score", metavar="HIT/TOTAL", help="explicit userland suite score when no log is available")
    parser.add_argument("--score", action="append", default=[], metavar="NAME=HIT/TOTAL", help="additional explicit score")
    parser.add_argument("--fail-under", metavar="PCT", type=coverage_threshold_arg, help="exit non-zero if combined coverage is below PCT")
    parser.add_argument("--verify-run-all-results", metavar="DIR", help="verify a completed --run-all artifact directory")
    parser.add_argument("--verify-repeat", type=positive_int_arg, metavar="N", help="with --verify-run-all-results, require exactly N iterations")
    parser.add_argument("--json", action="store_true", help="write the coverage report as JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    set_progress_stream(sys.stderr if args.json else sys.stdout)
    if args.jobs <= 0:
        parser.error("--jobs must be positive")
    if args.repeat <= 0:
        parser.error("--repeat must be positive")
    if args.repeat > 1 and not args.run_all:
        parser.error("--repeat requires --run-all")
    if args.resume_run_all and not args.run_all:
        parser.error("--resume-run-all requires --run-all")
    if args.resume_run_all and args.repeat <= 1:
        parser.error("--resume-run-all requires --repeat greater than 1")
    if args.userland_sync_timeout < 0:
        parser.error("--userland-sync-timeout must be nonnegative")
    if args.run_all and not args.skip_userland and not args.userland_no_lcov:
        requested_shutdown = env_assignment_value(args.userland_env, "WOS_SUITE_SHUTDOWN")
        if suite_shutdown_exits_vm(requested_shutdown):
            parser.error("--userland LCOV requires host-controlled shutdown; remove WOS_SUITE_SHUTDOWN or pass --userland-no-lcov")
    if args.verify_repeat is not None and not args.verify_run_all_results:
        parser.error("--verify-repeat requires --verify-run-all-results")
    if args.verify_run_all_results and args.verify_repeat is None:
        parser.error("--verify-run-all-results requires --verify-repeat")

    try:
        if args.verify_run_all_results:
            verified = verify_run_all_results(args)
            if args.json:
                print(
                    json.dumps(
                        {
                            "results_dir": str(verified.results_dir),
                            "repeat": verified.repeat,
                            "runtime_delta_percent": verified.runtime_delta_percent,
                            "elapsed_seconds": verified.elapsed_seconds,
                        },
                        indent=2,
                        sort_keys=True,
                    )
                )
            else:
                print(
                    "Verified run-all results: "
                    f"{verified.repeat} iteration(s), "
                    f"runtime delta {verified.runtime_delta_percent:.2f}%"
                )
            return 0

        auto_artifacts = None
        run_iterations: list[AutoRunIteration] = []
        if args.run_all:
            auto_artifacts, run_iterations = run_all_coverage(args)
        used_lcov_names: set[str] = set()
        lcov_inputs: list[LcovInput] = []
        if auto_artifacts is not None and auto_artifacts.kcov_lcov is not None:
            add_lcov_input(lcov_inputs, used_lcov_names, "kcov", auto_artifacts.kcov_lcov)
        if auto_artifacts is not None and auto_artifacts.host_lcov is not None:
            add_lcov_input(lcov_inputs, used_lcov_names, "host-gtest", auto_artifacts.host_lcov)
        if auto_artifacts is not None and auto_artifacts.userland_lcov is not None:
            add_lcov_input(lcov_inputs, used_lcov_names, "wos-userland", auto_artifacts.userland_lcov)
        for path in args.kcov_lcov:
            add_lcov_input(lcov_inputs, used_lcov_names, "kcov", Path(path))
        for path in args.host_lcov:
            add_lcov_input(lcov_inputs, used_lcov_names, "host-gtest", Path(path))
        for spec in args.lcov:
            name, path = parse_named_path(spec)
            add_lcov_input(lcov_inputs, used_lcov_names, name, path)

        lcov_input_scores = [
            score_lcov_lines(input_file.name, input_file.lines, str(input_file.path))
            for input_file in lcov_inputs
        ]
        merged_lcov_score = None
        combined_parts: list[CoverageScore] = []
        if lcov_inputs:
            merged_lcov_score = score_lcov_lines("lcov-merged", merge_lcov_lines(lcov_inputs))
            combined_parts.append(merged_lcov_score)

        used_runtime_names: set[str] = set()
        runtime_scores: list[CoverageScore] = []
        if auto_artifacts is not None and auto_artifacts.userland_log is not None:
            for score in scores_from_runtime_log(auto_artifacts.userland_log, args.allow_userland_skips):
                runtime_scores.append(dataclasses.replace(score, name=uniquify_name(score.name, used_runtime_names)))
        for path_text in args.runtime_log:
            for score in scores_from_runtime_log(Path(path_text), args.allow_userland_skips):
                runtime_scores.append(dataclasses.replace(score, name=uniquify_name(score.name, used_runtime_names)))
        for path_text in args.testd_log:
            text = read_runtime_log(Path(path_text))
            score = testd_score_from_text("testd", text, path_text)
            runtime_scores.append(dataclasses.replace(score, name=uniquify_name(score.name, used_runtime_names)))
        for path_text in args.userland_log:
            text = read_runtime_log(Path(path_text))
            score = userland_score_from_text("userland", text, path_text, args.allow_userland_skips)
            runtime_scores.append(dataclasses.replace(score, name=uniquify_name(score.name, used_runtime_names)))
        combined_parts.extend(runtime_scores)

        explicit_scores: list[CoverageScore] = []
        if args.host_score:
            hit, total = parse_ratio(args.host_score)
            explicit_scores.append(CoverageScore("host-gtest", hit, total, "units", "explicit score"))
        if args.testd_score:
            hit, total = parse_ratio(args.testd_score)
            explicit_scores.append(CoverageScore("testd", hit, total, "checks", "explicit score"))
        if args.userland_score:
            hit, total = parse_ratio(args.userland_score)
            explicit_scores.append(CoverageScore("userland", hit, total, "cases", "explicit score"))
        explicit_scores.extend(parse_named_score(score) for score in args.score)
        combined_parts.extend(explicit_scores)

        combined = combined_score(combined_parts)
    except subprocess.TimeoutExpired as exc:
        cmd = [str(part) for part in exc.cmd] if isinstance(exc.cmd, list | tuple) else [str(exc.cmd)]
        print(f"ERROR: command timed out after {exc.timeout}s: {display_command(cmd)}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        cmd = [str(part) for part in exc.cmd] if isinstance(exc.cmd, list | tuple) else [str(exc.cmd)]
        print(f"ERROR: command failed with status {exc.returncode}: {display_command(cmd)}", file=sys.stderr)
        return 1
    except (CoverageInputError, runtime_test_audit.AuditError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(
            json.dumps(
                {
                    "combined": score_to_json(combined),
                    "run_artifacts": artifacts_to_json(auto_artifacts),
                    "run_iterations": run_iterations_to_json(run_iterations),
                    "lcov_inputs": [score_to_json(score) for score in lcov_input_scores],
                    "lcov_merged": score_to_json(merged_lcov_score) if merged_lcov_score is not None else None,
                    "runtime_scores": [score_to_json(score) for score in runtime_scores],
                    "explicit_scores": [score_to_json(score) for score in explicit_scores],
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print_text_report(
            auto_artifacts,
            run_iterations,
            lcov_inputs,
            lcov_input_scores,
            merged_lcov_score,
            runtime_scores,
            explicit_scores,
            combined,
        )

    if args.fail_under is not None and combined.percent + 1e-9 < args.fail_under:
        print(
            f"ERROR: combined coverage {combined.percent:.1f}% is below required {args.fail_under:.1f}%",
            file=sys.stderr,
        )
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
