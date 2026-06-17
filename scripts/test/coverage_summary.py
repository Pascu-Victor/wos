#!/usr/bin/env python3
"""Combine WOS line coverage and runtime manifest scores."""

import argparse
import dataclasses
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import runtime_test_audit  # noqa: E402


class CoverageInputError(RuntimeError):
    pass


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


def normalize_source_path(path: str, root: Path = ROOT) -> str:
    source = Path(path)
    if not source.is_absolute():
        source = root / source
    return str(source.resolve(strict=False))


def display_command(cmd: list[str]) -> str:
    return " ".join(shlex_quote(part) for part in cmd)


def shlex_quote(text: str) -> str:
    return "'" + text.replace("'", "'\"'\"'") + "'" if re.search(r"\s|['\"\\$`!#&;|<>*?(){}[\]]", text) else text


def run_checked(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int | None = None,
    cwd: Path = ROOT,
) -> None:
    print(f"$ {display_command(cmd)}")
    subprocess.run(cmd, cwd=cwd, env=env, check=True, timeout=timeout)


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
    print("\n== Host gtest coverage ==")
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
    run_checked(configure_cmd, env=env, timeout=timeout)
    run_checked(["cmake", "--build", str(build_dir), "--target", *targets, "-j", str(jobs)], timeout=timeout)

    ctest_env = env.copy()
    ctest_env["LLVM_PROFILE_FILE"] = str(profile_dir / "%p-%m.profraw")
    run_checked(
        ["ctest", "--test-dir", str(build_dir), "-L", "unit", "--output-on-failure", "-j", str(jobs)],
        env=ctest_env,
        timeout=timeout,
    )

    profraws = sorted(profile_dir.glob("*.profraw"))
    if not profraws:
        raise CoverageInputError("host gtest run produced no .profraw files")
    profdata = results_dir / "host-gtest.profdata"
    run_checked([str(llvm_profdata), "merge", "-sparse", "-o", str(profdata), *map(str, profraws)], timeout=timeout)

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
    print(f"$ {display_command(export_cmd)} > {lcov_path}")
    with lcov_path.open("w") as output:
        subprocess.run(export_cmd, cwd=ROOT, stdout=output, check=True, timeout=timeout)
    parse_lcov(lcov_path)
    return lcov_path


def run_ktest_kcov(args: argparse.Namespace, results_dir: Path) -> Path:
    print("\n== KTEST KCOV coverage ==")
    cmd = [str(ROOT / "bin" / "wos-ktest")]
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
        run_checked(cmd, timeout=args.ktest_timeout)
    finally:
        if not args.ktest_keep_topology:
            try:
                run_checked([str(ROOT / "bin" / "wos-ktest"), "--teardown"], timeout=300)
            except Exception as exc:  # noqa: BLE001 - teardown diagnostics should not mask the KTEST result
                print(f"WARNING: failed to tear down KTEST topology: {exc}", file=sys.stderr)

    lcov_path = results_dir / "kcov.info"
    cov_cmd = [str(ROOT / "bin" / "wos-ktest-cov"), "--lcov", str(lcov_path), *args.ktest_cov_arg]
    run_checked(cov_cmd, timeout=args.ktest_cov_timeout)
    parse_lcov(lcov_path)
    return lcov_path


def pump_process_output(proc: subprocess.Popen[str], log_path: Path, ready_event: threading.Event) -> None:
    with log_path.open("w") as log:
        assert proc.stdout is not None
        for line in proc.stdout:
            print(line, end="")
            log.write(line)
            log.flush()
            if "Press Ctrl+C to stop all VMs" in line:
                ready_event.set()


def wait_for_cluster_ready(proc: subprocess.Popen[str], ready_event: threading.Event, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ready_event.is_set():
            return
        rc = proc.poll()
        if rc is not None:
            raise CoverageInputError(f"wos-cluster exited before launch completed (status {rc})")
        time.sleep(0.25)
    raise CoverageInputError(f"timed out waiting {timeout}s for wos-cluster launch")


def wait_for_wos_ssh(host: str, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    ssh_cmd = [str(ROOT / "scripts" / "remote" / "wos_ssh.sh"), host, "true"]
    while time.monotonic() < deadline:
        result = subprocess.run(ssh_cmd, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode == 0:
            return
        time.sleep(2.0)
    raise CoverageInputError(f"timed out waiting {timeout}s for SSH to {host}")


def stop_cluster(proc: subprocess.Popen[str] | None, timeout: int = 20) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout)


def run_userland_suite_coverage(args: argparse.Namespace, results_dir: Path) -> tuple[Path, Path | None]:
    print("\n== WOS userland suite coverage ==")
    cluster_proc: subprocess.Popen[str] | None = None
    cluster_log: Path | None = None
    pump_thread: threading.Thread | None = None
    try:
        if not args.userland_existing_cluster:
            if not args.userland_no_build:
                run_checked([str(ROOT / "scripts" / "dev" / "build_wos.sh")], timeout=args.userland_build_timeout)

            cluster_log = results_dir / "cluster-launch.log"
            launch_cmd = [str(ROOT / "bin" / "wos-cluster"), "--launch", "--config", args.userland_cluster_config]
            if args.userland_tcg is not None:
                launch_cmd.append("--tcg" if args.userland_tcg == "" else f"--tcg={args.userland_tcg}")
            print(f"$ {display_command(launch_cmd)}")
            ready_event = threading.Event()
            cluster_proc = subprocess.Popen(
                launch_cmd,
                cwd=ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            pump_thread = threading.Thread(
                target=pump_process_output,
                args=(cluster_proc, cluster_log, ready_event),
                daemon=True,
            )
            pump_thread.start()
            wait_for_cluster_ready(cluster_proc, ready_event, args.cluster_ready_timeout)
            wait_for_wos_ssh(args.userland_host, args.userland_ssh_timeout)

        userland_log = results_dir / "userland-suite.log"
        cmd = [
            str(ROOT / "bin" / "wos-userland-suite"),
            args.userland_host,
            "--scale",
            args.userland_scale,
            "--output",
            str(userland_log),
        ]
        for value in args.userland_env:
            cmd.extend(["--env", value])
        cmd.extend(args.userland_arg)
        run_checked(cmd, timeout=args.userland_timeout)
        scores_from_runtime_log(userland_log, args.allow_userland_skips)
        return userland_log, cluster_log
    finally:
        if not args.userland_existing_cluster:
            stop_cluster(cluster_proc)
            if pump_thread is not None:
                pump_thread.join(timeout=2)
            try:
                run_checked([str(ROOT / "bin" / "wos-cluster"), "--teardown", "--config", args.userland_cluster_config], timeout=300)
            except Exception as exc:  # noqa: BLE001 - teardown diagnostics should not hide the original run result
                print(f"WARNING: failed to tear down WOS cluster: {exc}", file=sys.stderr)


def run_all_coverage(args: argparse.Namespace) -> AutoRunArtifacts:
    results_dir = Path(args.results_dir)
    if not results_dir.is_absolute():
        results_dir = ROOT / results_dir
    results_dir.mkdir(parents=True, exist_ok=True)

    host_lcov = None
    kcov_lcov = None
    userland_log = None
    cluster_log = None
    if not args.skip_host:
        host_lcov = run_host_gtest_coverage(results_dir, Path(args.host_build_dir), args.jobs, args.host_timeout)
    if not args.skip_ktest:
        kcov_lcov = run_ktest_kcov(args, results_dir)
    if not args.skip_userland:
        userland_log, cluster_log = run_userland_suite_coverage(args, results_dir)
    return AutoRunArtifacts(host_lcov=host_lcov, kcov_lcov=kcov_lcov, userland_log=userland_log, cluster_log=cluster_log)


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
    }


def print_auto_artifacts(artifacts: AutoRunArtifacts | None) -> None:
    if artifacts is None:
        return
    print("Auto-run artifacts:")
    for label, path in (
        ("host LCOV", artifacts.host_lcov),
        ("KCOV LCOV", artifacts.kcov_lcov),
        ("userland log", artifacts.userland_log),
        ("cluster log", artifacts.cluster_log),
    ):
        if path is not None:
            print(f"  {label}: {path}")
    print()


def print_text_report(
    artifacts: AutoRunArtifacts | None,
    lcov_inputs: list[LcovInput],
    lcov_input_scores: list[CoverageScore],
    merged_lcov_score: CoverageScore | None,
    runtime_scores: list[CoverageScore],
    explicit_scores: list[CoverageScore],
    combined: CoverageScore,
) -> None:
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
    if value < 0.0 or value > 100.0:
        raise argparse.ArgumentTypeError("coverage threshold must be between 0 and 100")
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
    parser.add_argument("--jobs", type=int, default=default_jobs(), help="parallel build/test jobs for --run-all host coverage")
    parser.add_argument("--skip-host", action="store_true", help="with --run-all, skip host gtest coverage")
    parser.add_argument("--skip-ktest", action="store_true", help="with --run-all, skip KTEST/KCOV coverage")
    parser.add_argument("--skip-userland", action="store_true", help="with --run-all, skip WOS userland suite coverage")
    parser.add_argument("--host-build-dir", default="/tmp/wos-tests-coverage", help="host coverage CMake build directory")
    parser.add_argument("--host-timeout", type=int, default=None, help="timeout in seconds for each host coverage command")
    parser.add_argument("--ktest-timeout", type=int, default=3600, help="timeout in seconds for bin/wos-ktest")
    parser.add_argument("--ktest-cov-timeout", type=int, default=900, help="timeout in seconds for bin/wos-ktest-cov")
    parser.add_argument("--ktest-fast", action="store_true", help="pass --fast to bin/wos-ktest")
    parser.add_argument("--ktest-no-build", action="store_true", help="pass --no-build to bin/wos-ktest")
    parser.add_argument("--ktest-no-package", action="store_true", help="pass --no-package to bin/wos-ktest")
    parser.add_argument("--ktest-reset-sysroot", action="store_true", help="pass --reset-sysroot to bin/wos-ktest")
    parser.add_argument("--ktest-tcg", nargs="?", const="", default=None, metavar="LEVEL", help="pass --tcg[=LEVEL] to bin/wos-ktest")
    parser.add_argument("--ktest-keep-topology", action="store_true", help="do not run bin/wos-ktest --teardown after KCOV collection")
    parser.add_argument("--ktest-arg", action="append", default=[], help="extra argument for bin/wos-ktest; may be repeated")
    parser.add_argument("--ktest-cov-arg", action="append", default=[], help="extra argument for bin/wos-ktest-cov; may be repeated")
    parser.add_argument("--userland-host", default="vm0", help="host/IP/alias for bin/wos-userland-suite")
    parser.add_argument("--userland-scale", default="full", choices=("quick", "full", "stress"), help="WOS userland suite scale")
    parser.add_argument("--userland-existing-cluster", action="store_true", help="use an already running WOS cluster")
    parser.add_argument("--userland-no-build", action="store_true", help="with --run-all, skip Build WOS before launching userland cluster")
    parser.add_argument("--userland-cluster-config", default="configs/cluster.json", help="cluster config for the userland suite run")
    parser.add_argument("--userland-tcg", nargs="?", const="", default=None, metavar="LEVEL", help="launch userland cluster with --tcg[=LEVEL]")
    parser.add_argument("--cluster-ready-timeout", type=int, default=300, help="seconds to wait for wos-cluster launch")
    parser.add_argument("--userland-ssh-timeout", type=int, default=300, help="seconds to wait for SSH readiness")
    parser.add_argument("--userland-build-timeout", type=int, default=None, help="timeout in seconds for Build WOS before userland run")
    parser.add_argument("--userland-timeout", type=int, default=7200, help="timeout in seconds for bin/wos-userland-suite")
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
    parser.add_argument("--json", action="store_true", help="write the coverage report as JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.jobs <= 0:
        parser.error("--jobs must be positive")

    try:
        auto_artifacts = run_all_coverage(args) if args.run_all else None
        used_lcov_names: set[str] = set()
        lcov_inputs: list[LcovInput] = []
        if auto_artifacts is not None and auto_artifacts.kcov_lcov is not None:
            add_lcov_input(lcov_inputs, used_lcov_names, "kcov", auto_artifacts.kcov_lcov)
        if auto_artifacts is not None and auto_artifacts.host_lcov is not None:
            add_lcov_input(lcov_inputs, used_lcov_names, "host-gtest", auto_artifacts.host_lcov)
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
        print_text_report(auto_artifacts, lcov_inputs, lcov_input_scores, merged_lcov_score, runtime_scores, explicit_scores, combined)

    if args.fail_under is not None and combined.percent + 1e-9 < args.fail_under:
        print(
            f"ERROR: combined coverage {combined.percent:.1f}% is below required {args.fail_under:.1f}%",
            file=sys.stderr,
        )
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
