#!/usr/bin/env python3

import contextlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
COVERAGE_SUMMARY = ROOT / "scripts" / "test" / "coverage_summary.py"
COVERAGE_BIN = ROOT / "bin" / "wos-coverage-summary"


def load_module():
    spec = importlib.util.spec_from_file_location("coverage_summary", COVERAGE_SUMMARY)
    if spec is None or spec.loader is None:
        raise AssertionError(f"failed to load {COVERAGE_SUMMARY}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def assert_equal(actual, expected, msg):
    if actual != expected:
        raise AssertionError(f"{msg}: expected {expected!r}, got {actual!r}")


def command_env_assignments(cmd: list[str]) -> dict[str, str]:
    env: dict[str, str] = {}
    for index, word in enumerate(cmd[:-1]):
        if word != "--env":
            continue
        assignment = cmd[index + 1]
        key, separator, value = assignment.partition("=")
        if separator:
            env[key] = value
    return env


def test_bin_entrypoint_targets_coverage_summary(_module) -> None:
    if not COVERAGE_BIN.exists():
        raise AssertionError(f"missing coverage entrypoint: {COVERAGE_BIN}")
    if not COVERAGE_BIN.is_symlink():
        raise AssertionError(f"{COVERAGE_BIN} must be a symlink to avoid drifting from {COVERAGE_SUMMARY}")
    assert_equal(COVERAGE_BIN.resolve(), COVERAGE_SUMMARY.resolve(), "coverage entrypoint target")


def write_lcov(path: Path, records: dict[str, dict[int, int]]) -> None:
    lines: list[str] = []
    for source, coverage in records.items():
        lines.append(f"SF:{source}")
        for line, count in sorted(coverage.items()):
            lines.append(f"DA:{line},{count}")
        lines.append(f"LF:{len(coverage)}")
        lines.append(f"LH:{sum(1 for count in coverage.values() if count > 0)}")
        lines.append("end_of_record")
    path.write_text("\n".join(lines) + "\n")


def complete_testd_log(module) -> str:
    labels = module.runtime_test_audit.testd_expected_pass_labels()
    lines = ["[TESTD] starting"]
    for index, label in enumerate(labels, start=1):
        lines.append(f"[TESTD] {index}/{len(labels)} PASS: {label}")
    lines.append(f"[TESTD] DONE: {len(labels)} passed, 0 failed")
    return "\n".join(lines) + "\n"


def complete_userland_log(module, skip_first: bool = False) -> str:
    cases = sorted(module.runtime_test_audit.userland_expected_cases())
    lines = ["=== WOS USERLAND SUITE SUMMARY ==="]
    skipped = cases[0] if skip_first else None
    for case in cases:
        status = "SKIP" if case == skipped else "PASS"
        lines.append(f"{case}\t{status}\t1s")
    skipped_count = 1 if skip_first else 0
    lines.append(f"PASS={len(cases) - skipped_count} FAIL=0 SKIP={skipped_count}")
    lines.append("RESULT_DIR=/tmp/wos-userland-suite-test")
    return "\n".join(lines) + "\n"


def write_wos_userland_lcov(path: Path) -> None:
    write_lcov(path, {"/repo/modules/testprog/src/main.cpp": {1: 1, 2: 0}})


def write_fake_userland_artifacts(
    module,
    results_dir,
    *,
    log_name: str = "userland.log",
    cluster_name: str | None = None,
    lcov_name: str = "wos-userland.info",
    cluster_text: str | None = None,
) -> tuple[Path, Path | None, Path]:
    results_path = Path(results_dir)
    userland_log = results_path / log_name
    userland_lcov = results_path / lcov_name
    userland_log.write_text(complete_testd_log(module) + complete_userland_log(module))
    write_wos_userland_lcov(userland_lcov)
    cluster_log = None
    if cluster_name is not None:
        cluster_log = results_path / cluster_name
        cluster_log.write_text(
            cluster_text
            if cluster_text is not None
            else "$ /repo/bin/wos-cluster --launch --no-setup --config configs/cluster.json\ncluster ready\n"
        )
    return userland_log, cluster_log, userland_lcov


@contextlib.contextmanager
def fake_external_log_evidence(module, tmp_path: Path):
    proof_log = tmp_path / "external-proof.log"
    proof_log.write_text("external proof log healthy\n")
    old_expected = module.expected_external_health_log_paths
    old_observed = module.external_health_log_paths
    observed_calls = 0

    def fake_observed(_args):
        nonlocal observed_calls
        observed_calls += 1
        if observed_calls > 1:
            with proof_log.open("a") as output:
                output.write(f"external proof current run {observed_calls}\n")
        return [proof_log]

    module.expected_external_health_log_paths = lambda _args: [proof_log]
    module.external_health_log_paths = fake_observed
    try:
        yield proof_log
    finally:
        module.expected_external_health_log_paths = old_expected
        module.external_health_log_paths = old_observed


def test_lcov_inputs_merge_overlapping_lines(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        kcov = tmp_path / "kcov.info"
        host = tmp_path / "host.info"
        source_a = ROOT / "modules" / "kern" / "src" / "a.cpp"
        source_b = ROOT / "modules" / "kern" / "src" / "b.cpp"
        source_c = ROOT / "modules" / "kern" / "src" / "c.cpp"
        write_lcov(kcov, {str(source_a): {1: 1, 2: 0}, str(source_b): {7: 0}})
        write_lcov(host, {str(source_a): {2: 4}, str(source_c): {3: 1}})

        kcov_lines = module.parse_lcov(kcov)
        host_lines = module.parse_lcov(host)
        merged = module.merge_lcov_lines(
            [
                module.LcovInput("kcov", kcov, kcov_lines),
                module.LcovInput("host-gtest", host, host_lines),
            ]
        )

    assert_equal(module.score_lcov_lines("kcov", kcov_lines).hit, 1, "kcov hit lines")
    assert_equal(module.score_lcov_lines("kcov", kcov_lines).total, 3, "kcov total lines")
    assert_equal(module.score_lcov_lines("host-gtest", host_lines).hit, 2, "host hit lines")
    merged_score = module.score_lcov_lines("lcov-merged", merged)
    assert_equal(merged_score.hit, 3, "merged hit lines")
    assert_equal(merged_score.total, 4, "merged total lines")


def test_runtime_log_scores_testd_and_userland(module) -> None:
    log = complete_testd_log(module) + complete_userland_log(module)
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        path = Path(tmp_dir) / "runtime.log"
        path.write_text(log)
        scores = module.scores_from_runtime_log(path, allow_userland_skips=False)

    by_name = {score.name: score for score in scores}
    testd_total = len(module.runtime_test_audit.testd_expected_pass_labels())
    userland_total = len(module.runtime_test_audit.userland_expected_cases())
    assert_equal(by_name["testd"].hit, testd_total, "TESTD hit count")
    assert_equal(by_name["testd"].total, testd_total, "TESTD total count")
    assert_equal(by_name["userland"].hit, userland_total, "userland hit count")
    assert_equal(by_name["userland"].total, userland_total, "userland total count")


def test_userland_skips_can_count_as_uncovered(module) -> None:
    log = complete_userland_log(module, skip_first=True)
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        path = Path(tmp_dir) / "runtime.log"
        path.write_text(log)
        scores = module.scores_from_runtime_log(path, allow_userland_skips=True)

    total = len(module.runtime_test_audit.userland_expected_cases())
    assert_equal(len(scores), 1, "only userland score selected")
    assert_equal(scores[0].hit, total - 1, "skipped userland case hit count")
    assert_equal(scores[0].total, total, "skipped userland case denominator")


def test_cli_json_combines_merged_lcov_and_scores(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        kcov = tmp_path / "kcov.info"
        host = tmp_path / "host.info"
        write_lcov(kcov, {"/repo/a.cpp": {1: 1, 2: 0}})
        write_lcov(host, {"/repo/a.cpp": {2: 1}, "/repo/b.cpp": {1: 1}})
        result = subprocess.run(
            [
                sys.executable,
                str(COVERAGE_SUMMARY),
                "--kcov-lcov",
                str(kcov),
                "--host-lcov",
                str(host),
                "--testd-score",
                "3/4",
                "--userland-score",
                "5/5",
                "--json",
            ],
            capture_output=True,
            text=True,
            check=False,
        )

    if result.returncode != 0:
        raise AssertionError(f"coverage_summary.py failed:\nstdout={result.stdout}\nstderr={result.stderr}")
    data = json.loads(result.stdout)
    assert_equal(data["lcov_merged"]["hit"], 3, "merged LCOV JSON hit count")
    assert_equal(data["lcov_merged"]["total"], 3, "merged LCOV JSON total count")
    assert_equal(data["combined"]["hit"], 11, "combined JSON hit count")
    assert_equal(data["combined"]["total"], 12, "combined JSON total count")


def test_cli_fail_under_reports_combined_threshold(module) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(COVERAGE_SUMMARY),
            "--score",
            "synthetic=1/4",
            "--fail-under",
            "50",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert_equal(result.returncode, 3, "threshold failure exit code")
    if "below required 50.0%" not in result.stderr:
        raise AssertionError(f"missing threshold failure in stderr: {result.stderr!r}")


def test_host_unit_test_targets_are_discovered(module) -> None:
    targets = module.host_unit_test_targets()
    for expected in ("wki_wire_test", "wki_routing_test", "data_struct_test"):
        if expected not in targets:
            raise AssertionError(f"missing host gtest target {expected!r} in {targets!r}")


def test_run_all_host_timeout_is_bounded_by_default(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        seen_timeouts: list[int | None] = []
        old_host = module.run_host_gtest_coverage

        def fake_host(results_dir, _build, _jobs, timeout):
            seen_timeouts.append(timeout)
            output = Path(results_dir) / "host.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        module.run_host_gtest_coverage = fake_host
        try:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = module.main(
                    [
                        "--run-all",
                        "--skip-ktest",
                        "--skip-userland",
                        "--results-dir",
                        str(tmp_path),
                        "--json",
                    ]
                )
        finally:
            module.run_host_gtest_coverage = old_host

    assert_equal(rc, 0, "host-timeout default run-all return code")
    assert_equal(seen_timeouts, [module.DEFAULT_HOST_TIMEOUT_SECONDS], "default host timeout propagation")


def test_run_all_userland_defaults_are_bounded(module) -> None:
    args = module.build_parser().parse_args(["--run-all"])
    assert_equal(
        args.userland_build_timeout,
        module.DEFAULT_USERLAND_BUILD_TIMEOUT_SECONDS,
        "default userland build timeout",
    )
    assert_equal(args.ktest_timeout, module.DEFAULT_KTEST_TIMEOUT_SECONDS, "default ktest timeout")
    assert_equal(args.ktest_cov_timeout, module.DEFAULT_KTEST_COV_TIMEOUT_SECONDS, "default ktest cov timeout")
    assert_equal(args.cluster_ready_timeout, module.DEFAULT_CLUSTER_READY_TIMEOUT_SECONDS, "default cluster ready timeout")
    assert_equal(args.userland_ssh_timeout, module.DEFAULT_USERLAND_SSH_TIMEOUT_SECONDS, "default userland ssh timeout")
    assert_equal(args.userland_sync_rootfs, False, "default userland live rootfs sync")
    assert_equal(args.userland_sync_timeout, module.DEFAULT_USERLAND_SYNC_TIMEOUT_SECONDS, "default userland sync timeout")
    assert_equal(args.userland_timeout, module.DEFAULT_USERLAND_TIMEOUT_SECONDS, "default userland suite timeout")
    assert_equal(
        args.userland_profile_file_fetch_timeout,
        module.DEFAULT_USERLAND_PROFILE_FILE_FETCH_TIMEOUT_SECONDS,
        "default per-profile fetch timeout",
    )
    assert_equal(
        args.userland_shutdown_timeout,
        module.DEFAULT_USERLAND_SHUTDOWN_TIMEOUT_SECONDS,
        "default userland shutdown timeout",
    )
    cmd: list[str] = []
    module.append_userland_env_if_absent(
        cmd,
        [],
        "WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS",
        module.DEFAULT_USERLAND_NETBENCH_CASE_TIMEOUT_SECONDS,
    )
    assert_equal(
        cmd,
        ["--env", f"WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS={module.DEFAULT_USERLAND_NETBENCH_CASE_TIMEOUT_SECONDS}"],
        "default netbench timeout env",
    )
    override_cmd: list[str] = []
    module.append_userland_env_if_absent(
        override_cmd,
        ["WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS=7"],
        "WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS",
        module.DEFAULT_USERLAND_NETBENCH_CASE_TIMEOUT_SECONDS,
    )
    assert_equal(override_cmd, [], "explicit netbench timeout env should not be overwritten")
    coverage_cmd: list[str] = []
    module.append_userland_coverage_suite_env_defaults(
        coverage_cmd,
        ["WOS_SUITE_RENDER_SPP=99"],
    )
    assert_equal(
        command_env_assignments(coverage_cmd),
        {
            "WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS": str(module.DEFAULT_USERLAND_NETBENCH_CASE_TIMEOUT_SECONDS),
            "WOS_SUITE_RENDER_WIDTH": str(module.DEFAULT_USERLAND_COVERAGE_RENDER_WIDTH),
            "WOS_SUITE_RENDER_HEIGHT": str(module.DEFAULT_USERLAND_COVERAGE_RENDER_HEIGHT),
            "WOS_SUITE_RENDER_MAX_DEPTH": str(module.DEFAULT_USERLAND_COVERAGE_RENDER_MAX_DEPTH),
        },
        "coverage suite workload defaults should preserve explicit render overrides",
    )


def test_run_all_positive_timeout_args_reject_zero(module) -> None:
    timeout_args = [
        "--repeat",
        "--ktest-timeout",
        "--ktest-cov-timeout",
        "--cluster-ready-timeout",
        "--userland-ssh-timeout",
        "--userland-build-timeout",
        "--userland-timeout",
        "--userland-profile-file-fetch-timeout",
        "--userland-shutdown-timeout",
    ]
    for flag in timeout_args:
        result = subprocess.run(
            [sys.executable, str(COVERAGE_SUMMARY), "--score", "synthetic=1/1", flag, "0"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 2 or "value must be positive" not in result.stderr:
            raise AssertionError(f"{flag} accepted zero timeout: rc={result.returncode} stderr={result.stderr!r}")


def test_cli_float_args_reject_nonfinite_values(module) -> None:
    cases = [
        ("--max-run-time-delta-pct", "nan", "value must be finite"),
        ("--max-run-time-delta-pct", "inf", "value must be finite"),
        ("--fail-under", "nan", "coverage threshold must be finite"),
        ("--fail-under", "inf", "coverage threshold must be finite"),
    ]
    for flag, value, expected in cases:
        result = subprocess.run(
            [sys.executable, str(COVERAGE_SUMMARY), "--score", "synthetic=1/1", flag, value],
            capture_output=True,
            text=True,
        )
        if result.returncode != 2 or expected not in result.stderr:
            raise AssertionError(
                f"{flag} accepted non-finite value {value!r}: rc={result.returncode} stderr={result.stderr!r}"
            )


def test_verify_run_all_results_requires_explicit_repeat(module) -> None:
    result = subprocess.run(
        [sys.executable, str(COVERAGE_SUMMARY), "--verify-run-all-results", "."],
        capture_output=True,
        text=True,
    )
    if result.returncode != 2 or "--verify-run-all-results requires --verify-repeat" not in result.stderr:
        raise AssertionError(f"verify-run-all-results accepted implicit repeat: rc={result.returncode} stderr={result.stderr!r}")


def test_run_all_rejects_existing_results_without_override(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        (tmp_path / "run-all-iteration.json").write_text("{}\n")
        called = False

        old_host = module.run_host_gtest_coverage

        def fake_host(_results, _build, _jobs, _timeout):
            nonlocal called
            called = True
            raise AssertionError("run-all started despite existing results")

        module.run_host_gtest_coverage = fake_host
        try:
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                rc = module.main(["--run-all", "--results-dir", str(tmp_path)])
        finally:
            module.run_host_gtest_coverage = old_host

    assert_equal(rc, 1, "existing results default return code")
    assert_equal(called, False, "existing results should fail before host coverage starts")
    message = stderr.getvalue()
    for expected in ("existing run-all state found", "run-all-iteration.json", "--allow-existing-results"):
        if expected not in message:
            raise AssertionError(f"missing existing-results diagnostic {expected!r} in {message!r}")


def test_run_all_allows_existing_results_with_override(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        (tmp_path / "run-all-iteration.json").write_text("{}\n")

        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage

        def fake_host(results_dir, _build, _jobs, _timeout):
            output = Path(results_dir) / "host.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        def fake_ktest(_args, results_dir):
            output = Path(results_dir) / "kcov.info"
            write_lcov(output, {"/repo/kern.cpp": {1: 1}})
            return output

        def fake_userland(_args, results_dir):
            return write_fake_userland_artifacts(module, results_dir)

        module.run_host_gtest_coverage = fake_host
        module.run_ktest_kcov = fake_ktest
        module.run_userland_suite_coverage = fake_userland
        try:
            stdout = io.StringIO()
            with fake_external_log_evidence(module, tmp_path):
                with contextlib.redirect_stdout(stdout):
                    rc = module.main(["--run-all", "--allow-existing-results", "--results-dir", str(tmp_path), "--json"])
        finally:
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland

    assert_equal(rc, 0, "allow existing results override return code")
    data = json.loads(stdout.getvalue())
    assert_equal(data["run_iterations"][0]["results_dir"], str(tmp_path), "override reused requested results dir")


def test_run_checked_timeout_kills_process_group(module) -> None:
    class FakeProc:
        pid = 4242

        def __init__(self, *_args, **_kwargs) -> None:
            self.wait_calls = 0

        def wait(self, timeout=None):
            self.wait_calls += 1
            if self.wait_calls == 1:
                raise module.subprocess.TimeoutExpired(["fake"], timeout)
            return -9

    killpg_calls: list[tuple[int, int]] = []
    old_popen = module.subprocess.Popen
    old_killpg = module.os.killpg
    module.subprocess.Popen = FakeProc
    module.os.killpg = lambda pid, signum: killpg_calls.append((pid, signum))
    try:
        try:
            module.run_checked(["fake"], timeout=3)
        except module.subprocess.TimeoutExpired as exc:
            assert_equal(exc.timeout, 3, "run_checked timeout value")
        else:
            raise AssertionError("run_checked timeout did not raise")
    finally:
        module.subprocess.Popen = old_popen
        module.os.killpg = old_killpg

    assert_equal(killpg_calls, [(4242, module.signal.SIGKILL)], "run_checked timeout killpg call")


def test_cluster_exit_timeout_force_stops_group_with_log_tail(module) -> None:
    class FakeProc:
        pid = 5151

        def __init__(self) -> None:
            self.returncode = None
            self.wait_calls = 0

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            self.wait_calls += 1
            if self.wait_calls == 1:
                raise module.subprocess.TimeoutExpired(["cluster"], timeout)
            self.returncode = -15
            return self.returncode

        def terminate(self) -> None:
            self.returncode = -15

        def kill(self) -> None:
            self.returncode = -9

    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        log_path = Path(tmp_dir) / "cluster.log"
        log_path.write_text("line one\nimportant shutdown clue\n")
        proc = FakeProc()
        killpg_calls: list[tuple[int, int]] = []
        old_killpg = module.os.killpg
        module.os.killpg = lambda pid, signum: killpg_calls.append((pid, signum))
        try:
            try:
                module.wait_for_process_exit(proc, 7, "wos-cluster", log_path)
            except module.CoverageInputError as exc:
                message = str(exc)
            else:
                raise AssertionError("cluster exit timeout was accepted")
        finally:
            module.os.killpg = old_killpg

    if "timed out waiting 7s for wos-cluster to exit after guest shutdown" not in message:
        raise AssertionError(f"missing cluster exit timeout diagnostic: {message!r}")
    if "cluster log tail:" not in message or "important shutdown clue" not in message:
        raise AssertionError(f"missing cluster log tail in timeout diagnostic: {message!r}")
    assert_equal(killpg_calls, [(5151, module.signal.SIGTERM)], "cluster exit timeout forced TERM")


def test_guest_poweroff_accepts_ssh_disconnect_status(module) -> None:
    calls: list[tuple[list[str], int | None]] = []
    old_run_checked = module.run_checked

    def fake_disconnect(cmd, **kwargs):
        calls.append((cmd, kwargs.get("timeout")))
        raise module.subprocess.CalledProcessError(255, cmd)

    module.run_checked = fake_disconnect
    try:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            module.request_guest_poweroff("vm1", 9)
    finally:
        module.run_checked = old_run_checked

    assert_equal(
        calls,
        [([str(module.ROOT / "scripts" / "remote" / "wos_ssh.sh"), "vm1", "/sbin/poweroff -f"], 9)],
        "guest poweroff command",
    )
    assert_equal(stderr.getvalue(), "", "ssh disconnect during forced poweroff should not warn")

    def fake_failure(cmd, **_kwargs):
        raise module.subprocess.CalledProcessError(1, cmd)

    module.run_checked = fake_failure
    try:
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            module.request_guest_poweroff("vm2", 9)
    finally:
        module.run_checked = old_run_checked

    if "WARNING: failed to request poweroff on vm2" not in stderr.getvalue():
        raise AssertionError(f"non-ssh-disconnect poweroff failure did not warn: {stderr.getvalue()!r}")


def test_host_coverage_commands_write_health_logs(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        build_dir = tmp_path / "build"
        binary = tmp_path / "unit_a"
        binary.write_text("#!/bin/sh\n")
        binary.chmod(0o755)
        seen_logs: list[str] = []
        seen_exports: list[str] = []

        old_find_host_compiler = module.find_host_compiler
        old_find_llvm_tool = module.find_llvm_tool
        old_host_targets = module.host_unit_test_targets
        old_executables = module.executable_targets
        old_run_checked = module.run_checked
        old_export = module.run_checked_stdout_to_file

        def fake_run_checked(cmd, **kwargs):
            log_path = kwargs.get("log_path")
            if log_path is not None:
                seen_logs.append(Path(log_path).name)
            if cmd and cmd[0] == "ctest":
                pattern = kwargs["env"]["LLVM_PROFILE_FILE"]
                profraw = Path(pattern.replace("%p-%m", "fake-profile"))
                profraw.parent.mkdir(parents=True, exist_ok=True)
                profraw.write_text("profile")

        def fake_export(_cmd, stdout_path, log_path, **_kwargs):
            seen_exports.append(Path(log_path).name)
            write_lcov(Path(stdout_path), {"/repo/host.cpp": {1: 1}})

        module.find_host_compiler = lambda: (tmp_path / "clang", tmp_path / "clang++", "")
        module.find_llvm_tool = lambda name, _compiler=None: tmp_path / name
        module.host_unit_test_targets = lambda: ["unit_a"]
        module.executable_targets = lambda _build, _targets: [binary]
        module.run_checked = fake_run_checked
        module.run_checked_stdout_to_file = fake_export
        try:
            output = module.run_host_gtest_coverage(tmp_path, build_dir, jobs=2, timeout=30)
        finally:
            module.find_host_compiler = old_find_host_compiler
            module.find_llvm_tool = old_find_llvm_tool
            module.host_unit_test_targets = old_host_targets
            module.executable_targets = old_executables
            module.run_checked = old_run_checked
            module.run_checked_stdout_to_file = old_export

    assert_equal(output.name, "host-gtest.info", "host coverage lcov output")
    assert_equal(
        seen_logs,
        ["host-configure.log", "host-build.log", "host-ctest.log", "host-profdata.log"],
        "host coverage command logs",
    )
    assert_equal(seen_exports, ["host-export.log"], "host coverage export log")


def test_run_all_rejects_recoverable_host_ub_in_command_log(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)

        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage

        def fake_host(results_dir, _build, _jobs, _timeout):
            results_dir = Path(results_dir)
            (results_dir / "host-ctest.log").write_text("runtime error: signed integer overflow\n")
            output = results_dir / "host.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        def fake_ktest(_args, results_dir):
            output = Path(results_dir) / "kcov.info"
            write_lcov(output, {"/repo/kern.cpp": {1: 1}})
            return output

        def fake_userland(_args, results_dir):
            return write_fake_userland_artifacts(module, results_dir)

        module.run_host_gtest_coverage = fake_host
        module.run_ktest_kcov = fake_ktest
        module.run_userland_suite_coverage = fake_userland
        try:
            stderr = io.StringIO()
            with fake_external_log_evidence(module, tmp_path):
                with contextlib.redirect_stderr(stderr):
                    rc = module.main(["--run-all", "--results-dir", str(tmp_path)])
        finally:
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland

    assert_equal(rc, 1, "recoverable host UB should fail run-all")
    message = stderr.getvalue()
    for expected in ("host-ctest.log:1", "sanitizer runtime error", "runtime error: signed integer overflow"):
        if expected not in message:
            raise AssertionError(f"missing host UB health diagnostic {expected!r} in {message!r}")


def test_artifact_health_scan_rejects_failure_markers(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        bad_log = tmp_path / "serial-vm0.log"
        tool_log = tmp_path / "cluster-launch.log"
        lockup_log = tmp_path / "qemu-vm0.log"
        bug_log = tmp_path / "serial-vm1.log"
        timeout_log = tmp_path / "host-build.log"
        crash_log = tmp_path / "host-ctest.log"
        ub_log = tmp_path / "host-export.log"
        exit_log = tmp_path / "ktest-cov.log"
        good_log = tmp_path / "userland-suite.log"
        kcov_config_log = tmp_path / "ktest-run.log"
        bad_log.write_text("kernel panic: scheduler invariant failed\n")
        tool_log.write_text("ERROR: --no-setup requires an existing configured topology\n")
        lockup_log.write_text("watchdog: BUG: soft lockup - CPU#0 stuck for 23s\n")
        bug_log.write_text("kernel BUG: assertion tripped\n")
        timeout_log.write_text("TIMEOUT after 1800s\n")
        crash_log.write_text("ctest worker terminated by signal SIGSEGV\n")
        ub_log.write_text("Undefined behaviour observed in coverage export\n")
        exit_log.write_text("EXIT status 2\n")
        good_log.write_text("PASS=42 FAIL=0 SKIP=0\n[TESTD] DONE: 90 passed, 0 failed\n")
        kcov_config_log.write_text("-- [WOS] KCOV panic recent-PC trace enabled\n")

        bad_issues = module.scan_artifact_health(bad_log)
        tool_issues = module.scan_artifact_health(tool_log)
        lockup_issues = module.scan_artifact_health(lockup_log)
        bug_issues = module.scan_artifact_health(bug_log)
        timeout_issues = module.scan_artifact_health(timeout_log)
        crash_issues = module.scan_artifact_health(crash_log)
        ub_issues = module.scan_artifact_health(ub_log)
        exit_issues = module.scan_artifact_health(exit_log)
        good_issues = module.scan_artifact_health(good_log)
        kcov_config_issues = module.scan_artifact_health(kcov_config_log)

    assert_equal(len(bad_issues), 1, "artifact health panic issue count")
    assert_equal(bad_issues[0].pattern, "kernel panic", "artifact health panic pattern")
    assert_equal(len(tool_issues), 1, "artifact health tool error issue count")
    assert_equal(tool_issues[0].pattern, "tool error line", "artifact health tool error pattern")
    assert_equal(len(lockup_issues), 1, "artifact health lockup issue count")
    assert_equal(lockup_issues[0].pattern, "kernel lockup", "artifact health lockup pattern")
    assert_equal(len(bug_issues), 1, "artifact health kernel bug issue count")
    assert_equal(bug_issues[0].pattern, "kernel bug", "artifact health kernel bug pattern")
    assert_equal(len(timeout_issues), 1, "artifact health tool timeout issue count")
    assert_equal(timeout_issues[0].pattern, "tool timeout", "artifact health tool timeout pattern")
    assert_equal(len(crash_issues), 1, "artifact health signal crash issue count")
    assert_equal(crash_issues[0].pattern, "signal crash", "artifact health signal crash pattern")
    assert_equal(len(ub_issues), 1, "artifact health undefined behavior issue count")
    assert_equal(ub_issues[0].pattern, "undefined behavior marker", "artifact health undefined behavior pattern")
    assert_equal(len(exit_issues), 1, "artifact health command exit issue count")
    assert_equal(exit_issues[0].pattern, "nonzero command exit", "artifact health command exit pattern")
    assert_equal(good_issues, [], "benign zero-failure summaries should not trip health scan")
    assert_equal(kcov_config_issues, [], "KCOV panic-trace config banner should not trip health scan")


def test_artifact_health_scan_catches_marker_split_across_start_offset(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        split_log = tmp_path / "serial-split.log"
        prefix = b"partial line kernel pan"
        suffix = b"ic: split across saved offset\n"
        split_log.write_bytes(prefix + suffix)

        stale_log = tmp_path / "serial-stale.log"
        stale = b"kernel panic: stale before offset"
        stale_log.write_bytes(stale + b" with healthy suffix\n")

        split_issues = module.scan_artifact_health(
            split_log,
            start_offset=len(prefix),
            end_offset=len(prefix) + len(suffix),
        )
        stale_issues = module.scan_artifact_health(
            stale_log,
            start_offset=len(stale),
            end_offset=stale_log.stat().st_size,
        )

    assert_equal(len(split_issues), 1, "split marker issue count")
    assert_equal(split_issues[0].pattern, "kernel panic", "split marker pattern")
    assert_equal(stale_issues, [], "marker wholly before range should stay ignored")


def test_run_all_rejects_unhealthy_artifacts(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)

        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage

        def fake_host(results_dir, _build, _jobs, _timeout):
            output = Path(results_dir) / "host.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        def fake_ktest(_args, results_dir):
            output = Path(results_dir) / "kcov.info"
            write_lcov(output, {"/repo/kern.cpp": {1: 1}})
            return output

        def fake_userland(_args, results_dir):
            return write_fake_userland_artifacts(
                module,
                results_dir,
                cluster_name="cluster.log",
                cluster_text="[42.000] critical kernel: kernel panic: synthetic test marker\n",
            )

        module.run_host_gtest_coverage = fake_host
        module.run_ktest_kcov = fake_ktest
        module.run_userland_suite_coverage = fake_userland
        try:
            stderr = io.StringIO()
            with fake_external_log_evidence(module, tmp_path):
                with contextlib.redirect_stderr(stderr):
                    rc = module.main(["--run-all", "--results-dir", str(tmp_path)])
            failure_manifest = json.loads((tmp_path / "run-all-iteration.json").read_text())
        finally:
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland

    assert_equal(rc, 1, "unhealthy run-all return code")
    assert_equal(failure_manifest["status"], "fail", "unhealthy run-all manifest status")
    assert_equal(failure_manifest["artifacts"]["cluster_log"], str(tmp_path / "cluster.log"), "unhealthy run-all failure kept cluster log")
    message = stderr.getvalue()
    for expected in ("run-all artifact health scan", "kernel panic", "cluster.log:1"):
        if expected not in message:
            raise AssertionError(f"missing health-scan diagnostic {expected!r} in {message!r}")


def test_run_all_external_log_health_scan_starts_at_iteration_offset(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        cluster_config = tmp_path / "cluster.json"
        cluster_config.write_text(json.dumps({"zones": [{"id": "GLOBAL"}, {"id": 0, "nodes": 1}]}))
        serial_log = tmp_path / "serial-vm0.log"
        serial_log.write_text("kernel panic: old boot marker before this run\n")
        (tmp_path / "qemu-vm0.log").write_text("qemu healthy before run\n")
        ktest_dir = tmp_path / "ktest-data"
        ktest_dir.mkdir()
        (ktest_dir / "serial-vm0.log").write_text("ktest serial healthy before run\n")
        (ktest_dir / "qemu-vm0.log").write_text("ktest qemu healthy before run\n")
        results_clean = tmp_path / "clean-results"
        results_bad = tmp_path / "bad-results"
        append_new_marker = False

        old_root = module.ROOT
        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage

        def fake_host(results_dir, _build, _jobs, _timeout):
            output = Path(results_dir) / "host.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        def fake_ktest(_args, results_dir):
            with (ktest_dir / "serial-vm0.log").open("a") as output:
                output.write("ktest serial current run healthy\n")
            with (ktest_dir / "qemu-vm0.log").open("a") as output:
                output.write("ktest qemu current run healthy\n")
            output = Path(results_dir) / "kcov.info"
            write_lcov(output, {"/repo/kern.cpp": {1: 1}})
            return output

        def fake_userland(_args, results_dir):
            with serial_log.open("a") as output:
                if append_new_marker:
                    output.write("kernel panic: new current-run marker\n")
                else:
                    output.write("current run healthy\n")
            with (tmp_path / "qemu-vm0.log").open("a") as output:
                output.write("qemu current run healthy\n")
            return write_fake_userland_artifacts(module, results_dir)

        module.ROOT = tmp_path
        module.run_host_gtest_coverage = fake_host
        module.run_ktest_kcov = fake_ktest
        module.run_userland_suite_coverage = fake_userland
        try:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                clean_rc = module.main(
                    [
                        "--run-all",
                        "--results-dir",
                        str(results_clean),
                        "--userland-cluster-config",
                        str(cluster_config),
                        "--json",
                    ]
                )
            append_new_marker = True
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                bad_rc = module.main(
                    [
                        "--run-all",
                        "--results-dir",
                        str(results_bad),
                        "--userland-cluster-config",
                        str(cluster_config),
                    ]
                )
        finally:
            module.ROOT = old_root
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland

    assert_equal(clean_rc, 0, "old external panic marker should not fail new run")
    assert_equal(bad_rc, 1, "new external panic marker should fail run")
    message = stderr.getvalue()
    for expected in ("serial-vm0.log", "kernel panic", "new current-run marker"):
        if expected not in message:
            raise AssertionError(f"missing new external marker diagnostic {expected!r} in {message!r}")
    if "old boot marker" in message:
        raise AssertionError(f"old external marker leaked into current-run health scan: {message!r}")


def test_run_all_rejects_empty_current_external_log_range(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        stale_log = tmp_path / "external-proof.log"
        stale_log.write_text("external proof from before this run\n")

        old_expected = module.expected_external_health_log_paths
        old_observed = module.external_health_log_paths
        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage

        def fake_host(results_dir, _build, _jobs, _timeout):
            output = Path(results_dir) / "host-gtest.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        def fake_ktest(_args, results_dir):
            output = Path(results_dir) / "kcov.info"
            write_lcov(output, {"/repo/kern.cpp": {1: 1}})
            return output

        def fake_userland(_args, results_dir):
            return write_fake_userland_artifacts(module, results_dir, log_name="userland-suite.log")

        module.expected_external_health_log_paths = lambda _args: [stale_log]
        module.external_health_log_paths = lambda _args: [stale_log]
        module.run_host_gtest_coverage = fake_host
        module.run_ktest_kcov = fake_ktest
        module.run_userland_suite_coverage = fake_userland
        try:
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                rc = module.main(["--run-all", "--results-dir", str(tmp_path)])
            manifest = json.loads((tmp_path / "run-all-iteration.json").read_text())
        finally:
            module.expected_external_health_log_paths = old_expected
            module.external_health_log_paths = old_observed
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland

    assert_equal(rc, 1, "empty current external log range return code")
    assert_equal(manifest["status"], "fail", "empty current external log range manifest status")
    message = stderr.getvalue()
    for expected in ("captured no current-run bytes", "external-proof.log"):
        if expected not in message:
            raise AssertionError(f"missing empty current external range diagnostic {expected!r} in {message!r}")
    if "captured no current-run bytes" not in manifest.get("error", ""):
        raise AssertionError(f"empty current external range manifest lost diagnostic: {manifest!r}")


def test_run_all_accepts_quiet_qemu_external_log_range(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        qemu_log = tmp_path / "qemu-vm0.log"
        qemu_log.write_text("qemu sidecar from before this run\n")

        old_expected = module.expected_external_health_log_paths
        old_observed = module.external_health_log_paths
        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage

        def fake_host(results_dir, _build, _jobs, _timeout):
            output = Path(results_dir) / "host-gtest.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        def fake_ktest(_args, results_dir):
            output = Path(results_dir) / "kcov.info"
            write_lcov(output, {"/repo/kern.cpp": {1: 1}})
            return output

        def fake_userland(_args, results_dir):
            return write_fake_userland_artifacts(module, results_dir, log_name="userland-suite.log")

        module.expected_external_health_log_paths = lambda _args: [qemu_log]
        module.external_health_log_paths = lambda _args: [qemu_log]
        module.run_host_gtest_coverage = fake_host
        module.run_ktest_kcov = fake_ktest
        module.run_userland_suite_coverage = fake_userland
        try:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                rc = module.main(["--run-all", "--results-dir", str(tmp_path / "results"), "--json"])
            manifest = json.loads((tmp_path / "results" / "run-all-iteration.json").read_text())
        finally:
            module.expected_external_health_log_paths = old_expected
            module.external_health_log_paths = old_observed
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland

    assert_equal(rc, 0, "quiet qemu external log range return code")
    ranges = [item for item in manifest["external_log_ranges"] if item["path"] == str(qemu_log)]
    assert_equal(len(ranges), 1, "quiet qemu external log range manifest entry count")
    assert_equal(ranges[0]["end_offset"], ranges[0]["start_offset"], "quiet qemu external log range")


def test_run_all_artifacts_feed_aggregator(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        host_lcov = tmp_path / "host.info"
        kcov_lcov = tmp_path / "kcov.info"
        userland_log = tmp_path / "userland.log"
        cluster_log = tmp_path / "cluster.log"
        userland_lcov = tmp_path / "wos-userland.info"
        write_lcov(host_lcov, {"/repo/host.cpp": {1: 1, 2: 0}})
        write_lcov(kcov_lcov, {"/repo/kern.cpp": {1: 1, 2: 1}})
        userland_log.write_text(complete_testd_log(module) + complete_userland_log(module))
        cluster_log.write_text("$ /repo/bin/wos-cluster --launch --no-setup --config configs/cluster.json\ncluster ready\n")
        write_wos_userland_lcov(userland_lcov)

        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage
        module.run_host_gtest_coverage = lambda _results, _build, _jobs, _timeout: host_lcov
        module.run_ktest_kcov = lambda _args, _results: kcov_lcov
        module.run_userland_suite_coverage = lambda _args, _results: (userland_log, cluster_log, userland_lcov)
        try:
            stdout = io.StringIO()
            with fake_external_log_evidence(module, tmp_path):
                with contextlib.redirect_stdout(stdout):
                    rc = module.main(["--run-all", "--results-dir", str(tmp_path), "--json"])
        finally:
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland

    assert_equal(rc, 0, "run-all aggregate return code")
    data = json.loads(stdout.getvalue())
    assert_equal(data["run_artifacts"]["host_lcov"], str(host_lcov), "run-all host artifact")
    assert_equal(data["run_artifacts"]["kcov_lcov"], str(kcov_lcov), "run-all kcov artifact")
    assert_equal(data["run_artifacts"]["userland_log"], str(userland_log), "run-all userland artifact")
    assert_equal(data["run_artifacts"]["userland_lcov"], str(userland_lcov), "run-all WOS userland LCOV artifact")
    assert_equal(len(data["run_iterations"]), 1, "run-all iteration count")
    assert_equal(data["run_iterations"][0]["results_dir"], str(tmp_path), "single run-all results directory")
    assert_equal(
        data["run_iterations"][0]["run_config"]["userland_launch_no_setup"],
        True,
        "run-all iteration JSON exposes no-setup userland mode",
    )
    assert_equal(
        data["run_iterations"][0]["run_config"]["userland_auto_shutdown"],
        True,
        "run-all iteration JSON exposes userland auto-shutdown",
    )
    assert_equal(
        data["run_iterations"][0]["run_config"]["userland_shutdown_action"],
        "poweroff",
        "run-all iteration JSON exposes userland poweroff action",
    )
    assert_equal(
        data["run_iterations"][0]["run_config"]["skip_ktest"],
        False,
        "run-all iteration JSON exposes full KTEST participation",
    )
    if not data["run_iterations"][0]["external_log_ranges"]:
        raise AssertionError(f"run-all iteration JSON omitted external log ranges: {data!r}")
    if data["combined"]["hit"] <= 0 or data["combined"]["total"] <= 0:
        raise AssertionError(f"run-all did not aggregate produced artifacts: {data!r}")


def test_run_all_requires_wos_userland_lcov(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage

        def fake_host(results_dir, _build, _jobs, _timeout):
            output = Path(results_dir) / "host.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        def fake_ktest(_args, results_dir):
            output = Path(results_dir) / "kcov.info"
            write_lcov(output, {"/repo/kern.cpp": {1: 1}})
            return output

        def fake_userland(_args, results_dir):
            output = Path(results_dir) / "userland.log"
            output.write_text(complete_testd_log(module) + complete_userland_log(module))
            return output, None

        module.run_host_gtest_coverage = fake_host
        module.run_ktest_kcov = fake_ktest
        module.run_userland_suite_coverage = fake_userland
        try:
            stderr = io.StringIO()
            with fake_external_log_evidence(module, tmp_path):
                with contextlib.redirect_stderr(stderr):
                    rc = module.main(["--run-all", "--results-dir", str(tmp_path)])
            manifest = json.loads((tmp_path / "run-all-iteration.json").read_text())
        finally:
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland

    assert_equal(rc, 1, "run-all missing WOS userland LCOV return code")
    message = stderr.getvalue()
    if "WOS userland LCOV artifact was not produced" not in message:
        raise AssertionError(f"missing WOS userland LCOV diagnostic: {message!r}")
    assert_equal(manifest["status"], "fail", "missing WOS userland LCOV manifest status")
    assert_equal(manifest["artifacts"]["userland_lcov"], None, "missing WOS userland LCOV artifact")


def test_wos_userland_lcov_export_includes_all_userland_objects(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        build_dir = tmp_path / "build-userland-coverage"
        results_dir = tmp_path / "results"
        results_dir.mkdir()
        profraw = tmp_path / "vm.profraw"
        profraw.write_text("raw profile placeholder\n")

        expected_objects: list[str] = []
        for _label, relative_path in module.WOS_USERLAND_COVERAGE_OBJECTS:
            object_path = build_dir / relative_path
            object_path.parent.mkdir(parents=True, exist_ok=True)
            object_path.write_text("ELF placeholder\n")
            expected_objects.append(str(object_path))

        profdata_cmds: list[list[str]] = []
        export_cmds: list[list[str]] = []
        old_wos_clang = module.wos_clang
        old_find_llvm_tool = module.find_llvm_tool
        old_run_checked = module.run_checked
        old_run_checked_stdout_to_file = module.run_checked_stdout_to_file

        def fake_run_checked(cmd, **_kwargs):
            profdata_cmds.append(cmd)
            if "-o" in cmd:
                Path(cmd[cmd.index("-o") + 1]).write_text("merged profile placeholder\n")

        def fake_run_checked_stdout_to_file(cmd, stdout_path, log_path, **_kwargs):
            export_cmds.append(cmd)
            write_wos_userland_lcov(stdout_path)
            log_path.write_text("export ok\n")

        module.wos_clang = lambda: tmp_path / "clang"
        module.find_llvm_tool = lambda name, _compiler=None: tmp_path / name
        module.run_checked = fake_run_checked
        module.run_checked_stdout_to_file = fake_run_checked_stdout_to_file
        try:
            args = module.argparse.Namespace(
                userland_coverage_build_dir=str(build_dir),
                userland_profile_fetch_timeout=7,
            )
            lcov_path = module.export_wos_userland_lcov(args, results_dir, [profraw])
        finally:
            module.wos_clang = old_wos_clang
            module.find_llvm_tool = old_find_llvm_tool
            module.run_checked = old_run_checked
            module.run_checked_stdout_to_file = old_run_checked_stdout_to_file

    assert_equal(lcov_path, results_dir / "wos-userland.info", "WOS userland LCOV output path")
    if not profdata_cmds:
        raise AssertionError("WOS userland LCOV export did not merge profraw input")
    if not export_cmds:
        raise AssertionError("WOS userland LCOV export did not run llvm-cov export")
    export_cmd = export_cmds[0]
    primary_object = export_cmd[export_cmd.index(str(results_dir / "wos-userland.profdata")) + 1]
    actual_objects = [primary_object]
    actual_objects.extend(export_cmd[index + 1] for index, value in enumerate(export_cmd[:-1]) if value == "-object")
    assert_equal(actual_objects, expected_objects, "llvm-cov export WOS userland object list")
    if "-ignore-filename-regex" not in export_cmd:
        raise AssertionError(f"WOS userland LCOV export did not filter non-project sources: {export_cmd!r}")


def test_wos_userland_profraw_fetch_collects_all_cluster_nodes(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        cluster_config = tmp_path / "cluster.json"
        cluster_config.write_text(json.dumps({"zones": [{"id": "GLOBAL"}, {"id": 0, "nodes": 3}]}))
        remote_dir = module.DEFAULT_WOS_USERLAND_PROFILE_DIR
        snapshot_dir = f"{remote_dir}/.fetch"
        listed_hosts: list[str] = []
        list_commands: list[str] = []
        fetched_urls: list[str] = []
        fetch_timeouts: list[int | None] = []
        old_run_checked_capture = module.run_checked_capture
        old_fetch_http_to_file = module.fetch_http_to_file
        old_resolve_wos_http_target = module.resolve_wos_http_target

        def fake_run_checked_capture(cmd, **_kwargs):
            host = cmd[1]
            listed_hosts.append(host)
            list_commands.append(cmd[-1])
            return f"{snapshot_dir}/{host}-profile.profraw\n"

        def fake_fetch_http_to_file(url, local_path, timeout, log_path):
            _ = log_path
            fetched_urls.append(url)
            fetch_timeouts.append(timeout)
            Path(local_path).write_text(f"raw profile from {url}\n")

        module.run_checked_capture = fake_run_checked_capture
        module.fetch_http_to_file = fake_fetch_http_to_file
        module.resolve_wos_http_target = lambda host, _results_dir: f"10.0.0.{host[-1]}"
        try:
            args = module.argparse.Namespace(
                userland_cluster_config=str(cluster_config),
                userland_profile_fetch_timeout=7,
                userland_profile_file_fetch_timeout=5,
            )
            profraws = module.fetch_remote_userland_profraws(args, remote_dir, tmp_path)
        finally:
            module.run_checked_capture = old_run_checked_capture
            module.fetch_http_to_file = old_fetch_http_to_file
            module.resolve_wos_http_target = old_resolve_wos_http_target

    assert_equal(listed_hosts, ["vm0", "vm1", "vm2"], "WOS profile listing hosts")
    assert_equal(
        fetched_urls,
        [
            f"http://10.0.0.0{snapshot_dir}/vm0-profile.profraw",
            f"http://10.0.0.1{snapshot_dir}/vm1-profile.profraw",
            f"http://10.0.0.2{snapshot_dir}/vm2-profile.profraw",
        ],
        "WOS profile HTTP fetch URLs",
    )
    assert_equal(fetch_timeouts, [5, 5, 5], "WOS profile HTTP fetch timeouts")
    if not all(f"{remote_dir}/.fetch" in command and " cp " in f" {command} " for command in list_commands):
        raise AssertionError(f"WOS profile listing did not snapshot remote files first: {list_commands!r}")
    assert_equal([path.name for path in profraws], [
        "0001-vm0-vm0-profile.profraw",
        "0002-vm1-vm1-profile.profraw",
        "0003-vm2-vm2-profile.profraw",
    ], "local WOS profile names")
    assert_equal(
        module.userland_profile_file_pattern(remote_dir),
        f"{remote_dir}/%m-%p.profraw%c",
        "continuous WOS profile filename pattern",
    )


def test_wos_userland_profraw_fetch_skips_stuck_profile(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        cluster_config = tmp_path / "cluster.json"
        cluster_config.write_text(json.dumps({"zones": [{"id": "GLOBAL"}, {"id": 0, "nodes": 1}]}))
        remote_dir = module.DEFAULT_WOS_USERLAND_PROFILE_DIR
        snapshot_dir = f"{remote_dir}/.fetch"
        old_run_checked_capture = module.run_checked_capture
        old_fetch_http_to_file = module.fetch_http_to_file
        old_resolve_wos_http_target = module.resolve_wos_http_target
        fetch_timeouts: list[int | None] = []

        def fake_run_checked_capture(_cmd, **_kwargs):
            return f"{snapshot_dir}/ok.profraw\n{snapshot_dir}/stuck.profraw\n"

        def fake_fetch_http_to_file(url, local_path, timeout, log_path):
            _ = log_path
            fetch_timeouts.append(timeout)
            if url.endswith("stuck.profraw"):
                raise TimeoutError("stuck HTTP profile")
            Path(local_path).write_text("raw profile\n")

        module.run_checked_capture = fake_run_checked_capture
        module.fetch_http_to_file = fake_fetch_http_to_file
        module.resolve_wos_http_target = lambda _host, _results_dir: "10.0.0.8"
        try:
            args = module.argparse.Namespace(
                userland_cluster_config=str(cluster_config),
                userland_profile_fetch_timeout=300,
                userland_profile_file_fetch_timeout=4,
            )
            with contextlib.redirect_stderr(io.StringIO()):
                profraws = module.fetch_remote_userland_profraws(args, remote_dir, tmp_path)
            warning_text = (tmp_path / "userland-profraw-fetch-warnings.log").read_text()
        finally:
            module.run_checked_capture = old_run_checked_capture
            module.fetch_http_to_file = old_fetch_http_to_file
            module.resolve_wos_http_target = old_resolve_wos_http_target

    assert_equal([path.name for path in profraws], ["0001-vm0-ok.profraw"], "stuck WOS profile fetch should be skipped")
    assert_equal(fetch_timeouts, [4, 4], "per-profile fetch timeout")
    if "stuck.profraw" not in warning_text:
        raise AssertionError("stuck profile warning was not recorded")


def test_wos_userland_lcov_rejects_existing_cluster(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        cluster_config = tmp_path / "cluster.json"
        cluster_config.write_text(json.dumps({"zones": [{"id": "GLOBAL"}, {"id": 0, "nodes": 1}]}))
        args = module.argparse.Namespace(
            userland_existing_cluster=True,
            userland_no_lcov=False,
            userland_cluster_config=str(cluster_config),
        )
        try:
            module.run_userland_suite_coverage(args, tmp_path)
        except module.CoverageInputError as exc:
            message = str(exc)
        else:
            raise AssertionError("existing-cluster WOS userland LCOV run was accepted")

    if "requires an auto-launched cluster" not in message:
        raise AssertionError(f"missing existing-cluster LCOV diagnostic: {message!r}")


def test_run_all_repeat_uses_iteration_dirs_and_timing_gate(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        seen_dirs: list[Path] = []
        monotonic_values = iter([100.0, 110.0, 200.0, 210.1])

        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage
        old_monotonic = module.time.monotonic

        def fake_host(results_dir, _build, _jobs, _timeout):
            seen_dirs.append(Path(results_dir))
            output = Path(results_dir) / "host.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        def fake_ktest(_args, results_dir):
            output = Path(results_dir) / "kcov.info"
            write_lcov(output, {"/repo/kern.cpp": {1: 1}})
            return output

        def fake_userland(_args, results_dir):
            return write_fake_userland_artifacts(module, results_dir, cluster_name="cluster.log")

        module.run_host_gtest_coverage = fake_host
        module.run_ktest_kcov = fake_ktest
        module.run_userland_suite_coverage = fake_userland
        module.time.monotonic = lambda: next(monotonic_values)
        try:
            stdout = io.StringIO()
            with fake_external_log_evidence(module, tmp_path):
                with contextlib.redirect_stdout(stdout):
                    rc = module.main(["--run-all", "--repeat", "2", "--results-dir", str(tmp_path), "--json"])
            first_manifest = json.loads((tmp_path / "run-0001" / "run-all-iteration.json").read_text())
            second_manifest = json.loads((tmp_path / "run-0002" / "run-all-iteration.json").read_text())
        finally:
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland
            module.time.monotonic = old_monotonic

    assert_equal(rc, 0, "repeat run-all return code")
    data = json.loads(stdout.getvalue())
    assert_equal(seen_dirs, [tmp_path / "run-0001", tmp_path / "run-0002"], "repeat run-all result dirs")
    assert_equal(len(data["run_iterations"]), 2, "repeat run-all JSON iteration count")
    assert_equal(data["run_iterations"][0]["elapsed_seconds"], 10.0, "first repeat elapsed seconds")
    assert_equal(data["run_iterations"][1]["elapsed_seconds"], 10.099999999999994, "second repeat elapsed seconds")
    assert_equal(data["run_iterations"][1]["runtime_delta_percent"], 0.9999999999999432, "second repeat JSON runtime delta")
    if not data["run_iterations"][1]["run_config"]["expected_external_logs"]:
        raise AssertionError(f"repeat run JSON omitted expected external logs: {data!r}")
    assert_equal(data["run_artifacts"]["host_lcov"], str(tmp_path / "run-0002" / "host.info"), "final artifact comes from last repeat")
    assert_equal(first_manifest["status"], "pass", "first repeat manifest status")
    assert_equal(second_manifest["status"], "pass", "second repeat manifest status")
    assert_equal(second_manifest["runtime_delta_percent"], 0.9999999999999432, "second repeat manifest runtime delta")


def test_run_all_resume_continues_after_verified_prefix(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        fake_root = tmp_path / "repo-root"
        write_clean_run_all_manifest_set(module, tmp_path, [10.0, 10.1], external_root=fake_root, repeat=3)
        seen_dirs: list[Path] = []
        monotonic_values = iter([200.0, 210.05])

        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage
        old_monotonic = module.time.monotonic

        def fake_host(results_dir, _build, _jobs, _timeout):
            results_dir = Path(results_dir)
            seen_dirs.append(results_dir)
            output = results_dir / "host.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        def fake_ktest(_args, results_dir):
            output = Path(results_dir) / "kcov.info"
            write_lcov(output, {"/repo/kern.cpp": {1: 1}})
            return output

        def fake_userland(_args, results_dir):
            return write_fake_userland_artifacts(module, results_dir, cluster_name="cluster.log")

        module.run_host_gtest_coverage = fake_host
        module.run_ktest_kcov = fake_ktest
        module.run_userland_suite_coverage = fake_userland
        module.time.monotonic = lambda: next(monotonic_values)
        try:
            stdout = io.StringIO()
            with temporary_module_root(module, fake_root):
                with fake_external_log_evidence(module, tmp_path):
                    with contextlib.redirect_stdout(stdout):
                        rc = module.main(
                            [
                                "--run-all",
                                "--repeat",
                                "3",
                                "--resume-run-all",
                                "--results-dir",
                                str(tmp_path),
                                "--json",
                            ]
                        )
            third_manifest = json.loads((tmp_path / "run-0003" / "run-all-iteration.json").read_text())
        finally:
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland
            module.time.monotonic = old_monotonic

    assert_equal(rc, 0, "resume run-all return code")
    data = json.loads(stdout.getvalue())
    assert_equal(seen_dirs, [tmp_path / "run-0003"], "resume should only run missing iteration")
    assert_equal(len(data["run_iterations"]), 3, "resume JSON iteration count")
    assert_equal(data["run_iterations"][0]["results_dir"], str(tmp_path / "run-0001"), "resume kept first prior iteration")
    assert_equal(data["run_iterations"][1]["results_dir"], str(tmp_path / "run-0002"), "resume kept second prior iteration")
    assert_equal(data["run_iterations"][2]["results_dir"], str(tmp_path / "run-0003"), "resume appended new iteration")
    assert_equal(data["run_artifacts"]["host_lcov"], str(tmp_path / "run-0003" / "host.info"), "resume final artifact")
    assert_equal(third_manifest["status"], "pass", "resume new manifest status")


def test_run_all_resume_rejects_failed_prefix_before_running(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        fake_root = tmp_path / "repo-root"
        write_clean_run_all_manifest_set(module, tmp_path, [10.0], external_root=fake_root, repeat=2)
        mutate_json_file(
            tmp_path / "run-0001" / "run-all-iteration.json",
            lambda data: data.__setitem__("status", "fail"),
        )
        called = False
        old_host = module.run_host_gtest_coverage

        def fake_host(_results_dir, _build, _jobs, _timeout):
            nonlocal called
            called = True
            raise AssertionError("resume ran after failed prefix")

        module.run_host_gtest_coverage = fake_host
        try:
            stderr = io.StringIO()
            with temporary_module_root(module, fake_root):
                with contextlib.redirect_stderr(stderr):
                    rc = module.main(
                        [
                            "--run-all",
                            "--repeat",
                            "2",
                            "--resume-run-all",
                            "--results-dir",
                            str(tmp_path),
                        ]
                    )
        finally:
            module.run_host_gtest_coverage = old_host

    assert_equal(rc, 1, "resume failed prefix return code")
    assert_equal(called, False, "resume failed prefix should not run more iterations")
    message = stderr.getvalue()
    for expected in ("run-all results verification failed", "status='fail'"):
        if expected not in message:
            raise AssertionError(f"missing resume failed-prefix diagnostic {expected!r} in {message!r}")


def test_run_all_resume_rejects_incomplete_run_directory(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        (tmp_path / "run-0001").mkdir(parents=True)
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = module.main(
                [
                    "--run-all",
                    "--repeat",
                    "2",
                    "--resume-run-all",
                    "--results-dir",
                    str(tmp_path),
                ]
            )

    assert_equal(rc, 1, "resume incomplete dir return code")
    message = stderr.getvalue()
    for expected in ("cannot resume with incomplete run directory", "run-0001"):
        if expected not in message:
            raise AssertionError(f"missing resume incomplete-dir diagnostic {expected!r} in {message!r}")


def test_run_all_repeat_rejects_runtime_delta(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        monotonic_values = iter([0.0, 10.0, 100.0, 110.3, 110.4])

        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage
        old_monotonic = module.time.monotonic

        def fake_host(results_dir, _build, _jobs, _timeout):
            output = Path(results_dir) / "host.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        def fake_ktest(_args, results_dir):
            output = Path(results_dir) / "kcov.info"
            write_lcov(output, {"/repo/kern.cpp": {1: 1}})
            return output

        def fake_userland(_args, results_dir):
            return write_fake_userland_artifacts(module, results_dir)

        module.run_host_gtest_coverage = fake_host
        module.run_ktest_kcov = fake_ktest
        module.run_userland_suite_coverage = fake_userland
        module.time.monotonic = lambda: next(monotonic_values)
        try:
            stderr = io.StringIO()
            with fake_external_log_evidence(module, tmp_path):
                with contextlib.redirect_stderr(stderr):
                    rc = module.main(["--run-all", "--repeat", "2", "--results-dir", str(tmp_path)])
            second_manifest = json.loads((tmp_path / "run-0002" / "run-all-iteration.json").read_text())
        finally:
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland
            module.time.monotonic = old_monotonic

    assert_equal(rc, 1, "repeat runtime delta failure return code")
    if "run-all runtime delta" not in stderr.getvalue():
        raise AssertionError(f"missing runtime delta failure: {stderr.getvalue()!r}")
    assert_equal(second_manifest["status"], "fail", "runtime delta failure manifest status")
    assert_equal(second_manifest["artifacts"]["host_lcov"], str(tmp_path / "run-0002" / "host.info"), "runtime delta failure kept host artifact")
    assert_equal(second_manifest["artifacts"]["kcov_lcov"], str(tmp_path / "run-0002" / "kcov.info"), "runtime delta failure kept kcov artifact")
    assert_equal(second_manifest["artifacts"]["userland_log"], str(tmp_path / "run-0002" / "userland.log"), "runtime delta failure kept userland artifact")
    assert_equal(second_manifest["artifacts"]["userland_lcov"], str(tmp_path / "run-0002" / "wos-userland.info"), "runtime delta failure kept WOS userland LCOV artifact")
    if "run-all runtime delta" not in second_manifest.get("error", ""):
        raise AssertionError(f"runtime delta failure manifest lost diagnostic: {second_manifest!r}")


def test_run_all_repeat_rejects_runtime_delta_before_later_iterations(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        seen_dirs: list[Path] = []
        monotonic_values = iter([0.0, 10.0, 100.0, 110.3, 110.4])

        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage
        old_monotonic = module.time.monotonic

        def fake_host(results_dir, _build, _jobs, _timeout):
            results_dir = Path(results_dir)
            seen_dirs.append(results_dir)
            output = results_dir / "host.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        def fake_ktest(_args, results_dir):
            output = Path(results_dir) / "kcov.info"
            write_lcov(output, {"/repo/kern.cpp": {1: 1}})
            return output

        def fake_userland(_args, results_dir):
            return write_fake_userland_artifacts(module, results_dir)

        module.run_host_gtest_coverage = fake_host
        module.run_ktest_kcov = fake_ktest
        module.run_userland_suite_coverage = fake_userland
        module.time.monotonic = lambda: next(monotonic_values)
        try:
            stderr = io.StringIO()
            with fake_external_log_evidence(module, tmp_path):
                with contextlib.redirect_stderr(stderr):
                    rc = module.main(["--run-all", "--repeat", "3", "--results-dir", str(tmp_path)])
            first_manifest = json.loads((tmp_path / "run-0001" / "run-all-iteration.json").read_text())
            second_manifest = json.loads((tmp_path / "run-0002" / "run-all-iteration.json").read_text())
        finally:
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland
            module.time.monotonic = old_monotonic

    assert_equal(rc, 1, "early repeat runtime delta failure return code")
    assert_equal(seen_dirs, [tmp_path / "run-0001", tmp_path / "run-0002"], "repeat timing gate should stop before run 3")
    assert_equal(first_manifest["status"], "pass", "first early-stop manifest status")
    assert_equal(second_manifest["status"], "fail", "second early-stop manifest status")
    message = stderr.getvalue()
    for expected in ("run-all runtime delta", "slowest run 2"):
        if expected not in message:
            raise AssertionError(f"missing early runtime delta diagnostic {expected!r} in {message!r}")


def test_run_all_failure_manifest_keeps_partial_artifacts(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage

        def fake_host(results_dir, _build, _jobs, _timeout):
            output = Path(results_dir) / "host-gtest.info"
            write_lcov(output, {"/repo/host.cpp": {1: 1}})
            return output

        def fake_ktest(_args, results_dir):
            output = Path(results_dir) / "kcov.info"
            write_lcov(output, {"/repo/kern.cpp": {1: 1}})
            return output

        def fake_userland(_args, results_dir):
            results_dir = Path(results_dir)
            (results_dir / "userland-suite.log").write_text("suite stopped after netbench\n")
            (results_dir / "cluster-launch.log").write_text("cluster log preserved\n")
            write_wos_userland_lcov(results_dir / "wos-userland.info")
            raise module.CoverageInputError("synthetic userland failure")

        module.run_host_gtest_coverage = fake_host
        module.run_ktest_kcov = fake_ktest
        module.run_userland_suite_coverage = fake_userland
        try:
            stderr = io.StringIO()
            with fake_external_log_evidence(module, tmp_path):
                with contextlib.redirect_stderr(stderr):
                    rc = module.main(["--run-all", "--results-dir", str(tmp_path)])
            manifest = json.loads((tmp_path / "run-all-iteration.json").read_text())
        finally:
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland

    assert_equal(rc, 1, "partial artifact failure return code")
    assert_equal(manifest["status"], "fail", "partial artifact manifest status")
    assert_equal(manifest["error"], "synthetic userland failure", "partial artifact failure diagnostic")
    assert_equal(manifest["artifacts"]["host_lcov"], str(tmp_path / "host-gtest.info"), "partial host artifact")
    assert_equal(manifest["artifacts"]["kcov_lcov"], str(tmp_path / "kcov.info"), "partial kcov artifact")
    assert_equal(manifest["artifacts"]["userland_log"], str(tmp_path / "userland-suite.log"), "partial userland log")
    assert_equal(manifest["artifacts"]["cluster_log"], str(tmp_path / "cluster-launch.log"), "partial cluster log")
    assert_equal(manifest["artifacts"]["userland_lcov"], str(tmp_path / "wos-userland.info"), "partial WOS userland LCOV artifact")
    if not manifest["external_log_ranges"]:
        raise AssertionError(f"partial failure manifest omitted external evidence: {manifest!r}")
    if "synthetic userland failure" not in stderr.getvalue():
        raise AssertionError(f"partial failure stderr lost diagnostic: {stderr.getvalue()!r}")


def write_full_external_log_evidence(
    module,
    base_dir: Path,
    *extra_ranges,
    external_root: Path | None = None,
) -> tuple[list[str], list[object]]:
    root = external_root if external_root is not None else base_dir
    log_paths = []
    for node in range(4):
        log_paths.append(root / f"serial-vm{node}.log")
        log_paths.append(root / f"qemu-vm{node}.log")
    log_paths.append(root / "ktest-data" / "serial-vm0.log")
    log_paths.append(root / "ktest-data" / "qemu-vm0.log")

    ranges = []
    for log_path in log_paths:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(f"{log_path.name} current run healthy\n")
        ranges.append(module.LogRange(log_path, 0, log_path.stat().st_size))
    ranges.extend(extra_ranges)
    return [str(item.path) for item in ranges], ranges


def write_clean_run_all_manifest_set(
    module,
    tmp_path: Path,
    elapsed: list[float],
    *,
    runtime_delta_overrides: dict[int, float | None] | None = None,
    external_root: Path | None = None,
    repeat: int | None = None,
) -> None:
    repeat_value = repeat if repeat is not None else len(elapsed)
    for index, seconds in enumerate(elapsed, start=1):
        run_dir = tmp_path / f"run-{index:04d}"
        run_dir.mkdir(parents=True, exist_ok=True)
        host_lcov = run_dir / "host.info"
        kcov_lcov = run_dir / "kcov.info"
        userland_log = run_dir / "userland.log"
        cluster_log = run_dir / "cluster.log"
        userland_lcov = run_dir / "wos-userland.info"
        write_lcov(host_lcov, {"/repo/host.cpp": {1: 1}})
        write_lcov(kcov_lcov, {"/repo/kern.cpp": {1: 1}})
        userland_log.write_text(complete_testd_log(module) + complete_userland_log(module))
        cluster_log.write_text("$ /repo/bin/wos-cluster --launch --no-setup --config configs/cluster.json\ncluster ready\n")
        write_wos_userland_lcov(userland_lcov)
        run_config = module.full_run_all_config_json()
        expected_logs, external_log_ranges = write_full_external_log_evidence(
            module,
            run_dir,
            external_root=external_root,
        )
        run_config["expected_external_logs"] = expected_logs
        runtime_delta_value = module.runtime_delta_percent(
            [
                module.AutoRunIteration(i + 1, tmp_path / f"run-{i + 1:04d}", module.AutoRunArtifacts(), value)
                for i, value in enumerate(elapsed[:index])
            ]
        )
        if runtime_delta_overrides is not None and index in runtime_delta_overrides:
            runtime_delta_value = runtime_delta_overrides[index]
        kwargs = {
            "index": index,
            "repeat": repeat_value,
            "results_dir": run_dir,
            "status": "pass",
            "elapsed_seconds": seconds,
            "artifacts": module.AutoRunArtifacts(host_lcov, kcov_lcov, userland_log, cluster_log, userland_lcov),
            "external_log_ranges": external_log_ranges,
            "run_config": run_config,
        }
        if runtime_delta_value is not None:
            kwargs["runtime_delta_percent_value"] = runtime_delta_value
        module.write_iteration_manifest(run_dir / "run-all-iteration.json", **kwargs)


def mutate_json_file(path: Path, mutator) -> None:
    data = json.loads(path.read_text())
    mutator(data)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


@contextlib.contextmanager
def temporary_module_root(module, root: Path):
    old_root = module.ROOT
    module.ROOT = root
    try:
        yield
    finally:
        module.ROOT = old_root


def test_verify_run_all_results_accepts_clean_repeat_artifacts(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        fake_root = tmp_path / "repo-root"
        write_clean_run_all_manifest_set(module, tmp_path, [10.0, 10.1], external_root=fake_root)

        stdout = io.StringIO()
        with temporary_module_root(module, fake_root):
            with contextlib.redirect_stdout(stdout):
                rc = module.main(
                    [
                        "--verify-run-all-results",
                        str(tmp_path),
                        "--verify-repeat",
                        "2",
                        "--max-run-time-delta-pct",
                        "2",
                        "--json",
                    ]
                )

    assert_equal(rc, 0, "verify clean repeat return code")
    data = json.loads(stdout.getvalue())
    assert_equal(data["repeat"], 2, "verify clean repeat count")
    assert_equal(data["runtime_delta_percent"], 0.9999999999999964, "verify clean runtime delta")


def test_verify_run_all_results_rejects_stale_manifest_runtime_delta(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        write_clean_run_all_manifest_set(module, tmp_path, [10.0, 10.1], runtime_delta_overrides={2: 0.0})

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "2"])

    assert_equal(rc, 1, "verify stale manifest runtime delta return code")
    message = stderr.getvalue()
    for expected in ("runtime_delta_percent", "does not match cumulative elapsed delta"):
        if expected not in message:
            raise AssertionError(f"missing stale runtime delta diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_rejects_missing_manifest_runtime_delta(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        write_clean_run_all_manifest_set(module, tmp_path, [10.0, 10.1], runtime_delta_overrides={2: None})

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "2"])

    assert_equal(rc, 1, "verify missing manifest runtime delta return code")
    message = stderr.getvalue()
    for expected in ("runtime_delta_percent", "must be numeric"):
        if expected not in message:
            raise AssertionError(f"missing missing runtime delta diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_rejects_nonfinite_manifest_numbers(module) -> None:
    cases = [
        (
            "elapsed_seconds",
            lambda data: data.__setitem__("elapsed_seconds", float("nan")),
            "manifest field 'elapsed_seconds' must be finite",
        ),
        (
            "runtime_delta_percent",
            lambda data: data.__setitem__("runtime_delta_percent", float("inf")),
            "manifest field 'runtime_delta_percent' must be finite",
        ),
        (
            "run_config.max_run_time_delta_pct",
            lambda data: data["run_config"].__setitem__("max_run_time_delta_pct", float("nan")),
            "run_config.max_run_time_delta_pct must be finite",
        ),
    ]
    for label, mutator, expected in cases:
        with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
            tmp_path = Path(tmp_dir)
            write_clean_run_all_manifest_set(module, tmp_path, [10.0])
            mutate_json_file(tmp_path / "run-0001" / "run-all-iteration.json", mutator)

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

        assert_equal(rc, 1, f"verify non-finite manifest {label} return code")
        if expected not in stderr.getvalue():
            raise AssertionError(f"missing non-finite manifest diagnostic {expected!r} in {stderr.getvalue()!r}")


def test_verify_run_all_results_rejects_mixed_manifest_layout(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        single_dir = tmp_path / "single"
        single_dir.mkdir(parents=True, exist_ok=True)
        write_clean_run_all_manifest_set(module, single_dir, [10.0])
        (single_dir / "run-0001" / "run-all-iteration.json").replace(single_dir / "run-all-iteration.json")

        repeat_dir = tmp_path / "repeat-source"
        write_clean_run_all_manifest_set(module, repeat_dir, [10.0])
        mixed_run_dir = single_dir / "run-0001"
        mixed_run_dir.mkdir(parents=True, exist_ok=True)
        (repeat_dir / "run-0001" / "run-all-iteration.json").replace(mixed_run_dir / "run-all-iteration.json")

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = module.main(["--verify-run-all-results", str(single_dir), "--verify-repeat", "1"])

    assert_equal(rc, 1, "verify mixed manifest layout return code")
    message = stderr.getvalue()
    for expected in ("mixed single-run and repeated run-all manifests", "run-all-iteration.json", "run-0001"):
        if expected not in message:
            raise AssertionError(f"missing mixed manifest layout diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_rejects_mismatched_manifest_path_index(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        write_clean_run_all_manifest_set(module, tmp_path, [10.0])
        wrong_dir = tmp_path / "run-0002"
        wrong_dir.mkdir(parents=True, exist_ok=True)
        (tmp_path / "run-0001" / "run-all-iteration.json").replace(wrong_dir / "run-all-iteration.json")

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

    assert_equal(rc, 1, "verify mismatched manifest path return code")
    message = stderr.getvalue()
    for expected in ("repeated run manifest path must be run-0001", "index 1"):
        if expected not in message:
            raise AssertionError(f"missing mismatched manifest path diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_rejects_cross_iteration_artifact_path(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        write_clean_run_all_manifest_set(module, tmp_path, [10.0, 10.1])
        mutate_json_file(
            tmp_path / "run-0002" / "run-all-iteration.json",
            lambda data: data["artifacts"].__setitem__("host_lcov", str(tmp_path / "run-0001" / "host.info")),
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "2"])

    assert_equal(rc, 1, "verify cross-iteration artifact path return code")
    message = stderr.getvalue()
    for expected in ("artifact 'host_lcov' must be inside manifest result directory", "run-0002", "run-0001"):
        if expected not in message:
            raise AssertionError(f"missing cross-iteration artifact path diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_rejects_mismatched_manifest_results_dir(module) -> None:
    cases = [
        (
            "wrong path",
            lambda data, tmp_path: data.__setitem__("results_dir", str(tmp_path / "run-0001-other")),
            "manifest results_dir must match manifest directory",
        ),
        (
            "non-string",
            lambda data, _tmp_path: data.__setitem__("results_dir", True),
            "manifest field 'results_dir' must be a non-empty string",
        ),
    ]
    for label, mutator, expected in cases:
        with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
            tmp_path = Path(tmp_dir)
            write_clean_run_all_manifest_set(module, tmp_path, [10.0])
            mutate_json_file(
                tmp_path / "run-0001" / "run-all-iteration.json",
                lambda data: mutator(data, tmp_path),
            )

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

        assert_equal(rc, 1, f"verify mismatched manifest results_dir {label} return code")
        if expected not in stderr.getvalue():
            raise AssertionError(f"missing mismatched results_dir diagnostic {expected!r} in {stderr.getvalue()!r}")


def test_verify_run_all_results_rejects_non_full_userland_mode(module) -> None:
    cases = [
        (
            "quick scale",
            lambda data: data["run_config"].__setitem__("userland_scale", "quick"),
            "run_config.userland_scale must be 'full'",
        ),
        (
            "live rootfs sync enabled",
            lambda data: data["run_config"].__setitem__("userland_sync_rootfs", True),
            "run_config.userland_sync_rootfs must be false",
        ),
    ]
    for label, mutator, expected in cases:
        with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
            tmp_path = Path(tmp_dir)
            write_clean_run_all_manifest_set(module, tmp_path, [10.0])
            mutate_json_file(tmp_path / "run-0001" / "run-all-iteration.json", mutator)

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

        assert_equal(rc, 1, f"verify non-full userland mode {label} return code")
        if expected not in stderr.getvalue():
            raise AssertionError(f"missing non-full userland mode diagnostic {expected!r} in {stderr.getvalue()!r}")


def test_verify_run_all_results_rejects_wrong_userland_host_or_config(module) -> None:
    cases = [
        (
            "wrong host",
            lambda data: data["run_config"].__setitem__("userland_host", "vm1"),
            "run_config.userland_host must be 'vm0'",
        ),
        (
            "wrong config",
            lambda data: data["run_config"].__setitem__("userland_cluster_config", "configs/node.json"),
            "run_config.userland_cluster_config must resolve to",
        ),
        (
            "non-string config",
            lambda data: data["run_config"].__setitem__("userland_cluster_config", False),
            "run_config.userland_cluster_config must be a non-empty string",
        ),
    ]
    for label, mutator, expected in cases:
        with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
            tmp_path = Path(tmp_dir)
            write_clean_run_all_manifest_set(module, tmp_path, [10.0])
            mutate_json_file(tmp_path / "run-0001" / "run-all-iteration.json", mutator)

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

        assert_equal(rc, 1, f"verify wrong userland host/config {label} return code")
        if expected not in stderr.getvalue():
            raise AssertionError(f"missing wrong userland host/config diagnostic {expected!r} in {stderr.getvalue()!r}")


def test_verify_run_all_results_rejects_partial_run_config(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        run_dir = tmp_path / "run-0001"
        run_dir.mkdir(parents=True, exist_ok=True)
        host_lcov = run_dir / "host.info"
        kcov_lcov = run_dir / "kcov.info"
        userland_log = run_dir / "userland.log"
        userland_lcov = run_dir / "wos-userland.info"
        write_lcov(host_lcov, {"/repo/host.cpp": {1: 1}})
        write_lcov(kcov_lcov, {"/repo/kern.cpp": {1: 1}})
        write_wos_userland_lcov(userland_lcov)
        userland_log.write_text(complete_testd_log(module) + complete_userland_log(module))
        external_log = run_dir / "external.log"
        external_log.write_text("external proof log healthy\n")
        run_config = module.full_run_all_config_json()
        run_config["expected_external_logs"] = [str(external_log)]
        run_config["skip_ktest"] = True
        run_config["userland_launch_no_setup"] = False
        run_config["userland_auto_shutdown"] = False
        run_config["userland_shutdown_action"] = None
        run_config["max_run_time_delta_pct"] = 5.0
        module.write_iteration_manifest(
            run_dir / "run-all-iteration.json",
            index=1,
            repeat=1,
            results_dir=run_dir,
            status="pass",
            elapsed_seconds=10.0,
            artifacts=module.AutoRunArtifacts(
                host_lcov=host_lcov,
                kcov_lcov=kcov_lcov,
                userland_log=userland_log,
                userland_lcov=userland_lcov,
            ),
            external_log_ranges=[module.LogRange(external_log, 0, external_log.stat().st_size)],
            run_config=run_config,
            runtime_delta_percent_value=0.0,
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

    assert_equal(rc, 1, "verify partial run config return code")
    message = stderr.getvalue()
    for expected in (
        "run_config.skip_ktest must be false",
        "run_config.userland_launch_no_setup must be true",
        "run_config.userland_auto_shutdown must be true",
        "run_config.userland_shutdown_action must be 'poweroff'",
        "run_config.max_run_time_delta_pct=5.00 is looser",
    ):
        if expected not in message:
            raise AssertionError(f"missing partial run config diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_rejects_shortcut_run_config(module) -> None:
    cases = [
        (
            "ktest fast",
            lambda run_config: run_config.__setitem__("ktest_fast", True),
            "run_config.ktest_fast must be false",
        ),
        (
            "ktest no build",
            lambda run_config: run_config.__setitem__("ktest_no_build", True),
            "run_config.ktest_no_build must be false",
        ),
        (
            "userland no build",
            lambda run_config: run_config.__setitem__("userland_no_build", True),
            "run_config.userland_no_build must be false",
        ),
        (
            "ktest tcg",
            lambda run_config: run_config.__setitem__("ktest_tcg", ""),
            "run_config.ktest_tcg must be null",
        ),
        (
            "userland tcg",
            lambda run_config: run_config.__setitem__("userland_tcg", "1"),
            "run_config.userland_tcg must be null",
        ),
        (
            "filtered sync",
            lambda run_config: run_config.__setitem__(
                "userland_sync_filter",
                "configs/drive/srv/wos_userland_suite.sh",
            ),
            "run_config.userland_sync_filter must be null",
        ),
        (
            "ktest arg",
            lambda run_config: run_config.__setitem__("ktest_arg", ["--fast"]),
            "run_config.ktest_arg must be an empty list",
        ),
        (
            "missing ktest cov arg",
            lambda run_config: run_config.pop("ktest_cov_arg"),
            "run_config.ktest_cov_arg must be an empty list",
        ),
        (
            "userland env",
            lambda run_config: run_config.__setitem__("userland_env", ["WOS_SUITE_RUN_PERF_TRACE=0"]),
            "run_config.userland_env must be an empty list",
        ),
        (
            "userland arg",
            lambda run_config: run_config.__setitem__("userland_arg", ["--remote-script", "/tmp/suite"]),
            "run_config.userland_arg must be an empty list",
        ),
        (
            "host timeout too loose",
            lambda run_config: run_config.__setitem__("host_timeout", module.DEFAULT_HOST_TIMEOUT_SECONDS + 1),
            "run_config.host_timeout=1801.00 exceeds default bound 1800.00",
        ),
        (
            "missing ktest timeout",
            lambda run_config: run_config.pop("ktest_timeout"),
            "run_config.ktest_timeout must be numeric",
        ),
        (
            "disabled sync timeout",
            lambda run_config: run_config.__setitem__("userland_sync_timeout", 0),
            "run_config.userland_sync_timeout must be positive",
        ),
        (
            "nonfinite wrapper probe timeout",
            lambda run_config: run_config.__setitem__("userland_wrapper_probe_timeout", float("inf")),
            "run_config.userland_wrapper_probe_timeout must be finite",
        ),
        (
            "coverage render width",
            lambda run_config: run_config.__setitem__("userland_coverage_render_width", 800),
            "run_config.userland_coverage_render_width=800.00 must equal default",
        ),
        (
            "extra vm log",
            lambda run_config: run_config["expected_external_logs"].append(str(tmp_path / "serial-vm4.log")),
            "run_config.expected_external_logs contains unsupported userland VM log",
        ),
    ]
    for label, mutator, expected in cases:
        with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
            tmp_path = Path(tmp_dir)
            fake_root = tmp_path / "fake-root"
            write_clean_run_all_manifest_set(module, tmp_path, [10.0], external_root=fake_root)
            (tmp_path / "serial-vm4.log").write_text("extra vm healthy\n")
            mutate_json_file(
                tmp_path / "run-0001" / "run-all-iteration.json",
                lambda data: mutator(data["run_config"]),
            )

            stderr = io.StringIO()
            with temporary_module_root(module, fake_root):
                with contextlib.redirect_stderr(stderr):
                    rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

        assert_equal(rc, 1, f"verify shortcut run config {label} return code")
        if expected not in stderr.getvalue():
            raise AssertionError(f"missing shortcut run config diagnostic {expected!r} in {stderr.getvalue()!r}")


def test_verify_run_all_results_rejects_missing_expected_external_log_range(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        run_dir = tmp_path / "run-0001"
        run_dir.mkdir(parents=True, exist_ok=True)
        host_lcov = run_dir / "host.info"
        kcov_lcov = run_dir / "kcov.info"
        userland_log = run_dir / "userland.log"
        userland_lcov = run_dir / "wos-userland.info"
        external_log = run_dir / "external.log"
        write_lcov(host_lcov, {"/repo/host.cpp": {1: 1}})
        write_lcov(kcov_lcov, {"/repo/kern.cpp": {1: 1}})
        write_wos_userland_lcov(userland_lcov)
        userland_log.write_text(complete_testd_log(module) + complete_userland_log(module))
        external_log.write_text("external proof log healthy\n")
        run_config = module.full_run_all_config_json()
        run_config["expected_external_logs"] = [str(external_log)]
        module.write_iteration_manifest(
            run_dir / "run-all-iteration.json",
            index=1,
            repeat=1,
            results_dir=run_dir,
            status="pass",
            elapsed_seconds=10.0,
            artifacts=module.AutoRunArtifacts(
                host_lcov=host_lcov,
                kcov_lcov=kcov_lcov,
                userland_log=userland_log,
                userland_lcov=userland_lcov,
            ),
            external_log_ranges=[],
            run_config=run_config,
            runtime_delta_percent_value=0.0,
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

    assert_equal(rc, 1, "verify missing expected external log range return code")
    message = stderr.getvalue()
    for expected in ("missing external_log_ranges entry", "external.log"):
        if expected not in message:
            raise AssertionError(f"missing external log evidence diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_rejects_empty_expected_external_log_range(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        write_clean_run_all_manifest_set(module, tmp_path, [10.0])
        mutate_json_file(
            tmp_path / "run-0001" / "run-all-iteration.json",
            lambda data: data["external_log_ranges"][0].__setitem__(
                "end_offset",
                data["external_log_ranges"][0]["start_offset"],
            ),
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

    assert_equal(rc, 1, "verify empty expected external log range return code")
    message = stderr.getvalue()
    for expected in ("captured no current-run bytes", "serial-vm0.log"):
        if expected not in message:
            raise AssertionError(f"missing empty external log range diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_accepts_quiet_qemu_external_log_ranges(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        fake_root = tmp_path / "repo-root"
        write_clean_run_all_manifest_set(module, tmp_path, [10.0], external_root=fake_root)

        def quiet_qemu_ranges(data) -> None:
            for item in data["external_log_ranges"]:
                if module.external_log_range_may_be_empty(Path(item["path"])):
                    item["end_offset"] = item["start_offset"]

        mutate_json_file(
            tmp_path / "run-0001" / "run-all-iteration.json",
            quiet_qemu_ranges,
        )

        stdout = io.StringIO()
        with temporary_module_root(module, fake_root):
            with contextlib.redirect_stdout(stdout):
                rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1", "--json"])

    assert_equal(rc, 0, "verify quiet qemu external log range return code")


def test_verify_run_all_results_rejects_duplicate_external_log_evidence(module) -> None:
    cases = [
        (
            "expected logs",
            lambda data: data["run_config"]["expected_external_logs"].append(
                data["run_config"]["expected_external_logs"][0]
            ),
            "duplicates an earlier log",
        ),
        (
            "range paths",
            lambda data: data["external_log_ranges"].append(dict(data["external_log_ranges"][0])),
            "duplicates an earlier external log range",
        ),
    ]
    for label, mutator, expected in cases:
        with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
            tmp_path = Path(tmp_dir)
            write_clean_run_all_manifest_set(module, tmp_path, [10.0])
            mutate_json_file(tmp_path / "run-0001" / "run-all-iteration.json", mutator)

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

        assert_equal(rc, 1, f"verify duplicate external log evidence {label} return code")
        if expected not in stderr.getvalue():
            raise AssertionError(f"missing duplicate external log evidence diagnostic {expected!r} in {stderr.getvalue()!r}")


def test_verify_run_all_results_rejects_missing_required_full_external_log_label(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        write_clean_run_all_manifest_set(module, tmp_path, [10.0])
        mutate_json_file(
            tmp_path / "run-0001" / "run-all-iteration.json",
            lambda data: data["run_config"].__setitem__(
                "expected_external_logs",
                [
                    path
                    for path in data["run_config"]["expected_external_logs"]
                    if not path.endswith("qemu-vm3.log")
                ],
            ),
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

    assert_equal(rc, 1, "verify missing required full external log label return code")
    message = stderr.getvalue()
    for expected in ("missing required full-run log", "userland/qemu-vm3.log"):
        if expected not in message:
            raise AssertionError(f"missing required full external log diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_rejects_wrong_required_external_log_path(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        fake_root = tmp_path / "repo-root"
        write_clean_run_all_manifest_set(module, tmp_path, [10.0], external_root=fake_root)
        wrong_log = tmp_path / "run-0001" / "serial-vm0.log"
        wrong_log.write_text("wrong directory serial log\n")
        mutate_json_file(
            tmp_path / "run-0001" / "run-all-iteration.json",
            lambda data: data["run_config"]["expected_external_logs"].__setitem__(0, str(wrong_log)),
        )

        stderr = io.StringIO()
        with temporary_module_root(module, fake_root):
            with contextlib.redirect_stderr(stderr):
                rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

    assert_equal(rc, 1, "verify wrong required external log path return code")
    message = stderr.getvalue()
    for expected in ("entry for userland/serial-vm0.log must be", "repo-root/serial-vm0.log"):
        if expected not in message:
            raise AssertionError(f"missing wrong required external log path diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_rejects_health_marker(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        run_dir = tmp_path / "run-0001"
        run_dir.mkdir(parents=True, exist_ok=True)
        host_lcov = run_dir / "host.info"
        kcov_lcov = run_dir / "kcov.info"
        userland_log = run_dir / "userland.log"
        cluster_log = run_dir / "cluster.log"
        userland_lcov = run_dir / "wos-userland.info"
        write_lcov(host_lcov, {"/repo/host.cpp": {1: 1}})
        write_lcov(kcov_lcov, {"/repo/kern.cpp": {1: 1}})
        write_wos_userland_lcov(userland_lcov)
        userland_log.write_text(complete_testd_log(module) + complete_userland_log(module))
        cluster_log.write_text(
            "ERROR: synthetic cluster launch warning escaped status checks\n"
            "kernel: possible deadlock detected in scheduler\n"
            "ERROR synthetic top-level error without colon\n"
            "FAILED synthetic top-level failure without colon\n"
            "FAIL synthetic_case rc=1 (3s)\n"
        )
        (run_dir / "host-ctest.log").write_text("runtime error: synthetic undefined behavior\n")
        (run_dir / "host-build.log").write_text("ctest worker terminated by signal SIGABRT\n")
        (run_dir / "ktest-run.log").write_text("TIMEOUT after 3600s\n")
        (run_dir / "host-export.log").write_text("EXIT status 2\n")
        external_log = run_dir / "external.log"
        external_log.write_text("external proof log healthy\n")
        run_config = module.full_run_all_config_json()
        run_config["expected_external_logs"] = [str(external_log)]
        module.write_iteration_manifest(
            run_dir / "run-all-iteration.json",
            index=1,
            repeat=1,
            results_dir=run_dir,
            status="pass",
            elapsed_seconds=10.0,
            artifacts=module.AutoRunArtifacts(
                host_lcov=host_lcov,
                kcov_lcov=kcov_lcov,
                userland_log=userland_log,
                cluster_log=cluster_log,
                userland_lcov=userland_lcov,
            ),
            external_log_ranges=[module.LogRange(external_log, 0, external_log.stat().st_size)],
            run_config=run_config,
            runtime_delta_percent_value=0.0,
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

    assert_equal(rc, 1, "verify health marker return code")
    message = stderr.getvalue()
    for expected in (
        "run-all results verification failed",
        "host-ctest.log:1",
        "sanitizer runtime error",
        "cluster.log:1",
        "tool error line",
        "cluster.log:2",
        "deadlock or hung task",
        "cluster.log:3",
        "top-level error line",
        "cluster.log:4",
        "top-level failed line",
        "cluster.log:5",
        "suite failed case line",
        "host-build.log:1",
        "signal crash",
        "ktest-run.log:1",
        "tool timeout",
        "host-export.log:1",
        "nonzero command exit",
    ):
        if expected not in message:
            raise AssertionError(f"missing verify health diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_rejects_malformed_core_artifact(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        run_dir = tmp_path / "run-0001"
        run_dir.mkdir(parents=True, exist_ok=True)
        host_lcov = run_dir / "host.info"
        kcov_lcov = run_dir / "kcov.info"
        userland_log = run_dir / "userland.log"
        userland_lcov = run_dir / "wos-userland.info"
        host_lcov.write_text("TN:host without records\n")
        write_lcov(kcov_lcov, {"/repo/kern.cpp": {1: 1}})
        write_wos_userland_lcov(userland_lcov)
        userland_log.write_text(complete_testd_log(module) + complete_userland_log(module))
        external_log = run_dir / "external.log"
        external_log.write_text("external proof log healthy\n")
        run_config = module.full_run_all_config_json()
        run_config["expected_external_logs"] = [str(external_log)]
        module.write_iteration_manifest(
            run_dir / "run-all-iteration.json",
            index=1,
            repeat=1,
            results_dir=run_dir,
            status="pass",
            elapsed_seconds=10.0,
            artifacts=module.AutoRunArtifacts(
                host_lcov=host_lcov,
                kcov_lcov=kcov_lcov,
                userland_log=userland_log,
                userland_lcov=userland_lcov,
            ),
            external_log_ranges=[module.LogRange(external_log, 0, external_log.stat().st_size)],
            run_config=run_config,
            runtime_delta_percent_value=0.0,
        )

        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

    assert_equal(rc, 1, "verify malformed core artifact return code")
    message = stderr.getvalue()
    for expected in ("run-all results verification failed", "host_lcov", "failed LCOV parse", "no LCOV source-file records"):
        if expected not in message:
            raise AssertionError(f"missing malformed artifact diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_rejects_missing_no_setup_launch_evidence(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        fake_root = tmp_path / "repo-root"
        write_clean_run_all_manifest_set(module, tmp_path, [10.0], external_root=fake_root)
        (tmp_path / "run-0001" / "cluster.log").write_text("cluster ready without command proof\n")

        stderr = io.StringIO()
        with temporary_module_root(module, fake_root):
            with contextlib.redirect_stderr(stderr):
                rc = module.main(["--verify-run-all-results", str(tmp_path), "--verify-repeat", "1"])

    assert_equal(rc, 1, "verify missing no-setup launch evidence return code")
    message = stderr.getvalue()
    for expected in ("run-all results verification failed", "cluster_log", "wos-cluster --launch --no-setup"):
        if expected not in message:
            raise AssertionError(f"missing no-setup launch evidence diagnostic {expected!r} in {message!r}")


def test_verify_run_all_results_scans_recorded_external_log_ranges(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)

        clean_results = tmp_path / "clean-results"
        clean_run = clean_results / "run-0001"
        clean_run.mkdir(parents=True, exist_ok=True)
        clean_host = clean_run / "host.info"
        clean_kcov = clean_run / "kcov.info"
        clean_userland = clean_run / "userland.log"
        clean_cluster = clean_run / "cluster.log"
        clean_userland_lcov = clean_run / "wos-userland.info"
        clean_external = tmp_path / "serial-clean.log"
        write_lcov(clean_host, {"/repo/host.cpp": {1: 1}})
        write_lcov(clean_kcov, {"/repo/kern.cpp": {1: 1}})
        write_wos_userland_lcov(clean_userland_lcov)
        clean_userland.write_text(complete_testd_log(module) + complete_userland_log(module))
        clean_cluster.write_text("$ /repo/bin/wos-cluster --launch --no-setup --config configs/cluster.json\ncluster ready\n")
        stale_prefix = "kernel panic: stale before recorded range\n"
        recorded = "current run stayed healthy\n"
        clean_external.write_text(stale_prefix + recorded + "kernel panic: appended after recorded range\n")
        clean_start = len(stale_prefix.encode())
        clean_end = clean_start + len(recorded.encode())
        clean_config = module.full_run_all_config_json()
        clean_fake_root = tmp_path / "clean-root"
        clean_expected_logs, clean_ranges = write_full_external_log_evidence(
            module,
            clean_run,
            module.LogRange(clean_external, clean_start, clean_end),
            external_root=clean_fake_root,
        )
        clean_config["expected_external_logs"] = clean_expected_logs
        module.write_iteration_manifest(
            clean_run / "run-all-iteration.json",
            index=1,
            repeat=1,
            results_dir=clean_run,
            status="pass",
            elapsed_seconds=10.0,
            artifacts=module.AutoRunArtifacts(
                host_lcov=clean_host,
                kcov_lcov=clean_kcov,
                userland_log=clean_userland,
                cluster_log=clean_cluster,
                userland_lcov=clean_userland_lcov,
            ),
            external_log_ranges=clean_ranges,
            run_config=clean_config,
            runtime_delta_percent_value=0.0,
        )

        bad_results = tmp_path / "bad-results"
        bad_run = bad_results / "run-0001"
        bad_run.mkdir(parents=True, exist_ok=True)
        bad_host = bad_run / "host.info"
        bad_kcov = bad_run / "kcov.info"
        bad_userland = bad_run / "userland.log"
        bad_cluster = bad_run / "cluster.log"
        bad_userland_lcov = bad_run / "wos-userland.info"
        bad_external = tmp_path / "serial-bad.log"
        write_lcov(bad_host, {"/repo/host.cpp": {1: 1}})
        write_lcov(bad_kcov, {"/repo/kern.cpp": {1: 1}})
        write_wos_userland_lcov(bad_userland_lcov)
        bad_userland.write_text(complete_testd_log(module) + complete_userland_log(module))
        bad_cluster.write_text("$ /repo/bin/wos-cluster --launch --no-setup --config configs/cluster.json\ncluster ready\n")
        bad_recorded = "kernel panic: recorded current-run marker\n"
        bad_external.write_text(stale_prefix + bad_recorded + "kernel panic: later appended marker\n")
        bad_start = len(stale_prefix.encode())
        bad_end = bad_start + len(bad_recorded.encode())
        bad_config = module.full_run_all_config_json()
        bad_fake_root = tmp_path / "bad-root"
        bad_expected_logs, bad_ranges = write_full_external_log_evidence(
            module,
            bad_run,
            module.LogRange(bad_external, bad_start, bad_end),
            external_root=bad_fake_root,
        )
        bad_config["expected_external_logs"] = bad_expected_logs
        module.write_iteration_manifest(
            bad_run / "run-all-iteration.json",
            index=1,
            repeat=1,
            results_dir=bad_run,
            status="pass",
            elapsed_seconds=10.0,
            artifacts=module.AutoRunArtifacts(
                host_lcov=bad_host,
                kcov_lcov=bad_kcov,
                userland_log=bad_userland,
                cluster_log=bad_cluster,
                userland_lcov=bad_userland_lcov,
            ),
            external_log_ranges=bad_ranges,
            run_config=bad_config,
            runtime_delta_percent_value=0.0,
        )

        clean_stdout = io.StringIO()
        with temporary_module_root(module, clean_fake_root):
            with contextlib.redirect_stdout(clean_stdout):
                clean_rc = module.main(["--verify-run-all-results", str(clean_results), "--verify-repeat", "1", "--json"])

        stderr = io.StringIO()
        with temporary_module_root(module, bad_fake_root):
            with contextlib.redirect_stderr(stderr):
                bad_rc = module.main(["--verify-run-all-results", str(bad_results), "--verify-repeat", "1"])

    assert_equal(clean_rc, 0, "verify should ignore external markers outside recorded range")
    assert_equal(bad_rc, 1, "verify should reject external markers inside recorded range")
    message = stderr.getvalue()
    for expected in ("serial-bad.log", "recorded current-run marker", "kernel panic"):
        if expected not in message:
            raise AssertionError(f"missing recorded external range diagnostic {expected!r} in {message!r}")
    for unexpected in ("stale before recorded range", "later appended marker"):
        if unexpected in message:
            raise AssertionError(f"external range verifier scanned outside recorded range: {message!r}")


def test_wait_for_wos_ssh_reports_guest_network_failure(module) -> None:
    class FakeResult:
        returncode = 255

    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        (tmp_path / "serial-vm0.log").write_text(
            "[25.460] error netd: netd: no DHCP offer received, exiting\n"
            "[25.490] critical init: init[1]: network startup failed; stopping init\n"
        )
        old_root = module.ROOT
        old_run = module.subprocess.run
        module.ROOT = tmp_path
        module.subprocess.run = lambda *_args, **_kwargs: FakeResult()
        try:
            try:
                module.wait_for_wos_ssh("vm0", 30)
            except module.CoverageInputError as exc:
                message = str(exc)
            else:
                raise AssertionError("wait_for_wos_ssh did not reject guest network failure")
        finally:
            module.ROOT = old_root
            module.subprocess.run = old_run

    if "not providing DHCP/host resolution" not in message:
        raise AssertionError(f"missing topology hint in network failure: {message!r}")


def test_wait_for_wos_ssh_bounds_each_probe(module) -> None:
    class FakeSuccess:
        returncode = 0

    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        calls: list[float | None] = []
        monotonic_values = iter([0.0, 0.0, 0.0, 1.0, 1.0])

        old_root = module.ROOT
        old_run = module.subprocess.run
        old_monotonic = module.time.monotonic
        old_sleep = module.time.sleep

        def fake_run(cmd, **kwargs):
            calls.append(kwargs.get("timeout"))
            if len(calls) == 1:
                raise subprocess.TimeoutExpired(cmd, kwargs.get("timeout"))
            return FakeSuccess()

        module.ROOT = Path(tmp_dir)
        module.subprocess.run = fake_run
        module.time.monotonic = lambda: next(monotonic_values)
        module.time.sleep = lambda _seconds: None
        try:
            module.wait_for_wos_ssh("vm0", 30)
        finally:
            module.ROOT = old_root
            module.subprocess.run = old_run
            module.time.monotonic = old_monotonic
            module.time.sleep = old_sleep

    assert_equal(calls, [module.SSH_PROBE_TIMEOUT_SECONDS, module.SSH_PROBE_TIMEOUT_SECONDS], "SSH probe timeout propagation")


def test_wait_for_cluster_ready_reports_launch_log_tail(module) -> None:
    class FakeProc:
        def poll(self):
            return 1

    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        log_path = Path(tmp_dir) / "cluster-launch.log"
        log_path.write_text(
            "=== Launching VMs ===\n"
            "ERROR: --no-setup requires an existing configured topology\n"
            "  - missing bridge wos-lan-br for zone lan\n"
        )
        try:
            module.wait_for_cluster_ready(FakeProc(), module.threading.Event(), 30, log_path)
        except module.CoverageInputError as exc:
            message = str(exc)
        else:
            raise AssertionError("cluster launch failure was accepted")

    for expected in (
        "wos-cluster exited before launch completed",
        "cluster log tail:",
        "--no-setup requires an existing configured topology",
        "missing bridge wos-lan-br",
    ):
        if expected not in message:
            raise AssertionError(f"missing cluster launch diagnostic {expected!r} in {message!r}")


def test_ktest_run_uses_preconfigured_topology(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        commands: list[list[str]] = []
        old_run_checked = module.run_checked

        def fake_run_checked(cmd, **_kwargs):
            commands.append(cmd)
            if cmd and Path(cmd[0]).name == "wos-ktest-cov":
                output = Path(cmd[cmd.index("--lcov") + 1])
                write_lcov(output, {"/repo/kern.cpp": {1: 1}})

        module.run_checked = fake_run_checked
        try:
            args = module.argparse.Namespace(
                ktest_setup=False,
                ktest_fast=False,
                ktest_no_build=True,
                ktest_no_package=True,
                ktest_reset_sysroot=False,
                ktest_tcg=None,
                ktest_arg=[],
                ktest_timeout=10,
                ktest_keep_topology=False,
                ktest_cov_arg=[],
                ktest_cov_timeout=10,
            )
            module.run_ktest_kcov(args, tmp_path)
        finally:
            module.run_checked = old_run_checked

    ktest_cmds = [cmd for cmd in commands if cmd and Path(cmd[0]).name == "wos-ktest"]
    if not ktest_cmds:
        raise AssertionError(f"run_ktest_kcov did not invoke wos-ktest: {commands!r}")
    if "--no-setup" not in ktest_cmds[0]:
        raise AssertionError(f"run_ktest_kcov did not request no-setup KTEST launch: {ktest_cmds[0]!r}")
    teardown_cmds = [cmd for cmd in ktest_cmds if "--teardown" in cmd]
    assert_equal(teardown_cmds, [], "default KTEST coverage should not tear down preconfigured topology")


def test_auto_userland_run_requests_guest_poweroff(module) -> None:
    class FakeProc:
        def __init__(self, events: list[str]) -> None:
            self.events = events
            self.stdout = io.StringIO("Press Ctrl+C to stop all VMs.\n")
            self.returncode = None
            self.wait_timeout = None
            self.terminated = False
            self.killed = False

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            self.events.append("wait-cluster")
            self.wait_timeout = timeout
            self.returncode = 0
            return 0

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True
            self.returncode = -9

    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        events: list[str] = []
        fake_proc = FakeProc(events)
        commands: list[list[str]] = []
        popen_args: list[str] = []
        popen_kwargs = {}
        cluster_config = tmp_path / "cluster.json"
        cluster_config.write_text(json.dumps({"zones": [{"id": "GLOBAL"}, {"id": 0, "nodes": 4}, {"id": 1, "nodes": 4}]}))

        old_popen = module.subprocess.Popen
        old_run_checked = module.run_checked
        old_wait_for_wos_ssh = module.wait_for_wos_ssh
        run_checked_timeouts: list[int | None] = []

        def fake_popen(*_args, **_kwargs):
            popen_args[:] = list(_args[0])
            popen_kwargs.update(_kwargs)
            return fake_proc

        def fake_run_checked(cmd, **_kwargs):
            commands.append(cmd)
            run_checked_timeouts.append(_kwargs.get("timeout"))
            if not cmd:
                return
            command_name = Path(cmd[0]).name
            if command_name == "wos-userland-suite":
                events.append("suite")
                output = Path(cmd[cmd.index("--output") + 1])
                output.write_text(complete_testd_log(module) + complete_userland_log(module))
            elif command_name == "wos-cluster" and "--sync" in cmd:
                events.append("sync")
            elif command_name == "wos_ssh.sh" and cmd[-1] == "/sbin/poweroff -f":
                events.append(f"poweroff:{cmd[1]}")

        module.subprocess.Popen = fake_popen
        module.run_checked = fake_run_checked
        module.wait_for_wos_ssh = lambda _host, _timeout: None
        try:
            args = module.argparse.Namespace(
                userland_existing_cluster=False,
                userland_no_build=True,
                userland_no_lcov=True,
                userland_cluster_config=str(cluster_config),
                userland_tcg=None,
                cluster_ready_timeout=5,
                userland_ssh_timeout=5,
                userland_no_sync=False,
                userland_sync_rootfs=False,
                userland_sync_timeout=19,
                userland_sync_filter=None,
                userland_host="vm0",
                userland_scale="quick",
                userland_env=[],
                userland_arg=[],
                userland_timeout=10,
                userland_shutdown_timeout=17,
                allow_userland_skips=False,
            )
            userland_log, _cluster_log, userland_lcov = module.run_userland_suite_coverage(args, tmp_path)
            userland_log_exists = userland_log.exists()
            cluster_log_text = (tmp_path / "cluster-launch.log").read_text()
        finally:
            module.subprocess.Popen = old_popen
            module.run_checked = old_run_checked
            module.wait_for_wos_ssh = old_wait_for_wos_ssh

    suite_cmd = next(cmd for cmd in commands if cmd and Path(cmd[0]).name == "wos-userland-suite")
    assert_equal(
        popen_args[:3],
        [str(module.ROOT / "bin" / "wos-cluster"), "--launch", "--no-setup"],
        "auto userland cluster launch mode",
    )
    expected = ["--env", "WOS_SUITE_SHUTDOWN=poweroff"]
    if not any(suite_cmd[index : index + 2] == expected for index in range(len(suite_cmd) - 1)):
        raise AssertionError(f"auto userland run did not request guest poweroff: {suite_cmd!r}")
    if "--no-sync" not in suite_cmd:
        raise AssertionError(f"auto userland run did not disable wrapper-managed sync: {suite_cmd!r}")
    expected_netbench_timeout = [
        "--env",
        f"WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS={module.DEFAULT_USERLAND_NETBENCH_CASE_TIMEOUT_SECONDS}",
    ]
    if not any(suite_cmd[index : index + 2] == expected_netbench_timeout for index in range(len(suite_cmd) - 1)):
        raise AssertionError(f"auto userland run did not pass netbench timeout: {suite_cmd!r}")
    expected_suite_timeout = ["--timeout", "10"]
    if not any(suite_cmd[index : index + 2] == expected_suite_timeout for index in range(len(suite_cmd) - 1)):
        raise AssertionError(f"auto userland run did not pass wrapper timeout: {suite_cmd!r}")
    expected_probe_timeout = ["--probe-timeout", str(module.DEFAULT_USERLAND_WRAPPER_PROBE_TIMEOUT_SECONDS)]
    if not any(suite_cmd[index : index + 2] == expected_probe_timeout for index in range(len(suite_cmd) - 1)):
        raise AssertionError(f"auto userland run did not pass wrapper probe timeout: {suite_cmd!r}")
    suite_command_index = commands.index(suite_cmd)
    assert_equal(run_checked_timeouts[suite_command_index], 25, "auto userland wrapper timeout grace")
    sync_cmds = [cmd for cmd in commands if cmd and Path(cmd[0]).name == "wos-cluster" and "--sync" in cmd]
    assert_equal(sync_cmds, [], "auto userland run should not require live rootfs sync")
    teardown_cmds = [cmd for cmd in commands if cmd and Path(cmd[0]).name == "wos-cluster" and "--teardown" in cmd]
    assert_equal(teardown_cmds, [], "auto userland run should not tear down preconfigured topology")
    poweroff_cmds = [cmd for cmd in commands if cmd and Path(cmd[0]).name == "wos_ssh.sh" and cmd[-1] == "/sbin/poweroff -f"]
    assert_equal([cmd[1] for cmd in poweroff_cmds], ["vm1", "vm2", "vm3", "vm0"], "cluster poweroff host order")
    assert_equal(
        [event for event in events if event.startswith("poweroff:")],
        ["poweroff:vm1", "poweroff:vm2", "poweroff:vm3", "poweroff:vm0"],
        "cluster poweroff event order",
    )
    wait_index = events.index("wait-cluster")
    suite_index = events.index("suite")
    first_poweroff_index = events.index("poweroff:vm1")
    if suite_index >= first_poweroff_index:
        raise AssertionError(f"guest poweroff happened before the userland suite: {events!r}")
    last_poweroff_index = events.index("poweroff:vm0")
    if last_poweroff_index >= wait_index:
        raise AssertionError(f"cluster wait happened before all guest poweroff requests: {events!r}")
    assert_equal(fake_proc.wait_timeout, 17, "cluster exit wait timeout")
    if fake_proc.terminated or fake_proc.killed:
        raise AssertionError("auto userland run killed a cluster that exited after guest poweroff")
    if not userland_log_exists:
        raise AssertionError("fake userland run did not produce a log")
    assert_equal(userland_lcov, None, "manifest-only auto userland run should not return LCOV")
    if "--launch --no-setup" not in cluster_log_text:
        raise AssertionError(f"cluster launch log did not record no-setup command: {cluster_log_text!r}")
    if popen_kwargs.get("env", {}).get("PYTHONUNBUFFERED") != "1":
        raise AssertionError(f"cluster launch was not unbuffered: {popen_kwargs!r}")


def test_userland_lcov_run_bounds_renderbench_workload(module) -> None:
    class FakeProc:
        def __init__(self) -> None:
            self.stdout = io.StringIO("Press Ctrl+C to stop all VMs.\n")
            self.returncode = None

        def poll(self):
            return self.returncode

        def wait(self, timeout=None):
            self.returncode = 0
            return 0

        def terminate(self) -> None:
            self.returncode = -15

        def kill(self) -> None:
            self.returncode = -9

    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        commands: list[list[str]] = []
        cluster_config = tmp_path / "cluster.json"
        cluster_config.write_text(json.dumps({"zones": [{"id": "GLOBAL"}, {"id": 0, "nodes": 4}]}))

        old_popen = module.subprocess.Popen
        old_run_checked = module.run_checked
        old_wait_for_wos_ssh = module.wait_for_wos_ssh
        old_prepare = module.prepare_remote_userland_profile_dir
        old_sync = module.sync_userland_cluster_rootfs
        old_fetch = module.fetch_remote_userland_profraws
        old_export = module.export_wos_userland_lcov

        def fake_run_checked(cmd, **_kwargs):
            commands.append(cmd)
            if cmd and Path(cmd[0]).name == "wos-userland-suite":
                output = Path(cmd[cmd.index("--output") + 1])
                output.write_text(complete_testd_log(module) + complete_userland_log(module))

        def fake_export(_args, results_dir, _profraws):
            output = Path(results_dir) / "wos-userland.info"
            write_wos_userland_lcov(output)
            return output

        module.subprocess.Popen = lambda *_args, **_kwargs: FakeProc()
        module.run_checked = fake_run_checked
        module.wait_for_wos_ssh = lambda _host, _timeout: None
        module.prepare_remote_userland_profile_dir = lambda *_args, **_kwargs: None
        module.sync_userland_cluster_rootfs = lambda *_args, **_kwargs: None
        module.fetch_remote_userland_profraws = lambda *_args, **_kwargs: [tmp_path / "one.profraw"]
        module.export_wos_userland_lcov = fake_export
        try:
            args = module.argparse.Namespace(
                userland_existing_cluster=False,
                userland_no_build=True,
                userland_no_lcov=False,
                userland_cluster_config=str(cluster_config),
                userland_tcg=None,
                cluster_ready_timeout=5,
                userland_ssh_timeout=5,
                userland_no_sync=True,
                userland_sync_rootfs=False,
                userland_sync_timeout=19,
                userland_sync_filter=None,
                userland_host="vm0",
                userland_scale="full",
                userland_env=["WOS_SUITE_RENDER_SPP=99"],
                userland_arg=[],
                userland_timeout=10,
                userland_profile_fetch_timeout=11,
                userland_profile_file_fetch_timeout=3,
                userland_shutdown_timeout=17,
                allow_userland_skips=False,
            )
            _userland_log, _cluster_log, userland_lcov = module.run_userland_suite_coverage(args, tmp_path)
        finally:
            module.subprocess.Popen = old_popen
            module.run_checked = old_run_checked
            module.wait_for_wos_ssh = old_wait_for_wos_ssh
            module.prepare_remote_userland_profile_dir = old_prepare
            module.sync_userland_cluster_rootfs = old_sync
            module.fetch_remote_userland_profraws = old_fetch
            module.export_wos_userland_lcov = old_export

    suite_cmd = next(cmd for cmd in commands if cmd and Path(cmd[0]).name == "wos-userland-suite")
    env = command_env_assignments(suite_cmd)
    assert_equal(userland_lcov.name, "wos-userland.info", "LCOV userland run should return exported artifact")
    assert_equal(
        env["LLVM_PROFILE_FILE"],
        f"{module.DEFAULT_WOS_USERLAND_PROFILE_DIR}/%m-%p.profraw%c",
        "LCOV userland profile pattern",
    )
    assert_equal(
        env["WOS_SUITE_NETBENCH_CASE_TIMEOUT_SECONDS"],
        str(module.DEFAULT_USERLAND_NETBENCH_CASE_TIMEOUT_SECONDS),
        "LCOV userland netbench timeout",
    )
    assert_equal(
        env["WOS_SUITE_RENDER_WIDTH"],
        str(module.DEFAULT_USERLAND_COVERAGE_RENDER_WIDTH),
        "LCOV render width",
    )
    assert_equal(
        env["WOS_SUITE_RENDER_HEIGHT"],
        str(module.DEFAULT_USERLAND_COVERAGE_RENDER_HEIGHT),
        "LCOV render height",
    )
    assert_equal(env["WOS_SUITE_RENDER_SPP"], "99", "explicit render SPP should override LCOV default")
    assert_equal(
        env["WOS_SUITE_RENDER_MAX_DEPTH"],
        str(module.DEFAULT_USERLAND_COVERAGE_RENDER_MAX_DEPTH),
        "LCOV render max depth",
    )
    if "WOS_SUITE_SHUTDOWN" in env:
        raise AssertionError(f"LCOV userland suite shut down before profile fetch: {suite_cmd!r}")


def test_existing_userland_cluster_sync_rootfs_opt_in_before_suite(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        commands: list[list[str]] = []
        events: list[str] = []
        cluster_config = tmp_path / "cluster.json"
        cluster_config.write_text(json.dumps({"zones": [{"id": "GLOBAL"}, {"id": 0, "nodes": 1}]}))

        old_run_checked = module.run_checked
        run_checked_timeouts: list[int | None] = []

        def fake_run_checked(cmd, **_kwargs):
            commands.append(cmd)
            run_checked_timeouts.append(_kwargs.get("timeout"))
            command_name = Path(cmd[0]).name if cmd else ""
            if command_name == "wos-cluster" and "--sync" in cmd:
                events.append("sync")
            elif command_name == "wos-userland-suite":
                events.append("suite")
                output = Path(cmd[cmd.index("--output") + 1])
                output.write_text(complete_testd_log(module) + complete_userland_log(module))

        module.run_checked = fake_run_checked
        try:
            args = module.argparse.Namespace(
                userland_existing_cluster=True,
                userland_no_build=True,
                userland_no_lcov=True,
                userland_cluster_config=str(cluster_config),
                userland_tcg=None,
                cluster_ready_timeout=5,
                userland_ssh_timeout=5,
                userland_no_sync=False,
                userland_sync_rootfs=True,
                userland_sync_timeout=23,
                userland_sync_filter="configs/drive/srv/wos_userland_suite.sh",
                userland_host="vm0",
                userland_scale="quick",
                userland_env=[],
                userland_arg=[],
                userland_timeout=10,
                userland_shutdown_timeout=17,
                allow_userland_skips=False,
            )
            module.run_userland_suite_coverage(args, tmp_path)
        finally:
            module.run_checked = old_run_checked

    sync_cmd = next(cmd for cmd in commands if Path(cmd[0]).name == "wos-cluster")
    assert_equal(
        sync_cmd,
        [
            str(module.ROOT / "bin" / "wos-cluster"),
            "--sync",
            "--config",
            str(cluster_config),
            "--sync-timeout",
            "23",
            "--filter",
            "configs/drive/srv/wos_userland_suite.sh",
        ],
        "existing userland rootfs sync command",
    )
    suite_cmd = next(cmd for cmd in commands if Path(cmd[0]).name == "wos-userland-suite")
    if "--no-sync" not in suite_cmd:
        raise AssertionError(f"existing userland suite did not disable wrapper sync: {suite_cmd!r}")
    expected_suite_timeout = ["--timeout", "10"]
    if not any(suite_cmd[index : index + 2] == expected_suite_timeout for index in range(len(suite_cmd) - 1)):
        raise AssertionError(f"existing userland suite did not pass wrapper timeout: {suite_cmd!r}")
    expected_probe_timeout = ["--probe-timeout", str(module.DEFAULT_USERLAND_WRAPPER_PROBE_TIMEOUT_SECONDS)]
    if not any(suite_cmd[index : index + 2] == expected_probe_timeout for index in range(len(suite_cmd) - 1)):
        raise AssertionError(f"existing userland suite did not pass wrapper probe timeout: {suite_cmd!r}")
    suite_command_index = commands.index(suite_cmd)
    assert_equal(run_checked_timeouts[suite_command_index], 25, "existing userland wrapper timeout grace")
    if events != ["sync", "suite"]:
        raise AssertionError(f"existing userland sync did not precede suite: {events!r}")


def test_run_all_rejects_userland_topology_over_four_vms(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        cluster_config = tmp_path / "cluster.json"
        cluster_config.write_text(json.dumps({"zones": [{"id": "GLOBAL"}, {"id": 0, "nodes": 5}]}))
        called: list[str] = []
        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage

        def fail_if_called(name: str):
            def _inner(*_args, **_kwargs):
                called.append(name)
                raise AssertionError(f"{name} should not run after rejecting vm4 topology")

            return _inner

        module.run_host_gtest_coverage = fail_if_called("host")
        module.run_ktest_kcov = fail_if_called("ktest")
        module.run_userland_suite_coverage = fail_if_called("userland")
        try:
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                rc = module.main(
                    [
                        "--run-all",
                        "--userland-cluster-config",
                        str(cluster_config),
                        "--results-dir",
                        str(tmp_path / "results"),
                    ]
                )
        finally:
            module.run_host_gtest_coverage = old_host
            module.run_ktest_kcov = old_ktest
            module.run_userland_suite_coverage = old_userland

    assert_equal(rc, 1, "over-four-vm run-all return code")
    assert_equal(called, [], "over-four-vm run-all started a phase")
    message = stderr.getvalue()
    if "must use at most vm0..vm3" not in message or "vm4" not in message:
        raise AssertionError(f"missing over-four-vm diagnostic: {message!r}")


def main() -> None:
    module = load_module()
    tests = [
        test_bin_entrypoint_targets_coverage_summary,
        test_lcov_inputs_merge_overlapping_lines,
        test_runtime_log_scores_testd_and_userland,
        test_userland_skips_can_count_as_uncovered,
        test_cli_json_combines_merged_lcov_and_scores,
        test_cli_fail_under_reports_combined_threshold,
        test_host_unit_test_targets_are_discovered,
        test_run_all_host_timeout_is_bounded_by_default,
        test_run_all_userland_defaults_are_bounded,
        test_run_all_positive_timeout_args_reject_zero,
        test_cli_float_args_reject_nonfinite_values,
        test_verify_run_all_results_requires_explicit_repeat,
        test_run_all_rejects_existing_results_without_override,
        test_run_all_allows_existing_results_with_override,
        test_run_checked_timeout_kills_process_group,
        test_cluster_exit_timeout_force_stops_group_with_log_tail,
        test_guest_poweroff_accepts_ssh_disconnect_status,
        test_host_coverage_commands_write_health_logs,
        test_run_all_rejects_recoverable_host_ub_in_command_log,
        test_artifact_health_scan_rejects_failure_markers,
        test_artifact_health_scan_catches_marker_split_across_start_offset,
        test_run_all_rejects_unhealthy_artifacts,
        test_run_all_external_log_health_scan_starts_at_iteration_offset,
        test_run_all_rejects_empty_current_external_log_range,
        test_run_all_accepts_quiet_qemu_external_log_range,
        test_run_all_artifacts_feed_aggregator,
        test_run_all_requires_wos_userland_lcov,
        test_wos_userland_lcov_export_includes_all_userland_objects,
        test_wos_userland_profraw_fetch_collects_all_cluster_nodes,
        test_wos_userland_profraw_fetch_skips_stuck_profile,
        test_wos_userland_lcov_rejects_existing_cluster,
        test_run_all_repeat_uses_iteration_dirs_and_timing_gate,
        test_run_all_resume_continues_after_verified_prefix,
        test_run_all_resume_rejects_failed_prefix_before_running,
        test_run_all_resume_rejects_incomplete_run_directory,
        test_run_all_repeat_rejects_runtime_delta,
        test_run_all_repeat_rejects_runtime_delta_before_later_iterations,
        test_run_all_failure_manifest_keeps_partial_artifacts,
        test_verify_run_all_results_accepts_clean_repeat_artifacts,
        test_verify_run_all_results_rejects_stale_manifest_runtime_delta,
        test_verify_run_all_results_rejects_missing_manifest_runtime_delta,
        test_verify_run_all_results_rejects_nonfinite_manifest_numbers,
        test_verify_run_all_results_rejects_mixed_manifest_layout,
        test_verify_run_all_results_rejects_mismatched_manifest_path_index,
        test_verify_run_all_results_rejects_cross_iteration_artifact_path,
        test_verify_run_all_results_rejects_mismatched_manifest_results_dir,
        test_verify_run_all_results_rejects_non_full_userland_mode,
        test_verify_run_all_results_rejects_wrong_userland_host_or_config,
        test_verify_run_all_results_rejects_partial_run_config,
        test_verify_run_all_results_rejects_shortcut_run_config,
        test_verify_run_all_results_rejects_missing_expected_external_log_range,
        test_verify_run_all_results_rejects_empty_expected_external_log_range,
        test_verify_run_all_results_accepts_quiet_qemu_external_log_ranges,
        test_verify_run_all_results_rejects_duplicate_external_log_evidence,
        test_verify_run_all_results_rejects_missing_required_full_external_log_label,
        test_verify_run_all_results_rejects_wrong_required_external_log_path,
        test_verify_run_all_results_rejects_health_marker,
        test_verify_run_all_results_rejects_malformed_core_artifact,
        test_verify_run_all_results_rejects_missing_no_setup_launch_evidence,
        test_verify_run_all_results_scans_recorded_external_log_ranges,
        test_wait_for_wos_ssh_reports_guest_network_failure,
        test_wait_for_wos_ssh_bounds_each_probe,
        test_wait_for_cluster_ready_reports_launch_log_tail,
        test_ktest_run_uses_preconfigured_topology,
        test_auto_userland_run_requests_guest_poweroff,
        test_userland_lcov_run_bounds_renderbench_workload,
        test_existing_userland_cluster_sync_rootfs_opt_in_before_suite,
        test_run_all_rejects_userland_topology_over_four_vms,
    ]
    for test in tests:
        test(module)
    print(f"{len(tests)} coverage_summary tests passed")


if __name__ == "__main__":
    main()
