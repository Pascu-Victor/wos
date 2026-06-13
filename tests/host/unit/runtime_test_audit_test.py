#!/usr/bin/env python3

import importlib.util
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
AUDIT_SCRIPT = ROOT / "scripts" / "test" / "runtime_test_audit.py"


def load_module():
    spec = importlib.util.spec_from_file_location("runtime_test_audit", AUDIT_SCRIPT)
    if spec is None or spec.loader is None:
        raise AssertionError(f"failed to load {AUDIT_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def expect_raises(fn, expected: str) -> None:
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - tiny dependency-free test harness
        if expected not in str(exc):
            raise AssertionError(f"expected {expected!r} in {exc!r}") from exc
        return
    raise AssertionError(f"expected exception containing {expected!r}")


def complete_ktest_log(module, passed_multiplier: int = 2, serial_prefix: bool = False) -> str:
    enabled, disabled = module.ktest_manifest()
    prefix = "[0.001] info ktest: " if serial_prefix else "[KTEST] "
    lines = [
        f"{prefix}=== WOS Kernel Self-Test Suite ===",
    ]
    for key in sorted(enabled):
        lines.append(f"{prefix}RUN   {key}")
        lines.append(f"{prefix}PASS  {key}")
    for key in sorted(disabled):
        lines.append(f"{prefix}SKIP  {key}")
    lines.append(f"{prefix}=== {len(enabled) * passed_multiplier} passed, 0 failed ===")
    return "\n".join(lines) + "\n"


def complete_testd_log(module) -> str:
    labels = module.testd_expected_pass_labels()
    lines = ["[TESTD] starting"]
    for index, label in enumerate(labels, start=1):
        lines.append(f"[TESTD] {index}/{len(labels)} PASS: {label}")
    lines.append(f"[TESTD] DONE: {len(labels)} passed, 0 failed")
    return "\n".join(lines) + "\n"


def complete_userland_log(module) -> str:
    cases = sorted(module.userland_expected_cases())
    lines = ["=== WOS USERLAND SUITE SUMMARY ==="]
    for case in cases:
        lines.append(f"{case}\tPASS\t1s")
    lines.append(f"PASS={len(cases)} FAIL=0 SKIP=0")
    lines.append("RESULT_DIR=/tmp/wos-userland-suite-test")
    return "\n".join(lines) + "\n"


def userland_log_with_one_skip(module) -> str:
    cases = sorted(module.userland_expected_cases())
    skipped = cases[0]
    lines = ["=== WOS USERLAND SUITE SUMMARY ==="]
    for case in cases:
        if case == skipped:
            lines.append(f"{case}\tSKIP\tmissing synthetic dependency")
        else:
            lines.append(f"{case}\tPASS\t1s")
    lines.append(f"PASS={len(cases) - 1} FAIL=0 SKIP=1")
    lines.append("RESULT_DIR=/tmp/wos-userland-suite-test")
    return "\n".join(lines) + "\n"


def userland_log_with_missing_dependency_skips(module) -> str:
    skipped_cases = {
        "testprog_smoke",
        "make_large_file",
        "vfsbench_read",
        "vfsbench_stat",
        "cp_large_file",
        "dd_large_file",
        "testprog_perf_suite",
        "mandelbench_large",
        "netbench_loopback",
        "strace_vfsbench",
        "perf_stat",
        "perf_trace_vfs",
    }
    cases = sorted(module.userland_expected_cases())
    missing = skipped_cases - set(cases)
    if missing:
        raise AssertionError(f"synthetic dependency skip case is not in the manifest: {sorted(missing)}")

    lines = ["=== WOS USERLAND SUITE SUMMARY ==="]
    for case in cases:
        if case in skipped_cases:
            lines.append(f"{case}\tSKIP\tmissing synthetic dependency")
        else:
            lines.append(f"{case}\tPASS\t1s")
    lines.append(f"PASS={len(cases) - len(skipped_cases)} FAIL=0 SKIP={len(skipped_cases)}")
    lines.append("RESULT_DIR=/tmp/wos-userland-suite-test")
    return "\n".join(lines) + "\n"


def incomplete_userland_log() -> str:
    return "\n".join(
        [
            "WOS userland suite",
            "RUN_ID=runtime-audit-incomplete",
            "RESULT_DIR=/tmp/wos-userland-suite-test",
            "=== RUN testd ===",
        ]
    ) + "\n"


def test_complete_ktest_log_passes(module) -> None:
    log = "old noise\n[KTEST] === 1 passed, 9 failed ===\n" + complete_ktest_log(module)
    result = module.audit_ktest_text(log)
    enabled, disabled = module.ktest_manifest()
    if result.kind != "ktest" or result.failed != 0:
        raise AssertionError(f"unexpected KTEST result: {result}")
    if f"{len(enabled)} enabled tests passed; {len(disabled)} disabled tests skipped" not in result.detail:
        raise AssertionError(f"unexpected KTEST detail: {result.detail}")


def test_serial_ktest_log_passes(module) -> None:
    result = module.audit_ktest_text(complete_ktest_log(module, serial_prefix=True))
    enabled, _ = module.ktest_manifest()
    if result.kind != "ktest" or result.passed != len(enabled) * 2 or result.failed != 0:
        raise AssertionError(f"unexpected serial KTEST result: {result}")


def test_ktest_missing_pass_fails(module) -> None:
    enabled, _ = module.ktest_manifest()
    missing = sorted(enabled)[0]
    log = complete_ktest_log(module).replace(f"[KTEST] PASS  {missing}\n", "")
    expect_raises(lambda: module.audit_ktest_text(log), "missing PASS")


def test_ktest_fail_line_fails(module) -> None:
    enabled, _ = module.ktest_manifest()
    summary = f"[KTEST] === {len(enabled) * 2} passed, 0 failed ==="
    log = complete_ktest_log(module).replace(summary, f"[KTEST] FAIL synthetic: nope\n{summary}")
    expect_raises(lambda: module.audit_ktest_text(log), "FAIL lines")


def test_ktest_low_summary_count_fails(module) -> None:
    expect_raises(lambda: module.audit_ktest_text(complete_ktest_log(module, passed_multiplier=0)), "below enabled test count")


def test_ktest_missing_disabled_skip_fails(module) -> None:
    _, disabled = module.ktest_manifest()
    if not disabled:
        return
    missing = sorted(disabled)[0]
    log = complete_ktest_log(module).replace(f"[KTEST] SKIP  {missing}\n", "")
    expect_raises(lambda: module.audit_ktest_text(log), "missing SKIP")


def test_ktest_unexpected_entry_fails(module) -> None:
    enabled, _ = module.ktest_manifest()
    summary = f"[KTEST] === {len(enabled) * 2} passed, 0 failed ==="
    injected = "[KTEST] RUN   Ghost/Synthetic\n[KTEST] PASS  Ghost/Synthetic\n"
    log = complete_ktest_log(module).replace(summary, injected + summary)
    expect_raises(lambda: module.audit_ktest_text(log), "unexpected RUN")


def test_ktest_newer_incomplete_block_fails(module) -> None:
    enabled, _ = module.ktest_manifest()
    first_test = sorted(enabled)[0]
    log = complete_ktest_log(module) + f"[KTEST] === WOS Kernel Self-Test Suite ===\n[KTEST] RUN   {first_test}\n"
    expect_raises(lambda: module.audit_ktest_text(log), "newer incomplete run block")


def test_complete_testd_log_passes(module) -> None:
    result = module.audit_testd_text(complete_testd_log(module))
    labels = module.testd_expected_pass_labels()
    if result.kind != "testd" or result.passed != len(labels) or result.failed != 0:
        raise AssertionError(f"unexpected TESTD result: {result}")


def test_testd_duplicate_and_missing_label_fails(module) -> None:
    labels = module.testd_expected_pass_labels()
    lines = ["[TESTD] starting"]
    broken = list(labels)
    broken[-1] = broken[0]
    for index, label in enumerate(broken, start=1):
        lines.append(f"[TESTD] {index}/{len(labels)} PASS: {label}")
    lines.append(f"[TESTD] DONE: {len(labels)} passed, 0 failed")
    expect_raises(lambda: module.audit_testd_text("\n".join(lines) + "\n"), "duplicate PASS labels")


def test_testd_counter_mismatch_fails(module) -> None:
    labels = module.testd_expected_pass_labels()
    lines = ["[TESTD] starting"]
    for index, label in enumerate(labels, start=1):
        total = len(labels) + 1 if index == 1 else len(labels)
        lines.append(f"[TESTD] {index}/{total} PASS: {label}")
    lines.append(f"[TESTD] DONE: {len(labels)} passed, 0 failed")
    expect_raises(lambda: module.audit_testd_text("\n".join(lines) + "\n"), "PASS counter mismatch")


def test_testd_fail_line_fails(module) -> None:
    log = complete_testd_log(module).replace("[TESTD] DONE:", "[TESTD] FAIL: synthetic\n[TESTD] DONE:")
    expect_raises(lambda: module.audit_testd_text(log), "FAIL lines")


def test_testd_post_done_fail_line_fails(module) -> None:
    log = complete_testd_log(module) + "[TESTD] FAIL: accounting mismatch after DONE\n"
    expect_raises(lambda: module.audit_testd_text(log), "FAIL lines")


def test_testd_summary_mismatch_fails(module) -> None:
    labels = module.testd_expected_pass_labels()
    log = complete_testd_log(module).replace(
        f"[TESTD] DONE: {len(labels)} passed, 0 failed",
        f"[TESTD] DONE: {len(labels) - 1} passed, 0 failed",
    )
    expect_raises(lambda: module.audit_testd_text(log), "does not match expected")


def test_testd_newer_incomplete_block_fails(module) -> None:
    labels = module.testd_expected_pass_labels()
    log = complete_testd_log(module) + f"[TESTD] starting\n[TESTD] 1/{len(labels)} PASS: {labels[0]}\n"
    expect_raises(lambda: module.audit_testd_text(log), "newer incomplete run block")


def test_complete_userland_log_passes(module) -> None:
    result = module.audit_userland_text(complete_userland_log(module))
    cases = module.userland_expected_cases()
    if result.kind != "userland" or result.passed != len(cases) or result.failed != 0:
        raise AssertionError(f"unexpected userland result: {result}")
    if f"{len(cases)} cases accounted" not in result.detail:
        raise AssertionError(f"unexpected userland detail: {result.detail}")


def test_userland_missing_case_fails(module) -> None:
    missing = sorted(module.userland_expected_cases())[0]
    log = complete_userland_log(module).replace(f"{missing}\tPASS\t1s\n", "")
    expect_raises(lambda: module.audit_userland_text(log), "missing case rows")


def test_userland_duplicate_case_fails(module) -> None:
    duplicate = sorted(module.userland_expected_cases())[0]
    log = complete_userland_log(module).replace(
        f"{duplicate}\tPASS\t1s\n",
        f"{duplicate}\tPASS\t1s\n{duplicate}\tPASS\t2s\n",
        1,
    )
    expect_raises(lambda: module.audit_userland_text(log), "duplicate case rows")


def test_userland_failed_case_fails(module) -> None:
    failed = sorted(module.userland_expected_cases())[0]
    log = complete_userland_log(module).replace(f"{failed}\tPASS\t1s\n", f"{failed}\tFAIL\trc=1 1s\n", 1)
    expect_raises(lambda: module.audit_userland_text(log), "failed cases")


def test_userland_count_mismatch_fails(module) -> None:
    cases = module.userland_expected_cases()
    log = complete_userland_log(module).replace(f"PASS={len(cases)} FAIL=0 SKIP=0", f"PASS={len(cases) - 1} FAIL=0 SKIP=0")
    expect_raises(lambda: module.audit_userland_text(log), "count mismatch")


def test_userland_skip_fails_by_default(module) -> None:
    expect_raises(lambda: module.audit_userland_text(userland_log_with_one_skip(module)), "skipped cases")


def test_userland_skip_allowed_for_triage(module) -> None:
    result = module.audit_userland_text(userland_log_with_one_skip(module), allow_skips=True)
    cases = module.userland_expected_cases()
    if result.kind != "userland" or result.passed != len(cases) - 1 or result.failed != 0:
        raise AssertionError(f"unexpected userland skip-allowed result: {result}")
    if "1 skipped" not in result.detail:
        raise AssertionError(f"unexpected userland skip detail: {result.detail}")


def test_userland_dependency_skip_groups_are_accounted(module) -> None:
    result = module.audit_userland_text(userland_log_with_missing_dependency_skips(module), allow_skips=True)
    cases = module.userland_expected_cases()
    if result.kind != "userland" or result.passed != len(cases) - 12 or result.failed != 0:
        raise AssertionError(f"unexpected dependency-skip userland result: {result}")
    if "12 skipped" not in result.detail:
        raise AssertionError(f"unexpected dependency-skip detail: {result.detail}")


def test_userland_suite_dependency_gates_emit_manifest_rows(module) -> None:
    source = module.USERLAND_SUITE.read_text()
    required_tokens = [
        "skip_case_group()",
        'skip_case_group "missing executable: /usr/bin/testprog"',
        "make_large_file",
        "vfsbench_read",
        "vfsbench_stat",
        "cp_large_file",
        "dd_large_file",
        "testprog_perf_suite",
        "mandelbench_large",
        "netbench_loopback",
        'skip_case strace_vfsbench "missing executable: /usr/bin/testprog"',
        'skip_case perf_trace_vfs "missing executable: /usr/bin/testprog"',
        'skip_case perf_trace_vfs "missing executable: /usr/bin/perf"',
    ]
    missing = [token for token in required_tokens if token not in source]
    if missing:
        raise AssertionError("userland suite dependency gates are missing manifest rows: " + ", ".join(missing))


def test_userland_newer_incomplete_block_fails(module) -> None:
    first_case = sorted(module.userland_expected_cases())[0]
    log = complete_userland_log(module) + f"=== WOS USERLAND SUITE SUMMARY ===\n{first_case}\tPASS\t1s\n"
    expect_raises(lambda: module.audit_userland_text(log), "newer incomplete run block")


def test_source_manifests_ignore_cxx_comments_and_literals(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-runtime-audit-manifest-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        ktest_dir = tmp_path / "ktest"
        ktest_dir.mkdir()
        ktest_source = ktest_dir / "synthetic_ktest.cpp"
        ktest_source.write_text(
            """
KTEST(RealRuntimeAudit, Runs) { KEXPECT_TRUE(true); }
// KTEST(CommentGhost, ShouldNotExist) { KEXPECT_TRUE(false); }
char const* string_ghost = "KTEST(StringGhost, ShouldNotExist) {";
uint64_t const key = 0x00FF'FFFF'FFFF'FFFFULL;
KTEST_OFF(DisabledRuntimeAudit, Skips) {
    // Requires synthetic host coverage.
    KEXPECT_TRUE(false);
}
""".lstrip()
        )
        testd_main = tmp_path / "testd_main.cpp"
        testd_main.write_text(
            """
TESTD_RUN(test_real)
{
    TESTD_PASS("real.pass");
    TESTD_CHECK("real.check", true);
    // TESTD_PASS("comment.fake");
    char const* string_ghost = "TESTD_PASS(\\"string.fake\\")";
}
TESTD_RUN_END(test_real)
""".lstrip()
        )

        old_ktest_dir = module.KTEST_DIR
        old_testd_main = module.TESTD_MAIN
        module.KTEST_DIR = ktest_dir
        module.TESTD_MAIN = testd_main
        try:
            enabled, disabled = module.ktest_manifest()
            if enabled != {"RealRuntimeAudit/Runs"} or disabled != {"DisabledRuntimeAudit/Skips"}:
                raise AssertionError(f"unexpected synthetic KTEST manifest: enabled={enabled}, disabled={disabled}")
            labels = module.testd_expected_pass_labels()
            if labels != ["real.pass", "real.check"]:
                raise AssertionError(f"unexpected synthetic TESTD labels: {labels}")
        finally:
            module.KTEST_DIR = old_ktest_dir
            module.TESTD_MAIN = old_testd_main


def test_cli_auto_both_passes(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-runtime-audit-test-") as tmp_dir:
        log_path = Path(tmp_dir) / "runtime.log"
        log_path.write_text(complete_ktest_log(module) + complete_testd_log(module))
        result = subprocess.run(
            [sys.executable, str(AUDIT_SCRIPT), "--kind", "auto", str(log_path)],
            capture_output=True,
            text=True,
            check=False,
        )
    if result.returncode != 0:
        raise AssertionError(f"runtime_test_audit.py failed:\nstdout={result.stdout}\nstderr={result.stderr}")
    if "ktest: PASS" not in result.stdout or "testd: PASS" not in result.stdout:
        raise AssertionError(f"missing PASS summaries in stdout: {result.stdout}")


def test_cli_auto_with_userland_passes(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-runtime-audit-test-") as tmp_dir:
        log_path = Path(tmp_dir) / "runtime.log"
        log_path.write_text(complete_ktest_log(module) + complete_testd_log(module) + complete_userland_log(module))
        result = subprocess.run(
            [sys.executable, str(AUDIT_SCRIPT), "--kind", "auto", str(log_path)],
            capture_output=True,
            text=True,
            check=False,
        )
    if result.returncode != 0:
        raise AssertionError(f"runtime_test_audit.py failed:\nstdout={result.stdout}\nstderr={result.stderr}")
    if "ktest: PASS" not in result.stdout or "testd: PASS" not in result.stdout or "userland: PASS" not in result.stdout:
        raise AssertionError(f"missing PASS summaries in stdout: {result.stdout}")


def test_cli_auto_incomplete_userland_fails(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-runtime-audit-test-") as tmp_dir:
        log_path = Path(tmp_dir) / "runtime.log"
        log_path.write_text(complete_testd_log(module) + incomplete_userland_log())
        result = subprocess.run(
            [sys.executable, str(AUDIT_SCRIPT), "--kind", "auto", str(log_path)],
            capture_output=True,
            text=True,
            check=False,
        )
    if result.returncode == 0:
        raise AssertionError("runtime_test_audit.py accepted an incomplete userland suite in auto mode")
    if "userland suite log has a newer incomplete run block" not in result.stderr:
        raise AssertionError(f"missing incomplete-userland error in stderr: {result.stderr}")


def test_cli_userland_skip_requires_allow_flag(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-runtime-audit-test-") as tmp_dir:
        log_path = Path(tmp_dir) / "runtime.log"
        log_path.write_text(userland_log_with_one_skip(module))
        rejected = subprocess.run(
            [sys.executable, str(AUDIT_SCRIPT), "--kind", "userland", str(log_path)],
            capture_output=True,
            text=True,
            check=False,
        )
        accepted = subprocess.run(
            [sys.executable, str(AUDIT_SCRIPT), "--kind", "userland", "--allow-userland-skips", str(log_path)],
            capture_output=True,
            text=True,
            check=False,
        )
    if rejected.returncode == 0 or "skipped cases" not in rejected.stderr:
        raise AssertionError(f"runtime_test_audit.py accepted skipped userland rows:\nstderr={rejected.stderr}")
    if accepted.returncode != 0 or "userland: PASS" not in accepted.stdout:
        raise AssertionError(
            "runtime_test_audit.py rejected skipped userland rows despite allow flag:\n"
            f"stdout={accepted.stdout}\nstderr={accepted.stderr}"
        )


def test_cli_auto_serial_ktest_passes(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-runtime-audit-test-") as tmp_dir:
        log_path = Path(tmp_dir) / "runtime.log"
        log_path.write_text(complete_ktest_log(module, serial_prefix=True))
        result = subprocess.run(
            [sys.executable, str(AUDIT_SCRIPT), "--kind", "auto", str(log_path)],
            capture_output=True,
            text=True,
            check=False,
        )
    if result.returncode != 0:
        raise AssertionError(f"runtime_test_audit.py failed:\nstdout={result.stdout}\nstderr={result.stderr}")
    if "ktest: PASS" not in result.stdout:
        raise AssertionError(f"missing serial KTEST PASS summary in stdout: {result.stdout}")


def test_cli_auto_without_markers_fails(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-runtime-audit-test-") as tmp_dir:
        log_path = Path(tmp_dir) / "runtime.log"
        log_path.write_text("no test markers here\n")
        result = subprocess.run(
            [sys.executable, str(AUDIT_SCRIPT), "--kind", "auto", str(log_path)],
            capture_output=True,
            text=True,
            check=False,
        )
    if result.returncode == 0:
        raise AssertionError("runtime_test_audit.py accepted a log without KTEST/TESTD markers")
    if "no KTEST, TESTD, or userland suite markers" not in result.stderr:
        raise AssertionError(f"missing no-marker error in stderr: {result.stderr}")


def main() -> None:
    module = load_module()
    tests = [
        test_complete_ktest_log_passes,
        test_serial_ktest_log_passes,
        test_ktest_missing_pass_fails,
        test_ktest_fail_line_fails,
        test_ktest_low_summary_count_fails,
        test_ktest_missing_disabled_skip_fails,
        test_ktest_unexpected_entry_fails,
        test_ktest_newer_incomplete_block_fails,
        test_complete_testd_log_passes,
        test_testd_duplicate_and_missing_label_fails,
        test_testd_counter_mismatch_fails,
        test_testd_fail_line_fails,
        test_testd_post_done_fail_line_fails,
        test_testd_summary_mismatch_fails,
        test_testd_newer_incomplete_block_fails,
        test_complete_userland_log_passes,
        test_userland_missing_case_fails,
        test_userland_duplicate_case_fails,
        test_userland_failed_case_fails,
        test_userland_count_mismatch_fails,
        test_userland_skip_fails_by_default,
        test_userland_skip_allowed_for_triage,
        test_userland_dependency_skip_groups_are_accounted,
        test_userland_suite_dependency_gates_emit_manifest_rows,
        test_userland_newer_incomplete_block_fails,
        test_source_manifests_ignore_cxx_comments_and_literals,
        test_cli_auto_both_passes,
        test_cli_auto_with_userland_passes,
        test_cli_auto_incomplete_userland_fails,
        test_cli_userland_skip_requires_allow_flag,
        test_cli_auto_serial_ktest_passes,
        test_cli_auto_without_markers_fails,
    ]
    for test in tests:
        test(module)
    print(f"{len(tests)} runtime_test_audit tests passed")


if __name__ == "__main__":
    main()
