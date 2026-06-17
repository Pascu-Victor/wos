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


def test_run_all_artifacts_feed_aggregator(module) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-coverage-summary-test-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        host_lcov = tmp_path / "host.info"
        kcov_lcov = tmp_path / "kcov.info"
        userland_log = tmp_path / "userland.log"
        cluster_log = tmp_path / "cluster.log"
        write_lcov(host_lcov, {"/repo/host.cpp": {1: 1, 2: 0}})
        write_lcov(kcov_lcov, {"/repo/kern.cpp": {1: 1, 2: 1}})
        userland_log.write_text(complete_testd_log(module) + complete_userland_log(module))
        cluster_log.write_text("cluster ready\n")

        old_host = module.run_host_gtest_coverage
        old_ktest = module.run_ktest_kcov
        old_userland = module.run_userland_suite_coverage
        module.run_host_gtest_coverage = lambda _results, _build, _jobs, _timeout: host_lcov
        module.run_ktest_kcov = lambda _args, _results: kcov_lcov
        module.run_userland_suite_coverage = lambda _args, _results: (userland_log, cluster_log)
        try:
            stdout = io.StringIO()
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
    if data["combined"]["hit"] <= 0 or data["combined"]["total"] <= 0:
        raise AssertionError(f"run-all did not aggregate produced artifacts: {data!r}")


def main() -> None:
    module = load_module()
    tests = [
        test_lcov_inputs_merge_overlapping_lines,
        test_runtime_log_scores_testd_and_userland,
        test_userland_skips_can_count_as_uncovered,
        test_cli_json_combines_merged_lcov_and_scores,
        test_cli_fail_under_reports_combined_threshold,
        test_host_unit_test_targets_are_discovered,
        test_run_all_artifacts_feed_aggregator,
    ]
    for test in tests:
        test(module)
    print(f"{len(tests)} coverage_summary tests passed")


if __name__ == "__main__":
    main()
