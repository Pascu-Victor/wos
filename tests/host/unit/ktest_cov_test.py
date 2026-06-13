#!/usr/bin/env python3

import contextlib
import importlib.util
import io
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
KTEST_COV = ROOT / "scripts" / "test" / "ktest_cov.py"


def load_module():
    spec = importlib.util.spec_from_file_location("ktest_cov", KTEST_COV)
    if spec is None or spec.loader is None:
        raise AssertionError(f"failed to load {KTEST_COV}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def assert_equal(actual, expected, msg):
    if actual != expected:
        raise AssertionError(f"{msg}: expected {expected!r}, got {actual!r}")


def test_parse_fingerprint(module):
    fp = module.parse_kcov_elf_line(
        "[1.000] INFO kcov_elf: text_start=0xffffffff80000120 "
        "text_end=0xffffffff80100000 text_fnv64=0x1234abcd"
    )
    assert_equal(
        fp,
        module.TextFingerprint(0xFFFFFFFF80000120, 0xFFFFFFFF80100000, 0x1234ABCD),
        "journal fingerprint line",
    )

    bracketed = module.parse_kcov_elf_line(
        "[KCOV_ELF] text_start=0x10 text_end=0x20 text_fnv64=0x30"
    )
    assert_equal(bracketed, module.TextFingerprint(0x10, 0x20, 0x30), "bracketed fingerprint line")
    assert_equal(module.parse_kcov_elf_line("not a fingerprint"), None, "non-fingerprint line")


def test_extract_latest_kcov_block(module):
    log = """\
[0.001] INFO kcov_elf: text_start=0x10 text_end=0x20 text_fnv64=0x30
[0.002] INFO kcov_begin: 2
[0.003] INFO kcov: 0x111 0x222
[0.004] INFO kcov_end:
[0.005] INFO kcov_elf: text_start=0x40 text_end=0x50 text_fnv64=0x60
[0.006] INFO kcov_begin: 3
[0.007] INFO kcov: 0x333 0x444
[0.008] INFO kcov: 0x555 bad-token
[0.009] INFO kcov_end:
"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "serial.log"
        path.write_text(log)
        pcs, fp = module.extract_latest_kcov_block(str(path))

    assert_equal(pcs, [0x333, 0x444, 0x555], "latest KCOV PC block")
    assert_equal(fp, module.TextFingerprint(0x40, 0x50, 0x60), "latest KCOV fingerprint")


def test_extract_latest_kcov_report_tracks_truncation(module):
    log = """\
[0.001] INFO kcov_elf: text_start=0x10 text_end=0x20 text_fnv64=0x30
[0.002] INFO kcov_begin: 1
[0.003] INFO kcov: 0x111
[0.004] INFO kcov_end:
[0.005] INFO kcov_elf: text_start=0x40 text_end=0x50 text_fnv64=0x60
[0.006] INFO kcov_begin: 2
[0.007] INFO kcov_truncated: count=262144 capacity=262144
[0.008] INFO kcov: 0x333 0x444
[0.009] INFO kcov_end:
"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "serial.log"
        path.write_text(log)
        report = module.extract_latest_kcov_report(str(path))

    assert_equal(report.pcs, [0x333, 0x444], "latest truncated KCOV PCs")
    assert_equal(report.fingerprint, module.TextFingerprint(0x40, 0x50, 0x60), "latest truncated fingerprint")
    assert_equal(report.truncated, True, "latest truncated flag")
    assert_equal(report.declared_count, 2, "latest truncated declared count")


def test_extract_latest_kcov_report_rejects_incomplete_latest_block(module):
    log = """\
[0.001] INFO kcov_begin: 1
[0.002] INFO kcov: 0x111
[0.003] INFO kcov_end:
[0.004] INFO kcov_begin: 1
[0.005] INFO kcov: 0x222
"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "serial.log"
        path.write_text(log)
        try:
            module.extract_latest_kcov_report(str(path))
        except module.CoverageLogError as exc:
            if "incomplete coverage block" not in str(exc):
                raise AssertionError(f"unexpected incomplete-block error: {exc}") from exc
        else:
            raise AssertionError("incomplete latest KCOV block was accepted")


def test_build_coverage_map(module):
    by_file = module.build_coverage_map(
        {"/repo/a.cpp": {2, 3, 99}, "/repo/generated.cpp": {10}},
        {"/repo/a.cpp": {1, 2, 3}, "/repo/b.cpp": {7}},
    )

    assert_equal(by_file["/repo/a.cpp"], {1: 0, 2: 1, 3: 1}, "covered and uncovered lines")
    assert_equal(by_file["/repo/b.cpp"], {7: 0}, "compiled-only file")
    if "/repo/generated.cpp" in by_file:
        raise AssertionError("hit-only file was allowed to inflate coverage")
    if 99 in by_file["/repo/a.cpp"]:
        raise AssertionError("hit-only line was allowed to inflate coverage")


def test_coverage_exclusions_mark_uncoverable(module):
    with tempfile.TemporaryDirectory() as tmp_dir:
        src = Path(tmp_dir) / "a.cpp"
        src.write_text(
            "int coverable() { return 1; }\n"
            "[[clang::no_sanitize(\"coverage\")]]\n"
            "int no_coverage() {\n"
            "    return 0;\n"
            "}\n"
            "[[clang::no_sanitize(\"kcfi\")]] int no_kcfi() { return 2; }\n"
            "__attribute__((no_sanitize(\"address\"))) int still_coverable() { return 3; }\n"
        )
        asm = "/repo/trap.asm"
        compiled = {
            str(src): set(range(1, 8)),
            asm: {1, 2},
        }
        functions = {
            str(src): {
                "coverable": module.FunctionRecord(1, "coverable", end_line=1),
                "no_coverage": module.FunctionRecord(3, "no_coverage", end_line=5),
                "no_kcfi": module.FunctionRecord(6, "no_kcfi", end_line=6),
                "still_coverable": module.FunctionRecord(7, "still_coverable", end_line=7),
            },
            asm: {
                "trap": module.FunctionRecord(1, "trap", end_line=2),
            },
        }

        exclusions = module.collect_coverage_exclusions(
            compiled,
            functions,
            module.SanitizerIgnorelist((), ()),
        )

    assert_equal(set(exclusions.lines_by_file[str(src)]), {2, 3, 4, 5, 6}, "source uncoverable lines")
    assert_equal(set(exclusions.lines_by_file[asm]), {1, 2}, "assembly uncoverable lines")
    assert_equal(
        set(exclusions.functions_by_file[str(src)]),
        {"no_coverage", "no_kcfi"},
        "source uncoverable functions",
    )
    assert_equal(set(exclusions.functions_by_file[asm]), {"trap"}, "assembly uncoverable functions")

    coverable_lines = module.apply_line_exclusions(compiled, exclusions)
    assert_equal(coverable_lines[str(src)], {1, 7}, "source coverable lines")
    assert_equal(coverable_lines[asm], set(), "assembly coverable lines")

    coverable_functions = module.apply_function_exclusions(functions, exclusions)
    assert_equal(
        set(coverable_functions[str(src)]),
        {"coverable", "still_coverable"},
        "source coverable functions",
    )
    assert_equal(coverable_functions[asm], {}, "assembly coverable functions")


def test_unmapped_hit_lines(module):
    unmapped = module.unmapped_hit_lines(
        {"/repo/a.cpp": {2, 3, 99}, "/repo/generated.cpp": {10}},
        {"/repo/a.cpp": {1, 2, 3}, "/repo/b.cpp": {7}},
    )

    assert_equal(unmapped, {"/repo/a.cpp": {99}, "/repo/generated.cpp": {10}}, "unmapped hit lines")
    failures = module.unmapped_hit_failures(unmapped, "generated")
    if len(failures) != 1 or "refusing strict coverage" not in failures[0]:
        raise AssertionError(f"unexpected unmapped-hit failure: {failures!r}")

    with contextlib.redirect_stderr(io.StringIO()) as stderr:
        module.print_unmapped_hit_warning(unmapped, "generated")
    warning = stderr.getvalue()
    if "ignored 1 symbolized hit line" not in warning or "generated.cpp" not in warning:
        raise AssertionError(f"unexpected unmapped-hit warning: {warning!r}")


def test_coverage_thresholds(module):
    by_file = {
        "/repo/a.cpp": {1: 1, 2: 1, 3: 0},
        "/repo/b.cpp": {10: 1, 11: 0},
    }

    summary = module.coverage_summary(by_file)
    assert_equal(summary.files, 2, "coverage summary file count")
    assert_equal(summary.hit_lines, 3, "coverage summary hit count")
    assert_equal(summary.total_lines, 5, "coverage summary total count")
    assert_equal(round(summary.percent, 1), 60.0, "coverage summary percent")

    assert_equal(
        module.threshold_failures(by_file, "", 50.0, None),
        [],
        "total threshold below observed coverage",
    )
    total_failures = module.threshold_failures(by_file, "", 80.0, None)
    if len(total_failures) != 1 or "total line coverage 60.0%" not in total_failures[0]:
        raise AssertionError(f"unexpected total threshold failure: {total_failures}")

    file_failures = module.threshold_failures(by_file, "", None, 60.0)
    if len(file_failures) != 1 or "/repo/b.cpp" not in file_failures[0]:
        raise AssertionError(f"unexpected per-file threshold failure: {file_failures}")

    missing = module.threshold_failures(by_file, "missing", 1.0, None)
    assert_equal(missing, ["No coverage data matched (check --src filter)."], "missing src threshold failure")


def test_coverage_threshold_arg(module):
    assert_equal(module.coverage_threshold_arg("0"), 0.0, "minimum threshold")
    assert_equal(module.coverage_threshold_arg("100"), 100.0, "maximum threshold")
    for value in ("-0.01", "100.01", "not-a-number"):
        try:
            module.coverage_threshold_arg(value)
        except Exception:
            pass
        else:
            raise AssertionError(f"threshold value {value!r} was accepted")


def test_fingerprint_validation(module):
    with tempfile.TemporaryDirectory() as tmp_dir:
        text_path = Path(tmp_dir) / "text.bin"
        text_path.write_bytes(bytes([0x01, 0x02, 0x03, 0x04]))
        expected_hash = module.fnv1a64(text_path.read_bytes())

    assert_equal(expected_hash, 0x8010D29826A519FB, "FNV-1a64 reference value")

    observed = module.TextFingerprint(0x10, 0x20, expected_hash)
    module.elf_text_fingerprint = lambda _elf: observed
    with contextlib.redirect_stdout(io.StringIO()):
        module.validate_log_elf_match(observed, "dummy.elf", allow_mismatch=False, require_fingerprint=False)

    mismatched = module.TextFingerprint(0x10, 0x20, expected_hash ^ 1)
    with contextlib.redirect_stderr(io.StringIO()):
        try:
            module.validate_log_elf_match(mismatched, "dummy.elf", allow_mismatch=False, require_fingerprint=False)
        except SystemExit as exc:
            assert_equal(exc.code, 2, "mismatch exit code")
        else:
            raise AssertionError("mismatch did not exit")

    with contextlib.redirect_stderr(io.StringIO()):
        module.validate_log_elf_match(mismatched, "dummy.elf", allow_mismatch=True, require_fingerprint=False)

    with contextlib.redirect_stderr(io.StringIO()) as stderr:
        module.validate_log_elf_match(None, "dummy.elf", allow_mismatch=False, require_fingerprint=False)
    if "no KCOV ELF fingerprint" not in stderr.getvalue():
        raise AssertionError("missing-fingerprint warning not emitted")

    with contextlib.redirect_stderr(io.StringIO()):
        try:
            module.validate_log_elf_match(None, "dummy.elf", allow_mismatch=False, require_fingerprint=True)
        except SystemExit as exc:
            assert_equal(exc.code, 6, "missing fingerprint exit code")
        else:
            raise AssertionError("missing fingerprint did not exit under strict coverage")


def test_truncation_validation(module):
    module.validate_not_truncated(False, allow_truncated=False)

    with contextlib.redirect_stderr(io.StringIO()):
        try:
            module.validate_not_truncated(True, allow_truncated=False)
        except SystemExit as exc:
            assert_equal(exc.code, 4, "truncation exit code")
        else:
            raise AssertionError("truncated KCOV did not exit")

    with contextlib.redirect_stderr(io.StringIO()) as stderr:
        module.validate_not_truncated(True, allow_truncated=True)
    if "may be truncated" not in stderr.getvalue():
        raise AssertionError("truncation warning not emitted")


def test_declared_count_validation(module):
    good = module.KcovBlock([0x111, 0x222], None, False, 2)
    module.validate_declared_count(good)

    legacy = module.KcovBlock([0x111], None, False, None)
    module.validate_declared_count(legacy)

    bad = module.KcovBlock([0x111], None, False, 2)
    with contextlib.redirect_stderr(io.StringIO()):
        try:
            module.validate_declared_count(bad)
        except SystemExit as exc:
            assert_equal(exc.code, 5, "declared count mismatch exit code")
        else:
            raise AssertionError("declared count mismatch did not exit")


def test_function_coverage_accounts_known_and_inline_hits(module):
    normalize_wos_source_path = module.normalize_wos_source_path
    module.normalize_wos_source_path = lambda path: path
    definitions = {
        "/repo/a.cpp": {
            "_Z3foov": module.FunctionRecord(10, "_Z3foov"),
        }
    }
    hits = [
        module.SymbolizedLocation("_Z3foov", "/repo/a.cpp", 12),
        module.SymbolizedLocation("_Z3foov", "/repo/a.cpp", 14),
        module.SymbolizedLocation("_Z3barv", "/repo/a.cpp", 20),
        module.SymbolizedLocation("??", "/repo/a.cpp", 30),
        module.SymbolizedLocation("__cfi__Z3bazv", "/repo/a.cpp", 40),
        module.SymbolizedLocation("__cfi_memcmp", "/repo/a.cpp", 41),
    ]

    try:
        coverage = module.build_function_coverage(hits, definitions)
        filtered_coverage = module.build_function_coverage(
            hits,
            definitions,
            {"/repo/a.cpp": {12: 1, 14: 0, 20: 0, 40: 1}},
        )
    finally:
        module.normalize_wos_source_path = normalize_wos_source_path

    assert_equal(coverage["/repo/a.cpp"]["_Z3foov"], module.FunctionRecord(10, "_Z3foov", 2, 14), "known function hits")
    assert_equal(coverage["/repo/a.cpp"]["_Z3barv"], module.FunctionRecord(20, "_Z3barv", 1, 20), "hit-only inline function")
    assert_equal(
        filtered_coverage["/repo/a.cpp"]["_Z3foov"],
        module.FunctionRecord(10, "_Z3foov", 1, 12),
        "filtered function hit",
    )
    if "_Z3barv" in filtered_coverage["/repo/a.cpp"]:
        raise AssertionError("unmapped function hit was recorded")
    if "??" in coverage["/repo/a.cpp"]:
        raise AssertionError("unknown function was recorded")
    if "__cfi__Z3bazv" in coverage["/repo/a.cpp"]:
        raise AssertionError("compiler CFI thunk was recorded")
    if "__cfi_memcmp" in coverage["/repo/a.cpp"]:
        raise AssertionError("compiler CFI helper was recorded")


def test_write_lcov_includes_function_records(module):
    with tempfile.TemporaryDirectory() as tmp_dir:
        out = Path(tmp_dir) / "coverage.info"
        module.write_lcov(
            {"/repo/a.cpp": {10: 1, 11: 0}},
            str(out),
            {
                "/repo/a.cpp": {
                    "_Z3foov": module.FunctionRecord(10, "_Z3foov", 2),
                    "_Z3bazv": module.FunctionRecord(10, "_Z3bazv", 0, 15),
                    "_Z3barv": module.FunctionRecord(20, "_Z3barv", 0),
                }
            },
        )
        text = out.read_text()

    expected = [
        "FN:10,15,_Z3foov",
        "FN:10,15,_Z3bazv",
        "FN:20,20,_Z3barv",
        "FNDA:2,_Z3foov",
        "FNDA:1,_Z3bazv",
        "FNDA:0,_Z3barv",
        "FNF:3",
        "FNH:2",
        "DA:10,1",
        "DA:11,0",
    ]
    for line in expected:
        if line not in text:
            raise AssertionError(f"missing lcov line {line!r} in {text!r}")


def test_genhtml_command_keeps_function_coverage(module):
    command = module.genhtml_command("coverage info.info")
    if "--no-function-coverage" in command:
        raise AssertionError(f"genhtml command disabled function coverage: {command!r}")
    if "'coverage info.info'" not in command:
        raise AssertionError(f"genhtml command does not quote lcov path: {command!r}")


def test_main_rejects_mismatched_fingerprint():
    with tempfile.TemporaryDirectory() as tmp_dir:
        elf = Path(tmp_dir) / "mini.elf"
        src = Path(tmp_dir) / "mini.c"
        src.write_text("void wos_ktest_cov_fixture_entry(void) {}\n")
        subprocess.run(
            [
                "cc",
                "-g",
                "-ffreestanding",
                "-fno-builtin",
                "-nostdlib",
                "-nodefaultlibs",
                "-nostartfiles",
                "-static",
                str(src),
                "-Wl,-e,wos_ktest_cov_fixture_entry",
                "-Wl,--defsym,__kernel_text_start=0x10",
                "-Wl,--defsym,__kernel_text_end=0x20",
                "-o",
                str(elf),
            ],
            check=True,
        )

        log = Path(tmp_dir) / "serial.log"
        log.write_text(
            "[0.001] INFO kcov_elf: text_start=0x1 text_end=0x2 text_fnv64=0x3\n"
            "[0.002] INFO kcov_begin: 1\n"
            "[0.003] INFO kcov: 0x401000\n"
            "[0.004] INFO kcov_end:\n"
        )

        result = subprocess.run(
            [sys.executable, str(KTEST_COV), "--elf", str(elf), "--log", str(log), "--no-cache"],
            capture_output=True,
            text=True,
        )

    assert_equal(result.returncode, 2, "script mismatch return code")
    if "fingerprint mismatch" not in result.stderr:
        raise AssertionError(f"missing mismatch error in stderr: {result.stderr!r}")


def main():
    module = load_module()
    tests = [
        test_parse_fingerprint,
        test_extract_latest_kcov_block,
        test_extract_latest_kcov_report_tracks_truncation,
        test_extract_latest_kcov_report_rejects_incomplete_latest_block,
        test_build_coverage_map,
        test_coverage_exclusions_mark_uncoverable,
        test_unmapped_hit_lines,
        test_coverage_thresholds,
        test_coverage_threshold_arg,
        test_fingerprint_validation,
        test_truncation_validation,
        test_declared_count_validation,
        test_function_coverage_accounts_known_and_inline_hits,
        test_write_lcov_includes_function_records,
        test_genhtml_command_keeps_function_coverage,
    ]
    for test in tests:
        test(module)
    test_main_rejects_mismatched_fingerprint()
    print(f"{len(tests) + 1} ktest_cov tests passed")


if __name__ == "__main__":
    main()
