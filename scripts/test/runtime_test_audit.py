#!/usr/bin/env python3
"""Audit completed WOS runtime logs against the current source manifest."""

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
KTEST_DIR = ROOT / "modules" / "kern" / "src" / "test"
TESTD_MAIN = ROOT / "modules" / "testd" / "src" / "main.cpp"
USERLAND_SUITE = ROOT / "configs" / "drive" / "srv" / "wos_userland_suite.sh"

KTEST_DECL_RE = re.compile(
    r"\b(?P<kind>KTEST|KTEST_OFF)\(\s*(?P<suite>[A-Za-z_][A-Za-z0-9_]*)\s*,\s*"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\)\s*\{"
)
KTEST_LOG_PREFIX = r"(?:\[KTEST\]\s+|(?:\[[^\]]+\]\s+)?info\s+ktest:\s+)"
KTEST_START_RE = re.compile(KTEST_LOG_PREFIX + r"===\s+WOS Kernel Self-Test Suite\s+===")
KTEST_RUN_RE = re.compile(KTEST_LOG_PREFIX + r"RUN\s+(?P<key>\S+/\S+)")
KTEST_PASS_RE = re.compile(KTEST_LOG_PREFIX + r"PASS\s+(?P<key>\S+/\S+)")
KTEST_SKIP_RE = re.compile(KTEST_LOG_PREFIX + r"SKIP\s+(?P<key>\S+/\S+)")
KTEST_FAIL_RE = re.compile(KTEST_LOG_PREFIX + r"FAIL\b")
KTEST_SUMMARY_RE = re.compile(KTEST_LOG_PREFIX + r"===\s+(?P<passed>\d+)\s+passed,\s+(?P<failed>\d+)\s+failed\s+===")

TESTD_PASS_CALL_RE = re.compile(r"\bTESTD_(?:PASS|CHECK)\s*\(")
TESTD_PASS_LABEL_AT_RE = re.compile(r'\s*"((?:[^"\\]|\\.)*)"')
TESTD_BODY_RE = re.compile(
    r"^TESTD_RUN\((?P<name>\w+)\)\s*\{(?P<body>.*?)^TESTD_RUN_END\((?P=name)\)",
    flags=re.MULTILINE | re.DOTALL,
)
TESTD_START_RE = re.compile(r"\[TESTD\]\s+starting")
TESTD_PASS_RE = re.compile(r"\[TESTD\]\s+(?P<index>\d+)/(?P<total>\d+)\s+PASS:\s+(?P<label>\S+)")
TESTD_FAIL_RE = re.compile(r"\[TESTD\].*\bFAIL\b")
TESTD_DONE_RE = re.compile(r"\[TESTD\]\s+DONE:\s+(?P<passed>\d+)\s+passed,\s+(?P<failed>\d+)\s+failed")

USERLAND_CASE_RE = re.compile(r"^\s*(?:run_case|skip_case|require_exe)\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\b", re.MULTILINE)
USERLAND_RUN_START_RE = re.compile(r"^WOS userland suite$")
USERLAND_SUMMARY_START_RE = re.compile(r"^=== WOS USERLAND SUITE SUMMARY ===$")
USERLAND_BLOCK_START_RE = re.compile(r"^(?:WOS userland suite|=== WOS USERLAND SUITE SUMMARY ===)$")
USERLAND_ROW_RE = re.compile(r"^(?P<name>[^\t]+)\t(?P<status>PASS|FAIL|SKIP)\t(?P<detail>.*)$")
USERLAND_COUNTS_RE = re.compile(r"^PASS=(?P<passed>\d+)\s+FAIL=(?P<failed>\d+)\s+SKIP=(?P<skipped>\d+)$")


class AuditError(RuntimeError):
    pass


@dataclass(frozen=True)
class AuditResult:
    kind: str
    passed: int
    failed: int
    detail: str


def _duplicate_items(items: list[str]) -> list[str]:
    seen = set()
    duplicates = set()
    for item in items:
        if item in seen:
            duplicates.add(item)
            continue
        seen.add(item)
    return sorted(duplicates)


def _format_list(items: list[str], limit: int = 12) -> str:
    if len(items) <= limit:
        return ", ".join(items)
    return ", ".join(items[:limit]) + f", ... ({len(items) - limit} more)"


def _latest_block(lines: list[str], start_re: re.Pattern[str], done_re: re.Pattern[str], name: str) -> list[str]:
    current: list[str] | None = None
    latest: list[str] | None = None
    for line in lines:
        if start_re.search(line):
            current = [line]
            continue
        if current is not None:
            current.append(line)
            if done_re.search(line):
                latest = current
                current = None
    if current is not None:
        raise AuditError(f"{name} log has a newer incomplete run block")
    if latest is not None:
        return latest
    return lines


def _latest_started_block(lines: list[str], start_re: re.Pattern[str]) -> list[str]:
    latest_start = -1
    for index, line in enumerate(lines):
        if start_re.search(line):
            latest_start = index
    if latest_start < 0:
        return lines
    return lines[latest_start:]


def _mask_cxx_non_code_preserve_offsets(source: str) -> str:
    out: list[str] = []
    index = 0
    in_string = False
    in_char = False
    escaped = False

    def push_masked(char: str) -> None:
        out.append("\n" if char == "\n" else " ")

    def is_digit_separator_quote(offset: int) -> bool:
        if offset == 0 or offset + 1 >= len(source):
            return False
        previous = source[offset - 1]
        following = source[offset + 1]
        if not (previous.isalnum() or previous == "_"):
            return False
        if not (following.isalnum() or following == "_"):
            return False

        token_start = offset - 1
        while token_start >= 0 and (source[token_start].isalnum() or source[token_start] == "_"):
            token_start -= 1
        token_before_quote = source[token_start + 1 : offset]
        if token_before_quote in {"L", "U", "u", "u8"}:
            return False
        return any(char.isdigit() for char in token_before_quote)

    while index < len(source):
        char = source[index]
        next_char = source[index + 1] if index + 1 < len(source) else ""

        if in_string or in_char:
            push_masked(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif in_string and char == '"':
                in_string = False
            elif in_char and char == "'":
                in_char = False
            index += 1
            continue

        if char == "R" and next_char == '"':
            out.extend("  ")
            index += 2
            delimiter_start = index
            while index < len(source) and source[index] != "(":
                push_masked(source[index])
                index += 1
            delimiter = source[delimiter_start:index]
            if index >= len(source):
                break
            push_masked(source[index])
            index += 1
            terminator = ")" + delimiter + '"'
            while index < len(source):
                if source.startswith(terminator, index):
                    out.extend(" " for _ in terminator)
                    index += len(terminator)
                    break
                push_masked(source[index])
                index += 1
            continue
        if char == '"':
            in_string = True
            push_masked(char)
            index += 1
            continue
        if char == "'" and not is_digit_separator_quote(index):
            in_char = True
            push_masked(char)
            index += 1
            continue
        if char == "/" and next_char == "/":
            out.extend("  ")
            index += 2
            while index < len(source) and source[index] != "\n":
                push_masked(source[index])
                index += 1
            continue
        if char == "/" and next_char == "*":
            out.extend("  ")
            index += 2
            while index < len(source):
                if source[index] == "*" and index + 1 < len(source) and source[index + 1] == "/":
                    out.extend("  ")
                    index += 2
                    break
                push_masked(source[index])
                index += 1
            continue

        out.append(char)
        index += 1

    return "".join(out)


def ktest_manifest() -> tuple[set[str], set[str]]:
    enabled: set[str] = set()
    disabled: set[str] = set()
    for path in sorted(KTEST_DIR.rglob("*_ktest.cpp")):
        source = path.read_text()
        active_source = _mask_cxx_non_code_preserve_offsets(source)
        for match in KTEST_DECL_RE.finditer(active_source):
            key = f"{match.group('suite')}/{match.group('name')}"
            if match.group("kind") == "KTEST":
                enabled.add(key)
            else:
                disabled.add(key)
    if not enabled:
        raise AuditError("no enabled KTEST declarations found in source manifest")
    return enabled, disabled


def testd_expected_pass_labels() -> list[str]:
    source = TESTD_MAIN.read_text()
    active_source = _mask_cxx_non_code_preserve_offsets(source)
    labels: list[str] = []
    for match in TESTD_BODY_RE.finditer(active_source):
        source_body = source[match.start("body") : match.end("body")]
        active_body = active_source[match.start("body") : match.end("body")]
        for label_match in TESTD_PASS_CALL_RE.finditer(active_body):
            label = TESTD_PASS_LABEL_AT_RE.match(source_body, label_match.end())
            if label is not None:
                labels.append(label.group(1))
    if not labels:
        raise AuditError("no TESTD PASS/CHECK labels found in source manifest")
    duplicates = _duplicate_items(labels)
    if duplicates:
        raise AuditError(f"duplicate TESTD PASS/CHECK labels in source manifest: {_format_list(duplicates)}")
    return labels


def userland_expected_cases() -> set[str]:
    source = USERLAND_SUITE.read_text()
    cases = {match.group("name") for match in USERLAND_CASE_RE.finditer(source)}
    if not cases:
        raise AuditError("no userland suite cases found in source manifest")
    return cases


def audit_ktest_text(text: str) -> AuditResult:
    enabled, disabled = ktest_manifest()
    lines = _latest_block(text.splitlines(), KTEST_START_RE, KTEST_SUMMARY_RE, "KTEST")
    runs: list[str] = []
    passes: list[str] = []
    skips: list[str] = []
    saw_fail_line = False
    summary: tuple[int, int] | None = None

    for line in lines:
        if KTEST_FAIL_RE.search(line):
            saw_fail_line = True
        if match := KTEST_RUN_RE.search(line):
            runs.append(match.group("key"))
        if match := KTEST_PASS_RE.search(line):
            passes.append(match.group("key"))
        if match := KTEST_SKIP_RE.search(line):
            skips.append(match.group("key"))
        if match := KTEST_SUMMARY_RE.search(line):
            summary = (int(match.group("passed")), int(match.group("failed")))

    if summary is None:
        raise AuditError("KTEST log has no completed summary")
    if saw_fail_line:
        raise AuditError("KTEST log contains FAIL lines")
    if summary[1] != 0:
        raise AuditError(f"KTEST summary reports {summary[1]} failures")
    if summary[0] < len(enabled):
        raise AuditError(f"KTEST summary pass count {summary[0]} is below enabled test count {len(enabled)}")

    run_set = set(runs)
    pass_set = set(passes)
    skip_set = set(skips)
    duplicate_runs = _duplicate_items(runs)
    duplicate_passes = _duplicate_items(passes)
    duplicate_skips = _duplicate_items(skips)
    if duplicate_runs or duplicate_passes or duplicate_skips:
        raise AuditError(
            "KTEST log has duplicate entries: "
            f"RUN={_format_list(duplicate_runs)} PASS={_format_list(duplicate_passes)} SKIP={_format_list(duplicate_skips)}"
        )

    missing_runs = sorted(enabled - run_set)
    missing_passes = sorted(enabled - pass_set)
    unexpected_runs = sorted(run_set - enabled)
    unexpected_passes = sorted(pass_set - enabled)
    missing_skips = sorted(disabled - skip_set)
    unexpected_skips = sorted(skip_set - disabled)
    problems = []
    if missing_runs:
        problems.append(f"missing RUN for {_format_list(missing_runs)}")
    if missing_passes:
        problems.append(f"missing PASS for {_format_list(missing_passes)}")
    if unexpected_runs:
        problems.append(f"unexpected RUN for {_format_list(unexpected_runs)}")
    if unexpected_passes:
        problems.append(f"unexpected PASS for {_format_list(unexpected_passes)}")
    if missing_skips:
        problems.append(f"missing SKIP for disabled {_format_list(missing_skips)}")
    if unexpected_skips:
        problems.append(f"unexpected SKIP for {_format_list(unexpected_skips)}")
    if problems:
        raise AuditError("KTEST log/manifest mismatch: " + "; ".join(problems))

    return AuditResult(
        kind="ktest",
        passed=summary[0],
        failed=summary[1],
        detail=f"{len(enabled)} enabled tests passed; {len(disabled)} disabled tests skipped",
    )


def audit_testd_text(text: str) -> AuditResult:
    expected_labels = testd_expected_pass_labels()
    expected_set = set(expected_labels)
    all_lines = text.splitlines()
    lines = _latest_block(all_lines, TESTD_START_RE, TESTD_DONE_RE, "TESTD")
    latest_started_lines = _latest_started_block(all_lines, TESTD_START_RE)
    pass_entries: list[tuple[int, int, str]] = []
    fail_lines = [line.strip() for line in latest_started_lines if TESTD_FAIL_RE.search(line)]
    summary: tuple[int, int] | None = None

    for line in lines:
        if match := TESTD_PASS_RE.search(line):
            pass_entries.append((int(match.group("index")), int(match.group("total")), match.group("label")))
        if match := TESTD_DONE_RE.search(line):
            summary = (int(match.group("passed")), int(match.group("failed")))

    if summary is None:
        raise AuditError("TESTD log has no completed DONE summary")
    if fail_lines:
        raise AuditError("TESTD log contains FAIL lines: " + _format_list(fail_lines, limit=3))
    if summary != (len(expected_labels), 0):
        raise AuditError(f"TESTD summary {summary[0]} passed/{summary[1]} failed does not match expected {len(expected_labels)}/0")

    labels = [label for _, _, label in pass_entries]
    duplicates = _duplicate_items(labels)
    missing = sorted(expected_set - set(labels))
    unexpected = sorted(set(labels) - expected_set)
    if duplicates or missing or unexpected:
        problems = []
        if duplicates:
            problems.append(f"duplicate PASS labels {_format_list(duplicates)}")
        if missing:
            problems.append(f"missing PASS labels {_format_list(missing)}")
        if unexpected:
            problems.append(f"unexpected PASS labels {_format_list(unexpected)}")
        raise AuditError("TESTD log/manifest mismatch: " + "; ".join(problems))

    expected_total = len(expected_labels)
    for expected_index, (index, total, label) in enumerate(pass_entries, start=1):
        if index != expected_index or total != expected_total:
            raise AuditError(
                f"TESTD PASS counter mismatch at {label}: saw {index}/{total}, expected {expected_index}/{expected_total}"
            )

    return AuditResult(kind="testd", passed=summary[0], failed=summary[1], detail=f"{len(expected_labels)} PASS labels matched")


def audit_userland_text(text: str, allow_skips: bool = False) -> AuditResult:
    expected_cases = userland_expected_cases()
    lines = _latest_block(text.splitlines(), USERLAND_BLOCK_START_RE, USERLAND_COUNTS_RE, "userland suite")
    rows: list[tuple[str, str, str]] = []
    counts: tuple[int, int, int] | None = None

    for line in lines:
        if match := USERLAND_ROW_RE.search(line):
            rows.append((match.group("name"), match.group("status"), match.group("detail")))
        if match := USERLAND_COUNTS_RE.search(line):
            counts = (int(match.group("passed")), int(match.group("failed")), int(match.group("skipped")))

    if counts is None:
        raise AuditError("userland suite log has no completed PASS/FAIL/SKIP summary")

    names = [name for name, _, _ in rows]
    duplicate_names = _duplicate_items(names)
    missing = sorted(expected_cases - set(names))
    unexpected = sorted(set(names) - expected_cases)
    fail_rows = [f"{name}:{detail}" for name, status, detail in rows if status == "FAIL"]
    if duplicate_names or missing or unexpected or fail_rows:
        problems = []
        if duplicate_names:
            problems.append(f"duplicate case rows {_format_list(duplicate_names)}")
        if missing:
            problems.append(f"missing case rows {_format_list(missing)}")
        if unexpected:
            problems.append(f"unexpected case rows {_format_list(unexpected)}")
        if fail_rows:
            problems.append(f"failed cases {_format_list(fail_rows, limit=3)}")
        raise AuditError("userland suite summary mismatch: " + "; ".join(problems))

    passed = sum(1 for _, status, _ in rows if status == "PASS")
    failed = sum(1 for _, status, _ in rows if status == "FAIL")
    skipped = sum(1 for _, status, _ in rows if status == "SKIP")
    if counts != (passed, failed, skipped):
        raise AuditError(
            f"userland suite count mismatch: summary {counts[0]} PASS/{counts[1]} FAIL/{counts[2]} SKIP "
            f"but rows contain {passed} PASS/{failed} FAIL/{skipped} SKIP"
        )
    if counts[1] != 0:
        raise AuditError(f"userland suite summary reports {counts[1]} failures")
    if counts[2] != 0 and not allow_skips:
        raise AuditError(
            f"userland suite summary reports {counts[2]} skipped cases; "
            "rerun with all dependencies enabled or pass --allow-userland-skips for triage-only audits"
        )

    return AuditResult(
        kind="userland",
        passed=counts[0],
        failed=counts[1],
        detail=f"{len(expected_cases)} cases accounted; {counts[2]} skipped",
    )


def audit_log(log_path: Path, kind: str, allow_userland_skips: bool = False) -> list[AuditResult]:
    text = log_path.read_text(errors="replace")
    selected: list[str]
    if kind == "auto":
        has_ktest = "[KTEST]" in text or "info ktest:" in text
        has_testd = "[TESTD]" in text
        has_userland = any(
            USERLAND_RUN_START_RE.search(line) or USERLAND_SUMMARY_START_RE.search(line) for line in text.splitlines()
        )
        selected = []
        if has_ktest:
            selected.append("ktest")
        if has_testd:
            selected.append("testd")
        if has_userland:
            selected.append("userland")
        if not selected:
            raise AuditError(f"{log_path}: no KTEST, TESTD, or userland suite markers found")
    elif kind == "both":
        selected = ["ktest", "testd"]
    elif kind == "all":
        selected = ["ktest", "testd", "userland"]
    else:
        selected = [kind]

    results: list[AuditResult] = []
    if "ktest" in selected:
        results.append(audit_ktest_text(text))
    if "testd" in selected:
        results.append(audit_testd_text(text))
    if "userland" in selected:
        results.append(audit_userland_text(text, allow_skips=allow_userland_skips))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit completed WOS runtime logs against the current source manifest")
    parser.add_argument("--kind", choices=("auto", "ktest", "testd", "both", "userland", "all"), default="auto")
    parser.add_argument(
        "--allow-userland-skips",
        action="store_true",
        help="Accept SKIP rows in the userland suite summary for triage-only audits",
    )
    parser.add_argument("log", type=Path, help="Completed serial or test log to audit")
    args = parser.parse_args(argv)

    try:
        results = audit_log(args.log, args.kind, allow_userland_skips=args.allow_userland_skips)
    except AuditError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    for result in results:
        print(f"{result.kind}: PASS ({result.detail}; summary {result.passed} passed, {result.failed} failed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
