#!/usr/bin/env python3
"""Exercise incident fixtures through WOSDBG's shared JSON CLI backend.

The test intentionally has no build-system dependency.  It can run against an
arbitrary WOSDBG binary with ``--wosdbg PATH`` and exits successfully with a
clear SKIP when that binary predates the incident catalog.  Pass
``--require-incident-tools`` in CI/integration runs where absence is a failure.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Iterable

sys.dont_write_bytecode = True
import generate_incident_fixtures as fixture_generator


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_WOSDBG = REPO_ROOT / "tools" / "build" / "bin" / "wosdbg"
INCIDENT_TOOLS = {
    "wosdbg.load_incident": "path",
    "wosdbg.validate_incident": "path",
    "wosdbg.get_incident_inventory": "incidentId",
    "wosdbg.summarize_incident": "incidentId",
}
COMMON_RESULT_FIELDS = {
    "ok",
    "valid",
    "incidentId",
    "formatVersion",
    "inventory",
    "issues",
    "degraded",
}
SUMMARY_RESULT_FIELDS = COMMON_RESULT_FIELDS | {
    "clockQuality",
    "evidence",
    "timeline",
    "semanticDigest",
}
SEMANTIC_DIGEST_RE = re.compile(r"(?:sha256:)?[0-9a-f]{64}\Z")


class TestFailure(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise TestFailure(message)


def load_json_output(command: list[str], *, cwd: Path, input_text: str | None = None, timeout: float) -> tuple[int, dict[str, Any]]:
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            input=input_text,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as error:
        raise TestFailure(f"command timed out after {timeout:g}s: {' '.join(command)}") from error
    try:
        parsed = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        stderr = completed.stderr.strip()
        raise TestFailure(
            f"command did not return JSON (status {completed.returncode}): {' '.join(command)}"
            + (f"; stderr: {stderr}" if stderr else "")
        ) from error
    require(isinstance(parsed, dict), f"command returned a non-object JSON value: {' '.join(command)}")
    return completed.returncode, parsed


def catalog(binary: Path, *, cwd: Path, timeout: float) -> dict[str, dict[str, Any]]:
    status, result = load_json_output([str(binary), "--list-tools"], cwd=cwd, timeout=timeout)
    require(status == 0, f"--list-tools returned status {status}")
    entries = result.get("tools")
    require(isinstance(entries, list), "--list-tools result has no tools array")
    by_name: dict[str, dict[str, Any]] = {}
    for entry in entries:
        require(isinstance(entry, dict), "tool catalog contains a non-object entry")
        name = entry.get("name")
        require(isinstance(name, str) and name, "tool catalog entry has no non-empty name")
        require(name not in by_name, f"tool catalog contains duplicate entry {name}")
        by_name[name] = entry
    return by_name


def check_incident_catalog(by_name: dict[str, dict[str, Any]]) -> list[str]:
    missing = sorted(set(INCIDENT_TOOLS) - set(by_name))
    if missing:
        return missing
    for name, required_argument in INCIDENT_TOOLS.items():
        schema = by_name[name].get("inputSchema")
        require(isinstance(schema, dict), f"{name}: inputSchema is not an object")
        require(schema.get("type") == "object", f"{name}: inputSchema type is not object")
        properties = schema.get("properties")
        required = schema.get("required")
        require(isinstance(properties, dict), f"{name}: inputSchema properties is not an object")
        require(isinstance(required, list), f"{name}: inputSchema required is not an array")
        require(required_argument in properties, f"{name}: schema omits {required_argument} property")
        require(required_argument in required, f"{name}: schema does not require {required_argument}")
        for required_name in required:
            require(required_name in properties, f"{name}: required property {required_name!r} has no schema")
    return []


def invoke_tool(binary: Path, *, cwd: Path, tool: str, arguments: dict[str, Any], timeout: float) -> dict[str, Any]:
    status, result = load_json_output(
        [str(binary), "--tool", tool, "--arguments", json.dumps(arguments, sort_keys=True)],
        cwd=cwd,
        timeout=timeout,
    )
    require(status in {0, 1}, f"{tool}: unexpected process status {status}")
    return result


def invoke_batch(binary: Path, *, cwd: Path, calls: list[dict[str, Any]], timeout: float) -> dict[str, dict[str, Any]]:
    payload = json.dumps({"calls": calls, "continueOnError": True}, sort_keys=True)
    status, output = load_json_output([str(binary), "--batch", "-"], cwd=cwd, input_text=payload, timeout=timeout)
    require(status in {0, 1}, f"batch: unexpected process status {status}")
    entries = output.get("calls")
    require(isinstance(entries, list), "batch result has no calls array")
    results: dict[str, dict[str, Any]] = {}
    for entry in entries:
        require(isinstance(entry, dict), "batch calls contains a non-object entry")
        call_id = entry.get("id")
        result = entry.get("result")
        require(isinstance(call_id, str) and call_id, "batch result has no call id")
        require(isinstance(result, dict), f"batch result {call_id} is not an object")
        require(call_id not in results, f"batch returned duplicate id {call_id}")
        results[call_id] = result
    require(len(results) == len(calls), f"batch returned {len(results)} of {len(calls)} requested calls")
    return results


def issue_codes(value: Any, *, inside_issues: bool = False) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        code = value.get("code") if inside_issues else None
        if isinstance(code, str) and code:
            found.add(code)
        for key, child in value.items():
            found.update(issue_codes(child, inside_issues=inside_issues or key in {"issues", "loadIssues"}))
    elif isinstance(value, list):
        for child in value:
            found.update(issue_codes(child, inside_issues=inside_issues))
    return found


def assert_issue_contract(case_name: str, value: Any, expected: Iterable[str]) -> None:
    actual = issue_codes(value)
    for code in expected:
        require(code in actual, f"{case_name}: expected exact issue code {code!r}, got {sorted(actual)!r}")
    require(all(re.fullmatch(r"[a-z][a-z0-9_]*", code) for code in actual),
            f"{case_name}: issue codes must be stable snake_case identifiers: {sorted(actual)!r}")


def assert_fields(case_name: str, operation: str, result: dict[str, Any], required: set[str]) -> None:
    missing = sorted(required - set(result))
    require(not missing, f"{case_name}/{operation}: result omits stable fields {missing}")
    require(isinstance(result["ok"], bool), f"{case_name}/{operation}: ok is not boolean")
    require(isinstance(result["valid"], bool), f"{case_name}/{operation}: valid is not boolean")
    require(isinstance(result["incidentId"], str), f"{case_name}/{operation}: incidentId is not a string")
    require(isinstance(result["formatVersion"], int) and result["formatVersion"] >= 0,
            f"{case_name}/{operation}: formatVersion is not a nonnegative integer")
    require(isinstance(result["inventory"], (dict, list)), f"{case_name}/{operation}: inventory is not structured")
    require(isinstance(result["issues"], list), f"{case_name}/{operation}: issues is not an array")
    require(isinstance(result["degraded"], bool), f"{case_name}/{operation}: degraded is not boolean")


def bundle_manifest(path: Path) -> dict[str, Any]:
    manifest, _ = fixture_generator.manifest_from_bundle(path)
    return manifest


def inventory_members(result: dict[str, Any]) -> list[dict[str, Any]]:
    inventory = result["inventory"]
    if isinstance(inventory, list):
        members = inventory
    else:
        members = inventory.get("members")
    require(isinstance(members, list), "inventory does not contain a members array")
    require(all(isinstance(member, dict) for member in members), "inventory contains a non-object member")
    return members


def assert_inventory(case_name: str, result: dict[str, Any], manifest: dict[str, Any], fixture_root: Path) -> None:
    members = inventory_members(result)
    paths = [member.get("path") for member in members]
    require(all(isinstance(path, str) for path in paths), f"{case_name}: inventory contains a non-string path")
    require(paths == sorted(paths), f"{case_name}: inventory is not sorted by member path")
    expected_paths = [member["path"] for member in manifest["members"]]
    require(paths == expected_paths, f"{case_name}: inventory paths differ from manifest: {paths!r}")
    require(not walk_named(result, "absolutePath"), f"{case_name}: inventory exposes private snapshot absolutePath values")
    for expected, actual in zip(manifest["members"], members, strict=True):
        for key in ("path", "kind", "sha256", "required", "truncated", "sourceName"):
            require(actual.get(key) == expected.get(key), f"{case_name}: inventory field {key} differs for {expected['path']}")
        require(str(actual.get("size")) == str(expected["size"]),
                f"{case_name}: inventory field size differs for {expected['path']}")
        if "nodeId" in expected:
            require(str(actual.get("nodeId")) == str(expected["nodeId"]),
                    f"{case_name}: inventory lost numeric nodeId for {expected['path']}")


def coredump_versions(value: Any, *, in_coredump: bool = False) -> list[int]:
    versions: list[int] = []
    if isinstance(value, dict):
        this_is_coredump = in_coredump or value.get("kind") == "coredump" or "dumpId" in value
        for key, child in value.items():
            if this_is_coredump and key in {"coredumpVersion", "detectedVersion", "dumpVersion", "version"}:
                if isinstance(child, int):
                    versions.append(child)
                elif isinstance(child, str) and child.isdecimal():
                    versions.append(int(child))
            versions.extend(coredump_versions(child, in_coredump=this_is_coredump))
    elif isinstance(value, list):
        for child in value:
            versions.extend(coredump_versions(child, in_coredump=in_coredump))
    return versions


def walk_named(value: Any, name: str) -> list[Any]:
    found: list[Any] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key == name:
                found.append(child)
            found.extend(walk_named(child, name))
    elif isinstance(value, list):
        for child in value:
            found.extend(walk_named(child, name))
    return found


def assert_response_bounds(case_name: str, summary: dict[str, Any], max_events: int) -> None:
    event_arrays = [value for value in walk_named(summary.get("timeline"), "events") if isinstance(value, list)]
    require(event_arrays, f"{case_name}: bounded summary exposes no timeline events array")
    require(all(len(events) <= max_events for events in event_arrays),
            f"{case_name}: timeline exceeds requested maxEvents={max_events}")
    require(True in walk_named(summary.get("timeline"), "truncated"),
            f"{case_name}: bounded timeline does not report truncation")
    event_text = json.dumps(event_arrays, sort_keys=True)
    require("0.log" in event_text and "1.log" in event_text,
            f"{case_name}: bounded timeline does not retain evidence from both active log lanes")


def assert_honest_non_comparable_clock(case_name: str, summary: dict[str, Any]) -> None:
    timeline = summary.get("timeline")
    for field in ("globalOrderAvailable", "globallyComparable", "crossNodeComparable"):
        require(True not in walk_named(timeline, field), f"{case_name}: timeline falsely reports {field}=true")
    for ordered in walk_named(timeline, "clockOrderedEvents"):
        require(not isinstance(ordered, list) or not ordered,
                f"{case_name}: non-comparable clocks produced a cross-node clockOrderedEvents list")


def manifest_incident_id(manifest: dict[str, Any]) -> str:
    incident_id = manifest.get("incidentId")
    require(isinstance(incident_id, str) and incident_id.startswith("sha256:"), "fixture has no content-derived incidentId")
    return incident_id


def full_calls(path: Path, expected: dict[str, Any]) -> list[dict[str, Any]]:
    summary_arguments: dict[str, Any] = {"incidentId": "$load.incidentId"}
    if "maxEvents" in expected:
        summary_arguments["maxEvents"] = expected["maxEvents"]
    return [
        {"id": "validate", "tool": "validate_incident", "arguments": {"path": str(path)}},
        {"id": "load", "tool": "load_incident", "arguments": {"path": str(path)}},
        {
            "id": "inventory",
            "tool": "get_incident_inventory",
            "arguments": {"incidentId": "$load.incidentId", "start": 0, "count": 10000},
        },
        {"id": "summary1", "tool": "summarize_incident", "arguments": summary_arguments},
        {"id": "summary2", "tool": "summarize_incident", "arguments": summary_arguments},
    ]


def check_full_case(binary: Path, fixture_root: Path, case: dict[str, Any], timeout: float) -> None:
    name = case["name"]
    expected = case["expected"]
    path = fixture_root / case["path"]
    manifest = bundle_manifest(path)
    calls = full_calls(path, expected)
    first = invoke_batch(binary, cwd=fixture_root, calls=calls, timeout=timeout)
    second = invoke_batch(binary, cwd=fixture_root, calls=calls, timeout=timeout)

    for operation in ("load", "validate", "inventory"):
        assert_fields(name, operation, first[operation], COMMON_RESULT_FIELDS)
    for operation in ("summary1", "summary2"):
        assert_fields(name, operation, first[operation], SUMMARY_RESULT_FIELDS)

    manifest_id = manifest_incident_id(manifest)
    session_id = first["load"]["incidentId"]
    require(first["validate"]["incidentId"] == manifest_id, f"{name}/validate: incidentId differs from manifest")
    require(isinstance(session_id, str) and session_id, f"{name}/load: session incidentId is empty")
    for operation, result in first.items():
        if operation != "validate":
            require(result["incidentId"] == session_id, f"{name}/{operation}: incidentId differs from loaded session")
        require(result["formatVersion"] == 1, f"{name}/{operation}: formatVersion is not 1")
        require(result["valid"] is expected["valid"], f"{name}/{operation}: valid differs from expected")
        require(result["degraded"] is expected["degraded"], f"{name}/{operation}: degraded differs from expected")
    require(first["load"]["ok"], f"{name}: load operation failed")
    require(first["validate"]["ok"], f"{name}: validate operation failed")
    require(first["inventory"]["ok"], f"{name}: inventory operation failed")
    require(first["summary1"]["ok"], f"{name}: summarize operation failed")

    assert_inventory(name, first["inventory"], manifest, fixture_root)
    all_results = list(first.values())
    assert_issue_contract(name, all_results, expected["issueCodes"])
    if not expected["issueCodes"]:
        require(not issue_codes(all_results), f"{name}: clean fixture unexpectedly reports {sorted(issue_codes(all_results))!r}")

    summary1 = first["summary1"]
    summary2 = first["summary2"]
    semantic = summary1.get("semantic")
    if semantic is not None:
        require(isinstance(semantic, dict), f"{name}: semantic projection is not an object")
        require(str(fixture_root) not in json.dumps(semantic, sort_keys=True),
                f"{name}: normalized semantic projection leaks its input/snapshot root")
    require(summary1 == summary2, f"{name}: repeated summarize calls differ within one backend session")
    require(summary1 == second["summary1"], f"{name}: normalized summary differs across fresh backend processes")
    digest = summary1["semanticDigest"]
    require(isinstance(digest, str) and SEMANTIC_DIGEST_RE.fullmatch(digest) is not None,
            f"{name}: semanticDigest is not a SHA-256 value")
    require(digest == summary2["semanticDigest"] == second["summary1"]["semanticDigest"],
            f"{name}: semanticDigest changed across repeated summaries")

    if "clockQuality" in expected:
        require(summary1["clockQuality"] == expected["clockQuality"],
                f"{name}: expected clockQuality {expected['clockQuality']!r}, got {summary1['clockQuality']!r}")
        timeline = summary1["timeline"]
        require(isinstance(timeline, dict), f"{name}: timeline is not an object")
        require(timeline.get("manifestClockQuality") == expected["clockQuality"],
                f"{name}: timeline does not preserve declared manifest clock quality")
    if "timelineClockQuality" in expected:
        require(summary1["timeline"].get("clockQuality") == expected["timelineClockQuality"],
                f"{name}: expected timeline clockQuality {expected['timelineClockQuality']!r}, "
                f"got {summary1['timeline'].get('clockQuality')!r}")
    if "coredumpVersions" in expected:
        actual_versions = coredump_versions(summary1["evidence"])
        require(sorted(actual_versions) == sorted(expected["coredumpVersions"]),
                f"{name}: coredump evidence versions are {actual_versions!r}, expected {expected['coredumpVersions']!r}")
    if "maxEvents" in expected:
        assert_response_bounds(name, summary1, expected["maxEvents"])
    if expected.get("clockQuality") == "non-comparable":
        assert_honest_non_comparable_clock(name, summary1)


def check_invalid_loaded_case(binary: Path, fixture_root: Path, case: dict[str, Any], timeout: float) -> None:
    name = case["name"]
    expected = case["expected"]
    path = fixture_root / case["path"]
    manifest = bundle_manifest(path)
    calls = [
        {"id": "validate", "tool": "validate_incident", "arguments": {"path": str(path)}},
        {"id": "load", "tool": "load_incident", "arguments": {"path": str(path)}},
    ]
    results = invoke_batch(binary, cwd=fixture_root, calls=calls, timeout=timeout)
    for operation in ("load", "validate"):
        result = results[operation]
        assert_fields(name, operation, result, COMMON_RESULT_FIELDS)
        require(result["formatVersion"] == 1, f"{name}/{operation}: formatVersion is not 1")
        require(result["valid"] is False, f"{name}/{operation}: invalid coredump fixture was accepted")
        require(result["degraded"] is True, f"{name}/{operation}: invalid fixture is not marked degraded")
    require(results["validate"]["incidentId"] == manifest_incident_id(manifest),
            f"{name}/validate: incidentId differs from manifest")
    require(isinstance(results["load"]["incidentId"], str) and results["load"]["incidentId"],
            f"{name}/load: invalid evidence did not retain an incident session")
    assert_issue_contract(name, list(results.values()), expected["issueCodes"])


def check_rejected_case(binary: Path, fixture_root: Path, case: dict[str, Any], timeout: float) -> None:
    name = case["name"]
    path = fixture_root / case["path"]
    results = {
        operation: invoke_tool(binary, cwd=fixture_root, tool=operation, arguments={"path": str(path)}, timeout=timeout)
        for operation in ("validate_incident", "load_incident")
    }
    for operation, result in results.items():
        assert_fields(name, operation, result, COMMON_RESULT_FIELDS)
        require(result["valid"] is False, f"{name}/{operation}: hostile/corrupt bundle was accepted")
        assert_issue_contract(name, result, case["expected"]["issueCodes"])


def selected(case: dict[str, Any], patterns: list[str]) -> bool:
    return not patterns or any(fnmatch.fnmatchcase(case["name"], pattern) for pattern in patterns)


def verify_collector_partition(index: dict[str, Any], fixture_root: Path) -> None:
    loader_paths = {case["path"] for case in index["loaderCases"]}
    security_paths = {case["path"] for case in index["securityCases"]}
    for case in index["collectorCases"]:
        require(case["path"] not in loader_paths | security_paths,
                f"collector-only fixture {case['name']} is also routed to the loader")
        path = fixture_root / case["path"]
        require(path.is_dir(), f"collector-only fixture {case['name']} is not a raw artifact directory")
        require(not (path / "manifest.json").exists(),
                f"collector-only fixture {case['name']} unexpectedly looks like an already collected bundle")


def run_cases(binary: Path, fixture_root: Path, index: dict[str, Any], args: argparse.Namespace) -> int:
    modes = {"loader", "security", "collector"} if args.mode == "all" else {args.mode}
    verify_collector_partition(index, fixture_root)
    if modes == {"collector"}:
        print(f"PASS collector fixture partition ({len(index['collectorCases'])} raw cases)")
        return 0

    if not binary.is_file():
        message = f"WOSDBG binary is absent: {binary}"
        if args.require_incident_tools:
            raise TestFailure(message)
        print(f"SKIP incident CLI semantic/security: {message}; fixtures verified")
        return 0

    tool_catalog = catalog(binary, cwd=fixture_root, timeout=args.timeout)
    missing = check_incident_catalog(tool_catalog)
    if missing:
        message = "incident catalog is not implemented: " + ", ".join(missing)
        if args.require_incident_tools:
            raise TestFailure(message)
        print(f"SKIP incident CLI semantic/security: {message}; fixtures verified")
        return 0

    executed = 0
    if "loader" in modes:
        for case in index["loaderCases"]:
            if not selected(case, args.case):
                continue
            workflow = case["workflow"]
            if workflow == "full":
                check_full_case(binary, fixture_root, case, args.timeout)
            elif workflow == "validate-invalid":
                check_invalid_loaded_case(binary, fixture_root, case, args.timeout)
            elif workflow == "load-reject":
                check_rejected_case(binary, fixture_root, case, args.timeout)
            else:
                raise TestFailure(f"{case['name']}: unknown fixture workflow {workflow!r}")
            print(f"PASS {case['name']}")
            executed += 1
    if "security" in modes:
        for case in index["securityCases"]:
            if not selected(case, args.case):
                continue
            check_rejected_case(binary, fixture_root, case, args.timeout)
            print(f"PASS {case['name']}")
            executed += 1
    require(executed > 0, "no fixture cases matched the selected mode/patterns")
    print(f"PASS incident CLI catalog, dispatch, semantics, security, and determinism ({executed} cases)")
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--wosdbg", type=Path, default=DEFAULT_WOSDBG, help="WOSDBG executable to test")
    result.add_argument("--fixtures", type=Path, help="Existing generated fixture root (default: temporary corpus)")
    result.add_argument("--mode", choices=("all", "loader", "security", "collector"), default="all")
    result.add_argument("--case", action="append", default=[], metavar="GLOB", help="Run matching case name(s)")
    result.add_argument("--timeout", type=float, default=30.0, help="Per-process timeout in seconds")
    result.add_argument(
        "--require-incident-tools",
        action="store_true",
        help="Fail instead of skipping when WOSDBG or the four incident tools are absent",
    )
    result.add_argument("--list-cases", action="store_true", help="Print case names after verifying/generating fixtures")
    return result


def main() -> int:
    args = parser().parse_args()
    require(args.timeout > 0, "--timeout must be positive")
    binary = args.wosdbg.resolve()

    with tempfile.TemporaryDirectory(prefix="wosdbg-incident-test-") as generated_temporary:
        if args.fixtures is None:
            fixture_root = Path(generated_temporary)
            index = fixture_generator.generate(fixture_root)
            with tempfile.TemporaryDirectory(prefix="wosdbg-incident-repeat-") as repeated_temporary:
                repeated_root = Path(repeated_temporary)
                fixture_generator.generate(repeated_root)
                require(
                    fixture_generator.tree_digest(fixture_root) == fixture_generator.tree_digest(repeated_root),
                    "fixture generator is not byte-for-byte deterministic",
                )
        else:
            fixture_root = args.fixtures.resolve()
            fixture_generator.verify_generated_tree(fixture_root)
            index = json.loads((fixture_root / "cases.json").read_text(encoding="utf-8"))

        if args.list_cases:
            for group in ("loaderCases", "securityCases", "collectorCases"):
                for case in index[group]:
                    print(f"{group}: {case['name']}")
            return 0
        return run_cases(binary, fixture_root, index, args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TestFailure as error:
        print(f"FAIL: {error}", file=sys.stderr)
        raise SystemExit(1) from error
