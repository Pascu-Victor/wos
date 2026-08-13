#!/usr/bin/env python3
"""Exercise incident fixtures through WOSDBG's shared JSON CLI backend.

The test intentionally has no build-system dependency.  It can run against an
arbitrary WOSDBG binary with ``--wosdbg PATH`` and exits successfully with a
clear SKIP when that binary predates the incident catalog.  Pass
``--require-incident-tools`` in CI/integration runs where absence is a failure.
"""

from __future__ import annotations

import argparse
import copy
import fnmatch
import hashlib
import http.client
import json
import random
import re
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
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
    "issueCount",
    "issuesReturned",
    "issuesTruncated",
    "degraded",
}
SUMMARY_RESULT_FIELDS = COMMON_RESULT_FIELDS | {
    "analysisIssues",
    "clockQuality",
    "evidence",
    "loadIssues",
    "semantic",
    "semanticProjectionVersion",
    "timeline",
    "semanticDigest",
}
SEMANTIC_DIGEST_RE = re.compile(r"(?:sha256:)?[0-9a-f]{64}\Z")
MCP_PROTOCOL_VERSION = "2025-11-25"
GUI_TOOL_CATALOG_REQUEST = 24
GUI_TOOL_CATALOG_RESPONSE = 25
GUI_TOOL_CALL_REQUEST = 26
GUI_TOOL_CALL_RESPONSE = 27
MAX_INTERFACE_JSON_BYTES = 2 * 1024 * 1024


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
    require(len(completed.stdout.encode("utf-8")) <= MAX_INTERFACE_JSON_BYTES,
            f"command returned more than {MAX_INTERFACE_JSON_BYTES} JSON bytes: {' '.join(command)}")
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
        require(isinstance(properties[required_argument], dict) and properties[required_argument].get("type") == "string",
                f"{name}: required {required_argument} property is not a string schema")
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
            found.update(
                issue_codes(
                    child,
                    inside_issues=inside_issues
                    or key in {"issues", "validationIssues", "loadIssues", "analysisIssues"},
                )
            )
    elif isinstance(value, list):
        for child in value:
            found.update(issue_codes(child, inside_issues=inside_issues))
    return found


def assert_issue_contract(case_name: str, value: Any, expected: Iterable[str]) -> None:
    actual = issue_codes(value)
    expected_set = set(expected)
    require(actual == expected_set,
            f"{case_name}: issue-code set differs: expected {sorted(expected_set)!r}, got {sorted(actual)!r}")
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


def integer_field(value: Any, *, context: str) -> int:
    if isinstance(value, bool):
        raise TestFailure(f"{context}: boolean is not an integer field")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdecimal():
        return int(value)
    raise TestFailure(f"{context}: expected a nonnegative integer, got {value!r}")


def assert_inventory_page(
    case_name: str,
    result: dict[str, Any],
    manifest: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    inventory = result["inventory"]
    require(isinstance(inventory, dict), f"{case_name}: paged inventory is not an object")
    start = expected["start"]
    count = expected["count"]
    members = inventory.get("members")
    require(isinstance(members, list), f"{case_name}: paged inventory has no members array")
    require(integer_field(inventory.get("start"), context=f"{case_name}/inventory.start") == start,
            f"{case_name}: paged inventory start differs")
    require(integer_field(inventory.get("count"), context=f"{case_name}/inventory.count") == len(members),
            f"{case_name}: paged inventory count differs from returned members")
    require(integer_field(inventory.get("total"), context=f"{case_name}/inventory.total") == len(manifest["members"]),
            f"{case_name}: paged inventory total differs from manifest")
    require(len(members) <= count, f"{case_name}: paged inventory exceeds requested count={count}")
    expected_paths = [member["path"] for member in manifest["members"]][start : start + count]
    require([member.get("path") for member in members] == expected_paths,
            f"{case_name}: paged inventory returned the wrong deterministic slice")


def assert_summary_bounds(case_name: str, summary: dict[str, Any], expected: dict[str, Any]) -> None:
    if "maxCoredumps" in expected:
        coredumps = summary.get("coredumps")
        require(isinstance(coredumps, list), f"{case_name}: summary has no coredumps array")
        require(len(coredumps) <= expected["maxCoredumps"],
                f"{case_name}: summary exceeds maxCoredumps={expected['maxCoredumps']}")
        if len(coredump_versions(summary.get("evidence"))) > expected["maxCoredumps"]:
            require(summary.get("coredumpsTruncated") is True,
                    f"{case_name}: bounded coredump summary does not report truncation")
    if "maxIssues" in expected:
        issue_arrays = []
        for field in ("issues", "loadIssues", "analysisIssues"):
            values = summary.get(field)
            require(isinstance(values, list), f"{case_name}: summary {field} is not an array")
            issue_arrays.append(values)
        issues_returned = sum(len(values) for values in issue_arrays)
        require(issues_returned <= expected["maxIssues"],
                f"{case_name}: cumulative issue arrays exceed maxIssues={expected['maxIssues']}")
        require(integer_field(summary.get("issuesReturned"), context=f"{case_name}.issuesReturned") == issues_returned,
                f"{case_name}: issuesReturned differs from the cumulative issue arrays")
        issue_count = integer_field(summary.get("issueCount"), context=f"{case_name}.issueCount")
        require(issue_count >= issues_returned, f"{case_name}: issueCount is smaller than returned issue arrays")
        require(summary.get("issuesTruncated") is (issue_count > issues_returned),
                f"{case_name}: issuesTruncated does not describe logical issue-count truncation")
        semantic = summary.get("semantic")
        require(isinstance(semantic, dict), f"{case_name}: semantic replay projection is not an object")
        semantic_issue_arrays = []
        for field in ("validationIssues", "loadIssues", "analysisIssues"):
            values = semantic.get(field)
            require(isinstance(values, list), f"{case_name}: semantic {field} is not an array")
            semantic_issue_arrays.append(values)
        semantic_returned = sum(len(values) for values in semantic_issue_arrays)
        require(semantic_returned == issues_returned,
                f"{case_name}: semantic replay issue budget differs from the top-level response")
        chunk_hits = [
            hit
            for chunk_result in walk_named(summary.get("coredumps"), "chunkCorruption")
            if isinstance(chunk_result, dict) and isinstance(chunk_result.get("hits"), list)
            for hit in chunk_result["hits"]
        ]
        require(len(chunk_hits) <= expected["maxIssues"],
                f"{case_name}: aggregate chunk scan exceeds maxIssues={expected['maxIssues']}")
    if "issueCount" in expected:
        require(integer_field(summary.get("issueCount"), context=f"{case_name}.issueCount") == expected["issueCount"],
                f"{case_name}: logical issue count differs from expected")
    if "issuesReturned" in expected:
        require(integer_field(summary.get("issuesReturned"), context=f"{case_name}.issuesReturned")
                == expected["issuesReturned"], f"{case_name}: issuesReturned differs from expected")
    if "issuesTruncated" in expected:
        require(summary.get("issuesTruncated") is expected["issuesTruncated"],
                f"{case_name}: issuesTruncated differs from expected")
    if "maxSerializedBytes" in expected:
        serialized = json.dumps(summary, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        require(len(serialized) <= expected["maxSerializedBytes"],
                f"{case_name}: compact summary is {len(serialized)} bytes, exceeds {expected['maxSerializedBytes']}")


def assert_chunk_corruption(case_name: str, summary: dict[str, Any], expected: dict[str, Any]) -> None:
    if "minChunkCorruptionHits" not in expected:
        return
    chunk_results = [value for value in walk_named(summary.get("coredumps"), "chunkCorruption") if isinstance(value, dict)]
    require(chunk_results, f"{case_name}: summary exposes no chunkCorruption result")
    hits = [hit for result in chunk_results for hit in result.get("hits", []) if isinstance(hit, dict)]
    require(len(hits) >= expected["minChunkCorruptionHits"],
            f"{case_name}: expected at least {expected['minChunkCorruptionHits']} corrupt-chunk hit(s), got {len(hits)}")
    if "chunkSize" in expected:
        sizes: set[int] = set()
        for hit in hits:
            for clue in hit.get("chunkClues", []):
                if not isinstance(clue, dict):
                    continue
                value = clue.get("chunkSize")
                if isinstance(value, str):
                    try:
                        sizes.add(int(value, 0))
                    except ValueError:
                        pass
        require(expected["chunkSize"] in sizes,
                f"{case_name}: chunk clues omit expected size {expected['chunkSize']}, got {sorted(sizes)!r}")
    if "corrupt_chunk" in set(expected["issueCodes"]) | set(expected.get("summaryIssueCodes", [])):
        issues = summary.get("analysisIssues")
        require(isinstance(issues, list), f"{case_name}: analysisIssues is not an array")
        corrupt_issues = [issue for issue in issues if isinstance(issue, dict) and issue.get("code") == "corrupt_chunk"]
        require(len(corrupt_issues) == 1, f"{case_name}: corrupt chunk evidence has no single stable analysis issue")
        details = corrupt_issues[0].get("details")
        require(isinstance(details, dict), f"{case_name}: corrupt_chunk issue has no structured details")
        require(integer_field(details.get("hitCount"), context=f"{case_name}.corrupt_chunk.hitCount") >= 1,
                f"{case_name}: corrupt_chunk issue reports no hits")
        require(isinstance(details.get("hitsTruncated"), bool),
                f"{case_name}: corrupt_chunk issue has no hitsTruncated boolean")


def assert_build_id_contract(case_name: str, summary: dict[str, Any], expected: dict[str, Any]) -> None:
    if "buildIdChecks" not in expected:
        return
    coredumps = [
        item
        for item in summary.get("evidence", [])
        if isinstance(item, dict) and item.get("kind") == "coredump"
    ]
    require(len(coredumps) == 1, f"{case_name}: expected one coredump evidence record for build-ID checks")
    evidence = coredumps[0]
    require(evidence.get("buildIdChecks") == expected["buildIdChecks"],
            f"{case_name}: build-ID comparison matrix differs: {evidence.get('buildIdChecks')!r}")
    for field, value in expected.get("buildIds", {}).items():
        require(evidence.get(field) == value,
                f"{case_name}: evidence {field} differs: expected {value!r}, got {evidence.get(field)!r}")
    require(evidence.get("binaryQuarantined") is expected["binaryQuarantined"],
            f"{case_name}: binary quarantine decision differs")
    require(not evidence.get("binaryPath"), f"{case_name}: quarantined evidence exposes a usable binary path")


def assert_timeline_reference_contract(case_name: str, summary: dict[str, Any], expected: dict[str, Any]) -> None:
    timeline = summary.get("timeline")
    require(isinstance(timeline, dict), f"{case_name}: timeline is not an object")
    for field, expected_name in (
        ("globalOrderAvailable", "globalOrderAvailable"),
        ("referencesComplete", "referencesComplete"),
    ):
        if expected_name in expected:
            require(timeline.get(field) is expected[expected_name],
                    f"{case_name}: timeline {field} differs from expected {expected[expected_name]}")
    if expected.get("globalOrderAvailable") is False:
        clock_ordered = timeline.get("clockOrderedEvents")
        require(not isinstance(clock_ordered, list) or not clock_ordered,
                f"{case_name}: incomplete references produced globally ordered events")


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
    for expected_name, argument_name in (
        ("maxEvents", "maxEvents"),
        ("maxCoredumps", "maxCoredumps"),
        ("maxIssues", "maxIssues"),
    ):
        if expected_name in expected:
            summary_arguments[argument_name] = expected[expected_name]
    calls = [
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
    if "inventoryPage" in expected:
        calls.insert(
            3,
            {
                "id": "inventoryPage",
                "tool": "get_incident_inventory",
                "arguments": {"incidentId": "$load.incidentId", **expected["inventoryPage"]},
            },
        )
    return calls


def check_full_case(binary: Path, fixture_root: Path, case: dict[str, Any], timeout: float) -> None:
    name = case["name"]
    expected = case["expected"]
    path = fixture_root / case["path"]
    manifest = bundle_manifest(path)
    calls = full_calls(path, expected)
    first = invoke_batch(binary, cwd=fixture_root, calls=calls, timeout=timeout)
    second = invoke_batch(binary, cwd=fixture_root, calls=calls, timeout=timeout)

    inventory_operations = ("load", "validate", "inventory") + (("inventoryPage",) if "inventoryPage" in first else ())
    for operation in inventory_operations:
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
        expected_degraded = (
            expected.get("summaryDegraded", expected["degraded"])
            if operation in {"summary1", "summary2"}
            else expected["degraded"]
        )
        require(result["degraded"] is expected_degraded, f"{name}/{operation}: degraded differs from expected")
    require(first["load"]["ok"], f"{name}: load operation failed")
    require(first["validate"]["ok"], f"{name}: validate operation failed")
    require(first["inventory"]["ok"], f"{name}: inventory operation failed")
    require(first["summary1"]["ok"], f"{name}: summarize operation failed")

    assert_inventory(name, first["inventory"], manifest, fixture_root)
    if "inventoryPage" in expected:
        assert_inventory_page(name, first["inventoryPage"], manifest, expected["inventoryPage"])
    all_results = list(first.values())
    all_expected_codes = set(expected["issueCodes"]) | set(expected.get("summaryIssueCodes", []))
    assert_issue_contract(name, all_results, all_expected_codes)
    if not all_expected_codes:
        require(not issue_codes(all_results), f"{name}: clean fixture unexpectedly reports {sorted(issue_codes(all_results))!r}")

    summary1 = first["summary1"]
    summary2 = first["summary2"]
    require(summary1.get("semanticProjectionVersion") == 1,
            f"{name}: semantic projection version is not 1")
    semantic = summary1.get("semantic")
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
    canonical_semantic = json.dumps(
        semantic, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    require(digest == "sha256:" + hashlib.sha256(canonical_semantic).hexdigest(),
            f"{name}: semanticDigest is not derived from the semantic replay projection")

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
    assert_summary_bounds(name, summary1, expected)
    assert_chunk_corruption(name, summary1, expected)
    assert_build_id_contract(name, summary1, expected)
    assert_timeline_reference_contract(name, summary1, expected)
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
        if "coredumpVersions" in expected:
            versions = result.get("coredumpVersions")
            require(isinstance(versions, list), f"{name}/{operation}: coredumpVersions is not an array")
            actual_versions = [integer_field(value, context=f"{name}/{operation}.coredumpVersions") for value in versions]
            require(actual_versions == expected["coredumpVersions"],
                    f"{name}/{operation}: detected versions {actual_versions!r}, expected {expected['coredumpVersions']!r}")
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


def summary_projection(result: dict[str, Any]) -> dict[str, Any]:
    """Return the normalized, location-independent part of a summary response."""

    require(result.get("semanticProjectionVersion") == 1, "summary has no stable semantic projection version")
    require(isinstance(result.get("semantic"), dict), "summary has no semantic replay projection object")
    return {
        "semanticProjectionVersion": result["semanticProjectionVersion"],
        "semantic": result["semantic"],
        "semanticDigest": result["semanticDigest"],
    }


def replay_summary(binary: Path, *, cwd: Path, bundle_name: str, timeout: float) -> dict[str, Any]:
    calls = [
        {"id": "load", "tool": "load_incident", "arguments": {"path": bundle_name}},
        {"id": "summary", "tool": "summarize_incident", "arguments": {"incidentId": "$load.incidentId", "maxEvents": 32}},
    ]
    results = invoke_batch(binary, cwd=cwd, calls=calls, timeout=timeout)
    require(results["load"].get("ok") is True, "relocated replay failed to load")
    require(results["summary"].get("ok") is True, "relocated replay failed to summarize")
    return results["summary"]


def check_relocated_host_config_independence(binary: Path, fixture_root: Path, timeout: float) -> None:
    source = fixture_root / "bundles" / "valid-single-v3.wosincident"
    require(source.is_dir(), "relocated replay seed fixture is absent")
    with tempfile.TemporaryDirectory(prefix="wosdbg-relocated-a-") as first_temporary, tempfile.TemporaryDirectory(
        prefix="wosdbg-relocated-b-"
    ) as second_temporary:
        first_root = Path(first_temporary)
        second_root = Path(second_temporary)
        first_bundle = first_root / "replay.wosincident"
        second_bundle = second_root / "moved.wosincident"
        shutil.copytree(source, first_bundle)
        shutil.copytree(source, second_bundle)

        moved_manifest_path = second_bundle / "manifest.json"
        moved_manifest = json.loads(moved_manifest_path.read_text(encoding="utf-8"))
        moved_manifest["createdUtc"] = "2037-12-31T23:59:59Z"
        moved_manifest_path.write_bytes(fixture_generator.canonical_json(moved_manifest))

        wrong_binary = second_root / "host-wrong-build-id.elf"
        wrong_binary.write_bytes(fixture_generator.minimal_elf(fixture_generator.BUILD_ID_B))
        hostile_config = {
            "lookups": [{"from": "0x400000", "to": "0x500000", "path": wrong_binary.name}],
            "binaries": [{"name": "fixture-app", "path": wrong_binary.name}],
            "mcp": {"allowedRoots": ["."]},
        }
        (second_root / "wosdbg.json").write_bytes(fixture_generator.canonical_json(hostile_config))

        baseline = replay_summary(binary, cwd=first_root, bundle_name=first_bundle.name, timeout=timeout)
        moved = replay_summary(binary, cwd=second_root, bundle_name=second_bundle.name, timeout=timeout)
        require(baseline["incidentId"] == moved["incidentId"], "moving a bundle changed its incident identity")
        require(baseline["semanticDigest"] == moved["semanticDigest"],
                "bundle location, createdUtc, or host symbol configuration changed semanticDigest")
        require(summary_projection(baseline) == summary_projection(moved),
                "bundle location, createdUtc, or host symbol configuration changed normalized semantics")
        moved_json = json.dumps(summary_projection(moved), sort_keys=True)
        require(str(second_root) not in moved_json and wrong_binary.name not in moved_json,
                "offline replay leaked or consumed host-configured symbol paths")


def evidence_statuses_by_kind(summary: dict[str, Any], kind: str) -> set[str]:
    evidence = summary.get("evidence")
    require(isinstance(evidence, list), "incident summary evidence is not an array")
    return {
        item.get("status")
        for item in evidence
        if isinstance(item, dict) and item.get("kind") == kind and isinstance(item.get("status"), str)
    }


def check_cache_policy_upgrade(binary: Path, fixture_root: Path, timeout: float) -> None:
    path = fixture_root / "bundles" / "valid-single-v3.wosincident"
    calls = [
        {
            "id": "limitedLoad",
            "tool": "load_incident",
            "arguments": {"path": str(path), "loadEvidence": False},
        },
        {
            "id": "limitedSummary",
            "tool": "summarize_incident",
            "arguments": {"incidentId": "$limitedLoad.incidentId", "maxEvents": 16},
        },
        {"id": "fullLoad", "tool": "load_incident", "arguments": {"path": str(path)}},
        {
            "id": "fullSummary",
            "tool": "summarize_incident",
            "arguments": {"incidentId": "$fullLoad.incidentId", "maxEvents": 16},
        },
        {"id": "cachedFullLoad", "tool": "load_incident", "arguments": {"path": str(path)}},
    ]
    results = invoke_batch(binary, cwd=fixture_root, calls=calls, timeout=timeout)
    limited_load = results["limitedLoad"]
    full_load = results["fullLoad"]
    cached_full = results["cachedFullLoad"]
    require(limited_load.get("ok") is True and full_load.get("ok") is True and cached_full.get("ok") is True,
            "cache-policy regression could not load its incident")
    require(limited_load.get("incidentId") == full_load.get("incidentId") == cached_full.get("incidentId"),
            "changing load policy changed content-derived incident identity")
    require(limited_load.get("cached") is False and full_load.get("cached") is False,
            "a changed evidence policy reused a poisoned incident session")
    require(cached_full.get("cached") is True, "an identical load policy did not reuse the upgraded session")
    require(limited_load.get("loadPolicy", {}).get("loadEvidence") is False,
            "limited load did not report loadEvidence=false")
    require(full_load.get("loadPolicy", {}).get("loadEvidence") is True
            and cached_full.get("loadPolicy") == full_load.get("loadPolicy"),
            "upgraded/cached load policy is not the requested full-evidence policy")
    require(integer_field(full_load.get("loadedLogs"), context="cache-policy.fullLoad.loadedLogs") >= 1,
            "full-policy reload did not load serial-log evidence")
    require(integer_field(full_load.get("loadedCoredumps"), context="cache-policy.fullLoad.loadedCoredumps") >= 1,
            "full-policy reload did not load coredump evidence")

    limited_summary = results["limitedSummary"]
    full_summary = results["fullSummary"]
    require(evidence_statuses_by_kind(limited_summary, "serial-log") == {"not_loaded"}
            and evidence_statuses_by_kind(limited_summary, "coredump") == {"not_loaded"},
            "limited policy unexpectedly loaded log/coredump evidence")
    require(evidence_statuses_by_kind(full_summary, "serial-log") <= {"loaded", "loaded_truncated"}
            and "loaded" in evidence_statuses_by_kind(full_summary, "serial-log"),
            "full-policy reload did not upgrade serial-log evidence")
    require(evidence_statuses_by_kind(full_summary, "coredump") <= {"loaded", "loaded_truncated"}
            and "loaded" in evidence_statuses_by_kind(full_summary, "coredump"),
            "full-policy reload did not upgrade coredump evidence")


def reserve_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def receive_exact(connection: socket.socket, count: int) -> bytes:
    data = bytearray()
    while len(data) < count:
        chunk = connection.recv(count - len(data))
        if not chunk:
            raise TestFailure(f"GUI wire closed after {len(data)} of {count} expected bytes")
        data.extend(chunk)
    return bytes(data)


def receive_gui_frame(connection: socket.socket) -> tuple[int, bytes]:
    size = struct.unpack(">I", receive_exact(connection, 4))[0]
    require(0 < size <= MAX_INTERFACE_JSON_BYTES, f"GUI wire advertised invalid frame size {size}")
    body = receive_exact(connection, size)
    return body[0], body[1:]


def qbytearray(value: bytes) -> bytes:
    require(len(value) < 0xFFFFFFFE, "test QByteArray is too large for compact QDataStream encoding")
    return struct.pack(">I", len(value)) + value


def qstring(value: str) -> bytes:
    encoded = value.encode("utf-16-be")
    return qbytearray(encoded)


def parse_qbytearray(payload: bytes, offset: int = 0) -> tuple[bytes, int]:
    require(offset + 4 <= len(payload), "GUI wire QByteArray length is truncated")
    size = struct.unpack_from(">I", payload, offset)[0]
    require(size not in {0xFFFFFFFF, 0xFFFFFFFE}, "GUI wire returned unsupported null/extended QByteArray")
    start = offset + 4
    end = start + size
    require(end <= len(payload), "GUI wire QByteArray body is truncated")
    return payload[start:end], end


class GuiWireClient:
    def __init__(self, port: int, timeout: float):
        self.connection = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.connection.settimeout(timeout)
        message_type, _ = receive_gui_frame(self.connection)
        require(message_type == 2, f"GUI wire expected WELCOME=2, got {message_type}")
        self.next_request_id = 1

    def close(self) -> None:
        self.connection.close()

    def send(self, message_type: int, payload: bytes = b"") -> None:
        body = bytes([message_type]) + payload
        self.connection.sendall(struct.pack(">I", len(body)) + body)

    def catalog(self) -> dict[str, Any]:
        self.send(GUI_TOOL_CATALOG_REQUEST)
        message_type, payload = receive_gui_frame(self.connection)
        require(message_type == GUI_TOOL_CATALOG_RESPONSE,
                f"GUI wire expected TOOL_CATALOG_RESPONSE={GUI_TOOL_CATALOG_RESPONSE}, got {message_type}")
        encoded, end = parse_qbytearray(payload)
        require(end == len(payload), "GUI catalog response has trailing wire bytes")
        value = json.loads(encoded)
        require(isinstance(value, dict), "GUI catalog response is not an object")
        return value

    def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        request_id = self.next_request_id
        self.next_request_id += 1
        encoded_arguments = json.dumps(arguments, sort_keys=True, separators=(",", ":")).encode("utf-8")
        self.send(GUI_TOOL_CALL_REQUEST, struct.pack(">Q", request_id) + qstring(tool) + qbytearray(encoded_arguments))
        message_type, payload = receive_gui_frame(self.connection)
        require(message_type == GUI_TOOL_CALL_RESPONSE,
                f"GUI wire expected TOOL_CALL_RESPONSE={GUI_TOOL_CALL_RESPONSE}, got {message_type}")
        require(len(payload) >= 8, "GUI tool response omits its request id")
        returned_id = struct.unpack_from(">Q", payload)[0]
        require(returned_id == request_id, f"GUI tool response id {returned_id} differs from request {request_id}")
        encoded, end = parse_qbytearray(payload, 8)
        require(end == len(payload), "GUI tool response has trailing wire bytes")
        value = json.loads(encoded)
        require(isinstance(value, dict), f"GUI tool {tool} returned a non-object")
        return value


def mcp_open_session(port: int, timeout: float) -> str:
    deadline = time.monotonic() + timeout
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=min(timeout, 1.0)) as connection:
                connection.settimeout(min(timeout, 1.0))
                request = (
                    "GET /mcp HTTP/1.1\r\n"
                    f"Host: 127.0.0.1:{port}\r\n"
                    f"MCP-Protocol-Version: {MCP_PROTOCOL_VERSION}\r\n"
                    "Accept: text/event-stream\r\n"
                    "Connection: close\r\n\r\n"
                ).encode("ascii")
                connection.sendall(request)
                response = bytearray()
                while b"\r\n\r\n" not in response and len(response) <= 64 * 1024:
                    chunk = connection.recv(4096)
                    if not chunk:
                        break
                    response.extend(chunk)
                header, separator, _ = bytes(response).partition(b"\r\n\r\n")
                require(separator, "MCP session response has no complete HTTP header")
                lines = header.split(b"\r\n")
                require(lines and b" 200 " in lines[0], f"MCP session GET failed: {lines[0]!r}")
                headers = {
                    key.strip().lower(): value.strip()
                    for line in lines[1:]
                    if b":" in line
                    for key, value in [line.split(b":", 1)]
                }
                session = headers.get(b"mcp-session-id", b"").decode("ascii")
                require(session, "MCP session GET returned no MCP-Session-Id")
                return session
        except OSError as error:
            last_error = error
            time.sleep(0.025)
    raise TestFailure(f"MCP server on port {port} did not become ready: {last_error}")


def mcp_rpc(
    port: int,
    session: str,
    request_id: int,
    method: str,
    params: dict[str, Any],
    timeout: float,
) -> dict[str, Any]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    body = json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
                      sort_keys=True, separators=(",", ":"))
    try:
        connection.request(
            "POST",
            "/mcp",
            body=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
                "MCP-Protocol-Version": MCP_PROTOCOL_VERSION,
                "MCP-Session-Id": session,
            },
        )
        response = connection.getresponse()
        content_length = response.getheader("Content-Length")
        if content_length is not None:
            require(integer_field(content_length, context=f"MCP {method} Content-Length") <= MAX_INTERFACE_JSON_BYTES,
                    f"MCP {method} response exceeds {MAX_INTERFACE_JSON_BYTES} bytes")
        payload = response.read()
    finally:
        connection.close()
    require(response.status == 200, f"MCP {method} returned HTTP {response.status}: {payload!r}")
    require(len(payload) <= MAX_INTERFACE_JSON_BYTES, f"MCP {method} response exceeds {MAX_INTERFACE_JSON_BYTES} bytes")
    value = json.loads(payload)
    require(isinstance(value, dict), f"MCP {method} returned a non-object JSON-RPC envelope")
    require(value.get("id") == request_id, f"MCP {method} response id differs from request")
    require("error" not in value, f"MCP {method} failed: {value.get('error')!r}")
    result = value.get("result")
    require(isinstance(result, dict), f"MCP {method} result is not an object")
    return result


def mcp_tool_payload(result: dict[str, Any], tool: str) -> dict[str, Any]:
    payload = result.get("structuredContent")
    require(isinstance(payload, dict), f"MCP {tool} result has no structuredContent object")
    return payload


@contextmanager
def running_interface_server(binary: Path, cwd: Path, timeout: float):
    gui_port = reserve_loopback_port()
    mcp_port = reserve_loopback_port()
    while mcp_port == gui_port:
        mcp_port = reserve_loopback_port()
    process = subprocess.Popen(
        [
            str(binary),
            "--server",
            f"127.0.0.1:{gui_port}",
            "--mcp",
            "--mcp-host",
            "127.0.0.1",
            "--mcp-port",
            str(mcp_port),
        ],
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        session = mcp_open_session(mcp_port, timeout)
        require(process.poll() is None, "WOSDBG interface server exited during startup")
        yield process, gui_port, mcp_port, session
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=min(timeout, 5.0))
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=min(timeout, 5.0))


def catalog_by_name_from_value(value: dict[str, Any], interface: str) -> dict[str, dict[str, Any]]:
    entries = value.get("tools")
    require(isinstance(entries, list), f"{interface} catalog has no tools array")
    result: dict[str, dict[str, Any]] = {}
    for entry in entries:
        require(isinstance(entry, dict), f"{interface} catalog contains a non-object")
        name = entry.get("name")
        require(isinstance(name, str) and name, f"{interface} catalog entry has no name")
        require(name not in result, f"{interface} catalog duplicates {name}")
        result[name] = entry
    return result


def check_runtime_interface_parity(binary: Path, fixture_root: Path, timeout: float) -> None:
    bundle_path = "bundles/valid-single-v3.wosincident"
    cli_catalog = catalog(binary, cwd=fixture_root, timeout=timeout)
    cli_summary = replay_summary(binary, cwd=fixture_root, bundle_name=bundle_path, timeout=timeout)

    with running_interface_server(binary, fixture_root, timeout) as (_, gui_port, mcp_port, session):
        initialized = mcp_rpc(
            mcp_port,
            session,
            1,
            "initialize",
            {"protocolVersion": MCP_PROTOCOL_VERSION, "capabilities": {}, "clientInfo": {"name": "wosdbg-test", "version": "1"}},
            timeout,
        )
        require(initialized.get("protocolVersion") == MCP_PROTOCOL_VERSION, "MCP negotiated an unexpected protocol version")
        mcp_catalog_value = mcp_rpc(mcp_port, session, 2, "tools/list", {}, timeout)

        gui = GuiWireClient(gui_port, timeout)
        try:
            gui_catalog_value = gui.catalog()
            require(cli_catalog == catalog_by_name_from_value(mcp_catalog_value, "MCP"),
                    "MCP runtime catalog differs from CLI")
            require(cli_catalog == catalog_by_name_from_value(gui_catalog_value, "GUI wire"),
                    "GUI-wire runtime catalog differs from CLI")

            mcp_load = mcp_tool_payload(
                mcp_rpc(
                    mcp_port,
                    session,
                    3,
                    "tools/call",
                    {"name": "wosdbg.load_incident", "arguments": {"path": bundle_path}},
                    timeout,
                ),
                "load_incident",
            )
            require(mcp_load.get("ok") is True, "MCP failed to load the parity fixture")
            mcp_summary = mcp_tool_payload(
                mcp_rpc(
                    mcp_port,
                    session,
                    4,
                    "tools/call",
                    {
                        "name": "wosdbg.summarize_incident",
                        "arguments": {"incidentId": mcp_load["incidentId"], "maxEvents": 32},
                    },
                    timeout,
                ),
                "summarize_incident",
            )

            gui_load = gui.call("wosdbg.load_incident", {"path": bundle_path})
            require(gui_load.get("ok") is True, "GUI wire failed to load the parity fixture")
            gui_summary = gui.call(
                "wosdbg.summarize_incident",
                {"incidentId": gui_load["incidentId"], "maxEvents": 32},
            )
        finally:
            gui.close()

    projections = {
        "CLI": summary_projection(cli_summary),
        "MCP": summary_projection(mcp_summary),
        "GUI wire": summary_projection(gui_summary),
    }
    require(projections["CLI"] == projections["MCP"] == projections["GUI wire"],
            "normalized incident semantics differ across CLI, MCP, and GUI wire")
    digests = {interface: result.get("semanticDigest") for interface, result in projections.items()}
    require(len(set(digests.values())) == 1 and SEMANTIC_DIGEST_RE.fullmatch(str(next(iter(digests.values())))),
            f"semanticDigest differs across interfaces: {digests!r}")


def recompute_manifest_identity(manifest: dict[str, Any]) -> None:
    identity = copy.deepcopy(manifest)
    identity.pop("incidentId", None)
    identity.pop("createdUtc", None)
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    manifest["incidentId"] = "sha256:" + fixture_generator.sha256(encoded)


def write_coredump_mutant(
    output_root: Path,
    name: str,
    source_bundle: Path,
    mutated_dump: bytes,
    *,
    truncated: bool,
) -> Path:
    manifest, member_data = fixture_generator.manifest_from_bundle(source_bundle)
    manifest = copy.deepcopy(manifest)
    artifacts: list[fixture_generator.Artifact] = []
    changed_path = ""
    for member in manifest["members"]:
        data = member_data[member["path"]]
        member_truncated = bool(member["truncated"])
        if member["kind"] == "coredump" and not changed_path:
            changed_path = member["path"]
            data = mutated_dump
            member["size"] = len(data)
            member["sha256"] = fixture_generator.sha256(data)
            member["truncated"] = truncated
            member_truncated = truncated
        artifacts.append(
            fixture_generator.Artifact(
                member["path"],
                member["kind"],
                data,
                required=bool(member["required"]),
                truncated=member_truncated,
                node_id=member.get("nodeId"),
                build_id=member.get("buildId"),
                binary=member.get("binary"),
                source_name=member.get("sourceName"),
                clock_domain=member.get("clockDomain"),
            )
        )
    require(changed_path, f"{name}: source fixture has no coredump member")
    manifest["capture"]["complete"] = not truncated
    manifest["capture"]["truncatedMembers"] = [changed_path] if truncated else []
    recompute_manifest_identity(manifest)
    return fixture_generator.write_directory_bundle(output_root, name, manifest, artifacts)


def mutate_coredump(seed: bytes, iteration: int, rng: random.Random) -> tuple[bytes, bool]:
    data = bytearray(seed)
    operation = iteration % 5
    if operation == 0:
        cutoffs = [0, 1, 7, 15, 16, 487, 488, 1359, 1360, 1839, 1840, max(0, len(data) - 1)]
        cutoff = cutoffs[(iteration // 5) % len(cutoffs)]
        return bytes(data[: min(cutoff, len(data))]), True
    if operation == 1:
        offset = rng.randrange(len(data))
        data[offset] ^= 1 << rng.randrange(8)
    elif operation == 2 and len(data) >= 12:
        struct.pack_into("<I", data, 8, [0, 1, 2, 3, 4, 0xFFFFFFFF][(iteration // 5) % 6])
    elif operation == 3 and len(data) >= 16:
        struct.pack_into("<I", data, 12, [0, 16, 487, 488, 1360, 1840, 0xFFFFFFFF][(iteration // 5) % 7])
    elif operation == 4 and len(data) >= 64:
        offset = rng.randrange(16, min(len(data), 256))
        width = min(8, len(data) - offset)
        data[offset : offset + width] = rng.randbytes(width)
    return bytes(data), False


def assert_property_result(case_name: str, result: dict[str, Any]) -> None:
    require(isinstance(result.get("ok"), bool), f"{case_name}: result has no boolean ok field")
    if COMMON_RESULT_FIELDS <= set(result):
        assert_fields(case_name, "property", result, COMMON_RESULT_FIELDS)
        codes = issue_codes(result)
        require(all(re.fullmatch(r"[a-z][a-z0-9_]*", code) for code in codes),
                f"{case_name}: mutation produced unstable issue codes {sorted(codes)!r}")
    else:
        require(result["ok"] is False and isinstance(result.get("error"), str),
                f"{case_name}: parser rejection is neither a stable result nor a structured error")


def run_seeded_property_smoke(
    binary: Path,
    fixture_root: Path,
    index: dict[str, Any],
    runs: int,
    timeout: float,
) -> int:
    require(runs > 0, "property smoke run count must be positive")
    rng = random.Random(0x574F53444247)
    version_cases = [
        next(case for case in index["loaderCases"] if case["name"] == f"valid-single-v{version}")
        for version in (1, 2, 3, 4)
    ]
    archive_seed = (fixture_root / next(
        case["path"] for case in index["loaderCases"] if case["name"] == "valid-single-v3-archive"
    )).read_bytes()

    with tempfile.TemporaryDirectory(prefix="wosdbg-property-smoke-") as temporary:
        root = Path(temporary)
        mutant_root = root / "coredumps"
        archive_root = root / "archives"
        archive_root.mkdir(parents=True)
        executed = 0
        for iteration in range(runs):
            if iteration % 2 == 0:
                seed_case = version_cases[(iteration // 2) % len(version_cases)]
                seed_bundle = fixture_root / seed_case["path"]
                _, members = fixture_generator.manifest_from_bundle(seed_bundle)
                seed_dump = next(data for path, data in members.items() if "coredumps/" in path)
                mutated, truncated = mutate_coredump(seed_dump, iteration, rng)
                path = write_coredump_mutant(
                    mutant_root,
                    f"parser-mutation-{iteration:03d}",
                    seed_bundle,
                    mutated,
                    truncated=truncated,
                )
            else:
                mutated_archive = bytearray(archive_seed)
                if iteration % 4 == 1:
                    offset = rng.randrange(0, min(512, len(mutated_archive)))
                else:
                    offset = rng.randrange(len(mutated_archive))
                mutated_archive[offset] ^= 1 << rng.randrange(8)
                path = archive_root / f"container-mutation-{iteration:03d}.wosincident"
                path.write_bytes(mutated_archive)

            calls = [
                {"id": "validate", "tool": "validate_incident", "arguments": {"path": str(path)}},
                {"id": "load", "tool": "load_incident", "arguments": {"path": str(path)}},
            ]
            first = invoke_batch(binary, cwd=root, calls=calls, timeout=timeout)
            second = invoke_batch(binary, cwd=root, calls=calls, timeout=timeout)
            require(first == second, f"property mutation {iteration} produced nondeterministic parser results")
            for operation, result in first.items():
                assert_property_result(f"property-mutation-{iteration}/{operation}", result)
            executed += 1
        return executed


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
    modes = {"loader", "security", "collector", "property"} if args.mode == "all" else {args.mode}
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
        relocation_seed = next(case for case in index["loaderCases"] if case["name"] == "valid-single-v3")
        if selected(relocation_seed, args.case):
            check_relocated_host_config_independence(binary, fixture_root, args.timeout)
            print("PASS relocated replay and host-config independence")
            executed += 1
            check_cache_policy_upgrade(binary, fixture_root, args.timeout)
            print("PASS incident cache policy reload upgrades evidence")
            executed += 1
            check_runtime_interface_parity(binary, fixture_root, args.timeout)
            print("PASS runtime CLI, MCP, and GUI-wire catalog/semantic parity")
            executed += 1
    if "security" in modes:
        for case in index["securityCases"]:
            if not selected(case, args.case):
                continue
            check_rejected_case(binary, fixture_root, case, args.timeout)
            print(f"PASS {case['name']}")
            executed += 1
    property_case = {"name": "seeded-property-smoke"}
    if "property" in modes and selected(property_case, args.case):
        property_runs = run_seeded_property_smoke(binary, fixture_root, index, args.property_smoke_runs, args.timeout)
        print(f"PASS seeded incident-container/coredump parser property smoke ({property_runs} mutations)")
        executed += property_runs
    require(executed > 0, "no fixture cases matched the selected mode/patterns")
    print(f"PASS incident interface catalog, dispatch, semantics, security, and determinism ({executed} cases)")
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--wosdbg", type=Path, default=DEFAULT_WOSDBG, help="WOSDBG executable to test")
    result.add_argument("--fixtures", type=Path, help="Existing generated fixture root (default: temporary corpus)")
    result.add_argument("--mode", choices=("all", "loader", "security", "collector", "property"), default="all")
    result.add_argument("--case", action="append", default=[], metavar="GLOB", help="Run matching case name(s)")
    result.add_argument("--timeout", type=float, default=30.0, help="Per-process timeout in seconds")
    result.add_argument(
        "--property-smoke-runs",
        type=int,
        default=16,
        help="Deterministic coredump/container mutations used by property mode (default: 16)",
    )
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
    require(args.property_smoke_runs > 0, "--property-smoke-runs must be positive")
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
            print("propertyCases: seeded-property-smoke")
            return 0
        return run_cases(binary, fixture_root, index, args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TestFailure as error:
        print(f"FAIL: {error}", file=sys.stderr)
        raise SystemExit(1) from error
