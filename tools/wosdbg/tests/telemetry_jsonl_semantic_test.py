#!/usr/bin/env python3
"""Exercise bounded telemetry JSONL ingestion through WOSDBG's JSON CLI."""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any


class TestFailure(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise TestFailure(message)


def envelope(*, node: str | None, timestamp: str, request: str, extension: Any = None) -> dict[str, Any]:
    identity: dict[str, Any] = {
        "boot_id": "18446744073709551614",
        "pid": "18446744073709551613",
        "tid": "18446744073709551612",
        "cpu": 7,
    }
    if node is not None:
        identity["node_id"] = node
    result: dict[str, Any] = {
        "format": "wos.telemetry",
        "version": 1,
        "source": "strace",
        "source_version": 1,
        "kind": "wki.request",
        "identity": identity,
        "clock": {
            "domain": "boot_monotonic",
            "value": timestamp,
            "unit": "ns",
            "quality": "local",
        },
        "correlation": {"request_id": request},
        "payload": {"operation": "read", "message": "typed telemetry"},
    }
    if extension is not None:
        result["future_extension"] = extension
    return result


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n" for record in records))


def run_json(command: list[str], *, cwd: Path, stdin: str | None = None) -> tuple[int, dict[str, Any]]:
    completed = subprocess.run(command, cwd=cwd, input=stdin, text=True, capture_output=True, check=False, timeout=60)
    require(len(completed.stdout) < 4 * 1024 * 1024, "WOSDBG returned unexpectedly large JSON")
    try:
        output = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise TestFailure(f"WOSDBG returned non-JSON output: {completed.stderr.strip()}") from error
    require(isinstance(output, dict), "WOSDBG result is not an object")
    return completed.returncode, output


def batch(binary: Path, cwd: Path, calls: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    status, output = run_json(
        [str(binary), "--batch", "-"],
        cwd=cwd,
        stdin=json.dumps({"calls": calls, "continueOnError": True}, sort_keys=True),
    )
    require(status in {0, 1}, f"batch returned unexpected status {status}")
    results: dict[str, dict[str, Any]] = {}
    for item in output.get("calls", []):
        results[item["id"]] = item["result"]
    require(len(results) == len(calls), "batch omitted calls")
    return results


def rejected(binary: Path, cwd: Path, path: Path, expected: str) -> None:
    status, result = run_json(
        [str(binary), "--tool", "load_log", "--arguments", json.dumps({"path": str(path)})],
        cwd=cwd,
    )
    require(status == 1, f"{path.name}: malformed telemetry unexpectedly loaded")
    require(result.get("ok") is False, f"{path.name}: malformed telemetry result is not an error")
    error = result.get("error", "")
    require("Structured telemetry rejected" in error and expected in error, f"{path.name}: unexpected error: {error!r}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wosdbg", type=Path, required=True)
    args = parser.parse_args()
    binary = args.wosdbg.resolve()
    require(binary.is_file(), f"WOSDBG binary not found: {binary}")

    with tempfile.TemporaryDirectory(prefix="wosdbg-telemetry-") as temporary:
        root = Path(temporary)
        exact_timestamp = "18446744073709550000"
        exact_request = "18446744073709551611"
        same = root / "same.jsonl"
        mixed = root / "mixed.jsonl"
        missing = root / "missing.jsonl"
        empty = root / "empty.jsonl"
        legacy = root / "legacy.log"
        write_jsonl(
            same,
            [
                envelope(node="node-a", timestamp="9007199254740993", request=exact_request, extension={"new": [1, 2, 3]}),
                envelope(node="node-a", timestamp=exact_timestamp, request=exact_request),
            ],
        )
        write_jsonl(
            mixed,
            [
                envelope(node="node-a", timestamp="100", request="cross-node"),
                envelope(node="node-b", timestamp="101", request="cross-node"),
            ],
        )
        write_jsonl(
            missing,
            [
                envelope(node=None, timestamp="200", request="missing-node"),
                envelope(node=None, timestamp="201", request="missing-node"),
            ],
        )
        legacy_line = "WKI request=legacy-cookie timestamp=17ns"
        legacy.write_text(legacy_line + "\n")
        empty.write_text("")

        calls = [
            {"id": "same", "tool": "load_log", "arguments": {"path": str(same)}},
            {"id": "entries", "tool": "get_log_entries", "arguments": {"logId": "$same.logId", "count": 10}},
            {
                "id": "same_timeline",
                "tool": "build_distributed_timeline",
                "arguments": {"logIds": ["$same.logId"], "query": "wki.request", "maxEvents": 10},
            },
            {"id": "mixed", "tool": "load_log", "arguments": {"path": str(mixed)}},
            {
                "id": "mixed_timeline",
                "tool": "build_distributed_timeline",
                "arguments": {"logIds": ["$mixed.logId"], "query": "wki.request", "maxEvents": 10},
            },
            {"id": "missing", "tool": "load_log", "arguments": {"path": str(missing)}},
            {
                "id": "missing_timeline",
                "tool": "build_distributed_timeline",
                "arguments": {"logIds": ["$missing.logId"], "query": "wki.request", "maxEvents": 10},
            },
            {"id": "legacy", "tool": "load_log", "arguments": {"path": str(legacy)}},
            {"id": "legacy_entries", "tool": "get_log_entries", "arguments": {"logId": "$legacy.logId", "count": 10}},
            {"id": "empty", "tool": "load_log", "arguments": {"path": str(empty)}},
        ]
        results = batch(binary, root, calls)
        require(all(result.get("ok") is True for result in results.values()), f"valid batch failed: {results!r}")
        summary = results["same"]["summary"]
        require(summary["inputFormat"] == "wos-telemetry-jsonl" and summary["telemetryRecords"] == 2,
                "structured summary did not identify both records")
        entries = results["entries"]["entries"]
        require(entries[0]["telemetryEnvelope"]["future_extension"] == {"new": [1, 2, 3]},
                "unknown envelope extension was not retained")
        require(entries[0]["telemetryEnvelope"]["correlation"]["request_id"] == exact_request,
                "64-bit correlation identifier lost precision")
        require(exact_request in entries[0]["telemetryJson"], "canonical JSON projection lost the exact identifier")
        same_timeline = results["same_timeline"]
        require(same_timeline["globalOrderAvailable"] is True, "same-node/domain records were not orderable")
        require(same_timeline["clockOrderedEvents"][1]["timestampNs"] == exact_timestamp,
                "64-bit clock value lost precision")
        require(same_timeline["events"][0]["correlation"]["request_id"] == exact_request,
                "timeline did not use typed correlation fields")
        mixed_timeline = results["mixed_timeline"]
        require(mixed_timeline["globalOrderAvailable"] is False and len(mixed_timeline["clockPartitions"]) == 2,
                "cross-node local clocks were incorrectly globally ordered")
        missing_timeline = results["missing_timeline"]
        require(missing_timeline["globalOrderAvailable"] is False and len(missing_timeline["clockPartitions"]) == 2,
                "missing node identity did not produce per-entry unproven partitions")
        require(results["legacy"]["summary"]["inputFormat"] == "legacy-text",
                "legacy log was not routed through the compatibility reader")
        require(results["legacy_entries"]["entries"][0]["originalLine"] == legacy_line,
                "legacy reader changed the default text projection")
        require(results["empty"]["summary"]["inputFormat"] == "wos-telemetry-jsonl" and
                results["empty"]["summary"]["telemetryRecords"] == 0,
                "empty structured input was mislabeled as a legacy log")

        malformed = root / "malformed.jsonl"
        malformed.write_text('{"format":"wos.telemetry"\n')
        rejected(binary, root, malformed, "line 1")
        duplicate = root / "duplicate.jsonl"
        duplicate.write_text(
            '{"format":"wos.telemetry","format":"wos.telemetry","version":1,"source":"x",'
            '"source_version":1,"kind":"x","identity":{},"clock":{},"correlation":{},"payload":{}}\n'
        )
        rejected(binary, root, duplicate, "duplicate")
        bad_cpu = root / "bad-cpu.jsonl"
        bad_cpu_record = envelope(node="node-a", timestamp="1", request="bad-cpu")
        bad_cpu_record["identity"]["cpu"] = "7"
        write_jsonl(bad_cpu, [bad_cpu_record])
        rejected(binary, root, bad_cpu, "cpu")
        deep = root / "deep.jsonl"
        nested: Any = "leaf"
        for _ in range(20):
            nested = {"child": nested}
        write_jsonl(deep, [envelope(node="node-a", timestamp="1", request="deep", extension=nested)])
        rejected(binary, root, deep, "nesting")
        oversized = root / "oversized.jsonl"
        oversized.write_text(json.dumps(envelope(node="node-a", timestamp="1", request="large", extension="x" * (1024 * 1024))) + "\n")
        rejected(binary, root, oversized, "1 MiB line limit")

    print("WOSDBG telemetry JSONL semantic checks passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TestFailure as error:
        print(f"FAIL: {error}")
        raise SystemExit(1) from error
