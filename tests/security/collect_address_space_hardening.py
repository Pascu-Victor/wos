#!/usr/bin/env python3
"""Collect and validate the Goal 15 address-space hardening evidence.

The guest probe deliberately emits addresses and a boolean AT_RANDOM presence
flag, but never emits the entropy bytes.  This collector keeps that property:
it accepts JSON-lines probe output, computes only address diversity, and writes
the repository's versioned report format.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import pathlib
import sys
from datetime import datetime, timezone
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_TEMPLATE = ROOT / "tests/security/address_space_hardening_report.template.json"
LAYOUT_KIND = "wos-aslr-layout-v1"
PROTECTION_KIND = "wos-aslr-protection-v1"
ENTROPY_FIELDS = (
    "mainLoadBias",
    "interpreterLoadBias",
    "stackAddress",
    "tlsAddress",
    "anonymousMappingAddress",
    "atRandomAddress",
)
REPORT_LAYOUT_FIELDS = (
    "pid",
    "mainLoadBias",
    "interpreterLoadBias",
    "codeAddress",
    "stackAddress",
    "tlsAddress",
    "heapAddress",
    "anonymousMappingAddress",
    "atRandomAddress",
    "relroAddress",
    "atRandomPresent",
    "result",
)
VALID_STATUSES = {"not-run", "pass", "fail", "blocked"}
IMAGE_ALIGNMENT = 2 * 1024 * 1024
ADDRESS_WINDOWS = {
    "mainLoadBias": (0x0000000008000000, 0x0000000030000000, IMAGE_ALIGNMENT),
    "interpreterLoadBias": (0x0000000030000000, 0x0000000070000000, IMAGE_ALIGNMENT),
    # These are observed addresses within allocations, not the randomized
    # cursor/base itself.  The anonymous result remains page-aligned after
    # earlier rtld mappings have advanced the cursor; the TLS marker is an
    # interior address in mlibc's initial-thread TLS mapping.
    "anonymousMappingAddress": (0x0000200000000000, 0x0000600000000000, 4096),
    "stackAddress": (0x0000740000000000, 0x00007F0000000000, 1),
    "tlsAddress": (0x0000200000000000, 0x0000600000000000, 1),
    "atRandomAddress": (0x0000740000000000, 0x00007F0000000000, 1),
}


def parse_scoped_file(value: str) -> tuple[str, str, pathlib.Path]:
    try:
        context, node, path = value.split(":", 2)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected CONTEXT:NODE:PATH") from error
    if context not in {"local", "remote"} or not node or not path:
        raise argparse.ArgumentTypeError("context must be local or remote and node/path must be non-empty")
    return context, node, pathlib.Path(path)


def parse_protection_file(value: str) -> tuple[str, pathlib.Path]:
    try:
        context, path = value.split(":", 1)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected CONTEXT:PATH") from error
    if not context or not path:
        raise argparse.ArgumentTypeError("context and path must be non-empty")
    return context, pathlib.Path(path)


def parse_result(value: str) -> tuple[str, str, str, str, str | None]:
    fields = value.split(":", 4)
    if len(fields) < 4:
        raise argparse.ArgumentTypeError("expected SECTION:NAME:CONTEXT:STATUS[:EVIDENCE]")
    section, name, context, status = fields[:4]
    evidence = fields[4] if len(fields) == 5 and fields[4] else None
    if section not in {"protectionTests", "malformedElfTests", "compatibilityGates"}:
        raise argparse.ArgumentTypeError("unknown report section")
    if status not in VALID_STATUSES:
        raise argparse.ArgumentTypeError("invalid status")
    if not name or not context:
        raise argparse.ArgumentTypeError("name and context must be non-empty")
    return section, name, context, status, evidence


def parse_command_result(value: str) -> tuple[int, str, int | None, str | None]:
    fields = value.split(":", 3)
    if len(fields) < 2:
        raise argparse.ArgumentTypeError("expected INDEX:STATUS[:EXIT_CODE[:OUTPUT_ARTIFACT]]")
    try:
        index = int(fields[0])
    except ValueError as error:
        raise argparse.ArgumentTypeError("command index must be an integer") from error
    status = fields[1]
    if index < 0 or status not in VALID_STATUSES:
        raise argparse.ArgumentTypeError("invalid command index or status")
    exit_code: int | None = None
    if len(fields) >= 3 and fields[2]:
        try:
            exit_code = int(fields[2])
        except ValueError as error:
            raise argparse.ArgumentTypeError("exit code must be an integer") from error
    artifact = fields[3] if len(fields) == 4 and fields[3] else None
    return index, status, exit_code, artifact


def parse_artifact(value: str) -> tuple[str, pathlib.Path]:
    try:
        kind, path = value.split(":", 1)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected KIND:PATH") from error
    if not kind or not path:
        raise argparse.ArgumentTypeError("kind and path must be non-empty")
    return kind, pathlib.Path(path)


def iter_records(path: pathlib.Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8", errors="replace") as stream:
        for line_number, line in enumerate(stream, 1):
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                value["_evidence"] = f"{path}:{line_number}"
                records.append(value)
    return records


def add_layout_samples(report: dict[str, Any], specs: list[tuple[str, str, pathlib.Path]]) -> None:
    for context, node, path in specs:
        for record in iter_records(path):
            if record.get("kind") != LAYOUT_KIND:
                continue
            missing = [field for field in REPORT_LAYOUT_FIELDS if field not in record]
            if missing:
                raise ValueError(f"{record['_evidence']}: missing layout fields: {', '.join(missing)}")
            sample = {"context": context, "node": node}
            sample.update({field: record[field] for field in REPORT_LAYOUT_FIELDS})
            report["layoutSamples"].append(sample)


def replace_or_add_result(
    report: dict[str, Any], section: str, name: str, context: str, status: str, evidence: str | None
) -> None:
    matches = [entry for entry in report[section] if entry["name"] == name and entry["context"] == context]
    if len(matches) > 1:
        raise ValueError(f"duplicate template result {section}:{name}:{context}")
    value = {"name": name, "context": context, "status": status, "evidence": evidence}
    if matches:
        report[section][report[section].index(matches[0])] = value
    else:
        report[section].append(value)


def add_protection_results(report: dict[str, Any], specs: list[tuple[str, pathlib.Path]]) -> None:
    for context, path in specs:
        for record in iter_records(path):
            if record.get("kind") != PROTECTION_KIND:
                continue
            name = record.get("test")
            result = record.get("result")
            if not isinstance(name, str) or result not in {"pass", "fail"}:
                raise ValueError(f"{record['_evidence']}: invalid protection result")
            replace_or_add_result(report, "protectionTests", name, context, result, record["_evidence"])


def update_entropy_summary(report: dict[str, Any]) -> None:
    samples = report["layoutSamples"]
    summary = report["entropySummary"]
    summary["samplesCollected"] = len(samples)
    for field in ENTROPY_FIELDS:
        distinct = len({sample[field] for sample in samples})
        summary["fields"][field] = {
            "distinctValues": distinct,
            "allDistinct": distinct == len(samples) if samples else None,
        }
    # Keep this an explicit invariant, even if a modified template says otherwise.
    summary["atRandomBytesRecorded"] = False


def digest(path: pathlib.Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            hasher.update(chunk)
    return hasher.hexdigest()


def parse_address(value: Any, *, sample_index: int, field: str) -> int:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError(f"layout sample {sample_index} has an invalid {field}")
    try:
        address = int(value, 16)
    except ValueError as error:
        raise ValueError(f"layout sample {sample_index} has an invalid {field}") from error
    if value != f"0x{address:x}":
        raise ValueError(f"layout sample {sample_index} has a non-canonical {field}")
    return address


def validate_report(report: dict[str, Any], allow_incomplete: bool) -> None:
    if report.get("schemaVersion") != 1 or report.get("goal") != 15:
        raise ValueError("template is not the Goal 15 report schema version 1")
    samples = report.get("layoutSamples")
    if not isinstance(samples, list):
        raise ValueError("layoutSamples must be an array")
    for index, sample in enumerate(samples):
        for field in REPORT_LAYOUT_FIELDS:
            if field not in sample:
                raise ValueError(f"layout sample {index} is missing {field}")
        for field in ENTROPY_FIELDS:
            address = parse_address(sample[field], sample_index=index, field=field)
            window_begin, window_end, alignment = ADDRESS_WINDOWS[field]
            if address < window_begin or address >= window_end:
                raise ValueError(f"layout sample {index} places {field} outside its documented window")
            if address % alignment != 0:
                raise ValueError(f"layout sample {index} has a misaligned {field}")
        if sample["result"] not in {"pass", "fail"}:
            raise ValueError(f"layout sample {index} has an invalid result")
        if not isinstance(sample.get("context"), str) or sample["context"] not in {"local", "remote"}:
            raise ValueError(f"layout sample {index} has an invalid context")
        if not isinstance(sample.get("node"), str) or not sample["node"]:
            raise ValueError(f"layout sample {index} has an invalid node")
        if not isinstance(sample["atRandomPresent"], bool):
            raise ValueError(f"layout sample {index} has an invalid atRandomPresent value")
    if report["entropySummary"].get("atRandomBytesRecorded") is not False:
        raise ValueError("AT_RANDOM bytes must never be recorded")
    if allow_incomplete:
        return
    minimum = report["entropySummary"]["minimumSamples"]
    if len(samples) < minimum:
        raise ValueError(f"only {len(samples)} layout samples collected; {minimum} required")
    diversity_floor = max(2, math.ceil(minimum / 2))
    for field in ENTROPY_FIELDS:
        distinct = report["entropySummary"]["fields"][field]["distinctValues"]
        if distinct < diversity_floor:
            raise ValueError(f"only {distinct} distinct values observed in {field}; {diversity_floor} required")
    contexts = {sample["context"] for sample in samples}
    if contexts != {"local", "remote"}:
        raise ValueError("layout evidence must contain both local and remote execution samples")
    for context in sorted(contexts):
        context_samples = [sample for sample in samples if sample["context"] == context]
        for field in ENTROPY_FIELDS:
            if len({sample[field] for sample in context_samples}) < 2:
                raise ValueError(f"no observed {context} variation in {field}")
    failures: list[str] = []
    for index, sample in enumerate(samples):
        if sample["result"] != "pass" or sample["atRandomPresent"] is not True:
            failures.append(f"layoutSamples:{index}=fail")
    for command in report["commands"]:
        if command["status"] != "pass":
            failures.append(f"command:{command['purpose']}={command['status']}")
    for section in ("protectionTests", "malformedElfTests", "compatibilityGates"):
        for result in report[section]:
            if result["status"] != "pass":
                failures.append(f"{section}:{result['name']}={result['status']}")
    if failures:
        raise ValueError("incomplete or failed evidence: " + ", ".join(failures))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template", type=pathlib.Path, default=DEFAULT_TEMPLATE)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--layout", action="append", type=parse_scoped_file, default=[], metavar="CONTEXT:NODE:PATH")
    parser.add_argument("--protection", action="append", type=parse_protection_file, default=[], metavar="CONTEXT:PATH")
    parser.add_argument("--result", action="append", type=parse_result, default=[], metavar="SECTION:NAME:CONTEXT:STATUS[:EVIDENCE]")
    parser.add_argument("--command-result", action="append", type=parse_command_result, default=[], metavar="INDEX:STATUS[:EXIT[:ARTIFACT]]")
    parser.add_argument("--artifact", action="append", type=parse_artifact, default=[], metavar="KIND:PATH")
    parser.add_argument("--limitation", action="append", default=[])
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()

    with args.template.open(encoding="utf-8") as stream:
        report = copy.deepcopy(json.load(stream))
    report["generatedAt"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    add_layout_samples(report, args.layout)
    add_protection_results(report, args.protection)
    for section, name, context, status, evidence in args.result:
        replace_or_add_result(report, section, name, context, status, evidence)
    for index, status, exit_code, artifact in args.command_result:
        if index >= len(report["commands"]):
            raise ValueError(f"command index {index} is outside the template")
        report["commands"][index]["status"] = status
        report["commands"][index]["exitCode"] = exit_code
        if artifact is not None:
            report["commands"][index]["outputArtifact"] = artifact
    update_entropy_summary(report)
    for kind, path in args.artifact:
        report["artifacts"].append({
            "kind": kind,
            "path": str(path),
            "sha256": digest(path) if path.is_file() else None,
        })
    report["limitations"].extend(args.limitation)
    validate_report(report, args.allow_incomplete)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        print(f"address-space report collection failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
