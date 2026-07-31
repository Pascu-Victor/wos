#!/usr/bin/env python3
"""Validate one WOS physical-memory checkpoint and emit canonical JSON."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import unquote


PAGE_SIZE = 4096
FORBIDDEN_CATEGORY_FRAGMENTS = (
    "unaccounted",
    "unknown",
    "other",
    "estimated",
    "residual",
    "miscellaneous",
)
OWNER_FIELDS = ("name", "pages", "bytes", "objects", "lifetime", "reclaimability", "scaling_bound")
RECLAIMABILITY_VALUES = {
    "owner_teardown",
    "pressure_reclaim",
    "permanent_reserve",
    "diagnostic_lifetime",
}
QUIESCENT_RECORD_KEYS = {
    "wki_compute": ("submitted_active", "running_active", "pending_complete", "truncated"),
    "wki_ipc": (
        "exports",
        "proxies",
        "active_pumps",
        "ring_used",
        "pending_deliveries",
        "pending_bytes",
        "export_flush_queue",
        "export_close_pending",
        "export_close_waiting_for_bytes",
        "proxy_close_queue",
        "dev_op_queue",
    ),
    "wki_vfs_server": ("active", "retiring"),
}
BUILD_PROCESS_FRAGMENTS = ("cmake", "ninja", "clang", "clang++", "ld.lld", "bootstrap")
DIAGNOSTIC_FEATURES = ("page_callers", "kmalloc_debug")


class ValidationError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise ValidationError(message)


def parse_u64(value: str, context: str) -> int:
    if not value or not value.isdecimal():
        fail(f"{context}: expected an unsigned decimal integer, got {value!r}")
    parsed = int(value, 10)
    if parsed > (1 << 64) - 1:
        fail(f"{context}: value exceeds uint64")
    return parsed


def parse_record(line: str, context: str) -> tuple[str, dict[str, str]]:
    fields = line.split()
    if not fields:
        fail(f"{context}: empty record")
    values: dict[str, str] = {}
    for field in fields[1:]:
        if "=" not in field:
            fail(f"{context}: malformed field {field!r}")
        key, value = field.split("=", 1)
        if not key or key in values:
            fail(f"{context}: empty or duplicate key {key!r}")
        values[key] = unquote(value)
    return fields[0], values


def parse_sections(text: str, context: str) -> dict[str, list[tuple[str, dict[str, str]]]]:
    sections: dict[str, list[tuple[str, dict[str, str]]]] = {}
    current: str | None = None
    for line_number, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("== ") and line.endswith(" =="):
            current = line[3:-3]
            if not current or current in sections:
                fail(f"{context}:{line_number}: empty or duplicate section")
            sections[current] = []
            continue
        if current is None:
            fail(f"{context}:{line_number}: record before first section")
        sections[current].append(parse_record(line, f"{context}:{line_number}"))
    return sections


def required_u64(values: dict[str, str], key: str, context: str) -> int:
    if key not in values:
        fail(f"{context}: missing {key}")
    return parse_u64(values[key], f"{context}.{key}")


def checked_sum(values: list[int], context: str) -> int:
    total = sum(values)
    if total > (1 << 64) - 1:
        fail(f"{context}: uint64 overflow")
    return total


def validate_owner_rows(rows: list[tuple[str, dict[str, str]]], summary: dict[str, str], context: str) -> list[dict[str, Any]]:
    owners: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    runtime_owner_pages = 0
    fixed_owner_pages: dict[str, int] = {}

    for index, (record, values) in enumerate(rows):
        if record != "physical_owner":
            continue
        row_context = f"{context}.physical_owner[{index}]"
        missing = [key for key in OWNER_FIELDS if key not in values or values[key] == ""]
        if missing:
            fail(f"{row_context}: incomplete descriptor fields: {', '.join(missing)}")
        name = values["name"]
        lowered = name.lower()
        forbidden = [fragment for fragment in FORBIDDEN_CATEGORY_FRAGMENTS if fragment in lowered]
        if forbidden:
            fail(f"{row_context}: forbidden residual category {name!r}")
        if name in seen_names:
            fail(f"{row_context}: duplicate physical owner {name!r}")
        seen_names.add(name)

        pages = required_u64(values, "pages", row_context)
        byte_count = required_u64(values, "bytes", row_context)
        objects = required_u64(values, "objects", row_context)
        if byte_count != pages * PAGE_SIZE:
            fail(f"{row_context}: bytes={byte_count} does not equal pages*{PAGE_SIZE}")
        reclaimability = values["reclaimability"]
        if reclaimability not in RECLAIMABILITY_VALUES:
            fail(f"{row_context}: unsupported reclaimability {reclaimability!r}")

        owner = {
            "name": name,
            "pages": pages,
            "bytes": byte_count,
            "objects": objects,
            "lifetime": values["lifetime"],
            "reclaimability": reclaimability,
            "scaling_bound": values["scaling_bound"],
        }
        owners.append(owner)
        fixed_owner_pages[name] = pages

    if not owners:
        fail(f"{context}: no physical_owner records")

    fixed_names = {
        "physical_allocator_embedded_metadata",
        "physical_zone_descriptors",
        "per_cpu_physical_page_cache_metadata",
        "firmware_reserved",
        "acpi_reclaimable_reserved",
        "acpi_nvs_reserved",
        "hardware_bad_memory",
        "bootloader_reclaimable_reserved",
        "kernel_and_boot_modules",
        "firmware_framebuffer",
        "firmware_reserved_mapped",
    }
    runtime_owner_pages = checked_sum(
        [owner["pages"] for owner in owners if owner["name"] not in fixed_names],
        f"{context}.runtime_owner_pages",
    )
    if runtime_owner_pages != required_u64(summary, "owner_pages", context):
        fail(
            f"{context}: physical owner rows sum to {runtime_owner_pages}, "
            f"summary owner_pages={summary['owner_pages']}"
        )

    expected_fixed = {
        "physical_allocator_embedded_metadata": required_u64(summary, "allocator_metadata_pages", context),
        "physical_zone_descriptors": required_u64(summary, "zone_descriptor_pages", context),
        "per_cpu_physical_page_cache_metadata": required_u64(summary, "per_cpu_page_cache_reserve_pages", context),
    }
    for name, expected_pages in expected_fixed.items():
        if fixed_owner_pages.get(name) != expected_pages:
            fail(f"{context}: {name} pages do not match summary ({fixed_owner_pages.get(name)} != {expected_pages})")

    firmware_pages = checked_sum(
        [fixed_owner_pages.get(name, 0) for name in fixed_names - set(expected_fixed)],
        f"{context}.firmware_reserve_pages",
    )
    if firmware_pages != required_u64(summary, "firmware_reserve_pages", context):
        fail(f"{context}: firmware owner rows sum to {firmware_pages}, summary={summary['firmware_reserve_pages']}")
    return owners


def validate_summary(rows: list[tuple[str, dict[str, str]]], context: str) -> dict[str, Any]:
    summaries = [values for record, values in rows if record == "summary"]
    if len(summaries) != 1:
        fail(f"{context}: expected exactly one summary row, got {len(summaries)}")
    summary = summaries[0]
    if required_u64(summary, "schema", context) != 2:
        fail(f"{context}: expected schema 2")

    total = required_u64(summary, "total_pages", context)
    free = required_u64(summary, "free_pages", context)
    used = required_u64(summary, "used_pages", context)
    owner = required_u64(summary, "owner_pages", context)
    metadata = required_u64(summary, "allocator_metadata_pages", context)
    zone_descriptors = required_u64(summary, "zone_descriptor_pages", context)
    per_cpu = required_u64(summary, "per_cpu_page_cache_reserve_pages", context)
    identity = required_u64(summary, "identity_pages", context)
    mismatch = required_u64(summary, "identity_mismatch_pages", context)
    untracked = required_u64(summary, "untracked_unreclaimable_pages", context)
    firmware = required_u64(summary, "firmware_reserve_pages", context)
    physical_total = required_u64(summary, "physical_address_pages", context)
    physical_identity = required_u64(summary, "physical_address_identity_pages", context)

    computed_identity = checked_sum([free, owner, metadata, zone_descriptors, per_cpu], f"{context}.identity")
    if total != computed_identity or identity != computed_identity:
        fail(f"{context}: total={total} identity={identity} computed_identity={computed_identity}")
    if used != total - free:
        fail(f"{context}: used_pages={used} does not equal total-free={total - free}")
    if mismatch != 0:
        fail(f"{context}: identity_mismatch_pages={mismatch}")
    if untracked != 0:
        fail(f"{context}: untracked_unreclaimable_pages={untracked}")
    if physical_total != total + firmware or physical_identity != identity + firmware or physical_total != physical_identity:
        fail(f"{context}: physical-address equation does not balance")

    byte_fields = (
        ("total_pages", "total_bytes"),
        ("free_pages", "free_bytes"),
        ("used_pages", "used_bytes"),
        ("owner_pages", "owner_bytes"),
        ("allocator_metadata_pages", "allocator_metadata_bytes"),
        ("zone_descriptor_pages", "zone_descriptor_bytes"),
        ("per_cpu_page_cache_reserve_pages", "per_cpu_page_cache_reserve_bytes"),
        ("identity_pages", "identity_bytes"),
        ("firmware_reserve_pages", "firmware_reserve_bytes"),
    )
    for pages_key, bytes_key in byte_fields:
        pages = required_u64(summary, pages_key, context)
        byte_count = required_u64(summary, bytes_key, context)
        if byte_count != pages * PAGE_SIZE:
            fail(f"{context}: {bytes_key} does not equal {pages_key}*{PAGE_SIZE}")

    owners = validate_owner_rows(rows, summary, context)
    return {
        "schema": 2,
        "total_pages": total,
        "free_pages": free,
        "used_pages": used,
        "owner_pages": owner,
        "allocator_metadata_pages": metadata,
        "zone_descriptor_pages": zone_descriptors,
        "per_cpu_page_cache_reserve_pages": per_cpu,
        "identity_pages": identity,
        "identity_mismatch_pages": mismatch,
        "untracked_unreclaimable_pages": untracked,
        "firmware_reserve_pages": firmware,
        "physical_address_pages": physical_total,
        "physical_address_identity_pages": physical_identity,
        "owners": owners,
    }


def records_by_name(text: str, context: str, selected: set[str] | None = None) -> dict[str, list[dict[str, str]]]:
    records: dict[str, list[dict[str, str]]] = {}
    for line_number, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.strip()
        if not line:
            continue
        record_name = line.split(maxsplit=1)[0]
        if selected is not None and record_name not in selected:
            continue
        name, values = parse_record(line, f"{context}:{line_number}")
        records.setdefault(name, []).append(values)
    return records


def validate_quiescent(netdiag_text: str, memacc_sections: dict[str, list[tuple[str, dict[str, str]]]], context: str) -> None:
    selected_records = set(QUIESCENT_RECORD_KEYS) | {
        "wki_compute_task",
        "wki_ipc_diag",
        "wki_vfs_proxy",
    }
    records = records_by_name(netdiag_text, f"{context}.netdiag", selected_records)
    for record, keys in QUIESCENT_RECORD_KEYS.items():
        rows = records.get(record, [])
        if len(rows) != 1:
            fail(f"{context}: expected exactly one {record} row, got {len(rows)}")
        for key in keys:
            value = required_u64(rows[0], key, f"{context}.{record}")
            if value != 0:
                fail(f"{context}: {record}.{key}={value} at quiescence")
    if records.get("wki_compute_task") or records.get("wki_ipc_diag"):
        fail(f"{context}: retained WKI compute or IPC object detail rows")
    for row in records.get("wki_vfs_proxy", []):
        for key in ("op_pending", "attach_pending"):
            value = required_u64(row, key, f"{context}.wki_vfs_proxy")
            if value != 0:
                fail(f"{context}: wki_vfs_proxy.{key}={value} at quiescence")

    for record, values in memacc_sections.get("procs", []):
        if record != "proc":
            continue
        name = values.get("name", "").lower()
        command = values.get("cmd", "").lower()
        if any(fragment in name or fragment in command for fragment in BUILD_PROCESS_FRAGMENTS):
            fail(f"{context}: retained build process name={name!r} cmd={command!r}")


def validate_diagnostic_features(
    memacc_sections: dict[str, list[tuple[str, dict[str, str]]]],
    context: str,
) -> None:
    rows = memacc_sections.get("features", [])
    for feature_name in DIAGNOSTIC_FEATURES:
        matches = [values for record, values in rows if record == "feature" and values.get("name") == feature_name]
        if len(matches) != 1:
            fail(f"{context}: expected exactly one {feature_name} diagnostic feature row")
        for key in ("default", "enabled"):
            value = required_u64(matches[0], key, f"{context}.feature.{feature_name}")
            if value != 0:
                fail(f"{context}: diagnostic feature {feature_name}.{key}={value} in normal accounting run")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_status_tsv(path: Path, label: str, expected_hosts: list[str], snapshots: dict[str, dict[str, Any]]) -> str:
    if not path.is_file():
        fail(f"missing status table: {path}")
    with path.open(newline="") as source:
        reader = csv.DictReader(source, delimiter="\t")
        rows = list(reader)
    if [row.get("host") for row in rows] != expected_hosts:
        fail(f"{path}: host order or membership does not match --systems")
    timestamps = {row.get("timestamp_utc", "") for row in rows}
    if len(timestamps) != 1 or "" in timestamps:
        fail(f"{path}: expected one nonempty checkpoint timestamp")
    for row in rows:
        host = row["host"]
        snapshot = snapshots[host]
        if row.get("checkpoint") != label or row.get("status") != "pass":
            fail(f"{path}: {host} checkpoint/status mismatch")
        for key in (
            "total_pages",
            "free_pages",
            "owner_pages",
            "identity_pages",
            "identity_mismatch_pages",
            "untracked_unreclaimable_pages",
        ):
            if parse_u64(row.get(key, ""), f"{path}:{host}.{key}") != snapshot[key]:
                fail(f"{path}: {host}.{key} disagrees with coherent memacc snapshot")
    return timestamps.pop()


def validate_checkpoint(checkpoint_dir: Path, label: str, systems: list[str], require_quiescent: bool) -> dict[str, Any]:
    if not checkpoint_dir.is_dir():
        fail(f"checkpoint directory does not exist: {checkpoint_dir}")
    snapshots: dict[str, dict[str, Any]] = {}
    files: dict[str, dict[str, str]] = {}
    for host in systems:
        all_path = checkpoint_dir / f"{host}-memacc-all.txt"
        netdiag_path = checkpoint_dir / f"{host}-wki-netdiag.txt"
        if not all_path.is_file() or not netdiag_path.is_file():
            fail(f"{host}: checkpoint is missing memacc or WKI evidence")
        sections = parse_sections(all_path.read_text(), str(all_path))
        if "summary" not in sections:
            fail(f"{all_path}: missing summary section")
        snapshot = validate_summary(sections["summary"], f"{label}.{host}")
        validate_diagnostic_features(sections, f"{label}.{host}")
        if require_quiescent:
            validate_quiescent(netdiag_path.read_text(), sections, f"{label}.{host}")
        snapshots[host] = snapshot

        host_files: dict[str, str] = {}
        for suffix in ("memacc-all.txt", "memacc-summary.txt", "wki-netdiag.txt", "kipcstat.txt", "kcpustate.txt", "meminfo.txt"):
            path = checkpoint_dir / f"{host}-{suffix}"
            if not path.is_file():
                fail(f"{host}: missing checkpoint evidence {path.name}")
            host_files[path.name] = sha256_file(path)
        files[host] = host_files

    timestamp = validate_status_tsv(checkpoint_dir / "status.tsv", label, systems, snapshots)
    return {
        "schema": 1,
        "checkpoint": label,
        "timestamp_utc": timestamp,
        "require_quiescent": require_quiescent,
        "systems": systems,
        "status": "pass",
        "nodes": snapshots,
        "sha256": files,
    }


def parse_systems(value: str) -> list[str]:
    systems = value.split(",")
    if not systems or any(not host or not all(ch.isalnum() or ch in "._-" for ch in host) for host in systems):
        raise argparse.ArgumentTypeError("expected a comma-separated list of safe hostnames")
    if len(set(systems)) != len(systems):
        raise argparse.ArgumentTypeError("hostnames must be unique")
    return systems


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkpoint-dir", required=True, type=Path)
    parser.add_argument("--label", required=True)
    parser.add_argument("--systems", default="wos-0,wos-1,wos-2,wos-3", type=parse_systems)
    parser.add_argument("--require-quiescent", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    output = args.output or args.checkpoint_dir / "snapshot.json"
    try:
        payload = validate_checkpoint(args.checkpoint_dir, args.label, args.systems, args.require_quiescent)
        write_json(output, payload)
    except (OSError, UnicodeError, ValidationError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"checkpoint={args.label} nodes={len(args.systems)} status=pass json={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
