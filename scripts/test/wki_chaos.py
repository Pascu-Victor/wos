#!/usr/bin/env python3
"""Deterministic host runner and reducer for bounded WKI chaos scenarios."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[2]
CLUSTER_DIR = ROOT / "scripts" / "cluster"
if str(CLUSTER_DIR) not in sys.path:
    sys.path.insert(0, str(CLUSTER_DIR))

from qmp import QmpClient  # noqa: E402


SCENARIO_FORMAT = "wos.wki-chaos-scenario"
PLAN_FORMAT = "wos.wki-chaos-plan"
REPLAY_FORMAT = "wos.wki-chaos-replay"
RUN_FORMAT = "wos.wki-chaos-run"
REDUCTION_FORMAT = "wos.wki-chaos-reduction"
MATRIX_FORMAT = "wos.wki-chaos-matrix"
MATRIX_REPORT_FORMAT = "wos.wki-chaos-matrix-report"
LANE_RESULT_FORMAT = "wos.wki-chaos-lane-result"
SCENARIO_CATALOG_FORMAT = "wos.wki-chaos-scenario-catalog"
FORMAT_VERSION = 1
GENERATOR_NAME = "splitmix64"
GENERATOR_VERSION = 1
MASK64 = (1 << 64) - 1

MAX_INPUT_BYTES = 40 * 1024 * 1024
MAX_NODES = 32
MAX_EVENTS = 256
MAX_ALTERNATIVES = 64
MAX_ARGV = 64
MAX_ARG_BYTES = 4096
MAX_COMMAND_BYTES = 32768
MAX_COMMAND_OUTPUT_BYTES = 64 * 1024
# WKI netdiag and chaos proc files are generated into fixed 256 KiB kernel
# buffers. Snapshot capture may retain that producer ceiling without widening
# ordinary guest-command or stderr capture.
MAX_SNAPSHOT_OUTPUT_BYTES = 256 * 1024
MAX_PEER_EPOCH_OUTPUT_BYTES = MAX_SNAPSHOT_OUTPUT_BYTES + MAX_COMMAND_OUTPUT_BYTES
# WOSDBG deliberately rejects any single telemetry JSONL record larger than
# 1 MiB. Keep the producer bound identical so every emitted batch is directly
# consumable by the shared analysis backend.
MAX_TELEMETRY_LINE_BYTES = 1024 * 1024
MAX_GUEST_TELEMETRY_RECORDS = 256
MAX_GUEST_POLL_ATTEMPTS = 64
MAX_GUEST_POLL_INTERVAL_MS = 10_000
MAX_SNAPSHOT_ROWS = 1024
MAX_CONVERGENCE_SAMPLES = 32
MAX_EVENT_TIMEOUT_MS = 300_000
MAX_REDUCTION_ATTEMPTS = 128
MAX_ARTIFACTS = 8192
MAX_TEXT_BYTES = 4096
MAX_QMP_EVENTS_IN_REPLAY = 256
QUALIFIER_NAME = "wos-wki-chaos"
DEFAULT_MATRIX_PATH = ROOT / "configs" / "wki_chaos_matrix.json"
DEFAULT_CATALOG_PATH = ROOT / "configs" / "wki_chaos_scenarios.json"
KTEST_PREFIX_RE = r"(?:\[KTEST\]\s+|(?:^|\n)[^\n]*\bktest:\s+)"
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
SAFE_HOST_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,255}$")
SAFE_QMP_NAME_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,128}$")
SHELL_PROGRAMS = {
    "ash",
    "bash",
    "busybox-sh",
    "csh",
    "dash",
    "fish",
    "ksh",
    "sh",
    "tcsh",
    "zsh",
}
GUEST_ROLES = {"control", "workload", "snapshot", "invariant"}
QMP_COMMANDS = {"cont", "set_link", "stop", "system_reset"}
# Read the packet-pool-bearing source before opening the other per-source SSH
# observers.  Otherwise their just-closed TCP sessions can transiently retain
# a PacketBuffer and make an exact zero-delta convergence check observe its
# own collection traffic rather than the workload under test.
SNAPSHOT_SOURCES = ("netdiag", "chaos", "kipcstat", "peers", "pipes")
PEERS_HEADER_FIELDS = (
    "hostname",
    "node_id",
    "connected",
    "cpus",
    "load_pct",
    "last_update_us",
    "local",
)
PEER_EPOCH_PROBE_ARGV = [
    "/usr/bin/cat",
    "/proc/wki/peers",
    "/proc/wki/netdiag",
]
DEFAULT_SNAPSHOT_COMMANDS = {
    "chaos": ["/usr/bin/cat", "/proc/wki/chaos"],
    "kipcstat": ["/usr/bin/cat", "/proc/kipcstat"],
    "netdiag": ["/usr/bin/cat", "/proc/wki/netdiag"],
    "peers": ["/usr/bin/cat", "/proc/wki/peers"],
    "pipes": ["/usr/bin/cat", "/proc/wki/pipes"],
}
SNAPSHOT_END_ROWS = {
    "chaos": "wki_chaos_end",
    "netdiag": "wki_netdiag_end",
}
FAILURE_POLICIES = {"continue", "stop"}
FAULT_ACTIONS = {
    "corrupt",
    "delay",
    "drop",
    "duplicate",
    "fail",
    "partition",
    "pass",
    "reorder",
    "restart",
    "transport-failure",
}
FAULT_KEYS = {
    "action",
    "after",
    "boundary",
    "channel",
    "destination",
    "direction",
    "every",
    "limit",
    "message_type",
    "neighbor",
    "occurrence",
    "op",
    "ownership",
    "rule_id",
    "sequence",
    "source",
}
PLAN_SEED_PLACEHOLDER = "seed=$PLAN_SEED"
PLAN_SEED_ENABLE_ARGV = [
    "/usr/bin/wkictl",
    "chaos",
    "enable",
    PLAN_SEED_PLACEHOLDER,
]
FINAL_CLEANUP_COMMANDS = (
    ["/usr/bin/wkictl", "chaos-workload", "clear"],
    ["/usr/bin/wkictl", "chaos", "clear"],
)
WORKLOAD_CLEAR_ROW = {
    "equals": {"detached": "1", "op": "none", "status": "0"},
    "negative": [],
    "nonzero": [],
    "one_of": {},
    "prefix": "wki_chaos_workload",
}


class ChaosError(RuntimeError):
    pass


class ChaosInputError(ChaosError):
    pass


class SplitMix64:
    """Versioned PRNG whose unsigned-64-bit behavior is part of the plan ABI."""

    def __init__(self, seed: int):
        if seed < 0 or seed > MASK64:
            raise ChaosInputError("seed must be an unsigned 64-bit integer")
        self.state = seed

    def next_u64(self) -> int:
        self.state = (self.state + 0x9E3779B97F4A7C15) & MASK64
        value = self.state
        value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
        value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & MASK64
        return (value ^ (value >> 31)) & MASK64

    def bounded(self, bound: int) -> int:
        if bound <= 0:
            raise ValueError("PRNG bound must be positive")
        rejection_floor = ((1 << 64) - bound) % bound
        while True:
            value = self.next_u64()
            if value >= rejection_floor:
                return value % bound


class _BoundedBytes:
    def __init__(self, limit: int):
        self.limit = limit
        self.data = bytearray()
        self.total = 0

    def add(self, chunk: bytes) -> None:
        self.total += len(chunk)
        remaining = self.limit - len(self.data)
        if remaining > 0:
            self.data.extend(chunk[:remaining])

    @property
    def truncated(self) -> bool:
        return self.total > len(self.data)

    def text(self) -> str:
        return bytes(self.data).decode("utf-8", errors="replace")


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ChaosInputError(f"duplicate JSON member {key!r}")
        result[key] = value
    return result


def load_json(path: Path) -> dict[str, Any]:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise ChaosInputError(f"cannot stat {path}: {exc}") from exc
    if size > MAX_INPUT_BYTES:
        raise ChaosInputError(f"{path} exceeds the {MAX_INPUT_BYTES}-byte input limit")
    try:
        raw = path.read_text(encoding="utf-8")
        value = json.loads(raw, object_pairs_hook=_object_without_duplicates)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ChaosInputError(f"cannot parse {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ChaosInputError(f"{path} must contain one JSON object")
    return value


def canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def canonical_digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def write_new_bytes(path: Path, data: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(path, flags, mode)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        try:
            path.unlink()
        except OSError:
            pass
        raise


def write_new_json(path: Path, value: Any) -> None:
    write_new_bytes(path, canonical_json_bytes(value))


def create_output_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.mkdir(mode=0o700)
    except FileExistsError as exc:
        raise ChaosInputError(f"output path already exists: {path}") from exc


def _require_object(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ChaosInputError(f"{where} must be an object")
    return value


def _require_keys(value: dict[str, Any], allowed: set[str], required: set[str], where: str) -> None:
    missing = sorted(required - value.keys())
    unknown = sorted(value.keys() - allowed)
    if missing:
        raise ChaosInputError(f"{where} is missing required member(s): {', '.join(missing)}")
    if unknown:
        raise ChaosInputError(f"{where} has unknown member(s): {', '.join(unknown)}")


def _bounded_string(value: Any, where: str, limit: int = MAX_TEXT_BYTES) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > limit:
        raise ChaosInputError(f"{where} must be a non-empty string of at most {limit} bytes")
    if "\0" in value or "\r" in value or "\n" in value:
        raise ChaosInputError(f"{where} must not contain NUL or line breaks")
    return value


def _safe_id(value: Any, where: str) -> str:
    text = _bounded_string(value, where, 128)
    if SAFE_ID_RE.fullmatch(text) is None:
        raise ChaosInputError(f"{where} contains unsupported characters")
    return text


def _bounded_int(value: Any, minimum: int, maximum: int, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > maximum:
        raise ChaosInputError(f"{where} must be an integer in [{minimum}, {maximum}]")
    return value


def parse_seed(value: str | int) -> int:
    if isinstance(value, bool):
        raise ChaosInputError("seed must be an unsigned 64-bit integer")
    if isinstance(value, int):
        seed = value
    elif isinstance(value, str):
        try:
            seed = int(value, 0)
        except ValueError as exc:
            raise ChaosInputError(f"invalid seed {value!r}") from exc
    else:
        raise ChaosInputError("seed must be an unsigned 64-bit integer")
    if seed < 0 or seed > MASK64:
        raise ChaosInputError("seed must be an unsigned 64-bit integer")
    return seed


def _validate_node(raw: Any, index: int) -> dict[str, Any]:
    node = _require_object(raw, f"nodes[{index}]")
    _require_keys(
        node,
        {"boot_id", "host", "id", "nics", "qmp_socket"},
        {"id"},
        f"nodes[{index}]",
    )
    result: dict[str, Any] = {"id": _safe_id(node["id"], f"nodes[{index}].id")}
    if "host" in node:
        host = _bounded_string(node["host"], f"nodes[{index}].host", 255)
        if SAFE_HOST_RE.fullmatch(host) is None:
            raise ChaosInputError(f"nodes[{index}].host contains unsupported characters")
        result["host"] = host
    if "boot_id" in node:
        result["boot_id"] = _bounded_string(node["boot_id"], f"nodes[{index}].boot_id", 128)
    if "qmp_socket" in node:
        result["qmp_socket"] = _bounded_string(
            node["qmp_socket"], f"nodes[{index}].qmp_socket", 4096
        )
    if "nics" in node:
        nics = _require_object(node["nics"], f"nodes[{index}].nics")
        if len(nics) > 32:
            raise ChaosInputError(f"nodes[{index}].nics exceeds 32 entries")
        result["nics"] = {}
        for name, qmp_name in sorted(nics.items()):
            safe_name = _safe_id(name, f"nodes[{index}].nics key")
            qmp_text = _bounded_string(qmp_name, f"nodes[{index}].nics.{name}", 128)
            if SAFE_QMP_NAME_RE.fullmatch(qmp_text) is None:
                raise ChaosInputError(f"nodes[{index}].nics.{name} has an invalid QMP name")
            result["nics"][safe_name] = qmp_text
    return result


def _validate_failure_fields(operation: dict[str, Any], result: dict[str, Any], where: str) -> None:
    policy = operation.get("on_failure", "stop")
    if policy not in FAILURE_POLICIES:
        raise ChaosInputError(f"{where}.on_failure must be stop or continue")
    result["on_failure"] = policy
    if "failure_code" in operation:
        result["failure_code"] = _safe_id(operation["failure_code"], f"{where}.failure_code")
    if "failure_object" in operation:
        result["failure_object"] = _bounded_string(
            operation["failure_object"], f"{where}.failure_object", 256
        )


def _validate_direct_argv(raw: Any, where: str) -> list[str]:
    if not isinstance(raw, list) or not raw or len(raw) > MAX_ARGV:
        raise ChaosInputError(f"{where} must contain 1..{MAX_ARGV} string tokens")
    normalized: list[str] = []
    command_bytes = 0
    for arg_index, arg in enumerate(raw):
        text = _bounded_string(arg, f"{where}[{arg_index}]", MAX_ARG_BYTES)
        command_bytes += len(text.encode("utf-8"))
        normalized.append(text)
    if command_bytes > MAX_COMMAND_BYTES:
        raise ChaosInputError(f"{where} exceeds the {MAX_COMMAND_BYTES}-byte command limit")
    program = Path(normalized[0]).name
    if program in SHELL_PROGRAMS:
        raise ChaosInputError(f"{where} must execute a direct command, not shell {program!r}")
    seed_tokens = [token for token in normalized if "$PLAN_SEED" in token]
    if seed_tokens and normalized != PLAN_SEED_ENABLE_ARGV:
        raise ChaosInputError(
            f"{where} may use $PLAN_SEED only as the exact direct command "
            f"{' '.join(PLAN_SEED_ENABLE_ARGV)!r}"
        )
    return normalized


def _validate_expected_exits(raw: Any, where: str) -> list[int]:
    if not isinstance(raw, list) or not raw or len(raw) > 16:
        raise ChaosInputError(f"{where} must contain 1..16 exit codes")
    return sorted({_bounded_int(item, 0, 255, where) for item in raw})


def _validate_stdout_row(raw: Any, where: str) -> dict[str, Any]:
    row = _require_object(raw, where)
    _require_keys(
        row,
        {"equals", "negative", "nonzero", "one_of", "prefix"},
        {"prefix"},
        where,
    )
    prefix = _safe_id(row["prefix"], f"{where}.prefix")
    equals = _require_object(row.get("equals", {}), f"{where}.equals")
    one_of = _require_object(row.get("one_of", {}), f"{where}.one_of")
    if len(equals) > 64 or len(one_of) > 64:
        raise ChaosInputError(f"{where} exceeds 64 field assertions")

    normalized_equals: dict[str, str] = {}
    for key, value in sorted(equals.items()):
        field = _safe_id(key, f"{where}.equals key")
        normalized_equals[field] = _bounded_string(
            value, f"{where}.equals.{field}", 256
        )

    normalized_one_of: dict[str, list[str]] = {}
    for key, values in sorted(one_of.items()):
        field = _safe_id(key, f"{where}.one_of key")
        if not isinstance(values, list) or not values or len(values) > 16:
            raise ChaosInputError(f"{where}.one_of.{field} must contain 1..16 values")
        normalized_one_of[field] = sorted(
            {
                _bounded_string(value, f"{where}.one_of.{field}", 256)
                for value in values
            }
        )

    numeric_sets: dict[str, list[str]] = {}
    for constraint in ("negative", "nonzero"):
        values = row.get(constraint, [])
        if not isinstance(values, list) or len(values) > 64:
            raise ChaosInputError(f"{where}.{constraint} must contain at most 64 fields")
        numeric_sets[constraint] = sorted(
            {_safe_id(value, f"{where}.{constraint}") for value in values}
        )

    asserted = (
        set(normalized_equals)
        | set(normalized_one_of)
        | set(numeric_sets["negative"])
        | set(numeric_sets["nonzero"])
    )
    if not asserted:
        raise ChaosInputError(f"{where} must assert at least one field")
    if set(normalized_equals) & set(normalized_one_of):
        raise ChaosInputError(f"{where} cannot assert equals and one_of for one field")
    return {
        "equals": normalized_equals,
        "negative": numeric_sets["negative"],
        "nonzero": numeric_sets["nonzero"],
        "one_of": normalized_one_of,
        "prefix": prefix,
    }


def _validate_guest_operation(operation: dict[str, Any], nodes: dict[str, dict[str, Any]], where: str) -> dict[str, Any]:
    allowed = {
        "argv",
        "capture_telemetry",
        "expect_exit",
        "failure_code",
        "failure_object",
        "kind",
        "node",
        "on_failure",
        "role",
        "stdout_row",
        "timeout_ms",
    }
    _require_keys(operation, allowed, {"argv", "kind", "node", "role"}, where)
    node_id = _safe_id(operation["node"], f"{where}.node")
    if node_id not in nodes:
        raise ChaosInputError(f"{where}.node refers to unknown node {node_id!r}")
    if "host" not in nodes[node_id]:
        raise ChaosInputError(f"{where} requires nodes[{node_id!r}].host")
    role = operation["role"]
    if role not in GUEST_ROLES:
        raise ChaosInputError(f"{where}.role must be one of {sorted(GUEST_ROLES)}")
    normalized_argv = _validate_direct_argv(operation["argv"], f"{where}.argv")
    timeout_ms = _bounded_int(
        operation.get("timeout_ms", 10_000), 1, MAX_EVENT_TIMEOUT_MS, f"{where}.timeout_ms"
    )
    exits = _validate_expected_exits(operation.get("expect_exit", [0]), f"{where}.expect_exit")
    capture_telemetry = operation.get("capture_telemetry", False)
    if not isinstance(capture_telemetry, bool):
        raise ChaosInputError(f"{where}.capture_telemetry must be boolean")
    result: dict[str, Any] = {
        "argv": normalized_argv,
        "capture_telemetry": capture_telemetry,
        "expect_exit": exits,
        "kind": "guest",
        "node": node_id,
        "role": role,
        "timeout_ms": timeout_ms,
    }
    if "stdout_row" in operation:
        result["stdout_row"] = _validate_stdout_row(
            operation["stdout_row"], f"{where}.stdout_row"
        )
    _validate_failure_fields(operation, result, where)
    return result


def _validate_guest_start_operation(
    operation: dict[str, Any], nodes: dict[str, dict[str, Any]], where: str
) -> dict[str, Any]:
    allowed = {
        "argv",
        "cleanup_argv",
        "cleanup_timeout_ms",
        "failure_code",
        "failure_object",
        "kind",
        "lifetime_ms",
        "node",
        "on_failure",
        "process_id",
    }
    _require_keys(
        operation,
        allowed,
        {"argv", "kind", "lifetime_ms", "node", "process_id"},
        where,
    )
    node_id = _safe_id(operation["node"], f"{where}.node")
    if node_id not in nodes or "host" not in nodes[node_id]:
        raise ChaosInputError(f"{where} requires a known node with host")
    result: dict[str, Any] = {
        "argv": _validate_direct_argv(operation["argv"], f"{where}.argv"),
        "cleanup_timeout_ms": _bounded_int(
            operation.get("cleanup_timeout_ms", 10_000),
            1,
            MAX_EVENT_TIMEOUT_MS,
            f"{where}.cleanup_timeout_ms",
        ),
        "kind": "guest_start",
        "lifetime_ms": _bounded_int(
            operation["lifetime_ms"], 1, MAX_EVENT_TIMEOUT_MS, f"{where}.lifetime_ms"
        ),
        "node": node_id,
        "process_id": _safe_id(operation["process_id"], f"{where}.process_id"),
    }
    if "cleanup_argv" in operation:
        result["cleanup_argv"] = _validate_direct_argv(
            operation["cleanup_argv"], f"{where}.cleanup_argv"
        )
    _validate_failure_fields(operation, result, where)
    return result


def _validate_guest_poll_operation(
    operation: dict[str, Any], nodes: dict[str, dict[str, Any]], where: str
) -> dict[str, Any]:
    allowed = {
        "argv",
        "attempt_timeout_ms",
        "deadline_ms",
        "expect_exit",
        "failure_code",
        "failure_object",
        "interval_ms",
        "kind",
        "max_attempts",
        "node",
        "on_failure",
        "peer_epoch_probe",
        "role",
        "stdout_row",
    }
    _require_keys(
        operation,
        allowed,
        {"argv", "attempt_timeout_ms", "deadline_ms", "interval_ms", "kind", "max_attempts", "node", "role"},
        where,
    )
    node_id = _safe_id(operation["node"], f"{where}.node")
    if node_id not in nodes or "host" not in nodes[node_id]:
        raise ChaosInputError(f"{where} requires a known node with host")
    role = operation["role"]
    if role not in GUEST_ROLES:
        raise ChaosInputError(f"{where}.role must be one of {sorted(GUEST_ROLES)}")
    result: dict[str, Any] = {
        "argv": _validate_direct_argv(operation["argv"], f"{where}.argv"),
        "attempt_timeout_ms": _bounded_int(
            operation["attempt_timeout_ms"], 1, MAX_EVENT_TIMEOUT_MS, f"{where}.attempt_timeout_ms"
        ),
        "deadline_ms": _bounded_int(
            operation["deadline_ms"], 1, MAX_EVENT_TIMEOUT_MS, f"{where}.deadline_ms"
        ),
        "expect_exit": _validate_expected_exits(
            operation.get("expect_exit", [0]), f"{where}.expect_exit"
        ),
        "interval_ms": _bounded_int(
            operation["interval_ms"], 1, MAX_GUEST_POLL_INTERVAL_MS, f"{where}.interval_ms"
        ),
        "kind": "guest_poll",
        "max_attempts": _bounded_int(
            operation["max_attempts"], 1, MAX_GUEST_POLL_ATTEMPTS, f"{where}.max_attempts"
        ),
        "node": node_id,
        "role": role,
    }
    if "peer_epoch_probe" in operation:
        probe = _require_object(operation["peer_epoch_probe"], f"{where}.peer_epoch_probe")
        _require_keys(
            probe,
            {"baseline", "peer_host"},
            {"baseline", "peer_host"},
            f"{where}.peer_epoch_probe",
        )
        if result["argv"] != PEER_EPOCH_PROBE_ARGV:
            raise ChaosInputError(
                f"{where}.peer_epoch_probe requires exact argv "
                "/usr/bin/cat /proc/wki/peers /proc/wki/netdiag"
            )
        result["peer_epoch_probe"] = {
            "baseline": _safe_id(probe["baseline"], f"{where}.peer_epoch_probe.baseline"),
            "peer_host": _bounded_string(
                probe["peer_host"], f"{where}.peer_epoch_probe.peer_host", 255
            ),
        }
    if "stdout_row" in operation:
        result["stdout_row"] = _validate_stdout_row(
            operation["stdout_row"], f"{where}.stdout_row"
        )
    _validate_failure_fields(operation, result, where)
    return result


def _validate_guest_wait_operation(
    operation: dict[str, Any], nodes: dict[str, dict[str, Any]], where: str
) -> dict[str, Any]:
    allowed = {
        "expect_exit",
        "failure_code",
        "failure_object",
        "kind",
        "node",
        "on_failure",
        "process_id",
        "timeout_ms",
    }
    _require_keys(operation, allowed, {"kind", "node", "process_id", "timeout_ms"}, where)
    node_id = _safe_id(operation["node"], f"{where}.node")
    if node_id not in nodes or "host" not in nodes[node_id]:
        raise ChaosInputError(f"{where} requires a known node with host")
    result: dict[str, Any] = {
        "expect_exit": _validate_expected_exits(
            operation.get("expect_exit", [0]), f"{where}.expect_exit"
        ),
        "kind": "guest_wait",
        "node": node_id,
        "process_id": _safe_id(operation["process_id"], f"{where}.process_id"),
        "timeout_ms": _bounded_int(
            operation["timeout_ms"], 1, MAX_EVENT_TIMEOUT_MS, f"{where}.timeout_ms"
        ),
    }
    _validate_failure_fields(operation, result, where)
    return result


def _validate_snapshot_commands(
    raw: Any,
    node_id: str,
    nodes: dict[str, dict[str, Any]],
    timeout_ms: int,
    where: str,
) -> dict[str, list[str]]:
    commands = _require_object(raw, where)
    _require_keys(commands, set(SNAPSHOT_SOURCES), set(SNAPSHOT_SOURCES), where)
    result: dict[str, list[str]] = {}
    for source in SNAPSHOT_SOURCES:
        guest = _validate_guest_operation(
            {
                "argv": commands[source],
                "kind": "guest",
                "node": node_id,
                "role": "snapshot",
                "timeout_ms": timeout_ms,
            },
            nodes,
            f"{where}.{source}",
        )
        result[source] = guest["argv"]
    return result


def _validate_invariant_allowlist(raw: Any, where: str) -> dict[str, Any]:
    allow = _require_object(raw, where)
    _require_keys(
        allow,
        {"max", "nonzero", "packet_delta", "peer_disconnected", "peer_replaced"},
        set(),
        where,
    )
    nonzero = allow.get("nonzero", [])
    if not isinstance(nonzero, list) or len(nonzero) > 256:
        raise ChaosInputError(f"{where}.nonzero must be an array of at most 256 selectors")
    normalized_nonzero = sorted(
        {_safe_id(item, f"{where}.nonzero") for item in nonzero}
    )
    maximums = _require_object(allow.get("max", {}), f"{where}.max")
    if len(maximums) > 256:
        raise ChaosInputError(f"{where}.max exceeds 256 selectors")
    normalized_max: dict[str, int] = {}
    for selector, maximum in sorted(maximums.items()):
        normalized_max[_safe_id(selector, f"{where}.max key")] = _bounded_int(
            maximum, 0, (1 << 63) - 1, f"{where}.max.{selector}"
        )
    disconnected = allow.get("peer_disconnected", [])
    if not isinstance(disconnected, list) or len(disconnected) > MAX_NODES:
        raise ChaosInputError(f"{where}.peer_disconnected must be an array of node IDs")
    replaced = allow.get("peer_replaced", [])
    if not isinstance(replaced, list) or len(replaced) > MAX_NODES:
        raise ChaosInputError(f"{where}.peer_replaced must be an array of hostnames")
    normalized_replaced: set[str] = set()
    for item in replaced:
        hostname = _bounded_string(item, f"{where}.peer_replaced", 255)
        if SAFE_HOST_RE.fullmatch(hostname) is None:
            raise ChaosInputError(f"{where}.peer_replaced contains an invalid hostname")
        normalized_replaced.add(hostname)
    return {
        "max": normalized_max,
        "nonzero": normalized_nonzero,
        "packet_delta": _bounded_int(
            allow.get("packet_delta", 0), 0, 1 << 30, f"{where}.packet_delta"
        ),
        "peer_disconnected": sorted(
            {_safe_id(item, f"{where}.peer_disconnected") for item in disconnected}
        ),
        "peer_replaced": sorted(normalized_replaced),
    }


def _validate_snapshot_operation(
    operation: dict[str, Any], nodes: dict[str, dict[str, Any]], where: str
) -> dict[str, Any]:
    allowed = {
        "commands",
        "failure_code",
        "failure_object",
        "kind",
        "label",
        "node",
        "on_failure",
        "timeout_ms",
    }
    _require_keys(operation, allowed, {"kind", "label", "node"}, where)
    node_id = _safe_id(operation["node"], f"{where}.node")
    if node_id not in nodes or "host" not in nodes[node_id]:
        raise ChaosInputError(f"{where} requires a known node with host")
    timeout_ms = _bounded_int(
        operation.get("timeout_ms", 10_000), 1, MAX_EVENT_TIMEOUT_MS, f"{where}.timeout_ms"
    )
    result: dict[str, Any] = {
        "commands": _validate_snapshot_commands(
            operation.get("commands", DEFAULT_SNAPSHOT_COMMANDS),
            node_id,
            nodes,
            timeout_ms,
            f"{where}.commands",
        ),
        "kind": "snapshot",
        "label": _safe_id(operation["label"], f"{where}.label"),
        "node": node_id,
        "timeout_ms": timeout_ms,
    }
    _validate_failure_fields(operation, result, where)
    return result


def _validate_convergence_operation(
    operation: dict[str, Any], nodes: dict[str, dict[str, Any]], where: str
) -> dict[str, Any]:
    allowed = {
        "allow",
        "baseline",
        "commands",
        "deadline_ms",
        "failure_code",
        "failure_object",
        "interval_ms",
        "kind",
        "max_samples",
        "node",
        "on_failure",
        "timeout_ms",
    }
    _require_keys(
        operation,
        allowed,
        {"allow", "baseline", "deadline_ms", "kind", "node"},
        where,
    )
    node_id = _safe_id(operation["node"], f"{where}.node")
    if node_id not in nodes or "host" not in nodes[node_id]:
        raise ChaosInputError(f"{where} requires a known node with host")
    timeout_ms = _bounded_int(
        operation.get("timeout_ms", 10_000), 1, MAX_EVENT_TIMEOUT_MS, f"{where}.timeout_ms"
    )
    deadline_ms = _bounded_int(
        operation["deadline_ms"], 1, MAX_EVENT_TIMEOUT_MS, f"{where}.deadline_ms"
    )
    result: dict[str, Any] = {
        "allow": _validate_invariant_allowlist(operation["allow"], f"{where}.allow"),
        "baseline": _safe_id(operation["baseline"], f"{where}.baseline"),
        "commands": _validate_snapshot_commands(
            operation.get("commands", DEFAULT_SNAPSHOT_COMMANDS),
            node_id,
            nodes,
            timeout_ms,
            f"{where}.commands",
        ),
        "deadline_ms": deadline_ms,
        "kind": "convergence",
        "max_samples": _bounded_int(
            operation.get("max_samples", 8),
            2,
            MAX_CONVERGENCE_SAMPLES,
            f"{where}.max_samples",
        ),
        "node": node_id,
        "timeout_ms": timeout_ms,
    }
    if "interval_ms" in operation:
        result["interval_ms"] = _bounded_int(
            operation["interval_ms"],
            0,
            MAX_EVENT_TIMEOUT_MS,
            f"{where}.interval_ms",
        )
    _validate_failure_fields(operation, result, where)
    return result


def _validate_qmp_operation(operation: dict[str, Any], nodes: dict[str, dict[str, Any]], where: str) -> dict[str, Any]:
    allowed = {
        "arguments",
        "command",
        "failure_code",
        "failure_object",
        "kind",
        "name",
        "nic",
        "node",
        "on_failure",
        "timeout_ms",
        "up",
    }
    _require_keys(operation, allowed, {"command", "kind", "node"}, where)
    node_id = _safe_id(operation["node"], f"{where}.node")
    if node_id not in nodes:
        raise ChaosInputError(f"{where}.node refers to unknown node {node_id!r}")
    node = nodes[node_id]
    if "qmp_socket" not in node:
        raise ChaosInputError(f"{where} requires nodes[{node_id!r}].qmp_socket")
    command = operation["command"]
    if command not in QMP_COMMANDS:
        raise ChaosInputError(f"{where}.command must be one of {sorted(QMP_COMMANDS)}")
    timeout_ms = _bounded_int(
        operation.get("timeout_ms", 10_000), 1, MAX_EVENT_TIMEOUT_MS, f"{where}.timeout_ms"
    )
    result: dict[str, Any] = {
        "arguments": {},
        "command": command,
        "kind": "qmp",
        "node": node_id,
        "timeout_ms": timeout_ms,
    }
    if command == "set_link":
        arguments = operation.get("arguments")
        if arguments is not None:
            arguments = _require_object(arguments, f"{where}.arguments")
            _require_keys(arguments, {"name", "up"}, {"name", "up"}, f"{where}.arguments")
            if any(key in operation for key in ("name", "nic", "up")):
                raise ChaosInputError(f"{where} cannot mix arguments with name/nic/up")
            qmp_name = _bounded_string(arguments["name"], f"{where}.arguments.name", 128)
            up = arguments["up"]
        else:
            if "up" not in operation or ("name" in operation) == ("nic" in operation):
                raise ChaosInputError(f"{where} set_link requires up and exactly one of name or nic")
            up = operation["up"]
            if "nic" in operation:
                nic = _safe_id(operation["nic"], f"{where}.nic")
                try:
                    qmp_name = node.get("nics", {})[nic]
                except KeyError as exc:
                    raise ChaosInputError(f"{where}.nic refers to unknown NIC {nic!r}") from exc
            else:
                qmp_name = _bounded_string(operation["name"], f"{where}.name", 128)
        if SAFE_QMP_NAME_RE.fullmatch(qmp_name) is None:
            raise ChaosInputError(f"{where} has an invalid QMP link name")
        if not isinstance(up, bool):
            raise ChaosInputError(f"{where} set_link up must be boolean")
        result["arguments"] = {"name": qmp_name, "up": up}
    else:
        if any(key in operation for key in ("name", "nic", "up")):
            raise ChaosInputError(f"{where} {command} does not accept arguments")
        if "arguments" in operation:
            arguments = _require_object(operation["arguments"], f"{where}.arguments")
            _require_keys(arguments, set(), set(), f"{where}.arguments")
    _validate_failure_fields(operation, result, where)
    return result


def _validate_barrier_operation(operation: dict[str, Any], where: str) -> dict[str, Any]:
    _require_keys(operation, {"kind", "label"}, {"kind", "label"}, where)
    return {
        "kind": "barrier",
        "label": _safe_id(operation["label"], f"{where}.label"),
    }


def _validate_operation(raw: Any, nodes: dict[str, dict[str, Any]], where: str) -> dict[str, Any]:
    operation = _require_object(raw, where)
    kind = operation.get("kind")
    if kind == "guest":
        return _validate_guest_operation(operation, nodes, where)
    if kind == "qmp":
        return _validate_qmp_operation(operation, nodes, where)
    if kind == "barrier":
        return _validate_barrier_operation(operation, where)
    if kind == "snapshot":
        return _validate_snapshot_operation(operation, nodes, where)
    if kind == "convergence":
        return _validate_convergence_operation(operation, nodes, where)
    if kind == "guest_start":
        return _validate_guest_start_operation(operation, nodes, where)
    if kind == "guest_wait":
        return _validate_guest_wait_operation(operation, nodes, where)
    if kind == "guest_poll":
        return _validate_guest_poll_operation(operation, nodes, where)
    raise ChaosInputError(
        f"{where}.kind must be guest, guest_poll, guest_start, guest_wait, qmp, barrier, snapshot, or convergence"
    )


def _validate_fault(raw: Any, where: str) -> dict[str, str]:
    fault = _require_object(raw, where)
    _require_keys(fault, FAULT_KEYS, {"action"}, where)
    if fault["action"] not in FAULT_ACTIONS:
        raise ChaosInputError(f"{where}.action must be one of {sorted(FAULT_ACTIONS)}")
    result: dict[str, str] = {}
    for key, value in sorted(fault.items()):
        result[key] = _bounded_string(value, f"{where}.{key}", 128)
    return result


def _validate_convergence_baselines(events: list[dict[str, Any]], *, scenario: bool) -> None:
    prior_snapshot_ids: set[str] = set()
    for event in events:
        operations = event["operations"] if scenario else [event["operation"]]
        for operation in operations:
            if operation["kind"] == "convergence" and operation["baseline"] not in prior_snapshot_ids:
                raise ChaosInputError(
                    f"convergence event {event['id']!r} refers to missing or non-prior snapshot "
                    f"{operation['baseline']!r}"
                )
            probe = operation.get("peer_epoch_probe")
            if probe is not None and probe["baseline"] not in prior_snapshot_ids:
                raise ChaosInputError(
                    f"peer epoch poll event {event['id']!r} refers to missing or non-prior "
                    f"snapshot {probe['baseline']!r}"
                )
        if operations and all(operation["kind"] == "snapshot" for operation in operations):
            prior_snapshot_ids.add(event["id"])


def _validate_async_lifecycle(events: list[dict[str, Any]], *, scenario: bool) -> None:
    starts: dict[str, tuple[str, str]] = {}
    waits: set[str] = set()
    for event in events:
        operations = event["operations"] if scenario else [event["operation"]]
        if len(operations) > 1 and any(
            operation["kind"] in {"guest_start", "guest_wait"} for operation in operations
        ):
            raise ChaosInputError(
                f"async event {event['id']!r} cannot use randomized alternatives"
            )
        for operation in operations:
            process_id = operation.get("process_id")
            if operation["kind"] == "guest_start":
                if process_id in starts:
                    raise ChaosInputError(f"duplicate async process ID {process_id!r}")
                starts[process_id] = (operation["node"], event["group"])
            elif operation["kind"] == "guest_wait":
                if process_id not in starts:
                    raise ChaosInputError(
                        f"async wait {event['id']!r} has no prior start for {process_id!r}"
                    )
                if process_id in waits:
                    raise ChaosInputError(f"duplicate async wait for process ID {process_id!r}")
                start_node, start_group = starts[process_id]
                if operation["node"] != start_node:
                    raise ChaosInputError(
                        f"async process {process_id!r} starts and waits on different nodes"
                    )
                if event["group"] != start_group:
                    raise ChaosInputError(
                        f"async process {process_id!r} start/wait must share one reducer group"
                    )
                waits.add(process_id)
    missing = sorted(starts.keys() - waits)
    if missing:
        raise ChaosInputError(f"async process ID(s) lack a required wait: {', '.join(missing)}")


def validate_scenario(raw: dict[str, Any]) -> dict[str, Any]:
    _require_keys(raw, {"events", "format", "name", "nodes", "version"}, {"events", "format", "name", "nodes", "version"}, "scenario")
    if raw["format"] != SCENARIO_FORMAT or raw["version"] != FORMAT_VERSION:
        raise ChaosInputError(f"scenario must use {SCENARIO_FORMAT!r} version {FORMAT_VERSION}")
    name = _safe_id(raw["name"], "scenario.name")
    raw_nodes = raw["nodes"]
    if not isinstance(raw_nodes, list) or not raw_nodes or len(raw_nodes) > MAX_NODES:
        raise ChaosInputError(f"scenario.nodes must contain 1..{MAX_NODES} nodes")
    node_list = [_validate_node(node, index) for index, node in enumerate(raw_nodes)]
    node_list.sort(key=lambda item: item["id"])
    nodes = {node["id"]: node for node in node_list}
    if len(nodes) != len(node_list):
        raise ChaosInputError("scenario.nodes contains duplicate node IDs")

    raw_events = raw["events"]
    if not isinstance(raw_events, list) or not raw_events or len(raw_events) > MAX_EVENTS:
        raise ChaosInputError(f"scenario.events must contain 1..{MAX_EVENTS} events")
    events: list[dict[str, Any]] = []
    event_ids: set[str] = set()
    for index, raw_event in enumerate(raw_events):
        where = f"events[{index}]"
        event = _require_object(raw_event, where)
        _require_keys(
            event,
            {"alternatives", "fault", "group", "id", "operation", "phase", "reducible"},
            {"id", "phase"},
            where,
        )
        event_id = _safe_id(event["id"], f"{where}.id")
        if event_id in event_ids:
            raise ChaosInputError(f"duplicate event ID {event_id!r}")
        event_ids.add(event_id)
        if ("operation" in event) == ("alternatives" in event):
            raise ChaosInputError(f"{where} requires exactly one of operation or alternatives")
        normalized: dict[str, Any] = {
            "group": _safe_id(event.get("group", event_id), f"{where}.group"),
            "id": event_id,
            "phase": _safe_id(event["phase"], f"{where}.phase"),
            "reducible": event.get("reducible", True),
        }
        if not isinstance(normalized["reducible"], bool):
            raise ChaosInputError(f"{where}.reducible must be boolean")
        if "fault" in event:
            normalized["fault"] = _validate_fault(event["fault"], f"{where}.fault")
        if "operation" in event:
            normalized["operations"] = [
                _validate_operation(event["operation"], nodes, f"{where}.operation")
            ]
        else:
            alternatives = event["alternatives"]
            if not isinstance(alternatives, list) or not alternatives or len(alternatives) > MAX_ALTERNATIVES:
                raise ChaosInputError(f"{where}.alternatives must contain 1..{MAX_ALTERNATIVES} operations")
            normalized["operations"] = [
                _validate_operation(item, nodes, f"{where}.alternatives[{alt_index}]")
                for alt_index, item in enumerate(alternatives)
            ]
        events.append(normalized)
    _validate_convergence_baselines(events, scenario=True)
    _validate_async_lifecycle(events, scenario=True)
    return {
        "events": events,
        "format": SCENARIO_FORMAT,
        "name": name,
        "nodes": node_list,
        "version": FORMAT_VERSION,
    }


def _plan_without_digest(plan: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(plan)
    result.pop("planDigest", None)
    return result


def _set_plan_digest(plan: dict[str, Any]) -> dict[str, Any]:
    plan["planDigest"] = canonical_digest(_plan_without_digest(plan))
    return plan


def make_plan(scenario: dict[str, Any], seed: int) -> dict[str, Any]:
    normalized = validate_scenario(scenario)
    generator = SplitMix64(seed)
    planned_events: list[dict[str, Any]] = []
    for ordinal, event in enumerate(normalized["events"]):
        operations = event["operations"]
        choice = generator.bounded(len(operations)) if len(operations) > 1 else 0
        selected_operation = copy.deepcopy(operations[choice])
        if selected_operation.get("argv") == PLAN_SEED_ENABLE_ARGV:
            selected_operation["argv"] = [
                "/usr/bin/wkictl",
                "chaos",
                "enable",
                f"seed={seed}",
            ]
        planned: dict[str, Any] = {
            "choiceIndex": choice,
            "group": event["group"],
            "id": event["id"],
            "operation": selected_operation,
            "ordinal": ordinal,
            "phase": event["phase"],
            "reducible": event["reducible"],
        }
        if "fault" in event:
            planned["fault"] = copy.deepcopy(event["fault"])
        planned_events.append(planned)
    _validate_async_lifecycle(planned_events, scenario=False)
    scenario_identity = copy.deepcopy(normalized)
    return _set_plan_digest(
        {
            "events": planned_events,
            "format": PLAN_FORMAT,
            "generator": {
                "name": GENERATOR_NAME,
                "seed": str(seed),
                "version": GENERATOR_VERSION,
            },
            "limits": {
                "commandOutputBytes": MAX_COMMAND_OUTPUT_BYTES,
                "eventTimeoutMs": MAX_EVENT_TIMEOUT_MS,
                "events": MAX_EVENTS,
                "guestTelemetryRecordsPerEvent": MAX_GUEST_TELEMETRY_RECORDS,
            },
            "nodes": copy.deepcopy(normalized["nodes"]),
            "scenario": {
                "digest": canonical_digest(scenario_identity),
                "name": normalized["name"],
            },
            "version": FORMAT_VERSION,
        }
    )


def validate_plan(raw: dict[str, Any]) -> dict[str, Any]:
    _require_keys(
        raw,
        {"events", "format", "generator", "limits", "nodes", "planDigest", "scenario", "version"},
        {"events", "format", "generator", "limits", "nodes", "planDigest", "scenario", "version"},
        "plan",
    )
    if raw["format"] != PLAN_FORMAT or raw["version"] != FORMAT_VERSION:
        raise ChaosInputError(f"plan must use {PLAN_FORMAT!r} version {FORMAT_VERSION}")
    if raw["planDigest"] != canonical_digest(_plan_without_digest(raw)):
        raise ChaosInputError("planDigest does not match the canonical plan")
    generator = _require_object(raw["generator"], "plan.generator")
    _require_keys(generator, {"name", "seed", "version"}, {"name", "seed", "version"}, "plan.generator")
    if generator["name"] != GENERATOR_NAME or generator["version"] != GENERATOR_VERSION:
        raise ChaosInputError("plan uses an unsupported generator")
    plan_seed = parse_seed(generator["seed"])
    expected_limits = {
        "commandOutputBytes": MAX_COMMAND_OUTPUT_BYTES,
        "eventTimeoutMs": MAX_EVENT_TIMEOUT_MS,
        "events": MAX_EVENTS,
        "guestTelemetryRecordsPerEvent": MAX_GUEST_TELEMETRY_RECORDS,
    }
    if raw["limits"] != expected_limits:
        raise ChaosInputError(f"plan.limits must equal the version 1 bounds {expected_limits!r}")
    scenario = _require_object(raw["scenario"], "plan.scenario")
    _require_keys(scenario, {"digest", "name"}, {"digest", "name"}, "plan.scenario")
    _safe_id(scenario["name"], "plan.scenario.name")
    _bounded_string(scenario["digest"], "plan.scenario.digest", 80)
    raw_nodes = raw["nodes"]
    if not isinstance(raw_nodes, list) or not raw_nodes or len(raw_nodes) > MAX_NODES:
        raise ChaosInputError(f"plan.nodes must contain 1..{MAX_NODES} nodes")
    nodes_list = [_validate_node(node, index) for index, node in enumerate(raw_nodes)]
    nodes_list.sort(key=lambda item: item["id"])
    nodes = {node["id"]: node for node in nodes_list}
    if len(nodes) != len(nodes_list):
        raise ChaosInputError("plan.nodes contains duplicate node IDs")
    events = raw["events"]
    if not isinstance(events, list) or not events or len(events) > MAX_EVENTS:
        raise ChaosInputError(f"plan.events must contain 1..{MAX_EVENTS} events")
    normalized_events: list[dict[str, Any]] = []
    ids: set[str] = set()
    for index, event_value in enumerate(events):
        where = f"plan.events[{index}]"
        event = _require_object(event_value, where)
        _require_keys(
            event,
            {"choiceIndex", "fault", "group", "id", "operation", "ordinal", "phase", "reducible"},
            {"choiceIndex", "group", "id", "operation", "ordinal", "phase", "reducible"},
            where,
        )
        event_id = _safe_id(event["id"], f"{where}.id")
        if event_id in ids:
            raise ChaosInputError(f"duplicate plan event ID {event_id!r}")
        ids.add(event_id)
        ordinal = _bounded_int(event["ordinal"], 0, MAX_EVENTS - 1, f"{where}.ordinal")
        if ordinal != index:
            raise ChaosInputError(f"{where}.ordinal must equal its array position")
        normalized: dict[str, Any] = {
            "choiceIndex": _bounded_int(event["choiceIndex"], 0, MAX_ALTERNATIVES - 1, f"{where}.choiceIndex"),
            "group": _safe_id(event["group"], f"{where}.group"),
            "id": event_id,
            "operation": _validate_operation(event["operation"], nodes, f"{where}.operation"),
            "ordinal": ordinal,
            "phase": _safe_id(event["phase"], f"{where}.phase"),
            "reducible": event["reducible"],
        }
        if not isinstance(normalized["reducible"], bool):
            raise ChaosInputError(f"{where}.reducible must be boolean")
        if "fault" in event:
            normalized["fault"] = _validate_fault(event["fault"], f"{where}.fault")
        normalized_events.append(normalized)
    expected_enable_argv = [
        "/usr/bin/wkictl",
        "chaos",
        "enable",
        f"seed={plan_seed}",
    ]
    for event in normalized_events:
        argv = event["operation"].get("argv")
        if (
            isinstance(argv, list)
            and len(argv) >= 3
            and argv[:3] == ["/usr/bin/wkictl", "chaos", "enable"]
            and argv != expected_enable_argv
        ):
            raise ChaosInputError(
                f"plan event {event['id']!r} chaos-enable argv must carry the canonical generator seed"
            )
    _validate_convergence_baselines(normalized_events, scenario=False)
    _validate_async_lifecycle(normalized_events, scenario=False)
    normalized_plan = copy.deepcopy(raw)
    normalized_plan["nodes"] = nodes_list
    normalized_plan["events"] = normalized_events
    return normalized_plan


def _read_pipe(pipe, capture: _BoundedBytes) -> None:
    try:
        while True:
            chunk = pipe.read(8192)
            if not chunk:
                return
            capture.add(chunk)
    finally:
        pipe.close()


def run_bounded_command(
    argv: list[str],
    timeout_ms: int,
    *,
    stdout_limit_bytes: int = MAX_COMMAND_OUTPUT_BYTES,
) -> dict[str, Any]:
    stdout = _BoundedBytes(stdout_limit_bytes)
    stderr = _BoundedBytes(MAX_COMMAND_OUTPUT_BYTES)
    try:
        process = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except OSError as exc:
        return {
            "error": str(exc)[:MAX_TEXT_BYTES],
            "exitCode": None,
            "stderr": "",
            "stderrBytes": "0",
            "stderrTruncated": False,
            "stdout": "",
            "stdoutBytes": "0",
            "stdoutTruncated": False,
            "timedOut": False,
        }
    assert process.stdout is not None and process.stderr is not None
    readers = [
        threading.Thread(target=_read_pipe, args=(process.stdout, stdout), daemon=True),
        threading.Thread(target=_read_pipe, args=(process.stderr, stderr), daemon=True),
    ]
    for reader in readers:
        reader.start()
    timed_out = False
    try:
        process.wait(timeout=timeout_ms / 1000.0)
    except subprocess.TimeoutExpired:
        timed_out = True
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
    for reader in readers:
        reader.join(timeout=5.0)
    return {
        "exitCode": process.returncode,
        "stderr": stderr.text(),
        "stderrBytes": str(stderr.total),
        "stderrTruncated": stderr.truncated,
        "stdout": stdout.text(),
        "stdoutBytes": str(stdout.total),
        "stdoutTruncated": stdout.truncated,
        "timedOut": timed_out,
    }


class AsyncGuestProcess:
    """One foreground SSH command with bounded output and a lifetime watchdog."""

    def __init__(self, argv: list[str], lifetime_ms: int):
        self.stdout = _BoundedBytes(MAX_COMMAND_OUTPUT_BYTES)
        self.stderr = _BoundedBytes(MAX_COMMAND_OUTPUT_BYTES)
        self.process = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        assert self.process.stdout is not None and self.process.stderr is not None
        self.readers = [
            threading.Thread(
                target=_read_pipe, args=(self.process.stdout, self.stdout), daemon=True
            ),
            threading.Thread(
                target=_read_pipe, args=(self.process.stderr, self.stderr), daemon=True
            ),
        ]
        for reader in self.readers:
            reader.start()
        self.lifetime_expired = False
        self.collected = False
        self.result: dict[str, Any] | None = None
        self.timer = threading.Timer(lifetime_ms / 1000.0, self._expire)
        self.timer.daemon = True
        self.timer.start()

    def _signal(self, signal_number: int) -> None:
        if self.process.poll() is not None:
            return
        try:
            os.killpg(self.process.pid, signal_number)
        except ProcessLookupError:
            pass

    def _expire(self) -> None:
        if self.process.poll() is None:
            self.lifetime_expired = True
            self._signal(signal.SIGKILL)

    def collect(self, timeout_ms: int, *, forced: bool = False) -> dict[str, Any]:
        if self.collected:
            assert self.result is not None
            return copy.deepcopy(self.result)
        wait_timed_out = False
        forced_stop = forced and self.process.poll() is None
        if forced_stop:
            self._signal(signal.SIGTERM)
        try:
            self.process.wait(timeout=min(timeout_ms / 1000.0, 1.0) if forced_stop else timeout_ms / 1000.0)
        except subprocess.TimeoutExpired:
            wait_timed_out = not forced_stop
            self._signal(signal.SIGKILL)
            self.process.wait()
        self.timer.cancel()
        for reader in self.readers:
            reader.join(timeout=5.0)
        self.result = {
            "exitCode": self.process.returncode,
            "forcedStop": forced_stop,
            "lifetimeExpired": self.lifetime_expired,
            "stderr": self.stderr.text(),
            "stderrBytes": str(self.stderr.total),
            "stderrTruncated": self.stderr.truncated,
            "stdout": self.stdout.text(),
            "stdoutBytes": str(self.stdout.total),
            "stdoutTruncated": self.stdout.truncated,
            "timedOut": self.lifetime_expired or wait_timed_out,
            "waitExpired": wait_timed_out,
        }
        self.collected = True
        return copy.deepcopy(self.result)


class _SshControlScope:
    """One explicitly bounded OpenSSH control connection per scenario node."""

    def __init__(self, ssh_script: Path):
        self.ssh_script = ssh_script
        self.temporary = tempfile.TemporaryDirectory(prefix="wos-wki-chaos-ssh-", dir="/tmp")
        self.directory = Path(self.temporary.name)
        self.hosts: set[str] = set()
        self.previous: _SshControlScope | None = None

    def __enter__(self) -> _SshControlScope:
        global _ACTIVE_SSH_CONTROL_SCOPE
        self.previous = _ACTIVE_SSH_CONTROL_SCOPE
        if self.previous is not None:
            raise ChaosError("nested WKI chaos SSH control scopes are unsupported")
        _ACTIVE_SSH_CONTROL_SCOPE = self
        return self

    def command_argv(self, host: str, remote_argv: list[str]) -> list[str]:
        self.hosts.add(host)
        return [
            "/usr/bin/env",
            f"WOS_SSH_CONTROL_DIR={self.directory}",
            str(self.ssh_script),
            host,
            *remote_argv,
        ]

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        global _ACTIVE_SSH_CONTROL_SCOPE
        try:
            for host in sorted(self.hosts):
                run_bounded_command(
                    [
                        "/usr/bin/env",
                        f"WOS_SSH_CONTROL_DIR={self.directory}",
                        "WOS_SSH_CONTROL_COMMAND=exit",
                        str(self.ssh_script),
                        host,
                    ],
                    2000,
                )
        finally:
            _ACTIVE_SSH_CONTROL_SCOPE = self.previous
            self.temporary.cleanup()


_ACTIVE_SSH_CONTROL_SCOPE: _SshControlScope | None = None


def _run_guest_argv(
    node: dict[str, Any],
    argv: list[str],
    timeout_ms: int,
    ssh_script: Path,
    *,
    stdout_limit_bytes: int = MAX_COMMAND_OUTPUT_BYTES,
) -> dict[str, Any]:
    return run_bounded_command(
        _guest_transport_argv(node, argv, ssh_script),
        timeout_ms,
        stdout_limit_bytes=stdout_limit_bytes,
    )


def _guest_transport_argv(node: dict[str, Any], argv: list[str], ssh_script: Path) -> list[str]:
    remote_argv = [shlex.quote(token) for token in argv]
    if _ACTIVE_SSH_CONTROL_SCOPE is not None:
        if _ACTIVE_SSH_CONTROL_SCOPE.ssh_script != ssh_script:
            raise ChaosError("active WKI chaos SSH scope uses a different transport script")
        return _ACTIVE_SSH_CONTROL_SCOPE.command_argv(node["host"], remote_argv)
    return [str(ssh_script), node["host"], *remote_argv]


def _run_async_cleanup(
    handle: dict[str, Any], ssh_script: Path
) -> tuple[dict[str, Any] | None, bool, bool]:
    cleanup_argv = handle["operation"].get("cleanup_argv")
    if cleanup_argv is None:
        return None, True, True
    result = _run_guest_argv(
        handle["node"],
        cleanup_argv,
        handle["operation"]["cleanup_timeout_ms"],
        ssh_script,
    )
    success = (
        not result["timedOut"]
        and "error" not in result
        and result["exitCode"] == 0
    )
    complete = not result["stdoutTruncated"] and not result["stderrTruncated"]
    return result, success, complete


def parse_snapshot_rows(source: str, text: str) -> list[dict[str, Any]]:
    """Parse bounded proc diagnostics into named key=value rows."""
    if not text or not text.endswith("\n"):
        raise ChaosInputError(f"{source} snapshot is empty or lacks a terminating newline")
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        raise ChaosInputError(f"{source} snapshot contains no rows")
    rows: list[dict[str, Any]] = []
    if source == "peers" and lines[0].split() == list(PEERS_HEADER_FIELDS):
        for line_number, line in enumerate(lines[1:], 2):
            fields = line.split()
            if len(fields) != 7:
                raise ChaosInputError(f"peers row {line_number} has {len(fields)} columns, expected 7")
            rows.append(
                {
                    "row": "peer",
                    "values": dict(zip(lines[0].split(), fields, strict=True)),
                }
            )
        if not rows:
            raise ChaosInputError("peers snapshot contains no peer rows")
        return rows

    for line_number, line in enumerate(lines, 1):
        tokens = line.split()
        if source == "pipes" and tokens[0] == "waiters":
            if len(tokens) < 3 or tokens[1] not in {
                "read",
                "read_poll",
                "write",
                "write_poll",
            }:
                raise ChaosInputError(f"pipes row {line_number} has malformed waiter detail")
            waiter_pids = tokens[2:]
            omitted = "0"
            omitted_match = re.fullmatch(r"\.\.\.\(\+(\d+)\)", waiter_pids[-1])
            if omitted_match is not None:
                omitted = omitted_match.group(1)
                waiter_pids.pop()
            if not waiter_pids or any(not pid.isdecimal() for pid in waiter_pids):
                raise ChaosInputError(f"pipes row {line_number} has malformed waiter PID detail")
            rows.append(
                {
                    "row": "waiters",
                    "values": {
                        "kind": tokens[1],
                        "omitted": omitted,
                        "pids": ",".join(waiter_pids),
                    },
                }
            )
            if len(rows) > MAX_SNAPSHOT_ROWS:
                raise ChaosInputError(f"{source} exceeds the {MAX_SNAPSHOT_ROWS}-row snapshot bound")
            continue
        row_name = source
        if "=" not in tokens[0]:
            row_name = tokens.pop(0)
        if not tokens:
            raise ChaosInputError(f"{source} row {line_number} contains no key=value fields")
        values: dict[str, str] = {}
        for token in tokens:
            if "=" not in token:
                raise ChaosInputError(f"{source} row {line_number} has malformed token {token!r}")
            key, value = token.split("=", 1)
            if not key or key in values:
                raise ChaosInputError(f"{source} row {line_number} has invalid field {token!r}")
            if len(key) > 128 or len(value.encode("utf-8")) > 1024:
                raise ChaosInputError(f"{source} row {line_number} has an oversized field")
            values[key] = value
        rows.append({"row": row_name, "values": values})
        if len(rows) > MAX_SNAPSHOT_ROWS:
            raise ChaosInputError(f"{source} exceeds the {MAX_SNAPSHOT_ROWS}-row snapshot bound")
    return rows


def parse_peer_epoch_probe_rows(text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split one bounded peers+netdiag sample without using a guest shell."""
    if not text or not text.endswith("\n"):
        raise ChaosInputError("peer epoch snapshot is empty or lacks a terminating newline")
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines or lines[0].split() != list(PEERS_HEADER_FIELDS):
        raise ChaosInputError("peer epoch snapshot lacks the peers header")
    netdiag_start = next(
        (index for index, line in enumerate(lines) if line.startswith("packet_pool ")),
        None,
    )
    if netdiag_start is None:
        raise ChaosInputError("peer epoch snapshot lacks the netdiag section")
    peers_text = "\n".join(lines[:netdiag_start]) + "\n"
    netdiag_text = "\n".join(lines[netdiag_start:]) + "\n"
    return (
        parse_snapshot_rows("peers", peers_text),
        parse_snapshot_rows("netdiag", netdiag_text),
    )


def _match_peer_epoch_probe(
    text: str,
    *,
    peer_host: str,
    baseline_peer_id: int,
    baseline_epoch: int,
) -> tuple[bool, dict[str, Any]]:
    try:
        peer_rows, netdiag_rows = parse_peer_epoch_probe_rows(text)
    except ChaosInputError as exc:
        return False, {"error": str(exc)}
    terminal_positions = [
        index for index, row in enumerate(netdiag_rows) if row["row"] == "wki_netdiag_end"
    ]
    if (
        terminal_positions != [len(netdiag_rows) - 1]
        or netdiag_rows[-1]["values"] != {"complete": "1"}
    ):
        return False, {"error": "current netdiag snapshot is incomplete"}
    if any(
        "truncated" in row["row"].lower()
        or _numeric(row["values"].get("allocation_failed", "0")) != 0
        for row in netdiag_rows
    ):
        return False, {"error": "current netdiag snapshot reports truncation/allocation failure"}
    matching_peers = [
        row
        for row in peer_rows
        if row["values"].get("hostname") == peer_host
        and row["values"].get("local") == "0"
    ]
    connected_peers = [
        row for row in matching_peers if row["values"].get("connected") == "1"
    ]
    if len(connected_peers) != 1:
        return False, {
            "error": "current peers snapshot lacks one exact connected remote hostname row",
            "connectedMatchingPeerRows": len(connected_peers),
            "matchingPeerRows": len(matching_peers),
        }
    peer_values = connected_peers[0]["values"]
    current_peer_id = _numeric(peer_values.get("node_id", ""))
    if current_peer_id is None:
        return False, {"error": "current peer node ID is malformed"}
    lifecycle = [
        row
        for row in netdiag_rows
        if row["row"] == "wki_peer_lifecycle"
        and _numeric(row["values"].get("peer", "")) == current_peer_id
    ]
    if len(lifecycle) != 1:
        return False, {"error": "current snapshot lacks one exact successor lifecycle row"}
    values = lifecycle[0]["values"]
    current_epoch = _numeric(values.get("remote_boot_epoch", ""))
    matched = (
        peer_values.get("connected") == "1"
        and values.get("state") == "CONNECTED"
        and current_epoch is not None
        and current_epoch != 0
        and current_epoch != baseline_epoch
    )
    return matched, {
        "baselinePeerId": str(baseline_peer_id),
        "baselineRemoteBootEpoch": str(baseline_epoch),
        "currentPeerId": str(current_peer_id),
        "currentRemoteBootEpoch": values.get("remote_boot_epoch"),
        "connectedMatchingPeerRows": len(connected_peers),
        "matchingPeerRows": len(matching_peers),
        "matched": matched,
        "peerConnected": peer_values.get("connected"),
        "peerHost": peer_host,
        "state": values.get("state"),
    }


def _numeric(value: str) -> int | None:
    try:
        return int(value, 0)
    except ValueError:
        return None


def snapshot_completeness_issues(rows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    chaos_status_seen = False
    for source, source_rows in rows.items():
        terminal_row = SNAPSHOT_END_ROWS.get(source)
        if terminal_row is not None:
            positions = [
                index
                for index, row in enumerate(source_rows)
                if row["row"] == terminal_row
            ]
            terminal_complete = (
                len(positions) == 1
                and positions[0] == len(source_rows) - 1
                and source_rows[-1]["values"] == {"complete": "1"}
            )
            if not terminal_complete:
                issues.append(
                    {
                        "code": "diagnostic_terminal_incomplete",
                        "row": terminal_row,
                        "source": source,
                    }
                )
        for row in source_rows:
            if "truncated" in row["row"].lower():
                issues.append({"code": "diagnostic_rows_truncated", "row": row["row"]})
            for key, value in row["values"].items():
                number = _numeric(value)
                if "truncated" in key.lower() and (number is None or number != 0):
                    issues.append(
                        {"code": "diagnostic_rows_truncated", "row": row["row"], "field": key}
                    )
                if key == "allocation_failed" and (number is None or number != 0):
                    issues.append(
                        {"code": "diagnostic_allocation_failed", "row": row["row"]}
                    )
                if key in {"detail_complete", "io_detail", "lifecycle_detail"} and number != 1:
                    issues.append(
                        {
                            "code": "diagnostic_detail_incomplete",
                            "field": key,
                            "row": row["row"],
                        }
                    )
            if row["row"] == "wki_server_binding":
                rdma = _numeric(row["values"].get("rdma", "0"))
                ring_stable = _numeric(row["values"].get("ring_stable", "0"))
                if rdma == 1 and ring_stable != 1:
                    issues.append(
                        {"code": "rdma_ring_snapshot_unstable", "row": row["row"]}
                    )
            if row["row"] == "wki_chaos":
                chaos_status_seen = True
                for key in ("runtime_control", "supported"):
                    if _numeric(row["values"].get(key, "")) != 1:
                        issues.append(
                            {"code": "chaos_control_unavailable", "field": key}
                        )
        if source == "peers":
            for field in ("hostname", "node_id"):
                first_rows: dict[str, int] = {}
                for row_number, row in enumerate(source_rows, 1):
                    raw_value = row["values"].get(field, "")
                    value = raw_value
                    if field == "node_id":
                        numeric_value = _numeric(raw_value)
                        if numeric_value is not None:
                            value = str(numeric_value)
                    first_row = first_rows.setdefault(value, row_number)
                    if first_row != row_number:
                        issues.append(
                            {
                                "code": "peer_identity_duplicate",
                                "field": field,
                                "firstRow": first_row,
                                "row": row_number,
                                "value": raw_value,
                            }
                        )
    if not chaos_status_seen:
        issues.append({"code": "chaos_status_missing"})
    return issues


def _capture_snapshot(
    *,
    event_id: str,
    sample: str,
    operation: dict[str, Any],
    nodes: dict[str, dict[str, Any]],
    output_dir: Path,
    ssh_script: Path,
    deadline_ns: int | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    node = nodes[operation["node"]]
    rows: dict[str, list[dict[str, Any]]] = {}
    summaries: dict[str, Any] = {}
    complete = True
    snapshot_dir = output_dir / "snapshots" / event_id / sample
    for source in SNAPSHOT_SOURCES:
        timeout_ms = operation["timeout_ms"]
        if deadline_ns is not None:
            remaining_ns = deadline_ns - time.monotonic_ns()
            if remaining_ns <= 0:
                summaries[source] = {"error": "convergence deadline expired before capture"}
                complete = False
                continue
            timeout_ms = max(1, min(timeout_ms, remaining_ns // 1_000_000))
        result = _run_guest_argv(
            node,
            operation["commands"][source],
            timeout_ms,
            ssh_script,
            stdout_limit_bytes=MAX_SNAPSHOT_OUTPUT_BYTES,
        )
        stdout_path = snapshot_dir / f"{source}.txt"
        write_new_bytes(stdout_path, result["stdout"].encode("utf-8"))
        stderr_path: Path | None = None
        if result["stderr"]:
            stderr_path = snapshot_dir / f"{source}.stderr.txt"
            write_new_bytes(stderr_path, result["stderr"].encode("utf-8"))
        source_complete = (
            not result["timedOut"]
            and "error" not in result
            and result["exitCode"] == 0
            and not result["stdoutTruncated"]
            and not result["stderrTruncated"]
        )
        parse_error: str | None = None
        if source_complete:
            try:
                rows[source] = parse_snapshot_rows(source, result["stdout"])
            except ChaosInputError as exc:
                parse_error = str(exc)
                source_complete = False
        summary: dict[str, Any] = {
            "complete": source_complete,
            "exitCode": result["exitCode"],
            "sha256": "sha256:"
            + hashlib.sha256(result["stdout"].encode("utf-8")).hexdigest(),
            "stderrBytes": result["stderrBytes"],
            "stderrPath": stderr_path.relative_to(output_dir).as_posix() if stderr_path else None,
            "stderrTruncated": result["stderrTruncated"],
            "stdoutBytes": result["stdoutBytes"],
            "stdoutPath": stdout_path.relative_to(output_dir).as_posix(),
            "stdoutTruncated": result["stdoutTruncated"],
            "timedOut": result["timedOut"],
        }
        if "error" in result:
            summary["error"] = result["error"]
        if parse_error is not None:
            summary["parseError"] = parse_error
        summaries[source] = summary
        complete = complete and source_complete
    completeness_issues = snapshot_completeness_issues(rows) if complete else []
    if completeness_issues:
        complete = False
        summaries["invariantIssues"] = completeness_issues
    parsed_path = snapshot_dir / "parsed.json"
    write_new_json(parsed_path, {"complete": complete, "rows": rows})
    capture = {"complete": complete, "rows": rows}
    observation = {
        "complete": complete,
        "parsedPath": parsed_path.relative_to(output_dir).as_posix(),
        "sample": sample,
        "sources": summaries,
    }
    return capture, observation


ZERO_GAUGE_EXACT = {
    "ack_pending",
    "active_exports",
    "active_pumps",
    "active_proxies",
    "blocked_readers",
    "dev_op_work",
    "export_backlogs",
    "exports",
    "pending_complete",
    "proxies",
    "pump_tasks",
    "reorder_count",
    "retransmit_count",
}
ZERO_GAUGE_SUFFIXES = (
    "_pending",
    "_queue",
    "_queued",
    "_refs",
    "_waiters",
    "_work",
)
BASELINE_STABLE_GAUGES = {
    "kipcstat.local_pipe_active",
    "kipcstat.local_pipe_approx_alloc_bytes",
    "kipcstat.local_pipe_capacity",
    "kipcstat.local_pipe_direct_writes",
    "kipcstat.local_pipe_poll_waiters",
    "kipcstat.local_pipe_read_closed",
    "kipcstat.local_pipe_reader_waiters",
    "kipcstat.local_pipe_write_closed",
    "kipcstat.local_pipe_writer_waiters",
}
REPLACED_PEER_ZERO_GATES = (
    "block_resume",
    "compute_cleanup",
    "invalidate_discovery",
    "lifecycle",
    "owner_reboot",
    "vfs_rebind",
)
OWNER_IDENTITIES = {
    "wki_block_proxy": (
        "owner",
        "resource",
        "generation",
        "owner_boot",
        "incarnation",
        "ch",
        "ch_gen",
    ),
    # `kind` is a lifecycle state (submitted/running/pending_complete), not part
    # of the logical task identity. Seeing the same task in two states at the
    # quiescent convergence point is therefore duplicate ownership.
    "wki_compute_task": ("task", "peer", "ch_gen", "session_epoch"),
    "wki_net_proxy": ("owner", "resource", "generation", "ch", "ch_gen"),
    "wki_resource": ("kind", "node", "type", "id", "generation"),
    "wki_server_binding": (
        "consumer",
        "type",
        "resource",
        "owner_boot",
        "incarnation",
        "cookie",
        "ch",
        "ch_gen",
    ),
    "wki_vfs_proxy": ("owner", "res_id", "res_gen", "lane"),
}
IPC_OWNER_IDENTITIES = {
    "export": ("resource", "type", "peer", "ch", "cleanup_epoch"),
    "proxy": ("resource", "type", "peer", "ch", "cleanup_epoch"),
    "export_backlog": ("resource", "type", "peer", "ch"),
    "proxy_close": ("resource", "peer", "op"),
    "pending_delivery": ("resource", "peer", "cleanup_epoch"),
    "dev_op_work": ("peer", "ch", "seq", "cleanup_epoch"),
    "peer_cleanup": ("peer", "cleanup_epoch"),
}
# These keys intentionally omit fencing generations/epochs. At a quiescent
# convergence point there may be only one terminal owner for the logical
# object, even when stale and successor rows carry different exact identities.
# VFS keeps `lane`, since one mount legitimately owns multiple independent
# lanes concurrently.
LOGICAL_OWNER_IDENTITIES = {
    "wki_block_proxy": ("owner", "resource"),
    "wki_compute_task": ("task", "peer"),
    "wki_net_proxy": ("owner", "resource"),
    "wki_resource": ("kind", "node", "type", "id"),
    "wki_server_binding": ("consumer", "type", "resource"),
    "wki_vfs_proxy": ("owner", "res_id", "lane"),
}
IPC_LOGICAL_OWNER_IDENTITIES = {
    "export": ("resource", "type", "peer", "ch"),
    "proxy": ("resource", "type", "peer", "ch"),
    "export_backlog": ("resource", "type", "peer", "ch"),
    "proxy_close": ("resource", "peer", "op"),
    "pending_delivery": ("resource", "peer"),
    "dev_op_work": ("peer", "ch", "seq"),
    "peer_cleanup": ("peer",),
}


def _field_selector(row: dict[str, Any], key: str) -> str:
    return f"{row['row']}.{key}"


BLOCK_PROXY_RETIRED_ZERO_FIELDS = (
    "active",
    "epoch_reset",
    "cleanup",
    "resume_pending",
    "resume_active",
    "resume_after_detach",
    "detach_confirmed",
    "op_pending",
    "op_waiter",
    "attach_pending",
    "attach_waiter",
    "binding_cookie",
    "detach_pending",
    "detach_cookie",
    "rdma",
    "roce",
    "zone",
    "data_slots",
    "tags",
    "bulk",
    "bulk_max",
)

NET_PROXY_RETIRED_ZERO_FIELDS = (
    "refs",
    "active",
    "attaching",
    "registered",
    "epoch_reset",
    "op_pending",
    "op_id",
    "op_seq",
    "op_waiter",
    "attach_pending",
    "attach_waiter",
    "expected_cookie",
    "detach_pending",
    "detach_retry",
    "detach_cookie",
    "detach_boot",
)


def _is_retired_block_proxy_tombstone(row: dict[str, Any]) -> bool:
    """Return true only for the kernel's fully quiesced lifetime pin.

    Published ProxyBlockState storage intentionally survives unregister because
    external block users can retain raw bdev/private_data pointers.  Such a row
    is still useful diagnostic evidence, but after every ownership/resource
    gate is observably clear it no longer owns the remote resource identity.
    Missing or incomplete fields fail closed and remain ownership-bearing.
    """
    if row["row"] != "wki_block_proxy":
        return False
    values = row["values"]
    return (
        _numeric(values.get("published", "")) == 1
        and _numeric(values.get("lifecycle_detail", "")) == 1
        and _numeric(values.get("io_detail", "")) == 1
        and _numeric(values.get("owner_boot", "")) == 0
        and _numeric(values.get("incarnation", "")) == 0
        and _numeric(values.get("ch_gen", "")) == 0
        and all(_numeric(values.get(field, "")) == 0 for field in BLOCK_PROXY_RETIRED_ZERO_FIELDS)
    )


def _is_retired_net_proxy_tombstone(row: dict[str, Any]) -> bool:
    """Return true only for a fully quiesced published NetDevice lifetime pin."""
    if row["row"] != "wki_net_proxy":
        return False
    values = row["values"]
    return (
        _numeric(values.get("published", "")) == 1
        and _numeric(values.get("cleanup_started", "")) == 1
        and _numeric(values.get("cleanup_complete", "")) == 1
        and _numeric(values.get("retiring", "")) == 1
        and _numeric(values.get("detail_complete", "")) == 1
        and all(
            _numeric(values.get(field, "")) == 0
            for field in NET_PROXY_RETIRED_ZERO_FIELDS
        )
    )


def _is_retired_proxy_tombstone(row: dict[str, Any]) -> bool:
    return _is_retired_block_proxy_tombstone(row) or _is_retired_net_proxy_tombstone(row)


def _is_zero_gauge(row_name: str, key: str) -> bool:
    # /proc/wki/pipes exposes persistent local ownership and waiter detail.
    # Those references are not WKI work queues; the singleton kipcstat
    # lifecycle gauges below are compared with the pre-run baseline instead.
    if row_name in {"owner", "pipes", "waiters"} or key.startswith("local_pipe_"):
        return False
    return (
        key in ZERO_GAUGE_EXACT
        or key == "refs"
        or key.startswith("pending_")
        or key.endswith(ZERO_GAUGE_SUFFIXES)
    )


def _owner_identity(row: dict[str, Any]) -> tuple[str, tuple[str, ...]] | None:
    if _is_retired_proxy_tombstone(row):
        return None
    row_name = row["row"]
    values = row["values"]
    owner_kind = row_name
    identity_keys = OWNER_IDENTITIES.get(row_name)
    if row_name == "wki_ipc_diag":
        ipc_kind = values.get("kind")
        identity_keys = IPC_OWNER_IDENTITIES.get(ipc_kind or "")
        owner_kind = f"{row_name}:{ipc_kind}"
    if identity_keys is None or not all(key in values for key in identity_keys):
        return None
    return owner_kind, tuple(values[key] for key in identity_keys)


def _logical_owner_identity(row: dict[str, Any]) -> tuple[str, tuple[str, ...]] | None:
    if _is_retired_proxy_tombstone(row):
        return None
    row_name = row["row"]
    values = row["values"]
    owner_kind = row_name
    identity_keys = LOGICAL_OWNER_IDENTITIES.get(row_name)
    if row_name == "wki_ipc_diag":
        ipc_kind = values.get("kind")
        identity_keys = IPC_LOGICAL_OWNER_IDENTITIES.get(ipc_kind or "")
        owner_kind = f"{row_name}:{ipc_kind}"
    if identity_keys is None or not all(key in values for key in identity_keys):
        return None
    return owner_kind, tuple(values[key] for key in identity_keys)


def _logical_owner_fence_identity(
    row: dict[str, Any], exact_identity: tuple[str, ...]
) -> tuple[str, ...]:
    # One remote VFS resource intentionally owns several server bindings, one
    # per lane. Their channel/cookie identities differ, but every live lane
    # must carry the same resource-incarnation fence.
    if row["row"] == "wki_server_binding" and row["values"].get("type") == "vfs":
        return (row["values"]["owner_boot"], row["values"]["incarnation"])
    return exact_identity


def _stable_baseline_gauges(snapshot: dict[str, Any]) -> dict[str, int] | None:
    values: dict[str, int] = {}
    for source, rows in snapshot.get("rows", {}).items():
        for row in rows:
            for key, value in row["values"].items():
                selector = _field_selector(row, key)
                if selector not in BASELINE_STABLE_GAUGES:
                    continue
                number = _numeric(value)
                if number is None or selector in values:
                    return None
                values[selector] = number
    if values.keys() != BASELINE_STABLE_GAUGES:
        return None
    return values


def invariant_projection(snapshot: dict[str, Any], allow: dict[str, Any]) -> list[list[str]]:
    projection: list[list[str]] = []
    tracked = set(allow["nonzero"]) | set(allow["max"])
    for source in sorted(snapshot.get("rows", {})):
        for row_index, row in enumerate(snapshot["rows"][source]):
            for key, value in sorted(row["values"].items()):
                selector = _field_selector(row, key)
                if (
                    selector in tracked
                    or _is_zero_gauge(row["row"], key)
                    or selector in BASELINE_STABLE_GAUGES
                    or (row["row"] == "packet_pool" and key in {"free", "used"})
                    or (row["row"] == "peer" and key in {"connected", "node_id"})
                    or (source == "chaos" and ("overflow" in key or "held" in key or "queue" in key))
                ):
                    projection.append([source, str(row_index), row["row"], key, value])
    return projection


def _find_packet_pool(snapshot: dict[str, Any]) -> dict[str, str] | None:
    for row in snapshot.get("rows", {}).get("netdiag", []):
        if row["row"] == "packet_pool":
            return row["values"]
    return None


def _proven_replaced_peer_ids(
    baseline: dict[str, Any], snapshot: dict[str, Any], hostnames: list[str]
) -> tuple[set[str], list[dict[str, Any]]]:
    allowed_ids: set[str] = set()
    issues: list[dict[str, Any]] = []
    baseline_peers = baseline.get("rows", {}).get("peers", [])
    current_peers = snapshot.get("rows", {}).get("peers", [])
    baseline_lifecycles = [
        row
        for row in baseline.get("rows", {}).get("netdiag", [])
        if row["row"] == "wki_peer_lifecycle"
    ]
    current_lifecycles = [
        row
        for row in snapshot.get("rows", {}).get("netdiag", [])
        if row["row"] == "wki_peer_lifecycle"
    ]

    for hostname in hostnames:
        old_peer_rows = [
            row
            for row in baseline_peers
            if row["values"].get("hostname") == hostname
            and row["values"].get("local") == "0"
        ]
        if len(old_peer_rows) != 1:
            issues.append(
                {
                    "code": "peer_replacement_unproven",
                    "hostname": hostname,
                    "reason": "baseline_peer_not_unique",
                }
            )
            continue
        old_values = old_peer_rows[0]["values"]
        old_id = _numeric(old_values.get("node_id", ""))
        if old_id is None or old_values.get("connected") not in {"1", "CONNECTED", "connected"}:
            issues.append(
                {
                    "code": "peer_replacement_unproven",
                    "hostname": hostname,
                    "reason": "baseline_peer_not_connected",
                }
            )
            continue

        current_old_rows = [
            row
            for row in current_peers
            if _numeric(row["values"].get("node_id", "")) == old_id
        ]
        # A same-ID reboot does not leave a disconnected tombstone and needs
        # no exception. Only weaken the connected-peer invariant when the old
        # baseline identity is still visible and disconnected.
        if not current_old_rows or all(
            row["values"].get("connected") in {"1", "CONNECTED", "connected"}
            for row in current_old_rows
        ):
            continue
        if len(current_old_rows) != 1:
            issues.append(
                {
                    "code": "peer_replacement_unproven",
                    "hostname": hostname,
                    "reason": "old_peer_not_unique",
                }
            )
            continue

        successors = [
            row
            for row in current_peers
            if row["values"].get("hostname") == hostname
            and row["values"].get("local") == "0"
            and row["values"].get("connected") in {"1", "CONNECTED", "connected"}
        ]
        successor_id = (
            _numeric(successors[0]["values"].get("node_id", ""))
            if len(successors) == 1
            else None
        )
        old_lifecycle = [
            row
            for row in current_lifecycles
            if _numeric(row["values"].get("peer", "")) == old_id
        ]
        successor_lifecycle = [
            row
            for row in current_lifecycles
            if successor_id is not None
            and _numeric(row["values"].get("peer", "")) == successor_id
        ]
        old_baseline_lifecycle = [
            row
            for row in baseline_lifecycles
            if _numeric(row["values"].get("peer", "")) == old_id
        ]
        replacement_claims = [
            row
            for row in current_lifecycles
            if successor_id is not None
            and _numeric(row["values"].get("replacement", "")) == successor_id
        ]

        proven = (
            successor_id is not None
            and successor_id != old_id
            and len(old_baseline_lifecycle) == 1
            and len(old_lifecycle) == 1
            and len(successor_lifecycle) == 1
            and len(replacement_claims) == 1
        )
        if proven:
            baseline_values = old_baseline_lifecycle[0]["values"]
            retired_values = old_lifecycle[0]["values"]
            successor_values = successor_lifecycle[0]["values"]
            baseline_epoch = _numeric(baseline_values.get("remote_boot_epoch", ""))
            retired_epoch = _numeric(retired_values.get("remote_boot_epoch", ""))
            successor_epoch = _numeric(successor_values.get("remote_boot_epoch", ""))
            proven = (
                baseline_values.get("state") == "CONNECTED"
                and retired_values.get("state") == "FENCED"
                and successor_values.get("state") == "CONNECTED"
                and baseline_epoch is not None
                and baseline_epoch != 0
                and retired_epoch == baseline_epoch
                and successor_epoch is not None
                and successor_epoch != 0
                and successor_epoch != baseline_epoch
                and _numeric(retired_values.get("replacement", "")) == successor_id
                and _numeric(replacement_claims[0]["values"].get("peer", "")) == old_id
                and all(
                    _numeric(retired_values.get(field, "")) == 0
                    for field in REPLACED_PEER_ZERO_GATES
                )
            )

        if proven:
            allowed_ids.add(old_values["node_id"])
        else:
            issues.append(
                {
                    "code": "peer_replacement_unproven",
                    "hostname": hostname,
                    "oldNode": old_values.get("node_id"),
                    "reason": "identity_epoch_or_cleanup_fence_mismatch",
                    "successorNode": successors[0]["values"].get("node_id") if len(successors) == 1 else None,
                }
            )

    return allowed_ids, issues


def _proven_baseline_peer_tombstone_ids(
    baseline: dict[str, Any], snapshot: dict[str, Any]
) -> tuple[set[str], list[dict[str, Any]]]:
    allowed_ids: set[str] = set()
    issues: list[dict[str, Any]] = []
    baseline_peers = baseline.get("rows", {}).get("peers", [])
    current_peers = snapshot.get("rows", {}).get("peers", [])
    baseline_lifecycles = [
        row
        for row in baseline.get("rows", {}).get("netdiag", [])
        if row["row"] == "wki_peer_lifecycle"
    ]
    current_lifecycles = [
        row
        for row in snapshot.get("rows", {}).get("netdiag", [])
        if row["row"] == "wki_peer_lifecycle"
    ]

    for peer_row in baseline_peers:
        peer_values = peer_row["values"]
        if (
            peer_values.get("local") != "0"
            or peer_values.get("connected") in {"1", "CONNECTED", "connected"}
        ):
            continue
        node_id = _numeric(peer_values.get("node_id", ""))
        if node_id is None:
            issues.append({"code": "baseline_peer_tombstone_unproven", "reason": "node_id_invalid"})
            continue
        current_rows = [
            row
            for row in current_peers
            if _numeric(row["values"].get("node_id", "")) == node_id
        ]
        if not current_rows:
            continue
        if len(current_rows) != 1:
            issues.append(
                {
                    "code": "baseline_peer_tombstone_unproven",
                    "node": peer_values.get("node_id"),
                    "reason": "current_peer_not_unique",
                }
            )
            continue
        if current_rows[0]["values"].get("connected") in {"1", "CONNECTED", "connected"}:
            issues.append(
                {
                    "code": "baseline_peer_tombstone_reactivated",
                    "node": peer_values.get("node_id"),
                }
            )
            continue
        baseline_lifecycle = [
            row
            for row in baseline_lifecycles
            if _numeric(row["values"].get("peer", "")) == node_id
        ]
        current_lifecycle = [
            row
            for row in current_lifecycles
            if _numeric(row["values"].get("peer", "")) == node_id
        ]
        proven = len(baseline_lifecycle) == 1 and len(current_lifecycle) == 1
        if proven:
            before = baseline_lifecycle[0]["values"]
            after = current_lifecycle[0]["values"]
            before_epoch = _numeric(before.get("remote_boot_epoch", ""))
            after_epoch = _numeric(after.get("remote_boot_epoch", ""))
            before_replacement = _numeric(before.get("replacement", ""))
            after_replacement = _numeric(after.get("replacement", ""))
            proven = (
                before.get("state") == "FENCED"
                and after.get("state") == "FENCED"
                and before_epoch is not None
                and before_epoch != 0
                and after_epoch == before_epoch
                and before_replacement is not None
                and before_replacement != 0
                and after_replacement == before_replacement
                and all(
                    _numeric(before.get(field, "")) == 0
                    and _numeric(after.get(field, "")) == 0
                    for field in REPLACED_PEER_ZERO_GATES
                )
            )
        if proven:
            allowed_ids.add(peer_values["node_id"])
        else:
            issues.append(
                {
                    "code": "baseline_peer_tombstone_unproven",
                    "node": peer_values.get("node_id"),
                    "reason": "identity_epoch_or_cleanup_fence_mismatch",
                }
            )
    return allowed_ids, issues


def evaluate_invariants(
    baseline: dict[str, Any], snapshot: dict[str, Any], allow: dict[str, Any]
) -> tuple[str, list[dict[str, Any]]]:
    if not baseline.get("complete") or not snapshot.get("complete"):
        return "incomplete", [{"code": "snapshot_incomplete"}]
    issues: list[dict[str, Any]] = []
    allowed_nonzero = set(allow["nonzero"])
    maximums = allow["max"]
    replaced_peer_ids, replacement_issues = _proven_replaced_peer_ids(
        baseline, snapshot, allow.get("peer_replaced", [])
    )
    issues.extend(replacement_issues)
    baseline_tombstone_ids, baseline_tombstone_issues = (
        _proven_baseline_peer_tombstone_ids(baseline, snapshot)
    )
    issues.extend(baseline_tombstone_issues)
    baseline_gauges = _stable_baseline_gauges(baseline)
    current_gauges = _stable_baseline_gauges(snapshot)
    if baseline_gauges is None or current_gauges is None:
        return "incomplete", [{"code": "baseline_gauge_observability_missing"}]
    chaos_overflow_field = False
    for source in sorted(snapshot["rows"]):
        seen_owners: dict[tuple[str, tuple[str, ...]], int] = {}
        seen_logical_owners: dict[
            tuple[str, tuple[str, ...]], tuple[tuple[str, ...], tuple[str, ...]]
        ] = {}
        for row in snapshot["rows"][source]:
            owner = _owner_identity(row)
            if owner is not None:
                owner_kind, identity = owner
                owner_key = (owner_kind, identity)
                seen_owners[owner_key] = seen_owners.get(owner_key, 0) + 1
                if seen_owners[owner_key] > 1:
                    issues.append(
                        {
                            "code": "duplicate_owner",
                            "identity": list(identity),
                            "ownerKind": owner_kind,
                            "row": row["row"],
                        }
                    )
            logical_owner = _logical_owner_identity(row)
            if logical_owner is not None:
                logical_kind, logical_identity = logical_owner
                logical_key = (logical_kind, logical_identity)
                exact_identity = owner[1] if owner is not None else ()
                fence_identity = _logical_owner_fence_identity(row, exact_identity)
                prior_identity = seen_logical_owners.get(logical_key)
                if prior_identity is not None and prior_identity[0] != fence_identity:
                    issues.append(
                        {
                            "code": "stale_successor_owner_conflict",
                            "exactIdentity": list(exact_identity),
                            "logicalIdentity": list(logical_identity),
                            "ownerKind": logical_kind,
                            "priorExactIdentity": list(prior_identity[1]),
                            "row": row["row"],
                        }
                    )
                else:
                    seen_logical_owners[logical_key] = (fence_identity, exact_identity)
            for key, value in row["values"].items():
                selector = _field_selector(row, key)
                number = _numeric(value)
                if source == "chaos" and ("overflow" in key or "truncated" in key):
                    chaos_overflow_field = True
                    if number is None or number != 0:
                        issues.append({"code": "chaos_overflow", "selector": selector, "value": value})
                if (
                    source == "chaos"
                    and row["row"] == "wki_chaos"
                    and key == "queued"
                    and selector not in allowed_nonzero
                    and selector not in maximums
                    and (number is None or number != 0)
                ):
                    issues.append({"code": "chaos_queue_nonzero", "selector": selector, "value": value})
                if source == "chaos" and row["row"] == "wki_chaos" and key == "invalid":
                    if number is None or number != 0:
                        issues.append({"code": "chaos_state_invalid", "value": value})
                if row["row"] in {"wki_block_proxy", "wki_server_binding"} and key == "rdma" and number == 1:
                    for required in ("geometry_valid", "indices_valid"):
                        required_value = row["values"].get(required)
                        if _numeric(required_value or "") != 1:
                            issues.append(
                                {
                                    "code": "rdma_ring_invalid",
                                    "field": required,
                                    "row": row["row"],
                                    "value": required_value,
                                }
                            )
                if selector in maximums:
                    if number is None or number > maximums[selector]:
                        issues.append(
                            {
                                "code": "gauge_above_max",
                                "maximum": maximums[selector],
                                "selector": selector,
                                "value": value,
                            }
                        )
                elif selector in BASELINE_STABLE_GAUGES and selector not in allowed_nonzero:
                    if number != baseline_gauges[selector]:
                        issues.append(
                            {
                                "baseline": baseline_gauges[selector],
                                "code": "gauge_changed_from_baseline",
                                "selector": selector,
                                "value": value,
                            }
                        )
                elif _is_zero_gauge(row["row"], key) and selector not in allowed_nonzero:
                    if number is None or number != 0:
                        issues.append({"code": "gauge_nonzero", "selector": selector, "value": value})
        if source == "peers":
            allowed_disconnected = (
                set(allow["peer_disconnected"])
                | replaced_peer_ids
                | baseline_tombstone_ids
            )
            for row in snapshot["rows"][source]:
                node_id = row["values"].get("node_id")
                connected = row["values"].get("connected")
                if node_id not in allowed_disconnected and connected not in {"1", "CONNECTED", "connected"}:
                    issues.append(
                        {"code": "peer_not_connected", "node": node_id, "value": connected}
                    )
    if not chaos_overflow_field:
        return "incomplete", [{"code": "chaos_overflow_observability_missing"}]
    baseline_pool = _find_packet_pool(baseline)
    current_pool = _find_packet_pool(snapshot)
    if baseline_pool is None or current_pool is None:
        return "incomplete", [{"code": "packet_pool_snapshot_missing"}]
    for key in ("used", "free"):
        before = _numeric(baseline_pool.get(key, ""))
        after = _numeric(current_pool.get(key, ""))
        if before is None or after is None:
            return "incomplete", [{"code": "packet_pool_value_invalid", "field": key}]
        if abs(after - before) > allow["packet_delta"]:
            issues.append(
                {
                    "baseline": before,
                    "code": "packet_pool_delta",
                    "current": after,
                    "field": key,
                    "maximumDelta": allow["packet_delta"],
                }
            )
    return ("failed" if issues else "passed"), issues


def _execute_snapshot(
    event: dict[str, Any],
    operation: dict[str, Any],
    nodes: dict[str, dict[str, Any]],
    output_dir: Path,
    ssh_script: Path,
) -> tuple[dict[str, Any], str, dict[str, Any], bool]:
    capture, observation = _capture_snapshot(
        event_id=event["id"],
        sample=operation["label"],
        operation=operation,
        nodes=nodes,
        output_dir=output_dir,
        ssh_script=ssh_script,
    )
    status = "passed" if capture["complete"] else "incomplete"
    return observation, status, capture, capture["complete"]


def _convergence_sample_interval_ms(operation: dict[str, Any]) -> int:
    if "interval_ms" in operation:
        return operation["interval_ms"]
    return max(1, operation["deadline_ms"] // operation["max_samples"])


def _execute_convergence(
    event: dict[str, Any],
    operation: dict[str, Any],
    nodes: dict[str, dict[str, Any]],
    output_dir: Path,
    ssh_script: Path,
    snapshots: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], str, bool]:
    interval_ms = _convergence_sample_interval_ms(operation)
    baseline = snapshots.get(operation["baseline"])
    if baseline is None or not baseline.get("complete"):
        return {
            "complete": False,
            "issues": [{"code": "baseline_snapshot_missing_or_incomplete"}],
            "sampleIntervalMs": interval_ms,
            "samples": [],
            "stableSamples": 0,
        }, "incomplete", False
    started_ns = time.monotonic_ns()
    deadline_ns = started_ns + operation["deadline_ms"] * 1_000_000
    sample_summaries: list[dict[str, Any]] = []
    previous_projection: list[list[str]] | None = None
    stable_samples = 0
    complete_samples = 0
    last_complete: dict[str, Any] | None = None
    last_status: str | None = None
    last_issues: list[dict[str, Any]] = []
    any_incomplete = False
    for sample_index in range(operation["max_samples"]):
        if sample_index > 0 and interval_ms > 0:
            target_ns = started_ns + sample_index * interval_ms * 1_000_000
            now_ns = time.monotonic_ns()
            wait_ns = min(max(0, target_ns - now_ns), max(0, deadline_ns - now_ns))
            if wait_ns > 0:
                time.sleep(wait_ns / 1_000_000_000)
        if time.monotonic_ns() >= deadline_ns:
            break
        capture, summary = _capture_snapshot(
            event_id=event["id"],
            sample=f"sample-{sample_index + 1:02d}",
            operation=operation,
            nodes=nodes,
            output_dir=output_dir,
            ssh_script=ssh_script,
            deadline_ns=deadline_ns,
        )
        sample_summaries.append(summary)
        if not capture["complete"]:
            any_incomplete = True
            previous_projection = None
            stable_samples = 0
            continue
        complete_samples += 1
        projection = invariant_projection(capture, operation["allow"])
        stable_samples = stable_samples + 1 if projection == previous_projection else 1
        previous_projection = projection
        last_complete = capture
        last_status, last_issues = evaluate_invariants(
            baseline, capture, operation["allow"]
        )
        if last_status == "incomplete":
            any_incomplete = True
        if stable_samples >= 2 and last_status == "passed":
            if any_incomplete:
                return {
                    "complete": False,
                    "issues": [{"code": "convergence_sample_incomplete"}],
                    "sampleIntervalMs": interval_ms,
                    "samples": sample_summaries,
                    "stableSamples": stable_samples,
                }, "incomplete", False
            return {
                "complete": True,
                "issues": [],
                "sampleIntervalMs": interval_ms,
                "samples": sample_summaries,
                "stableSamples": stable_samples,
            }, "passed", True
    if any_incomplete:
        return {
            "complete": False,
            "issues": [{"code": "convergence_sample_incomplete"}, *last_issues],
            "sampleIntervalMs": interval_ms,
            "samples": sample_summaries,
            "stableSamples": stable_samples,
        }, "incomplete", False
    if complete_samples >= 2:
        issues = (
            [{"code": "convergence_not_reached_within_bound"}, *last_issues]
            if last_status == "failed"
            else [{"code": "convergence_not_stable_within_bound"}]
        )
        status = "failed"
    else:
        status = "incomplete"
        issues = [{"code": "insufficient_complete_convergence_snapshots"}]
    return {
        "complete": status != "incomplete",
        "issues": issues,
        "sampleIntervalMs": interval_ms,
        "samples": sample_summaries,
        "stableSamples": stable_samples,
    }, status, status != "incomplete" and not any_incomplete


class TelemetryWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self._streams: dict[str, Any] = {}
        self._record_count = 0

    def append(self, lane: str, record: dict[str, Any]) -> None:
        if lane not in self._streams:
            path = self.output_dir / "telemetry" / f"{lane}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            self._streams[lane] = path.open("xb")
        encoded = canonical_json_bytes(record)
        if len(encoded) > MAX_TELEMETRY_LINE_BYTES:
            encoded = canonical_json_bytes(_compact_telemetry_record(record, encoded))
        if len(encoded) > MAX_TELEMETRY_LINE_BYTES:
            raise ChaosError("compacted telemetry record exceeds the WOSDBG line bound")
        self._streams[lane].write(encoded)
        self._streams[lane].flush()
        self._record_count += 1

    def lanes(self) -> list[str]:
        return sorted(self._streams)

    def record_count(self) -> int:
        return self._record_count

    def close(self) -> None:
        for stream in self._streams.values():
            stream.close()
        self._streams.clear()


def _telemetry_result_summary(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {"type": type(result).__name__}
    scalars: dict[str, Any] = {}
    collections: dict[str, dict[str, Any]] = {}
    for key in sorted(result):
        value = result[key]
        if value is None or isinstance(value, (bool, int, float)):
            scalars[key] = value
        elif isinstance(value, str):
            encoded = value.encode("utf-8")
            if len(encoded) <= MAX_TEXT_BYTES:
                scalars[key] = value
            else:
                collections[key] = {
                    "bytes": len(encoded),
                    "sha256": "sha256:" + hashlib.sha256(encoded).hexdigest(),
                    "type": "string",
                }
        elif isinstance(value, (dict, list)):
            collections[key] = {"items": len(value), "type": type(value).__name__}
        else:
            collections[key] = {"type": type(value).__name__}
    return {"collections": collections, "scalars": scalars}


def _compact_telemetry_record(
    record: dict[str, Any], original_bytes: bytes
) -> dict[str, Any]:
    compact = copy.deepcopy(record)
    payload = record.get("payload")
    retained: dict[str, Any] = {}
    if isinstance(payload, dict):
        for key in (
            "evidenceComplete",
            "failureSignature",
            "fault",
            "operation",
            "outcome",
            "state",
            "status",
        ):
            if key in payload:
                retained[key] = copy.deepcopy(payload[key])
        if "result" in payload:
            retained["resultSummary"] = _telemetry_result_summary(payload["result"])
    compact["payload"] = {
        **retained,
        "telemetryCompaction": {
            "fullEvidencePath": "replay.json",
            "originalBytes": len(original_bytes),
            "originalSha256": "sha256:" + hashlib.sha256(original_bytes).hexdigest(),
        },
    }
    return compact


def telemetry_record(
    *,
    run_id: str,
    scenario: str,
    kind: str,
    payload: dict[str, Any],
    event: dict[str, Any] | None = None,
    node: dict[str, Any] | None = None,
    timestamp_ns: int | None = None,
) -> dict[str, Any]:
    identity: dict[str, Any] = {"node_id": node["id"] if node is not None else "host"}
    if node is not None and "boot_id" in node:
        identity["boot_id"] = node["boot_id"]
    correlation: dict[str, str] = {"run_id": run_id, "scenario": scenario}
    if event is not None:
        correlation.update(
            {
                "event_id": event["id"],
                "event_group": event["group"],
                "phase": event["phase"],
            }
        )
        for key, value in event.get("fault", {}).items():
            if key in {"channel", "neighbor", "occurrence", "rule_id", "sequence"}:
                correlation[key] = value
    return {
        "clock": {
            "domain": "host_monotonic",
            "quality": "local_observer",
            "unit": "ns",
            "value": str(time.monotonic_ns() if timestamp_ns is None else timestamp_ns),
        },
        "correlation": correlation,
        "format": "wos.telemetry",
        "identity": identity,
        "kind": kind,
        "payload": payload,
        "source": "wki-chaos",
        "source_version": 1,
        "version": 1,
    }


def _parse_guest_telemetry(stdout: str, stdout_truncated: bool) -> tuple[list[dict[str, Any]], str | None]:
    if stdout_truncated:
        return [], "guest telemetry output was truncated"
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(stdout.splitlines(), 1):
        if not line.strip():
            continue
        if len(line.encode("utf-8")) > MAX_INPUT_BYTES:
            return records, f"guest telemetry line {line_number} exceeds the input bound"
        if len(records) >= MAX_GUEST_TELEMETRY_RECORDS:
            return records, "guest telemetry record count exceeds the configured bound"
        try:
            record = json.loads(line, object_pairs_hook=_object_without_duplicates)
        except (json.JSONDecodeError, ChaosInputError) as exc:
            return records, f"invalid guest telemetry line {line_number}: {exc}"
        if not isinstance(record, dict) or record.get("format") != "wos.telemetry" or record.get("version") != 1:
            return records, f"guest telemetry line {line_number} is not wos.telemetry version 1"
        records.append(record)
    if not records:
        return [], "guest telemetry capture produced no records"
    return records, None


def _failure_signature(scenario: str, event: dict[str, Any], operation: dict[str, Any], code: str) -> dict[str, str]:
    return {
        "code": operation.get("failure_code", code),
        "node": operation.get("node", "host"),
        "object": operation.get("failure_object", event["id"]),
        "scenario": scenario,
    }


def _match_stdout_row(stdout: str, contract: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        return False, {
            "error": "stdout row contract requires exactly one non-empty line",
            "lineCount": len(lines),
            "matched": False,
        }
    tokens = lines[0].split()
    if not tokens or tokens[0] != contract["prefix"]:
        return False, {
            "error": "stdout row prefix mismatch",
            "matched": False,
            "observedPrefix": tokens[0] if tokens else "",
        }
    fields: dict[str, str] = {}
    for token in tokens[1:]:
        if "=" not in token:
            return False, {
                "error": f"stdout row has malformed token {token!r}",
                "matched": False,
            }
        key, value = token.split("=", 1)
        try:
            field = _safe_id(key, "stdout row field")
        except ChaosInputError as exc:
            return False, {"error": str(exc), "matched": False}
        if field in fields:
            return False, {
                "error": f"stdout row repeats field {field!r}",
                "matched": False,
            }
        fields[field] = value
        if len(fields) > 128:
            return False, {
                "error": "stdout row exceeds 128 fields",
                "matched": False,
            }

    asserted: dict[str, str] = {}
    errors: list[str] = []
    for field, expected in contract["equals"].items():
        observed = fields.get(field)
        asserted[field] = "" if observed is None else observed
        if observed != expected:
            errors.append(f"{field} expected {expected!r}, observed {observed!r}")
    for field, choices in contract["one_of"].items():
        observed = fields.get(field)
        asserted[field] = "" if observed is None else observed
        if observed not in choices:
            errors.append(f"{field} expected one of {choices!r}, observed {observed!r}")
    for field in contract["negative"]:
        observed = fields.get(field)
        asserted[field] = "" if observed is None else observed
        numeric = _numeric(observed) if observed is not None else None
        if numeric is None or numeric >= 0:
            errors.append(f"{field} expected a negative integer, observed {observed!r}")
    for field in contract["nonzero"]:
        observed = fields.get(field)
        asserted[field] = "" if observed is None else observed
        numeric = _numeric(observed) if observed is not None else None
        if numeric is None or numeric == 0:
            errors.append(f"{field} expected a nonzero integer, observed {observed!r}")
    return not errors, {
        "assertedFields": asserted,
        "errors": errors,
        "matched": not errors,
        "prefix": tokens[0],
    }


def _execute_guest(
    operation: dict[str, Any],
    nodes: dict[str, dict[str, Any]],
    ssh_script: Path,
) -> tuple[dict[str, Any], bool, list[dict[str, Any]], bool]:
    node = nodes[operation["node"]]
    result = _run_guest_argv(node, operation["argv"], operation["timeout_ms"], ssh_script)
    success = (
        not result["timedOut"]
        and "error" not in result
        and result["exitCode"] in operation["expect_exit"]
    )
    telemetry_records: list[dict[str, Any]] = []
    evidence_complete = not result["stdoutTruncated"] and not result["stderrTruncated"]
    if operation["capture_telemetry"]:
        telemetry_records, telemetry_error = _parse_guest_telemetry(
            result["stdout"], result["stdoutTruncated"]
        )
        result["guestTelemetry"] = {
            "complete": telemetry_error is None,
            "error": telemetry_error,
            "records": len(telemetry_records),
        }
        if telemetry_error is not None:
            success = False
            evidence_complete = False
    if "stdout_row" in operation:
        row_matched, row_observation = _match_stdout_row(
            result["stdout"], operation["stdout_row"]
        )
        result["stdoutRow"] = row_observation
        success = success and row_matched
    return result, success, telemetry_records, evidence_complete


def _poll_guest_argv(
    *,
    argv: list[str],
    attempt_timeout_ms: int,
    deadline_ms: int,
    expect_exit: list[int],
    interval_ms: int,
    max_attempts: int,
    node: dict[str, Any],
    ssh_script: Path,
    result_probe: Callable[[dict[str, Any]], tuple[bool, dict[str, Any]]] | None = None,
    stdout_limit_bytes: int = MAX_COMMAND_OUTPUT_BYTES,
) -> tuple[dict[str, Any], bool, bool]:
    deadline_ns = time.monotonic_ns() + deadline_ms * 1_000_000
    attempts: list[dict[str, Any]] = []
    evidence_complete = True
    success = False
    for attempt_index in range(max_attempts):
        remaining_ns = deadline_ns - time.monotonic_ns()
        if remaining_ns <= 0:
            break
        timeout_ms = max(1, min(attempt_timeout_ms, remaining_ns // 1_000_000))
        started_ns = time.monotonic_ns()
        command_result = _run_guest_argv(
            node,
            argv,
            timeout_ms,
            ssh_script,
            stdout_limit_bytes=stdout_limit_bytes,
        )
        finished_ns = time.monotonic_ns()
        complete = (
            not command_result["stdoutTruncated"]
            and not command_result["stderrTruncated"]
        )
        evidence_complete = evidence_complete and complete
        attempt_success = (
            not command_result["timedOut"]
            and "error" not in command_result
            and command_result["exitCode"] in expect_exit
        )
        probe_observation: dict[str, Any] | None = None
        if attempt_success and result_probe is not None:
            attempt_success, probe_observation = result_probe(command_result)
        attempt = {
            "attempt": attempt_index + 1,
            "finishedHostMonotonicNs": str(finished_ns),
            "result": command_result,
            "startedHostMonotonicNs": str(started_ns),
            "status": "passed" if attempt_success else "failed",
        }
        if probe_observation is not None:
            attempt["probe"] = probe_observation
        attempts.append(attempt)
        if attempt_success:
            success = True
            break
        remaining_ns = deadline_ns - time.monotonic_ns()
        if remaining_ns <= 0 or attempt_index + 1 >= max_attempts:
            break
        time.sleep(min(interval_ms / 1000.0, remaining_ns / 1_000_000_000.0))
    return {
        "attempts": attempts,
        "attemptsUsed": len(attempts),
        "deadlineExpired": not success and time.monotonic_ns() >= deadline_ns,
        "maxAttempts": max_attempts,
    }, success, evidence_complete


def _execute_guest_poll(
    operation: dict[str, Any],
    nodes: dict[str, dict[str, Any]],
    ssh_script: Path,
    snapshots: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any], bool, bool]:
    result_probe = None
    if "peer_epoch_probe" in operation:
        probe = operation["peer_epoch_probe"]
        baseline = snapshots.get(probe["baseline"])
        if baseline is None or not baseline.get("complete"):
            return {
                "attempts": [],
                "attemptsUsed": 0,
                "error": "peer epoch baseline snapshot is missing or incomplete",
            }, False, False
        peer_rows = [
            row
            for row in baseline.get("rows", {}).get("peers", [])
            if row["values"].get("hostname") == probe["peer_host"]
            and row["values"].get("local") == "0"
        ]
        if len(peer_rows) != 1:
            return {
                "attempts": [],
                "attemptsUsed": 0,
                "error": "peer epoch baseline does not contain one exact remote hostname row",
            }, False, False
        peer_id = _numeric(peer_rows[0]["values"].get("node_id", ""))
        baseline_lifecycle = [
            row
            for row in baseline.get("rows", {}).get("netdiag", [])
            if row["row"] == "wki_peer_lifecycle"
            and _numeric(row["values"].get("peer", "")) == peer_id
        ]
        if peer_id is None or len(baseline_lifecycle) != 1:
            return {
                "attempts": [],
                "attemptsUsed": 0,
                "error": "peer epoch baseline lacks one exact lifecycle row",
            }, False, False
        baseline_epoch = _numeric(
            baseline_lifecycle[0]["values"].get("remote_boot_epoch", "")
        )
        if baseline_epoch is None or baseline_epoch == 0:
            return {
                "attempts": [],
                "attemptsUsed": 0,
                "error": "peer epoch baseline is zero or malformed",
            }, False, False

        def peer_epoch_changed(result: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
            return _match_peer_epoch_probe(
                result["stdout"],
                peer_host=probe["peer_host"],
                baseline_peer_id=peer_id,
                baseline_epoch=baseline_epoch,
            )

        result_probe = peer_epoch_changed
    if "stdout_row" in operation:
        prior_probe = result_probe

        def stdout_row_probe(result: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
            row_matched, row_observation = _match_stdout_row(
                result["stdout"], operation["stdout_row"]
            )
            if prior_probe is None:
                return row_matched, {"stdoutRow": row_observation}
            prior_matched, prior_observation = prior_probe(result)
            return row_matched and prior_matched, {
                "peerEpoch": prior_observation,
                "stdoutRow": row_observation,
            }

        result_probe = stdout_row_probe
    return _poll_guest_argv(
        argv=operation["argv"],
        attempt_timeout_ms=operation["attempt_timeout_ms"],
        deadline_ms=operation["deadline_ms"],
        expect_exit=operation["expect_exit"],
        interval_ms=operation["interval_ms"],
        max_attempts=operation["max_attempts"],
        node=nodes[operation["node"]],
        result_probe=result_probe,
        ssh_script=ssh_script,
        stdout_limit_bytes=(
            MAX_PEER_EPOCH_OUTPUT_BYTES
            if "peer_epoch_probe" in operation
            else MAX_COMMAND_OUTPUT_BYTES
        ),
    )


def _execute_guest_start(
    operation: dict[str, Any],
    nodes: dict[str, dict[str, Any]],
    ssh_script: Path,
) -> tuple[dict[str, Any] | None, dict[str, Any], str, bool]:
    node = nodes[operation["node"]]
    try:
        process = AsyncGuestProcess(
            _guest_transport_argv(node, operation["argv"], ssh_script),
            operation["lifetime_ms"],
        )
    except OSError as exc:
        return None, {
            "error": str(exc)[:MAX_TEXT_BYTES],
            "errorType": type(exc).__name__,
            "processId": operation["process_id"],
        }, "failed", True
    handle = {"node": node, "operation": operation, "process": process}
    return handle, {
        "lifetimeMs": operation["lifetime_ms"],
        "localPid": str(process.process.pid),
        "processId": operation["process_id"],
    }, "passed", True


def _execute_guest_wait(
    operation: dict[str, Any],
    handle: dict[str, Any] | None,
    ssh_script: Path,
) -> tuple[dict[str, Any], str, bool]:
    if handle is None:
        return {
            "error": "async process is not active",
            "processId": operation["process_id"],
        }, "failed", True
    process_result = handle["process"].collect(operation["timeout_ms"])
    cleanup_result, cleanup_success, cleanup_complete = _run_async_cleanup(handle, ssh_script)
    result = {
        "cleanup": cleanup_result,
        "process": process_result,
        "processId": operation["process_id"],
    }
    complete = (
        not process_result["stdoutTruncated"]
        and not process_result["stderrTruncated"]
        and cleanup_complete
    )
    success = (
        not process_result["timedOut"]
        and process_result["exitCode"] in operation["expect_exit"]
        and cleanup_success
    )
    if not success:
        return result, "failed", complete
    return result, "passed" if complete else "incomplete", complete


def _force_cleanup_async(
    handle: dict[str, Any], ssh_script: Path
) -> tuple[dict[str, Any], bool]:
    process_result = handle["process"].collect(
        handle["operation"]["cleanup_timeout_ms"], forced=True
    )
    cleanup_result, cleanup_success, cleanup_complete = _run_async_cleanup(handle, ssh_script)
    return {
        "cleanup": cleanup_result,
        "cleanupSucceeded": cleanup_success,
        "process": process_result,
        "processId": handle["operation"]["process_id"],
    }, (
        cleanup_success
        and cleanup_complete
        and not process_result["stdoutTruncated"]
        and not process_result["stderrTruncated"]
    )


def _run_final_cleanup(
    *,
    dry_run: bool,
    nodes: dict[str, dict[str, Any]],
    run_id: str,
    scenario_name: str,
    ssh_script: Path,
    writer: TelemetryWriter,
) -> tuple[list[dict[str, Any]], str | None, bool]:
    observations: list[dict[str, Any]] = []
    problem_status: str | None = None
    evidence_complete = True
    for node_id in sorted(nodes):
        node = nodes[node_id]
        node_steps: list[dict[str, Any]] = []
        node_status = "passed"
        for command in FINAL_CLEANUP_COMMANDS:
            argv = list(command)
            if dry_run:
                result: dict[str, Any] = {"dryRun": True}
                success = True
                complete = True
            else:
                result_probe = None
                if argv == FINAL_CLEANUP_COMMANDS[0]:

                    def workload_clear_probe(
                        command_result: dict[str, Any],
                    ) -> tuple[bool, dict[str, Any]]:
                        matched, observation = _match_stdout_row(
                            command_result["stdout"], WORKLOAD_CLEAR_ROW
                        )
                        return matched, {"stdoutRow": observation}

                    result_probe = workload_clear_probe
                result, success, complete = _poll_guest_argv(
                    argv=argv,
                    attempt_timeout_ms=2_000,
                    deadline_ms=10_000,
                    expect_exit=[0],
                    interval_ms=250,
                    max_attempts=32,
                    node=node,
                    result_probe=result_probe,
                    ssh_script=ssh_script,
                )
            step_status = "passed" if success and complete else "incomplete" if success else "failed"
            if step_status == "failed":
                node_status = "failed"
                if problem_status is None:
                    problem_status = "failed"
            elif step_status == "incomplete" and node_status == "passed":
                node_status = "incomplete"
                if problem_status is None:
                    problem_status = "incomplete"
            evidence_complete = evidence_complete and complete
            node_steps.append({"argv": argv, "result": result, "status": step_status})
            writer.append(
                f"node-{node_id}",
                telemetry_record(
                    run_id=run_id,
                    scenario=scenario_name,
                    kind="wki.chaos.final_cleanup",
                    payload={
                        "argv": argv,
                        "evidenceComplete": complete,
                        "result": result,
                        "status": step_status,
                    },
                    node=node,
                ),
            )
        observations.append({"node": node_id, "status": node_status, "steps": node_steps})
    return observations, problem_status, evidence_complete


def _execute_qmp(
    operation: dict[str, Any], nodes: dict[str, dict[str, Any]]
) -> tuple[dict[str, Any], bool, bool]:
    node = nodes[operation["node"]]
    deadline = time.monotonic() + operation["timeout_ms"] / 1000.0
    try:
        with QmpClient(Path(node["qmp_socket"]), operation["timeout_ms"] / 1000.0) as client:
            response = client.execute(
                operation["command"], operation["arguments"], deadline=deadline
            )
            events, discarded = client.drain_events()
        if len(events) > MAX_QMP_EVENTS_IN_REPLAY:
            discarded += len(events) - MAX_QMP_EVENTS_IN_REPLAY
            events = events[-MAX_QMP_EVENTS_IN_REPLAY:]
        return {
            "discardedEvents": discarded,
            "events": events,
            "response": response,
        }, True, discarded == 0
    except Exception as exc:  # QMP/socket failures are evidence, not runner crashes.
        return {
            "discardedEvents": 0,
            "error": str(exc)[:MAX_TEXT_BYTES],
            "errorType": type(exc).__name__,
            "events": [],
        }, False, True


def _event_lane(operation: dict[str, Any]) -> str:
    if "node" in operation:
        return "node-" + re.sub(r"[^A-Za-z0-9_.-]", "_", operation["node"])
    return "host"


def _event_kind(operation: dict[str, Any]) -> str:
    if operation["kind"] in {"guest", "guest_poll"}:
        return f"wki.chaos.{operation['role']}"
    return f"wki.chaos.{operation['kind']}"


def _make_batch(lanes: list[str], max_events: int) -> dict[str, Any]:
    calls: list[dict[str, Any]] = []
    log_references: list[str] = []
    for index, lane in enumerate(sorted(lanes)):
        call_id = f"lane{index}"
        calls.append(
            {
                "arguments": {"path": f"telemetry/{lane}.jsonl"},
                "id": call_id,
                "tool": "load_log",
            }
        )
        log_references.append(f"${call_id}.logId")
    calls.append(
        {
            "arguments": {
                "context": 0,
                "correlationKeys": [
                    "run_id",
                    "event_id",
                    "event_group",
                    "phase",
                    "rule_id",
                    "channel",
                    "sequence",
                ],
                "expandCorrelations": True,
                "logIds": log_references,
                "maxEvents": max(1, min(max_events, 4096)),
            },
            "id": "timeline",
            "tool": "build_distributed_timeline",
        }
    )
    return {"calls": calls}


def _artifact_inventory(output_dir: Path) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for path in sorted(output_dir.rglob("*")):
        if not path.is_file() or path == output_dir / "manifest.json":
            continue
        if len(artifacts) >= MAX_ARTIFACTS:
            raise ChaosError(f"run produced more than {MAX_ARTIFACTS} artifacts")
        hasher = hashlib.sha256()
        size = 0
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(1024 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)
                size += len(chunk)
        artifacts.append(
            {
                "path": path.relative_to(output_dir).as_posix(),
                "sha256": "sha256:" + hasher.hexdigest(),
                "size": size,
            }
        )
    return artifacts


def _manifest_for_replay(replay: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    return {
        "artifacts": _artifact_inventory(output_dir),
        "completion": {
            "evidenceComplete": replay["evidenceComplete"],
            "executionComplete": replay["executionComplete"],
            "outcome": replay["outcome"],
        },
        "failureSignature": replay["failureSignature"],
        "format": RUN_FORMAT,
        "generator": copy.deepcopy(replay["generator"]),
        "limits": {
            "artifacts": MAX_ARTIFACTS,
            "commandOutputBytes": MAX_COMMAND_OUTPUT_BYTES,
            "events": MAX_EVENTS,
            "snapshotOutputBytes": MAX_SNAPSHOT_OUTPUT_BYTES,
            "telemetryLineBytes": MAX_TELEMETRY_LINE_BYTES,
        },
        "planDigest": replay["planDigest"],
        "runId": replay["runId"],
        "scenario": copy.deepcopy(replay["scenario"]),
        "version": FORMAT_VERSION,
    }


def _execute_plan_once(
    raw_plan: dict[str, Any],
    output_dir: Path,
    *,
    ssh_script: Path = ROOT / "scripts" / "remote" / "wos_ssh.sh",
    dry_run: bool = False,
    provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = validate_plan(raw_plan)
    create_output_dir(output_dir)
    nodes = {node["id"]: node for node in plan["nodes"]}
    run_id = plan["planDigest"]
    writer = TelemetryWriter(output_dir)
    scenario_name = plan["scenario"]["name"]
    writer.append(
        "host",
        telemetry_record(
            run_id=run_id,
            scenario=scenario_name,
            kind="wki.chaos.run",
            payload={"dryRun": dry_run, "planDigest": plan["planDigest"], "state": "begin"},
        ),
    )
    replay_events: list[dict[str, Any]] = []
    first_failure: dict[str, str] | None = None
    first_problem_status: str | None = None
    evidence_complete = True
    stopped = False
    snapshots: dict[str, dict[str, Any]] = {}
    async_handles: dict[str, dict[str, Any]] = {}
    async_start_replays: dict[str, dict[str, Any]] = {}
    cleanup_observations: list[dict[str, Any]] = []
    try:
        for event in plan["events"]:
            replay_event = copy.deepcopy(event)
            operation = event["operation"]
            if stopped:
                replay_event["observation"] = {"status": "not-run"}
                replay_events.append(replay_event)
                continue
            started_ns = time.monotonic_ns()
            guest_telemetry: list[dict[str, Any]] = []
            if dry_run:
                result: dict[str, Any] = {"dryRun": True}
                event_status = "passed"
                event_evidence_complete = True
            elif operation["kind"] == "guest":
                result, success, guest_telemetry, event_evidence_complete = _execute_guest(
                    operation, nodes, ssh_script
                )
                event_status = (
                    "failed"
                    if not success
                    else "passed"
                    if event_evidence_complete
                    else "incomplete"
                )
            elif operation["kind"] == "guest_poll":
                result, success, event_evidence_complete = _execute_guest_poll(
                    operation, nodes, ssh_script, snapshots
                )
                event_status = (
                    "failed"
                    if not success
                    else "passed"
                    if event_evidence_complete
                    else "incomplete"
                )
            elif operation["kind"] == "qmp":
                result, success, event_evidence_complete = _execute_qmp(operation, nodes)
                event_status = (
                    "failed"
                    if not success
                    else "passed"
                    if event_evidence_complete
                    else "incomplete"
                )
            elif operation["kind"] == "snapshot":
                result, event_status, capture, event_evidence_complete = _execute_snapshot(
                    event, operation, nodes, output_dir, ssh_script
                )
                snapshots[event["id"]] = capture
            elif operation["kind"] == "convergence":
                result, event_status, event_evidence_complete = _execute_convergence(
                    event, operation, nodes, output_dir, ssh_script, snapshots
                )
            elif operation["kind"] == "guest_start":
                handle, result, event_status, event_evidence_complete = _execute_guest_start(
                    operation, nodes, ssh_script
                )
                if handle is not None:
                    async_handles[operation["process_id"]] = handle
            elif operation["kind"] == "guest_wait":
                handle = async_handles.pop(operation["process_id"], None)
                result, event_status, event_evidence_complete = _execute_guest_wait(
                    operation, handle, ssh_script
                )
            else:
                result = {"label": operation["label"]}
                event_status = "passed"
                event_evidence_complete = True
            finished_ns = time.monotonic_ns()
            evidence_complete = evidence_complete and event_evidence_complete
            observation = {
                "finishedHostMonotonicNs": str(finished_ns),
                "result": result,
                "startedHostMonotonicNs": str(started_ns),
                "status": event_status,
            }
            replay_event["observation"] = observation
            replay_events.append(replay_event)
            if operation["kind"] == "guest_start" and event_status == "passed":
                async_start_replays[operation["process_id"]] = replay_event
            node = nodes.get(operation.get("node"))
            writer.append(
                _event_lane(operation),
                telemetry_record(
                    run_id=run_id,
                    scenario=scenario_name,
                    kind=_event_kind(operation),
                    payload={
                        "evidenceComplete": event_evidence_complete,
                        "fault": event.get("fault"),
                        "operation": operation,
                        "result": result,
                        "status": observation["status"],
                    },
                    event=event,
                    node=node,
                    timestamp_ns=finished_ns,
                ),
            )
            for record in guest_telemetry:
                writer.append(_event_lane(operation), record)
            if event_status != "passed" and (
                first_failure is None
                or (first_problem_status == "incomplete" and event_status == "failed")
            ):
                if operation["kind"] == "guest" and result.get("guestTelemetry", {}).get("complete") is False:
                    failure_code = "guest_telemetry_invalid"
                elif operation["kind"] == "guest" and result.get("timedOut"):
                    failure_code = "guest_timeout"
                else:
                    failure_code = f"{operation['kind']}_event_{event_status}"
                first_failure = _failure_signature(
                    scenario_name, event, operation, failure_code
                )
                first_problem_status = event_status
            if event_status != "passed" and operation.get("on_failure", "stop") == "stop":
                stopped = True
    finally:
        for process_id in sorted(async_handles):
            cleanup_result, cleanup_complete = _force_cleanup_async(
                async_handles[process_id], ssh_script
            )
            evidence_complete = evidence_complete and cleanup_complete
            start_replay = async_start_replays.get(process_id)
            if start_replay is not None:
                start_replay["observation"]["result"]["forcedCleanup"] = cleanup_result
            writer.append(
                _event_lane(async_handles[process_id]["operation"]),
                telemetry_record(
                    run_id=run_id,
                    scenario=scenario_name,
                    kind="wki.chaos.guest_cleanup",
                    payload={
                        "evidenceComplete": cleanup_complete,
                        "result": cleanup_result,
                    },
                    node=async_handles[process_id]["node"],
                ),
            )
        cleanup_observations, cleanup_problem_status, cleanup_complete = _run_final_cleanup(
            dry_run=dry_run,
            nodes=nodes,
            run_id=run_id,
            scenario_name=scenario_name,
            ssh_script=ssh_script,
            writer=writer,
        )
        evidence_complete = evidence_complete and cleanup_complete
        if not dry_run and first_problem_status is None and cleanup_problem_status is not None:
            first_problem_status = cleanup_problem_status
            first_failure = {
                "code": "final_cleanup_" + cleanup_problem_status,
                "node": "host",
                "object": scenario_name,
                "scenario": scenario_name,
            }
        execution_complete = not stopped or all(
            event.get("observation", {}).get("status") != "not-run" for event in replay_events
        )
        if not dry_run and first_problem_status is None and not evidence_complete:
            first_problem_status = "incomplete"
            first_failure = {
                "code": "run_evidence_incomplete",
                "node": "host",
                "object": scenario_name,
                "scenario": scenario_name,
            }
        outcome = "dry-run" if dry_run else (first_problem_status or "passed")
        writer.append(
            "host",
            telemetry_record(
                run_id=run_id,
                scenario=scenario_name,
                kind="wki.chaos.run",
                payload={
                    "evidenceComplete": evidence_complete,
                    "executionComplete": execution_complete,
                    "failureSignature": first_failure,
                    "state": "end",
                    "outcome": outcome,
                },
            ),
        )
        lanes = writer.lanes()
        telemetry_record_count = writer.record_count()
        writer.close()

    replay: dict[str, Any] = {
        "cleanup": cleanup_observations,
        "evidenceComplete": evidence_complete,
        "events": replay_events,
        "executionComplete": execution_complete,
        "failureSignature": first_failure,
        "format": REPLAY_FORMAT,
        "generator": copy.deepcopy(plan["generator"]),
        "nodes": copy.deepcopy(plan["nodes"]),
        "outcome": outcome,
        "planDigest": plan["planDigest"],
        "runId": run_id,
        "scenario": copy.deepcopy(plan["scenario"]),
        "version": FORMAT_VERSION,
    }
    if provenance is not None:
        replay["provenance"] = copy.deepcopy(provenance)
    write_new_json(output_dir / "replay.json", replay)
    write_new_json(output_dir / "wosdbg-batch.json", _make_batch(lanes, telemetry_record_count))
    write_new_json(output_dir / "manifest.json", _manifest_for_replay(replay, output_dir))
    return replay


def execute_plan(
    raw_plan: dict[str, Any],
    output_dir: Path,
    *,
    ssh_script: Path = ROOT / "scripts" / "remote" / "wos_ssh.sh",
    dry_run: bool = False,
    provenance: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if dry_run:
        return _execute_plan_once(
            raw_plan,
            output_dir,
            ssh_script=ssh_script,
            dry_run=True,
            provenance=provenance,
        )
    with _SshControlScope(ssh_script):
        return _execute_plan_once(
            raw_plan,
            output_dir,
            ssh_script=ssh_script,
            provenance=provenance,
        )


def validate_failure_signature(value: Any, where: str = "failureSignature") -> dict[str, str]:
    signature = _require_object(value, where)
    _require_keys(signature, {"code", "node", "object", "scenario"}, {"code", "node", "object", "scenario"}, where)
    return {
        "code": _safe_id(signature["code"], f"{where}.code"),
        "node": _safe_id(signature["node"], f"{where}.node"),
        "object": _bounded_string(signature["object"], f"{where}.object", 256),
        "scenario": _safe_id(signature["scenario"], f"{where}.scenario"),
    }


def validate_replay(raw: dict[str, Any]) -> dict[str, Any]:
    allowed = {
        "cleanup",
        "evidenceComplete",
        "events",
        "executionComplete",
        "failureSignature",
        "format",
        "generator",
        "nodes",
        "outcome",
        "planDigest",
        "provenance",
        "runId",
        "scenario",
        "version",
    }
    required = allowed - {"cleanup", "provenance"}
    _require_keys(raw, allowed, required, "replay")
    if raw["format"] != REPLAY_FORMAT or raw["version"] != FORMAT_VERSION:
        raise ChaosInputError(f"replay must use {REPLAY_FORMAT!r} version {FORMAT_VERSION}")
    if raw["outcome"] not in {"dry-run", "failed", "incomplete", "passed"}:
        raise ChaosInputError("replay.outcome is invalid")
    if raw["runId"] != raw["planDigest"]:
        raise ChaosInputError("replay.runId must equal planDigest")
    for key in ("evidenceComplete", "executionComplete"):
        if not isinstance(raw[key], bool):
            raise ChaosInputError(f"replay.{key} must be boolean")
    if raw["failureSignature"] is not None:
        validate_failure_signature(raw["failureSignature"], "replay.failureSignature")
    if raw["outcome"] in {"dry-run", "passed"} and raw["failureSignature"] is not None:
        raise ChaosInputError("successful/dry-run replay cannot carry a failureSignature")
    if raw["outcome"] in {"failed", "incomplete"} and raw["failureSignature"] is None:
        raise ChaosInputError("failed/incomplete replay requires a failureSignature")
    if raw["outcome"] == "passed" and (
        not raw["evidenceComplete"] or not raw["executionComplete"]
    ):
        raise ChaosInputError("passed replay requires complete execution and evidence")
    plan = replay_to_plan_unchecked(raw)
    validated_plan = validate_plan(plan)
    normalized = copy.deepcopy(raw)
    cleanup = raw.get("cleanup", [])
    if not isinstance(cleanup, list) or len(cleanup) > MAX_NODES:
        raise ChaosInputError("replay.cleanup must be a bounded array")
    normalized["cleanup"] = copy.deepcopy(cleanup)
    normalized["events"] = []
    for index, (raw_event, plan_event) in enumerate(zip(raw["events"], validated_plan["events"], strict=True)):
        observation = _require_object(raw_event.get("observation"), f"replay.events[{index}].observation")
        status = observation.get("status")
        if status not in {"failed", "incomplete", "not-run", "passed"}:
            raise ChaosInputError(f"replay.events[{index}].observation.status is invalid")
        event = copy.deepcopy(plan_event)
        event["observation"] = copy.deepcopy(observation)
        normalized["events"].append(event)
    return normalized


def replay_to_plan_unchecked(replay: dict[str, Any]) -> dict[str, Any]:
    events = []
    for raw_event in replay.get("events", []):
        event = copy.deepcopy(raw_event)
        event.pop("observation", None)
        events.append(event)
    return {
        "events": events,
        "format": PLAN_FORMAT,
        "generator": copy.deepcopy(replay["generator"]),
        "limits": {
            "commandOutputBytes": MAX_COMMAND_OUTPUT_BYTES,
            "eventTimeoutMs": MAX_EVENT_TIMEOUT_MS,
            "events": MAX_EVENTS,
            "guestTelemetryRecordsPerEvent": MAX_GUEST_TELEMETRY_RECORDS,
        },
        "nodes": copy.deepcopy(replay["nodes"]),
        "planDigest": replay["planDigest"],
        "scenario": copy.deepcopy(replay["scenario"]),
        "version": FORMAT_VERSION,
    }


def replay_to_plan(replay: dict[str, Any]) -> dict[str, Any]:
    normalized = validate_replay(replay)
    return validate_plan(replay_to_plan_unchecked(normalized))


def _reindex_plan_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for ordinal, event in enumerate(events):
        item = copy.deepcopy(event)
        item["ordinal"] = ordinal
        result.append(item)
    return result


def grouped_ddmin(
    events: list[dict[str, Any]],
    predicate: Callable[[list[dict[str, Any]], list[str]], bool],
    max_attempts: int,
) -> tuple[list[dict[str, Any]], int]:
    """Delete reducible groups while preserving order and indivisible pairs."""
    if max_attempts <= 0:
        raise ValueError("max_attempts must be positive")
    irreducible_groups = {event["group"] for event in events if not event["reducible"]}
    groups: list[str] = []
    for event in events:
        group = event["group"]
        if group not in irreducible_groups and group not in groups:
            groups.append(group)
    retained = list(groups)
    attempts = 0
    granularity = 2
    while retained and attempts < max_attempts:
        granularity = min(granularity, len(retained))
        chunk_size = (len(retained) + granularity - 1) // granularity
        reduced = False
        for start in range(0, len(retained), chunk_size):
            if attempts >= max_attempts:
                break
            removed = retained[start : start + chunk_size]
            removed_set = set(removed)
            candidate_groups = [group for group in retained if group not in removed_set]
            keep = irreducible_groups | set(candidate_groups)
            candidate = _reindex_plan_events(
                [event for event in events if event["group"] in keep]
            )
            attempts += 1
            if predicate(candidate, removed):
                retained = candidate_groups
                granularity = max(2, granularity - 1)
                reduced = True
                break
        if reduced:
            continue
        if granularity >= len(retained):
            break
        granularity = min(len(retained), granularity * 2)
    keep = irreducible_groups | set(retained)
    return _reindex_plan_events([event for event in events if event["group"] in keep]), attempts


def minimize_replay(
    raw_replay: dict[str, Any],
    output_dir: Path,
    *,
    ssh_script: Path,
    max_attempts: int,
) -> dict[str, Any]:
    replay = validate_replay(raw_replay)
    if replay["failureSignature"] is None or replay["outcome"] != "failed":
        raise ChaosInputError("minimize requires a failed replay with a failureSignature")
    if not replay["evidenceComplete"]:
        raise ChaosInputError("minimize refuses an evidence-incomplete replay")
    target_signature = validate_failure_signature(replay["failureSignature"])
    base_plan = replay_to_plan(replay)
    create_output_dir(output_dir)
    transcript_path = output_dir / "minimize.jsonl"
    transcript = transcript_path.open("xb")
    attempt_index = 0
    best_replay = replay

    def predicate(candidate_events: list[dict[str, Any]], removed: list[str]) -> bool:
        nonlocal attempt_index, best_replay
        attempt_index += 1
        candidate_plan = copy.deepcopy(base_plan)
        candidate_plan["events"] = candidate_events
        _set_plan_digest(candidate_plan)
        candidate_dir = output_dir / "candidates" / f"attempt-{attempt_index:03d}"
        try:
            observed = execute_plan(
                candidate_plan,
                candidate_dir,
                ssh_script=ssh_script,
                provenance={
                    "kind": "reduction-candidate",
                    "sourceReplayDigest": canonical_digest(replay),
                },
            )
            observed_signature = observed["failureSignature"]
            candidate_error = None
        except ChaosError as exc:
            observed = None
            observed_signature = None
            candidate_error = str(exc)[:MAX_TEXT_BYTES]
        matched = observed_signature == target_signature
        transcript.write(
            canonical_json_bytes(
                {
                    "attempt": attempt_index,
                    "error": candidate_error,
                    "eventCount": len(candidate_events),
                    "failureSignature": observed_signature,
                    "matched": matched,
                    "planDigest": candidate_plan["planDigest"],
                    "removedGroups": removed,
                }
            )
        )
        transcript.flush()
        if matched and observed is not None:
            best_replay = observed
        return matched

    try:
        minimized_events, attempts = grouped_ddmin(base_plan["events"], predicate, max_attempts)
    finally:
        transcript.close()
    if len(minimized_events) == len(base_plan["events"]):
        best_replay = replay
    write_new_json(output_dir / "minimized-replay.json", best_replay)
    reduction = {
        "attempts": attempts,
        "evidenceComplete": bool(best_replay["evidenceComplete"]),
        "format": REDUCTION_FORMAT,
        "minimizedEventCount": len(minimized_events),
        "originalEventCount": len(base_plan["events"]),
        "sourceReplayDigest": canonical_digest(replay),
        "targetFailureSignature": target_signature,
        "version": FORMAT_VERSION,
    }
    write_new_json(output_dir / "reduction.json", reduction)
    manifest = {
        "artifacts": _artifact_inventory(output_dir),
        "completion": {
            "evidenceComplete": reduction["evidenceComplete"],
            "outcome": "reduced",
        },
        "failureSignature": target_signature,
        "format": RUN_FORMAT,
        "limits": {"artifacts": MAX_ARTIFACTS, "reductionAttempts": max_attempts},
        "runId": best_replay["runId"],
        "scenario": copy.deepcopy(best_replay["scenario"]),
        "version": FORMAT_VERSION,
    }
    write_new_json(output_dir / "manifest.json", manifest)
    return reduction


def validate_matrix(raw: dict[str, Any]) -> dict[str, Any]:
    _require_keys(raw, {"format", "lanes", "scenarios", "version"}, {"format", "lanes", "scenarios", "version"}, "matrix")
    if raw["format"] != MATRIX_FORMAT or raw["version"] != FORMAT_VERSION:
        raise ChaosInputError(f"matrix must use {MATRIX_FORMAT!r} version {FORMAT_VERSION}")
    raw_lanes = raw["lanes"]
    if not isinstance(raw_lanes, list) or not raw_lanes or len(raw_lanes) > 16:
        raise ChaosInputError("matrix.lanes must contain 1..16 lanes")
    lanes: list[dict[str, Any]] = []
    lane_ids: set[str] = set()
    for index, raw_lane in enumerate(raw_lanes):
        lane = _require_object(raw_lane, f"matrix.lanes[{index}]")
        _require_keys(lane, {"description", "id", "kind"}, {"description", "id", "kind"}, f"matrix.lanes[{index}]")
        lane_id = _safe_id(lane["id"], f"matrix.lanes[{index}].id")
        if lane_id in lane_ids:
            raise ChaosInputError(f"duplicate matrix lane {lane_id!r}")
        lane_ids.add(lane_id)
        kind = lane["kind"]
        if kind not in {"host-model", "ktest-model", "live"}:
            raise ChaosInputError(f"matrix.lanes[{index}].kind is invalid")
        lanes.append(
            {
                "description": _bounded_string(lane["description"], f"matrix.lanes[{index}].description"),
                "id": lane_id,
                "kind": kind,
            }
        )
    required_phases = {"setup", "workload", "fault", "heal", "snapshot", "convergence"}
    raw_scenarios = raw["scenarios"]
    if not isinstance(raw_scenarios, list) or not raw_scenarios or len(raw_scenarios) > 64:
        raise ChaosInputError("matrix.scenarios must contain 1..64 scenarios")
    scenarios: list[dict[str, Any]] = []
    scenario_ids: set[str] = set()
    for index, raw_scenario in enumerate(raw_scenarios):
        where = f"matrix.scenarios[{index}]"
        scenario = _require_object(raw_scenario, where)
        _require_keys(
            scenario,
            {
                "deadline_ms",
                "execution",
                "fixed_seed",
                "id",
                "invariant",
                "phases",
                "prerequisites",
                "required_lanes",
                "title",
            },
            {
                "deadline_ms",
                "execution",
                "fixed_seed",
                "id",
                "invariant",
                "phases",
                "prerequisites",
                "required_lanes",
                "title",
            },
            where,
        )
        scenario_id = _safe_id(scenario["id"], f"{where}.id")
        if scenario_id in scenario_ids:
            raise ChaosInputError(f"duplicate matrix scenario {scenario_id!r}")
        scenario_ids.add(scenario_id)
        execution = scenario["execution"]
        if execution not in {"live-ready", "model-only"}:
            raise ChaosInputError(f"{where}.execution must be live-ready or model-only")
        fixed_seed = str(parse_seed(scenario["fixed_seed"]))
        phases = _require_object(scenario["phases"], f"{where}.phases")
        _require_keys(phases, required_phases, required_phases, f"{where}.phases")
        normalized_phases: dict[str, list[str]] = {}
        for phase in sorted(required_phases):
            actions = phases[phase]
            if not isinstance(actions, list) or not actions or len(actions) > 32:
                raise ChaosInputError(f"{where}.phases.{phase} must contain 1..32 actions")
            normalized_phases[phase] = [
                _bounded_string(action, f"{where}.phases.{phase}") for action in actions
            ]
        prerequisites = _require_object(scenario["prerequisites"], f"{where}.prerequisites")
        _require_keys(
            prerequisites,
            {"chaos_boot_opt_in", "min_nodes", "qmp", "transports"},
            {"chaos_boot_opt_in", "min_nodes", "qmp", "transports"},
            f"{where}.prerequisites",
        )
        transports = prerequisites["transports"]
        if not isinstance(transports, list) or not transports or len(transports) > 8:
            raise ChaosInputError(f"{where}.prerequisites.transports is invalid")
        normalized_transports = sorted(
            {_safe_id(item, f"{where}.prerequisites.transports") for item in transports}
        )
        for boolean in ("chaos_boot_opt_in", "qmp"):
            if not isinstance(prerequisites[boolean], bool):
                raise ChaosInputError(f"{where}.prerequisites.{boolean} must be boolean")
        required_lanes = scenario["required_lanes"]
        if not isinstance(required_lanes, list) or not required_lanes:
            raise ChaosInputError(f"{where}.required_lanes must be non-empty")
        normalized_required_lanes = sorted(
            {_safe_id(item, f"{where}.required_lanes") for item in required_lanes}
        )
        unknown_lanes = set(normalized_required_lanes) - lane_ids
        if unknown_lanes:
            raise ChaosInputError(f"{where}.required_lanes names unknown lanes: {sorted(unknown_lanes)}")
        invariant = _require_object(scenario["invariant"], f"{where}.invariant")
        _require_keys(invariant, {"postconditions", "signature"}, {"postconditions", "signature"}, f"{where}.invariant")
        postconditions = invariant["postconditions"]
        if not isinstance(postconditions, list) or not postconditions or len(postconditions) > 32:
            raise ChaosInputError(f"{where}.invariant.postconditions is invalid")
        scenarios.append(
            {
                "deadline_ms": _bounded_int(
                    scenario["deadline_ms"], 1, MAX_EVENT_TIMEOUT_MS, f"{where}.deadline_ms"
                ),
                "execution": execution,
                "fixed_seed": fixed_seed,
                "id": scenario_id,
                "invariant": {
                    "postconditions": [
                        _bounded_string(item, f"{where}.invariant.postconditions")
                        for item in postconditions
                    ],
                    "signature": _safe_id(invariant["signature"], f"{where}.invariant.signature"),
                },
                "phases": normalized_phases,
                "prerequisites": {
                    "chaos_boot_opt_in": prerequisites["chaos_boot_opt_in"],
                    "min_nodes": _bounded_int(
                        prerequisites["min_nodes"], 1, MAX_NODES, f"{where}.prerequisites.min_nodes"
                    ),
                    "qmp": prerequisites["qmp"],
                    "transports": normalized_transports,
                },
                "required_lanes": normalized_required_lanes,
                "title": _bounded_string(scenario["title"], f"{where}.title"),
            }
        )
    return {
        "format": MATRIX_FORMAT,
        "lanes": sorted(lanes, key=lambda item: item["id"]),
        "scenarios": scenarios,
        "version": FORMAT_VERSION,
    }


def validate_scenario_catalog(
    raw: dict[str, Any], raw_matrix: dict[str, Any]
) -> dict[str, Any]:
    _require_keys(
        raw,
        {"format", "matrix", "scenarios", "version"},
        {"format", "matrix", "scenarios", "version"},
        "scenario catalog",
    )
    if raw["format"] != SCENARIO_CATALOG_FORMAT or raw["version"] != FORMAT_VERSION:
        raise ChaosInputError(
            f"scenario catalog must use {SCENARIO_CATALOG_FORMAT!r} version {FORMAT_VERSION}"
        )
    matrix_path = _bounded_string(raw["matrix"], "scenario catalog.matrix", 4096)
    matrix = validate_matrix(raw_matrix)
    matrix_by_id = {scenario["id"]: scenario for scenario in matrix["scenarios"]}
    raw_scenarios = raw["scenarios"]
    if not isinstance(raw_scenarios, list) or not raw_scenarios or len(raw_scenarios) > 64:
        raise ChaosInputError("scenario catalog.scenarios must contain 1..64 entries")
    scenarios: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw_entry in enumerate(raw_scenarios):
        where = f"scenario catalog.scenarios[{index}]"
        entry = _require_object(raw_entry, where)
        _require_keys(
            entry,
            {
                "execution",
                "host_model",
                "id",
                "ktest_passes",
                "live_scenario",
                "missing_adapters",
            },
            {
                "execution",
                "host_model",
                "id",
                "ktest_passes",
                "live_scenario",
                "missing_adapters",
            },
            where,
        )
        scenario_id = _safe_id(entry["id"], f"{where}.id")
        if scenario_id in seen:
            raise ChaosInputError(f"duplicate scenario catalog ID {scenario_id!r}")
        seen.add(scenario_id)
        if scenario_id not in matrix_by_id:
            raise ChaosInputError(f"{where}.id is not present in the committed matrix")
        execution = entry["execution"]
        if execution != matrix_by_id[scenario_id]["execution"]:
            raise ChaosInputError(
                f"{where}.execution disagrees with the committed matrix"
            )
        missing = entry["missing_adapters"]
        if not isinstance(missing, list) or len(missing) > 32:
            raise ChaosInputError(f"{where}.missing_adapters must be a bounded array")
        missing_adapters = [
            _bounded_string(item, f"{where}.missing_adapters[{item_index}]", 1024)
            for item_index, item in enumerate(missing)
        ]
        live_scenario = entry["live_scenario"]
        if live_scenario is not None:
            live_scenario = copy.deepcopy(
                _require_object(live_scenario, f"{where}.live_scenario")
            )
            validated_live_scenario = validate_scenario(live_scenario)
            if validated_live_scenario["name"] != scenario_id:
                raise ChaosInputError(f"{where}.live_scenario.name must equal {scenario_id!r}")
        if execution == "live-ready":
            if live_scenario is None or missing_adapters:
                raise ChaosInputError(
                    f"{where} claims live-ready without a complete live scenario"
                )
        elif live_scenario is not None or not missing_adapters:
            raise ChaosInputError(
                f"{where} model-only entry must have blockers and no executable live scenario"
            )
        host_model = entry["host_model"]
        if not isinstance(host_model, list) or not host_model or len(host_model) > 32:
            raise ChaosInputError(f"{where}.host_model must contain 1..32 checks")
        normalized_checks: list[dict[str, Any]] = []
        for check_index, raw_check in enumerate(host_model):
            check_where = f"{where}.host_model[{check_index}]"
            check = _require_object(raw_check, check_where)
            kind = check.get("kind")
            if kind == "binary":
                _require_keys(check, {"args", "kind", "name"}, {"args", "kind", "name"}, check_where)
                args = check["args"]
                if not isinstance(args, list) or len(args) > MAX_ARGV - 1:
                    raise ChaosInputError(f"{check_where}.args is invalid")
                normalized_checks.append(
                    {
                        "args": [
                            _bounded_string(arg, f"{check_where}.args[{arg_index}]", MAX_ARG_BYTES)
                            for arg_index, arg in enumerate(args)
                        ],
                        "kind": "binary",
                        "name": _safe_id(check["name"], f"{check_where}.name"),
                    }
                )
            elif kind == "python":
                _require_keys(check, {"kind", "path"}, {"kind", "path"}, check_where)
                path = _bounded_string(check["path"], f"{check_where}.path", 4096)
                candidate = Path(path)
                if candidate.is_absolute() or ".." in candidate.parts:
                    raise ChaosInputError(f"{check_where}.path must be repository-relative")
                normalized_checks.append({"kind": "python", "path": path})
            else:
                raise ChaosInputError(f"{check_where}.kind must be binary or python")
        ktest_passes = entry["ktest_passes"]
        if not isinstance(ktest_passes, list) or not ktest_passes or len(ktest_passes) > 64:
            raise ChaosInputError(f"{where}.ktest_passes must contain 1..64 markers")
        normalized_ktest = [
            _bounded_string(marker, f"{where}.ktest_passes[{marker_index}]", 256)
            for marker_index, marker in enumerate(ktest_passes)
        ]
        if any(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", marker) is None for marker in normalized_ktest):
            raise ChaosInputError(f"{where}.ktest_passes contains an invalid KTEST marker")
        scenarios.append(
            {
                "execution": execution,
                "host_model": normalized_checks,
                "id": scenario_id,
                "ktest_passes": normalized_ktest,
                "live_scenario": live_scenario,
                "missing_adapters": missing_adapters,
            }
        )
    if seen != set(matrix_by_id):
        missing_ids = sorted(set(matrix_by_id) - seen)
        extra_ids = sorted(seen - set(matrix_by_id))
        raise ChaosInputError(
            f"scenario catalog IDs must exactly match matrix; missing={missing_ids}, extra={extra_ids}"
        )
    return {
        "format": SCENARIO_CATALOG_FORMAT,
        "matrix": matrix_path,
        "scenarios": scenarios,
        "version": FORMAT_VERSION,
    }


def _catalog_entry(catalog: dict[str, Any], scenario_id: str) -> dict[str, Any]:
    safe_id = _safe_id(scenario_id, "scenario ID")
    for entry in catalog["scenarios"]:
        if entry["id"] == safe_id:
            return entry
    raise ChaosInputError(f"scenario catalog does not contain {safe_id!r}")


def scenario_readiness_report(catalog: dict[str, Any]) -> dict[str, Any]:
    entries = []
    for entry in catalog["scenarios"]:
        entries.append(
            {
                "execution": entry["execution"],
                "id": entry["id"],
                "liveReady": entry["live_scenario"] is not None,
                "missingAdapters": copy.deepcopy(entry["missing_adapters"]),
            }
        )
    return {
        "catalogDigest": canonical_digest(catalog),
        "format": "wos.wki-chaos-readiness",
        "scenarios": entries,
        "version": FORMAT_VERSION,
    }


def materialize_catalog_scenario(
    catalog: dict[str, Any], scenario_id: str, output: Path
) -> dict[str, Any]:
    entry = _catalog_entry(catalog, scenario_id)
    if entry["live_scenario"] is None:
        blockers = "; ".join(entry["missing_adapters"])
        raise ChaosInputError(
            f"scenario {entry['id']} is not live-ready; missing adapter/workload: {blockers}"
        )
    scenario = copy.deepcopy(entry["live_scenario"])
    validate_scenario(scenario)
    write_new_json(output, scenario)
    return scenario


def _resolve_host_test_binary(build_dir: Path, name: str) -> Path:
    candidates = [build_dir / "host" / name, build_dir / name]
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    raise ChaosInputError(
        f"host test {name!r} is missing; checked {', '.join(str(path) for path in candidates)}"
    )


def _write_qualifier_result(
    output_dir: Path,
    *,
    scenario_id: str,
    lane: str,
    checks: list[dict[str, Any]],
    catalog: dict[str, Any],
) -> dict[str, Any]:
    evidence_complete = all(check.get("evidenceComplete") is True for check in checks)
    execution_complete = all(check.get("executionComplete") is True for check in checks)
    outcome = (
        "incomplete"
        if not evidence_complete or not execution_complete
        else "passed"
        if all(check.get("passed") is True for check in checks)
        else "failed"
    )
    result = {
        "catalogDigest": canonical_digest(catalog),
        "checks": checks,
        "evidence": _artifact_inventory(output_dir),
        "evidenceComplete": evidence_complete,
        "executionComplete": execution_complete,
        "format": LANE_RESULT_FORMAT,
        "generator": {"name": QUALIFIER_NAME, "version": FORMAT_VERSION},
        "lane": lane,
        "outcome": outcome,
        "scenario": scenario_id,
        "version": FORMAT_VERSION,
    }
    write_new_json(output_dir / "result.json", result)
    return result


def qualify_host_model(
    catalog: dict[str, Any], scenario_id: str, output_dir: Path, host_test_build: Path
) -> dict[str, Any]:
    entry = _catalog_entry(catalog, scenario_id)
    commands: list[tuple[list[str], str]] = []
    for check in entry["host_model"]:
        if check["kind"] == "binary":
            argv = [str(_resolve_host_test_binary(host_test_build, check["name"])), *check["args"]]
            check_id = check["name"]
        else:
            source = ROOT / check["path"]
            if not source.is_file():
                raise ChaosInputError(f"host model source check is missing: {source}")
            argv = [sys.executable, str(source)]
            check_id = Path(check["path"]).stem
        commands.append((argv, check_id))
    create_output_dir(output_dir)
    checks: list[dict[str, Any]] = []
    for index, (argv, check_id) in enumerate(commands):
        result = run_bounded_command(argv, MAX_EVENT_TIMEOUT_MS)
        evidence_dir = output_dir / "evidence"
        stdout_path = evidence_dir / f"check-{index:02d}.stdout.txt"
        stderr_path = evidence_dir / f"check-{index:02d}.stderr.txt"
        write_new_bytes(stdout_path, result["stdout"].encode("utf-8"))
        write_new_bytes(stderr_path, result["stderr"].encode("utf-8"))
        complete = (
            not result["timedOut"]
            and "error" not in result
            and not result["stdoutTruncated"]
            and not result["stderrTruncated"]
        )
        checks.append(
            {
                "argv": argv,
                "evidenceComplete": complete,
                "executionComplete": result.get("exitCode") is not None,
                "exitCode": result.get("exitCode"),
                "id": check_id,
                "passed": complete and result.get("exitCode") == 0,
                "stderrPath": stderr_path.relative_to(output_dir).as_posix(),
                "stdoutPath": stdout_path.relative_to(output_dir).as_posix(),
                "timedOut": result["timedOut"],
            }
        )
    return _write_qualifier_result(
        output_dir,
        scenario_id=entry["id"],
        lane="host-model",
        checks=checks,
        catalog=catalog,
    )


def qualify_ktest_model(
    catalog: dict[str, Any], scenario_id: str, output_dir: Path, serial_path: Path
) -> dict[str, Any]:
    entry = _catalog_entry(catalog, scenario_id)
    try:
        serial_size = serial_path.stat().st_size
        if serial_size > MAX_INPUT_BYTES:
            raise ChaosInputError(
                f"KTEST serial log exceeds the {MAX_INPUT_BYTES}-byte evidence bound"
            )
        serial_data = serial_path.read_bytes()
        serial_text = serial_data.decode("utf-8", errors="replace")
    except OSError as exc:
        raise ChaosInputError(f"cannot read KTEST serial evidence {serial_path}: {exc}") from exc
    create_output_dir(output_dir)
    evidence_path = output_dir / "evidence" / "serial.log"
    write_new_bytes(evidence_path, serial_data)
    suite_starts = list(
        re.finditer(
            KTEST_PREFIX_RE + r"===\s+WOS Kernel Self-Test Suite\s+===",
            serial_text,
        )
    )
    suite_text = serial_text[suite_starts[-1].start() :] if suite_starts else ""
    summaries = list(
        re.finditer(
            KTEST_PREFIX_RE + r"===\s+(\d+) passed, (\d+) failed\s+===",
            suite_text,
        )
    )
    suite_complete = len(summaries) == 1
    completed_text = suite_text[: summaries[0].end()] if suite_complete else ""
    passed_markers = set(
        re.findall(
            KTEST_PREFIX_RE
            + r"PASS\s+([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)",
            completed_text,
        )
    )
    fail_lines = re.findall(KTEST_PREFIX_RE + r"FAIL(?:\s|$)", completed_text)
    summary_ok = (
        suite_complete
        and int(summaries[0].group(2)) == 0
        and not fail_lines
    )
    checks: list[dict[str, Any]] = [
        {
            "evidenceComplete": suite_complete,
            "executionComplete": suite_complete,
            "id": "ktest-suite-summary",
            "passed": summary_ok,
            "serialPath": evidence_path.relative_to(output_dir).as_posix(),
        }
    ]
    for marker in entry["ktest_passes"]:
        checks.append(
            {
                "evidenceComplete": suite_complete,
                "executionComplete": suite_complete,
                "id": marker,
                "passed": marker in passed_markers,
                "serialPath": evidence_path.relative_to(output_dir).as_posix(),
            }
        )
    return _write_qualifier_result(
        output_dir,
        scenario_id=entry["id"],
        lane="ktest-model",
        checks=checks,
        catalog=catalog,
    )


def _verify_evidence_inventory(
    result_path: Path, raw_inventory: Any
) -> str | None:
    if not isinstance(raw_inventory, list) or not raw_inventory or len(raw_inventory) > MAX_ARTIFACTS:
        return "evidence inventory must contain 1..8192 artifacts"
    base = result_path.parent.resolve()
    seen: set[str] = set()
    for index, raw_artifact in enumerate(raw_inventory):
        if not isinstance(raw_artifact, dict) or set(raw_artifact) != {"path", "sha256", "size"}:
            return f"evidence artifact {index} has an invalid schema"
        artifact_path = raw_artifact.get("path")
        digest = raw_artifact.get("sha256")
        size = raw_artifact.get("size")
        if not isinstance(artifact_path, str) or not artifact_path or artifact_path in seen:
            return f"evidence artifact {index} has an invalid or duplicate path"
        seen.add(artifact_path)
        relative = Path(artifact_path)
        if relative.is_absolute() or ".." in relative.parts:
            return f"evidence artifact {artifact_path!r} escapes the lane directory"
        candidate = result_path.parent / relative
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(base)
            actual_size = resolved.stat().st_size
        except (OSError, ValueError):
            return f"evidence artifact {artifact_path!r} is missing or escapes the lane directory"
        if not resolved.is_file() or isinstance(size, bool) or not isinstance(size, int) or size != actual_size:
            return f"evidence artifact {artifact_path!r} size does not match"
        if not isinstance(digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
            return f"evidence artifact {artifact_path!r} digest is invalid"
        hasher = hashlib.sha256()
        try:
            with resolved.open("rb") as stream:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    hasher.update(chunk)
        except OSError:
            return f"evidence artifact {artifact_path!r} cannot be read"
        if digest != "sha256:" + hasher.hexdigest():
            return f"evidence artifact {artifact_path!r} digest does not match"
    return None


def _load_matrix_lane_result(
    path: Path,
    scenario_id: str,
    lane_id: str,
    expected_catalog_digest: str | None = None,
) -> tuple[str, bool, bool, str | None]:
    if not path.exists():
        return "incomplete", False, False, "required lane result is missing"
    try:
        raw = load_json(path)
    except ChaosInputError as exc:
        return "incomplete", False, False, str(exc)
    if raw.get("format") == LANE_RESULT_FORMAT and raw.get("version") == FORMAT_VERSION:
        if lane_id not in {"host-model", "ktest-model"}:
            return "incomplete", False, False, "qualifier result cannot satisfy a live lane"
        expected_members = {
            "catalogDigest",
            "checks",
            "evidence",
            "evidenceComplete",
            "executionComplete",
            "format",
            "generator",
            "lane",
            "outcome",
            "scenario",
            "version",
        }
        if set(raw) != expected_members:
            return "incomplete", False, False, "lane result schema is invalid"
        if raw.get("scenario") != scenario_id or raw.get("lane") != lane_id:
            return "incomplete", False, False, "lane result identity does not match its matrix path"
        if raw.get("generator") != {"name": QUALIFIER_NAME, "version": FORMAT_VERSION}:
            return "incomplete", False, False, "lane result was not emitted by the qualifier"
        catalog_digest = raw.get("catalogDigest")
        if not isinstance(catalog_digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", catalog_digest) is None:
            return "incomplete", False, False, "lane result catalog digest is invalid"
        if expected_catalog_digest is not None and catalog_digest != expected_catalog_digest:
            return "incomplete", False, False, "lane result was emitted for a different scenario catalog"
        checks = raw.get("checks")
        if not isinstance(checks, list) or not checks:
            return "incomplete", False, False, "lane result has no executed checks"
        check_ids: set[str] = set()
        referenced_paths: set[str] = set()
        for check in checks:
            if not isinstance(check, dict):
                return "incomplete", False, False, "lane result check schema is invalid"
            common = {"evidenceComplete", "executionComplete", "id", "passed"}
            if lane_id == "host-model":
                expected_check_members = common | {
                    "argv",
                    "exitCode",
                    "stderrPath",
                    "stdoutPath",
                    "timedOut",
                }
                argv = check.get("argv")
                if (
                    not isinstance(argv, list)
                    or not argv
                    or any(not isinstance(arg, str) or not arg for arg in argv)
                    or not isinstance(check.get("timedOut"), bool)
                    or (
                        check.get("exitCode") is not None
                        and (
                            isinstance(check.get("exitCode"), bool)
                            or not isinstance(check.get("exitCode"), int)
                        )
                    )
                ):
                    return "incomplete", False, False, "host qualifier check schema is invalid"
                for member in ("stdoutPath", "stderrPath"):
                    if not isinstance(check.get(member), str) or not check[member]:
                        return "incomplete", False, False, "host qualifier evidence path is invalid"
                    referenced_paths.add(check[member])
            else:
                expected_check_members = common | {"serialPath"}
                if not isinstance(check.get("serialPath"), str) or not check["serialPath"]:
                    return "incomplete", False, False, "KTEST qualifier evidence path is invalid"
                referenced_paths.add(check["serialPath"])
            if set(check) != expected_check_members:
                return "incomplete", False, False, "lane result check schema is invalid"
            if (
                not isinstance(check.get("id"), str)
                or not check["id"]
                or check["id"] in check_ids
                or not isinstance(check.get("passed"), bool)
                or not isinstance(check.get("executionComplete"), bool)
                or not isinstance(check.get("evidenceComplete"), bool)
            ):
                return "incomplete", False, False, "lane result check schema is invalid"
            check_ids.add(check["id"])
        inventory_issue = _verify_evidence_inventory(path, raw.get("evidence"))
        if inventory_issue is not None:
            return "incomplete", False, False, inventory_issue
        inventory_paths = {artifact["path"] for artifact in raw["evidence"]}
        if referenced_paths != inventory_paths:
            return "incomplete", False, False, "lane result evidence does not exactly match its checks"
        outcome = raw.get("outcome")
        evidence_complete = raw.get("evidenceComplete") is True
        execution_complete = raw.get("executionComplete") is True
        expected_evidence = all(check["evidenceComplete"] for check in checks)
        expected_execution = all(check["executionComplete"] for check in checks)
        expected_outcome = (
            "incomplete"
            if not expected_evidence or not expected_execution
            else "passed"
            if all(check["passed"] for check in checks)
            else "failed"
        )
        if (
            evidence_complete != expected_evidence
            or execution_complete != expected_execution
            or outcome != expected_outcome
        ):
            return "incomplete", False, False, "lane result contradicts its executed checks"
    elif raw.get("format") == RUN_FORMAT and raw.get("version") == FORMAT_VERSION:
        if lane_id in {"host-model", "ktest-model"}:
            return "incomplete", False, False, "live run manifest cannot satisfy a model lane"
        if raw.get("scenario", {}).get("name") != scenario_id:
            return "incomplete", False, False, "run manifest scenario does not match its matrix path"
        inventory_issue = _verify_evidence_inventory(path, raw.get("artifacts"))
        if inventory_issue is not None:
            return "incomplete", False, False, inventory_issue
        completion = raw.get("completion", {})
        outcome = completion.get("outcome")
        evidence_complete = completion.get("evidenceComplete") is True
        execution_complete = completion.get("executionComplete") is True
    else:
        return "incomplete", False, False, "unsupported lane result format"
    if outcome not in {"failed", "incomplete", "passed", "skipped"}:
        return "incomplete", evidence_complete, execution_complete, "lane outcome is invalid"
    if outcome == "passed" and (not evidence_complete or not execution_complete):
        return "incomplete", evidence_complete, execution_complete, "pass lacks complete execution/evidence"
    if outcome == "skipped":
        return "incomplete", evidence_complete, execution_complete, "required lane was skipped"
    return outcome, evidence_complete, execution_complete, None


def evaluate_matrix(
    raw_matrix: dict[str, Any],
    results_dir: Path,
    raw_catalog: dict[str, Any] | None = None,
) -> dict[str, Any]:
    matrix = validate_matrix(raw_matrix)
    expected_catalog_digest: str | None = None
    if raw_catalog is not None:
        catalog = validate_scenario_catalog(raw_catalog, raw_matrix)
        expected_catalog_digest = canonical_digest(catalog)
    scenario_results: list[dict[str, Any]] = []
    overall = "passed"
    for scenario in matrix["scenarios"]:
        lanes: list[dict[str, Any]] = []
        scenario_outcome = "passed"
        for lane_id in scenario["required_lanes"]:
            path = results_dir / scenario["id"] / lane_id / "result.json"
            manifest_path = path.with_name("manifest.json")
            if not path.exists() and manifest_path.exists():
                path = manifest_path
            outcome, evidence_complete, execution_complete, issue = _load_matrix_lane_result(
                path, scenario["id"], lane_id, expected_catalog_digest
            )
            if outcome == "failed":
                scenario_outcome = "failed"
            elif outcome != "passed" and scenario_outcome == "passed":
                scenario_outcome = "incomplete"
            lane_result: dict[str, Any] = {
                "evidenceComplete": evidence_complete,
                "executionComplete": execution_complete,
                "lane": lane_id,
                "outcome": outcome,
                "path": path.relative_to(results_dir).as_posix(),
            }
            if issue is not None:
                lane_result["issue"] = issue
            lanes.append(lane_result)
        if scenario_outcome == "failed":
            overall = "failed"
        elif scenario_outcome != "passed" and overall == "passed":
            overall = "incomplete"
        scenario_results.append(
            {
                "execution": scenario["execution"],
                "id": scenario["id"],
                "invariantSignature": scenario["invariant"]["signature"],
                "lanes": lanes,
                "outcome": scenario_outcome,
            }
        )
    return {
        "format": MATRIX_REPORT_FORMAT,
        "fullMatrixPassed": overall == "passed",
        "matrixDigest": canonical_digest(matrix),
        "outcome": overall,
        "scenarios": scenario_results,
        "version": FORMAT_VERSION,
    }


def _add_run_arguments(parser: argparse.ArgumentParser, *, replay: bool = False) -> None:
    if replay:
        parser.add_argument("--input", type=Path, required=True, help="Observed replay JSON")
    else:
        parser.add_argument("--scenario", type=Path, required=True)
        parser.add_argument("--seed", required=True, help="Unsigned 64-bit decimal or 0x seed")
    parser.add_argument("--output", type=Path, required=True, help="New artifact directory")
    parser.add_argument(
        "--ssh-script",
        type=Path,
        default=ROOT / "scripts" / "remote" / "wos_ssh.sh",
        help="Guest argv transport",
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate and record without external actions")


def _add_catalog_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--catalog", type=Path, default=DEFAULT_CATALOG_PATH,
        help="Committed scenario readiness catalog",
    )
    parser.add_argument(
        "--matrix", type=Path, default=DEFAULT_MATRIX_PATH,
        help="Committed qualification matrix",
    )


def _load_catalog(catalog_path: Path, matrix_path: Path) -> dict[str, Any]:
    return validate_scenario_catalog(load_json(catalog_path), load_json(matrix_path))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan_parser = subparsers.add_parser("plan", help="Generate a byte-stable plan")
    plan_parser.add_argument("--scenario", type=Path, required=True)
    plan_parser.add_argument("--seed", required=True)
    plan_parser.add_argument("--output", type=Path, required=True, help="New plan JSON path")
    for command in ("run", "attach"):
        command_parser = subparsers.add_parser(
            command,
            help="Execute a scenario against an already-running WOS topology",
        )
        _add_run_arguments(command_parser)
    replay_parser = subparsers.add_parser("replay", help="Execute the exact schedule from a replay")
    _add_run_arguments(replay_parser, replay=True)
    minimize_parser = subparsers.add_parser("minimize", help="Group-preserving replay reduction")
    minimize_parser.add_argument("--input", type=Path, required=True)
    minimize_parser.add_argument("--output", type=Path, required=True)
    minimize_parser.add_argument(
        "--max-attempts", type=int, default=32, choices=range(1, MAX_REDUCTION_ATTEMPTS + 1)
    )
    minimize_parser.add_argument(
        "--ssh-script", type=Path, default=ROOT / "scripts" / "remote" / "wos_ssh.sh"
    )
    matrix_parser = subparsers.add_parser(
        "matrix", help="Evaluate every required model/live matrix lane"
    )
    matrix_parser.add_argument("--matrix", type=Path, required=True)
    matrix_parser.add_argument(
        "--catalog", type=Path, default=DEFAULT_CATALOG_PATH,
        help="Catalog digest required for generated model-lane evidence",
    )
    matrix_parser.add_argument("--results", type=Path, required=True)
    matrix_parser.add_argument("--output", type=Path, required=True)

    readiness_parser = subparsers.add_parser(
        "readiness", help="Report executable scenarios and exact missing adapters"
    )
    _add_catalog_arguments(readiness_parser)
    readiness_parser.add_argument(
        "--output", type=Path, help="Optional new JSON report path"
    )

    materialize_parser = subparsers.add_parser(
        "materialize", help="Emit a catalog scenario only when it is live-ready"
    )
    _add_catalog_arguments(materialize_parser)
    materialize_parser.add_argument("--scenario", required=True, help="Matrix scenario ID")
    materialize_parser.add_argument("--output", type=Path, required=True)

    host_parser = subparsers.add_parser(
        "qualify-host", help="Run and record the scenario's real host-model checks"
    )
    _add_catalog_arguments(host_parser)
    host_parser.add_argument("--scenario", required=True, help="Matrix scenario ID")
    host_parser.add_argument("--output", type=Path, required=True)
    host_parser.add_argument(
        "--host-test-build",
        type=Path,
        default=Path(os.environ.get("WOS_TEST_BUILD_DIR", "/tmp/wos-tests")),
        help="Configured WOS host-test build directory",
    )

    ktest_parser = subparsers.add_parser(
        "qualify-ktest", help="Qualify one scenario from a real KTEST serial log"
    )
    _add_catalog_arguments(ktest_parser)
    ktest_parser.add_argument("--scenario", required=True, help="Matrix scenario ID")
    ktest_parser.add_argument("--serial", type=Path, required=True)
    ktest_parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.command == "plan":
            plan = make_plan(load_json(args.scenario), parse_seed(args.seed))
            write_new_json(args.output, plan)
            print(plan["planDigest"])
            return 0
        if args.command == "matrix":
            report = evaluate_matrix(
                load_json(args.matrix), args.results, load_json(args.catalog)
            )
            write_new_json(args.output, report)
            print(canonical_json_bytes(report).decode("utf-8"), end="")
            return 0 if report["fullMatrixPassed"] else 1
        if args.command == "readiness":
            catalog = _load_catalog(args.catalog, args.matrix)
            report = scenario_readiness_report(catalog)
            if args.output is not None:
                write_new_json(args.output, report)
            print(canonical_json_bytes(report).decode("utf-8"), end="")
            return 0
        if args.command == "materialize":
            catalog = _load_catalog(args.catalog, args.matrix)
            scenario = materialize_catalog_scenario(catalog, args.scenario, args.output)
            print(canonical_digest(scenario))
            return 0
        if args.command == "qualify-host":
            catalog = _load_catalog(args.catalog, args.matrix)
            result = qualify_host_model(
                catalog, args.scenario, args.output, args.host_test_build
            )
            print(canonical_json_bytes(result).decode("utf-8"), end="")
            return 0 if result["outcome"] == "passed" else 1
        if args.command == "qualify-ktest":
            catalog = _load_catalog(args.catalog, args.matrix)
            result = qualify_ktest_model(
                catalog, args.scenario, args.output, args.serial
            )
            print(canonical_json_bytes(result).decode("utf-8"), end="")
            return 0 if result["outcome"] == "passed" else 1
        if args.command in {"run", "attach"}:
            plan = make_plan(load_json(args.scenario), parse_seed(args.seed))
            replay = execute_plan(
                plan,
                args.output,
                ssh_script=args.ssh_script,
                dry_run=args.dry_run,
            )
        elif args.command == "replay":
            source_replay = validate_replay(load_json(args.input))
            plan = replay_to_plan(source_replay)
            replay = execute_plan(
                plan,
                args.output,
                ssh_script=args.ssh_script,
                dry_run=args.dry_run,
                provenance={
                    "kind": "exact-replay",
                    "sourceReplayDigest": canonical_digest(source_replay),
                },
            )
        else:
            reduction = minimize_replay(
                load_json(args.input),
                args.output,
                ssh_script=args.ssh_script,
                max_attempts=args.max_attempts,
            )
            print(canonical_json_bytes(reduction).decode("utf-8"), end="")
            return 0
        print(
            canonical_json_bytes(
                {
                    "evidenceComplete": replay["evidenceComplete"],
                    "failureSignature": replay["failureSignature"],
                    "outcome": replay["outcome"],
                    "runId": replay["runId"],
                }
            ).decode("utf-8"),
            end="",
        )
        return 0 if replay["outcome"] in {"dry-run", "passed"} else 1
    except ChaosError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
