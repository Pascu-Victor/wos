#!/usr/bin/env python3
"""Validate and run the versioned WOS mlibc conformance baseline.

The host owns orchestration and result classification. Target-side commands are
selected by a generated, fixed dispatch script; manifest strings are never
evaluated directly by a WOS shell at run time.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import selectors
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = ROOT / "tests" / "conformance" / "mlibc" / "baseline-v1.json"
DEFAULT_MLIBC_ROOT = ROOT / "toolchain" / "src" / "mlibc"
DEFAULT_MLIBC_BUILD_SCRIPT = ROOT / "scripts" / "build" / "build_mlibc.sh"
DEFAULT_REMOTE_ROOT = "/usr/libexec/wos-mlibc-conformance"
DEFAULT_REMOTE_RUNNER = "/usr/libexec/wos-mlibc-conformance/run-case"
DEFAULT_SSH_COMMAND = ROOT / "scripts" / "remote" / "wos_ssh.sh"

CASE_ID_RE = re.compile(r"^(ansi|posix|linux|linux-wrappers|rtld)/[a-z0-9][a-z0-9_.-]*$")
BASELINE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")
FEATURE_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")
ENV_NAME_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
INVOCATION_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")

RESULT_STATUSES = (
    "pass",
    "assertion_failure",
    "crash",
    "timeout",
    "build_failure",
    "unsupported",
    "infrastructure_failure",
)
PORT_IDS = ("busybox", "dropbear", "bash", "python", "cmake", "git")
LOCALE_SPECS = (
    ("C", "UTF-8", "C.UTF-8"),
    ("en_US", "UTF-8", "en_US.UTF-8"),
    ("en_US", "ISO-8859-1", "en_US"),
    ("de_DE", "UTF-8", "de_DE.UTF-8"),
    ("de_DE", "ISO-8859-1", "de_DE"),
    ("ru_RU", "UTF-8", "ru_RU.UTF-8"),
    ("ru_RU", "KOI8-R", "ru_RU"),
)


class ManifestError(ValueError):
    """Raised when the baseline is ambiguous, incomplete, or unsafe."""


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ManifestError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _expect_keys(value: dict[str, Any], required: set[str], optional: set[str], context: str) -> None:
    missing = sorted(required - value.keys())
    unknown = sorted(value.keys() - required - optional)
    if missing:
        raise ManifestError(f"{context}: missing keys: {', '.join(missing)}")
    if unknown:
        raise ManifestError(f"{context}: unknown keys: {', '.join(unknown)}")


def _expect_string(value: Any, context: str, *, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or len(value) > maximum:
        raise ManifestError(f"{context}: expected a non-empty bounded string")
    return value


def _expect_string_list(value: Any, context: str, *, maximum: int = 64) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or len(value) > maximum:
        raise ManifestError(f"{context}: expected a non-empty bounded list")
    result = tuple(_expect_string(item, f"{context}[]") for item in value)
    if len(set(result)) != len(result):
        raise ManifestError(f"{context}: duplicate values are not allowed")
    return result


def _validate_relative_artifact(value: Any, context: str) -> str:
    artifact = _expect_string(value, context, maximum=256)
    path = PurePosixPath(artifact)
    if path.is_absolute() or ".." in path.parts or "." in path.parts:
        raise ManifestError(f"{context}: artifact must be a normalized relative path")
    if any(not part or part.startswith("-") for part in path.parts):
        raise ManifestError(f"{context}: unsafe artifact component")
    return artifact


@dataclass(frozen=True)
class Invocation:
    name: str
    arguments: tuple[str, ...]
    environment: tuple[tuple[str, str], ...]
    expected_stdout: str | None
    accepted_returncodes: tuple[int, ...]


@dataclass(frozen=True)
class Case:
    identifier: str
    feature_group: str
    prerequisites: tuple[str, ...]
    timeout_seconds: int
    expected: str
    rationale: str | None
    artifact: str | None
    invocations: tuple[Invocation, ...]


@dataclass(frozen=True)
class PortSmoke:
    identifier: str
    timeout_seconds: int
    command: tuple[str, ...]
    rationale: str


@dataclass(frozen=True)
class Manifest:
    path: Path
    schema_version: int
    baseline_id: str
    selection_rule: str
    active_options: tuple[tuple[str, bool], ...]
    max_output_bytes: int
    cases: tuple[Case, ...]
    port_smokes: tuple[PortSmoke, ...]
    digest: str


def _parse_invocation(raw: Any, context: str) -> Invocation:
    if not isinstance(raw, dict):
        raise ManifestError(f"{context}: expected an object")
    _expect_keys(
        raw,
        {"name"},
        {"arguments", "environment", "expected_stdout", "accepted_returncodes"},
        context,
    )
    name = _expect_string(raw["name"], f"{context}.name", maximum=64)
    if not INVOCATION_RE.fullmatch(name):
        raise ManifestError(f"{context}.name: invalid invocation name")

    arguments_raw = raw.get("arguments", [])
    if not isinstance(arguments_raw, list) or len(arguments_raw) > 32:
        raise ManifestError(f"{context}.arguments: expected a bounded list")
    arguments = tuple(_expect_string(item, f"{context}.arguments[]", maximum=1024) for item in arguments_raw)

    environment_raw = raw.get("environment", {})
    if not isinstance(environment_raw, dict) or len(environment_raw) > 32:
        raise ManifestError(f"{context}.environment: expected a bounded object")
    environment: list[tuple[str, str]] = []
    for key in sorted(environment_raw):
        if not ENV_NAME_RE.fullmatch(key):
            raise ManifestError(f"{context}.environment: invalid name {key!r}")
        value = environment_raw[key]
        if not isinstance(value, str) or "\x00" in value or len(value) > 4096:
            raise ManifestError(f"{context}.environment.{key}: expected a bounded string")
        environment.append((key, value))

    expected_stdout_raw = raw.get("expected_stdout")
    expected_stdout = None
    if expected_stdout_raw is not None:
        expected_stdout = _expect_string(expected_stdout_raw, f"{context}.expected_stdout", maximum=65536)

    returncodes_raw = raw.get("accepted_returncodes", [0])
    if not isinstance(returncodes_raw, list) or not returncodes_raw or len(returncodes_raw) > 16:
        raise ManifestError(f"{context}.accepted_returncodes: expected a non-empty bounded list")
    returncodes: list[int] = []
    for value in returncodes_raw:
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 255:
            raise ManifestError(f"{context}.accepted_returncodes: values must be integers from 0 through 255")
        if value in (124, 126, 127, 255):
            raise ManifestError(
                f"{context}.accepted_returncodes: {value} is reserved for timeout or infrastructure failures"
            )
        returncodes.append(value)
    if len(set(returncodes)) != len(returncodes):
        raise ManifestError(f"{context}.accepted_returncodes: duplicate values are not allowed")
    return Invocation(name, arguments, tuple(environment), expected_stdout, tuple(returncodes))


def _parse_case(raw: Any, index: int) -> Case:
    context = f"cases[{index}]"
    if not isinstance(raw, dict):
        raise ManifestError(f"{context}: expected an object")
    _expect_keys(
        raw,
        {"id", "feature_group", "prerequisites", "timeout_seconds", "expected"},
        {"rationale", "artifact", "invocations"},
        context,
    )
    identifier = _expect_string(raw["id"], f"{context}.id", maximum=128)
    if not CASE_ID_RE.fullmatch(identifier):
        raise ManifestError(f"{context}.id: invalid case identifier {identifier!r}")
    feature_group = _expect_string(raw["feature_group"], f"{context}.feature_group", maximum=96)
    if not FEATURE_RE.fullmatch(feature_group):
        raise ManifestError(f"{context}.feature_group: invalid feature group")
    prerequisites = _expect_string_list(raw["prerequisites"], f"{context}.prerequisites")

    timeout_seconds = raw["timeout_seconds"]
    if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int) or not 1 <= timeout_seconds <= 300:
        raise ManifestError(f"{context}.timeout_seconds: expected an integer from 1 through 300")
    expected = raw["expected"]
    if expected not in ("pass", "unsupported"):
        raise ManifestError(f"{context}.expected: must be pass or unsupported")

    rationale_raw = raw.get("rationale")
    rationale = None if rationale_raw is None else _expect_string(rationale_raw, f"{context}.rationale")
    artifact_raw = raw.get("artifact")
    artifact = None if artifact_raw is None else _validate_relative_artifact(artifact_raw, f"{context}.artifact")
    invocations_raw = raw.get("invocations", [{"name": "default"}])
    if not isinstance(invocations_raw, list) or not invocations_raw or len(invocations_raw) > 64:
        raise ManifestError(f"{context}.invocations: expected a non-empty bounded list")
    invocations = tuple(_parse_invocation(item, f"{context}.invocations[{i}]") for i, item in enumerate(invocations_raw))
    invocation_names = [item.name for item in invocations]
    if len(set(invocation_names)) != len(invocation_names):
        raise ManifestError(f"{context}.invocations: invocation names must be unique")

    if expected == "unsupported":
        if rationale is None:
            raise ManifestError(f"{context}: unsupported cases require a rationale")
        if artifact is not None:
            raise ManifestError(f"{context}: unsupported cases must not name an artifact")
        if invocations != (Invocation("default", (), (), None, (0,)),):
            raise ManifestError(f"{context}: unsupported cases must not define invocations")
    elif artifact is None:
        raise ManifestError(f"{context}: passing cases require an artifact")

    return Case(identifier, feature_group, prerequisites, timeout_seconds, expected, rationale, artifact, invocations)


def _parse_port(raw: Any, index: int) -> PortSmoke:
    context = f"port_smokes[{index}]"
    if not isinstance(raw, dict):
        raise ManifestError(f"{context}: expected an object")
    _expect_keys(raw, {"id", "timeout_seconds", "command", "rationale"}, set(), context)
    identifier = _expect_string(raw["id"], f"{context}.id", maximum=32)
    timeout = raw["timeout_seconds"]
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= 300:
        raise ManifestError(f"{context}.timeout_seconds: expected an integer from 1 through 300")
    command = _expect_string_list(raw["command"], f"{context}.command", maximum=32)
    if not command[0].startswith("/") or any("\x00" in part or "\n" in part for part in command):
        raise ManifestError(f"{context}.command: executable must be absolute and arguments single-line")
    rationale = _expect_string(raw["rationale"], f"{context}.rationale")
    return PortSmoke(identifier, timeout, command, rationale)


def load_manifest(path: Path) -> Manifest:
    try:
        raw_bytes = path.read_bytes()
        raw = json.loads(raw_bytes, object_pairs_hook=_reject_duplicate_keys)
    except (OSError, json.JSONDecodeError) as exc:
        raise ManifestError(f"cannot read manifest {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ManifestError("manifest root must be an object")
    _expect_keys(
        raw,
        {"schema_version", "baseline_id", "selection", "max_output_bytes", "cases", "port_smokes"},
        set(),
        "manifest",
    )
    if raw["schema_version"] != 1:
        raise ManifestError("manifest.schema_version: only version 1 is supported")
    baseline_id = _expect_string(raw["baseline_id"], "manifest.baseline_id", maximum=96)
    if not BASELINE_ID_RE.fullmatch(baseline_id):
        raise ManifestError("manifest.baseline_id: invalid baseline identifier")

    selection = raw["selection"]
    if not isinstance(selection, dict):
        raise ManifestError("manifest.selection: expected an object")
    _expect_keys(selection, {"rule", "active_options"}, set(), "manifest.selection")
    selection_rule = _expect_string(selection["rule"], "manifest.selection.rule", maximum=8192)
    active_raw = selection["active_options"]
    expected_option_names = {"ansi", "posix", "linux", "linux-wrappers", "rtld"}
    if not isinstance(active_raw, dict) or set(active_raw) != expected_option_names:
        raise ManifestError("manifest.selection.active_options: expected ansi, posix, linux, linux-wrappers, and rtld")
    if any(not isinstance(value, bool) for value in active_raw.values()):
        raise ManifestError("manifest.selection.active_options: option values must be booleans")
    active_options = tuple((key, active_raw[key]) for key in sorted(active_raw))

    max_output = raw["max_output_bytes"]
    if isinstance(max_output, bool) or not isinstance(max_output, int) or not 1024 <= max_output <= 1_048_576:
        raise ManifestError("manifest.max_output_bytes: expected an integer from 1024 through 1048576")
    cases_raw = raw["cases"]
    if not isinstance(cases_raw, list) or not cases_raw or len(cases_raw) > 1000:
        raise ManifestError("manifest.cases: expected a non-empty bounded list")
    cases = tuple(_parse_case(item, i) for i, item in enumerate(cases_raw))
    identifiers = [case.identifier for case in cases]
    if identifiers != sorted(identifiers):
        raise ManifestError("manifest.cases: cases must be sorted by id")
    if len(set(identifiers)) != len(identifiers):
        raise ManifestError("manifest.cases: duplicate case id")

    active = dict(active_options)
    for case in cases:
        suite = case.identifier.split("/", 1)[0]
        if not active[suite] and case.expected != "unsupported":
            raise ManifestError(f"{case.identifier}: disabled suites must be explicitly unsupported")

    ports_raw = raw["port_smokes"]
    if not isinstance(ports_raw, list):
        raise ManifestError("manifest.port_smokes: expected a list")
    ports = tuple(_parse_port(item, i) for i, item in enumerate(ports_raw))
    port_ids = tuple(port.identifier for port in ports)
    if port_ids != PORT_IDS:
        raise ManifestError(f"manifest.port_smokes: expected exact ordered profile {', '.join(PORT_IDS)}")

    return Manifest(
        path=path,
        schema_version=1,
        baseline_id=baseline_id,
        selection_rule=selection_rule,
        active_options=active_options,
        max_output_bytes=max_output,
        cases=cases,
        port_smokes=ports,
        digest=hashlib.sha256(raw_bytes).hexdigest(),
    )


def registered_corpus_cases(mlibc_root: Path) -> set[str]:
    tests_root = mlibc_root / "tests"
    main_path = tests_root / "meson.build"
    rtld_path = tests_root / "rtld" / "meson.build"
    try:
        main_text = main_path.read_text(encoding="utf-8")
        rtld_text = rtld_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestError(f"cannot read mlibc corpus metadata: {exc}") from exc

    cases = {
        f"{match.group(1)}/{match.group(2)}"
        for match in re.finditer(r"'(ansi|posix|linux|linux-wrappers)/([^']+)'", main_text)
    }
    blocks = re.findall(r"rtld_test_cases\s*(?:\+?=)\s*\[(.*?)\]", rtld_text, flags=re.DOTALL)
    if not blocks:
        raise ManifestError("could not parse mlibc RTLD case declarations")
    for block in blocks:
        for name in re.findall(r"'([^']+)'", block):
            cases.add(f"rtld/{name}")
    return cases


def validate_corpus_coverage(manifest: Manifest, mlibc_root: Path) -> None:
    registered = registered_corpus_cases(mlibc_root)
    declared = {case.identifier for case in manifest.cases}
    missing = sorted(registered - declared)
    extra = sorted(declared - registered)
    if missing or extra:
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if extra:
            details.append("not registered upstream: " + ", ".join(extra))
        raise ManifestError("manifest corpus coverage mismatch; " + "; ".join(details))


def active_wos_sysdep_tags(mlibc_root: Path) -> set[str]:
    path = mlibc_root / "sysdeps" / "wos" / "include" / "mlibc" / "sysdeps.hpp"
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestError(f"cannot read WOS sysdep tag declaration: {exc}") from exc
    match = re.search(r"struct\s+WosSysdepTags\s*:(.*?)\n};", text, flags=re.DOTALL)
    if match is None:
        raise ManifestError("cannot locate WosSysdepTags declaration")
    tags = set(re.findall(r"^\s+([A-Z][A-Za-z0-9_]*)\s*(?:,|\{)", match.group(1), flags=re.MULTILINE))
    if not tags:
        raise ManifestError("WosSysdepTags declaration yielded no active tags")
    return tags


def configured_suite_options(build_script: Path) -> dict[str, bool]:
    try:
        text = build_script.read_text(encoding="utf-8")
    except OSError as exc:
        raise ManifestError(f"cannot read mlibc build configuration: {exc}") from exc

    def feature_enabled(name: str) -> bool:
        matches = set(re.findall(rf"-D{name}_option=(enabled|disabled)", text))
        if len(matches) != 1:
            raise ManifestError(f"mlibc build configuration does not set {name}_option unambiguously")
        return matches.pop() == "enabled"

    posix = feature_enabled("posix")
    linux = feature_enabled("linux")
    if '-Dbuild_tests="$MLIBC_BUILD_TESTS"' not in text:
        raise ManifestError("mlibc build configuration does not expose the audited build_tests switch")
    return {
        "ansi": True,
        "posix": posix,
        "linux": linux,
        "linux-wrappers": linux,
        "rtld": True,
    }


def validate_selection_policy(
    manifest: Manifest,
    mlibc_root: Path,
    build_script: Path = DEFAULT_MLIBC_BUILD_SCRIPT,
) -> None:
    configured = configured_suite_options(build_script)
    declared = dict(manifest.active_options)
    if declared != configured:
        raise ManifestError(
            "manifest active options do not match build_mlibc.sh: "
            f"declared={declared}, configured={configured}"
        )
    tags = active_wos_sysdep_tags(mlibc_root)
    prerequisite_re = re.compile(
        r"^(?:mlibc-option:[a-z][a-z-]*|mlibc-tag:[A-Z][A-Za-z0-9_]*|"
        r"runtime:[a-z][a-z-]*|compiler-feature:[a-z][a-z-]*)$"
    )
    for case in manifest.cases:
        for prerequisite in case.prerequisites:
            if prerequisite_re.fullmatch(prerequisite) is None:
                raise ManifestError(f"{case.identifier}: unknown prerequisite syntax {prerequisite!r}")
        suite = case.identifier.split("/", 1)[0]
        if not configured[suite]:
            if case.expected != "unsupported":
                raise ManifestError(f"{case.identifier}: disabled suite case is not reviewed unsupported")
            continue
        required_tags = {
            prerequisite.removeprefix("mlibc-tag:")
            for prerequisite in case.prerequisites
            if prerequisite.startswith("mlibc-tag:")
        }
        unavailable_tags = required_tags - tags
        compiler_blockers = {
            prerequisite
            for prerequisite in case.prerequisites
            if prerequisite.startswith("compiler-feature:")
        }
        if case.expected == "unsupported":
            if not unavailable_tags and not compiler_blockers:
                raise ManifestError(
                    f"{case.identifier}: active-suite unsupported case lacks an objective missing prerequisite"
                )
        elif unavailable_tags:
            raise ManifestError(
                f"{case.identifier}: passing case requires absent WosSysdepTags: "
                + ", ".join(sorted(unavailable_tags))
            )


@dataclass(frozen=True)
class RawRun:
    returncode: int | None
    stdout: bytes
    stderr: bytes
    duration_ms: int
    timed_out: bool = False
    output_limited: bool = False
    spawn_error: str | None = None


def run_bounded(command: list[str], timeout_seconds: int, max_output_bytes: int) -> RawRun:
    started = time.monotonic()
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except OSError as exc:
        return RawRun(None, b"", b"", 0, spawn_error=str(exc))

    assert process.stdout is not None
    assert process.stderr is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ, "stdout")
    selector.register(process.stderr, selectors.EVENT_READ, "stderr")
    chunks: dict[str, list[bytes]] = {"stdout": [], "stderr": []}
    total = 0
    timed_out = False
    output_limited = False

    def terminate_group() -> None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    while selector.get_map():
        remaining = timeout_seconds - (time.monotonic() - started)
        if remaining <= 0:
            timed_out = True
            terminate_group()
            remaining = 0.1
        events = selector.select(min(max(remaining, 0.0), 0.1))
        if not events and process.poll() is not None:
            events = [(key, selectors.EVENT_READ) for key in list(selector.get_map().values())]
        for key, _mask in events:
            data = os.read(key.fileobj.fileno(), 4096)
            if not data:
                selector.unregister(key.fileobj)
                continue
            keep = max_output_bytes - total
            if keep > 0:
                chunks[key.data].append(data[:keep])
                total += min(len(data), keep)
            if len(data) > keep:
                output_limited = True
                terminate_group()
        if output_limited:
            terminate_group()
    selector.close()
    returncode = process.wait()
    duration_ms = round((time.monotonic() - started) * 1000)
    return RawRun(
        returncode,
        b"".join(chunks["stdout"]),
        b"".join(chunks["stderr"]),
        duration_ms,
        timed_out=timed_out,
        output_limited=output_limited,
    )


def classify_run(
    raw: RawRun,
    expected_stdout: str | None = None,
    accepted_returncodes: tuple[int, ...] = (0,),
) -> tuple[str, str | None]:
    if raw.spawn_error is not None:
        return "infrastructure_failure", f"spawn failed: {raw.spawn_error}"
    if raw.output_limited:
        return "infrastructure_failure", "combined output exceeded the manifest limit"
    if raw.timed_out or raw.returncode == 124:
        return "timeout", "case exceeded its manifest timeout"
    if raw.returncode in accepted_returncodes:
        if expected_stdout is not None and raw.stdout.decode("utf-8", errors="replace") != expected_stdout:
            return "assertion_failure", "stdout did not match the reviewed golden output"
        return "pass", None
    if raw.returncode in (126, 127, 255):
        return "infrastructure_failure", f"remote infrastructure returned {raw.returncode}"
    combined = (raw.stdout + b"\n" + raw.stderr).lower()
    if raw.returncode == 134 or b"assert" in combined:
        return "assertion_failure", "test assertion failed"
    if raw.returncode is not None and (raw.returncode < 0 or 128 <= raw.returncode <= 192):
        return "crash", f"test terminated by signal-style status {raw.returncode}"
    return "assertion_failure", f"test returned {raw.returncode}"


def _decode_output(value: bytes) -> str:
    return value.decode("utf-8", errors="replace")


def _git_identity(path: Path) -> str:
    result = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else "unavailable"


def _git_dirty(path: Path) -> bool | None:
    result = subprocess.run(
        ["git", "-C", str(path), "status", "--porcelain", "--untracked-files=normal"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    return bool(result.stdout) if result.returncode == 0 else None


def _selected_cases(manifest: Manifest, suites: set[str], identifiers: set[str]) -> list[Case]:
    cases = list(manifest.cases)
    if suites:
        cases = [case for case in cases if case.identifier.split("/", 1)[0] in suites]
    if identifiers:
        known = {case.identifier for case in manifest.cases}
        unknown = sorted(identifiers - known)
        if unknown:
            raise ManifestError("unknown selected cases: " + ", ".join(unknown))
        cases = [case for case in cases if case.identifier in identifiers]
    return cases


def _build_artifact_path(build_dir: Path, case: Case) -> Path:
    suite, name = case.identifier.split("/", 1)
    if suite == "rtld":
        return build_dir / "tests" / "rtld" / f"rtld-{name}"
    return build_dir / "tests" / f"{suite}-{name}"


def _copy_file(source: Path, destination: Path) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination, follow_symlinks=True)
    return hashlib.sha256(destination.read_bytes()).hexdigest()


def _copy_rtld_case_data(build_dir: Path, case: Case, stage_root: Path, hashes: dict[str, str]) -> None:
    _suite, name = case.identifier.split("/", 1)
    source_root = build_dir / "tests" / "rtld" / name
    if not source_root.is_dir():
        return
    for source in sorted(source_root.rglob("*")):
        relative = source.relative_to(source_root)
        if not source.is_file() or any(part.endswith(".p") for part in relative.parts):
            continue
        if "native" in source.name:
            continue
        destination = stage_root / "cases" / "rtld" / name / relative
        digest = _copy_file(source, destination)
        hashes[destination.relative_to(stage_root).as_posix()] = digest


def _stage_locale_data(localedef: str, stage_root: Path, hashes: dict[str, str]) -> None:
    with tempfile.TemporaryDirectory(prefix="wos-mlibc-locales-", dir=stage_root.parent) as directory:
        prefix = Path(directory)
        (prefix / "usr" / "lib" / "locale").mkdir(parents=True)
        for source_name, charmap, output_name in LOCALE_SPECS:
            command = [
                localedef,
                "--no-archive",
                "--prefix",
                str(prefix),
                "-i",
                source_name,
                "-f",
                charmap,
                output_name,
            ]
            raw = run_bounded(command, 60, 65536)
            if raw.spawn_error is not None:
                raise ManifestError(f"cannot execute localedef: {raw.spawn_error}")
            if raw.timed_out:
                raise ManifestError(f"localedef timed out while generating {output_name}")
            if raw.returncode != 0:
                details = _decode_output(raw.stderr).strip() or _decode_output(raw.stdout).strip()
                raise ManifestError(f"localedef failed for {output_name}: {details}")

        locale_root = prefix / "usr" / "lib" / "locale"
        for source in sorted(locale_root.rglob("*")):
            if not source.is_file():
                continue
            relative = source.relative_to(locale_root)
            destination = stage_root / "locales" / relative
            digest = _copy_file(source, destination)
            hashes[destination.relative_to(stage_root).as_posix()] = digest


def _validate_remote_root(value: str) -> str:
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts or "." in path.parts:
        raise ManifestError("remote root must be a normalized absolute path")
    if path.parts[:3] != ("/", "usr", "libexec") or len(path.parts) < 4:
        raise ManifestError("remote root must be a child of /usr/libexec")
    return value.rstrip("/")


def _dispatch_arm(
    key: str,
    timeout_seconds: int,
    environment: tuple[tuple[str, str], ...],
    command: tuple[str, ...],
) -> list[str]:
    lines = [f"  {shlex.quote(key)})", "    unset LD_LIBRARY_PATH TZ"]
    for name, value in environment:
        lines.append(f"    {name}={shlex.quote(value)}")
        lines.append(f"    export {name}")
    quoted_command = " ".join(shlex.quote(part) for part in command)
    lines.extend(
        [
            f"    /usr/bin/timeout {timeout_seconds} {quoted_command}",
            "    status=$?",
            "    ;;",
        ]
    )
    return lines


def generate_dispatch(manifest: Manifest, remote_root: str) -> str:
    remote_root = _validate_remote_root(remote_root)
    lines = [
        "#!/bin/sh",
        "# Generated from the versioned WOS mlibc conformance baseline.",
        "set -u",
        'if [ "$#" -ne 2 ]; then',
        "  echo 'usage: run-case CASE-ID INVOCATION-INDEX' >&2",
        "  exit 64",
        "fi",
        "key=$1:$2",
        "umask 077",
        "work=/tmp/wos-mlibc-conformance.$$",
        'if ! mkdir "$work"; then',
        "  echo 'cannot create isolated conformance work directory' >&2",
        "  exit 70",
        "fi",
        """trap 'rm -rf "$work"' EXIT HUP INT TERM""",
        "HOME=$work/home",
        "TMPDIR=$work/tmp",
        "PATH=/usr/bin:/usr/sbin:/bin:/sbin",
        "LANG=en_US.UTF-8",
        f"LOCPATH={shlex.quote(remote_root + '/locales')}",
        "unset LC_ALL",
        "export HOME TMPDIR PATH LANG LOCPATH",
        'mkdir "$HOME" "$TMPDIR" || exit 70',
        'cd "$work" || exit 70',
        "status=65",
        'case "$key" in',
    ]
    for case in manifest.cases:
        if case.expected == "unsupported":
            continue
        assert case.artifact is not None
        executable = f"{remote_root}/{case.artifact}"
        for index, invocation in enumerate(case.invocations):
            command = (executable, *invocation.arguments)
            lines.extend(
                _dispatch_arm(
                    f"{case.identifier}:{index}",
                    case.timeout_seconds,
                    invocation.environment,
                    command,
                )
            )
    for port in manifest.port_smokes:
        lines.extend(_dispatch_arm(f"port/{port.identifier}:0", port.timeout_seconds, (), port.command))
    lines.extend(
        [
            "  *)",
            '    echo "unknown conformance case: $key" >&2',
            "    status=64",
            "    ;;",
            "esac",
            'exit "$status"',
            "",
        ]
    )
    return "\n".join(lines)


def stage_cases(args: argparse.Namespace, manifest: Manifest) -> int:
    build_dir = args.build_dir.resolve()
    output_dir = args.output_dir.resolve()
    if not (build_dir / "build.ninja").is_file():
        raise ManifestError(f"mlibc build directory is not configured: {build_dir}")
    if output_dir in (Path("/"), ROOT):
        raise ManifestError("refusing unsafe staging output directory")
    output_dir.parent.mkdir(parents=True, exist_ok=True)

    temporary = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    try:
        hashes: dict[str, str] = {}
        build_results: list[dict[str, Any]] = []
        missing: list[str] = []
        _stage_locale_data(args.localedef, temporary, hashes)
        for case in manifest.cases:
            if case.expected == "unsupported":
                build_results.append(
                    {"id": case.identifier, "status": "unsupported", "reason": case.rationale}
                )
                continue
            assert case.artifact is not None
            source = _build_artifact_path(build_dir, case)
            if not source.is_file():
                missing.append(f"{case.identifier} ({source})")
                build_results.append(
                    {"id": case.identifier, "status": "build_failure", "reason": "artifact is missing"}
                )
                continue
            destination = temporary / case.artifact
            hashes[case.artifact] = _copy_file(source, destination)
            if case.identifier.startswith("rtld/"):
                _copy_rtld_case_data(build_dir, case, temporary, hashes)
            build_results.append({"id": case.identifier, "status": "pass", "reason": None})

        report = {
            "schema_version": 1,
            "baseline_id": manifest.baseline_id,
            "manifest_sha256": manifest.digest,
            "repository_commit": _git_identity(ROOT),
            "repository_dirty": _git_dirty(ROOT),
            "mlibc_commit": _git_identity(DEFAULT_MLIBC_ROOT),
            "mlibc_dirty": _git_dirty(DEFAULT_MLIBC_ROOT),
            "artifacts": dict(sorted(hashes.items())),
            "cases": build_results,
        }
        (temporary / "build-results.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        if missing:
            raise ManifestError("missing built conformance artifacts: " + "; ".join(missing))

        shutil.copy2(manifest.path, temporary / "baseline.json")
        dispatcher = temporary / "run-case"
        dispatcher.write_text(generate_dispatch(manifest, args.remote_root), encoding="utf-8")
        dispatcher.chmod(0o755)
        owner = {
            "owner": "wos-mlibc-conformance-stage",
            "baseline_id": manifest.baseline_id,
            "manifest_sha256": manifest.digest,
        }
        (temporary / ".stage-owner.json").write_text(
            json.dumps(owner, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

        if output_dir.exists():
            marker = output_dir / ".stage-owner.json"
            if not output_dir.is_dir() or not marker.is_file():
                raise ManifestError(f"refusing to replace unowned staging path: {output_dir}")
            shutil.rmtree(output_dir)
        temporary.rename(output_dir)
        temporary = Path()
    finally:
        if temporary != Path() and temporary.exists():
            shutil.rmtree(temporary)

    passed = sum(item["status"] == "pass" for item in build_results)
    unsupported = sum(item["status"] == "unsupported" for item in build_results)
    print(f"staged {passed} runnable cases and {unsupported} reviewed unsupported cases at {output_dir}")
    return 0


def run_cases(args: argparse.Namespace, manifest: Manifest) -> int:
    suites = set(args.suite)
    identifiers = set(args.case)
    cases = _selected_cases(manifest, suites, identifiers)
    if not cases:
        raise ManifestError("selection matched no cases")

    include_ports = getattr(args, "port_smokes", None)
    if include_ports is None:
        include_ports = not suites and not identifiers
    started = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()
    results: list[dict[str, Any]] = []
    for case in cases:
        if case.expected == "unsupported":
            results.append(
                {
                    "id": case.identifier,
                    "expected": case.expected,
                    "status": "unsupported",
                    "duration_ms": 0,
                    "reason": case.rationale,
                    "invocations": [],
                }
            )
            continue

        invocation_results: list[dict[str, Any]] = []
        aggregate_status = "pass"
        aggregate_reason = None
        duration_ms = 0
        for index, invocation in enumerate(case.invocations):
            command = [
                str(args.ssh_command),
                args.target,
                args.remote_runner,
                case.identifier,
                str(index),
            ]
            raw = run_bounded(command, case.timeout_seconds + args.transport_grace_seconds, manifest.max_output_bytes)
            status, reason = classify_run(
                raw,
                invocation.expected_stdout,
                invocation.accepted_returncodes,
            )
            duration_ms += raw.duration_ms
            invocation_results.append(
                {
                    "name": invocation.name,
                    "status": status,
                    "duration_ms": raw.duration_ms,
                    "returncode": raw.returncode,
                    "reason": reason,
                    "stdout": _decode_output(raw.stdout),
                    "stderr": _decode_output(raw.stderr),
                }
            )
            if status != "pass":
                aggregate_status = status
                aggregate_reason = f"invocation {invocation.name}: {reason}"
                if not args.keep_going:
                    break
        results.append(
            {
                "id": case.identifier,
                "expected": case.expected,
                "status": aggregate_status,
                "duration_ms": duration_ms,
                "reason": aggregate_reason,
                "invocations": invocation_results,
            }
        )
        print(f"{aggregate_status.upper():24} {case.identifier}", flush=True)

    if include_ports:
        for port in manifest.port_smokes:
            command = [
                str(args.ssh_command),
                args.target,
                args.remote_runner,
                f"port/{port.identifier}",
                "0",
            ]
            raw = run_bounded(
                command,
                port.timeout_seconds + args.transport_grace_seconds,
                manifest.max_output_bytes,
            )
            status, reason = classify_run(raw)
            results.append(
                {
                    "id": f"port/{port.identifier}",
                    "expected": "pass",
                    "status": status,
                    "duration_ms": raw.duration_ms,
                    "reason": reason,
                    "invocations": [
                        {
                            "name": "default",
                            "status": status,
                            "duration_ms": raw.duration_ms,
                            "returncode": raw.returncode,
                            "reason": reason,
                            "stdout": _decode_output(raw.stdout),
                            "stderr": _decode_output(raw.stderr),
                        }
                    ],
                }
            )
            print(f"{status.upper():24} port/{port.identifier}", flush=True)

    counts = Counter(result["status"] for result in results)
    completed_at = datetime.now(timezone.utc).isoformat()
    total_duration_ms = round((time.monotonic() - started) * 1000)
    suite_identity = {
        "manifest_sha256": manifest.digest,
        "repository_commit": _git_identity(ROOT),
        "repository_dirty": _git_dirty(ROOT),
        "mlibc_commit": _git_identity(DEFAULT_MLIBC_ROOT),
        "mlibc_dirty": _git_dirty(DEFAULT_MLIBC_ROOT),
        "target": args.target,
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "duration_ms": total_duration_ms,
        "selection": {
            "suites": sorted(suites),
            "cases": sorted(identifiers),
            "port_smokes": include_ports,
        },
    }
    document = {
        "schema_version": 1,
        "baseline_id": manifest.baseline_id,
        "suite_identity": suite_identity,
        "summary": {status: counts.get(status, 0) for status in RESULT_STATUSES},
        "cases": results,
    }
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "results.json"
    junit_path = output_dir / "results.xml"
    json_path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_junit(junit_path, manifest, suite_identity, results)
    print(f"JSON:  {json_path}")
    print(f"JUnit: {junit_path}")

    unexpected = [result for result in results if result["status"] not in ("pass", "unsupported")]
    return 1 if unexpected else 0


def write_junit(
    path: Path,
    manifest: Manifest,
    suite_identity: dict[str, Any],
    results: list[dict[str, Any]],
) -> None:
    failures = sum(result["status"] not in ("pass", "unsupported") for result in results)
    skipped = sum(result["status"] == "unsupported" for result in results)
    duration = sum(result["duration_ms"] for result in results) / 1000
    suite = ET.Element(
        "testsuite",
        {
            "name": manifest.baseline_id,
            "tests": str(len(results)),
            "failures": str(failures),
            "errors": "0",
            "skipped": str(skipped),
            "time": f"{duration:.3f}",
        },
    )
    properties = ET.SubElement(suite, "properties")
    for name in (
        "manifest_sha256",
        "repository_commit",
        "repository_dirty",
        "mlibc_commit",
        "mlibc_dirty",
        "target",
        "started_at_utc",
        "completed_at_utc",
        "duration_ms",
        "selection",
    ):
        value = suite_identity[name]
        if isinstance(value, (dict, list)):
            rendered = json.dumps(value, sort_keys=True, separators=(",", ":"))
        elif value is None:
            rendered = "unavailable"
        elif isinstance(value, bool):
            rendered = "true" if value else "false"
        else:
            rendered = str(value)
        ET.SubElement(properties, "property", {"name": name, "value": rendered})
    for result in results:
        suite_name, case_name = result["id"].split("/", 1)
        node = ET.SubElement(
            suite,
            "testcase",
            {"classname": suite_name, "name": case_name, "time": f"{result['duration_ms'] / 1000:.3f}"},
        )
        if result["status"] == "unsupported":
            ET.SubElement(node, "skipped", {"message": result["reason"] or "unsupported"})
        elif result["status"] != "pass":
            failure = ET.SubElement(node, "failure", {"type": result["status"], "message": result["reason"] or "failure"})
            failure.text = json.dumps(result["invocations"], indent=2, sort_keys=True)
        if result["invocations"]:
            stdout = ET.SubElement(node, "system-out")
            stdout.text = "".join(item["stdout"] for item in result["invocations"])
            stderr = ET.SubElement(node, "system-err")
            stderr.text = "".join(item["stderr"] for item in result["invocations"])
    ET.ElementTree(suite).write(path, encoding="utf-8", xml_declaration=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--mlibc-root", type=Path, default=DEFAULT_MLIBC_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("validate", help="Validate schema and complete upstream corpus coverage")

    stage = subparsers.add_parser("stage", help="Stage cross-built WOS cases and generate the fixed dispatcher")
    stage.add_argument("--build-dir", type=Path, required=True)
    stage.add_argument("--output-dir", type=Path, required=True)
    stage.add_argument("--remote-root", default=DEFAULT_REMOTE_ROOT)
    stage.add_argument("--localedef", default=shutil.which("localedef") or "localedef")

    run = subparsers.add_parser("run", help="Run selected cases through the fixed WOS target dispatcher")
    run.add_argument("--target", default="wos-ktest")
    run.add_argument("--ssh-command", type=Path, default=DEFAULT_SSH_COMMAND)
    run.add_argument("--remote-runner", default=DEFAULT_REMOTE_RUNNER)
    run.add_argument("--output-dir", type=Path, default=ROOT / "test-results" / "mlibc-conformance")
    run.add_argument("--suite", action="append", default=[], choices=("ansi", "posix", "linux", "linux-wrappers", "rtld"))
    run.add_argument("--case", action="append", default=[])
    run.add_argument("--transport-grace-seconds", type=int, default=15)
    run.add_argument("--keep-going", action=argparse.BooleanOptionalAction, default=True)
    run.add_argument(
        "--port-smokes",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="include port smokes (default: yes for an unfiltered full run, otherwise no)",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        manifest = load_manifest(args.manifest.resolve())
        validate_corpus_coverage(manifest, args.mlibc_root.resolve())
        validate_selection_policy(manifest, args.mlibc_root.resolve())
        if args.command == "validate":
            print(
                f"validated {manifest.baseline_id}: {len(manifest.cases)} corpus cases, "
                f"{len(manifest.port_smokes)} port smokes, sha256={manifest.digest}"
            )
            return 0
        if args.command == "stage":
            return stage_cases(args, manifest)
        if args.command == "run":
            if args.transport_grace_seconds < 0 or args.transport_grace_seconds > 300:
                raise ManifestError("transport grace must be from 0 through 300 seconds")
            return run_cases(args, manifest)
    except ManifestError as exc:
        parser.error(str(exc))
    raise AssertionError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
