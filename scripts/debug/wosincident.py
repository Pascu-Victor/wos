#!/usr/bin/env python3
"""Capture bounded, portable WOS incident bundles.

The directory and USTAR envelopes contain the same canonical ``manifest.json``
and regular-file members.  Capture is intentionally allowlist-driven: VM disk
images, overlays, SSH material, arbitrary guest files, and recursive build-tree
copies are never collected.
"""

from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import errno
import hashlib
import io
import json
import os
import re
import resource
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[2]
CLUSTER_SCRIPTS = ROOT / "scripts" / "cluster"
if str(CLUSTER_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(CLUSTER_SCRIPTS))

import node_setup  # noqa: E402

sys.modules.setdefault("wosincident", sys.modules[__name__])


INCIDENT_FORMAT = "wosincident"
INCIDENT_VERSION = 1
MANIFEST_NAME = "manifest.json"

DEFAULT_MAX_MEMBERS = 256
DEFAULT_MAX_FILE_BYTES = 128 * 1024 * 1024
# Keep uncompressed USTAR output below WOSDBG's separate 256 MiB archive-input
# ceiling after manifest/header/padding overhead.
DEFAULT_MAX_TOTAL_BYTES = 240 * 1024 * 1024
DEFAULT_MAX_PATH_BYTES = 240
DEFAULT_MAX_METADATA_BYTES = 8 * 1024 * 1024
DEFAULT_MAX_MANIFEST_BYTES = 1024 * 1024
LIVE_FETCH_TIMEOUT_SECONDS = 45.0

ALLOWED_KINDS = {
    "config",
    "serial-log",
    "qemu-log",
    "coverage-manifest",
    "coverage-artifact",
    "coverage-log",
    "coredump",
    "binary",
}
TEXT_KINDS = {
    "config",
    "serial-log",
    "qemu-log",
    "coverage-manifest",
    "coverage-log",
}
FORBIDDEN_PARTS = {".ssh", "cluster-data", "ssh-keys"}
FORBIDDEN_NAMES = {
    "authorized_keys",
    "dropbear_rsa_host_key",
    "id_dsa",
    "id_ecdsa",
    "id_ed25519",
    "id_rsa",
}
FORBIDDEN_SUFFIXES = {".img", ".iso", ".key", ".pem", ".qcow2", ".raw"}
SENSITIVE_KEY_RE = re.compile(
    r"(?:password|passwd|authorization|auth_token|access_token|refresh_token|"
    r"api_key|host_key|ssh_key|authorized_keys|private_key|secret|cookie)",
    re.IGNORECASE,
)
SENSITIVE_LINE_RE = re.compile(
    r"\b(?:password|passwd|authorization|bearer|token|api[_ -]?key|"
    r"private[_ -]?key|secret|cookie)\b",
    re.IGNORECASE,
)
PRIVATE_KEY_BEGIN_RE = re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")
PRIVATE_KEY_END_RE = re.compile(r"-----END [A-Z0-9 ]*PRIVATE KEY-----")
BUILD_ID_RE = re.compile(r"Build ID:\s*([0-9a-fA-F]+)")
HOST_RE = re.compile(r"[A-Za-z0-9._-]{1,255}\Z")
LIVE_COREDUMP_NAME_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]*_coredump\.bin\Z"
)

# Explicit executable allowlist.  Coredump v3 embeds the crashing executable;
# these host binaries provide kernel and common userspace symbols without
# recursively copying a build tree.
BINARY_CANDIDATES = (
    ("kernel", "modules/kern/wos"),
    ("init", "modules/init/init"),
    ("testprog", "modules/testprog/testprog"),
    ("testd", "modules/testd/testd"),
    ("netd", "modules/netd/netd"),
    ("debugserver", "modules/debugserver/debugserver"),
    ("httpd", "modules/httpd/httpd"),
    ("perf", "modules/perf/perf"),
    ("top", "modules/top/top"),
    ("memacc", "modules/memacc/memacc"),
    ("journal", "modules/journal/journal"),
    ("wkictl", "modules/wkictl/wkictl"),
    ("powerctl", "modules/powerctl/powerctl"),
    ("renderbench", "modules/renderbench/renderbench"),
    ("strace", "modules/strace/strace"),
    ("sftp-server", "modules/sftpserver/sftp-server"),
)


class IncidentError(RuntimeError):
    """A safe incident capture could not be completed."""


@dataclass(frozen=True)
class CaptureLimits:
    max_members: int = DEFAULT_MAX_MEMBERS
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES
    max_path_bytes: int = DEFAULT_MAX_PATH_BYTES

    def validate(self) -> None:
        for name, value in (
            ("max_members", self.max_members),
            ("max_file_bytes", self.max_file_bytes),
            ("max_total_bytes", self.max_total_bytes),
            ("max_path_bytes", self.max_path_bytes),
        ):
            if value <= 0:
                raise IncidentError(f"{name} must be positive")

    def effective(self) -> "CaptureLimits":
        """Clamp requested limits to the writer's loader-compatible ceilings."""
        self.validate()
        return CaptureLimits(
            max_members=min(self.max_members, DEFAULT_MAX_MEMBERS),
            max_file_bytes=min(self.max_file_bytes, DEFAULT_MAX_FILE_BYTES),
            max_total_bytes=min(self.max_total_bytes, DEFAULT_MAX_TOTAL_BYTES),
            max_path_bytes=min(self.max_path_bytes, DEFAULT_MAX_PATH_BYTES),
        )


@dataclass(frozen=True)
class SourceSnapshot:
    device: int
    inode: int
    size: int
    mtime_ns: int


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _manifest_bytes(manifest: dict[str, Any]) -> bytes:
    return (
        json.dumps(manifest, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _created_utc() -> str:
    return (
        dt.datetime.now(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _safe_component(value: str, fallback: str = "artifact") -> str:
    component = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip(".-")
    return component[:80] or fallback


def _member_path(value: str, limit: int) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts:
        raise IncidentError("incident member paths must be relative")
    if any(part in {"", ".", ".."} for part in path.parts):
        raise IncidentError("incident member path contains an unsafe component")
    normalized = path.as_posix()
    if len(normalized.encode("utf-8")) > limit:
        raise IncidentError("incident member path exceeds the configured limit")
    encoded_parts = [part.encode("utf-8") for part in path.parts]
    ustar_encodable = len(b"/".join(encoded_parts)) <= 100 or any(
        len(b"/".join(encoded_parts[:split])) <= 155
        and len(b"/".join(encoded_parts[split:])) <= 100
        for split in range(1, len(encoded_parts))
    )
    if not ustar_encodable:
        raise IncidentError("incident member path cannot be encoded as USTAR")
    return normalized


def _source_name(path: Path) -> str:
    name = path.name
    if SENSITIVE_KEY_RE.search(name) or name in FORBIDDEN_NAMES:
        return "[redacted]"
    return name


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _forbidden_source(path: Path) -> bool:
    lowered_parts = {part.lower() for part in path.parts}
    return (
        bool(lowered_parts & FORBIDDEN_PARTS)
        or path.name.lower() in FORBIDDEN_NAMES
        or SENSITIVE_KEY_RE.search(path.name) is not None
        or path.suffix.lower() in FORBIDDEN_SUFFIXES
    )


def _sanitize_json(
    value: Any,
    *,
    key: str | None = None,
    repo_root: Path = ROOT,
) -> Any:
    if key is not None and SENSITIVE_KEY_RE.search(key):
        return "[REDACTED]"
    if key is not None and key.lower() in {
        "bios",
        "disk0",
        "disk1",
        "disk",
        "disk0_overlay",
        "disk1_overlay",
        "overlay_dir",
        "overlay",
        "root_path",
    }:
        return "[OMITTED]"
    if isinstance(value, dict):
        return {
            str(item_key): _sanitize_json(
                item_value,
                key=str(item_key),
                repo_root=repo_root,
            )
            for item_key, item_value in sorted(
                value.items(), key=lambda item: str(item[0])
            )
        }
    if isinstance(value, list):
        return [_sanitize_json(item, repo_root=repo_root) for item in value]
    if isinstance(value, str):
        replacements = ((str(repo_root), "<repo>"), (str(Path.home()), "<home>"))
        sanitized = value
        for source, replacement in replacements:
            if source:
                sanitized = sanitized.replace(source, replacement)
        if sanitized.startswith("/"):
            return f"<absolute>/{Path(sanitized).name}"
        return sanitized
    return value


def redact_text(data: bytes) -> tuple[bytes, int]:
    """Redact common credential-bearing log lines and private-key blocks."""
    text = data.decode("utf-8", errors="replace")
    redactions = 0
    in_private_key = False
    output: list[str] = []
    for line in text.splitlines(keepends=True):
        if PRIVATE_KEY_BEGIN_RE.search(line):
            in_private_key = True
            redactions += 1
            output.append("[WOSINCIDENT REDACTED PRIVATE KEY]\n")
            continue
        if in_private_key:
            if PRIVATE_KEY_END_RE.search(line):
                in_private_key = False
            continue
        if SENSITIVE_LINE_RE.search(line) or line.lstrip().startswith(
            ("ssh-rsa ", "ssh-ed25519 ", "ecdsa-sha2-")
        ):
            redactions += 1
            ending = "\n" if line.endswith(("\n", "\r")) else ""
            output.append(f"[WOSINCIDENT REDACTED]{ending}")
            continue
        output.append(line)
    sanitized = "".join(output)
    sanitized = sanitized.replace(str(ROOT), "<repo>").replace(
        str(Path.home()), "<home>"
    )
    return sanitized.encode("utf-8"), redactions


def _read_build_id(path: Path) -> str | None:
    for tool_name in ("readelf", "llvm-readelf"):
        tool = shutil.which(tool_name)
        if tool is None:
            continue
        try:
            result = subprocess.run(
                [tool, "-n", str(path)],
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        match = BUILD_ID_RE.search(result.stdout)
        if match is not None:
            return match.group(1).lower()
    return None


def _git_metadata(repo_root: Path) -> tuple[str | None, bool | None]:
    try:
        revision_result = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        status_result = subprocess.run(
            ["git", "-C", str(repo_root), "status", "--porcelain"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None, None
    revision = (
        revision_result.stdout.strip() if revision_result.returncode == 0 else None
    )
    dirty = bool(status_result.stdout) if status_result.returncode == 0 else None
    return revision, dirty


def detect_build_profile(build_dir: Path, fallback: str = "unknown") -> str:
    cache = build_dir / "CMakeCache.txt"
    try:
        if cache.is_symlink() or not cache.is_file():
            return fallback
        with cache.open(encoding="utf-8", errors="replace") as stream:
            for line in stream:
                if line.startswith("CMAKE_BUILD_TYPE:STRING="):
                    return line.partition("=")[2].strip() or fallback
    except OSError:
        pass
    return fallback


class IncidentBuilder:
    def __init__(
        self,
        staging: Path,
        *,
        allowed_roots: Iterable[Path],
        limits: CaptureLimits,
    ) -> None:
        limits = limits.effective()
        self.staging = staging
        self.allowed_roots = [root.resolve() for root in allowed_roots]
        self.limits = limits
        self.members: list[dict[str, Any]] = []
        self.errors: list[dict[str, Any]] = []
        self.redactions = 0
        self.total_bytes = 0
        self._paths: set[str] = set()
        self.required_error = False

    def add_allowed_root(self, root: Path) -> None:
        resolved = root.resolve()
        if resolved not in self.allowed_roots:
            self.allowed_roots.append(resolved)

    def error(
        self,
        code: str,
        source_name: str,
        message: str,
        *,
        required: bool,
    ) -> None:
        self.required_error = self.required_error or required
        if len(self.errors) >= self.limits.max_members:
            self.errors[-1] = {
                "code": "error-limit",
                "message": "additional capture errors were suppressed",
                "required": self.required_error,
                "sourceName": "manifest",
            }
            return
        self.errors.append(
            {
                "code": code,
                "message": message,
                "required": required,
                "sourceName": source_name,
            }
        )

    def _resolve_source(self, source: Path, *, required: bool) -> Path | None:
        source_name = _source_name(source)
        try:
            metadata = source.lstat()
        except FileNotFoundError:
            self.error(
                "missing", source_name, "source file is missing", required=required
            )
            return None
        except OSError:
            self.error(
                "unreadable",
                source_name,
                "source metadata is unavailable",
                required=required,
            )
            return None
        if stat.S_ISLNK(metadata.st_mode):
            self.error(
                "unsafe-type",
                source_name,
                "symbolic links are not collected",
                required=required,
            )
            return None
        if not stat.S_ISREG(metadata.st_mode):
            self.error(
                "unsafe-type",
                source_name,
                "only regular files are collected",
                required=required,
            )
            return None
        try:
            resolved = source.resolve(strict=True)
        except OSError:
            self.error(
                "unreadable",
                source_name,
                "source cannot be resolved",
                required=required,
            )
            return None
        if _forbidden_source(resolved):
            self.error(
                "forbidden",
                source_name,
                "source is excluded by the incident allowlist",
                required=required,
            )
            return None
        if not any(_is_relative_to(resolved, root) for root in self.allowed_roots):
            self.error(
                "outside-root",
                source_name,
                "source is outside the allowed roots",
                required=required,
            )
            return None
        return resolved

    def add_bytes(
        self,
        data: bytes,
        member_path: str,
        *,
        kind: str,
        source_name: str,
        required: bool,
        node_id: int | None = None,
        build_id: str | None = None,
        binary: str | None = None,
        clock_domain: str | None = None,
        source_name_override: str | None = None,
        truncated: bool = False,
        redact: bool | None = None,
    ) -> dict[str, Any] | None:
        if kind not in ALLOWED_KINDS:
            raise IncidentError(f"unsupported incident member kind: {kind}")
        try:
            normalized = _member_path(member_path, self.limits.max_path_bytes)
        except IncidentError as exc:
            self.error("unsafe-path", source_name, str(exc), required=required)
            return None
        if normalized in self._paths:
            self.error(
                "duplicate",
                source_name,
                "duplicate incident member path",
                required=required,
            )
            return None
        if len(self.members) >= self.limits.max_members:
            self.error(
                "member-limit",
                source_name,
                "incident member limit reached",
                required=required,
            )
            return None

        should_redact = kind in TEXT_KINDS if redact is None else redact
        if should_redact:
            data, count = redact_text(data)
            self.redactions += count

        available = min(
            self.limits.max_file_bytes,
            self.limits.max_total_bytes - self.total_bytes,
        )
        if available < 0:
            available = 0
        if len(data) > available:
            if not should_redact or available == 0:
                self.error(
                    "size-limit",
                    source_name,
                    "source exceeds incident byte limits",
                    required=required,
                )
                return None
            data = data[-available:]
            truncated = True

        destination = self.staging / normalized
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            with destination.open("xb") as output:
                output.write(data)
        except OSError as exc:
            raise IncidentError(
                f"failed to write incident member {normalized}: {exc.strerror}"
            ) from exc

        digest = hashlib.sha256(data).hexdigest()
        member: dict[str, Any] = {
            "path": normalized,
            "kind": kind,
            "size": len(data),
            "sha256": digest,
            "required": required,
            "truncated": truncated,
            "sourceName": source_name_override or source_name,
        }
        if node_id is not None:
            member["nodeId"] = int(node_id)
        if build_id is not None:
            member["buildId"] = build_id
        if binary is not None:
            member["binary"] = binary
        if clock_domain is not None:
            member["clockDomain"] = clock_domain
        self._paths.add(normalized)
        self.total_bytes += len(data)
        self.members.append(member)
        return member

    def add_file(
        self,
        source: Path,
        member_path: str,
        *,
        kind: str,
        required: bool,
        node_id: int | None = None,
        start_offset: int = 0,
        end_offset: int | None = None,
        allow_truncate: bool | None = None,
        build_id: str | None = None,
        detect_build_id: bool = False,
        binary: str | None = None,
        clock_domain: str | None = None,
        source_name_override: str | None = None,
    ) -> dict[str, Any] | None:
        resolved = self._resolve_source(source, required=required)
        if resolved is None:
            return None
        source_name = _source_name(resolved)
        truncate_ok = kind in TEXT_KINDS if allow_truncate is None else allow_truncate
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(resolved, flags)
        except OSError:
            self.error(
                "unreadable", source_name, "source cannot be opened", required=required
            )
            return None
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                self.error(
                    "unsafe-type",
                    source_name,
                    "only regular files are collected",
                    required=required,
                )
                return None
            file_size = before.st_size
            requested_start = max(0, int(start_offset))
            requested_end = file_size if end_offset is None else max(0, int(end_offset))
            truncated = requested_start > file_size or requested_end > file_size
            end = min(file_size, requested_end)
            start = min(requested_start, end)
            wanted = end - start
            remaining = max(0, self.limits.max_total_bytes - self.total_bytes)
            permitted = min(self.limits.max_file_bytes, remaining)
            if wanted > permitted:
                if not truncate_ok or permitted == 0:
                    self.error(
                        "size-limit",
                        source_name,
                        "source exceeds incident byte limits",
                        required=required,
                    )
                    return None
                start = end - permitted
                wanted = permitted
                truncated = True
            os.lseek(descriptor, start, os.SEEK_SET)
            chunks: list[bytes] = []
            unread = wanted
            while unread:
                chunk = os.read(descriptor, min(unread, 1024 * 1024))
                if not chunk:
                    truncated = True
                    break
                chunks.append(chunk)
                unread -= len(chunk)
            after = os.fstat(descriptor)
        except OSError:
            self.error(
                "unreadable",
                source_name,
                "source changed or became unreadable",
                required=required,
            )
            return None
        finally:
            os.close(descriptor)

        append_only_growth = (
            kind in {"serial-log", "qemu-log", "coverage-log"}
            and before.st_dev == after.st_dev
            and before.st_ino == after.st_ino
            and after.st_size > before.st_size
            and end <= before.st_size
        )
        if append_only_growth:
            # The copied bytes end at the pre-read snapshot boundary.  Preserve
            # that bounded snapshot, but report that newer bytes were omitted.
            truncated = True
        elif (
            before.st_dev != after.st_dev
            or before.st_ino != after.st_ino
            or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
        ):
            self.error(
                "changed",
                source_name,
                "source changed during capture",
                required=required,
            )
            return None
        member = self.add_bytes(
            b"".join(chunks),
            member_path,
            kind=kind,
            source_name=source_name,
            required=required,
            node_id=node_id,
            build_id=build_id,
            binary=binary,
            clock_domain=clock_domain,
            source_name_override=source_name_override,
            truncated=truncated,
        )
        if member is not None and detect_build_id:
            detected_build_id = _read_build_id(self.staging / member["path"])
            if detected_build_id is not None:
                member["buildId"] = detected_build_id
        return member


def _absolute(path: Path, repo_root: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _node_log_paths(
    spec: dict[str, Any],
    *,
    tcg_level: str | None,
    repo_root: Path,
) -> list[tuple[str, Path]]:
    normalized = node_setup.normalize_node_spec(spec)
    paths: list[tuple[str, Path]] = [
        ("serial-log", _absolute(node_setup.serial_log_path(normalized), repo_root))
    ]
    qemu = _absolute(
        node_setup.qemu_log_path(normalized, tcg_level=tcg_level), repo_root
    )
    if tcg_level is not None and "%d" in qemu.name:
        matches = sorted(qemu.parent.glob(qemu.name.replace("%d", "*")))
        paths.extend(("qemu-log", match) for match in matches)
    else:
        paths.append(("qemu-log", qemu))
    return paths


def snapshot_node_logs(
    node_specs: Iterable[dict[str, Any]],
    *,
    tcg_level: str | None,
    repo_root: Path = ROOT,
) -> dict[str, SourceSnapshot]:
    snapshots: dict[str, SourceSnapshot] = {}
    for spec in node_specs:
        for _kind, path in _node_log_paths(
            spec, tcg_level=tcg_level, repo_root=repo_root
        ):
            try:
                metadata = path.lstat()
            except OSError:
                continue
            if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
                continue
            snapshots[str(path.resolve())] = SourceSnapshot(
                device=metadata.st_dev,
                inode=metadata.st_ino,
                size=metadata.st_size,
                mtime_ns=metadata.st_mtime_ns,
            )
    return snapshots


def _snapshot_start(path: Path, snapshots: dict[str, SourceSnapshot] | None) -> int:
    if not snapshots:
        return 0
    try:
        resolved = str(path.resolve(strict=True))
        metadata = path.lstat()
    except OSError:
        return 0
    snapshot = snapshots.get(resolved)
    if snapshot is None:
        return 0
    if (
        metadata.st_dev == snapshot.device
        and metadata.st_ino == snapshot.inode
        and metadata.st_size >= snapshot.size
    ):
        if (
            metadata.st_size == snapshot.size
            and metadata.st_mtime_ns != snapshot.mtime_ns
        ):
            # A same-inode, same-size rewrite has no append-only suffix.
            return 0
        return snapshot.size
    return 0


def topology_manifest(node_specs: Iterable[dict[str, Any]]) -> dict[str, Any]:
    nodes: list[dict[str, Any]] = []
    for raw_spec in node_specs:
        spec = node_setup.normalize_node_spec(raw_spec)
        nics = []
        for nic in spec.get("nics", []):
            nics.append(
                {
                    "driver": str(nic.get("driver", "unmanaged")),
                    "mac": str(nic.get("mac", "")),
                    "model": str(nic.get("model", "virtio-net-pci")),
                    "name": str(nic.get("name", "")),
                    "queues": int(nic.get("queues", 1)),
                    "zoneId": int(nic.get("zone_id", 0)),
                }
            )
        nodes.append(
            {
                "debug": bool(spec.get("debug", False)),
                "hostname": node_setup.node_hostname(spec),
                "id": node_setup.node_id(spec),
                "nics": sorted(nics, key=lambda nic: (nic["zoneId"], nic["name"])),
            }
        )
    return {"nodes": sorted(nodes, key=lambda node: node["id"])}


def _coverage_source_path(value: str, manifest_path: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return manifest_path.parent / path


def _coverage_member_component(name: str, fallback: str) -> str:
    return _safe_component(name, fallback=fallback)


def _read_stable_regular_bytes(
    builder: IncidentBuilder,
    source: Path,
    *,
    required: bool,
    max_bytes: int,
) -> tuple[Path, bytes] | None:
    """Read one bounded regular source through a stable open descriptor."""
    resolved = builder._resolve_source(source, required=required)
    if resolved is None:
        return None
    source_name = _source_name(resolved)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(resolved, flags)
    except OSError:
        builder.error(
            "unreadable", source_name, "source cannot be opened", required=required
        )
        return None

    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            builder.error(
                "unsafe-type",
                source_name,
                "only regular files are collected",
                required=required,
            )
            return None
        if before.st_size > max_bytes:
            builder.error(
                "size-limit",
                source_name,
                "source exceeds the metadata byte limit",
                required=required,
            )
            return None

        chunks: list[bytes] = []
        unread = max_bytes + 1
        while unread:
            chunk = os.read(descriptor, min(unread, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            unread -= len(chunk)
        after = os.fstat(descriptor)
    except OSError:
        builder.error(
            "unreadable",
            source_name,
            "source changed or became unreadable",
            required=required,
        )
        return None
    finally:
        os.close(descriptor)

    data = b"".join(chunks)
    if len(data) > max_bytes:
        builder.error(
            "size-limit",
            source_name,
            "source exceeds the metadata byte limit",
            required=required,
        )
        return None
    if (
        before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
        or before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or len(data) != before.st_size
    ):
        builder.error(
            "changed", source_name, "source changed during capture", required=required
        )
        return None
    return resolved, data


def _consume_coverage_manifest(
    builder: IncidentBuilder,
    manifest_path: Path,
    *,
    index: int,
    repo_root: Path,
) -> None:
    stable_source = _read_stable_regular_bytes(
        builder,
        manifest_path,
        required=True,
        max_bytes=min(builder.limits.max_file_bytes, DEFAULT_MAX_METADATA_BYTES),
    )
    if stable_source is None:
        return
    resolved, data = stable_source
    try:
        document = json.loads(data.decode("utf-8"))
    except (UnicodeError, ValueError, RecursionError):
        builder.error(
            "invalid-coverage-manifest",
            _source_name(resolved),
            "coverage manifest is not bounded valid JSON",
            required=True,
        )
        return
    if not isinstance(document, dict):
        builder.error(
            "invalid-coverage-manifest",
            _source_name(resolved),
            "coverage manifest root must be an object",
            required=True,
        )
        return

    prefix = f"evidence/coverage/{index:03d}"
    sanitized = _sanitize_json(document, repo_root=repo_root)
    builder.add_bytes(
        _manifest_bytes(sanitized),
        f"{prefix}/run-all-iteration.json",
        kind="coverage-manifest",
        source_name=_source_name(resolved),
        required=True,
        redact=False,
    )

    artifacts = document.get("artifacts")
    if not isinstance(artifacts, dict):
        builder.error(
            "invalid-coverage-manifest",
            _source_name(resolved),
            "coverage artifacts must be an object",
            required=True,
        )
        artifacts = {}
    else:
        if len(artifacts) > builder.limits.max_members:
            builder.error(
                "member-limit",
                _source_name(resolved),
                "coverage artifact inventory exceeds the member limit",
                required=True,
            )
            artifacts = {}
        for artifact_name, artifact_value in sorted(
            artifacts.items(), key=lambda item: str(item[0])
        ):
            if artifact_value is None:
                continue
            if not isinstance(artifact_value, str):
                builder.error(
                    "invalid-coverage-artifact",
                    str(artifact_name),
                    "coverage artifact path must be a string or null",
                    required=True,
                )
                continue
            source = _coverage_source_path(artifact_value, resolved)
            suffix = source.suffix.lower()
            if suffix not in {".info", ".json", ".log", ".profdata", ".profraw"}:
                builder.error(
                    "forbidden",
                    _source_name(source),
                    "coverage artifact type is not allowlisted",
                    required=False,
                )
                continue
            component = _coverage_member_component(str(artifact_name), "artifact")
            kind = "coverage-log" if suffix == ".log" else "coverage-artifact"
            builder.add_file(
                source,
                f"{prefix}/artifacts/{component}-{_safe_component(source.name)}",
                kind=kind,
                required=True,
            )

    ranges = document.get("external_log_ranges")
    if not isinstance(ranges, list):
        builder.error(
            "invalid-coverage-manifest",
            _source_name(resolved),
            "coverage external_log_ranges must be a list",
            required=True,
        )
        ranges = []
    else:
        if len(ranges) > builder.limits.max_members:
            builder.error(
                "member-limit",
                _source_name(resolved),
                "coverage log-range inventory exceeds the member limit",
                required=True,
            )
            ranges = []
        for range_index, item in enumerate(ranges):
            if not isinstance(item, dict) or not isinstance(item.get("path"), str):
                builder.error(
                    "invalid-log-range",
                    f"range-{range_index}",
                    "coverage external log range is malformed",
                    required=True,
                )
                continue
            start_value = item.get("start_offset", 0)
            end_value = item.get("end_offset")
            if (
                not isinstance(start_value, int)
                or isinstance(start_value, bool)
                or not isinstance(end_value, int)
                or isinstance(end_value, bool)
            ):
                builder.error(
                    "invalid-log-range",
                    f"range-{range_index}",
                    "coverage external log offsets are malformed",
                    required=True,
                )
                continue
            start = start_value
            end = end_value
            if start < 0 or end < start:
                builder.error(
                    "invalid-log-range",
                    f"range-{range_index}",
                    "coverage external log offsets are out of order",
                    required=True,
                )
                continue
            source = _coverage_source_path(item["path"], resolved)
            node_match = re.search(r"(?:serial|qemu)-vm(\d+)", source.name)
            node_id = int(node_match.group(1)) if node_match else None
            if node_id is None:
                clock_domain = None
            elif source.name.startswith("qemu-"):
                clock_domain = f"node-{node_id}-qemu-process"
            else:
                clock_domain = f"node-{node_id}-boot-monotonic"
            builder.add_file(
                source,
                f"{prefix}/external/{range_index:03d}-{_safe_component(source.name)}",
                kind="coverage-log",
                required=True,
                node_id=node_id,
                start_offset=start,
                end_offset=end,
                clock_domain=clock_domain,
            )


def _live_target(value: str) -> tuple[str, str, str]:
    host, separator, remote = value.partition(":")
    if not separator or HOST_RE.fullmatch(host) is None:
        raise IncidentError("live coredump target must be HOST:/tmp/NAME_coredump.bin")
    remote_path = PurePosixPath(remote)
    if (
        remote_path.parent != PurePosixPath("/tmp")
        or LIVE_COREDUMP_NAME_RE.fullmatch(remote_path.name) is None
        or len(remote_path.name.encode("utf-8")) > 255
    ):
        raise IncidentError(
            "live coredump target must name one /tmp/*_coredump.bin file"
        )
    return host, remote_path.as_posix(), remote_path.name


def _limit_live_fetch(max_bytes: int) -> None:
    resource.setrlimit(resource.RLIMIT_FSIZE, (max_bytes, max_bytes))


def _fetch_live_coredumps(
    builder: IncidentBuilder,
    targets: Iterable[str],
    *,
    repo_root: Path,
    temporary_root: Path,
    node_ids: dict[str, int],
) -> None:
    helper = repo_root / "scripts" / "remote" / "wos_sftp_get.sh"
    unique_targets = sorted(set(targets))
    if len(unique_targets) > builder.limits.max_members:
        builder.error(
            "member-limit",
            "live-coredump",
            "live coredump target count exceeds the member limit",
            required=True,
        )
        return
    for index, target in enumerate(unique_targets):
        try:
            host, remote, basename = _live_target(target)
        except IncidentError as exc:
            builder.error(
                "invalid-live-target", "live-coredump", str(exc), required=True
            )
            continue
        destination = temporary_root / f"fetch-{index:03d}.bin"
        try:
            result = subprocess.run(
                [str(helper), host, remote, str(destination)],
                cwd=repo_root,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=LIVE_FETCH_TIMEOUT_SECONDS,
                preexec_fn=lambda: _limit_live_fetch(builder.limits.max_file_bytes),
            )
        except (OSError, subprocess.TimeoutExpired):
            builder.error(
                "live-fetch",
                basename,
                "live coredump fetch failed or timed out",
                required=True,
            )
            continue
        if result.returncode != 0:
            builder.error(
                "live-fetch", basename, "live coredump fetch failed", required=True
            )
            continue
        builder.add_file(
            destination,
            f"live/{_safe_component(host)}/coredumps/{_safe_component(basename)}",
            kind="coredump",
            required=True,
            node_id=node_ids.get(host),
            allow_truncate=False,
            source_name_override=basename,
        )


def _clock_manifest(members: list[dict[str, Any]]) -> dict[str, Any]:
    domains: dict[str, dict[str, Any]] = {}
    for member in members:
        clock_id = member.get("clockDomain")
        node_id = member.get("nodeId")
        if not isinstance(clock_id, str) or node_id is None:
            continue
        source = (
            "qemu-process-log"
            if clock_id.endswith("-qemu-process")
            else "guest-boot-monotonic"
        )
        domains[clock_id] = {
            "comparableAcrossNodes": False,
            "id": clock_id,
            "nodeId": int(node_id),
            "source": source,
            "synchronized": False,
        }
    ordered = [domains[key] for key in sorted(domains)]
    node_ids = {domain["nodeId"] for domain in ordered}
    if not ordered:
        quality = "unavailable"
    elif len(node_ids) == 1:
        quality = "single-node-unsynchronized"
    else:
        quality = "partial"
    return {"quality": quality, "domains": ordered}


def _write_deterministic_ustar(staging: Path, destination: io.BufferedWriter) -> None:
    with tarfile.open(
        fileobj=destination, mode="w", format=tarfile.USTAR_FORMAT
    ) as archive:
        paths: list[Path] = []
        for path in sorted(staging.rglob("*")):
            metadata = path.lstat()
            if stat.S_ISDIR(metadata.st_mode):
                continue
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise IncidentError("incident staging contains a non-regular member")
            paths.append(path)
        for path in paths:
            relative = path.relative_to(staging).as_posix()
            data = path.read_bytes()
            info = tarfile.TarInfo(relative)
            info.size = len(data)
            info.mode = 0o644
            info.mtime = 0
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            archive.addfile(info, fileobj=io.BytesIO(data))


def _rename_noreplace(source: Path, destination: Path) -> None:
    """Atomically publish without overwriting an existing path (Linux)."""
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = libc.renameat2
    except (AttributeError, OSError) as exc:
        raise IncidentError("atomic no-overwrite rename is unavailable") from exc
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        -100,
        os.fsencode(source),
        -100,
        os.fsencode(destination),
        1,
    )
    if result == 0:
        return
    error = ctypes.get_errno()
    if error == errno.EEXIST:
        raise FileExistsError(destination)
    raise OSError(error, os.strerror(error), destination)


def _publish(staging: Path, output: Path, *, archive: bool) -> None:
    output_parent = output.parent
    if os.path.lexists(output):
        raise FileExistsError(output)
    if not archive:
        _rename_noreplace(staging, output)
        return
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output.name}.tmp-",
        suffix=".tar",
        dir=output_parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as destination:
            descriptor = -1
            _write_deterministic_ustar(staging, destination)
        _rename_noreplace(temporary, output)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary.exists():
            temporary.unlink()


def capture_incident(
    *,
    output: Path,
    archive: bool,
    kind: str,
    config: dict[str, Any],
    config_path: Path,
    node_specs: Iterable[dict[str, Any]],
    build_dir: Path,
    tcg_level: str | None = None,
    profile: str | None = None,
    coverage_manifests: Iterable[Path] = (),
    live_coredumps: Iterable[str] = (),
    snapshots: dict[str, SourceSnapshot] | None = None,
    run_complete: bool = True,
    run_error: str | None = None,
    repo_root: Path = ROOT,
    limits: CaptureLimits = CaptureLimits(),
    created_utc: str | None = None,
) -> dict[str, Any]:
    """Capture an incident and atomically publish it at ``output``."""
    if kind not in {"ktest", "cluster"}:
        raise IncidentError("incident source kind must be 'ktest' or 'cluster'")
    limits = limits.effective()
    repo_root = repo_root.resolve()
    output = _absolute(output, repo_root)
    output.parent.mkdir(parents=True, exist_ok=True)
    if os.path.lexists(output):
        raise FileExistsError(output)

    node_specs = [node_setup.normalize_node_spec(spec) for spec in node_specs]
    if (
        len(node_specs) > limits.max_members
        or sum(len(spec.get("nics", [])) for spec in node_specs) > limits.max_members
    ):
        raise IncidentError("topology exceeds the incident member limit")
    coverage_paths = sorted(
        (_absolute(Path(path), repo_root) for path in coverage_manifests),
        key=lambda path: str(path),
    )
    revision, dirty = _git_metadata(repo_root)
    allowed_roots = [repo_root]
    allowed_roots.extend(path.parent for path in coverage_paths)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output.name}.staging-", dir=output.parent)
    )
    published = False
    try:
        builder = IncidentBuilder(staging, allowed_roots=allowed_roots, limits=limits)
        config_member = "metadata/config.json"
        builder.add_bytes(
            _manifest_bytes(_sanitize_json(config, repo_root=repo_root)),
            config_member,
            kind="config",
            source_name=_source_name(config_path),
            required=True,
            redact=False,
        )

        # Preserve symbol-bearing executables before large text logs consume the
        # total-byte budget.  This list is fixed rather than recursive.
        build_dir = _absolute(build_dir, repo_root)
        binary_members: dict[str, str] = {}
        for logical_name, relative in BINARY_CANDIDATES:
            source = build_dir / relative
            try:
                source.lstat()
            except FileNotFoundError:
                if logical_name == "kernel":
                    builder.error(
                        "missing",
                        source.name,
                        "matching kernel binary is missing",
                        required=True,
                    )
                continue
            except OSError:
                builder.error(
                    "unreadable",
                    source.name,
                    "binary metadata is unavailable",
                    required=logical_name == "kernel",
                )
                continue
            destination = (
                f"binaries/{_safe_component(logical_name)}/"
                f"{_safe_component(source.name)}"
            )
            member = builder.add_file(
                source,
                destination,
                kind="binary",
                required=logical_name == "kernel",
                detect_build_id=True,
                allow_truncate=False,
            )
            if member is not None:
                binary_members[logical_name] = member["path"]

        for spec in sorted(node_specs, key=node_setup.node_id):
            node_id = node_setup.node_id(spec)
            log_paths = _node_log_paths(
                spec,
                tcg_level=tcg_level,
                repo_root=repo_root,
            )
            for log_index, (kind_name, log_path) in enumerate(log_paths):
                if kind_name == "serial-log":
                    destination = f"nodes/node-{node_id}/serial.log"
                    clock_domain = f"node-{node_id}-boot-monotonic"
                elif len(log_paths) == 2:
                    destination = f"nodes/node-{node_id}/qemu.log"
                    clock_domain = f"node-{node_id}-qemu-process"
                else:
                    destination = (
                        f"nodes/node-{node_id}/qemu/"
                        f"{log_index:03d}-{_safe_component(log_path.name)}"
                    )
                    clock_domain = f"node-{node_id}-qemu-process"
                builder.add_file(
                    log_path,
                    destination,
                    kind=kind_name,
                    required=kind_name == "serial-log",
                    node_id=node_id,
                    start_offset=_snapshot_start(log_path, snapshots),
                    clock_domain=clock_domain,
                )

        for index, manifest_path in enumerate(coverage_paths):
            _consume_coverage_manifest(
                builder, manifest_path, index=index, repo_root=repo_root
            )

        if live_coredumps:
            live_node_ids: dict[str, int] = {}
            for spec in node_specs:
                node_id = node_setup.node_id(spec)
                live_node_ids[node_setup.node_hostname(spec)] = node_id
                live_node_ids[f"wos-{node_id}"] = node_id
                live_node_ids[f"vm{node_id}"] = node_id
            with tempfile.TemporaryDirectory(
                prefix=f".{output.name}.live-", dir=output.parent
            ) as live_directory:
                live_root = Path(live_directory)
                builder.add_allowed_root(live_root)
                _fetch_live_coredumps(
                    builder,
                    live_coredumps,
                    repo_root=repo_root,
                    temporary_root=live_root,
                    node_ids=live_node_ids,
                )

        # Associate coredumps with an allowlisted binary by their conventional
        # <program>_<ticks>_coredump.bin basename.  Unknown programs remain
        # explicit and unassociated rather than guessing a build artifact.
        for member in builder.members:
            if member["kind"] != "coredump":
                continue
            source_name = member["sourceName"]
            for logical_name, binary_path in sorted(binary_members.items()):
                if source_name.startswith(f"{logical_name}_"):
                    member["binary"] = binary_path
                    break

        if not run_complete:
            builder.error(
                "source-run-failed",
                kind,
                run_error or "source run did not complete successfully",
                required=True,
            )

        builder.members.sort(key=lambda member: member["path"])
        builder.errors.sort(
            key=lambda error: (
                error["code"],
                error["sourceName"],
                error["message"],
                error["required"],
            )
        )
        effective_profile = profile or detect_build_profile(build_dir)
        manifest: dict[str, Any] = {
            "format": INCIDENT_FORMAT,
            "version": INCIDENT_VERSION,
            "incidentId": "",
            "createdUtc": created_utc or _created_utc(),
            "source": {
                "kind": kind,
                "revision": revision,
                "dirty": dirty,
                "profile": effective_profile,
                "configMember": config_member,
            },
            "capture": {
                "complete": run_complete and not builder.required_error,
                "errors": builder.errors,
                "redactions": builder.redactions,
                "truncatedMembers": sorted(
                    member["path"] for member in builder.members if member["truncated"]
                ),
            },
            "limits": {
                "maxMembers": limits.max_members,
                "maxFileBytes": limits.max_file_bytes,
                "maxTotalBytes": limits.max_total_bytes,
                "maxPathBytes": limits.max_path_bytes,
                "maxManifestBytes": DEFAULT_MAX_MANIFEST_BYTES,
            },
            "clocks": _clock_manifest(builder.members),
            "topology": topology_manifest(node_specs),
            "members": builder.members,
        }
        identity_payload = dict(manifest)
        identity_payload.pop("incidentId")
        identity_payload.pop("createdUtc")
        manifest["incidentId"] = (
            "sha256:" + hashlib.sha256(_canonical_json(identity_payload)).hexdigest()
        )
        encoded_manifest = _manifest_bytes(manifest)
        if len(encoded_manifest) > DEFAULT_MAX_MANIFEST_BYTES:
            raise IncidentError("incident manifest exceeds the loader-compatible limit")
        (staging / MANIFEST_NAME).write_bytes(encoded_manifest)
        _publish(staging, output, archive=archive)
        published = True
        return manifest
    finally:
        if not published and staging.exists():
            shutil.rmtree(staging)
        elif archive and staging.exists():
            shutil.rmtree(staging)


def capture_safely(**kwargs: Any) -> dict[str, Any] | None:
    """Capture without changing a launcher's existing success/failure result."""
    try:
        manifest = capture_incident(**kwargs)
    except Exception as exc:
        print(f"WARNING: incident capture failed: {exc}", file=sys.stderr)
        return None
    print(
        f"WOS incident captured: {kwargs['output']} ({manifest['incidentId']})",
        file=sys.stderr,
    )
    return manifest


def _load_regular_json(path: Path, *, repo_root: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise IncidentError("config must be a regular file")
    resolved = path.resolve(strict=True)
    if not _is_relative_to(resolved, repo_root.resolve()):
        raise IncidentError("config is outside the repository allowed root")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(resolved, flags)
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_size > DEFAULT_MAX_METADATA_BYTES:
                raise IncidentError("config exceeds its regular-file size limit")
            chunks: list[bytes] = []
            unread = DEFAULT_MAX_METADATA_BYTES + 1
            while unread:
                chunk = os.read(descriptor, unread)
                if not chunk:
                    break
                chunks.append(chunk)
                unread -= len(chunk)
            data = b"".join(chunks)
            after = os.fstat(descriptor)
            if len(data) > DEFAULT_MAX_METADATA_BYTES:
                raise IncidentError("config exceeds its regular-file size limit")
            if (
                before.st_dev != after.st_dev
                or before.st_ino != after.st_ino
                or before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns
            ):
                raise IncidentError("config changed while it was read")
        finally:
            os.close(descriptor)
        value = json.loads(data.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError, RecursionError) as exc:
        raise IncidentError("config is not valid bounded JSON") from exc
    if not isinstance(value, dict):
        raise IncidentError("config root must be an object")
    return value


def _cli_node_specs(kind: str, config: dict[str, Any]) -> list[dict[str, Any]]:
    if kind == "ktest":
        return [node_setup.normalize_node_spec(config)]
    import cluster_setup

    nodes = cluster_setup.collect_unique_nodes(config)
    return [
        cluster_setup.cluster_node_spec(node_id, nodes[node_id], config)
        for node_id in sorted(nodes)
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Capture a bounded WOS incident bundle"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    capture_parser = subparsers.add_parser(
        "capture", help="Capture current WOS run artifacts"
    )
    capture_parser.add_argument("--kind", choices=("ktest", "cluster"), required=True)
    capture_parser.add_argument("--config", help="Source VM/cluster config")
    capture_parser.add_argument("--build-dir", help="Matching CMake build directory")
    capture_parser.add_argument("--profile", help="Build profile override")
    capture_parser.add_argument("--output", required=True, help="New output path")
    capture_parser.add_argument(
        "--archive",
        action="store_true",
        help="Write deterministic USTAR instead of a directory",
    )
    capture_parser.add_argument(
        "--coverage-manifest",
        action="append",
        default=[],
        help="Coverage run-all-iteration.json to import; may be repeated",
    )
    capture_parser.add_argument(
        "--live-coredump",
        action="append",
        default=[],
        metavar="HOST:/tmp/NAME_coredump.bin",
        help="Fetch one explicit live tmpfs coredump over the existing SFTP helper",
    )
    capture_parser.add_argument("--max-members", type=int, default=DEFAULT_MAX_MEMBERS)
    capture_parser.add_argument(
        "--max-file-bytes", type=int, default=DEFAULT_MAX_FILE_BYTES
    )
    capture_parser.add_argument(
        "--max-total-bytes", type=int, default=DEFAULT_MAX_TOTAL_BYTES
    )
    args = parser.parse_args(argv)

    config_default = (
        "configs/node_ktest.json" if args.kind == "ktest" else "configs/cluster.json"
    )
    config_path = _absolute(Path(args.config or config_default), ROOT)
    config = _load_regular_json(config_path, repo_root=ROOT)
    node_specs = _cli_node_specs(args.kind, config)
    if args.build_dir:
        build_dir = Path(args.build_dir)
    elif args.kind == "ktest":
        build_dir = Path(config.get("build", {}).get("dir", "build-ktest"))
    else:
        build_dir = Path("build")
    manifest = capture_incident(
        output=Path(args.output),
        archive=args.archive,
        kind=args.kind,
        config=config,
        config_path=config_path,
        node_specs=node_specs,
        build_dir=build_dir,
        profile=args.profile,
        coverage_manifests=[Path(path) for path in args.coverage_manifest],
        live_coredumps=args.live_coredump,
        repo_root=ROOT,
        limits=CaptureLimits(
            max_members=args.max_members,
            max_file_bytes=args.max_file_bytes,
            max_total_bytes=args.max_total_bytes,
        ),
    )
    print(f"{args.output}: {manifest['incidentId']}")
    return 0 if manifest["capture"]["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
