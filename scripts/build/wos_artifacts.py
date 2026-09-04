#!/usr/bin/env python3
"""Deterministic WOS artifact comparison, provenance, and SBOM tooling.

The tool intentionally has no third-party Python dependencies.  Expensive or
privileged filesystem-image extraction is kept outside this program: callers
can provide read-only extracted trees when comparing qcow2 payloads, while
qemu-img supplies the container metadata layer.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import stat
import struct
import subprocess
import sys
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10 is still used by the host test suite.
    tomllib = None


FORMAT_VERSION = 1
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_ENVIRONMENT = frozenset(
    {
        "CMAKE_BUILD_PARALLEL_LEVEL",
        "CMAKE_BUILD_TYPE",
        "LANG",
        "LC_ALL",
        "SOURCE_DATE_EPOCH",
        "TZ",
        "WOS_BUILD_JOBS",
        "WOS_BUILD_TARGET",
        "WOS_CMAKE_GENERATOR",
        "WOS_MAKE_JOBS",
        "WOS_NINJA_JOBS",
        "WOS_OFFLINE",
        "WOS_SOURCE_LOCK",
        "WOS_SOURCE_MODE",
        "WOS_SOURCE_STORE",
    }
)
SECRET_NAME_RE = re.compile(
    r"(?:^|[_-])(authorization|authorized[_-]?keys?|credential|password|passwd|private[_-]?key|secret|token)(?:$|[_-])",
    re.IGNORECASE,
)
SECRET_OPTION_RE = re.compile(
    r"^--?(?:authorization|credential|password|passwd|private-key|secret|token)(?:=|$)",
    re.IGNORECASE,
)
REDACTED = "<redacted>"
ELF_MAGIC = b"\x7fELF"
ELF_SHF_ALLOC = 0x2
ELF_VOLATILE_ALLOC_SECTIONS = frozenset({".note.gnu.build-id"})


class ArtifactError(RuntimeError):
    """A user-facing validation or comparison failure."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def document_digest(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value))


def write_json(value: Any, output: Path | None) -> None:
    rendered = json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n"
    if output is None:
        sys.stdout.write(rendered)
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp.{os.getpid()}")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)


def load_data(path: Path) -> Any:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ArtifactError(f"cannot read {path}: {exc}") from exc
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ArtifactError(f"cannot parse source data {path}: {exc}") from exc
    if path.suffix.lower() == ".toml":
        if tomllib is None:
            raise ArtifactError(f"cannot parse TOML source data {path}: Python 3.11 or newer is required")
        try:
            return tomllib.loads(text)
        except tomllib.TOMLDecodeError as exc:
            raise ArtifactError(f"cannot parse source data {path}: {exc}") from exc
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ArtifactError(f"cannot parse source data {path}: {exc}") from exc


def safe_relative_path(path: str) -> bool:
    if not path or path.startswith("/") or "\\" in path or "\x00" in path:
        return False
    return all(part not in ("", ".", "..") for part in path.split("/"))


def xattr_digests(path: Path) -> list[dict[str, Any]]:
    try:
        names = os.listxattr(path, follow_symlinks=False)
    except (AttributeError, OSError):
        return []
    result: list[dict[str, Any]] = []
    for name in sorted(names):
        try:
            value = os.getxattr(path, name, follow_symlinks=False)
        except OSError as exc:
            raise ArtifactError(f"cannot read xattr {name!r} on {path}: {exc}") from exc
        result.append({"name": name, "size": len(value), "sha256": sha256_bytes(value)})
    return result


def tree_entries(root: Path, *, include_times: bool = False) -> list[dict[str, Any]]:
    if not root.is_dir():
        raise ArtifactError(f"tree root is not a directory: {root}")
    entries: list[dict[str, Any]] = []
    hardlinks: dict[tuple[int, int], str] = {}
    paths = sorted(root.rglob("*"), key=lambda path: path.relative_to(root).as_posix())
    for path in paths:
        relative = path.relative_to(root).as_posix()
        if not safe_relative_path(relative):
            raise ArtifactError(f"unsafe tree path: {relative!r}")
        metadata = path.lstat()
        entry: dict[str, Any] = {
            "path": relative,
            "mode": stat.S_IMODE(metadata.st_mode),
            "uid": metadata.st_uid,
            "gid": metadata.st_gid,
        }
        if include_times:
            entry["mtimeNs"] = metadata.st_mtime_ns
        if stat.S_ISDIR(metadata.st_mode):
            entry["type"] = "directory"
        elif stat.S_ISLNK(metadata.st_mode):
            entry.update({"type": "symlink", "target": os.readlink(path)})
            entry["xattrs"] = xattr_digests(path)
        elif stat.S_ISREG(metadata.st_mode):
            entry.update({"type": "file", "size": metadata.st_size, "sha256": sha256_file(path)})
            inode = (metadata.st_dev, metadata.st_ino)
            if metadata.st_nlink > 1:
                if inode in hardlinks:
                    entry["hardlinkTo"] = hardlinks[inode]
                else:
                    hardlinks[inode] = relative
            entry["xattrs"] = xattr_digests(path)
        else:
            entry.update({"type": "special", "device": metadata.st_rdev})
        if not entry.get("xattrs"):
            entry.pop("xattrs", None)
        entries.append(entry)
    return entries


def make_tree_manifest(root: Path, *, include_times: bool = False) -> dict[str, Any]:
    entries = tree_entries(root, include_times=include_times)
    body = {
        "format": "wos-tree-manifest",
        "version": FORMAT_VERSION,
        "timestampPolicy": "exact" if include_times else "omitted",
        "entries": entries,
    }
    return {**body, "treeSha256": document_digest(body)}


def compare_bytes(left: Path, right: Path) -> dict[str, Any]:
    if not left.is_file() or not right.is_file():
        raise ArtifactError("byte comparison requires two regular files")
    left_info = {"size": left.stat().st_size, "sha256": sha256_file(left)}
    right_info = {"size": right.stat().st_size, "sha256": sha256_file(right)}
    return {"equal": left_info == right_info, "left": left_info, "right": right_info}


@dataclass(frozen=True)
class ElfSection:
    index: int
    name: str
    section_type: int
    flags: int
    address: int
    offset: int
    size: int
    alignment: int
    entry_size: int
    digest: str


def _bounded_slice(data: bytes, offset: int, size: int, label: str) -> bytes:
    if offset < 0 or size < 0 or offset + size > len(data):
        raise ArtifactError(f"ELF {label} lies outside the file")
    return data[offset : offset + size]


def parse_elf(path: Path) -> tuple[dict[str, Any], list[ElfSection]]:
    data = path.read_bytes()
    if len(data) < 16 or data[:4] != ELF_MAGIC:
        raise ArtifactError(f"not an ELF file: {path}")
    elf_class, data_encoding = data[4], data[5]
    if elf_class not in (1, 2) or data_encoding not in (1, 2):
        raise ArtifactError(f"unsupported ELF class/encoding in {path}")
    endian = "<" if data_encoding == 1 else ">"
    if elf_class == 2:
        header_format = endian + "HHIQQQIHHHHHH"
        section_format = endian + "IIQQQQIIQQ"
    else:
        header_format = endian + "HHIIIIIHHHHHH"
        section_format = endian + "IIIIIIIIII"
    header_size = struct.calcsize(header_format)
    values = struct.unpack(header_format, _bounded_slice(data, 16, header_size, "header"))
    (
        elf_type,
        machine,
        version,
        entry,
        _program_offset,
        section_offset,
        flags,
        _elf_header_size,
        _program_entry_size,
        _program_count,
        section_entry_size,
        section_count,
        string_section_index,
    ) = values
    expected_section_size = struct.calcsize(section_format)
    if section_count == 0 or section_entry_size < expected_section_size:
        raise ArtifactError(f"ELF has no supported section table: {path}")

    raw_sections: list[tuple[int, ...]] = []
    for index in range(section_count):
        offset = section_offset + index * section_entry_size
        raw_sections.append(struct.unpack(section_format, _bounded_slice(data, offset, expected_section_size, "section header")))
    if string_section_index >= len(raw_sections):
        raise ArtifactError(f"ELF string section index is invalid: {path}")
    strings_header = raw_sections[string_section_index]
    strings = _bounded_slice(data, strings_header[4], strings_header[5], "section-name table")

    def section_name(offset: int) -> str:
        if offset >= len(strings):
            raise ArtifactError(f"ELF section-name offset is invalid: {path}")
        end = strings.find(b"\0", offset)
        if end < 0:
            end = len(strings)
        return strings[offset:end].decode("utf-8", errors="surrogateescape")

    sections: list[ElfSection] = []
    for index, raw in enumerate(raw_sections):
        name_offset, section_type, section_flags, address, offset, size, _link, _info, alignment, entry_size = raw
        # SHT_NOBITS has a logical size but no corresponding file bytes.
        content = b"" if section_type == 8 else _bounded_slice(data, offset, size, f"section {index}")
        sections.append(
            ElfSection(
                index=index,
                name=section_name(name_offset),
                section_type=section_type,
                flags=section_flags,
                address=address,
                offset=offset,
                size=size,
                alignment=alignment,
                entry_size=entry_size,
                digest=sha256_bytes(content),
            )
        )
    header = {
        "class": 64 if elf_class == 2 else 32,
        "encoding": "little" if data_encoding == 1 else "big",
        "type": elf_type,
        "machine": machine,
        "version": version,
        "entry": entry,
        "flags": flags,
    }
    return header, sections


def elf_semantic_manifest(path: Path) -> dict[str, Any]:
    header, sections = parse_elf(path)
    alloc_sections = [
        {
            "index": section.index,
            "name": section.name,
            "type": section.section_type,
            "flags": section.flags,
            "address": section.address,
            "size": section.size,
            "alignment": section.alignment,
            "entrySize": section.entry_size,
            "sha256": section.digest,
        }
        for section in sections
        if section.flags & ELF_SHF_ALLOC and section.name not in ELF_VOLATILE_ALLOC_SECTIONS
    ]
    body = {"header": header, "allocatedSections": alloc_sections}
    return {
        **body,
        "semanticSha256": document_digest(body),
        "excludedSections": sorted(
            section.name
            for section in sections
            if not section.flags & ELF_SHF_ALLOC or section.name in ELF_VOLATILE_ALLOC_SECTIONS
        ),
    }


def compare_elf(left: Path, right: Path) -> dict[str, Any]:
    byte_report = compare_bytes(left, right)
    left_manifest = elf_semantic_manifest(left)
    right_manifest = elf_semantic_manifest(right)
    left_digest = left_manifest["semanticSha256"]
    right_digest = right_manifest["semanticSha256"]
    return {
        "equal": byte_report["equal"],
        "semanticEqual": left_digest == right_digest,
        "bytes": byte_report,
        "leftSemanticSha256": left_digest,
        "rightSemanticSha256": right_digest,
        "excludedDeltaClasses": ["build-id", "debug/non-allocated-sections", "section-table-layout"],
    }


def compare_trees(left: Path, right: Path, *, include_times: bool = False) -> dict[str, Any]:
    left_manifest = make_tree_manifest(left, include_times=include_times)
    right_manifest = make_tree_manifest(right, include_times=include_times)
    left_by_path = {entry["path"]: entry for entry in left_manifest["entries"]}
    right_by_path = {entry["path"]: entry for entry in right_manifest["entries"]}
    paths = sorted(set(left_by_path) | set(right_by_path))
    differences = [
        {"path": path, "left": left_by_path.get(path), "right": right_by_path.get(path)}
        for path in paths
        if left_by_path.get(path) != right_by_path.get(path)
    ]
    return {
        "equal": not differences,
        "leftTreeSha256": left_manifest["treeSha256"],
        "rightTreeSha256": right_manifest["treeSha256"],
        "differences": differences,
    }


def _flatten_json(value: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key in sorted(value):
            child = f"{prefix}.{key}" if prefix else str(key)
            result.update(_flatten_json(value[key], child))
        return result
    if isinstance(value, list):
        result = {}
        for index, item in enumerate(value):
            child = f"{prefix}[{index}]"
            result.update(_flatten_json(item, child))
        return result
    return {prefix: value}


def qemu_image_info(path: Path, qemu_img: str = "qemu-img") -> dict[str, Any]:
    try:
        result = subprocess.run(
            [qemu_img, "info", "--output=json", str(path)],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ArtifactError(f"cannot inspect qcow image {path}: {exc}") from exc
    if result.returncode != 0:
        raise ArtifactError(result.stderr.strip() or f"qemu-img info failed for {path}")
    try:
        info = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ArtifactError(f"qemu-img returned invalid JSON for {path}: {exc}") from exc
    if not isinstance(info, dict):
        raise ArtifactError("qemu-img info JSON must be an object")
    # Filenames identify observation locations, not container semantics. qemu
    # may report them at the top level and again for protocol child nodes.
    def remove_filenames(value: Any) -> None:
        if isinstance(value, dict):
            value.pop("filename", None)
            for child in value.values():
                remove_filenames(child)
        elif isinstance(value, list):
            for child in value:
                remove_filenames(child)

    remove_filenames(info)
    return info


def classify_qcow_metadata(
    left_info: dict[str, Any], right_info: dict[str, Any], expected_fields: set[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    left_flat = _flatten_json(left_info)
    right_flat = _flatten_json(right_info)
    expected: list[dict[str, Any]] = []
    unexplained: list[dict[str, Any]] = []
    for field in sorted(set(left_flat) | set(right_flat)):
        left_value, right_value = left_flat.get(field), right_flat.get(field)
        if left_value == right_value:
            continue
        row = {"field": field, "left": left_value, "right": right_value}
        if field in expected_fields:
            row["classification"] = "expected-container-metadata"
            expected.append(row)
        else:
            row["classification"] = "unexplained"
            unexplained.append(row)
    return expected, unexplained


def compare_qcow(
    left: Path,
    right: Path,
    *,
    expected_fields: set[str],
    expect_allocation_delta: bool,
    left_tree: Path | None,
    right_tree: Path | None,
    qemu_img: str = "qemu-img",
) -> dict[str, Any]:
    byte_report = compare_bytes(left, right)
    left_info, right_info = qemu_image_info(left, qemu_img), qemu_image_info(right, qemu_img)
    expected, unexplained = classify_qcow_metadata(left_info, right_info, expected_fields)
    filesystem = None
    if (left_tree is None) != (right_tree is None):
        raise ArtifactError("qcow filesystem comparison requires both --left-tree and --right-tree")
    if left_tree is not None and right_tree is not None:
        filesystem = compare_trees(left_tree, right_tree)
    raw_classification = "equal"
    if not byte_report["equal"]:
        raw_classification = "expected-qcow-allocation-metadata" if expect_allocation_delta else "unexplained"
    semantic_equal: bool | None = None
    if filesystem is not None:
        semantic_equal = (
            filesystem["equal"]
            and not unexplained
            and raw_classification != "unexplained"
        )
    return {
        "equal": byte_report["equal"],
        "semanticEqual": semantic_equal,
        "complete": filesystem is not None,
        "bytes": byte_report,
        "container": {
            "left": left_info,
            "right": right_info,
            "expectedDifferences": expected,
            "unexplainedDifferences": unexplained,
            "rawDifferenceClassification": raw_classification,
        },
        "filesystem": filesystem,
    }


def sanitize_url(value: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(value)
    except ValueError:
        return REDACTED if "@" in value else value
    if parsed.scheme not in ("http", "https", "ssh", "git", "file"):
        return value
    hostname = parsed.hostname or ""
    try:
        port = parsed.port
    except ValueError:
        return REDACTED if "@" in value else value
    if port is not None:
        hostname += f":{port}"
    if parsed.username is not None or parsed.password is not None:
        hostname = f"{REDACTED}@{hostname}"
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    safe_query = urllib.parse.urlencode(
        [(key, REDACTED if SECRET_NAME_RE.search(key) else item) for key, item in query]
    )
    return urllib.parse.urlunsplit((parsed.scheme, hostname, parsed.path, safe_query, parsed.fragment))


def redact_argument(argument: str, roots: list[tuple[str, Path]]) -> str:
    if SECRET_OPTION_RE.match(argument):
        if "=" in argument:
            return argument.split("=", 1)[0] + f"={REDACTED}"
        return argument
    is_url = re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", argument) is not None
    if "=" in argument and not is_url:
        key, value = argument.split("=", 1)
        if SECRET_NAME_RE.search(key):
            return f"{key}={REDACTED}"
        argument = f"{key}={sanitize_url(value)}"
    else:
        argument = sanitize_url(argument)
    for label, root in sorted(roots, key=lambda item: len(str(item[1])), reverse=True):
        root_text = str(root)
        if argument == root_text:
            argument = f"<{label}>"
        elif argument.startswith(root_text + os.sep):
            argument = f"<{label}>/{argument[len(root_text) + 1:]}"
    return argument


def redact_command(command: list[str], roots: list[tuple[str, Path]]) -> list[str]:
    result: list[str] = []
    redact_next = False
    for argument in command:
        if redact_next:
            result.append(REDACTED)
            redact_next = False
            continue
        sanitized = redact_argument(str(argument), roots)
        result.append(sanitized)
        if SECRET_OPTION_RE.match(str(argument)) and "=" not in str(argument):
            redact_next = True
    return result


def parse_assignments(values: Iterable[str], label: str) -> list[tuple[str, Path]]:
    result: list[tuple[str, Path]] = []
    names: set[str] = set()
    for raw in values:
        name, separator, path = raw.partition("=")
        if not separator or not name or not path or name in names:
            raise ArtifactError(f"{label} must use unique NAME=PATH values: {raw!r}")
        if SECRET_NAME_RE.search(name):
            raise ArtifactError(f"secret-bearing {label} name is not allowed: {name!r}")
        names.add(name)
        result.append((name, Path(path).resolve(strict=False)))
    return result


def logical_path(path: Path, roots: list[tuple[str, Path]]) -> str:
    resolved = path.resolve(strict=False)
    for label, root in sorted(roots, key=lambda item: len(str(item[1])), reverse=True):
        try:
            relative = resolved.relative_to(root)
        except ValueError:
            continue
        return f"<{label}>" if relative == Path(".") else f"<{label}>/{relative.as_posix()}"
    return f"<external>/{path.name}"


def artifact_subject(name: str, path: Path, roots: list[tuple[str, Path]]) -> dict[str, Any]:
    if path.is_file():
        return {
            "name": name,
            "path": logical_path(path, roots),
            "type": "file",
            "size": path.stat().st_size,
            "sha256": sha256_file(path),
        }
    if path.is_dir():
        manifest = make_tree_manifest(path)
        return {
            "name": name,
            "path": logical_path(path, roots),
            "type": "tree",
            "entries": len(manifest["entries"]),
            "treeSha256": manifest["treeSha256"],
        }
    raise ArtifactError(f"artifact does not exist or is unsupported: {path}")


def tool_subject(name: str, path: Path, roots: list[tuple[str, Path]]) -> dict[str, Any]:
    if not path.is_file():
        raise ArtifactError(f"tool is not a regular file: {path}")
    version = ""
    try:
        result = subprocess.run([str(path), "--version"], check=False, capture_output=True, text=True, timeout=5)
        lines = (result.stdout or result.stderr).splitlines()
        if lines:
            version = redact_argument(lines[0][:512], roots)
    except (OSError, subprocess.TimeoutExpired):
        pass
    return {
        "name": name,
        "path": logical_path(path, roots),
        "size": path.stat().st_size,
        "sha256": sha256_file(path),
        "version": version,
    }


def git_source_subject(root: Path) -> dict[str, Any]:
    def git(*args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(root), *args], check=False, capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            raise ArtifactError(result.stderr.strip() or f"git {' '.join(args)} failed in {root}")
        return result.stdout.strip()

    status = git("status", "--porcelain", "--untracked-files=no")
    return {
        "commit": git("rev-parse", "HEAD"),
        "tree": git("rev-parse", "HEAD^{tree}"),
        "trackedDirty": bool(status),
    }


def make_provenance(
    lock_path: Path,
    artifacts: list[tuple[str, Path]],
    tools: list[tuple[str, Path]],
    commands: list[list[str]],
    requested_environment: list[str],
    roots: list[tuple[str, Path]],
    source_root: Path | None,
) -> dict[str, Any]:
    lock = load_data(lock_path)
    if not isinstance(lock, dict):
        raise ArtifactError("source lock must be an object/table")
    unknown_environment = sorted(set(requested_environment) - SAFE_ENVIRONMENT)
    if unknown_environment:
        raise ArtifactError("environment field is not allowlisted: " + ", ".join(unknown_environment))
    root_map = roots or [("workspace", Path.cwd().resolve())]
    environment = {
        name: redact_argument(os.environ[name], root_map)
        for name in sorted(set(requested_environment))
        if name in os.environ
    }
    provenance: dict[str, Any] = {
        "format": "wos-build-provenance",
        "version": FORMAT_VERSION,
        "sourceLock": {
            "path": logical_path(lock_path, root_map),
            "sha256": sha256_file(lock_path),
        },
        "commands": [redact_command(command, root_map) for command in commands],
        "environment": environment,
        "tools": [tool_subject(name, path, root_map) for name, path in sorted(tools)],
        "artifacts": [artifact_subject(name, path, root_map) for name, path in sorted(artifacts)],
    }
    if source_root is not None:
        provenance["source"] = git_source_subject(source_root)
    provenance["statementSha256"] = document_digest(provenance)
    return provenance


def _component_lists(lock: dict[str, Any]) -> Iterable[tuple[str, Any]]:
    source_lock_v1_keys = ("gitSources", "archives", "patches", "generatedInputs")
    if lock.get("schemaVersion") == 1 or any(key in lock for key in source_lock_v1_keys):
        for key in source_lock_v1_keys:
            value = lock.get(key)
            if not isinstance(value, list):
                raise ArtifactError(f"source-lock {key} must be an array")
            for item in value:
                yield key, item
        return

    for key in (
        "components",
        "sources",
        "submodules",
        "git",
        "archives",
        "distfiles",
        "tools",
        "generatedTools",
    ):
        value = lock.get(key)
        if isinstance(value, list):
            for item in value:
                yield key, item
        elif isinstance(value, dict):
            for name, item in value.items():
                if isinstance(item, dict):
                    yield key, {"name": name, **item}


def source_components(lock: dict[str, Any]) -> list[dict[str, Any]]:
    components: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for category, raw in _component_lists(lock):
        if not isinstance(raw, dict):
            raise ArtifactError(f"source-lock {category} entry must be an object")
        name = str(raw.get("name") or raw.get("id") or raw.get("path") or "").strip()
        identity = (category, name)
        if not name or identity in seen:
            raise ArtifactError(f"source-lock component has missing/duplicate identity: {name!r}")
        seen.add(identity)
        urls = raw.get("urls")
        if not isinstance(urls, list):
            urls = [raw.get("url") or raw.get("repository") or raw.get("sourceUrl")]
        url = next((str(item) for item in urls if isinstance(item, str) and item), "NOASSERTION")
        digest = str(raw.get("sha256") or raw.get("treeSha256") or raw.get("digest") or "")
        if digest.startswith("sha256:"):
            digest = digest.removeprefix("sha256:")
        revision = str(raw.get("commit") or raw.get("revision") or raw.get("version") or "")
        license_name = str(
            raw.get("license") or raw.get("licenseDeclared") or raw.get("license_expression") or "NOASSERTION"
        )
        components.append(
            {
                "name": name,
                "category": category,
                "downloadLocation": sanitize_url(url),
                "version": revision,
                "sha256": digest if SHA256_RE.fullmatch(digest) else "",
                "license": license_name,
                "path": str(raw.get("path") or raw.get("filename") or name),
            }
        )
    return sorted(components, key=lambda item: (item["name"], item["category"]))


def spdx_id(prefix: str, name: str, used: set[str]) -> str:
    stem = re.sub(r"[^A-Za-z0-9.-]+", "-", name).strip("-.") or "item"
    candidate = f"SPDXRef-{prefix}-{stem}"
    suffix = 2
    while candidate in used:
        candidate = f"SPDXRef-{prefix}-{stem}-{suffix}"
        suffix += 1
    used.add(candidate)
    return candidate


def spdx_created() -> str:
    raw = os.environ.get("SOURCE_DATE_EPOCH", "0")
    try:
        epoch = max(0, int(raw))
    except ValueError as exc:
        raise ArtifactError("SOURCE_DATE_EPOCH must be a non-negative integer") from exc
    return dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_sbom(lock_path: Path, artifacts: list[tuple[str, Path]], roots: list[tuple[str, Path]]) -> dict[str, Any]:
    lock = load_data(lock_path)
    if not isinstance(lock, dict):
        raise ArtifactError("source lock must be an object/table")
    lock_digest = sha256_file(lock_path)
    components = source_components(lock)
    workspace = lock.get("workspace")
    workspace_license = workspace.get("license") if isinstance(workspace, dict) else None
    used = {"SPDXRef-DOCUMENT", "SPDXRef-Package-WOS"}
    packages: list[dict[str, Any]] = [
        {
            "name": "WOS",
            "SPDXID": "SPDXRef-Package-WOS",
            "downloadLocation": "NOASSERTION",
            # Shipped files are listed and related below, but this tool does
            # not make a source-level license conclusion for their contents.
            "filesAnalyzed": False,
            "licenseConcluded": "NOASSERTION",
            "licenseDeclared": str(workspace_license or lock.get("license") or "NOASSERTION"),
            "copyrightText": "NOASSERTION",
        }
    ]
    relationships: list[dict[str, str]] = [
        {
            "spdxElementId": "SPDXRef-DOCUMENT",
            "relationshipType": "DESCRIBES",
            "relatedSpdxElement": "SPDXRef-Package-WOS",
        }
    ]
    files: list[dict[str, Any]] = []
    for component in components:
        if component["category"] in {"patches", "generatedInputs"}:
            if not component["sha256"]:
                raise ArtifactError(f"source-lock input lacks SHA-256: {component['name']}")
            identifier = spdx_id("File", component["name"], used)
            source_file: dict[str, Any] = {
                "fileName": component["path"],
                "SPDXID": identifier,
                "checksums": [{"algorithm": "SHA256", "checksumValue": component["sha256"]}],
                "licenseConcluded": "NOASSERTION",
                "copyrightText": "NOASSERTION",
                "fileComment": f"WOS source-lock category: {component['category']}",
            }
            files.append(source_file)
            relationships.append(
                {
                    "spdxElementId": "SPDXRef-Package-WOS",
                    "relationshipType": "GENERATED_FROM",
                    "relatedSpdxElement": identifier,
                }
            )
            continue
        identifier = spdx_id("Package", component["name"], used)
        package: dict[str, Any] = {
            "name": component["name"],
            "SPDXID": identifier,
            "downloadLocation": component["downloadLocation"],
            "filesAnalyzed": False,
            "licenseConcluded": component["license"],
            "licenseDeclared": component["license"],
            "copyrightText": "NOASSERTION",
            "sourceInfo": f"WOS source-lock category: {component['category']}",
        }
        if component["version"]:
            package["versionInfo"] = component["version"]
        if component["sha256"]:
            package["checksums"] = [{"algorithm": "SHA256", "checksumValue": component["sha256"]}]
        packages.append(package)
        relationships.append(
            {
                "spdxElementId": "SPDXRef-Package-WOS",
                "relationshipType": "DEPENDS_ON",
                "relatedSpdxElement": identifier,
            }
        )

    for label, path in sorted(artifacts):
        if path.is_file():
            rows = [(label, path)]
        elif path.is_dir():
            rows = [
                (f"{label}/{entry['path']}", path / entry["path"])
                for entry in tree_entries(path)
                if entry["type"] == "file"
            ]
        else:
            raise ArtifactError(f"SBOM artifact does not exist or is unsupported: {path}")
        for logical_name, file_path in rows:
            identifier = spdx_id("File", logical_name, used)
            files.append(
                {
                    "fileName": logical_name,
                    "SPDXID": identifier,
                    "checksums": [{"algorithm": "SHA256", "checksumValue": sha256_file(file_path)}],
                    "licenseConcluded": "NOASSERTION",
                    "copyrightText": "NOASSERTION",
                }
            )
            relationships.append(
                {
                    "spdxElementId": "SPDXRef-Package-WOS",
                    "relationshipType": "CONTAINS",
                    "relatedSpdxElement": identifier,
                }
            )

    namespace_seed = {
        "lock": lock_digest,
        "files": [(item["fileName"], item["checksums"][0]["checksumValue"]) for item in files],
    }
    return {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": "WOS build SBOM",
        "documentNamespace": f"https://wos.invalid/spdx/{document_digest(namespace_seed)}",
        "creationInfo": {"created": spdx_created(), "creators": ["Tool: scripts/build/wos_artifacts.py"]},
        "documentDescribes": ["SPDXRef-Package-WOS"],
        "packages": packages,
        "files": files,
        "relationships": relationships,
        "annotations": [
            {
                "annotationDate": spdx_created(),
                "annotationType": "OTHER",
                "annotator": "Tool: scripts/build/wos_artifacts.py",
                "comment": f"WOS source lock SHA256: {lock_digest}",
            }
        ],
    }


def require_sha256(value: Any, context: str) -> None:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise ArtifactError(f"{context} must be a lowercase SHA-256 digest")


def validate_tree_manifest(data: Any) -> None:
    if not isinstance(data, dict) or data.get("format") != "wos-tree-manifest" or data.get("version") != FORMAT_VERSION:
        raise ArtifactError("invalid WOS tree manifest header")
    entries = data.get("entries")
    if not isinstance(entries, list):
        raise ArtifactError("tree manifest entries must be an array")
    paths: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            raise ArtifactError("tree manifest entry must contain a path")
        path = entry["path"]
        if not safe_relative_path(path):
            raise ArtifactError(f"unsafe tree manifest path: {path!r}")
        paths.append(path)
        if entry.get("type") == "file":
            require_sha256(entry.get("sha256"), f"tree file {path}")
    if paths != sorted(set(paths)):
        raise ArtifactError("tree manifest paths must be unique and sorted")
    body = {key: value for key, value in data.items() if key != "treeSha256"}
    require_sha256(data.get("treeSha256"), "treeSha256")
    if document_digest(body) != data["treeSha256"]:
        raise ArtifactError("tree manifest digest mismatch")


def _walk_json(value: Any) -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            yield key, child
            yield from _walk_json(child)
    elif isinstance(value, list):
        for child in value:
            if not isinstance(child, (dict, list)):
                yield "", child
            yield from _walk_json(child)


def unredacted_url_secret(value: str) -> bool:
    try:
        parsed = urllib.parse.urlsplit(value)
    except ValueError:
        return "@" in value and "://" in value
    if parsed.scheme not in ("http", "https", "ssh", "git", "file"):
        return False
    if parsed.password is not None or (parsed.username is not None and parsed.username != REDACTED):
        return True
    return any(SECRET_NAME_RE.search(key) and item != REDACTED for key, item in urllib.parse.parse_qsl(parsed.query))


def validate_provenance(data: Any) -> None:
    if not isinstance(data, dict) or data.get("format") != "wos-build-provenance" or data.get("version") != FORMAT_VERSION:
        raise ArtifactError("invalid WOS provenance header")
    lock = data.get("sourceLock")
    if not isinstance(lock, dict):
        raise ArtifactError("provenance sourceLock must be an object")
    require_sha256(lock.get("sha256"), "sourceLock.sha256")
    environment = data.get("environment")
    if not isinstance(environment, dict) or not set(environment).issubset(SAFE_ENVIRONMENT):
        raise ArtifactError("provenance contains a non-allowlisted environment field")
    commands = data.get("commands")
    if not isinstance(commands, list):
        raise ArtifactError("provenance commands must be an array")
    for command in commands:
        if not isinstance(command, list) or any(not isinstance(argument, str) for argument in command):
            raise ArtifactError("provenance command must be an array of strings")
        redact_next = False
        for argument in command:
            if redact_next and argument != REDACTED:
                raise ArtifactError("provenance contains an unredacted secret option value")
            redact_next = SECRET_OPTION_RE.match(argument) is not None and "=" not in argument
    for key, value in _walk_json(data):
        if SECRET_NAME_RE.search(key) and value != REDACTED:
            raise ArtifactError(f"provenance contains secret-bearing field {key!r}")
        if isinstance(value, str) and unredacted_url_secret(value):
            raise ArtifactError("provenance contains unredacted URL credentials")
        if isinstance(value, str) and "://" not in value and "=" in value:
            name, item = value.split("=", 1)
            if SECRET_NAME_RE.search(name) and item != REDACTED:
                raise ArtifactError("provenance contains an unredacted secret assignment")
    provided = data.get("statementSha256")
    require_sha256(provided, "statementSha256")
    body = {key: value for key, value in data.items() if key != "statementSha256"}
    if document_digest(body) != provided:
        raise ArtifactError("provenance statement digest mismatch")


def validate_sbom(data: Any, *, strict: bool = False) -> None:
    if not isinstance(data, dict) or data.get("spdxVersion") != "SPDX-2.3":
        raise ArtifactError("SBOM is not SPDX 2.3 JSON")
    if data.get("dataLicense") != "CC0-1.0" or data.get("SPDXID") != "SPDXRef-DOCUMENT":
        raise ArtifactError("SBOM document header is invalid")
    packages, files = data.get("packages"), data.get("files")
    relationships = data.get("relationships")
    if not isinstance(packages, list) or not packages or not isinstance(files, list) or not isinstance(relationships, list):
        raise ArtifactError("SBOM packages/files/relationships must be arrays")
    identifiers = {"SPDXRef-DOCUMENT"}
    for kind, entries in (("package", packages), ("file", files)):
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("SPDXID"), str):
                raise ArtifactError(f"SBOM {kind} lacks SPDXID")
            identifier = entry["SPDXID"]
            if identifier in identifiers:
                raise ArtifactError(f"duplicate SPDXID: {identifier}")
            identifiers.add(identifier)
            if strict and kind == "package" and entry.get("licenseDeclared") == "NOASSERTION":
                raise ArtifactError(f"SBOM package lacks declared license: {entry.get('name')}")
            for checksum in entry.get("checksums", []):
                if checksum.get("algorithm") == "SHA256":
                    require_sha256(checksum.get("checksumValue"), f"{identifier} checksum")
    for relationship in relationships:
        if not isinstance(relationship, dict):
            raise ArtifactError("SBOM relationship must be an object")
        if relationship.get("spdxElementId") not in identifiers or relationship.get("relatedSpdxElement") not in identifiers:
            raise ArtifactError("SBOM relationship references an unknown SPDXID")


def validate_comparison(data: Any) -> None:
    if not isinstance(data, dict) or data.get("format") != "wos-artifact-comparison":
        raise ArtifactError("invalid WOS comparison header")
    if data.get("kind") not in {"byte", "elf", "tree", "qcow"} or not isinstance(data.get("result"), dict):
        raise ArtifactError("comparison kind/result is invalid")
    if not isinstance(data["result"].get("equal"), bool):
        raise ArtifactError("comparison result must contain boolean equal")


def make_comparison(args: argparse.Namespace) -> dict[str, Any]:
    left, right = Path(args.left), Path(args.right)
    kind = args.kind
    if kind == "auto":
        if left.is_dir() and right.is_dir():
            kind = "tree"
        elif left.suffix.lower() in (".qcow", ".qcow2") or right.suffix.lower() in (".qcow", ".qcow2"):
            kind = "qcow"
        elif left.is_file() and right.is_file() and file_prefix(left, 4) == ELF_MAGIC and file_prefix(right, 4) == ELF_MAGIC:
            kind = "elf"
        else:
            kind = "byte"
    if kind == "byte":
        result = compare_bytes(left, right)
    elif kind == "elf":
        result = compare_elf(left, right)
    elif kind == "tree":
        result = compare_trees(left, right, include_times=args.include_times)
    elif kind == "qcow":
        result = compare_qcow(
            left,
            right,
            expected_fields=set(args.expect_container_delta),
            expect_allocation_delta=args.expect_qcow_allocation_delta,
            left_tree=Path(args.left_tree) if args.left_tree else None,
            right_tree=Path(args.right_tree) if args.right_tree else None,
            qemu_img=args.qemu_img,
        )
    else:
        raise ArtifactError(f"unsupported comparison kind: {kind}")
    return {"format": "wos-artifact-comparison", "version": FORMAT_VERSION, "kind": kind, "result": result}


def file_prefix(path: Path, size: int) -> bytes:
    with path.open("rb") as handle:
        return handle.read(size)


def read_commands(path: Path | None) -> list[list[str]]:
    if path is None:
        return []
    data = load_data(path)
    if not isinstance(data, list) or any(
        not isinstance(command, list) or any(not isinstance(argument, str) for argument in command) for command in data
    ):
        raise ArtifactError("command JSON must be an array of string arrays")
    return data


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    tree = subparsers.add_parser("tree-manifest", help="write a canonical manifest for a directory tree")
    tree.add_argument("root")
    tree.add_argument("--include-times", action="store_true")
    tree.add_argument("-o", "--output", type=Path)

    compare = subparsers.add_parser("compare", help="perform a layered artifact comparison")
    compare.add_argument("left")
    compare.add_argument("right")
    compare.add_argument("--kind", choices=("auto", "byte", "elf", "tree", "qcow"), default="auto")
    compare.add_argument("--include-times", action="store_true", help="include exact mtimes in tree equality")
    compare.add_argument("--expect-container-delta", action="append", default=[], metavar="FIELD")
    compare.add_argument("--expect-qcow-allocation-delta", action="store_true")
    compare.add_argument("--left-tree", help="read-only extracted filesystem tree for the left qcow")
    compare.add_argument("--right-tree", help="read-only extracted filesystem tree for the right qcow")
    compare.add_argument("--qemu-img", default="qemu-img")
    compare.add_argument("-o", "--output", type=Path)

    provenance = subparsers.add_parser("provenance", help="emit allowlisted, redacted build provenance")
    provenance.add_argument("--lock", required=True, type=Path)
    provenance.add_argument("--artifact", action="append", default=[])
    provenance.add_argument("--tool", action="append", default=[])
    provenance.add_argument("--root", action="append", default=[])
    provenance.add_argument("--command-json", type=Path)
    provenance.add_argument("--env", action="append", default=[])
    provenance.add_argument("--source-root", type=Path)
    provenance.add_argument("-o", "--output", type=Path)

    sbom = subparsers.add_parser("sbom", help="emit an SPDX 2.3 JSON SBOM")
    sbom.add_argument("--lock", required=True, type=Path)
    sbom.add_argument("--artifact", action="append", default=[])
    sbom.add_argument("--root", action="append", default=[])
    sbom.add_argument("-o", "--output", type=Path)

    validate = subparsers.add_parser("validate", help="validate a generated WOS document")
    validate.add_argument("kind", choices=("tree", "comparison", "provenance", "sbom"))
    validate.add_argument("path", type=Path)
    validate.add_argument("--strict", action="store_true", help="require declared licenses in SPDX packages")
    return parser


def run(args: argparse.Namespace) -> int:
    if args.command == "tree-manifest":
        write_json(make_tree_manifest(Path(args.root), include_times=args.include_times), args.output)
        return 0
    if args.command == "compare":
        report = make_comparison(args)
        write_json(report, args.output)
        result = report["result"]
        if report["kind"] == "qcow":
            if not result["complete"]:
                return 2
            return 0 if result["semanticEqual"] else 1
        if report["kind"] == "elf":
            return 0 if result["semanticEqual"] else 1
        return 0 if result["equal"] else 1
    if args.command == "provenance":
        roots = parse_assignments(args.root, "root")
        artifacts = parse_assignments(args.artifact, "artifact")
        tools = parse_assignments(args.tool, "tool")
        data = make_provenance(
            args.lock,
            artifacts,
            tools,
            read_commands(args.command_json),
            args.env,
            roots,
            args.source_root,
        )
        validate_provenance(data)
        write_json(data, args.output)
        return 0
    if args.command == "sbom":
        roots = parse_assignments(args.root, "root")
        artifacts = parse_assignments(args.artifact, "artifact")
        data = make_sbom(args.lock, artifacts, roots)
        validate_sbom(data)
        write_json(data, args.output)
        return 0
    if args.command == "validate":
        data = load_data(args.path)
        validators = {
            "tree": validate_tree_manifest,
            "comparison": validate_comparison,
            "provenance": validate_provenance,
        }
        if args.kind == "sbom":
            validate_sbom(data, strict=args.strict)
        else:
            validators[args.kind](data)
        return 0
    raise ArtifactError(f"unsupported command: {args.command}")


def main() -> int:
    try:
        return run(build_parser().parse_args())
    except ArtifactError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
