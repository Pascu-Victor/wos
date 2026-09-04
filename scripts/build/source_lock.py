#!/usr/bin/env python3
"""Validate and materialize the pinned WOS source set.

Network access is deliberately confined to the ``populate`` command. Build
scripts use ``materialize-*`` and ``verify-*`` commands, all of which operate
only on the local content-addressed store.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOCK = ROOT / "configs" / "reproducibility" / "source-lock.v1.json"
HEX40_OR_64 = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
SAFE_ID = re.compile(r"[a-z0-9][a-z0-9._+-]*\Z")


class SourceLockError(RuntimeError):
    pass


def fail(message: str) -> None:
    raise SourceLockError(message)


def read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read JSON {path}: {exc}")


def require_dict(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, dict):
        fail(f"{context} must be an object")
    return value


def require_list(value: object, context: str) -> list[object]:
    if not isinstance(value, list):
        fail(f"{context} must be an array")
    return value


def require_string(value: object, context: str) -> str:
    if not isinstance(value, str) or not value:
        fail(f"{context} must be a non-empty string")
    return value


def validate_relative_path(value: object, context: str, *, allow_dot: bool = False) -> str:
    path = require_string(value, context)
    candidate = Path(path)
    if candidate.is_absolute() or ".." in candidate.parts or (path == "." and not allow_dot):
        fail(f"{context} must be a safe repository-relative path")
    return path


def validate_unique_ids(entries: list[object], context: str) -> None:
    seen: set[str] = set()
    for index, raw in enumerate(entries):
        entry = require_dict(raw, f"{context}[{index}]")
        identifier = require_string(entry.get("id"), f"{context}[{index}].id")
        if SAFE_ID.fullmatch(identifier) is None:
            fail(f"{context}[{index}].id is not a safe identifier: {identifier}")
        if identifier in seen:
            fail(f"duplicate {context} id: {identifier}")
        seen.add(identifier)


def validate_lock_data(data: object) -> dict[str, object]:
    lock = require_dict(data, "source lock")
    if lock.get("schemaVersion") != 1:
        fail("source lock schemaVersion must be 1")
    require_string(lock.get("generatedBy"), "source lock.generatedBy")

    workspace = require_dict(lock.get("workspace"), "source lock.workspace")
    require_string(workspace.get("source"), "source lock.workspace.source")
    require_string(workspace.get("license"), "source lock.workspace.license")
    # The containing WOS commit belongs in per-build provenance. Putting it in
    # this checked-in file would make every lock update self-referential.

    git_sources = require_list(lock.get("gitSources"), "source lock.gitSources")
    archives = require_list(lock.get("archives"), "source lock.archives")
    patches = require_list(lock.get("patches"), "source lock.patches")
    generated = require_list(lock.get("generatedInputs"), "source lock.generatedInputs")
    for entries, context in (
        (git_sources, "gitSources"),
        (archives, "archives"),
        (patches, "patches"),
        (generated, "generatedInputs"),
    ):
        validate_unique_ids(entries, context)

    filenames: set[str] = set()
    for index, raw in enumerate(git_sources):
        entry = require_dict(raw, f"gitSources[{index}]")
        urls = require_list(entry.get("urls"), f"gitSources[{index}].urls")
        if not urls:
            fail(f"gitSources[{index}].urls must not be empty")
        for url_index, url in enumerate(urls):
            require_string(url, f"gitSources[{index}].urls[{url_index}]")
        revision = require_string(entry.get("commit"), f"gitSources[{index}].commit")
        if HEX40_OR_64.fullmatch(revision) is None:
            fail(f"gitSources[{index}].commit must be a lowercase Git object ID")
        if "path" in entry:
            validate_relative_path(entry["path"], f"gitSources[{index}].path")
        tree_digest = entry.get("treeSha256")
        if tree_digest is not None and (not isinstance(tree_digest, str) or SHA256.fullmatch(tree_digest) is None):
            fail(f"gitSources[{index}].treeSha256 must be a lowercase SHA-256 digest")
        overlays = require_list(entry.get("allowedOverlays", []), f"gitSources[{index}].allowedOverlays")
        overlay_paths: set[str] = set()
        for overlay_index, raw_overlay in enumerate(overlays):
            overlay = require_dict(raw_overlay, f"gitSources[{index}].allowedOverlays[{overlay_index}]")
            overlay_path = validate_relative_path(
                overlay.get("path"), f"gitSources[{index}].allowedOverlays[{overlay_index}].path"
            )
            if overlay_path in overlay_paths:
                fail(f"duplicate allowed overlay path for {entry['id']}: {overlay_path}")
            overlay_paths.add(overlay_path)
            overlay_digest = require_string(
                overlay.get("sha256"), f"gitSources[{index}].allowedOverlays[{overlay_index}].sha256"
            )
            if SHA256.fullmatch(overlay_digest) is None:
                fail(f"gitSources[{index}].allowedOverlays[{overlay_index}].sha256 must be lowercase SHA-256")
        require_string(entry.get("license"), f"gitSources[{index}].license")

    for index, raw in enumerate(archives):
        entry = require_dict(raw, f"archives[{index}]")
        filename = require_string(entry.get("filename"), f"archives[{index}].filename")
        if Path(filename).name != filename or filename in filenames:
            fail(f"archives[{index}].filename must be a unique basename")
        filenames.add(filename)
        digest = require_string(entry.get("sha256"), f"archives[{index}].sha256")
        if SHA256.fullmatch(digest) is None:
            fail(f"archives[{index}].sha256 must be a lowercase SHA-256 digest")
        urls = require_list(entry.get("urls"), f"archives[{index}].urls")
        if not urls:
            fail(f"archives[{index}].urls must not be empty")
        for url_index, url in enumerate(urls):
            require_string(url, f"archives[{index}].urls[{url_index}]")
        require_string(entry.get("version"), f"archives[{index}].version")
        require_string(entry.get("license"), f"archives[{index}].license")

    for context, entries in (("patches", patches), ("generatedInputs", generated)):
        for index, raw in enumerate(entries):
            entry = require_dict(raw, f"{context}[{index}]")
            relative_path = validate_relative_path(entry.get("path"), f"{context}[{index}].path")
            if "source" in entry:
                source = require_string(entry.get("source"), f"{context}[{index}].source")
                if source != f"workspace:{relative_path}":
                    fail(f"{context}[{index}].source must identify its portable workspace path")
            if "license" in entry:
                require_string(entry.get("license"), f"{context}[{index}].license")
            digest = require_string(entry.get("sha256"), f"{context}[{index}].sha256")
            if SHA256.fullmatch(digest) is None:
                fail(f"{context}[{index}].sha256 must be a lowercase SHA-256 digest")

    return lock


def load_lock(path: Path) -> dict[str, object]:
    return validate_lock_data(read_json(path))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def lock_sha256(path: Path) -> str:
    return sha256_file(path)


def run_git(args: list[str], *, cwd: Path | None = None, capture: bool = False) -> str:
    command = ["git", *args]
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            check=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        detail = ""
        if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
            detail = ": " + exc.stderr.decode("utf-8", "replace").strip()
        fail(f"Git command failed: {' '.join(command)}{detail}")
    return result.stdout.decode("utf-8", "strict").strip() if capture else ""


def git_tree_sha256(repo: Path, revision: str) -> str:
    # Temporary files keep large trees such as LLVM bounded in memory while
    # preserving the v1 length-framed digest format.
    with tempfile.TemporaryFile() as tree, tempfile.TemporaryFile() as archive:
        try:
            subprocess.run(
                ["git", "-C", str(repo), "ls-tree", "-rz", "-r", "--full-tree", revision],
                check=True,
                stdout=tree,
                stderr=subprocess.PIPE,
            )
            subprocess.run(
                ["git", "-C", str(repo), "archive", "--format=tar", revision],
                check=True,
                stdout=archive,
                stderr=subprocess.PIPE,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            fail(f"cannot enumerate Git tree {revision} in {repo}: {exc}")

        # The archive contributes every tracked byte, path, mode and symlink
        # target. The raw tree listing also binds gitlinks, which git archive
        # does not descend into.
        digest = hashlib.sha256()
        for field in (b"wos-git-tree-sha256-v1\0",):
            digest.update(struct.pack(">Q", len(field)))
            digest.update(field)
        for stream in (tree, archive):
            length = stream.seek(0, os.SEEK_END)
            stream.seek(0)
            digest.update(struct.pack(">Q", length))
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    return digest.hexdigest()


def find_by_id(lock: dict[str, object], collection: str, identifier: str) -> dict[str, object]:
    for raw in require_list(lock[collection], collection):
        entry = require_dict(raw, collection)
        if entry["id"] == identifier:
            return entry
    fail(f"source lock has no {collection} entry named {identifier}")


def find_archive(lock: dict[str, object], filename: str) -> dict[str, object]:
    matches = [
        require_dict(raw, "archive")
        for raw in require_list(lock["archives"], "archives")
        if require_dict(raw, "archive").get("filename") == filename
    ]
    if len(matches) != 1:
        fail(f"source lock has no unique archive entry for {filename}")
    return matches[0]


def archive_store_path(store: Path, digest: str) -> Path:
    return store / "sha256" / digest


def git_store_path(store: Path, identifier: str) -> Path:
    return store / "git" / f"{identifier}.git"


def verify_archive(path: Path, entry: dict[str, object]) -> None:
    if not path.is_file():
        fail(f"locked archive is missing: {path}")
    actual = sha256_file(path)
    if actual != entry["sha256"]:
        fail(f"archive SHA-256 mismatch for {path}: got {actual}, expected {entry['sha256']}")


def verify_git_repo(repo: Path, entry: dict[str, object], *, require_worktree: bool) -> None:
    if not repo.exists():
        fail(f"locked Git source is missing: {repo}")
    revision = str(entry["commit"])
    run_git(["-C", str(repo), "cat-file", "-e", f"{revision}^{{commit}}"])
    if require_worktree:
        current = run_git(["-C", str(repo), "rev-parse", "HEAD"], capture=True)
        if current != revision:
            fail(f"Git source {repo} is at {current}, expected {revision}")
        status = run_git(
            ["-C", str(repo), "status", "--porcelain=v1", "--untracked-files=all", "--ignored=no"],
            capture=True,
        )
        allowed = {
            str(require_dict(raw, "allowed overlay")["path"]): require_dict(raw, "allowed overlay")
            for raw in require_list(entry.get("allowedOverlays", []), "allowed overlays")
        }
        for line in status.splitlines():
            state = line[:2]
            relative = line[3:]
            overlay = allowed.get(relative)
            if state != "??" or overlay is None:
                fail(f"Git source {repo} has non-locked worktree change: {line}")
            overlay_path = repo / relative
            actual = sha256_file(overlay_path) if overlay_path.is_file() else ""
            if actual != overlay.get("sha256"):
                fail(
                    f"Git source {repo} overlay SHA-256 mismatch for {relative}: "
                    f"got {actual}, expected {overlay.get('sha256')}"
                )
    expected_tree = entry.get("treeSha256")
    if expected_tree is not None:
        actual_tree = git_tree_sha256(repo, revision)
        if actual_tree != expected_tree:
            fail(f"Git tree SHA-256 mismatch for {entry['id']}: got {actual_tree}, expected {expected_tree}")


def verify_declared_files(lock: dict[str, object], workspace: Path) -> None:
    for collection in ("patches", "generatedInputs"):
        for raw in require_list(lock[collection], collection):
            entry = require_dict(raw, collection)
            path = workspace / str(entry["path"])
            if not path.is_file():
                fail(f"locked {collection} input is missing: {path}")
            actual = sha256_file(path)
            if actual != entry["sha256"]:
                fail(f"locked {collection} SHA-256 mismatch for {path}: got {actual}, expected {entry['sha256']}")


def verify_workspace(lock: dict[str, object], workspace: Path) -> None:
    verify_declared_files(lock, workspace)
    for raw in require_list(lock["gitSources"], "gitSources"):
        entry = require_dict(raw, "git source")
        path_value = entry.get("path")
        if path_value is None:
            continue
        verify_git_repo(workspace / str(path_value), entry, require_worktree=True)


def verify_store(lock: dict[str, object], store: Path) -> None:
    for raw in require_list(lock["archives"], "archives"):
        entry = require_dict(raw, "archive")
        archive_path = archive_store_path(store, str(entry["sha256"]))
        if archive_path.exists():
            verify_archive(archive_path, entry)
        elif entry.get("alternativeGitSourceId"):
            alternative = find_by_id(lock, "gitSources", str(entry["alternativeGitSourceId"]))
            verify_git_repo(git_store_path(store, str(alternative["id"])), alternative, require_worktree=False)
        elif entry.get("storeRequired", True):
            fail(f"locked archive is missing: {archive_path}")
    for raw in require_list(lock["gitSources"], "gitSources"):
        entry = require_dict(raw, "git source")
        repo = git_store_path(store, str(entry["id"]))
        if repo.exists() or entry.get("storeRequired", True):
            verify_git_repo(repo, entry, require_worktree=False)


def seed_local_git(store: Path, workspace: Path, entry: dict[str, object]) -> None:
    path_value = entry.get("path")
    if path_value is None:
        if entry.get("storeRequired", True):
            fail(f"Git source {entry['id']} has no local path for seed-local")
        return
    source = workspace / str(path_value)
    if not source.exists():
        if entry.get("storeRequired", True):
            fail(f"local Git source is missing for seed-local: {source}")
        return
    verify_git_repo(source, entry, require_worktree=True)
    destination = git_store_path(store, str(entry["id"]))
    if destination.exists():
        verify_git_repo(destination, entry, require_worktree=False)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{entry['id']}.", dir=destination.parent))
    shutil.rmtree(temporary)
    try:
        run_git(["clone", "--mirror", str(source), str(temporary)])
        verify_git_repo(temporary, entry, require_worktree=False)
        os.replace(temporary, destination)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


def seed_local_archive(store: Path, workspace: Path, lock: dict[str, object], entry: dict[str, object]) -> None:
    destination = archive_store_path(store, str(entry["sha256"]))
    if destination.exists():
        verify_archive(destination, entry)
        return
    for raw_candidate in require_list(entry.get("localCandidates", []), "archive localCandidates"):
        candidate = workspace / validate_relative_path(raw_candidate, "archive local candidate")
        if not candidate.is_file():
            continue
        verify_archive(candidate, entry)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(destination.name + f".tmp.{os.getpid()}")
        try:
            shutil.copyfile(candidate, temporary)
            verify_archive(temporary, entry)
            os.replace(temporary, destination)
            return
        finally:
            temporary.unlink(missing_ok=True)
    alternative_id = entry.get("alternativeGitSourceId")
    if alternative_id:
        alternative = find_by_id(lock, "gitSources", str(alternative_id))
        verify_git_repo(git_store_path(store, str(alternative["id"])), alternative, require_worktree=False)
        return
    if entry.get("storeRequired", True):
        fail(f"no verified local candidate for archive {entry['id']}")


def seed_local(lock: dict[str, object], store: Path, workspace: Path) -> None:
    for raw in require_list(lock["gitSources"], "gitSources"):
        seed_local_git(store, workspace, require_dict(raw, "git source"))
    for raw in require_list(lock["archives"], "archives"):
        seed_local_archive(store, workspace, lock, require_dict(raw, "archive"))
    verify_store(lock, store)


def download_to(url: str, destination: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "wos-source-lock/1"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response, destination.open("wb") as output:
            shutil.copyfileobj(response, output)
    except (OSError, urllib.error.URLError) as exc:
        fail(f"cannot download {url}: {exc}")


def populate_archive(store: Path, entry: dict[str, object]) -> None:
    destination = archive_store_path(store, str(entry["sha256"]))
    if destination.exists():
        verify_archive(destination, entry)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix="wos-source-", dir=destination.parent, delete=False) as temp:
        temporary = Path(temp.name)
    try:
        errors: list[str] = []
        for url in require_list(entry["urls"], "archive urls"):
            try:
                download_to(str(url), temporary)
                verify_archive(temporary, entry)
                os.replace(temporary, destination)
                return
            except SourceLockError as exc:
                errors.append(str(exc))
                temporary.unlink(missing_ok=True)
                temporary.touch()
        fail(f"cannot populate archive {entry['id']}: {'; '.join(errors)}")
    finally:
        temporary.unlink(missing_ok=True)


def populate_git(store: Path, entry: dict[str, object]) -> None:
    destination = git_store_path(store, str(entry["id"]))
    if destination.exists():
        verify_git_repo(destination, entry, require_worktree=False)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{entry['id']}.", dir=destination.parent))
    shutil.rmtree(temporary)
    errors: list[str] = []
    try:
        for url in require_list(entry["urls"], "git urls"):
            try:
                run_git(["clone", "--mirror", str(url), str(temporary)])
                verify_git_repo(temporary, entry, require_worktree=False)
                os.replace(temporary, destination)
                return
            except SourceLockError as exc:
                errors.append(str(exc))
                shutil.rmtree(temporary, ignore_errors=True)
        fail(f"cannot populate Git source {entry['id']}: {'; '.join(errors)}")
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


def materialize_archive(lock: dict[str, object], store: Path, filename: str, destination: Path) -> None:
    entry = find_archive(lock, filename)
    source = archive_store_path(store, str(entry["sha256"]))
    verify_archive(source, entry)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + f".tmp.{os.getpid()}")
    try:
        shutil.copyfile(source, temporary)
        verify_archive(temporary, entry)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def materialize_archive_tree(
    lock: dict[str, object], store: Path, filename: str, destination: Path, strip_components: int
) -> None:
    entry = find_archive(lock, filename)
    source = archive_store_path(store, str(entry["sha256"]))
    verify_archive(source, entry)
    if strip_components < 0:
        fail("strip-components must not be negative")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + f".tmp.{os.getpid()}")
    shutil.rmtree(temporary, ignore_errors=True)
    temporary.mkdir()
    try:
        with tarfile.open(source, mode="r:*") as archive:
            members: list[tarfile.TarInfo] = []
            for member in archive.getmembers():
                parts = Path(member.name).parts
                if member.name.startswith("/") or ".." in parts:
                    fail(f"unsafe path in locked archive {filename}: {member.name}")
                if len(parts) <= strip_components:
                    continue
                member.name = str(Path(*parts[strip_components:]))
                if member.issym() or member.islnk():
                    target = Path(member.linkname)
                    if target.is_absolute() or ".." in target.parts:
                        fail(f"unsafe link in locked archive {filename}: {member.name}")
                members.append(member)
            archive.extractall(temporary, members=members)
        shutil.rmtree(destination, ignore_errors=True)
        os.replace(temporary, destination)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


def materialize_git(lock: dict[str, object], store: Path, identifier: str, destination: Path) -> None:
    entry = find_by_id(lock, "gitSources", identifier)
    if destination.exists():
        verify_git_repo(destination, entry, require_worktree=True)
        return
    source = git_store_path(store, identifier)
    verify_git_repo(source, entry, require_worktree=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + f".tmp.{os.getpid()}")
    try:
        shutil.rmtree(temporary, ignore_errors=True)
        run_git(["clone", "--no-checkout", str(source), str(temporary)])
        run_git(["-C", str(temporary), "checkout", "--detach", str(entry["commit"])])
        verify_git_repo(temporary, entry, require_worktree=True)
        os.replace(temporary, destination)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


def expand_artifacts(patterns: list[str]) -> list[Path]:
    expanded: list[Path] = []
    for pattern in patterns:
        matches = sorted(Path(item) for item in glob.glob(pattern)) if glob.has_magic(pattern) else [Path(pattern)]
        if not matches or any(not path.exists() for path in matches):
            fail(f"preseeded artifact is missing: {pattern}")
        expanded.extend(matches)
    return expanded


def artifact_identity(path: Path) -> tuple[str, str, str]:
    info = path.lstat()
    mode = format(stat.S_IMODE(info.st_mode), "04o")
    if path.is_symlink():
        return "symlink", mode, hashlib.sha256(os.readlink(path).encode("utf-8")).hexdigest()
    if path.is_file():
        return "file", mode, sha256_file(path)
    fail(f"preseed manifest only supports files and symlinks: {path}")


def parse_roots(values: list[str]) -> dict[str, Path]:
    roots: dict[str, Path] = {}
    for value in values:
        name, separator, raw_path = value.partition("=")
        if not separator or SAFE_ID.fullmatch(name) is None or not raw_path:
            fail(f"root must use safe NAME=PATH syntax: {value}")
        if name in roots:
            fail(f"duplicate logical root: {name}")
        roots[name] = Path(raw_path).resolve()
    if not roots:
        fail("at least one --root NAME=PATH is required")
    return roots


def files_under(path: Path) -> list[Path]:
    if path.is_symlink() or path.is_file():
        return [path]
    if path.is_dir():
        return sorted(candidate for candidate in path.rglob("*") if candidate.is_file() or candidate.is_symlink())
    fail(f"artifact is not a file, symlink, or directory: {path}")


def logical_path(path: Path, roots: dict[str, Path]) -> tuple[str, str]:
    # Keep a symlink's own path as its identity. Resolving it here aliases the
    # link to its target and can create duplicate manifest records (for
    # example, sysroot/bin/clang and sysroot/bin/clang-22).
    canonical = path.absolute()
    matches: list[tuple[int, str, Path]] = []
    for name, root in roots.items():
        try:
            relative = canonical.relative_to(root)
        except ValueError:
            continue
        matches.append((len(root.parts), name, relative))
    if not matches:
        fail(f"artifact {path} is outside every declared logical root")
    _, name, relative = max(matches)
    return name, relative.as_posix()


def create_preseed_manifest(lock_path: Path, manifest_path: Path, roots: dict[str, Path]) -> None:
    load_lock(lock_path)
    root_records: dict[str, list[dict[str, str]]] = {}
    for name, root in sorted(roots.items()):
        if not root.is_dir():
            fail(f"preseed root is not a directory: {root}")
        records: list[dict[str, str]] = []
        for path in files_under(root):
            kind, mode, digest = artifact_identity(path)
            records.append(
                {
                    # Preserve the directory-entry name for symlinks. Resolving
                    # it would collapse every BusyBox applet (and versioned
                    # compiler link) onto the shared target record.
                    "path": path.absolute().relative_to(root).as_posix(),
                    "type": kind,
                    "mode": mode,
                    "sha256": digest,
                }
            )
        root_records[name] = records
    document = {
        "schemaVersion": 1,
        "sourceLockSha256": lock_sha256(lock_path),
        "roots": root_records,
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = manifest_path.with_name(manifest_path.name + f".tmp.{os.getpid()}")
    try:
        temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, manifest_path)
    finally:
        temporary.unlink(missing_ok=True)


def verify_preseed(
    lock_path: Path,
    manifest_path: Path,
    label: str,
    patterns: list[str],
    roots: dict[str, Path],
) -> None:
    load_lock(lock_path)
    manifest = require_dict(read_json(manifest_path), "preseed manifest")
    if manifest.get("schemaVersion") != 1:
        fail("preseed manifest schemaVersion must be 1")
    if manifest.get("sourceLockSha256") != lock_sha256(lock_path):
        fail("preseed manifest sourceLockSha256 does not match the selected source lock")
    manifest_roots = require_dict(manifest.get("roots"), "preseed manifest.roots")
    by_root: dict[str, dict[str, dict[str, object]]] = {}
    for root_name in roots:
        records = require_list(manifest_roots.get(root_name), f"preseed manifest.roots.{root_name}")
        by_path: dict[str, dict[str, object]] = {}
        for index, raw in enumerate(records):
            record = require_dict(raw, f"preseed manifest.roots.{root_name}[{index}]")
            record_path = validate_relative_path(
                record.get("path"), f"preseed manifest.roots.{root_name}[{index}].path", allow_dot=True
            )
            if record_path in by_path:
                fail(f"duplicate preseed artifact record: {root_name}:{record_path}")
            by_path[record_path] = record
        by_root[root_name] = by_path

    resolved = expand_artifacts(patterns)
    for requested in resolved:
        for path in files_under(requested):
            root_name, relative = logical_path(path, roots)
            record = by_root[root_name].get(relative)
            if record is None:
                fail(f"preseed manifest has no {label} record for {root_name}:{relative}")
            kind, mode, digest = artifact_identity(path)
            for key, actual in (("type", kind), ("mode", mode), ("sha256", digest)):
                if record.get(key) != actual:
                    fail(
                        f"preseed artifact {key} mismatch for {root_name}:{relative}: "
                        f"got {actual}, expected {record.get(key)}"
                    )


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("validate")
    workspace = subparsers.add_parser("verify-workspace")
    workspace.add_argument("--workspace", type=Path, default=ROOT)

    for command in ("verify-store", "populate"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("--store", type=Path, required=True)

    seed = subparsers.add_parser("seed-local")
    seed.add_argument("--store", type=Path, required=True)
    seed.add_argument("--workspace", type=Path, default=ROOT)

    archive = subparsers.add_parser("verify-archive")
    archive.add_argument("--file", type=Path, required=True)
    archive.add_argument("--filename")

    materialize_archive_parser = subparsers.add_parser("materialize-archive")
    materialize_archive_parser.add_argument("--store", type=Path, required=True)
    materialize_archive_parser.add_argument("--filename", required=True)
    materialize_archive_parser.add_argument("--destination", type=Path, required=True)

    materialize_tree_parser = subparsers.add_parser("materialize-archive-tree")
    materialize_tree_parser.add_argument("--store", type=Path, required=True)
    materialize_tree_parser.add_argument("--filename", required=True)
    materialize_tree_parser.add_argument("--destination", type=Path, required=True)
    materialize_tree_parser.add_argument("--strip-components", type=int, default=1)

    materialize_git_parser = subparsers.add_parser("materialize-git")
    materialize_git_parser.add_argument("--store", type=Path, required=True)
    materialize_git_parser.add_argument("--id", required=True)
    materialize_git_parser.add_argument("--destination", type=Path, required=True)

    preseed = subparsers.add_parser("verify-preseed")
    preseed.add_argument("--manifest", type=Path, required=True)
    preseed.add_argument("--label", required=True)
    preseed.add_argument("--root", action="append", default=[], metavar="NAME=PATH")
    preseed.add_argument("artifact", nargs="+")

    create_preseed = subparsers.add_parser("create-preseed-manifest")
    create_preseed.add_argument("--manifest", type=Path, required=True)
    create_preseed.add_argument("--root", action="append", default=[], metavar="NAME=PATH")

    tree = subparsers.add_parser("git-tree-sha256")
    tree.add_argument("--repo", type=Path, required=True)
    tree.add_argument("--revision", required=True)
    return parser


def main() -> int:
    args = make_parser().parse_args()
    lock_path = args.lock.resolve()
    lock = load_lock(lock_path)

    if args.command == "validate":
        verify_declared_files(lock, ROOT)
    elif args.command == "verify-workspace":
        verify_workspace(lock, args.workspace.resolve())
    elif args.command == "verify-store":
        verify_store(lock, args.store.resolve())
    elif args.command == "populate":
        for raw in require_list(lock["archives"], "archives"):
            populate_archive(args.store.resolve(), require_dict(raw, "archive"))
        for raw in require_list(lock["gitSources"], "gitSources"):
            populate_git(args.store.resolve(), require_dict(raw, "git source"))
        verify_store(lock, args.store.resolve())
    elif args.command == "seed-local":
        seed_local(lock, args.store.resolve(), args.workspace.resolve())
    elif args.command == "verify-archive":
        entry = find_archive(lock, args.filename or args.file.name)
        verify_archive(args.file, entry)
    elif args.command == "materialize-archive":
        materialize_archive(lock, args.store.resolve(), args.filename, args.destination)
    elif args.command == "materialize-archive-tree":
        materialize_archive_tree(
            lock,
            args.store.resolve(),
            args.filename,
            args.destination,
            args.strip_components,
        )
    elif args.command == "materialize-git":
        materialize_git(lock, args.store.resolve(), args.id, args.destination)
    elif args.command == "verify-preseed":
        verify_preseed(
            lock_path,
            args.manifest.resolve(),
            args.label,
            args.artifact,
            parse_roots(args.root),
        )
    elif args.command == "create-preseed-manifest":
        create_preseed_manifest(lock_path, args.manifest.resolve(), parse_roots(args.root))
    elif args.command == "git-tree-sha256":
        print(git_tree_sha256(args.repo.resolve(), args.revision))
    else:
        fail(f"unknown command: {args.command}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SourceLockError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
