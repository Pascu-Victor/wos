#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[3]
TOOL = ROOT / "scripts" / "build" / "source_lock.py"
LOCK = ROOT / "configs" / "reproducibility" / "source-lock.v1.json"
SCHEMA = ROOT / "configs" / "reproducibility" / "source-lock.schema.json"


def run(*args: object, ok: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [sys.executable, str(TOOL), *map(str, args)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if ok and result.returncode != 0:
        raise AssertionError(f"command failed: {result.args!r}\n{result.stdout}\n{result.stderr}")
    if not ok and result.returncode == 0:
        raise AssertionError(f"command unexpectedly passed: {result.args!r}")
    return result


def git(repo: Path, *args: str, capture: bool = False) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        text=True,
        stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
    )
    return result.stdout.strip() if capture else ""


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fixture_lock(root: Path, archive: Path, repo: Path) -> Path:
    commit = git(repo, "rev-parse", "HEAD", capture=True)
    tree = run("git-tree-sha256", "--repo", repo, "--revision", commit).stdout.strip()
    generated = root / "generator.txt"
    generated.write_text("generator\n", encoding="utf-8")
    document = {
        "schemaVersion": 1,
        "generatedBy": "source_lock_test.py",
        "workspace": {"source": "fixture", "license": "Apache-2.0"},
        "gitSources": [
            {
                "id": "fixture-git",
                "path": "git-source",
                "urls": [str(repo)],
                "commit": commit,
                "treeSha256": tree,
                "license": "MIT",
            }
        ],
        "archives": [
            {
                "id": "fixture-archive",
                "filename": archive.name,
                "version": "1",
                "urls": [archive.as_uri()],
                "sha256": sha256(archive),
                "license": "MIT",
                "localCandidates": [archive.name],
            }
        ],
        "patches": [],
        "generatedInputs": [
            {"id": "fixture-generator", "path": "generator.txt", "sha256": sha256(generated)}
        ],
    }
    path = root / "lock.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_checked_in_lock() -> None:
    run("validate")
    lock = json.loads(LOCK.read_text(encoding="utf-8"))
    json.loads(SCHEMA.read_text(encoding="utf-8"))
    if "commit" in lock["workspace"]:
        raise AssertionError("checked-in lock must not self-reference the containing WOS commit")
    git_ids = {entry["id"] for entry in lock["gitSources"]}
    archive_ids = {entry["id"] for entry in lock["archives"]}
    expected_git = {
        "limine", "llvm-project", "mlibc", "dropbear", "busybox", "wiremcp", "cmake",
        "ninja", "python", "nasm", "clang-tidy-cache", "git", "doom-ascii",
        "git-sha1collisiondetection", "mlibc-freestnd-c-hdrs", "mlibc-freestnd-cxx-hdrs",
        "mlibc-frigg", "mlibc-libsmarter", "mlibc-bragi", "googletest",
    }
    expected_archives = {"gnu-make", "bash", "zlib", "libressl", "curl", "meson", "nasm-release", "ncurses", "nano"}
    if git_ids != expected_git or archive_ids != expected_archives:
        raise AssertionError("checked-in lock does not enumerate the expected source inputs")


def test_local_store_and_materialization() -> None:
    with tempfile.TemporaryDirectory(prefix="wos-source-lock-test-") as raw:
        root = Path(raw)
        repo = root / "git-source"
        repo.mkdir()
        git(repo, "init")
        git(repo, "config", "user.name", "WOS Test")
        git(repo, "config", "user.email", "wos-test@example.invalid")
        (repo / "source.txt").write_text("locked source\n", encoding="utf-8")
        git(repo, "add", "source.txt")
        env = os.environ.copy()
        env.update({"GIT_AUTHOR_DATE": "@1 +0000", "GIT_COMMITTER_DATE": "@1 +0000"})
        subprocess.run(["git", "-C", str(repo), "commit", "-m", "fixture"], check=True, env=env, stdout=subprocess.DEVNULL)
        archive = root / "fixture-1.tar"
        archive.write_bytes(b"fixture archive\n")
        lock = fixture_lock(root, archive, repo)
        store = root / "store"
        run("--lock", lock, "seed-local", "--store", store, "--workspace", root)
        run("--lock", lock, "verify-store", "--store", store)

        archive_output = root / "output" / archive.name
        run("--lock", lock, "materialize-archive", "--store", store, "--filename", archive.name, "--destination", archive_output)
        if archive_output.read_bytes() != archive.read_bytes():
            raise AssertionError("materialized archive differs")
        git_output = root / "output-git"
        run("--lock", lock, "materialize-git", "--store", store, "--id", "fixture-git", "--destination", git_output)
        if (git_output / "source.txt").read_text(encoding="utf-8") != "locked source\n":
            raise AssertionError("materialized Git source differs")

        archive_output.write_bytes(b"corrupt")
        run("--lock", lock, "verify-archive", "--file", archive_output, ok=False)


def test_portable_preseed_manifest() -> None:
    with tempfile.TemporaryDirectory(prefix="wos-preseed-test-") as raw:
        base = Path(raw)
        archive = base / "fixture-1.tar"
        archive.write_bytes(b"fixture archive\n")
        repo = base / "git-source"
        repo.mkdir()
        git(repo, "init")
        git(repo, "config", "user.name", "WOS Test")
        git(repo, "config", "user.email", "wos-test@example.invalid")
        (repo / "source.txt").write_text("source\n", encoding="utf-8")
        git(repo, "add", "source.txt")
        git(repo, "commit", "-m", "fixture")
        lock = fixture_lock(base, archive, repo)

        root_a = base / "state-a" / "sysroot"
        root_a.mkdir(parents=True)
        artifact_a = root_a / "bin" / "tool"
        artifact_a.parent.mkdir()
        artifact_a.write_bytes(b"portable artifact\n")
        artifact_a.chmod(0o755)
        (artifact_a.parent / "tool-link").symlink_to("tool")
        manifest = base / "preseed.json"
        run("--lock", lock, "create-preseed-manifest", "--manifest", manifest, "--root", f"sysroot={root_a}")
        if str(base) in manifest.read_text(encoding="utf-8"):
            raise AssertionError("portable preseed manifest contains an absolute state-root path")

        root_b = base / "state-b" / "sysroot"
        shutil.copytree(root_a, root_b, copy_function=shutil.copy2, symlinks=True)
        artifact_b = root_b / "bin" / "tool"
        link_b = root_b / "bin" / "tool-link"
        run(
            "--lock", lock, "verify-preseed", "--manifest", manifest, "--label", "tool",
            "--root", f"sysroot={root_b}", artifact_b, link_b,
        )
        artifact_b.write_bytes(b"mutated\n")
        run(
            "--lock", lock, "verify-preseed", "--manifest", manifest, "--label", "tool",
            "--root", f"sysroot={root_b}", artifact_b, ok=False,
        )


def main() -> int:
    test_checked_in_lock()
    test_local_store_and_materialization()
    test_portable_preseed_manifest()
    print("WOS source lock invariants hold")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
