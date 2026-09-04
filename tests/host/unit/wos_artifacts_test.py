#!/usr/bin/env python3
"""Focused tests for deterministic artifact, provenance, and SBOM tooling."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "build" / "wos_artifacts.py"
SPEC = importlib.util.spec_from_file_location("wos_artifacts", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
wos = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = wos
SPEC.loader.exec_module(wos)

ZERO_SHA256 = "0" * 64
ONE_SHA256 = "1" * 64
TWO_SHA256 = "2" * 64


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def write_minimal_elf(path: Path, text: bytes, comment: bytes) -> None:
    names = b"\0.text\0.comment\0.shstrtab\0"
    text_offset, comment_offset, names_offset, sections_offset = 64, 80, 96, 128
    image = bytearray(sections_offset + 4 * 64)
    image[:16] = b"\x7fELF" + bytes((2, 1, 1, 0)) + bytes(8)
    struct.pack_into(
        "<HHIQQQIHHHHHH",
        image,
        16,
        2,
        62,
        1,
        0x400000,
        0,
        sections_offset,
        0,
        64,
        0,
        0,
        64,
        4,
        3,
    )
    image[text_offset : text_offset + len(text)] = text
    image[comment_offset : comment_offset + len(comment)] = comment
    image[names_offset : names_offset + len(names)] = names
    section_format = "<IIQQQQIIQQ"
    struct.pack_into(section_format, image, sections_offset + 64, 1, 1, 2, 0x400000, text_offset, len(text), 0, 0, 1, 0)
    struct.pack_into(section_format, image, sections_offset + 128, 7, 1, 0, 0, comment_offset, len(comment), 0, 0, 1, 0)
    struct.pack_into(section_format, image, sections_offset + 192, 16, 3, 0, 0, names_offset, len(names), 0, 0, 1, 0)
    path.write_bytes(image)


def populate_tree(root: Path, payload: bytes = b"payload") -> None:
    (root / "dir").mkdir(parents=True)
    (root / "dir" / "payload").write_bytes(payload)
    os.chmod(root / "dir" / "payload", 0o640)
    os.link(root / "dir" / "payload", root / "hardlink")
    os.symlink("dir/payload", root / "symlink")


def test_tree_manifest(root: Path) -> None:
    left, right = root / "tree-left", root / "tree-right"
    populate_tree(left)
    populate_tree(right)
    first = wos.make_tree_manifest(left)
    second = wos.make_tree_manifest(left)
    check(first == second, "tree manifests are not canonical")
    wos.validate_tree_manifest(first)
    entries = {entry["path"]: entry for entry in first["entries"]}
    check(entries["dir/payload"]["mode"] == 0o640, "file mode was not recorded")
    check(entries["hardlink"]["hardlinkTo"] == "dir/payload", "hardlink identity was not recorded")
    check(entries["symlink"]["target"] == "dir/payload", "symlink target was not recorded")
    check(wos.compare_trees(left, right)["equal"], "equal trees compare unequal")
    (right / "dir" / "payload").write_bytes(b"changed")
    check(not wos.compare_trees(left, right)["equal"], "payload difference was missed")

    tampered = json.loads(json.dumps(first))
    tampered["entries"][0]["mode"] ^= 1
    try:
        wos.validate_tree_manifest(tampered)
    except wos.ArtifactError:
        pass
    else:
        raise AssertionError("tampered tree manifest passed validation")


def test_elf_comparison(root: Path) -> None:
    left, metadata_delta, payload_delta = root / "left.elf", root / "metadata.elf", root / "payload.elf"
    write_minimal_elf(left, b"text-v1", b"builder-a")
    write_minimal_elf(metadata_delta, b"text-v1", b"builder-b")
    write_minimal_elf(payload_delta, b"text-v2", b"builder-a")
    metadata_report = wos.compare_elf(left, metadata_delta)
    check(not metadata_report["equal"] and metadata_report["semanticEqual"], "non-allocated ELF delta misclassified")
    check(not wos.compare_elf(left, payload_delta)["semanticEqual"], "allocated ELF delta was missed")


def test_qcow_comparison(root: Path) -> None:
    left, right = root / "left.qcow2", root / "right.qcow2"
    left.write_bytes(b"qcow-left")
    right.write_bytes(b"qcow-right")
    left_tree, right_tree = root / "qcow-left-tree", root / "qcow-right-tree"
    populate_tree(left_tree)
    populate_tree(right_tree)
    fake_qemu = root / "qemu-img"
    fake_qemu.write_text(
        "#!/bin/sh\n"
        "case \"$3\" in *left.qcow2) actual=10;; *) actual=20;; esac\n"
        "printf '{\"filename\":\"%s\",\"format\":\"qcow2\",\"virtual-size\":4096,\"actual-size\":%s}\\n' \"$3\" \"$actual\"\n",
        encoding="utf-8",
    )
    fake_qemu.chmod(0o755)
    unexplained = wos.compare_qcow(
        left,
        right,
        expected_fields=set(),
        expect_allocation_delta=False,
        left_tree=left_tree,
        right_tree=right_tree,
        qemu_img=str(fake_qemu),
    )
    check(unexplained["complete"] and not unexplained["semanticEqual"], "unclassified qcow delta was accepted")
    expected = wos.compare_qcow(
        left,
        right,
        expected_fields={"actual-size"},
        expect_allocation_delta=True,
        left_tree=left_tree,
        right_tree=right_tree,
        qemu_img=str(fake_qemu),
    )
    check(expected["semanticEqual"], "explicit qcow container metadata allowance was ignored")
    check("filename" not in expected["container"]["left"], "qcow observation path leaked into semantics")
    check(
        expected["container"]["expectedDifferences"][0]["classification"] == "expected-container-metadata",
        "qcow metadata classification is not explicit",
    )


def source_lock_fixture() -> dict[str, object]:
    return {
        "schemaVersion": 1,
        "generatedBy": "wos_artifacts_test",
        "workspace": {"source": ".", "license": "Apache-2.0"},
        "gitSources": [
            {
                "id": "mlibc",
                "path": "toolchain/src/mlibc",
                "urls": ["https://example.invalid/mlibc.git"],
                "commit": "a" * 40,
                "treeSha256": ZERO_SHA256,
                "license": "MIT",
            }
        ],
        "archives": [
            {
                "id": "zlib",
                "filename": "zlib.tar.gz",
                "version": "1.0",
                "urls": ["https://example.invalid/zlib.tar.gz"],
                "sha256": ONE_SHA256,
                "license": "Zlib",
            }
        ],
        "patches": [{"id": "mlibc-patch", "path": "patches/mlibc.patch", "sha256": TWO_SHA256}],
        "generatedInputs": [{"id": "font", "path": "assets/font.bin", "sha256": "3" * 64}],
    }


def test_provenance(root: Path, lock_path: Path) -> None:
    artifact = root / "artifact.bin"
    artifact.write_bytes(b"artifact")
    environment = {
        "SOURCE_DATE_EPOCH": "1234",
        "WOS_SOURCE_MODE": "locked",
        "WOS_SOURCE_LOCK": str(lock_path),
        "WOS_SOURCE_STORE": str(root / "source-store"),
    }
    previous = {name: os.environ.get(name) for name in environment}
    os.environ.update(environment)
    try:
        provenance = wos.make_provenance(
            lock_path,
            [("kernel", artifact)],
            [],
            [["fetch", "https://alice:hunter2@example.invalid/src?token=abc", "--password", "hunter2", str(artifact)]],
            list(environment),
            [("test", root)],
            None,
        )
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
    wos.validate_provenance(provenance)
    rendered = json.dumps(provenance)
    check("hunter2" not in rendered and "alice" not in rendered and "token=abc" not in rendered, "secret leaked")
    check("<test>/artifact.bin" in rendered, "workspace path was not normalized")
    check(provenance["environment"]["WOS_SOURCE_LOCK"] == "<test>/source-lock.v1.json", "lock path leaked")
    check(provenance["environment"]["WOS_SOURCE_STORE"] == "<test>/source-store", "store path leaked")
    try:
        wos.make_provenance(lock_path, [], [], [], ["CI_TOKEN"], [("test", root)], None)
    except wos.ArtifactError:
        pass
    else:
        raise AssertionError("non-allowlisted environment variable was accepted")


def test_sbom(root: Path, lock_path: Path) -> None:
    shipped = root / "shipped"
    populate_tree(shipped)
    old_epoch = os.environ.get("SOURCE_DATE_EPOCH")
    os.environ["SOURCE_DATE_EPOCH"] = "1"
    try:
        sbom = wos.make_sbom(lock_path, [("rootfs", shipped)], [("test", root)])
    finally:
        if old_epoch is None:
            os.environ.pop("SOURCE_DATE_EPOCH", None)
        else:
            os.environ["SOURCE_DATE_EPOCH"] = old_epoch
    wos.validate_sbom(sbom)
    check(sbom["creationInfo"]["created"] == "1970-01-01T00:00:01Z", "SBOM timestamp is not reproducible")
    package_categories = {package.get("sourceInfo", "").rsplit(": ", 1)[-1] for package in sbom["packages"][1:]}
    input_categories = {item.get("fileComment", "").rsplit(": ", 1)[-1] for item in sbom["files"] if item.get("fileComment")}
    check(package_categories == {"gitSources", "archives"}, "source-lock packages were omitted")
    check(input_categories == {"patches", "generatedInputs"}, "source-lock file inputs were omitted")
    package_by_name = {package["name"]: package for package in sbom["packages"]}
    check(package_by_name["mlibc"]["checksums"][0]["checksumValue"] == ZERO_SHA256, "Git tree digest omitted")
    check(len(sbom["files"]) == 4, "source inputs or shipped regular files were not enumerated")
    wos.validate_sbom(sbom, strict=True)


def test_cli(root: Path) -> None:
    tree = root / "cli-tree"
    populate_tree(tree)
    manifest = root / "tree.json"
    subprocess.run([sys.executable, str(SCRIPT), "tree-manifest", str(tree), "-o", str(manifest)], check=True)
    subprocess.run([sys.executable, str(SCRIPT), "validate", "tree", str(manifest)], check=True)


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="wos-artifacts-test-") as temporary:
        root = Path(temporary)
        lock_path = root / "source-lock.v1.json"
        lock_path.write_text(json.dumps(source_lock_fixture()), encoding="utf-8")
        test_tree_manifest(root)
        test_elf_comparison(root)
        test_qcow_comparison(root)
        test_provenance(root, lock_path)
        test_sbom(root, lock_path)
        test_cli(root)
    print("wos_artifacts_test: all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
