#!/usr/bin/env python3
"""Execute the real image packager against private, tiny external-tool ports.

These check firmware/menu/manifest wiring, not FAT, qcow2 or libguestfs itself.
All produced images are tiny stand-ins. Limit-boundary inputs are sparse files;
the upload port retains at most 128 bytes and never materializes their holes.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts/build/make_image.sh"
SOURCE_LIMIT = 512 * 1024 * 1024

TOOL_PORT = r'''
import json
import os
from pathlib import Path
import shlex
import shutil
import sys

tool = Path(sys.argv[0]).name
args = sys.argv[1:]
event = {"tool": tool, "args": args}
capture = Path(os.environ["WOS_TEST_CAPTURE"])

def upload(source, destination):
    path = Path(source)
    target = capture / destination.lstrip(":/")
    target.parent.mkdir(parents=True, exist_ok=True)
    with path.open("rb") as data:
        target.write_bytes(data.read(128 if path.stat().st_size > 1024 * 1024 else -1))
    return {"source": str(path), "destination": destination, "bytes": path.stat().st_size}

if tool == "truncate":
    Path(args[-1]).write_bytes(b"")
elif tool == "mformat":
    Path(args[args.index("-i") + 1]).write_bytes(b"tiny FAT stand-in\n")
elif tool in ("sgdisk", "mmd"):
    pass
elif tool == "mcopy":
    event["upload"] = upload(args[-2], args[-1])
elif tool == "dd":
    options = dict(a.split("=", 1) for a in args if "=" in a)
    shutil.copyfile(options["if"], options["of"])
elif tool == "qemu-img":
    if args[0] == "create":
        Path(args[-2]).write_bytes(b"tiny qcow stand-in\n")
    elif args[0] == "convert":
        shutil.copyfile(args[-2], args[-1])
    else:
        raise ValueError("unexpected qemu-img operation")
elif tool == "guestfish":
    event["stdin"] = sys.stdin.read()
    event["uploads"] = []
    for line in event["stdin"].splitlines():
        fields = shlex.split(line)
        if fields and fields[0] == "upload":
            if len(fields) != 3:
                raise ValueError("upload arguments were not quoted")
            event["uploads"].append(upload(fields[1], fields[2]))
else:
    raise ValueError("unexpected tool " + tool)

with open(os.environ["WOS_TEST_TRACE"], "a", encoding="utf-8") as log:
    log.write(json.dumps(event) + "\n")
'''


class Workspace:
    def __init__(self, root):
        self.root = root
        self.build = root / "build"
        self.output = root / "output"
        self.capture = root / "capture"
        self.temp = root / "tmp"
        self.trace = root / "trace.jsonl"
        self.manifest = self.output / "boot.manifest"
        self.disk = self.output / "boot.qcow2"
        for directory in (root / "scripts/build", root / "modules/kern", root / "bin", self.output, self.capture, self.temp,
                          self.build / "modules/kern"):
            directory.mkdir(parents=True, exist_ok=True)
        (self.build / "modules/kern/wos").write_bytes(b"fixture kernel\n")
        self.bootloader = root / "BOOTX64.EFI"
        self.bootloader.write_bytes(b"fixture bootloader\n")
        shutil.copyfile(ROOT / "modules/kern/limine.conf", root / "modules/kern/limine.conf")
        # Retain production image/menu/manifest logic; replace unrelated rootfs
        # construction and host appliance lifecycle with explicit test ports.
        (root / "scripts/build/make_initramfs.sh").write_text(
            '#!/bin/bash\nset -eu\nprintf "fixture initramfs\\n" > "$WOS_BUILD_DIR/initramfs.cpio"\n')
        (root / "scripts/build/qcow_common.sh").write_text(
            'wos_qcow_prepare_libguestfs_env() { :; }\n'
            'wos_qcow_cleanup_libguestfs_env() { :; }\n'
            'wos_qcow_guard_replace() { :; }\n'
            'wos_qcow_guestfish() { shift 2; guestfish "$@"; }\n')
        driver = root / "bin/tool-port.py"
        driver.write_text(f"#!{sys.executable}\n" + TOOL_PORT)
        driver.chmod(0o700)
        for name in ("sgdisk", "mformat", "mmd", "mcopy", "qemu-img", "dd", "truncate", "guestfish"):
            (root / "bin" / name).symlink_to(driver.name)

    def environment(self, firmware=None, generated=False, legacy=False):
        env = {key: value for key, value in os.environ.items() if not key.startswith("WOS_")}
        env.update(PATH=str(self.root / "bin") + os.pathsep + os.defpath,
                   TMPDIR=str(self.temp), WOS_WORKSPACE_ROOT=str(self.root), WOS_BUILD_DIR=str(self.build),
                   WOS_BOOT_DISK=str(self.disk), WOS_BOOTLOADER=str(self.bootloader),
                   WOS_BOOT_IMAGE_MANIFEST=str(self.manifest), WOS_TEST_CAPTURE=str(self.capture),
                   WOS_TEST_TRACE=str(self.trace), WOS_REPRODUCIBLE_BUILD="0" if legacy else "1",
                   WOS_BOOT_IMAGE_LEGACY_GUESTFS="1" if legacy else "0")
        if firmware is not None:
            env["WOS_FIRMWARE_ARCHIVE"] = str(firmware)
        if generated:
            env["WOS_KERNEL_CMDLINE"] = "--firmware-fixture"
        return env

    def run(self, **options):
        result = subprocess.run(["bash", str(SCRIPT)], cwd=self.root, env=self.environment(**options),
                                capture_output=True, text=True, timeout=30)
        allocated = sum(path.lstat().st_blocks * 512 for path in self.root.rglob("*") if not path.is_symlink())
        if allocated > 4 * 1024 * 1024:
            raise AssertionError(f"test port materialized excessive storage: {allocated}")
        return result

    def events(self):
        return [json.loads(line) for line in self.trace.read_text().splitlines()] if self.trace.exists() else []

    def files(self):
        return {fields[1]: fields[2:] for line in self.manifest.read_text().splitlines()
                if (fields := line.split("\t"))[0] == "file"}


class FirmwarePackagingTest(unittest.TestCase):
    def workspace(self):
        temporary = tempfile.TemporaryDirectory(prefix="wos-firmware-packaging-")
        self.addCleanup(temporary.cleanup)
        return Workspace(Path(temporary.name))

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def assert_firmware_binding(self, workspace, generated):
        menu = (workspace.capture / "limine/limine.conf").read_text()
        expected = 1 if generated else len(re.findall(r"^\s*module_path: boot\(\):/initramfs\.cpio\s*$",
                                                      (ROOT / "modules/kern/limine.conf").read_text(), re.MULTILINE))
        self.assertGreater(expected, 0)
        self.assertEqual(menu.count("module_path: boot():/firmware.cpio"), expected)
        self.assertEqual(menu.count("module_string: wos-firmware-v1"), expected)
        self.assertEqual(len(re.findall(r"module_path: boot\(\):/initramfs\.cpio\s*\n"
                                        r"\s*module_path: boot\(\):/firmware\.cpio\s*\n"
                                        r"\s*module_string: wos-firmware-v1", menu)), expected)
        if generated:
            self.assertIn("cmdline: --firmware-fixture", menu)
        self.assertEqual(workspace.files()["limine"][2], "/limine/limine.conf")
        self.assertEqual(workspace.files()["limine"][1], hashlib.sha256(menu.encode()).hexdigest())

    def test_optional_absence_preserves_stock_and_generated_menus_for_both_backends(self):
        for generated in (False, True):
            for legacy in (False, True):
                with self.subTest(generated=generated, legacy=legacy):
                    workspace = self.workspace()
                    self.assert_success(workspace.run(generated=generated, legacy=legacy))
                    menu = (workspace.capture / "limine/limine.conf").read_text()
                    self.assertNotIn("firmware.cpio", menu)
                    self.assertNotIn("wos-firmware-v1", menu)
                    self.assertNotIn("firmware", workspace.files())
                    self.assertFalse((workspace.capture / "firmware.cpio").exists())
                    if not generated:
                        self.assertEqual(menu, (ROOT / "modules/kern/limine.conf").read_text())

    def test_both_backends_bind_module_upload_exact_bytes_and_hash_quoted_source(self):
        for generated in (False, True):
            for legacy in (False, True):
                with self.subTest(generated=generated, legacy=legacy):
                    workspace = self.workspace()
                    firmware = workspace.root / 'firmware "quoted" \\ $literal `ticks`.cpio'
                    payload = b"synthetic redistributable firmware archive\n"
                    firmware.write_bytes(payload)
                    self.assert_success(workspace.run(firmware=firmware, generated=generated, legacy=legacy))
                    self.assert_firmware_binding(workspace, generated)
                    self.assertEqual((workspace.capture / "firmware.cpio").read_bytes(), payload)
                    self.assertEqual(workspace.files()["firmware"],
                                     [str(len(payload)), hashlib.sha256(payload).hexdigest(), str(firmware.resolve())])
                    events = workspace.events()
                    uploads = [event["upload"] for event in events if event["tool"] == "mcopy"]
                    uploads += [upload for event in events if event["tool"] == "guestfish" for upload in event["uploads"]]
                    actual = [upload for upload in uploads if upload["destination"].lstrip(":/") == "firmware.cpio"]
                    self.assertEqual(actual, [{"source": str(firmware.resolve()),
                                               "destination": "/firmware.cpio" if legacy else "::/firmware.cpio",
                                               "bytes": len(payload)}])

    def test_identical_payload_skips_image_creation_and_changed_firmware_invalidates_manifest(self):
        for generated in (False, True):
            for legacy in (False, True):
                with self.subTest(generated=generated, legacy=legacy):
                    workspace = self.workspace()
                    firmware = workspace.root / "firmware.cpio"
                    firmware.write_bytes(b"first firmware\n")
                    options = dict(firmware=firmware, generated=generated, legacy=legacy)
                    self.assert_success(workspace.run(**options))
                    original = workspace.manifest.read_bytes()
                    calls = workspace.events()
                    result = workspace.run(**options)
                    self.assert_success(result)
                    self.assertIn("payload unchanged", result.stdout)
                    self.assertEqual(workspace.manifest.read_bytes(), original)
                    self.assertEqual(workspace.events(), calls)
                    firmware.write_bytes(b"second firmware\n")
                    self.assert_success(workspace.run(**options))
                    self.assertNotEqual(workspace.manifest.read_bytes(), original)
                    self.assertGreater(len(workspace.events()), len(calls))
                    self.assertEqual((workspace.capture / "firmware.cpio").read_bytes(), firmware.read_bytes())

    def test_missing_directory_separator_and_oversized_sources_refuse_before_packaging(self):
        for kind in ("missing", "directory", "newline", "carriage", "tab", "oversized", "oversized_symlink",
                     "separator_symlink", "trailing_newline_symlink"):
            with self.subTest(kind=kind):
                workspace = self.workspace()
                firmware = workspace.root / "firmware.cpio"
                if kind == "directory":
                    firmware.mkdir()
                elif kind in ("newline", "carriage", "tab"):
                    firmware = workspace.root / ("firmware" + {"newline": "\n", "carriage": "\r", "tab": "\t"}[kind] + ".cpio")
                    firmware.write_bytes(b"fixture")
                elif kind in ("oversized", "oversized_symlink"):
                    target = workspace.root / "oversized-target.cpio"
                    with target.open("wb") as stream:
                        stream.truncate(SOURCE_LIMIT + 1)
                    if kind == "oversized_symlink":
                        firmware.symlink_to(target.name)
                    else:
                        firmware = target
                elif kind == "separator_symlink":
                    target = workspace.root / "bad\tname.cpio"
                    target.write_bytes(b"fixture")
                    firmware.symlink_to(target.name)
                elif kind == "trailing_newline_symlink":
                    target = workspace.root / "target.cpio\n"
                    target.write_bytes(b"actual symlink target")
                    # A command substitution that strips the target newline
                    # must not quietly substitute this different existing file.
                    (workspace.root / "target.cpio").write_bytes(b"shadow file")
                    firmware.symlink_to(target.name)
                result = workspace.run(firmware=firmware)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertFalse(workspace.disk.exists())
                self.assertFalse(workspace.manifest.exists())
                self.assertEqual(workspace.events(), [])
                self.assertFalse((workspace.build / "initramfs.cpio").exists())

    def test_exact_archive_limit_is_admitted_without_materializing_sparse_payload(self):
        workspace = self.workspace()
        firmware = workspace.root / "boundary.cpio"
        with firmware.open("wb") as stream:
            stream.truncate(SOURCE_LIMIT)
        self.assert_success(workspace.run(firmware=firmware, generated=True))
        self.assert_firmware_binding(workspace, True)
        self.assertEqual(workspace.files()["firmware"][0], str(SOURCE_LIMIT))
        digest = hashlib.sha256()
        zero = bytes(1024 * 1024)
        for _ in range(SOURCE_LIMIT // len(zero)):
            digest.update(zero)
        self.assertEqual(workspace.files()["firmware"][1], digest.hexdigest())
        self.assertEqual((workspace.capture / "firmware.cpio").stat().st_size, 128)


if __name__ == "__main__":
    unittest.main()
