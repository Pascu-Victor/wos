#!/usr/bin/env python3
"""Exercise installed directory modes in both rootfs staging implementations."""
import argparse
import importlib.util
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("wos_rootfs_delta", ROOT / "scripts/build/rootfs_delta.py")
delta = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = delta
spec.loader.exec_module(delta)

qcow_spec = importlib.util.spec_from_file_location("wos_rootfs_qcow_delta", ROOT / "scripts/build/rootfs_qcow_xfs_delta.py")
qcow = importlib.util.module_from_spec(qcow_spec)
sys.modules[qcow_spec.name] = qcow
qcow_spec.loader.exec_module(qcow)


class DirectoryPolicy(unittest.TestCase):
    def test_non_power_of_two_allocation_groups_use_linear_disk_offsets(self):
        output = ("blocksize = 512\ndblocks = 30\nagblocks = 12\nagcount = 3\nagblklog = 4\n"
                  "core.mode = 0100644\ncore.size = 512\ncore.nblocks = 1\ncore.format = 2\n"
                  "data offset 0 startblock 19 (1/3) count 1 flag 0\n")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            image = root / "partition"
            source = root / "source"
            source.write_bytes(b"n" * 512)
            original = b"o" * (30 * 512)
            image.write_bytes(original)
            with patch.object(qcow, "xfs_db", return_value=output):
                self.assertEqual(qcow.read_inode_info(image, "/file").extents[0].fs_block, 15)
                qcow.copy_regular_file(image, source, "/file", 0o644)
                qcow.verify_regular_file(image, source, "/file", 0o644)
            expected = original[:15 * 512] + source.read_bytes() + original[16 * 512:]
            self.assertEqual(image.read_bytes(), expected)
            self.assertEqual(image.read_bytes()[19 * 512:20 * 512], b"o" * 512)

    def test_xfs_extent_geometry_and_unwritten_ranges_are_rejected(self):
        output = ("blocksize = 4096\ndblocks = 26214400\nagblocks = 6553600\nagcount = 4\nagblklog = 23\n"
                  "core.mode = 0100755\ncore.size = 1321576\ncore.nblocks = 323\ncore.format = 2\n"
                  "data offset 0 startblock 8413599 (1/24991) count 323 flag 0\n")
        with patch.object(qcow, "xfs_db", return_value=output):
            self.assertEqual(qcow.read_inode_info(Path("fixture"), "/file").extents[0].fs_block, 6578591)
        cases = [output.replace("agblklog = 23", "agblklog = 22"),
                 output.replace("agcount = 4", "agcount = 5"),
                 output.replace("startblock 8413599", "startblock 33554432"),
                 output.replace("startblock 8413599", "startblock 14942207"),
                 output.replace("startblock 8413599", "startblock 31719423"),
                 output.replace("count 323", "count 0"),
                 output.replace("flag 0", "flag 1"),
                 output + "data offset 1 startblock 8415000 (1/26392) count 2 flag 0\n"]
        for invalid in cases:
            with self.subTest(invalid=invalid), patch.object(qcow, "xfs_db", return_value=invalid):
                with self.assertRaises(RuntimeError):
                    qcow.read_inode_info(Path("fixture"), "/file")

    def test_qcow_fuse_exit_waits_for_daemon_and_flushes_disk(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            disk = root / "disk.qcow2"
            disk.write_bytes(b"fixture")
            fuse = qcow.QcowPartitionFuse(disk, qcow.Partition(1, 1), root)
            fuse.part_file.touch()
            fuse.pid_file.write_text("1234\n")
            stopped = []
            with patch.object(qcow.shutil, "which", side_effect=lambda name: f"/usr/bin/{name}"), \
                 patch.object(qcow, "run", return_value=subprocess.CompletedProcess([], 0, "", "")) as command, \
                 patch.object(qcow.os, "kill") as kill, \
                 patch.object(qcow.QcowPartitionFuse, "_wait_stopped", side_effect=lambda pid: stopped.append(pid)), \
                 patch.object(qcow.os, "fsync") as fsync:
                fuse.__exit__(None, None, None)
            command.assert_called_once_with(["fusermount3", "-u", str(fuse.part_file)])
            kill.assert_called_once_with(1234, 15)
            self.assertEqual(stopped, [1234])
            fsync.assert_called_once()

    def test_qcow_fuse_exit_rejects_unmount_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            disk = root / "disk.qcow2"
            disk.write_bytes(b"fixture")
            fuse = qcow.QcowPartitionFuse(disk, qcow.Partition(1, 1), root)
            fuse.part_file.touch()
            with patch.object(qcow.shutil, "which", return_value="/usr/bin/fusermount3"), \
                 patch.object(qcow, "run", return_value=subprocess.CompletedProcess([], 1, "", "busy")):
                with self.assertRaisesRegex(RuntimeError, "busy"):
                    fuse.__exit__(None, None, None)

    def test_qcow_fuse_wait_has_a_bounded_timeout(self):
        clock = iter((10.0, 10.5, 11.1))
        with patch.object(qcow.QcowPartitionFuse, "_process_stopped", return_value=False), \
             patch.object(qcow.time, "monotonic", side_effect=lambda: next(clock)), \
             patch.object(qcow.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "did not stop"):
                qcow.QcowPartitionFuse._wait_stopped(1234, timeout_seconds=1.0)

    def test_full_staging_does_not_install_mktemp_mode(self):
        with tempfile.TemporaryDirectory() as temporary:
            staging = Path(temporary) / "staging"
            staging.mkdir(mode=0o700)
            # Skip payload collection; exercise the real stage-tree orchestration,
            # directory declarations and manifests on a small disposable fixture.
            script = '''
source "$1/scripts/build/rootfs_common.sh"
for helper in sysroot_libs sysroot_headers sysroot_cmake_data sysroot_python busybox_install manifest etc root_home srv; do
    eval "rootfs_stage_${helper}() { :; }"
done
rootfs_finalize_usr_merge() { :; }
rootfs_stage_tree "$1" "$2"
'''
            subprocess.run(["bash", "-eu", "-c", script, "fixture", str(ROOT), str(staging)], check=True)
            self.assertEqual(staging.stat().st_mode & 0o7777, 0o755)
            self.assertEqual((staging / "var/log/journal").stat().st_mode & 0o7777, 0o700)
            manifest = (staging / "etc/wos-rootfs-manifest.tsv").read_text()
            self.assertIn("/\tD\t755\t", manifest)
            self.assertIn("/var/log/journal\tD\t700\t", manifest)
            self.assertNotIn("/", (staging / "etc/wos-managed-paths").read_text().splitlines())

    def test_full_and_incremental_service_accounts_match(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            full = root / "full"
            full.mkdir()
            script = """
source "$1/scripts/build/rootfs_common.sh"
ROOTFS_REPO="$1"
ROOTFS_STAGING="$2"
ROOTFS_MANAGED_TMP="$2/managed"
ROOTFS_REPRODUCIBLE=1
umask 022
rootfs_stage_etc
"""
            subprocess.run(["bash", "-eu", "-c", script, "fixture", str(ROOT), str(full)], check=True)
            args = argparse.Namespace(repo=ROOT, **{name: root / name for name in
                ("staging", "cache", "new_cache", "stamp", "changed_paths", "tar_paths")})
            collector = delta.RootfsDelta(args)
            collector.collect_generated_etc()
            self.assertEqual(collector.entries["/etc/dropbear"].mode, 0o700)
            self.assertEqual((full / "etc/dropbear").stat().st_mode & 0o777, 0o700)
            expected = {
                "/etc/passwd": b"root:!:0:0:root:/root:/bin/bash\nhttpd:!:100:100:HTTP server:/nonexistent:/bin/false\njournald:!:101:101:Journal daemon:/nonexistent:/bin/false\nnetd:!:102:102:Network daemon:/nonexistent:/bin/false\nkeygen:!:103:103:Host key generator:/nonexistent:/bin/false\nsshd:!:104:104:SSH network worker:/nonexistent:/bin/false\n",
                "/etc/group": b"root:x:0:root\nhttpd:x:100:\njournald:x:101:\nnetd:x:102:\nkeygen:x:103:\nsshd:x:104:\n",
            }
            old = delta.RootfsDelta(args)
            old.collect_generated_etc()
            old.add_generated_file("/etc/passwd", b"root:!:0:0:root:/root:/bin/bash\n")
            old.add_generated_file("/etc/group", b"root:x:0:root\n")
            changed = collector.changed_targets({key: entry.state_line() for key, entry in old.entries.items()})
            self.assertEqual(changed, set(expected))
            for path, content in expected.items():
                entry = collector.entries[path]
                self.assertEqual(entry.generated, content)
                self.assertEqual(entry.mode, 0o644)
                self.assertEqual((full / path.lstrip("/")).read_bytes(), content)
                self.assertEqual((full / path.lstrip("/")).stat().st_mode & 0o777, 0o644)
                collector.stage_entry(entry)
                self.assertEqual((args.staging / path.lstrip("/")).read_bytes(), content)

    def test_root_is_allowed_only_for_verified_directory_metadata(self):
        output = ("blocksize = 4096\ndblocks = 24\nagblocks = 12\nagcount = 2\nagblklog = 4\n"
                  "core.mode = 040700\ncore.size = 6\ncore.nblocks = 0\ncore.format = 1\n")
        with patch.object(qcow, "xfs_db", return_value=output) as database:
            qcow.verify_existing_directory(Path("fixture"), "/", 0o755)
            self.assertEqual(database.call_args.args[1], ["path /", "write core.uid 0", "write core.gid 0", "write core.mode 040755", "crc -r"])
            with self.assertRaises(RuntimeError):
                qcow.read_inode_info(Path("fixture"), "/")
            for path in ("/../root", "/var/../root", "/a b", "/a\nwrite core.mode 040777"):
                with self.assertRaises(RuntimeError):
                    qcow.verify_existing_directory(Path("fixture"), path, 0o755)
        output = output.replace("040700", "0100700")
        with patch.object(qcow, "xfs_db", return_value=output) as database:
            with self.assertRaises(RuntimeError):
                qcow.verify_existing_directory(Path("fixture"), "/", 0o755)
            self.assertEqual(database.call_count, 1)

    @unittest.skipUnless(shutil.which("mkfs.xfs") and shutil.which("xfs_db"), "real XFS tools are unavailable")
    def test_real_xfs_delta_restores_managed_owners_and_preserves_service_state(self):
        with tempfile.TemporaryDirectory(prefix="wos-rootfs-owner-") as temporary:
            root = Path(temporary)
            old = root / "old"
            old.write_bytes(b"x" * 4096)
            source = root / "passwd"
            source.write_bytes(b"root:!:0:0:root:/root:/bin/bash\n")
            large = root / "large"
            large.write_bytes(b"z" * 8192)
            prototype = root / "prototype"
            prototype.write_text(
                "dummy\n0 0\nd--750 1001 2001\n"
                "etc d--750 1002 2002\npasswd ---640 1003 2003 " + str(old) + "\n$\n"
                "service d--700 101 101\nstate ---600 101 101 " + str(old) + "\n$\n$\n")
            image = root / "rootfs.xfs"
            with image.open("wb") as handle:
                handle.truncate(400 * 1024 * 1024)
            result = subprocess.run(["mkfs.xfs", "-f", "-m", "reflink=0,rmapbt=0", "-p", str(prototype), str(image)],
                                    text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

            def metadata(path):
                output = qcow.xfs_db(image, ["path " + path, "p core.uid core.gid core.mode core.size"])
                return tuple(qcow.parse_int_field(output, field, base=8 if field == "core.mode" else 10)
                             for field in ("core.uid", "core.gid", "core.mode", "core.size"))

            def data(path):
                inode = qcow.read_inode_info(image, path)
                self.assertEqual(len(inode.extents), 1)
                with image.open("rb") as handle:
                    handle.seek(inode.extents[0].fs_block * inode.block_size)
                    return handle.read(inode.size)

            original = metadata("/etc/passwd")
            self.assertEqual(original[:2], (1003, 2003))
            self.assertEqual(metadata("/")[:2], (1001, 2001))
            self.assertEqual(metadata("/etc")[:2], (1002, 2002))
            preserved = (metadata("/service"), metadata("/service/state"), data("/service/state"))
            with self.assertRaises(RuntimeError):
                qcow.copy_regular_file(image, large, "/etc/passwd", 0o600)
            with self.assertRaises(RuntimeError):
                qcow.copy_regular_file(image, source, "/etc", 0o600)
            self.assertEqual(metadata("/etc/passwd"), original)
            self.assertEqual(data("/etc/passwd"), old.read_bytes())
            for _ in range(2):
                qcow.copy_regular_file(image, source, "/etc/passwd", 0o644)
                qcow.verify_regular_file(image, source, "/etc/passwd", 0o644)
                qcow.verify_existing_directory(image, "/etc", 0o755)
                qcow.verify_existing_directory(image, "/", 0o755)
                self.assertEqual(metadata("/etc/passwd"), (0, 0, 0o100644, source.stat().st_size))
                self.assertEqual(data("/etc/passwd"), source.read_bytes())
                self.assertEqual(metadata("/")[:3], (0, 0, 0o40755))
                self.assertEqual(metadata("/etc")[:3], (0, 0, 0o40755))
                self.assertEqual((metadata("/service"), metadata("/service/state"), data("/service/state")), preserved)
            source.write_bytes(b"different persisted contents\n")
            with self.assertRaisesRegex(RuntimeError, "size is|data verification failed"):
                qcow.verify_regular_file(image, source, "/etc/passwd", 0o644)
            if shutil.which("xfs_repair"):
                checked = subprocess.run(["xfs_repair", "-n", str(image)], text=True, capture_output=True)
                self.assertEqual(checked.returncode, 0, checked.stdout + checked.stderr)

    def test_incremental_root_and_journal_modes_are_persistent(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = argparse.Namespace(repo=ROOT, **{name: root / name for name in
                ("staging", "cache", "new_cache", "stamp", "changed_paths", "tar_paths")})
            args.staging.mkdir(mode=0o700)
            collector = delta.RootfsDelta(args)
            collector.collect_misc()
            self.assertEqual(collector.entries["/"].mode, 0o755)
            self.assertEqual(collector.entries["/var/log/journal"].mode, 0o700)
            self.assertNotIn("/", collector.managed_paths)
            # Model an older source cache that omitted root and declared the
            # journal public. Both modes must enter the next installed delta.
            old = {key: entry.state_line() for key, entry in collector.entries.items() if key != "/"}
            old["/var/log/journal"] = old["/var/log/journal"].replace("\tdir\t700\t", "\tdir\t755\t")
            changed = collector.changed_targets(old)
            self.assertEqual(changed, {"/", "/var/log/journal"})
            for name in sorted(changed):
                collector.stage_entry(collector.entries[name])
            collector.write_path_lists(changed)
            self.assertEqual(args.tar_paths.read_text().splitlines(), [".", "var/log/journal"])
            self.assertEqual(args.staging.stat().st_mode & 0o7777, 0o755)
            self.assertEqual((args.staging / "var/log/journal").stat().st_mode & 0o7777, 0o700)
            collector.write_state_files(changed)
            self.assertEqual(collector.changed_targets(collector.read_state(args.new_cache)), set())

    def test_incremental_payload_keeps_host_only_state_out_of_image(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = argparse.Namespace(repo=ROOT, **{name: root / name for name in
                ("staging", "cache", "new_cache", "stamp", "changed_paths", "tar_paths")})
            collector = delta.RootfsDelta(args)
            collector.entries["/usr/bin/example"] = delta.Entry(
                "/usr/bin/example", "file", 0o755, 1, 1, "", "/host/example"
            )
            changed = {"/usr/bin/example"}
            collector.write_state_files(changed)
            self.assertEqual(changed, {"/usr/bin/example", "/etc/wos-rootfs-manifest.tsv"})
            self.assertNotIn("/etc/wos-managed-paths", changed)
            self.assertNotIn("/etc/wos-rootfs-source-state.tsv", changed)
            self.assertTrue(args.new_cache.is_file())


if __name__ == "__main__":
    unittest.main()
