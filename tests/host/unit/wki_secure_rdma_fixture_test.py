#!/usr/bin/env python3
"""Preparation-only Goal 25 fixtures tested through the real cluster schema."""

import contextlib
import importlib.util
import io
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.dont_write_bytecode = True
HELPER = ROOT / "scripts/test/wki_secure_rdma_fixture.py"
module_spec = importlib.util.spec_from_file_location("wki_secure_rdma_fixture", HELPER)
fixture = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(fixture)


class FixtureTests(unittest.TestCase):
    def setUp(self):
        # Synthetic QMP paths must fit the real UNIX socket limit even when
        # the build wrapper selects a deeply nested TMPDIR.
        self.temp = tempfile.TemporaryDirectory(prefix="wki-secure-fixture-test-", dir="/tmp")
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name)
        (self.repo / "ktest-data").mkdir()
        self.boot = self.repo / "packaged-boot.qcow2"
        self.rootfs = self.repo / "packaged-rootfs.qcow2"
        self.kernel = self.repo / "kernel"
        self.evidence = self.repo / "evidence.json"
        for path, data in ((self.boot, b"opaque boot input"), (self.rootfs, b"opaque rootfs input"),
                           (self.kernel, b"kernel artifact"), (self.evidence, b'{"status":"pending"}\n')):
            path.write_bytes(data)
        # No host commands or QEMU/overlay/credential builders may be reached.
        self.traps = ExitStack()
        self.addCleanup(self.traps.close)
        for owner, name in ((fixture.node_setup, "build_qemu_args"),
                            (fixture.node_setup, "prepare_node_overlays"),
                            (fixture.cluster_setup, "build_qemu_args"),
                            (fixture.cluster_setup, "provision_wki_auth"),
                            (fixture.cluster_setup, "launch_guarded"),
                            (fixture.cluster_setup, "setup"),
                            (fixture.cluster_setup, "teardown"),
                            (fixture.cluster_setup, "run_topology_probe"),
                            (fixture.cluster_setup.subprocess, "Popen"),
                            (fixture.cluster_setup.subprocess, "run")):
            self.traps.enter_context(patch.object(owner, name, side_effect=AssertionError(f"forbidden {name}")))

    def construct(self, **overrides):
        values = {"state_root": self.repo / "ktest-data/goal25-unit", "run_id": "unit",
                  "profile": "candidate", "mode": "ethernet-roce", "group": "239.192.25.40",
                  "port": 47951, "boot": self.boot, "rootfs": self.rootfs,
                  "profile_tokens": ["wki.secure-pending"], "repo": self.repo}
        values.update(overrides)
        return fixture.construct_fixture(**values)

    def prepare(self, spec):
        return fixture.prepare_fixture(spec, kernel=self.kernel, evidence=[self.evidence], repo=self.repo)

    def test_dry_constructor_is_read_only_actual_schema_and_policies(self):
        before = sorted(str(p.relative_to(self.repo)) for p in self.repo.rglob("*"))
        spec = self.construct(identity_policy="same-domain-root")
        json.loads(json.dumps(spec))
        self.assertEqual(before, sorted(str(p.relative_to(self.repo)) for p in self.repo.rglob("*")))
        nodes = fixture.cluster_setup.collect_unique_nodes(spec["config"])
        policies = fixture.cluster_setup.configured_wki_peer_policies(nodes)
        self.assertEqual(set(policies), {(248, 249), (249, 248)})
        for index, node in enumerate(spec["node_specs"]):
            self.assertEqual(node["id"], 248 + index)
            self.assertEqual(len(node["nics"]), 1)
            self.assertEqual(node["nics"][0]["queues"], 1)
            self.assertFalse(node["nics"][0]["vhost"])
            self.assertEqual(node["vm"]["disk_cache"], "writeback")
            self.assertTrue(node["vm"]["iommu"])
            for key in ("disk0", "disk1", "overlay_dir", "serial_log", "qemu_log", "qmp_socket"):
                self.assertTrue(Path(node["vm"][key]).is_relative_to(Path(spec["state_root"])))
            self.assertTrue(Path(spec["credential_paths"][index]).is_relative_to(Path(node["vm"]["overlay_dir"])))
        self.assertEqual(spec["launcher_environment"], {"TMPDIR": "/tmp"})
        self.assertIn("--no-setup", spec["planned_launcher_argv"])
        self.assertEqual(spec["node_specs"][0]["nics"][0]["qemu_netdev"], spec["node_specs"][1]["nics"][0]["qemu_netdev"])
        self.assertFalse(spec["launch_ready"])
        self.assertFalse(spec["secure_runtime_qualified"])
        self.assertFalse(spec["kernel_in_images_verified"])
        self.assertFalse(spec["init_manifest_validated"])
        self.assertFalse(spec["profile_tokens_installed_verified"])

    def test_ivshmem_peer_wire_ids_and_ethernet_isolation(self):
        for mode in ("ivshmem-plain", "ivshmem-doorbell"):
            with self.subTest(mode=mode):
                spec = self.construct(mode=mode)
                a, b = spec["node_specs"]
                self.assertEqual((a["ivshmem"][0]["role"], b["ivshmem"][0]["role"]), (0, 1))
                self.assertEqual((a["ivshmem"][0]["peer"], b["ivshmem"][0]["peer"]), (250, 249))
                self.assertNotEqual(a["nics"][0]["qemu_netdev"], b["nics"][0]["qemu_netdev"])
                if mode == "ivshmem-doorbell":
                    self.assertIsNone(spec["planned_launcher_argv"])
                    self.assertIsNone(spec["plain_backing"])
                    self.assertTrue(any("concurrent" in text for text in spec["doorbell_requirements"]))
                else:
                    self.assertEqual(a["ivshmem"][0]["path"], b["ivshmem"][0]["path"])
                    self.assertEqual(spec["plain_backing_bytes"], 16 * 1024 * 1024)

    def test_public_policy_schema_rejects_wire_ids_and_empty_privilege_mask(self):
        for policy in ({"peer_policy": {"250": 1023}}, {"peer_policy": {"249": 0}},
                       {"peer_identity": {"249": "allow-all"}}):
            spec = self.construct()
            spec["config"]["zones"][1]["nodes_config"][0]["wki_auth"] = policy
            nodes = fixture.cluster_setup.collect_unique_nodes(spec["config"])
            with self.assertRaises(ValueError):
                fixture.cluster_setup.configured_wki_peer_policies(nodes)

    def test_actual_cli_dry_run_has_no_filesystem_side_effects(self):
        state = ROOT / "ktest-data" / f"goal25-unit-cli-{os.getpid()}"
        self.assertFalse(os.path.lexists(state))
        argv = [str(HELPER), "--dry-run", "--mode", "ethernet-roce", "--profile", "baseline",
                "--run-id", "cli", "--state-root", str(state), "--group", "239.192.25.60",
                "--port", "47971", "--boot", str(self.boot), "--rootfs", str(self.rootfs),
                "--profile-token", "baseline-pending"]
        output = io.StringIO()
        with patch.object(sys, "argv", argv), contextlib.redirect_stdout(output):
            self.assertEqual(fixture.main(), 0)
        parsed = json.loads(output.getvalue())
        self.assertEqual(parsed["profile_tokens"], ["baseline-pending"])
        self.assertFalse(parsed["profile_tokens_installed_verified"])
        self.assertFalse(os.path.lexists(state))

    def test_invalid_arguments_fail_before_state_mutation(self):
        invalid = ({"group": "127.0.0.1"}, {"group": "239.0.0.1"}, {"group": "ff02::1"},
                   {"port": True}, {"port": 0}, {"port": 65536},
                   {"mode": "ivshmem-plain", "port": 65535},
                   {"mode": "ivshmem-plain", "group": "239.195.255.255"},
                   {"node_ids": (248, 248)}, {"node_ids": (249, 248)}, {"node_ids": (False, 1)},
                   {"identity_policy": "allow-all"}, {"profile": "secure-enabled"},
                   {"profile_tokens": []}, {"profile_tokens": ["a\nb"]},
                   {"run_id": "../escape"}, {"state_root": self.repo / "build/goal25-unit"})
        for values in invalid:
            with self.subTest(values=values), self.assertRaises(ValueError):
                self.construct(**values)
        self.assertFalse((self.repo / "ktest-data/goal25-unit").exists())

    def test_unix_socket_path_bound_and_symlink_escape(self):
        with self.assertRaises(ValueError):
            self.construct(state_root=self.repo / "ktest-data" / ("goal25-" + "x" * 110))
        (self.repo / "ktest-data").rmdir()
        outside = self.repo / "outside"
        outside.mkdir()
        (self.repo / "ktest-data").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(ValueError):
            self.construct()
        self.assertEqual(list(outside.iterdir()), [])

    def test_existing_state_and_backing_are_never_overwritten(self):
        state = self.repo / "ktest-data/goal25-unit"
        state.mkdir()
        marker = state / "marker"
        marker.write_bytes(b"retained")
        with self.assertRaises(ValueError):
            self.construct()
        self.assertEqual(marker.read_bytes(), b"retained")
        backing = self.repo / "already-shared"
        backing.write_bytes(b"do not truncate")
        with self.assertRaises(FileExistsError):
            fixture.create_plain_backing(backing)
        self.assertEqual(backing.read_bytes(), b"do not truncate")

    def test_private_preparation_copies_pins_and_permissions(self):
        for mode in fixture.MODES:
            with self.subTest(mode=mode):
                spec = self.construct(mode=mode, state_root=self.repo / "ktest-data" / f"goal25-{mode}")
                source_bytes = (self.boot.read_bytes(), self.rootfs.read_bytes())
                result = self.prepare(spec)
                state = Path(spec["state_root"])
                self.assertTrue(result["completed"])
                self.assertEqual(result["errors"], [])
                self.assertEqual(result["launches"], 0)
                self.assertEqual(source_bytes, (self.boot.read_bytes(), self.rootfs.read_bytes()))
                self.assertEqual(result["kernel_artifact"]["sha256"], fixture.digest(self.kernel))
                self.assertEqual(result["caller_evidence"][0]["sha256"], fixture.digest(self.evidence))
                self.assertEqual(len(result["copied_images"]), 4)
                for copied in result["copied_images"]:
                    path = Path(copied["path"])
                    original = self.boot if path.name == "boot.qcow2" else self.rootfs
                    self.assertEqual(path.read_bytes(), original.read_bytes())
                    self.assertNotEqual(path.stat().st_ino, original.stat().st_ino)
                for path in (state, *state.rglob("*")):
                    self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o700 if path.is_dir() else 0o600)
                self.assertFalse(any(Path(path).exists() for path in spec["credential_paths"]))
                if spec["plain_backing"]:
                    self.assertEqual(Path(spec["plain_backing"]).stat().st_size, fixture.BACKING_BYTES)
                fixture.cluster_setup.validate_no_setup_topology(spec["config"])
                with self.assertRaises(ValueError):
                    self.prepare(spec)

    def test_tampered_serialized_specs_cannot_escape_private_root(self):
        for mutate in (lambda s: s.__setitem__("plain_backing", str(self.repo / "outside-backing")),
                       lambda s: s["config"]["zones"][1]["nodes_config"][0]["vm"].__setitem__("disk1", str(self.rootfs)),
                       lambda s: s["node_specs"][0]["ivshmem"][0].__setitem__("peer", 251)):
            spec = self.construct(mode="ivshmem-plain")
            mutate(spec)
            with self.assertRaises(ValueError):
                self.prepare(spec)
            self.assertFalse(Path(spec["state_root"]).exists())
        self.assertFalse((self.repo / "outside-backing").exists())

    def test_preparation_rejects_unvalidated_log_path(self):
        spec = self.construct()
        outside_log = self.repo / "outside-serial.log"
        spec["config"]["zones"][1]["nodes_config"][0]["vm"]["serial_log"] = str(outside_log)
        with self.assertRaises(ValueError):
            self.prepare(spec)
        self.assertFalse(Path(spec["state_root"]).exists())
        self.assertFalse(outside_log.exists())

    def test_missing_evidence_and_symlink_input_reject_before_root_creation(self):
        spec = self.construct()
        with self.assertRaises(ValueError):
            fixture.prepare_fixture(spec, kernel=self.kernel, evidence=[], repo=self.repo)
        alias = self.repo / "boot-alias"
        alias.symlink_to(self.boot)
        spec = self.construct(boot=alias)
        with self.assertRaises(ValueError):
            self.prepare(spec)
        self.assertFalse(Path(spec["state_root"]).exists())

    def test_source_pin_mismatch_refuses_copy_and_preserves_existing_target(self):
        target = self.repo / "target"
        pin = fixture.file_pin(self.boot)
        self.boot.write_bytes(b"changed source")
        with self.assertRaises(ValueError):
            fixture.copy_pinned(self.boot, target, pin)
        self.assertFalse(target.exists())
        target.write_bytes(b"existing")
        with self.assertRaises(FileExistsError):
            fixture.copy_pinned(self.boot, target, fixture.file_pin(self.boot))
        self.assertEqual(target.read_bytes(), b"existing")

    def test_source_change_during_copy_and_failed_prep_remain_unqualified(self):
        original_pin = fixture.file_pin

        def source_change(path):
            pin = original_pin(path)
            if path.name == "boot.qcow2":
                self.boot.write_bytes(b"concurrent writer")
            return pin

        spec = self.construct()
        with patch.object(fixture, "file_pin", side_effect=source_change), self.assertRaises(ValueError):
            self.prepare(spec)
        result = json.loads((Path(spec["state_root"]) / "preparation-result.json").read_text())
        self.assertFalse(result["completed"])
        self.assertEqual(result["launches"], 0)
        self.assertEqual(len(result["errors"]), 1)
        self.assertTrue((Path(spec["state_root"]) / "a/boot.qcow2").exists())
        self.assertFalse((Path(spec["state_root"]) / "cluster.json").exists())


@unittest.skipUnless(shutil.which("qemu-img") and shutil.which("qemu-io"), "qemu image tools unavailable")
class OverlayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="wki-overlay-", dir="/tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.base = self.root / "base.qcow2"
        subprocess.run(["qemu-img", "create", "-f", "qcow2", str(self.base), "64M"],
                       check=True, stdout=subprocess.DEVNULL)
        subprocess.run(["qemu-io", "-f", "qcow2", "-c", "write -P 0x44 0 1M", str(self.base)],
                       check=True, stdout=subprocess.DEVNULL)
        self.base.chmod(0o444)
        self.expected = fixture.file_pin(self.base)

    def test_real_overlay_preserves_base_and_isolates_private_writes(self):
        a, b = self.root / "a.qcow2", self.root / "b.qcow2"
        for image in (a, b):
            receipt = fixture.overlay_pinned(self.base, image, self.expected)
            self.assertEqual(receipt["backing"], self.expected)
            self.assertEqual(receipt["image_mode"], "qcow2-overlay")
            self.assertEqual(stat.S_IMODE(image.stat().st_mode), 0o600)
            self.assertLess(image.stat().st_blocks * 512, 1024 * 1024)
        subprocess.run(["qemu-io", "-f", "qcow2", "-c", "write -P 0x55 0 1M", str(a)],
                       check=True, stdout=subprocess.DEVNULL)
        for image, pattern in ((self.base, "0x44"), (b, "0x44"), (a, "0x55")):
            subprocess.run(["qemu-io", "-r", "-f", "qcow2", "-c", f"read -P {pattern} 0 1M", str(image)],
                           check=True, stdout=subprocess.DEVNULL)
        self.assertEqual(fixture.file_pin(self.base), self.expected)
        self.assertLess(a.stat().st_blocks * 512, 4 * 1024 * 1024)

    def test_writable_changed_indirect_and_existing_inputs_are_rejected(self):
        target = self.root / "target.qcow2"
        self.base.chmod(0o644)
        with self.assertRaises(ValueError):
            fixture.overlay_pinned(self.base, target, self.expected)
        self.assertFalse(target.exists())
        self.base.chmod(0o444)
        with self.assertRaises(ValueError):
            fixture.overlay_pinned(self.base, target, {**self.expected, "sha256": "0" * 64})
        self.assertFalse(target.exists())
        indirect = self.root / "indirect.qcow2"
        fixture.overlay_pinned(self.base, indirect, self.expected)
        indirect.chmod(0o444)
        with self.assertRaises(ValueError):
            fixture.overlay_pinned(indirect, target, fixture.file_pin(indirect))
        self.assertFalse(target.exists())
        target.write_bytes(b"preserve existing target")
        with self.assertRaises(FileExistsError):
            fixture.overlay_pinned(self.base, target, self.expected)
        self.assertEqual(target.read_bytes(), b"preserve existing target")
        self.assertEqual(fixture.file_pin(self.base), self.expected)


if __name__ == "__main__":
    unittest.main()
