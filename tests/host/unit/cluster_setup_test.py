#!/usr/bin/env python3

import importlib.util
import subprocess
import sys
import tempfile
import threading
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CLUSTER_SETUP = ROOT / "scripts" / "cluster" / "cluster_setup.py"
CLUSTER_DIR = CLUSTER_SETUP.parent


def load_module():
    if str(CLUSTER_DIR) not in sys.path:
        sys.path.insert(0, str(CLUSTER_DIR))
    spec = importlib.util.spec_from_file_location("cluster_setup", CLUSTER_SETUP)
    if spec is None or spec.loader is None:
        raise AssertionError(f"failed to load {CLUSTER_SETUP}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def assert_equal(actual, expected, msg):
    if actual != expected:
        raise AssertionError(f"{msg}: expected {expected!r}, got {actual!r}")


def sample_config() -> dict:
    return {
        "zones": [
            {"id": "GLOBAL", "nic_queues": 1},
            {
                "id": 0,
                "name": "lan",
                "nodes": 2,
                "nic_queues": 2,
                "bridge": {"ip": "10.10.0.100/24"},
            },
            {"id": 1, "name": "wki", "nodes": 2, "nic_queues": 1},
        ]
    }


def up_link(master: str | None = None) -> dict:
    link = {"flags": ["BROADCAST", "MULTICAST", "UP"]}
    if master is not None:
        link["master"] = master
    return link


def test_topology_probe_is_timeout_bounded(module) -> None:
    calls: list[tuple[list[str], float | None]] = []
    old_run = module.subprocess.run

    def fake_run(args, **kwargs):
        calls.append((args, kwargs.get("timeout")))
        raise module.subprocess.TimeoutExpired(args, kwargs.get("timeout"))

    module.subprocess.run = fake_run
    try:
        result = module.run_topology_probe(["ip", "-j", "link", "show", "dev", "wos-lan-br"])
    finally:
        module.subprocess.run = old_run

    assert_equal(calls, [(["ip", "-j", "link", "show", "dev", "wos-lan-br"], module.TOPOLOGY_PROBE_TIMEOUT_SECONDS)], "topology probe timeout")
    assert_equal(result.returncode, 124, "topology probe timeout return code")


def test_no_setup_topology_accepts_configured_links(module) -> None:
    links = {
        "wos-lan-br": up_link(),
        "wos-wki-br": up_link(),
        "wos-lan-N0": up_link("wos-lan-br"),
        "wos-lan-N1": up_link("wos-lan-br"),
        "wos-wki-N0": up_link("wos-wki-br"),
        "wos-wki-N1": up_link("wos-wki-br"),
    }
    old_link_json = module.link_json
    old_tap_has_multiqueue = module.tap_has_multiqueue
    module.link_json = lambda name: links.get(name)
    module.tap_has_multiqueue = lambda name: name.startswith("wos-lan-")
    try:
        module.validate_no_setup_topology(sample_config())
    finally:
        module.link_json = old_link_json
        module.tap_has_multiqueue = old_tap_has_multiqueue


def test_no_setup_topology_rejects_missing_or_stale_links(module) -> None:
    links = {
        "wos-lan-N0": up_link(),
        "wos-lan-N1": {"flags": ["BROADCAST"], "master": "wos-lan-br"},
        "wos-wki-br": up_link(),
        "wos-wki-N0": up_link("wos-wki-br"),
    }
    old_link_json = module.link_json
    old_tap_has_multiqueue = module.tap_has_multiqueue
    module.link_json = lambda name: links.get(name)
    module.tap_has_multiqueue = lambda _name: False
    try:
        try:
            module.validate_no_setup_topology(sample_config())
        except module.NoSetupTopologyError as exc:
            message = str(exc)
        else:
            raise AssertionError("missing topology was accepted")
    finally:
        module.link_json = old_link_json
        module.tap_has_multiqueue = old_tap_has_multiqueue

    for expected in (
        "missing bridge wos-lan-br",
        "TAP wos-lan-N0 for node 0 zone lan is not enslaved to wos-lan-br",
        "TAP wos-lan-N0 for node 0 zone lan is not multi_queue",
        "TAP wos-lan-N1 for node 1 zone lan is not UP",
        "missing TAP wos-wki-N1",
    ):
        if expected not in message:
            raise AssertionError(f"missing diagnostic {expected!r} in {message!r}")


def test_node_overlay_creation_failure_aborts_launch_prep(module) -> None:
    node_setup = module.node_setup
    lines: list[str] = []
    calls: list[list[str]] = []
    old_run = node_setup.subprocess.run

    def fake_run(args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 1, "", "synthetic qemu-img failure")

    node_setup.subprocess.run = fake_run
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            spec = {
                "id": 0,
                "vm": {
                    "disk0": str(tmp_path / "missing-disk.qcow2"),
                    "disk1": str(tmp_path / "missing-mountfs.qcow2"),
                    "overlay_dir": str(tmp_path / "overlays"),
                },
            }
            try:
                node_setup.prepare_node_overlays(spec, log=lines.append)
            except node_setup.OverlayCreationError as exc:
                message = str(exc)
            else:
                raise AssertionError("overlay creation failure was accepted")
    finally:
        node_setup.subprocess.run = old_run

    assert_equal(len(calls), 1, "overlay creation should fail fast")
    for expected in ("failed to create overlay", "synthetic qemu-img failure"):
        if expected not in message:
            raise AssertionError(f"missing overlay failure diagnostic {expected!r} in {message!r}")
    if not any("ERROR creating overlay" in line for line in lines):
        raise AssertionError(f"overlay creation failure was not logged: {lines!r}")


def test_launch_one_vm_wraps_overlay_creation_failure_without_popen(module) -> None:
    old_build_qemu_args = module.build_qemu_args
    old_popen = module.subprocess.Popen
    popen_called = False

    def fake_build_qemu_args(*_args, **_kwargs):
        raise module.node_setup.OverlayCreationError(Path("overlay.qcow2"), Path("base.qcow2"), "boom")

    def fake_popen(*_args, **_kwargs):
        nonlocal popen_called
        popen_called = True
        raise AssertionError("QEMU launched after overlay prep failure")

    module.build_qemu_args = fake_build_qemu_args
    module.subprocess.Popen = fake_popen
    try:
        try:
            module.launch_one_vm(
                7,
                {"effective": {}, "zones": []},
                {"zones": [{"id": "GLOBAL"}]},
                None,
                None,
                [],
                threading.Lock(),
                threading.Event(),
            )
        except module.LaunchError as exc:
            if not isinstance(exc.cause, module.node_setup.OverlayCreationError):
                raise AssertionError(f"wrong launch error cause: {exc.cause!r}") from exc
        else:
            raise AssertionError("launch_one_vm did not wrap overlay prep failure")
    finally:
        module.build_qemu_args = old_build_qemu_args
        module.subprocess.Popen = old_popen

    assert_equal(popen_called, False, "QEMU must not launch after overlay prep failure")


def main() -> None:
    module = load_module()
    tests = [
        test_topology_probe_is_timeout_bounded,
        test_no_setup_topology_accepts_configured_links,
        test_no_setup_topology_rejects_missing_or_stale_links,
        test_node_overlay_creation_failure_aborts_launch_prep,
        test_launch_one_vm_wraps_overlay_creation_failure_without_popen,
    ]
    for test in tests:
        test(module)
    print(f"{len(tests)} cluster_setup tests passed")


if __name__ == "__main__":
    main()
