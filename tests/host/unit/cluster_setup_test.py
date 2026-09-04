#!/usr/bin/env python3

import contextlib
import importlib.util
import io
import json
import struct
import subprocess
import sys
import tempfile
import threading
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
CLUSTER_SETUP = ROOT / "scripts" / "cluster" / "cluster_setup.py"
CLUSTER_DIR = CLUSTER_SETUP.parent
KTEST_SETUP = ROOT / "scripts" / "test" / "ktest_setup.py"
USB_HOTPLUG = ROOT / "scripts" / "test" / "usb_hotplug_stress.py"
WKI_CHAOS_CONFIGS = {
    "ethernet": ROOT / "configs" / "cluster_wki_chaos.json",
    "ivshmem": ROOT / "configs" / "cluster_wki_chaos_ivshmem.json",
    "roce": ROOT / "configs" / "cluster_wki_chaos_roce.json",
}
ROUTED_WKI_CHAOS_CONFIG = ROOT / "configs" / "cluster_wki_chaos_routed.json"
ROOTLESS_WKI_AUTH_CONFIG = ROOT / "configs" / "cluster_wki_auth_rootless.json"
ROOTLESS_WKI_AUTH_MIXED_CONFIG = ROOT / "configs" / "cluster_wki_auth_mixed_rootless.json"
BENCHMARK_LAYOUTS = {
    1: [(32, 32768)],
    2: [(16, 16384), (16, 16384)],
    3: [(11, 10923), (11, 10923), (10, 10922)],
    4: [(8, 8192), (8, 8192), (8, 8192), (8, 8192)],
}


def load_module():
    if str(CLUSTER_DIR) not in sys.path:
        sys.path.insert(0, str(CLUSTER_DIR))
    spec = importlib.util.spec_from_file_location("cluster_setup", CLUSTER_SETUP)
    if spec is None or spec.loader is None:
        raise AssertionError(f"failed to load {CLUSTER_SETUP}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_usb_hotplug_module():
    spec = importlib.util.spec_from_file_location("usb_hotplug_stress", USB_HOTPLUG)
    if spec is None or spec.loader is None:
        raise AssertionError(f"failed to load {USB_HOTPLUG}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def assert_equal(actual, expected, msg):
    if actual != expected:
        raise AssertionError(f"{msg}: expected {expected!r}, got {actual!r}")


def memory_mib(value: object) -> int:
    text = str(value).strip().upper()
    if text.endswith("M"):
        return int(text[:-1])
    if text.endswith("G"):
        return int(text[:-1]) * 1024
    raise AssertionError(f"benchmark memory must use an exact M/G QEMU size: {value!r}")


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


def routed_sample_config() -> dict:
    return {
        "zones": [
            {"id": "GLOBAL", "nic_queues": 1},
            {"id": 0, "name": "lan", "nodes": 3, "nic_queues": 1},
            {
                "id": 1,
                "name": "wki-a",
                "nodes": 2,
                "node_ids": [0, 1],
                "nic_queues": 1,
            },
            {
                "id": 2,
                "name": "wki-b",
                "nodes": 2,
                "node_ids": [1, 2],
                "nic_queues": 1,
            },
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


def test_zone_node_ids_preserve_legacy_and_validate_explicit_membership(module) -> None:
    assert_equal(
        module.zone_node_ids({"id": 7, "name": "legacy", "nodes": 3}),
        [0, 1, 2],
        "legacy contiguous membership",
    )
    assert_equal(
        module.zone_node_ids(
            {"id": 7, "name": "explicit", "nodes": 2, "node_ids": [2, 1]}
        ),
        [1, 2],
        "explicit global node membership",
    )
    assert_equal(
        module.ivshmem_links(
            {
                "id": 7,
                "name": "explicit",
                "node_ids": [1, 3],
                "ivshmem": {"enabled": True, "topology": "full-mesh"},
            }
        ),
        [(1, 3)],
        "ivshmem uses explicit global node IDs",
    )

    invalid_zones = [
        ({"id": 1, "name": "empty", "node_ids": []}, "non-empty array"),
        ({"id": 1, "name": "bool", "node_ids": [True]}, "must be an integer"),
        (
            {"id": 1, "name": "duplicate", "node_ids": [1, 1]},
            "must not contain duplicates",
        ),
        (
            {"id": 1, "name": "mismatch", "nodes": 3, "node_ids": [1, 2]},
            "disagrees with 2 explicit node_ids",
        ),
    ]
    for zone, expected in invalid_zones:
        try:
            module.zone_node_ids(zone)
        except ValueError as exc:
            if expected not in str(exc):
                raise AssertionError(f"wrong membership error for {zone!r}: {exc}") from exc
        else:
            raise AssertionError(f"invalid zone membership was accepted: {zone!r}")

    invalid_configs = [
        {
            "zones": [
                {"id": "GLOBAL"},
                {
                    "id": 1,
                    "name": "override",
                    "node_ids": [1, 2],
                    "nodes_config": [{"id": 0}],
                },
            ]
        },
        {"zones": [{"id": "GLOBAL", "node_ids": [0]}]},
        {"zones": [{"id": "GLOBAL"}, {"id": [1], "nodes": 1}]},
    ]
    for config in invalid_configs:
        try:
            module.validate_cluster_config(config)
        except ValueError:
            pass
        else:
            raise AssertionError(f"invalid explicit-membership config was accepted: {config!r}")

    with tempfile.TemporaryDirectory() as temporary:
        invalid_path = Path(temporary) / "invalid-cluster.json"
        invalid_path.write_text(json.dumps(invalid_configs[0]))
        try:
            module.load_config(invalid_path)
        except ValueError:
            pass
        else:
            raise AssertionError("load_config accepted an invalid explicit-membership schema")


def test_no_setup_topology_uses_only_explicit_zone_members(module) -> None:
    links = {
        "wos-lan-br": up_link(),
        "wos-wki-a-br": up_link(),
        "wos-wki-b-br": up_link(),
        "wos-lan-N0": up_link("wos-lan-br"),
        "wos-lan-N1": up_link("wos-lan-br"),
        "wos-lan-N2": up_link("wos-lan-br"),
        "wos-wki-a-N0": up_link("wos-wki-a-br"),
        "wos-wki-a-N1": up_link("wos-wki-a-br"),
        "wos-wki-b-N1": up_link("wos-wki-b-br"),
        "wos-wki-b-N2": up_link("wos-wki-b-br"),
    }
    probes: list[str] = []
    old_link_json = module.link_json
    old_tap_has_multiqueue = module.tap_has_multiqueue

    def probe(name: str):
        probes.append(name)
        return links.get(name)

    module.link_json = probe
    module.tap_has_multiqueue = lambda _name: True
    try:
        module.validate_no_setup_topology(routed_sample_config())
        missing = dict(links)
        missing.pop("wos-wki-b-N2")
        module.link_json = lambda name: missing.get(name)
        try:
            module.validate_no_setup_topology(routed_sample_config())
        except module.NoSetupTopologyError as exc:
            message = str(exc)
        else:
            raise AssertionError("missing explicit-member TAP was accepted")
    finally:
        module.link_json = old_link_json
        module.tap_has_multiqueue = old_tap_has_multiqueue

    expected_probes = set(links)
    assert_equal(set(probes), expected_probes, "rootless explicit-membership probes")
    for forbidden in ("wos-wki-a-N2", "wos-wki-b-N0"):
        if forbidden in probes:
            raise AssertionError(f"--no-setup probed non-member TAP {forbidden}")
    if "missing TAP wos-wki-b-N2 for node 2 zone wki-b" not in message:
        raise AssertionError(f"missing explicit-member diagnostic: {message!r}")


def test_running_wos_qemu_probe_filters_unrelated_processes(module) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        proc_root = Path(tmp)
        commands = {
            101: [
                "/usr/bin/qemu-system-x86_64",
                "-name",
                "guest=OPNsense",
            ],
            202: [
                "qemu-system-x86_64",
                "-fw_cfg",
                "name=opt/wos/hostname,string=wos-2",
            ],
            303: ["not-qemu", "name=opt/wos/hostname,string=wos-3"],
            404: [
                "/usr/bin/qemu-system-x86_64",
                "-name",
                "guest=wos-ubuntu-vm1,debug-threads=on",
            ],
            505: [
                "/usr/bin/qemu-system-x86_64",
                "-name",
                "guest=unrelated-linux,debug-threads=on",
            ],
        }
        for pid, args in commands.items():
            process_dir = proc_root / str(pid)
            process_dir.mkdir()
            (process_dir / "cmdline").write_bytes(
                b"\0".join(arg.encode() for arg in args) + b"\0"
            )

        assert_equal(
            module.find_running_wos_qemus(proc_root),
            [(202, "wos-2"), (404, "wos-ubuntu-vm1")],
            "running benchmark QEMU probe",
        )


def test_cluster_launch_guard_rejects_second_launcher(module) -> None:
    old_lock_path = module.cluster_launch_lock_path
    old_find_qemus = module.find_running_wos_qemus
    with tempfile.TemporaryDirectory() as tmp:
        module.cluster_launch_lock_path = lambda: Path(tmp) / "launch.lock"
        module.find_running_wos_qemus = lambda: []
        try:
            with module.cluster_launch_guard():
                try:
                    with module.cluster_launch_guard():
                        pass
                except module.LaunchConflictError as exc:
                    message = str(exc)
                else:
                    raise AssertionError("second cluster launcher was accepted")
        finally:
            module.cluster_launch_lock_path = old_lock_path
            module.find_running_wos_qemus = old_find_qemus

    if "another WOS cluster launcher is active" not in message:
        raise AssertionError(f"missing launch conflict diagnostic in {message!r}")


def test_cluster_launch_guard_rejects_preexisting_wos_qemu(module) -> None:
    old_lock_path = module.cluster_launch_lock_path
    old_find_qemus = module.find_running_wos_qemus
    with tempfile.TemporaryDirectory() as tmp:
        module.cluster_launch_lock_path = lambda: Path(tmp) / "launch.lock"
        module.find_running_wos_qemus = lambda: [(4242, "wos-0")]
        try:
            try:
                with module.cluster_launch_guard():
                    pass
            except module.LaunchConflictError as exc:
                message = str(exc)
            else:
                raise AssertionError("preexisting WOS QEMU was accepted")

            with module.cluster_launch_guard(reject_running_qemus=False):
                pass

            module.find_running_wos_qemus = lambda: []
            with module.cluster_launch_guard():
                pass
        finally:
            module.cluster_launch_lock_path = old_lock_path
            module.find_running_wos_qemus = old_find_qemus

    for expected in ("existing WOS/Linux benchmark QEMU processes", "wos-0 pid=4242"):
        if expected not in message:
            raise AssertionError(f"missing launch conflict diagnostic {expected!r} in {message!r}")


def test_fixed_resource_benchmark_topologies(module) -> None:
    expected_nics = {
        "lan": {
            "zone_id": 0,
            "model": "virtio-net-pci",
            "queues": 2,
            "vhost": False,
            "driver": "dhcp",
        },
        "wki": {
            "zone_id": 1,
            "model": "virtio-net-pci",
            "queues": 8,
            "vhost": True,
            "driver": "wki",
        },
    }

    for node_count, expected_layout in BENCHMARK_LAYOUTS.items():
        path = ROOT / "configs" / f"cluster_bench_{node_count}.json"
        config = module.load_config(str(path))
        zones = [zone for zone in config["zones"] if zone.get("id") != "GLOBAL"]
        assert_equal([zone.get("name") for zone in zones], ["lan", "wki"], f"{path.name} zone order")
        assert_equal(module.find_global(config["zones"]).get("mtu"), 9000, f"{path.name} MTU")
        for zone in zones:
            assert_equal(zone.get("nodes"), node_count, f"{path.name} {zone['name']} membership")
            if node_count == 3:
                raw_layout = [
                    (int(node["id"]), int(node["vm"]["cpus"]), memory_mib(node["vm"]["memory"]))
                    for node in zone.get("nodes_config", [])
                ]
                expected_raw_layout = [
                    (node_id, cpus, memory) for node_id, (cpus, memory) in enumerate(expected_layout)
                ]
                assert_equal(raw_layout, expected_raw_layout, f"{path.name} {zone['name']} explicit resources")

        nodes = module.collect_unique_nodes(config)
        assert_equal(sorted(nodes), list(range(node_count)), f"{path.name} contiguous node ids")

        actual_layout: list[tuple[int, int]] = []
        for node_id in range(node_count):
            spec = module.cluster_node_spec(node_id, nodes[node_id], config)
            assert_equal(spec["hostname"], f"wos-{node_id}", f"{path.name} node {node_id} hostname")
            actual_layout.append((int(spec["vm"]["cpus"]), memory_mib(spec["vm"]["memory"])))

            actual_nics = {
                nic["name"]: {
                    "zone_id": nic["zone_id"],
                    "model": nic["model"],
                    "queues": int(nic["queues"]),
                    "vhost": nic["vhost"],
                    "driver": nic["driver"],
                }
                for nic in spec["nics"]
            }
            assert_equal(actual_nics, expected_nics, f"{path.name} node {node_id} NIC policy")

        assert_equal(actual_layout, expected_layout, f"{path.name} per-node resources")
        assert_equal(sum(cpus for cpus, _memory in actual_layout), 32, f"{path.name} aggregate vCPUs")
        assert_equal(sum(memory for _cpus, memory in actual_layout), 32768, f"{path.name} aggregate memory MiB")


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


def test_usb_hotplug_qemu_args_and_qmp_device_shape(module) -> None:
    node_setup = module.node_setup
    old_prepare = node_setup.prepare_node_overlays
    old_cleanup = node_setup.cleanup_node_logs
    node_setup.prepare_node_overlays = lambda _spec, log=print: (Path("disk0-overlay"), Path("disk1-overlay"))
    node_setup.cleanup_node_logs = lambda _spec: None
    try:
        args = node_setup.build_qemu_args(
            {
                "id": 7,
                "vm": {
                    "usb_hotplug": {
                        "enabled": True,
                        "controller_id": "usbctl",
                        "usb2_ports": 2,
                        "usb3_ports": 3,
                        "qmp_socket": "state/qmp-vm7.sock",
                    }
                },
            },
            log=lambda _line: None,
        )
    finally:
        node_setup.prepare_node_overlays = old_prepare
        node_setup.cleanup_node_logs = old_cleanup

    device_index = args.index("qemu-xhci,id=usbctl,p2=2,p3=3")
    assert_equal(args[device_index - 1], "-device", "xHCI QEMU option")
    qmp_index = args.index("unix:state/qmp-vm7.sock,server=on,wait=off")
    assert_equal(args[qmp_index - 1], "-qmp", "QMP QEMU option")

    hotplug = load_usb_hotplug_module()

    class FakeClient:
        def __init__(self):
            self.calls = []

        def execute(self, command, arguments=None):
            self.calls.append((command, arguments))

    client = FakeClient()
    hotplug.add_usb_net(client, "usbnet9", "usbhot9", "usbctl", "52:54:00:12:34:99")
    assert_equal(
        client.calls,
        [
            (
                "device_add",
                {
                    "driver": "usb-net",
                    "id": "usbnet9",
                    "bus": "usbctl.0",
                    "netdev": "usbhot9",
                    "mac": "52:54:00:12:34:99",
                },
            )
        ],
        "QMP usb-net request",
    )


def test_generic_qmp_socket_is_opt_in_and_independent_of_usb(module) -> None:
    node_setup = module.node_setup
    assert_equal(node_setup.qmp_socket_path({"id": 4}), None, "default QMP socket")
    assert_equal(
        node_setup.qmp_socket_path(
            {"id": 4, "vm": {"usb_hotplug": {"enabled": True}}}
        ),
        Path("qmp-vm4.sock"),
        "legacy USB QMP socket",
    )
    assert_equal(
        node_setup.qmp_socket_path(
            {
                "id": 4,
                "vm": {
                    "qmp_socket": "state/generic.sock",
                    "usb_hotplug": {
                        "enabled": True,
                        "qmp_socket": "state/legacy.sock",
                    },
                },
            }
        ),
        Path("state/generic.sock"),
        "generic QMP socket precedence",
    )

    old_prepare = node_setup.prepare_node_overlays
    old_cleanup = node_setup.cleanup_node_logs
    node_setup.prepare_node_overlays = lambda _spec, log=print: (
        Path("disk0-overlay"),
        Path("disk1-overlay"),
    )
    node_setup.cleanup_node_logs = lambda _spec: None
    try:
        args = node_setup.build_qemu_args(
            {"id": 4, "vm": {"qmp_socket": "state/qmp-vm4.sock"}},
            log=lambda _line: None,
        )
    finally:
        node_setup.prepare_node_overlays = old_prepare
        node_setup.cleanup_node_logs = old_cleanup
    assert_equal(args.count("-qmp"), 1, "generic QMP option count")
    qmp_index = args.index("unix:state/qmp-vm4.sock,server=on,wait=off")
    assert_equal(args[qmp_index - 1], "-qmp", "generic QMP QEMU option")
    if any("qemu-xhci" in arg for arg in args):
        raise AssertionError("generic QMP unexpectedly enabled xHCI")


def test_debug_nodes_get_qmp_and_loopback_only_debug_sockets(module) -> None:
    node_setup = module.node_setup
    with tempfile.TemporaryDirectory() as temporary:
        state = Path(temporary)
        debug_spec = {
            "id": 4,
            "vm": {"overlay_dir": str(state)},
        }
        assert_equal(
            node_setup.qmp_socket_path(debug_spec),
            None,
            "ordinary node QMP remains opt-in",
        )

        old_prepare = node_setup.prepare_node_overlays
        old_cleanup = node_setup.cleanup_node_logs
        node_setup.prepare_node_overlays = lambda _spec, log=print: (
            state / "disk0-overlay",
            state / "disk1-overlay",
        )
        node_setup.cleanup_node_logs = lambda _spec: None
        try:
            args = node_setup.build_qemu_args(
                debug_spec,
                force_debug=True,
                log=lambda _line: None,
            )
        finally:
            node_setup.prepare_node_overlays = old_prepare
            node_setup.cleanup_node_logs = old_cleanup

        qmp = state / "qmp-vm4.sock"
        qmp_index = args.index(f"unix:{qmp},server=on,wait=off")
        assert_equal(args[qmp_index - 1], "-qmp", "forced debug QMP option")
        for expected in (
            "tcp:127.0.0.1:1238",
            "socket,id=debugger,port=23460,host=127.0.0.1,server=on,wait=off,telnet=on",
            "tcp:127.0.0.1:3006,server,nowait",
        ):
            if expected not in args:
                raise AssertionError(f"missing loopback debug endpoint {expected!r}")
        if "0.0.0.0" in " ".join(args):
            raise AssertionError("debug QEMU arguments expose a wildcard TCP bind")


def test_live_debug_descriptor_is_private_atomic_and_nonce_owned(module) -> None:
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "state" / "live.json"
        first = {
            "format": module.LIVE_DEBUG_DESCRIPTOR_FORMAT,
            "version": module.LIVE_DEBUG_DESCRIPTOR_VERSION,
            "state": "running",
            "launchNonce": "first",
        }
        second = dict(first, launchNonce="second")

        resolved = module.atomic_write_live_debug_descriptor(path, first)
        assert_equal(resolved, path.resolve(), "resolved descriptor path")
        assert_equal(path.stat().st_mode & 0o777, 0o600, "private descriptor mode")
        assert_equal(json.loads(path.read_text()), first, "first descriptor payload")

        module.atomic_write_live_debug_descriptor(path, second)
        if module.remove_live_debug_descriptor(path, "first"):
            raise AssertionError("old launch nonce removed a newer descriptor")
        assert_equal(
            json.loads(path.read_text()), second, "new descriptor survived old cleanup"
        )
        assert_equal(
            module.remove_live_debug_descriptor(path, "second"),
            True,
            "owning launch removes descriptor",
        )
        if path.exists():
            raise AssertionError("owned descriptor remained after cleanup")
        leftovers = list(path.parent.glob(f".{path.name}.*.tmp"))
        assert_equal(leftovers, [], "atomic descriptor temporary cleanup")


def test_live_debug_descriptor_schema_and_process_identity(module) -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        config = sample_config()
        config["zones"][0]["vm"] = {
            "overlay_dir": str(root / "overlays"),
            "serial_log": str(root / "serial-vm0.log"),
            "qemu_log": str(root / "qemu-vm0.log"),
        }
        nodes = module.collect_unique_nodes(config)

        class FakeProcess:
            def __init__(self, pid):
                self.pid = pid

        results = [
            module.LaunchResult(
                node_id=node_id, process=FakeProcess(9000 + node_id), lines=[]
            )
            for node_id in sorted(nodes)
        ]
        old_start_ticks = module.process_start_ticks
        old_boot_id = module.host_boot_id
        module.process_start_ticks = lambda pid: pid + 100
        module.host_boot_id = lambda: "test-boot-id"
        try:
            descriptor = module.build_live_debug_descriptor(
                config,
                nodes,
                results,
                launch_nonce="0123456789abcdef",
                topology_kind="cluster",
                config_path=root / "cluster.json",
                build_dir=root / "build",
                symbol_path=root / "build/modules/kern/wos",
                tcg_level=None,
                debug_nodes={1},
            )
        finally:
            module.process_start_ticks = old_start_ticks
            module.host_boot_id = old_boot_id

        for key, expected in (
            ("format", "wosdbg-live"),
            ("version", 1),
            ("state", "running"),
            ("launchNonce", "0123456789abcdef"),
            ("kind", "cluster"),
            ("bootId", "test-boot-id"),
        ):
            assert_equal(descriptor[key], expected, f"descriptor {key}")
        assert_equal(
            descriptor["topology"]["nodes"],
            [
                {"nodeId": 0, "hostname": "wos-0"},
                {"nodeId": 1, "hostname": "wos-1"},
            ],
            "normalized descriptor nodes",
        )
        targets = {target["nodeId"]: target for target in descriptor["targets"]}
        assert_equal(targets[0]["port"], None, "non-debug target has no GDB port")
        assert_equal(
            targets[0]["qmpSocket"], None, "non-debug target has no implicit QMP"
        )
        assert_equal(targets[1]["host"], "127.0.0.1", "debug GDB host")
        assert_equal(targets[1]["port"], 1235, "debug GDB port")
        assert_equal(targets[1]["processStartTicks"], "9101", "process identity")
        expected_qmp = (root / "overlays/qmp-vm1.sock").resolve()
        assert_equal(Path(targets[1]["qmpSocket"]), expected_qmp, "debug QMP path")
        for key in ("configPath", "buildDir", "symbolPath"):
            if not Path(descriptor[key]).is_absolute():
                raise AssertionError(f"descriptor {key} is not absolute")
        for target in targets.values():
            if any(not Path(path).is_absolute() for path in target["logPaths"]):
                raise AssertionError(f"target log path is not absolute: {target!r}")

        proc_root = root / "proc"
        process_dir = proc_root / "77"
        process_dir.mkdir(parents=True)
        fields = ["S", *("0" for _ in range(18)), "424242"]
        (process_dir / "stat").write_text(f"77 (qemu ) worker) {' '.join(fields)}\n")
        assert_equal(
            module.process_start_ticks(77, proc_root),
            424242,
            "comm-safe /proc start identity",
        )


def test_launch_lifecycle_publishes_then_removes_descriptor(module) -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        descriptor_path = root / "live.json"
        config = sample_config()
        config["zones"][0]["vm"] = {"overlay_dir": str(root / "overlays")}
        observed = []

        class FakeProcess:
            def __init__(self, pid):
                self.pid = pid
                self.returncode = 0

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                if timeout is not None:
                    raise AssertionError("completed fake process used a timeout")
                observed.append(json.loads(descriptor_path.read_text()))
                return self.returncode

        def fake_launch_one_vm(
            node_id,
            _node_info,
            _config,
            _tcg_level,
            _debug_nodes,
            processes,
            processes_lock,
            _stopping,
        ):
            process = FakeProcess(7000 + node_id)
            with processes_lock:
                processes.append(process)
            return module.LaunchResult(node_id=node_id, process=process, lines=[])

        old_setup = module.setup
        old_provision = module.provision_wki_auth
        old_launch_one_vm = module.launch_one_vm
        old_start_ticks = module.process_start_ticks
        old_boot_id = module.host_boot_id
        old_sigint = module.signal.getsignal(module.signal.SIGINT)
        old_sigterm = module.signal.getsignal(module.signal.SIGTERM)
        module.setup = lambda _config: None
        module.provision_wki_auth = lambda _nodes: {}
        module.launch_one_vm = fake_launch_one_vm
        module.process_start_ticks = lambda pid: pid + 50
        module.host_boot_id = lambda: "lifecycle-boot"
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                module.launch_guarded(
                    config,
                    debug_nodes={0},
                    live_debug_descriptor=descriptor_path,
                    config_path=root / "cluster.json",
                    build_dir=root / "build",
                    symbol_path=root / "build/modules/kern/wos",
                )
        finally:
            module.setup = old_setup
            module.provision_wki_auth = old_provision
            module.launch_one_vm = old_launch_one_vm
            module.process_start_ticks = old_start_ticks
            module.host_boot_id = old_boot_id
            module.signal.signal(module.signal.SIGINT, old_sigint)
            module.signal.signal(module.signal.SIGTERM, old_sigterm)

        assert_equal(len(observed), 2, "each live VM observed the published descriptor")
        assert_equal(observed[0]["state"], "running", "published lifecycle state")
        if descriptor_path.exists():
            raise AssertionError("normal VM exit left a live-debug descriptor")


def test_partial_launch_failure_removes_stale_descriptor(module) -> None:
    with tempfile.TemporaryDirectory() as temporary:
        descriptor_path = Path(temporary) / "live.json"
        module.atomic_write_live_debug_descriptor(
            descriptor_path,
            {
                "format": "wosdbg-live",
                "version": 1,
                "state": "running",
                "launchNonce": "stale",
            },
        )
        nodes = {0: {"effective": {}, "zones": []}}

        def fail_launch(node_id, *_args, **_kwargs):
            raise module.LaunchError(node_id, [], RuntimeError("synthetic failure"))

        old_setup = module.setup
        old_collect = module.collect_unique_nodes
        old_provision = module.provision_wki_auth
        old_launch_one_vm = module.launch_one_vm
        old_sigint = module.signal.getsignal(module.signal.SIGINT)
        old_sigterm = module.signal.getsignal(module.signal.SIGTERM)
        module.setup = lambda _config: None
        module.collect_unique_nodes = lambda _config: nodes
        module.provision_wki_auth = lambda _nodes: {}
        module.launch_one_vm = fail_launch
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                try:
                    module.launch_guarded(
                        {"zones": [{"id": "GLOBAL"}]},
                        live_debug_descriptor=descriptor_path,
                    )
                except module.LaunchError:
                    pass
                else:
                    raise AssertionError("synthetic partial launch failure was accepted")
        finally:
            module.setup = old_setup
            module.collect_unique_nodes = old_collect
            module.provision_wki_auth = old_provision
            module.launch_one_vm = old_launch_one_vm
            module.signal.signal(module.signal.SIGINT, old_sigint)
            module.signal.signal(module.signal.SIGTERM, old_sigterm)

        if descriptor_path.exists():
            raise AssertionError("partial launch failure left a stale descriptor")


def test_cluster_and_ktest_help_expose_descriptor_override(module) -> None:
    for script, default_path in (
        (CLUSTER_SETUP, "cluster-overlays/live-debug.json"),
        (KTEST_SETUP, "ktest-data/live-debug.json"),
    ):
        result = subprocess.run(
            [sys.executable, str(script), "--help"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        assert_equal(result.returncode, 0, f"{script.name} help exit")
        compact_help = "".join(result.stdout.split())
        for expected in ("--live-debug-descriptor", default_path):
            if expected not in compact_help:
                raise AssertionError(
                    f"{script.name} help omits {expected!r}: {result.stdout!r}"
                )


def test_wki_chaos_transport_configs_use_isolated_artifacts(module) -> None:
    expected_nodes = {"ethernet": 3, "ivshmem": 2, "roce": 2}
    qmp_paths: set[str] = set()
    for lane, path in WKI_CHAOS_CONFIGS.items():
        config = module.load_config(path)
        global_zone = module.find_global(config["zones"])
        assert_equal(
            global_zone["vm"]["disk0"],
            "wki-chaos-data/disk.qcow2",
            f"{lane} isolated boot disk",
        )
        assert_equal(
            global_zone["vm"]["disk1"],
            "wki-chaos-data/mountfs.qcow2",
            f"{lane} isolated rootfs disk",
        )
        nodes = module.collect_unique_nodes(config)
        assert_equal(len(nodes), expected_nodes[lane], f"{lane} node count")
        for node_id, node in nodes.items():
            socket_path = node["effective"]["vm"].get("qmp_socket")
            if not socket_path or not socket_path.startswith(f"wki-chaos-data/{lane}-qmp-vm"):
                raise AssertionError(
                    f"{lane} node {node_id} has a missing or non-isolated QMP path: {socket_path!r}"
                )
            if socket_path in qmp_paths:
                raise AssertionError(f"duplicate chaos QMP path {socket_path!r}")
            qmp_paths.add(socket_path)

        wki_zone = next(zone for zone in config["zones"] if zone.get("name") == "wki")
        assert_equal(wki_zone["netdev_driver"], "wki", f"{lane} WKI NIC driver")
        assert_equal(
            bool(wki_zone.get("ivshmem", {}).get("enabled", False)),
            lane == "ivshmem",
            f"{lane} shared-memory selection",
        )


def test_routed_wki_chaos_config_resolves_overlap_and_specs(module) -> None:
    config = module.load_config(ROUTED_WKI_CHAOS_CONFIG)
    global_zone = module.find_global(config["zones"])
    assert_equal(global_zone["vm"]["disk0"], "wki-chaos-data/disk.qcow2", "routed boot disk")
    assert_equal(global_zone["vm"]["disk1"], "wki-chaos-data/mountfs.qcow2", "routed rootfs disk")

    zones = {
        zone["name"]: zone
        for zone in config["zones"]
        if zone.get("id") != "GLOBAL"
    }
    assert_equal(module.zone_node_ids(zones["lan"]), [0, 1, 2], "routed LAN members")
    assert_equal(module.zone_node_ids(zones["wki-a"]), [0, 1], "routed WKI-A members")
    assert_equal(module.zone_node_ids(zones["wki-b"]), [1, 2], "routed WKI-B members")
    assert_equal(
        zones["wki-a"]["qemu_netdev"],
        {
            "type": "socket-mcast",
            "address": "239.192.87.1",
            "port": 47801,
            "localaddr": "127.0.0.1",
        },
        "routed WKI-A rootless backend",
    )
    assert_equal(
        zones["wki-b"]["qemu_netdev"],
        {
            "type": "socket-mcast",
            "address": "239.192.87.2",
            "port": 47802,
            "localaddr": "127.0.0.1",
        },
        "routed WKI-B rootless backend",
    )

    nodes = module.collect_unique_nodes(config)
    assert_equal(sorted(nodes), [0, 1, 2], "routed unique node IDs")
    expected_zone_names = {
        0: ["lan", "wki-a"],
        1: ["lan", "wki-a", "wki-b"],
        2: ["lan", "wki-b"],
    }
    qmp_paths: set[str] = set()
    for node_id in sorted(nodes):
        spec = module.cluster_node_spec(node_id, nodes[node_id], config)
        assert_equal(spec["hostname"], f"wos-{node_id}", f"routed node {node_id} hostname")
        assert_equal(
            [nic["name"] for nic in spec["nics"]],
            expected_zone_names[node_id],
            f"routed node {node_id} NIC membership/order",
        )
        assert_equal(
            [nic["tap"] for nic in spec["nics"]],
            [f"wos-{name}-N{node_id}" for name in expected_zone_names[node_id]],
            f"routed node {node_id} TAP naming",
        )
        backends = {
            nic["name"]: nic.get("qemu_netdev")
            for nic in spec["nics"]
        }
        assert_equal(backends["lan"], None, f"routed node {node_id} LAN remains TAP-backed")
        for name in set(expected_zone_names[node_id]) - {"lan"}:
            assert_equal(
                backends[name],
                zones[name]["qemu_netdev"],
                f"routed node {node_id} {name} socket group",
            )
        socket_path = spec["vm"].get("qmp_socket")
        if socket_path != f"wki-chaos-data/routed-qmp-vm{node_id}.sock":
            raise AssertionError(f"routed node {node_id} QMP path is not isolated: {socket_path!r}")
        if socket_path in qmp_paths:
            raise AssertionError(f"duplicate routed QMP path {socket_path!r}")
        qmp_paths.add(socket_path)


def test_rootless_wki_auth_config_has_no_host_link_dependency(module) -> None:
    config = module.load_config(ROOTLESS_WKI_AUTH_CONFIG)
    module.validate_cluster_config(config)
    nodes = module.collect_unique_nodes(config)
    assert_equal(sorted(nodes), [0, 1, 2], "rootless auth node IDs")
    for node_id in sorted(nodes):
        spec = module.cluster_node_spec(node_id, nodes[node_id], config)
        if not spec["nics"]:
            raise AssertionError(f"rootless auth node {node_id} has no WKI NIC")
        for nic in spec["nics"]:
            backend = nic.get("qemu_netdev")
            if backend is None or backend.get("type") != "socket-mcast":
                raise AssertionError(
                    f"rootless auth node {node_id} retained a host-link NIC: {nic!r}"
                )
    # Socket-backed zones are intentionally ignored by the --no-setup host
    # topology validator, so this must succeed on a machine with no WOS links.
    module.validate_no_setup_topology(config)


def test_rootless_wki_auth_mixed_config_validates_shared_memory(module) -> None:
    config = module.load_config(ROOTLESS_WKI_AUTH_MIXED_CONFIG)
    module.validate_cluster_config(config)
    nodes = module.collect_unique_nodes(config)
    assert_equal(sorted(nodes), [0, 1], "mixed rootless auth node IDs")
    for node_id in sorted(nodes):
        spec = module.cluster_node_spec(node_id, nodes[node_id], config)
        assert_equal(len(spec["nics"]), 1, f"mixed node {node_id} Ethernet path")
        assert_equal(len(spec["ivshmem"]), 1, f"mixed node {node_id} ivshmem path")
        assert_equal(spec["ivshmem"][0]["role"], node_id, f"mixed node {node_id} stable ivshmem role")
        assert_equal(spec["ivshmem"][0]["peer"], 2 - node_id, f"mixed node {node_id} direct WKI peer")
        old_prepare = module.node_setup.prepare_node_overlays
        old_cleanup = module.node_setup.cleanup_node_logs
        module.node_setup.prepare_node_overlays = lambda _spec, log=print: (
            Path("disk0-overlay"),
            Path("disk1-overlay"),
        )
        module.node_setup.cleanup_node_logs = lambda _spec: None
        try:
            args = module.node_setup.build_qemu_args(spec, log=lambda _line: None)
        finally:
            module.node_setup.prepare_node_overlays = old_prepare
            module.node_setup.cleanup_node_logs = old_cleanup
        fw_cfg = [args[index + 1] for index, token in enumerate(args) if token == "-fw_cfg"]
        if "name=opt/wos/wki-ivshmem,string=1" not in fw_cfg:
            raise AssertionError(f"mixed node {node_id} did not reserve ivshmem for WKI: {fw_cfg!r}")
        if f"name=opt/wos/wki-ivshmem-role,string={node_id}" not in fw_cfg:
            raise AssertionError(f"mixed node {node_id} did not publish its stable ivshmem role: {fw_cfg!r}")
        if f"name=opt/wos/wki-ivshmem-peer,string={2 - node_id}" not in fw_cfg:
            raise AssertionError(f"mixed node {node_id} did not publish its direct WKI peer: {fw_cfg!r}")

    zone = next(item for item in config["zones"] if item.get("id") != "GLOBAL")
    with tempfile.TemporaryDirectory() as temporary:
        temporary_config = json.loads(json.dumps(config))
        temporary_zone = next(item for item in temporary_config["zones"] if item.get("id") != "GLOBAL")
        temporary_zone["ivshmem"]["root_path"] = temporary
        backing = Path(module.ivshmem_file(temporary, temporary_zone, 0, 1))

        try:
            module.validate_no_setup_topology(temporary_config)
        except module.NoSetupTopologyError as exc:
            if f"missing ivshmem backing file {backing}" not in str(exc):
                raise AssertionError(f"wrong missing mixed backing diagnostic: {exc}") from exc
        else:
            raise AssertionError("--no-setup accepted a missing mixed ivshmem backing file")

        backing.parent.mkdir(parents=True, exist_ok=True)
        backing.write_bytes(b"\0" * (16 * 1024 * 1024))
        module.validate_no_setup_topology(temporary_config)

        backing.write_bytes(b"short")
        try:
            module.validate_no_setup_topology(temporary_config)
        except module.NoSetupTopologyError as exc:
            if "expected 16777216" not in str(exc):
                raise AssertionError(f"wrong mixed backing size diagnostic: {exc}") from exc
        else:
            raise AssertionError("--no-setup accepted a truncated mixed ivshmem backing file")


def test_socket_multicast_backend_validation_qemu_and_no_setup(module) -> None:
    config = module.load_config(ROUTED_WKI_CHAOS_CONFIG)
    nodes = module.collect_unique_nodes(config)
    node1 = module.cluster_node_spec(1, nodes[1], config)
    node_setup = module.node_setup
    old_prepare = node_setup.prepare_node_overlays
    old_cleanup = node_setup.cleanup_node_logs
    node_setup.prepare_node_overlays = lambda _spec, log=print: (
        Path("disk0-overlay"), Path("disk1-overlay")
    )
    node_setup.cleanup_node_logs = lambda _spec: None
    try:
        args = node_setup.build_qemu_args(node1, log=lambda _line: None)
    finally:
        node_setup.prepare_node_overlays = old_prepare
        node_setup.cleanup_node_logs = old_cleanup
    netdevs = [args[index + 1] for index, token in enumerate(args) if token == "-netdev"]
    assert_equal(
        netdevs,
        [
            "tap,id=net0,ifname=wos-lan-N1,script=no,downscript=no,vnet_hdr=off,queues=2",
            "socket,id=net1,mcast=239.192.87.1:47801,localaddr=127.0.0.1",
            "socket,id=net2,mcast=239.192.87.2:47802,localaddr=127.0.0.1",
        ],
        "node1 joins distinct routed multicast groups",
    )
    fw_cfg = [args[index + 1] for index, token in enumerate(args) if token == "-fw_cfg"]
    if "name=opt/wos/netdevs,string=eth0 dhcp;eth1 wki;eth2 wki" not in fw_cfg:
        raise AssertionError(f"node1 early netdev policy is missing from fw_cfg: {fw_cfg!r}")
    assert_equal(
        node_setup.netdevs_content(node1).splitlines()[-3:],
        ["eth0 dhcp", "eth1 wki", "eth2 wki"],
        "node1 fw_cfg and rootfs netdev policy share one assignment source",
    )

    probed: list[str] = []
    old_link_json = module.link_json
    old_tap_has_multiqueue = module.tap_has_multiqueue
    lan_links = {
        "wos-lan-br": up_link(),
        "wos-lan-N0": up_link("wos-lan-br"),
        "wos-lan-N1": up_link("wos-lan-br"),
        "wos-lan-N2": up_link("wos-lan-br"),
    }

    def probe(name):
        probed.append(name)
        return lan_links.get(name)

    module.link_json = probe
    module.tap_has_multiqueue = lambda _name: True
    try:
        module.validate_no_setup_topology(config)
    finally:
        module.link_json = old_link_json
        module.tap_has_multiqueue = old_tap_has_multiqueue
    if any("wki-a" in name or "wki-b" in name for name in probed):
        raise AssertionError(f"--no-setup probed socket-backed host links: {probed!r}")

    invalid_cases = [
        ("address", "127.0.0.1", "multicast IPv4"),
        ("port", 0, "integer in [1, 65535]"),
        ("localaddr", "::1", "must be an IPv4 address"),
    ]
    for key, value, diagnostic in invalid_cases:
        invalid = json.loads(json.dumps(config))
        invalid["zones"][2]["qemu_netdev"][key] = value
        try:
            module.validate_cluster_config(invalid)
        except ValueError as exc:
            if diagnostic not in str(exc):
                raise AssertionError(f"missing socket validation diagnostic: {exc}") from exc
        else:
            raise AssertionError(f"invalid socket multicast {key} was accepted")

    legacy_backend = {
        "type": "socket-mcast",
        "address": "239.192.87.3",
        "port": 47803,
    }
    assert_equal(
        node_setup.normalize_qemu_netdev(
            legacy_backend,
            queues=1,
            vhost=False,
            where="legacy qemu_netdev",
        ),
        legacy_backend,
        "socket multicast localaddr remains optional",
    )

    collision = json.loads(json.dumps(config))
    collision["zones"][3]["qemu_netdev"] = dict(collision["zones"][2]["qemu_netdev"])
    try:
        module.validate_cluster_config(collision)
    except ValueError as exc:
        if "collides with zone" not in str(exc):
            raise AssertionError(f"missing endpoint collision diagnostic: {exc}") from exc
    else:
        raise AssertionError("duplicate socket multicast endpoints were accepted")


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


def test_wait_for_launched_vms_reaps_and_reports_nonzero_nodes(module) -> None:
    class FakeProcess:
        def __init__(self, returncode: int):
            self.returncode = returncode
            self.wait_calls = 0

        def wait(self, timeout=None):
            if timeout is not None:
                raise AssertionError("normal VM wait unexpectedly used a timeout")
            self.wait_calls += 1
            return self.returncode

    processes = [FakeProcess(0), FakeProcess(7), FakeProcess(-9)]
    results = [
        module.LaunchResult(node_id=node_id, process=process, lines=[])
        for node_id, process in zip((2, 0, 1), processes, strict=True)
    ]
    try:
        module.wait_for_launched_vms(results)
    except module.VmExitError as exc:
        assert_equal(exc.failures, [(0, 7), (1, -9)], "failed VM statuses")
        for expected in ("VM0 exit=7", "VM1 exit=-9"):
            if expected not in str(exc):
                raise AssertionError(f"missing VM exit diagnostic {expected!r}: {exc}")
        assert_equal(
            module.incident_launch_error(exc),
            "one or more WOS VMs exited unsuccessfully: VM0 exit=7, VM1 exit=-9",
            "incident VM exit diagnostic",
        )
    else:
        raise AssertionError("nonzero VM exit statuses were accepted")
    assert_equal(
        [process.wait_calls for process in processes],
        [1, 1, 1],
        "all launched VMs reaped",
    )

    successful = FakeProcess(0)
    module.wait_for_launched_vms(
        [module.LaunchResult(node_id=3, process=successful, lines=[])]
    )
    assert_equal(successful.wait_calls, 1, "successful VM reaped")
    launch_error = module.LaunchError(
        node_id=4,
        lines=[],
        cause=RuntimeError("do not copy this detail into the incident"),
    )
    assert_equal(
        module.incident_launch_error(launch_error),
        "VM4 launch failed (RuntimeError)",
        "sanitized incident launch diagnostic",
    )
    bounded = module.VmExitError([(node_id, 1) for node_id in range(40)])
    if "+8 more" not in str(bounded) or "VM39" in str(bounded):
        raise AssertionError(f"VM failure diagnostic is not bounded: {bounded}")


def test_cluster_main_preserves_vm_exit_error_for_incident(module) -> None:
    with tempfile.TemporaryDirectory() as temporary:
        tmp = Path(temporary)
        config_path = tmp / "cluster.json"
        config_path.write_text('{"zones":[{"id":"GLOBAL"}]}')
        captured: dict = {}

        old_argv = sys.argv
        old_collect_unique_nodes = module.collect_unique_nodes
        old_cluster_node_spec = module.cluster_node_spec
        old_snapshot_node_logs = module.wosincident.snapshot_node_logs
        old_capture_safely = module.wosincident.capture_safely
        old_cluster_launch_guard = module.cluster_launch_guard
        old_launch_guarded = module.launch_guarded
        module.collect_unique_nodes = lambda _config: {0: {}}
        module.cluster_node_spec = lambda *_args, **_kwargs: {"id": 0}
        module.wosincident.snapshot_node_logs = lambda *_args, **_kwargs: {}
        module.wosincident.capture_safely = lambda **kwargs: captured.update(kwargs)
        module.cluster_launch_guard = lambda **_kwargs: contextlib.nullcontext()

        def fail_launch(*_args, **_kwargs):
            raise module.VmExitError([(0, 7)])

        module.launch_guarded = fail_launch
        sys.argv = [
            str(CLUSTER_SETUP),
            "--launch",
            "--no-setup",
            "--config",
            str(config_path),
            "--incident-output",
            str(tmp / "failure.wosincident"),
        ]
        diagnostics = io.StringIO()
        try:
            with contextlib.redirect_stderr(diagnostics):
                try:
                    module.main()
                except SystemExit as exc:
                    assert_equal(exc.code, 1, "cluster VM failure exit code")
                else:
                    raise AssertionError("cluster VM failure did not exit nonzero")
        finally:
            sys.argv = old_argv
            module.collect_unique_nodes = old_collect_unique_nodes
            module.cluster_node_spec = old_cluster_node_spec
            module.wosincident.snapshot_node_logs = old_snapshot_node_logs
            module.wosincident.capture_safely = old_capture_safely
            module.cluster_launch_guard = old_cluster_launch_guard
            module.launch_guarded = old_launch_guarded

        assert_equal(captured.get("run_complete"), False, "failed cluster run status")
        if "VM0 exit=7" not in diagnostics.getvalue():
            raise AssertionError("cluster VM failure was not printed")
        assert_equal(
            captured.get("run_error"),
            "one or more WOS VMs exited unsuccessfully: VM0 exit=7",
            "captured cluster VM failure",
        )


def test_wki_auth_provisioning_is_private_pairwise_and_file_backed(module) -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        nodes = {
            node_id: {
                "effective": {
                    "vm": {"overlay_dir": str(root / f"node-{node_id}")},
                },
                "zones": [],
            }
            for node_id in range(3)
        }
        paths = module.provision_wki_auth(nodes)
        assert_equal(sorted(paths), [0, 1, 2], "credential node set")

        parsed: dict[int, dict[int, tuple[int, int, bytes]]] = {}
        for node_id, path in paths.items():
            if path.stat().st_mode & 0o077:
                raise AssertionError(f"credential file is not private: {path}")
            blob = path.read_bytes()
            magic, version, local_node, count, reserved = struct.unpack_from(
                "<8sHHHH", blob
            )
            assert_equal(magic, module.WKI_AUTH_MAGIC, "credential magic")
            assert_equal(version, module.WKI_AUTH_VERSION, "credential version")
            assert_equal(local_node, node_id + 1, "stable WKI node identity")
            assert_equal(count, 2, "pairwise credential count")
            assert_equal(reserved, 0, "credential reserved field")
            entries = {}
            offset = struct.calcsize("<8sHHHH")
            for _ in range(count):
                peer, key_id, policy, key = struct.unpack_from("<HHQ32s", blob, offset)
                offset += struct.calcsize("<HHQ32s")
                entries[peer - 1] = (key_id, policy, key)
            assert_equal(offset, len(blob), "exact credential file size")
            parsed[node_id] = entries

        for left in range(3):
            for right in range(left + 1, 3):
                assert_equal(
                    parsed[left][right],
                    parsed[right][left],
                    f"pair {left}-{right} shares one key and policy",
                )
        if parsed[0][1][2] == parsed[0][2][2]:
            raise AssertionError("distinct peer pairs reused a PSK")

        spec = {
            "id": 0,
            "hostname": "wos-0",
            "vm": {"overlay_dir": str(root / "node-0")},
            "nics": [],
            "fw_cfg_files": [
                {"name": "opt/wos/wki-auth", "path": str(paths[0])}
            ],
        }
        args = module.node_setup.build_qemu_args(spec, log=lambda _line: None)
        fw_cfg = [args[index + 1] for index, value in enumerate(args) if value == "-fw_cfg"]
        expected = f"name=opt/wos/wki-auth,file={paths[0].resolve()}"
        if expected not in fw_cfg:
            raise AssertionError(f"private fw_cfg file missing: {fw_cfg!r}")
        for _key_id, _policy, key in parsed[0].values():
            if key.hex() in " ".join(args):
                raise AssertionError("credential bytes leaked into QEMU argv")


def main() -> None:
    module = load_module()
    tests = [
        test_topology_probe_is_timeout_bounded,
        test_no_setup_topology_accepts_configured_links,
        test_no_setup_topology_rejects_missing_or_stale_links,
        test_zone_node_ids_preserve_legacy_and_validate_explicit_membership,
        test_no_setup_topology_uses_only_explicit_zone_members,
        test_running_wos_qemu_probe_filters_unrelated_processes,
        test_cluster_launch_guard_rejects_second_launcher,
        test_cluster_launch_guard_rejects_preexisting_wos_qemu,
        test_fixed_resource_benchmark_topologies,
        test_node_overlay_creation_failure_aborts_launch_prep,
        test_usb_hotplug_qemu_args_and_qmp_device_shape,
        test_generic_qmp_socket_is_opt_in_and_independent_of_usb,
        test_debug_nodes_get_qmp_and_loopback_only_debug_sockets,
        test_live_debug_descriptor_is_private_atomic_and_nonce_owned,
        test_live_debug_descriptor_schema_and_process_identity,
        test_launch_lifecycle_publishes_then_removes_descriptor,
        test_partial_launch_failure_removes_stale_descriptor,
        test_cluster_and_ktest_help_expose_descriptor_override,
        test_wki_chaos_transport_configs_use_isolated_artifacts,
        test_routed_wki_chaos_config_resolves_overlap_and_specs,
        test_rootless_wki_auth_config_has_no_host_link_dependency,
        test_rootless_wki_auth_mixed_config_validates_shared_memory,
        test_socket_multicast_backend_validation_qemu_and_no_setup,
        test_launch_one_vm_wraps_overlay_creation_failure_without_popen,
        test_wait_for_launched_vms_reaps_and_reports_nonzero_nodes,
        test_cluster_main_preserves_vm_exit_error_for_incident,
        test_wki_auth_provisioning_is_private_pairwise_and_file_backed,
    ]
    for test in tests:
        test(module)
    print(f"{len(tests)} cluster_setup tests passed")


if __name__ == "__main__":
    main()
