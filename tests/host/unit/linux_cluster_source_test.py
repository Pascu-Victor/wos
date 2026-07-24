#!/usr/bin/env python3

import copy
import contextlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[3]
CLUSTER_DIR = ROOT / "scripts" / "cluster"
HELPER_PATH = CLUSTER_DIR / "linux_cluster_config.py"
LAUNCHER_PATH = CLUSTER_DIR / "run_linux_cluster.sh"
ORCHESTRATOR_PATH = ROOT / "scripts" / "bench" / "benchmark_orchestrator.py"
EXPECTED_LAYOUTS = {
    1: [(0, 32, 32768 * 1024)],
    2: [(0, 16, 16384 * 1024), (1, 16, 16384 * 1024)],
    3: [
        (0, 11, 10923 * 1024),
        (1, 11, 10923 * 1024),
        (2, 10, 10922 * 1024),
    ],
    4: [(node_id, 8, 32768 * 1024) for node_id in range(4)],
}


def fail(message: str) -> None:
    raise AssertionError(message)


def assert_equal(actual, expected, context: str) -> None:
    if actual != expected:
        fail(f"{context}: expected {expected!r}, got {actual!r}")


def load_module(path: Path, module_name: str):
    if str(CLUSTER_DIR) not in sys.path:
        sys.path.insert(0, str(CLUSTER_DIR))
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        fail(f"failed to load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def base_domain_xml(*, current_memory: bool = True, numa: bool = False) -> str:
    current = "<currentMemory unit='MiB'>4096</currentMemory>" if current_memory else ""
    numa_xml = "<numa><cell id='0' cpus='0-3' memory='4194304'/></numa>" if numa else ""
    return f"""
<domain type='kvm'>
  <name>ubuntu-base</name>
  <uuid>00000000-0000-0000-0000-000000000001</uuid>
  <memory unit='MiB'>4096</memory>
  {current}
  <maxMemory slots='16' unit='MiB'>8192</maxMemory>
  <vcpu placement='static' current='3' cpuset='0-3'>4</vcpu>
  <vcpus><vcpu id='0' enabled='yes'/><vcpu id='3' enabled='no'/></vcpus>
  <cputune>
    <vcpupin vcpu='0' cpuset='0'/>
    <vcpusched vcpus='0-3' scheduler='fifo'/>
    <emulatorpin cpuset='4'/>
    <shares>1024</shares>
  </cputune>
  <cpu mode='host-passthrough'>
    <topology sockets='2' cores='2' threads='1'/>
    {numa_xml}
    <feature policy='require' name='invtsc'/>
  </cpu>
  <os><type>hvm</type><nvram>/old_VARS.fd</nvram></os>
  <metadata><keep-me value='yes'/></metadata>
  <devices>
    <disk type='file' device='disk'>
      <source file='/var/lib/libvirt/images/ubuntu-base.qcow2'/>
      <target dev='vda' bus='virtio'/>
    </disk>
    <interface type='bridge'>
      <mac address='52:54:00:00:00:01'/>
      <source bridge='old-bridge'/>
      <target dev='vnet0'/>
      <script path='/old/network-script'/>
      <link state='down'/>
    </interface>
  </devices>
</domain>
"""


def domain_identity(helper):
    return helper.DomainIdentity(
        domain_name="wos-ubuntu-vm2",
        base_disk="/var/lib/libvirt/images/ubuntu-base.qcow2",
        disk_path="/var/lib/libvirt/images/wos-ubuntu-vm2.qcow2",
        nvram_path="/var/lib/libvirt/qemu/nvram/wos-ubuntu-vm2_VARS.fd",
        tap_name="wlu-lan-N2",
        mac_address="52:54:00:22:34:82",
    )


def test_fixed_resource_resolution(helper) -> None:
    for node_count, expected in EXPECTED_LAYOUTS.items():
        resources = helper.resolve_node_resources(
            ROOT / "configs" / f"cluster_bench_{node_count}.json", node_count
        )
        actual = [
            (resource.node_id, resource.vcpus, resource.memory_kib)
            for resource in resources
        ]
        assert_equal(actual, expected, f"{node_count}-node Linux resources")

    result = subprocess.run(
        [
            sys.executable,
            str(HELPER_PATH),
            "resources",
            "--config",
            "configs/cluster_bench_3.json",
            "--num-vms",
            "3",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert_equal(result.returncode, 0, "resource CLI exit")
    assert_equal(
        result.stdout.splitlines(),
        ["0\t11\t11185152", "1\t11\t11185152", "2\t10\t11184128"],
        "resource CLI original node IDs",
    )
    matched = helper.resolve_node_resources(
        ROOT / "configs" / "cluster_selfhost_linux_kvm.json", 1
    )
    assert_equal(
        [(resource.node_id, resource.vcpus, resource.memory_kib) for resource in matched],
        [(0, 36, 98304 * 1024)],
        "matched Linux KVM self-host resources",
    )


def test_resource_validation(helper) -> None:
    config = json.loads(
        (ROOT / "configs" / "cluster_bench_1.json").read_text(encoding="utf-8")
    )
    mutations = [
        (lambda value: value["zones"][0]["vm"].pop("cpus"), "CPU and memory"),
        (lambda value: value["zones"][0]["vm"].__setitem__("cpus", True), "vCPU count"),
        (lambda value: value["zones"][0]["vm"].__setitem__("memory", "0M"), "VM memory"),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        for index, (mutate, diagnostic) in enumerate(mutations):
            broken = copy.deepcopy(config)
            mutate(broken)
            path = Path(tmp) / f"broken-{index}.json"
            path.write_text(json.dumps(broken), encoding="utf-8")
            try:
                helper.resolve_node_resources(path, 1)
            except ValueError as exc:
                if diagnostic not in str(exc):
                    fail(f"missing {diagnostic!r} in resource diagnostic: {exc}")
            else:
                fail(f"invalid resource config {index} was accepted")

    for configured, requested in ((3, 2), (2, 3)):
        try:
            helper.resolve_node_resources(
                ROOT / "configs" / f"cluster_bench_{configured}.json", requested
            )
        except ValueError as exc:
            message = str(exc)
            if str(configured) not in message or str(requested) not in message:
                fail(f"node mismatch diagnostic lacks both counts: {message}")
        else:
            fail(f"{configured}-node config was accepted for {requested} guests")


def test_configured_domain_patch(helper) -> None:
    identity = domain_identity(helper)
    resources = helper.NodeResources(node_id=2, vcpus=10, memory_kib=10922 * 1024)
    root = ET.fromstring(base_domain_xml())
    helper.patch_domain_root(root, identity, resources)
    helper.verify_domain_root(root, identity, resources)

    assert_equal(root.findtext("name"), identity.domain_name, "patched domain name")
    assert_equal(root.find("uuid"), None, "patched domain UUID removal")
    assert_equal(root.findtext("./os/nvram"), identity.nvram_path, "patched NVRAM")
    assert_equal(root.find("memory").attrib, {"unit": "KiB"}, "patched memory unit")
    assert_equal(root.findtext("memory"), str(10922 * 1024), "patched memory")
    assert_equal(root.findtext("currentMemory"), str(10922 * 1024), "patched current memory")
    assert_equal(root.findtext("maxMemory"), str(10922 * 1024), "raised max memory")
    vcpu = root.find("vcpu")
    assert_equal(vcpu.text, "10", "patched vCPU count")
    assert_equal(vcpu.attrib, {"placement": "static"}, "patched vCPU attributes")
    assert_equal(
        root.find("./cpu/topology").attrib,
        {"sockets": "1", "cores": "10", "threads": "1"},
        "patched CPU topology",
    )
    assert_equal(root.find("vcpus"), None, "stale hotplug removal")
    assert_equal(
        [child.tag for child in root.find("cputune")],
        ["emulatorpin", "shares"],
        "stale per-vCPU tuning removal",
    )
    assert_equal(root.find("./cpu/feature").get("name"), "invtsc", "unrelated CPU feature")
    assert_equal(root.find("./metadata/keep-me").get("value"), "yes", "unrelated metadata")
    assert_equal(
        root.find("./devices/disk/source").get("file"),
        identity.disk_path,
        "patched disk path",
    )
    interface = root.find("./devices/interface")
    assert_equal(interface.get("type"), "ethernet", "patched interface type")
    assert_equal(interface.find("source"), None, "patched interface source removal")
    assert_equal(interface.find("script"), None, "patched interface script removal")
    assert_equal(interface.find("target").get("dev"), identity.tap_name, "patched TAP")
    assert_equal(interface.find("mac").get("address"), identity.mac_address, "patched MAC")

    root.find("memory").text = "1"
    try:
        helper.verify_domain_root(root, identity, resources)
    except ValueError as exc:
        if "memory" not in str(exc):
            fail(f"unexpected defined-resource diagnostic: {exc}")
    else:
        fail("defined XML with wrong memory was accepted")

    with tempfile.TemporaryDirectory() as tmp:
        xml_path = Path(tmp) / "domain.xml"
        xml_path.write_text(base_domain_xml(), encoding="utf-8")
        identity_args = [
            "--domain-name",
            identity.domain_name,
            "--base-disk",
            identity.base_disk,
            "--disk-path",
            identity.disk_path,
            "--nvram-path",
            identity.nvram_path,
            "--tap-name",
            identity.tap_name,
            "--mac-address",
            identity.mac_address,
            "--cpus",
            "10",
            "--memory-kib",
            str(10922 * 1024),
        ]
        patch_result = subprocess.run(
            [sys.executable, str(HELPER_PATH), "patch-domain", str(xml_path), *identity_args],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert_equal(patch_result.returncode, 0, "domain patch CLI exit")
        verify_result = subprocess.run(
            [sys.executable, str(HELPER_PATH), "verify-domain", str(xml_path), *identity_args],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert_equal(verify_result.returncode, 0, "domain verify CLI exit")


def test_managed_network_domain_patch(helper) -> None:
    identity = helper.DomainIdentity(
        domain_name="wos-linux-perf",
        base_disk="/var/lib/libvirt/images/ubuntu-base.qcow2",
        disk_path="/var/lib/libvirt/images/wos-linux-perf.qcow2",
        nvram_path="/var/lib/libvirt/qemu/nvram/wos-linux-perf_VARS.fd",
        tap_name="",
        mac_address="52:54:00:22:34:80",
        network_name="bridge",
    )
    root = ET.fromstring(base_domain_xml())
    helper.patch_domain_root(root, identity)
    helper.verify_domain_root(root, identity)

    interface = root.find("./devices/interface")
    assert_equal(interface.get("type"), "network", "managed interface type")
    assert_equal(interface.find("source").attrib, {"network": "bridge"}, "managed source")
    assert_equal(interface.find("target"), None, "managed fixed TAP removal")
    assert_equal(interface.find("script"), None, "managed script removal")
    assert_equal(interface.find("link"), None, "managed fixed link removal")
    assert_equal(interface.find("mac").get("address"), identity.mac_address, "managed MAC")

    with tempfile.TemporaryDirectory() as tmp:
        xml_path = Path(tmp) / "domain.xml"
        xml_path.write_text(base_domain_xml(), encoding="utf-8")
        identity_args = [
            "--domain-name",
            identity.domain_name,
            "--base-disk",
            identity.base_disk,
            "--disk-path",
            identity.disk_path,
            "--nvram-path",
            identity.nvram_path,
            "--network-name",
            identity.network_name,
            "--mac-address",
            identity.mac_address,
        ]
        for command in ("patch-domain", "verify-domain"):
            result = subprocess.run(
                [sys.executable, str(HELPER_PATH), command, str(xml_path), *identity_args],
                cwd=ROOT,
                check=False,
                capture_output=True,
                text=True,
            )
            assert_equal(result.returncode, 0, f"managed {command} CLI exit")


def test_legacy_domain_patch_and_numa_rejection(helper) -> None:
    identity = domain_identity(helper)
    root = ET.fromstring(base_domain_xml(current_memory=False))
    original_resources = {
        "memory": ET.tostring(root.find("memory")),
        "vcpu": ET.tostring(root.find("vcpu")),
        "topology": ET.tostring(root.find("./cpu/topology")),
        "vcpus": ET.tostring(root.find("vcpus")),
        "cputune": ET.tostring(root.find("cputune")),
    }
    helper.patch_domain_root(root, identity)
    helper.verify_domain_root(root, identity)
    assert_equal(root.find("currentMemory"), None, "legacy missing currentMemory")
    for name, original in original_resources.items():
        path = name if name != "topology" else "./cpu/topology"
        assert_equal(ET.tostring(root.find(path)), original, f"legacy {name}")

    numa_root = ET.fromstring(base_domain_xml(numa=True))
    try:
        helper.patch_domain_root(
            numa_root,
            identity,
            helper.NodeResources(node_id=0, vcpus=4, memory_kib=4096 * 1024),
        )
    except ValueError as exc:
        if "NUMA" not in str(exc):
            fail(f"unexpected NUMA diagnostic: {exc}")
    else:
        fail("guest NUMA XML was silently resized")


def test_launcher_source_and_argument_errors() -> None:
    source = LAUNCHER_PATH.read_text(encoding="utf-8")
    for expected in (
        "--cluster-config)",
        "--libvirt-network)",
        'python3 "$RESOURCE_HELPER" resources',
        'python3 "$RESOURCE_HELPER" patch-domain',
        "dumpxml --inactive",
        'python3 "$RESOURCE_HELPER" verify-domain',
        "vol-create-as",
        "--backing-vol",
        '--cpus "${NODE_VCPUS[i]}"',
        '--memory-kib "${NODE_MEMORY_KIB[i]}"',
    ):
        if expected not in source:
            fail(f"Linux launcher is missing configured-resource plumbing {expected!r}")

    env = {"WOS_WORKSPACE_ROOT": str(ROOT)}
    for arguments, diagnostic in (
        (["--cluster-config"], "requires a path"),
        (["--unknown-option"], "unknown option"),
        (["0"], "positive integer"),
        (["2", "--skip", "2"], "count minus one"),
    ):
        result = subprocess.run(
            [str(LAUNCHER_PATH), *arguments],
            cwd=ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        assert_equal(result.returncode, 2, f"launcher rejection exit for {arguments}")
        if diagnostic not in result.stderr:
            fail(f"missing launcher diagnostic {diagnostic!r}: {result.stderr!r}")


def test_orchestrator_config_forwarding(orchestrator) -> None:
    config_path = "configs/a benchmark topology.json"
    args = SimpleNamespace(
        cluster_config=config_path,
        num_vms=3,
        skip=2,
        router_ip="10.10.0.1",
        host_ip="10.10.0.100",
        provision_hosts=[],
        wait_seconds=0,
    )
    calls: list[tuple[list[str], bool]] = []
    old_run = orchestrator.run_command
    old_ping = orchestrator.ping_once

    def fake_run(command, stream=False):
        calls.append((command, stream))
        stdout = "0\t11\t11185152\n1\t11\t11185152\n2\t10\t11184128\n" if "resources" in command else ""
        return subprocess.CompletedProcess(command, 0, stdout, "")

    orchestrator.run_command = fake_run
    orchestrator.ping_once = lambda _host: True
    output = io.StringIO()
    try:
        with contextlib.redirect_stdout(output):
            assert_equal(orchestrator.prepare_linux_cluster(args), 0, "configured linux-up")
    finally:
        orchestrator.run_command = old_run
        orchestrator.ping_once = old_ping

    assert_equal(
        calls,
        [
            (
                [
                    sys.executable,
                    str(orchestrator.LINUX_CLUSTER_CONFIG),
                    "resources",
                    "--config",
                    config_path,
                    "--num-vms",
                    "3",
                ],
                False,
            ),
            (
                [
                    str(orchestrator.CLUSTER_SCRIPTS / "cluster_setup.py"),
                    "--setup",
                    "--config",
                    config_path,
                ],
                True,
            ),
            (
                [
                    str(orchestrator.CLUSTER_SCRIPTS / "run_linux_cluster.sh"),
                    "3",
                    "--detach",
                    "--cluster-config",
                    config_path,
                    "--skip",
                    "2",
                ],
                True,
            ),
        ],
        "configured linux-up commands",
    )
    summary = json.loads(output.getvalue().splitlines()[-1])
    assert_equal(summary["cluster_config"], config_path, "linux-up config provenance")
    assert_equal(summary["launched_vms"], 1, "linux-up launched VM count")
    assert_equal(summary["resources"][2]["vcpus"], 10, "linux-up node 2 provenance")

    args.cluster_config = None
    args.skip = 0
    calls.clear()
    orchestrator.run_command = fake_run
    orchestrator.ping_once = lambda _host: False
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            assert_equal(orchestrator.prepare_linux_cluster(args), 0, "legacy linux-up")
    finally:
        orchestrator.run_command = old_run
        orchestrator.ping_once = old_ping
    assert_equal(
        calls,
        [
            ([str(orchestrator.CLUSTER_SCRIPTS / "cluster_setup.py"), "--setup"], True),
            (
                [
                    str(orchestrator.CLUSTER_SCRIPTS / "run_linux_cluster.sh"),
                    "3",
                    "--detach",
                ],
                True,
            ),
        ],
        "legacy linux-up commands",
    )


def main() -> None:
    helper = load_module(HELPER_PATH, "linux_cluster_config_under_test")
    orchestrator = load_module(ORCHESTRATOR_PATH, "benchmark_orchestrator_under_test")
    tests = [
        lambda: test_fixed_resource_resolution(helper),
        lambda: test_resource_validation(helper),
        lambda: test_configured_domain_patch(helper),
        lambda: test_managed_network_domain_patch(helper),
        lambda: test_legacy_domain_patch_and_numa_rejection(helper),
        test_launcher_source_and_argument_errors,
        lambda: test_orchestrator_config_forwarding(orchestrator),
    ]
    for test in tests:
        test()
    print(f"{len(tests)} Linux cluster source tests passed")


if __name__ == "__main__":
    main()
