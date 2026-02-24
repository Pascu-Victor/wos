#!/usr/bin/env python3
"""
cluster_setup.py — Declarative WKI cluster topology manager.

Reads configs/cluster.json and creates/tears down the entire host-side cluster
topology: bridges, TAPs, ivshmem shared memory files. Replaces the ad-hoc
shell scripts with a declarative, per-zone, per-node configurable system.

Usage:
    python3 scripts/cluster_setup.py                     # setup (default)
    python3 scripts/cluster_setup.py --setup              # explicit setup
    python3 scripts/cluster_setup.py --launch             # setup + launch VMs
    python3 scripts/cluster_setup.py --teardown           # destroy everything
    python3 scripts/cluster_setup.py --config path.json   # custom config

Note: sudo is used internally only for privileged operations (network setup).
      The script will prompt for your password once if needed.
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from copy import deepcopy
from pathlib import Path
from itertools import combinations


# ---------------------------------------------------------------------------
# Config loading and resolution
# ---------------------------------------------------------------------------


def load_config(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def find_global(zones: list) -> dict:
    for z in zones:
        if z.get("id") == "GLOBAL":
            return z
    return {}


def find_zone(zones: list, zone_id) -> dict:
    for z in zones:
        if z.get("id") == zone_id:
            return z
    return {}


def merge_section(base: dict, override: dict) -> dict:
    """Shallow merge: override keys replace base keys within a section."""
    result = deepcopy(base)
    result.update(override)
    return result


def resolve_config(global_cfg: dict, zone_cfg: dict, node_cfg: dict = None) -> dict:
    """Cascade: GLOBAL -> ZONE -> NODE. Each section merges independently."""
    result = deepcopy(global_cfg)

    # Merge zone-level overrides
    for key in zone_cfg:
        if key in ("id", "name", "nodes", "nodes_config"):
            continue
        if isinstance(zone_cfg[key], dict) and isinstance(result.get(key), dict):
            result[key] = merge_section(result[key], zone_cfg[key])
        else:
            result[key] = deepcopy(zone_cfg[key])

    # Merge node-level overrides
    if node_cfg:
        for key in node_cfg:
            if key in ("id", "name"):
                continue
            if isinstance(node_cfg[key], dict) and isinstance(result.get(key), dict):
                result[key] = merge_section(result[key], node_cfg[key])
            else:
                result[key] = deepcopy(node_cfg[key])

    return result


def get_node_config(zone_cfg: dict, node_id: int) -> dict:
    """Find node-specific overrides from nodes_config array."""
    for nc in zone_cfg.get("nodes_config", []):
        if nc.get("id") == node_id:
            return nc
    return {}


# ---------------------------------------------------------------------------
# Naming conventions — deterministic from config, no state file
# ---------------------------------------------------------------------------


def zone_name(zone_cfg: dict) -> str:
    if "name" in zone_cfg:
        return zone_cfg["name"]
    return f"Z{zone_cfg['id']}"


def bridge_name(zone_cfg: dict) -> str:
    return f"wos-{zone_name(zone_cfg)}-br"


def tap_name(zone_cfg: dict, node_id: int) -> str:
    return f"wos-{zone_name(zone_cfg)}-N{node_id}"


def ivshmem_file(root_path: str, zone_cfg: dict, node_a: int, node_b: int) -> str:
    a, b = min(node_a, node_b), max(node_a, node_b)
    return os.path.join(root_path, f"wos-{zone_name(zone_cfg)}-{a}-{b}")


def mac_addr(zone_id: int, node_id: int, seq: int = 0) -> str:
    return f"52:54:00:{zone_id:02x}:{node_id:02x}:{seq:02x}"


# ---------------------------------------------------------------------------
# ivshmem topology — compute link pairs
# ---------------------------------------------------------------------------


def ivshmem_links(zone_cfg: dict, num_nodes: int) -> list:
    """Return list of (nodeA, nodeB) pairs based on topology."""
    ivshmem = zone_cfg.get("ivshmem", {})
    if not ivshmem.get("enabled", False):
        return []

    topology = ivshmem.get("topology", "full-mesh")

    if topology == "full-mesh":
        return list(combinations(range(num_nodes), 2))
    elif topology == "ring":
        pairs = [(i, (i + 1) % num_nodes) for i in range(num_nodes)]
        return [(min(a, b), max(a, b)) for a, b in pairs]
    elif topology == "star":
        return [(0, i) for i in range(1, num_nodes)]
    elif topology == "pairs":
        explicit = ivshmem.get("ivshmem_links", [])
        return [(min(a, b), max(a, b)) for a, b in explicit]
    else:
        print(
            f"WARNING: Unknown ivshmem topology '{topology}', defaulting to full-mesh"
        )
        return list(combinations(range(num_nodes), 2))


# ---------------------------------------------------------------------------
# Shell helpers
# ---------------------------------------------------------------------------


def run(cmd: str, check=True, quiet=False, privileged=False):
    """Run a shell command, optionally with sudo, optionally ignoring errors."""
    if privileged:
        cmd = f"sudo {cmd}"
    if not quiet:
        print(f"  $ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"    FAILED: {result.stderr.strip()}")
        return False
    return True


def ensure_sudo():
    """Validate/cache sudo credentials so the user is prompted once upfront."""
    result = subprocess.run(["sudo", "-v"], capture_output=False)
    if result.returncode != 0:
        print("ERROR: Failed to obtain sudo privileges.", file=sys.stderr)
        sys.exit(1)


def link_exists(name: str) -> bool:
    return (
        subprocess.run(
            f"ip link show {name}", shell=True, capture_output=True
        ).returncode
        == 0
    )


def parse_size(s: str) -> int:
    """Parse size string like '16M' to bytes."""
    s = s.strip().upper()
    if s.endswith("M"):
        return int(s[:-1]) * 1024 * 1024
    elif s.endswith("G"):
        return int(s[:-1]) * 1024 * 1024 * 1024
    elif s.endswith("K"):
        return int(s[:-1]) * 1024
    return int(s)


def reattach_libvirt_vms(bridge: str):
    """Re-attach any running libvirt VM interfaces that target *bridge*.

    When the cluster setup script (re)creates a bridge, any libvirt TAP
    interfaces previously enslaved to it lose their master.  This queries
    ``virsh`` for every running VM and re-attaches interfaces whose
    configured source bridge matches *bridge*.
    """
    # List running VMs via the system connection
    result = subprocess.run(
        "virsh -c qemu:///system list --name 2>/dev/null",
        shell=True,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return  # libvirt not available — nothing to do

    vms = [name.strip() for name in result.stdout.splitlines() if name.strip()]
    for vm in vms:
        result = subprocess.run(
            f"virsh -c qemu:///system domiflist {vm} 2>/dev/null",
            shell=True,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            continue
        for line in result.stdout.splitlines():
            cols = line.split()
            # columns: Interface  Type  Source  Model  MAC
            if len(cols) >= 3 and cols[2] == bridge:
                iface = cols[0]
                if iface == "Interface" or iface.startswith("-"):
                    continue
                if not link_exists(iface):
                    continue
                # Check if already attached to the correct bridge
                check = subprocess.run(
                    f"ip -o link show {iface}",
                    shell=True,
                    capture_output=True,
                    text=True,
                )
                if f"master {bridge}" in check.stdout:
                    continue
                run(f"ip link set {iface} master {bridge}", privileged=True)
                run(f"ip link set {iface} up", privileged=True)
                print(f"  Re-attached libvirt VM '{vm}' interface {iface} -> {bridge}")


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def setup(config: dict):
    zones = [z for z in config["zones"] if z.get("id") != "GLOBAL"]
    global_cfg = find_global(config["zones"])

    print("=== Setting up cluster topology ===\n")

    # Disable bridge netfilter so iptables doesn't filter bridged traffic
    # (DHCP, ICMP, ARP all need to pass through bridges unfiltered)
    run("modprobe br_netfilter 2>/dev/null || true", quiet=True, privileged=True)
    run(
        "sysctl -q -w net.bridge.bridge-nf-call-iptables=0", quiet=True, privileged=True
    )
    run(
        "sysctl -q -w net.bridge.bridge-nf-call-ip6tables=0",
        quiet=True,
        privileged=True,
    )
    run(
        "sysctl -q -w net.bridge.bridge-nf-call-arptables=0",
        quiet=True,
        privileged=True,
    )

    for zone_cfg in zones:
        zname = zone_name(zone_cfg)
        num_nodes = zone_cfg.get("nodes", 2)
        effective = resolve_config(global_cfg, zone_cfg)
        mtu = effective.get("mtu", 9000)

        print(f"--- Zone: {zname} ({num_nodes} nodes) ---")

        # Create bridge
        br = bridge_name(zone_cfg)
        bridge_cfg = effective.get("bridge", {})
        if not link_exists(br):
            run(f"ip link add {br} type bridge", privileged=True)
            run(f"ip link set {br} mtu {mtu}", privileged=True)

            # Disable STP if configured
            if not bridge_cfg.get("stp", False):
                run(
                    f"sh -c 'echo 0 > /sys/class/net/{br}/bridge/stp_state'",
                    quiet=True,
                    privileged=True,
                )

            run(f"ip link set {br} up", privileged=True)
            print(f"  Created bridge: {br} (MTU {mtu})")
        else:
            print(f"  Bridge {br} already exists")

        # Re-attach any running libvirt VM interfaces that belong on this bridge
        # (they get detached when the bridge is recreated)
        reattach_libvirt_vms(br)

        # Attach uplink device to bridge (e.g. vmnet1 for router VM access)
        uplink = bridge_cfg.get("uplink")
        if uplink:
            if link_exists(uplink):
                # Remove existing IP from uplink to avoid conflicts
                run(f"ip addr flush dev {uplink}", quiet=True, privileged=True)
                run(f"ip link set {uplink} master {br}", privileged=True)
                run(f"ip link set {uplink} up", privileged=True)
                print(f"  Attached uplink: {uplink} -> {br}")
            else:
                print(f"  WARNING: uplink {uplink} not found — skipping")

        # Assign IP to bridge if specified
        ip_addr = bridge_cfg.get("ip")
        if ip_addr:
            run(f"ip addr add {ip_addr} dev {br}", check=False, privileged=True)
            print(f"  Bridge IP: {ip_addr}")

        # Create TAP devices
        real_user = os.environ.get("USER", "nobody")
        for node_id in range(num_nodes):
            tap = tap_name(zone_cfg, node_id)
            if not link_exists(tap):
                run(
                    f"ip tuntap add dev {tap} mode tap user {real_user}",
                    privileged=True,
                )
                run(f"ip link set {tap} master {br}", privileged=True)
                run(f"ip link set {tap} mtu {mtu}", privileged=True)
                run(f"ip link set {tap} up", privileged=True)
                print(f"  Created TAP: {tap}")
            else:
                print(f"  TAP {tap} already exists")

        # Create ivshmem backing files (pre-created so both VMs share the same file)
        links = ivshmem_links(zone_cfg, num_nodes)
        if links:
            ivshmem_cfg = effective.get("ivshmem", {})
            root_path = ivshmem_cfg.get("root_path", "/dev/shm")
            size_str = ivshmem_cfg.get("size", "16M")
            size_bytes = parse_size(size_str)
            size_mb = size_bytes // (1024 * 1024)

            for node_a, node_b in links:
                fpath = ivshmem_file(root_path, zone_cfg, node_a, node_b)
                # Always recreate for clean state; use sudo for dd/chmod in case
                # a previous root-owned file exists
                run(
                    f"dd if=/dev/zero of={fpath} bs=1M count={size_mb} 2>/dev/null",
                    quiet=True,
                    privileged=True,
                )
                run(f"chmod 666 {fpath}", quiet=True, privileged=True)
                print(f"  Created ivshmem: {fpath} ({size_str})")

        print()

    print("=== Cluster topology ready ===")


# ---------------------------------------------------------------------------
# Teardown — stateless, derived from config
# ---------------------------------------------------------------------------


def teardown(config: dict):
    zones = [z for z in config["zones"] if z.get("id") != "GLOBAL"]
    global_cfg = find_global(config["zones"])

    print("=== Tearing down cluster topology ===\n")

    for zone_cfg in zones:
        zname = zone_name(zone_cfg)
        num_nodes = zone_cfg.get("nodes", 2)
        effective = resolve_config(global_cfg, zone_cfg)

        print(f"--- Zone: {zname} ---")

        # Detach uplink from bridge before deleting
        bridge_cfg = effective.get("bridge", {})
        uplink = bridge_cfg.get("uplink")
        if uplink and link_exists(uplink):
            run(
                f"ip link set {uplink} nomaster",
                check=False,
                quiet=True,
                privileged=True,
            )
            print(f"  Detached uplink: {uplink}")

        # Delete TAP devices
        for node_id in range(num_nodes):
            tap = tap_name(zone_cfg, node_id)
            if link_exists(tap):
                run(f"ip link set {tap} down", check=False, quiet=True, privileged=True)
                run(f"ip tuntap del dev {tap} mode tap", check=False, privileged=True)
                print(f"  Deleted TAP: {tap}")

        # Delete bridge
        br = bridge_name(zone_cfg)
        if link_exists(br):
            run(f"ip link set {br} down", check=False, quiet=True, privileged=True)
            run(f"ip link delete {br} type bridge", check=False, privileged=True)
            print(f"  Deleted bridge: {br}")

        # Delete ivshmem files
        links = ivshmem_links(zone_cfg, num_nodes)
        if links:
            ivshmem_cfg = effective.get("ivshmem", {})
            root_path = ivshmem_cfg.get("root_path", "/dev/shm")
            for node_a, node_b in links:
                fpath = ivshmem_file(root_path, zone_cfg, node_a, node_b)
                if os.path.exists(fpath):
                    run(f"rm -f {fpath}", check=False, quiet=True, privileged=True)
                    print(f"  Deleted ivshmem: {fpath}")

        print()

    # Remove overlay directory
    overlay_dir = "cluster-overlays"
    if os.path.isdir(overlay_dir):
        import shutil

        shutil.rmtree(overlay_dir)
        print(f"  Removed {overlay_dir}/")

    print("=== Teardown complete ===")


# ---------------------------------------------------------------------------
# Launch — setup + start VMs
# ---------------------------------------------------------------------------


def collect_unique_nodes(config: dict) -> dict:
    """Build a map: node_id -> list of (zone_cfg, effective_cfg, node_cfg)."""
    zones = [z for z in config["zones"] if z.get("id") != "GLOBAL"]
    global_cfg = find_global(config["zones"])

    nodes = {}
    for zone_cfg in zones:
        num_nodes = zone_cfg.get("nodes", 2)
        for node_id in range(num_nodes):
            node_override = get_node_config(zone_cfg, node_id)
            effective = resolve_config(global_cfg, zone_cfg, node_override)
            if node_id not in nodes:
                nodes[node_id] = {
                    "effective": effective,
                    "node_override": node_override,
                    "zones": [],
                }
            nodes[node_id]["zones"].append(zone_cfg)
            # Merge node-level settings (debug, memory, etc.) from any zone
            if node_override.get("debug"):
                nodes[node_id]["effective"]["debug"] = True
            vm_override = node_override.get("vm", {})
            if vm_override:
                nodes[node_id]["effective"]["vm"] = merge_section(
                    nodes[node_id]["effective"].get("vm", {}), vm_override
                )
    return nodes


def build_qemu_args(node_id: int, node_info: dict, config: dict) -> list:
    """Build QEMU command line for a single node."""
    eff = node_info["effective"]
    vm_cfg = eff.get("vm", {})
    global_cfg = find_global(config["zones"])

    memory = vm_cfg.get("memory", "4G")
    cpus = vm_cfg.get("cpus", 2)
    disk0 = vm_cfg.get("disk0", "disk.qcow2")
    disk1 = vm_cfg.get("disk1", "test_fat32.qcow2")
    is_debug = eff.get("debug", False)

    # Create overlay disks
    overlay_dir = "cluster-overlays"
    os.makedirs(overlay_dir, exist_ok=True)

    overlay0 = os.path.join(overlay_dir, f"disk-vm{node_id}.qcow2")
    overlay1 = os.path.join(overlay_dir, f"fat32-vm{node_id}.qcow2")

    # Remove old overlays for clean state (may be root-owned from previous sudo run)
    for f in [overlay0, overlay1]:
        if os.path.exists(f):
            try:
                os.remove(f)
            except PermissionError:
                run(f"rm -f {f}", quiet=True, privileged=True)

    abs_disk0 = os.path.abspath(disk0)
    abs_disk1 = os.path.abspath(disk1)

    result = subprocess.run(
        f"qemu-img create -f qcow2 -b {abs_disk0} -F qcow2 {overlay0}",
        shell=True,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  ERROR creating overlay {overlay0}: {result.stderr.strip()}")

    result = subprocess.run(
        f"qemu-img create -f qcow2 -b {abs_disk1} -F qcow2 {overlay1}",
        shell=True,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"  ERROR creating overlay {overlay1}: {result.stderr.strip()}")

    serial_log = f"serial-vm{node_id}.log"
    qemu_log = f"qemu-vm{node_id}.%d.log"

    # Remove old logs
    for f in Path(".").glob(f"qemu-vm{node_id}.*log"):
        f.unlink()
    if os.path.exists(serial_log):
        os.remove(serial_log)

    args = [
        "qemu-system-x86_64",
        "-M",
        "q35",
        "-cpu",
        "host",
        "--enable-kvm",
        "-m",
        memory,
        "-smp",
        str(cpus),
        "-drive",
        f"file={overlay0},if=none,id=drive0,format=qcow2",
        "-device",
        "ahci,id=ahci",
        "-device",
        "ide-hd,drive=drive0,bus=ahci.0",
        "-drive",
        f"file={overlay1},if=none,id=drive1,format=qcow2",
        "-device",
        "ide-hd,drive=drive1,bus=ahci.1",
        "-chardev",
        f"file,id=char0,path={serial_log}",
        "-serial",
        "chardev:char0",
        "-display",
        "none",
        "-bios",
        "/usr/share/OVMF/x64/OVMF.4m.fd",
        "-d",
        "cpu_reset,int,tid,in_asm,nochain,guest_errors,page,trace:ps2_keyboard_set_translation",
        "-D",
        qemu_log,
        "-no-reboot",
    ]

    # Add NICs — one per zone this node participates in
    nic_idx = 0
    for zone_cfg in node_info["zones"]:
        zone_id = zone_cfg["id"]
        node_override = get_node_config(zone_cfg, node_id)
        zone_eff = resolve_config(global_cfg, zone_cfg, node_override)
        nic_model = zone_eff.get("nic_model", "virtio-net-pci")
        tap = tap_name(zone_cfg, node_id)
        mac = mac_addr(zone_id, node_id, 0)

        args.extend(
            [
                "-netdev",
                f"tap,id=net{nic_idx},ifname={tap},script=no,downscript=no",
                "-device",
                f"{nic_model},netdev=net{nic_idx},mac={mac}",
            ]
        )
        nic_idx += 1

    # Add ivshmem devices — one per ivshmem link involving this node.
    # Pre-created /dev/shm files avoid BAR placement issues with hugepages
    # on hosts with above-4G decoding enabled.
    ivshmem_idx = 0
    for zone_cfg in node_info["zones"]:
        zone_eff = resolve_config(global_cfg, zone_cfg)
        num_nodes = zone_cfg.get("nodes", 2)
        links = ivshmem_links(zone_cfg, num_nodes)

        ivshmem_cfg = zone_eff.get("ivshmem", {})
        root_path = ivshmem_cfg.get("root_path", "/dev/shm")
        size_str = ivshmem_cfg.get("size", "16M")

        for node_a, node_b in links:
            if node_a == node_id or node_b == node_id:
                fpath = ivshmem_file(root_path, zone_cfg, node_a, node_b)
                args.extend(
                    [
                        "-object",
                        f"memory-backend-file,size={size_str},share=on,mem-path={fpath},id=hmem{ivshmem_idx}",
                        "-device",
                        f"ivshmem-plain,memdev=hmem{ivshmem_idx}",
                    ]
                )
                ivshmem_idx += 1

    # Debug mode
    if is_debug:
        gdb_port = 1234 + node_id
        debugcon_port = 23456 + node_id
        monitor_port = 3002 + node_id

        args.extend(["-gdb", f"tcp:127.0.0.1:{gdb_port}", "-S"])
        args.extend(
            [
                "-chardev",
                f"socket,id=debugger,port={debugcon_port},host=0.0.0.0,server=on,wait=off,telnet=on",
                "-device",
                "isa-debugcon,iobase=0x402,chardev=debugger",
            ]
        )
        args.extend(["-monitor", f"tcp:0.0.0.0:{monitor_port},server,nowait"])

        print(
            f"  [VM{node_id}] DEBUG: gdb=127.0.0.1:{gdb_port} debugcon={debugcon_port} monitor={monitor_port}"
        )

    return args


def launch(config: dict):
    """Setup topology, then launch VMs."""
    setup(config)
    print()

    nodes = collect_unique_nodes(config)
    pids = []

    print("=== Launching VMs ===\n")

    for node_id in sorted(nodes.keys()):
        node_info = nodes[node_id]
        args = build_qemu_args(node_id, node_info, config)

        zone_names = [zone_name(z) for z in node_info["zones"]]
        is_debug = node_info["effective"].get("debug", False)

        print(f"  [VM{node_id}] zones={zone_names} debug={is_debug}")
        print(f"    cmd: {' '.join(args)}")

        proc = subprocess.Popen(args)
        pids.append(proc)
        print(f"    PID: {proc.pid}")

        # Brief delay between launches to avoid races on shared ivshmem files
        if node_id < max(nodes.keys()):
            time.sleep(0.5)

    print(f"\n=== {len(pids)} VMs launched ===")
    print("Press Ctrl+C to stop all VMs.\n")

    def shutdown(signum, frame):
        print("\n=== Shutting down cluster ===")
        for p in pids:
            try:
                p.terminate()
            except OSError:
                pass
        for p in pids:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        print("=== Cluster stopped ===")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Wait for all VMs
    for p in pids:
        try:
            p.wait()
        except KeyboardInterrupt:
            shutdown(None, None)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="WKI cluster topology manager")
    parser.add_argument(
        "--setup", action="store_true", help="Create topology (default)"
    )
    parser.add_argument("--launch", action="store_true", help="Setup + launch VMs")
    parser.add_argument("--teardown", action="store_true", help="Destroy topology")
    parser.add_argument(
        "--config", default="configs/cluster.json", help="Config file path"
    )
    args = parser.parse_args()

    config = load_config(args.config)

    # Cache sudo credentials once upfront for privileged network operations
    ensure_sudo()

    if args.teardown:
        teardown(config)
    elif args.launch:
        launch(config)
    else:
        setup(config)


if __name__ == "__main__":
    main()
