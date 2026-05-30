#!/usr/bin/env python3
"""
cluster_setup.py - Declarative WKI cluster topology manager.

Reads configs/cluster.json and creates/tears down the entire host-side cluster
topology: bridges, TAPs, ivshmem shared memory files. Replaces the ad-hoc
shell scripts with a declarative, per-zone, per-node configurable system.

Usage:
    wos-cluster                     # setup (default)
    wos-cluster --setup              # explicit setup
    wos-cluster --launch             # setup + launch VMs
    wos-cluster --sync               # live-sync rootfs/sysroot files
    wos-cluster --teardown           # destroy everything
    wos-cluster --config path.json   # custom config

Note: sudo is used internally only for privileged operations (network setup).
      The script will prompt for your password once if needed.
"""

import argparse
import base64
import concurrent.futures
import hashlib
import json
import os
import pwd
import queue
import shutil
import signal
import stat
import struct
import subprocess
import sys
import tempfile
import threading
import time
from copy import deepcopy
from dataclasses import dataclass
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
# Naming conventions - deterministic from config, no state file
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
# ivshmem topology - compute link pairs
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


def ensure_vhost_net_available():
    if not os.path.exists("/dev/vhost-net"):
        raise RuntimeError(
            "multiqueue TAP requested, but /dev/vhost-net is unavailable "
            "after modprobe vhost_net"
        )
    if not os.access("/dev/vhost-net", os.R_OK | os.W_OK):
        raise RuntimeError(
            "multiqueue TAP requested, but the current user cannot access "
            "/dev/vhost-net"
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


def tap_owner_uid(name: str) -> int | None:
    result = subprocess.run(
        ["sudo", "-n", "ip", "tuntap", "show", "dev", name],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        result = subprocess.run(
            ["ip", "tuntap", "show", "dev", name],
            capture_output=True,
            text=True,
        )
    if result.returncode != 0:
        return None

    parts = result.stdout.strip().split()
    for idx, part in enumerate(parts):
        if part == "user" and idx + 1 < len(parts):
            try:
                return int(parts[idx + 1])
            except ValueError:
                pass
    return None


def tap_has_multiqueue(name: str) -> bool:
    """Return True if the TAP device was created with IFF_MULTI_QUEUE."""
    result = subprocess.run(
        ["sudo", "-n", "ip", "tuntap", "show", "dev", name],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        result = subprocess.run(
            ["ip", "tuntap", "show", "dev", name],
            capture_output=True,
            text=True,
        )
    return result.returncode == 0 and "multi_queue" in result.stdout


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
        return  # libvirt not available - nothing to do

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
# SSH host key management - persistent per-node dropbear keys
# ---------------------------------------------------------------------------

CLUSTER_DATA_DIR = "cluster-data"
SSH_KEYS_DIR = os.path.join(CLUSTER_DATA_DIR, "ssh-keys")


def _parse_der_length(data: bytes, offset: int) -> tuple:
    """Parse a DER length field. Returns (length, new_offset)."""
    b = data[offset]
    offset += 1
    if b < 0x80:
        return b, offset
    num_bytes = b & 0x7F
    length = int.from_bytes(data[offset : offset + num_bytes], "big")
    return length, offset + num_bytes


def _parse_der_integer(data: bytes, offset: int) -> tuple:
    """Parse a DER-encoded INTEGER. Returns (value, new_offset)."""
    if data[offset] != 0x02:
        raise ValueError(f"Expected INTEGER tag 0x02, got 0x{data[offset]:02x}")
    offset += 1
    length, offset = _parse_der_length(data, offset)
    value = int.from_bytes(data[offset : offset + length], "big")
    return value, offset + length


def _mpint_to_bytes(n: int) -> bytes:
    """Encode an integer as SSH mpint payload (without the 4-byte length)."""
    if n == 0:
        return b""
    byte_len = (n.bit_length() + 7) // 8
    raw = n.to_bytes(byte_len, "big")
    if raw[0] & 0x80:
        raw = b"\x00" + raw
    return raw


def _ssh_string(data: bytes) -> bytes:
    """Encode bytes as an SSH string (4-byte BE length + data)."""
    return struct.pack(">I", len(data)) + data


def _convert_pem_to_dropbear(pem_path: str, dropbear_path: str):
    """Convert a PKCS#1 PEM RSA private key to dropbear's native format.

    Parses the DER-encoded RSA key components (n, e, d, p, q) and writes
    them in the binary format that dropbear's buf_get_priv_key() expects:
        string "ssh-rsa" | mpint e | mpint n | mpint d | mpint p | mpint q
    """
    with open(pem_path) as f:
        pem = f.read()
    lines = pem.strip().split("\n")
    der_b64 = "".join(l for l in lines if not l.startswith("-----"))
    der = base64.b64decode(der_b64)

    # Parse PKCS#1 RSAPrivateKey SEQUENCE
    offset = 0
    if der[offset] != 0x30:
        raise ValueError("Expected SEQUENCE tag")
    offset += 1
    _, offset = _parse_der_length(der, offset)

    _version, offset = _parse_der_integer(der, offset)  # version (0)
    n, offset = _parse_der_integer(der, offset)
    e, offset = _parse_der_integer(der, offset)
    d, offset = _parse_der_integer(der, offset)
    p, offset = _parse_der_integer(der, offset)
    q, offset = _parse_der_integer(der, offset)
    # dp, dq, qinv not needed for dropbear format

    # Build dropbear RSA private key blob
    buf = b""
    buf += _ssh_string(b"ssh-rsa")
    buf += _ssh_string(_mpint_to_bytes(e))
    buf += _ssh_string(_mpint_to_bytes(n))
    buf += _ssh_string(_mpint_to_bytes(d))
    buf += _ssh_string(_mpint_to_bytes(p))
    buf += _ssh_string(_mpint_to_bytes(q))

    fd = os.open(dropbear_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, buf)
    finally:
        os.close(fd)


def ensure_ssh_host_keys(node_ids: list):
    """Generate persistent dropbear RSA host keys for each cluster node.

    Keys are generated once with ssh-keygen and converted to dropbear's
    native format.  They persist in cluster-data/ssh-keys/vm{N}/ across
    cluster rebuilds so each node keeps a stable host identity.
    """
    os.makedirs(SSH_KEYS_DIR, exist_ok=True)
    has_dropbearconvert = shutil.which("dropbearconvert") is not None

    for node_id in node_ids:
        key_dir = os.path.join(SSH_KEYS_DIR, f"vm{node_id}")
        os.makedirs(key_dir, exist_ok=True)

        dropbear_key = os.path.join(key_dir, "dropbear_rsa_host_key")

        if os.path.exists(dropbear_key):
            print(f"  [VM{node_id}] SSH host key: {dropbear_key} (existing)")
            continue

        # Generate OpenSSH RSA key pair (PEM / PKCS#1 format)
        openssh_key = os.path.join(key_dir, "ssh_host_rsa_key")
        subprocess.run(
            [
                "ssh-keygen",
                "-t",
                "rsa",
                "-b",
                "2048",
                "-f",
                openssh_key,
                "-N",
                "",
                "-m",
                "PEM",
            ],
            check=True,
            capture_output=True,
        )

        if has_dropbearconvert:
            subprocess.run(
                ["dropbearconvert", "openssh", "dropbear", openssh_key, dropbear_key],
                check=True,
                capture_output=True,
            )
            print(
                f"  [VM{node_id}] Generated SSH host key (ssh-keygen + dropbearconvert)"
            )
        else:
            _convert_pem_to_dropbear(openssh_key, dropbear_key)
            print(
                f"  [VM{node_id}] Generated SSH host key (ssh-keygen + PEM->dropbear)"
            )


def inject_into_overlay(
    overlay_path: str,
    dropbear_key_path: str | None = None,
    hostname: str | None = None,
    netdevs_content: str | None = None,
    log=print,
) -> bool:
    """Inject per-node files directly into a mountfs overlay's XFS rootfs.

    Uses guestfish to write the SSH host key, hostname, and /etc/netdevs into
    the overlay without any CPIO extract/repack cycle.
    """
    abs_overlay = os.path.abspath(overlay_path)
    if not os.path.exists(abs_overlay):
        log(f"  WARNING: {abs_overlay} not found, skipping overlay injection")
        return False

    gf_cmds = "run\nmount /dev/sda1 /\n"
    temp_files = []

    if dropbear_key_path and os.path.exists(dropbear_key_path):
        abs_key = os.path.abspath(dropbear_key_path)
        gf_cmds += "mkdir-p /etc/dropbear\n"
        gf_cmds += f"upload {abs_key} /etc/dropbear/dropbear_rsa_host_key\n"
        gf_cmds += "chmod 0600 /etc/dropbear/dropbear_rsa_host_key\n"

    if hostname:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".hostname", delete=False
        ) as hf:
            hf.write(hostname)
            temp_files.append(hf.name)
        gf_cmds += f"upload {hf.name} /etc/hostname\n"

    if netdevs_content is not None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".netdevs", delete=False
        ) as nf:
            nf.write(netdevs_content)
            temp_files.append(nf.name)
        gf_cmds += f"upload {nf.name} /etc/netdevs\n"

    gf_cmds += "umount /\n"

    result = subprocess.run(
        ["guestfish", "--rw", "-a", abs_overlay],
        input=gf_cmds,
        capture_output=True,
        text=True,
    )

    for f in temp_files:
        os.unlink(f)

    if result.returncode != 0:
        log(f"  WARNING: guestfish failed: {result.stderr}")
        return False

    return True


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


def setup(config: dict):
    zones = [z for z in config["zones"] if z.get("id") != "GLOBAL"]
    global_cfg = find_global(config["zones"])
    uses_multiqueue_tap = any(
        resolve_config(global_cfg, zone_cfg).get("nic_queues", 1) > 1
        for zone_cfg in zones
    )

    print("=== Setting up cluster topology ===\n")

    # Disable bridge netfilter so iptables doesn't filter bridged traffic
    # (DHCP, ICMP, ARP all need to pass through bridges unfiltered)
    run("modprobe br_netfilter 2>/dev/null || true", quiet=True, privileged=True)
    if uses_multiqueue_tap:
        run("modprobe vhost_net 2>/dev/null || true", quiet=True, privileged=True)
        ensure_vhost_net_available()
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
        bridge_created = False
        if not link_exists(br):
            run(f"ip link add {br} type bridge", privileged=True)
            bridge_created = True

        # Reconcile existing bridges too; stale/down links leave TAP traffic
        # invisible even when QEMU has opened the right interface names.
        run(f"ip link set {br} mtu {mtu}", privileged=True)
        if not bridge_cfg.get("stp", False):
            run(
                f"sh -c 'echo 0 > /sys/class/net/{br}/bridge/stp_state'",
                quiet=True,
                privileged=True,
            )
        run(f"ip link set {br} up", privileged=True)
        if bridge_created:
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
                print(f"  WARNING: uplink {uplink} not found - skipping")

        # Assign IP to bridge if specified
        ip_addr = bridge_cfg.get("ip")
        if ip_addr:
            run(f"ip addr add {ip_addr} dev {br}", check=False, privileged=True)
            print(f"  Bridge IP: {ip_addr}")

        # Create TAP devices
        real_user = os.environ.get("SUDO_USER") or pwd.getpwuid(os.getuid()).pw_name
        real_uid = pwd.getpwnam(real_user).pw_uid
        for node_id in range(num_nodes):
            tap = tap_name(zone_cfg, node_id)
            queue_count = effective.get("nic_queues", 1)
            need_mq = queue_count > 1
            tap_exists = link_exists(tap)
            existing_owner = tap_owner_uid(tap) if tap_exists else None
            missing_mq = need_mq and tap_exists and not tap_has_multiqueue(tap)

            # Force-delete the TAP if the owner is wrong OR if multi_queue is needed
            # but the existing TAP was created without it (QEMU will fail otherwise).
            needs_recreate = (existing_owner is not None and existing_owner != real_uid) or missing_mq
            if needs_recreate:
                reason = (
                    f"uid mismatch ({existing_owner} != {real_uid})"
                    if existing_owner != real_uid
                    else "multi_queue flag missing"
                )
                print(f"  TAP {tap}: recreating ({reason})")
                run(f"ip link set {tap} down", check=False, quiet=True, privileged=True)
                run(f"ip link set {tap} nomaster", check=False, quiet=True, privileged=True)
                if not run(f"ip tuntap del dev {tap} mode tap", privileged=True):
                    raise RuntimeError(f"failed to delete stale TAP {tap}")
                if link_exists(tap):
                    raise RuntimeError(f"stale TAP {tap} still exists after delete")

            tap_created = False
            if not link_exists(tap):
                mq_flag = " multi_queue" if need_mq else ""
                run(
                    f"ip tuntap add dev {tap} mode tap{mq_flag} user {real_user}",
                    privileged=True,
                )
                tap_created = True

            # Re-attach and bring up existing TAPs every time.  A stale TAP can
            # retain multi_queue but lose its bridge master or UP state.
            run(f"ip link set {tap} master {br}", privileged=True)
            run(f"ip link set {tap} mtu {mtu}", privileged=True)
            run(f"ip link set {tap} up", privileged=True)
            if tap_created:
                print(f"  Created TAP: {tap}" + (" (multi_queue)" if need_mq else ""))
            else:
                print(f"  TAP {tap} already exists; reconciled {br}/MTU/up" + (" (multi_queue)" if need_mq else ""))

            if need_mq and not tap_has_multiqueue(tap):
                raise RuntimeError(f"TAP {tap} is not multi_queue but nic_queues={queue_count}")

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

    # Generate persistent SSH host keys for all unique nodes
    all_node_ids = set()
    for zone_cfg in zones:
        num_nodes = zone_cfg.get("nodes", 2)
        for nid in range(num_nodes):
            all_node_ids.add(nid)

    print("--- SSH Host Keys ---")
    ensure_ssh_host_keys(sorted(all_node_ids))
    print()

    print("=== Cluster topology ready ===")


# ---------------------------------------------------------------------------
# Teardown - stateless, derived from config
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
# Launch - setup + start VMs
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


# ---------------------------------------------------------------------------
# Live rootfs/sysroot sync
# ---------------------------------------------------------------------------

LIVE_SYNC_MANIFEST = "/etc/wos-live-sync-manifest.json"
LIVE_SYNC_BATCH_FILES = 24
# These are injected per node during launch and must not be flattened by a
# host-wide live rootfs refresh.
LIVE_SYNC_EXCLUDED_PATHS = {
    "/etc/hostname",
    "/etc/netdevs",
    "/etc/dropbear/dropbear_rsa_host_key",
}
LIVE_SYNC_PROTECTED_ACCESS_PATHS = {
    "/etc/group",
    "/etc/passwd",
    "/root/.ssh/authorized_keys",
    "/usr/bin/dropbearmulti",
    "/usr/libexec/sftp-server",
    "/usr/lib/ld.so",
    "/usr/lib/libc.so",
}


@dataclass(frozen=True)
class SyncItem:
    remote_path: str
    local_path: Path
    source_path: Path
    size: int
    mode: int
    digest: str

    def signature(self) -> dict:
        return {
            "kind": "file",
            "sha256": self.digest,
            "size": self.size,
            "mode": self.mode,
        }


@dataclass
class HostSyncSummary:
    hostname: str
    ok: bool
    uploaded_files: int = 0
    uploaded_bytes: int = 0
    skipped_files: int = 0
    error: str | None = None


@dataclass
class TuiHostState:
    phase: str = "queued"
    total_files: int = 0
    done_files: int = 0
    total_bytes: int = 0
    done_bytes: int = 0
    skipped_files: int = 0
    current: str = ""
    error: str | None = None


class SftpBatchError(RuntimeError):
    def __init__(
        self,
        operation: str,
        hostname: str,
        user: str,
        port: str | None,
        result: subprocess.CompletedProcess,
        lines: list[str],
    ):
        super().__init__(operation)
        self.operation = operation
        self.hostname = hostname
        self.user = user
        self.port = port
        self.result = result
        self.lines = lines

    def __str__(self) -> str:
        target = f"{self.user}@{self.hostname}"
        if self.port:
            target += f" port {self.port}"

        details = [
            f"{self.operation} failed for {target}",
            f"sftp exit code: {self.result.returncode}",
        ]

        stderr = self.result.stderr.strip()
        stdout = self.result.stdout.strip()
        if stderr:
            details.append("stderr:")
            details.append(indent_text(stderr, "  "))
        if stdout:
            details.append("stdout:")
            details.append(indent_text(stdout, "  "))

        if "Connection closed" in stderr or "Connection closed" in stdout:
            details.append(
                "hint: the SSH login succeeded but the SFTP subsystem closed; "
                "check the VM serial log for the sftp-server child and the last "
                "remote path in the batch preview below."
            )
        if "No such file or directory" in stderr and "dest open" in stderr:
            details.append(
                "hint: if the VM serial log also says 'xfs_ialloc: no free inodes', "
                "the remote XFS image ran out of allocatable inodes while creating "
                "new files or directories. Use --filter for a smaller update, or "
                "rebuild/relaunch with a rootfs image that already contains those paths."
            )

        details.append("batch preview:")
        details.append(indent_text("\n".join(preview_sftp_batch(self.lines)), "  "))
        return "\n".join(details)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def sync_target_hostname(node_id: int) -> str:
    suffix = os.environ.get("WOS_SYNC_DNS_SUFFIX", ".wos")
    return f"wos-{node_id}{suffix}"


def format_bytes(count: int) -> str:
    value = float(count)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024.0 or unit == "GiB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} GiB"


def truncate_middle(text: str, width: int) -> str:
    if width <= 0:
        return ""
    if len(text) <= width:
        return text
    if width <= 3:
        return text[:width]
    keep_left = (width - 1) // 2
    keep_right = width - keep_left - 1
    return f"{text[:keep_left]}...{text[-keep_right:]}"


def progress_bar(done: int, total: int, width: int) -> str:
    if width <= 0:
        return ""
    if total <= 0:
        return "[" + (" " * max(0, width - 2)) + "]"
    inner = max(1, width - 2)
    filled = min(inner, int((done / total) * inner))
    return "[" + ("#" * filled) + ("." * (inner - filled)) + "]"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def remote_parent_dirs(path: str) -> list[str]:
    parts = [p for p in path.split("/") if p]
    dirs = []
    cur = ""
    for part in parts[:-1]:
        cur += f"/{part}"
        dirs.append(cur)
    return dirs


def sftp_quote(path: str) -> str:
    return '"' + path.replace("\\", "\\\\").replace('"', '\\"') + '"'


def indent_text(text: str, prefix: str) -> str:
    return "\n".join(f"{prefix}{line}" for line in text.splitlines())


def first_line(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return text


def preview_sftp_batch(lines: list[str], limit: int = 18) -> list[str]:
    if len(lines) <= limit:
        return lines
    head = max(1, limit - 6)
    tail = 4
    omitted = len(lines) - head - tail
    return [
        *lines[:head],
        f"... {omitted} command(s) omitted ...",
        *lines[-tail:],
    ]


def rootfs_source_hints(repo: Path) -> dict[str, Path]:
    hints: dict[str, Path] = {}
    manifest = repo / "configs/rootfs/aliases.tsv"
    if manifest.exists():
        with manifest.open() as f:
            for raw_line in f:
                line = raw_line.rstrip("\n")
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) < 3:
                    continue
                action, source, target = parts[:3]
                if action not in ("copy", "copy-mode"):
                    continue
                source_path = Path(source)
                if not source_path.is_absolute():
                    source_path = repo / source_path
                hints[target] = source_path

    busybox_install = repo / "toolchain/busybox-install"
    for source_dir, target_dir in (
        ("lib", "/usr/lib"),
        ("bin", "/usr/bin"),
        ("sbin", "/usr/sbin"),
        ("usr/bin", "/usr/bin"),
        ("usr/sbin", "/usr/sbin"),
    ):
        source_root = busybox_install / source_dir
        if not source_root.is_dir():
            continue
        for entry in source_root.iterdir():
            hints[f"{target_dir}/{entry.name}"] = entry

    return hints


def stage_rootfs_tree(repo: Path, staging: Path):
    cmd = 'source "$1/scripts/build/rootfs_common.sh"; rootfs_stage_tree "$1" "$2"'
    result = subprocess.run(
        ["bash", "-c", cmd, "wos-rootfs-stage", str(repo), str(staging)],
        cwd=repo,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "rootfs staging failed")


def make_sync_item(remote_path: str, local_path: Path, source_path: Path) -> SyncItem | None:
    try:
        st = local_path.lstat()
    except FileNotFoundError:
        return None
    # Rootfs symlinks are part of image creation; the in-tree SFTP server does
    # not implement SSH_FXP_SYMLINK, so live sync handles regular files only.
    if not stat.S_ISREG(st.st_mode):
        return None
    return SyncItem(
        remote_path=remote_path,
        local_path=local_path,
        source_path=source_path,
        size=st.st_size,
        mode=stat.S_IMODE(st.st_mode),
        digest=file_sha256(local_path),
    )


def add_item(items: dict[str, SyncItem], item: SyncItem | None):
    if item is None:
        return
    if item.remote_path in LIVE_SYNC_EXCLUDED_PATHS:
        return
    items[item.remote_path] = item


def collect_live_sync_items(repo: Path, staging: Path) -> list[SyncItem]:
    items: dict[str, SyncItem] = {}
    source_hints = rootfs_source_hints(repo)

    for root, dirs, files in os.walk(staging):
        dirs.sort()
        files.sort()
        root_path = Path(root)
        for name in files:
            local_path = root_path / name
            rel = local_path.relative_to(staging).as_posix()
            remote_path = f"/{rel}"
            source_path = source_hints.get(remote_path, local_path)
            add_item(items, make_sync_item(remote_path, local_path, source_path))

    sysroot = repo / "toolchain/sysroot"
    for subdir in ("bin", "lib", "include"):
        source_root = sysroot / subdir
        if not source_root.is_dir():
            continue
        for root, dirs, files in os.walk(source_root):
            dirs.sort()
            files.sort()
            root_path = Path(root)
            for name in files:
                local_path = root_path / name
                rel = local_path.relative_to(source_root).as_posix()
                remote_path = f"/usr/{subdir}/{rel}"
                add_item(items, make_sync_item(remote_path, local_path, local_path))

    return [items[path] for path in sorted(items)]


def path_is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def filter_matches(item: SyncItem, raw_filter: str | None, repo: Path, staging: Path) -> bool:
    if not raw_filter:
        return True

    wanted = raw_filter.strip()
    if not wanted:
        return True

    remote_candidate = wanted if wanted.startswith("/") else f"/{wanted.lstrip('./')}"
    if item.remote_path == remote_candidate:
        return True
    if "/" not in wanted and item.remote_path.rsplit("/", 1)[-1] == wanted:
        return True

    filter_path = Path(wanted).expanduser()
    candidates = [filter_path] if filter_path.is_absolute() else [repo / filter_path, staging / filter_path]
    source = item.source_path.resolve(strict=False)
    local = item.local_path.resolve(strict=False)
    for candidate in candidates:
        candidate = candidate.resolve(strict=False)
        if source == candidate or local == candidate:
            return True
        if candidate.exists() and candidate.is_dir():
            if path_is_under(source, candidate) or path_is_under(local, candidate):
                return True

    return False


def filter_sync_items(
    items: list[SyncItem],
    filter_arg: str | None,
    include_live_access: bool,
    repo: Path,
    staging: Path,
) -> tuple[list[SyncItem], list[SyncItem]]:
    matched = [item for item in items if filter_matches(item, filter_arg, repo, staging)]
    if include_live_access:
        return matched, []

    selected = [
        item for item in matched if item.remote_path not in LIVE_SYNC_PROTECTED_ACCESS_PATHS
    ]
    protected = [
        item for item in matched if item.remote_path in LIVE_SYNC_PROTECTED_ACCESS_PATHS
    ]
    return selected, protected


def run_sftp_batch(
    hostname: str,
    lines: list[str],
    user: str,
    port: str | None,
    operation: str,
) -> subprocess.CompletedProcess:
    with tempfile.NamedTemporaryFile(
        mode="w", prefix="wos-live-sftp-", suffix=".batch", delete=False
    ) as batch_file:
        batch_file.write("\n".join(lines))
        batch_file.write("\n")
        batch_path = batch_file.name

    args = [
        "sftp",
        "-q",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "LogLevel=ERROR",
    ]
    if port:
        args.extend(["-P", port])
    args.extend(["-b", batch_path, f"{user}@{hostname}"])

    try:
        result = subprocess.run(args, capture_output=True, text=True)
        if result.returncode != 0:
            raise SftpBatchError(operation, hostname, user, port, result, lines)
        return result
    finally:
        os.unlink(batch_path)


def load_remote_sync_manifest(hostname: str, user: str, port: str | None) -> dict:
    with tempfile.NamedTemporaryFile(prefix="wos-live-manifest-", delete=False) as tmp:
        local_manifest = tmp.name
    os.unlink(local_manifest)

    try:
        result = run_sftp_batch(
            hostname,
            [f"-get {sftp_quote(LIVE_SYNC_MANIFEST)} {sftp_quote(local_manifest)}"],
            user,
            port,
            f"read remote manifest {LIVE_SYNC_MANIFEST}",
        )
        if not os.path.exists(local_manifest) or os.path.getsize(local_manifest) == 0:
            return {"version": 1, "files": {}}
        with open(local_manifest) as f:
            loaded = json.load(f)
        if not isinstance(loaded, dict) or not isinstance(loaded.get("files"), dict):
            return {"version": 1, "files": {}}
        return loaded
    finally:
        if os.path.exists(local_manifest):
            os.unlink(local_manifest)


def write_manifest_file(path: Path, files: dict[str, dict]):
    payload = {
        "version": 1,
        "generated_by": "wos-cluster --sync",
        "generated_at": int(time.time()),
        "files": files,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def upload_manifest(
    hostname: str,
    manifest_file: Path,
    user: str,
    port: str | None,
):
    lines = []
    for directory in remote_parent_dirs(LIVE_SYNC_MANIFEST):
        lines.append(f"-mkdir {sftp_quote(directory)}")
    lines.extend(
        [
            f"put -p {sftp_quote(str(manifest_file))} {sftp_quote(LIVE_SYNC_MANIFEST)}",
        ]
    )
    run_sftp_batch(
        hostname,
        lines,
        user,
        port,
        f"write remote manifest {LIVE_SYNC_MANIFEST}",
    )


def upload_sync_chunk(
    hostname: str,
    chunk: list[SyncItem],
    user: str,
    port: str | None,
):
    lines = []
    mkdirs: set[str] = set()
    for item in chunk:
        mkdirs.update(remote_parent_dirs(item.remote_path))

    for directory in sorted(mkdirs, key=lambda p: (p.count("/"), p)):
        lines.append(f"-mkdir {sftp_quote(directory)}")

    for item in chunk:
        lines.append(f"put -p {sftp_quote(str(item.local_path))} {sftp_quote(item.remote_path)}")

    first_path = chunk[0].remote_path if chunk else "<empty>"
    last_path = chunk[-1].remote_path if chunk else "<empty>"
    run_sftp_batch(
        hostname,
        lines,
        user,
        port,
        (
            f"upload {len(chunk)} file(s), {format_bytes(sum(item.size for item in chunk))} "
            f"from {first_path} through {last_path}"
        ),
    )


def chunked(items: list[SyncItem], size: int):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def sync_host(
    hostname: str,
    selected_items: list[SyncItem],
    filter_used: bool,
    events: queue.Queue,
) -> HostSyncSummary:
    user = os.environ.get("WOS_SSH_USER", "root")
    port = os.environ.get("WOS_SSH_PORT") or None

    try:
        events.put({"type": "host_status", "host": hostname, "phase": "probing"})
        remote_manifest = load_remote_sync_manifest(hostname, user, port)
        remote_files = remote_manifest.get("files", {})

        changed = [
            item
            for item in selected_items
            if remote_files.get(item.remote_path) != item.signature()
        ]
        skipped = len(selected_items) - len(changed)
        total_bytes = sum(item.size for item in changed)
        events.put(
            {
                "type": "host_plan",
                "host": hostname,
                "total_files": len(changed),
                "total_bytes": total_bytes,
                "skipped": skipped,
            }
        )

        uploaded_files = 0
        uploaded_bytes = 0
        for chunk in chunked(changed, LIVE_SYNC_BATCH_FILES):
            upload_sync_chunk(hostname, chunk, user, port)
            for item in chunk:
                uploaded_files += 1
                uploaded_bytes += item.size
                events.put(
                    {
                        "type": "file_done",
                        "host": hostname,
                        "path": item.remote_path,
                        "bytes": item.size,
                    }
                )

        manifest_files = dict(remote_files) if filter_used else {}
        if filter_used:
            for item in selected_items:
                manifest_files[item.remote_path] = item.signature()
        else:
            manifest_files = {item.remote_path: item.signature() for item in selected_items}

        with tempfile.NamedTemporaryFile(
            mode="w", prefix="wos-live-manifest-", suffix=".json", delete=False
        ) as manifest_tmp:
            manifest_path = Path(manifest_tmp.name)
        try:
            write_manifest_file(manifest_path, manifest_files)
            upload_manifest(hostname, manifest_path, user, port)
        finally:
            manifest_path.unlink(missing_ok=True)

        events.put(
            {
                "type": "host_done",
                "host": hostname,
                "uploaded_files": uploaded_files,
                "uploaded_bytes": uploaded_bytes,
                "skipped": skipped,
            }
        )
        return HostSyncSummary(
            hostname=hostname,
            ok=True,
            uploaded_files=uploaded_files,
            uploaded_bytes=uploaded_bytes,
            skipped_files=skipped,
        )
    except Exception as exc:
        error = str(exc)
        events.put({"type": "host_error", "host": hostname, "error": error})
        return HostSyncSummary(hostname=hostname, ok=False, error=error)


class LiveSyncTui:
    def __init__(self, hostnames: list[str], events: queue.Queue, enabled: bool):
        self.hosts = {hostname: TuiHostState() for hostname in hostnames}
        self.events = events
        self.enabled = enabled
        self.recent: list[str] = []

    def __enter__(self):
        if self.enabled:
            sys.stdout.write("\033[?1049h\033[?25l\033[H\033[2J")
            sys.stdout.flush()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.enabled:
            sys.stdout.write("\033[?25h\033[?1049l")
            sys.stdout.flush()

    def drain_events(self):
        while True:
            try:
                event = self.events.get_nowait()
            except queue.Empty:
                return
            self.apply_event(event)

    def apply_event(self, event: dict):
        host = event.get("host")
        state = self.hosts.get(host) if host else None
        event_type = event.get("type")

        if event_type == "host_status" and state:
            state.phase = event["phase"]
        elif event_type == "host_plan" and state:
            state.total_files = event["total_files"]
            state.total_bytes = event["total_bytes"]
            state.skipped_files = event["skipped"]
            state.phase = "current" if state.total_files == 0 else "syncing"
        elif event_type == "file_done" and state:
            state.done_files += 1
            state.done_bytes += event["bytes"]
            state.current = event["path"]
            self.recent.append(f"{host}  put  {event['path']}")
            self.recent = self.recent[-200:]
        elif event_type == "host_done" and state:
            state.phase = "done"
            state.done_files = state.total_files
            state.done_bytes = state.total_bytes
            state.skipped_files = event["skipped"]
        elif event_type == "host_error" and state:
            state.phase = "failed"
            state.error = first_line(event["error"])
            self.recent.append(f"{host}  error  {state.error}")
            self.recent = self.recent[-200:]

    def run_until_done(self, futures: list[concurrent.futures.Future]):
        while True:
            self.drain_events()
            if self.enabled:
                self.render()
            else:
                self.print_plain_updates()
            if all(f.done() for f in futures) and self.events.empty():
                break
            time.sleep(0.05)
        self.drain_events()
        if self.enabled:
            self.render()
            time.sleep(0.15)

    def print_plain_updates(self):
        while self.recent:
            print(self.recent.pop(0))

    def render(self):
        size = shutil.get_terminal_size((100, 30))
        cols = size.columns
        rows = size.lines
        total_bytes = sum(host.total_bytes for host in self.hosts.values())
        done_bytes = sum(host.done_bytes for host in self.hosts.values())
        total_files = sum(host.total_files for host in self.hosts.values())
        done_files = sum(host.done_files for host in self.hosts.values())

        lines = [
            "WOS live rootfs sync",
            f"{progress_bar(done_bytes, total_bytes, min(42, max(12, cols - 44)))}  "
            f"{done_files}/{total_files} files  {format_bytes(done_bytes)}/{format_bytes(total_bytes)}",
            "",
        ]

        host_col = min(20, max(12, max((len(h) for h in self.hosts), default=12)))
        bar_width = min(28, max(10, cols - host_col - 56))
        for hostname, state in self.hosts.items():
            phase = state.phase
            if state.error:
                detail = state.error
            elif state.current:
                detail = state.current
            elif state.skipped_files:
                detail = f"{state.skipped_files} unchanged"
            else:
                detail = ""
            prefix = (
                f"{hostname:<{host_col}} {phase:<8} "
                f"{progress_bar(state.done_bytes, state.total_bytes, bar_width)} "
                f"{state.done_files:>4}/{state.total_files:<4} "
                f"{format_bytes(state.done_bytes):>9}/{format_bytes(state.total_bytes):<9} "
            )
            lines.append(prefix + truncate_middle(detail, max(0, cols - len(prefix))))

        lines.append("")
        lines.append("Waterfall")
        waterfall_rows = max(0, rows - len(lines) - 1)
        for entry in self.recent[-waterfall_rows:]:
            lines.append(truncate_middle(entry, cols))

        sys.stdout.write("\033[H\033[J")
        sys.stdout.write("\n".join(line[:cols] for line in lines[:rows]))
        sys.stdout.flush()


def sync_live_rootfs(config: dict, filter_arg: str | None, include_live_access: bool) -> bool:
    repo = repo_root()
    nodes = collect_unique_nodes(config)
    if not nodes:
        print("No WOS nodes found in cluster config.")
        return True

    with tempfile.TemporaryDirectory(prefix="wos-live-rootfs-") as staging_tmp:
        staging = Path(staging_tmp)
        stage_rootfs_tree(repo, staging)
        all_items = collect_live_sync_items(repo, staging)
        selected_items, protected_items = filter_sync_items(
            all_items,
            filter_arg,
            include_live_access,
            repo,
            staging,
        )

        if not selected_items:
            if protected_items:
                protected_paths = ", ".join(item.remote_path for item in protected_items)
                print(
                    "Sync payload matched only live-access protected path(s): "
                    f"{protected_paths}. Rerun with --include-live-access to force this.",
                    file=sys.stderr,
                )
                return False
            print(f"No sync payload matched filter: {filter_arg!r}")
            return False

        hostnames = [sync_target_hostname(node_id) for node_id in sorted(nodes.keys())]
        events: queue.Queue = queue.Queue()
        max_workers = min(len(hostnames), 32)
        use_tui = sys.stdout.isatty()

        if not use_tui:
            print(
                f"Syncing {len(selected_items)} candidate files to "
                f"{len(hostnames)} WOS node(s)"
                + (f" with filter {filter_arg!r}" if filter_arg else "")
                + "..."
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(sync_host, hostname, selected_items, bool(filter_arg), events)
                for hostname in hostnames
            ]
            with LiveSyncTui(hostnames, events, use_tui) as tui:
                tui.run_until_done(futures)
            summaries = [future.result() for future in futures]

    failures = [summary for summary in summaries if not summary.ok]
    for summary in summaries:
        if summary.ok:
            print(
                f"{summary.hostname}: uploaded {summary.uploaded_files} file(s), "
                f"{format_bytes(summary.uploaded_bytes)}; "
                f"{summary.skipped_files} unchanged"
            )
        else:
            print(
                f"{summary.hostname}: ERROR:\n{indent_text(summary.error or 'unknown error', '  ')}",
                file=sys.stderr,
            )

    if failures:
        print(f"Live sync failed on {len(failures)} host(s).", file=sys.stderr)
        return False
    if protected_items:
        shown = ", ".join(item.remote_path for item in protected_items[:5])
        more = "" if len(protected_items) <= 5 else f", +{len(protected_items) - 5} more"
        print(
            "Skipped live-access protected path(s): "
            f"{shown}{more}. Use --include-live-access only when you can recover the VM console/image."
        )
    print("Live sync complete.")
    return True


# TCG log-level presets (only useful with software emulation, no-ops under KVM)
TCG_LOG_LEVELS = {
    "": "cpu_reset,int,tid,in_asm,nochain,guest_errors,page",
    "int": "cpu_reset,int,tid,pcall,in_asm,nochain,guest_errors,page",
    "full": "cpu_reset,int,tid,exec,cpu,fpu,pcall,in_asm,nochain,guest_errors,page,mmu",
    "none": "",
}


def build_qemu_args(
    node_id: int,
    node_info: dict,
    config: dict,
    tcg_level: str | None = None,
    debug_nodes: set[int] | None = None,
    log=print,
) -> list:
    """Build QEMU command line for a single node.

    tcg_level: None = KVM (default), "" = basic TCG, "int" = TCG+interrupts, "full" = TCG+cpu state
    """
    eff = node_info["effective"]
    vm_cfg = eff.get("vm", {})
    global_cfg = find_global(config["zones"])

    memory = vm_cfg.get("memory", "4G")
    cpus = vm_cfg.get("cpus", 2)
    disk0 = vm_cfg.get("disk0", "disk.qcow2")
    disk1 = vm_cfg.get("disk1", "mountfs.qcow2")
    is_debug = eff.get("debug", False) or (
        debug_nodes is not None and node_id in debug_nodes
    )

    # Create overlay disks
    overlay_dir = "cluster-overlays"
    os.makedirs(overlay_dir, exist_ok=True)

    overlay0 = os.path.join(overlay_dir, f"disk-vm{node_id}.qcow2")
    overlay1 = os.path.join(overlay_dir, f"mountfs-vm{node_id}.qcow2")

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
        log(f"  ERROR creating overlay {overlay0}: {result.stderr.strip()}")

    result = subprocess.run(
        f"qemu-img create -f qcow2 -b {abs_disk1} -F qcow2 {overlay1}",
        shell=True,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        log(f"  ERROR creating overlay {overlay1}: {result.stderr.strip()}")

    serial_log = f"serial-vm{node_id}.log"
    qemu_log = f"qemu-vm{node_id}.log"

    # Remove old logs
    for pattern in (
        f"qemu-vm{node_id}.log",
        f"qemu-vm{node_id}.*log",
        f"qemu-vm{node_id}-cpu*.log",
    ):
        for f in Path(".").glob(pattern):
            f.unlink()
    if os.path.exists(serial_log):
        os.remove(serial_log)

    # Acceleration: KVM (default) or TCG (software emulation for full tracing)
    if tcg_level is not None:
        accel_args = ["-accel", "tcg,thread=multi", "-cpu", "max"]
        log_flags = TCG_LOG_LEVELS.get(tcg_level, TCG_LOG_LEVELS[""])
        qemu_log = f"qemu-vm{node_id}-cpu%d.log"
        log(
            f"  [VM{node_id}] Using TCG (software emulation) — level: {tcg_level or 'default'}"
        )
    else:
        accel_args = ["-cpu", "host,migratable=no,+invtsc", "--enable-kvm"]
        # Under KVM only trace events and guest_errors produce output;
        # in_asm/nochain/page/exec/cpu are TCG-only and silently ignored.
        log_flags = "cpu_reset,guest_errors"

    args = [
        "qemu-system-x86_64",
        "-M",
        "q35",
        *accel_args,
        "-m",
        memory,
        "-smp",
        str(cpus),
        "-drive",
        f"file={overlay0},if=none,id=drive0,format=qcow2,cache=unsafe",
        "-device",
        "ahci,id=ahci",
        "-device",
        "ide-hd,drive=drive0,bus=ahci.0",
        "-drive",
        f"file={overlay1},if=none,id=drive1,format=qcow2,cache=unsafe",
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
        *(["-d", log_flags] if log_flags else []),
        "-D",
        qemu_log,
        "-no-reboot",
    ]

    # Add NICs - one per zone this node participates in
    nic_idx = 0
    for zone_cfg in node_info["zones"]:
        zone_id = zone_cfg["id"]
        node_override = get_node_config(zone_cfg, node_id)
        zone_eff = resolve_config(global_cfg, zone_cfg, node_override)
        nic_model = zone_eff.get("nic_model", "virtio-net-pci")
        tap = tap_name(zone_cfg, node_id)
        mac = mac_addr(zone_id, node_id, 0)
        num_queues = zone_eff.get("nic_queues", 1)
        use_vhost = bool(zone_eff.get("vhost", False))
        netdev_options = [f"tap,id=net{nic_idx}", f"ifname={tap}", "script=no", "downscript=no"]
        if use_vhost:
            netdev_options.append("vhost=on")
        else:
            # Firmware DHCP uses raw Ethernet packets and cannot participate in
            # the host-side virtio-net header contract.
            netdev_options.append("vnet_hdr=off")
        if num_queues > 1:
            netdev_options.append(f"queues={num_queues}")
        device_options = [f"{nic_model},netdev=net{nic_idx}", f"mac={mac}"]
        if nic_model.startswith("virtio-net"):
            device_options.append("mrg_rxbuf=on")
            if num_queues > 1:
                device_options.extend(["mq=on", f"vectors={2 * num_queues + 2}"])

        args.extend(
            [
                "-netdev",
                ",".join(netdev_options),
                "-device",
                ",".join(device_options),
            ]
        )
        nic_idx += 1

    # Add ivshmem devices - one per ivshmem link involving this node.
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

    # Per-node hostname via QEMU fw_cfg (read by kernel before VFS is up)
    node_hostname = f"wos-{node_id}"
    args.extend(["-fw_cfg", f"name=opt/wos/hostname,string={node_hostname}"])

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

        log(
            f"  [VM{node_id}] DEBUG: gdb=127.0.0.1:{gdb_port} debugcon={debugcon_port} monitor={monitor_port}"
        )

    return args


@dataclass
class LaunchError(RuntimeError):
    node_id: int
    lines: list[str]
    cause: BaseException

    def __str__(self) -> str:
        return f"VM{self.node_id} launch failed: {self.cause}"


@dataclass
class LaunchResult:
    node_id: int
    process: subprocess.Popen
    lines: list[str]


def launch_one_vm(
    node_id: int,
    node_info: dict,
    config: dict,
    tcg_level: str | None,
    debug_nodes: set[int] | None,
    processes: list,
    processes_lock: threading.Lock,
    stopping: threading.Event,
) -> LaunchResult:
    lines = []

    try:
        args = build_qemu_args(
            node_id,
            node_info,
            config,
            tcg_level=tcg_level,
            debug_nodes=debug_nodes,
            log=lines.append,
        )

        zone_names = [zone_name(z) for z in node_info["zones"]]
        is_debug = node_info["effective"].get("debug", False) or (
            debug_nodes is not None and node_id in debug_nodes
        )

        lines.append(f"  [VM{node_id}] zones={zone_names} debug={is_debug}")
        lines.append(f"    cmd: {' '.join(args)}")

        # Inject per-node files into the mountfs overlay.
        mountfs_overlay = os.path.join(
            "cluster-overlays", f"mountfs-vm{node_id}.qcow2"
        )
        dropbear_key = os.path.join(
            SSH_KEYS_DIR, f"vm{node_id}", "dropbear_rsa_host_key"
        )
        node_hostname = f"wos-{node_id}"
        key_path = dropbear_key if os.path.exists(dropbear_key) else None

        # Build /etc/netdevs from zone order, matching NIC enumeration.
        global_cfg = find_global(config["zones"])
        netdevs_lines = [
            "# /etc/netdevs - generated by cluster_setup.py",
            "# Format: <ifname> <driver>",
            "# Drivers: wki, dhcp, linklocal, unmanaged",
            "",
        ]
        for nic_idx, zone_cfg in enumerate(node_info["zones"]):
            node_override = get_node_config(zone_cfg, node_id)
            zone_eff = resolve_config(global_cfg, zone_cfg, node_override)
            driver = zone_eff.get("netdev_driver", "unmanaged")
            netdevs_lines.append(f"eth{nic_idx} {driver}")
        netdevs_content = "\n".join(netdevs_lines) + "\n"

        if os.path.exists(mountfs_overlay):
            if inject_into_overlay(
                mountfs_overlay,
                dropbear_key_path=key_path,
                hostname=node_hostname,
                netdevs_content=netdevs_content,
                log=lines.append,
            ):
                lines.append(
                    f"    Injected per-node overlay (hostname={node_hostname})"
                )
            else:
                lines.append("    WARNING: Failed to inject SSH host key")

        with processes_lock:
            if stopping.is_set():
                raise RuntimeError("launch cancelled")
            proc = subprocess.Popen(args)
            processes.append(proc)
        lines.append(f"    PID: {proc.pid}")

        return LaunchResult(node_id=node_id, process=proc, lines=lines)
    except Exception as exc:
        raise LaunchError(node_id=node_id, lines=lines, cause=exc) from exc


def launch(
    config: dict,
    tcg_level: str | None = None,
    debug_nodes: set[int] | None = None,
):
    """Setup topology, then launch VMs."""
    setup(config)
    print()

    nodes = collect_unique_nodes(config)
    pids = []
    pids_lock = threading.Lock()
    stopping = threading.Event()

    print("=== Launching VMs ===\n")
    if not nodes:
        print("No WOS nodes found in cluster config.")
        return

    def stop_all_vms(grace_seconds: float = 0.15):
        """Stop all VMs with a short grace period, then force-kill survivors."""
        with pids_lock:
            processes = list(pids)
        running = [p for p in processes if p.poll() is None]
        for p in running:
            try:
                p.terminate()
            except OSError:
                pass

        # Use a global deadline instead of per-process waits to keep Ctrl+C snappy.
        deadline = time.monotonic() + grace_seconds
        while time.monotonic() < deadline:
            if all(p.poll() is not None for p in running):
                return
            time.sleep(0.01)

        for p in running:
            if p.poll() is None:
                try:
                    p.kill()
                except OSError:
                    pass

    def shutdown(signum, frame):
        stopping.set()
        print("\n=== Shutting down cluster ===")
        stop_all_vms()
        print("=== Cluster stopped ===")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    max_workers = len(nodes)
    futures = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    launch_one_vm,
                    node_id,
                    nodes[node_id],
                    config,
                    tcg_level,
                    debug_nodes,
                    pids,
                    pids_lock,
                    stopping,
                ): node_id
                for node_id in sorted(nodes.keys())
            }

            try:
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    for line in result.lines:
                        print(line)
            except LaunchError as exc:
                stopping.set()
                for line in exc.lines:
                    print(line)
                print(f"ERROR: {exc}", file=sys.stderr)
                for future in futures:
                    future.cancel()
                stop_all_vms()
                sys.exit(1)
            except KeyboardInterrupt:
                shutdown(None, None)
    except KeyboardInterrupt:
        shutdown(None, None)

    print(f"\n=== {len(pids)} VMs launched ===")
    print("Press Ctrl+C to stop all VMs.\n")

    # Wait for all VMs
    with pids_lock:
        launched = list(pids)
    for p in launched:
        try:
            p.wait()
        except KeyboardInterrupt:
            shutdown(None, None)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    os.chdir(repo_root())

    parser = argparse.ArgumentParser(description="WKI cluster topology manager")
    parser.add_argument(
        "--setup", action="store_true", help="Create topology (default)"
    )
    parser.add_argument("--launch", action="store_true", help="Setup + launch VMs")
    parser.add_argument("--teardown", action="store_true", help="Destroy topology")
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Live-sync changed rootfs/sysroot files to running WOS nodes over SFTP",
    )
    parser.add_argument(
        "--filter",
        metavar="PATH",
        help="With --sync, update only one matching source or remote path",
    )
    parser.add_argument(
        "--include-live-access",
        action="store_true",
        help="With --sync, also update SSH/SFTP access files that are skipped by default",
    )
    parser.add_argument(
        "--tcg",
        nargs="?",
        const="",
        default=None,
        metavar="LEVEL",
        help="Use TCG instead of KVM. Optional level: int, full (default: basic)",
    )
    parser.add_argument(
        "--debug-node",
        action="append",
        type=int,
        default=[],
        metavar="NODE_ID",
        help="With --launch, start the selected node paused with a GDB stub. May be repeated.",
    )
    parser.add_argument(
        "--config", default="configs/cluster.json", help="Config file path"
    )
    args = parser.parse_args()

    modes = [args.setup, args.launch, args.teardown, args.sync]
    if sum(1 for enabled in modes if enabled) > 1:
        parser.error("choose only one of --setup, --launch, --teardown, or --sync")
    if args.filter and not args.sync:
        parser.error("--filter is only valid with --sync")
    if args.include_live_access and not args.sync:
        parser.error("--include-live-access is only valid with --sync")
    if args.debug_node and not args.launch:
        parser.error("--debug-node is only valid with --launch")

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = repo_root() / config_path
    config = load_config(str(config_path))

    if args.sync:
        if not sync_live_rootfs(config, args.filter, args.include_live_access):
            sys.exit(1)
    elif args.teardown:
        # Cache sudo credentials once upfront for privileged network operations.
        ensure_sudo()
        teardown(config)
    elif args.launch:
        ensure_sudo()
        launch(
            config,
            tcg_level=args.tcg,
            debug_nodes=set(args.debug_node) if args.debug_node else None,
        )
    else:
        ensure_sudo()
        setup(config)


if __name__ == "__main__":
    main()
