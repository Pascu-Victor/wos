#!/usr/bin/env python3
"""Prepare private Goal 25 transport inputs; never build, launch, or stop a VM.

Inputs must already be packaged. Profile tokens describe the caller's intended
image; this helper does not install tokens, inspect Limine, or qualify security.
Dry-run uses only the shared cluster/node schema helpers, not QEMU construction
(which creates overlays). Doorbell launch requires a later ordered server/client
driver: the public concurrent launcher cannot certify role/connection ordering.
"""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "scripts" / "cluster"))
import cluster_setup  # noqa: E402
import node_setup  # noqa: E402

MODES = ("ethernet-roce", "ivshmem-plain", "ivshmem-doorbell")
BACKING_BYTES = 16 * 1024 * 1024
SOURCE_PATHS = (
    "scripts/test/wki_secure_rdma_fixture.py",
    "scripts/cluster/node_setup.py",
    "scripts/cluster/cluster_setup.py",
    "scripts/bench/run_block_qualification.py",
    "modules/init/src/service_manifest.cpp",
    "modules/kern/src/dev/virtio/virtio_net.cpp",
    "modules/kern/src/net/wki/wki.hpp",
    "modules/kern/src/net/wki/rdma_protocol.hpp",
    "modules/kern/src/net/wki/secure_rdma.hpp",
    "modules/kern/src/net/wki/transport_eth.cpp",
    "modules/kern/src/net/wki/transport_eth.hpp",
    "modules/kern/src/net/wki/transport_roce.cpp",
    "modules/kern/src/net/wki/transport_roce.hpp",
    "modules/kern/src/net/wki/transport_ivshmem.cpp",
    "modules/kern/src/net/wki/transport_ivshmem.hpp",
)


def private_root(path: Path, repo: Path = ROOT) -> Path:
    """Require a fresh direct child of this repository's ktest-data, without links."""
    repo = repo.resolve()
    path = Path(os.path.abspath(path))
    if path.parent != repo / "ktest-data" or not re.fullmatch(r"goal25-[a-z0-9-]+", path.name):
        raise ValueError("state root must be a goal25-* direct child of ktest-data")
    for ancestor in (path, *path.parents):
        if ancestor.is_symlink():
            raise ValueError(f"symlink in private state path: {ancestor}")
        if ancestor == repo:
            break
    if os.path.lexists(path):
        raise ValueError(f"refusing existing state root: {path}")
    # Paths appear in QEMU's comma-separated device strings and UNIX sockets.
    if any(char in str(path) for char in ",\n\r"):
        raise ValueError("state path must not contain commas or line breaks")
    return path


def unix_path(path: Path) -> str:
    if len(os.fsencode(path)) > 107:
        raise ValueError(f"UNIX socket path exceeds 107 bytes: {path}")
    return str(path)


def construct_fixture(*, state_root: Path, run_id: str, profile: str, mode: str,
                      group: str, port: int, boot: Path, rootfs: Path,
                      profile_tokens: list[str], node_ids: tuple[int, int] = (248, 249),
                      identity_policy: str = "deny", repo: Path = ROOT) -> dict:
    """Return JSON-ready specs through actual pure validators, with no writes."""
    state = private_root(state_root, repo)
    if not isinstance(run_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,15}", run_id):
        raise ValueError("run-id must be 1..16 lowercase letters/digits/hyphens")
    if profile not in ("baseline", "candidate") or mode not in MODES:
        raise ValueError("invalid profile or transport mode")
    if (len(node_ids) != 2 or any(type(n) is not int or not 0 <= n <= 255 for n in node_ids)
            or node_ids[0] >= node_ids[1]):
        raise ValueError("node IDs must be two increasing distinct integers in [0, 255]")
    if not profile_tokens or any(not isinstance(t, str) or not t or len(t) > 128
                                 or any(c.isspace() for c in t) for t in profile_tokens):
        raise ValueError("at least one nonempty, bounded, whitespace-free profile token is required")
    if identity_policy not in ("deny", "same-domain", "same-domain-root"):
        raise ValueError("invalid public WKI identity policy")
    address = ipaddress.ip_address(group)
    if address.version != 4 or address not in ipaddress.ip_network("239.192.0.0/14"):
        raise ValueError("group must be a private site-local multicast IPv4 address (239.192/14)")
    base_backend = {"type": "socket-mcast", "address": str(address), "port": port,
                    "localaddr": "127.0.0.1"}
    node_setup.normalize_qemu_netdev(base_backend, queues=1, vhost=False, where="fixture backend")
    zone = {"id": 25, "name": f"g25-{run_id}", "nodes": 2, "node_ids": list(node_ids),
            "nic_queues": 1, "vhost": False, "netdev_driver": "wki",
            "qemu_netdev": base_backend, "ivshmem": {"enabled": False}, "nodes_config": []}
    if mode == "ivshmem-plain":
        zone["ivshmem"] = {"enabled": True, "size": "16M", "topology": "pairs",
                           "ivshmem_links": [list(node_ids)], "root_path": str(state / "shm")}
    for index, nid in enumerate(node_ids):
        peer = node_ids[1 - index]
        directory = state / ("a" if index == 0 else "b")
        node = {"id": nid, "hostname": f"g25-{run_id}-{profile}-{index}",
                "vm": {"disk0": str(directory / "boot.qcow2"),
                       "disk1": str(directory / "rootfs.qcow2"),
                       "overlay_dir": str(directory / "overlays"),
                       "serial_log": str(directory / "serial.log"),
                       "qemu_log": str(directory / "qemu.log"),
                       "qmp_socket": unix_path(directory / "qmp.sock")},
                "wki_auth": {"peer_policy": {str(peer): 1023},
                             "peer_identity": {str(peer): identity_policy}}}
        if mode != "ethernet-roce":
            # Prevent ordinary Ethernet from hiding a broken ivshmem transport.
            isolated = dict(base_backend, address=str(ipaddress.IPv4Address(int(address) + index)),
                            port=port + index)
            node_setup.normalize_qemu_netdev(isolated, queues=1, vhost=False, where="isolated backend")
            if ipaddress.ip_address(isolated["address"]) not in ipaddress.ip_network("239.192.0.0/14"):
                raise ValueError("isolated multicast group leaves private site-local range")
            node["qemu_netdev"] = isolated
        if mode == "ivshmem-doorbell":
            node["ivshmem_devices"] = [{"mode": "doorbell", "size": "16M",
                                         "server": unix_path(state / "shm" / "server.sock"),
                                         "role": index, "peer": peer + 1}]
        zone["nodes_config"].append(node)
    config = {"zones": [{"id": "GLOBAL", "debug": False, "mtu": 9000,
                         "nic_model": "virtio-net-pci", "ivshmem": {"enabled": False},
                         "bridge": {"stp": False, "ip": None},
                         "vm": {"memory": "4G", "cpus": 2, "machine": "q35",
                                "iommu": True, "disk_cache": "writeback"}}, zone]}
    cluster_setup.validate_cluster_config(config)
    nodes = cluster_setup.collect_unique_nodes(config)
    policies = cluster_setup.configured_wki_peer_policies(nodes)
    specs = [cluster_setup.cluster_node_spec(nid, nodes[nid], config) for nid in node_ids]
    for index, spec in enumerate(specs):
        expected_iv = 0 if mode == "ethernet-roce" else 1
        if len(spec["ivshmem"]) != expected_iv:
            raise ValueError("unexpected ivshmem topology")
        if expected_iv:
            device = spec["ivshmem"][0]
            if device["role"] != index or device["peer"] != node_ids[1 - index] + 1:
                raise ValueError("ivshmem role/direct-peer mapping mismatch")
    backing = (cluster_setup.ivshmem_file(str(state / "shm"), zone, *node_ids)
               if mode == "ivshmem-plain" else None)
    launch = ["env", "TMPDIR=/tmp", str(repo / "bin" / "wos-cluster"), "--config",
              str(state / "cluster.json"), "--launch", "--no-setup", "--live-debug-descriptor",
              str(state / "live.json")]
    return {"schema": 1, "run_id": run_id, "profile": profile, "mode": mode,
            "node_ids": list(node_ids), "identity_policy": identity_policy,
            "state_root": str(state), "config": config, "node_specs": specs,
            "input_images": {"boot": str(boot), "rootfs": str(rootfs)},
            "profile_tokens": list(profile_tokens), "profile_tokens_installed_verified": False,
            "image_format_validated": False,
            "kernel_in_images_verified": False, "init_manifest_validated": False,
            "secure_runtime_qualified": False, "launch_ready": False,
            "credential_paths": [str(Path(s["vm"]["overlay_dir"]) / f"wki-auth-vm{s['id']}.bin")
                                 for s in specs],
            "credential_provisioning": "later public launcher; pairwise PSK files mode 0600; no secrets in JSON",
            "receiver_policies": [{"receiver_cluster_id": a, "peer_cluster_id": b, "policy": p}
                                  for (a, b), p in sorted(policies.items())],
            "plain_backing": backing, "plain_backing_bytes": BACKING_BYTES if backing else 0,
            "planned_launcher_argv": launch if mode != "ivshmem-doorbell" else None,
            "launcher_environment": {"TMPDIR": "/tmp"},
            "pending_runtime_checks": ["packaged kernel/tokens and actual init manifest",
                                       "owned global guarded launch and cleanup",
                                       "guest virtio MTU 9000 (driver default, not configured by global mtu)",
                                       "current transport/session and secure engine/consumer qualification",
                                       "multicast endpoint availability and source-image writer quiescence",
                                       "SSH disabled by packaged services; existing global host keys must not be injected"],
            "doorbell_requirements": (["external ivshmem-server not started by this helper",
                                       "ordered connection IDs must equal roles 0/1; concurrent launch cannot prove this",
                                       "actual peer IRQ delivery plus ordinary ring fallback evidence",
                                       "reap only the owned server after both guests retire"]
                                      if mode == "ivshmem-doorbell" else [])}


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def regular_input(path: Path, repo: Path) -> Path:
    path = Path(os.path.abspath(path))
    if not path.is_relative_to(repo.resolve()):
        raise ValueError(f"input must remain inside repository: {path}")
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError(f"input symlink refused: {path}")
    if not stat.S_ISREG(path.stat().st_mode):
        raise ValueError(f"input must be a regular file: {path}")
    return path


def file_pin(path: Path) -> dict:
    return {"path": str(path), "sha256": digest(path), "bytes": path.stat().st_size}


def write_private(path: Path, value: dict) -> None:
    data = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def create_plain_backing(path: Path) -> None:
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, BACKING_BYTES)
        os.fsync(fd)
    finally:
        os.close(fd)


def copy_pinned(source: Path, target: Path, expected: dict) -> dict:
    """Copy exclusively and verify the source and copied bytes against the pin."""
    if file_pin(source) != expected:
        raise ValueError(f"source changed before copy: {source}")
    dst_fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(dst_fd, "wb") as dst:
        src_fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(src_fd, "rb") as src:
            os.fchmod(dst.fileno(), 0o600)
            for block in iter(lambda: src.read(1024 * 1024), b""):
                dst.write(block)
        dst.flush()
        os.fsync(dst.fileno())
    result = file_pin(target)
    if (result["sha256"], result["bytes"]) != (expected["sha256"], expected["bytes"]) or file_pin(source) != expected:
        raise ValueError(f"source/copy changed during preparation: {source}")
    if source.stat().st_ino == target.stat().st_ino and source.stat().st_dev == target.stat().st_dev:
        raise ValueError("private image unexpectedly aliases source inode")
    return result


def overlay_pinned(source: Path, target: Path, expected: dict) -> dict:
    """Create an exclusive private qcow2 overlay over an immutable pinned base."""
    if file_pin(source) != expected:
        raise ValueError(f"source changed before overlay: {source}")
    if source.stat().st_mode & 0o222:
        raise ValueError(f"overlay base must be read-only: {source}")
    source_info = json.loads(subprocess.check_output(
        ["qemu-img", "info", "--output=json", str(source)], text=True))
    if source_info["format"] != "qcow2" or source_info.get("backing-filename"):
        raise ValueError(f"overlay base must be standalone qcow2: {source}")
    # Reserve the name before qemu-img can truncate it. Private state directories
    # prevent unrelated users replacing this exclusively created regular file.
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(fd)
    subprocess.run(["qemu-img", "create", "-f", "qcow2", "-F", "qcow2",
                    "-b", str(source), str(target)], check=True, stdout=subprocess.DEVNULL)
    os.chmod(target, 0o600)
    info = json.loads(subprocess.check_output(
        ["qemu-img", "info", "--output=json", str(target)], text=True))
    if (info["format"] != "qcow2" or info.get("full-backing-filename") != str(source)
            or info["virtual-size"] != source_info["virtual-size"] or file_pin(source) != expected):
        raise ValueError(f"source/overlay changed during preparation: {source}")
    return {**file_pin(target), "backing": expected, "image_mode": "qcow2-overlay"}


def prepare_fixture(spec: dict, *, kernel: Path, evidence: list[Path], repo: Path = ROOT,
                    image_mode: str = "copy") -> dict:
    if image_mode not in ("copy", "qcow2-overlay"):
        raise ValueError("invalid image preparation mode")
    # Reconstruct through the pure validators before trusting any serialized path.
    canonical = construct_fixture(state_root=Path(spec["state_root"]), run_id=spec["run_id"],
                                  profile=spec["profile"], mode=spec["mode"], group=spec["config"]["zones"][1]["qemu_netdev"]["address"],
                                  port=spec["config"]["zones"][1]["qemu_netdev"]["port"],
                                  boot=Path(spec["input_images"]["boot"]), rootfs=Path(spec["input_images"]["rootfs"]),
                                  profile_tokens=spec["profile_tokens"], node_ids=tuple(spec["node_ids"]),
                                  identity_policy=spec["identity_policy"], repo=repo)
    if spec != canonical:
        raise ValueError("fixture spec differs from the canonical private constructor")
    state = private_root(Path(spec["state_root"]), repo)
    images = {name: regular_input(Path(path), repo) for name, path in spec["input_images"].items()}
    inputs = {name: file_pin(path) for name, path in images.items()}
    kernel_pin = file_pin(regular_input(kernel, repo))
    evidence_pins = [file_pin(regular_input(path, repo)) for path in evidence]
    if not evidence_pins:
        raise ValueError("at least one caller evidence file is required")
    source_pins = [file_pin(ROOT / path) for path in SOURCE_PATHS]
    state.parent.mkdir(mode=0o700, exist_ok=True)
    state.mkdir(mode=0o700)  # Exclusive; failure never removes or overwrites previous state.
    os.chmod(state, 0o700)
    result = {"schema": 1, "completed": False, "errors": [], "launches": 0,
              "spec": spec, "input_images": inputs, "kernel_artifact": kernel_pin,
              "caller_evidence": evidence_pins, "source_pins": source_pins,
              "image_mode": image_mode, "copied_images": [], "prepared_images": []}
    try:
        for lane in ("a", "b"):
            directory = state / lane
            directory.mkdir(mode=0o700)
            os.chmod(directory, 0o700)
            (directory / "overlays").mkdir(mode=0o700)
            os.chmod(directory / "overlays", 0o700)
            for name, source in images.items():
                target = directory / ("boot.qcow2" if name == "boot" else "rootfs.qcow2")
                prepare_image = overlay_pinned if image_mode == "qcow2-overlay" else copy_pinned
                image = prepare_image(source, target, inputs[name])
                result["prepared_images"].append(image)
                if image_mode == "copy":
                    result["copied_images"].append(image)
        (state / "shm").mkdir(mode=0o700)
        os.chmod(state / "shm", 0o700)
        if spec["plain_backing"]:
            create_plain_backing(Path(spec["plain_backing"]))
        # These shared preflight functions are read-only for socket-mcast specs.
        cluster_setup.validate_no_setup_topology(spec["config"])
        write_private(state / "cluster.json", spec["config"])
        write_private(state / "spec.json", spec)
        result["config_pin"] = file_pin(state / "cluster.json")
        result["spec_pin"] = file_pin(state / "spec.json")
        for pin in (*inputs.values(), kernel_pin, *evidence_pins, *source_pins):
            if file_pin(Path(pin["path"])) != pin:
                raise ValueError(f"pinned input changed during preparation: {pin['path']}")
        result["completed"] = True
    except Exception as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
        write_private(state / "preparation-result.json", result)
        raise
    write_private(state / "preparation-result.json", result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--profile", choices=("baseline", "candidate"), required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--boot", type=Path, required=True, help="already packaged input boot image")
    parser.add_argument("--rootfs", type=Path, required=True, help="already packaged input rootfs image")
    parser.add_argument("--profile-token", action="append", required=True, help="caller declaration only")
    parser.add_argument("--identity-policy", choices=("deny", "same-domain", "same-domain-root"), default="deny")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--dry-run", action="store_true", help="pure spec/schema validation; no writes")
    action.add_argument("--prepare", action="store_true", help="exclusive private copies; no image injection or launch")
    parser.add_argument("--kernel", type=Path, help="artifact to pin; not installed or checked against images")
    parser.add_argument("--evidence", type=Path, action="append", default=[])
    parser.add_argument("--image-mode", choices=("copy", "qcow2-overlay"), default="copy",
                        help="use qcow2-overlay with immutable read-only bases for VM campaigns")
    args = parser.parse_args()
    try:
        spec = construct_fixture(state_root=args.state_root, run_id=args.run_id, profile=args.profile,
                                 mode=args.mode, group=args.group, port=args.port, boot=args.boot,
                                 rootfs=args.rootfs, profile_tokens=args.profile_token,
                                 identity_policy=args.identity_policy)
        if args.prepare:
            if args.kernel is None or not args.evidence:
                parser.error("--prepare requires --kernel and at least one --evidence")
            output = prepare_fixture(spec, kernel=args.kernel, evidence=args.evidence,
                                     image_mode=args.image_mode)
        else:
            output = spec
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (OSError, ValueError) as error:
        print(f"fixture preparation rejected: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
