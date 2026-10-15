#!/usr/bin/env python3
"""
Shared single-node VM setup helpers for WOS launch scripts.

The cluster topology code owns bridges, TAP creation, and multi-node layout.
This module owns the common per-node VM spec: overlay disks, QEMU arguments,
fw_cfg hostname/early NIC policy, NIC device arguments, and per-node mountfs
injection.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import subprocess
import tempfile
from copy import deepcopy
from pathlib import Path


DEFAULT_VM_CONFIG = {
    "memory": "4G",
    "cpus": 2,
    "disk0": "disk.qcow2",
    "disk1": "mountfs.qcow2",
    "overlay_dir": "cluster-overlays",
    "bios": "/usr/share/OVMF/x64/OVMF.4m.fd",
    "disk_cache": "unsafe",
}

TCG_LOG_LEVELS = {
    "": "cpu_reset,int,tid,in_asm,nochain,guest_errors,page",
    "int": "cpu_reset,int,tid,pcall,in_asm,nochain,guest_errors,page",
    "full": "cpu_reset,int,tid,exec,cpu,fpu,pcall,in_asm,nochain,guest_errors,page,mmu",
    "none": "",
}


class OverlayCreationError(RuntimeError):
    def __init__(self, overlay: Path, base_disk: Path, stderr: str):
        self.overlay = overlay
        self.base_disk = base_disk
        self.stderr = stderr
        detail = stderr or "qemu-img exited without an error message"
        super().__init__(f"failed to create overlay {overlay} from {base_disk}: {detail}")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_json_config(path: str | Path) -> dict:
    with open(path) as f:
        return json.load(f)


def merge_section(base: dict, override: dict) -> dict:
    result = deepcopy(base)
    result.update(override)
    return result


def normalize_qemu_netdev(raw: dict, *, queues: int, vhost: bool, where: str) -> dict:
    if not isinstance(raw, dict):
        raise ValueError(f"{where} must be an object")
    required_fields = {"type", "address", "port"}
    allowed_fields = required_fields | {"localaddr"}
    if not required_fields.issubset(raw) or not set(raw).issubset(allowed_fields):
        raise ValueError(
            f"{where} must contain type, address, and port, with optional localaddr"
        )
    if raw["type"] != "socket-mcast":
        raise ValueError(f"{where}.type must be socket-mcast")
    address = raw["address"]
    if not isinstance(address, str):
        raise ValueError(f"{where}.address must be a multicast IPv4 address")
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError as exc:
        raise ValueError(f"{where}.address must be a multicast IPv4 address") from exc
    if parsed.version != 4 or not parsed.is_multicast:
        raise ValueError(f"{where}.address must be a multicast IPv4 address")
    port = raw["port"]
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError(f"{where}.port must be an integer in [1, 65535]")
    localaddr = raw.get("localaddr")
    if localaddr is not None:
        if not isinstance(localaddr, str):
            raise ValueError(f"{where}.localaddr must be an IPv4 address")
        try:
            parsed_localaddr = ipaddress.ip_address(localaddr)
        except ValueError as exc:
            raise ValueError(f"{where}.localaddr must be an IPv4 address") from exc
        if parsed_localaddr.version != 4:
            raise ValueError(f"{where}.localaddr must be an IPv4 address")
    if isinstance(queues, bool) or int(queues) != 1:
        raise ValueError(f"{where} requires queues=1")
    if vhost:
        raise ValueError(f"{where} requires vhost=false")
    normalized = {"type": "socket-mcast", "address": str(parsed), "port": port}
    if localaddr is not None:
        normalized["localaddr"] = str(parsed_localaddr)
    return normalized


def _numa_memory_mib(value: object) -> int:
    if not isinstance(value, str) or (match := re.fullmatch(r"([1-9][0-9]*)([MG])", value)) is None:
        raise ValueError("NUMA vm.memory must be a canonical positive M or G size")
    memory = int(match[1]) * (1024 if match[2] == "G" else 1)
    if memory > ((1 << 64) - 1) // (1 << 20):
        raise ValueError("NUMA vm.memory exceeds the QEMU byte-size range")
    return memory


def normalize_numa_spec(raw: object, *, cpus: object, memory: object) -> dict:
    """Validate a bounded guest topology without touching host resources."""
    if not isinstance(raw, dict) or not {"nodes"} <= raw.keys() or not raw.keys() <= {"nodes", "distances"}:
        raise ValueError("vm.numa must contain nodes and optional distances only")
    if type(cpus) is not int or not 1 <= cpus <= 64:
        raise ValueError("NUMA vm.cpus must be an integer in [1, 64]")
    total_memory = _numa_memory_mib(memory)
    nodes = raw["nodes"]
    if not isinstance(nodes, list) or not 1 <= len(nodes) <= 64:
        raise ValueError("vm.numa.nodes must contain between 1 and 64 nodes")
    normalized = []
    assigned: set[int] = set()
    for index, node in enumerate(nodes):
        if not isinstance(node, dict) or set(node) != {"id", "cpus", "memoryMiB"}:
            raise ValueError(f"vm.numa.nodes[{index}] requires exactly id, cpus and memoryMiB")
        if type(node["id"]) is not int or node["id"] != index:
            raise ValueError("NUMA node IDs must be contiguous and match their array positions")
        members = node["cpus"]
        if not isinstance(members, list) or any(type(cpu) is not int or not 0 <= cpu < cpus for cpu in members):
            raise ValueError(f"vm.numa.nodes[{index}].cpus must contain in-range integer CPU indexes")
        if members != sorted(set(members)) or assigned.intersection(members):
            raise ValueError("NUMA CPU lists must be sorted, unique and nonoverlapping")
        size = node["memoryMiB"]
        if type(size) is not int or not 0 <= size <= total_memory:
            raise ValueError("NUMA memoryMiB must be a nonnegative integer within vm.memory")
        if size == 0 and not members:
            raise ValueError("NUMA nodes must contain CPUs or positive memoryMiB")
        assigned.update(members)
        normalized.append({"id": index, "cpus": list(members), "memoryMiB": size})
    if assigned != set(range(cpus)):
        raise ValueError("NUMA nodes must assign every configured CPU exactly once")
    if sum(node["memoryMiB"] for node in normalized) != total_memory:
        raise ValueError("NUMA node memoryMiB must sum exactly to vm.memory")
    result = {"nodes": normalized}
    if "distances" in raw:
        distances = raw["distances"]
        count = len(nodes)
        if not isinstance(distances, list) or len(distances) != count:
            raise ValueError("NUMA distances must be a complete node-by-node matrix")
        for src, row in enumerate(distances):
            if not isinstance(row, list) or len(row) != count:
                raise ValueError("NUMA distances must be a complete node-by-node matrix")
            for dst, value in enumerate(row):
                if type(value) is not int or (value != 10 if src == dst else not 10 < value <= 255):
                    raise ValueError("NUMA distances require diagonal 10 and off-diagonal integers in [11, 255]")
        result["distances"] = [list(row) for row in distances]
    return result


def build_numa_qemu_args(vm: dict) -> list[str]:
    """Pure optional guest NUMA arguments; no host NUMA binding is implied."""
    if "numa" not in vm:
        return []
    numa = normalize_numa_spec(vm["numa"], cpus=vm.get("cpus"), memory=vm.get("memory"))
    args = []
    for node in numa["nodes"]:
        if node["memoryMiB"]:
            args.extend(["-object", f"memory-backend-ram,id=wos_numa_mem{node['id']},size={node['memoryMiB']}M"])
    for node in numa["nodes"]:
        options = [f"node,nodeid={node['id']}"]
        members = node["cpus"]
        first = 0
        while first < len(members):
            last = first
            while last + 1 < len(members) and members[last + 1] == members[last] + 1:
                last += 1
            interval = str(members[first]) if first == last else f"{members[first]}-{members[last]}"
            options.append(f"cpus={interval}")
            first = last + 1
        if node["memoryMiB"]:
            options.append(f"memdev=wos_numa_mem{node['id']}")
        args.extend(["-numa", ",".join(options)])
    for src, row in enumerate(numa.get("distances", [])):
        for dst, distance in enumerate(row):
            args.extend(["-numa", f"dist,src={src},dst={dst},val={distance}"])
    return args


def normalize_node_spec(raw: dict) -> dict:
    """Return a node spec with defaults applied.

    Accepted input is either the node object itself or a wrapper containing
    ``{"node": {...}}``.
    """
    source = raw.get("node", raw)
    spec = deepcopy(source)
    node_id = int(spec.get("id", 0))

    spec["id"] = node_id
    spec["hostname"] = spec.get("hostname", f"wos-{node_id}")
    spec["debug"] = bool(spec.get("debug", False))
    spec["vm"] = merge_section(DEFAULT_VM_CONFIG, spec.get("vm", {}))
    disk_cache = spec["vm"]["disk_cache"]
    if not isinstance(disk_cache, str) or disk_cache not in ("unsafe", "writeback", "none", "writethrough", "directsync"):
        raise ValueError("vm.disk_cache must be unsafe, writeback, none, writethrough, or directsync")
    iommu = spec["vm"].get("iommu", False)
    if not isinstance(iommu, bool):
        raise ValueError("vm.iommu must be a boolean")
    if iommu and spec["vm"].get("machine", "q35").split(",", 1)[0] != "q35":
        raise ValueError("vm.iommu requires the q35 machine")
    if "numa" in spec["vm"]:
        spec["vm"]["numa"] = normalize_numa_spec(
            spec["vm"]["numa"], cpus=spec["vm"]["cpus"], memory=spec["vm"]["memory"]
        )
    spec["nics"] = [deepcopy(nic) for nic in spec.get("nics", [])]
    for nic_index, nic in enumerate(spec["nics"]):
        backend = nic.get("qemu_netdev")
        if backend is not None:
            nic["qemu_netdev"] = normalize_qemu_netdev(
                backend,
                queues=nic.get("queues", nic.get("nic_queues", 1)),
                vhost=bool(nic.get("vhost", False)),
                where=f"nics[{nic_index}].qemu_netdev",
            )
    spec["ivshmem"] = [deepcopy(dev) for dev in spec.get("ivshmem", [])]
    for dev in spec["ivshmem"]:
        mode = dev.get("mode", "plain")
        if mode not in ("plain", "doorbell"):
            raise ValueError("ivshmem mode must be plain or doorbell")
        if mode == "doorbell":
            server = dev.get("server")
            if not isinstance(server, str) or not server or "," in server:
                raise ValueError("ivshmem doorbell requires a UNIX server path without commas")
    return spec


def load_node_config(path: str | Path) -> dict:
    return normalize_node_spec(load_json_config(path))


def node_id(spec: dict) -> int:
    return int(spec.get("id", 0))


def node_hostname(spec: dict) -> str:
    return str(spec.get("hostname", f"wos-{node_id(spec)}"))


def node_debug_enabled(spec: dict, force_debug: bool = False) -> bool:
    return bool(spec.get("debug", False) or force_debug)


def netdev_assignments(spec: dict) -> list[tuple[str, str]]:
    spec = normalize_node_spec(spec)
    assignments = []
    for nic_idx, nic in enumerate(spec.get("nics", [])):
        ifname = nic.get("ifname", f"eth{nic_idx}")
        driver = nic.get("driver", nic.get("netdev_driver", "unmanaged"))
        assignments.append((ifname, driver))
    return assignments


def overlay_paths(spec: dict) -> tuple[Path, Path]:
    spec = normalize_node_spec(spec)
    vm_cfg = spec["vm"]
    nid = node_id(spec)
    overlay_dir = Path(vm_cfg.get("overlay_dir", "cluster-overlays"))
    overlay0 = Path(vm_cfg.get("disk0_overlay", overlay_dir / f"disk-vm{nid}.qcow2"))
    overlay1 = Path(vm_cfg.get("disk1_overlay", overlay_dir / f"mountfs-vm{nid}.qcow2"))
    return overlay0, overlay1


def serial_log_path(spec: dict) -> Path:
    spec = normalize_node_spec(spec)
    vm_cfg = spec["vm"]
    return Path(vm_cfg.get("serial_log", f"serial-vm{node_id(spec)}.log"))


def qemu_log_path(spec: dict, tcg_level: str | None = None) -> Path:
    spec = normalize_node_spec(spec)
    vm_cfg = spec["vm"]
    if tcg_level is not None:
        return Path(vm_cfg.get("tcg_log", f"qemu-vm{node_id(spec)}-cpu%d.log"))
    return Path(vm_cfg.get("qemu_log", f"qemu-vm{node_id(spec)}.log"))


def qmp_socket_path(spec: dict) -> Path | None:
    """Return the configured QMP socket, including debug and legacy USB defaults."""
    spec = normalize_node_spec(spec)
    vm_cfg = spec["vm"]
    configured = vm_cfg.get("qmp_socket")
    if configured is not None:
        if not isinstance(configured, str) or not configured:
            raise ValueError("vm.qmp_socket must be a non-empty path string")
        return Path(configured)

    usb_hotplug = vm_cfg.get("usb_hotplug", {})
    if isinstance(usb_hotplug, dict) and usb_hotplug.get("enabled", False):
        legacy = usb_hotplug.get("qmp_socket", f"qmp-vm{node_id(spec)}.sock")
        if not isinstance(legacy, str) or not legacy:
            raise ValueError("vm.usb_hotplug.qmp_socket must be a non-empty path string")
        return Path(legacy)

    # Live debug sessions use QMP for bounded pause/resume ownership.  Keep
    # QMP opt-in for ordinary nodes, but guarantee it for every debug node.
    if node_debug_enabled(spec):
        overlay_dir = Path(vm_cfg.get("overlay_dir", "cluster-overlays"))
        return overlay_dir / f"qmp-vm{node_id(spec)}.sock"
    return None


def run_shell(cmd: str, check=True, quiet=False, privileged=False) -> bool:
    if privileged:
        cmd = f"sudo {cmd}"
    if not quiet:
        print(f"  $ {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"    FAILED: {result.stderr.strip()}")
        return False
    return True


def remove_existing_file(path: Path):
    if not path.exists():
        return
    try:
        path.unlink()
    except PermissionError:
        run_shell(f"rm -f {path}", quiet=True, privileged=True)


def prepare_node_overlays(spec: dict, log=print) -> tuple[Path, Path]:
    spec = normalize_node_spec(spec)
    vm_cfg = spec["vm"]
    overlay0, overlay1 = overlay_paths(spec)
    overlay0.parent.mkdir(parents=True, exist_ok=True)
    overlay1.parent.mkdir(parents=True, exist_ok=True)

    for path in (overlay0, overlay1):
        remove_existing_file(path)

    created_overlays: list[Path] = []
    try:
        for base_disk, overlay in (
            (Path(vm_cfg.get("disk0", "disk.qcow2")), overlay0),
            (Path(vm_cfg.get("disk1", "mountfs.qcow2")), overlay1),
        ):
            result = subprocess.run(
                [
                    "qemu-img",
                    "create",
                    "-f",
                    "qcow2",
                    "-b",
                    str(base_disk.resolve()),
                    "-F",
                    "qcow2",
                    str(overlay),
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                stderr = result.stderr.strip()
                log(f"  ERROR creating overlay {overlay}: {stderr}")
                remove_existing_file(overlay)
                raise OverlayCreationError(overlay, base_disk, stderr)
            created_overlays.append(overlay)
    except Exception:
        for overlay in created_overlays:
            remove_existing_file(overlay)
        raise

    return overlay0, overlay1


def cleanup_node_logs(spec: dict):
    spec = normalize_node_spec(spec)
    nid = node_id(spec)
    qemu_log = qemu_log_path(spec)
    log_dir = qemu_log.parent
    log_stem = qemu_log.name.removesuffix(".log")

    patterns = (
        qemu_log.name,
        f"{log_stem}.*log",
        f"qemu-vm{nid}-cpu*.log",
    )
    for pattern in patterns:
        for path in log_dir.glob(pattern):
            path.unlink()

    serial_log = serial_log_path(spec)
    if serial_log.exists():
        serial_log.unlink()

    qmp_socket = qmp_socket_path(spec)
    if qmp_socket is not None and (qmp_socket.exists() or qmp_socket.is_socket()):
        qmp_socket.unlink()


def build_qemu_args(
    spec: dict,
    tcg_level: str | None = None,
    force_debug: bool = False,
    log=print,
) -> list[str]:
    """Build the QEMU command line for one WOS node.

    ``tcg_level``: None = KVM, "" = basic TCG, "int" = TCG+interrupts,
    "full" = TCG+CPU state.
    """
    spec = normalize_node_spec(spec)
    if force_debug:
        spec["debug"] = True
    vm_cfg = spec["vm"]
    nid = node_id(spec)

    memory = vm_cfg.get("memory", "4G")
    cpus = vm_cfg.get("cpus", 2)
    numa_args = build_numa_qemu_args(vm_cfg)
    overlay0, overlay1 = prepare_node_overlays(spec, log=log)
    cleanup_node_logs(spec)

    serial_log = serial_log_path(spec)
    serial_log.parent.mkdir(parents=True, exist_ok=True)

    if tcg_level is not None:
        accel_args = ["-accel", "tcg,thread=multi", "-cpu", str(vm_cfg.get("cpu", "max"))]
        log_flags = TCG_LOG_LEVELS.get(tcg_level, TCG_LOG_LEVELS[""])
        qemu_log = qemu_log_path(spec, tcg_level=tcg_level)
        log(f"  [VM{nid}] Using TCG (software emulation) - level: {tcg_level or 'default'}")
    else:
        accel_args = ["-cpu", str(vm_cfg.get("cpu", "host,migratable=no,+invtsc")), "--enable-kvm"]
        log_flags = "cpu_reset,guest_errors"
        qemu_log = qemu_log_path(spec)

    qemu_log.parent.mkdir(parents=True, exist_ok=True)

    args = [
        "qemu-system-x86_64",
        "-M",
        vm_cfg.get("machine", "q35"),
        *accel_args,
        "-m",
        str(memory),
        "-smp",
        str(cpus),
        *numa_args,
        "-drive",
        f"file={overlay0},if=none,id=drive0,format=qcow2,cache={vm_cfg['disk_cache']}",
        "-device",
        "ahci,id=ahci",
        "-device",
        "ide-hd,drive=drive0,bus=ahci.0",
        "-drive",
        f"file={overlay1},if=none,id=drive1,format=qcow2,cache={vm_cfg['disk_cache']}",
        "-device",
        "ide-hd,drive=drive1,bus=ahci.1",
        "-chardev",
        f"file,id=char0,path={serial_log}",
        "-serial",
        "chardev:char0",
        "-display",
        "none",
        "-bios",
        vm_cfg.get("bios", DEFAULT_VM_CONFIG["bios"]),
        *(["-d", log_flags] if log_flags else []),
        "-D",
        str(qemu_log),
    ]

    if vm_cfg.get("iommu", False):
        args.extend(["-device", "intel-iommu,intremap=off,caching-mode=on,aw-bits=48"])

    usb_hotplug = vm_cfg.get("usb_hotplug", {})
    if isinstance(usb_hotplug, dict) and usb_hotplug.get("enabled", False):
        xhci_id = str(usb_hotplug.get("controller_id", "xhci"))
        usb2_ports = int(usb_hotplug.get("usb2_ports", 4))
        usb3_ports = int(usb_hotplug.get("usb3_ports", 4))
        args.extend(
            [
                "-device",
                f"qemu-xhci,id={xhci_id},p2={usb2_ports},p3={usb3_ports}",
            ]
        )

    qmp_socket = qmp_socket_path(spec)
    if qmp_socket is not None:
        qmp_socket.parent.mkdir(parents=True, exist_ok=True)
        args.extend(["-qmp", f"unix:{qmp_socket},server=on,wait=off"])

    for nic_idx, nic in enumerate(spec.get("nics", [])):
        nic_model = nic.get("model", nic.get("nic_model", "virtio-net-pci"))
        mac = nic["mac"]
        num_queues = int(nic.get("queues", nic.get("nic_queues", 1)))
        use_vhost = bool(nic.get("vhost", False))
        backend = nic.get("qemu_netdev")
        if backend is not None:
            netdev_options = [
                f"socket,id=net{nic_idx}",
                f"mcast={backend['address']}:{backend['port']}",
            ]
            if "localaddr" in backend:
                netdev_options.append(f"localaddr={backend['localaddr']}")
        else:
            tap = nic["tap"]
            netdev_options = [
                f"tap,id=net{nic_idx}",
                f"ifname={tap}",
                "script=no",
                "downscript=no",
            ]
            if use_vhost:
                netdev_options.append("vhost=on")
            else:
                netdev_options.append("vnet_hdr=off")
            if num_queues > 1:
                netdev_options.append(f"queues={num_queues}")

        device_options = [f"{nic_model},netdev=net{nic_idx}", f"mac={mac}"]
        if nic_model.startswith("virtio-net"):
            device_options.append("mrg_rxbuf=on")
            if num_queues > 1:
                device_options.extend(["mq=on", f"vectors={2 * num_queues + 2}"])

        args.extend(["-netdev", ",".join(netdev_options), "-device", ",".join(device_options)])

    for idx, dev in enumerate(spec.get("ivshmem", [])):
        if dev.get("mode", "plain") == "doorbell":
            args.extend(
                [
                    "-chardev",
                    f"socket,id=ivserver{idx},path={dev['server']}",
                    "-device",
                    f"ivshmem-doorbell,chardev=ivserver{idx},vectors=1",
                ]
            )
            continue
        args.extend(
            [
                "-object",
                f"memory-backend-file,size={dev['size']},share=on,mem-path={dev['path']},id=hmem{idx}",
                "-device",
                f"ivshmem-plain,memdev=hmem{idx}",
            ]
        )

    fw_cfg = {"opt/wos/hostname": node_hostname(spec)}
    if spec.get("ivshmem"):
        # The generic dmaN driver probes before WKI. Reserve cluster-provisioned
        # shared-memory devices for the authenticated WKI message transport.
        fw_cfg["opt/wos/wki-ivshmem"] = "1"
        role = int(spec["ivshmem"][0].get("role", 0))
        if role not in (0, 1):
            raise ValueError("ivshmem role must be 0 or 1")
        fw_cfg["opt/wos/wki-ivshmem-role"] = str(role)
        peer = int(spec["ivshmem"][0].get("peer", 0))
        if peer <= 0 or peer >= 0xFFFF:
            raise ValueError("ivshmem WKI peer must be in [1, 65534]")
        fw_cfg["opt/wos/wki-ivshmem-peer"] = str(peer)
    assignments = netdev_assignments(spec)
    if assignments:
        fw_cfg["opt/wos/netdevs"] = ";".join(
            f"{ifname} {driver}" for ifname, driver in assignments
        )
    for entry in spec.get("fw_cfg", []):
        fw_cfg[entry["name"]] = entry["string"]
    for name, value in fw_cfg.items():
        args.extend(["-fw_cfg", f"name={name},string={value}"])
    for entry in spec.get("fw_cfg_files", []):
        name = entry["name"]
        path = Path(entry["path"])
        if not path.is_file():
            raise ValueError(f"fw_cfg file for {name} does not exist: {path}")
        if path.stat().st_mode & 0o077:
            raise ValueError(f"fw_cfg secret file for {name} must be mode 0600: {path}")
        args.extend(["-fw_cfg", f"name={name},file={path.resolve()}"])

    if node_debug_enabled(spec, force_debug=force_debug):
        gdb_port = int(vm_cfg.get("gdb_port", 1234 + nid))
        debugcon_port = int(vm_cfg.get("debugcon_port", 23456 + nid))
        monitor_port = int(vm_cfg.get("monitor_port", 3002 + nid))

        args.extend(["-gdb", f"tcp:127.0.0.1:{gdb_port}", "-S"])
        args.extend(
            [
                "-chardev",
                f"socket,id=debugger,port={debugcon_port},host=127.0.0.1,server=on,wait=off,telnet=on",
                "-device",
                "isa-debugcon,iobase=0x402,chardev=debugger",
            ]
        )
        args.extend(["-monitor", f"tcp:127.0.0.1:{monitor_port},server,nowait"])
        log(
            f"  [VM{nid}] DEBUG: gdb=127.0.0.1:{gdb_port} "
            f"debugcon=127.0.0.1:{debugcon_port} monitor=127.0.0.1:{monitor_port}"
        )

    return args


def netdevs_content(spec: dict, generated_by: str = "node_setup.py") -> str:
    lines = [
        f"# /etc/netdevs - generated by {generated_by}",
        "# Format: <ifname> <driver>",
        "# Drivers: wki, dhcp, linklocal, unmanaged",
        "",
    ]
    for ifname, driver in netdev_assignments(spec):
        lines.append(f"{ifname} {driver}")
    return "\n".join(lines) + "\n"


def inject_into_overlay(
    overlay_path: str | Path,
    dropbear_key_path: str | Path | None = None,
    hostname: str | None = None,
    netdevs: str | None = None,
    log=print,
) -> bool:
    """Inject per-node files directly into a mountfs overlay's XFS rootfs."""
    abs_overlay = Path(overlay_path).resolve()
    if not abs_overlay.exists():
        log(f"  WARNING: {abs_overlay} not found, skipping overlay injection")
        return False

    gf_cmds = "run\nmount /dev/sda1 /\n"
    temp_files: list[str] = []

    if dropbear_key_path and Path(dropbear_key_path).exists():
        abs_key = Path(dropbear_key_path).resolve()
        gf_cmds += "mkdir-p /etc/dropbear\n"
        gf_cmds += f"upload {abs_key} /etc/dropbear/dropbear_rsa_host_key\n"
        gf_cmds += "chmod 0600 /etc/dropbear/dropbear_rsa_host_key\n"

    if hostname:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".hostname", delete=False) as hf:
            hf.write(hostname)
            temp_files.append(hf.name)
        gf_cmds += f"upload {hf.name} /etc/hostname\n"

    if netdevs is not None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".netdevs", delete=False) as nf:
            nf.write(netdevs)
            temp_files.append(nf.name)
        gf_cmds += f"upload {nf.name} /etc/netdevs\n"

    gf_cmds += "umount /\n"

    result = subprocess.run(
        ["guestfish", "--rw", "-a", str(abs_overlay)],
        input=gf_cmds,
        capture_output=True,
        text=True,
    )

    for temp_file in temp_files:
        os.unlink(temp_file)

    if result.returncode != 0:
        log(f"  WARNING: guestfish failed: {result.stderr}")
        return False

    return True


def inject_node_overlay(
    spec: dict,
    dropbear_key_path: str | Path | None = None,
    generated_by: str = "node_setup.py",
    log=print,
) -> bool:
    _, mountfs_overlay = overlay_paths(spec)
    return inject_into_overlay(
        mountfs_overlay,
        dropbear_key_path=dropbear_key_path,
        hostname=node_hostname(normalize_node_spec(spec)),
        netdevs=netdevs_content(spec, generated_by=generated_by),
        log=log,
    )


def cluster_config_from_node_spec(spec: dict) -> dict:
    """Build a one-node cluster topology config from a normalized node spec."""
    spec = normalize_node_spec(spec)
    vm_cfg = deepcopy(spec["vm"])
    zones = [
        {
            "id": "GLOBAL",
            "debug": bool(spec.get("debug", False)),
            "mtu": int(spec.get("mtu", 9000)),
            "nic_model": "virtio-net-pci",
            "ivshmem": {
                "enabled": False,
                "size": "16M",
                "root_path": "/dev/shm",
                "topology": "full-mesh",
            },
            "bridge": {
                "stp": False,
                "ip": None,
            },
            "vm": vm_cfg,
        }
    ]

    for nic in spec.get("nics", []):
        zone_id = nic["zone_id"]
        zone = {
            "id": zone_id,
            "name": nic.get("name", f"z{zone_id}"),
            "nodes": 1,
            "nic_model": nic.get("model", nic.get("nic_model", "virtio-net-pci")),
            "nic_queues": int(nic.get("queues", nic.get("nic_queues", 1))),
            "vhost": bool(nic.get("vhost", False)),
            "netdev_driver": nic.get("driver", nic.get("netdev_driver", "unmanaged")),
            "bridge": deepcopy(nic.get("bridge", {})),
            "ivshmem": deepcopy(nic.get("ivshmem", {"enabled": False})),
            "nodes_config": [
                {
                    "id": node_id(spec),
                    "hostname": node_hostname(spec),
                    "debug": bool(spec.get("debug", False)),
                    "vm": deepcopy(vm_cfg),
                    # Explicit single-node devices are separate from the
                    # zone's shared-link topology dictionary.
                    "ivshmem_devices": deepcopy(spec.get("ivshmem", [])),
                    "fw_cfg": deepcopy(spec.get("fw_cfg", [])),
                }
            ],
        }
        # Preserve the validated rootless backend through KTEST's node-to-zone
        # conversion; dropping it silently turns a socket NIC into a TAP NIC.
        if "qemu_netdev" in nic:
            zone["qemu_netdev"] = deepcopy(nic["qemu_netdev"])
        zones.append(zone)

    return {"zones": zones}
