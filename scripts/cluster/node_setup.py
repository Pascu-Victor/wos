#!/usr/bin/env python3
"""
Shared single-node VM setup helpers for WOS launch scripts.

The cluster topology code owns bridges, TAP creation, and multi-node layout.
This module owns the common per-node VM spec: overlay disks, QEMU arguments,
fw_cfg hostname, NIC device arguments, and per-node mountfs injection.
"""

from __future__ import annotations

import json
import os
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
    spec["nics"] = [deepcopy(nic) for nic in spec.get("nics", [])]
    spec["ivshmem"] = [deepcopy(dev) for dev in spec.get("ivshmem", [])]
    return spec


def load_node_config(path: str | Path) -> dict:
    return normalize_node_spec(load_json_config(path))


def node_id(spec: dict) -> int:
    return int(spec.get("id", 0))


def node_hostname(spec: dict) -> str:
    return str(spec.get("hostname", f"wos-{node_id(spec)}"))


def node_debug_enabled(spec: dict, force_debug: bool = False) -> bool:
    return bool(spec.get("debug", False) or force_debug)


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
    vm_cfg = spec["vm"]
    nid = node_id(spec)

    memory = vm_cfg.get("memory", "4G")
    cpus = vm_cfg.get("cpus", 2)
    overlay0, overlay1 = prepare_node_overlays(spec, log=log)
    cleanup_node_logs(spec)

    serial_log = serial_log_path(spec)
    serial_log.parent.mkdir(parents=True, exist_ok=True)

    if tcg_level is not None:
        accel_args = ["-accel", "tcg,thread=multi", "-cpu", "max"]
        log_flags = TCG_LOG_LEVELS.get(tcg_level, TCG_LOG_LEVELS[""])
        qemu_log = qemu_log_path(spec, tcg_level=tcg_level)
        log(f"  [VM{nid}] Using TCG (software emulation) - level: {tcg_level or 'default'}")
    else:
        accel_args = ["-cpu", "host,migratable=no,+invtsc", "--enable-kvm"]
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
        vm_cfg.get("bios", DEFAULT_VM_CONFIG["bios"]),
        *(["-d", log_flags] if log_flags else []),
        "-D",
        str(qemu_log),
    ]

    for nic_idx, nic in enumerate(spec.get("nics", [])):
        nic_model = nic.get("model", nic.get("nic_model", "virtio-net-pci"))
        tap = nic["tap"]
        mac = nic["mac"]
        num_queues = int(nic.get("queues", nic.get("nic_queues", 1)))
        use_vhost = bool(nic.get("vhost", False))
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
        args.extend(
            [
                "-object",
                f"memory-backend-file,size={dev['size']},share=on,mem-path={dev['path']},id=hmem{idx}",
                "-device",
                f"ivshmem-plain,memdev=hmem{idx}",
            ]
        )

    fw_cfg = {"opt/wos/hostname": node_hostname(spec)}
    for entry in spec.get("fw_cfg", []):
        fw_cfg[entry["name"]] = entry["string"]
    for name, value in fw_cfg.items():
        args.extend(["-fw_cfg", f"name={name},string={value}"])

    if node_debug_enabled(spec, force_debug=force_debug):
        gdb_port = int(vm_cfg.get("gdb_port", 1234 + nid))
        debugcon_port = int(vm_cfg.get("debugcon_port", 23456 + nid))
        monitor_port = int(vm_cfg.get("monitor_port", 3002 + nid))

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
        log(f"  [VM{nid}] DEBUG: gdb=127.0.0.1:{gdb_port} debugcon={debugcon_port} monitor={monitor_port}")

    return args


def netdevs_content(spec: dict, generated_by: str = "node_setup.py") -> str:
    spec = normalize_node_spec(spec)
    lines = [
        f"# /etc/netdevs - generated by {generated_by}",
        "# Format: <ifname> <driver>",
        "# Drivers: wki, dhcp, linklocal, unmanaged",
        "",
    ]
    for nic_idx, nic in enumerate(spec.get("nics", [])):
        ifname = nic.get("ifname", f"eth{nic_idx}")
        driver = nic.get("driver", nic.get("netdev_driver", "unmanaged"))
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
                }
            ],
        }
        zones.append(zone)

    return {"zones": zones}
