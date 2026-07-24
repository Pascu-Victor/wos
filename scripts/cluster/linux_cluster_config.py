#!/usr/bin/env python3
"""Resolve Linux-clone resources and patch deterministic libvirt XML."""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

import cluster_setup


ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class NodeResources:
    node_id: int
    vcpus: int
    memory_kib: int


@dataclass(frozen=True)
class DomainIdentity:
    domain_name: str
    base_disk: str
    disk_path: str
    nvram_path: str
    tap_name: str
    mac_address: str
    network_name: str = ""


def positive_integer(value: object, context: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{context} must be a positive integer")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and value.strip().isdigit():
        result = int(value.strip())
    else:
        raise ValueError(f"{context} must be a positive integer")
    if result <= 0:
        raise ValueError(f"{context} must be a positive integer")
    return result


def strict_positive_integer(value: object, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{context} must be a positive integer")
    return value


def nonnegative_integer(value: object, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{context} must be a nonnegative integer")
    return value


def qemu_memory_kib(value: object) -> int:
    """Normalize QEMU ``-m`` syntax; an unsuffixed value means MiB."""
    if isinstance(value, bool):
        raise ValueError("VM memory must be a positive QEMU K/M/G/T size")
    text = str(value).strip().upper()
    multipliers = {
        "K": 1,
        "M": 1024,
        "G": 1024 * 1024,
        "T": 1024 * 1024 * 1024,
    }
    suffix = text[-1:] if text[-1:] in multipliers else ""
    amount = text[:-1] if suffix else text
    if not amount.isdigit() or int(amount) <= 0:
        raise ValueError(f"VM memory must be a positive QEMU K/M/G/T size, got {value!r}")
    multiplier = multipliers[suffix] if suffix else multipliers["M"]
    return int(amount) * multiplier


def resolve_node_resources(raw_path: str | Path, num_vms: int) -> list[NodeResources]:
    num_vms = strict_positive_integer(num_vms, "VM count")
    config_path = Path(raw_path)
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    config = cluster_setup.load_config(str(config_path.resolve()))
    if not isinstance(config, dict) or not isinstance(config.get("zones"), list):
        raise ValueError("cluster config root must contain a zones array")
    if any(not isinstance(zone, dict) for zone in config["zones"]):
        raise ValueError("cluster config zones must be objects")

    global_zones = [zone for zone in config["zones"] if zone.get("id") == "GLOBAL"]
    if len(global_zones) != 1:
        raise ValueError("cluster config must define exactly one GLOBAL zone")

    expected_ids = list(range(num_vms))
    zones = [zone for zone in config["zones"] if zone.get("id") != "GLOBAL"]
    if not zones:
        raise ValueError("cluster config must define at least one non-GLOBAL zone")
    for zone in zones:
        zone_name = zone.get("name", zone.get("id", "unnamed"))
        zone_nodes = strict_positive_integer(
            zone.get("nodes", 2), f"{zone_name} zone node count"
        )
        if zone_nodes != num_vms:
            raise ValueError(
                f"{zone_name} zone configures {zone_nodes} nodes but the launcher requested {num_vms}"
            )
        overrides = zone.get("nodes_config", [])
        if not isinstance(overrides, list) or any(
            not isinstance(override, dict) for override in overrides
        ):
            raise ValueError(f"{zone_name} zone nodes_config must be an array of objects")
        override_ids: list[int] = []
        for override in overrides:
            node_id = nonnegative_integer(override.get("id"), "node override id")
            if node_id not in expected_ids:
                raise ValueError(
                    f"{zone_name} zone node override id {node_id} is outside {expected_ids}"
                )
            override_ids.append(node_id)
        if len(set(override_ids)) != len(override_ids):
            raise ValueError(f"{zone_name} zone contains duplicate node override ids")

    nodes = cluster_setup.collect_unique_nodes(config)
    if sorted(nodes) != expected_ids:
        raise ValueError(
            f"resolved cluster node ids {sorted(nodes)} do not match requested ids {expected_ids}"
        )

    resources: list[NodeResources] = []
    for node_id in expected_ids:
        effective = nodes[node_id].get("effective")
        configured_vm = effective.get("vm") if isinstance(effective, dict) else None
        if not isinstance(configured_vm, dict) or not {
            "cpus",
            "memory",
        }.issubset(configured_vm):
            raise ValueError(f"node {node_id} must explicitly resolve CPU and memory resources")
        spec = cluster_setup.cluster_node_spec(node_id, nodes[node_id], config)
        vm = spec.get("vm")
        if not isinstance(vm, dict):
            raise ValueError(f"node {node_id} is missing VM resources")
        resources.append(
            NodeResources(
                node_id=node_id,
                vcpus=strict_positive_integer(
                    vm.get("cpus"), f"node {node_id} vCPU count"
                ),
                memory_kib=qemu_memory_kib(vm.get("memory")),
            )
        )
    return resources


def xml_memory_kib(node: ET.Element, context: str) -> int:
    amount = positive_integer(node.text or "", context)
    unit = node.get("unit", "KiB").lower()
    byte_multipliers = {
        "b": 1,
        "byte": 1,
        "bytes": 1,
        "kb": 1000,
        "kib": 1024,
        "mb": 1000 * 1000,
        "mib": 1024 * 1024,
        "gb": 1000 * 1000 * 1000,
        "gib": 1024 * 1024 * 1024,
        "tb": 1000 * 1000 * 1000 * 1000,
        "tib": 1024 * 1024 * 1024 * 1024,
    }
    if unit not in byte_multipliers:
        raise ValueError(f"{context} has unsupported unit {node.get('unit')!r}")
    size_bytes = amount * byte_multipliers[unit]
    if size_bytes % 1024 != 0:
        raise ValueError(f"{context} is not representable as whole KiB")
    return size_bytes // 1024


def set_xml_memory_kib(node: ET.Element, memory_kib: int) -> None:
    node.text = str(positive_integer(memory_kib, "memory KiB"))
    node.set("unit", "KiB")


def patch_domain_resources(root: ET.Element, resources: NodeResources) -> None:
    memory = root.find("memory")
    if memory is None:
        raise ValueError("base libvirt XML is missing <memory>")
    set_xml_memory_kib(memory, resources.memory_kib)

    current_memory = root.find("currentMemory")
    if current_memory is not None:
        set_xml_memory_kib(current_memory, resources.memory_kib)

    max_memory = root.find("maxMemory")
    if max_memory is not None and xml_memory_kib(max_memory, "<maxMemory>") < resources.memory_kib:
        set_xml_memory_kib(max_memory, resources.memory_kib)

    vcpu = root.find("vcpu")
    if vcpu is None:
        raise ValueError("base libvirt XML is missing <vcpu>")
    vcpu.text = str(resources.vcpus)
    vcpu.attrib.pop("current", None)
    vcpu.attrib.pop("cpuset", None)

    cpu = root.find("cpu")
    if cpu is not None:
        if cpu.find("numa") is not None:
            raise ValueError("config-driven resizing does not support guest CPU NUMA XML")
        topology = cpu.find("topology")
        if topology is not None:
            topology.attrib.clear()
            topology.set("sockets", "1")
            topology.set("cores", str(resources.vcpus))
            topology.set("threads", "1")

    vcpus = root.find("vcpus")
    if vcpus is not None:
        root.remove(vcpus)

    cputune = root.find("cputune")
    if cputune is not None:
        for child in list(cputune):
            if child.tag in ("vcpupin", "vcpusched") or "vcpus" in child.attrib:
                cputune.remove(child)


def patch_domain_root(
    root: ET.Element,
    identity: DomainIdentity,
    resources: NodeResources | None = None,
) -> None:
    name = root.find("name")
    if name is None:
        raise ValueError("base libvirt XML is missing <name>")
    name.text = identity.domain_name

    uuid = root.find("uuid")
    if uuid is not None:
        root.remove(uuid)

    os_node = root.find("os")
    if os_node is None:
        raise ValueError("base libvirt XML is missing <os>")
    nvram = os_node.find("nvram")
    if nvram is not None:
        nvram.text = identity.nvram_path

    devices = root.find("devices")
    if devices is None:
        raise ValueError("base libvirt XML is missing <devices>")

    disk_replaced = False
    for disk in devices.findall("disk"):
        target = disk.find("target")
        source = disk.find("source")
        if target is None or source is None:
            continue
        if target.get("dev") == "vda" and source.get("file") == identity.base_disk:
            source.set("file", identity.disk_path)
            disk_replaced = True
    if not disk_replaced:
        raise ValueError("base libvirt XML does not contain the expected vda backing disk")

    interface = devices.find("interface")
    if interface is None:
        raise ValueError("base libvirt XML is missing a network interface")
    source = interface.find("source")
    target = interface.find("target")
    if identity.network_name:
        interface.set("type", "network")
        if source is None:
            source = ET.SubElement(interface, "source")
        source.attrib.clear()
        source.set("network", identity.network_name)
        if target is not None:
            interface.remove(target)
    else:
        interface.set("type", "ethernet")
        if source is not None:
            interface.remove(source)
        if target is None:
            target = ET.SubElement(interface, "target")
        target.attrib.clear()
        target.set("dev", identity.tap_name)
        target.set("managed", "no")
    script = interface.find("script")
    if script is not None:
        interface.remove(script)
    link = interface.find("link")
    if identity.network_name:
        if link is not None:
            interface.remove(link)
    else:
        if link is None:
            link = ET.SubElement(interface, "link")
        link.attrib.clear()
        link.set("state", "up")
    mac = interface.find("mac")
    if mac is None:
        raise ValueError("base libvirt XML interface is missing a MAC address")
    mac.set("address", identity.mac_address)

    if resources is not None:
        patch_domain_resources(root, resources)


def patch_domain_file(
    xml_path: str | Path,
    identity: DomainIdentity,
    resources: NodeResources | None = None,
) -> None:
    tree = ET.parse(xml_path)
    patch_domain_root(tree.getroot(), identity, resources)
    tree.write(xml_path, encoding="unicode")


def verify_domain_root(
    root: ET.Element,
    identity: DomainIdentity,
    resources: NodeResources | None = None,
) -> None:
    name = root.find("name")
    if name is None or name.text != identity.domain_name:
        raise ValueError("defined domain name does not match the requested name")

    nvram = root.find("./os/nvram")
    if nvram is not None and nvram.text != identity.nvram_path:
        raise ValueError("defined domain NVRAM path does not match")

    devices = root.find("devices")
    if devices is None:
        raise ValueError("defined domain XML is missing <devices>")
    disk_matches = False
    for disk in devices.findall("disk"):
        target = disk.find("target")
        source = disk.find("source")
        if target is not None and source is not None and target.get("dev") == "vda":
            disk_matches = source.get("file") == identity.disk_path
            break
    if not disk_matches:
        raise ValueError("defined domain vda path does not match")

    interface = devices.find("interface")
    expected_interface_type = "network" if identity.network_name else "ethernet"
    if interface is None or interface.get("type") != expected_interface_type:
        raise ValueError(f"defined domain {expected_interface_type} interface does not match")
    source = interface.find("source")
    target = interface.find("target")
    mac = interface.find("mac")
    link = interface.find("link")
    if identity.network_name:
        if source is None or source.get("network") != identity.network_name:
            raise ValueError("defined domain managed network source does not match")
        if target is not None:
            raise ValueError("defined domain managed network retained a fixed TAP target")
    elif target is None or target.get("dev") != identity.tap_name or target.get("managed") != "no":
        raise ValueError("defined domain TAP target does not match")
    if mac is None or mac.get("address", "").lower() != identity.mac_address.lower():
        raise ValueError("defined domain MAC address does not match")
    if not identity.network_name and (link is None or link.get("state") != "up"):
        raise ValueError("defined domain interface link state does not match")

    if resources is None:
        return
    memory = root.find("memory")
    if memory is None or xml_memory_kib(memory, "<memory>") != resources.memory_kib:
        raise ValueError("defined domain memory does not match the requested KiB")
    current_memory = root.find("currentMemory")
    if current_memory is not None and (
        xml_memory_kib(current_memory, "<currentMemory>") != resources.memory_kib
    ):
        raise ValueError("defined domain current memory does not match the requested KiB")
    vcpu = root.find("vcpu")
    if vcpu is None:
        raise ValueError("defined domain XML is missing <vcpu>")
    max_vcpus = positive_integer(vcpu.text or "", "defined domain maximum vCPU count")
    current_vcpus = positive_integer(
        vcpu.get("current", str(max_vcpus)), "defined domain current vCPU count"
    )
    if max_vcpus != resources.vcpus or current_vcpus != resources.vcpus:
        raise ValueError("defined domain vCPU count does not match")
    topology = root.find("./cpu/topology")
    if topology is not None:
        topology_vcpus = 1
        for field in ("sockets", "dies", "clusters", "cores", "threads"):
            topology_vcpus *= positive_integer(
                topology.get(field, "1"), f"defined CPU topology {field}"
            )
        if topology_vcpus != resources.vcpus:
            raise ValueError("defined CPU topology does not match the vCPU count")
    if root.find("vcpus") is not None:
        raise ValueError("defined domain retained stale per-vCPU hotplug XML")
    cputune = root.find("cputune")
    if cputune is not None and any(
        child.tag in ("vcpupin", "vcpusched") or "vcpus" in child.attrib
        for child in cputune
    ):
        raise ValueError("defined domain retained stale per-vCPU tuning XML")


def verify_domain_file(
    xml_path: str | Path,
    identity: DomainIdentity,
    resources: NodeResources | None = None,
) -> None:
    verify_domain_root(ET.parse(xml_path).getroot(), identity, resources)


def add_identity_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("xml", type=Path)
    parser.add_argument("--domain-name", required=True)
    parser.add_argument("--base-disk", required=True)
    parser.add_argument("--disk-path", required=True)
    parser.add_argument("--nvram-path", required=True)
    network = parser.add_mutually_exclusive_group(required=True)
    network.add_argument("--tap-name")
    network.add_argument("--network-name")
    parser.add_argument("--mac-address", required=True)
    parser.add_argument("--cpus", type=int)
    parser.add_argument("--memory-kib", type=int)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    resources = subparsers.add_parser("resources")
    resources.add_argument("--config", required=True)
    resources.add_argument("--num-vms", required=True, type=int)
    patch = subparsers.add_parser("patch-domain")
    add_identity_arguments(patch)
    verify = subparsers.add_parser("verify-domain")
    add_identity_arguments(verify)
    return parser


def identity_from_args(args: argparse.Namespace) -> DomainIdentity:
    return DomainIdentity(
        domain_name=args.domain_name,
        base_disk=args.base_disk,
        disk_path=args.disk_path,
        nvram_path=args.nvram_path,
        tap_name=args.tap_name or "",
        mac_address=args.mac_address,
        network_name=args.network_name or "",
    )


def optional_resources_from_args(args: argparse.Namespace) -> NodeResources | None:
    if (args.cpus is None) != (args.memory_kib is None):
        raise ValueError("--cpus and --memory-kib must be supplied together")
    if args.cpus is None:
        return None
    return NodeResources(
        node_id=-1,
        vcpus=positive_integer(args.cpus, "vCPU count"),
        memory_kib=positive_integer(args.memory_kib, "memory KiB"),
    )


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "resources":
            for resources in resolve_node_resources(args.config, args.num_vms):
                print(f"{resources.node_id}\t{resources.vcpus}\t{resources.memory_kib}")
            return 0
        identity = identity_from_args(args)
        resources = optional_resources_from_args(args)
        if args.command == "patch-domain":
            patch_domain_file(args.xml, identity, resources)
            return 0
        if args.command == "verify-domain":
            verify_domain_file(args.xml, identity, resources)
            return 0
    except (ET.ParseError, json.JSONDecodeError, KeyError, OSError, TypeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    raise AssertionError(f"unhandled command {args.command!r}")


if __name__ == "__main__":
    sys.exit(main())
