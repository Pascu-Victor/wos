#!/usr/bin/env python3

"""Source invariants for block-device errno handling."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BLOCK_DEVICE = ROOT / "modules/kern/src/dev/block_device.cpp"
BLOCK_DEVICE_HPP = ROOT / "modules/kern/src/dev/block_device.hpp"
BLOCK_DEVICE_KTEST = ROOT / "modules/kern/src/test/block_device_ktest.cpp"
AHCI = ROOT / "modules/kern/src/dev/ahci.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|void)\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
        source,
    )
    if match is None:
        fail(f"missing function {name}")

    depth = 1
    pos = match.end()
    while pos < len(source) and depth > 0:
        if source[pos] == "{":
            depth += 1
        elif source[pos] == "}":
            depth -= 1
        pos += 1
    if depth != 0:
        fail(f"unterminated function {name}")
    return source[match.end() : pos - 1]


def require(source: str, token: str, context: str) -> None:
    if token not in source:
        fail(f"{context}: missing {token!r}")


def require_absent(source: str, token: str, context: str) -> None:
    if token in source:
        fail(f"{context}: unexpected {token!r}")


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token!r}")
        cursor = found + len(token)


def main() -> None:
    block_source = BLOCK_DEVICE.read_text()
    block_header = BLOCK_DEVICE_HPP.read_text()
    block_ktest = BLOCK_DEVICE_KTEST.read_text()
    ahci_source = AHCI.read_text()

    for token in [
        "enum class BlockWriterLeaseOwner",
        "class BlockWriterLease",
        "BlockWriterLease(BlockWriterLease&& other) noexcept",
        "auto try_acquire(const BlockDevice* device, BlockWriterLeaseOwner owner) -> bool",
    ]:
        require(block_header, token, "block writer lease API")

    lease_acquire = function_body(block_source, "BlockWriterLease::try_acquire")
    for token in [
        "block_writer_lease_lock.lock_irqsave()",
        "owner == BlockWriterLeaseOwner::REMOTE_BINDING",
        "lease->owner_ == BlockWriterLeaseOwner::REMOTE_BINDING",
        "block_devices_overlap(lease->device_, device)",
        "block_writer_leases = this",
    ]:
        require(lease_acquire, token, "block writer lease acquisition")
    for forbidden in ["new ", "delete", "kern_yield", "mount_lock", "s_server_lock"]:
        require_absent(lease_acquire, forbidden, "block writer lease acquisition must remain allocation-free and lock-local")
    require(block_source, "take_locked(other);", "block writer lease move must relink atomically")
    require(
        block_ktest,
        "KTEST(BlockWriterLease, RemoteIsExclusiveWhileLocalMountsMayCoexist)",
        "block writer lease policy KTEST",
    )

    normalize_body = function_body(block_source, "normalize_io_result")
    require(normalize_body, "return -EIO;", "raw block driver -1 must normalize to EIO")
    require(normalize_body, "return (rc < 0) ? rc : -rc;", "positive driver errors must normalize to negative errno")

    for name in ("block_read", "block_write", "block_flush"):
        body = function_body(block_source, name)
        require(body, "normalize_io_result(", f"{name} must normalize driver errors")
        require_absent(body, "return -1;", f"{name} must not leak raw -1")

    register_body = function_body(block_source, "block_device_register")
    unregister_body = function_body(block_source, "block_device_unregister")
    remove_node_body = function_body(block_source, "remove_block_dev_node")

    require(block_source, "ker::util::SmallVec<Device*, BDEV_INLINE_ALLOC_COUNT> block_dev_nodes;", "block /dev wrappers must be pointer-stable")
    require_absent(
        block_source,
        "ker::util::SmallVec<Device, BDEV_INLINE_ALLOC_COUNT> block_dev_nodes;",
        "block /dev wrappers must not be stored by value",
    )
    require_order(
        register_body,
        [
            "auto* dev_node = new (std::nothrow) Device{}",
            "if (dev_node == nullptr)",
            "block_devices.remove_at(block_devices.size() - 1)",
            "dev_node->private_data = bdev",
            "if (!block_dev_nodes.push_back(dev_node))",
            "delete dev_node",
            "block_devices.remove_at(block_devices.size() - 1)",
            "if (dev_register(dev_node) != 0)",
            "block_dev_nodes.remove_at(block_dev_nodes.size() - 1)",
            "delete dev_node",
            "block_devices.remove_at(block_devices.size() - 1)",
        ],
        "block device register must own stable /dev wrappers and unwind failures",
    )
    require_absent(
        register_body,
        "dev_register(&block_dev_nodes.at",
        "block device register must not register addresses inside SmallVec storage",
    )
    require_order(
        remove_node_body,
        [
            "size_t const NODE_INDEX = find_block_dev_node_index(bdev)",
            "Device* dev_node = block_dev_nodes.at(NODE_INDEX)",
            "static_cast<void>(dev_unregister(dev_node))",
            "block_dev_nodes.remove_at(NODE_INDEX)",
            "delete dev_node",
        ],
        "block device unregister must remove and free /dev wrapper",
    )
    require(unregister_body, "remove_block_dev_node(bdev);", "block device unregister must free /dev wrapper storage")

    for name in ("partition_read", "partition_write", "partition_flush"):
        body = function_body(block_source, name)
        require_absent(body, "return -1;", f"{name} must not leak raw -1")
        require(body, "return -EINVAL;", f"{name} invalid arguments must use errno")

    for name in ("ahci_flush_blocks", "ahci_read_blocks", "ahci_write_blocks"):
        body = function_body(ahci_source, name)
        require_absent(body, "return -1;", f"{name} must not leak raw -1")
        require(body, "return -EINVAL;", f"{name} invalid arguments must use errno")
        require(body, "-EIO", f"{name} failed disk commands must report I/O error")

    require(ahci_source, '#include "platform/sys/mutex.hpp"', "AHCI port command locks must be sleeping locks")
    require(
        ahci_source,
        "std::array<ker::mod::sys::Mutex, MAX_PORTS> port_locks{};",
        "AHCI must serialize non-NCQ commands per port",
    )
    require(
        ahci_source,
        "non-NCQ commands, so the device must not see another command on this port",
        "AHCI completion wait must document non-NCQ serialization",
    )
    require(
        ahci_source,
        'ker::mod::mm::phys::page_alloc(CLB_SIZE, "ahci_clb")',
        "AHCI command-list memory must be physically contiguous DMA memory",
    )
    require(
        ahci_source,
        'ker::mod::mm::phys::page_alloc(FIS_RECEIVE_SIZE, "ahci_fis")',
        "AHCI received-FIS memory must be physically contiguous DMA memory",
    )
    require(
        ahci_source,
        'ker::mod::mm::phys::page_alloc(CTB_SIZE, "ahci_ctb")',
        "AHCI command-table memory must be physically contiguous DMA memory",
    )
    require(
        ahci_source,
        "constexpr uint32_t AHCI_MAX_SECTORS_PER_CMD = 1024;",
        "AHCI must split large ordinary ATA DMA transfers into conservative commands",
    )
    require_absent(
        ahci_source,
        "new (std::nothrow) uint8_t[CTB_SIZE]",
        "AHCI command tables must not use virtually contiguous heap arrays for DMA",
    )

    read_write_body = function_body(ahci_source, "read_write_disk")
    issue_start = read_write_body.find("mmio_write32(port->ci, 1U << slot);")
    wait_start = read_write_body.find("// Wait for completion while holding the per-port command lock")
    success_unlock = read_write_body.find("port_lock.unlock();\n\n    return true;", wait_start)
    if issue_start < 0 or wait_start < 0 or success_unlock < 0:
        fail("read_write_disk must hold the port lock until command completion")
    early_unlock = read_write_body.find("port_lock.unlock();", issue_start, wait_start)
    if early_unlock >= 0:
        fail("read_write_disk must not unlock the port before the non-NCQ command completes")


if __name__ == "__main__":
    main()
