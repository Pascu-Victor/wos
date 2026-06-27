#!/usr/bin/env python3

"""Source invariants for block-device errno handling."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BLOCK_DEVICE = ROOT / "modules/kern/src/dev/block_device.cpp"
AHCI = ROOT / "modules/kern/src/dev/ahci.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\bauto\s+{re.escape(name)}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
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


def main() -> None:
    block_source = BLOCK_DEVICE.read_text()
    ahci_source = AHCI.read_text()

    normalize_body = function_body(block_source, "normalize_io_result")
    require(normalize_body, "return -EIO;", "raw block driver -1 must normalize to EIO")
    require(normalize_body, "return (rc < 0) ? rc : -rc;", "positive driver errors must normalize to negative errno")

    for name in ("block_read", "block_write", "block_flush"):
        body = function_body(block_source, name)
        require(body, "normalize_io_result(", f"{name} must normalize driver errors")
        require_absent(body, "return -1;", f"{name} must not leak raw -1")

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
        "constexpr uint32_t AHCI_MAX_SECTORS_PER_CMD = 4096;",
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
