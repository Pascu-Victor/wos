#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
IVSHMEM_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "transport_ivshmem.cpp"


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{", source)
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


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def test_peer_ready_wait_deadline_is_saturating() -> None:
    source = IVSHMEM_CPP.read_text()
    if "#include <net/wki/timer_math.hpp>" not in source:
        fail("ivshmem peer-ready wait must use the shared WKI timer helpers")

    body = function_body(source, "wki_ivshmem_transport_init")
    require_order(
        body,
        [
            "constexpr uint64_t PEER_READY_TIMEOUT_US = 5'000'000",
            "uint64_t const DEADLINE = wki_future_deadline_us(ker::mod::time::get_us(), PEER_READY_TIMEOUT_US)",
            "while (hdr->peer_ready == 0)",
            "if (ker::mod::time::get_us() >= DEADLINE)",
            "peer_ready timeout",
        ],
        "ivshmem peer-ready wait",
    )
    if "ker::mod::time::get_us() + PEER_READY_TIMEOUT_US" in body:
        fail("ivshmem peer-ready wait must not use wrapping deadline arithmetic")


def test_rdma_bitmap_allocation_and_free_are_irq_safe() -> None:
    source = IVSHMEM_CPP.read_text()
    alloc = function_body(source, "rdma_bitmap_alloc")
    free = function_body(source, "rdma_bitmap_free")

    if "#include <platform/sys/spinlock.hpp>" not in source or "s_rdma_bitmap_lock" not in source:
        fail("ivshmem RDMA bitmap must have an explicit IRQ-safe lock")
    require_order(
        alloc,
        [
            "s_rdma_bitmap_lock.lock_irqsave()",
            "page zero permanently reserved",
            "uint32_t start_page = 1",
            "for (uint32_t page = 1",
            "s_rdma_bitmap_lock.unlock_irqrestore(FLAGS)",
        ],
        "ivshmem RDMA bitmap allocation lock lifetime",
    )
    if "offset <= 0" not in free:
        fail("ivshmem RDMA bitmap free must retain page zero as the invalid-rkey sentinel")
    require_order(
        free,
        [
            "s_rdma_bitmap_lock.lock_irqsave()",
            "for (uint32_t p = start_page",
            "s_rdma_bitmap_lock.unlock_irqrestore(FLAGS)",
        ],
        "ivshmem RDMA bitmap free lock lifetime",
    )


def main() -> None:
    test_peer_ready_wait_deadline_is_saturating()
    test_rdma_bitmap_allocation_and_free_are_irq_safe()
    print("WKI ivshmem source invariants hold")


if __name__ == "__main__":
    main()
