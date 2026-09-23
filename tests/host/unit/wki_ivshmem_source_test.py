#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
IVSHMEM_CPP = ROOT / "modules" / "kern" / "src" / "net" / "wki" / "transport_ivshmem.cpp"
INIT_WRAPPERS_CPP = ROOT / "modules" / "kern" / "src" / "platform" / "init" / "init_wrappers.cpp"


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
            "while (shared_ring::load_word(ready) == 0)",
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


def test_explicit_wki_boot_selection_reserves_the_single_ivshmem_device() -> None:
    source = INIT_WRAPPERS_CPP.read_text()
    body = function_body(source, "ivshmem_init")
    require_order(
        body,
        [
            'cmdline_has_token(get_kernel_cmdline(), "wki.ivshmem")',
            "Reserving ivshmem device for WKI RDMA transport",
            "return;",
            "dev::ivshmem::ivshmem_net_init()",
        ],
        "single-device ivshmem ownership selection",
    )


def test_receive_capacity_includes_complete_authenticated_frame() -> None:
    source = IVSHMEM_CPP.read_text()
    if "using IvshmemFrame = std::array<uint8_t, WKI_MAX_FRAME_SIZE>;" not in source:
        fail("ivshmem receive capacity must include the full common authenticated envelope")
    if "IvshmemFrame buf{};" not in function_body(source, "drain_rx_ring"):
        fail("the real single-drainer must use the complete frame buffer")
    if "s_ivshmem_transport.mtu = static_cast<uint16_t>(WKI_MAX_FRAME_SIZE - WKI_HEADER_SIZE);" not in source:
        fail("ivshmem MTU must describe payload capacity excluding the WKI header")


def main() -> None:
    test_peer_ready_wait_deadline_is_saturating()
    test_rdma_bitmap_allocation_and_free_are_irq_safe()
    test_explicit_wki_boot_selection_reserves_the_single_ivshmem_device()
    test_receive_capacity_includes_complete_authenticated_frame()
    test_shared_pci_owner_precedes_publication()
    test_terminal_callback_coverage_and_deferred_irq()
    print("WKI ivshmem source invariants hold")


def test_shared_pci_owner_precedes_publication() -> None:
    source = IVSHMEM_CPP.read_text()
    net = (ROOT / "modules/kern/src/dev/ivshmem/ivshmem_net.cpp").read_text()
    for driver in (source, net):
        for forbidden in ("pci_enable_bus_master(", "pci_enable_msi(", "interrupt_line + 32", "IVSHMEM_REG_INTRMASK", "IVSHMEM_REG_INTRSTATUS"):
            if forbidden in driver:
                fail(f"ivshmem must use shared validated revision-1 PCI ownership: {forbidden}")
    for driver, name in ((source, "wki_ivshmem_transport_init"), (net, "init_device")):
        body = function_body(driver, name)
        require_order(body, ["resources.prepare(", "shared_ring::snapshot(", "shared_ring::bind(", "resources.activate("],
                      "validated immutable geometry before interrupt/BME admission")
        if "shmem + hdr->" in body:
            fail("peer-owned header offsets must not be reread during pointer construction")
    require_order(function_body(source, "wki_ivshmem_transport_init"),
                  ["resources.prepare(", "// Set up TX/RX rings", "resources.activate(", "wki_transport_register("],
                  "WKI private state and IRQ ownership before publication")
    require_order(function_body(net, "init_device"),
                  ["ivshmem_net_is_claimed(pci_dev)", "probe_count++", "resources.prepare(", "napi_init(", "resources.activate(", "netdev_register("],
                  "net private state and IRQ ownership before publication")
    if "resources.device == dev" not in function_body(net, "ivshmem_net_is_claimed"):
        fail("failed owner metadata must prevent a second driver claiming its PCI device")


def test_terminal_callback_coverage_and_deferred_irq() -> None:
    source = IVSHMEM_CPP.read_text()
    for name in ("ivshmem_wki_tx", "ivshmem_wki_tx_pkt", "ivshmem_wki_set_rx_handler", "ivshmem_wki_rdma_register_region",
                 "ivshmem_wki_rdma_read", "ivshmem_wki_rdma_write", "ivshmem_wki_doorbell", "drain_rx_ring", "ivshmem_wki_irq",
                 "wki_ivshmem_rdma_alloc", "wki_ivshmem_rdma_free", "wki_ivshmem_rdma_ptr", "wki_ivshmem_irq_mailbox_write",
                 "wki_ivshmem_transport_poll"):
        if "CallbackGuard const GUARD" not in function_body(source, name):
            fail(f"{name} must own terminal callback admission")
    irq = function_body(source, "ivshmem_wki_irq")
    if "drain_rx_ring" in irq or "mask_vector(true)" not in irq:
        fail("WKI IRQ must only mask and defer protocol dispatch to task context")
    require_order(function_body(source, "wki_ivshmem_transport_shutdown"),
                  ["g_wki.transport_lock.lock()", "wki_chaos_transport_close", "g_wki.transport_lock.unlock()",
                   "resources.retire(", "wki_transport_unregister", "chaos_in_flight.load"],
                  "both registry/chaos lease admissions close before bounded drain and unregister")
    core = (ROOT / "modules/kern/src/net/wki/wki.cpp").read_text()
    require_order(function_body(core, "wki_shutdown"),
                  ["wki_send_raw", "if (wki_ivshmem_transport_shutdown() != 0)", "return;", "wki_peer_graceful_leave", "wki_auth_shutdown"],
                  "failed ivshmem retirement must retain peer and auth state")
    chaos = (ROOT / "modules/kern/src/net/wki/chaos.cpp").read_text()
    for name in ("deliver_ready_once", "wki_transport_send", "wki_transport_send_pkt", "wki_chaos_rx_ingress"):
        if "hold_kernel_shutdown()" not in function_body(chaos, name):
            fail(f"{name} must not abandon a transport lease during cooperative exit")
    net = (ROOT / "modules/kern/src/dev/ivshmem/ivshmem_net.cpp").read_text()
    for name in ("ivshmem_open", "ivshmem_close", "ivshmem_start_xmit", "ivshmem_irq", "ivshmem_poll"):
        # Older netdevice operations use a leading int return type.
        parsed = net.replace("int " + name + "(", "auto " + name + "(")
        if "CallbackGuard const GUARD" not in function_body(parsed, name):
            fail(f"{name} must own terminal callback admission")
    guard = (ROOT / "modules/kern/src/dev/ivshmem/callback.hpp").read_text()
    require_order(guard, ["KernelShutdownHold shutdown_", "lifecycle::Guard callback_"], "callback releases before shutdown ownership")


if __name__ == "__main__":
    main()
