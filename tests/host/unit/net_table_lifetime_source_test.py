#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
NETDEVICE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "netdevice.cpp"
NETDEVICE_HPP = ROOT / "modules" / "kern" / "src" / "net" / "netdevice.hpp"
BACKLOG_CPP = ROOT / "modules" / "kern" / "src" / "net" / "backlog.cpp"
LOOPBACK_CPP = ROOT / "modules" / "kern" / "src" / "net" / "loopback.cpp"
CDC_ETHER_CPP = ROOT / "modules" / "kern" / "src" / "dev" / "usb" / "cdc_ether.cpp"
ROUTE_CPP = ROOT / "modules" / "kern" / "src" / "net" / "route.cpp"
NETIF_CPP = ROOT / "modules" / "kern" / "src" / "net" / "netif.cpp"
PACKET_READERS = [
    ROOT / "modules" / "kern" / "src" / "net" / "proto" / name
    for name in ["arp.cpp", "ipv4.cpp", "ipv6.cpp", "udp.cpp", "tcp.cpp", "ndp.cpp"]
] + [ROOT / "modules" / "kern" / "src" / "net" / "wki" / "dev_server.cpp"]


def fail(message: str) -> None:
    raise AssertionError(message)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:void|auto)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
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


def require_tokens(source: str, tokens: list[str], context: str) -> None:
    missing = [token for token in tokens if token not in source]
    if missing:
        fail(f"{context}: missing {', '.join(missing)}")


def require_order(source: str, before: str, after: str, context: str) -> None:
    before_pos = source.find(before)
    after_pos = source.find(after)
    if before_pos < 0 or after_pos < 0 or before_pos >= after_pos:
        fail(f"{context}: expected {before!r} before {after!r}")


def require_teardown_is_nonallocating(body: str, context: str) -> None:
    forbidden = ["new (", "delete ", "allocate_", "kern_sleep", "kern_yield"]
    present = [token for token in forbidden if token in body]
    if present:
        fail(f"{context} allocates or blocks: {', '.join(present)}")


def test_tables_use_permanent_backing_and_bounded_live_indexes() -> None:
    route = ROUTE_CPP.read_text()
    netif = NETIF_CPP.read_text()

    require_tokens(
        route,
        [
            "struct RouteStorageEntry",
            "RouteStorageEntry* route_storage = nullptr",
            "std::array<std::atomic<RouteEntry*>, MAX_ROUTES> live_routes = {}",
            "std::atomic<size_t> route_scan_limit{0}",
            "std::atomic<size_t> route_live_count{0}",
            "mod::sys::Spinlock route_registry_lock",
            'static_assert(std::atomic<RouteEntry*>::is_always_lock_free, "route live slots must stay lock-free")',
            "new (std::nothrow) RouteStorageEntry{}",
            "storage->next = route_storage",
            "route_storage = storage",
        ],
        "permanent route backing",
    )
    require_tokens(
        netif,
        [
            "struct NetInterfaceStorageEntry",
            "NetInterfaceStorageEntry* interface_storage = nullptr",
            "std::array<std::atomic<NetInterface*>, MAX_NET_INTERFACES> live_interfaces = {}",
            "std::atomic<size_t> interface_scan_limit{0}",
            "std::atomic<size_t> interface_live_count{0}",
            "mod::sys::Spinlock interface_registry_lock",
            'static_assert(std::atomic<NetInterface*>::is_always_lock_free, "netif live slots must stay lock-free")',
            "new (std::nothrow) NetInterfaceStorageEntry{}",
            "storage->next = interface_storage",
            "interface_storage = storage",
        ],
        "permanent interface backing",
    )
    if "std::array<RouteEntry, MAX_ROUTES>" in route:
        fail("routes must not return pointers into reusable value-array slots")
    if "std::array<NetInterface, MAX_NET_INTERFACES>" in netif:
        fail("netifs must not return pointers into reusable value-array slots")
    for source, storage_name in ((route, "route_storage"), (netif, "interface_storage")):
        if f"delete {storage_name}" in source or f"{storage_name} = {storage_name}->next" in source:
            fail(f"published {storage_name} backing must remain permanent")


def test_live_caps_and_hot_path_scans_remain_lock_free_and_bounded() -> None:
    route = ROUTE_CPP.read_text()
    netif = NETIF_CPP.read_text()

    route_lookup = function_body(route, "route_lookup")
    require_tokens(
        route_lookup,
        [
            "route_scan_limit.load(std::memory_order_acquire)",
            "i < SCAN_LIMIT",
            "live_routes.at(i).load(std::memory_order_acquire)",
        ],
        "lock-free bounded route lookup",
    )
    if "route_storage" in route_lookup or "route_registry_lock" in route_lookup:
        fail("route lookup must scan only atomic live slots without taking the writer lock")

    find_interface = function_body(netif, "find_live_interface")
    require_tokens(
        find_interface,
        ["interface_scan_limit.load(order)", "i < SCAN_LIMIT", "live_interfaces.at(i).load(order)"],
        "lock-free bounded interface lookup",
    )
    if "interface_storage" in find_interface or "interface_registry_lock" in find_interface:
        fail("interface lookup must scan only atomic live slots without taking the writer lock")

    find_by_dev = function_body(netif, "netif_find_by_dev")
    require_tokens(
        find_by_dev,
        ["find_live_interface(dev, std::memory_order_acquire)"],
        "nonallocating packet-path interface lookup",
    )
    if "new (" in find_by_dev or "netif_get(" in find_by_dev:
        fail("netif_find_by_dev must never allocate or create interface state")

    for name in ("netif_find_by_ipv4", "netif_find_by_ipv6"):
        body = function_body(netif, name)
        require_tokens(
            body,
            [
                "interface_scan_limit.load(std::memory_order_acquire)",
                "i < SCAN_LIMIT",
                "live_interfaces.at(i).load(std::memory_order_acquire)",
            ],
            f"lock-free bounded {name}",
        )
        if "interface_storage" in body or "interface_registry_lock" in body:
            fail(f"{name} must not scan historical storage or take the writer lock")


def test_add_allocates_outside_writer_lock_and_rechecks_publication() -> None:
    route_add = function_body(ROUTE_CPP.read_text(), "route_add")
    require_order(route_add, "new (std::nothrow) RouteStorageEntry{}", "route_registry_lock.lock_irqsave()", "route allocation outside lock")
    require_order(route_add, "route_registry_lock.lock_irqsave()", "LIVE_COUNT < MAX_ROUTES", "route cap recheck under lock")
    require_order(
        route_add,
        "LIVE_COUNT < MAX_ROUTES",
        "live_routes.at(SLOT).store(route, std::memory_order_release)",
        "route publication after cap recheck",
    )
    require_order(
        route_add,
        "route_registry_lock.unlock_irqrestore(FLAGS)",
        "delete storage",
        "unpublished route storage freed after unlock",
    )
    locked_route_add = route_add[
        route_add.find("route_registry_lock.lock_irqsave()") : route_add.find("route_registry_lock.unlock_irqrestore(FLAGS)")
    ]
    if "new (" in locked_route_add or "delete " in locked_route_add:
        fail("route writer critical section must not allocate or free")

    netif_get = function_body(NETIF_CPP.read_text(), "netif_get")
    netdevice = NETDEVICE_CPP.read_text()
    require_tokens(
        netdevice,
        [
            "NetDeviceRegistryLease::NetDeviceRegistryLease() : irq_flags_(devices_lock.lock_irqsave())",
            "NetDeviceRegistryLease::~NetDeviceRegistryLease() { devices_lock.unlock_irqrestore(irq_flags_); }",
        ],
        "registration lease owns the netdevice registry lock",
    )
    require_order(
        netif_get,
        "netif_find_by_dev(dev)",
        "if (!netdev_is_registered(dev))",
        "retired interface recreation guard",
    )
    if "netif_find_by_dev(dev)" not in netif_get or "find_live_interface(dev, std::memory_order_relaxed)" not in netif_get:
        fail("netif_get must recheck duplicate concurrent creation under the writer lock")
    require_order(
        netif_get,
        "new (std::nothrow) NetInterfaceStorageEntry{}",
        "NetDeviceRegistryLease const REGISTRATION",
        "netif allocation outside lock",
    )
    require_order(
        netif_get,
        "NetDeviceRegistryLease const REGISTRATION",
        "REGISTRATION.contains(dev)",
        "registration lease before final membership recheck",
    )
    require_order(
        netif_get,
        "REGISTRATION.contains(dev)",
        "interface_registry_lock.lock_irqsave()",
        "netdevice registry before interface registry lock order",
    )
    require_order(
        netif_get,
        "interface_registry_lock.lock_irqsave()",
        "LIVE_COUNT < MAX_NET_INTERFACES",
        "netif cap recheck under lock",
    )
    require_order(
        netif_get,
        "LIVE_COUNT < MAX_NET_INTERFACES",
        "live_interfaces.at(SLOT).store(nif, std::memory_order_release)",
        "netif publication after duplicate/cap recheck",
    )
    require_order(
        netif_get,
        "interface_registry_lock.unlock_irqrestore(FLAGS)",
        "delete storage",
        "unpublished netif storage freed after unlock",
    )
    locked_netif_get = netif_get[
        netif_get.find("NetDeviceRegistryLease const REGISTRATION") : netif_get.find("interface_registry_lock.unlock_irqrestore(FLAGS)")
    ]
    if "new (" in locked_netif_get or "delete " in locked_netif_get:
        fail("netif writer critical section must not allocate or free")


def test_packet_readers_never_create_interface_state() -> None:
    for path in PACKET_READERS:
        source = path.read_text()
        if "netif_get(" in source:
            fail(f"packet/RX reader must use nonallocating netif_find_by_dev: {path.relative_to(ROOT)}")


def test_retirement_tombstones_without_reusing_or_freeing_entries() -> None:
    route = ROUTE_CPP.read_text()
    netif = NETIF_CPP.read_text()

    remove_route = function_body(route, "remove_live_route_at")
    require_tokens(
        remove_route,
        [
            "live_routes.at(index).store(nullptr, std::memory_order_release)",
            "route_live_count.store",
            "route_scan_limit.store(scan_limit, std::memory_order_release)",
        ],
        "single-route tombstone",
    )
    require_teardown_is_nonallocating(remove_route, "single-route retirement")
    if "->valid =" in remove_route:
        fail("single-route retirement must not mutate an escaped route object")

    route_del_for_dev = function_body(route, "route_del_for_dev")
    require_tokens(
        route_del_for_dev,
        [
            "route_registry_lock.lock_irqsave()",
            "live_routes.at(i).store(nullptr, std::memory_order_release)",
            "route_scan_limit.store(trimmed_scan_limit, std::memory_order_release)",
            "route_live_count.store(live_count, std::memory_order_release)",
            "route_registry_lock.unlock_irqrestore(FLAGS)",
        ],
        "device-route tombstones",
    )
    require_teardown_is_nonallocating(route_del_for_dev, "device-route retirement")
    for assignment in ("route->dest =", "route->netmask =", "route->gateway =", "route->dev =", "route->valid ="):
        if assignment in route_del_for_dev:
            fail(f"device-route retirement must preserve held-pointer snapshot: {assignment}")

    netif_del = function_body(netif, "netif_del_for_dev")
    require_tokens(
        netif_del,
        [
            "interface_registry_lock.lock_irqsave()",
            "live_interfaces.at(i).store(nullptr, std::memory_order_release)",
            "interface_live_count.store",
            "trim_interface_scan_limit_locked()",
            "interface_registry_lock.unlock_irqrestore(FLAGS)",
        ],
        "interface tombstone",
    )
    require_teardown_is_nonallocating(netif_del, "interface retirement")
    if "nif->dev =" in netif_del or "*nif =" in netif_del:
        fail("interface retirement must not mutate or clear a held object")

    route_init = function_body(route, "route_init")
    require_tokens(
        route_init,
        [
            "route_registry_lock.lock_irqsave()",
            "live_routes.at(i).store(nullptr, std::memory_order_release)",
            "route_scan_limit.store(0, std::memory_order_release)",
            "route_live_count.store(0, std::memory_order_release)",
        ],
        "route init tombstones",
    )
    if "route_storage" in route_init or "delete " in route_init or "->valid =" in route_init:
        fail("route_init must not invalidate previously returned route pointers")


def test_wki_rx_forward_hook_is_atomic_and_loaded_once_per_packet() -> None:
    header = NETDEVICE_HPP.read_text()
    backlog = BACKLOG_CPP.read_text()
    netdevice = NETDEVICE_CPP.read_text()
    cdc_ether = CDC_ETHER_CPP.read_text()

    require_tokens(
        header,
        [
            "using WkiRxForwardHook = void (*)(NetDevice* dev, PacketBuffer* pkt);",
            'static_assert(std::atomic<WkiRxForwardHook>::is_always_lock_free, "WKI RX forward hook must stay lock-free")',
            "std::atomic<WkiRxForwardHook> wki_rx_forward{nullptr}",
        ],
        "lock-free atomic WKI RX hook",
    )

    for source, function_name, call in (
        (backlog, "process_backlog_packet", "RX_FORWARD(pkt->dev, pkt)"),
        (netdevice, "netdev_rx", "RX_FORWARD(dev, pkt)"),
    ):
        body = function_body(source, function_name)
        require_tokens(
            body,
            [
                "WkiRxForwardHook const RX_FORWARD",
                "wki_rx_forward.load(std::memory_order_acquire)",
                call,
            ],
            f"{function_name} atomic WKI hook snapshot",
        )
        if body.count("wki_rx_forward.load(std::memory_order_acquire)") != 1:
            fail(f"{function_name} must take exactly one WKI hook snapshot per packet")
        if "->wki_rx_forward(" in body:
            fail(f"{function_name} must call only its atomic WKI hook snapshot")

    snapshot = function_body(netdevice, "netdev_snapshot")
    require_tokens(
        snapshot,
        ["dev->wki_rx_forward.load(std::memory_order_acquire) != nullptr"],
        "netdevice diagnostic WKI hook snapshot",
    )
    require_tokens(
        cdc_ether,
        ["cdc.netdev.wki_rx_forward.store(nullptr, std::memory_order_release)"],
        "CDC teardown WKI hook retirement",
    )


def test_loopback_reconstructs_nonassignable_atomic_netdevice() -> None:
    body = function_body(LOOPBACK_CPP.read_text(), "loopback_init")
    require_tokens(body, ["new (&lo_dev) NetDevice{}"], "loopback atomic NetDevice initialization")
    if "lo_dev = {};" in body:
        fail("loopback_init must not assign a NetDevice containing an atomic RX hook")


def main() -> None:
    test_tables_use_permanent_backing_and_bounded_live_indexes()
    test_live_caps_and_hot_path_scans_remain_lock_free_and_bounded()
    test_add_allocates_outside_writer_lock_and_rechecks_publication()
    test_packet_readers_never_create_interface_state()
    test_retirement_tombstones_without_reusing_or_freeing_entries()
    test_wki_rx_forward_hook_is_atomic_and_loaded_once_per_packet()
    test_loopback_reconstructs_nonassignable_atomic_netdevice()
    print("network table lifetime source invariants hold")


if __name__ == "__main__":
    main()
