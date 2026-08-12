#!/usr/bin/env python3

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
UDP_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "udp.cpp"
IPV4_CPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "ipv4.cpp"
IPV4_HPP = ROOT / "modules" / "kern" / "src" / "net" / "proto" / "ipv4.hpp"
SYS_NET_CPP = ROOT / "modules" / "kern" / "src" / "syscalls_impl" / "net" / "sys_net.cpp"
NETD_SRC_DIR = ROOT / "modules" / "netd" / "src"
NETD_INCLUDE_DIR = ROOT / "modules" / "netd" / "include"


def fail(message: str) -> None:
    raise AssertionError(message)


def read_netd_source() -> str:
    paths = [*sorted(NETD_SRC_DIR.glob("*.cpp")), *sorted(NETD_INCLUDE_DIR.rglob("*.hpp"))]
    return "\n".join(path.read_text() for path in paths)


def function_body(source: str, name: str) -> str:
    match = re.search(
        rf"\b(?:auto|int|void)\s+{name}\([^)]*\)\s*(?:->\s*[A-Za-z0-9_:<>*]+)?\s*\{{",
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


def require_order(source: str, tokens: list[str], context: str) -> None:
    cursor = 0
    for token in tokens:
        found = source.find(token, cursor)
        if found < 0:
            fail(f"{context}: missing ordered token {token}")
        cursor = found + len(token)


def block_before_token(source: str, start_token: str, end_token: str, context: str) -> str:
    start = source.find(start_token)
    if start < 0:
        fail(f"{context}: missing start token {start_token}")
    end = source.find(end_token, start + len(start_token))
    if end < 0:
        fail(f"{context}: missing end token {end_token}")
    return source[start:end]


def case_body(source: str, start_token: str, end_token: str, context: str) -> str:
    start = source.find(start_token)
    if start < 0:
        fail(f"{context}: missing switch case {start_token}")
    end = source.find(end_token, start + len(start_token))
    if end < 0:
        fail(f"{context}: missing following switch case {end_token}")
    return source[start:end]


def test_so_bindtodevice_stores_ifindex() -> None:
    source = UDP_CPP.read_text()
    setsockopt = function_body(source, "udp_setsockopt")
    require_order(
        setsockopt,
        [
            "if (level == SOL_SOCKET_LEVEL && optname == SO_BINDTODEVICE)",
            "sock->bound_ifindex = 0",
            "NetDeviceRef dev = netdev_find_by_name_ref(ifname.data())",
            "if (!dev)",
            "sock->bound_ifindex = dev->ifindex",
        ],
        "UDP SO_BINDTODEVICE handling",
    )


def test_udp_sendto_bound_device_tx_precedes_route_auto() -> None:
    source = UDP_CPP.read_text()
    transmit = function_body(source, "udp_transmit_v4")
    require_order(
        transmit,
        [
            "IPv4Address const SRC = sock->local.ipv4_address()",
            "if (sock->bound_ifindex != 0)",
            "NetDeviceRef bound_ref = netdev_find_by_ifindex(sock->bound_ifindex)",
            "auto* bound_dev = bound_ref.get()",
            "pkt_adopt_netdev_ref(pkt, std::move(bound_ref))",
            "IPv4Address const BOUND_SRC = SRC.is_any() ? first_ipv4_or_any(bound_dev) : SRC",
            "tx_ret = ipv4_tx_on_dev(pkt, bound_dev, BOUND_SRC, DST, IPPROTO_UDP, UDP_IPV4_TTL)",
            "} else if (SRC.is_any())",
            "tx_ret = ipv4_tx_auto(pkt, DST, IPPROTO_UDP)",
        ],
        "UDP sendto bound-device transmit path",
    )

    bound_branch = block_before_token(
        transmit,
        "if (sock->bound_ifindex != 0)",
        "} else if (SRC.is_any())",
        "UDP sendto bound-device branch",
    )
    if "ipv4_tx_auto(" in bound_branch:
        fail("UDP bound-device sendto branch must not fall back to route-auto transmit")
    if re.search(r"\bipv4_tx\s*\(", bound_branch) is not None:
        fail("UDP bound-device sendto branch must use ipv4_tx_on_dev, not route-selected ipv4_tx")


def test_udp_bind_port_zero_allocates_ephemeral_once() -> None:
    source = UDP_CPP.read_text()
    for token in [
        "constexpr uint16_t UDP_EPHEMERAL_PORT_FIRST = 49152",
        "constexpr uint16_t UDP_EPHEMERAL_PORT_LAST = 65535",
        "auto alloc_ephemeral_port_locked(const Socket* sock, SocketEndpoint local) -> uint16_t",
        "auto bind_udp_socket_locked(Socket* sock, SocketEndpoint local) -> int",
        "auto ensure_udp_bound_locked(Socket* sock) -> int",
    ]:
        if token not in source:
            fail(f"UDP port-zero bind support is missing {token!r}")

    bind_helper = function_body(source, "bind_udp_socket_locked")
    require_order(
        bind_helper,
        [
            "if (local.port == 0)",
            "local.port = alloc_ephemeral_port_locked(sock, local)",
            "if (local.port == 0)",
            "return -EADDRNOTAVAIL",
            "if (binding_conflicts_locked(sock, local))",
            "binding->sock = sock",
            "sock->local = local",
            "sock->state = SocketState::BOUND",
        ],
        "UDP bind helper port-zero allocation",
    )

    sendto = function_body(source, "udp_sendto")
    require_order(
        sendto,
        [
            "udp_lock.lock()",
            "int const BIND_RESULT = ensure_udp_bound_locked(sock)",
            "udp_lock.unlock()",
            "if (BIND_RESULT < 0)",
            "return BIND_RESULT",
        ],
        "UDP sendto auto-bind",
    )

    connect = function_body(source, "udp_connect")
    require_order(
        connect,
        [
            "udp_lock.lock()",
            "int const BIND_RESULT = ensure_udp_bound_locked(sock)",
            "if (BIND_RESULT == 0)",
            "sock->remote = remote",
            "sock->state = SocketState::CONNECTED",
            "udp_lock.unlock()",
            "return BIND_RESULT",
        ],
        "UDP connect auto-bind preserves connected state",
    )

    if "udp_ephemeral_port++" in source:
        fail("UDP auto-bind must not bypass the shared ephemeral allocator")


def test_udp_tx_rejects_oversize_before_packet_copy() -> None:
    source = UDP_CPP.read_text()
    if "constexpr size_t UDP_MAX_PACKET_PAYLOAD" not in source:
        fail("UDP TX must define a bounded max packet payload")
    if "PKT_BUF_SIZE - PKT_HEADROOM" not in source:
        fail("UDP TX max payload must include PacketBuffer tailroom")
    if "static_cast<size_t>(UINT16_MAX) - sizeof(UdpHeader)" not in source:
        fail("UDP TX max payload must include the UDP length-field capacity")

    helper = function_body(source, "udp_payload_too_large")
    if "len > UDP_MAX_PACKET_PAYLOAD" not in helper:
        fail("UDP oversize helper must reject payloads beyond UDP_MAX_PACKET_PAYLOAD")

    preflight = function_body(source, "udp_send_preflight")
    require_order(
        preflight,
        [
            "if (udp_payload_too_large(len))",
            "return -EMSGSIZE",
        ],
        "UDP shared oversize preflight",
    )

    transmit = function_body(source, "udp_transmit")
    require_order(
        transmit,
        [
            "int const PREFLIGHT_RESULT = udp_send_preflight(sock, buf, len)",
            "udp_transmit_prechecked(sock, destination, buf, len)",
        ],
        "UDP connected-send preflight",
    )

    sendto = function_body(source, "udp_sendto")
    require_order(
        sendto,
        [
            "socket_parse_sockaddr_endpoint(sock->domain, addr_raw, addr_len, &destination)",
            "int const PREFLIGHT_RESULT = udp_send_preflight(sock, buf, len)",
            "if (PREFLIGHT_RESULT < 0)",
            "return PREFLIGHT_RESULT",
            "int const BIND_RESULT = ensure_udp_bound_locked(sock)",
        ],
        "UDP sendto oversize rejection before auto-bind",
    )


def test_udp_receive_preserves_datagram_boundaries() -> None:
    source = UDP_CPP.read_text()
    if "struct UdpRecvRecord" not in source:
        fail("UDP receive path must keep per-datagram metadata in the socket receive ring")
    if "static_assert(sizeof(UdpRecvRecord) == 28)" not in source:
        fail("UDP receive record size must stay fixed")

    recv = function_body(source, "udp_recv")
    if "return udp_recvfrom(sock, buf, len, flags, nullptr, nullptr)" not in recv:
        fail("UDP recv must share recvfrom datagram-boundary handling while preserving call flags")

    recvfrom = function_body(source, "udp_recvfrom")
    require_order(
        recvfrom,
        [
            "sock->rcvbuf.available() < sizeof(UdpRecvRecord)",
            "sock->rcvbuf.read(&record, sizeof(record))",
            "size_t const PAYLOAD_LEN = record.payload_len",
            "socket_fill_sockaddr_endpoint(addr_raw, CAPACITY, addr_len, record.source)",
            "size_t const TO_COPY = std::min(len, PAYLOAD_LEN)",
            "sock->rcvbuf.read(buf, TO_COPY)",
            "size_t remaining = PAYLOAD_LEN - TO_COPY",
            "sock->rcvbuf.read(discard.data(), CHUNK)",
        ],
        "UDP recvfrom datagram record consumption",
    )

    poll = function_body(source, "udp_poll_check")
    if "sock->rcvbuf.available() >= sizeof(UdpRecvRecord)" not in poll:
        fail("UDP poll must only report readable once a datagram record header is queued")

    queue = function_body(source, "queue_datagram_locked")
    require_order(
        queue,
        [
            "sock->rcvbuf.free_space() < sizeof(UdpRecvRecord) + data_len",
            "UdpRecvRecord record{.payload_len = static_cast<uint16_t>(data_len), .source = source}",
            "sock->rcvbuf.write_pair(&record, sizeof(record), data, data_len)",
            "std::cmp_equal(WRITTEN, sizeof(UdpRecvRecord) + data_len)",
        ],
        "UDP RX datagram record enqueue",
    )


def test_ipv4_forced_tx_preserves_requested_device() -> None:
    header = IPV4_HPP.read_text()
    if "auto ipv4_tx_on_dev(PacketBuffer* pkt, NetDevice* out_dev" not in header:
        fail("ipv4.hpp must declare the forced-device IPv4 transmit helper")

    source = IPV4_CPP.read_text()
    tx_on_dev = function_body(source, "ipv4_tx_on_dev")
    require_order(
        tx_on_dev,
        [
            "write_ipv4_header(pkt, src, dst, proto, ttl)",
            "if (out_dev == nullptr || out_dev->state == 0)",
            "pkt_free(pkt)",
            "return -1",
            "IPv4Address next_hop = dst",
            "auto* route = route_lookup(dst)",
            "route->dev == out_dev",
            "next_hop = route->gateway",
            "NetDeviceIdentity identity = pkt->retained_netdev",
            "identity = netdev_registered_identity(out_dev)",
            "return deliver_local_or_emit(pkt, dst, out_dev, next_hop, identity)",
        ],
        "forced-device IPv4 transmit",
    )

    forbidden = [
        "out_dev = route->dev",
        "ipv4_tx_auto(",
        "return ipv4_tx(",
    ]
    present = [token for token in forbidden if token in tx_on_dev]
    if present:
        fail("forced-device IPv4 transmit must not replace the requested device: " + ", ".join(present))


def test_legacy_ipv4_ioctls_notify_wki_l3_state() -> None:
    source = SYS_NET_CPP.read_text()
    notify_body = function_body(source, "notify_netdev_l3_changed")
    require_order(
        notify_body,
        [
            "ker::net::wki::wki_dev_server_notify_net_changed(dev)",
            "ker::net::wki::wki_remotable_notify_net_changed(dev)",
        ],
        "netdev L3 notification helper",
    )

    set_addr = case_body(source, "case SIOC_SIFADDR:", "case SIOC_GIFNETMASK:", "SIOCSIFADDR")
    require_order(
        set_addr,
        [
            "nif->ipv4_addrs[0].addr = ADDR",
            "notify_netdev_l3_changed(dev)",
            "return static_cast<uint64_t>(ker::net::netif_add_ipv4(dev, ADDR, 0xFFFFFF00))",
        ],
        "SIOCSIFADDR resource update notification",
    )

    set_netmask = case_body(source, "case SIOC_SIFNETMASK:", "case SIOC_GIFHWADDR:", "SIOCSIFNETMASK")
    require_order(
        set_netmask,
        [
            "nif->ipv4_addrs[0].netmask = MASK",
            "notify_netdev_l3_changed(dev)",
            "return 0",
        ],
        "SIOCSIFNETMASK resource update notification",
    )


def test_netd_lease_apply_fails_when_l3_ioctl_fails() -> None:
    source = read_netd_source()
    apply_lease = function_body(source, "apply_lease")
    require_order(
        apply_lease,
        [
            "bool applied = true",
            "int const RET = ioctl(SOCK, SIOCSIFADDR, &ifr)",
            "logger::error(\"netd: failed to set %s IPv4 address: ret=%d errno=%d\", ifname, RET, errno)",
            "applied = false",
            "int const RET = ioctl(SOCK, SIOCSIFNETMASK, &ifr)",
            "logger::error(\"netd: failed to set %s IPv4 netmask: ret=%d errno=%d\", ifname, RET, errno)",
            "applied = false",
            "if (applied)",
            "write_resolv_conf(lease)",
            "return applied",
        ],
        "netd DHCP lease L3 apply failure handling",
    )


def main() -> None:
    test_so_bindtodevice_stores_ifindex()
    test_udp_sendto_bound_device_tx_precedes_route_auto()
    test_udp_bind_port_zero_allocates_ephemeral_once()
    test_udp_tx_rejects_oversize_before_packet_copy()
    test_udp_receive_preserves_datagram_boundaries()
    test_ipv4_forced_tx_preserves_requested_device()
    test_legacy_ipv4_ioctls_notify_wki_l3_state()
    test_netd_lease_apply_fails_when_l3_ioctl_fails()
    print("UDP SO_BINDTODEVICE DHCP startup source invariants hold")


if __name__ == "__main__":
    main()
