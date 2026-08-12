#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/address.hpp>
#include <net/checksum.hpp>
#include <net/endian.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/packet.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/icmpv6.hpp>
#include <net/proto/ipv6.hpp>
#include <net/proto/ndp.hpp>
#include <net/route6.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <test/ktest.hpp>

namespace {
using ker::net::IPv6Addr;
using ker::net::IPv6RouteSnapshot;
using ker::net::IPv6RouteSpec;
using ker::net::NetDevice;
using ker::net::NetDeviceIdentity;
using ker::net::NetDeviceOps;
using ker::net::PacketBuffer;
using ker::net::proto::ICMPv6Header;
using ker::net::proto::IPv6Address;
using ker::net::proto::IPv6Header;
using ker::net::proto::IPv6ParseError;
using ker::net::proto::IPv6RxInfo;
using ker::net::proto::MacAddress;
using ker::net::proto::NdpEntry;
using ker::net::proto::NdpNeighborSolicit;
using ker::net::proto::NdpOptionHeader;

constexpr auto ipv6(std::array<uint8_t, IPv6Address::SIZE_BYTES> bytes) -> IPv6Address { return IPv6Address::from_bytes(bytes); }

constexpr IPv6Address GLOBAL_A = ipv6({0x20, 0x01, 0x0D, 0xB8, 0xA0, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 1});
constexpr IPv6Address GLOBAL_B = ipv6({0x20, 0x01, 0x0D, 0xB8, 0xA0, 0x00, 0x00, 0x02, 0, 0, 0, 0, 0, 0, 0, 1});
constexpr IPv6Address PEER = ipv6({0x20, 0x01, 0x0D, 0xB8, 0xA0, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0, 0, 0x22});
constexpr IPv6Address LINK_LOCAL = ipv6({0xFE, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x11});
constexpr MacAddress PEER_MAC = MacAddress{{0x02, 0x44, 0x55, 0x66, 0x77, 0x88}};

std::atomic<uint32_t> control_xmit_count{};
std::atomic<uint32_t> control_xmit_preempt_depth{};
std::atomic<bool> control_xmit_ipv6_source_is_multicast{};
std::atomic<bool> control_xmit_ipv6_source_is_global_a{};

auto control_xmit(NetDevice* /*dev*/, PacketBuffer* pkt) -> int {
    control_xmit_count.fetch_add(1, std::memory_order_relaxed);
    control_xmit_preempt_depth.store(ker::mod::sched::preempt_count(), std::memory_order_relaxed);
    if (pkt->len >= ker::net::proto::ETH_HLEN + ker::net::proto::IPV6_HLEN) {
        auto const* ethernet = reinterpret_cast<const ker::net::proto::EthernetHeader*>(pkt->data);
        if (ker::net::ntohs(ethernet->ethertype) == ker::net::proto::ETH_TYPE_IPV6) {
            auto const* header = reinterpret_cast<const IPv6Header*>(pkt->data + ker::net::proto::ETH_HLEN);
            control_xmit_ipv6_source_is_multicast.store(header->src.is_multicast(), std::memory_order_relaxed);
            control_xmit_ipv6_source_is_global_a.store(header->src == GLOBAL_A, std::memory_order_relaxed);
        }
    }
    ker::net::pkt_free(pkt);
    return 0;
}

constexpr NetDeviceOps CONTROL_OPS = {
    .open = nullptr,
    .close = nullptr,
    .start_xmit = control_xmit,
    .set_mac = nullptr,
    .set_queue_cpu = nullptr,
};

void prepare_device(NetDevice& dev, const std::array<char, ker::net::NETDEV_NAME_LEN>& name, uint8_t mac_tail) {
    dev.name = name;
    dev.mac = MacAddress{{0x02, 0x00, 0x00, 0x00, 0x00, mac_tail}};
    dev.ops = &CONTROL_OPS;
    dev.mtu = 1500;
}

void make_ipv6_packet(std::array<uint8_t, 384>& bytes, size_t payload_length, uint8_t next_header, const IPv6Address& src,
                      const IPv6Address& dst) {
    bytes.fill(0);
    bytes.at(0) = 0x60;
    bytes.at(4) = static_cast<uint8_t>((payload_length >> 8U) & 0xFFU);
    bytes.at(5) = static_cast<uint8_t>(payload_length & 0xFFU);
    bytes.at(6) = next_header;
    bytes.at(7) = 64;
    std::memcpy(bytes.data() + 8, src.data(), src.size());
    std::memcpy(bytes.data() + 24, dst.data(), dst.size());
}

void init_stack_packet(PacketBuffer& pkt, size_t length) {
    pkt = {};
    pkt.data = pkt.storage.data() + ker::net::PKT_HEADROOM;
    pkt.len = length;
}

void set_icmp_checksum(PacketBuffer& pkt, const IPv6RxInfo& info) {
    auto* header = reinterpret_cast<ICMPv6Header*>(pkt.data);
    header->checksum = 0;
    header->checksum = ker::net::checksum_pseudo_ipv6(info.src, info.dst, ker::net::proto::IPV6_PROTO_ICMPV6,
                                                      static_cast<uint32_t>(pkt.len), pkt.data, pkt.len);
}

auto find_ndp(NetDeviceIdentity identity, const IPv6Address& ip, ker::net::proto::NdpSnapshot& out) -> bool {
    std::array<ker::net::proto::NdpSnapshot, ker::net::proto::NDP_CACHE_SIZE> entries{};
    size_t const TOTAL = ker::net::proto::ndp_snapshot(entries.data(), entries.size());
    for (size_t i = 0; i < TOTAL; ++i) {
        if (entries.at(i).dev_identity == identity && entries.at(i).ip == ip) {
            out = entries.at(i);
            return true;
        }
    }
    return false;
}

void packet_release_count(void* context) {
    auto* count = static_cast<uint32_t*>(context);
    ++*count;
}

auto alloc_counted_packet(uint32_t& release_count) -> PacketBuffer* {
    PacketBuffer* packet = ker::net::pkt_alloc();
    if (packet != nullptr) {
        packet->lifetime_ctx = &release_count;
        packet->lifetime_release = packet_release_count;
    }
    return packet;
}

auto indexed_peer(size_t index) -> IPv6Address {
    IPv6Address address = PEER;
    address.bytes.at(14) = static_cast<uint8_t>(index >> 8U);
    address.bytes.at(15) = static_cast<uint8_t>(index);
    return address;
}
}  // namespace

KTEST(IPv6Address, NonBytePrefixesAndScopes) {
    IPv6Address const A = ipv6({0x20, 0x01, 0x0D, 0xB8, 0, 0, 0, 0, 0x80, 0, 0, 0, 0, 0, 0, 1});
    IPv6Address const B = ipv6({0x20, 0x01, 0x0D, 0xB8, 0, 0, 0, 0, 0xFF, 0, 0, 0, 0, 0, 0, 2});
    IPv6Address const C = ipv6({0x20, 0x01, 0x0D, 0xB8, 0, 0, 0, 0, 0x7F, 0, 0, 0, 0, 0, 0, 3});
    KEXPECT_TRUE(B.matches_prefix(A, 65));
    KEXPECT_FALSE(C.matches_prefix(A, 65));
    KEXPECT_EQ(B.masked(65).bytes.at(8), static_cast<uint8_t>(0x80));
    KEXPECT_EQ(A.common_prefix_length(B), static_cast<uint8_t>(65));
    KEXPECT_TRUE(LINK_LOCAL.is_link_local_unicast());
    KEXPECT_TRUE(ker::net::proto::IPV6_ALL_ROUTERS_MULTICAST.is_link_local_multicast());
    KEXPECT_EQ(ker::net::proto::IPV6_ALL_ROUTERS_MULTICAST.multicast_scope(), static_cast<uint8_t>(2));
}

KTEST(IPv6Address, AutomaticLoopbackHasNoMacDerivedLinkLocal) {
    ker::net::NetDeviceRef loopback = ker::net::netdev_find_by_name_ref("lo");
    KREQUIRE_TRUE(static_cast<bool>(loopback));
    std::array<IPv6Addr, ker::net::MAX_ADDRS_PER_IF> addresses{};
    size_t const TOTAL = ker::net::netif_ipv6_snapshot(loopback.get(), addresses.data(), addresses.size());
    size_t loopback_count = 0;
    size_t link_local_count = 0;
    for (size_t i = 0; i < TOTAL; ++i) {
        loopback_count += addresses.at(i).addr.is_loopback() ? 1U : 0U;
        link_local_count += addresses.at(i).addr.is_link_local_unicast() ? 1U : 0U;
    }
    KEXPECT_EQ(loopback_count, static_cast<size_t>(1));
    KEXPECT_EQ(link_local_count, static_cast<size_t>(0));
}

KTEST(IPv6Parser, ValidChainAndDeterministicRejection) {
    std::array<uint8_t, 384> bytes{};
    IPv6RxInfo info{};

    make_ipv6_packet(bytes, 24, ker::net::proto::IPV6_PROTO_HOP_BY_HOP, GLOBAL_A, GLOBAL_B);
    bytes.at(40) = ker::net::proto::IPV6_PROTO_DEST_OPTIONS;
    bytes.at(41) = 0;
    bytes.at(42) = 1;
    bytes.at(43) = 4;
    bytes.at(48) = ker::net::proto::IPV6_PROTO_UDP;
    bytes.at(49) = 0;
    bytes.at(50) = 1;
    bytes.at(51) = 4;
    KEXPECT_EQ(ker::net::proto::ipv6_parse(bytes.data(), 64, info), IPv6ParseError::NONE);
    KEXPECT_EQ(info.upper_layer_offset, static_cast<size_t>(56));
    KEXPECT_EQ(info.upper_layer_length, static_cast<size_t>(8));
    KEXPECT_EQ(info.next_header, ker::net::proto::IPV6_PROTO_UDP);

    KEXPECT_EQ(ker::net::proto::ipv6_parse(bytes.data(), 63, info), IPv6ParseError::TRUNCATED);
    bytes.at(48) = ker::net::proto::IPV6_PROTO_HOP_BY_HOP;
    KEXPECT_EQ(ker::net::proto::ipv6_parse(bytes.data(), 64, info), IPv6ParseError::BAD_EXTENSION_ORDER);
    bytes.at(40) = ker::net::proto::IPV6_PROTO_FRAGMENT;
    KEXPECT_EQ(ker::net::proto::ipv6_parse(bytes.data(), 64, info), IPv6ParseError::FRAGMENT_UNSUPPORTED);
    bytes.at(40) = ker::net::proto::IPV6_PROTO_DEST_OPTIONS;
    bytes.at(42) = 0x80;
    bytes.at(43) = 0;
    KEXPECT_EQ(ker::net::proto::ipv6_parse(bytes.data(), 64, info), IPv6ParseError::UNSUPPORTED_EXTENSION);

    make_ipv6_packet(bytes, 16, ker::net::proto::IPV6_PROTO_DEST_OPTIONS, GLOBAL_A, GLOBAL_B);
    bytes.at(40) = ker::net::proto::IPV6_PROTO_DEST_OPTIONS;
    bytes.at(41) = 0;
    bytes.at(48) = ker::net::proto::IPV6_PROTO_UDP;
    bytes.at(49) = 0;
    KEXPECT_EQ(ker::net::proto::ipv6_parse(bytes.data(), 56, info), IPv6ParseError::BAD_EXTENSION_ORDER);

    make_ipv6_packet(bytes, 64, ker::net::proto::IPV6_PROTO_UDP, GLOBAL_A, GLOBAL_B);
    KEXPECT_EQ(ker::net::proto::ipv6_parse_quoted(bytes.data(), 48, info), IPv6ParseError::NONE);
    KEXPECT_EQ(info.upper_layer_length, static_cast<size_t>(8));
}

KTEST(IPv6Parser, BoundsOversizedAndNoNext) {
    std::array<uint8_t, 384> bytes{};
    IPv6RxInfo info{};
    make_ipv6_packet(bytes, 264, ker::net::proto::IPV6_PROTO_HOP_BY_HOP, GLOBAL_A, GLOBAL_B);
    bytes.at(40) = ker::net::proto::IPV6_PROTO_UDP;
    bytes.at(41) = 32;
    KEXPECT_EQ(ker::net::proto::ipv6_parse(bytes.data(), 304, info), IPv6ParseError::TOO_MANY_EXTENSION_BYTES);

    make_ipv6_packet(bytes, 8, ker::net::proto::IPV6_PROTO_NO_NEXT, GLOBAL_A, GLOBAL_B);
    KEXPECT_EQ(ker::net::proto::ipv6_parse(bytes.data(), 48, info), IPv6ParseError::NO_NEXT_HEADER);
    make_ipv6_packet(bytes, 0, ker::net::proto::IPV6_PROTO_UDP, GLOBAL_A, GLOBAL_B);
    KEXPECT_EQ(ker::net::proto::ipv6_parse(bytes.data(), 40, info), IPv6ParseError::BAD_PAYLOAD_LENGTH);
}

KTEST(IPv6Route, LongestPrefixDefaultSourceAndTeardown) {
    static NetDevice dev;  // NOLINT
    prepare_device(dev, {'k', 't', '6', 'r', 't', '0'}, 0x31);
    KREQUIRE_EQ(ker::net::netdev_register(&dev), 0);
    NetDeviceIdentity const IDENTITY = ker::net::netdev_registered_identity(&dev);
    KREQUIRE_TRUE(IDENTITY.valid());
    KREQUIRE_EQ(ker::net::netif_set_ipv6(&dev, GLOBAL_A, 64, ker::net::IPV6_ADDR_F_NODAD, UINT64_MAX, UINT64_MAX, false), 0);

    IPv6RouteSpec default_route{
        .gateway = LINK_LOCAL, .prefix_len = 0, .metric = 50, .flags = ker::net::IPV6_ROUTE_F_GATEWAY, .dev_identity = IDENTITY};
    IPv6RouteSpec specific_route{.prefix = GLOBAL_B.masked(65), .prefix_len = 65, .metric = 100, .dev_identity = IDENTITY};
    KREQUIRE_EQ(ker::net::route6_add(default_route), 0);
    KREQUIRE_EQ(ker::net::route6_add(specific_route), 0);

    IPv6RouteSnapshot route{};
    ker::net::NetDeviceRef device{};
    KEXPECT_EQ(ker::net::route6_lookup(GLOBAL_B, 0, route, device), 0);
    KEXPECT_EQ(route.prefix_len, static_cast<uint8_t>(65));
    KEXPECT_EQ(route.dev_identity.generation, IDENTITY.generation);
    device.reset();

    IPv6Address source{};
    KEXPECT_EQ(ker::net::netif_ipv6_select_source(&dev, GLOBAL_B, nullptr, source), 0);
    KEXPECT_TRUE(source == GLOBAL_A);
    IPv6RouteSpec expired = specific_route;
    expired.expires_at_ms = 0;
    KEXPECT_EQ(ker::net::route6_add(expired), 0);
    KEXPECT_EQ(ker::net::route6_lookup(GLOBAL_B, 0, route, device), 0);
    KEXPECT_EQ(route.prefix_len, static_cast<uint8_t>(0));
    KEXPECT_TRUE(route.gateway == LINK_LOCAL);
    device.reset();
    IPv6RouteSpec internal_flag = specific_route;
    internal_flag.flags = ker::net::IPV6_ROUTE_F_CONNECTED;
    KEXPECT_EQ(ker::net::route6_add(internal_flag), -EINVAL);

    ker::net::NetDeviceRetireToken token{};
    KREQUIRE_EQ(ker::net::netdev_unregister_begin(&dev, token), 0);
    KEXPECT_EQ(ker::net::route6_lookup(GLOBAL_B, 0, route, device), -ENETUNREACH);
    ker::net::netdev_unregister_wait(token);
}

KTEST(IPv6Route, ConnectedReferenceAndTransactionalFullTable) {
    static NetDevice dev;  // NOLINT
    prepare_device(dev, {'k', 't', '6', 'f', 'u', 'l', 'l'}, 0x32);
    KREQUIRE_EQ(ker::net::netdev_register(&dev), 0);
    NetDeviceIdentity const IDENTITY = ker::net::netdev_registered_identity(&dev);
    size_t const BASELINE = ker::net::route6_snapshot(nullptr, 0);
    KREQUIRE_EQ(ker::net::netif_set_ipv6(&dev, GLOBAL_A, 64, ker::net::IPV6_ADDR_F_NODAD, UINT64_MAX, UINT64_MAX, false), 0);
    IPv6Address const SECOND = ipv6({0x20, 0x01, 0x0D, 0xB8, 0xA0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2});
    KREQUIRE_EQ(ker::net::netif_set_ipv6(&dev, SECOND, 64, ker::net::IPV6_ADDR_F_NODAD, UINT64_MAX, UINT64_MAX, false), 0);
    KEXPECT_EQ(ker::net::route6_snapshot(nullptr, 0), BASELINE + 1);
    KREQUIRE_EQ(ker::net::netif_del_ipv6(&dev, GLOBAL_A, 64), 0);
    KEXPECT_EQ(ker::net::route6_snapshot(nullptr, 0), BASELINE + 1);
    KREQUIRE_EQ(ker::net::netif_del_ipv6(&dev, SECOND, 64), 0);
    KEXPECT_EQ(ker::net::route6_snapshot(nullptr, 0), BASELINE);

    std::array<IPv6RouteSpec, ker::net::MAX_IPV6_ROUTES> added{};
    size_t added_count = 0;
    for (; added_count < added.size(); ++added_count) {
        IPv6Address prefix = GLOBAL_B;
        prefix.bytes.at(14) = static_cast<uint8_t>(added_count >> 8U);
        prefix.bytes.at(15) = static_cast<uint8_t>(added_count);
        IPv6RouteSpec spec{.prefix = prefix, .prefix_len = 128, .metric = static_cast<uint32_t>(added_count), .dev_identity = IDENTITY};
        int const result = ker::net::route6_add(spec);
        if (result == -ENOSPC) {
            break;
        }
        KREQUIRE_EQ(result, 0);
        added.at(added_count) = spec;
    }
    KEXPECT_EQ(ker::net::route6_snapshot(nullptr, 0), ker::net::MAX_IPV6_ROUTES);
    IPv6Address const ROLLBACK = ipv6({0x20, 0x01, 0x0D, 0xB8, 0xA1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9});
    KEXPECT_EQ(ker::net::netif_set_ipv6(&dev, ROLLBACK, 64, ker::net::IPV6_ADDR_F_NODAD, UINT64_MAX, UINT64_MAX, false), -ENOSPC);
    IPv6Addr absent{};
    KEXPECT_FALSE(ker::net::netif_ipv6_find(&dev, ROLLBACK, absent));
    for (size_t i = 0; i < added_count; ++i) {
        KEXPECT_EQ(ker::net::route6_del(added.at(i)), 0);
    }
    KEXPECT_EQ(ker::net::route6_snapshot(nullptr, 0), BASELINE);
    KEXPECT_EQ(ker::net::netdev_unregister(&dev), 0);
}

KTEST(IPv6Address, DadDeprecationExpiryAndConflict) {
    static NetDevice dev;  // NOLINT
    prepare_device(dev, {'k', 't', '6', 'd', 'a', 'd'}, 0x33);
    KREQUIRE_EQ(ker::net::netdev_register(&dev), 0);
    NetDeviceIdentity const IDENTITY = ker::net::netdev_registered_identity(&dev);
    uint64_t const NOW = ker::mod::time::get_ms();
    KREQUIRE_EQ(ker::net::netif_set_ipv6(&dev, LINK_LOCAL, 64, ker::net::IPV6_ADDR_F_PERMANENT | ker::net::IPV6_ADDR_F_NOPREFIXROUTE,
                                         NOW + 2000, NOW + 3000, false),
                0);
    IPv6Addr state{};
    KREQUIRE_TRUE(ker::net::netif_ipv6_find(&dev, LINK_LOCAL, state));
    KEXPECT_EQ(state.state, IPv6Addr::State::TENTATIVE);
    std::array<ker::net::IPv6DadProbe, ker::net::MAX_NET_INTERFACES * ker::net::MAX_ADDRS_PER_IF> probes{};
    size_t const PROBE_COUNT = ker::net::netif_ipv6_timer_tick(NOW, probes.data(), probes.size());
    KEXPECT_TRUE(PROBE_COUNT >= 1);
    for (auto& probe : probes) {
        probe.device.reset();
    }
    static_cast<void>(ker::net::netif_ipv6_timer_tick(NOW + 1000, nullptr, 0));
    KREQUIRE_TRUE(ker::net::netif_ipv6_find(&dev, LINK_LOCAL, state));
    KEXPECT_EQ(state.state, IPv6Addr::State::PREFERRED);
    KEXPECT_EQ(state.dad_probes_sent, static_cast<uint8_t>(0));

    uint32_t const SLAAC_FLAGS = ker::net::IPV6_ADDR_F_AUTOCONF | ker::net::IPV6_ADDR_F_NOPREFIXROUTE;
    KREQUIRE_EQ(ker::net::netif_set_ipv6(&dev, LINK_LOCAL, 64, SLAAC_FLAGS, NOW + 3000, NOW + 5000, true), 0);
    KREQUIRE_TRUE(ker::net::netif_ipv6_find(&dev, LINK_LOCAL, state));
    KEXPECT_EQ(state.state, IPv6Addr::State::PREFERRED);
    // A completed DAD cycle is represented by the usable state. Probe/deadline
    // bookkeeping is meaningful only while the address remains tentative.
    KEXPECT_EQ(state.dad_probes_sent, static_cast<uint8_t>(0));
    static_cast<void>(ker::net::netif_ipv6_timer_tick(NOW + 3000, nullptr, 0));
    KREQUIRE_TRUE(ker::net::netif_ipv6_find(&dev, LINK_LOCAL, state));
    KEXPECT_EQ(state.state, IPv6Addr::State::DEPRECATED);
    KREQUIRE_EQ(ker::net::netif_set_ipv6(&dev, LINK_LOCAL, 64, SLAAC_FLAGS, NOW + 6000, NOW + 7000, true), 0);
    KREQUIRE_TRUE(ker::net::netif_ipv6_find(&dev, LINK_LOCAL, state));
    KEXPECT_EQ(state.state, IPv6Addr::State::PREFERRED);
    KEXPECT_EQ(state.dad_probes_sent, static_cast<uint8_t>(0));
    KEXPECT_EQ(ker::net::netif_set_ipv6(&dev, LINK_LOCAL, 64, SLAAC_FLAGS | ker::net::IPV6_ADDR_F_TENTATIVE, NOW + 6000, NOW + 7000, true),
               -EINVAL);
    KEXPECT_EQ(ker::net::netif_set_ipv6(&dev, LINK_LOCAL, 64, SLAAC_FLAGS, 0, 0, true), 0);
    KEXPECT_FALSE(ker::net::netif_ipv6_find(&dev, LINK_LOCAL, state));

    IPv6Address const DUPLICATE = ipv6({0xFE, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x12});
    KREQUIRE_EQ(ker::net::netif_set_ipv6(&dev, DUPLICATE, 64, ker::net::IPV6_ADDR_F_NOPREFIXROUTE, UINT64_MAX, UINT64_MAX, false), 0);
    KEXPECT_TRUE(ker::net::netif_ipv6_dad_failed(IDENTITY, DUPLICATE));
    KREQUIRE_TRUE(ker::net::netif_ipv6_find(&dev, DUPLICATE, state));
    KEXPECT_EQ(state.state, IPv6Addr::State::DADFAILED);
    KEXPECT_TRUE((state.flags & ker::net::IPV6_ADDR_F_DADFAILED) != 0);

    IPv6Address const NEEDS_DAD = ipv6({0xFE, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x13});
    KREQUIRE_EQ(ker::net::netif_set_ipv6(&dev, NEEDS_DAD, 64, ker::net::IPV6_ADDR_F_NODAD | ker::net::IPV6_ADDR_F_NOPREFIXROUTE, UINT64_MAX,
                                         UINT64_MAX, false),
                0);
    KREQUIRE_EQ(ker::net::netif_set_ipv6(&dev, NEEDS_DAD, 64, ker::net::IPV6_ADDR_F_NOPREFIXROUTE, UINT64_MAX, UINT64_MAX, true), 0);
    KREQUIRE_TRUE(ker::net::netif_ipv6_find(&dev, NEEDS_DAD, state));
    KEXPECT_EQ(state.state, IPv6Addr::State::TENTATIVE);
    KEXPECT_EQ(ker::net::netdev_unregister(&dev), 0);
}

KTEST(Ndp, OptionBoundsAndLinkAddressSemantics) {
    std::array<uint8_t, 16> options{};
    ker::net::proto::NdpParsedOptions parsed{};
    options.at(0) = ker::net::proto::NDP_OPT_SRC_LINK_ADDR;
    options.at(1) = 1;
    std::memcpy(options.data() + sizeof(NdpOptionHeader), PEER_MAC.data(), PEER_MAC.size());
    KEXPECT_EQ(ker::net::proto::ndp_parse_options(options.data(), 8, parsed), ker::net::proto::NdpOptionError::NONE);
    KEXPECT_TRUE(parsed.has_source_link_address);
    options.at(1) = 0;
    KEXPECT_EQ(ker::net::proto::ndp_parse_options(options.data(), 8, parsed), ker::net::proto::NdpOptionError::ZERO_LENGTH);
    options.at(1) = 1;
    std::memset(options.data() + sizeof(NdpOptionHeader), 0, MacAddress::SIZE_BYTES);
    KEXPECT_EQ(ker::net::proto::ndp_parse_options(options.data(), 8, parsed), ker::net::proto::NdpOptionError::INVALID_LINK_ADDRESS);
    options.at(2) = 0x01;
    KEXPECT_EQ(ker::net::proto::ndp_parse_options(options.data(), 8, parsed), ker::net::proto::NdpOptionError::INVALID_LINK_ADDRESS);
    std::memcpy(options.data() + sizeof(NdpOptionHeader), PEER_MAC.data(), PEER_MAC.size());
    std::memcpy(options.data() + 8, options.data(), 8);
    KEXPECT_EQ(ker::net::proto::ndp_parse_options(options.data(), options.size(), parsed),
               ker::net::proto::NdpOptionError::DUPLICATE_LINK_ADDRESS);
}

KTEST(Ndp, NeighborHandlersRejectBadTargetsAndWrongOptions) {
    static NetDevice dev;  // NOLINT
    prepare_device(dev, {'k', 't', '6', 'n', 'd', 'p', 'v'}, 0x3B);
    KREQUIRE_EQ(ker::net::netdev_register(&dev), 0);
    NetDeviceIdentity const IDENTITY = ker::net::netdev_registered_identity(&dev);
    ker::net::pkt_pool_init();

    auto make_ns = [](const IPv6Address& target, uint8_t option_type) -> PacketBuffer* {
        PacketBuffer* packet = ker::net::pkt_alloc();
        if (packet == nullptr) {
            return nullptr;
        }
        constexpr size_t LENGTH = sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit) + 8;
        auto* data = packet->put(LENGTH);
        std::memset(data, 0, LENGTH);
        reinterpret_cast<ICMPv6Header*>(data)->type = ker::net::proto::ICMPV6_NEIGHBOR_SOLICIT;
        reinterpret_cast<NdpNeighborSolicit*>(data + sizeof(ICMPv6Header))->target = target;
        auto* option = reinterpret_cast<NdpOptionHeader*>(data + sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit));
        option->type = option_type;
        option->length = 1;
        std::memcpy(reinterpret_cast<uint8_t*>(option) + sizeof(NdpOptionHeader), PEER_MAC.data(), PEER_MAC.size());
        return packet;
    };
    auto make_na = [](const IPv6Address& target, uint8_t option_type) -> PacketBuffer* {
        PacketBuffer* packet = ker::net::pkt_alloc();
        if (packet == nullptr) {
            return nullptr;
        }
        constexpr size_t LENGTH = sizeof(ICMPv6Header) + sizeof(ker::net::proto::NdpNeighborAdvert) + 8;
        auto* data = packet->put(LENGTH);
        std::memset(data, 0, LENGTH);
        reinterpret_cast<ICMPv6Header*>(data)->type = ker::net::proto::ICMPV6_NEIGHBOR_ADVERT;
        reinterpret_cast<ker::net::proto::NdpNeighborAdvert*>(data + sizeof(ICMPv6Header))->target = target;
        auto* option = reinterpret_cast<NdpOptionHeader*>(data + sizeof(ICMPv6Header) + sizeof(ker::net::proto::NdpNeighborAdvert));
        option->type = option_type;
        option->length = 1;
        std::memcpy(reinterpret_cast<uint8_t*>(option) + sizeof(NdpOptionHeader), PEER_MAC.data(), PEER_MAC.size());
        return packet;
    };

    IPv6RxInfo ns_info{.src = PEER,
                       .dst = ker::net::proto::ipv6_make_solicited_node(GLOBAL_B),
                       .next_header = ker::net::proto::IPV6_PROTO_ICMPV6,
                       .hop_limit = 255};
    PacketBuffer* unspecified_ns = make_ns(IPv6Address::unspecified(), ker::net::proto::NDP_OPT_SRC_LINK_ADDR);
    KREQUIRE_NE(unspecified_ns, nullptr);
    ker::net::proto::ndp_handle_ns(&dev, unspecified_ns, ns_info);
    ker::net::proto::NdpSnapshot snapshot{};
    KEXPECT_FALSE(find_ndp(IDENTITY, PEER, snapshot));

    PacketBuffer* wrong_ns = make_ns(GLOBAL_B, ker::net::proto::NDP_OPT_TGT_LINK_ADDR);
    KREQUIRE_NE(wrong_ns, nullptr);
    ker::net::proto::ndp_handle_ns(&dev, wrong_ns, ns_info);
    KEXPECT_FALSE(find_ndp(IDENTITY, PEER, snapshot));

    IPv6RxInfo na_info{.src = PEER, .dst = GLOBAL_A, .next_header = ker::net::proto::IPV6_PROTO_ICMPV6, .hop_limit = 255};
    PacketBuffer* wrong_na = make_na(PEER, ker::net::proto::NDP_OPT_SRC_LINK_ADDR);
    KREQUIRE_NE(wrong_na, nullptr);
    ker::net::proto::ndp_handle_na(&dev, wrong_na, na_info);
    KEXPECT_FALSE(find_ndp(IDENTITY, PEER, snapshot));

    PacketBuffer* unspecified_na = make_na(IPv6Address::unspecified(), ker::net::proto::NDP_OPT_TGT_LINK_ADDR);
    KREQUIRE_NE(unspecified_na, nullptr);
    ker::net::proto::ndp_handle_na(&dev, unspecified_na, na_info);
    KEXPECT_FALSE(find_ndp(IDENTITY, IPv6Address::unspecified(), snapshot));
    KEXPECT_EQ(ker::net::netdev_unregister(&dev), 0);
}

KTEST(Ndp, QueueLimitRetriesAndTeardownBalance) {
    static NetDevice dev;  // NOLINT
    prepare_device(dev, {'k', 't', '6', 'n', 'd', 'p', '0'}, 0x34);
    KREQUIRE_EQ(ker::net::netdev_register(&dev), 0);
    NetDeviceIdentity const IDENTITY = ker::net::netdev_registered_identity(&dev);
    ker::net::pkt_pool_init();
    size_t const BASELINE_FREE = ker::net::pkt_pool_free_count();
    size_t const BASELINE_QUEUED = ker::net::proto::ndp_queued_packet_count();
    uint32_t releases = 0;
    MacAddress resolved{};
    control_xmit_count.store(0, std::memory_order_relaxed);
    for (size_t i = 0; i < ker::net::proto::NDP_PENDING_PER_NEIGHBOR + 1; ++i) {
        PacketBuffer* packet = alloc_counted_packet(releases);
        KREQUIRE_NE(packet, nullptr);
        auto const result = ker::net::proto::ndp_resolve(IDENTITY, PEER, GLOBAL_A, resolved, packet);
        if (i < ker::net::proto::NDP_PENDING_PER_NEIGHBOR) {
            KEXPECT_EQ(result, ker::net::proto::NdpResolveResult::QUEUED);
        } else {
            KEXPECT_EQ(result, ker::net::proto::NdpResolveResult::DROPPED);
        }
    }
    KEXPECT_EQ(ker::net::proto::ndp_queued_packet_count(), BASELINE_QUEUED + ker::net::proto::NDP_PENDING_PER_NEIGHBOR);
    KEXPECT_EQ(releases, 1U);
    ker::net::proto::NdpSnapshot snapshot{};
    KREQUIRE_TRUE(find_ndp(IDENTITY, PEER, snapshot));
    KEXPECT_EQ(snapshot.state, NdpEntry::State::INCOMPLETE);
    KEXPECT_EQ(snapshot.probes_sent, static_cast<uint8_t>(1));
    ker::net::proto::ndp_timer_tick(snapshot.next_event_ms);
    KREQUIRE_TRUE(find_ndp(IDENTITY, PEER, snapshot));
    KEXPECT_EQ(snapshot.probes_sent, static_cast<uint8_t>(2));
    ker::net::proto::ndp_timer_tick(snapshot.next_event_ms);
    KREQUIRE_TRUE(find_ndp(IDENTITY, PEER, snapshot));
    KEXPECT_EQ(snapshot.probes_sent, static_cast<uint8_t>(3));
    ker::net::proto::ndp_timer_tick(snapshot.next_event_ms);
    KEXPECT_FALSE(find_ndp(IDENTITY, PEER, snapshot));
    KEXPECT_EQ(releases, static_cast<uint32_t>(ker::net::proto::NDP_PENDING_PER_NEIGHBOR + 1));
    KEXPECT_EQ(ker::net::proto::ndp_queued_packet_count(), BASELINE_QUEUED);
    KEXPECT_EQ(ker::net::pkt_pool_free_count(), BASELINE_FREE);
    KEXPECT_EQ(control_xmit_preempt_depth.load(std::memory_order_relaxed), 0U);
    KEXPECT_EQ(ker::net::netdev_unregister(&dev), 0);
}

KTEST(Ndp, NeighborSolicitationLearnsAndFlushesPending) {
    static NetDevice dev;  // NOLINT
    prepare_device(dev, {'k', 't', '6', 'n', 'd', 'p', '1'}, 0x35);
    KREQUIRE_EQ(ker::net::netdev_register(&dev), 0);
    NetDeviceIdentity const IDENTITY = ker::net::netdev_registered_identity(&dev);
    ker::net::pkt_pool_init();
    uint32_t releases = 0;
    MacAddress resolved{};
    PacketBuffer* pending = alloc_counted_packet(releases);
    KREQUIRE_NE(pending, nullptr);
    KREQUIRE_EQ(ker::net::proto::ndp_resolve(IDENTITY, PEER, GLOBAL_A, resolved, pending), ker::net::proto::NdpResolveResult::QUEUED);
    uint32_t const AFTER_PROBE = control_xmit_count.load(std::memory_order_relaxed);

    PacketBuffer* solicitation = ker::net::pkt_alloc();
    KREQUIRE_NE(solicitation, nullptr);
    constexpr size_t LENGTH = sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit) + 8;
    auto* data = solicitation->put(LENGTH);
    std::memset(data, 0, LENGTH);
    reinterpret_cast<ICMPv6Header*>(data)->type = ker::net::proto::ICMPV6_NEIGHBOR_SOLICIT;
    auto* body = reinterpret_cast<NdpNeighborSolicit*>(data + sizeof(ICMPv6Header));
    body->target = GLOBAL_B;
    auto* option = reinterpret_cast<NdpOptionHeader*>(data + sizeof(ICMPv6Header) + sizeof(NdpNeighborSolicit));
    option->type = ker::net::proto::NDP_OPT_SRC_LINK_ADDR;
    option->length = 1;
    std::memcpy(reinterpret_cast<uint8_t*>(option) + sizeof(NdpOptionHeader), PEER_MAC.data(), PEER_MAC.size());
    IPv6RxInfo info{.src = PEER,
                    .dst = ker::net::proto::ipv6_make_solicited_node(GLOBAL_B),
                    .next_header = ker::net::proto::IPV6_PROTO_ICMPV6,
                    .hop_limit = 255,
                    .upper_layer_length = LENGTH};
    ker::net::proto::ndp_handle_ns(&dev, solicitation, info);
    KEXPECT_EQ(releases, 1U);
    KEXPECT_EQ(control_xmit_count.load(std::memory_order_relaxed), AFTER_PROBE + 1);
    KEXPECT_EQ(ker::net::proto::ndp_queued_packet_count(), static_cast<size_t>(0));
    ker::net::proto::NdpSnapshot learned{};
    KREQUIRE_TRUE(find_ndp(IDENTITY, PEER, learned));
    KEXPECT_EQ(learned.state, NdpEntry::State::STALE);
    KEXPECT_EQ(learned.mac.bytes.at(5), PEER_MAC.bytes.at(5));

    PacketBuffer* immediate = ker::net::pkt_alloc();
    KREQUIRE_NE(immediate, nullptr);
    KEXPECT_EQ(ker::net::proto::ndp_resolve(IDENTITY, PEER, GLOBAL_B, resolved, immediate), ker::net::proto::NdpResolveResult::RESOLVED);
    ker::net::pkt_free(immediate);
    KREQUIRE_TRUE(find_ndp(IDENTITY, PEER, learned));
    KEXPECT_EQ(learned.state, NdpEntry::State::DELAY);
    ker::net::proto::ndp_timer_tick(learned.next_event_ms);
    KREQUIRE_TRUE(find_ndp(IDENTITY, PEER, learned));
    KEXPECT_EQ(learned.state, NdpEntry::State::PROBE);
    ker::net::proto::ndp_forget_device(IDENTITY);
    KEXPECT_EQ(ker::net::netdev_unregister(&dev), 0);
}

KTEST(Ndp, NeighborAdvertisementRecoversAndFlushesPending) {
    static NetDevice dev;  // NOLINT
    prepare_device(dev, {'k', 't', '6', 'n', 'd', 'p', 'a'}, 0x39);
    KREQUIRE_EQ(ker::net::netdev_register(&dev), 0);
    NetDeviceIdentity const IDENTITY = ker::net::netdev_registered_identity(&dev);
    ker::net::pkt_pool_init();
    uint32_t releases = 0;
    MacAddress resolved{};
    PacketBuffer* pending = alloc_counted_packet(releases);
    KREQUIRE_NE(pending, nullptr);
    KREQUIRE_EQ(ker::net::proto::ndp_resolve(IDENTITY, PEER, GLOBAL_A, resolved, pending), ker::net::proto::NdpResolveResult::QUEUED);
    uint32_t const AFTER_PROBE = control_xmit_count.load(std::memory_order_relaxed);

    PacketBuffer* advertisement = ker::net::pkt_alloc();
    KREQUIRE_NE(advertisement, nullptr);
    constexpr size_t LENGTH = sizeof(ICMPv6Header) + sizeof(ker::net::proto::NdpNeighborAdvert) + 8;
    auto* data = advertisement->put(LENGTH);
    std::memset(data, 0, LENGTH);
    reinterpret_cast<ICMPv6Header*>(data)->type = ker::net::proto::ICMPV6_NEIGHBOR_ADVERT;
    auto* body = reinterpret_cast<ker::net::proto::NdpNeighborAdvert*>(data + sizeof(ICMPv6Header));
    body->flags = ker::net::htonl(ker::net::proto::NDP_NA_FLAG_SOLICITED | ker::net::proto::NDP_NA_FLAG_OVERRIDE);
    body->target = PEER;
    auto* option = reinterpret_cast<NdpOptionHeader*>(data + sizeof(ICMPv6Header) + sizeof(ker::net::proto::NdpNeighborAdvert));
    option->type = ker::net::proto::NDP_OPT_TGT_LINK_ADDR;
    option->length = 1;
    std::memcpy(reinterpret_cast<uint8_t*>(option) + sizeof(NdpOptionHeader), PEER_MAC.data(), PEER_MAC.size());
    IPv6RxInfo info{
        .src = PEER, .dst = GLOBAL_A, .next_header = ker::net::proto::IPV6_PROTO_ICMPV6, .hop_limit = 255, .upper_layer_length = LENGTH};
    ker::net::proto::ndp_handle_na(&dev, advertisement, info);
    KEXPECT_EQ(releases, 1U);
    KEXPECT_EQ(control_xmit_count.load(std::memory_order_relaxed), AFTER_PROBE + 1);
    ker::net::proto::NdpSnapshot recovered{};
    KREQUIRE_TRUE(find_ndp(IDENTITY, PEER, recovered));
    KEXPECT_EQ(recovered.state, NdpEntry::State::REACHABLE);
    KEXPECT_EQ(recovered.pending_count, static_cast<uint8_t>(0));
    ker::net::proto::ndp_forget_device(IDENTITY);
    KEXPECT_EQ(ker::net::netdev_unregister(&dev), 0);
}

KTEST(Ndp, GlobalQueuePressureAndCacheEvictionAreBounded) {
    static NetDevice dev;  // NOLINT
    prepare_device(dev, {'k', 't', '6', 'n', 'd', 'p', 'g'}, 0x3A);
    KREQUIRE_EQ(ker::net::netdev_register(&dev), 0);
    NetDeviceIdentity const IDENTITY = ker::net::netdev_registered_identity(&dev);
    ker::net::pkt_pool_init();
    size_t const BASELINE_FREE = ker::net::pkt_pool_free_count();
    uint32_t releases = 0;
    MacAddress resolved{};
    constexpr size_t NEIGHBORS_AT_GLOBAL_LIMIT = ker::net::proto::NDP_PENDING_GLOBAL / ker::net::proto::NDP_PENDING_PER_NEIGHBOR;
    for (size_t neighbor = 0; neighbor < NEIGHBORS_AT_GLOBAL_LIMIT; ++neighbor) {
        for (size_t queued = 0; queued < ker::net::proto::NDP_PENDING_PER_NEIGHBOR; ++queued) {
            PacketBuffer* packet = alloc_counted_packet(releases);
            KREQUIRE_NE(packet, nullptr);
            KEXPECT_EQ(ker::net::proto::ndp_resolve(IDENTITY, indexed_peer(neighbor + 1), GLOBAL_A, resolved, packet),
                       ker::net::proto::NdpResolveResult::QUEUED);
        }
    }
    KEXPECT_EQ(ker::net::proto::ndp_queued_packet_count(), ker::net::proto::NDP_PENDING_GLOBAL);
    PacketBuffer* overflow = alloc_counted_packet(releases);
    KREQUIRE_NE(overflow, nullptr);
    KEXPECT_EQ(ker::net::proto::ndp_resolve(IDENTITY, indexed_peer(100), GLOBAL_A, resolved, overflow),
               ker::net::proto::NdpResolveResult::DROPPED);
    KEXPECT_EQ(releases, 1U);
    ker::net::proto::ndp_forget_device(IDENTITY);
    KEXPECT_EQ(releases, static_cast<uint32_t>(ker::net::proto::NDP_PENDING_GLOBAL + 1));
    KEXPECT_EQ(ker::net::proto::ndp_queued_packet_count(), static_cast<size_t>(0));
    KEXPECT_EQ(ker::net::pkt_pool_free_count(), BASELINE_FREE);

    releases = 0;
    for (size_t i = 0; i < ker::net::proto::NDP_CACHE_SIZE + 1; ++i) {
        PacketBuffer* packet = alloc_counted_packet(releases);
        KREQUIRE_NE(packet, nullptr);
        KEXPECT_EQ(ker::net::proto::ndp_resolve(IDENTITY, indexed_peer(0x100 + i), GLOBAL_A, resolved, packet),
                   ker::net::proto::NdpResolveResult::QUEUED);
    }
    KEXPECT_EQ(releases, 1U);
    KEXPECT_EQ(ker::net::proto::ndp_queued_packet_count(), ker::net::proto::NDP_CACHE_SIZE);
    KEXPECT_EQ(ker::net::proto::ndp_snapshot(nullptr, 0), ker::net::proto::NDP_CACHE_SIZE);
    ker::net::proto::ndp_forget_device(IDENTITY);
    KEXPECT_EQ(releases, static_cast<uint32_t>(ker::net::proto::NDP_CACHE_SIZE + 1));
    KEXPECT_EQ(ker::net::pkt_pool_free_count(), BASELINE_FREE);
    KEXPECT_EQ(ker::net::netdev_unregister(&dev), 0);
}

KTEST(Ndp, RetirementCancelsPendingPacketReference) {
    static NetDevice dev;  // NOLINT
    prepare_device(dev, {'k', 't', '6', 'n', 'd', 'p', '2'}, 0x36);
    KREQUIRE_EQ(ker::net::netdev_register(&dev), 0);
    NetDeviceIdentity const IDENTITY = ker::net::netdev_registered_identity(&dev);
    ker::net::pkt_pool_init();
    uint32_t releases = 0;
    PacketBuffer* packet = alloc_counted_packet(releases);
    KREQUIRE_NE(packet, nullptr);
    MacAddress resolved{};
    KREQUIRE_EQ(ker::net::proto::ndp_resolve(IDENTITY, PEER, GLOBAL_A, resolved, packet), ker::net::proto::NdpResolveResult::QUEUED);
    ker::net::NetDeviceRetireToken token{};
    KREQUIRE_EQ(ker::net::netdev_unregister_begin(&dev, token), 0);
    constexpr uint32_t RETIRING = uint32_t{1} << 31U;
    KEXPECT_EQ(releases, 1U);
    KEXPECT_EQ(dev.lifetime_readers.load(std::memory_order_acquire), RETIRING);
    ker::net::netdev_unregister_wait(token);
}

KTEST(ICMPv6, MandatoryValidationAndRouterOptions) {
    PacketBuffer packet{};
    constexpr size_t RA_LENGTH = sizeof(ICMPv6Header) + sizeof(ker::net::proto::ICMPv6RouterAdvertisement) + 32;
    init_stack_packet(packet, RA_LENGTH);
    std::memset(packet.data, 0, RA_LENGTH);
    auto* header = reinterpret_cast<ICMPv6Header*>(packet.data);
    header->type = ker::net::proto::ICMPV6_ROUTER_ADVERT;
    size_t const OPTION = sizeof(ICMPv6Header) + sizeof(ker::net::proto::ICMPv6RouterAdvertisement);
    packet.data[OPTION] = ker::net::proto::NDP_OPT_PREFIX_INFORMATION;
    packet.data[OPTION + 1] = 4;
    packet.data[OPTION + 2] = 64;
    packet.data[OPTION + 3] = 0x40;
    packet.data[OPTION + 7] = 20;
    packet.data[OPTION + 11] = 10;
    IPv6RxInfo info{.src = LINK_LOCAL,
                    .dst = ker::net::proto::IPV6_ALL_NODES_MULTICAST,
                    .next_header = ker::net::proto::IPV6_PROTO_ICMPV6,
                    .hop_limit = 255,
                    .upper_layer_length = RA_LENGTH};
    set_icmp_checksum(packet, info);
    KEXPECT_EQ(ker::net::proto::icmpv6_validate(&packet, info), ker::net::proto::ICMPv6ValidationError::NONE);
    info.hop_limit = 254;
    set_icmp_checksum(packet, info);
    KEXPECT_EQ(ker::net::proto::icmpv6_validate(&packet, info), ker::net::proto::ICMPv6ValidationError::BAD_HOP_LIMIT);
    info.hop_limit = 255;
    info.src = GLOBAL_A;
    set_icmp_checksum(packet, info);
    KEXPECT_EQ(ker::net::proto::icmpv6_validate(&packet, info), ker::net::proto::ICMPv6ValidationError::BAD_SOURCE);
    info.src = LINK_LOCAL;
    packet.data[OPTION + 2] = 63;
    set_icmp_checksum(packet, info);
    KEXPECT_EQ(ker::net::proto::icmpv6_validate(&packet, info), ker::net::proto::ICMPv6ValidationError::BAD_OPTIONS);
    packet.data[OPTION + 2] = 64;
    packet.data[OPTION + 1] = 0;
    set_icmp_checksum(packet, info);
    KEXPECT_EQ(ker::net::proto::icmpv6_validate(&packet, info), ker::net::proto::ICMPv6ValidationError::BAD_OPTIONS);

    constexpr size_t SOURCE_LINK_LENGTH = sizeof(ICMPv6Header) + sizeof(ker::net::proto::ICMPv6RouterAdvertisement) + 8;
    init_stack_packet(packet, SOURCE_LINK_LENGTH);
    std::memset(packet.data, 0, SOURCE_LINK_LENGTH);
    header = reinterpret_cast<ICMPv6Header*>(packet.data);
    header->type = ker::net::proto::ICMPV6_ROUTER_ADVERT;
    packet.data[OPTION] = ker::net::proto::NDP_OPT_SRC_LINK_ADDR;
    packet.data[OPTION + 1] = 1;
    info.upper_layer_length = SOURCE_LINK_LENGTH;
    set_icmp_checksum(packet, info);
    KEXPECT_EQ(ker::net::proto::icmpv6_validate(&packet, info), ker::net::proto::ICMPv6ValidationError::BAD_OPTIONS);
    std::memcpy(packet.data + OPTION + sizeof(NdpOptionHeader), PEER_MAC.data(), PEER_MAC.size());
    set_icmp_checksum(packet, info);
    KEXPECT_EQ(ker::net::proto::icmpv6_validate(&packet, info), ker::net::proto::ICMPv6ValidationError::NONE);
    packet.data[OPTION + sizeof(NdpOptionHeader)] |= 1U;
    set_icmp_checksum(packet, info);
    KEXPECT_EQ(ker::net::proto::icmpv6_validate(&packet, info), ker::net::proto::ICMPv6ValidationError::BAD_OPTIONS);
}

KTEST(ICMPv6, MulticastEchoUsesIngressUnicastSource) {
    static NetDevice dev;  // NOLINT
    prepare_device(dev, {'k', 't', '6', 'e', 'c', 'h', 'o'}, 0x3C);
    KREQUIRE_EQ(ker::net::netdev_register(&dev), 0);
    NetDeviceIdentity const IDENTITY = ker::net::netdev_registered_identity(&dev);
    KREQUIRE_EQ(ker::net::netif_set_ipv6(&dev, GLOBAL_A, 64, ker::net::IPV6_ADDR_F_NODAD | ker::net::IPV6_ADDR_F_NOPREFIXROUTE, UINT64_MAX,
                                         UINT64_MAX, false),
                0);
    ker::net::pkt_pool_init();

    // Seed a usable neighbor without transmitting an advertisement: the target
    // is remote, and this unsolicited NA only installs a STALE entry.
    PacketBuffer* advertisement = ker::net::pkt_alloc();
    KREQUIRE_NE(advertisement, nullptr);
    constexpr size_t NA_LENGTH = sizeof(ICMPv6Header) + sizeof(ker::net::proto::NdpNeighborAdvert) + 8;
    auto* na_data = advertisement->put(NA_LENGTH);
    std::memset(na_data, 0, NA_LENGTH);
    reinterpret_cast<ICMPv6Header*>(na_data)->type = ker::net::proto::ICMPV6_NEIGHBOR_ADVERT;
    auto* na = reinterpret_cast<ker::net::proto::NdpNeighborAdvert*>(na_data + sizeof(ICMPv6Header));
    na->flags = ker::net::htonl(ker::net::proto::NDP_NA_FLAG_OVERRIDE);
    na->target = PEER;
    auto* option = reinterpret_cast<NdpOptionHeader*>(na_data + sizeof(ICMPv6Header) + sizeof(ker::net::proto::NdpNeighborAdvert));
    option->type = ker::net::proto::NDP_OPT_TGT_LINK_ADDR;
    option->length = 1;
    std::memcpy(reinterpret_cast<uint8_t*>(option) + sizeof(NdpOptionHeader), PEER_MAC.data(), PEER_MAC.size());
    IPv6RxInfo na_info{
        .src = PEER, .dst = GLOBAL_A, .next_header = ker::net::proto::IPV6_PROTO_ICMPV6, .hop_limit = 255, .upper_layer_length = NA_LENGTH};
    ker::net::proto::ndp_handle_na(&dev, advertisement, na_info);

    PacketBuffer* request = ker::net::pkt_alloc();
    KREQUIRE_NE(request, nullptr);
    constexpr size_t ECHO_LENGTH = sizeof(ICMPv6Header) + sizeof(ker::net::proto::ICMPv6Echo);
    auto* request_data = request->put(ker::net::proto::IPV6_HLEN + ECHO_LENGTH);
    std::array<uint8_t, 384> encoded{};
    make_ipv6_packet(encoded, ECHO_LENGTH, ker::net::proto::IPV6_PROTO_ICMPV6, PEER, ker::net::proto::IPV6_ALL_NODES_MULTICAST);
    std::memcpy(request_data, encoded.data(), ker::net::proto::IPV6_HLEN);
    auto* echo = reinterpret_cast<ICMPv6Header*>(request_data + ker::net::proto::IPV6_HLEN);
    std::memset(echo, 0, ECHO_LENGTH);
    echo->type = ker::net::proto::ICMPV6_ECHO_REQUEST;
    echo->checksum = ker::net::checksum_pseudo_ipv6(PEER, ker::net::proto::IPV6_ALL_NODES_MULTICAST, ker::net::proto::IPV6_PROTO_ICMPV6,
                                                    ECHO_LENGTH, echo, ECHO_LENGTH);

    control_xmit_ipv6_source_is_multicast.store(true, std::memory_order_relaxed);
    control_xmit_ipv6_source_is_global_a.store(false, std::memory_order_relaxed);
    uint32_t const BEFORE_XMIT = control_xmit_count.load(std::memory_order_relaxed);
    ker::net::proto::ipv6_rx(&dev, request);
    KEXPECT_EQ(control_xmit_count.load(std::memory_order_relaxed), BEFORE_XMIT + 1);
    KEXPECT_FALSE(control_xmit_ipv6_source_is_multicast.load(std::memory_order_relaxed));
    KEXPECT_TRUE(control_xmit_ipv6_source_is_global_a.load(std::memory_order_relaxed));
    ker::net::proto::ndp_forget_device(IDENTITY);
    KEXPECT_EQ(ker::net::netdev_unregister(&dev), 0);
}

KTEST(ICMPv6, ErrorQuoteBoundsAndMulticastSuppression) {
    PacketBuffer packet{};
    constexpr size_t ERROR_LENGTH = sizeof(ICMPv6Header) + sizeof(ker::net::proto::ICMPv6ErrorBody) + ker::net::proto::IPV6_HLEN;
    init_stack_packet(packet, ERROR_LENGTH);
    std::memset(packet.data, 0, ERROR_LENGTH);
    auto* header = reinterpret_cast<ICMPv6Header*>(packet.data);
    header->type = ker::net::proto::ICMPV6_PACKET_TOO_BIG;
    IPv6RxInfo info{.src = LINK_LOCAL,
                    .dst = GLOBAL_A,
                    .next_header = ker::net::proto::IPV6_PROTO_ICMPV6,
                    .hop_limit = 64,
                    .upper_layer_length = ERROR_LENGTH};
    set_icmp_checksum(packet, info);
    KEXPECT_EQ(ker::net::proto::icmpv6_validate(&packet, info), ker::net::proto::ICMPv6ValidationError::NONE);
    packet.len = ERROR_LENGTH - 1;
    set_icmp_checksum(packet, info);
    KEXPECT_EQ(ker::net::proto::icmpv6_validate(&packet, info), ker::net::proto::ICMPv6ValidationError::TRUNCATED);

    static NetDevice dev;  // NOLINT
    prepare_device(dev, {'k', 't', '6', 'i', 'c', 'm', 'p'}, 0x37);
    KREQUIRE_EQ(ker::net::netdev_register(&dev), 0);
    ker::net::pkt_pool_init();
    uint32_t releases = 0;
    PacketBuffer* invoking = alloc_counted_packet(releases);
    KREQUIRE_NE(invoking, nullptr);
    static_cast<void>(invoking->put(8));
    IPv6RxInfo invoking_info{.src = GLOBAL_A,
                             .dst = ker::net::proto::IPV6_ALL_NODES_MULTICAST,
                             .next_header = ker::net::proto::IPV6_PROTO_UDP,
                             .hop_limit = 64,
                             .upper_layer_length = 8};
    uint32_t const BEFORE_XMIT = control_xmit_count.load(std::memory_order_relaxed);
    ker::net::proto::icmpv6_send_port_unreachable(&dev, invoking, invoking_info);
    KEXPECT_EQ(releases, 1U);
    KEXPECT_EQ(control_xmit_count.load(std::memory_order_relaxed), BEFORE_XMIT);
    KEXPECT_EQ(ker::net::netdev_unregister(&dev), 0);
}

KTEST(IPv6Input, MulticastTransportNeverReachesResponder) {
    static NetDevice dev;  // NOLINT
    prepare_device(dev, {'k', 't', '6', 'm', 'c', 'a', 's', 't'}, 0x38);
    KREQUIRE_EQ(ker::net::netdev_register(&dev), 0);
    ker::net::pkt_pool_init();
    PacketBuffer* packet = ker::net::pkt_alloc();
    KREQUIRE_NE(packet, nullptr);
    constexpr size_t TCP_LENGTH = 20;
    auto* bytes = packet->put(ker::net::proto::IPV6_HLEN + TCP_LENGTH);
    std::array<uint8_t, 384> encoded{};
    make_ipv6_packet(encoded, TCP_LENGTH, ker::net::proto::IPV6_PROTO_TCP, GLOBAL_A, ker::net::proto::IPV6_ALL_NODES_MULTICAST);
    std::memcpy(bytes, encoded.data(), ker::net::proto::IPV6_HLEN + TCP_LENGTH);
    uint32_t const BEFORE_XMIT = control_xmit_count.load(std::memory_order_relaxed);
    ker::net::proto::ipv6_rx(&dev, packet);
    KEXPECT_EQ(control_xmit_count.load(std::memory_order_relaxed), BEFORE_XMIT);
    KEXPECT_EQ(ker::net::netdev_unregister(&dev), 0);
}
