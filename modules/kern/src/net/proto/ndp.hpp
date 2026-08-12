#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <net/netdevice.hpp>
#include <net/packet.hpp>
#include <net/proto/ipv6.hpp>

namespace ker::net::proto {

struct NdpNeighborSolicit {
    uint32_t reserved{};
    IPv6Address target{};
} __attribute__((packed));

struct NdpNeighborAdvert {
    uint32_t flags{};
    IPv6Address target{};
} __attribute__((packed));

constexpr uint32_t NDP_NA_FLAG_ROUTER = (1U << 31U);
constexpr uint32_t NDP_NA_FLAG_SOLICITED = (1U << 30U);
constexpr uint32_t NDP_NA_FLAG_OVERRIDE = (1U << 29U);

constexpr uint8_t NDP_OPT_SRC_LINK_ADDR = 1;
constexpr uint8_t NDP_OPT_TGT_LINK_ADDR = 2;
constexpr uint8_t NDP_OPT_PREFIX_INFORMATION = 3;
constexpr uint8_t NDP_OPT_MTU = 5;

struct NdpOptionHeader {
    uint8_t type{};
    uint8_t length{};
} __attribute__((packed));

enum class NdpOptionError : uint8_t {
    NONE,
    TRUNCATED,
    ZERO_LENGTH,
    BAD_LINK_ADDRESS_LENGTH,
    DUPLICATE_LINK_ADDRESS,
    INVALID_LINK_ADDRESS,
};

struct NdpParsedOptions {
    bool has_source_link_address{};
    bool has_target_link_address{};
    MacAddress source_link_address{};
    MacAddress target_link_address{};
};

[[nodiscard]] auto ndp_parse_options(const uint8_t* data, size_t length, NdpParsedOptions& out) -> NdpOptionError;

constexpr size_t NDP_CACHE_SIZE = 64;
constexpr size_t NDP_PENDING_PER_NEIGHBOR = 8;
constexpr size_t NDP_PENDING_GLOBAL = 128;
constexpr uint8_t NDP_MAX_SOLICITATIONS = 3;
constexpr uint64_t NDP_RETRANS_TIMER_MS = 1000;
constexpr uint64_t NDP_REACHABLE_TIME_MS = 30000;
constexpr uint64_t NDP_DELAY_FIRST_PROBE_MS = 5000;

struct NdpEntry {
    enum class State : uint8_t { FREE = 0, INCOMPLETE, REACHABLE, STALE, DELAY, PROBE };

    NetDeviceIdentity dev_identity{};
    IPv6Address ip{};
    IPv6Address source{};
    MacAddress mac{};
    State state = State::FREE;
    bool is_router{};
    uint8_t probes_sent{};
    uint64_t last_used_ms{};
    uint64_t reachable_until_ms{};
    uint64_t next_event_ms{};
    std::array<PacketBuffer*, NDP_PENDING_PER_NEIGHBOR> pending{};
    uint8_t pending_count{};
};

struct NdpSnapshot {
    NetDeviceIdentity dev_identity{};
    IPv6Address ip{};
    MacAddress mac{};
    NdpEntry::State state = NdpEntry::State::FREE;
    uint8_t probes_sent{};
    uint8_t pending_count{};
    uint64_t next_event_ms{};
};

enum class NdpResolveResult : uint8_t { RESOLVED, QUEUED, DROPPED };

void ndp_handle_ns(NetDevice* dev, PacketBuffer* pkt, const IPv6RxInfo& info);
void ndp_handle_na(NetDevice* dev, PacketBuffer* pkt, const IPv6RxInfo& info);

// QUEUED and DROPPED consume pkt exactly once. RESOLVED leaves pkt with the
// caller and fills dst_mac.
auto ndp_resolve(NetDeviceIdentity dev, const IPv6Address& ip, const IPv6Address& source, MacAddress& dst_mac, PacketBuffer* pkt)
    -> NdpResolveResult;

void ndp_send_dad_probe(NetDeviceIdentity dev, const IPv6Address& target);
void ndp_timer_tick(uint64_t now_ms);
void ndp_forget_device(NetDeviceIdentity identity);
auto ndp_snapshot(NdpSnapshot* out, size_t capacity) -> size_t;
auto ndp_queued_packet_count() -> size_t;
void ndp_init();
void ndp_timer_thread_start();

}  // namespace ker::net::proto
