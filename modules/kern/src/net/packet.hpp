#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>

namespace ker::net {

// Forward declaration
struct NetDevice;
constexpr size_t PKT_BUF_SIZE = 10240;  // supports jumbo frames (9000 MTU + headers)
constexpr size_t PKT_HEADROOM = 128;    // room for VirtIO + Ethernet + headroom
// Minimum pool size (used before NIC count is known)
constexpr size_t PKT_POOL_MIN_SIZE = 1024;
// Buffers to allocate per NIC (RX ring + TX ring + headroom)
constexpr size_t PKT_POOL_PER_NIC = 1024;
// Runtime expansion granularity. Rounded to keep slab counts close to ring sizes.
constexpr size_t PKT_POOL_GROW_CHUNK = 256;
// Emergency free-buffer floor preserved by RX refill so TX/control traffic can
// still allocate packets under receive bursts.
constexpr size_t PKT_POOL_TX_RESERVE = 256;
// RX refill should stop above the TX reserve. This leaves one growth chunk for
// bulk TX before it has to grow the pool again, while pkt_alloc() can still use
// the hard reserve for bounded control/liveness frames.
constexpr size_t PKT_POOL_RX_REFILL_RESERVE = PKT_POOL_TX_RESERVE + PKT_POOL_GROW_CHUNK;

struct PacketPoolSnapshot {
    size_t capacity = 0;
    size_t free = 0;
    size_t used = 0;
    size_t rx_reserve = 0;
    size_t grow_chunk = 0;
    size_t buffer_size = 0;
    size_t object_size = 0;
    size_t headroom = 0;
    size_t baseline_capacity = 0;
    size_t active_capacity = 0;
    size_t draining_buffers = 0;
    size_t draining_free = 0;
    uint32_t tx_refused = 0;
    bool expand_in_progress = false;
};

struct PacketPoolReclaimStats {
    size_t before_capacity = 0;
    size_t after_capacity = 0;
    size_t before_free = 0;
    size_t after_free = 0;
    size_t before_draining_buffers = 0;
    size_t after_draining_buffers = 0;
    size_t before_draining_free = 0;
    size_t after_draining_free = 0;
    size_t freed_chunks = 0;
    size_t freed_buffers = 0;
    size_t marked_draining_chunks = 0;
    size_t marked_draining_buffers = 0;
    size_t deactivated_free_buffers = 0;
};

struct PacketBuffer {
    std::array<uint8_t, PKT_BUF_SIZE> storage{};
    uint8_t* data{};       // current data pointer
    size_t len{};          // current data length
    PacketBuffer* next{};  // freelist / queue linkage
    NetDevice* dev{};      // source/dest device
    void* lifetime_ctx{};  // optional owner released when pkt_free() consumes the buffer
    void (*lifetime_release)(void*) = nullptr;
    uint16_t protocol{};        // EtherType (host byte order)
    proto::MacAddress src_mac;  // incoming source MAC (for reply use)
#ifdef WOS_NET_PACKET_DEBUG
    bool debug_in_use = false;
    uint16_t debug_alloc_cpu = 0;
    uint16_t debug_free_cpu = 0;
    uint32_t debug_alloc_seq = 0;
    uintptr_t debug_alloc_site = 0;
    uintptr_t debug_free_site = 0;
#endif

    // Prepend: move data pointer back by n bytes, increase length
    auto push(size_t n) -> uint8_t* {
        data -= n;
        len += n;
        return data;
    }

    // Strip: advance data pointer by n bytes, decrease length
    auto pull(size_t n) -> uint8_t* {
        uint8_t* old = data;
        data += n;
        len -= n;
        return old;
    }

    // Append: extend data region by n bytes at the tail
    auto put(size_t n) -> uint8_t* {
        uint8_t* tail = data + len;
        len += n;
        return tail;
    }

    [[nodiscard]] auto headroom() const -> size_t { return static_cast<size_t>(data - storage.data()); }
    [[nodiscard]] auto tailroom() const -> size_t { return PKT_BUF_SIZE - headroom() - len; }
};

void pkt_pool_init();
void pkt_pool_expand_for_nics();       // Call after NIC drivers have registered
auto pkt_pool_size() -> size_t;        // Get current pool size
auto pkt_pool_free_count() -> size_t;  // Approximate free buffers available
auto pkt_pool_snapshot() -> PacketPoolSnapshot;
auto pkt_pool_reclaim_free(size_t target_capacity) -> PacketPoolReclaimStats;
void pkt_pool_ensure_free(size_t min_free);
auto pkt_alloc() -> PacketBuffer*;
auto pkt_alloc_tx() -> PacketBuffer*;  // TX-only: fails if pool is low (reserves for RX)
void pkt_free(PacketBuffer* pkt);

}  // namespace ker::net
