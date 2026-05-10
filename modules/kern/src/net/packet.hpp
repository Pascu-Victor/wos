#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

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

struct PacketBuffer {
    std::array<uint8_t, PKT_BUF_SIZE> storage{};
    uint8_t* data{};                   // current data pointer
    size_t len{};                      // current data length
    PacketBuffer* next{};              // freelist / queue linkage
    NetDevice* dev{};                  // source/dest device
    uint16_t protocol{};               // EtherType (host byte order)
    std::array<uint8_t, 6> src_mac{};  // incoming source MAC (for reply use)
    bool debug_in_use = false;
    uint16_t debug_alloc_cpu = 0;
    uint16_t debug_free_cpu = 0;
    uint32_t debug_alloc_seq = 0;
    uintptr_t debug_alloc_site = 0;
    uintptr_t debug_free_site = 0;

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
void pkt_pool_ensure_free(size_t min_free);
auto pkt_alloc() -> PacketBuffer*;
auto pkt_alloc_tx() -> PacketBuffer*;  // TX-only: fails if pool is low (reserves for RX)
void pkt_free(PacketBuffer* pkt);

}  // namespace ker::net
