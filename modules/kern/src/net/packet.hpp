#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::net {

// Forward declaration
struct NetDevice;

constexpr size_t PKT_BUF_SIZE = 2048;
constexpr size_t PKT_HEADROOM = 64;
constexpr size_t PKT_POOL_SIZE = 512;

struct PacketBuffer {
    uint8_t storage[PKT_BUF_SIZE];
    uint8_t* data;       // current data pointer
    size_t len;          // current data length
    PacketBuffer* next;  // freelist / queue linkage
    NetDevice* dev;      // source/dest device
    uint16_t protocol;   // EtherType (host byte order)
    uint8_t src_mac[6];  // incoming source MAC (for reply use)

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

    auto headroom() const -> size_t { return static_cast<size_t>(data - storage); }
    auto tailroom() const -> size_t { return PKT_BUF_SIZE - headroom() - len; }
};

void pkt_pool_init();
auto pkt_alloc() -> PacketBuffer*;
void pkt_free(PacketBuffer* pkt);

}  // namespace ker::net
