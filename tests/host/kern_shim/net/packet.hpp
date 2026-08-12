#pragma once
// Host shim for net/packet.hpp — minimal stub for PacketBuffer.

#include <cstddef>
#include <cstdint>

namespace ker::net {

constexpr size_t PKT_BUF_SIZE = 10240;
constexpr size_t PKT_HEADROOM = 128;

struct NetDevice;

struct PacketBuffer {
    uint8_t* data = nullptr;
    size_t len = 0;
    size_t capacity = 0;
    NetDevice* dev = nullptr;
    PacketBuffer* next = nullptr;
};

}  // namespace ker::net
