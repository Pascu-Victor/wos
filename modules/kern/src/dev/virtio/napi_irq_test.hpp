#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

namespace ker::dev::virtio::napi_test {

constexpr size_t FRAME_BYTES = 64;
constexpr std::array<uint8_t, 8> MAGIC{'W', 'O', 'S', 'I', 'R', 'Q', '4', '9'};
constexpr uint64_t RECOVERY_LIMIT_US = 150000;

constexpr auto packet_byte(size_t index, uint64_t cookie, bool reply) -> uint8_t {
    constexpr std::array<uint8_t, 6> SOURCE{2, 0, 0, 0x49, 0, 1};
    if (index < 6) return 0xff;
    if (index < 12) return SOURCE.at(index - 6);
    if (index == 12) return 0x88;
    if (index == 13) return 0xb5;
    if (index < 22) return MAGIC.at(index - 14);
    if (index == 22) return reply ? 2 : 1;
    if (index == 23) return 0;
    if (index < 32) return static_cast<uint8_t>(cookie >> ((index - 24) * 8));
    return static_cast<uint8_t>(index * 17 + cookie);
}

constexpr auto has_magic(std::span<const uint8_t> frame) -> bool {
    if (frame.size() < 22 || frame[12] != 0x88 || frame[13] != 0xb5) return false;
    for (size_t i = 0; i < MAGIC.size(); ++i)
        if (frame[14 + i] != MAGIC.at(i)) return false;
    return true;
}

struct Frame {
    uint64_t cookie{};
    bool reply{};
};

constexpr auto inspect(std::span<const uint8_t> frame) -> std::optional<Frame> {
    if (frame.size() != FRAME_BYTES || !has_magic(frame) || (frame[22] != 1 && frame[22] != 2)) return std::nullopt;
    uint64_t cookie = 0;
    for (size_t i = 0; i < 8; ++i) cookie |= uint64_t{frame[24 + i]} << (i * 8);
    if (cookie == 0) return std::nullopt;
    bool const REPLY = frame[22] == 2;
    for (size_t i = 0; i < frame.size(); ++i)
        if (frame[i] != packet_byte(i, cookie, REPLY)) return std::nullopt;
    return Frame{cookie, REPLY};
}

}  // namespace ker::dev::virtio::napi_test
