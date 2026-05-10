#pragma once

#include <cstdint>

namespace ker::net {

constexpr auto htons(uint16_t host) -> uint16_t { return __builtin_bswap16(host); }

constexpr auto ntohs(uint16_t net) -> uint16_t { return __builtin_bswap16(net); }

constexpr auto htonl(uint32_t host) -> uint32_t { return __builtin_bswap32(host); }

constexpr auto ntohl(uint32_t net) -> uint32_t { return __builtin_bswap32(net); }

// 64-bit byte-swap helpers (host <-> big-endian)
constexpr auto htonll(uint64_t host) -> uint64_t { return __builtin_bswap64(host); }
constexpr auto ntohll(uint64_t net) -> uint64_t { return __builtin_bswap64(net); }

// Convenience aliases matching Linux XFS naming convention
constexpr auto be16_to_cpu(uint16_t be) -> uint16_t { return __builtin_bswap16(be); }
constexpr auto be32_to_cpu(uint32_t be) -> uint32_t { return __builtin_bswap32(be); }
constexpr auto be64_to_cpu(uint64_t be) -> uint64_t { return __builtin_bswap64(be); }
constexpr auto cpu_to_be16(uint16_t cpu) -> uint16_t { return __builtin_bswap16(cpu); }
constexpr auto cpu_to_be32(uint32_t cpu) -> uint32_t { return __builtin_bswap32(cpu); }
constexpr auto cpu_to_be64(uint64_t cpu) -> uint64_t { return __builtin_bswap64(cpu); }

}  // namespace ker::net

// ============================================================================
// Type-safe big-endian wrapper types for on-disk format definitions.
//
// These mirror the Linux Be16 / Be32 / Be64 types used in XFS on-disk
// structures.  The wrapper types make endian conversions explicit - you must
// call .to_cpu() or the static ::from_cpu() to convert, which prevents
// accidentally interpreting raw big-endian bytes as native values.
//
// Defined in the global namespace so XFS format headers can use them without
// namespace qualification (matching the Linux convention).
// ============================================================================

struct Be16 {
    uint16_t raw;  // stored in big-endian byte order

    Be16() = default;
    constexpr explicit Be16(uint16_t be_val) : raw(be_val) {}

    // Convert from big-endian to host (CPU) byte order
    [[nodiscard]] constexpr auto to_cpu() const -> uint16_t { return __builtin_bswap16(raw); }

    // Create from host byte order
    static constexpr auto from_cpu(uint16_t host) -> Be16 { return Be16(__builtin_bswap16(host)); }

    constexpr auto operator==(const Be16& o) const -> bool { return raw == o.raw; }
    constexpr auto operator!=(const Be16& o) const -> bool { return raw != o.raw; }
} __attribute__((packed));

struct Be32 {
    uint32_t raw;  // stored in big-endian byte order

    Be32() = default;
    constexpr explicit Be32(uint32_t be_val) : raw(be_val) {}

    [[nodiscard]] constexpr auto to_cpu() const -> uint32_t { return __builtin_bswap32(raw); }
    static constexpr auto from_cpu(uint32_t host) -> Be32 { return Be32(__builtin_bswap32(host)); }

    constexpr auto operator==(const Be32& o) const -> bool { return raw == o.raw; }
    constexpr auto operator!=(const Be32& o) const -> bool { return raw != o.raw; }
} __attribute__((packed));

struct Be64 {
    uint64_t raw;  // stored in big-endian byte order

    Be64() = default;
    constexpr explicit Be64(uint64_t be_val) : raw(be_val) {}

    [[nodiscard]] constexpr auto to_cpu() const -> uint64_t { return __builtin_bswap64(raw); }
    static constexpr auto from_cpu(uint64_t host) -> Be64 { return Be64(__builtin_bswap64(host)); }

    constexpr auto operator==(const Be64& o) const -> bool { return raw == o.raw; }
    constexpr auto operator!=(const Be64& o) const -> bool { return raw != o.raw; }
} __attribute__((packed));
