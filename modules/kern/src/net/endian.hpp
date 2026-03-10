#pragma once

#include <cstdint>

namespace ker::net {

inline auto htons(uint16_t host) -> uint16_t { return __builtin_bswap16(host); }

inline auto ntohs(uint16_t net) -> uint16_t { return __builtin_bswap16(net); }

inline auto htonl(uint32_t host) -> uint32_t { return __builtin_bswap32(host); }

inline auto ntohl(uint32_t net) -> uint32_t { return __builtin_bswap32(net); }

// 64-bit byte-swap helpers (host <-> big-endian)
inline auto htonll(uint64_t host) -> uint64_t { return __builtin_bswap64(host); }
inline auto ntohll(uint64_t net) -> uint64_t { return __builtin_bswap64(net); }

// Convenience aliases matching Linux XFS naming convention
inline auto be16_to_cpu(uint16_t be) -> uint16_t { return __builtin_bswap16(be); }
inline auto be32_to_cpu(uint32_t be) -> uint32_t { return __builtin_bswap32(be); }
inline auto be64_to_cpu(uint64_t be) -> uint64_t { return __builtin_bswap64(be); }
inline auto cpu_to_be16(uint16_t cpu) -> uint16_t { return __builtin_bswap16(cpu); }
inline auto cpu_to_be32(uint32_t cpu) -> uint32_t { return __builtin_bswap32(cpu); }
inline auto cpu_to_be64(uint64_t cpu) -> uint64_t { return __builtin_bswap64(cpu); }

}  // namespace ker::net

// ============================================================================
// Type-safe big-endian wrapper types for on-disk format definitions.
//
// These mirror the Linux __be16 / __be32 / __be64 types used in XFS on-disk
// structures.  The wrapper types make endian conversions explicit — you must
// call .to_cpu() or the static ::from_cpu() to convert, which prevents
// accidentally interpreting raw big-endian bytes as native values.
//
// Defined in the global namespace so XFS format headers can use them without
// namespace qualification (matching the Linux convention).
// ============================================================================

struct __be16 {
    uint16_t raw;  // stored in big-endian byte order

    __be16() = default;
    constexpr explicit __be16(uint16_t be_val) : raw(be_val) {}

    // Convert from big-endian to host (CPU) byte order
    [[nodiscard]] auto to_cpu() const -> uint16_t { return __builtin_bswap16(raw); }

    // Create from host byte order
    static auto from_cpu(uint16_t host) -> __be16 { return __be16(__builtin_bswap16(host)); }

    bool operator==(const __be16& o) const { return raw == o.raw; }
    bool operator!=(const __be16& o) const { return raw != o.raw; }
} __attribute__((packed));

struct __be32 {
    uint32_t raw;  // stored in big-endian byte order

    __be32() = default;
    constexpr explicit __be32(uint32_t be_val) : raw(be_val) {}

    [[nodiscard]] auto to_cpu() const -> uint32_t { return __builtin_bswap32(raw); }
    static auto from_cpu(uint32_t host) -> __be32 { return __be32(__builtin_bswap32(host)); }

    bool operator==(const __be32& o) const { return raw == o.raw; }
    bool operator!=(const __be32& o) const { return raw != o.raw; }
} __attribute__((packed));

struct __be64 {
    uint64_t raw;  // stored in big-endian byte order

    __be64() = default;
    constexpr explicit __be64(uint64_t be_val) : raw(be_val) {}

    [[nodiscard]] auto to_cpu() const -> uint64_t { return __builtin_bswap64(raw); }
    static auto from_cpu(uint64_t host) -> __be64 { return __be64(__builtin_bswap64(host)); }

    bool operator==(const __be64& o) const { return raw == o.raw; }
    bool operator!=(const __be64& o) const { return raw != o.raw; }
} __attribute__((packed));
