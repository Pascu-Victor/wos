#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace ker::dev::firmware {
inline constexpr size_t MAX_SOURCE_BYTES = 512ULL * 1024 * 1024;
inline constexpr size_t MAX_BLOB_BYTES = 256ULL * 1024 * 1024;
inline constexpr size_t MAX_RECORDS = 4096;
inline constexpr size_t MAX_ARCHIVE_NAME_BYTES = 512;
inline constexpr size_t MAX_NAME_BYTES = 256;
inline constexpr size_t MAX_INDEX_ENTRIES = 512;
inline constexpr size_t MAX_TRAILER_PADDING = 511;
inline constexpr size_t CACHE_SLOTS = 64;
inline constexpr uint32_t MAX_REFERENCES = 1024;
inline constexpr size_t SCOPE_LEASES = 64;
using Digest = std::array<uint8_t, 32>;
enum class Error : uint8_t {
    none,
    unavailable,
    ambiguous_source,
    unsafe_source,
    source_limit,
    truncated,
    magic,
    header,
    path,
    duplicate,
    entry_type,
    trailer,
    limit,
    not_found,
    policy,
    length,
    digest,
    revision,
    policy_conflict,
    cache_full,
    reference_limit,
    stale,
    generation_exhausted,
    context,
    scope_full
};
struct ArchiveEntry {
    std::string_view name{};
    std::span<const uint8_t> bytes{};
};
struct ArchiveResult {
    Error error = Error::none;
    size_t offset = 0;
    size_t count = 0;
    [[nodiscard]] auto valid() const -> bool { return error == Error::none; }
};
// Kernel-trusted policy, never accepted from an untrusted userspace descriptor
// or a manifest that accompanies the untrusted payload being checked.
struct Policy {
    std::string_view name{};
    uint64_t length = 0;
    uint64_t maximum = 0;
    Digest sha256{};
    uint32_t revision = 0;
};
struct Handle {
    uint16_t slot = UINT16_MAX;
    uint64_t generation = 0;
    auto operator==(const Handle&) const -> bool = default;
};
struct CacheResult {
    Error error = Error::none;
    Handle handle{};
    std::span<const uint8_t> bytes{};
    [[nodiscard]] auto valid() const -> bool { return error == Error::none; }
};
struct Stats {
    uint64_t objects = 0;
    uint64_t references = 0;
    uint64_t verifications = 0;
    uint64_t hits = 0;
};
}  // namespace ker::dev::firmware
