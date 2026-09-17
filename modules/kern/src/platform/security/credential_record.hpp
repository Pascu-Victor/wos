#pragma once

#include <platform/security/credentials.hpp>

namespace ker::mod::security::credential_record {

// Canonical little-endian bytes, with no compiler padding or native bool ABI:
// version:u16, length:u16, flags:u8, group_count:u8, reserved:u16,
// user:{real,effective,saved}:u32, group:{real,effective,saved}:u32,
// groups[8]:u32 (unused slots zero), ceiling:u64, service_privileges:u64.
inline constexpr uint16_t VERSION = 1;
inline constexpr size_t SIZE = 80;
inline constexpr uint8_t NO_NEW_PRIVILEGES = 1;
inline constexpr uint8_t DUMPABLE = 2;
inline constexpr uint8_t SERVICE_LOCKED = 4;
using Record = std::array<uint8_t, SIZE>;
static_assert(MAX_GROUPS == 8);

namespace detail {
constexpr void put(Record& bytes, size_t offset, uint64_t value, size_t width) {
    for (size_t i = 0; i < width; ++i) bytes[offset + i] = static_cast<uint8_t>(value >> (i * 8));
}
constexpr auto get(std::span<const uint8_t> bytes, size_t offset, size_t width) -> uint64_t {
    uint64_t value = 0;
    for (size_t i = 0; i < width; ++i) value |= uint64_t{bytes[offset + i]} << (i * 8);
    return value;
}
}  // namespace detail

[[nodiscard]] constexpr auto encode(const Snapshot& snapshot) -> Record {
    Record bytes{};
    detail::put(bytes, 0, VERSION, 2);
    detail::put(bytes, 2, SIZE, 2);
    bytes[4] = (snapshot.no_new_privileges() ? NO_NEW_PRIVILEGES : 0) | (snapshot.dumpable() ? DUMPABLE : 0) |
               (snapshot.service_privileges_locked() ? SERVICE_LOCKED : 0);
    bytes[5] = static_cast<uint8_t>(snapshot.groups().size());
    auto const USER = snapshot.user();
    auto const GROUP = snapshot.group();
    auto const IDS = std::array{USER.real, USER.effective, USER.saved, GROUP.real, GROUP.effective, GROUP.saved};
    for (size_t i = 0; i < IDS.size(); ++i) detail::put(bytes, 8 + i * 4, IDS[i], 4);
    auto const GROUPS = snapshot.groups();
    for (size_t i = 0; i < GROUPS.size(); ++i) detail::put(bytes, 32 + i * 4, GROUPS[i], 4);
    detail::put(bytes, 64, snapshot.authority_ceiling(), 8);
    detail::put(bytes, 72, snapshot.service_privileges(), 8);
    return bytes;
}

// Decode only. The result remains untrusted until peer identity mapping admits
// it. Validate the entire record before assigning out, including unused slots.
[[nodiscard]] constexpr auto decode(std::span<const uint8_t> bytes, Snapshot& out) -> int {
    if (bytes.size() != SIZE) return -EINVAL;
    if (detail::get(bytes, 0, 2) != VERSION) return -ENOTSUP;
    if (detail::get(bytes, 2, 2) != SIZE || (bytes[4] & ~(NO_NEW_PRIVILEGES | DUMPABLE | SERVICE_LOCKED)) != 0 || bytes[5] > MAX_GROUPS ||
        bytes[6] != 0 || bytes[7] != 0)
        return -EINVAL;
    std::array<uint32_t, 6> ids{};
    for (size_t i = 0; i < ids.size(); ++i) ids[i] = static_cast<uint32_t>(detail::get(bytes, 8 + i * 4, 4));
    std::array<uint32_t, MAX_GROUPS> groups{};
    for (size_t i = 0; i < groups.size(); ++i) {
        groups[i] = static_cast<uint32_t>(detail::get(bytes, 32 + i * 4, 4));
        if (i >= bytes[5] && groups[i] != 0) return -EINVAL;
    }
    return Snapshot::restore_fields({.user = {ids[0], ids[1], ids[2]},
                                     .group = {ids[3], ids[4], ids[5]},
                                     .groups = {groups.data(), bytes[5]},
                                     .authority_ceiling = detail::get(bytes, 64, 8),
                                     .service_privileges = detail::get(bytes, 72, 8),
                                     .no_new_privileges = (bytes[4] & NO_NEW_PRIVILEGES) != 0,
                                     .dumpable = (bytes[4] & DUMPABLE) != 0,
                                     .service_privileges_locked = (bytes[4] & SERVICE_LOCKED) != 0},
                                    out);
}

}  // namespace ker::mod::security::credential_record
