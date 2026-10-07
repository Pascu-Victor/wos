#pragma once

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <span>

#include "file_operations.hpp"

namespace ker::vfs::owner_observation {

// Native size is already signed Stat ABI data. No File position is consulted
// or changed, and every error preserves the caller's candidate position.
constexpr auto seek_end(int64_t size, int64_t offset, int64_t& output) -> int {
    if (size < 0) return -EIO;
    if (offset > 0 && size > INT64_MAX - offset) return -EOVERFLOW;
    int64_t const POSITION = size + offset;
    if (POSITION < 0) return -EINVAL;
    output = POSITION;
    return 0;
}

constexpr auto cookie_advances(uint64_t previous, uint64_t next) -> bool { return next > previous; }
// Preserve native callback contracts: XFS -ENOENT is a backend error, while
// TMPFS uses it as its end marker. Only XFS uses the legacy -1 end marker.
constexpr auto directory_eof(int status, bool tmpfs) -> bool { return tmpfs ? status == -ENOENT : status == -1; }

inline auto directory_entry_valid(const DirEntry& entry, uint64_t previous) -> bool {
    if (!cookie_advances(previous, entry.d_off) || entry.d_reclen < DIRENT_MIN_RECLEN || entry.d_reclen > sizeof(entry) ||
        entry.d_reclen % DIRENT_RECORD_ALIGNMENT != 0)
        return false;
    size_t const RECORD_NAME_BYTES = entry.d_reclen - DIRENT_HEADER_SIZE;
    size_t const NAME_BYTES = RECORD_NAME_BYTES < entry.d_name.size() ? RECORD_NAME_BYTES : entry.d_name.size();
    return std::memchr(entry.d_name.data(), '\0', NAME_BYTES) != nullptr;
}

inline auto directory_end_cookie(std::span<const uint8_t> bytes, uint16_t count, uint64_t start, uint64_t& output) -> bool {
    if (bytes.size() != static_cast<size_t>(count) * sizeof(DirEntry)) return false;
    uint64_t cookie = start;
    for (uint16_t i = 0; i < count; ++i) {
        DirEntry entry{};
        std::memcpy(&entry, bytes.data() + static_cast<size_t>(i) * sizeof(entry), sizeof(entry));
        if (!directory_entry_valid(entry, cookie)) return false;
        cookie = entry.d_off;
    }
    output = cookie;
    return true;
}

}  // namespace ker::vfs::owner_observation
