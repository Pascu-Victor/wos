#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <utility>

namespace ker::vfs {

// Metadata may select a candidate, but only reads from the admitted source
// can validate its bytes. The caller owns the candidate and scratch lifetimes
// and supplies uncached positional I/O without holding a cache lock.
template <typename Reader, typename Continue>
auto content_matches(std::span<const uint8_t> expected, std::span<uint8_t> scratch, Reader&& read, Continue&& keep_going) -> bool {
    if (expected.empty() || scratch.empty()) return false;
    size_t offset = 0;
    while (offset < expected.size()) {
        if (!keep_going()) return false;
        size_t const CHUNK = std::min(scratch.size(), expected.size() - offset);
        auto const COUNT = read(scratch.data(), CHUNK, offset);
        if (COUNT <= 0 || std::cmp_greater(COUNT, CHUNK)) return false;
        auto const SIZE = static_cast<size_t>(COUNT);
        if (std::memcmp(scratch.data(), expected.data() + offset, SIZE) != 0) return false;
        offset += SIZE;
    }
    // Equal prefixes do not establish equal files, even if cached stat says
    // their sizes match. Require EOF from the same retained source.
    if (!keep_going() || read(scratch.data(), 1, offset) != 0) return false;
    return keep_going();
}

}  // namespace ker::vfs
