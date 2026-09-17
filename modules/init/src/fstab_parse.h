#pragma once

#include <sys/types.h>

#include <array>
#include <cerrno>
#include <cstddef>

namespace wos::init {
// Consume the whole field even on overflow so later columns cannot inherit its
// suffix. Zero means absent, one means complete, and errors leave output intact.
template <size_t Size>
auto parse_fstab_field(const char*& cursor, std::array<char, Size>& output) -> int {
    static_assert(Size > 0);
    std::array<char, Size> parsed{};
    size_t length = 0;
    bool overflow = false;
    while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t') {
        if (length < Size - 1)
            parsed.at(length++) = *cursor;
        else
            overflow = true;
        ++cursor;
    }
    if (overflow) return -E2BIG;
    output = parsed;
    return length != 0 ? 1 : 0;
}

// Read to EOF before any table entry is acted on. A full buffer needs an extra
// EOF check; accepting its prefix could silently discard a mount policy token.
template <size_t Size, typename Read>
auto read_complete_fstab(std::array<char, Size>& output, Read&& read) -> ssize_t {
    static_assert(Size > 1);
    size_t used = 0;
    while (used < Size - 1) {
        ssize_t const COUNT = read(output.data() + used, Size - 1 - used);
        if (COUNT < 0) return COUNT;
        if (COUNT == 0) {
            output.at(used) = '\0';
            return static_cast<ssize_t>(used);
        }
        for (ssize_t i = 0; i < COUNT; ++i) {
            if (output.at(used + static_cast<size_t>(i)) == '\0') return -EINVAL;
        }
        used += static_cast<size_t>(COUNT);
    }
    char extra{};
    ssize_t const COUNT = read(&extra, 1);
    if (COUNT < 0) return COUNT;
    if (COUNT != 0) return -E2BIG;
    output.at(used) = '\0';
    return static_cast<ssize_t>(used);
}
}  // namespace wos::init
