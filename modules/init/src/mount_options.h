#pragma once

#include <sys/mount.h>

#include <array>
#include <cerrno>
#include <string_view>

namespace wos::init {
struct MountOptions {
    unsigned long flags{};
    std::array<char, 256> data{};
};

// Generic access/execution policy belongs in mount flags, not backend data.
// Preserve every other token and apply ro/rw and suid/nosuid in input order.
// Failure does not publish a partial policy or truncated backend option.
inline auto parse_mount_options(std::string_view input, MountOptions& output) -> int {
    MountOptions parsed;
    if (input.size() >= parsed.data.size()) return -E2BIG;
    size_t used = 0;
    while (!input.empty()) {
        size_t const END = input.find(',');
        auto token = input.substr(0, END);
        if (token == "nosuid")
            parsed.flags |= MS_NOSUID;
        else if (token == "suid")
            parsed.flags &= ~static_cast<unsigned long>(MS_NOSUID);
        else if (token == "ro")
            parsed.flags |= MS_RDONLY;
        else if (token == "rw")
            parsed.flags &= ~static_cast<unsigned long>(MS_RDONLY);
        else if (!token.empty()) {
            if (used) parsed.data.at(used++) = ',';
            for (char value : token) parsed.data.at(used++) = value;
        }
        if (END == std::string_view::npos) break;
        input.remove_prefix(END + 1);
    }
    output = parsed;
    return 0;
}
}  // namespace wos::init
