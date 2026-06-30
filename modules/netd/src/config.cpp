#include "netd/config.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>

namespace netd {

auto find_ifname_for_driver(const char* driver, const char* fallback) -> const char* {
    static std::array<char, 16> s_ifname{};

    FILE* f = fopen("/etc/netdevs", "r");
    if (f == nullptr) {
        return fallback;
    }

    std::array<char, 128> line{};
    while (fgets(line.data(), static_cast<int>(line.size()), f) != nullptr) {
        char const* p = line.data();
        while (*p == ' ' || *p == '\t') {
            p++;
        }
        if (*p == '#' || *p == '\n' || *p == '\r' || *p == '\0') {
            continue;
        }

        std::array<char, 16> tok_ifname{};
        std::array<char, 32> tok_driver{};
        if (sscanf(p, "%15s %31s", tok_ifname.data(), tok_driver.data()) != 2) {
            continue;
        }

        if (std::strcmp(tok_driver.data(), driver) == 0) {
            fclose(f);
            std::ranges::fill(s_ifname, '\0');
            size_t const LEN = std::min(std::strlen(tok_ifname.data()), s_ifname.size() - 1);
            std::copy_n(tok_ifname.data(), LEN, s_ifname.data());
            return s_ifname.data();
        }
    }
    fclose(f);
    return fallback;
}

}  // namespace netd
