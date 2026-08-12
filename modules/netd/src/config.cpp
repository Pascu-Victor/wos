#include "netd/config.hpp"

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>

namespace netd {

namespace {

auto parse_policy(const char* text, InterfacePolicy& out) -> bool {
    if (std::strcmp(text, "dhcp") == 0) {
        out = InterfacePolicy::DHCP;
        return true;
    }
    if (std::strcmp(text, "linklocal") == 0) {
        out = InterfacePolicy::LINKLOCAL;
        return true;
    }
    if (std::strcmp(text, "wki") == 0) {
        out = InterfacePolicy::WKI;
        return true;
    }
    if (std::strcmp(text, "unmanaged") == 0) {
        out = InterfacePolicy::UNMANAGED;
        return true;
    }
    return false;
}

}  // namespace

auto load_interface_configs(std::span<InterfaceConfig> out) -> size_t {
    if (out.empty()) {
        return 0;
    }

    FILE* file = std::fopen("/etc/netdevs", "r");
    if (file == nullptr) {
        return 0;
    }

    size_t count = 0;
    std::array<char, 128> line{};
    while (count < out.size() && std::fgets(line.data(), static_cast<int>(line.size()), file) != nullptr) {
        const char* cursor = line.data();
        while (*cursor == ' ' || *cursor == '\t') {
            ++cursor;
        }
        if (*cursor == '#' || *cursor == '\n' || *cursor == '\r' || *cursor == '\0') {
            continue;
        }

        std::array<char, INTERFACE_NAME_CAPACITY> ifname{};
        std::array<char, 32> driver{};
        std::array<char, 2> trailing{};
        if (std::sscanf(cursor, "%15s %31s %1s", ifname.data(), driver.data(), trailing.data()) != 2) {
            continue;
        }
        InterfacePolicy policy{};
        if (!parse_policy(driver.data(), policy)) {
            continue;
        }

        bool duplicate = false;
        for (size_t i = 0; i < count; ++i) {
            if (std::strcmp(out[i].ifname.data(), ifname.data()) == 0) {
                duplicate = true;
                break;
            }
        }
        if (duplicate) {
            continue;
        }
        out[count].ifname = ifname;
        out[count].policy = policy;
        ++count;
    }
    std::fclose(file);
    return count;
}

auto interface_policy_name(InterfacePolicy policy) -> const char* {
    switch (policy) {
        case InterfacePolicy::DHCP:
            return "dhcp";
        case InterfacePolicy::LINKLOCAL:
            return "linklocal";
        case InterfacePolicy::WKI:
            return "wki";
        case InterfacePolicy::UNMANAGED:
            return "unmanaged";
    }
    return "unknown";
}

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
