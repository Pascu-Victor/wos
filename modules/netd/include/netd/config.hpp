#pragma once

#include <array>
#include <cstddef>
#include <span>

namespace netd {

constexpr size_t MAX_CONFIGURED_INTERFACES = 16;
constexpr size_t INTERFACE_NAME_CAPACITY = 16;

enum class InterfacePolicy {
    DHCP,
    LINKLOCAL,
    WKI,
    UNMANAGED,
};

struct InterfaceConfig {
    std::array<char, INTERFACE_NAME_CAPACITY> ifname{};
    InterfacePolicy policy = InterfacePolicy::UNMANAGED;
};

auto load_interface_configs(std::span<InterfaceConfig> out) -> size_t;
auto interface_policy_name(InterfacePolicy policy) -> const char*;

auto find_ifname_for_driver(const char* driver, const char* fallback) -> const char*;

}  // namespace netd
