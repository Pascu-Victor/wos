#pragma once

#include <cstdint>
#include <limits>
#include <string_view>

namespace ker::mod::mm::phys {

struct PhysicalBalanceComponents {
    uint64_t managed_pages;
    uint64_t free_pages;
    uint64_t allocator_metadata_pages;
    uint64_t zone_descriptor_pages;
    uint64_t owner_pages;
};

[[nodiscard]] constexpr auto physical_balance_saturating_add(uint64_t left, uint64_t right) -> uint64_t {
    return right > std::numeric_limits<uint64_t>::max() - left ? std::numeric_limits<uint64_t>::max() : left + right;
}

[[nodiscard]] constexpr auto physical_balance_identity_pages(const PhysicalBalanceComponents& components) -> uint64_t {
    uint64_t identity = physical_balance_saturating_add(components.free_pages, components.allocator_metadata_pages);
    identity = physical_balance_saturating_add(identity, components.zone_descriptor_pages);
    return physical_balance_saturating_add(identity, components.owner_pages);
}

[[nodiscard]] constexpr auto physical_balance_mismatch_pages(const PhysicalBalanceComponents& components) -> uint64_t {
    uint64_t const IDENTITY = physical_balance_identity_pages(components);
    return IDENTITY > components.managed_pages ? IDENTITY - components.managed_pages : components.managed_pages - IDENTITY;
}

[[nodiscard]] constexpr auto physical_balance_category_name_is_forbidden(std::string_view name) -> bool {
    return name.find("unaccounted") != std::string_view::npos || name.find("unknown") != std::string_view::npos ||
           name.find("other") != std::string_view::npos || name.find("estimated") != std::string_view::npos ||
           name.find("residual") != std::string_view::npos || name.find("miscellaneous") != std::string_view::npos;
}

[[nodiscard]] constexpr auto physical_balance_descriptor_is_complete(const char* name, const char* lifetime, const char* scaling_bound)
    -> bool {
    if (name == nullptr || lifetime == nullptr || scaling_bound == nullptr || *name == '\0' || *lifetime == '\0' ||
        *scaling_bound == '\0') {
        return false;
    }
    return !physical_balance_category_name_is_forbidden(name);
}

}  // namespace ker::mod::mm::phys
