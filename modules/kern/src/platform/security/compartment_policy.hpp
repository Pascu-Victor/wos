#pragma once

#include <abi/resource.hpp>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <platform/security/compartment_network.hpp>
#include <platform/security/credentials.hpp>

namespace ker::mod::security::compartment {

// Private preparation values, not syscall layouts or namespace authority.
// A live installer must retain/revalidate its creator's observations, own the
// namespace transaction, and enforce this policy in every affected consumer.
using Limits = std::array<ker::abi::resource::Limit, ker::abi::resource::COUNT>;

struct Profile {
    Snapshot credentials{};
    Limits limits{};
    Network network{Network::DENIED};
};

struct Request {
    uint64_t authority_ceiling{};
    uint64_t service_privileges{};
    // Explicit values for every inherited slot; default infinity is rejected
    // against a finite parent. There is no privileged limit-raising exception.
    Limits limits{};
    Network network{Network::DENIED};
    bool dumpable{};
};

// Pure candidate construction. IDs/groups are immutable here; use existing
// checked credential changes before launch if a service needs another UID.
// Success enables no namespace or access. Failure preserves output and parent.
[[nodiscard]] constexpr auto prepare(const Profile& parent, const Request& request, uint64_t fd_capacity, Profile& out) -> int {
    if ((request.authority_ceiling & ~ALL_AUTHORITY) != 0 ||
        (request.service_privileges & ~ker::abi::process::SERVICE_PRIVILEGE_ALL) != 0 ||
        (request.network != Network::DENIED && request.network != Network::SHARED_HOST) ||
        (parent.network != Network::DENIED && parent.network != Network::SHARED_HOST) || fd_capacity == 0)
        return -EINVAL;
    if ((request.authority_ceiling & ~parent.credentials.authority_ceiling()) != 0 ||
        (request.service_privileges & ~parent.credentials.effective_service_privileges()) != 0 ||
        (request.dumpable && !parent.credentials.dumpable()) ||
        (request.network == Network::SHARED_HOST && parent.network != Network::SHARED_HOST))
        return -EPERM;
    for (size_t i = 0; i < request.limits.size(); ++i) {
        auto const PARENT = parent.limits[i];
        auto const LIMIT = request.limits[i];
        if (PARENT.soft > PARENT.hard || LIMIT.soft > LIMIT.hard) return -EINVAL;
        if (LIMIT.soft > PARENT.soft || LIMIT.hard > PARENT.hard) return -EPERM;
    }
    if (request.limits[static_cast<unsigned>(ker::abi::resource::Id::NOFILE)].hard > fd_capacity) return -EINVAL;
    Profile candidate{};
    int const RESULT = Snapshot::restore_fields({.user = parent.credentials.user(),
                                                 .group = parent.credentials.group(),
                                                 .groups = parent.credentials.groups(),
                                                 .authority_ceiling = request.authority_ceiling,
                                                 .service_privileges = request.service_privileges,
                                                 .no_new_privileges = true,
                                                 .dumpable = request.dumpable,
                                                 .service_privileges_locked = true},
                                                candidate.credentials);
    if (RESULT != 0) return RESULT;
    candidate.limits = request.limits;
    candidate.network = request.network;
    out = candidate;
    return 0;
}

}  // namespace ker::mod::security::compartment
