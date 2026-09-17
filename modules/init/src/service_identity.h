#pragma once

#include <cerrno>

#include "service_manifest.h"

namespace wos::init {
constexpr size_t MAX_SERVICE_ACCOUNTS = 32;
constexpr size_t MAX_ACCOUNT_FILE_BYTES = 16 * 1024;
struct ServiceUser {
    FixedString<MAX_SERVICE_NAME_BYTES> name;
    uint32_t uid{}, gid{};
};
struct ServiceGroup {
    FixedString<MAX_SERVICE_NAME_BYTES> name;
    uint32_t gid{};
};
struct ServiceAccounts {
    std::array<ServiceUser, MAX_SERVICE_ACCOUNTS> users{};
    std::array<ServiceGroup, MAX_SERVICE_ACCOUNTS> groups{};
    size_t user_count{}, group_count{};
};
struct ResolvedServiceIdentity {
    uint32_t uid{}, gid{};
    std::array<uint32_t, MAX_SERVICE_GROUPS> groups{};
    size_t group_count{};
    uint16_t mask{};
    uint64_t authority_ceiling{};
    bool no_new_privileges{};
    uint64_t service_privileges{};
    bool valid{};
};
auto parse_service_accounts(std::string_view passwd, std::string_view group, ServiceAccounts& out) -> bool;
auto resolve_service_identity(const ServiceIdentityPolicy& policy, const ServiceAccounts& accounts, ResolvedServiceIdentity& out) -> bool;

// All fallible setup precedes exec, and no stage follows a failed restriction.
// Operations return positive errno values and affect only the forked child.
template <typename Operations>
auto apply_service_identity(const ResolvedServiceIdentity& identity, Operations& ops) -> int {
    if (!identity.valid || identity.group_count > MAX_SERVICE_GROUPS || identity.mask > 0777 ||
        !valid_service_authority(identity.uid, identity.no_new_privileges, identity.authority_ceiling, identity.service_privileges))
        return EINVAL;
    int error = 0;
    if (identity.no_new_privileges && (error = ops.no_new_privileges()) != 0) return error;
    if (identity.no_new_privileges && (error = ops.service_privileges(identity.service_privileges)) != 0) return error;
    if ((error = ops.groups(std::span(identity.groups.data(), identity.group_count))) != 0) return error;
    if ((error = ops.gid(identity.gid)) != 0) return error;
    if ((error = ops.uid(identity.uid)) != 0) return error;
    if ((error = ops.mask(identity.mask)) != 0) return error;
    if ((error = ops.ceiling(identity.authority_ceiling)) != 0) return error;
    return ops.verify(identity);
}
}  // namespace wos::init
