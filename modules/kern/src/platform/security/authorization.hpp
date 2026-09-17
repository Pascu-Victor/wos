#pragma once

#include <platform/security/credentials.hpp>

namespace ker::mod::security {

enum class RemoteIdentityTrust : uint8_t { DENY, SAME_DOMAIN, SAME_DOMAIN_ROOT };

// Identity mapping for nodes explicitly provisioned into a shared numeric-ID
// namespace. Trust in nonroot identities does not include any root ID slot or
// supplementary group. A successful mapping preserves the entire snapshot.
[[nodiscard]] constexpr auto map_remote_identity(const Snapshot& supplied, RemoteIdentityTrust trust, Snapshot& mapped) -> int {
    if (trust != RemoteIdentityTrust::SAME_DOMAIN && trust != RemoteIdentityTrust::SAME_DOMAIN_ROOT) return -EACCES;
    if (trust == RemoteIdentityTrust::SAME_DOMAIN) {
        // Numeric nonroot identity trust cannot import active service authority.
        // Inert grants under an irreversibly dropped ceiling remain harmless.
        if (supplied.effective_service_privileges() != 0) return -EACCES;
        auto const USER = supplied.user();
        auto const GROUP = supplied.group();
        if (USER.real == 0 || USER.effective == 0 || USER.saved == 0 || GROUP.real == 0 || GROUP.effective == 0 || GROUP.saved == 0)
            return -EACCES;
        for (auto group : supplied.groups()) {
            if (group == 0) return -EACCES;
        }
    }
    mapped = supplied;
    return 0;
}

// Model the exact credential reconstructed by the legacy six-ID WKI record.
// This is decoding, not peer authorization. The receiver must separately admit
// the authenticated request. Errors leave out unchanged.
[[nodiscard]] constexpr auto restore_legacy_remote_exec_identity(Ids user, Ids group, Snapshot& out) -> int {
    if (user.real == UNCHANGED_ID || user.effective == UNCHANGED_ID || user.saved == UNCHANGED_ID || group.real == UNCHANGED_ID ||
        group.effective == UNCHANGED_ID || group.saved == UNCHANGED_ID)
        return -EINVAL;
    auto restored = Snapshot::bootstrap_root();
    int error =
        restored.prepare({.change = Change::SET_RESGID, .real = group.real, .effective = group.effective, .saved = group.saved}, restored);
    if (error != 0) return error;
    error = restored.prepare({.change = Change::SET_RESUID, .real = user.real, .effective = user.effective, .saved = user.saved}, restored);
    if (error != 0) return error;
    out = restored;
    return 0;
}

// Admit only snapshots that round-trip without losing groups, dumpability,
// irreversible restrictions, or sealed service privileges. Optional placement
// stays local; strict placement fails until a negotiated format preserves them.
[[nodiscard]] constexpr auto authorize_legacy_remote_exec(const Snapshot& caller) -> int {
    Snapshot restored;
    return restore_legacy_remote_exec_identity(caller.user(), caller.group(), restored) == 0 && restored == caller ? 0 : -ENOTSUP;
}

// Mount, unmount and root-namespace changes share one administration domain.
[[nodiscard]] constexpr auto authorize_mount_administration(const Snapshot& caller) -> int {
    return caller.allows(Authority::MOUNT) ? 0 : -EPERM;
}

// Global network configuration and raw traffic creation share this domain.
[[nodiscard]] constexpr auto authorize_network_administration(const Snapshot& caller) -> int {
    return caller.allows(Authority::NETWORK_ADMIN) || caller.allows_service(ServicePrivilege::NETWORK_ADMIN) ? 0 : -EPERM;
}

[[nodiscard]] constexpr auto authorize_hostname_change(const Snapshot& caller) -> int {
    return caller.allows(Authority::HOSTNAME) ? 0 : -EPERM;
}

// The init role restricts who may install/select service resource policy; it
// never replaces the credential authority that the process may have discarded.
[[nodiscard]] constexpr auto authorize_service_resource_management(const Snapshot& caller, bool original_init) -> int {
    return original_init && caller.allows(Authority::INIT_CONTROL) && caller.allows(Authority::RESOURCE_LIMIT) ? 0 : -EPERM;
}

[[nodiscard]] constexpr auto authorize_journal_read(const Snapshot& caller) -> int {
    return caller.allows(Authority::KERNEL_DEBUG) || caller.allows_service(ServicePrivilege::JOURNAL_READ) ? 0 : -EACCES;
}

enum class DiagnosticAccess : uint8_t { DENY, PUBLIC_READ, KERNEL_READ, KERNEL_CONTROL, WKI_CONTROL };

[[nodiscard]] constexpr auto authorize_diagnostic(const Snapshot& caller, DiagnosticAccess access, bool write) -> int {
    switch (access) {
        case DiagnosticAccess::PUBLIC_READ:
            return write ? -EPERM : 0;
        case DiagnosticAccess::KERNEL_READ:
            if (write) return -EPERM;
            [[fallthrough]];
        case DiagnosticAccess::KERNEL_CONTROL:
            return caller.allows(Authority::KERNEL_DEBUG) ? 0 : -EACCES;
        case DiagnosticAccess::WKI_CONTROL:
            return caller.allows(Authority::WKI_ADMIN) ? 0 : -EACCES;
        default:
            return -EACCES;
    }
}

struct IpcOwnership {
    uint32_t uid{};
    uint32_t gid{};
    uint32_t creator_uid{};
    uint32_t creator_gid{};
    uint32_t mode{};
};

[[nodiscard]] constexpr auto may_access_ipc(const Snapshot& caller, const IpcOwnership& object, uint32_t required) -> bool {
    if ((required & ~6U) != 0) {
        return false;
    }
    if (caller.allows(Authority::DAC_OVERRIDE)) {
        return true;
    }
    auto const UID = caller.user().effective;
    uint32_t shift = 0;
    if (UID == object.uid || UID == object.creator_uid) {
        shift = 6;
    } else if (caller.has_group(object.gid) || caller.has_group(object.creator_gid)) {
        shift = 3;
    }
    return ((object.mode >> shift) & required) == required;
}

[[nodiscard]] constexpr auto may_control_ipc(const Snapshot& caller, const IpcOwnership& object) -> bool {
    auto const UID = caller.user().effective;
    return caller.allows(Authority::FILE_OWNER) || UID == object.uid || UID == object.creator_uid;
}

// Callers retain these values for the whole operation. No task registry,
// scheduler, allocation or credentials publication belongs in policy code.
[[nodiscard]] constexpr auto may_signal(const Snapshot& caller, const Snapshot& target, bool same_session, bool is_continue) -> bool {
    if (caller.allows(Authority::SIGNAL) || (is_continue && same_session)) {
        return true;
    }
    auto const USER = caller.user();
    auto const TARGET = target.user();
    return USER.real == TARGET.real || USER.real == TARGET.saved || USER.effective == TARGET.real || USER.effective == TARGET.saved;
}

[[nodiscard]] constexpr auto clamp_priority(int requested) -> int { return requested < -20 ? -20 : (requested > 19 ? 19 : requested); }

// The caller serializes this check and scheduler commit against other process
// priority changes. Lower nice values represent higher scheduling priority.
[[nodiscard]] constexpr auto authorize_priority(const Snapshot& caller, const Snapshot& target, int current, int requested) -> int {
    if (caller.allows(Authority::PRIORITY)) {
        return 0;
    }
    auto const UID = caller.user().effective;
    auto const TARGET = target.user();
    if (UID != TARGET.real && UID != TARGET.effective) {
        return -EPERM;
    }
    return clamp_priority(requested) < current ? -EACCES : 0;
}

[[nodiscard]] constexpr auto may_trace(const Snapshot& caller, const Snapshot& target) -> bool {
    // Debugging controls the target's execution. It must not provide a route
    // around an irreversible authority ceiling or no-new-privileges contract.
    if ((target.authority_ceiling() & ~caller.authority_ceiling()) != 0 || (caller.no_new_privileges() && !target.no_new_privileges()) ||
        (target.effective_service_privileges() & ~caller.effective_service_privileges()) != 0 ||
        (caller.service_privileges_locked() && !target.service_privileges_locked())) {
        return false;
    }
    if (caller.allows(Authority::PTRACE)) {
        return true;
    }
    auto const USER = caller.user().effective;
    auto const GROUP = caller.group().effective;
    auto const TARGET_USER = target.user();
    auto const TARGET_GROUP = target.group();
    if (!target.dumpable() || USER != TARGET_USER.real || USER != TARGET_USER.effective || USER != TARGET_USER.saved ||
        GROUP != TARGET_GROUP.real || GROUP != TARGET_GROUP.effective || GROUP != TARGET_GROUP.saved) {
        return false;
    }
    for (uint32_t gid : target.groups()) {
        if (!caller.has_group(gid)) {
            return false;
        }
    }
    return true;
}

// Descriptor transfer and private process links expose the target's authority.
// Same-process access does not depend on dumpability; cross-process access uses
// the same complete identity and irreversible-authority rules as tracing.
[[nodiscard]] constexpr auto may_access_process_private(const Snapshot& caller, const Snapshot& target, bool same_process) -> bool {
    return same_process || may_trace(caller, target);
}

}  // namespace ker::mod::security
