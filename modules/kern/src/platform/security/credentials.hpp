#pragma once

#include <abi/callnums/process.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <span>

namespace ker::mod::security {

inline constexpr size_t MAX_GROUPS = 8;
inline constexpr uint32_t UNCHANGED_ID = UINT32_MAX;

// Bounded root-authority restrictions shared with the userspace prctl ABI.
using Authority = ker::abi::process::CredentialAuthority;
using ServicePrivilege = ker::abi::process::ServicePrivilege;
inline constexpr uint64_t ALL_AUTHORITY = ker::abi::process::CREDENTIAL_AUTHORITY_ALL;
static_assert(static_cast<unsigned>(Authority::COUNT) == 16);

struct Ids {
    uint32_t real{};
    uint32_t effective{};
    uint32_t saved{};
    constexpr auto operator==(const Ids&) const -> bool = default;
};

enum class Change : uint8_t {
    SET_UID,
    SET_GID,
    SET_EUID,
    SET_EGID,
    SET_RESUID,
    SET_RESGID,
    SET_REUID,
    SET_REGID,
    SET_GROUPS,
    NO_NEW_PRIVILEGES,
    DROP_AUTHORITY,
    SET_DUMPABLE,
    SET_SERVICE_PRIVILEGES,
};

struct Request {
    Change change{};
    uint32_t real{UNCHANGED_ID};
    uint32_t effective{UNCHANGED_ID};
    uint32_t saved{UNCHANGED_ID};
    std::span<const uint32_t> groups{};
    uint64_t authority_ceiling{};
    bool dumpable{};
    uint64_t service_privileges{};
};

struct ExecIdentity {
    uint32_t uid{};
    uint32_t gid{};
    bool set_uid{};
    bool set_gid{};
    // Caller derives these from the retained executable mount and trace state.
    bool nosuid{};
    bool traced{};
    uint32_t mode{0444};
};

// A decoded value is not authority. Remote callers must separately map it
// through authenticated peer policy before installing or using the snapshot.
struct SnapshotFields {
    Ids user{65534, 65534, 65534};
    Ids group{65534, 65534, 65534};
    std::span<const uint32_t> groups{};
    uint64_t authority_ceiling{};
    uint64_t service_privileges{};
    bool no_new_privileges{};
    bool dumpable{};
    bool service_privileges_locked{};
};

class Snapshot {
   public:
    // Default construction must never create an ambient root credential.
    constexpr Snapshot() = default;
    [[nodiscard]] static constexpr auto bootstrap_root() -> Snapshot {
        Snapshot result;
        result.m_user = {};
        result.m_group = {};
        result.m_authority_ceiling = ALL_AUTHORITY;
        result.m_dumpable = true;
        return result;
    }

    // Restore one complete, validated value. Unlike prepare(), an error leaves
    // the caller's output unchanged. This never publishes process credentials.
    [[nodiscard]] static constexpr auto restore_fields(const SnapshotFields& fields, Snapshot& out) -> int {
        auto const IDS = std::array{fields.user.real,  fields.user.effective,  fields.user.saved,
                                    fields.group.real, fields.group.effective, fields.group.saved};
        for (auto id : IDS) {
            if (id == UNCHANGED_ID) return -EINVAL;
        }
        if (fields.groups.size() > MAX_GROUPS || (fields.authority_ceiling & ~ALL_AUTHORITY) != 0 ||
            (fields.service_privileges & ~ker::abi::process::SERVICE_PRIVILEGE_ALL) != 0 ||
            (fields.service_privileges_locked && !fields.no_new_privileges) ||
            (fields.service_privileges != 0 && !fields.service_privileges_locked))
            return -EINVAL;
        Snapshot restored;
        for (size_t i = 0; i < fields.groups.size(); ++i) {
            if (fields.groups[i] == UNCHANGED_ID) return -EINVAL;
            restored.m_groups[i] = fields.groups[i];
        }
        restored.m_user = fields.user;
        restored.m_group = fields.group;
        restored.m_group_count = static_cast<uint8_t>(fields.groups.size());
        restored.m_authority_ceiling = fields.authority_ceiling;
        restored.m_service_privileges = static_cast<uint8_t>(fields.service_privileges);
        restored.m_no_new_privileges = fields.no_new_privileges;
        restored.m_dumpable = fields.dumpable;
        restored.m_service_privileges_locked = fields.service_privileges_locked;
        out = restored;
        return 0;
    }

    [[nodiscard]] constexpr auto user() const -> Ids { return m_user; }
    [[nodiscard]] constexpr auto group() const -> Ids { return m_group; }
    // Decision-only identity for access()/faccessat() without AT_EACCESS.
    // Never publish this value as process credentials. All restrictions and
    // supplementary groups remain those of the one admitted caller snapshot.
    [[nodiscard]] constexpr auto for_real_id_access() const -> Snapshot {
        Snapshot result = *this;
        result.m_user.effective = m_user.real;
        result.m_group.effective = m_group.real;
        return result;
    }
    [[nodiscard]] constexpr auto groups() const -> std::span<const uint32_t> { return {m_groups.data(), m_group_count}; }
    [[nodiscard]] constexpr auto no_new_privileges() const -> bool { return m_no_new_privileges; }
    [[nodiscard]] constexpr auto dumpable() const -> bool { return m_dumpable; }
    [[nodiscard]] constexpr auto authority_ceiling() const -> uint64_t { return m_authority_ceiling; }
    [[nodiscard]] constexpr auto service_privileges() const -> uint64_t { return m_service_privileges; }
    [[nodiscard]] constexpr auto service_privileges_locked() const -> bool { return m_service_privileges_locked; }
    [[nodiscard]] constexpr auto allows_service(ServicePrivilege privilege) const -> bool {
        auto const BIT = static_cast<unsigned>(privilege);
        if (BIT >= static_cast<unsigned>(ServicePrivilege::COUNT)) return false;
        auto const MASK = uint64_t{1} << BIT;
        auto const CEILING = ker::abi::process::service_privilege_ceiling(MASK);
        return m_no_new_privileges && m_service_privileges_locked && (m_service_privileges & MASK) != 0 &&
               (m_authority_ceiling & CEILING) == CEILING;
    }
    // Include root-equivalent permissions when comparing execution authority.
    [[nodiscard]] constexpr auto effective_service_privileges() const -> uint64_t {
        return ((allows(Authority::KERNEL_DEBUG) || allows_service(ServicePrivilege::JOURNAL_READ)) ? 1U : 0U) |
               ((allows(Authority::NETWORK_ADMIN) || allows_service(ServicePrivilege::NETWORK_ADMIN)) ? 2U : 0U);
    }
    [[nodiscard]] constexpr auto allows(Authority authority) const -> bool {
        auto const BIT = static_cast<unsigned>(authority);
        return BIT < static_cast<unsigned>(Authority::COUNT) && m_user.effective == 0 && (m_authority_ceiling & (uint64_t{1} << BIT)) != 0;
    }
    [[nodiscard]] constexpr auto has_group(uint32_t gid, bool real = false) const -> bool {
        if ((real ? m_group.real : m_group.effective) == gid) return true;
        for (uint32_t const GROUP : groups()) {
            if (GROUP == gid) return true;
        }
        return false;
    }
    constexpr auto operator==(const Snapshot&) const -> bool = default;

    // Pure prepare phase. On every error out equals the original snapshot;
    // callers must reserve dependent resources before publishing success.
    [[nodiscard]] constexpr auto prepare(const Request& request, Snapshot& out) const -> int {
        Snapshot next = *this;
        int error = 0;
        switch (request.change) {
            case Change::SET_UID:
            case Change::SET_EUID:
            case Change::SET_RESUID:
            case Change::SET_REUID:
                error = change_ids(next.m_user, request, allows(Authority::SET_UID));
                break;
            case Change::SET_GID:
            case Change::SET_EGID:
            case Change::SET_RESGID:
            case Change::SET_REGID:
                error = change_ids(next.m_group, request, allows(Authority::SET_GID));
                break;
            case Change::SET_GROUPS:
                if (!allows(Authority::SET_GID)) {
                    error = -EPERM;
                } else if (request.groups.size() > MAX_GROUPS) {
                    error = -EINVAL;
                } else {
                    next.m_groups = {};
                    next.m_group_count = static_cast<uint8_t>(request.groups.size());
                    for (size_t i = 0; i < request.groups.size(); ++i) {
                        if (request.groups[i] == UNCHANGED_ID) {
                            error = -EINVAL;
                            break;
                        }
                        next.m_groups.at(i) = request.groups[i];
                    }
                }
                break;
            case Change::NO_NEW_PRIVILEGES:
                next.m_no_new_privileges = true;
                break;
            case Change::DROP_AUTHORITY:
                if ((request.authority_ceiling & ~m_authority_ceiling) != 0) {
                    error = -EPERM;
                } else {
                    next.m_authority_ceiling = request.authority_ceiling;
                }
                break;
            case Change::SET_DUMPABLE:
                next.m_dumpable = request.dumpable;
                break;
            case Change::SET_SERVICE_PRIVILEGES: {
                auto const MASK = request.service_privileges;
                auto const REQUIRED = ker::abi::process::service_privilege_ceiling(MASK);
                if ((MASK & ~ker::abi::process::SERVICE_PRIVILEGE_ALL) != 0) {
                    error = -EINVAL;
                } else if (!m_no_new_privileges || (m_service_privileges_locked && (MASK & ~m_service_privileges) != 0) ||
                           (!m_service_privileges_locked && MASK != 0 &&
                            (!allows(Authority::INIT_CONTROL) || (m_authority_ceiling & REQUIRED) != REQUIRED))) {
                    error = -EPERM;
                } else {
                    next.m_service_privileges = static_cast<uint8_t>(MASK);
                    next.m_service_privileges_locked = true;
                    if (!m_service_privileges_locked || MASK != m_service_privileges) next.m_dumpable = false;
                }
                break;
            }
            default:
                error = -EINVAL;
                break;
        }
        if (next.m_user.effective != m_user.effective || next.m_group.effective != m_group.effective) {
            next.m_dumpable = false;
        }
        out = error == 0 ? next : *this;
        return error;
    }

    [[nodiscard]] constexpr auto prepare_exec(const ExecIdentity& image, Snapshot& out) const -> int {
        Snapshot next = *this;
        if (!m_no_new_privileges && !image.nosuid && !image.traced) {
            if ((image.set_uid && image.uid == UNCHANGED_ID) || (image.set_gid && image.gid == UNCHANGED_ID)) {
                out = *this;
                return -EINVAL;
            }
            if (image.set_uid) next.m_user.effective = image.uid;
            if (image.set_gid) next.m_group.effective = image.gid;
        }
        // POSIX exec saves the resulting effective IDs even without set-ID bits.
        next.m_user.saved = next.m_user.effective;
        next.m_group.saved = next.m_group.effective;
        unsigned const SHIFT = m_user.effective == image.uid ? 6 : (has_group(image.gid) ? 3 : 0);
        bool const READABLE = allows(Authority::DAC_OVERRIDE) || ((image.mode >> SHIFT) & 4U) != 0;
        next.m_dumpable = READABLE && next.m_user.effective == m_user.effective && next.m_group.effective == m_group.effective;
        out = next;
        return 0;
    }

   private:
    Ids m_user{65534, 65534, 65534};
    Ids m_group{65534, 65534, 65534};
    std::array<uint32_t, MAX_GROUPS> m_groups{};
    uint64_t m_authority_ceiling{};
    uint8_t m_group_count{};
    bool m_no_new_privileges{};
    bool m_dumpable{};
    uint8_t m_service_privileges{};
    bool m_service_privileges_locked{};

    static constexpr auto matches(uint32_t id, const Ids& old) -> bool { return id == old.real || id == old.effective || id == old.saved; }

    static constexpr auto change_ids(Ids& ids, const Request& request, bool privileged) -> int {
        auto const OLD = ids;
        switch (request.change) {
            case Change::SET_UID:
            case Change::SET_GID:
                if (request.real == UNCHANGED_ID) return -EINVAL;
                if (privileged) {
                    ids = {request.real, request.real, request.real};
                } else if (request.real == OLD.real || request.real == OLD.saved) {
                    ids.effective = request.real;
                } else {
                    return -EPERM;
                }
                return 0;
            case Change::SET_EUID:
            case Change::SET_EGID:
                if (request.effective == UNCHANGED_ID) return -EINVAL;
                if (!privileged && !matches(request.effective, OLD)) return -EPERM;
                ids.effective = request.effective;
                return 0;
            case Change::SET_RESUID:
            case Change::SET_RESGID:
                if (!privileged && ((request.real != UNCHANGED_ID && !matches(request.real, OLD)) ||
                                    (request.effective != UNCHANGED_ID && !matches(request.effective, OLD)) ||
                                    (request.saved != UNCHANGED_ID && !matches(request.saved, OLD)))) {
                    return -EPERM;
                }
                if (request.real != UNCHANGED_ID) ids.real = request.real;
                if (request.effective != UNCHANGED_ID) ids.effective = request.effective;
                if (request.saved != UNCHANGED_ID) ids.saved = request.saved;
                return 0;
            case Change::SET_REUID:
            case Change::SET_REGID:
                if (!privileged && ((request.real != UNCHANGED_ID && request.real != OLD.real && request.real != OLD.effective) ||
                                    (request.effective != UNCHANGED_ID && !matches(request.effective, OLD)))) {
                    return -EPERM;
                }
                if (request.real != UNCHANGED_ID) ids.real = request.real;
                if (request.effective != UNCHANGED_ID) ids.effective = request.effective;
                if (request.real != UNCHANGED_ID || (request.effective != UNCHANGED_ID && request.effective != OLD.real)) {
                    ids.saved = ids.effective;
                }
                return 0;
            default:
                return -EINVAL;
        }
    }
};

}  // namespace ker::mod::security
