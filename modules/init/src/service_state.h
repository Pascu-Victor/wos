#pragma once

#include <array>
#include <cerrno>
#include <cstdint>
#include <span>

namespace wos::init {
struct PrivateStateInode {
    int fd{-1};
    uint32_t uid{}, gid{}, mode{};
    uint64_t links{};
    bool directory{};
};

// Callers exclusively own startup: retain every inode, validate the complete
// flat namespace, and start no services until this transaction succeeds.
inline auto validate_private_state_owners(std::span<const PrivateStateInode> entries, uint32_t uid, uint32_t gid) -> int {
    if (uid == 0 || uid == UINT32_MAX || gid == 0 || gid == UINT32_MAX || entries.empty() || entries.size() > 3) return EINVAL;
    for (size_t i = 0; i < entries.size(); ++i) {
        auto const& entry = entries[i];
        if (entry.fd < 0 || entry.directory != (i == entries.size() - 1) ||
            !((entry.uid == 0 && entry.gid == 0) || (entry.uid == uid && entry.gid == gid)) ||
            entry.mode != (entry.directory ? 0040700U : 0100600U) || (!entry.directory && entry.links != 1))
            return EPERM;
    }
    return 0;
}

template <typename ChangeOwner>
auto change_private_state_owners(std::span<const PrivateStateInode> entries, uint32_t uid, uint32_t gid, ChangeOwner change) -> int {
    int const ADMISSION = validate_private_state_owners(entries, uid, gid);
    if (ADMISSION != 0) return ADMISSION;
    std::array<bool, 3> changed{};
    for (size_t i = 0; i < entries.size(); ++i) {
        auto const& entry = entries[i];
        if (entry.uid == uid && entry.gid == gid) continue;
        int const ERROR = change(entry.fd, uid, gid);
        if (ERROR != 0) {
            bool restored = true;
            for (size_t previous = i; previous > 0; --previous) {
                auto const& old = entries[previous - 1];
                if (changed[previous - 1] && change(old.fd, old.uid, old.gid) != 0) restored = false;
            }
            return restored ? ERROR : EIO;
        }
        changed[i] = true;
    }
    return 0;
}

auto prepare_journal_state_at(int directory, uint32_t uid, uint32_t gid) -> int;
auto prepare_private_journal_state(uint32_t uid, uint32_t gid) -> int;
auto prepare_keygen_state_at(int directory, uint32_t uid, uint32_t gid) -> int;
auto prepare_private_keygen_state(uint32_t uid, uint32_t gid) -> int;
// Caller retains an exclusively controlled parent directory. Production validates
// root-owned, non-writable ancestors and the bounded root-owned seed first.
auto prepare_netd_state_at(int directory, uint32_t uid, uint32_t gid, std::span<const char> seed) -> int;
auto prepare_netd_runtime_state(uint32_t uid, uint32_t gid) -> int;
}  // namespace wos::init
