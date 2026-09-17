#pragma once

#include <cerrno>
#include <cstdint>

namespace ker::mod::security {

// Node-local allocation identity. The low byte preserves the public PTY
// number; the generation prevents an old binding from naming a reused slot.
// Zero is detached. Issuers must stop at MAX_GENERATION rather than wrap.
struct TerminalIdentity {
    static constexpr uint64_t MAX_GENERATION = UINT64_MAX >> 8;
    uint64_t value = 0;

    [[nodiscard]] static constexpr auto create(uint64_t generation, unsigned index) -> TerminalIdentity {
        return generation != 0 && generation <= MAX_GENERATION && index < 256 ? TerminalIdentity{(generation << 8) | index}
                                                                              : TerminalIdentity{};
    }
    [[nodiscard]] constexpr auto index() const -> int { return value != 0 ? static_cast<int>(value & 255) : -1; }
    auto operator==(const TerminalIdentity&) const -> bool = default;
};

// A terminal's kernel-generated signals carry its allocation authority, not
// the credentials of whoever writes the master or a bare process-group ID.
[[nodiscard]] constexpr auto may_receive_terminal_signal(TerminalIdentity origin, TerminalIdentity target) -> bool {
    return origin.value != 0 && origin == target;
}

[[nodiscard]] constexpr auto may_receive_terminal_signal(TerminalIdentity origin, uint64_t origin_generation, TerminalIdentity target,
                                                         uint64_t target_generation) -> bool {
    return origin_generation != 0 && origin_generation == target_generation && may_receive_terminal_signal(origin, target);
}

[[nodiscard]] constexpr auto authorize_terminal_claim(uint64_t pid, uint64_t session, TerminalIdentity current, TerminalIdentity requested,
                                                      uint64_t owner_session) -> int {
    if (pid == 0 || session != pid || requested.value == 0 || (current.value != 0 && current != requested) ||
        (owner_session != 0 && owner_session != session)) {
        return -EPERM;
    }
    return 0;
}

[[nodiscard]] constexpr auto authorize_terminal_control(uint64_t session, TerminalIdentity current, TerminalIdentity requested,
                                                        uint64_t owner_session, uint64_t current_generation, uint64_t owner_generation)
    -> int {
    return session != 0 && session == owner_session && may_receive_terminal_signal(requested, owner_generation, current, current_generation)
               ? 0
               : -ENOTTY;
}

struct ProcessGroupSubject {
    uint64_t pid;
    uint64_t session;
    uint64_t parent;
    bool exec_completed;
    bool local_session = true;
};

[[nodiscard]] constexpr auto authorize_process_group_change(uint64_t caller_pid, uint64_t caller_session, ProcessGroupSubject target)
    -> int {
    if ((target.local_session && target.pid == target.session) || target.session != caller_session ||
        (target.pid != caller_pid && target.parent != caller_pid)) {
        return -EPERM;
    }
    if (target.pid != caller_pid && target.exec_completed) {
        return -EACCES;
    }
    return 0;
}

}  // namespace ker::mod::security
