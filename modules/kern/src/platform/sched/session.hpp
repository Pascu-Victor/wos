#pragma once

#include <cstdint>
#include <optional>
#include <platform/security/terminal.hpp>

namespace ker::dev::pty {
struct PtyPair;
}
namespace ker::mod::sched::task {
struct Task;
}
namespace ker::mod::sched::session {

struct Origin {
    uint16_t node = 0;  // zero/zero means this kernel's namespace
    uint32_t boot = 0;
    [[nodiscard]] constexpr auto local() const -> bool { return node == 0 && boot == 0; }
    constexpr auto operator==(const Origin&) const -> bool = default;
};

struct Identity {
    uint64_t session_id = 0;
    uint64_t pgid = 0;
    security::TerminalIdentity terminal{};
    uint64_t terminal_generation = 0;
    bool exec_completed = false;
    Origin origin{};
    Origin group_origin{};
};

// Durable public names, carried by group/session members after leader exit.
// Host names equal physical IDs. A private zero is unknown, never a host alias.
struct Names {
    uint64_t session_id = 0;
    uint64_t pgid = 0;
};
struct IdentitySnapshot {
    Identity internal{};
    Names visible{};
};

// Bounded, order-independent resolution of one numeric group alias. Remote
// queries classify origins relative to the caller's namespace, not their own.
struct GroupCandidates {
    static constexpr uint8_t LOCAL = 1, OWN = 2, OTHER = 4, AMBIGUOUS = 8;
    uint8_t flags = 0;
    Origin other{};

    constexpr void observe(Origin candidate, Origin local, std::optional<Origin> own) {
        if (candidate == local) {
            flags |= LOCAL;
        } else if (own && candidate == *own) {
            flags |= OWN;
        } else {
            if ((flags & OTHER) != 0 && other != candidate) flags |= AMBIGUOUS;
            if ((flags & OTHER) == 0) other = candidate;
            flags |= OTHER;
        }
    }
    constexpr void merge(const GroupCandidates& candidate) {
        if ((candidate.flags & OTHER) != 0) {
            if ((flags & OTHER) != 0 && other != candidate.other) flags |= AMBIGUOUS;
            if ((flags & OTHER) == 0) other = candidate.other;
        }
        flags |= candidate.flags;
    }
    [[nodiscard]] constexpr auto resolve(std::optional<Origin> own) const -> std::optional<Origin> {
        if ((flags & LOCAL) != 0) return Origin{};
        if ((flags & OWN) != 0 && own) return own;
        if ((flags & OTHER) != 0 && (flags & AMBIGUOUS) == 0) return other;
        return std::nullopt;
    }
};

[[nodiscard]] constexpr auto same_session(const Identity& first, const Identity& second) -> bool {
    return first.session_id != 0 && first.session_id == second.session_id && first.origin == second.origin;
}

[[nodiscard]] constexpr auto session_leader(const Identity& identity, uint64_t pid) -> bool {
    return pid != 0 && identity.origin.local() && identity.session_id == pid;
}

[[nodiscard]] constexpr auto group_leader(const Identity& identity, uint64_t pid) -> bool {
    return pid != 0 && identity.group_origin.local() && identity.pgid == pid;
}

[[nodiscard]] constexpr auto same_group(const Identity& first, const Identity& second) -> bool {
    return first.pgid != 0 && first.pgid == second.pgid && first.group_origin == second.group_origin;
}

// A terminal is owned by a receiver-local session. Keep the full group selected
// at admission: a later local numeric alias must not retarget its signals.
[[nodiscard]] constexpr auto terminal_foreground_matches(const Identity& identity, security::TerminalIdentity terminal, uint64_t session_id,
                                                         uint64_t generation, uint64_t pgid, Origin group_origin) -> bool {
    return identity.origin.local() && session_id != 0 && identity.session_id == session_id && pgid != 0 && identity.pgid == pgid &&
           identity.group_origin == group_origin &&
           security::may_receive_terminal_signal(terminal, generation, identity.terminal, identity.terminal_generation);
}

// Runtime mutations serialize admission and commit. Call without scheduler,
// lifecycle, PTY, resource or task-registry locks.
auto create(task::Task& caller) -> int64_t;
auto set_group(task::Task& caller, int64_t pid, int64_t pgid) -> int;
auto prepare_spawn_group(task::Task& child, int64_t pgid) -> int;
// Positive names select in the explicit requester's scope; native threads
// resolve their retained canonical process. Failures preserve output.
auto query(task::Task& caller, int64_t pid, Names& out) -> int;
auto claim_terminal(task::Task& caller, ker::dev::pty::PtyPair& pair) -> int;
auto detach_terminal(task::Task& caller, ker::dev::pty::PtyPair& pair) -> int;
auto set_foreground(task::Task& caller, ker::dev::pty::PtyPair& pair, int64_t pgid) -> int;
auto read_foreground(const task::Task& caller, ker::dev::pty::PtyPair& pair, uint64_t& out) -> int;

// Cleanup may already hold VM publication: these acquire only session mutation.
void process_exit(task::Task& owner);
void hangup_terminal(ker::dev::pty::PtyPair& pair);

}  // namespace ker::mod::sched::session
