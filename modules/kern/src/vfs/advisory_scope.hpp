#pragma once

#include <cstdint>
#include <vfs/advisory.hpp>

namespace ker::vfs {
enum class RemoteAdvisoryScopeState : uint8_t { EMPTY, ACTIVE, CLOSED };
enum class RemoteAdvisoryScopeClaim : uint8_t { ACTIVE, CLOSED, STALE, MISMATCH, INVALID };

struct RemoteAdvisoryScopeIdentity {
    RemoteAdvisoryOwner owner{};
    uint64_t object = 0;
    uint64_t dev = 0;
    uint64_t ino = 0;
    constexpr auto operator==(const RemoteAdvisoryScopeIdentity&) const -> bool = default;
    constexpr auto valid() const -> bool { return owner.valid() && object != 0 && ino != 0; }
};
struct RemoteAdvisoryScopeResult {
    uint64_t token = 0;
    uint64_t object = 0;
    RemoteAdvisoryScopeState state = RemoteAdvisoryScopeState::EMPTY;
};

// Caller serializes this with the native inode arbiter. A token has one
// immutable identity and one ACTIVE->CLOSED transition. Clearing table storage
// requires a new nonreused token; neither close replay nor ACK can reactivate it.
class RemoteAdvisoryScope {
   public:
    auto initialize(uint64_t token, const RemoteAdvisoryScopeIdentity& identity) -> bool {
        if (state_ != RemoteAdvisoryScopeState::EMPTY || token == 0 || !identity.valid()) return false;
        token_ = token;
        identity_ = identity;
        state_ = RemoteAdvisoryScopeState::ACTIVE;
        return true;
    }
    auto probe(uint64_t token, const RemoteAdvisoryOwner& owner) const -> RemoteAdvisoryScopeClaim {
        if (token == 0 || !owner.valid()) return RemoteAdvisoryScopeClaim::INVALID;
        if (state_ == RemoteAdvisoryScopeState::EMPTY || token != token_) return RemoteAdvisoryScopeClaim::STALE;
        if (owner != identity_.owner) return RemoteAdvisoryScopeClaim::MISMATCH;
        return state_ == RemoteAdvisoryScopeState::ACTIVE ? RemoteAdvisoryScopeClaim::ACTIVE : RemoteAdvisoryScopeClaim::CLOSED;
    }
    auto close(uint64_t token, const RemoteAdvisoryOwner& owner) -> RemoteAdvisoryScopeClaim {
        auto const CLAIM = probe(token, owner);
        if (CLAIM == RemoteAdvisoryScopeClaim::ACTIVE) state_ = RemoteAdvisoryScopeState::CLOSED;
        return CLAIM;
    }
    auto matches(const RemoteAdvisoryScopeIdentity& identity) const -> bool {
        return state_ == RemoteAdvisoryScopeState::ACTIVE && identity_ == identity;
    }
    auto snapshot() const -> RemoteAdvisoryScopeResult { return {.token = token_, .object = identity_.object, .state = state_}; }
    auto identity() const -> const RemoteAdvisoryScopeIdentity& { return identity_; }

   private:
    RemoteAdvisoryScopeIdentity identity_{};
    uint64_t token_ = 0;
    RemoteAdvisoryScopeState state_ = RemoteAdvisoryScopeState::EMPTY;
};
}  // namespace ker::vfs
