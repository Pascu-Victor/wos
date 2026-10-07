#pragma once

#include <cerrno>
#include <cstdint>
#include <vfs/advisory.hpp>

namespace ker::vfs {

enum class RemoteAdvisoryPhase : uint8_t { START = 1, POLL = 2, CANCEL = 3, ACK = 4 };
enum class RemoteAdvisoryState : uint8_t { EMPTY, PENDING, COMPLETE, CANCELLED };
enum class RemoteAdvisoryClaim : uint8_t { NEW, PENDING, REPLAY, STALE, MISMATCH, BUSY, INVALID, CLOSED };

// Immutable syscall intent. Saved normalized native ranges belong separately
// to its first owner admission, so replay/poll cannot move SEEK_END locks.
struct RemoteAdvisoryCommand {
    RemoteAdvisoryOwner owner{};
    uint64_t scope_token = 0;
    RemoteAdvisoryKind kind = RemoteAdvisoryKind::PROCESS;
    int64_t start = 0;
    int64_t length = 0;
    int64_t position = 0;
    int16_t type = 0;
    int16_t whence = 0;
    bool query = false;
    bool wait = false;
    constexpr auto operator==(const RemoteAdvisoryCommand&) const -> bool = default;
    constexpr auto valid() const -> bool {
        bool const KIND = kind == RemoteAdvisoryKind::PROCESS || kind == RemoteAdvisoryKind::OPEN_FILE || kind == RemoteAdvisoryKind::FLOCK;
        return owner.valid() && KIND && (kind == RemoteAdvisoryKind::PROCESS || scope_token == 0) && type >= 0 && type <= 2 &&
               whence >= 0 && whence <= 2 && (!query || (!wait && kind != RemoteAdvisoryKind::FLOCK)) && (!wait || type != 2);
    }
};
struct RemoteAdvisoryPrepared {
    uint64_t dev = 0;
    uint64_t ino = 0;
    uint64_t start = 0;
    uint64_t end = 0;
    uint64_t object = 0;
    constexpr auto valid() const -> bool { return ino != 0 && start < end; }
};
struct RemoteAdvisoryOutcome {
    int32_t status = 0;
    int32_t pid = 0;
    int64_t start = 0;
    int64_t length = 0;
    int16_t type = 2;
    int16_t whence = 0;
    constexpr auto operator==(const RemoteAdvisoryOutcome&) const -> bool = default;
};
struct RemoteAdvisoryReply {
    uint64_t operation = 0;
    RemoteAdvisoryState state = RemoteAdvisoryState::EMPTY;
    RemoteAdvisoryOutcome outcome{};
};

// Serialized by the existing native advisory manager mutex, including actual
// lock grant/unlock and terminal publication. No heap, clocks or File ownership.
class RemoteAdvisoryReceipt {
   public:
    auto probe(uint64_t operation, const RemoteAdvisoryCommand& command) const -> RemoteAdvisoryClaim {
        if (closed_) return RemoteAdvisoryClaim::CLOSED;
        if (operation == 0 || !command.valid()) return RemoteAdvisoryClaim::INVALID;
        if (operation < operation_) return RemoteAdvisoryClaim::STALE;
        if (operation == operation_) {
            if (command != command_) return RemoteAdvisoryClaim::MISMATCH;
            return pending() ? RemoteAdvisoryClaim::PENDING : RemoteAdvisoryClaim::REPLAY;
        }
        return pending() ? RemoteAdvisoryClaim::BUSY : RemoteAdvisoryClaim::NEW;
    }
    auto claim(uint64_t operation, const RemoteAdvisoryCommand& command, const RemoteAdvisoryPrepared& prepared) -> RemoteAdvisoryClaim {
        auto const CLAIM = probe(operation, command);
        if (CLAIM != RemoteAdvisoryClaim::NEW) return CLAIM;
        if (!prepared.valid()) return RemoteAdvisoryClaim::INVALID;
        operation_ = operation;
        command_ = command;
        prepared_ = prepared;
        outcome_ = {.status = -EAGAIN};
        state_ = RemoteAdvisoryState::PENDING;
        waiting_ = false;
        return RemoteAdvisoryClaim::NEW;
    }
    // A cancel arriving before START burns its identity without a native effect.
    // A delayed START must replay interruption, never create a new grant.
    auto cancel_unstarted(uint64_t operation, const RemoteAdvisoryCommand& command) -> bool {
        if (probe(operation, command) != RemoteAdvisoryClaim::NEW) return false;
        operation_ = operation;
        command_ = command;
        prepared_ = {};
        outcome_ = {.status = -EINTR};
        state_ = RemoteAdvisoryState::CANCELLED;
        waiting_ = false;
        return true;
    }
    auto matches(uint64_t operation, const RemoteAdvisoryCommand& command) const -> bool {
        return !closed_ && operation != 0 && operation == operation_ && command == command_;
    }
    auto mark_waiting(uint64_t operation) -> bool {
        if (closed_ || !pending() || waiting_ || operation != operation_) return false;
        waiting_ = true;
        return true;
    }
    auto complete(uint64_t operation, const RemoteAdvisoryOutcome& outcome, bool cancelled = false) -> bool {
        if (closed_ || !pending() || operation != operation_ || outcome.status > 0 || (cancelled && outcome.status != -EINTR)) return false;
        outcome_ = outcome;
        state_ = cancelled ? RemoteAdvisoryState::CANCELLED : RemoteAdvisoryState::COMPLETE;
        waiting_ = false;
        return true;
    }
    // Caller removes a true pending-capacity membership under the same mutex.
    auto retire() -> bool {
        bool const RELEASE_WAIT = waiting_;
        if (pending()) {
            outcome_ = {.status = -ENOTCONN};
            state_ = RemoteAdvisoryState::COMPLETE;
        }
        waiting_ = false;
        closed_ = true;
        return RELEASE_WAIT;
    }
    auto snapshot(uint64_t operation, RemoteAdvisoryReply& output) const -> bool {
        if (operation == 0 || operation != operation_ || state_ == RemoteAdvisoryState::EMPTY) return false;
        output = {.operation = operation_, .state = state_, .outcome = outcome_};
        return true;
    }
    auto pending() const -> bool { return state_ == RemoteAdvisoryState::PENDING; }
    auto waiting() const -> bool { return waiting_; }
    auto operation() const -> uint64_t { return operation_; }
    auto command() const -> const RemoteAdvisoryCommand& { return command_; }
    auto prepared() const -> const RemoteAdvisoryPrepared& { return prepared_; }

   private:
    uint64_t operation_ = 0;
    RemoteAdvisoryCommand command_{};
    RemoteAdvisoryPrepared prepared_{};
    RemoteAdvisoryOutcome outcome_{};
    RemoteAdvisoryState state_ = RemoteAdvisoryState::EMPTY;
    bool waiting_ = false;
    bool closed_ = false;
};
}  // namespace ker::vfs
