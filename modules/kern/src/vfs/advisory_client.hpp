#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <vfs/advisory_receipt.hpp>

namespace ker::vfs::advisory_client {

constexpr std::size_t OPERATION_CAPACITY = 64;
constexpr std::size_t SCOPE_CAPACITY = 512;

// Serialized by the client registry Spin. The claiming stack separately owns
// its File pin and this nonreused ticket before it can park in a transport wait.
// No implicit waiter storage or sleeping mutex is involved.
class RpcGate {
   public:
    auto claim(uint64_t ticket) -> bool {
        if (ticket == 0 || holder_ != 0) return false;
        holder_ = ticket;
        return true;
    }
    auto release(uint64_t ticket) -> bool {
        if (ticket == 0 || holder_ != ticket) return false;
        holder_ = 0;
        return true;
    }
    auto holder() const -> uint64_t { return holder_; }

   private:
    uint64_t holder_ = 0;
};

// A process/inode scope spans exports, independent descriptions and lanes.
// Captured witness channel/binding/open authority belongs to the enclosing
// record, never to this key. PID is immutable diagnostic metadata separately.
struct ScopeKey {
    uint64_t object = 0;
    uint64_t process = 0;
    uint32_t boot = 0;
    uint16_t owner = UINT16_MAX;
    constexpr auto operator==(const ScopeKey&) const -> bool = default;
    constexpr auto valid() const -> bool { return object != 0 && process != 0 && boot != 0 && owner != 0 && owner != UINT16_MAX; }
};

enum class ScopePhase : uint8_t { EMPTY, REGISTERING, ACTIVE, CLOSING, CLOSED };

// Fixed-arena lifecycle; caller holds the client registry Spin. Slot serials
// never repeat. Users retain old tokens through close/ACK, so a confirmed close
// can allow a successor without reusing the predecessor's record prematurely.
class Scope {
   public:
    auto reserve(uint64_t serial, const ScopeKey& key, int32_t pid, uint64_t registration) -> bool {
        if (phase_ != ScopePhase::EMPTY || serial == 0 || !key.valid() || pid <= 0 || registration == 0) return false;
        serial_ = serial;
        key_ = key;
        pid_ = pid;
        registration_ = registration;
        phase_ = ScopePhase::REGISTERING;
        return true;
    }
    auto request_close(uint64_t serial) -> bool {
        if (!matches(serial)) return false;
        close_requested_ = true;
        if (phase_ == ScopePhase::ACTIVE) phase_ = ScopePhase::CLOSING;
        return true;
    }
    auto registered(uint64_t serial, uint64_t registration, uint64_t token, uint64_t object, bool closed) -> bool {
        if (!matches(serial) || phase_ != ScopePhase::REGISTERING || registration != registration_ || token == 0 || object != key_.object)
            return false;
        token_ = token;
        phase_ = closed ? ScopePhase::CLOSED : close_requested_ ? ScopePhase::CLOSING : ScopePhase::ACTIVE;
        return true;
    }
    auto retain(uint64_t serial, uint64_t token) -> bool {
        return grants_allowed(serial) && token != 0 && token == token_ && borrow(serial);
    }
    auto release(uint64_t serial, uint64_t token) -> bool { return token != 0 && token == token_ && drop(serial); }
    // Registration/close observers may pin a record before its remote token is
    // known. They can finish watching a confirmed close while a successor runs.
    auto borrow(uint64_t serial) -> bool {
        if (!matches(serial) || users_ == UINT16_MAX) return false;
        ++users_;
        return true;
    }
    auto drop(uint64_t serial) -> bool {
        if (!matches(serial) || users_ == 0) return false;
        --users_;
        return true;
    }
    auto closed(uint64_t serial, uint64_t token, uint64_t object) -> bool {
        if (!matches(serial) || (phase_ != ScopePhase::CLOSING && phase_ != ScopePhase::CLOSED) || token == 0 || token != token_ ||
            object != key_.object)
            return false;
        phase_ = ScopePhase::CLOSED;
        return true;
    }
    auto acknowledged(uint64_t serial, uint64_t token, uint64_t object) -> bool {
        if (!matches(serial) || phase_ != ScopePhase::CLOSED || token == 0 || token != token_ || object != key_.object) return false;
        acknowledged_ = true;
        return true;
    }
    // Caller must first prove captured owner-boot invalidation or exact witness
    // binding retirement. Timeout/inactivity/another numeric FD is no proof.
    auto retire(uint64_t serial) -> bool {
        if (!matches(serial)) return false;
        phase_ = ScopePhase::CLOSED;
        close_requested_ = true;
        acknowledged_ = true;
        return true;
    }
    auto claim_rpc(uint64_t serial, uint64_t ticket) -> bool { return matches(serial) && rpc_.claim(ticket); }
    auto release_rpc(uint64_t serial, uint64_t ticket) -> bool { return matches(serial) && rpc_.release(ticket); }
    auto grants_allowed(uint64_t serial) const -> bool { return matches(serial) && phase_ == ScopePhase::ACTIVE && !close_requested_; }
    auto blocks_successor(const ScopeKey& key) const -> bool {
        return phase_ != ScopePhase::EMPTY && phase_ != ScopePhase::CLOSED && key == key_;
    }
    auto reclaimable(uint64_t serial) const -> bool {
        return matches(serial) && phase_ == ScopePhase::CLOSED && acknowledged_ && users_ == 0 && rpc_.holder() == 0;
    }
    auto serial() const -> uint64_t { return serial_; }
    auto key() const -> const ScopeKey& { return key_; }
    auto pid() const -> int32_t { return pid_; }
    auto registration() const -> uint64_t { return registration_; }
    auto token() const -> uint64_t { return token_; }
    auto phase() const -> ScopePhase { return phase_; }
    auto close_requested() const -> bool { return close_requested_; }
    auto acknowledgement_complete() const -> bool { return acknowledged_; }

   private:
    auto matches(uint64_t serial) const -> bool { return serial != 0 && serial == serial_ && phase_ != ScopePhase::EMPTY; }
    ScopeKey key_{};
    RpcGate rpc_{};
    uint64_t serial_ = 0;
    uint64_t registration_ = 0;
    uint64_t token_ = 0;
    int32_t pid_ = 0;
    uint16_t users_ = 0;
    ScopePhase phase_ = ScopePhase::EMPTY;
    bool close_requested_ = false;
    bool acknowledged_ = false;
};

enum class OperationPhase : uint8_t { EMPTY, PREPARED, STARTED, CANCEL, ACK, DONE };

// Every acquisition owns fixed cleanup storage before START. Its enclosing
// record retains the immutable wire command, File pin and captured authority.
// Query/unlock have no acquisition effect and use the stack owner instead.
class Operation {
   public:
    auto reserve(uint64_t serial) -> bool {
        if (serial == 0 || phase_ != OperationPhase::EMPTY) return false;
        serial_ = serial;
        phase_ = OperationPhase::PREPARED;
        return true;
    }
    auto started(uint64_t serial, uint64_t operation) -> bool {
        if (!matches(serial) || phase_ != OperationPhase::PREPARED || operation == 0) return false;
        operation_ = operation;
        phase_ = OperationPhase::STARTED;
        return true;
    }
    auto abandon(uint64_t serial) -> bool {
        if (!matches(serial)) return false;
        if (phase_ == OperationPhase::PREPARED)
            phase_ = OperationPhase::DONE;  // No START could have been published.
        else if (phase_ == OperationPhase::STARTED)
            phase_ = OperationPhase::CANCEL;
        return true;
    }
    auto observe(uint64_t serial, const RemoteAdvisoryReply& reply) -> bool {
        if (!matches(serial) || reply.operation == 0 || reply.operation != operation_ || reply.outcome.status > 0) return false;
        if (phase_ == OperationPhase::ACK) return terminal_ == reply.state && outcome_ == reply.outcome;
        if (phase_ != OperationPhase::STARTED && phase_ != OperationPhase::CANCEL) return false;
        if (reply.state == RemoteAdvisoryState::PENDING) return phase_ == OperationPhase::STARTED && reply.outcome.status == -EAGAIN;
        if (reply.state != RemoteAdvisoryState::COMPLETE &&
            (reply.state != RemoteAdvisoryState::CANCELLED || reply.outcome.status != -EINTR))
            return false;
        outcome_ = reply.outcome;
        terminal_ = reply.state;
        phase_ = OperationPhase::ACK;
        return true;
    }
    auto acknowledged(uint64_t serial, uint64_t operation) -> bool {
        if (!matches(serial) || phase_ != OperationPhase::ACK || operation == 0 || operation != operation_) return false;
        phase_ = OperationPhase::DONE;
        return true;
    }
    // Exact CANCEL returning ESTALE proves no retained acquisition receipt:
    // every admitted acquisition keeps one until this client's ACK or File
    // retirement. This branch has never sent ACK. Caller validates that exact
    // response on the captured binding; generic transport errors cannot use it.
    auto cancelled_without_receipt(uint64_t serial, uint64_t operation) -> bool {
        if (!matches(serial) || phase_ != OperationPhase::CANCEL || operation == 0 || operation != operation_) return false;
        outcome_ = {.status = -EINTR};
        terminal_ = RemoteAdvisoryState::CANCELLED;
        phase_ = OperationPhase::DONE;
        return true;
    }
    // Same explicit captured retirement proof requirement as Scope::retire.
    auto retire(uint64_t serial) -> bool {
        if (!matches(serial)) return false;
        phase_ = OperationPhase::DONE;
        return true;
    }
    auto reclaimable(uint64_t serial) const -> bool { return matches(serial) && phase_ == OperationPhase::DONE; }
    auto serial() const -> uint64_t { return serial_; }
    auto operation() const -> uint64_t { return operation_; }
    auto phase() const -> OperationPhase { return phase_; }
    auto terminal() const -> RemoteAdvisoryState { return terminal_; }
    auto outcome() const -> const RemoteAdvisoryOutcome& { return outcome_; }

   private:
    auto matches(uint64_t serial) const -> bool { return serial != 0 && serial_ == serial && phase_ != OperationPhase::EMPTY; }
    RemoteAdvisoryOutcome outcome_{};
    uint64_t serial_ = 0;
    uint64_t operation_ = 0;
    OperationPhase phase_ = OperationPhase::EMPTY;
    RemoteAdvisoryState terminal_ = RemoteAdvisoryState::EMPTY;
};

}  // namespace ker::vfs::advisory_client
