#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::net::wki::jobs {

// Private values, not wire layouts. The adapter supplies the authenticated node
// and effective UID from its authorized mapped credential snapshot.
struct Owner {
    uint16_t node{};
    uint32_t uid{};
    constexpr auto operator==(const Owner&) const -> bool = default;
    [[nodiscard]] constexpr auto valid() const -> bool { return node != 0 && node != UINT16_MAX && uid != UINT32_MAX; }
};

struct Scope {
    Owner owner{};
    uint64_t submitter_incarnation{};
    constexpr auto operator==(const Scope&) const -> bool = default;
};

struct Key {
    Scope scope{};
    uint64_t serial{};
    uint64_t receiver_incarnation{};
    uint32_t attempt{};
    constexpr auto operator==(const Key&) const -> bool = default;
    [[nodiscard]] constexpr auto valid() const -> bool {
        return scope.owner.valid() && scope.submitter_incarnation != 0 && serial != 0 && receiver_incarnation != 0 && attempt != 0;
    }
};

enum class Phase : uint8_t { UNKNOWN, PROPOSED, ACCEPTED, COMMITTED, STARTING, RUNNING, TERMINAL, CANCELED, EXPIRED, LOST };

class Lifecycle {
   public:
    [[nodiscard]] constexpr auto phase() const -> Phase { return phase_; }
    [[nodiscard]] constexpr auto detached() const -> bool { return detached_; }
    [[nodiscard]] constexpr auto cancellation_requested() const -> bool { return cancel_requested_; }
    [[nodiscard]] constexpr auto cancellation_claimed() const -> bool { return cancel_claimed_; }
    [[nodiscard]] constexpr auto publication_claimed() const -> bool { return publication_claimed_; }
    [[nodiscard]] constexpr auto terminal() const -> bool {
        return phase_ == Phase::TERMINAL || phase_ == Phase::CANCELED || phase_ == Phase::EXPIRED;
    }

    constexpr auto accept() -> bool { return advance(Phase::PROPOSED, Phase::ACCEPTED); }
    constexpr auto commit() -> bool { return advance(Phase::ACCEPTED, Phase::COMMITTED); }
    constexpr auto start() -> bool { return advance(Phase::COMMITTED, Phase::STARTING); }

    // One worker owns native construction. This is a reservation, not a call to
    // the scheduler: its adapter must retain tracked Task/plan references.
    constexpr auto claim_publication() -> bool {
        if (phase_ != Phase::STARTING || cancel_requested_ || publication_claimed_) return false;
        publication_claimed_ = true;
        return true;
    }

    // Call after checked native post and its bookkeeping, or private teardown.
    // An early exit notification must stay owned until RUNNING is published.
    constexpr auto publication_result(bool posted) -> bool {
        if (phase_ != Phase::STARTING || !publication_claimed_) return false;
        phase_ = posted ? Phase::RUNNING : cancel_requested_ ? Phase::CANCELED : Phase::TERMINAL;
        return true;
    }
    constexpr auto abort_start() -> bool {
        if (phase_ != Phase::STARTING || publication_claimed_) return false;
        phase_ = cancel_requested_ ? Phase::CANCELED : Phase::TERMINAL;
        return true;
    }

    // True means the first cancellation won admission, not that a live Task
    // has exited. The adapter delivers termination after unlocking as needed.
    constexpr auto cancel() -> bool {
        if (terminal() || cancel_requested_) return false;
        cancel_requested_ = true;
        if (phase_ == Phase::PROPOSED || phase_ == Phase::ACCEPTED || phase_ == Phase::COMMITTED) phase_ = Phase::CANCELED;
        return true;
    }
    // Retain the exact published Task and deferred work before claiming. Queue
    // pressure before this claim leaves the recorded request available to retry.
    constexpr auto claim_cancellation() -> bool {
        if (phase_ != Phase::RUNNING || !cancel_requested_ || cancel_claimed_) return false;
        cancel_claimed_ = true;
        return true;
    }
    constexpr auto complete() -> bool {
        if (phase_ != Phase::RUNNING) return false;
        phase_ = cancel_requested_ ? Phase::CANCELED : Phase::TERMINAL;
        return true;
    }
    constexpr auto expire() -> bool {
        if (phase_ != Phase::PROPOSED && phase_ != Phase::ACCEPTED && phase_ != Phase::TERMINAL && phase_ != Phase::CANCELED) return false;
        phase_ = Phase::EXPIRED;
        return true;
    }
    constexpr void disconnect() { detached_ = true; }
    constexpr void reattach() { detached_ = false; }

   private:
    constexpr auto advance(Phase from, Phase to) -> bool {
        if (phase_ != from) return false;
        phase_ = to;
        return true;
    }
    Phase phase_{Phase::PROPOSED};
    bool cancel_requested_{};
    bool cancel_claimed_{};
    bool publication_claimed_{};
    bool detached_{};
};

using Digest = std::array<uint8_t, 32>;
struct Handle {
    size_t index{};
    uint64_t generation{};
    constexpr auto operator==(const Handle&) const -> bool = default;
};
enum class Admission : uint8_t { NEW, REPLAY, CONFLICT, EXPIRED, LOST, UNKNOWN, INVALID, CAPACITY };
struct Result {
    Admission admission{Admission::INVALID};
    Handle handle{};
};
struct RetentionPolicy {
    uint64_t byte_limit{};
    uint64_t proposal_ttl_us{};
    uint64_t result_ttl_us{};
    [[nodiscard]] constexpr auto valid() const -> bool { return byte_limit != 0 && proposal_ttl_us != 0 && result_ttl_us != 0; }
};

// Caller serializes all shared access. No allocation or native reference is
// hidden here. Borrowed lifecycle pointers may not escape the registry lock.
// Scopes retain compact watermarks until receiver restart, even after records
// expire; capacity refusal is preferable to repeating an accepted execution.
template <size_t ScopeCount, size_t RecordCount>
class Registry {
    static_assert(ScopeCount != 0 && RecordCount != 0);

   public:
    constexpr Registry(uint64_t receiver_incarnation, RetentionPolicy policy, uint64_t first_generation = 1)
        : receiver_incarnation_(receiver_incarnation), next_generation_(first_generation), policy_(policy) {}
    Registry(const Registry&) = delete;
    auto operator=(const Registry&) -> Registry& = delete;
    Registry(Registry&&) = delete;
    auto operator=(Registry&&) -> Registry& = delete;

    // The adapter reserves its bounded maximum payload/stream cost before any
    // native allocation. Keep the charge until cleanup and record reclamation.
    constexpr auto propose(const Key& key, const Digest& digest, uint64_t reserved_bytes, uint64_t now_us) -> Result {
        if (reserved_bytes == 0) return {Admission::INVALID};
        auto const EXISTING = lookup(key);
        if (EXISTING.admission == Admission::REPLAY) {
            auto const& record = records_[EXISTING.handle.index];
            return record.digest == digest ? EXISTING : Result{Admission::CONFLICT};
        }
        if (EXISTING.admission != Admission::UNKNOWN) return EXISTING;
        if (reserved_bytes > policy_.byte_limit - reserved_bytes_) return {Admission::CAPACITY};
        ScopeRow* scope = nullptr;
        ScopeRow* free_scope = nullptr;
        for (auto& row : scopes_) {
            if (row.used && row.scope == key.scope) scope = &row;
            if (!row.used && free_scope == nullptr) free_scope = &row;
        }
        if (scope == nullptr) scope = free_scope;
        if (scope == nullptr || next_generation_ == 0) return {Admission::CAPACITY};
        for (size_t index = 0; index < records_.size(); ++index) {
            auto& record = records_[index];
            if (record.used) continue;
            auto const GENERATION = next_generation_;
            next_generation_ = GENERATION == UINT64_MAX ? 0 : GENERATION + 1;
            record = {.used = true,
                      .key = key,
                      .digest = digest,
                      .generation = GENERATION,
                      .reserved_bytes = reserved_bytes,
                      .created_us = now_us};
            if (!scope->used) ++scopes_in_use_;
            *scope = {.used = true, .scope = key.scope, .high_water = key.serial};
            ++records_in_use_;
            reserved_bytes_ += reserved_bytes;
            return {Admission::NEW, {index, GENERATION}};
        }
        return {Admission::CAPACITY};
    }

    [[nodiscard]] constexpr auto lookup(const Key& key) const -> Result {
        if (!key.valid() || receiver_incarnation_ == 0 || !policy_.valid()) return {Admission::INVALID};
        if (key.receiver_incarnation != receiver_incarnation_) return {Admission::LOST};
        for (size_t index = 0; index < records_.size(); ++index) {
            auto const& record = records_[index];
            if (record.used && record.key.scope == key.scope && record.key.serial == key.serial)
                return record.key == key ? Result{Admission::REPLAY, {index, record.generation}} : Result{Admission::CONFLICT};
        }
        for (auto const& row : scopes_)
            if (row.used && row.scope == key.scope && key.serial <= row.high_water) return {Admission::EXPIRED};
        return {Admission::UNKNOWN};
    }

    constexpr auto lifecycle(Handle handle) -> Lifecycle* {
        auto* record = find(handle);
        return record != nullptr ? &record->lifecycle : nullptr;
    }
    constexpr auto retain(Handle handle) -> bool {
        auto* record = find(handle);
        if (record == nullptr || record->holders == UINT32_MAX) return false;
        ++record->holders;
        return true;
    }
    constexpr auto release(Handle handle) -> bool {
        auto* record = find(handle);
        if (record == nullptr || record->holders == 0) return false;
        --record->holders;
        return true;
    }
    [[nodiscard]] constexpr auto records_in_use() const -> size_t { return records_in_use_; }
    [[nodiscard]] constexpr auto scopes_in_use() const -> size_t { return scopes_in_use_; }
    [[nodiscard]] constexpr auto reserved_bytes() const -> uint64_t { return reserved_bytes_; }
    [[nodiscard]] constexpr auto handle_at(size_t index) const -> Handle {
        if (index >= records_.size() || !records_[index].used) return {};
        return {index, records_[index].generation};
    }

    // Stamp the first terminal transition under the owner lock. A missing stamp
    // retains safely; status/reattach/replay must never renew terminal retention.
    constexpr auto mark_terminal(Handle handle, uint64_t now_us) -> bool {
        auto* record = find(handle);
        if (record == nullptr || record->terminal_stamped ||
            (record->lifecycle.phase() != Phase::TERMINAL && record->lifecycle.phase() != Phase::CANCELED))
            return false;
        record->terminal_us = now_us;
        record->terminal_stamped = true;
        return true;
    }
    constexpr auto expire_due(Handle handle, uint64_t now_us) -> bool {
        auto* record = find(handle);
        if (record == nullptr) return false;
        auto const PHASE = record->lifecycle.phase();
        if (PHASE == Phase::EXPIRED) return expire(handle);
        bool const DUE = (PHASE == Phase::PROPOSED || PHASE == Phase::ACCEPTED)
                             ? elapsed(now_us, record->created_us, policy_.proposal_ttl_us)
                             : (PHASE == Phase::TERMINAL || PHASE == Phase::CANCELED) && record->terminal_stamped &&
                                   elapsed(now_us, record->terminal_us, policy_.result_ttl_us);
        return DUE && expire(handle);
    }

    // The adapter checks TTL/ACK policy and releases all native ownership first.
    // Active or held storage cannot be recycled to satisfy a result quota.
    constexpr auto expire(Handle handle) -> bool {
        auto* record = find(handle);
        if (record == nullptr || record->holders != 0) return false;
        if (record->lifecycle.phase() != Phase::EXPIRED && !record->lifecycle.expire()) return false;
        reserved_bytes_ -= record->reserved_bytes;
        --records_in_use_;
        *record = {};
        return true;
    }

   private:
    struct ScopeRow {
        bool used{};
        Scope scope{};
        uint64_t high_water{};
    };
    struct Record {
        bool used{};
        Key key{};
        Digest digest{};
        uint64_t generation{};
        uint64_t reserved_bytes{};
        uint64_t created_us{};
        uint64_t terminal_us{};
        uint32_t holders{};
        Lifecycle lifecycle{};
        bool terminal_stamped{};
    };
    [[nodiscard]] static constexpr auto elapsed(uint64_t now_us, uint64_t since_us, uint64_t ttl_us) -> bool {
        return now_us >= since_us && now_us - since_us >= ttl_us;
    }
    constexpr auto find(Handle handle) -> Record* {
        if (handle.index >= records_.size() || handle.generation == 0) return nullptr;
        auto& record = records_[handle.index];
        return record.used && record.generation == handle.generation ? &record : nullptr;
    }
    uint64_t receiver_incarnation_{};
    uint64_t next_generation_{};
    RetentionPolicy policy_{};
    uint64_t reserved_bytes_{};
    size_t records_in_use_{};
    size_t scopes_in_use_{};
    std::array<ScopeRow, ScopeCount> scopes_{};
    std::array<Record, RecordCount> records_{};
};

}  // namespace ker::net::wki::jobs
