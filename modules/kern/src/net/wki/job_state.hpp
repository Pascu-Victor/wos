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

// Caller serializes all shared access. No allocation or native reference is
// hidden here. Borrowed lifecycle pointers may not escape the registry lock.
// Scopes retain compact watermarks until receiver restart, even after records
// expire; capacity refusal is preferable to repeating an accepted execution.
template <size_t ScopeCount, size_t RecordCount>
class Registry {
    static_assert(ScopeCount != 0 && RecordCount != 0);

   public:
    constexpr explicit Registry(uint64_t receiver_incarnation, uint64_t first_generation = 1)
        : receiver_incarnation_(receiver_incarnation), next_generation_(first_generation) {}
    Registry(const Registry&) = delete;
    auto operator=(const Registry&) -> Registry& = delete;
    Registry(Registry&&) = delete;
    auto operator=(Registry&&) -> Registry& = delete;

    constexpr auto propose(const Key& key, const Digest& digest) -> Result {
        auto const EXISTING = lookup(key);
        if (EXISTING.admission == Admission::REPLAY) {
            auto const& record = records_[EXISTING.handle.index];
            return record.digest == digest ? EXISTING : Result{Admission::CONFLICT};
        }
        if (EXISTING.admission != Admission::UNKNOWN) return EXISTING;
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
            record = {.used = true, .key = key, .digest = digest, .generation = GENERATION};
            *scope = {.used = true, .scope = key.scope, .high_water = key.serial};
            return {Admission::NEW, {index, GENERATION}};
        }
        return {Admission::CAPACITY};
    }

    [[nodiscard]] constexpr auto lookup(const Key& key) const -> Result {
        if (!key.valid() || receiver_incarnation_ == 0) return {Admission::INVALID};
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

    // The adapter checks TTL/ACK policy and releases all native ownership first.
    // Active or held storage cannot be recycled to satisfy a result quota.
    constexpr auto expire(Handle handle) -> bool {
        auto* record = find(handle);
        if (record == nullptr || record->holders != 0) return false;
        if (record->lifecycle.phase() != Phase::EXPIRED && !record->lifecycle.expire()) return false;
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
        uint32_t holders{};
        Lifecycle lifecycle{};
    };
    constexpr auto find(Handle handle) -> Record* {
        if (handle.index >= records_.size() || handle.generation == 0) return nullptr;
        auto& record = records_[handle.index];
        return record.used && record.generation == handle.generation ? &record : nullptr;
    }
    uint64_t receiver_incarnation_{};
    uint64_t next_generation_{};
    std::array<ScopeRow, ScopeCount> scopes_{};
    std::array<Record, RecordCount> records_{};
};

}  // namespace ker::net::wki::jobs
