#pragma once

#include <atomic>
#include <cstdint>

namespace ker::vfs::coherency {

// Private owner authority, scoped externally by boot/export/open incarnation.
// A single counter covers identities and publications, including an inode
// re-instantiated after cache eviction. Zero permanently means exhaustion.
// Allocation is lock-free, task-context work; it is not a wait-free IRQ API.
class Counter {
   public:
    explicit constexpr Counter(uint64_t first = 1) : next_(first) {}
    Counter(const Counter&) = delete;
    auto operator=(const Counter&) -> Counter& = delete;

    auto take() -> uint64_t {
        uint64_t value = next_.load(std::memory_order_relaxed);
        while (value != 0) {
            uint64_t const NEXT = value == UINT64_MAX ? 0 : value + 1;
            if (next_.compare_exchange_weak(value, NEXT, std::memory_order_relaxed)) return value;
        }
        return 0;
    }

   private:
    std::atomic<uint64_t> next_;
};

inline Counter owner_counter;

enum class Domain : uint8_t { NONE = 0, CONTENT = 1, METADATA = 2, DIRECTORY = 4, ALL = 7 };

constexpr auto operator|(Domain left, Domain right) -> Domain {
    return static_cast<Domain>(static_cast<uint8_t>(left) | static_cast<uint8_t>(right));
}

constexpr auto valid_domain(Domain domain) -> bool {
    uint8_t const BITS = static_cast<uint8_t>(domain);
    return BITS != 0 && (BITS & ~static_cast<uint8_t>(Domain::ALL)) == 0;
}

constexpr auto includes(Domain domains, Domain domain) -> bool {
    return (static_cast<uint8_t>(domains) & static_cast<uint8_t>(domain)) != 0;
}

constexpr auto mutation_domains(Domain domains) -> Domain {
    // Data and membership changes also change observable inode metadata.
    return includes(domains, Domain::CONTENT | Domain::DIRECTORY) ? domains | Domain::METADATA : domains;
}

struct Stamp {
    uint64_t object = 0;
    uint64_t content = 0;
    uint64_t metadata = 0;
    uint64_t directory = 0;

    constexpr auto operator==(const Stamp&) const -> bool = default;
};

constexpr auto valid_stamp(const Stamp& stamp) -> bool {
    return stamp.object != 0 && stamp.content >= stamp.object && stamp.directory >= stamp.object && stamp.metadata >= stamp.content &&
           stamp.metadata >= stamp.directory;
}

// The current stamp must come from an authenticated validation of this exact
// retained object. No elapsed time or best-effort notification grants freshness.
constexpr auto cache_matches(const Stamp& cached, const Stamp& current, Domain domains) -> bool {
    return valid_domain(domains) && valid_stamp(cached) && valid_stamp(current) && cached.object == current.object &&
           (!includes(domains, Domain::CONTENT) || cached.content == current.content) &&
           (!includes(domains, Domain::METADATA) || cached.metadata == current.metadata) &&
           (!includes(domains, Domain::DIRECTORY) || cached.directory == current.directory);
}

// Caller serializes the cache bytes and this authority together. A fill cannot
// acquire a newer revision merely by observing it after fetching older bytes.
class CacheValidation {
   public:
    constexpr void invalidate() {
        stamp_ = {};
        domains_ = Domain::NONE;
    }
    constexpr auto matches(const Stamp& current, Domain domains) const -> bool {
        return (static_cast<uint8_t>(domains) & ~static_cast<uint8_t>(domains_)) == 0 && cache_matches(stamp_, current, domains);
    }
    constexpr auto publish(const Stamp& before, const Stamp& after, Domain domains) -> bool {
        invalidate();
        if (!cache_matches(before, after, domains)) return false;
        stamp_ = after;
        domains_ = domains;
        return true;
    }

   private:
    Stamp stamp_{};
    Domain domains_ = Domain::NONE;
};

// This ticket is private state, never an untrusted wire authority. Reservation
// happens before effects and consumes a serial even if the mutation is denied.
struct Ticket {
    uint64_t serial = 0;
    Domain domains = Domain::NONE;

    constexpr auto operator==(const Ticket&) const -> bool = default;
};

// All methods require the canonical object's caller-held sleeping serialization
// lock. This class neither acquires locks nor performs backend/network work.
// Keep the counter alive through the state. Do not copy or relocate identity.
class State {
   public:
    State() = default;
    State(const State&) = delete;
    State(State&&) = delete;
    auto operator=(const State&) -> State& = delete;
    auto operator=(State&&) -> State& = delete;

    auto initialize(Counter& counter = owner_counter) -> bool {
        if (stamp_.object != 0 || closed_) return false;
        uint64_t const ID = counter.take();
        if (ID == 0) return false;
        stamp_ = {ID, ID, ID, ID};
        counter_ = &counter;
        return true;
    }

    auto snapshot(Stamp& output) const -> bool {
        if (counter_ == nullptr || closed_ || pending_.serial != 0) return false;
        output = stamp_;
        return true;
    }

    auto prepare(Domain domains, Ticket& output) -> bool {
        if (counter_ == nullptr || closed_ || pending_.serial != 0 || !valid_domain(domains)) return false;
        uint64_t const SERIAL = counter_->take();
        if (SERIAL == 0) return false;
        pending_ = {.serial = SERIAL, .domains = mutation_domains(domains)};
        output = pending_;
        return true;
    }

    // Publish after backend commit and before releasing serialization. A native
    // partial write or possibly mutating error publishes conservatively too.
    // Closing admission does not revoke an already-reserved effect's drain.
    auto publish(const Ticket& ticket) -> bool {
        if (!matches_pending(ticket)) return false;
        if (includes(ticket.domains, Domain::CONTENT)) stamp_.content = ticket.serial;
        if (includes(ticket.domains, Domain::METADATA)) stamp_.metadata = ticket.serial;
        if (includes(ticket.domains, Domain::DIRECTORY)) stamp_.directory = ticket.serial;
        pending_ = {};
        return true;
    }

    // Only use this when the caller proves no backend effect occurred. An error
    // return alone does not prove rollback or authorize preserving old versions.
    auto cancel_no_effect(const Ticket& ticket) -> bool {
        if (!matches_pending(ticket)) return false;
        pending_ = {};
        return true;
    }

    auto close_admission() -> bool {
        closed_ = true;
        return quiescent();
    }

    auto quiescent() const -> bool { return pending_.serial == 0; }

   private:
    auto matches_pending(const Ticket& ticket) const -> bool { return ticket.serial != 0 && ticket == pending_; }

    Counter* counter_ = nullptr;
    Stamp stamp_{};
    Ticket pending_{};
    bool closed_ = false;
};

}  // namespace ker::vfs::coherency
