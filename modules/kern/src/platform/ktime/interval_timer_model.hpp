#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <platform/ktime/arithmetic.hpp>

namespace ker::mod::time::interval {

enum class Kind : uint8_t { REAL = 0, VIRTUAL = 1, PROF = 2 };
enum class ConfigureResult : uint8_t { APPLIED, INVALID_KIND, CLOSED, GENERATION_EXHAUSTED };

struct Clocks {
    uint64_t monotonic_us{};
    uint64_t user_us{};
    uint64_t total_us{};
};

struct Value {
    uint64_t remaining_us{};
    uint64_t interval_us{};
};

class ProcessTimers;
struct Token {
    const ProcessTimers* owner{};
    uint64_t generation{};
    uint64_t sequence{};
    Kind kind{};
};

struct Expirations {
    std::array<Token, 3> tokens{};
    size_t count{};
};

// One instance per retained process owner. Every method requires the caller's
// IRQ-safe owner lock. Tokens borrow this object's lifetime; cancellation alone
// does not make it safe to free the owning process. No method calls out.
class ProcessTimers {
   public:
    // A nonzero initial generation supports persistent/imported generation
    // epochs and boundary tests. It is never decreased or reused.
    explicit constexpr ProcessTimers(uint64_t initial_generation = 0, uint64_t initial_sequence = 0) {
        for (auto& timer : timers_) {
            timer.generation = initial_generation;
            timer.sequence = initial_sequence;
        }
    }
    ProcessTimers(const ProcessTimers&) = delete;
    auto operator=(const ProcessTimers&) -> ProcessTimers& = delete;
    ProcessTimers(ProcessTimers&&) = delete;
    auto operator=(ProcessTimers&&) -> ProcessTimers& = delete;

    static constexpr auto valid(Kind kind) -> bool { return static_cast<size_t>(kind) < 3; }
    static constexpr auto signal_mask(Kind kind) -> uint64_t {
        switch (kind) {
            case Kind::REAL:
                return uint64_t{1} << 13;  // SIGALRM 14
            case Kind::VIRTUAL:
                return uint64_t{1} << 25;  // SIGVTALRM 26
            case Kind::PROF:
                return uint64_t{1} << 26;  // SIGPROF 27
        }
        return 0;
    }

    constexpr auto configure(Kind kind, Value value, Clocks clocks, Value* previous = nullptr) -> ConfigureResult {
        if (!valid(kind)) return ConfigureResult::INVALID_KIND;
        if (closed_) return ConfigureResult::CLOSED;
        auto& timer = timers_[static_cast<size_t>(kind)];
        if (value.remaining_us != 0 && (timer.generation == UINT64_MAX || timer.sequence == UINT64_MAX))
            return ConfigureResult::GENERATION_EXHAUSTED;
        if (previous != nullptr) *previous = observe(timer, now(kind, clocks));
        if (timer.generation != UINT64_MAX) ++timer.generation;
        timer.deadline = arithmetic::saturating_add(now(kind, clocks), value.remaining_us);
        timer.interval = value.interval_us;
        timer.armed = value.remaining_us != 0;
        timer.pending = false;
        return ConfigureResult::APPLIED;
    }

    constexpr auto read(Kind kind, Clocks clocks, Value& value) const -> bool {
        if (!valid(kind)) return false;
        value = observe(timers_[static_cast<size_t>(kind)], now(kind, clocks));
        return true;
    }

    [[nodiscard]] constexpr auto next_deadline(Kind kind, uint64_t& deadline) const -> bool {
        if (closed_ || !valid(kind)) return false;
        auto const& timer = timers_[static_cast<size_t>(kind)];
        if (!timer.armed) return false;
        deadline = timer.deadline;
        return true;
    }

    // Cancellation is always available, including after generation exhaustion.
    // Exhausted timers cannot subsequently rearm, so their version never wraps.
    constexpr auto cancel(Kind kind) -> bool {
        if (!valid(kind)) return false;
        auto& timer = timers_[static_cast<size_t>(kind)];
        if (timer.generation != UINT64_MAX) ++timer.generation;
        timer.armed = false;
        timer.pending = false;
        timer.interval = 0;
        return true;
    }

    constexpr void close() {
        if (closed_) return;
        closed_ = true;
        for (size_t i = 0; i < timers_.size(); ++i) static_cast<void>(cancel(static_cast<Kind>(i)));
    }
    [[nodiscard]] constexpr auto closed() const -> bool { return closed_; }

    // Fixed three-record extraction, even if billions of periods were missed.
    // A pending standard signal coalesces subsequent expirations until claimed.
    // Domain bits permit CPU charging to collect VIRTUAL/PROF (0b110) without
    // changing the REAL deadline owned by its independent clockevent queue.
    constexpr auto collect(Clocks clocks, uint8_t domains = 0b111) -> Expirations {
        Expirations result{};
        if (closed_) return result;
        for (size_t i = 0; i < timers_.size(); ++i) {
            if ((domains & (1U << i)) == 0) continue;
            auto& timer = timers_[i];
            auto const KIND = static_cast<Kind>(i);
            uint64_t const NOW = now(KIND, clocks);
            if (!timer.armed || NOW < timer.deadline) continue;
            if (!timer.pending) {
                if (timer.sequence == UINT64_MAX) {
                    timer.armed = false;
                    continue;
                }
                ++timer.sequence;
                result.tokens[result.count++] = {.owner = this, .generation = timer.generation, .sequence = timer.sequence, .kind = KIND};
                timer.pending = true;
            }
            if (timer.interval == 0 || timer.deadline == UINT64_MAX || NOW == UINT64_MAX) {
                timer.armed = false;
            } else {
                timer.deadline = arithmetic::periodic_next(timer.deadline, timer.interval, NOW);
            }
        }
        return result;
    }

    // Claim and signal enqueue must share the owner's serialization boundary;
    // dropping that lock between them would admit stale delivery after set/cancel.
    // The returned mask is only a publication request, never callback execution.
    constexpr auto claim(Token token) -> uint64_t {
        if (closed_ || token.owner != this || !valid(token.kind)) return 0;
        auto& timer = timers_[static_cast<size_t>(token.kind)];
        if (!timer.pending || timer.generation != token.generation || timer.sequence != token.sequence) return 0;
        timer.pending = false;
        return signal_mask(token.kind);
    }

   private:
    struct Timer {
        uint64_t deadline{};
        uint64_t interval{};
        uint64_t generation{};
        uint64_t sequence{};
        bool armed{};
        bool pending{};
    };
    std::array<Timer, 3> timers_{};
    bool closed_{};

    static constexpr auto now(Kind kind, Clocks clocks) -> uint64_t {
        switch (kind) {
            case Kind::REAL:
                return clocks.monotonic_us;
            case Kind::VIRTUAL:
                return clocks.user_us;
            case Kind::PROF:
                return clocks.total_us;
        }
        return 0;
    }
    static constexpr auto observe(const Timer& timer, uint64_t now_us) -> Value {
        return {.remaining_us = timer.armed ? arithmetic::remaining(now_us, timer.deadline) : 0, .interval_us = timer.interval};
    }
};
}  // namespace ker::mod::time::interval
