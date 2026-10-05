#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace ker::mod::sched {

// These are observations, not scheduling decisions. ARM records precede the
// APIC write; TIMER_EXIT precedes the return assembly and may be absent when
// the timer body takes a non-returning exit. HANDOFF is a completed commit.
enum class SchedulerTimerTraceKind : uint8_t {
    TIMER_ENTER = 1,
    TIMER_EXIT,
    HANDOFF,
    LOCAL_REQUEST,
    LOCAL_COALESCED,
    SYSCALL_DEFERRED,
    STACK_REJECTED,
    ARM_STACK_FALLBACK,
    ARM_LOCAL,
    ARM_DEFERRED,
    ARM_IDLE_WORK,
    ARM_IDLE_PROBE,
    ARM_IDLE_DEADLINE,
    ARM_WAKE_IPI,
    ARM_QUANTUM,
    ARM_DEADLINE,
    ARM_START,
    ARM_INFLATE,
    TIMER_DISARM,
    LOCAL_BATCHED,
};

struct SchedulerTimerTraceEvent {
    uint64_t sequence{};
    uint64_t at_us{};
    uint64_t pid{};
    uint64_t value{};     // ticks for ARM, RIP for ENTER, outgoing PID for HANDOFF
    uint64_t metadata{};  // kind[7:0], task type[15:8], pinned[16], detail[63:32]
};

struct SchedulerTimerTraceWindow {
    uint64_t start_sequence{};
    uint64_t end_sequence{};
    bool enabled{};
};

constexpr size_t SCHEDULER_TIMER_TRACE_SNAPSHOT_EVENTS = 64;
struct SchedulerTimerTraceSnapshot {
    std::array<SchedulerTimerTraceEvent, SCHEDULER_TIMER_TRACE_SNAPSHOT_EVENTS> events{};
    uint64_t start_sequence{};
    uint64_t end_sequence{};
    uint64_t omitted{};
    uint64_t missing{};
    size_t count{};
    bool enabled{};

    [[nodiscard]] auto complete() const -> bool { return enabled && omitted == 0 && missing == 0; }
};

// Per-CPU bounded storage. Writers may be interrupted between reservation and
// publication. A busy slot is dropped rather than overwritten or waited for.
// Every payload is atomic: a reader validates its generation before and after
// copying, and reports any incomplete or overwritten reservation as missing.
// Release payload stores/acquire loads make a newly observed payload order the
// writer's BUSY publication before the reader's final generation check.
class SchedulerTimerTrace {
    static constexpr size_t CAPACITY = 128;
    static constexpr uint64_t BUSY = 1ULL << 63;
    static_assert(std::atomic<uint64_t>::is_always_lock_free);

    struct Slot {
        std::atomic<uint64_t> stamp{};
        std::atomic<uint64_t> at_us{};
        std::atomic<uint64_t> pid{};
        std::atomic<uint64_t> value{};
        std::atomic<uint64_t> metadata{};
    };
    std::array<Slot, CAPACITY> slots{};
    std::atomic<uint64_t> next_sequence{};

   public:
    struct Reservation {
        uint64_t sequence{};
        bool claimed{};
    };

    [[nodiscard]] auto sequence() const -> uint64_t { return next_sequence.load(std::memory_order_acquire); }

    [[nodiscard]] auto reserve() -> Reservation {
        uint64_t const SEQUENCE = next_sequence.fetch_add(1, std::memory_order_acq_rel) + 1;
        // The generation uses 63 bits. Exhaustion is an explicitly missing
        // reservation; generations beyond this boundary are unsupported.
        if (SEQUENCE == 0 || SEQUENCE >= BUSY) {
            return {SEQUENCE, false};
        }
        auto& slot = slots.at((SEQUENCE - 1) % CAPACITY);
        uint64_t observed = slot.stamp.load(std::memory_order_acquire);
        if ((observed & BUSY) != 0 || !slot.stamp.compare_exchange_strong(observed, BUSY | SEQUENCE, std::memory_order_acq_rel)) {
            return {SEQUENCE, false};
        }
        return {SEQUENCE, true};
    }

    void publish(Reservation reservation, SchedulerTimerTraceEvent const& event) {
        if (!reservation.claimed) {
            return;
        }
        auto& slot = slots.at((reservation.sequence - 1) % CAPACITY);
        slot.at_us.store(event.at_us, std::memory_order_release);
        slot.pid.store(event.pid, std::memory_order_release);
        slot.value.store(event.value, std::memory_order_release);
        slot.metadata.store(event.metadata, std::memory_order_release);
        slot.stamp.store(reservation.sequence, std::memory_order_release);
    }

    [[nodiscard]] auto snapshot(uint64_t start, uint64_t end) const -> SchedulerTimerTraceSnapshot {
        SchedulerTimerTraceSnapshot result{};
        result.enabled = true;
        result.start_sequence = start;
        result.end_sequence = end;
        if (end < start || end >= BUSY) {
            result.missing = 1;
            return result;
        }
        uint64_t const SPAN = end - start;
        result.omitted = SPAN > result.events.size() ? SPAN - result.events.size() : 0;
        for (uint64_t seq = start + result.omitted + 1; seq <= end; ++seq) {
            auto const& slot = slots.at((seq - 1) % CAPACITY);
            if (slot.stamp.load(std::memory_order_acquire) != seq) {
                ++result.missing;
                continue;
            }
            SchedulerTimerTraceEvent const EVENT{
                .sequence = seq,
                .at_us = slot.at_us.load(std::memory_order_acquire),
                .pid = slot.pid.load(std::memory_order_acquire),
                .value = slot.value.load(std::memory_order_acquire),
                .metadata = slot.metadata.load(std::memory_order_acquire),
            };
            if (slot.stamp.load(std::memory_order_acquire) != seq) {
                ++result.missing;
                continue;
            }
            result.events.at(result.count++) = EVENT;
        }
        return result;
    }
};

void set_scheduler_timer_trace_enabled(bool enabled);
auto scheduler_timer_trace_enabled() -> bool;
void record_scheduler_timer_trace(SchedulerTimerTraceKind kind, uint64_t value = 0, uint32_t detail = 0);
void capture_scheduler_timer_trace(uint64_t cpu_no, SchedulerTimerTraceWindow const& window, SchedulerTimerTraceSnapshot& snapshot);

}  // namespace ker::mod::sched
