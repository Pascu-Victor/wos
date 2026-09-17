#pragma once

#include <cstdint>
#include <optional>

namespace ker::mod::sched::job_control {

class StopBarrier;

// Embedded in a thread. Its retained process owner must outlive attachment.
// Stable addresses are required: a membership must never be copied or moved.
class Member {
   public:
    Member() = default;
    Member(const Member&) = delete;
    auto operator=(const Member&) -> Member& = delete;
    [[nodiscard]] auto attached() const -> bool { return m_owner != nullptr; }
    [[nodiscard]] auto quiescent() const -> bool { return attached() && m_quiescent; }

   private:
    friend class StopBarrier;
    const StopBarrier* m_owner = nullptr;
    bool m_quiescent = false;
};

struct StopTicket {
    const StopBarrier* owner;
    uint64_t generation;
};

// Allocation-free process stop barrier. EVERY access, including observations
// and Member access, requires the same external process lock. No callbacks,
// scheduler operations or child-event publication occur inside this component.
//
// Quiescent means the caller has already prevented user execution: either an
// unpublished thread or a thread held at a validated safe stop boundary.
// An acknowledgment is not itself a scheduler stop. Likewise, successful
// release is permission to resume, not a wakeup. The runtime adapter must make
// these changes atomic with its execution gate under that same lock.
class StopBarrier {
   public:
    // Fresh processes use zero. A seed permits checking exhaustion without
    // wrapping a generation and accepting an arbitrarily old acknowledgment.
    explicit StopBarrier(uint64_t generation = 0) : m_generation(generation) {}
    StopBarrier(const StopBarrier&) = delete;
    auto operator=(const StopBarrier&) -> StopBarrier& = delete;

    // Admit before runnable publication. Admission never grants execution,
    // including when it races with a completed stop.
    [[nodiscard]] auto attach(Member& member) -> bool {
        if (member.attached() || m_members == UINT64_MAX) return false;
        member.m_owner = this;
        member.m_quiescent = true;
        ++m_members;
        ++m_quiescent;
        return true;
    }

    // Called after the thread cannot execute again, before releasing its
    // retained owner. Removing an unacknowledged exiting thread may complete
    // the remaining group's barrier; an empty group produces no STOP event.
    [[nodiscard]] auto detach(Member& member) -> bool {
        if (member.m_owner != this) return false;
        --m_members;
        if (member.m_quiescent) --m_quiescent;
        member.m_owner = nullptr;
        member.m_quiescent = false;
        return true;
    }

    // Invoke only after a stop action is selected (not for caught/blocked
    // TSTP/TTIN/TTOU generation). Repeated stops coalesce within one epoch.
    // Invalid signals/exhausted generations fail without modifying state;
    // the adapter must handle failure explicitly, never silently run a target.
    [[nodiscard]] auto request_stop(uint32_t signal) -> std::optional<StopTicket> {
        if (signal < 19 || signal > 22) return std::nullopt;
        if (!m_stopping) {
            if (m_generation == UINT64_MAX) return std::nullopt;
            ++m_generation;
            m_stop_signal = signal;
            m_stopping = true;
        }
        return StopTicket{this, m_generation};
    }

    // Cancels the stop gate, including an incomplete stop. Physical quiescence
    // persists until each thread passes release(); STOP racing ahead of that
    // release can reuse the acknowledgment without running the thread first.
    // Parent-event backpressure must be resolved by the adapter before it
    // grants this transition. SIGKILL uses a separate teardown path.
    auto request_continue() -> bool {
        bool const WAS_STOPPING = m_stopping;
        m_stopping = false;
        return WAS_STOPPING;
    }

    [[nodiscard]] auto acknowledge(Member& member, StopTicket ticket) -> bool {
        if (member.m_owner != this || ticket.owner != this || !m_stopping || ticket.generation != m_generation) return false;
        if (!member.m_quiescent) {
            member.m_quiescent = true;
            ++m_quiescent;
        }
        return true;
    }

    // Every first entry/resume must pass this gate. A generic event wake does
    // not confer permission to release a stopped member.
    [[nodiscard]] auto release(Member& member) -> bool {
        if (member.m_owner != this || m_stopping) return false;
        if (member.m_quiescent) {
            member.m_quiescent = false;
            --m_quiescent;
        }
        return true;
    }

    [[nodiscard]] auto stopping() const -> bool { return m_stopping; }
    [[nodiscard]] auto complete() const -> bool { return m_stopping && m_members != 0 && m_quiescent == m_members; }
    [[nodiscard]] auto members() const -> uint64_t { return m_members; }
    [[nodiscard]] auto quiescent_members() const -> uint64_t { return m_quiescent; }
    [[nodiscard]] auto generation() const -> uint64_t { return m_generation; }
    [[nodiscard]] auto stop_signal() const -> uint32_t { return m_stop_signal; }

   private:
    uint64_t m_members = 0;
    uint64_t m_quiescent = 0;
    uint64_t m_generation = 0;
    uint32_t m_stop_signal = 0;
    bool m_stopping = false;
};

}  // namespace ker::mod::sched::job_control
