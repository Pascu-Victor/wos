#pragma once

#include <atomic>
#include <cstdint>
#include <utility>

namespace ker::mod::sched {

// Reserves one pending occurrence while its delivery frame is prepared. The
// pending bit stays in its original pool until commit, so abandoning a claim
// neither loses a process signal nor resurrects a canceled job-control signal.
// The caller keeps both atomics alive and commits before noreturn teardown.
class SignalPendingClaim {
   public:
    SignalPendingClaim() = default;
    ~SignalPendingClaim() { cancel(); }
    SignalPendingClaim(const SignalPendingClaim&) = delete;
    auto operator=(const SignalPendingClaim&) -> SignalPendingClaim& = delete;
    SignalPendingClaim(SignalPendingClaim&& other) noexcept
        : m_pending(std::exchange(other.m_pending, nullptr)), m_claimed(other.m_claimed), m_mask(other.m_mask) {}
    auto operator=(SignalPendingClaim&&) -> SignalPendingClaim& = delete;

    [[nodiscard]] static auto acquire(std::atomic<uint64_t>& pending, std::atomic<uint64_t>& claimed, uint64_t mask) -> SignalPendingClaim {
        if (mask == 0 || (mask & (mask - 1)) != 0 || (claimed.fetch_or(mask, std::memory_order_acq_rel) & mask) != 0) {
            return {};
        }
        SignalPendingClaim result;
        result.m_pending = &pending;
        result.m_claimed = &claimed;
        result.m_mask = mask;
        if ((pending.load(std::memory_order_acquire) & mask) == 0) {
            result.cancel();
        }
        return result;
    }

    [[nodiscard]] explicit operator bool() const { return m_pending != nullptr; }

    // Inspect provenance before commit clears the reservation. The caller
    // retains the originating Task and serializes any associated metadata.
    [[nodiscard]] auto is_for(const std::atomic<uint64_t>& pending, uint64_t mask) const -> bool {
        return m_pending == &pending && m_mask == mask;
    }

    [[nodiscard]] auto commit() -> bool {
        if (m_pending == nullptr) {
            return false;
        }
        bool const CONSUMED = (m_pending->fetch_and(~m_mask, std::memory_order_acq_rel) & m_mask) != 0;
        cancel();
        return CONSUMED;
    }

   private:
    void cancel() {
        if (m_pending != nullptr) {
            m_pending = nullptr;
            m_claimed->fetch_and(~m_mask, std::memory_order_release);
        }
    }

    std::atomic<uint64_t>* m_pending = nullptr;
    std::atomic<uint64_t>* m_claimed = nullptr;
    uint64_t m_mask = 0;
};

}  // namespace ker::mod::sched
