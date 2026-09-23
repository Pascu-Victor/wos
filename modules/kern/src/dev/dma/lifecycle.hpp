#pragma once

#include <atomic>
#include <cstdint>

namespace ker::dev::dma::lifecycle {

// Monotonic gate: construction admits callbacks, retirement permanently closes
// it. Device storage outlives every possible late caller, including failed ones.
class Admission {
    static constexpr uint32_t CLOSED = uint32_t{1} << 31;
    std::atomic<uint32_t> state_{0};

   public:
    auto enter() -> bool {
        auto state = state_.load(std::memory_order_acquire);
        for (;;) {
            if ((state & CLOSED) != 0 || state == CLOSED - 1) return false;
            if (state_.compare_exchange_weak(state, state + 1, std::memory_order_acq_rel, std::memory_order_acquire)) return true;
        }
    }
    void leave() { state_.fetch_sub(1, std::memory_order_release); }
    void close() { state_.fetch_or(CLOSED, std::memory_order_acq_rel); }
    [[nodiscard]] auto closed() const -> bool { return (state_.load(std::memory_order_acquire) & CLOSED) != 0; }
    [[nodiscard]] auto drained() const -> bool { return state_.load(std::memory_order_acquire) == CLOSED; }
    [[nodiscard]] auto readers() const -> uint32_t { return state_.load(std::memory_order_acquire) & ~CLOSED; }
};

class Guard {
    Admission* admission_;

   public:
    explicit Guard(Admission& admission) : admission_(admission.enter() ? &admission : nullptr) {}
    ~Guard() {
        if (admission_ != nullptr) admission_->leave();
    }
    Guard(const Guard&) = delete;
    auto operator=(const Guard&) -> Guard& = delete;
    [[nodiscard]] explicit operator bool() const { return admission_ != nullptr; }
};

enum class Result : uint8_t { released, drain_failed, quiesce_failed, release_failed };

// Shared by the real retirement path and ordering/failure tests. Only a drained
// callback gate permits touching queue ownership or detaching CPU borrows.
template <typename Backend>
auto retire(Backend& backend) -> Result {
    backend.close_admission();
    if (!backend.drain_readers()) {
        (void)backend.disable_bus_master();
        return Result::drain_failed;
    }
    backend.unpublish();
    bool const MASKED = backend.mask_interrupts();
    bool const RESET = backend.reset();
    bool const BME_OFF = backend.disable_bus_master();
    if (MASKED) backend.drain_interrupts();
    if (!MASKED || !RESET || !BME_OFF) {
        backend.quarantine();
        return Result::quiesce_failed;
    }
    return backend.release() ? Result::released : Result::release_failed;
}
}  // namespace ker::dev::dma::lifecycle
