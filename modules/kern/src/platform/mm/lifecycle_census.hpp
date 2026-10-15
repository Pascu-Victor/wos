#pragma once

#ifdef WOS_SELFTEST
#include <atomic>
#include <cstdint>
#include <platform/sys/spinlock.hpp>

namespace ker::mod::mm::lifecycle_census {

// A version is compared only within one complete double collect, never to the
// original baseline. Invalid/exhausted stamps permanently refuse acceptance.
struct Stamp {
    uint64_t version{};
    uint64_t inflight{};
    bool valid{};
    auto operator==(const Stamp&) const -> bool = default;
};

[[nodiscard]] inline auto quiescent(const Stamp& value) noexcept -> bool {
    return value.valid && value.version != 0 && value.inflight == 0;
}

[[nodiscard]] inline auto unchanged(const Stamp& before, const Stamp& after) noexcept -> bool {
    return quiescent(before) && quiescent(after) && before.version == after.version;
}

// Diagnostic arithmetic is lock-free and never wraps. It takes no owner or
// diagnostic lock, including when called with an existing IRQ lock held.
class Epoch final {
   public:
    void begin() noexcept {
        if (!valid_.load(std::memory_order_seq_cst)) return;
        if (!increment(inflight_)) return;
        static_cast<void>(increment(version_));
    }
    void end() noexcept {
        if (!valid_.load(std::memory_order_seq_cst)) return;
        if (!increment(version_)) return;
        auto value = inflight_.load(std::memory_order_seq_cst);
        for (;;) {
            if (value == 0) {
                valid_.store(false, std::memory_order_seq_cst);
                return;
            }
            if (inflight_.compare_exchange_weak(value, value - 1, std::memory_order_seq_cst)) return;
        }
    }
    [[nodiscard]] auto snapshot() const noexcept -> Stamp {
        auto const before = version_.load(std::memory_order_seq_cst);
        auto const active = inflight_.load(std::memory_order_seq_cst);
        auto const valid = valid_.load(std::memory_order_seq_cst);
        auto const after = version_.load(std::memory_order_seq_cst);
        return {.version = after, .inflight = active, .valid = valid && before == after};
    }
    constexpr Epoch() = default;
    Epoch(const Epoch&) = delete;
    auto operator=(const Epoch&) -> Epoch& = delete;

   private:
    auto increment(std::atomic<uint64_t>& counter) noexcept -> bool {
        auto value = counter.load(std::memory_order_seq_cst);
        for (;;) {
            if (value == UINT64_MAX) {
                valid_.store(false, std::memory_order_seq_cst);
                return false;
            }
            if (counter.compare_exchange_weak(value, value + 1, std::memory_order_seq_cst)) return true;
        }
    }
    static_assert(std::atomic<uint64_t>::is_always_lock_free);
    static_assert(std::atomic<bool>::is_always_lock_free);
    std::atomic<uint64_t> version_{1};
    std::atomic<uint64_t> inflight_{0};
    std::atomic<bool> valid_{true};
};

class Operation final {
   public:
    explicit Operation(Epoch& epoch) noexcept : epoch_(epoch) { epoch_.begin(); }
    ~Operation() { epoch_.end(); }
    Operation(const Operation&) = delete;
    auto operator=(const Operation&) -> Operation& = delete;

   private:
    Epoch& epoch_;
};

// Composition intentionally forbids raw Spinlock conversion or an escaping
// alias. All existing acquisition forms are instrumented before they wait;
// completion is AFTER the real unlock/IRQ/preemption restoration returns.
class OwnerLock final {
   public:
    constexpr explicit OwnerLock(Epoch& epoch) noexcept : epoch_(epoch) {}
    void lock() {
        epoch_.begin();
        lock_.lock();
    }
    [[nodiscard]] auto try_lock() -> bool {
        epoch_.begin();
        if (lock_.try_lock()) return true;
        epoch_.end();
        return false;
    }
    void unlock() {
        lock_.unlock();
        epoch_.end();
    }
    [[nodiscard]] auto lock_irqsave() -> uint64_t {
        epoch_.begin();
        return lock_.lock_irqsave();
    }
    void unlock_irqrestore(uint64_t flags) {
        lock_.unlock_irqrestore(flags);
        epoch_.end();
    }

    // Only census readers use these. Production helper calls remain on the
    // instrumented methods, including otherwise read-only production helpers.
    [[nodiscard]] auto observe_lock_irqsave() -> uint64_t { return lock_.lock_irqsave(); }
    void observe_unlock_irqrestore(uint64_t flags) { lock_.unlock_irqrestore(flags); }
    OwnerLock(const OwnerLock&) = delete;
    auto operator=(const OwnerLock&) -> OwnerLock& = delete;

   private:
    Epoch& epoch_;
    sys::Spinlock lock_;
};

}  // namespace ker::mod::mm::lifecycle_census
#endif
