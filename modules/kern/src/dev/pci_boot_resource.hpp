#pragma once

#include <atomic>
#include <cstdint>
#include <optional>
#include <utility>

namespace ker::dev::pci::resource {

enum class BootState : uint8_t { empty, preparing, ready, failed };

// Kernel-lifetime storage. Exactly one producer may initialize the retained
// value; competing/reentrant callers return busy instead of waiting. Producers
// return normally with either a complete value or nullopt, and own any required
// device quiescence/config locking. Publication grants no reset/reuse authority.
template <typename T>
class BootResource {
    static_assert(std::atomic<BootState>::is_always_lock_free);

   public:
    BootResource() = default;
    BootResource(const BootResource&) = delete;
    BootResource(BootResource&&) = delete;
    auto operator=(const BootResource&) -> BootResource& = delete;
    auto operator=(BootResource&&) -> BootResource& = delete;

    [[nodiscard]] auto state() const -> BootState { return state_.load(std::memory_order_acquire); }
    [[nodiscard]] auto get() const -> const T* { return state() == BootState::ready ? &value_ : nullptr; }

    template <typename Producer>
    [[nodiscard]] auto prepare(Producer producer) -> const T* {
        auto expected = state();
        if (expected == BootState::ready) return &value_;
        if (expected != BootState::empty ||
            !state_.compare_exchange_strong(expected, BootState::preparing, std::memory_order_acquire, std::memory_order_acquire))
            return expected == BootState::ready ? &value_ : nullptr;
        auto value = producer();
        if (!value) {
            state_.store(BootState::failed, std::memory_order_release);
            return nullptr;
        }
        value_ = std::move(*value);
        state_.store(BootState::ready, std::memory_order_release);
        return &value_;
    }

   private:
    std::atomic<BootState> state_{BootState::empty};
    T value_{};
};

}  // namespace ker::dev::pci::resource
