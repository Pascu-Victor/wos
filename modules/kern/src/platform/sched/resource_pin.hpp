#pragma once

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::sched::resource {
namespace detail {
class PinAccountingRuntime;
}

// Independent quota ownership only. This does not retain a page, root, Task or
// DMA mapping. Its eventual frame owner must release backing before reset().
class PinCharge {
   public:
    PinCharge() = default;
    ~PinCharge();
    PinCharge(const PinCharge&) = delete;
    auto operator=(const PinCharge&) -> PinCharge& = delete;
    PinCharge(PinCharge&&) noexcept;
    auto operator=(PinCharge&&) noexcept -> PinCharge&;
    [[nodiscard]] explicit operator bool() const noexcept { return bytes_ != 0; }
    [[nodiscard]] auto bytes() const noexcept -> uint64_t { return bytes_; }
    // Lock-free copied observation while this charge retains the exact cell.
    // Empty is closed. This alone never authorizes VM/DMA publication.
    [[nodiscard]] auto closed() const noexcept -> bool;
    // Bounded IRQ-safe quota refund, with no Task access or backing release.
    void reset() noexcept;

   private:
    friend class detail::PinAccountingRuntime;
    uint32_t slot_ = UINT32_MAX;
    uint64_t generation_{};
    uint64_t bytes_{};
};

// The canonical process owns one lazy account. Threads resolve that process;
// fork constructs a fresh owner and native exec retains the existing member.
class PinAccountOwner {
   public:
    PinAccountOwner() = default;
    ~PinAccountOwner();
    PinAccountOwner(const PinAccountOwner&) = delete;
    auto operator=(const PinAccountOwner&) -> PinAccountOwner& = delete;
    PinAccountOwner(PinAccountOwner&&) = delete;
    auto operator=(PinAccountOwner&&) -> PinAccountOwner& = delete;

   private:
    friend class detail::PinAccountingRuntime;
    uint32_t slot_ = UINT32_MAX;
    uint64_t generation_{};
    bool closed_{};
};

struct PinAccountingSnapshot {
    uint64_t current_bytes{};
    uint64_t closed_bytes{};
    uint64_t peak_bytes{};
    uint64_t charges{};
    uint64_t admitted_charges{};  // Saturating diagnostics, not drain predicates.
    uint64_t refunded_charges{};
    uint32_t capacity{};
    uint32_t free_accounts{};
    uint32_t open_accounts{};
    uint32_t closed_accounts{};
    uint32_t exhausted_accounts{};
};

// Caller keeps subject alive, with no MM/backend/resource lock held. Resolve
// its real canonical process, then serialize policy -> account admission.
// Nonzero whole-page bytes and an empty output are required. No backing or
// hardware permission follows from a successful quota reservation.
[[nodiscard]] auto reserve_pin_charge(task::Task& subject, uint64_t bytes, PinCharge& empty) -> int;
// Canonical process resource_lock is already held. Neither helper acquires it.
// Order is resource_lock -> account lock; no allocation, callbacks or waits.
void refresh_pin_usage_locked(task::Task& process) noexcept;
void close_pin_account_locked(task::Task& process) noexcept;
[[nodiscard]] auto pin_accounting_snapshot() noexcept -> PinAccountingSnapshot;
}  // namespace ker::mod::sched::resource
