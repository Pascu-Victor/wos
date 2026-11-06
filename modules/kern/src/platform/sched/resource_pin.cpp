#include "resource_pin.hpp"

#include <abi/resource.hpp>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <platform/sched/resource.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <utility>

namespace ker::mod::sched::resource {
namespace detail {
class PinAccountingRuntime {
   public:
    static constexpr uint32_t ACCOUNT_CAPACITY = 4096;
    static constexpr uint64_t GENERATION_LIMIT = UINT64_MAX;

    constexpr PinAccountingRuntime() noexcept {
        for (uint32_t index = 0; index < ACCOUNT_CAPACITY; ++index) {
            cells_[index].next_free = index + 1 == ACCOUNT_CAPACITY ? NO_SLOT : index + 1;
        }
        statistics_.capacity = ACCOUNT_CAPACITY;
        statistics_.free_accounts = ACCOUNT_CAPACITY;
    }

    // The caller owns the canonical process resource lock before this lock.
    auto reserve_locked(task::Task& process, uint64_t bytes, PinCharge& empty) noexcept -> int {
        uint64_t const FLAGS = lock_.lock_irqsave();
        auto& owner = process.pin_account;
        Cell* cell = owner.slot_ == NO_SLOT ? nullptr : &owner_cell_locked(owner);
        auto& usage = process.resources.entries[MEMLOCK];
        usage.current = cell != nullptr ? cell->current_bytes : 0;
        int result = 0;
        if (owner.closed_ || (cell != nullptr && cell->closed.load(std::memory_order_relaxed))) {
            result = -ESRCH;
        } else if (cell == nullptr && free_head_ == NO_SLOT) {
            result = statistics_.exhausted_accounts == ACCOUNT_CAPACITY ? -EOVERFLOW : -ENOSPC;
        } else if (bytes > UINT64_MAX - statistics_.current_bytes || statistics_.charges == UINT64_MAX ||
                   (cell != nullptr && (cell->references == UINT64_MAX || cell->charges == UINT64_MAX))) {
            process.resources.deny(MEMLOCK, Denial::OVERFLOW);
            result = -EOVERFLOW;
        } else if (!process.resources.reserve(MEMLOCK, bytes)) {
            result = usage.reason == Denial::OVERFLOW ? -EOVERFLOW : -ENOMEM;
        } else {
            // No fallible step remains. A failed policy check never consumes
            // an account cell or generation.
            if (cell == nullptr) cell = &allocate_owner_locked(owner);
            cell->current_bytes = usage.current;
            ++cell->charges;
            ++cell->references;
            statistics_.current_bytes += bytes;
            ++statistics_.charges;
            if (statistics_.current_bytes > statistics_.peak_bytes) statistics_.peak_bytes = statistics_.current_bytes;
            saturating_increment(statistics_.admitted_charges);
            empty.slot_ = owner.slot_;
            empty.generation_ = owner.generation_;
            empty.bytes_ = bytes;
        }
        lock_.unlock_irqrestore(FLAGS);
        return result;
    }

    void refresh_locked(task::Task& process) noexcept {
        uint64_t const FLAGS = lock_.lock_irqsave();
        auto const& owner = process.pin_account;
        process.resources.entries[MEMLOCK].current = owner.slot_ == NO_SLOT ? 0 : owner_cell_locked(owner).current_bytes;
        lock_.unlock_irqrestore(FLAGS);
    }

    void close_locked(task::Task& process) noexcept {
        uint64_t const FLAGS = lock_.lock_irqsave();
        process.pin_account.closed_ = true;
        if (process.pin_account.slot_ != NO_SLOT) close_cell_locked(owner_cell_locked(process.pin_account));
        lock_.unlock_irqrestore(FLAGS);
    }

    void drop_owner(PinAccountOwner& owner) noexcept {
        owner.closed_ = true;
        if (owner.slot_ == NO_SLOT) return;
        uint64_t const FLAGS = lock_.lock_irqsave();
        auto& cell = owner_cell_locked(owner);
        uint32_t const SLOT = owner.slot_;
        close_cell_locked(cell);
        owner.slot_ = NO_SLOT;
        owner.generation_ = 0;
        cell.owner_present = false;
        drop_reference_locked(cell, SLOT);
        lock_.unlock_irqrestore(FLAGS);
    }

    void refund(PinCharge& charge) noexcept {
        if (charge.bytes_ == 0) return;
        uint64_t const FLAGS = lock_.lock_irqsave();
        auto& cell = charge_cell(charge);
        uint32_t const SLOT = charge.slot_;
        uint64_t const BYTES = charge.bytes_;
        bool const CLOSED = cell.closed.load(std::memory_order_relaxed);
        if (cell.references == 0 || cell.charges == 0 || BYTES > cell.current_bytes || statistics_.charges == 0 ||
            BYTES > statistics_.current_bytes || (CLOSED && BYTES > statistics_.closed_bytes))
            __builtin_trap();
        cell.current_bytes -= BYTES;
        --cell.charges;
        statistics_.current_bytes -= BYTES;
        --statistics_.charges;
        if (CLOSED) statistics_.closed_bytes -= BYTES;
        saturating_increment(statistics_.refunded_charges);
        charge.slot_ = NO_SLOT;
        charge.generation_ = 0;
        charge.bytes_ = 0;
        drop_reference_locked(cell, SLOT);
        lock_.unlock_irqrestore(FLAGS);
    }

    // A real charge retains this permanent cell, so its generation cannot
    // change during this load. No account lock is taken from future MM PIN.
    auto closed(const PinCharge& charge) const noexcept -> bool {
        return charge.bytes_ == 0 || charge_cell(charge).closed.load(std::memory_order_acquire);
    }

    auto snapshot() noexcept -> PinAccountingSnapshot {
        uint64_t const FLAGS = lock_.lock_irqsave();
        auto const result = statistics_;
        lock_.unlock_irqrestore(FLAGS);
        return result;
    }

   private:
    static constexpr uint32_t NO_SLOT = UINT32_MAX;
    static constexpr unsigned MEMLOCK = static_cast<unsigned>(Id::MEMLOCK);
    struct Cell {
        uint64_t generation{};
        uint64_t current_bytes{};
        uint64_t charges{};
        uint64_t references{};
        uint32_t next_free = NO_SLOT;
        bool owner_present{};
        std::atomic<bool> closed{true};
    };

    static void saturating_increment(uint64_t& value) noexcept {
        if (value != UINT64_MAX) ++value;
    }

    auto owner_cell_locked(const PinAccountOwner& owner) noexcept -> Cell& {
        if (owner.slot_ >= ACCOUNT_CAPACITY || owner.generation_ == 0) __builtin_trap();
        auto& cell = cells_[owner.slot_];
        if (cell.generation != owner.generation_ || !cell.owner_present || cell.references == 0) __builtin_trap();
        return cell;
    }

    auto charge_cell(const PinCharge& charge) noexcept -> Cell& {
        if (charge.slot_ >= ACCOUNT_CAPACITY || charge.generation_ == 0) __builtin_trap();
        auto& cell = cells_[charge.slot_];
        if (cell.generation != charge.generation_) __builtin_trap();
        return cell;
    }

    auto charge_cell(const PinCharge& charge) const noexcept -> const Cell& {
        if (charge.slot_ >= ACCOUNT_CAPACITY || charge.generation_ == 0) __builtin_trap();
        auto const& cell = cells_[charge.slot_];
        if (cell.generation != charge.generation_) __builtin_trap();
        return cell;
    }

    auto allocate_owner_locked(PinAccountOwner& owner) noexcept -> Cell& {
        if (owner.slot_ != NO_SLOT || free_head_ == NO_SLOT || statistics_.free_accounts == 0) __builtin_trap();
        uint32_t const SLOT = free_head_;
        auto& cell = cells_[SLOT];
        if (cell.references != 0 || cell.current_bytes != 0 || cell.charges != 0 || cell.owner_present ||
            !cell.closed.load(std::memory_order_relaxed) || cell.generation >= GENERATION_LIMIT)
            __builtin_trap();
        free_head_ = cell.next_free;
        cell.next_free = NO_SLOT;
        ++cell.generation;
        cell.references = 1;  // The canonical process member.
        cell.owner_present = true;
        cell.closed.store(false, std::memory_order_relaxed);
        owner.slot_ = SLOT;
        owner.generation_ = cell.generation;
        --statistics_.free_accounts;
        ++statistics_.open_accounts;
        return cell;
    }

    void close_cell_locked(Cell& cell) noexcept {
        if (cell.closed.load(std::memory_order_relaxed)) return;
        if (!cell.owner_present || statistics_.open_accounts == 0 || cell.current_bytes > UINT64_MAX - statistics_.closed_bytes)
            __builtin_trap();
        cell.closed.store(true, std::memory_order_release);
        --statistics_.open_accounts;
        ++statistics_.closed_accounts;
        statistics_.closed_bytes += cell.current_bytes;
    }

    void drop_reference_locked(Cell& cell, uint32_t slot) noexcept {
        if (cell.references == 0) __builtin_trap();
        if (--cell.references != 0) return;
        if (cell.owner_present || cell.current_bytes != 0 || cell.charges != 0 || !cell.closed.load(std::memory_order_relaxed) ||
            statistics_.closed_accounts == 0)
            __builtin_trap();
        --statistics_.closed_accounts;
        if (cell.generation == GENERATION_LIMIT) {
            ++statistics_.exhausted_accounts;
        } else {
            cell.next_free = free_head_;
            free_head_ = slot;
            ++statistics_.free_accounts;
        }
    }

    sys::Spinlock lock_;
    std::array<Cell, ACCOUNT_CAPACITY> cells_{};
    uint32_t free_head_{};
    PinAccountingSnapshot statistics_{};
};

static_assert(PinAccountingRuntime::ACCOUNT_CAPACITY > 0 && PinAccountingRuntime::ACCOUNT_CAPACITY < UINT32_MAX);
static_assert(PinAccountingRuntime::GENERATION_LIMIT > 0);
static_assert(std::atomic<bool>::is_always_lock_free);
static_assert(sizeof(PinAccountingRuntime) <= 512 * 1024);
}  // namespace detail

namespace {
constinit detail::PinAccountingRuntime pin_runtime;
constexpr uint64_t PIN_PAGE_BYTES = 4096;
}  // namespace

PinCharge::~PinCharge() { reset(); }
PinCharge::PinCharge(PinCharge&& other) noexcept
    : slot_(std::exchange(other.slot_, UINT32_MAX)),
      generation_(std::exchange(other.generation_, 0)),
      bytes_(std::exchange(other.bytes_, 0)) {}
auto PinCharge::operator=(PinCharge&& other) noexcept -> PinCharge& {
    if (this != &other) {
        reset();
        slot_ = std::exchange(other.slot_, UINT32_MAX);
        generation_ = std::exchange(other.generation_, 0);
        bytes_ = std::exchange(other.bytes_, 0);
    }
    return *this;
}
auto PinCharge::closed() const noexcept -> bool { return pin_runtime.closed(*this); }
void PinCharge::reset() noexcept { pin_runtime.refund(*this); }
PinAccountOwner::~PinAccountOwner() { pin_runtime.drop_owner(*this); }

auto reserve_pin_charge(task::Task& subject, uint64_t bytes, PinCharge& empty) -> int {
    if (empty || bytes == 0 || bytes % PIN_PAGE_BYTES != 0) return -EINVAL;
    if (subject.type != task::TaskType::PROCESS || subject.state.load(std::memory_order_acquire) != task::TaskState::ACTIVE ||
        subject.exit_in_progress.load(std::memory_order_acquire))
        return -ESRCH;
    Owner owner(&subject);
    auto* process = owner.get();
    if (process == nullptr || process->is_thread || process->type != task::TaskType::PROCESS) return -ESRCH;
    uint64_t const FLAGS = process->resource_lock.lock_irqsave();
    bool const AVAILABLE = subject.state.load(std::memory_order_acquire) == task::TaskState::ACTIVE &&
                           !subject.exit_in_progress.load(std::memory_order_acquire) &&
                           process->state.load(std::memory_order_acquire) == task::TaskState::ACTIVE &&
                           !process->exit_in_progress.load(std::memory_order_acquire) && !process->resource_cpu_admission_closed &&
                           !process->resource_exec_admission_closed;
    int const RESULT = AVAILABLE ? pin_runtime.reserve_locked(*process, bytes, empty) : -ESRCH;
    process->resource_lock.unlock_irqrestore(FLAGS);
    return RESULT;
}

void refresh_pin_usage_locked(task::Task& process) noexcept { pin_runtime.refresh_locked(process); }
void close_pin_account_locked(task::Task& process) noexcept { pin_runtime.close_locked(process); }
auto pin_accounting_snapshot() noexcept -> PinAccountingSnapshot { return pin_runtime.snapshot(); }
}  // namespace ker::mod::sched::resource
