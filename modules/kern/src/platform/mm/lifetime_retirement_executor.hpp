#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/mm/file_mapping_lifetime.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::mod::sched::task {
struct Task;
}
namespace ker::mod::sched {

// Only the permanent executor may install/remove this exact retained Task.
// The implementation shares the scheduler registry/shutdown publication lock.
class FileRetirementSchedulerPort {
    friend class mm::file_mapping::LifetimeRetirementExecutor;
    static auto install(task::Task& worker, void (*entry)(), void* context) noexcept -> bool;
    static auto remove(task::Task& worker) noexcept -> bool;
};
}  // namespace ker::mod::sched

namespace ker::mod::mm::file_mapping {

struct LifetimeRetirementTicket {
   private:
    friend class LifetimeRetirementExecutor;
    enum class State : uint8_t { FREE, RESERVED, READY, RUNNING, DONE, EXHAUSTED };
    LifetimeRetirementExecutor* owner_{};
    LifetimeRetirementTicket* next_{};
    LifetimeRetirementRun run_{};
    void* argument_{};
    State state_{State::FREE};
    uint64_t generation_{};
    uint32_t observers_{};
    int callback_result_{};
};

struct LifetimeRetirementSnapshot {
    bool initialized{};
    bool available{};
    bool admission_open{};
    bool stop_requested{};
    bool loop_finished{};
    uint32_t outstanding{};  // RESERVED + READY + RUNNING, until callback return.
    uint32_t reserved{};
    uint32_t ready{};
    uint32_t running{};
    uint64_t completed{};  // Saturating diagnostics, not a drain predicate.
    uint64_t cancelled{};  // Unused reservations; no callback return is claimed.
    uint64_t close_errors{};
    uint64_t protocol_errors{};
    int first_close_result{};
    uint32_t completed_storage_slots{};  // DONE observers, not callback work.
    uint64_t observer_handles{};
    uint32_t exhausted_slots{};
};

struct LifetimeRetirementDrain {
    int result{};
    bool joined{};
    LifetimeRetirementSnapshot snapshot{};
};

class LifetimeRetirementExecutor {
   public:
    static constexpr size_t CAPACITY = 4096;
    static constexpr uint64_t POLL_US = 1000;
    static constexpr uint64_t GENERATION_LIMIT = UINT64_MAX;
    static constexpr uint32_t OBSERVER_LIMIT = UINT32_MAX;

    // Serialized boot, after PID1 and before AP/user admission. Permanent state
    // is never replaced. Failure leaves new native selection unavailable.
    static auto initialize_boot() -> bool;
    static auto port() noexcept -> const LifetimeRetirementPort*;
    static void close_new_reservations() noexcept;
    static auto snapshot() noexcept -> LifetimeRetirementSnapshot;
    static auto is_current_worker() noexcept -> bool;

    // Ordinary schedulable controller, no MM/root/publication/backend guard.
    // Closes new reservations, requests stop only once all producers/callbacks
    // have retired, and joins only after actual worker DEAD. EAGAIN is pending;
    // no GC/backend/wait occurs here. Concurrent polls use one exact join claim.
    static auto poll_shutdown() -> LifetimeRetirementDrain;
    // Optional polling wrapper. It never runs GC/backend progress inline. The
    // caller must keep providers alive and retire every native producer/root
    // before generic worker stop. A timeout preserves all queue/Task ownership.
    static auto finish_shutdown(uint64_t timeout_us) -> LifetimeRetirementDrain;

    LifetimeRetirementExecutor(const LifetimeRetirementExecutor&) = delete;
    auto operator=(const LifetimeRetirementExecutor&) -> LifetimeRetirementExecutor& = delete;

   private:
    friend class RetirementCompletion;
    constexpr LifetimeRetirementExecutor() noexcept : port_{this, reserve_adapter, submit_adapter, observe_adapter, cancel_adapter} {}
    static LifetimeRetirementExecutor singleton_;
    static void worker_entry();
    void drain_loop();
    static auto reserve_adapter(void* context, LifetimeRetirementRun run, void* argument) noexcept -> LifetimeRetirementTicket*;
    static void submit_adapter(void* context, LifetimeRetirementTicket& ticket) noexcept;
    static auto cancel_adapter(void* context, LifetimeRetirementTicket& ticket, LifetimeRetirementRun run, void* argument) noexcept -> bool;
    auto reserve(LifetimeRetirementRun run, void* argument) noexcept -> LifetimeRetirementTicket*;
    void submit_ready(LifetimeRetirementTicket& ticket) noexcept;
    auto cancel_reserved(LifetimeRetirementTicket& ticket, LifetimeRetirementRun run, void* argument) noexcept -> bool;
    auto snapshot_locked() const noexcept -> LifetimeRetirementSnapshot;
    void note_protocol_error_locked() noexcept;
    static auto observe_adapter(void* context, LifetimeRetirementTicket& ticket, LifetimeRetirementRun run, void* argument,
                                RetirementCompletion& empty) noexcept -> bool;
    auto observe_return(LifetimeRetirementTicket& ticket, LifetimeRetirementRun run, void* argument, RetirementCompletion& empty) noexcept
        -> bool;
    auto query_completion(const LifetimeRetirementTicket* ticket, uint64_t generation) noexcept -> RetirementReturn;
    void release_completion(LifetimeRetirementTicket* ticket, uint64_t generation) noexcept;
    auto contains_ticket(const LifetimeRetirementTicket* ticket) const noexcept -> bool;
    void recycle_completed_locked(LifetimeRetirementTicket& ticket) noexcept;

    sys::Spinlock lock_{};
    const LifetimeRetirementPort port_;
    std::array<LifetimeRetirementTicket, CAPACITY> tickets_{};
    LifetimeRetirementTicket* free_{};
    LifetimeRetirementTicket* head_{};
    LifetimeRetirementTicket* tail_{};
    sched::task::Task* worker_{};  // Queue-owned lifetime ref through exact join.
    bool initialized_{};
    bool available_{};
    bool admission_open_{};
    bool stop_requested_{};
    bool joining_{};
    std::atomic<bool> loop_finished_{false};
    uint32_t outstanding_{};
    uint32_t ready_{};
    uint32_t running_{};
    uint64_t completed_{};
    uint64_t cancelled_{};
    uint64_t close_errors_{};
    uint64_t protocol_errors_{};
    int first_close_result_{};
    uint32_t completed_storage_slots_{};
    uint64_t observer_handles_{};
    uint32_t exhausted_slots_{};
};

static_assert(sizeof(LifetimeRetirementTicket) * LifetimeRetirementExecutor::CAPACITY <= 256 * 1024);
static_assert(LifetimeRetirementExecutor::GENERATION_LIMIT > 0);
static_assert(LifetimeRetirementExecutor::OBSERVER_LIMIT > 0);
static_assert(LifetimeRetirementExecutor::CAPACITY <= UINT32_MAX);
}  // namespace ker::mod::mm::file_mapping
