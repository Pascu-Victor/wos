#include "compartment_lifecycle.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/ipc_namespace.hpp>
#include <platform/sched/pid_namespace.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <syscalls_impl/process/exit.hpp>
#include <syscalls_impl/shm/shm.hpp>
#include <utility>
#include <vfs/mount.hpp>

namespace ker::mod::sched::compartment {
namespace {
using task::Task;
constexpr uint64_t RETRY_US = 10'000;
constexpr uint64_t BUSY_RETRY_US = 250'000;
enum class Phase : uint8_t { FREE, PENDING, RUNNING };
struct Retirement {
    pid_ns::NamespaceRef pid;
    ker::vfs::mount_ns::NamespaceRef mount;
    ipc::NamespaceRef ipc;
    Phase phase{Phase::FREE};
    uint64_t retry_at_us{};
};
// Each nonfree row owns a distinct PID slot, so an init retaining another slot
// always has capacity. Records never retain Tasks or acquire their locks.
std::array<Retirement, pid_ns::MAX_NAMESPACES> retirements{};
sys::Spinlock record_lock;
size_t cursor{};
uint64_t completed{};
uint64_t exit_requests{};
std::atomic<Task*> worker_task{};
std::atomic<bool> worker_starting{};
std::atomic<bool> worker_ready{};

struct Selection {
    const pid_ns::NamespaceRef& scope;
    bool include_unpublished{};
};

auto in_scope(Task* candidate, const Selection& selection) -> bool {
    auto const* scope = candidate != nullptr ? candidate->pid_namespace() : nullptr;
    return scope != nullptr && *scope == selection.scope && candidate->type == task::TaskType::PROCESS;
}

auto needs_request(Task* candidate, void* opaque) -> bool {
    auto const& selection = *static_cast<Selection*>(opaque);
    return in_scope(candidate, selection) && candidate->state.load(std::memory_order_acquire) == task::TaskState::ACTIVE &&
           (selection.include_unpublished || candidate->scheduler_published.load(std::memory_order_acquire)) &&
           !candidate->exit_in_progress.load(std::memory_order_acquire) && !candidate->exit_notify_ready.load(std::memory_order_acquire) &&
           !candidate->process_exit_requested.load(std::memory_order_acquire);
}

auto still_cleaning(Task* candidate, void* opaque) -> bool {
    auto const& selection = *static_cast<Selection*>(opaque);
    if (!in_scope(candidate, selection)) return false;
    auto const STATE = candidate->state.load(std::memory_order_acquire);
    // Include registry-before-runqueue publication and ongoing executor-owned
    // cleanup. Neither may disappear from the drain merely because no new
    // exit request is needed or because the init is already exiting.
    return (STATE == task::TaskState::ACTIVE || STATE == task::TaskState::EXITING) &&
           !candidate->exit_notify_ready.load(std::memory_order_acquire);
}

void wake_worker() {
    if (auto* worker = worker_task.load(std::memory_order_acquire); worker != nullptr) wake_task_from_event(worker);
}

template <typename Request>
void dispatch_once(uint64_t now, Request&& request, bool include_unpublished) {
    Retirement* selected = nullptr;
    uint64_t const FLAGS = record_lock.lock_irqsave();
    for (size_t offset = 0; offset < retirements.size(); ++offset) {
        auto const INDEX = (cursor + offset) % retirements.size();
        auto& row = retirements[INDEX];
        if (row.phase == Phase::PENDING && row.retry_at_us <= now) {
            row.phase = Phase::RUNNING;
            selected = &row;
            cursor = (INDEX + 1) % retirements.size();
            break;
        }
    }
    record_lock.unlock_irqrestore(FLAGS);
    if (selected == nullptr) return;

    // RUNNING grants this invocation exclusive access to stable retained refs.
    // No pool lock is held during registry selection, delivery or backend work.
    Selection selection{selected->pid, include_unpublished};
    size_t requested = 0;
    for (size_t work = 0; work < EXIT_REQUEST_BUDGET; ++work) {
        auto* target = find_active_task_lifetime_ref_if(needs_request, &selection);
        if (target == nullptr) break;
        if (request(*target, selected->pid)) ++requested;
        target->release();
    }
    auto* cleaning = find_active_task_lifetime_ref_if(still_cleaning, &selection);
    bool const CLEAN = cleaning == nullptr;
    if (cleaning != nullptr) cleaning->release();
    if (!selected->ipc.is_host()) ker::syscall::shm::shm_retire_namespace(selected->ipc);
    // Files and admitted operations may still own backing. EBUSY is expected;
    // leave the namespace closed and retry after those owners finish normally.
    int const MOUNT = selected->mount.is_host() ? 0 : ker::vfs::retire_mount_namespace_step(selected->mount);
    bool const DONE = CLEAN && selected->pid.operations_drained() && MOUNT == 0;
    if (!DONE) request_gc();

    Retirement retired;
    uint64_t const COMMIT_FLAGS = record_lock.lock_irqsave();
    exit_requests += requested;
    if (DONE) {
        retired = std::move(*selected);
        *selected = Retirement{};
        ++completed;
    } else {
        selected->retry_at_us = saturating_deadline_us(now, MOUNT == -EBUSY ? BUSY_RETRY_US : RETRY_US);
        selected->phase = Phase::PENDING;
    }
    record_lock.unlock_irqrestore(COMMIT_FLAGS);
    // Retired namespace refs drop after all service locks are released.
}

void worker_main() {
    for (;;) {
        dispatch_once(time::get_us(), ker::syscall::process::request_compartment_exit, false);
        if (status().pending != 0)
            kern_sleep_us(RETRY_US);
        else
            kern_block();
    }
}
}  // namespace

auto begin_init_retirement(Task& subject) -> int {
    auto const* pid = subject.pid_namespace();
    if (subject.type != task::TaskType::PROCESS || subject.is_thread || pid == nullptr || pid->is_host() || subject.namespace_tid() != 1)
        return 0;
    if (!subject.exit_in_progress.load(std::memory_order_acquire)) return -EINVAL;
    if (pid->retiring()) return 0;
    auto const* mount = subject.mount_namespace();
    auto const* ipc = subject.ipc_namespace();
    if (mount == nullptr || ipc == nullptr) return -EINVAL;
    Retirement candidate;
    int result = pid->retain(candidate.pid);
    if (result == 0) result = mount->retain(candidate.mount);
    if (result == 0) result = ipc->retain(candidate.ipc);
    if (result != 0) return result;
    if (!pid->begin_retirement()) return 0;
    if (!candidate.mount.is_host()) candidate.mount.close();
    if (!candidate.ipc.is_host()) candidate.ipc.close();
    candidate.phase = Phase::PENDING;

    bool installed = false;
    uint64_t const FLAGS = record_lock.lock_irqsave();
    for (auto& row : retirements) {
        if (row.phase != Phase::FREE) continue;
        row = std::move(candidate);
        installed = true;
        break;
    }
    record_lock.unlock_irqrestore(FLAGS);
    if (!installed) return -ENOSPC;  // Impossible while each row retains a distinct PID slot.
    wake_worker();
    return 0;
}

auto status() -> Status {
    uint64_t const FLAGS = record_lock.lock_irqsave();
    Status value{.completed = completed, .exit_requests = exit_requests};
    for (auto const& row : retirements)
        if (row.phase != Phase::FREE) ++value.pending;
    record_lock.unlock_irqrestore(FLAGS);
    return value;
}

auto init() -> bool {
    bool expected = false;
    if (!worker_starting.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        return worker_ready.load(std::memory_order_acquire);
    auto* worker = Task::create_kernel_thread("compartment_gc", worker_main);
    if (worker == nullptr) {
        worker_starting.store(false, std::memory_order_release);
        return false;
    }
    if (!post_task_balanced(worker)) return false;  // Bootstrap caller treats publication failure as fatal.
    worker_task.store(worker, std::memory_order_release);
    worker_ready.store(true, std::memory_order_release);
    return true;
}

#ifdef WOS_SELFTEST
void selftest_dispatch_once(void (*deliver)(Task&, void*), void* context, bool include_unpublished_fixtures) {
    // Each call represents a timer-expired retry; native boot tests do not
    // sleep with no current executor. Production uses actual monotonic time.
    dispatch_once(
        UINT64_MAX,
        [deliver, context](Task& target, const pid_ns::NamespaceRef& scope) {
            return ker::syscall::process::selftest_request_compartment_exit(target, scope, deliver, context);
        },
        include_unpublished_fixtures);
}
#endif
}  // namespace ker::mod::sched::compartment
