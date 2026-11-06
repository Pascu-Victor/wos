#include <atomic>
#include <dev/e1000e/e1000e.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <string_view>
#include <test/e1000_mmio_runtime_ktest.hpp>

namespace ker::test {
namespace {
namespace sched = ker::mod::sched;
namespace task = sched::task;
using log = ker::mod::dbg::logger<"e1000-mmio-test">;
std::atomic<bool> started{false};

void require(bool condition, const char* reason) {
    if (condition) return;
    log::error("E1000_MMIO_RUNTIME FAIL %s", reason);
    ker::mod::dbg::panic_handler("scheduled e1000 MMIO selftest failed");
}

void wait_for_init_mounts() {
    auto const DEADLINE = sched::saturating_deadline_us(ker::mod::time::get_us(), 30'000'000);
    for (;;) {
        auto* init = sched::find_task_by_pid_safe(1);
        if (init != nullptr) {
            task::Task::FilesystemContext context;
            bool const READY = init->capture_filesystem_context(context) && context.root_len == 7 &&
                               std::string_view(context.root.data(), context.root_len) == "/rootfs";
            init->release();
            if (READY) {
                log::info("E1000_MMIO_RUNTIME INIT_READY root=/rootfs");
                return;
            }
        }
        require(ker::mod::time::get_us() < DEADLINE, "PID 1 finishes root pivot before normal shutdown");
        sched::kern_sleep_us(10000);
    }
}

void runtime_entry() {
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        require(ker::mod::mm::phys::can_wait_for_reclaim(), "ordinary scheduled diagnostic context");
        log::info("E1000_MMIO_RUNTIME BEGIN first_retirement=init-sigusr2");
        require(ker::dev::e1000e::e1000e_mmio_runtime_check(), "real typed 82574 traffic and joined register borrowers");
        wait_for_init_mounts();
    }
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        auto* init = sched::find_task_by_pid_safe(1);
        require(init != nullptr, "retain PID 1 for normal userspace shutdown request");
        constexpr int WOS_SIGUSR2 = 12;
        init->signal_generate_pending_mask(uint64_t{1} << (WOS_SIGUSR2 - 1));
        sched::wake_task_for_signal(init);
        init->release();
        log::info("E1000_MMIO_RUNTIME POWEROFF_REQUESTED via=init-sigusr2 helpers_joined=1 first_retirement_pending=1");
    }
    sched::exit_current_kernel_thread();
}
}  // namespace

auto start_e1000_mmio_runtime_selftest() -> bool {
    bool expected = false;
    if (!started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return false;
    auto* worker = task::Task::create_kernel_thread("e1000_mmio_runtime_selftest", runtime_entry);
    if (worker == nullptr || !sched::post_task_balanced(worker)) {
        if (worker != nullptr) {
            if (!worker->scheduler_published.load(std::memory_order_acquire) && worker->ref_count.load(std::memory_order_acquire) == 1 &&
                worker->sched_queue == task::Task::sched_queue::NONE && worker->heap_index == -1 && worker->sched_next == nullptr) {
                task::destroy_unpublished_user_thread(worker);
            } else {
                // Failed runnable publication can leave an admitted registry reader.
                // Keep the initial owner until reboot rather than free a borrowed Task.
                log::error("MMIO coordinator retains uncertain unpublished lifetime pid=%lu refs=%u", worker->pid,
                           worker->ref_count.load(std::memory_order_acquire));
            }
        }
        started.store(false, std::memory_order_release);
        return false;
    }
    return true;
}
}  // namespace ker::test
