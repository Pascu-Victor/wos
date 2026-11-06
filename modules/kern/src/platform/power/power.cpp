#include "power.hpp"

#include <abi/callnums/power.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <dev/ahci.hpp>
#include <dev/e1000e/e1000e.hpp>
#include <dev/ivshmem/ivshmem_net.hpp>
#include <dev/usb/xhci.hpp>
#include <dev/virtio/virtio_net.hpp>
#include <mod/io/port/port.hpp>
#include <net/wki/wki.hpp>
#include <platform/acpi/power.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/cpu_maintenance_controller.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/compartment_host.hpp>
#include <platform/smt/smt.hpp>
#include <vfs/vfs.hpp>

namespace ker::mod::power {
[[noreturn]] void run_finalizer(ShutdownAction action);

// Defined only beside the actual accepted nonreturning power continuation.
class FinalizerExecution final {
    friend class mm::cpu_maintenance::detail::Controller;
    friend void run_finalizer(ShutdownAction action);
    sched::KernelShutdownHold hold_;
    ShutdownAction action_;
    uint64_t lineage_{1};
    bool live_{true};
    bool sealed_{};
    explicit FinalizerExecution(ShutdownAction action);
    [[noreturn]] void run();
    FinalizerExecution(const FinalizerExecution&) = delete;
    FinalizerExecution(FinalizerExecution&&) = delete;
    auto operator=(const FinalizerExecution&) -> FinalizerExecution& = delete;
    auto operator=(FinalizerExecution&&) -> FinalizerExecution& = delete;
};
namespace {

using log = ker::mod::dbg::logger<"power">;

constexpr int WOS_SIGKILL = 9;
constexpr int WOS_SIGTERM = 15;
constexpr uint64_t TERM_WAIT_US = 2'000'000;
constexpr uint64_t KILL_WAIT_US = 1'000'000;
constexpr uint64_t KERNEL_THREAD_WAIT_US = 2'000'000;
constexpr uint64_t FILE_RETIREMENT_WAIT_US = 5'000'000;
constexpr uint64_t FILE_RETIREMENT_JOIN_SLICE_US = 100'000;

std::atomic<ShutdownPhase> g_phase{ShutdownPhase::IDLE};
std::atomic<ShutdownAction> g_action{ShutdownAction::NONE};
std::atomic<bool> g_finalizer_issued{};

auto command_to_action(uint64_t cmd, ShutdownAction& action) -> int {
    switch (cmd) {
        case ker::abi::power::RB_AUTOBOOT:
            action = ShutdownAction::REBOOT;
            return 0;
        case ker::abi::power::RB_POWER_OFF:
            action = ShutdownAction::POWEROFF;
            return 0;
        case ker::abi::power::RB_HALT_SYSTEM:
            action = ShutdownAction::HALT;
            return 0;
        case ker::abi::power::RB_ENABLE_CAD:
        case ker::abi::power::RB_DISABLE_CAD:
            action = ShutdownAction::NONE;
            return 0;
        default:
            return -EINVAL;
    }
}

auto current_process_pid(const sched::task::Task* task) -> uint64_t { return task != nullptr ? sched::task::process_pid(*task) : 0; }

auto count_shutdown_targets(uint64_t excluded_pid, uint64_t excluded_owner_pid) -> size_t {
    return sched::signal_visible_processes_except(excluded_pid, excluded_owner_pid, 0, true);
}

auto authorize_current_power(const sched::task::Task* subject) -> int {
    auto const* task = subject;
    if (task == nullptr) {
        return -ESRCH;
    }
    int const SCOPE = security::compartment::host_namespace_admission(task);
    if (SCOPE != 0) return SCOPE;
    if (!task->credentials().allows(ker::mod::security::Authority::POWER)) {
        return -EPERM;
    }
    return 0;
}

auto quiesce_kernel_threads(uint64_t timeout_us) -> bool {
    auto const RESULT = sched::request_kernel_threads_shutdown(timeout_us);
    if (RESULT.requested != 0) {
        log::info("shutdown requested stop for %lu kernel threads", static_cast<unsigned long>(RESULT.requested));
    }
    if (RESULT.remaining != 0) {
        log::error("shutdown has %lu kernel threads still active; dependent teardown refused",
                   static_cast<unsigned long>(RESULT.remaining));
    }
    return RESULT.remaining == 0;
}

auto drain_file_retirement_before_provider_stop(uint64_t join_timeout_us) -> int {
    using mm::file_mapping::LifetimeRetirementExecutor;
    auto const BEFORE = LifetimeRetirementExecutor::snapshot();
    if (!BEFORE.initialized) return 0;  // Existing pre-scheduler diagnostic path.
    bool const ROOTS_DRAINED = sched::task::close_native_file_mapping_admission();
    LifetimeRetirementExecutor::close_new_reservations();
    // Native publication is closed. Its exact unpublished owner must finish
    // root/producer retirement before shutdown may stop the required services.
    // Refusal preserves every existing queue/Task/provider owner for a retry.
    if (!ROOTS_DRAINED || LifetimeRetirementExecutor::snapshot().outstanding != 0 || sched::task::native_unpublished_retirements_pending())
        return -EBUSY;
    auto* current = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    if (current == nullptr || sched::preempt_count() != 0 || current->scheduler_transition_active.load(std::memory_order_acquire) ||
        current->deferred_task_switch || current->wants_block)
        return -EWOULDBLOCK;
    bool const IRQ_ENABLED = sched::interrupts_enabled();
    if (!IRQ_ENABLED) asm volatile("sti" ::: "memory");
    auto const DRAIN = LifetimeRetirementExecutor::finish_shutdown(join_timeout_us);
    if (!IRQ_ENABLED) asm volatile("cli" ::: "memory");
    if (!DRAIN.joined) return DRAIN.result == -ETIMEDOUT || DRAIN.result == -EAGAIN ? -EBUSY : DRAIN.result;
    if (DRAIN.snapshot.protocol_errors != 0) return -EPROTO;
    if (DRAIN.result != 0) log::warn("file retirement completed with destructive close result %d", DRAIN.result);
    return 0;
}

void clear_current_finalizer_sched_state() {
    auto* current = sched::get_current_task();
    if (current == nullptr) {
        return;
    }

    current->deferred_task_switch = false;
    current->yield_switch = false;
    current->wakeup_pending.store(false, std::memory_order_release);
    current->set_voluntary_blocked(false);
    current->wants_block = false;
    current->wake_at_us = 0;
    current->clear_wait_channel();
}

void wait_for_shutdown_progress() { sched::kern_yield(); }

auto wait_for_process_quiescence(uint64_t excluded_pid, uint64_t excluded_owner_pid, uint64_t timeout_us) -> bool {
    uint64_t const START = ker::mod::time::get_us();
    while (count_shutdown_targets(excluded_pid, excluded_owner_pid) != 0) {
        if (ker::mod::time::get_us() - START >= timeout_us) {
            return false;
        }
        wait_for_shutdown_progress();
    }
    return true;
}

auto teardown_processes() -> bool {
    auto* current = sched::get_current_task();
    uint64_t const EXCLUDED_PID = current != nullptr ? current->pid : 0;
    uint64_t const EXCLUDED_OWNER_PID = current_process_pid(current);

    g_phase.store(ShutdownPhase::PROCESS_TEARDOWN, std::memory_order_release);
    size_t const TERM_COUNT = sched::signal_visible_processes_except(EXCLUDED_PID, EXCLUDED_OWNER_PID, WOS_SIGTERM, true);
    log::info("shutdown SIGTERM sent to %lu processes", static_cast<unsigned long>(TERM_COUNT));
    if (wait_for_process_quiescence(EXCLUDED_PID, EXCLUDED_OWNER_PID, TERM_WAIT_US)) return true;

    size_t const REMAINING = count_shutdown_targets(EXCLUDED_PID, EXCLUDED_OWNER_PID);
    if (REMAINING == 0) {
        return true;
    }

    size_t const KILL_COUNT = sched::signal_visible_processes_except(EXCLUDED_PID, EXCLUDED_OWNER_PID, WOS_SIGKILL, true);
    log::warn("shutdown SIGKILL sent to %lu remaining processes", static_cast<unsigned long>(KILL_COUNT));
    return wait_for_process_quiescence(EXCLUDED_PID, EXCLUDED_OWNER_PID, KILL_WAIT_US);
}

auto wait_for_file_retirement() -> int {
    uint64_t const START = ker::mod::time::get_us();
    for (;;) {
        uint64_t const ELAPSED = ker::mod::time::get_us() - START;
        if (ELAPSED >= FILE_RETIREMENT_WAIT_US) return -ETIMEDOUT;
        uint64_t const REMAINING = FILE_RETIREMENT_WAIT_US - ELAPSED;
        int const RESULT = drain_file_retirement_before_provider_stop(
            REMAINING < FILE_RETIREMENT_JOIN_SLICE_US ? REMAINING : FILE_RETIREMENT_JOIN_SLICE_US);
        clear_current_finalizer_sched_state();
        if (RESULT != -EBUSY) return RESULT;
        // Exit and asynchronous GC may still own a native root or a prepared
        // callback. Keep every provider running until those owners return.
        wait_for_shutdown_progress();
        clear_current_finalizer_sched_state();
    }
}

auto current_root_path() -> const char* {
    auto* current = sched::get_current_task();
    if (current == nullptr || current->root.front() == '\0') {
        return "/";
    }
    return current->root.data();
}

[[noreturn]] void halt_forever() {
    asm volatile("cli" ::: "memory");
    for (;;) {
        asm volatile("hlt" ::: "memory");
    }
}

void io_delay_loop() {
    for (uint32_t i = 0; i < 100000; ++i) {
        asm volatile("pause" ::: "memory");
    }
}

[[noreturn]] void platform_reboot() {
    if (ker::mod::acpi::power::reboot_via_fadt()) {
        io_delay_loop();
    }
    outb(0xcf9, 0x02);
    io_wait();
    outb(0xcf9, 0x06);
    io_delay_loop();
    outb(0x64, 0xfe);
    halt_forever();
}

[[noreturn]] void platform_poweroff() {
    if (ker::mod::acpi::power::poweroff_via_s5()) {
        io_delay_loop();
    }
    outw(0x604, 0x2000);
    io_wait();
    outw(0xb004, 0x2000);
    halt_forever();
}

}  // namespace

FinalizerExecution::FinalizerExecution(ShutdownAction action) : hold_(sched::hold_kernel_shutdown()), action_(action) {
    if (g_phase.load(std::memory_order_acquire) != ShutdownPhase::FINALIZING || g_action.load(std::memory_order_acquire) != action ||
        action == ShutdownAction::NONE || g_finalizer_issued.exchange(true, std::memory_order_acq_rel)) {
        dbg::panic_handler("shutdown finalizer lacks accepted execution; providers retained");
        halt_forever();
    }
}

[[noreturn]] void run_finalizer(ShutdownAction action) {
    FinalizerExecution execution(action);
    execution.run();
}

[[noreturn]] void FinalizerExecution::run() {
    auto* const CURRENT = sched::get_current_task();
    if (CURRENT == nullptr || CURRENT->is_thread || !sched::resource::close_idle_thread_cpu_admission(*CURRENT)) {
        dbg::panic_handler("shutdown finalizer could not seal sole caller CPU admission; providers retained");
        halt_forever();
    }
    log::info("system shutdown finalizer starting action=%lu", static_cast<unsigned long>(action_));

    // The power syscall enters with IF clear. Admission has established an
    // ordinary task with no preemption-disabled ownership; this finalizer
    // never returns, and its completion/retirement waits require IRQ progress.
    asm volatile("sti" ::: "memory");
    sealed_ = true;
    {
        const mm::cpu_maintenance::detail::Controller controller(*this);

        bool const CONSUMERS_DRAINED = teardown_processes();
        clear_current_finalizer_sched_state();
        if (!CONSUMERS_DRAINED) {
            dbg::panic_handler("shutdown userspace consumers remain; providers retained");
            halt_forever();
        }
        int const FILE_DRAIN = wait_for_file_retirement();
        clear_current_finalizer_sched_state();
        if (FILE_DRAIN != 0) {
            log::error("shutdown native File retirement returned %d; providers retained", FILE_DRAIN);
            dbg::panic_handler("shutdown native File retirement did not join; providers retained");
            halt_forever();
        }
        // An already admitted construction may have published after the first
        // census. Admission is now closed and native retirement has joined, so
        // refuse any late consumer before requesting provider shutdown.
        if (count_shutdown_targets(CURRENT->pid, current_process_pid(CURRENT)) != 0) {
            dbg::panic_handler("shutdown late userspace consumers remain; providers retained");
            halt_forever();
        }

        bool const KERNEL_THREADS_DRAINED = quiesce_kernel_threads(KERNEL_THREAD_WAIT_US);
        clear_current_finalizer_sched_state();
        if (!KERNEL_THREADS_DRAINED) {
            dbg::panic_handler("shutdown kernel workers remain; WKI/VFS/devices retained");
            halt_forever();
        }

        ker::net::wki::wki_shutdown();
        clear_current_finalizer_sched_state();

        g_phase.store(ShutdownPhase::VFS_SYNC, std::memory_order_release);
        int const SYNC_RET = ker::vfs::vfs_shutdown_sync();
        clear_current_finalizer_sched_state();
        if (SYNC_RET != 0) {
            log::warn("shutdown vfs sync returned %d", SYNC_RET);
        }

        g_phase.store(ShutdownPhase::ROOT_UNMOUNT, std::memory_order_release);
        int const UMOUNT_RET = ker::vfs::vfs_shutdown_unmount_all(current_root_path());
        clear_current_finalizer_sched_state();
        if (UMOUNT_RET != 0) {
            log::warn("shutdown root/mount unmount returned %d; issuing final sync", UMOUNT_RET);
            (void)ker::vfs::vfs_shutdown_sync();
            clear_current_finalizer_sched_state();
        }

        int const AHCI_RET = ker::dev::ahci::ahci_shutdown();
        clear_current_finalizer_sched_state();
        if (AHCI_RET != 0) log::warn("AHCI shutdown returned %d; unproven DMA backing retained", AHCI_RET);

        int const VIRTIO_RET = ker::dev::virtio::virtio_net_shutdown();
        clear_current_finalizer_sched_state();
        if (VIRTIO_RET != 0) log::warn("virtio shutdown returned %d; unproven DMA backing retained", VIRTIO_RET);

        int E1000_RET = 0;
        for (unsigned attempt = 0; attempt < 3; ++attempt) {
            E1000_RET = ker::dev::e1000e::e1000e_shutdown(controller);
            clear_current_finalizer_sched_state();
            if (E1000_RET != -EAGAIN) break;
        }
        if (E1000_RET != 0) log::warn("e1000 shutdown returned %d; unproven DMA backing retained", E1000_RET);

        int const IVSHMEM_RET = ker::dev::ivshmem::ivshmem_net_shutdown();
        clear_current_finalizer_sched_state();
        if (IVSHMEM_RET != 0) log::warn("ivshmem shutdown returned %d; callback/IRQ ownership retained", IVSHMEM_RET);

        int const XHCI_RET = ker::dev::usb::xhci_shutdown();
        clear_current_finalizer_sched_state();
        if (XHCI_RET != 0) log::warn("xHCI shutdown returned %d; unproven DMA backing retained", XHCI_RET);

    }  // controller expires before CPU service is stopped
    live_ = false;
    g_phase.store(ShutdownPhase::CPU_QUIESCE, std::memory_order_release);
    clear_current_finalizer_sched_state();
    smt::halt_other_cores();

    g_phase.store(ShutdownPhase::PLATFORM_ACTION, std::memory_order_release);
    switch (action_) {
        case ShutdownAction::REBOOT:
            platform_reboot();
        case ShutdownAction::POWEROFF:
            platform_poweroff();
        case ShutdownAction::HALT:
        case ShutdownAction::NONE:
        default:
            halt_forever();
    }
}

auto shutdown_in_progress() -> bool { return g_phase.load(std::memory_order_acquire) != ShutdownPhase::IDLE; }

auto shutdown_phase() -> ShutdownPhase { return g_phase.load(std::memory_order_acquire); }

namespace {

auto prepare_shutdown(sched::task::Task* subject) -> int {
    int const AUTHORITY_RET = authorize_current_power(subject);
    if (AUTHORITY_RET != 0) {
        return AUTHORITY_RET;
    }
    ShutdownPhase phase = g_phase.load(std::memory_order_acquire);
    for (;;) {
        if (phase == ShutdownPhase::IDLE) {
            if (g_phase.compare_exchange_weak(phase, ShutdownPhase::PREPARING, std::memory_order_acq_rel, std::memory_order_acquire)) {
                break;
            }
            continue;
        }
        if (phase == ShutdownPhase::PREPARING || phase == ShutdownPhase::FINALIZING || phase == ShutdownPhase::PROCESS_TEARDOWN ||
            phase == ShutdownPhase::VFS_SYNC || phase == ShutdownPhase::ROOT_UNMOUNT || phase == ShutdownPhase::CPU_QUIESCE ||
            phase == ShutdownPhase::PLATFORM_ACTION) {
            break;
        }
        return -EBUSY;
    }

    // This operation returns to PID1 while journald and PID1 itself may still
    // fault or perform I/O. PREPARING closes new consumer creation, while all
    // fault, retirement and completion providers stay available until REBOOT.
    return 0;
}

auto begin_finalization(ShutdownAction action) -> int {
    // Do not enable IRQs or claim shutdown while a caller owns a locked or
    // incomplete scheduler transition. IF itself may be clear at syscall entry.
    auto* const CURRENT = sched::get_current_task();
    if (!sched::has_run_queues() || sched::preempt_count() != 0 || CURRENT == nullptr ||
        CURRENT->state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE ||
        CURRENT->exit_in_progress.load(std::memory_order_acquire) ||
        (CURRENT->type != sched::task::TaskType::PROCESS && CURRENT->type != sched::task::TaskType::DAEMON) ||
        CURRENT->scheduler_transition_active.load(std::memory_order_acquire) || CURRENT->deferred_task_switch || CURRENT->wants_block) {
        return -EWOULDBLOCK;
    }
    // Excluding an owning process from TERM/KILL is safe only for a sole CPU
    // executor. The accepted nonreturning finalizer seals admission under the
    // resource lock; an already in-flight sibling binding then fails closed.
    if (CURRENT->is_thread || CURRENT->resource_cpu_threads.load(std::memory_order_acquire) != 0) return -EBUSY;

    ShutdownPhase phase = g_phase.load(std::memory_order_acquire);
    for (;;) {
        if (phase != ShutdownPhase::IDLE && phase != ShutdownPhase::PREPARING) {
            return -EBUSY;
        }
        if (g_phase.compare_exchange_weak(phase, ShutdownPhase::FINALIZING, std::memory_order_acq_rel, std::memory_order_acquire)) {
            break;
        }
    }
    g_action.store(action, std::memory_order_release);
    run_finalizer(action);
}

auto begin_reboot_command(sched::task::Task* subject, uint64_t cmd, bool current_subject) -> int {
    ShutdownAction action = ShutdownAction::NONE;
    int const PARSE_RET = command_to_action(cmd, action);
    if (PARSE_RET < 0) {
        return PARSE_RET;
    }
    // Invalid commands must not observe scheduler state, even before boot has
    // established run queues. Diagnostic subjects never fall back to current.
    if (current_subject) subject = sched::get_current_task();
    int const AUTHORITY_RET = authorize_current_power(subject);
    if (AUTHORITY_RET != 0) {
        return AUTHORITY_RET;
    }
    if (action == ShutdownAction::NONE) {
        return 0;
    }

    return begin_finalization(action);
}

}  // namespace

auto prepare_shutdown() -> int { return prepare_shutdown(sched::get_current_task()); }

auto begin_reboot_command(uint64_t cmd) -> int { return begin_reboot_command(nullptr, cmd, true); }

#ifdef WOS_SELFTEST
auto selftest_prepare_shutdown_for_subject(sched::task::Task* subject) -> int { return prepare_shutdown(subject); }

auto selftest_begin_reboot_for_subject(sched::task::Task* subject, uint64_t cmd) -> int {
    return begin_reboot_command(subject, cmd, false);
}

[[noreturn]] void selftest_shutdown() {
    int const result = begin_finalization(ShutdownAction::POWEROFF);
    log::error("selftest shutdown admission refused %d; providers retained", result);
    dbg::panic_handler("selftest shutdown lacks accepted finalizer");
    halt_forever();
}

[[noreturn]] void selftest_poweroff() {
    g_action.store(ShutdownAction::POWEROFF, std::memory_order_release);
    g_phase.store(ShutdownPhase::PLATFORM_ACTION, std::memory_order_release);
    log::info("selftest complete; powering off");
    platform_poweroff();
}
#endif

}  // namespace ker::mod::power

namespace ker::mod::mm::cpu_maintenance::detail {
Controller::Controller(const ker::mod::power::FinalizerExecution& issuer) noexcept : origin_(Origin::finalizer), lineage_(issuer.lineage_) {
    issuer_.finalizer = &issuer;
    birth_status_ = validate_finalizer();
}

auto Controller::validate_finalizer() const noexcept -> Status {
    if (!phys::can_wait_for_reclaim()) return Status::invalid_context;
    auto* const current = sched::get_current_task();
    if (current == nullptr || current->is_thread || current->state.load(std::memory_order_acquire) != sched::task::TaskState::ACTIVE ||
        current->exit_in_progress.load(std::memory_order_acquire) || !current->kernel_shutdown_ownership.held() ||
        current->resource_cpu_threads.load(std::memory_order_acquire) != 0 ||
        (current->type != sched::task::TaskType::PROCESS && current->type != sched::task::TaskType::DAEMON))
        return Status::invalid_context;
    uint64_t const top = current->context.syscall_kernel_stack;
    if (top < KERNEL_STACK_SIZE) return Status::invalid_context;
    uint64_t const base = top - KERNEL_STACK_SIZE;
    auto const contains = [base, top](const void* pointer, size_t bytes) {
        uint64_t const address = reinterpret_cast<uint64_t>(pointer);
        return bytes <= top - base && address >= base && address <= top - bytes;
    };
    const auto* issuer = issuer_.finalizer;
    if (!contains(this, sizeof(*this)) || !contains(issuer, sizeof(*issuer))) return Status::invalid_context;
    auto const phase = power::g_phase.load(std::memory_order_acquire);
    if (phase != power::ShutdownPhase::FINALIZING && phase != power::ShutdownPhase::PROCESS_TEARDOWN &&
        phase != power::ShutdownPhase::VFS_SYNC && phase != power::ShutdownPhase::ROOT_UNMOUNT)
        return Status::invalid_context;
    if (lineage_ == 0 || issuer->lineage_ != lineage_ || !issuer->live_ || !issuer->sealed_ ||
        issuer->action_ == power::ShutdownAction::NONE || power::g_action.load(std::memory_order_acquire) != issuer->action_ ||
        !sched::interrupts_enabled() || sched::preempt_count() != 0 || sched::get_current_task() != current)
        return Status::invalid_context;
    return Status::ready;
}
}  // namespace ker::mod::mm::cpu_maintenance::detail
