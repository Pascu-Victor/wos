#include "power.hpp"

#include <abi/callnums/power.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <mod/io/port/port.hpp>
#include <net/wki/wki.hpp>
#include <platform/acpi/power.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <vfs/vfs.hpp>

namespace ker::mod::power {
namespace {

using log = ker::mod::dbg::logger<"power">;

constexpr int WOS_SIGKILL = 9;
constexpr int WOS_SIGTERM = 15;
constexpr uint64_t TERM_WAIT_US = 2'000'000;
constexpr uint64_t KILL_WAIT_US = 1'000'000;
constexpr uint64_t KERNEL_THREAD_WAIT_US = 2'000'000;

std::atomic<ShutdownPhase> g_phase{ShutdownPhase::IDLE};
std::atomic<ShutdownAction> g_action{ShutdownAction::NONE};

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
    return sched::signal_visible_processes_except(excluded_pid, excluded_owner_pid, 0);
}

auto current_task_is_root() -> int {
    auto* task = sched::get_current_task();
    if (task == nullptr) {
        return -ESRCH;
    }
    if (task->euid != 0) {
        return -EPERM;
    }
    return 0;
}

void quiesce_kernel_threads() {
    auto const RESULT = sched::request_kernel_threads_shutdown(KERNEL_THREAD_WAIT_US);
    if (RESULT.requested != 0) {
        log::info("shutdown requested stop for %lu kernel threads", static_cast<unsigned long>(RESULT.requested));
    }
    if (RESULT.remaining != 0) {
        log::warn("shutdown continuing with %lu kernel threads still active", static_cast<unsigned long>(RESULT.remaining));
    }
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
    current->wait_channel = nullptr;
}

void wait_for_process_quiescence(uint64_t excluded_pid, uint64_t excluded_owner_pid, uint64_t timeout_us) {
    uint64_t const START = ker::mod::time::get_us();
    while (count_shutdown_targets(excluded_pid, excluded_owner_pid) != 0) {
        if (ker::mod::time::get_us() - START >= timeout_us) {
            return;
        }
        sched::kern_yield();
    }
}

void teardown_processes() {
    auto* current = sched::get_current_task();
    uint64_t const EXCLUDED_PID = current != nullptr ? current->pid : 0;
    uint64_t const EXCLUDED_OWNER_PID = current_process_pid(current);

    g_phase.store(ShutdownPhase::PROCESS_TEARDOWN, std::memory_order_release);
    size_t const TERM_COUNT = sched::signal_visible_processes_except(EXCLUDED_PID, EXCLUDED_OWNER_PID, WOS_SIGTERM);
    log::info("shutdown SIGTERM sent to %lu processes", static_cast<unsigned long>(TERM_COUNT));
    wait_for_process_quiescence(EXCLUDED_PID, EXCLUDED_OWNER_PID, TERM_WAIT_US);

    size_t const REMAINING = count_shutdown_targets(EXCLUDED_PID, EXCLUDED_OWNER_PID);
    if (REMAINING == 0) {
        return;
    }

    size_t const KILL_COUNT = sched::signal_visible_processes_except(EXCLUDED_PID, EXCLUDED_OWNER_PID, WOS_SIGKILL);
    log::warn("shutdown SIGKILL sent to %lu remaining processes", static_cast<unsigned long>(KILL_COUNT));
    wait_for_process_quiescence(EXCLUDED_PID, EXCLUDED_OWNER_PID, KILL_WAIT_US);
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

[[noreturn]] void run_finalizer(ShutdownAction action) {
    g_phase.store(ShutdownPhase::FINALIZING, std::memory_order_release);
    log::info("system shutdown finalizer starting action=%lu", static_cast<unsigned long>(action));

    quiesce_kernel_threads();
    clear_current_finalizer_sched_state();

    teardown_processes();
    clear_current_finalizer_sched_state();

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

    g_phase.store(ShutdownPhase::CPU_QUIESCE, std::memory_order_release);
    clear_current_finalizer_sched_state();
    smt::halt_other_cores();

    g_phase.store(ShutdownPhase::PLATFORM_ACTION, std::memory_order_release);
    switch (action) {
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

}  // namespace

auto shutdown_in_progress() -> bool { return g_phase.load(std::memory_order_acquire) != ShutdownPhase::IDLE; }

auto shutdown_phase() -> ShutdownPhase { return g_phase.load(std::memory_order_acquire); }

auto prepare_shutdown() -> int {
    int const ROOT_RET = current_task_is_root();
    if (ROOT_RET != 0) {
        return ROOT_RET;
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

    quiesce_kernel_threads();
    return 0;
}

auto begin_reboot_command(uint64_t cmd) -> int {
    ShutdownAction action = ShutdownAction::NONE;
    int const PARSE_RET = command_to_action(cmd, action);
    if (PARSE_RET < 0) {
        return PARSE_RET;
    }
    if (action == ShutdownAction::NONE) {
        return 0;
    }

    int const ROOT_RET = current_task_is_root();
    if (ROOT_RET != 0) {
        return ROOT_RET;
    }

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

}  // namespace ker::mod::power
