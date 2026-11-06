#pragma once

// Finite scheduler/device endpoints for the extracted complete native issuers.
// Production constructors, validators, executor and finalizer/admission bodies
// are generated unchanged except the actual STI instruction is a finite port.
#include <platform/init/init_executor.hpp>
#include <platform/mm/cpu_maintenance_controller.hpp>
#include <platform/power/power.hpp>
#include <platform/sched/shutdown_ownership.hpp>
#include <platform/security/credentials.hpp>
#include <string_view>

#include "cpu_maintenance_controller_context.hpp"

namespace controller_host {
namespace cm = ker::mod::mm::cpu_maintenance;
using Callback = void (*)(const cm::detail::Controller&);
struct Finished {};
struct Refused {};
inline Callback boot_callback{}, finalizer_callback{};
inline ker::mod::sched::task::Task* current{};
inline bool scheduled{}, queues{true}, safe{true}, seal_allowed{true};
inline unsigned preempt{}, seals{}, boot_calls{}, finalizer_calls{};
inline std::vector<std::string_view> effects;
inline std::vector<int> retirement_results;
inline std::vector<uintptr_t> finalizer_scopes;
inline unsigned retirement_clears{};
inline bool retirement_needs_clear{};
inline int last_retirement_result{};
inline const char* panic_reason{};
void enable_irq() { runtime_ports::flags |= runtime_ports::IF; }
void module(const char*) {}
void initialize(ker::mod::sched::task::Task& task) {
    uint64_t local = 0;
    task.context.syscall_kernel_stack = reinterpret_cast<uint64_t>(&local) + 256 * 1024;
    task.credential_snapshot = ker::mod::security::Snapshot::bootstrap_root();
    current = &task;
    scheduled = true;
}
[[noreturn]] void finished() { throw Finished{}; }
}  // namespace controller_host

namespace ker::mod::sched {
auto can_query_current_task() -> bool { return controller_host::scheduled; }
auto get_current_task() -> task::Task* { return can_query_current_task() ? controller_host::current : nullptr; }
auto has_run_queues() -> bool { return controller_host::queues; }
auto preempt_count() -> unsigned { return controller_host::preempt; }
auto interrupts_enabled() -> bool { return (runtime_ports::flags & runtime_ports::IF) != 0; }
auto hold_kernel_shutdown() -> KernelShutdownHold {
    auto* current = get_current_task();
    return KernelShutdownHold(current != nullptr ? &current->kernel_shutdown_ownership : nullptr);
}
namespace resource {
auto close_idle_thread_cpu_admission(task::Task& task) -> bool {
    ++controller_host::seals;
    return controller_host::seal_allowed && !task.is_thread && task.resource_cpu_threads.load() == 0;
}
}  // namespace resource
}  // namespace ker::mod::sched
namespace ker::mod::mm {
inline constexpr size_t KERNEL_STACK_SIZE = 0x80000;
namespace phys {
auto can_wait_for_reclaim() -> bool {
    auto* task = sched::get_current_task();
    return controller_host::safe && sched::has_run_queues() && task != nullptr && sched::interrupts_enabled() &&
           sched::preempt_count() == 0 && !task->scheduler_transition_active.load() && !task->deferred_task_switch && !task->wants_block;
}
}  // namespace phys
}  // namespace ker::mod::mm
namespace ker::mod::dbg {
void panic_handler(const char* reason) {
    controller_host::panic_reason = reason;
    std::fprintf(stderr, "controller panic: %s\n", reason);
    throw controller_host::Refused{};
}
}  // namespace ker::mod::dbg
namespace ker::net::wki {
void wki_shutdown() { controller_host::effects.push_back("wki"); }
}  // namespace ker::net::wki
namespace ker::vfs {
auto vfs_shutdown_sync() -> int {
    controller_host::effects.push_back("sync");
    return 0;
}
auto vfs_shutdown_unmount_all(const char*) -> int {
    controller_host::effects.push_back("unmount");
    return 0;
}
}  // namespace ker::vfs
namespace ker::dev::ahci {
auto ahci_shutdown() -> int {
    controller_host::effects.push_back("ahci");
    return 0;
}
}  // namespace ker::dev::ahci
namespace ker::dev::virtio {
auto virtio_net_shutdown() -> int {
    controller_host::effects.push_back("virtio");
    return 0;
}
}  // namespace ker::dev::virtio
namespace ker::dev::e1000e {
void e1000e_init(const controller_host::cm::detail::Controller&);
auto e1000e_shutdown(const controller_host::cm::detail::Controller&) -> int;
#ifndef WOS_HOST_ACTUAL_E1000
void e1000e_init(const controller_host::cm::detail::Controller& controller) {
    ++controller_host::boot_calls;
    if (controller_host::boot_callback != nullptr) controller_host::boot_callback(controller);
}
auto e1000e_shutdown(const controller_host::cm::detail::Controller& controller) -> int {
    ++controller_host::finalizer_calls;
    controller_host::effects.push_back("e1000");
    controller_host::finalizer_scopes.push_back(reinterpret_cast<uintptr_t>(&controller));
    if (controller_host::finalizer_callback != nullptr) controller_host::finalizer_callback(controller);
    controller_host::retirement_needs_clear = true;
    size_t const index = controller_host::finalizer_calls - 1;
    return index < controller_host::retirement_results.size() ? controller_host::retirement_results[index] : 0;
}
#endif
}  // namespace ker::dev::e1000e
namespace ker::dev::ivshmem {
auto ivshmem_net_shutdown() -> int { return 0; }
}  // namespace ker::dev::ivshmem
namespace ker::dev::usb {
auto xhci_shutdown() -> int { return 0; }
}  // namespace ker::dev::usb
namespace ker::mod::smt {
void halt_other_cores() { controller_host::effects.push_back("halt"); }
}  // namespace ker::mod::smt
namespace ker::mod::power {
namespace {
struct log {
    template <typename... Args>
    static void info(const char*, Args...) {}
    template <typename... Args>
    static void warn(const char*, Args...) {}
    template <typename... Args>
    static void error(const char*, Args...) {}
};
auto current_process_pid(const sched::task::Task* task) -> uint64_t { return task != nullptr ? task->pid : 0; }
auto teardown_processes() -> bool {
    controller_host::effects.push_back("processes");
    return true;
}
void clear_current_finalizer_sched_state() {
    if (controller_host::retirement_needs_clear) {
        ++controller_host::retirement_clears;
        controller_host::retirement_needs_clear = false;
    }
}
auto wait_for_file_retirement() -> int {
    controller_host::effects.push_back("files");
    return 0;
}
auto count_shutdown_targets(uint64_t, uint64_t) -> size_t { return 0; }
inline constexpr uint64_t KERNEL_THREAD_WAIT_US = 1;
auto quiesce_kernel_threads(uint64_t) -> bool {
    controller_host::effects.push_back("workers");
    return true;
}
auto current_root_path() -> const char* { return "/"; }
[[noreturn]] void halt_forever() { throw controller_host::Refused{}; }
[[noreturn]] void platform_reboot() { controller_host::finished(); }
[[noreturn]] void platform_poweroff() { controller_host::finished(); }
}  // namespace
}  // namespace ker::mod::power

#ifdef WOS_HOST_ACTUAL_E1000
namespace controller_host {
void after_actual_boot_driver(const cm::detail::Controller&);
void after_actual_retirement(const cm::detail::Controller&, int);
}  // namespace controller_host
#endif

#ifndef WOS_MAINTENANCE_CONTROLLERS_HEADER
#define WOS_MAINTENANCE_CONTROLLERS_HEADER "cpu_maintenance_controllers_generated.hpp"
#endif
#include WOS_MAINTENANCE_CONTROLLERS_HEADER

namespace controller_host {
class CallbackScope {
    Callback& slot_;
    Callback saved_;

   public:
    CallbackScope(Callback& slot, Callback callback) : slot_(slot), saved_(slot) { slot_ = callback; }
    ~CallbackScope() { slot_ = saved_; }
    CallbackScope(const CallbackScope&) = delete;
    auto operator=(const CallbackScope&) -> CallbackScope& = delete;
};

// These traverse the actual extracted issuers. Context inputs remain explicit
// finite scheduler/credential ports; no controller or acceptance is fabricated.
auto with_actual_boot_controller(Callback callback) -> bool {
    CallbackScope scope(boot_callback, callback);
    try {
        ker::init::InitExecutor::run_all();
    } catch (const Finished&) {
        return true;
    }
}
auto with_actual_finalizer_controller(Callback callback) -> int {
    CallbackScope scope(finalizer_callback, callback);
    try {
        return ker::mod::power::begin_reboot_command(ker::abi::power::RB_POWER_OFF);
    } catch (const Finished&) {
        return 0;
    }
}

#ifdef WOS_HOST_ACTUAL_E1000
// Observation-only hooks added around the exact real calls in compile copies.
// They receive only the already-minted synchronous lexical permission.
void after_actual_boot_driver(const cm::detail::Controller& controller) {
    ++boot_calls;
    if (boot_callback != nullptr) boot_callback(controller);
}
void after_actual_retirement(const cm::detail::Controller& controller, int result) {
    ++finalizer_calls;
    last_retirement_result = result;
    if (finalizer_callback != nullptr) finalizer_callback(controller);
}
#endif
}  // namespace controller_host
