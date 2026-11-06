#pragma once

// Generated unchanged from the finite-port prefix of mmio_cache_runtime_test;
// it contains no registry implementation or production state setter.
#include <mutex>
#include <platform/mm/cpu_maintenance.hpp>
#include <platform/mm/cpu_maintenance_platform.hpp>

#include "mmio_cache_runtime_ports_generated.hpp"

namespace maintenance_host {
namespace cm = ker::mod::mm::cpu_maintenance;
namespace cache = ker::mod::mm::mmio_cache;
using Phase = cm::Phase;
struct Action {
    size_t cpu;
    Phase phase;
    uint64_t cr3;
    uint64_t request;
};
std::mutex event_lock;
std::vector<Action> actions;
std::atomic<uint64_t> pending{};
std::atomic<uint64_t> now{1'000'000};
std::atomic<uint64_t> clock_step{50'000};
std::atomic<bool> clock_available{true};
std::atomic<bool> auto_progress{true};
std::atomic<unsigned> ipis{}, pauses{}, yields{};
std::atomic<bool> nested_service{};
std::atomic<bool> pre_armed{}, pre_entered{}, pre_release{};
std::atomic<bool> post_armed{}, post_entered{}, post_release{};
std::atomic<size_t> pause_cpu{1};
thread_local bool controller = true;
thread_local bool kernel_root = true;
thread_local size_t last_action = SIZE_MAX;

void bounded_pause(std::atomic<bool>& release) {
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!release.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() >= deadline) {
            runtime_ports::violations.fetch_add(1);
            return;
        }
        std::this_thread::yield();
    }
}

void before_join(void*) {
    if (runtime_ports::selected == pause_cpu.load() && pre_armed.exchange(false)) {
        pre_entered.store(true, std::memory_order_release);
        bounded_pause(pre_release);
    }
}

void after_ack(void*, uint64_t id, size_t logical) {
    {
        std::lock_guard lock(event_lock);
        if (last_action >= actions.size() || actions[last_action].cpu != logical)
            runtime_ports::violations.fetch_add(1);
        else
            actions[last_action].request = id;
    }
    if (runtime_ports::selected == pause_cpu.load() && post_armed.exchange(false)) {
        post_entered.store(true, std::memory_order_release);
        bounded_pause(post_release);
    }
}

void service(size_t cpu) {
    size_t const saved_cpu = runtime_ports::selected;
    uint32_t const saved_apic = runtime_ports::local_apic;
    uint64_t const saved_flags = runtime_ports::flags;
    unsigned const saved_depth = runtime_ports::irq_depth;
    runtime_ports::select(cpu, 2);
    cm::detail::service_current_cpu();
    runtime_ports::selected = saved_cpu;
    runtime_ports::local_apic = saved_apic;
    runtime_ports::flags = saved_flags;
    runtime_ports::irq_depth = saved_depth;
}

void drain() {
    uint64_t const mask = pending.exchange(0, std::memory_order_acq_rel);
    for (size_t i = 0; i < 64; ++i) {
        if ((mask & (uint64_t{1} << i)) != 0) service(i);
    }
}

auto events() -> std::vector<Action> {
    std::lock_guard lock(event_lock);
    return actions;
}

void setup(size_t count, ker::mod::mm::cache_arch::Vendor vendor = ker::mod::mm::cache_arch::Vendor::intel) {
    runtime_ports::setup(count, vendor);
    for (size_t i = 0; i < count && i < runtime_ports::cpus.size(); ++i) runtime_ports::cpus[i].msr[0xc0000080] = uint64_t{1} << 11;
    pending.store(0);
    now.store(1'000'000);
    clock_step.store(50'000);
    clock_available.store(true);
    auto_progress.store(true);
    ipis.store(0);
    pauses.store(0);
    yields.store(0);
    nested_service.store(false);
    pre_armed.store(false);
    pre_entered.store(false);
    pre_release.store(false);
    post_armed.store(false);
    post_entered.store(false);
    post_release.store(false);
    pause_cpu.store(1);
    controller = kernel_root = true;
    last_action = SIZE_MAX;
    std::lock_guard lock(event_lock);
    actions.clear();
}
}  // namespace maintenance_host

namespace ker::mod::mm::cpu_maintenance::detail::platform {
auto configured_cpu_count() -> size_t { return runtime_ports::configured.load(); }
auto current_cpu(size_t& logical, uint32_t& apic) -> mmio_cache::Status { return mmio_cache::detail::platform::current_cpu(logical, apic); }
auto save_disable_interrupts() -> uint64_t { return mmio_cache::detail::platform::save_disable_interrupts(); }
void restore_interrupts(uint64_t flags) { mmio_cache::detail::platform::restore_interrupts(flags); }
auto current_root_is_kernel() -> bool {
    runtime_ports::masked();
    return maintenance_host::kernel_root;
}
auto execute_local(Phase phase) noexcept -> uint64_t {
    runtime_ports::masked();
    uint64_t const cr3 = (runtime_ports::selected + 1) * 4096 + 0x100000;
    {
        std::lock_guard lock(maintenance_host::event_lock);
        maintenance_host::last_action = maintenance_host::actions.size();
        maintenance_host::actions.push_back({runtime_ports::selected, phase, cr3, 0});
    }
    if (maintenance_host::nested_service.exchange(false)) detail::service_current_cpu();
    return cr3;
}
void send_ipi(uint32_t apic, uint8_t vector) noexcept {
    if (vector != 0x70) runtime_ports::violations.fetch_add(1);
    size_t const count = runtime_ports::configured.load();
    for (size_t i = 0; i < count && i < 64; ++i) {
        if (runtime_ports::cpus[i].apic == apic) {
            maintenance_host::pending.fetch_or(uint64_t{1} << i);
            maintenance_host::ipis.fetch_add(1);
            return;
        }
    }
    runtime_ports::violations.fetch_add(1);
}
auto clock_now_ns(uint64_t& output) -> bool {
    if (!maintenance_host::clock_available.load()) return false;
    output = maintenance_host::now.fetch_add(maintenance_host::clock_step.load());
    return true;
}
auto controller_context_ready() -> bool {
    return maintenance_host::controller && (runtime_ports::flags & runtime_ports::IF) != 0 && runtime_ports::irq_depth == 0;
}
void controller_yield() {
    maintenance_host::yields.fetch_add(1);
    if (maintenance_host::auto_progress.load()) maintenance_host::drain();
    std::this_thread::yield();
}
void spin_pause() noexcept {
    maintenance_host::pauses.fetch_add(1);
    if (maintenance_host::auto_progress.load()) maintenance_host::drain();
}
}  // namespace ker::mod::mm::cpu_maintenance::detail::platform

#include "cpu_maintenance_controller_ports.inc"
