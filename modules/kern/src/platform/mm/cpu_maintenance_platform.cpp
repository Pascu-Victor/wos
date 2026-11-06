#include "cpu_maintenance_platform.hpp"

#include <platform/acpi/apic/apic.hpp>
#include <platform/asm/tlb.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>
#include <platform/tsc/tsc.hpp>

#include "cpu_maintenance_controller.hpp"
#include "mmio_cache_platform.hpp"

namespace ker::mod::mm::cpu_maintenance::detail {
Controller::~Controller() {
    origin_ = Origin::closed;
    lineage_ = 0;
}

auto Controller::context_status() const noexcept -> Status {
    if (birth_status_ != Status::ready) return birth_status_;
    if (lineage_ == 0) return Status::invalid_context;
    switch (origin_) {
        case Origin::boot:
            return validate_boot();
        case Origin::finalizer:
            return validate_finalizer();
        case Origin::closed:
            return Status::invalid_context;
    }
    return Status::invalid_context;
}
}  // namespace ker::mod::mm::cpu_maintenance::detail

namespace ker::mod::mm::cpu_maintenance::detail::platform {
namespace observed = mmio_cache::detail::platform;

auto configured_cpu_count() -> size_t { return observed::configured_cpu_count(); }
auto current_cpu(size_t& logical, uint32_t& apic) -> mmio_cache::Status { return observed::current_cpu(logical, apic); }
auto save_disable_interrupts() -> uint64_t { return observed::save_disable_interrupts(); }
void restore_interrupts(uint64_t saved_flags) { observed::restore_interrupts(saved_flags); }

auto current_root_is_kernel() -> bool {
    auto* const ROOT = virt::get_kernel_pagemap();
    return ROOT != nullptr && virt::active_pagemap_is(ROOT);
}

auto private_protection_status() noexcept -> mmio_cache::Status {
    observed::CpuIo io;
    if (!io.local_idt_ready()) return mmio_cache::Status::local_idt_unavailable;
    uint64_t efer = 0;
    if (!io.read_msr(0xc0000080, efer)) return mmio_cache::Status::read_fault;
    if ((efer & (uint64_t{1} << 11)) == 0 || (io.read_cr0() & (uint64_t{1} << 16)) == 0) return mmio_cache::Status::unsupported_mode;
    return mmio_cache::Status::ready;
}

auto execute_local(Phase phase) noexcept -> uint64_t {
    uint64_t const CURRENT = rdcr3();
    if (phase == Phase::translations) {
        // The actual18b profile refuses PGE, PCIDE and LA57. Reload this
        // CPU's current root, including when an ordinary Task root is active.
        wrcr3(CURRENT);
    } else if (phase == Phase::caches) {
        asm volatile("mfence; wbinvd; mfence" : : : "memory");
    } else {
        // A protocol programming error cannot produce a successful local ACK.
        __builtin_trap();
    }
    return CURRENT;
}

void send_ipi(uint32_t target_apic, uint8_t actual_vector) noexcept {
    apic::IPIConfig const IPI{
        .vector = actual_vector,
        .delivery_mode = apic::IPIDeliveryMode::FIXED,
        .destination_mode = apic::IPIDestinationMode::PHYSICAL,
        .level = apic::IPILevel::ASSERT,
        .trigger_mode = apic::IPITriggerMode::EDGE,
        .destination_shorthand = apic::IPIDestinationShorthand::NONE,
    };
    apic::send_ipi(IPI, target_apic);
}

auto clock_now_ns(uint64_t& output) -> bool {
    auto const BEFORE = tsc::get_status();
    if (BEFORE.state == time::clocksource::State::unavailable || BEFORE.resolution_ns == 0) return false;
    uint64_t const VALUE = time::get_monotonic_ns();
    auto const AFTER = tsc::get_status();
    if (AFTER.state == time::clocksource::State::unavailable || AFTER.resolution_ns == 0) return false;
    output = VALUE;
    return true;
}

auto controller_context_ready() -> bool {
    if (!phys::can_wait_for_reclaim()) return false;
    auto* const CURRENT = sched::get_current_task();
    return CURRENT != nullptr && CURRENT->type == sched::task::TaskType::DAEMON &&
           CURRENT->state.load(std::memory_order_acquire) == sched::task::TaskState::ACTIVE &&
           !CURRENT->exit_in_progress.load(std::memory_order_acquire) && CURRENT->kernel_shutdown_ownership.held() &&
           sched::interrupts_enabled() && sched::preempt_count() == 0 && sched::get_current_task() == CURRENT;
}

void controller_yield() { sched::kern_yield(); }
void spin_pause() noexcept { asm volatile("pause" : : : "memory"); }

}  // namespace ker::mod::mm::cpu_maintenance::detail::platform
