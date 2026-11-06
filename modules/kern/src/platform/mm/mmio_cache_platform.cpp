#include "mmio_cache_platform.hpp"

#include <platform/asm/cpu.hpp>
#include <platform/asm/cpu_safe_msr.hpp>
#include <platform/interrupt/idt.hpp>
#include <platform/smt/smt.hpp>

namespace ker::mod::mm::mmio_cache::detail::platform {

auto CpuIo::cpuid(uint32_t leaf, uint32_t subleaf) -> Cpuid {
    Cpuid value{};
    // Every subleaf is an actual input, unlike the legacy leaf-only helper.
    asm volatile("cpuid" : "=a"(value.eax), "=b"(value.ebx), "=c"(value.ecx), "=d"(value.edx) : "a"(leaf), "c"(subleaf) : "memory");
    return value;
}
auto CpuIo::read_msr(uint32_t index, uint64_t& output) -> bool { return cpu::try_read_msr(index, output); }
auto CpuIo::read_cr0() -> uint64_t {
    uint64_t value = 0;
    asm volatile("mov %%cr0, %0" : "=r"(value) : : "memory");
    return value;
}
auto CpuIo::read_cr4() -> uint64_t {
    uint64_t value = 0;
    cpu::rdcr4(&value);
    return value;
}
auto CpuIo::local_idt_ready() -> bool { return desc::idt::kernel_idt_loaded_on_current_cpu(); }

auto save_disable_interrupts() -> uint64_t {
    uint64_t flags = 0;
    asm volatile("pushfq; popq %0; cli" : "=r"(flags) : : "memory");
    return flags;
}
void restore_interrupts(uint64_t saved_flags) {
    if ((saved_flags & cpu::GATE_IF_MASK) != 0)
        asm volatile("sti" : : : "memory");
    else
        asm volatile("cli" : : : "memory");
}
auto configured_cpu_count() -> size_t { return static_cast<size_t>(smt::get_core_count()); }

namespace {
auto read_local_identity(uint32_t& actual_apic) -> Status {
    CpuIo io;
    if (!io.local_idt_ready()) return Status::local_idt_unavailable;
    auto const basic = io.cpuid(0, 0);
    if (basic.eax < 1) return Status::missing_features;
    auto const features = io.cpuid(1, 0);
    if ((features.edx & (uint32_t{1} << 5)) == 0 || (features.ecx & (uint32_t{1} << 21)) == 0) return Status::missing_features;
    uint64_t apic_base = 0, apic_id = 0;
    if (!io.read_msr(IA32_APIC_BASE, apic_base)) return Status::read_fault;
    if ((apic_base & (uint64_t{3} << 10)) != (uint64_t{3} << 10)) return Status::unsupported_mode;
    if (!io.read_msr(0x802, apic_id)) return Status::read_fault;
    if (apic_id > UINT32_MAX) return Status::wrong_cpu;
    actual_apic = static_cast<uint32_t>(apic_id);
    return Status::ready;
}
}  // namespace

auto verify_boot_cpu(size_t expected_logical, uint32_t& actual_apic) -> Status {
    if (!smt::has_cpu_data() || expected_logical >= configured_cpu_count()) return Status::wrong_cpu;
    uint32_t apic = 0;
    auto const status = read_local_identity(apic);
    if (status != Status::ready) return status;
    if (smt::get_cpu(expected_logical).lapic_id != apic) return Status::wrong_cpu;
    auto* expected_context = smt::get_kernel_per_cpu(expected_logical);
    if (expected_context == nullptr) return Status::wrong_cpu;
    uint64_t gs_base = 0;
    if (!cpu::try_read_msr(IA32_GS_BASE, gs_base)) return Status::read_fault;
    if (gs_base != reinterpret_cast<uint64_t>(expected_context)) return Status::wrong_cpu;
    // Only dereference GS after the actual MSR names a retained kernel PerCpu.
    if (cpu::current_cpu_fast() != expected_logical) return Status::wrong_cpu;
    actual_apic = apic;
    return Status::ready;
}

auto current_cpu(size_t& logical, uint32_t& actual_apic) -> Status {
    if (!smt::has_cpu_data()) return Status::wrong_cpu;
    size_t const count = configured_cpu_count();
    if (count == 0 || count > 64) return Status::invalid_configuration;
    uint32_t apic = 0;
    auto const status = read_local_identity(apic);
    if (status != Status::ready) return status;
    // The legacy lookup's unknown-ID fallback is logical zero. Use checked
    // exact membership instead, without requiring a current Task/GS pointer.
    size_t found = count;
    for (size_t i = 0; i < count; ++i) {
        if (smt::get_cpu(i).lapic_id != apic) continue;
        if (found != count) return Status::wrong_cpu;
        found = i;
    }
    if (found == count) return Status::wrong_cpu;
    logical = found;
    actual_apic = apic;
    return Status::ready;
}

}  // namespace ker::mod::mm::mmio_cache::detail::platform
