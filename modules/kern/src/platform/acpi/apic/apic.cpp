#include "apic.hpp"

#include <cstdint>
#include <platform/acpi/hpet/hpet.hpp>

#include "mod/io/serial/serial.hpp"
#include "platform/acpi/acpi.hpp"
#include "platform/acpi/madt/madt.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/mm/addr.hpp"
#include "util/hcf.hpp"
namespace ker::mod::apic {

namespace {

auto check_x2_apic_support() -> bool {
    cpu::CpuidContext cpuid_context{};
    cpuid_context.function = 0x1;
    cpu::cpuid(&cpuid_context);
    return (cpuid_context.ecx & (1 << 21)) > 0;
}

}  // namespace

void write_reg(uint32_t reg, uint64_t value) { cpu_set_msr(reg, value); }

uint32_t read_reg(uint32_t reg) {
    uint64_t res = 0;
    cpu_get_msr(reg, &res);
    return res;
}

// end of apic interrupt
void eoi() { cpu_set_msr(static_cast<uint32_t>(X2APICMSRs::EOI), 0); }

void send_ipi(IPIConfig message_type, uint32_t destination) {
    uint64_t const ICR_VALUE = ((static_cast<uint64_t>(destination)) << 32) | message_type.packed_value;
    write_reg(static_cast<uint32_t>(X2APICMSRs::ICR), ICR_VALUE);
}

void reset_apic_counter() { write_reg(static_cast<uint32_t>(APICRegisters::TMR_INIT_CNT), 0xFFFFFFFF); }

void init() {
    acpi::ACPIResult const MADT = acpi::parse_acpi_tables("APIC");
    if (MADT.success) {
        acpi::madt::ApicInfo const APIC_INFO = acpi::madt::parse_madt(MADT.data);
        apic_base = reinterpret_cast<uint64_t>(mm::addr::get_virt_pointer(APIC_INFO.lapic_addr));
    } else {
        io::serial::write("Failed to parse MADT table\n");
        hcf();
    }
}

void init_apic_mp() {
    if (!check_x2_apic_support()) {
        [[unlikely]] io::serial::write("X2APIC not supported\n");
        hcf();
    }
    uint64_t msr = 0;
    cpu_get_msr(IA32_APIC_BASE, &msr);
    cpu_set_msr(IA32_APIC_BASE, msr | (1 << 10) | (1 << 11));  // enable x2apic

    // setup the spurious interrupt vector
    write_reg(static_cast<uint32_t>(X2APICMSRs::SPURIOUS_INT_VEC), 0x1FF | (1 << 8));  // interrupt vector 0x1FF, enable APIC
}

auto calibrate_timer(uint64_t us) -> uint32_t {
    write_reg(static_cast<uint32_t>(X2APICMSRs::TIMER_DIVIDE_CONFIG), 0x3);
    write_reg(static_cast<uint32_t>(X2APICMSRs::TIMER_INIT_COUNT), 0xFFFFFFFF);
    hpet::sleep_us(us);
    write_reg(static_cast<uint32_t>(X2APICMSRs::LVT_TIMER), (0 << 16) | (0 << 17) | 32);
    return 0xFFFFFFFF - read_reg(static_cast<uint32_t>(X2APICMSRs::TIMER_CURRENT_COUNT));
}

void one_shot_timer(uint64_t ticks) { write_reg(static_cast<uint32_t>(X2APICMSRs::TIMER_INIT_COUNT), ticks); }

uint64_t get_ticks() { return read_reg(static_cast<uint32_t>(X2APICMSRs::TIMER_CURRENT_COUNT)); }

uint32_t get_apic_id() { return read_reg(static_cast<uint32_t>(X2APICMSRs::ID)); }

// void startInterrupts() { asm volatile("sti"); }
}  // namespace ker::mod::apic
