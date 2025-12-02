#include "apic.hpp"

#include <cstdint>
#include <platform/mm/paging.hpp>
#include <platform/mm/virt.hpp>

#include "mod/io/serial/serial.hpp"
#include "platform/acpi/acpi.hpp"
#include "platform/acpi/madt/madt.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/mm/addr.hpp"
#include "util/hcf.hpp"
namespace ker::mod::apic {
void writeReg(uint32_t reg, uint64_t value) { cpuSetMSR(reg, value); }

uint32_t readReg(uint32_t reg) {
    uint64_t res;
    cpuGetMSR(reg, &res);
    return res;
}

// end of apic interrupt
void eoi() { cpuSetMSR((uint32_t)X2APICMSRs::EOI, 0); }

void sendIpi(IPIConfig messageType, uint32_t destination) {
    writeReg((uint32_t)X2APICMSRs::ICR, (((uint64_t)destination) << 32) | messageType.packedValue);
}

void selfIpi(uint8_t vector) { cpuSetMSR((uint32_t)X2APICMSRs::SELF_IPI, vector, 0); }

void resetApicCounter() { writeReg((uint32_t)APICRegisters::TMR_INIT_CNT, 0xFFFFFFFF); }

auto checkX2APICSupport() -> bool {
    cpu::CpuidContext cpuidContext;
    cpuidContext.function = 0x1;
    cpu::cpuid(&cpuidContext);
    return (cpuidContext.ecx & (1 << 21)) > 0;
}

void init() {
    char ident[] = "APIC";
    acpi::ACPIResult madt = acpi::parseAcpiTables(ident);
    if (madt.success) {
        acpi::madt::ApicInfo apicInfo = acpi::madt::parseMadt(madt.data);
        APIC_BASE = (uint64_t)mm::addr::getVirtPointer(apicInfo.lapicAddr);
    } else {
        io::serial::write("Failed to parse MADT table\n");
        hcf();
    }
}

void initApicMP() {
    if (!checkX2APICSupport()) {
        [[unlikely]] io::serial::write("X2APIC not supported\n");
        hcf();
    }
    uint64_t msr;
    cpuGetMSR(IA32_APIC_BASE, &msr);
    cpuSetMSR(IA32_APIC_BASE, msr | (1 << 10) | (1 << 11));  // enable x2apic

    // setup the spurious interrupt vector
    writeReg((uint32_t)X2APICMSRs::SPURIOUS_INT_VEC, 0x1FF | (1 << 8));  // interrupt vector 0x1FF, enable APIC
}

auto calibrateTimer(uint64_t us) -> uint32_t {
    writeReg((uint32_t)X2APICMSRs::TIMER_DIVIDE_CONFIG, 0x3);
    writeReg((uint32_t)X2APICMSRs::TIMER_INIT_COUNT, 0xFFFFFFFF);
    uint64_t start = rdtsc();
    while (rdtsc() - start < us * 1000);
    writeReg((uint32_t)X2APICMSRs::LVT_TIMER, (0 << 16) | (0 << 17) | 32);
    return 0xFFFFFFFF - readReg((uint32_t)X2APICMSRs::TIMER_CURRENT_COUNT);
}

void oneShotTimer(uint64_t ticks) { writeReg((uint32_t)X2APICMSRs::TIMER_INIT_COUNT, ticks); }

uint64_t getTicks() { return readReg((uint32_t)X2APICMSRs::TIMER_CURRENT_COUNT); }

uint32_t getApicId() { return readReg((uint32_t)X2APICMSRs::ID); }

// void startInterrupts() { asm volatile("sti"); }
}  // namespace ker::mod::apic
