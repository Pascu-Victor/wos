#include "apic.hpp"

#include <platform/mm/paging.hpp>
#include <platform/mm/virt.hpp>
namespace ker::mod::apic {
void writeReg(uint32_t reg, uint32_t value) { cpuSetMSR(reg, value); }

uint32_t readReg(uint32_t reg) {
    uint64_t res;
    cpuGetMSR(reg, &res);
    return res;
}

// end of apic interrupt
void eoi() { writeReg((uint32_t)APICRegisters::EOI, 0); }

void sendIpi(uint32_t lapicId, uint32_t vector, ICR0MessageType messageType) {
    while (readReg((uint32_t)APICRegisters::ICR0) & (uint32_t)APICQueries::ICR0_DELIVERY_STATUS) {
        asm volatile("pause");
    }
    // set the destination
    writeReg((uint32_t)APICRegisters::ICR1, lapicId << 24);
    // signal the interrupt
    writeReg((uint32_t)APICRegisters::ICR0, (uint32_t)messageType | vector);
}

void resetApicCounter() { writeReg((uint32_t)APICRegisters::TMR_INIT_CNT, 0xFFFFFFFF); }

bool checkX2APICSupport() {
    cpu::CpuidContext cpuidContext;
    cpuidContext.function = 0x1;
    cpu::cpuid(&cpuidContext);
    return (cpuidContext.ecx & (1 << 21)) > 0;
}

void init() {
    acpi::madt::ApicInfo apicInfo;
    char ident[] = "APIC";
    acpi::ACPIResult madt = acpi::parseAcpiTables(ident);
    if (madt.success) {
        apicInfo = acpi::madt::parseMadt(madt.data);
    } else {
        io::serial::write("Failed to parse MADT table\n");
        hcf();
    }

    APIC_BASE = (uint64_t)mm::addr::getVirtPointer(apicInfo.lapicAddr);
    // mm::virt::mapToKernelPageTable((uint64_t)APIC_BASE, (uint64_t)apicInfo.lapicAddr, mm::paging::pageTypes::KERNEL);
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

uint32_t calibrateTimer(uint64_t us) {
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
