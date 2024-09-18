#include "apic.hpp"

namespace ker::mod::apic {
void write(uint32_t offset, uint32_t value) { *(volatile uint32_t*)(LAPIC_BASE + offset) = value; }

uint32_t read(uint32_t offset) { return *(volatile uint32_t*)(LAPIC_BASE + offset); }

void enable(void) { write(LAPIC_LVT_TIMER, LAPIC_TIMER_MASK); }

void eoi(void) { write(LAPIC_EOI, 0); }

uint32_t calibrateTimer(uint64_t us) {
    write(LAPIC_TIMER_DIV, 0x3);
    write(LAPIC_TIMER_INIT_COUNT, 0xFFFFFFFF);
    uint64_t start = rdtsc();
    while (rdtsc() - start < us * 1000);
    write(LAPIC_LVT_TIMER, (0 << 16) | (0 << 17) | 32);
    return 0xFFFFFFFF - read(LAPIC_TIMER_CURRENT_COUNT);
}

void setTimeout(uint64_t numTicks) { write(LAPIC_TIMER_INIT_COUNT, numTicks); }

int cpuid(void) { return read(LAPIC_ID) >> 24; }

void init(void) {
    acpi::madt::ApicInfo apicInfo;
    char ident[] = "APIC";
    acpi::ACPIResult madt = acpi::parseAcpiTables(ident);
    if (madt.success) {
        apicInfo = acpi::madt::parseMadt(madt.data);
    } else {
        io::serial::write("Failed to parse MADT table\n");
        hcf();
    }

    LAPIC_BASE = (uint64_t)mm::addr::getVirtPointer(apicInfo.lapicAddr);
    enable();
}
}  // namespace ker::mod::apic
