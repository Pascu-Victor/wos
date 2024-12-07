#include "apic.hpp"

#include <platform/mm/paging.hpp>
#include <platform/mm/virt.hpp>

namespace ker::mod::apic {
void write(uint32_t offset, uint32_t value) { *(volatile uint32_t *)(LAPIC_BASE + offset) = value; }

uint32_t read(uint32_t offset) { return *(volatile uint32_t *)(LAPIC_BASE + offset); }

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

void setCpuId(uint32_t id) { write(LAPIC_ID, id << 24); }

void lapicWrite(acpi::madt::LAPIC *lapic, uint32_t offset, uint32_t value) {
    auto lapicAddr = mm::addr::getPhysPointer((uint64_t)lapic);
    *(volatile uint32_t *)(lapicAddr + offset) = value;
}

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
    mm::virt::mapToKernelPageTable((uint64_t)LAPIC_BASE, (uint64_t)apicInfo.lapicAddr, mm::paging::pageTypes::KERNEL);
    enable();
}

void oneShotTimer(uint64_t ticks) { write(LAPIC_TIMER_INIT_COUNT, ticks); }
}  // namespace ker::mod::apic

namespace ker::mod::apic2 {
static void writeReg(uint32_t reg, uint32_t value) { ((uint32_t *)APIC_BASE)[reg >> 2] = value; }

static uint32_t readReg(uint32_t reg) { return ((uint32_t *)APIC_BASE)[reg >> 2]; }

// end of apic interrupt
static void eoi() { writeReg((uint32_t)APICRegisters::EOI, 0); }

static void sendIpi(uint32_t lapicId, uint32_t vector, ICR0MessageType messageType) {
    while (readReg((uint32_t)APICRegisters::ICR0) & (uint32_t)APICQueries::ICR0_DELIVERY_STATUS) {
        asm volatile("pause");
    }
    // set the destination
    writeReg((uint32_t)APICRegisters::ICR1, lapicId << 24);
    // signal the interrupt
    writeReg((uint32_t)APICRegisters::ICR0, (uint32_t)messageType | vector);
}

static void resetApicCounter() { writeReg((uint32_t)APICRegisters::TMR_INIT_CNT, 0xFFFFFFFF); }

static bool checkX2APICSupport() {
    cpu::CpuidContext cpuidContext;
    cpuidContext.function = 0x1;
    cpu::cpuid(&cpuidContext);
    return (cpuidContext.ecx & (1 << 21)) > 0;
}

void initForCpu(uint64_t cpuNo) {
    if (!checkX2APICSupport()) {
        [[unlikely]] io::serial::write("X2APIC not supported\n");
        hcf();
    }
    uint64_t msr;
    cpuGetMSR(IA32_APIC_BASE, &msr);
    cpuSetMSR(IA32_APIC_BASE, msr | (1 << 10) | (1 << 11));  // enable x2apic
}

}  // namespace ker::mod::apic2
