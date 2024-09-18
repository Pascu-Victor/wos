#include "hpet.hpp"

namespace ker::mod::hpet {
static volatile Hpet* hpet = nullptr;
// Intel hpet spec 1-0a: General Capabilities and ID Register Bit Definitions
// This indicates the period at which
// the counter increments in femtoseconds (10^-15 seconds)
static int tickPeriod = 0;

void init() {
    if (hpet != nullptr) {
        return;
    }

    const char* hpetSig = "HPET";

    acpi::ACPIResult hpetResult = acpi::parseAcpiTables(hpetSig);

    if (!hpetResult.success) {
        // Panic!
        hcf();
    }

    auto hpetAddr = mm::addr::getVirtPointer(*(uint64_t*)((uint64_t)hpetResult.data + HPET_OFFSET));

    mm::virt::mapPage((mm::paging::PageTable*)mm::addr::getVirtPointer(rdcr3()), (uint64_t)hpetAddr,
                      (uint64_t)mm::addr::getPhysPointer((uint64_t)hpetAddr), mm::virt::pageTypes::KERNEL);

    hpet = (Hpet*)hpetAddr;
    tickPeriod = (hpet->capabilities >> 32) & 0xFFFFFFFF;

    hpet->configuration = 0;  // clear hpet configuration
    hpet->counter_value = 0;  // reset counter value
    hpet->configuration = 1;  // enable hpet
}

uint64_t getTicks() { return hpet->counter_value; }

uint64_t usecToTicks(uint64_t us) { return (us * 1000000000) / tickPeriod; }

uint64_t ticksToUsec(uint64_t ticks) { return (ticks * tickPeriod) / 1000000000; }

uint64_t getUs() { return ticksToUsec(getTicks()); }

void sleepTicks(uint64_t ticks) {
    uint64_t start = getTicks();
    while (getTicks() - start < ticks) {
        asm volatile("pause");
    }
}

void sleepUs(uint64_t us) { sleepTicks(usecToTicks(us)); }

}  // namespace ker::mod::hpet
