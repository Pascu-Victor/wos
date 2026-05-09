#include "hpet.hpp"

#include <cstdint>

#include "platform/acpi/acpi.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "util/hcf.hpp"

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

    acpi::ACPIResult hpetResult = acpi::parse_acpi_tables(hpetSig);

    if (!hpetResult.success) {
        // Panic!
        hcf();
    }

    uint64_t hpetPhys;
    __builtin_memcpy(&hpetPhys, (const void*)((uint64_t)hpetResult.data + HPET_OFFSET), sizeof(hpetPhys));
    auto hpetAddr = mm::addr::get_virt_pointer(hpetPhys);

    // Map HPET to the kernel page table so all CPUs can access it
    mm::virt::map_to_kernel_page_table((uint64_t)hpetAddr, (uint64_t)mm::addr::get_phys_pointer((uint64_t)hpetAddr),
                                       mm::virt::page_types::KERNEL);

    hpet = (Hpet*)hpetAddr;
    tickPeriod = (hpet->capabilities >> 32) & 0xFFFFFFFF;

    hpet->configuration = 0;  // clear hpet configuration
    hpet->counter_value = 0;  // reset counter value
    hpet->configuration = 1;  // enable hpet
}

uint64_t get_ticks() { return hpet->counter_value; }

uint64_t usecToTicks(uint64_t us) { return (us * 1000000000) / tickPeriod; }

uint64_t ticksToUsec(uint64_t ticks) { return (ticks * tickPeriod) / 1000000000; }

uint64_t get_us() { return ticksToUsec(get_ticks()); }

void sleep_ticks(uint64_t ticks) {
    uint64_t start = get_ticks();
    while (get_ticks() - start < ticks) {
        asm volatile("pause");
    }
}

void sleep_us(uint64_t us) { sleep_ticks(usecToTicks(us)); }

}  // namespace ker::mod::hpet
