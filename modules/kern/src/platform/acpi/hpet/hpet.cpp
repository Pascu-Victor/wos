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
static int tick_period = 0;

void init() {
    if (hpet != nullptr) {
        return;
    }

    const char* hpet_sig = "HPET";

    acpi::ACPIResult const HPET_RESULT = acpi::parse_acpi_tables(hpet_sig);

    if (!HPET_RESULT.success) {
        // Panic!
        hcf();
    }

    uint64_t hpet_phys = 0;
    __builtin_memcpy(&hpet_phys, (const void*)((uint64_t)HPET_RESULT.data + HPET_OFFSET), sizeof(hpet_phys));
    auto* hpet_addr = mm::addr::get_virt_pointer(hpet_phys);

    // Map HPET to the kernel page table so all CPUs can access it
    mm::virt::map_to_kernel_page_table((uint64_t)hpet_addr, (uint64_t)mm::addr::get_phys_pointer((uint64_t)hpet_addr),
                                       mm::virt::page_types::KERNEL);

    hpet = reinterpret_cast<Hpet*>(hpet_addr);
    tick_period = (hpet->capabilities >> 32) & 0xFFFFFFFF;

    hpet->configuration = 0;  // clear hpet configuration
    hpet->counter_value = 0;  // reset counter value
    hpet->configuration = 1;  // enable hpet
}

uint64_t get_ticks() { return hpet->counter_value; }

static uint64_t usec_to_ticks(uint64_t us) { return (us * 1000000000) / tick_period; }

static uint64_t ticks_to_usec(uint64_t ticks) { return (ticks * tick_period) / 1000000000; }

uint64_t get_us() { return ticks_to_usec(get_ticks()); }

void sleep_ticks(uint64_t ticks) {
    uint64_t const START = get_ticks();
    while (get_ticks() - START < ticks) {
        asm volatile("pause");
    }
}

void sleep_us(uint64_t us) { sleep_ticks(usec_to_ticks(us)); }

}  // namespace ker::mod::hpet
