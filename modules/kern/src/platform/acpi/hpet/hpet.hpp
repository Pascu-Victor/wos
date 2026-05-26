#pragma once

#include <cstddef>
#include <defines/defines.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/acpi/tables/sdt.hpp>
#include <platform/asm/tlb.hpp>
#include <platform/mm/mm.hpp>

namespace ker::mod::hpet {
static const uint64_t HPET_OFFSET = 44;

struct Hpet {
    uint64_t capabilities;
    uint64_t reserved0;
    uint64_t configuration;
    uint64_t reserved1;
    uint64_t interrupt_status;
    uint64_t reserved2;
    uint64_t reserved3[24];  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): MMIO register gap.
    uint64_t counter_value;
    uint64_t reserved4;
};

static_assert(offsetof(Hpet, counter_value) == 0xF0);
static_assert(sizeof(Hpet) == 0x100);

void init();

uint64_t get_ticks();
uint64_t get_us();

void sleep_ticks(uint64_t ticks);
void sleep_us(uint64_t us);
}  // namespace ker::mod::hpet
