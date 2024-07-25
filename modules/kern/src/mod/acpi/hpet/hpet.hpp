#pragma once

#include <defines/defines.hpp>
#include <mod/acpi/tables/sdt.hpp>
#include <mod/mm/mm.hpp>
#include <mod/acpi/acpi.hpp>
#include <mod/asm/tlb.hpp>

namespace ker::mod::hpet {
    static const uint64_t HPET_OFFSET = 44;

    struct Hpet {
        uint64_t capabilities;
        uint64_t reserved0;
        uint64_t configuration;
        uint64_t reserved1;
        uint64_t interrupt_status;
        uint64_t reserved2;
        uint64_t reserved3[24];
        uint64_t counter_value;
        uint64_t reserved4;
    };

    void init();

    uint64_t getTicks(void);
    uint64_t getUs(void);

    void sleepTicks(uint64_t ticks);
    void sleepUs(uint64_t us);
}