#pragma once

#include <util/mem.hpp>
#include <mod/acpi/apic/apic.hpp>
#include <mod/interrupt/gates.hpp>
#include <mod/io/serial/serial.hpp>

namespace ker::mod::time {

    #define IA32_APIC_BASE_MSR 0x1B
    #define IA32_APIC_BASE_MSR_BSP 0x100 // Processor is a BSP
    #define IA32_APIC_BASE_MSR_ENABLE 0x800

    void init(void);
    uint64_t getTicks(void);
    uint64_t getUs(void);
    uint64_t getMs(void);
    uint64_t getTimerTicks(void);
    void sleep(uint64_t us);
    void sleepTicks(uint64_t ticks);
    void sleepMs(uint64_t ms);
}