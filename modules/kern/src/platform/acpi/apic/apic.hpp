#pragma once

#include <defines/defines.hpp>
#include <platform/asm/msr.hpp>
#include <platform/acpi/madt/madt.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/mm/addr.hpp>
#include <mod/io/port/port.hpp>
#include <mod/io/serial/serial.hpp>

namespace ker::mod::apic {
    static uint64_t LAPIC_BASE = 0x0;
    static const uint64_t LAPIC_ID = 0x20;
    static const uint64_t LAPIC_EOI = 0xb0;
    static const uint64_t LAPIC_SPURIOUS = 0xf0;
    static const uint64_t LAPIC_LVT_TIMER = 0x320;
    static const uint64_t LAPIC_TIMER_MASK = 0x10000;
    static const uint64_t LAPIC_TIMER_PERIODIC = 0x20000;
    static const uint64_t LAPIC_TIMER_INIT_COUNT = 0x380;
    static const uint64_t LAPIC_TIMER_CURRENT_COUNT = 0x390;
    static const uint64_t LAPIC_TIMER_DIV = 0x3e0;

    void write(uint32_t offset, uint32_t value);
    uint32_t read(uint32_t offset);

    void enable(void);
    void eoi(void);

    uint32_t calibrateTimer(uint64_t us);
    void setTimeout(uint64_t numTicks);
    int cpuid(void);

    void init(void);
}