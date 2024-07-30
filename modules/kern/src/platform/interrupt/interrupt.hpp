#pragma once
#include <platform/interrupt/idt.hpp>
#include <platform/acpi/apic/apic.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>

namespace ker::mod::interrupt {
    void init(void);
}