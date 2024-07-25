#pragma once
#include <mod/interrupt/idt.hpp>
#include <mod/acpi/apic/apic.hpp>
#include <mod/interrupt/gates.hpp>
#include <mod/ktime/ktime.hpp>

namespace ker::mod::interrupt {
    void init(void);
}