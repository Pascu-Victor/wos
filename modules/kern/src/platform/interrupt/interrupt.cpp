#include "interrupt.hpp"

#include "platform/acpi/acpi.hpp"
#include "platform/acpi/apic/apic.hpp"
#include "platform/interrupt/idt.hpp"
#include "platform/ktime/ktime.hpp"
#include "platform/pic/pic.hpp"

namespace ker::mod::interrupt {

void init() {
    pic::remap();
    // Init ACPI.
    ker::mod::acpi::init();
    // Init APIC.
    ker::mod::apic::init();
    ker::mod::apic::init_apic_mp();
    // Init ktime
    ker::mod::time::init();
    // Init interrupt descriptor table.
    ker::mod::desc::idt::idt_init();
}
}  // namespace ker::mod::interrupt
