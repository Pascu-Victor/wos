#include "interrupt.hpp"

namespace ker::mod::interrupt {


    void init(void) {
        pic::remap();
        // Init ACPI.
        ker::mod::acpi::init();
        // Init APIC.
        ker::mod::apic::init();
        // Init ktime
        ker::mod::time::init();
        // Init interrupt descriptor table.
        ker::mod::desc::idt::idtInit();
    }
}