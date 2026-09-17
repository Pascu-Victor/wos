#include "ktime.hpp"

#include <cstdint>
#include <platform/rtc/rtc.hpp>
#include <platform/tsc/tsc.hpp>

#include "platform/acpi/apic/apic.hpp"
#include "platform/acpi/hpet/hpet.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/sched/task.hpp"
namespace ker::mod::time {

namespace {

bool is_init = false;

}  // namespace

void init() {
    if (is_init) {
        return;
    }

    // HPET must be first - it is the calibration reference for TSC.
    hpet::init();

    // Calibrate the invariant TSC against the now-ready HPET.
    tsc::init();

    // Read the CMOS real-time clock for wall-clock initialisation.
    rtc::init();

    is_init = true;
}

}  // namespace ker::mod::time
