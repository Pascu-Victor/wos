#include "ktime.hpp"

#include <cstdint>
#include <platform/rtc/rtc.hpp>
#include <platform/tsc/tsc.hpp>
#include <util/list.hpp>

#include "platform/acpi/apic/apic.hpp"
#include "platform/acpi/hpet/hpet.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/sched/task.hpp"
namespace ker::mod::time {

namespace {

using log = ker::mod::dbg::logger<"ktime">;

constexpr uint64_t APIC_CALIBRATION_US = 2000;

bool is_init = false;
util::List<void (*)(gates::InterruptFrame*)> tasks;

uint64_t ktime_pit_tick = 0;

[[maybe_unused]] void handle_pit(sched::task::Context ctx, gates::InterruptFrame* frame) {
    (void)ctx;
    ktime_pit_tick++;
    for (auto* task = tasks.get_head(); task != nullptr; task = task->next) {
        task->data(frame);
    }
    apic::eoi();
    auto const CALIBRATION_TICKS = apic::calibrate_timer(APIC_CALIBRATION_US);
    apic::one_shot_timer(CALIBRATION_TICKS);
    log::trace("PIT tick");
    asm volatile("iretq");
}

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

void push_task(uint64_t ticks, void (*task)(gates::InterruptFrame*), void* arg) {
    (void)ticks;
    (void)arg;
    tasks.push_back(task);
}
}  // namespace ker::mod::time
