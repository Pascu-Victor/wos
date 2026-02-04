#include "ktime.hpp"

namespace ker::mod::time {
bool isInit = false;

std::list<void (*)(gates::interruptFrame*)> tasks;

static uint64_t ktimePITTick = 0;

void handle_pit(sched::task::Context ctx, gates::interruptFrame* frame) {
    (void)ctx;
    ktimePITTick++;
    for (auto& task : tasks) {
        task(frame);
    }
    apic::eoi();
    auto c = apic::calibrateTimer(2000);
    apic::oneShotTimer(c);
    dbg::log("PIT tick!");
    asm volatile("iretq");
}

void init() {
    if (isInit) {
        return;
    }

    // HPET
    hpet::init();

    isInit = true;
}

void pushTask(uint64_t ticks, void (*task)(gates::interruptFrame*), void* arg) {
    (void)ticks;
    (void)arg;
    tasks.push_back(task);
}
}  // namespace ker::mod::time
