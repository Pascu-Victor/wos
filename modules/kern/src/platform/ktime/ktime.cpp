#include "ktime.hpp"

namespace ker::mod::time {
bool isInit = false;

static uint64_t ktimePITTick = 0;

void handle_pit(gates::interruptFrame *frame) {
    (void)frame;
    ktimePITTick++;
}

void init(void) {
    if (isInit) {
        return;
    }

    gates::setInterruptHandler(0x0, handle_pit);

    // HPET
    hpet::init();

    dbg::enableTime();
    isInit = true;
}
}  // namespace ker::mod::time
