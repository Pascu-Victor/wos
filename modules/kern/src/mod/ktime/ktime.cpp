#include "ktime.hpp"



inline static bool isInit = false;

namespace ker::mod::time {

    static uint64_t ktimeTicks = 0;
    static uint64_t ktimePITTick = 0;

    void handle_timer(gates::interruptFrame *frame) {
        (void)frame;
        ktimeTicks++;
    }

    void handle_pit(gates::interruptFrame *frame) {
        (void)frame;
        ktimePITTick++;
    }

    void init(void) {
        if (true || isInit) {
            return;
        }

        gates::setInterruptHandler(0x0, handle_timer);

        isInit = true;
    }


    uint64_t getTicks(void) {
        return 0;
        uint64_t ticks = getTimerTicks();
        return ticks;
    }

    void sleep(uint64_t ms) {
        uint64_t start = getTicks();
        while (getTicks() - start < ms) {
            asm volatile("hlt");
        }
    }

    uint64_t getUs(void) {
        return getTicks() / 1000;
    }

    uint64_t getMs(void) {
        return getUs() / 1000;
    }

    void sleepTicks(uint64_t ticks) {
        uint64_t start = getTicks();
        while (getTicks() - start < ticks) {
            asm volatile("hlt");
        }
    }

    void sleepMs(uint64_t ms) {
        sleepTicks(ms * 1000);
    }

    void sleepUs(uint64_t us) {
        sleepTicks(us);
    }

}