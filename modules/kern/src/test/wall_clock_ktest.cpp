#include <platform/rtc/wall_clock.hpp>
#include <test/ktest.hpp>

KTEST(WallClock, BoundedStepAndInterpolatedSlew) {
    ker::mod::rtc::WallClock clock{.anchor_epoch_ns = 10000000000ULL};
    KEXPECT_TRUE(!clock.adjust(0, INT64_MIN));
    KEXPECT_TRUE(clock.adjust(0, 0));
    KEXPECT_TRUE(clock.adjust(0, -1000000));
    KEXPECT_EQ(clock.epoch(1000000000), 10999500000ULL);
    KEXPECT_EQ(clock.epoch(2000000000), 11999000000ULL);
    KEXPECT_EQ(clock.epoch(3000000000), 12999000000ULL);
    uint64_t epoch = 0;
    KEXPECT_TRUE(ker::mod::rtc::calendar_epoch(2000, 2, 29, 0, 0, 0, epoch));
    KEXPECT_EQ(epoch, 951782400ULL);
    KEXPECT_TRUE(!ker::mod::rtc::calendar_epoch(2100, 2, 29, 0, 0, 0, epoch));
}

KTEST(WallClock, NegativeSlewAtEpochSaturation) {
    ker::mod::rtc::WallClock clock{.anchor_epoch_ns = UINT64_MAX - 1000};
    KEXPECT_TRUE(clock.adjust(0, 0));
    KEXPECT_TRUE(clock.adjust(0, -1000000));
    KEXPECT_EQ(clock.epoch(999), UINT64_MAX - 1);
    KEXPECT_EQ(clock.epoch(2000), UINT64_MAX);
    KEXPECT_EQ(clock.epoch(4000), UINT64_MAX);
    KEXPECT_EQ(clock.epoch(UINT64_MAX), UINT64_MAX);
}
