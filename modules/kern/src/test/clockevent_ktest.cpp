#include <platform/ktime/clockevent.hpp>
#include <test/ktest.hpp>

KTEST(Clockevent, RoundUpAndCounterLimit) {
    using ker::mod::time::ClockeventCalibration;
    ClockeventCalibration const SCALE{3, 10};
    KEXPECT_EQ(SCALE.ticks_for_delay_us(4), uint32_t{2});
    KEXPECT_EQ(SCALE.ticks_for_delay_us(10), uint32_t{3});
    KEXPECT_EQ(SCALE.ticks_for_delay_us(UINT64_MAX), UINT32_MAX);
    KEXPECT_EQ(SCALE.resolution_ns(), uint64_t{3334});
    ClockeventCalibration const MISSING{};
    KEXPECT_EQ(MISSING.ticks_for_delay_us(10), uint32_t{1});
    KEXPECT_FALSE(MISSING.valid());
}
