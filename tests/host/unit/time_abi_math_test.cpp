#include <abi/callnums/time.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <syscalls_impl/time/time_math.hpp>

TEST(TimeAbiMath, ConversionClocksAndDeadlines) {
    namespace math = ker::syscall::time::math;
    static_assert(static_cast<uint64_t>(ker::abi::sys_time_ops::GETITIMER) == 5);
    static_assert(static_cast<uint64_t>(ker::abi::sys_time_ops::CLOCK_GETRES) == 6);
    static_assert(static_cast<uint64_t>(ker::abi::sys_time_ops::CLOCK_NANOSLEEP) == 7);
    static_assert(offsetof(ker::abi::ClockSleepRequest, remaining_address) == 24);
    static_assert(static_cast<uint64_t>(ker::abi::sys_time_ops::SETITIMER_V1) == 8);
    static_assert(sizeof(ker::abi::SetItimerRequest) == 24);
    static_assert(offsetof(ker::abi::SetItimerRequest, old_value_address) == 16);
    uint64_t value = 0;
    ASSERT_TRUE(!math::to_ns(-1, 0, value));
    ASSERT_TRUE(!math::to_ns(0, -1, value));
    ASSERT_TRUE(!math::to_ns(0, 1000000000, value));
    ASSERT_TRUE(!math::to_ns(INT64_MAX, 0, value));
    ASSERT_TRUE(math::to_ns(1, 999999999, value) && value == 1999999999);
    ASSERT_TRUE(math::to_ns(UINT64_MAX / 1000000000, UINT64_MAX % 1000000000, value) && value == UINT64_MAX);
    ASSERT_TRUE(!math::to_ns(UINT64_MAX / 1000000000, UINT64_MAX % 1000000000 + 1, value));
    ASSERT_TRUE(math::add(UINT64_MAX - 1, 2) == UINT64_MAX);
    ASSERT_TRUE(math::ceil_us(1) == 1);
    ASSERT_TRUE(math::ceil_us(UINT64_MAX) == UINT64_MAX / 1000 + 1);
    ASSERT_TRUE(math::park_deadline_us(1001, 1, false) == 2);
    // Realtime absolute deadlines are reevaluated within one millisecond;
    // relative/monotonic deadlines are not shortened by wall-clock policy.
    ASSERT_TRUE(math::park_deadline_us(0, 1000000000, true) == 1000);
    ASSERT_TRUE(math::park_deadline_us(0, 1000000000, false) == 1000000);
    ASSERT_TRUE(math::park_deadline_us(UINT64_MAX - 1, 5000, false) == math::ceil_us(UINT64_MAX));
    ASSERT_TRUE(!math::valid_clock(-1) && !math::valid_clock(4));
    ASSERT_TRUE(math::valid_clock(2) && !math::sleep_clock(2));
    ASSERT_TRUE(math::sleep_clock(0) && math::sleep_clock(1));
}
