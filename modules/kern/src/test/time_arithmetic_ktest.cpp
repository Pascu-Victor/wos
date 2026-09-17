#include <platform/ktime/arithmetic.hpp>
#include <test/ktest.hpp>

namespace arithmetic = ker::mod::time::arithmetic;

KTEST(TimeArithmetic, LongUptimeAndSaturation) {
    KEXPECT_EQ(arithmetic::mul_div(259200000000000ULL, 10000000, 1000000000), 2592000000000ULL);
    KEXPECT_EQ(arithmetic::mul_div(UINT64_MAX, UINT64_MAX, UINT64_MAX), UINT64_MAX);
    KEXPECT_EQ(arithmetic::mul_div(UINT64_MAX, UINT64_MAX, 1), UINT64_MAX);
    KEXPECT_EQ(arithmetic::mul_div(1, 1000000000, 3000000000ULL, true), 1ULL);
    KEXPECT_EQ(arithmetic::mul_div(0, 1, 0), UINT64_MAX);
    KEXPECT_EQ(arithmetic::add_signed(0, INT64_MIN), 0ULL);
    KEXPECT_EQ(arithmetic::add_signed(UINT64_MAX, INT64_MIN), static_cast<uint64_t>(INT64_MAX));
    KEXPECT_EQ(arithmetic::saturating_signed_add(INT64_MAX, 1), INT64_MAX);
    KEXPECT_EQ(arithmetic::periodic_next(10, 7, 32), 38ULL);
}
