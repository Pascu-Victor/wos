#include <platform/interrupt/gates.hpp>
#include <test/ktest.hpp>

KTEST(InterruptLifetime, RetirementClosesAdmission) { KEXPECT_TRUE(ker::mod::gates::irq_selftest_retirement_admission()); }

KTEST(InterruptLifetime, VectorAllocationReservesDistinctSlots) { KEXPECT_TRUE(ker::mod::gates::irq_selftest_vector_reservation()); }
