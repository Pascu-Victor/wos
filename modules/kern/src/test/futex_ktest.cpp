#include <syscalls_impl/futex/futex.hpp>
#include <test/ktest.hpp>

KTEST(Futex, TableInitIsSerialized) { KEXPECT_TRUE(ker::syscall::futex::futex_selftest_table_init_is_serialized()); }

KTEST(Futex, RejectsUnalignedUserWord) { KEXPECT_TRUE(ker::syscall::futex::futex_selftest_addr_alignment_guard()); }

KTEST(Futex, StaleWakeDoesNotClaimWaiter) { KEXPECT_TRUE(ker::syscall::futex::futex_selftest_stale_wake_does_not_claim_waiter()); }

KTEST(Futex, WakeCountLimitIsValidated) { KEXPECT_TRUE(ker::syscall::futex::futex_selftest_wake_count_limit()); }
