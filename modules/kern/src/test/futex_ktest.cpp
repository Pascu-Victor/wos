#include <syscalls_impl/futex/futex.hpp>
#include <test/ktest.hpp>

KTEST(Futex, TableInitIsSerialized) { KEXPECT_TRUE(ker::syscall::futex::futex_selftest_table_init_is_serialized()); }

KTEST(Futex, RejectsUnalignedUserWord) { KEXPECT_TRUE(ker::syscall::futex::futex_selftest_addr_alignment_guard()); }
