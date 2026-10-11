#pragma once

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::syscall::net {
uint64_t sys_net(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5);

#if defined(WOS_SELFTEST) && WOS_SELFTEST
// The real dispatcher with an explicit subject; boot fixtures use only
// nonblocking operations and never replace scheduler current-task state.
auto net_selftest_call_as(ker::mod::sched::task::Task& subject, uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4,
                          uint64_t a5) -> uint64_t;
auto net_selftest_socket_zero_read_does_not_receive() -> bool;
auto net_selftest_link_writer_admission() -> bool;
auto net_selftest_link_open_uses_admitted_snapshot() -> bool;
auto net_selftest_link_close_uses_admitted_snapshot() -> bool;
auto net_selftest_link_combined_admission_precedes_all_effects() -> bool;
auto net_selftest_link_rename_admission_precedes_effects() -> bool;
auto net_selftest_open_error_preserves_link_request() -> bool;
#endif

}  // namespace ker::syscall::net
