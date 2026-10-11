#pragma once

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::syscall::process {

// Caller retains subject; global init control/status requires canonical Host scope.
auto init_control_submit_for_subject(ker::mod::sched::task::Task* subject, uint64_t address) -> uint64_t;
auto init_control_receive_for_subject(ker::mod::sched::task::Task* subject, uint64_t address) -> uint64_t;
auto init_status_publish_for_subject(ker::mod::sched::task::Task* subject, uint64_t address) -> uint64_t;
auto init_status_read_for_subject(ker::mod::sched::task::Task* subject, uint64_t address) -> uint64_t;

auto wos_proc_init_control_submit(uint64_t request_addr) -> uint64_t;
auto wos_proc_init_control_receive(uint64_t request_addr) -> uint64_t;
auto wos_proc_init_status_publish(uint64_t status_addr) -> uint64_t;
auto wos_proc_init_status_read(uint64_t status_addr) -> uint64_t;

#ifdef WOS_SELFTEST
auto init_control_selftest_mailbox() -> bool;
auto init_control_selftest_validation() -> bool;
auto init_control_selftest_process_role() -> bool;
#endif

}  // namespace ker::syscall::process
