#pragma once

#include <cstdint>

namespace ker::syscall::process {

auto wos_proc_init_control_submit(uint64_t request_addr) -> uint64_t;
auto wos_proc_init_control_receive(uint64_t request_addr) -> uint64_t;
auto wos_proc_init_status_publish(uint64_t status_addr) -> uint64_t;
auto wos_proc_init_status_read(uint64_t status_addr) -> uint64_t;

}  // namespace ker::syscall::process
