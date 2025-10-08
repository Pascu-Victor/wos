#include "sys_net.hpp"

#include <mod/io/serial/serial.hpp>

namespace ker::syscall::net {
uint64_t sys_net(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4) {
    mod::io::serial::write("sys_net called\n");
    return (uint64_t)-38; /* -ENOSYS */
}
}  // namespace ker::syscall::net
