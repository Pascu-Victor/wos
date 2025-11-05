#include "process.hpp"

#include <cerrno>
#include <string_view>

#include "abi/callnums/process.h"
#include "syscalls_impl/process/exec.hpp"
#include "syscalls_impl/process/exit.hpp"

namespace ker::syscall::process {
auto process(abi::process::procmgmt_ops op, u_int64_t a1, u_int64_t a2, u_int64_t a3, u_int64_t a4) -> u_int64_t {
    switch (op) {
        case abi::process::procmgmt_ops::exit:
            wos_proc_exit(static_cast<int>(a1));
            return 0;  // Should not reach here
        case abi::process::procmgmt_ops::exec: {
            return wos_proc_exec(reinterpret_cast<const char*>(a1), reinterpret_cast<const char* const*>(a2),
                                 reinterpret_cast<const char* const*>(a4));
        }

        default:
            mod::io::serial::write("sys_process: unknown op\n");
            return static_cast<u_int64_t>(ENOSYS);
    }
}
}  // namespace ker::syscall::process
