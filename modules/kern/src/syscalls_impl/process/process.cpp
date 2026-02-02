#include "process.hpp"

#include <cerrno>
#include <cstdint>
#include <string_view>

#include "abi/callnums/process.h"
#include "platform/asm/cpu.hpp"
#include "syscalls_impl/process/exec.hpp"
#include "syscalls_impl/process/exit.hpp"
#include "syscalls_impl/process/getpid.hpp"
#include "syscalls_impl/process/waitpid.hpp"

namespace ker::syscall::process {
auto process(abi::process::procmgmt_ops op, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    switch (op) {
        case abi::process::procmgmt_ops::exit:
            wos_proc_exit(static_cast<int>(a2));
            return 0;  // Should not reach here
        case abi::process::procmgmt_ops::exec: {
            return wos_proc_exec(reinterpret_cast<const char*>(a2), reinterpret_cast<const char* const*>(a3),
                                 reinterpret_cast<const char* const*>(a4));
        }
        case abi::process::procmgmt_ops::waitpid: {
            return wos_proc_waitpid(static_cast<int64_t>(a2), reinterpret_cast<int32_t*>(a3), static_cast<int32_t>(a4), gpr);
        }
        case abi::process::procmgmt_ops::getpid: {
            return wos_proc_getpid();
        }

        default:
            mod::io::serial::write("sys_process: unknown op\n");
            return static_cast<uint64_t>(ENOSYS);
    }
}
}  // namespace ker::syscall::process
