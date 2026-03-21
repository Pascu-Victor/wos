#pragma once
#include <cstdint>
#include <platform/asm/cpu.hpp>

namespace ker::syscall::process {

// Mirror of POSIX struct rusage (x86-64 layout, only the fields we populate).
// Matches toolchain/src/mlibc/abis/wos/resource.h.
struct KernRusage {
    // struct timeval ru_utime
    int64_t ru_utime_sec;
    int64_t ru_utime_usec;
    // struct timeval ru_stime
    int64_t ru_stime_sec;
    int64_t ru_stime_usec;
    // remaining fields (ru_maxrss … ru_nivcsw) — zero-initialised by caller, not filled here
};

// rusage_vaddr: user-space virtual address of struct rusage to fill (0 if unused)
auto wos_proc_waitpid(int64_t pid, int32_t* status, int32_t options, uint64_t rusage_vaddr, ker::mod::cpu::GPRegs& gpr) -> uint64_t;
}
