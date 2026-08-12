#pragma once

#include <cstdint>
#include <platform/asm/cpu.hpp>

namespace ker::syscall::process {

// Exact x86-64 WOS/POSIX struct rusage layout.  Child-event snapshots populate
// the two timeval fields; every other field is deliberately zero-initialized.
struct KernRusage {
    int64_t ru_utime_sec;
    int64_t ru_utime_usec;
    int64_t ru_stime_sec;
    int64_t ru_stime_usec;
    int64_t ru_maxrss;
    int64_t ru_ixrss;
    int64_t ru_idrss;
    int64_t ru_isrss;
    int64_t ru_minflt;
    int64_t ru_majflt;
    int64_t ru_nswap;
    int64_t ru_inblock;
    int64_t ru_oublock;
    int64_t ru_msgsnd;
    int64_t ru_msgrcv;
    int64_t ru_nsignals;
    int64_t ru_nvcsw;
    int64_t ru_nivcsw;
};

static_assert(sizeof(KernRusage) == 144);

auto wos_proc_waitpid(int64_t pid, int32_t* status, int32_t options, uint64_t rusage_vaddr, ker::mod::cpu::GPRegs& gpr) -> uint64_t;

}  // namespace ker::syscall::process
