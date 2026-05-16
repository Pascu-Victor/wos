#pragma once
#include <cstdint>

namespace ker::abi::process {
// Syscall operation selectors are carried in 64-bit registers.
// NOLINTNEXTLINE(performance-enum-size)
enum class procmgmt_ops : uint64_t {
    EXIT,          // 0
    EXEC,          // 1
    WAITPID,       // 2
    GETPID,        // 3
    GETPPID,       // 4
    FORK,          // 5
    SIGACTION,     // 6
    SIGPROCMASK,   // 7
    KILL,          // 8
    SIGRETURN,     // 9
    GETUID,        // 10
    GETEUID,       // 11
    GETGID,        // 12
    GETEGID,       // 13
    SETUID,        // 14
    SETGID,        // 15
    SETEUID,       // 16
    SETEGID,       // 17
    GETUMASK,      // 18
    SETUMASK,      // 19
    SETSID,        // 20
    GETSID,        // 21
    SETPGID,       // 22
    GETPGID,       // 23
    EXECVE,        // 24 - POSIX replace-process execve
    GETHOSTNAME,   // 25
    SETHOSTNAME,   // 26
    SETPRIORITY,   // 27
    SETWKITARGET,  // 28
    GETWKITARGET,  // 29
    PTRACE,        // 30
    GETGROUPS,     // 31
    SETGROUPS,     // 32
};
}  // namespace ker::abi::process
