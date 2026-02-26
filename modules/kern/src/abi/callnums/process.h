#pragma once
#include <cstdint>

namespace ker::abi::process {
enum class procmgmt_ops : uint64_t {
    exit,         // 0
    exec,         // 1
    waitpid,      // 2
    getpid,       // 3
    getppid,      // 4
    fork,         // 5
    sigaction,    // 6
    sigprocmask,  // 7
    kill,         // 8
    sigreturn,    // 9
    getuid,       // 10
    geteuid,      // 11
    getgid,       // 12
    getegid,      // 13
    setuid,       // 14
    setgid,       // 15
    seteuid,      // 16
    setegid,      // 17
    getumask,     // 18
    setumask,     // 19
    setsid,       // 20
    getsid,       // 21
    setpgid,      // 22
    getpgid,      // 23
    execve,       // 24 — POSIX replace-process execve
};
}  // namespace ker::abi::process
