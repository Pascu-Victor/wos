#pragma once
#include <cstdint>

namespace ker::abi::process {
// Syscall operation selectors are carried in 64-bit registers.
// NOLINTNEXTLINE(performance-enum-size)
enum class procmgmt_ops : uint64_t {
    EXIT,           // 0
    EXEC,           // 1
    WAITPID,        // 2
    GETPID,         // 3
    GETPPID,        // 4
    FORK,           // 5
    SIGACTION,      // 6
    SIGPROCMASK,    // 7
    KILL,           // 8
    SIGRETURN,      // 9
    GETUID,         // 10
    GETEUID,        // 11
    GETGID,         // 12
    GETEGID,        // 13
    SETUID,         // 14
    SETGID,         // 15
    SETEUID,        // 16
    SETEGID,        // 17
    GETUMASK,       // 18
    SETUMASK,       // 19
    SETSID,         // 20
    GETSID,         // 21
    SETPGID,        // 22
    GETPGID,        // 23
    EXECVE,         // 24 - POSIX replace-process execve
    GETHOSTNAME,    // 25
    SETHOSTNAME,    // 26
    SETPRIORITY,    // 27
    SETWKITARGET,   // 28
    GETWKITARGET,   // 29
    PTRACE,         // 30
    GETGROUPS,      // 31
    SETGROUPS,      // 32
    SIGSUSPEND,     // 33
    UNAME,          // 34
    CLONE_VM_PROC,  // 35
    PRCTL,          // 36
    ARCH_PRCTL,     // 37
    SIGALTSTACK,    // 38
    GETRESUID,      // 39
    GETRESGID,      // 40
    SIGPENDING,     // 41
    GETPRIORITY,    // 42
    SPAWN,          // 43 - WOS fast posix_spawn-style create-process path
};

enum class SpawnFdActionType : uint32_t {
    CLOSE = 1,
    DUP2 = 2,
    OPEN = 3,
};

constexpr uint64_t SPAWN_FLAG_SETSIGMASK = 1ULL << 0;
constexpr uint64_t SPAWN_FLAG_SETPGROUP = 1ULL << 1;
constexpr uint64_t SPAWN_FLAG_USEVFORK = 1ULL << 2;
constexpr uint64_t SPAWN_SUPPORTED_FLAGS = SPAWN_FLAG_SETSIGMASK | SPAWN_FLAG_SETPGROUP | SPAWN_FLAG_USEVFORK;
constexpr uint64_t SPAWN_OPTIONS_VERSION = 1;

struct SpawnFdAction {
    uint32_t type;
    int32_t fd;
    int32_t srcfd;
    int32_t oflag;
    uint32_t mode;
    const char* path;
};

struct SpawnOptions {
    uint64_t size;
    uint64_t version;
    uint64_t flags;
    uint64_t sig_mask;
    int64_t pgroup;
    const SpawnFdAction* actions;
    uint64_t action_count;
    uint64_t reserved0;
    uint64_t reserved1;
};
}  // namespace ker::abi::process
