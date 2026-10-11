#pragma once
#include <cstdint>

namespace ker::abi::process {
// Fixed WOS authority ceiling: bits alone do not grant nonroot authority.
// Explicit service privileges below are also bounded by matching ceiling bits.
enum class CredentialAuthority : uint8_t {
    SET_UID = 0,
    SET_GID = 1,
    DAC_OVERRIDE = 2,
    FILE_OWNER = 3,
    SIGNAL = 4,
    PTRACE = 5,
    PRIORITY = 6,
    POWER = 7,
    INIT_CONTROL = 8,
    RESOURCE_LIMIT = 9,
    MOUNT = 10,
    DEVICE_CONTROL = 11,
    NETWORK_ADMIN = 12,
    HOSTNAME = 13,
    KERNEL_DEBUG = 14,
    WKI_ADMIN = 15,
    COUNT = 16,
};
inline constexpr uint64_t CREDENTIAL_AUTHORITY_ALL = (uint64_t{1} << 16) - 1;

enum class ServicePrivilege : uint8_t { JOURNAL_READ = 0, NETWORK_ADMIN = 1, COUNT = 2 };
inline constexpr uint64_t SERVICE_PRIVILEGE_ALL = 3;
inline constexpr uint64_t SERVICE_PRIVILEGES_LOCKED = 1U << 8;
// Only the two declared operations can be delegated; this is not a general
// nonroot version of CredentialAuthority. Validate unknown mask bits first.
constexpr auto service_privilege_ceiling(uint64_t mask) -> uint64_t {
    return ((mask & 1U) != 0 ? uint64_t{1} << static_cast<unsigned>(CredentialAuthority::KERNEL_DEBUG) : 0) |
           ((mask & 2U) != 0 ? uint64_t{1} << static_cast<unsigned>(CredentialAuthority::NETWORK_ADMIN) : 0);
}

enum class CredentialControl : uint32_t {
    SET_NO_NEW_PRIVILEGES = 38,
    GET_NO_NEW_PRIVILEGES = 39,
    GET_AUTHORITY_CEILING = 0x574f5301,
    DROP_AUTHORITY_CEILING = 0x574f5302,
    SET_SERVICE_PRIVILEGES = 0x574f5303,
    GET_SERVICE_PRIVILEGES = 0x574f5304,
};

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
    INIT_CONTROL_SUBMIT = 44,
    INIT_CONTROL_RECEIVE = 45,
    INIT_STATUS_PUBLISH = 46,
    INIT_STATUS_READ = 47,
    GETRLIMIT = 48,
    SETRLIMIT = 49,
    RESOURCE_USAGE = 50,
    SERVICE_RESOURCE_CONFIGURE = 51,
    SERVICE_RESOURCE_SELECT = 52,
    SERVICE_RESOURCE_READ = 53,
    COMPARTMENT_CREATE = 54,
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
