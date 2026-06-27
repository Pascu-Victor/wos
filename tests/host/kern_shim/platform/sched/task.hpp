#pragma once

// Host shim: provides an opaque Task type so WKI headers that reference
// Task* compile on the host.  No real scheduling is needed.

#include <cstdint>

namespace ker::mod::sched::task {

enum class TaskType : uint8_t {
    DAEMON,
    PROCESS,
    IDLE,
};

enum class WaitChannelKind : uint8_t {
    NONE,
    GENERIC,
    LOCAL_PIPE,
    LOCAL_PTY,
    WAITPID,
    FUTEX,
    SIGSUSPEND,
    WKI_EXECVE_PROXY,
    PTRACE,
};

struct Task {
    uint64_t id = 0;
    uint64_t pid = 0;
    TaskType type = TaskType::DAEMON;
};

}  // namespace ker::mod::sched::task
