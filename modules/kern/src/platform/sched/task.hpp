#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <defines/defines.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/mm/paging.hpp>
// #include <platform/sys/context_switch.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/sched/threading.hpp>
#include <util/list.hpp>
// Avoid pulling in in-tree std headers into kernel translation units from here.

namespace ker::mod::sched::task {

enum TaskType {
    DAEMON,
    PROCESS,
    IDLE,
};

struct Context {
    uint64_t syscallKernelStack;
    uint64_t syscallScratchArea;  // Small scratch area for syscall handler (RIP, RSP, RFLAGS, DS, ES)

    cpu::GPRegs regs;

    uint64_t intNo;
    uint64_t errorCode;

    gates::interruptFrame frame;
} __attribute__((packed));

struct Task {
    Task(Task&&) = delete;
    auto operator=(const Task&) -> Task& = delete;
    auto operator=(Task&&) -> Task& = delete;
    Task(const char* name, uint64_t elfStart, uint64_t kernelRsp, TaskType type);

    Task(const Task& task);

    mm::paging::PageTable* pagemap;
    Context context;
    uint64_t entry;

    const char* name;
    TaskType type;
    uint64_t cpu;
    threading::Thread* thread;
    uint64_t pid;
    uint64_t parentPid;  // Parent process ID (0 for orphaned/init processes)

    // has the task run at least once
    bool hasRun;

    // ELF buffer for cleanup
    uint8_t* elfBuffer;
    size_t elfBufferSize;

    // ELF metadata for auxv setup
    uint64_t programHeaderAddr;  // Virtual address of program headers (AT_PHDR)
    uint64_t elfHeaderAddr;      // Virtual address of ELF header (AT_EHDR)

    // File descriptor table for the task (per-process model planned).
    // Small fixed-size table for now; will be moved/made dynamic when process struct is available.
    static constexpr unsigned FD_TABLE_SIZE = 256;
    void* fds[FD_TABLE_SIZE];  // opaque pointers to kernel file descriptor objects (ker::vfs::File)

    // Exit status of this process (set when process exits, used by waitpid)
    int exitStatus;
    bool hasExited;

    // List of task IDs waiting for this task to exit
    // When this task exits, all tasks in this list will be rescheduled on their respective CPUs
    static constexpr unsigned MAX_AWAITEE_COUNT = 512;
    uint64_t awaitee_on_exit[MAX_AWAITEE_COUNT];
    uint64_t awaitee_on_exit_count;

    // Flag indicating that this task should be moved to wait queue after syscall returns
    bool deferredTaskSwitch;

    // Waitpid state: when this task is waiting for another task to exit
    uint64_t waitingForPid;       // PID we're waiting for (for waitpid return value)
    uint64_t waitStatusPhysAddr;  // Physical address of status variable (for waitpid)

    void loadContext(cpu::GPRegs* gpr);
    void saveContext(cpu::GPRegs* gpr);
} __attribute__((packed));

auto getNextPid() -> uint64_t;
}  // namespace ker::mod::sched::task
