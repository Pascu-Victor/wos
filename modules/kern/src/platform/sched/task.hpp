#pragma once

#include <defines/defines.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/mm/paging.hpp>
// #include <platform/sys/context_switch.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/sched/threading.hpp>
// Avoid pulling in in-tree std headers into kernel translation units from here.

namespace ker::mod::sched::task {

enum TaskType {
    DAEMON,
    PROCESS,
    IDLE,
};

struct Context {
    uint64_t syscallKernelStack;
    uint64_t syscallUserStack;

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

    void loadContext(cpu::GPRegs* gpr);
    void saveContext(cpu::GPRegs* gpr);
} __attribute__((packed));

auto getNextPid() -> uint64_t;
}  // namespace ker::mod::sched::task
