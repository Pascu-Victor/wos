#pragma once

#include <defines/defines.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/mm/paging.hpp>
// #include <platform/sys/context_switch.hpp>
#include <platform/interrupt/gates.hpp>
#include <std/rbtree.hpp>

namespace ker::mod::sched::task {

enum TaskType {
    DAEMON,
    PROCESS,
};

struct Context {
    uint64_t syscallKernelStack;
    uint64_t syscallUserStack;

    cpu::GPRegs regs;

    uint64_t intNo;
    uint64_t errorCode;

    gates::interruptFrame frame;
} __attribute__((packed));

struct Thread {
    desc::gdt::GdtEntry tls[3];
    uint64_t fsindex;
    uint64_t gsindex;

    uint64_t fsbase;
    uint64_t gsbase;

    uint64_t stack;
    uint64_t stackSize;
} __attribute__((packed));
struct Task {
    Task(const char* name, uint64_t elfStart, uint64_t kernelRsp, TaskType type);

    Task(const Task& task);

    mm::paging::PageTable* pagemap;
    Context context;
    uint64_t entry;

    const char* name;
    TaskType type;
    uint64_t cpu;
    Thread* thread;

    void loadContext(cpu::GPRegs* gpr);
    void saveContext(cpu::GPRegs* gpr);
} __attribute__((packed));
}  // namespace ker::mod::sched::task
