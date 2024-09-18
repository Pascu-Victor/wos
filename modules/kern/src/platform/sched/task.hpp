#pragma once

#include <defines/defines.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/mm/paging.hpp>
#include <platform/sys/context_switch.hpp>
#include <std/rbtree.hpp>

namespace ker::mod::sched {
namespace task {

enum TaskType {
    DAEMON,
    PROCESS,
};

struct TaskRegisters {
    cpu::GPRegs regs;
    uint64_t ip;
    uint64_t rsp;
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
    const char* name;
    mm::paging::PageTable* pagemap;
    TaskType type;
    uint64_t cpu;
    TaskRegisters regs;
    Thread* thread;
} __attribute__((packed));
}  // namespace task
}  // namespace ker::mod::sched
