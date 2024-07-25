#pragma once

#include <defines/defines.hpp>
#include <mod/mm/paging.hpp>
#include <mod/asm/cpu.hpp>
#include <mod/sys/context_switch.hpp>

namespace ker::mod::sched::task
{

    enum TaskType {
        DAEMON,
        PROCESS,
    };

    struct Task {
        const char* name;
        mm::paging::PageTable* pagemap;
        uint64_t entry;
        TaskType type;
        uint64_t stack;
        sys::context_switch::TaskRegisters regs;

        Task(const char* name, mm::paging::PageTable* pagemap, uint64_t entry, TaskType type, uint64_t stack, sys::context_switch::TaskRegisters regs);

        Task(const char* name, uint64_t entry);

    } __attribute__((packed));
}