#pragma once

#include <platform/asm/cpu.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/spinlock.hpp>
#include <std/list.hpp>

namespace ker::mod::sched {
struct RunQueue {
    std::list<task::Task*> activeTasks;
    std::list<task::Task*> expiredTasks;
    sys::Spinlock lock;
};

struct SchedEntry {
    uint32_t weight;
    uint32_t inverseWeight;

}

    void init();
bool postTask(task::Task* task);
task::Task getCurrentTask();
void processTasks();
}  // namespace ker::mod::sched
