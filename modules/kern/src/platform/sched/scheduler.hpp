#pragma once

#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sched/task.hpp>
#include <platform/smt/smt.hpp>
#include <platform/sys/context_switch.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/list.hpp>

namespace ker::mod::sched {
struct RunQueue {
    std::list<task::Task*> activeTasks;   // TODO: replace with fast priority queue
    std::list<task::Task*> expiredTasks;  // TODO: replace with fast priority queue
    task::Task* currentTask;
    [[nodiscard]]
    RunQueue()
        : activeTasks(), expiredTasks(), currentTask(nullptr) {}
};

struct SchedEntry {
    uint32_t weight;
    uint32_t inverseWeight;
};

void init();
auto postTask(task::Task* task) -> bool;
auto postTaskForCpu(uint64_t cpuNo, task::Task* task) -> bool;
auto getCurrentTask() -> task::Task*;
void removeCurrentTask();  // Remove current task from runqueue (for exit)
void startScheduler();
void percpuInit();
void processTasks(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame);
void jumpToNextTask(ker::mod::cpu::GPRegs& gpr, ker::mod::gates::interruptFrame& frame);
}  // namespace ker::mod::sched
