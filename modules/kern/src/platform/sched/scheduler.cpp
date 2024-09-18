#include "scheduler.hpp"

namespace ker::mod::sched {
// One run queue per cpu
RunQueue *runQueues = nullptr;

void init() { runQueues = new RunQueue[smt::getCoreCount()]; }

bool postTask(task::Task *task) {
    uint64_t cpu = smt::getCpu(task->cpu)->lapic_id;
    runQueues[cpu].lock.lock();
    runQueues[cpu].activeTasks.push_back(task);
    runQueues[cpu].lock.unlock();
    return true;
}

task::Task getCurrentTask() {
    uint64_t cpu = cpu::currentCpu();
    runQueues[cpu].lock.lock();
    task::Task *task = runQueues[cpu].activeTasks.front();
    runQueues[cpu].lock.unlock();
    return *task;
}

void processTasks() {
    uint64_t cpu = cpu::currentCpu();
    runQueues[cpu].lock.lock();
    task::Task task = *runQueues[cpu].activeTasks.pop_front();
    runQueues[cpu].lock.unlock();
    sys::context_switch::switchTo(task);
}
}  // namespace ker::mod::sched
