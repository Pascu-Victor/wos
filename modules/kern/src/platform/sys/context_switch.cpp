#include "context_switch.hpp"

namespace ker::mod::sys::context_switch {
extern "C" void _wOS_asm_switchTo(sched::task::TaskRegisters regs);

static inline void save_fsgs(sched::task::Task* task) {
    savesegment(fs, task->thread->fsindex);
    savesegment(gs, task->thread->gsindex);
}

extern "C" sched::task::Task* _wOS_switchTo(sched::task::Task* prev_task, sched::task::Task* next_task) {
    sched::task::TaskRegisters* prev_regs = &prev_task->regs;
    sched::task::TaskRegisters* next_regs = &next_task->regs;
}
}  // namespace ker::mod::sys::context_switch
