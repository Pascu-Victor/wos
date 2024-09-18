#pragma once

#include <platform/asm/cpu.hpp>
#include <platform/sched/task.hpp>
namespace ker::mod::sys::context_switch {
void schedCallback(void* handlerFunc);

void switchTo(sched::task::Task* from, sched::task::Task* to);
}  // namespace ker::mod::sys::context_switch
