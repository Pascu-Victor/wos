#include "context_switch.hpp"

#include <cstdint>
#include <platform/asm/msr.hpp>
#include <platform/sched/scheduler.hpp>

#include "platform/acpi/apic/apic.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/task.hpp"

namespace ker::mod::sys::context_switch {

// Debug helper: update per-CPU task pointer for panic inspection
static constexpr uint64_t DEBUG_TASK_PTR_BASE = 0xffff800000500000ULL;
static inline void updateDebugTaskPtr(sched::task::Task* task) {
    auto** debug_ptrs = reinterpret_cast<sched::task::Task**>(DEBUG_TASK_PTR_BASE);
    debug_ptrs[cpu::currentCpu()] = task;
}

void switchTo(cpu::GPRegs& gpr, gates::interruptFrame& frame, sched::task::Task* nextTask) {
    frame.rip = nextTask->context.frame.rip;
    frame.rsp = nextTask->context.frame.rsp;
    frame.cs = nextTask->context.frame.cs;
    frame.ss = nextTask->context.frame.ss;
    frame.flags = nextTask->context.frame.flags;

    gpr = nextTask->context.regs;

    // User GS_BASE = TLS/stack base, KERNEL_GS_BASE = scratch area for kernel after swapgs
    if (nextTask->thread) {
        cpu::wrgsbase(nextTask->thread->gsbase);                               // User's TLS/stack base
        cpuSetMSR(IA32_KERNEL_GS_BASE, nextTask->context.syscallScratchArea);  // Scratch for kernel
        cpu::wrfsbase(nextTask->thread->fsbase);
    } else {
        // Idle task uses kernel-allocated scratch area for both
        cpu::wrgsbase(nextTask->context.syscallScratchArea);
        cpuSetMSR(IA32_KERNEL_GS_BASE, nextTask->context.syscallScratchArea);
    }

    mm::virt::switchPagemap(nextTask);
}

static long timerQuantum;

extern "C" void _wOS_schedTimer(void* stack_ptr) {
    // The stack layout at this point (from low to high address):
    // [GPRegs] <- stack_ptr points here (15 registers)
    // [intNum]
    // [errCode]
    // [RIP]
    // [CS]
    // [RFLAGS]
    // [RSP]
    // [SS]

    apic::eoi();
    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = reinterpret_cast<gates::interruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs));

    // This may not return if we switch contexts
    // Note: processTasks will arm the timer when appropriate
    sched::processTasks(*gpr_ptr, *frame_ptr);

    // Re-arm timer for next quantum (only reached if processTasks returned,
    // meaning we're continuing with idle task or same task)
    apic::oneShotTimer(timerQuantum);
}

extern "C" void _wOS_jumpToNextTaskNoSave(void* stack_ptr) {
    auto* gpr_ptr = reinterpret_cast<cpu::GPRegs*>(stack_ptr);
    auto* frame_ptr = reinterpret_cast<gates::interruptFrame*>(reinterpret_cast<uint8_t*>(stack_ptr) + sizeof(cpu::GPRegs));

    sched::jumpToNextTask(*gpr_ptr, *frame_ptr);
}

void startSchedTimer() {
    timerQuantum = apic::calibrateTimer(10000);  // 10ms
    apic::oneShotTimer(timerQuantum);
}
}  // namespace ker::mod::sys::context_switch
