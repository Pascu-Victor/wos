#include "scheduler.hpp"

#include <platform/asm/segment.hpp>
#include <platform/sys/userspace.hpp>
// Debug helpers
#include <platform/loader/debug_info.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>

namespace ker::mod::sched {
// One run queue per cpu
static smt::PerCpuCrossAccess<RunQueue> *runQueues;

// TODO: may be unique_ptr
bool postTask(task::Task *task) {
    runQueues->thisCpu()->activeTasks.push_back(task);
    return true;
}

bool postTaskForCpu(uint64_t cpuNo, task::Task *task) {
    runQueues->thatCpu(cpuNo)->activeTasks.push_back(task);
    return true;
}

task::Task *getCurrentTask() {
    task::Task *task = runQueues->thisCpu()->activeTasks.front();
    return task;
}
void processTasks(ker::mod::cpu::GPRegs &gpr, ker::mod::gates::interruptFrame &frame) {
    apic::eoi();
    auto currentTask = getCurrentTask();
    currentTask->context.regs = gpr;
    currentTask->context.frame = frame;
    runQueues->thisCpu()->activeTasks.push_back(currentTask);
    task::Task *nextTask = runQueues->thisCpu()->activeTasks.front();
    runQueues->thisCpu()->activeTasks.pop_front();
    sys::context_switch::switchTo(gpr, frame, nextTask);
}

void percpuInit() {
    auto cpu = cpu::currentCpu();
    dbg::log("Initializing scheduler, CPU:%x", cpu);
    runQueues->thisCpu()->activeTasks = std::list<task::Task *>();
    runQueues->thisCpu()->expiredTasks = std::list<task::Task *>();
}

void startScheduler() {
    dbg::log("Starting scheduler, CPU:%x", cpu::currentCpu());
    auto firstTask = runQueues->thisCpu()->activeTasks.front();
    cpuSetMSR(IA32_KERNEL_GS_BASE, (uint64_t)firstTask->context.syscallKernelStack - KERNEL_STACK_SIZE);
    cpuSetMSR(IA32_GS_BASE, (uint64_t)firstTask->context.syscallUserStack - USER_STACK_SIZE);
    cpuSetMSR(IA32_FS_BASE, (uint64_t)firstTask->thread->fsbase);

    // Debug: verify fs register value before usermode
    uint64_t currentFsBase = cpu::rdfsbase();
    mod::dbg::log("Before usermode: fsbase MSR set to 0x%x, current rdfsbase = 0x%x", (uint64_t)firstTask->thread->fsbase, currentFsBase);

    mm::virt::switchPagemap(firstTask);

    // Sanity: log final user CR3 mappings for GOT sections before entering usermode
    auto pinfo = ker::loader::debug::getProcessDebugInfo(firstTask->pid);
    dbg::log("Before usermode: task '%s' pid=%x pagemap=%x (phys=%x)", firstTask->name ? firstTask->name : "<noname>", firstTask->pid,
             firstTask->pagemap, (uint64_t)ker::mod::mm::addr::getPhysPointer((ker::mod::mm::addr::vaddr_t)firstTask->pagemap));
    if (pinfo) {
        const char *targets[] = {".got.plt", ".got"};
        for (const char *tname : targets) {
            for (auto &sec : pinfo->sections) {
                if (!sec.name) continue;
                if (std::strncmp(sec.name, tname, std::strlen(tname)) != 0) continue;
                // Dump first few qwords from GOT to verify contents and physical backing
                uint64_t entries = sec.size / sizeof(uint64_t);
                if (entries > 8) entries = 8;  // limit spam
                dbg::log("GOT sanity: section %s vaddr=%x size=%x (dumping %d qwords)", sec.name, sec.vaddr, sec.size, entries);
                for (uint64_t i = 0; i < entries; ++i) {
                    uint64_t vaddr = sec.vaddr + i * sizeof(uint64_t);
                    uint64_t paddr = ker::mod::mm::virt::translate(firstTask->pagemap, vaddr);
                    if (!paddr) {
                        dbg::log("  [%d] vaddr=%x -> paddr=0 (unmapped)", (int)i, vaddr);
                        continue;
                    }
                    // Convert physical address to a kernel-mapped virtual pointer via HHDM
                    uint64_t *vptr = (uint64_t *)ker::mod::mm::addr::getVirtPointer(paddr);
                    uint64_t val = *vptr;
                    dbg::log("  [%d] vaddr=%x paddr=%x -> %x", (int)i, vaddr, paddr, val);
                }
                // No de-lazifier: GOT/PLT entries are eagerly bound during load; RELRO is enforced afterwards.
                // Only dump the first matching section
                goto _got_dump_done;
            }
        }
    _got_dump_done:;
    } else {
        dbg::log("Before usermode: no debug info found for pid=%x to locate GOT", firstTask->pid);
    }
    // Write TLS self-pointer after switching pagemaps so it goes to the correct user-mapped physical memory
    *((uint64_t *)firstTask->thread->fsbase) = firstTask->thread->fsbase;
    sys::context_switch::startSchedTimer();
    for (;;) _wOS_asm_enterUsermode(firstTask->entry, firstTask->thread->stack);
}

void init() {
    ker::mod::smt::init();
    runQueues = new smt::PerCpuCrossAccess<RunQueue>();
}

}  // namespace ker::mod::sched
