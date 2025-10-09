#include "task.hpp"

#include <platform/loader/debug_info.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>

namespace ker::mod::sched::task {
Task::Task(const char *name, uint64_t elfStart, uint64_t kernelRsp, TaskType type) {
    this->name = name;
    if (type == TaskType::IDLE) {
        return;
    }
    // this->entry = entry;
    // this->regs.ip = entry;
    this->pagemap = mm::virt::createPagemap();
    if (!this->pagemap) {
        dbg::log("Failed to create pagemap for task %s", name);
        hcf();
    }
    this->context.frame.rsp =
        (uint64_t)mm::addr::getPhysPointer((mm::addr::vaddr_t)mm::phys::pageAlloc(mm::paging::PAGE_SIZE)) + mm::paging::PAGE_SIZE;
    this->context.regs = cpu::GPRegs();
    this->type = type;
    this->cpu = cpu::currentCpu();
    this->context.syscallKernelStack = kernelRsp;
    this->context.syscallUserStack = (uint64_t)(new uint8_t[USER_STACK_SIZE]) + USER_STACK_SIZE;
    this->pid = sched::task::getNextPid();

    // FIXED: Parse ELF first to get actual TLS size, then create thread
    ker::loader::elf::TlsModule actualTlsInfo = loader::elf::extractTlsInfo((void *)elfStart);
    this->thread = threading::createThread(USER_STACK_SIZE, actualTlsInfo.tlsSize, this->pagemap, actualTlsInfo);

    mm::virt::copyKernelMappings(this);
    uint64_t stackVirtAddr = (uint64_t)this->context.frame.rsp - mm::paging::PAGE_SIZE;
    uint64_t stackPhysAddr = (uint64_t)mm::addr::getPhysPointer(stackVirtAddr);
    mm::virt::mapPage(this->pagemap, stackVirtAddr, stackPhysAddr, mm::paging::pageTypes::USER);
    if (elfStart == 0) {
        dbg::log("No elf start\n halting");
        hcf();
    }
    uint64_t elfEntry = loader::elf::loadElf((loader::elf::ElfFile *)elfStart, this->pagemap, this->pid, this->name);
    if (elfEntry == 0) {
        dbg::log("Failed to load ELF for task %s", name);
        hcf();
    }
    this->entry = elfEntry;
    this->context.frame.rip = elfEntry;

    // Initialize important TLS symbols (e.g. SafeStack pointer) now that ELF is loaded and relocations processed
    ker::loader::debug::DebugSymbol *ssym = ker::loader::debug::getProcessSymbol(this->pid, "__safestack_unsafe_stack_ptr");
    if (ssym && ssym->isTlsOffset) {
        uint64_t destVaddr = this->thread->tlsBaseVirt + ssym->rawValue;  // rawValue stores st_value (TLS offset)
        uint64_t destPaddr = mm::virt::translate(this->pagemap, destVaddr);
        if (destPaddr != 0) {
            uint64_t *destPtr = (uint64_t *)mm::addr::getPhysPointer(destPaddr);
            *destPtr = this->thread->safestackPtrValue;
            dbg::log("Wrote SafeStack ptr for PID %x at vaddr=%x (phys=%x) value=%x", this->pid, destVaddr, destPaddr,
                     this->thread->safestackPtrValue);
        } else {
            dbg::log("Failed to translate SafeStack TLS vaddr %x for PID %x", destVaddr, this->pid);
        }
    }

    // Initialize other special non-TLS symbols if present
    const char *specials[] = {"__ehdr_start",     "__init_array_start",    "__init_array_end",    "__fini_array_start",
                              "__fini_array_end", "__preinit_array_start", "__preinit_array_end", "__dso_handle"};
    ker::loader::debug::ProcessDebugInfo *pinfo = ker::loader::debug::getProcessDebugInfo(this->pid);
    if (pinfo) {
        for (auto &name : specials) {
            ker::loader::debug::DebugSymbol *sym = ker::loader::debug::getProcessSymbol(this->pid, name);
            if (!sym) continue;
            if (sym->isTlsOffset) continue;  // TLS handled above

            uint64_t storeValue = pinfo->baseAddress;
            if (!std::strncmp(name, "__ehdr_start", 11)) {
                storeValue = pinfo->elfHeaderAddr;
            }

            uint64_t destVaddr = sym->vaddr;
            uint64_t destPaddr = mm::virt::translate(this->pagemap, destVaddr);
            if (destPaddr != 0) {
                uint64_t *destPtr = (uint64_t *)mm::addr::getPhysPointer(destPaddr);
                *destPtr = storeValue;
                dbg::log("Wrote special symbol %s for PID %x at vaddr=%x value=%x", name, this->pid, destVaddr, storeValue);
            }
        }
    }
}

Task::Task(const Task &task) {
    this->name = task.name;
    this->entry = task.entry;
    this->context = task.context;
    this->pagemap = task.pagemap;
    this->type = task.type;
    this->cpu = task.cpu;
    this->thread = task.thread;
}

void Task::loadContext(cpu::GPRegs *gpr) { this->context.regs = *gpr; }

void Task::saveContext(cpu::GPRegs *gpr) {
    cpuSetMSR(IA32_GS_BASE, (uint64_t)this->context.syscallUserStack - USER_STACK_SIZE);
    cpuSetMSR(IA32_KERNEL_GS_BASE, (uint64_t)this->context.syscallKernelStack - KERNEL_STACK_SIZE);
    *gpr = context.regs;
}

uint64_t getNextPid() {
    static uint64_t nextPid = 1;  // Start from 1 to avoid confusion with kernel tasks
    return nextPid++;
}

}  // namespace ker::mod::sched::task
