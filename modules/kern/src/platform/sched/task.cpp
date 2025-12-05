#include "task.hpp"

#include <cstdint>
#include <platform/loader/debug_info.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/virt.hpp>

#include "platform/asm/msr.hpp"

namespace ker::mod::sched::task {
Task::Task(const char* name, uint64_t elfStart, uint64_t kernelRsp, TaskType type) {
    this->name = name;
    this->parentPid = 0;        // Initialize to 0 (no parent by default, will be set by exec or fork)
    this->elfBuffer = nullptr;  // No ELF buffer by default
    this->elfBufferSize = 0;
    this->hasRun = false;              // Task hasn't run yet, context.frame contains initial setup
    this->exitStatus = 0;              // Initialize exit status
    this->hasExited = false;           // Task hasn't exited yet
    this->awaitee_on_exit_count = 0;   // Initialize awaitee counter
    this->deferredTaskSwitch = false;  // No deferred switch by default

    // Initialize file descriptor table to null
    for (unsigned i = 0; i < FD_TABLE_SIZE; ++i) {
        this->fds[i] = nullptr;
    }

    if (type == TaskType::IDLE) {
        // Idle tasks use the kernel pagemap
        this->pagemap = mm::virt::getKernelPagemap();
        this->type = type;
        this->cpu = cpu::currentCpu();
        this->context.syscallKernelStack = kernelRsp;

        // Initialize syscall scratch area even for idle tasks
        // This is needed because switchTo() sets GS_BASE from this field
        this->context.syscallScratchArea = (uint64_t)(new cpu::PerCpu());
        cpu::PerCpu* scratchArea = (cpu::PerCpu*)this->context.syscallScratchArea;
        scratchArea->syscallStack = kernelRsp;
        scratchArea->cpuId = cpu::currentCpu();

        this->pid = sched::task::getNextPid();
        this->entry = 0;
        this->thread = nullptr;
        return;
    }
    // this->entry = entry;
    // this->regs.ip = entry;
    this->pagemap = mm::virt::createPagemap();
    if (!this->pagemap) {
        dbg::log("Failed to create pagemap for task %s", name);
        hcf();
    }
    this->context.frame.rsp = 0;
    this->context.regs = cpu::GPRegs();
    this->type = type;
    this->cpu = cpu::currentCpu();
    this->context.syscallKernelStack = kernelRsp;
    
    this->pid = sched::task::getNextPid();

    // CRITICAL: Copy kernel mappings FIRST so we can access kernel memory (like elfBuffer)
    // The elfStart pointer points to kernel heap memory allocated by the parent process
    mm::virt::copyKernelMappings(this);

    // Validate ELF pointer before any operations
    if (elfStart == 0) {
        dbg::log("ERROR: Task created with null ELF pointer");
        hcf();
    }

    // Add compiler memory barrier to ensure elfStart is fully visible
    __asm__ volatile("mfence" ::: "memory");

    // Validate ELF magic bytes before proceeding
    auto* elfHeader = (uint8_t*)elfStart;

    if (elfHeader[0] != 0x7F || elfHeader[1] != 'E' || elfHeader[2] != 'L' || elfHeader[3] != 'F') {
        dbg::log("ERROR: Invalid ELF magic at 0x%p: [0x%x 0x%x 0x%x 0x%x]", (void*)elfStart, elfHeader[0], elfHeader[1], elfHeader[2],
                 elfHeader[3]);
        dbg::log("Expected ELF magic: [0x7F 'E' 'L' 'F'] = [0x7F 0x45 0x4C 0x46]");
        hcf();
    }

    // FIXED: Parse ELF first to get actual TLS size, then create thread
    ker::loader::elf::TlsModule actualTlsInfo = loader::elf::extractTlsInfo((void*)elfStart);
    this->thread = threading::createThread(USER_STACK_SIZE, actualTlsInfo.tlsSize, this->pagemap, actualTlsInfo);

    // Use the bottom of user stack as syscall scratch area (GS_BASE will point here)
    this->context.syscallScratchArea = this->thread->gsbase;
    
    // Initialize the scratch area with kernel stack pointer
    // Note: We need to translate to physical address to write to it before pagemap switch
    uint64_t scratchPhys = mm::virt::translate(this->pagemap, this->context.syscallScratchArea);
    if (scratchPhys != 0) {
        cpu::PerCpu* scratchArea = (cpu::PerCpu*)mm::addr::getPhysPointer(scratchPhys);
        scratchArea->syscallStack = kernelRsp;
        scratchArea->cpuId = cpu::currentCpu();
    }

    this->context.frame.rsp = this->thread->stack;

    loader::elf::ElfLoadResult elfResult = loader::elf::loadElf((loader::elf::ElfFile*)elfStart, this->pagemap, this->pid, this->name);
    if (elfResult.entryPoint == 0) {
        dbg::log("Failed to load ELF for task %s", name);
        hcf();
    }
    this->entry = elfResult.entryPoint;
    this->context.frame.rip = elfResult.entryPoint;
    this->programHeaderAddr = elfResult.programHeaderAddr;
    this->elfHeaderAddr = elfResult.elfHeaderAddr;

    // Initialize interrupt frame fields for usermode
    this->context.frame.intNum = 0;     // Not from a real interrupt
    this->context.frame.errCode = 0;    // No error code
    this->context.frame.ss = 0x1b;      // User stack segment (GDT entry 3, RPL=3) NOLINT
    this->context.frame.cs = 0x23;      // User code segment (GDT entry 4, RPL=3) NOLINT
    this->context.frame.flags = 0x202;  // IF (interrupts enabled) + reserved bit 1 NOLINT

    // Initialize important TLS symbols (e.g. SafeStack pointer) now that ELF is loaded and relocations processed
    ker::loader::debug::DebugSymbol* ssym = ker::loader::debug::getProcessSymbol(this->pid, "__safestack_unsafe_stack_ptr");
    if (ssym && ssym->isTlsOffset) {
        uint64_t destVaddr = this->thread->tlsBaseVirt + ssym->rawValue;  // rawValue stores st_value (TLS offset)
        uint64_t destPaddr = mm::virt::translate(this->pagemap, destVaddr);
        if (destPaddr != 0) {
            auto* destPtr = (uint64_t*)mm::addr::getPhysPointer(destPaddr);
            *destPtr = this->thread->safestackPtrValue;
            dbg::log("Wrote SafeStack ptr for PID %x at vaddr=%x (phys=%x) value=%x", this->pid, destVaddr, destPaddr,
                     this->thread->safestackPtrValue);
        } else {
            dbg::log("Failed to translate SafeStack TLS vaddr %x for PID %x", destVaddr, this->pid);
        }
    }
}

Task::Task(const Task& task) {
    this->name = task.name;
    this->entry = task.entry;
    this->context = task.context;
    this->pagemap = task.pagemap;
    this->type = task.type;
    this->cpu = task.cpu;
    this->thread = task.thread;
}

void Task::loadContext(cpu::GPRegs* gpr) { this->context.regs = *gpr; }

void Task::saveContext(cpu::GPRegs* gpr) {
    cpuSetMSR(IA32_KERNEL_GS_BASE, this->context.syscallScratchArea);
    *gpr = context.regs;
}

uint64_t getNextPid() {
    static uint64_t nextPid = 1;  // Start from 1 to avoid confusion with kernel tasks
    return nextPid++;
}

}  // namespace ker::mod::sched::task
