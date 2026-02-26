#include "task.hpp"

#include <cstdint>
#include <cstring>
#include <platform/loader/debug_info.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>

#include "platform/asm/msr.hpp"

namespace ker::mod::sched::task {
Task::Task(const char* name, uint64_t elfStart, uint64_t kernelRsp, TaskType type) {
    // CRITICAL: Copy the name string to kernel heap memory!
    // The passed 'name' might point to Limine boot memory or user memory
    // which won't be mapped when we switch pagemaps.
    if (name != nullptr) {
        size_t nameLen = strlen(name);
        char* nameCopy = new char[nameLen + 1];
        memcpy(nameCopy, name, nameLen + 1);
        this->name = nameCopy;
    } else {
        this->name = nullptr;
    }
    this->parentPid = 0;        // Initialize to 0 (no parent by default, will be set by exec or fork)
    this->elfBuffer = nullptr;  // No ELF buffer by default
    this->elfBufferSize = 0;
    this->hasRun = false;              // Task hasn't run yet, context.frame contains initial setup
    this->exitStatus = 0;              // Initialize exit status
    this->hasExited = false;           // Task hasn't exited yet
    this->awaitee_on_exit_count = 0;   // Initialize awaitee counter
    this->deferredTaskSwitch = false;  // No deferred switch by default
    this->yieldSwitch = false;
    this->kthreadEntry = nullptr;

    // EEVDF scheduling fields
    this->vruntime = 0;
    this->vdeadline = 0;
    this->schedWeight = 1024;    // nice-0 baseline
    this->sliceNs = 10'000'000;  // 10ms
    this->sliceUsedNs = 0;
    this->heapIndex = -1;
    this->schedQueue = SchedQueue::NONE;
    this->schedNext = nullptr;

    // Signal infrastructure
    this->sigPending = 0;
    this->sigMask = 0;
    this->inSignalHandler = false;
    this->doSigreturn = false;
    for (auto& sig_handler : this->sigHandlers) {
        sig_handler = {.handler = 0, .flags = 0, .restorer = 0, .mask = 0};  // SIG_DFL for all
    }

    // Initialize file descriptor table to null
    for (auto& fd : this->fds) {
        fd = nullptr;
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
        auto* scratch_area = (cpu::PerCpu*)this->context.syscallScratchArea;
        scratch_area->syscallStack = kernelRsp;
        scratch_area->cpuId = cpu::currentCpu();

        // Idle tasks get PID 0 (kernel/swapper convention) - they don't consume real PIDs
        // This ensures the first user process (init) always gets PID 1 regardless of core count
        this->pid = 0;
        this->entry = 0;
        this->kthreadEntry = nullptr;
        this->thread = nullptr;
        return;
    }

    if (type == TaskType::DAEMON) {
        // Kernel thread: ring 0, kernel pagemap, no user thread/TLS, no ELF
        this->pagemap = mm::virt::getKernelPagemap();
        this->type = type;
        this->cpu = cpu::currentCpu();
        this->context.syscallKernelStack = kernelRsp;
        this->thread = nullptr;

        auto* perCpu = new cpu::PerCpu();
        perCpu->syscallStack = kernelRsp;
        perCpu->cpuId = cpu::currentCpu();
        this->context.syscallScratchArea = (uint64_t)perCpu;

        this->pid = sched::task::getNextPid();
        this->entry = 0;

        // Ring 0 interrupt frame for kernel-mode execution
        this->context.frame.rip = 0;        // Set by createKernelThread
        this->context.frame.cs = 0x08;      // GDT_KERN_CS
        this->context.frame.ss = 0x10;      // GDT_KERN_DS
        this->context.frame.flags = 0x202;  // IF=1, reserved bit 1
        this->context.frame.rsp = kernelRsp;
        this->context.frame.intNum = 0;
        this->context.frame.errCode = 0;
        this->context.regs = cpu::GPRegs();
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
    // POSIX: default process group = own pid (processes start in their own group)
    if (this->pgid == 0) {
        this->pgid = this->pid;
    }

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
    if (this->thread == nullptr) {
        dbg::log("Failed to create thread for task %s - OOM", name);
        // Can't continue without a thread - this is a fatal error for the task
        // Mark task as invalid so it won't be scheduled
        this->type = TaskType::IDLE;  // Abuse IDLE type to prevent scheduling
        this->pagemap = nullptr;
        return;
    }

    // Allocate a KERNEL-space PerCpu structure for syscall scratch area
    // This must be in kernel memory, not user memory! The user's gsbase/TLS is separate.
    // After swapgs in syscall entry: GS_BASE will point to this kernel scratch area.
    auto* perCpu = new cpu::PerCpu();
    perCpu->syscallStack = kernelRsp;
    perCpu->cpuId = cpu::currentCpu();
    this->context.syscallScratchArea = (uint64_t)perCpu;

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
            auto* destPtr = (uint64_t*)mm::addr::getVirtPointer(destPaddr);
            *destPtr = this->thread->safestackPtrValue;
            dbg::log("Wrote SafeStack ptr for PID %x at vaddr=%x (phys=%x) value=%x", this->pid, destVaddr, destPaddr,
                     this->thread->safestackPtrValue);
        } else {
            dbg::log("Failed to translate SafeStack TLS vaddr %x for PID %x", destVaddr, this->pid);
        }
    }
}

Task* Task::createKernelThread(const char* name, void (*entryFunc)()) {
    auto stackBase = (uint64_t)mm::phys::pageAlloc(KERNEL_STACK_SIZE);
    if (stackBase == 0) {
        dbg::log("createKernelThread: OOM allocating kernel stack for '%s'", name);
        return nullptr;
    }
    uint64_t kernelRsp = stackBase + KERNEL_STACK_SIZE;

    auto* task = new Task(name, 0, kernelRsp, TaskType::DAEMON);
    task->kthreadEntry = entryFunc;
    task->context.frame.rip = (uint64_t)entryFunc;
    return task;
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
