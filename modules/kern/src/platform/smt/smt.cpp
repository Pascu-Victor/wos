#include "smt.hpp"

#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "mod/io/serial/serial.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/boot/handover.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/mm.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sys/syscall.hpp"
#include "vfs/fs/devfs.hpp"

__attribute__((used, section(".requests"))) const static volatile limine_smp_request smp_request = {
    .id = LIMINE_SMP_REQUEST,
    .revision = 0,
    .response = nullptr,
    .flags = 1,
};
namespace ker::mod::smt {
namespace {
static PerCpuCrossAccess<CpuInfo>* cpuData;
uint32_t flags;
uint32_t bsp_lapic_id;
uint64_t cpu_count;

// Array to store kernel PerCpu structure addresses for each CPU
// These are the PerCpu structures allocated during boot that have correct cpuId
// Used to restore GS_BASE when entering idle loop (no task context)
static cpu::PerCpu** kernelPerCpuPtrs = nullptr;

void cpuParamInit(uint64_t cpuNo, uint64_t stackTop) {
    // Enable CPU features FIRST (must be done on each CPU)
    // FSGSBASE must be enabled before using wrgsbase instruction
    cpu::enableFSGSBASE();
    cpu::enableSSE();

    // Set up per-CPU data
    // Allocate a dedicated PerCpu structure for this CPU (don't reuse stack bottom
    // as that can corrupt adjacent memory or heap metadata)
    auto* perCpuData = new cpu::PerCpu();
    uint64_t perCpuAddr = (uint64_t)perCpuData;

    // Zero out the PerCpu area
    memset((void*)perCpuAddr, 0, sizeof(cpu::PerCpu));

    // Store kernel stack in the PerCpu structure
    perCpuData->syscallStack = stackTop;

    cpu::wrgsbase(perCpuAddr);
    cpuSetMSR(IA32_KERNEL_GS_BASE, perCpuAddr);

    // Write cpuId directly to the memory location to verify
    perCpuData->cpuId = cpuNo;

    // Store the per-CPU PerCpu pointer for later retrieval (e.g., when entering idle loop)
    if (kernelPerCpuPtrs != nullptr) {
        kernelPerCpuPtrs[cpuNo] = perCpuData;
    }

    // Also use setCurrentCpuid for consistency
    cpu::setCurrentCpuid(cpuNo);

    // Verify the write worked
    uint64_t readBack = cpu::currentCpu();
    if (readBack != cpuNo) {
        // Use serial directly since dbg might not be ready
        dbg::log("CPU INIT ERROR: wrote cpuId=%d but read back %d, perCpuAddr=%p\n", cpuNo, readBack, perCpuAddr);
    }

    // Initialize GDT for this CPU (includes per-CPU TSS)
    // NOTE: GDT/IDT asm routines no longer load GS selector to preserve GS.base
    desc::gdt::initDescriptors((uint64_t*)stackTop, cpuNo);

    // Initialize IDT for this CPU (loads the shared IDT)
    desc::idt::idtInit();

    // Initialize syscall MSRs for this CPU
    sys::init();

    // Initialize APIC for this CPU
    apic::initApicMP();

    // Initialize scheduler for this CPU
    sched::percpuInit();

    // Create idle task for this CPU
    // Pass the kernel stack TOP (stack grows downward, so syscall needs to start from top)
    auto idleTask = new sched::task::Task("idle", 0, stackTop, sched::task::TaskType::IDLE);
    sched::postTask(idleTask);

    dbg::log("CPU %d initialized and ready", cpuNo);

    // Start the scheduler on this CPU
    sched::startScheduler();
}

void nonPrimaryCpuInit(limine_smp_info* smpInfo) {
    // FIRST: Switch to kernel page table before accessing any kernel data
    mm::virt::switchToKernelPagemap();

    uint64_t cpuNo = 0;

    // Find our CPU number from the LAPIC ID
    for (uint64_t i = 0; i < cpu_count; i++) {
        if (smp_request.response->cpus[i]->lapic_id == smpInfo->lapic_id) {
            cpuNo = i;
            break;
        }
    }

    uint64_t stackTop = (uint64_t)cpuData->thatCpu(cpuNo)->stack_pointer_ref;

    // Initialize this CPU fully
    cpuParamInit(cpuNo, stackTop);

    // Should never reach here
    hcf();
}

// Create init task(s) from handover modules WITHOUT starting scheduler
// This is called early to ensure init gets PID 1
void createInitTasks(boot::HandoverModules& modStruct, uint64_t kernelRsp) {
    for (uint64_t i = 0; i < modStruct.count; i++) {
        const auto& module = modStruct.modules[i];
        auto* newTask = new sched::task::Task(module.name, (uint64_t)module.entry, kernelRsp, sched::task::TaskType::PROCESS);

        if (newTask == nullptr || newTask->thread == nullptr || newTask->pagemap == nullptr) {
            dbg::log("FATAL: Failed to create handover task %s - OOM", module.name);
            hcf();
        }

        // Setup stdin/stdout/stderr for init process
        // Open /dev/console as fd 0, 1, 2
        ker::vfs::File* console_stdin = ker::vfs::devfs::devfs_open_path("/dev/console", 0, 0);
        ker::vfs::File* console_stdout = ker::vfs::devfs::devfs_open_path("/dev/console", 0, 0);
        ker::vfs::File* console_stderr = ker::vfs::devfs::devfs_open_path("/dev/console", 0, 0);

        if (console_stdin != nullptr && console_stdout != nullptr && console_stderr != nullptr) {
            console_stdin->fops = ker::vfs::devfs::get_devfs_fops();
            console_stdout->fops = ker::vfs::devfs::get_devfs_fops();
            console_stderr->fops = ker::vfs::devfs::get_devfs_fops();

            // Assign file descriptors 0, 1, 2
            newTask->fds[0] = console_stdin;
            console_stdin->fd = 0;
            dbg::log("Setup fd 0 (stdin): %p", console_stdin);
            newTask->fds[1] = console_stdout;
            console_stdout->fd = 1;
            dbg::log("Setup fd 1 (stdout): %p", console_stdout);
            newTask->fds[2] = console_stderr;
            console_stderr->fd = 2;
            dbg::log("Setup fd 2 (stderr): %p", console_stderr);
            dbg::log("Verifying: fds[0]=%p, fds[1]=%p, fds[2]=%p", newTask->fds[0], newTask->fds[1], newTask->fds[2]);

            dbg::log("Setup stdin/stdout/stderr for task %s", module.name);
        } else {
            dbg::log("WARNING: Failed to open /dev/console for task %s", module.name);
        }

        // Setup minimal argc/argv/envp on user stack
        uint64_t userStackVirt = newTask->thread->stack;
        uint64_t currentVirtOffset = 0;

        // Helper to push data onto stack
        auto pushToStack = [&](const void* data, size_t size) -> uint64_t {
            if (currentVirtOffset + size > USER_STACK_SIZE) {
                return 0;
            }
            currentVirtOffset += size;
            uint64_t virtAddr = userStackVirt - currentVirtOffset;

            uint64_t pageVirt = virtAddr & ~(mm::paging::PAGE_SIZE - 1);
            uint64_t pageOffset = virtAddr & (mm::paging::PAGE_SIZE - 1);

            uint64_t pagePhys = mm::virt::translate(newTask->pagemap, pageVirt);
            if (pagePhys == 0) {
                dbg::log("ERROR: Failed to translate page virt=0x%x for stack data", pageVirt);
                return 0;
            }

            auto* destPtr = reinterpret_cast<uint8_t*>(mm::addr::getVirtPointer(pagePhys)) + pageOffset;
            std::memcpy(destPtr, data, size);
#ifdef TASK_DEBUG
            dbg::log("Pushed %d bytes: virt=0x%x, phys=0x%x, data[0]=0x%x", size, virtAddr, pagePhys, *(uint32_t*)data);
#endif
            return virtAddr;
        };

        // Helper to push string onto stack
        auto pushString = [&](const char* str) -> uint64_t {
            size_t len = std::strlen(str) + 1;
            if (currentVirtOffset + len > USER_STACK_SIZE) {
                return 0;
            }
            currentVirtOffset += len;
            uint64_t virtAddr = userStackVirt - currentVirtOffset;

            uint64_t pageVirt = virtAddr & ~(mm::paging::PAGE_SIZE - 1);
            uint64_t pageOffset = virtAddr & (mm::paging::PAGE_SIZE - 1);

            uint64_t pagePhys = mm::virt::translate(newTask->pagemap, pageVirt);
            if (pagePhys == 0) {
                dbg::log("ERROR: Failed to translate page virt=0x%x for string '%s'", pageVirt, str);
                return 0;
            }

            auto* destPtr = reinterpret_cast<uint8_t*>(mm::addr::getVirtPointer(pagePhys)) + pageOffset;
            std::memcpy(destPtr, str, len);
#ifdef TASK_DEBUG
            dbg::log("Pushed string '%s' at virt=0x%x (len=%d)", str, virtAddr, len);
#endif
            return virtAddr;
        };

        // Push program name as argv[0]
        uint64_t argv0 = pushString(module.name);

        // Align stack to 16 bytes
        // no return address to push, so just align
        constexpr uint64_t alignment = 16;
        uint64_t currentAddr = userStackVirt - currentVirtOffset;
        uint64_t aligned = currentAddr & ~(alignment - 1);
        currentVirtOffset += (currentAddr - aligned);

        constexpr uint64_t AT_NULL = 0;
        constexpr uint64_t AT_PAGESZ = 6;
        constexpr uint64_t AT_ENTRY = 9;
        constexpr uint64_t AT_PHDR = 3;
        constexpr uint64_t AT_EHDR = 33;  // AT_EHDR (glibc extension but widely supported)

        std::array<uint64_t, 10> auxvEntries = {AT_PAGESZ, (uint64_t)mm::paging::PAGE_SIZE,
                                                AT_ENTRY,  newTask->entry,
                                                AT_PHDR,   newTask->programHeaderAddr,
                                                AT_EHDR,   newTask->elfHeaderAddr,
                                                AT_NULL,   0};

        // Push auxv in reverse order
        for (int j = auxvEntries.size() - 1; j >= 0; j--) {
            uint64_t val = auxvEntries[static_cast<size_t>(j)];
            pushToStack(&val, sizeof(uint64_t));
        }

        // Push envp array first (will end up lowest in memory)
        uint64_t nullPtr = 0;
        uint64_t envpPtr = pushToStack(&nullPtr, sizeof(uint64_t));

        // Push argv array
        uint64_t argvData[2] = {argv0, 0};  // NOLINT
        uint64_t argvPtr = pushToStack(static_cast<const void*>(argvData), 2 * sizeof(uint64_t));

        // Push argc last
        uint64_t argc = 1;
        pushToStack(&argc, sizeof(uint64_t));

        // Set user stack pointer to point to argc
        newTask->context.frame.rsp = userStackVirt - currentVirtOffset;
        // System V x86-64 ABI: RDI=argc, RSI=argv, RDX=envp
        newTask->context.regs.rdi = argc;
        newTask->context.regs.rsi = argvPtr;
        newTask->context.regs.rdx = envpPtr;
#ifdef TASK_DEBUG
        dbg::log("Task %s: argc=%d, argv=0x%x, envp=0x%x, rsp=0x%x", module.name, argc, argvPtr, envpPtr, newTask->context.frame.rsp);
        dbg::log("  userStackVirt=0x%x, currentVirtOffset=0x%x, argv0=0x%x", userStackVirt, currentVirtOffset, argv0);
#endif

        sched::postTaskBalanced(newTask);
    }
#ifdef TASK_DEBUG
    dbg::log("Posted init task(s)");
#endif
    // NOTE: Do NOT start scheduler here - it will be started in startSMT()
    // after secondary CPUs are initialized
}
}  // namespace

auto thisCpuInfo() -> const CpuInfo& { return *cpuData->thisCpu(); }

// Future NUMA support here
auto getCpuNode(uint64_t cpuNo) -> uint64_t { return cpuNo; }

auto getCoreCount() -> uint64_t { return cpu_count; }

auto getCpu(uint64_t number) -> CpuInfo& { return *cpuData->thatCpu(number); }

// Get logical CPU index from APIC ID - doesn't depend on GS register
auto getCpuIndexFromApicId(uint32_t apicId) -> uint64_t {
    for (uint64_t i = 0; i < cpu_count; i++) {
        if (cpuData->thatCpu(i)->lapic_id == apicId) {
            return i;
        }
    }
    // Fallback - shouldn't happen
    return 0;
}

// IPI vector used for halting other CPUs (must not conflict with existing vectors)
constexpr uint8_t HALT_IPI_VECTOR = 0x31;

// Helper interrupt handler executed on other CPUs to halt them in a tight HLT loop.
static void haltIpiHandler(ker::mod::cpu::GPRegs gpr, ker::mod::gates::interruptFrame frame) {
    (void)gpr;
    (void)frame;
    // Mark this CPU as halted for the OOM/panic sequence so the sender can wait
    cpuData->thisCpuLockedVoid([](CpuInfo* c) { c->isHaltedForOom.store(true, std::memory_order_release); });
    asm volatile("cli");
    for (;;) {
        asm volatile("hlt");
    }
}

void init() {
    assert(smp_request.response != nullptr);
    flags = smp_request.response->flags;
    bsp_lapic_id = smp_request.response->bsp_lapic_id;
    cpu_count = smp_request.response->cpu_count;

    // Allocate the kernel PerCpu pointers array
    kernelPerCpuPtrs = new cpu::PerCpu*[cpu_count];
    for (uint64_t i = 0; i < cpu_count; ++i) {
        kernelPerCpuPtrs[i] = nullptr;
    }

    // Register the halt IPI handler so we can broadcast a halting IPI in panic/OOM paths.
    gates::setInterruptHandler(HALT_IPI_VECTOR, haltIpiHandler);
    dbg::log("Registered halt IPI handler for vector 0x%x", (int)HALT_IPI_VECTOR);
}

// init per cpu data
__attribute__((noreturn)) void startSMT(boot::HandoverModules& modules, uint64_t kernelRsp) {
    assert(smp_request.response != nullptr);
    
    // Allocate a dedicated PerCpu structure for BSP (like APs do in cpuParamInit)
    // Don't reuse stack bottom as that can cause issues
    auto* bspPerCpu = new cpu::PerCpu();
    uint64_t perCpuAddr = (uint64_t)bspPerCpu;

    // Zero out the PerCpu area before use
    memset((void*)perCpuAddr, 0, sizeof(cpu::PerCpu));
    
    // Store kernel stack in the PerCpu structure
    bspPerCpu->syscallStack = kernelRsp;

    cpu::wrgsbase(perCpuAddr);
    cpuSetMSR(IA32_KERNEL_GS_BASE, perCpuAddr);

    // Write cpuId directly to the memory location
    bspPerCpu->cpuId = 0;
    cpu::setCurrentCpuid(0);
    
    // Store the BSP's PerCpu pointer for later retrieval
    if (kernelPerCpuPtrs != nullptr) {
        kernelPerCpuPtrs[0] = bspPerCpu;
    }

    // Verify the write worked
    uint64_t readBack = cpu::currentCpu();
    if (readBack != 0) {
        dbg::log("BSP CPU INIT ERROR: wrote cpuId=0 but read back %d, perCpuAddr=%p\n", readBack, perCpuAddr);
    }

    cpuData = new PerCpuCrossAccess<CpuInfo>();

    // Initialize CPU info for all CPUs first
    for (uint64_t i = 0; i < getCoreCount(); i++) {
        cpuData->thatCpu(i)->processor_id = smp_request.response->cpus[i]->processor_id;
        cpuData->thatCpu(i)->lapic_id = smp_request.response->cpus[i]->lapic_id;
        cpuData->thatCpu(i)->goto_address = &smp_request.response->cpus[i]->goto_address;
        cpuData->thatCpu(i)->stack_pointer_ref = (uint64_t*)(smp_request.response->cpus[i]->extra_argument);
    }

    // Allocate stacks for all CPUs first (don't start secondary CPUs yet)
    for (uint64_t i = 0; i < smp_request.response->cpu_count; i++) {
        auto stack = mm::Stack();
        cpuData->thatCpu(i)->stack_pointer_ref = stack.sp;
    }

    // CRITICAL: Initialize scheduler and create init task BEFORE starting secondary CPUs
    // This ensures init gets PID 1, regardless of how many CPUs exist
    sched::percpuInit();
    dbg::log("Creating init task(s) on BSP BEFORE starting secondary CPUs to ensure PID 1");
    createInitTasks(modules, kernelRsp);

    // Start secondary CPUs (their idle tasks all get PID 0 - kernel/swapper convention)
    for (uint64_t i = 0; i < smp_request.response->cpu_count; i++) {
        // Skip BSP - it's already running
        if (smp_request.response->cpus[i]->lapic_id == bsp_lapic_id) {
            continue;
        }

        dbg::log("Starting CPU %d (LAPIC ID: %d)", i, smp_request.response->cpus[i]->lapic_id);

        // Use Limine's goto_address to start the CPU
        // The CPU will call nonPrimaryCpuInit with its limine_smp_info as argument
        __atomic_store_n(&smp_request.response->cpus[i]->goto_address, (limine_goto_address)nonPrimaryCpuInit, __ATOMIC_SEQ_CST);
    }

    // Small delay to let secondary CPUs start
    for (volatile int i = 0; i < 10000000; i++) {
    }

    dbg::log("All CPUs started, starting scheduler on BSP");
    // Create idle task for BSP (gets PID 0 like all idle tasks)
    auto idleTask = new sched::task::Task("idle", 0, kernelRsp, sched::task::TaskType::IDLE);
    sched::postTask(idleTask);
    sched::startScheduler();
    hcf();
}

auto cpuCount() -> uint64_t { return cpu_count; }

// update fsbase in current thread and switch the fs_base register
auto setTcb(void* tcb) -> uint64_t {
    asm volatile("cli");
    // Use scheduler's getCurrentTask() to get the correct per-CPU task
    auto* currentTask = sched::getCurrentTask();
#ifdef TASK_DEBUG
    mod::dbg::log("setTcb: task=%s pid=%d tcb=0x%x old_fsbase=0x%x", currentTask->name ? currentTask->name : "null", currentTask->pid,
                  (uint64_t)tcb, currentTask->thread ? currentTask->thread->fsbase : 0);
#endif
    currentTask->thread->fsbase = (uint64_t)tcb;
    *(uint64_t*)tcb = (uint64_t)tcb;
    cpu::wrfsbase((uint64_t)tcb);
    asm volatile("sti");
    return 0;
}

void haltOtherCores() {
    uint64_t coreCount = getCoreCount();

    // Clear halted flags on all cpus first (so stale flags won't confuse us)
    for (uint64_t i = 0; i < coreCount; ++i) {
        if (i == cpu::currentCpu()) continue;
        cpuData->withLockVoid(i, [](CpuInfo* c) { c->isHaltedForOom.store(false, std::memory_order_relaxed); });
    }

    // Use APIC to broadcast an IPI to all other CPUs without allocating memory.
    ker::mod::apic::IPIConfig ipi = {};
    ipi.vector = HALT_IPI_VECTOR;
    ipi.deliveryMode = ker::mod::apic::IPIDeliveryMode::FIXED;
    ipi.destinationMode = ker::mod::apic::IPIDestinationMode::PHYSICAL;
    ipi.level = ker::mod::apic::IPILevel::ASSERT;
    ipi.triggerMode = ker::mod::apic::IPITriggerMode::EDGE;
    ipi.destinationShorthand = ker::mod::apic::IPIDestinationShorthand::ALL_EXCLUDING_SELF;

    // Destination is ignored when using ALL_EXCLUDING_SELF shorthand but provide a value anyway.
    ker::mod::apic::sendIpi(ipi, ker::mod::apic::IPI_BROADCAST_ID);

    // Wait for other CPUs to set their halted flag, with a timeout. Best-effort.
    const uint64_t MAX_ITER = 2000000;  // tuned for reasonable timeout
    uint64_t iter = 0;
    while (iter++ < MAX_ITER) {
        bool allHalted = true;
        for (uint64_t i = 0; i < coreCount; ++i) {
            if (i == cpu::currentCpu()) continue;
            bool halted = cpuData->withLock(i, [](CpuInfo* c) -> bool { return c->isHaltedForOom.load(std::memory_order_acquire); });
            if (!halted) {
                allHalted = false;
                break;
            }
        }
        if (allHalted) {
            dbg::log("haltOtherCores: all other CPUs reported halted");
            return;
        }
        asm volatile("pause");
    }

    // Timed out waiting - log which cpus did not report halt (best-effort)
    dbg::log("haltOtherCores: timeout waiting for halted CPUs");
    for (uint64_t i = 0; i < coreCount; ++i) {
        if (i == cpu::currentCpu()) continue;
        bool halted = cpuData->withLock(i, [](CpuInfo* c) -> bool { return c->isHaltedForOom.load(std::memory_order_acquire); });
        dbg::log("  CPU %d halted=%d", (int)i, (int)halted);
    }
}

// Get the kernel PerCpu structure for a given CPU index
// This is used when entering idle loop to restore GS_BASE to a valid PerCpu with correct cpuId
cpu::PerCpu* getKernelPerCpu(uint64_t cpuIndex) {
    if (kernelPerCpuPtrs == nullptr || cpuIndex >= cpu_count) {
        return nullptr;
    }
    return kernelPerCpuPtrs[cpuIndex];
}

extern "C" void ker_smt_halt_other_cpus(void) { haltOtherCores(); }

}  // namespace ker::mod::smt
