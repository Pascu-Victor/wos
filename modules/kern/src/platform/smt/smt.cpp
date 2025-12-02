#include "smt.hpp"

#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/boot/handover.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/mm.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/scheduler.hpp"
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

void cpuParamInit() {
    apic::initApicMP();
    uint64_t cpuNo = apic::getApicId();

    cpuSetMSR(IA32_KERNEL_GS_BASE, (uint64_t)cpuData->thatCpu(cpuNo)->stack_pointer_ref - KERNEL_STACK_SIZE);

    cpu::setCurrentCpuid(cpuNo);
    desc::idt::idtInit();
    sched::percpuInit();
    auto idleTask = new sched::task::Task("idle", 0, 0, sched::task::TaskType::IDLE);
    sched::postTask(idleTask);
    sched::startScheduler();
}

void nonPrimaryCpuInit() {
    desc::idt::loadIdt();
    for (;;) {
        asm volatile("hlt");
    }
    cpuParamInit();
}

void runHandoverTasks(boot::HandoverModules& modStruct, uint64_t kernelRsp) {
    sched::percpuInit();
    for (uint64_t i = 0; i < modStruct.count; i++) {
        const auto& module = modStruct.modules[i];
        auto* newTask = new sched::task::Task(module.name, (uint64_t)module.entry, kernelRsp, sched::task::TaskType::PROCESS);

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

        sched::postTask(newTask);
    }
#ifdef TASK_DEBUG
    dbg::log("Posted task for main thread");
#endif
    sched::startScheduler();
}
}  // namespace

auto thisCpuInfo() -> const CpuInfo& { return *cpuData->thisCpu(); }

// Future NUMA support here
auto getCpuNode(uint64_t cpuNo) -> uint64_t { return cpuNo; }

auto getCoreCount() -> uint64_t { return cpu_count; }

auto getCpu(uint64_t number) -> const CpuInfo& { return *cpuData->thatCpu(number); }

void init() {
    assert(smp_request.response != nullptr);
    flags = smp_request.response->flags;
    bsp_lapic_id = smp_request.response->bsp_lapic_id;
    cpu_count = smp_request.response->cpu_count;
}

// init per cpu data
__attribute__((noreturn)) void startSMT(boot::HandoverModules& modules, uint64_t kernelRsp) {
    assert(smp_request.response != nullptr);
    cpuSetMSR(IA32_KERNEL_GS_BASE, kernelRsp - KERNEL_STACK_SIZE);
    cpu::setCurrentCpuid(0);

    cpuData = new PerCpuCrossAccess<CpuInfo>();

    for (uint64_t i = 0; i < getCoreCount(); i++) {
        cpuData->thatCpu(i)->processor_id = smp_request.response->cpus[i]->processor_id;
        cpuData->thatCpu(i)->lapic_id = smp_request.response->cpus[i]->lapic_id;
        cpuData->thatCpu(i)->goto_address = &smp_request.response->cpus[i]->goto_address;
        cpuData->thatCpu(i)->stack_pointer_ref = (uint64_t*)(smp_request.response->cpus[i]->extra_argument);
    }
    for (uint64_t i = 1; i < smp_request.response->cpu_count; i++) {
        auto stack = mm::Stack();
        cpuData->thatCpu(i)->stack_pointer_ref = stack.sp;
        startCpuTask(i, reinterpret_cast<CpuGotoAddr>(nonPrimaryCpuInit), stack);
    }
    runHandoverTasks(modules, kernelRsp);
    hcf();
}

auto cpuCount() -> uint64_t { return cpu_count; }

// update fsbase in current thread and switch the fs_base register
auto setTcb(void* tcb) -> uint64_t {
#ifdef SMT_DEBUG
    mod::dbg::log("setTcb called with tcb = 0x%x", (uint64_t)tcb);
#endif
    asm volatile("cli");
    cpuData->thisCpu()->currentTask->thread->fsbase = (uint64_t)tcb;
    *(uint64_t*)tcb = (uint64_t)tcb;
    cpu::wrfsbase((uint64_t)tcb);
    asm volatile("sti");
#ifdef SMT_DEBUG
    mod::dbg::log("setTcb: fsbase set to 0x%x", (uint64_t)tcb);
#endif
    return 0;
}

}  // namespace ker::mod::smt
