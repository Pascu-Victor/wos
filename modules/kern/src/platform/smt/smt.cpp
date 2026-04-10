#include "smt.hpp"

#include <extern/limine.h>

#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "mod/io/serial/serial.hpp"
#include "net/proto/tcp.hpp"
#include "net/wki/peer.hpp"
#include "platform/acpi/apic/apic.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/boot/handover.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/interrupt/gates.hpp"
#include "platform/interrupt/gdt.hpp"
#include "platform/interrupt/idt.hpp"
#include "platform/mm/addr.hpp"
#include "platform/mm/dyn/kmalloc.hpp"
#include "platform/mm/mm.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "platform/sys/syscall.hpp"
#include "vfs/file.hpp"
#include "vfs/fs/devfs.hpp"
#include "vfs/fs/tmpfs.hpp"

__attribute__((used, section(".requests"))) const static volatile limine_mp_request smp_request = {
    .id = LIMINE_MP_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
    .flags = 1,
};
namespace ker::mod::smt {
namespace {
static PerCpuCrossAccess<CpuInfo>* cpuData;
uint32_t flags;
uint32_t bsp_lapic_id;
uint64_t g_cpu_count;

// ============================================================================
// CPU Domain registry
// ============================================================================
static CpuDomain domain_table[MAX_CPU_DOMAINS];
static uint32_t domain_count = 0;

}  // namespace

// ============================================================================
// Domain API (public, in ker::mod::smt namespace)
// ============================================================================

auto get_apic_id_for_cpu(uint64_t cpu_no) -> uint32_t {
    if (cpuData == nullptr || cpu_no >= g_cpu_count) {
        return 0;
    }
    return cpuData->that_cpu(cpu_no)->lapic_id;
}

auto get_cpu_domain_count() -> uint32_t { return domain_count; }

auto get_cpu_domain(uint32_t id) -> CpuDomain* {
    for (uint32_t i = 0; i < domain_count; ++i) {
        if (domain_table[i].id == id) {
            return &domain_table[i];
        }
    }
    return nullptr;
}

auto find_group_for_cpu(uint64_t cpu_no) -> uint32_t {
    for (uint32_t i = 0; i < domain_count; ++i) {
        if (domain_table[i].level == CpuDomainLevel::GROUP && (domain_table[i].cpu_mask & (1ULL << cpu_no)) != 0) {
            return domain_table[i].id;
        }
    }
    return 0;  // fall back to ROOT
}

auto create_leaf_domain(const char* name, uint64_t cpu_mask, bool soft_exclusive, bool hard) -> uint32_t {
    if (domain_count >= MAX_CPU_DOMAINS) {
        return DOMAIN_ID_INVALID;
    }
    uint32_t new_id = domain_count;
    CpuDomain& d = domain_table[domain_count++];
    d.id = new_id;
    d.level = CpuDomainLevel::LEAF;
    d.cpu_mask = cpu_mask;
    d.parent_id = 1;  // child of GROUP_0 by default
    d.soft_exclusive = soft_exclusive;
    d.hard = hard;
    d.name[0] = '\0';
    for (int i = 0; i < 31 && name[i] != '\0'; ++i) {
        d.name[i] = name[i];
        d.name[i + 1] = '\0';
    }
    dbg::log("smt: created LEAF domain %u '%s' mask=0x%llx soft_excl=%d hard=%d", new_id, d.name, (unsigned long long)cpu_mask,
             (int)soft_exclusive, (int)hard);
    return new_id;
}

void init_cpu_domains() {
    domain_count = 0;

    // Build full online CPU mask
    uint64_t all_mask = 0;
    for (uint64_t i = 0; i < g_cpu_count && i < 64; ++i) {
        all_mask |= (1ULL << i);
    }

    // Domain 0: ROOT
    CpuDomain& root = domain_table[domain_count++];
    root.id = 0;
    root.level = CpuDomainLevel::ROOT;
    root.cpu_mask = all_mask;
    root.parent_id = DOMAIN_ID_INVALID;
    root.soft_exclusive = false;
    root.hard = false;
    __builtin_memcpy(root.name, "root", 5);

    // Domain 1: single GROUP_0 (flat topology - one socket)
    // Future NUMA support: parse MADT/SRAT to create one group per proximity domain
    CpuDomain& grp = domain_table[domain_count++];
    grp.id = 1;
    grp.level = CpuDomainLevel::GROUP;
    grp.cpu_mask = all_mask;
    grp.parent_id = 0;
    grp.soft_exclusive = false;
    grp.hard = false;
    __builtin_memcpy(grp.name, "group0", 7);

    dbg::log("smt: cpu_domains initialised: root mask=0x%llx group0 mask=0x%llx", (unsigned long long)all_mask,
             (unsigned long long)all_mask);
}

namespace {

// Array to store kernel PerCpu structure addresses for each CPU
// These are the PerCpu structures allocated during boot that have correct cpuId
// Used to restore GS_BASE when entering idle loop (no task context)
cpu::PerCpu** kernelPerCpuPtrs = nullptr;

// Atomic counter to track how many CPUs have completed GS_BASE initialization
// The last CPU to reach cpu_count enables per-CPU allocations globally
std::atomic<uint64_t> cpusInitialized{0};

void cpuParamInit(uint64_t cpuNo, uint64_t stackTop) {
    // Enable CPU features FIRST (must be done on each CPU)
    // FSGSBASE must be enabled before using wrgsbase instruction
    cpu::enableFSGSBASE();
    cpu::enableSSE();
    cpu::enableXSave();

    // Set up per-CPU data
    // Allocate a dedicated PerCpu structure for this CPU (don't reuse stack bottom
    // as that can corrupt adjacent memory or heap metadata)
    auto* per_cpu_data = new cpu::PerCpu();
    auto per_cpu_addr = (uint64_t)per_cpu_data;

    // Zero out the PerCpu area
    memset((void*)per_cpu_addr, 0, sizeof(cpu::PerCpu));

    // Store kernel stack in the PerCpu structure
    per_cpu_data->syscallStack = stackTop;

    cpu::wrgsbase(per_cpu_addr);
    cpuSetMSR(IA32_KERNEL_GS_BASE, per_cpu_addr);

    // Write cpuId directly to the memory location to verify
    per_cpu_data->cpuId = cpuNo;

    // Store the per-CPU PerCpu pointer for later retrieval (e.g., when entering idle loop)
    if (kernelPerCpuPtrs != nullptr) {
        kernelPerCpuPtrs[cpuNo] = per_cpu_data;
    }

    // Also use setCurrentCpuid for consistency
    cpu::setCurrentCpuid(cpuNo);

    // Verify the write worked
    uint64_t readBack = cpu::currentCpu();
    if (readBack != cpuNo) {
        // Use serial directly since dbg might not be ready
        dbg::log("CPU INIT ERROR: wrote cpuId=%d but read back %d, perCpuAddr=%p\n", cpuNo, readBack, per_cpu_addr);
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
    sched::percpu_init();

    // Create idle task for this CPU
    // Pass the kernel stack TOP (stack grows downward, so syscall needs to start from top)
    auto* idle_task = new sched::task::Task("idle", 0, stackTop, sched::task::TaskType::IDLE);
    sched::post_task(idle_task);

    // Atomically increment the counter of initialized CPUs
    // If we're the last CPU, enable per-CPU allocations globally
    uint64_t initialized_count = cpusInitialized.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (initialized_count == g_cpu_count) {
        dbg::log("CPU %d: Last CPU initialized, enabling per-CPU allocations globally", cpuNo);
        mm::phys::enablePerCpuAllocations();
        mm::dyn::kmalloc::enablePerCpuAllocations();
    }

    dbg::log("CPU %d initialized and ready (%d/%d CPUs ready)", cpuNo, initialized_count, g_cpu_count);

    // Start the scheduler on this CPU
    sched::start_scheduler();
}

void non_primary_cpu_init(limine_mp_info* smp_info) {
    // FIRST: Switch to kernel page table before accessing any kernel data
    mm::virt::switchToKernelPagemap();

    uint64_t cpu_no = 0;

    // Find our CPU number from the LAPIC ID
    for (uint64_t i = 0; i < g_cpu_count; i++) {
        if (smp_request.response->cpus[i]->lapic_id == smp_info->lapic_id) {
            cpu_no = i;
            break;
        }
    }

    auto stack_top = (uint64_t)cpuData->that_cpu(cpu_no)->stack_pointer_ref;

    // Initialize this CPU fully
    cpuParamInit(cpu_no, stack_top);

    // Should never reach here
    hcf();
}

// Create init task(s) from handover modules WITHOUT starting scheduler
// This is called early to ensure init gets PID 1
void create_init_tasks(boot::HandoverModules& mod_struct, uint64_t kernel_rsp) {
    // Try loading /sbin/init from tmpfs (unpacked from CPIO initramfs)
    auto* init_node = ker::vfs::tmpfs::tmpfs_walk_path("sbin/init", false);
    if (init_node != nullptr && init_node->type == ker::vfs::tmpfs::TmpNodeType::FILE && init_node->data != nullptr) {
        dbg::log("Loading init from tmpfs (/sbin/init, %u bytes)", static_cast<unsigned>(init_node->size));
        // Override modules with the tmpfs init binary
        mod_struct.count = 1;
        mod_struct.modules[0].name = "/sbin/init";
        mod_struct.modules[0].entry = init_node->data;
        mod_struct.modules[0].size = init_node->size;
        mod_struct.modules[0].cmdline = "/sbin/init";
    }

    for (uint64_t i = 0; i < mod_struct.count; i++) {
        const auto& module = mod_struct.modules[i];
        auto* new_task = new sched::task::Task(module.name, (uint64_t)module.entry, kernel_rsp, sched::task::TaskType::PROCESS);

        if (new_task == nullptr || new_task->thread == nullptr || new_task->pagemap == nullptr) {
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
            new_task->fds[0] = console_stdin;
            console_stdin->fd = 0;
            dbg::log("Setup fd 0 (stdin): %p", console_stdin);
            new_task->fds[1] = console_stdout;
            console_stdout->fd = 1;
            dbg::log("Setup fd 1 (stdout): %p", console_stdout);
            new_task->fds[2] = console_stderr;
            console_stderr->fd = 2;
            dbg::log("Setup fd 2 (stderr): %p", console_stderr);
            dbg::log("Verifying: fds[0]=%p, fds[1]=%p, fds[2]=%p", new_task->fds[0], new_task->fds[1], new_task->fds[2]);

            dbg::log("Setup stdin/stdout/stderr for task %s", module.name);
        } else {
            dbg::log("WARNING: Failed to open /dev/console for task %s", module.name);
        }

        // Setup minimal argc/argv/envp on user stack
        uint64_t user_stack_virt = new_task->thread->stack;
        uint64_t current_virt_offset = 0;

        // Helper to push data onto stack
        auto push_to_stack = [&](const void* data, size_t size) -> uint64_t {
            if (current_virt_offset + size > USER_STACK_SIZE) {
                return 0;
            }
            current_virt_offset += size;
            uint64_t virt_addr = user_stack_virt - current_virt_offset;

            uint64_t page_virt = virt_addr & ~(mm::paging::PAGE_SIZE - 1);
            uint64_t page_offset = virt_addr & (mm::paging::PAGE_SIZE - 1);

            uint64_t page_phys = mm::virt::translate(new_task->pagemap, page_virt);
            if (page_phys == mm::virt::PADDR_INVALID) {
                dbg::log("PANIC: Failed to translate page virt=0x%x for stack data - stack page not mapped", page_virt);
                hcf();
            }

            auto* dest_ptr = reinterpret_cast<uint8_t*>(mm::addr::get_virt_pointer(page_phys)) + page_offset;
            std::memcpy(dest_ptr, data, size);
#ifdef TASK_DEBUG
            dbg::log("Pushed %d bytes: virt=0x%x, phys=0x%x, data[0]=0x%x", size, virt_addr, page_phys, *(uint32_t*)data);
#endif
            return virt_addr;
        };

        // Helper to push string onto stack
        auto push_string = [&](const char* str) -> uint64_t {
            size_t len = std::strlen(str) + 1;
            if (current_virt_offset + len > USER_STACK_SIZE) {
                return 0;
            }
            current_virt_offset += len;
            uint64_t virt_addr = user_stack_virt - current_virt_offset;

            uint64_t page_virt = virt_addr & ~(mm::paging::PAGE_SIZE - 1);
            uint64_t page_offset = virt_addr & (mm::paging::PAGE_SIZE - 1);

            uint64_t page_phys = mm::virt::translate(new_task->pagemap, page_virt);
            if (page_phys == mm::virt::PADDR_INVALID) {
                dbg::log("PANIC: Failed to translate page virt=0x%x for string '%s' - stack page not mapped", page_virt, str);
                hcf();
            }

            auto* dest_ptr = reinterpret_cast<uint8_t*>(mm::addr::get_virt_pointer(page_phys)) + page_offset;
            std::memcpy(dest_ptr, str, len);
#ifdef TASK_DEBUG
            dbg::log("Pushed string '%s' at virt=0x%x (len=%d)", str, virt_addr, len);
#endif
            return virt_addr;
        };

        // Push program name as argv[0]
        uint64_t argv0 = push_string(module.name);

        // Align stack to 16 bytes
        // no return address to push, so just align
        constexpr uint64_t ALIGNMENT = 16;
        uint64_t current_addr = user_stack_virt - current_virt_offset;
        uint64_t aligned = current_addr & ~(ALIGNMENT - 1);
        current_virt_offset += (current_addr - aligned);

        constexpr uint64_t AT_NULL = 0;
        constexpr uint64_t AT_PAGESZ = 6;
        constexpr uint64_t AT_ENTRY = 9;
        constexpr uint64_t AT_PHDR = 3;
        constexpr uint64_t AT_EHDR = 33;  // AT_EHDR (glibc extension but widely supported)

        std::array<uint64_t, 10> auxv_entries = {AT_PAGESZ, (uint64_t)mm::paging::PAGE_SIZE,
                                                 AT_ENTRY,  new_task->entry,
                                                 AT_PHDR,   new_task->programHeaderAddr,
                                                 AT_EHDR,   new_task->elfHeaderAddr,
                                                 AT_NULL,   0};

        // Push auxv in reverse order
        for (int j = auxv_entries.size() - 1; j >= 0; j--) {
            uint64_t val = auxv_entries[static_cast<size_t>(j)];
            push_to_stack(&val, sizeof(uint64_t));
        }

        // Push envp array first (will end up lowest in memory)
        uint64_t null_ptr = 0;
        uint64_t envp_ptr = push_to_stack(&null_ptr, sizeof(uint64_t));

        // Push argv array
        uint64_t argv_data[2] = {argv0, 0};  // NOLINT
        uint64_t argv_ptr = push_to_stack(static_cast<const void*>(argv_data), 2 * sizeof(uint64_t));

        // Push argc last
        uint64_t argc = 1;
        push_to_stack(&argc, sizeof(uint64_t));

        // Set user stack pointer to point to argc
        new_task->context.frame.rsp = user_stack_virt - current_virt_offset;
        // System V x86-64 ABI: RDI=argc, RSI=argv, RDX=envp
        new_task->context.regs.rdi = argc;
        new_task->context.regs.rsi = argv_ptr;
        new_task->context.regs.rdx = envp_ptr;
#ifdef TASK_DEBUG
        dbg::log("Task %s: argc=%d, argv=0x%x, envp=0x%x, rsp=0x%x", module.name, argc, argv_ptr, envp_ptr, new_task->context.frame.rsp);
        dbg::log("  userStackVirt=0x%x, currentVirtOffset=0x%x, argv0=0x%x", user_stack_virt, current_virt_offset, argv0);
#endif

        sched::post_task_balanced(new_task);
    }
#ifdef TASK_DEBUG
    dbg::log("Posted init task(s)");
#endif
    // NOTE: Do NOT start scheduler here - it will be started in startSMT()
    // after secondary CPUs are initialized
}
}  // namespace

auto thisCpuInfo() -> const CpuInfo& { return *cpuData->this_cpu(); }

// Future NUMA support here
auto get_cpu_node(uint64_t cpu_no) -> uint64_t { return cpu_no; }

auto get_core_count() -> uint64_t { return g_cpu_count; }

auto get_early_cpu_count() -> uint64_t {
    if (g_cpu_count > 0) {
        return g_cpu_count;
    }
    if (smp_request.response != nullptr) {
        return smp_request.response->cpu_count;
    }
    return 1;
}

auto get_cpu(uint64_t number) -> CpuInfo& { return *cpuData->that_cpu(number); }

// Get logical CPU index from APIC ID - doesn't depend on GS register
auto get_cpu_index_from_apic_id(uint32_t apicId) -> uint64_t {
    for (uint64_t i = 0; i < g_cpu_count; i++) {
        if (cpuData->that_cpu(i)->lapic_id == apicId) {
            return i;
        }
    }
    // Fallback - shouldn't happen
    return 0;
}
namespace {
// IPI vector used for halting other CPUs, dynamically allocated via gates::allocateVector()
uint8_t halt_ipi_vector = 0;

// Helper interrupt handler executed on other CPUs to halt them in a tight HLT loop.
void halt_ipi_handler(uint8_t vector, void* private_data) {
    (void)vector;
    (void)private_data;
    // Mark this CPU as halted for the OOM/panic sequence so the sender can wait
    cpuData->this_cpu_locked_void([](CpuInfo* c) { c->isHaltedForOom.store(true, std::memory_order_release); });
    asm volatile("cli");
    for (;;) {
        asm volatile("hlt");
    }
}
}  // namespace

void init() {
    assert(smp_request.response != nullptr);
    flags = smp_request.response->flags;
    bsp_lapic_id = smp_request.response->bsp_lapic_id;
    g_cpu_count = smp_request.response->cpu_count;

    // Allocate the kernel PerCpu pointers array
    kernelPerCpuPtrs = new cpu::PerCpu*[g_cpu_count];
    for (uint64_t i = 0; i < g_cpu_count; ++i) {
        kernelPerCpuPtrs[i] = nullptr;
    }

    // Dynamically allocate a vector and register the halt IPI handler so we can
    // broadcast a halting IPI in panic/OOM paths.
    halt_ipi_vector = gates::allocateVector();
    assert(halt_ipi_vector != 0);
    gates::requestIrq(halt_ipi_vector, halt_ipi_handler, nullptr, "halt_ipi");
    dbg::log("Registered halt IPI handler for vector 0x%x", (int)halt_ipi_vector);

    // Initialise CPU domain hierarchy (ROOT + GROUP_0; no LEAFs yet)
    init_cpu_domains();
}

// init per cpu data
void start_smt(boot::HandoverModules& modules, uint64_t kernel_rsp) {
    assert(smp_request.response != nullptr);

    // Allocate a dedicated PerCpu structure for BSP (like APs do in cpuParamInit)
    // Don't reuse stack bottom as that can cause issues
    auto* bsp_per_cpu = new cpu::PerCpu();
    auto per_cpu_addr = reinterpret_cast<uint64_t>(bsp_per_cpu);

    // Zero out the PerCpu area before use
    memset((void*)per_cpu_addr, 0, sizeof(cpu::PerCpu));

    // Store kernel stack in the PerCpu structure
    bsp_per_cpu->syscallStack = kernel_rsp;

    cpu::wrgsbase(per_cpu_addr);
    cpuSetMSR(IA32_KERNEL_GS_BASE, per_cpu_addr);

    // Write cpuId directly to the memory location
    bsp_per_cpu->cpuId = 0;
    cpu::setCurrentCpuid(0);

    // GS base is now valid - cpu::currentCpu() works, safe to enable CPU ID in serial logs
    io::serial::markCpuIdAvailable();

    // Store the BSP's PerCpu pointer for later retrieval
    if (kernelPerCpuPtrs != nullptr) {
        kernelPerCpuPtrs[0] = bsp_per_cpu;
    }

    // Verify the write worked
    uint64_t read_back = cpu::currentCpu();
    if (read_back != 0) {
        dbg::log("BSP CPU INIT ERROR: wrote cpuId=0 but read back %d, perCpuAddr=%p\n", read_back, per_cpu_addr);
    }

    cpuData = new PerCpuCrossAccess<CpuInfo>();

    // Initialize CPU info for all CPUs first
    for (uint64_t i = 0; i < get_core_count(); i++) {
        cpuData->that_cpu(i)->processor_id = smp_request.response->cpus[i]->processor_id;
        cpuData->that_cpu(i)->lapic_id = smp_request.response->cpus[i]->lapic_id;
        cpuData->that_cpu(i)->goto_address = &smp_request.response->cpus[i]->goto_address;
        cpuData->that_cpu(i)->stack_pointer_ref = (uint64_t*)(smp_request.response->cpus[i]->extra_argument);
    }

    // Allocate stacks for all CPUs first (don't start secondary CPUs yet)
    for (uint64_t i = 0; i < smp_request.response->cpu_count; i++) {
        auto stack = mm::Stack();
        cpuData->that_cpu(i)->stack_pointer_ref = stack.sp;
    }

    // CRITICAL: Initialize scheduler and create init task BEFORE starting secondary CPUs
    // This ensures init gets PID 1, regardless of how many CPUs exist
    sched::percpu_init();
    dbg::log("Creating init task(s) on BSP BEFORE starting secondary CPUs to ensure PID 1");
    create_init_tasks(modules, kernel_rsp);

    // Start the TCP timer as a kernel thread (DAEMON) instead of running it in interrupt context
    ker::net::proto::tcp_timer_thread_start();

    // Start the WKI timer as a kernel thread (heartbeats, fencing, retransmit, load reports)
    ker::net::wki::wki_timer_thread_start();

    // Start secondary CPUs (their idle tasks all get PID 0 - kernel/swapper convention)
    for (uint64_t i = 0; i < smp_request.response->cpu_count; i++) {
        // Skip BSP - it's already running
        if (smp_request.response->cpus[i]->lapic_id == bsp_lapic_id) {
            continue;
        }

        dbg::log("Starting CPU %d (LAPIC ID: %d)", i, smp_request.response->cpus[i]->lapic_id);

        // Use Limine's goto_address to start the CPU
        // The CPU will call nonPrimaryCpuInit with its limine_smp_info as argument
        __atomic_store_n(&smp_request.response->cpus[i]->goto_address, (limine_goto_address)non_primary_cpu_init, __ATOMIC_SEQ_CST);
    }

    dbg::log("All CPUs started, starting scheduler on BSP");
    // Create idle task for BSP (gets PID 0 like all idle tasks)
    auto* idle_task = new sched::task::Task("idle", 0, kernel_rsp, sched::task::TaskType::IDLE);
    sched::post_task(idle_task);

    // BSP will participate in the atomic counter via cpuParamInit-style logic
    // We need to call the same logic here for BSP
    uint64_t initialized_count = cpusInitialized.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (initialized_count == g_cpu_count) {
        dbg::log("BSP: Last CPU initialized, enabling per-CPU allocations globally");
        mm::phys::enablePerCpuAllocations();
        mm::dyn::kmalloc::enablePerCpuAllocations();
    }
    dbg::log("BSP ready (%d/%d CPUs ready)", initialized_count, g_cpu_count);
}

auto cpu_count() -> uint64_t { return g_cpu_count; }

// update fsbase in current thread and switch the fs_base register
auto set_tcb(void* tcb) -> uint64_t {
    asm volatile("cli");
    // Use scheduler's getCurrentTask() to get the correct per-CPU task
    auto* currentTask = sched::get_current_task();
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

void halt_other_cores() {
    uint64_t core_count = get_core_count();

    // Clear halted flags on all cpus first (so stale flags won't confuse us)
    for (uint64_t i = 0; i < core_count; ++i) {
        if (i == cpu::currentCpu()) {
            continue;
        }
        cpuData->with_lock_void(i, [](CpuInfo* c) -> void { c->isHaltedForOom.store(false, std::memory_order_relaxed); });
    }

    // Use APIC to broadcast an IPI to all other CPUs without allocating memory.
    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access)
    ker::mod::apic::IPIConfig ipi = {};
    ipi.vector = halt_ipi_vector;
    ipi.deliveryMode = ker::mod::apic::IPIDeliveryMode::FIXED;
    ipi.destinationMode = ker::mod::apic::IPIDestinationMode::PHYSICAL;
    ipi.level = ker::mod::apic::IPILevel::ASSERT;
    ipi.triggerMode = ker::mod::apic::IPITriggerMode::EDGE;
    ipi.destinationShorthand = ker::mod::apic::IPIDestinationShorthand::ALL_EXCLUDING_SELF;
    // NOLINTEND(cppcoreguidelines-pro-type-union-access)

    // Destination is ignored when using ALL_EXCLUDING_SELF shorthand but provide a value anyway.
    ker::mod::apic::sendIpi(ipi, ker::mod::apic::IPI_BROADCAST_ID);

    // Wait for other CPUs to set their halted flag, with a timeout. Best-effort.
    const uint64_t MAX_ITER = 2000000;  // tuned for reasonable timeout
    uint64_t iter = 0;
    while (iter++ < MAX_ITER) {
        bool all_halted = true;
        for (uint64_t i = 0; i < core_count; ++i) {
            if (i == cpu::currentCpu()) {
                continue;
            }
            bool halted = cpuData->with_lock(i, [](CpuInfo* c) -> bool { return c->isHaltedForOom.load(std::memory_order_acquire); });
            if (!halted) {
                all_halted = false;
                break;
            }
        }
        if (all_halted) {
            dbg::log("haltOtherCores: all other CPUs reported halted");
            return;
        }
        asm volatile("pause");
    }

    // Timed out waiting - log which cpus did not report halt (best-effort)
    dbg::log("haltOtherCores: timeout waiting for halted CPUs");
    for (uint64_t i = 0; i < core_count; ++i) {
        if (i == cpu::currentCpu()) {
            continue;
        }
        bool halted = cpuData->with_lock(i, [](CpuInfo* c) -> bool { return c->isHaltedForOom.load(std::memory_order_acquire); });
        dbg::log("  CPU %d halted=%d", (int)i, (int)halted);
    }
}

// Get the kernel PerCpu structure for a given CPU index
// This is used when entering idle loop to restore GS_BASE to a valid PerCpu with correct cpuId
auto get_kernel_per_cpu(uint64_t cpu_index) -> cpu::PerCpu* {
    if (kernelPerCpuPtrs == nullptr || cpu_index >= g_cpu_count) {
        return nullptr;
    }
    return kernelPerCpuPtrs[cpu_index];
}

extern "C" void ker_smt_halt_other_cpus(void) { halt_other_cores(); }

}  // namespace ker::mod::smt
