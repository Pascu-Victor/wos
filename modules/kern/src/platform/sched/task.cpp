#include "task.hpp"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <platform/loader/debug_info.hpp>
#include <platform/loader/elf_loader.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <string_view>
#include <vfs/file.hpp>
#include <vfs/vfs.hpp>

#include "platform/asm/cpu.hpp"
#include "platform/asm/msr.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/sched/threading.hpp"
#include "util/hcf.hpp"

extern "C" void _wOS_kernel_thread_trampoline();  // NOLINT(readability-identifier-naming)

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
    this->parent_pid = 0;        // Initialize to 0 (no parent by default, will be set by exec or fork)
    this->elf_buffer = nullptr;  // No ELF buffer by default
    this->elf_buffer_size = 0;
    this->has_run = false;     // Task hasn't run yet, context.frame contains initial setup
    this->exit_status = 0;     // Initialize exit status
    this->has_exited = false;  // Task hasn't exited yet
    this->waited_on = false;
    this->deferred_task_switch = false;  // No deferred switch by default
    this->yield_switch = false;
    this->kthread_entry = nullptr;
    this->preempt_disable_depth = 0;
    this->preempt_pending = false;
    this->preempt_disable_start_us = 0;
    this->preempt_disable_max_us = 0;
    this->preempt_disable_owner = 0;

    // Waitpid state
    this->waiting_for_pid = 0;
    this->wait_status_user_addr = 0;
    this->wait_status_phys_addr = 0;
    this->wait_rusage_user_addr = 0;
    this->wait_rusage_phys_addr = 0;

    // Process time accounting
    this->start_time_us = 0;  // Will be set when task is first scheduled
    this->user_time_us = 0;
    this->system_time_us = 0;

    // EEVDF scheduling fields
    this->vruntime = 0;
    this->vdeadline = 0;
    this->sched_weight = 1024;  // nice-0 baseline
    this->sched_nice = 0;
    this->slice_ns = 10'000'000;  // 10ms
    this->slice_used_ns = 0;
    this->heap_index = -1;
    this->sched_queue = sched_queue::NONE;
    this->sched_next = nullptr;
    this->wake_at_us = 0;
    this->poll_wait_deadline_us = 0;
    this->wants_block = false;

    // Signal infrastructure
    this->sig_pending = 0;
    this->sig_mask = 0;
    this->in_signal_handler = false;
    this->do_sigreturn = false;
    for (auto& sig_handler : this->sig_handlers) {
        sig_handler = {.handler = 0, .flags = 0, .restorer = 0, .mask = 0};  // SIG_DFL for all
    }

    // fd_table is initialized by RadixTree default constructor (empty)

    if (type == TaskType::IDLE) {
        // Idle tasks use the kernel pagemap
        this->pagemap = mm::virt::get_kernel_pagemap();
        this->type = type;
        this->cpu = cpu::current_cpu();
        this->context.syscall_kernel_stack = kernelRsp;

        // Initialize syscall scratch area even for idle tasks
        // This is needed because switchTo() sets GS_BASE from this field
        this->context.syscall_scratch_area = (uint64_t)(new cpu::PerCpu());
        auto* scratch_area = (cpu::PerCpu*)this->context.syscall_scratch_area;
        scratch_area->syscall_stack = kernelRsp;
        scratch_area->cpu_id = cpu::current_cpu();

        // Idle tasks get PID 0 (kernel/swapper convention) - they don't consume real PIDs
        // This ensures the first user process (init) always gets PID 1 regardless of core count
        this->pid = 0;
        this->entry = 0;
        this->kthread_entry = nullptr;
        this->thread = nullptr;
        return;
    }

    if (type == TaskType::DAEMON) {
        // Kernel thread: ring 0, kernel pagemap, no user thread/TLS, no ELF
        this->pagemap = mm::virt::get_kernel_pagemap();
        this->type = type;
        this->cpu = cpu::current_cpu();
        // Daemons are mostly background service work. Give them a much smaller
        // fair-share budget than user compute threads so periodic wakeups do not
        // dominate a CPU and stretch long-running PROCESS benchmarks by 2-4x.
        this->sched_weight = 128;
        this->sched_nice = 10;
        this->slice_ns = 2'000'000;
        this->context.syscall_kernel_stack = kernelRsp;
        this->thread = nullptr;

        auto* perCpu = new cpu::PerCpu();
        perCpu->syscall_stack = kernelRsp;
        perCpu->cpu_id = cpu::current_cpu();
        this->context.syscall_scratch_area = (uint64_t)perCpu;

        this->pid = sched::task::get_next_pid();
        this->entry = 0;

        // Ring 0 interrupt frame for kernel-mode execution
        this->context.frame.rip = 0;        // Set by create_kernel_thread
        this->context.frame.cs = 0x08;      // GDT_KERN_CS
        this->context.frame.ss = 0x10;      // GDT_KERN_DS
        this->context.frame.flags = 0x202;  // IF=1, reserved bit 1
        this->context.frame.rsp = kernelRsp;
        this->context.frame.int_num = 0;
        this->context.frame.err_code = 0;
        this->context.regs = cpu::GPRegs();
        return;
    }

    // this->entry = entry;
    // this->regs.ip = entry;
    this->pagemap = mm::virt::create_pagemap();
    if (!this->pagemap) {
        dbg::log("Failed to create pagemap for task %s", name);
        hcf();
    }
    this->context.frame.rsp = 0;
    this->context.regs = cpu::GPRegs();
    this->type = type;
    this->cpu = cpu::current_cpu();
    this->context.syscall_kernel_stack = kernelRsp;

    this->pid = sched::task::get_next_pid();
    // POSIX: default process group = own pid (processes start in their own group)
    if (this->pgid == 0) {
        this->pgid = this->pid;
    }

    // CRITICAL: Copy kernel mappings FIRST so we can access kernel memory (like elfBuffer)
    // The elfStart pointer points to kernel heap memory allocated by the parent process
    mm::virt::copy_kernel_mappings(this);

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
    ker::loader::elf::TlsModule actualTlsInfo = loader::elf::extract_tls_info((void*)elfStart);
    this->thread = threading::create_thread(USER_STACK_SIZE, actualTlsInfo.tlsSize, this->pagemap, actualTlsInfo);
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
    perCpu->syscall_stack = kernelRsp;
    perCpu->cpu_id = cpu::current_cpu();
    this->context.syscall_scratch_area = (uint64_t)perCpu;

    this->context.frame.rsp = this->thread->stack;

    loader::elf::ElfLoadResult elfResult = loader::elf::load_elf((loader::elf::ElfFile*)elfStart, this->pagemap, this->pid, this->name);
    if (elfResult.entryPoint == 0) {
        dbg::log("Failed to load ELF for task %s", name);
        hcf();
    }
    this->entry = elfResult.entryPoint;
    this->context.frame.rip = elfResult.entryPoint;
    this->program_header_addr = elfResult.programHeaderAddr;
    this->elf_header_addr = elfResult.elfHeaderAddr;
    this->program_header_count = elfResult.programHeaderCount;
    this->program_header_ent_size = elfResult.programHeaderEntSize;

    // If the binary requests a dynamic linker (PT_INTERP), load it now.
    // The interpreter (ld.so) is loaded at a high base address to avoid
    // conflicting with the main binary's address space.
    if (elfResult.hasInterp) {
        constexpr uint64_t INTERP_BASE = 0x40000000ULL;

        int interpFd = ker::vfs::vfs_open(std::string_view(elfResult.interpPath, __builtin_strlen(elfResult.interpPath)), 0, 0);
        if (interpFd < 0) {
            dbg::log("Failed to open interpreter '%s' for task %s", elfResult.interpPath, name);
            hcf();
        }

        ssize_t interpSize = ker::vfs::vfs_lseek(interpFd, 0, 2);
        ker::vfs::vfs_lseek(interpFd, 0, 0);
        if (interpSize <= 0) {
            ker::vfs::vfs_close(interpFd);
            dbg::log("Invalid interpreter file size for '%s'", elfResult.interpPath);
            hcf();
        }

        auto* interpBuf = new uint8_t[interpSize];
        ssize_t interpRead = 0;
        ker::vfs::vfs_read(interpFd, interpBuf, interpSize, (size_t*)&interpRead);
        ker::vfs::vfs_close(interpFd);

        if (interpRead != interpSize) {
            delete[] interpBuf;
            dbg::log("Short read loading interpreter '%s'", elfResult.interpPath);
            hcf();
        }

        loader::elf::ElfLoadResult interpResult =
            loader::elf::load_elf((loader::elf::ElfFile*)(uint64_t)interpBuf, this->pagemap, this->pid, "ld.so",
                                  false /* don't register debug symbols for interp */, INTERP_BASE);

        if (interpResult.entryPoint == 0) {
            delete[] interpBuf;
            dbg::log("Failed to load interpreter ELF '%s'", elfResult.interpPath);
            hcf();
        }

        // Entry point becomes the interpreter's entry (ld.so _start).
        // ld.so reads AT_ENTRY and AT_PHDR from auxv to find the real binary.
        this->context.frame.rip = interpResult.entryPoint;
        // Store interp_base for AT_BASE in auxv (set by exec.cpp)
        this->interp_base = INTERP_BASE;

        delete[] interpBuf;
    }

    // Initialize interrupt frame fields for usermode
    this->context.frame.int_num = 0;    // Not from a real interrupt
    this->context.frame.err_code = 0;   // No error code
    this->context.frame.ss = 0x1b;      // User stack segment (GDT entry 3, RPL=3) NOLINT
    this->context.frame.cs = 0x23;      // User code segment (GDT entry 4, RPL=3) NOLINT
    this->context.frame.flags = 0x202;  // IF (interrupts enabled) + reserved bit 1 NOLINT

    // Initialize important TLS symbols (e.g. SafeStack pointer) now that ELF is loaded and relocations processed
    ker::loader::debug::DebugSymbol* ssym = ker::loader::debug::get_process_symbol(this->pid, "__safestack_unsafe_stack_ptr");
    if (ssym && ssym->is_tls_offset) {
        uint64_t destVaddr = this->thread->tls_base_virt + ssym->raw_value;  // rawValue stores st_value (TLS offset)
        uint64_t destPaddr = mm::virt::translate(this->pagemap, destVaddr);
        if (destPaddr != mm::virt::PADDR_INVALID) {
            auto* destPtr = (uint64_t*)mm::addr::get_virt_pointer(destPaddr);
            *destPtr = this->thread->safestack_ptr_value;
            dbg::log("Wrote SafeStack ptr for PID %x at vaddr=%x (phys=%x) value=%x", this->pid, destVaddr, destPaddr,
                     this->thread->safestack_ptr_value);
        } else {
            dbg::log("Failed to translate SafeStack TLS vaddr %x for PID %x", destVaddr, this->pid);
        }
    }
}

Task* Task::create_user_thread(Task* parent, uint64_t tcbVaddr, uint64_t userSp, uint64_t enterThreadVa) {
    uint64_t kstackBase = (uint64_t)mm::phys::page_alloc(KERNEL_STACK_SIZE);
    if (kstackBase == 0) {
        dbg::log("createUserThread: OOM allocating kernel stack");
        return nullptr;
    }
    uint64_t kRsp = kstackBase + KERNEL_STACK_SIZE;

    auto* t = new Task{};  // default-constructed; all fields set explicitly below

    // Copy name from parent
    if (parent->name != nullptr) {
        size_t nameLen = strlen(parent->name);
        char* nameCopy = new char[nameLen + 1];
        memcpy(nameCopy, parent->name, nameLen + 1);
        t->name = nameCopy;
    } else {
        t->name = nullptr;
    }

    // Identity
    t->pid = sched::task::get_next_pid();
    t->parent_pid = parent->parent_pid ? parent->parent_pid : parent->pid;
    t->type = TaskType::PROCESS;
    t->is_thread = true;
    t->owner_pid = parent->pid;
    t->cpu = cpu::current_cpu();

    // Share the parent's pagemap - do NOT create a new one
    t->pagemap = parent->pagemap;

    // Thread struct: only fsbase and stack are meaningful; mlibc manages the TLS allocation
    auto* thr = new threading::Thread{};
    thr->magic = 0xDEADBEEF;
    thr->fsbase = tcbVaddr;
    thr->gsbase = 0;
    thr->stack = userSp;
    thr->stack_size = 0;
    thr->tls_size = 0;
    thr->tls_base_virt = 0;
    thr->tls_phys_ptr = 0;
    thr->stack_phys_ptr = 0;
    t->thread = thr;

    // Kernel-space PerCpu scratch area for syscall entry.
    // gsbase must also point here: after swapgs on syscall entry, GS_BASE becomes
    // gsbase, so it must be the PerCpu scratch area - not 0.
    auto* perCpu = new cpu::PerCpu();
    perCpu->syscall_stack = kRsp;
    perCpu->cpu_id = cpu::current_cpu();
    t->context.syscall_kernel_stack = kRsp;
    t->context.syscall_scratch_area = (uint64_t)perCpu;
    thr->gsbase = (uint64_t)perCpu;

    // User-mode interrupt frame: jump straight into __mlibc_enter_thread.
    // sys_prepare_stack pushed [ user_arg, entry ] below userSp (i.e. at userSp and userSp+8).
    // Read them out now so the kernel can set RDI/RSI directly, and advance RSP past them.
    uint64_t entry_va = 0;
    uint64_t user_arg_va = 0;
    {
        // Translate the two words on the prepared stack via the parent pagemap
        uint64_t pa_entry = mm::virt::translate(parent->pagemap, userSp);
        if (pa_entry != mm::virt::PADDR_INVALID) {
            entry_va = *reinterpret_cast<uint64_t*>(mm::addr::get_virt_pointer(pa_entry));
        }
        uint64_t pa_arg = mm::virt::translate(parent->pagemap, userSp + 8);
        if (pa_arg != mm::virt::PADDR_INVALID) {
            user_arg_va = *reinterpret_cast<uint64_t*>(mm::addr::get_virt_pointer(pa_arg));
        }
    }

    t->context.frame.rip = enterThreadVa;  // __mlibc_enter_thread
    t->context.frame.rsp = userSp + 16;    // skip the two pushed words
    t->context.frame.cs = 0x23;            // user code segment
    t->context.frame.ss = 0x1b;            // user stack segment
    t->context.frame.flags = 0x202;        // IF=1, reserved=1
    t->context.regs = cpu::GPRegs{};
    t->context.regs.rdi = entry_va;     // arg1: entry function
    t->context.regs.rsi = user_arg_va;  // arg2: user_arg

    // Inherit FDs: share the same File* pointers and bump each refcount
    parent->fd_table.for_each([&](uint64_t key, void* val) {
        if (val != nullptr) {
            reinterpret_cast<ker::vfs::File*>(val)->refcount.fetch_add(1, std::memory_order_relaxed);
        }
        t->fd_table.insert(key, val);
    });
    // Copy per-fd close-on-exec bitmap
    for (unsigned i = 0; i < Task::FD_TABLE_SIZE / 64; i++) {
        t->fd_cloexec[i] = parent->fd_cloexec[i];
    }
    memcpy(t->cwd, parent->cwd, sizeof(t->cwd));
    memcpy(t->root, parent->root, sizeof(t->root));
    memcpy(t->exe_path, parent->exe_path, sizeof(t->exe_path));
    t->uid = parent->uid;
    t->gid = parent->gid;
    t->euid = parent->euid;
    t->egid = parent->egid;
    t->suid = parent->suid;
    t->sgid = parent->sgid;
    t->umask = parent->umask;
    t->session_id = parent->session_id;
    t->pgid = parent->pgid;
    t->controlling_tty = parent->controlling_tty;
    memcpy(t->wki_target_hostname, parent->wki_target_hostname, sizeof(t->wki_target_hostname));
    t->wki_target_flags = parent->wki_target_flags;
    memcpy(t->wki_submitter_hostname, parent->wki_submitter_hostname, sizeof(t->wki_submitter_hostname));
    t->wki_remote_pid = parent->wki_remote_pid;
    t->wki_vfs_rules.clone_from(parent->wki_vfs_rules);
    t->wki_skip_legacy_placement = false;

    // ELF buffer: threads have no separate ELF
    t->elf_buffer = nullptr;
    t->elf_buffer_size = 0;

    // Scheduling defaults
    t->has_run = false;
    t->has_exited = false;
    t->exit_status = 0;
    t->waited_on = false;
    t->deferred_task_switch = false;
    t->yield_switch = false;
    t->voluntary_block = false;
    t->waiting_for_pid = 0;
    t->wait_status_user_addr = 0;
    t->wait_status_phys_addr = 0;
    t->wait_rusage_user_addr = 0;
    t->wait_rusage_phys_addr = 0;
    t->vruntime = 0;
    t->vdeadline = 0;
    t->sched_weight = parent->sched_weight;
    t->sched_nice = parent->sched_nice;
    t->slice_ns = 10'000'000;
    t->slice_used_ns = 0;
    t->heap_index = -1;
    t->sched_queue = sched_queue::NONE;
    t->sched_next = nullptr;

    // Signals: inherit mask and handlers from parent (POSIX  2.4)
    t->sig_pending = 0;
    t->sig_mask = parent->sig_mask;
    t->in_signal_handler = false;
    t->do_sigreturn = false;
    for (unsigned i = 0; i < MAX_SIGNALS; i++) {
        t->sig_handlers[i] = parent->sig_handlers[i];
    }

    // Time accounting
    t->start_time_us = 0;
    t->user_time_us = 0;
    t->system_time_us = 0;
    t->itimer_real_expire_us = 0;
    t->itimer_real_interval_us = 0;

    return t;
}

Task* Task::create_kernel_thread(const char* name, void (*entryFunc)()) {
    if (entryFunc == nullptr) {
        dbg::log("create_kernel_thread: null entry for '%s'", name != nullptr ? name : "?");
        return nullptr;
    }

    auto stackBase = (uint64_t)mm::phys::page_alloc(KERNEL_STACK_SIZE);
    if (stackBase == 0) {
        dbg::log("create_kernel_thread: OOM allocating kernel stack for '%s'", name);
        return nullptr;
    }
    uint64_t kernelRsp = stackBase + KERNEL_STACK_SIZE;

    auto* task = new Task(name, 0, kernelRsp, TaskType::DAEMON);
    task->kthread_entry = entryFunc;
    task->context.frame.rip = reinterpret_cast<uint64_t>(_wOS_kernel_thread_trampoline);
    task->context.regs.rdi = reinterpret_cast<uint64_t>(entryFunc);
    return task;
}

void Task::load_context(cpu::GPRegs* gpr) { this->context.regs = *gpr; }

void Task::save_context(cpu::GPRegs* gpr) {
    cpu_set_msr(IA32_KERNEL_GS_BASE, this->context.syscall_scratch_area);
    *gpr = context.regs;
}

auto get_next_pid() -> uint64_t {
    static uint64_t next_pid = 1;  // Start from 1 to avoid confusion with kernel tasks
    return next_pid++;
}

}  // namespace ker::mod::sched::task
