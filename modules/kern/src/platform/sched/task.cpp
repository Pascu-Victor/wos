#include "task.hpp"

#include <bits/ssize_t.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <iterator>
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

extern "C" void wos_kernel_thread_trampoline();  // NOLINT(readability-identifier-naming)

namespace ker::mod::sched::task {
Task::Task(const char* name, uint64_t elf_start, uint64_t kernel_rsp, TaskType type) {
    // CRITICAL: Copy the name string to kernel heap memory!
    // The passed 'name' might point to Limine boot memory or user memory
    // which won't be mapped when we switch pagemaps.
    if (name != nullptr) {
        size_t const NAME_LEN = std::strlen(name);
        char* name_copy = new char[NAME_LEN + 1];
        std::memcpy(name_copy, name, NAME_LEN + 1);
        this->name = name_copy;
    } else {
        this->name = nullptr;
    }
    this->parent_pid = 0;        // Initialize to 0 (no parent by default, will be set by exec or fork)
    this->elf_buffer = nullptr;  // No ELF buffer by default
    this->elf_buffer_size = 0;
    this->has_run = false;     // Task hasn't run yet, context.frame contains initial setup
    this->exit_status = 0;     // Initialize exit status
    this->has_exited = false;  // Task hasn't exited yet
    this->exit_notify_ready.store(false, std::memory_order_relaxed);
    this->waited_on = false;
    this->zombie_resources_reclaiming.store(false, std::memory_order_relaxed);
    this->zombie_resources_reclaimed.store(false, std::memory_order_relaxed);
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
    this->syscall_account_start_us = 0;
    this->precharged_syscall_time_us = 0;

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
    this->sigsuspend_saved_mask = 0;
    this->sigsuspend_active = false;
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
        this->context.syscall_kernel_stack = kernel_rsp;

        // Initialize syscall scratch area even for idle tasks
        // This is needed because switchTo() sets GS_BASE from this field
        this->context.syscall_scratch_area = reinterpret_cast<uint64_t>(new cpu::PerCpu());
        auto* scratch_area = reinterpret_cast<cpu::PerCpu*>(this->context.syscall_scratch_area);
        scratch_area->syscall_stack = kernel_rsp;
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
        this->context.syscall_kernel_stack = kernel_rsp;
        this->thread = nullptr;

        auto* per_cpu = new cpu::PerCpu();
        per_cpu->syscall_stack = kernel_rsp;
        per_cpu->cpu_id = cpu::current_cpu();
        this->context.syscall_scratch_area = reinterpret_cast<uint64_t>(per_cpu);

        this->pid = sched::task::get_next_pid();
        this->entry = 0;

        // Ring 0 interrupt frame for kernel-mode execution
        this->context.frame.rip = 0;        // Set by create_kernel_thread
        this->context.frame.cs = 0x08;      // GDT_KERN_CS
        this->context.frame.ss = 0x10;      // GDT_KERN_DS
        this->context.frame.flags = 0x202;  // IF=1, reserved bit 1
        this->context.frame.rsp = kernel_rsp;
        this->context.frame.int_num = 0;
        this->context.frame.err_code = 0;
        this->context.regs = cpu::GPRegs();
        return;
    }

    // this->entry = entry;
    // this->regs.ip = entry;
    this->pagemap = mm::virt::create_pagemap();
    if (this->pagemap == nullptr) {
        dbg::log("Failed to create pagemap for task %s", name);
        hcf();
    }
    this->context.frame.rsp = 0;
    this->context.regs = cpu::GPRegs();
    this->type = type;
    this->cpu = cpu::current_cpu();
    this->context.syscall_kernel_stack = kernel_rsp;

    this->pid = sched::task::get_next_pid();
    // POSIX: default process group = own pid (processes start in their own group)
    if (this->pgid == 0) {
        this->pgid = this->pid;
    }

    // CRITICAL: Copy kernel mappings FIRST so we can access kernel memory (like elfBuffer)
    // The elfStart pointer points to kernel heap memory allocated by the parent process
    mm::virt::copy_kernel_mappings(this);

    // Validate ELF pointer before any operations
    if (elf_start == 0) {
        dbg::log("ERROR: Task created with null ELF pointer");
        hcf();
    }

    // Add compiler memory barrier to ensure elfStart is fully visible
    __asm__ volatile("mfence" ::: "memory");

    // Validate ELF magic bytes before proceeding
    auto* elf_header = reinterpret_cast<uint8_t*>(elf_start);

    if (elf_header[0] != 0x7F || elf_header[1] != 'E' || elf_header[2] != 'L' || elf_header[3] != 'F') {
        dbg::log("ERROR: Invalid ELF magic at 0x%p: [0x%x 0x%x 0x%x 0x%x]", reinterpret_cast<void*>(elf_start), elf_header[0],
                 elf_header[1], elf_header[2], elf_header[3]);
        dbg::log("Expected ELF magic: [0x7F 'E' 'L' 'F'] = [0x7F 0x45 0x4C 0x46]");
        hcf();
    }

    // FIXED: Parse ELF first to get actual TLS size, then create thread
    ker::loader::elf::TlsModule const ACTUAL_TLS_INFO = loader::elf::extract_tls_info(reinterpret_cast<void*>(elf_start));
    this->thread = threading::create_thread(ker::mod::mm::USER_STACK_SIZE, ACTUAL_TLS_INFO.tls_size, this->pagemap, ACTUAL_TLS_INFO);
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
    auto* per_cpu = new cpu::PerCpu();
    per_cpu->syscall_stack = kernel_rsp;
    per_cpu->cpu_id = cpu::current_cpu();
    this->context.syscall_scratch_area = reinterpret_cast<uint64_t>(per_cpu);

    this->context.frame.rsp = this->thread->stack;

    loader::elf::ElfLoadResult elf_result =
        loader::elf::load_elf(reinterpret_cast<loader::elf::ElfFile*>(elf_start), this->pagemap, this->pid, this->name);
    if (elf_result.entry_point == 0) {
        dbg::log("Failed to load ELF for task %s", name);
        hcf();
    }
    this->entry = elf_result.entry_point;
    this->context.frame.rip = elf_result.entry_point;
    this->program_header_addr = elf_result.program_header_addr;
    this->elf_header_addr = elf_result.elf_header_addr;
    this->program_header_count = elf_result.program_header_count;
    this->program_header_ent_size = elf_result.program_header_ent_size;

    // If the binary requests a dynamic linker (PT_INTERP), load it now.
    // The interpreter (ld.so) is loaded at a high base address to avoid
    // conflicting with the main binary's address space.
    if (elf_result.has_interp) {
        constexpr uint64_t INTERP_BASE = 0x40000000ULL;
        const char* const INTERP_PATH = std::begin(elf_result.interp_path);

        int const INTERP_FD = ker::vfs::vfs_open(std::string_view(INTERP_PATH, __builtin_strlen(INTERP_PATH)), 0, 0);
        if (INTERP_FD < 0) {
            dbg::log("Failed to open interpreter '%s' for task %s", INTERP_PATH, name);
            hcf();
        }

        ssize_t const INTERP_SIZE = ker::vfs::vfs_lseek(INTERP_FD, 0, 2);
        ker::vfs::vfs_lseek(INTERP_FD, 0, 0);
        if (INTERP_SIZE <= 0) {
            ker::vfs::vfs_close(INTERP_FD);
            dbg::log("Invalid interpreter file size for '%s'", INTERP_PATH);
            hcf();
        }

        auto* interp_buf = new uint8_t[INTERP_SIZE];
        ssize_t interp_read = 0;
        ker::vfs::vfs_read(INTERP_FD, interp_buf, INTERP_SIZE, reinterpret_cast<size_t*>(&interp_read));
        ker::vfs::vfs_close(INTERP_FD);

        if (interp_read != INTERP_SIZE) {
            delete[] interp_buf;
            dbg::log("Short read loading interpreter '%s'", INTERP_PATH);
            hcf();
        }

        loader::elf::ElfLoadResult const INTERP_RESULT =
            loader::elf::load_elf(reinterpret_cast<loader::elf::ElfFile*>(interp_buf), this->pagemap, this->pid, "ld.so",
                                  false /* don't register debug symbols for interp */, INTERP_BASE);

        if (INTERP_RESULT.entry_point == 0) {
            delete[] interp_buf;
            dbg::log("Failed to load interpreter ELF '%s'", INTERP_PATH);
            hcf();
        }

        // Entry point becomes the interpreter's entry (ld.so _start).
        // ld.so reads AT_ENTRY and AT_PHDR from auxv to find the real binary.
        this->context.frame.rip = INTERP_RESULT.entry_point;
        // Store interp_base for AT_BASE in auxv (set by exec.cpp)
        this->interp_base = INTERP_BASE;

        delete[] interp_buf;
    }

    // Initialize interrupt frame fields for usermode
    this->context.frame.int_num = 0;    // Not from a real interrupt
    this->context.frame.err_code = 0;   // No error code
    this->context.frame.ss = 0x1b;      // User stack segment (GDT entry 3, RPL=3) NOLINT
    this->context.frame.cs = 0x23;      // User code segment (GDT entry 4, RPL=3) NOLINT
    this->context.frame.flags = 0x202;  // IF (interrupts enabled) + reserved bit 1 NOLINT

    // Initialize important TLS symbols (e.g. SafeStack pointer) now that ELF is loaded and relocations processed
    ker::loader::debug::DebugSymbol const* ssym = ker::loader::debug::get_process_symbol(this->pid, "__safestack_unsafe_stack_ptr");
    if ((ssym != nullptr) && ssym->is_tls_offset) {
        uint64_t const DEST_VADDR = this->thread->tls_base_virt + ssym->raw_value;  // rawValue stores st_value (TLS offset)
        uint64_t const DEST_PADDR = mm::virt::translate(this->pagemap, DEST_VADDR);
        if (DEST_PADDR != mm::virt::PADDR_INVALID) {
            auto* dest_ptr = static_cast<uint64_t*>(mm::addr::get_virt_pointer(DEST_PADDR));
            *dest_ptr = this->thread->safestack_ptr_value;
            dbg::log("Wrote SafeStack ptr for PID %x at vaddr=%x (phys=%x) value=%x", this->pid, DEST_VADDR, DEST_PADDR,
                     this->thread->safestack_ptr_value);
        } else {
            dbg::log("Failed to translate SafeStack TLS vaddr %x for PID %x", DEST_VADDR, this->pid);
        }
    }
}

Task* Task::create_user_thread(Task* parent, uint64_t tcb_vaddr, uint64_t user_sp, uint64_t enter_thread_va) {
    auto const KSTACK_BASE = reinterpret_cast<uint64_t>(mm::phys::page_alloc(ker::mod::mm::KERNEL_STACK_SIZE));
    if (KSTACK_BASE == 0) {
        dbg::log("createUserThread: OOM allocating kernel stack");
        return nullptr;
    }
    uint64_t const K_RSP = KSTACK_BASE + ker::mod::mm::KERNEL_STACK_SIZE;

    auto* t = new Task{};  // default-constructed; all fields set explicitly below

    // Copy name from parent
    if (parent->name != nullptr) {
        size_t const NAME_LEN = std::strlen(parent->name);
        char* name_copy = new char[NAME_LEN + 1];
        std::memcpy(name_copy, parent->name, NAME_LEN + 1);
        t->name = name_copy;
    } else {
        t->name = nullptr;
    }

    // Identity
    uint64_t const OWNER_PID = sched::task::process_pid(*parent);
    t->pid = sched::task::get_next_pid();
    t->parent_pid = (parent->parent_pid != 0U) ? parent->parent_pid : OWNER_PID;
    t->type = TaskType::PROCESS;
    t->is_thread = true;
    t->owner_pid = OWNER_PID;
    t->cpu = cpu::current_cpu();

    // Share the parent's pagemap - do NOT create a new one
    t->pagemap = parent->pagemap;
    t->mmap_next.store(parent->mmap_next.load(std::memory_order_relaxed), std::memory_order_relaxed);

    // Thread struct: only fsbase and stack are meaningful; mlibc manages the TLS allocation
    auto* thr = new threading::Thread{};
    thr->magic = 0xDEADBEEF;
    thr->fsbase = tcb_vaddr;
    thr->gsbase = 0;
    thr->stack = user_sp;
    thr->stack_size = 0;
    thr->tls_size = 0;
    thr->tls_base_virt = 0;
    thr->tls_phys_ptr = 0;
    thr->stack_phys_ptr = 0;
    t->thread = thr;

    // Kernel-space PerCpu scratch area for syscall entry.
    // gsbase must also point here: after swapgs on syscall entry, GS_BASE becomes
    // gsbase, so it must be the PerCpu scratch area - not 0.
    auto* per_cpu = new cpu::PerCpu();
    per_cpu->syscall_stack = K_RSP;
    per_cpu->cpu_id = cpu::current_cpu();
    t->context.syscall_kernel_stack = K_RSP;
    t->context.syscall_scratch_area = reinterpret_cast<uint64_t>(per_cpu);
    thr->gsbase = reinterpret_cast<uint64_t>(per_cpu);

    // User-mode interrupt frame: jump straight into __mlibc_enter_thread.
    // sys_prepare_stack pushed [ user_arg, entry ] below userSp (i.e. at userSp and userSp+8).
    // Read them out now so the kernel can set RDI/RSI directly, and advance RSP past them.
    uint64_t entry_va = 0;
    uint64_t user_arg_va = 0;
    {
        // Translate the two words on the prepared stack via the parent pagemap
        uint64_t const PA_ENTRY = mm::virt::translate(parent->pagemap, user_sp);
        if (PA_ENTRY != mm::virt::PADDR_INVALID) {
            entry_va = *reinterpret_cast<uint64_t*>(mm::addr::get_virt_pointer(PA_ENTRY));
        }
        uint64_t const PA_ARG = mm::virt::translate(parent->pagemap, user_sp + 8);
        if (PA_ARG != mm::virt::PADDR_INVALID) {
            user_arg_va = *reinterpret_cast<uint64_t*>(mm::addr::get_virt_pointer(PA_ARG));
        }
    }

    t->context.frame.rip = enter_thread_va;  // __mlibc_enter_thread
    t->context.frame.rsp = user_sp + 16;     // skip the two pushed words
    t->context.frame.cs = 0x23;              // user code segment
    t->context.frame.ss = 0x1b;              // user stack segment
    t->context.frame.flags = 0x202;          // IF=1, reserved=1
    t->context.regs = cpu::GPRegs{};
    t->context.regs.rdi = entry_va;     // arg1: entry function
    t->context.regs.rsi = user_arg_va;  // arg2: user_arg

    // Inherit FDs: share the same File* pointers and bump each refcount
    parent->fd_table.for_each([&](uint64_t key, void* val) {
        if (val != nullptr) {
            reinterpret_cast<ker::vfs::File*>(val)->refcount.fetch_add(1, std::memory_order_relaxed);
        }
        (void)t->fd_table.insert(key, val);
    });
    // Copy fixed per-process storage inherited by userspace threads.
    t->fd_cloexec = parent->fd_cloexec;
    t->cwd = parent->cwd;
    t->root = parent->root;
    t->exe_path = parent->exe_path;
    t->uid = parent->uid;
    t->gid = parent->gid;
    t->euid = parent->euid;
    t->egid = parent->egid;
    t->suid = parent->suid;
    t->sgid = parent->sgid;
    t->umask = parent->umask;
    (void)t->supplementary_groups.clone_from(parent->supplementary_groups);
    t->session_id = parent->session_id;
    t->pgid = parent->pgid;
    t->controlling_tty = parent->controlling_tty;
    t->wki_target_hostname = parent->wki_target_hostname;
    t->wki_target_flags = parent->wki_target_flags;
    t->wki_submitter_hostname = parent->wki_submitter_hostname;
    t->wki_remote_pid = parent->wki_remote_pid;
    (void)t->wki_vfs_rules.clone_from(parent->wki_vfs_rules);
    t->wki_skip_legacy_placement = false;

    // ELF buffer: threads have no separate ELF
    t->elf_buffer = nullptr;
    t->elf_buffer_size = 0;

    // Scheduling defaults
    t->has_run = false;
    t->has_exited = false;
    t->exit_notify_ready.store(false, std::memory_order_relaxed);
    t->exit_status = 0;
    t->waited_on = false;
    t->zombie_resources_reclaiming.store(false, std::memory_order_relaxed);
    t->zombie_resources_reclaimed.store(false, std::memory_order_relaxed);
    t->deferred_task_switch = false;
    t->yield_switch = false;
    t->set_voluntary_blocked(false);
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
    t->sigsuspend_saved_mask = 0;
    t->sigsuspend_active = false;
    t->in_signal_handler = false;
    t->do_sigreturn = false;
    t->sig_handlers = parent->sig_handlers;

    // Time accounting
    t->start_time_us = 0;
    t->user_time_us = 0;
    t->system_time_us = 0;
    t->syscall_account_start_us = 0;
    t->precharged_syscall_time_us = 0;
    t->itimer_real_expire_us = 0;
    t->itimer_real_interval_us = 0;

    return t;
}

Task* Task::create_kernel_thread(const char* name, void (*entry_func)()) {
    if (entry_func == nullptr) {
        dbg::log("create_kernel_thread: null entry for '%s'", name != nullptr ? name : "?");
        return nullptr;
    }

    auto stack_base = reinterpret_cast<uint64_t>(mm::phys::page_alloc(ker::mod::mm::KERNEL_STACK_SIZE));
    if (stack_base == 0) {
        dbg::log("create_kernel_thread: OOM allocating kernel stack for '%s'", name);
        return nullptr;
    }
    uint64_t const KERNEL_RSP = stack_base + ker::mod::mm::KERNEL_STACK_SIZE;

    auto* task = new Task(name, 0, KERNEL_RSP, TaskType::DAEMON);
    task->kthread_entry = entry_func;
    task->context.frame.rip = reinterpret_cast<uint64_t>(wos_kernel_thread_trampoline);
    task->context.regs.rdi = reinterpret_cast<uint64_t>(entry_func);
    return task;
}

void Task::load_context(cpu::GPRegs* gpr) { this->context.regs = *gpr; }

void Task::save_context(cpu::GPRegs* gpr) const {
    cpu_set_msr(IA32_KERNEL_GS_BASE, this->context.syscall_scratch_area);
    *gpr = context.regs;
}

auto get_next_pid() -> uint64_t {
    static uint64_t next_pid = 1;  // Start from 1 to avoid confusion with kernel tasks
    return next_pid++;
}

}  // namespace ker::mod::sched::task
