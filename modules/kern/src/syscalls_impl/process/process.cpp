#include "process.hpp"

#include <bits/posix/posix_string.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <utility>

#include "abi/callnums/process.h"
#include "abi/ptrace.hpp"
#include "net/wki/remote_compute.hpp"
#include "net/wki/wki.hpp"
#include "platform/asm/cpu.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/debug/ptrace.hpp"
#include "platform/interrupt/gdt.hpp"
#include "platform/ktime/ktime.hpp"
#include "platform/mm/mm.hpp"
#include "platform/mm/phys.hpp"
#include "platform/mm/virt.hpp"
#include "platform/perf/perf_events.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "platform/sched/threading.hpp"
#include "syscalls_impl/process/exec.hpp"
#include "syscalls_impl/process/exit.hpp"
#include "syscalls_impl/process/getpid.hpp"
#include "syscalls_impl/process/getppid.hpp"
#include "syscalls_impl/process/waitpid.hpp"
#include "syscalls_impl/shm/shm.hpp"
#include "util/hostname.hpp"
#include "util/smallvec.hpp"
#include "vfs/file.hpp"
#include "vfs/vfs.hpp"

// Signal constants (matching Linux ABI from abi-bits/signal.h)
static constexpr int WOS_SIGKILL = 9;
static constexpr int WOS_SIGSTOP = 19;
static constexpr uint64_t WOS_SA_RESTORER = 0x04000000;
static constexpr int WOS_PRIO_PROCESS = 0;

namespace ker::syscall::process {

namespace {
using fork_log = ker::mod::dbg::logger<"fork">;
using process_log = ker::mod::dbg::logger<"process">;

inline auto signal_handler_slot(ker::mod::sched::task::Task& task, size_t index) -> ker::mod::sched::task::Task::SigHandler& {
    // Signal numbers are range-checked before conversion to this zero-based index.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return task.sig_handlers[index];
}

inline auto mutable_hostname_char(ker::mod::sched::task::Task::HostnameBuffer& hostname, size_t index) -> char& {
    // Hostname setters validate len against hostname.size() before indexing.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return hostname[index];
}

inline auto local_wki_hostname() -> const char* { return std::begin(ker::net::wki::g_wki.local_hostname); }

inline auto is_ptrace_launch_stop_signal(int sig) -> bool { return sig == WOS_SIGSTOP; }

inline auto is_live_signal_target(const ker::mod::sched::task::Task& task) -> bool {
    return task.state.load(std::memory_order_acquire) == ker::mod::sched::task::TaskState::ACTIVE && !task.has_exited;
}

auto wos_proc_getgroups(size_t size, uint32_t* list) -> uint64_t {
    constexpr size_t GETGROUPS_SIZE_MAX = 0x7FFFFFFFU;

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (size > GETGROUPS_SIZE_MAX) {
        return static_cast<uint64_t>(-EINVAL);
    }

    size_t const COUNT = task->supplementary_groups.size();
    if (size == 0) {
        return COUNT;
    }
    if (list == nullptr) {
        return static_cast<uint64_t>(-EFAULT);
    }
    if (size < COUNT) {
        return static_cast<uint64_t>(-EINVAL);
    }

    for (size_t i = 0; i < COUNT; ++i) {
        list[i] = task->supplementary_groups.at(i);
    }
    return COUNT;
}

auto wos_proc_setgroups(size_t size, const uint32_t* list) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (task->euid != 0) {
        return static_cast<uint64_t>(-EPERM);
    }
    if (size > ker::mod::sched::task::Task::SUPPLEMENTARY_GROUPS_MAX) {
        return static_cast<uint64_t>(-EINVAL);
    }
    if (size > 0 && list == nullptr) {
        return static_cast<uint64_t>(-EFAULT);
    }

    ker::util::SmallVec<uint32_t, ker::mod::sched::task::Task::SUPPLEMENTARY_GROUPS_MAX> groups;
    for (size_t i = 0; i < size; ++i) {
        if (!groups.push_back(list[i])) {
            return static_cast<uint64_t>(-ENOMEM);
        }
    }

    task->supplementary_groups = std::move(groups);
    return 0;
}

inline void record_local_proc_event(ker::mod::sched::task::Task* task, ker::mod::perf::WkiPerfLocalProcOp op,
                                    ker::mod::perf::WkiPerfPhase phase, uint32_t correlation, int32_t status, uint32_t aux,
                                    uint64_t callsite) {
    if (task == nullptr) {
        return;
    }

    ker::mod::perf::record_wki_event(static_cast<uint32_t>(ker::mod::cpu::current_cpu()), task->pid,
                                     ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op), phase, 0, 0, correlation, status,
                                     aux, callsite);
}

inline auto clamp_perf_aux(uint64_t value) -> uint32_t { return value > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(value); }

struct LocalProcStage {
    uint32_t correlation;
    uint64_t started_us;
};

auto begin_local_proc_stage(ker::mod::sched::task::Task* task, ker::mod::perf::WkiPerfLocalProcOp op, uint32_t aux, uint64_t callsite)
    -> LocalProcStage {
    LocalProcStage const STAGE = {
        .correlation = ker::mod::perf::next_wki_trace_correlation(),
        .started_us = ker::mod::time::get_us(),
    };
    record_local_proc_event(task, op, ker::mod::perf::WkiPerfPhase::BEGIN, STAGE.correlation, 0, aux, callsite);
    return STAGE;
}

auto end_local_proc_stage(ker::mod::sched::task::Task* task, ker::mod::perf::WkiPerfLocalProcOp op, const LocalProcStage& stage,
                          int32_t status, uint64_t bytes, uint64_t callsite) -> uint32_t {
    uint32_t const ELAPSED_US = clamp_perf_aux(ker::mod::time::get_us() - stage.started_us);
    record_local_proc_event(task, op, ker::mod::perf::WkiPerfPhase::END, stage.correlation, status, ELAPSED_US, callsite);
    ker::mod::perf::record_wki_summary(ker::mod::perf::WkiPerfScope::LOCAL_PROC, static_cast<uint8_t>(op), 0, 0, status, ELAPSED_US, true,
                                       0, bytes);
    return ELAPSED_US;
}

inline void log_unmapped_child_resume_state(const ker::mod::sched::task::Task* parent, const ker::mod::sched::task::Task* child,
                                            uint64_t saved_rip, uint64_t saved_rsp, uint64_t saved_flags) {
    if (child == nullptr || child->pagemap == nullptr) {
        return;
    }

    const uint64_t RIP_PHYS = ker::mod::mm::virt::translate(child->pagemap, child->context.frame.rip);
    const uint64_t RSP_PHYS = ker::mod::mm::virt::translate(child->pagemap, child->context.frame.rsp);
    const bool RIP_BAD =
        child->context.frame.rip == 0 || child->context.frame.rip >= 0x0000800000000000ULL || RIP_PHYS == ker::mod::mm::virt::PADDR_INVALID;
    const bool RSP_BAD =
        child->context.frame.rsp == 0 || child->context.frame.rsp >= 0x0000800000000000ULL || RSP_PHYS == ker::mod::mm::virt::PADDR_INVALID;
    if (!RIP_BAD && !RSP_BAD) {
        return;
    }

    fork_log::warn(
        "fork child resume anomaly: parent=%lu child=%lu pagemap=%p rip=0x%llx rip_phys=0x%llx rsp=0x%llx rsp_phys=0x%llx gs_rip=0x%llx "
        "gs_rsp=0x%llx gs_rflags=0x%llx parent_pagemap=%p",
        parent != nullptr ? parent->pid : 0, child->pid, static_cast<void*>(child->pagemap),
        static_cast<unsigned long long>(child->context.frame.rip),
        static_cast<unsigned long long>((RIP_PHYS == ker::mod::mm::virt::PADDR_INVALID) ? 0 : RIP_PHYS),
        static_cast<unsigned long long>(child->context.frame.rsp),
        static_cast<unsigned long long>((RSP_PHYS == ker::mod::mm::virt::PADDR_INVALID) ? 0 : RSP_PHYS),
        static_cast<unsigned long long>(saved_rip), static_cast<unsigned long long>(saved_rsp),
        static_cast<unsigned long long>(saved_flags), parent != nullptr ? static_cast<void*>(parent->pagemap) : nullptr);
}

auto wos_proc_fork(ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    using namespace ker::mod;

    auto* parent = sched::get_current_task();
    if (parent == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    LocalProcStage const FORK_STAGE = begin_local_proc_stage(parent, perf::WkiPerfLocalProcOp::FORK, 0, WOS_PERF_CALLSITE());
    auto finish_fork = [&](int64_t result, uint64_t bytes = 0) -> uint64_t {
        int32_t const STATUS = result < 0 ? static_cast<int32_t>(result) : 0;
        end_local_proc_stage(parent, perf::WkiPerfLocalProcOp::FORK, FORK_STAGE, STATUS, bytes, WOS_PERF_CALLSITE());
        return static_cast<uint64_t>(result);
    };

    // Save parent's register context (will be copied to child)
    parent->context.regs = gpr;

    // --- Allocate child kernel stack ---
    auto kernel_stack_base = reinterpret_cast<uint64_t>(mm::phys::page_alloc(ker::mod::mm::KERNEL_STACK_SIZE));
    if (kernel_stack_base == 0) {
        return finish_fork(-ENOMEM);
    }
    uint64_t const KERNEL_RSP = kernel_stack_base + ker::mod::mm::KERNEL_STACK_SIZE;

    // --- Allocate child Task without the ELF-loading constructor ---
    auto* child = new sched::task::Task{};
    if (child == nullptr) {
        mm::phys::page_free(reinterpret_cast<void*>(kernel_stack_base));
        return finish_fork(-ENOMEM);
    }

    // --- Initialize child task fields ---
    // Copy name
    if (parent->name != nullptr) {
        size_t const NAME_LEN = std::strlen(parent->name);
        char* name_copy = new char[NAME_LEN + 1];
        std::memcpy(name_copy, parent->name, NAME_LEN + 1);
        child->name = name_copy;
    }

    child->pid = sched::task::get_next_pid();
    child->parent_pid = parent->pid;
    child->type = sched::task::TaskType::PROCESS;
    child->cpu = cpu::current_cpu();
    child->has_run = false;
    child->exit_status = 0;
    child->has_exited = false;
    child->exit_notify_ready.store(false, std::memory_order_relaxed);
    child->waited_on = false;
    child->deferred_task_switch = false;
    child->yield_switch = false;
    child->voluntary_block = false;
    child->kthread_entry = nullptr;
    child->elf_buffer = nullptr;
    child->elf_buffer_size = 0;
    child->is_elf_buffer_shared = false;
    child->mmap_next.store(parent->mmap_next.load(std::memory_order_relaxed), std::memory_order_relaxed);
    child->waiting_for_pid = 0;
    child->wait_status_phys_addr = 0;

    // EEVDF scheduling fields - start fresh
    child->vruntime = 0;
    child->vdeadline = 0;
    child->sched_weight = parent->sched_weight;
    child->sched_nice = parent->sched_nice;
    child->slice_ns = parent->slice_ns;
    child->slice_used_ns = 0;
    child->heap_index = -1;
    child->sched_queue = sched::task::Task::sched_queue::NONE;
    child->sched_next = nullptr;

    // Copy fixed per-process path storage.
    child->cwd = parent->cwd;
    child->root = parent->root;
    child->exe_path = parent->exe_path;

    // Copy WKI spawn configuration. NOINHERIT applies when a process creates
    // a child: the parent keeps its policy, but the child starts automatic.
    if ((parent->wki_target_flags & sched::task::Task::WKI_TARGET_FLAG_NOINHERIT) != 0) {
        child->wki_target_hostname.front() = '\0';
        child->wki_target_flags = 0;
    } else {
        child->wki_target_hostname = parent->wki_target_hostname;
        child->wki_target_flags = parent->wki_target_flags;
    }
    child->wki_submitter_hostname = parent->wki_submitter_hostname;
    child->wki_remote_pid =
        (child->wki_submitter_hostname.front() != '\0' && std::strcmp(child->wki_submitter_hostname.data(), local_wki_hostname()) != 0)
            ? child->pid
            : 0;
    if (!child->wki_vfs_rules.clone_from(parent->wki_vfs_rules)) {
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(kernel_stack_base));
        delete child;
        return finish_fork(-ENOMEM);
    }
    child->wki_skip_legacy_placement = false;

    // Copy POSIX credentials
    child->uid = parent->uid;
    child->gid = parent->gid;
    child->euid = parent->euid;
    child->egid = parent->egid;
    child->suid = parent->suid;
    child->sgid = parent->sgid;
    child->umask = parent->umask;
    if (!child->supplementary_groups.clone_from(parent->supplementary_groups)) {
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(kernel_stack_base));
        delete child;
        return finish_fork(-ENOMEM);
    }

    // Copy session, process group, and controlling terminal
    child->session_id = parent->session_id;
    child->pgid = (parent->pgid != 0) ? parent->pgid : parent->pid;  // POSIX: pgid must never be 0 for user processes
    child->controlling_tty = parent->controlling_tty;

    // Copy signal dispositions from parent (fork inherits signal handlers)
    child->sig_pending = 0;  // Pending signals are NOT inherited
    child->sig_mask = parent->sig_mask;
    child->in_signal_handler = false;
    child->do_sigreturn = false;
    child->sig_handlers = parent->sig_handlers;

    // --- Create child pagemap with COW ---
    child->pagemap = mm::virt::create_pagemap();
    if (child->pagemap == nullptr) {
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(kernel_stack_base));
        delete child;
        return finish_fork(-ENOMEM);
    }

    // Copy kernel mappings
    mm::virt::copy_kernel_mappings(child);

    // Deep-copy user pages with COW
    if (!mm::virt::deep_copy_user_pagemap_cow(parent->pagemap, child->pagemap)) {
        mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-cow-fail");
        mm::phys::page_free(child->pagemap);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(kernel_stack_base));
        delete child;
        return finish_fork(-ENOMEM);
    }

    if (!ker::syscall::shm::shm_clone_for_fork(parent, child)) {
        mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-shm-clone-fail");
        mm::phys::page_free(child->pagemap);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(kernel_stack_base));
        delete child;
        return finish_fork(-ENOMEM);
    }

    // --- Clone thread metadata ---
    // The child shares the same user-space layout (stack, TLS) via COW.
    // Allocate a Thread struct for the child that mirrors the parent's.
    if (parent->thread != nullptr) {
        auto* child_thread = new sched::threading::Thread();
        if (child_thread == nullptr) {
            ker::syscall::shm::shm_cleanup_for_task(child);
            mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-thread-alloc-fail");
            mm::phys::page_free(child->pagemap);
            delete[] child->name;
            mm::phys::page_free(reinterpret_cast<void*>(kernel_stack_base));
            delete child;
            return finish_fork(-ENOMEM);
        }
        // Copy all fields - virtual addresses are the same (same address space layout via COW)
        *child_thread = *parent->thread;
        // The physical pointers are now shared via COW so the child shouldn't free them
        // on thread destroy - we zero them to prevent double-free
        child_thread->tls_phys_ptr = 0;
        child_thread->stack_phys_ptr = 0;
        child->thread = child_thread;
    } else {
        child->thread = nullptr;
    }

    // --- Set up child context ---
    // Child's kernel stack and per-CPU scratch area
    child->context.syscall_kernel_stack = KERNEL_RSP;

    auto* per_cpu = new cpu::PerCpu();
    per_cpu->syscall_stack = KERNEL_RSP;
    per_cpu->cpu_id = cpu::current_cpu();
    child->context.syscall_scratch_area = reinterpret_cast<uint64_t>(per_cpu);

    // Copy parent's register context - child will resume at the same RIP
    child->context.regs = parent->context.regs;
    child->context.int_no = 0;
    child->context.error_code = 0;

    // Build the child's interrupt frame from the PerCpu scratch area.
    // parent->context.frame is STALE - it was saved during the last timer
    // preemption / context switch, NOT during this syscall.  The real
    // syscall return state lives in the scratch area populated by the
    // syscall entry path in syscall.asm:
    //   gs:0x28 = RCX at entry = user return RIP
    //   gs:0x30 = R11 at entry = user RFLAGS
    //   gs:0x08 = user RSP at entry
    // NOLINTBEGIN(misc-const-correctness)
    uint64_t return_rip = 0;
    uint64_t return_flags = 0;
    uint64_t user_rsp = 0;
    // NOLINTEND(misc-const-correctness)
    {
        asm volatile("movq %%gs:0x28, %0" : "=r"(return_rip));
        asm volatile("movq %%gs:0x30, %0" : "=r"(return_flags));
        asm volatile("movq %%gs:0x08, %0" : "=r"(user_rsp));

        child->context.frame.rip = return_rip;
        child->context.frame.rsp = user_rsp;
        child->context.frame.flags = return_flags;
        child->context.frame.cs = desc::gdt::GDT_USER_CS;
        child->context.frame.ss = desc::gdt::GDT_USER_DS;
        child->context.frame.int_num = 0;
        child->context.frame.err_code = 0;
    }

    log_unmapped_child_resume_state(parent, child, return_rip, user_rsp, return_flags);

    // Child returns 0 from fork
    child->context.regs.rax = 0;

    // Copy entry and ELF metadata pointers
    child->entry = parent->entry;
    child->program_header_addr = parent->program_header_addr;
    child->elf_header_addr = parent->elf_header_addr;
    child->program_header_count = parent->program_header_count;
    child->program_header_ent_size = parent->program_header_ent_size;

    // --- Clone file descriptors ---
    parent->fd_table.for_each([&](uint64_t key, void* val) {
        if (val != nullptr) {
            auto* file = static_cast<ker::vfs::File*>(val);
            file->refcount.fetch_add(1, std::memory_order_relaxed);  // Increment refcount for shared file
            // TODO: Checked insertion, should have a process cleanup system that can handle partially initialized processes if this fails
            // instead of leaking
            (void)child->fd_table.insert(key, file);
        }
    });

    child->fd_cloexec = parent->fd_cloexec;

    // --- Enqueue child ---
    if (!sched::post_task_balanced(child)) {
        // Undo FD refcount increments
        child->fd_table.for_each([](uint64_t /*key*/, void* val) {
            if (val != nullptr) {
                static_cast<ker::vfs::File*>(val)->refcount.fetch_sub(1, std::memory_order_relaxed);
            }
        });

        delete child->thread;
        delete reinterpret_cast<cpu::PerCpu*>(child->context.syscall_scratch_area);
        ker::syscall::shm::shm_cleanup_for_task(child);
        mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-post-task-fail");
        mm::phys::page_free(child->pagemap);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(kernel_stack_base));
        delete child;
        return finish_fork(-ENOMEM);
    }
    record_local_proc_event(child, perf::WkiPerfLocalProcOp::FORK, perf::WkiPerfPhase::POINT, perf::next_wki_trace_correlation(), 0,
                            static_cast<uint32_t>(child->cpu), WOS_PERF_CALLSITE());

    // Return child PID to parent
    return finish_fork(static_cast<int64_t>(child->pid), child->cpu);
}

// --- Signal infrastructure ---

// Userspace sigaction struct layout (must match abi-bits/signal.h)
struct KernelSigaction {
    uint64_t handler;   // sa_handler / sa_sigaction (union, 8 bytes)
    uint64_t flags;     // sa_flags (unsigned long)
    uint64_t restorer;  // sa_restorer (function pointer)
    // sigset_t sa_mask is 128 bytes (unsigned long[16]) but we only use first word
    uint64_t mask;  // First word of sa_mask
    // Remaining 120 bytes of sa_mask are unused padding
};

auto wos_proc_sigaction(int signum, uint64_t act_ptr, uint64_t oldact_ptr) -> uint64_t {
    using namespace ker::mod;

    auto* task = sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    // Signal numbers are 1-based, array is 0-based
    if (signum < 1 || std::cmp_greater(signum, sched::task::Task::MAX_SIGNALS)) {
        return static_cast<uint64_t>(-EINVAL);
    }

    // SIGKILL and SIGSTOP cannot have their handlers changed
    if (signum == WOS_SIGKILL || signum == WOS_SIGSTOP) {
        return static_cast<uint64_t>(-EINVAL);
    }

    auto const IDX = static_cast<unsigned>(signum - 1);

    // Return old handler if requested
    if (oldact_ptr != 0) {
        auto* old = reinterpret_cast<KernelSigaction*>(oldact_ptr);
        const auto& handler = signal_handler_slot(*task, IDX);
        old->handler = handler.handler;
        old->flags = handler.flags;
        old->restorer = handler.restorer;
        old->mask = handler.mask;
    }

    // Set new handler if provided
    if (act_ptr != 0) {
        const auto* act = reinterpret_cast<const KernelSigaction*>(act_ptr);
        auto& handler = signal_handler_slot(*task, IDX);
        handler.handler = act->handler;
        handler.flags = act->flags;
        handler.mask = act->mask;
        // Store restorer if SA_RESTORER flag is set
        if ((act->flags & WOS_SA_RESTORER) != 0U) {
            handler.restorer = act->restorer;
        }
    }

    return 0;
}

auto wos_proc_sigprocmask(int how, uint64_t set_ptr, uint64_t oldset_ptr) -> uint64_t {
    using namespace ker::mod;

    auto* task = sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    // Return old mask if requested (sigset_t first word)
    if (oldset_ptr != 0) {
        auto* oldset = reinterpret_cast<uint64_t*>(oldset_ptr);
        *oldset = task->sig_mask;
    }

    // Apply new mask if provided
    if (set_ptr != 0) {
        const auto* setp = reinterpret_cast<const uint64_t*>(set_ptr);
        uint64_t set = *setp;

        // SIGKILL and SIGSTOP can never be blocked
        uint64_t const UNBLOCKABLE = (1ULL << (WOS_SIGKILL - 1)) | (1ULL << (WOS_SIGSTOP - 1));
        set &= ~UNBLOCKABLE;

        switch (how) {
            case 0:  // SIG_BLOCK
                task->sig_mask |= set;
                break;
            case 1:  // SIG_UNBLOCK
                task->sig_mask &= ~set;
                break;
            case 2:  // SIG_SETMASK
                task->sig_mask = set;
                break;
            default:
                return static_cast<uint64_t>(-EINVAL);
        }
    }

    return 0;
}

auto wos_proc_kill(int64_t pid, int sig) -> uint64_t {
    using namespace ker::mod;

    if (sig < 0 || std::cmp_greater(sig, sched::task::Task::MAX_SIGNALS)) {
        return static_cast<uint64_t>(-EINVAL);
    }

    if (pid == 0) {
        // pid==0: target all live processes in caller's process group.
        auto* self = sched::get_current_task();
        if (self == nullptr) {
            return static_cast<uint64_t>(-ESRCH);
        }
        uint64_t const PGRP = (self->pgid != 0) ? self->pgid : self->pid;
        return sched::signal_process_group(PGRP, sig) != 0 ? 0 : static_cast<uint64_t>(-ESRCH);
    }
    if (pid < 0) {
        // pid < -1: send signal to process group -pid
        // pid == -1: send to all processes (simplified: send to caller's pgrp)
        uint64_t pgrp = 0;
        if (pid == -1) {
            auto* self = sched::get_current_task();
            if (self == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            pgrp = (self->pgid != 0) ? self->pgid : self->pid;
        } else {
            pgrp = static_cast<uint64_t>(-(pid + 1)) + 1;
        }
        return sched::signal_process_group(pgrp, sig) != 0 ? 0 : static_cast<uint64_t>(-ESRCH);
    }

    auto* target = sched::find_task_by_pid_safe(static_cast<uint64_t>(pid));
    if (target == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (!is_live_signal_target(*target)) {
        target->release();
        return static_cast<uint64_t>(-ESRCH);
    }
    if (sig == 0) {
        target->release();
        return 0;
    }

    // Forward interrupt/termination signals to the remote node if target is a
    // WKI proxy task. When forwarding succeeds, the remote task owns signal
    // termination semantics; do not also queue the signal locally on the proxy
    // task or execve()-proxy handoff can be interrupted by a duplicate fatal
    // signal before TASK_COMPLETE finalizes it.
    bool const FORWARDED = ker::net::wki::wki_proxy_task_forward_signal(target, sig);
    if (!FORWARDED) {
        if (is_ptrace_launch_stop_signal(sig) && ker::mod::debug::ptrace::report_signal_stop(*target, static_cast<uint32_t>(sig))) {
            target->release();
            return 0;
        }

        // Set the signal pending bit (signal N is bit N-1)
        target->sig_pending |= (1ULL << (sig - 1));

        // Ensure blocked tasks become runnable so pending signals are delivered promptly.
        sched::wake_task_for_signal(target);
    }

    target->release();
    return 0;
}

auto wos_proc_setpriority(int which, int64_t who, int prio) -> uint64_t {
    if (which != WOS_PRIO_PROCESS) {
        return static_cast<uint64_t>(-EINVAL);
    }

    auto* self = mod::sched::get_current_task();
    if (self == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    if (who == 0) {
        who = static_cast<int64_t>(self->pid);
    }
    if (who < 0) {
        return static_cast<uint64_t>(-EINVAL);
    }

    auto* target = mod::sched::find_task_by_pid_safe(static_cast<uint64_t>(who));
    if (target == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    if (self->euid != 0 && target->pid != self->pid) {
        target->release();
        return static_cast<uint64_t>(-EPERM);
    }

    mod::sched::set_task_nice(target, prio);
    target->release();
    return 0;
}

auto wos_proc_setwkitarget(const char* hostname, size_t len, uint32_t flags) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    if ((flags & ~ker::mod::sched::task::Task::WKI_TARGET_FLAGS_ALL) != 0) {
        return static_cast<uint64_t>(-EINVAL);
    }
    if ((flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL) != 0 &&
        (flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_REMOTE) != 0) {
        return static_cast<uint64_t>(-EINVAL);
    }
    if (hostname != nullptr && len != 0 && (flags & ker::mod::sched::task::Task::WKI_TARGET_FLAG_LOCAL) != 0) {
        return static_cast<uint64_t>(-EINVAL);
    }

    if (hostname == nullptr || len == 0) {
        task->wki_target_hostname.front() = '\0';
        task->wki_target_flags = flags;
        return 0;
    }

    if (len >= task->wki_target_hostname.size()) {
        return static_cast<uint64_t>(-ENAMETOOLONG);
    }

    std::memcpy(task->wki_target_hostname.data(), hostname, len);
    mutable_hostname_char(task->wki_target_hostname, len) = '\0';
    task->wki_target_flags = flags;
    return 0;
}

auto wos_proc_getwkitarget(char* hostname_out, size_t hostname_out_size, uint32_t* flags_out) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    size_t const LEN = strnlen(task->wki_target_hostname.data(), task->wki_target_hostname.size());
    if (hostname_out != nullptr) {
        if (hostname_out_size == 0 || LEN + 1 > hostname_out_size) {
            return static_cast<uint64_t>(-ENAMETOOLONG);
        }
        std::memcpy(hostname_out, task->wki_target_hostname.data(), LEN + 1);
    }

    if (flags_out != nullptr) {
        *flags_out = task->wki_target_flags;
    }

    return LEN;
}
}  // namespace

auto process(abi::process::procmgmt_ops op, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    switch (op) {
        case abi::process::procmgmt_ops::EXIT:
            wos_proc_exit(static_cast<int>(a2));
            return 0;  // Should not reach here
        case abi::process::procmgmt_ops::EXEC: {
            return wos_proc_exec(reinterpret_cast<const char*>(a2), reinterpret_cast<const char* const*>(a3),
                                 reinterpret_cast<const char* const*>(a4));
        }
        case abi::process::procmgmt_ops::WAITPID: {
            return wos_proc_waitpid(static_cast<int64_t>(a2), reinterpret_cast<int32_t*>(a3), static_cast<int32_t>(a4), a5, gpr);
        }
        case abi::process::procmgmt_ops::GETPID: {
            return wos_proc_getpid();
        }
        case abi::process::procmgmt_ops::GETPPID: {
            return wos_proc_getppid();
        }
        case abi::process::procmgmt_ops::FORK: {
            return wos_proc_fork(gpr);
        }
        case abi::process::procmgmt_ops::SIGACTION: {
            return wos_proc_sigaction(static_cast<int>(a2), a3, a4);
        }
        case abi::process::procmgmt_ops::SIGPROCMASK: {
            return wos_proc_sigprocmask(static_cast<int>(a2), a3, a4);
        }
        case abi::process::procmgmt_ops::KILL: {
            return wos_proc_kill(static_cast<int64_t>(a2), static_cast<int>(a3));
        }
        case abi::process::procmgmt_ops::SIGRETURN: {
            // Signal the asm-level check_pending_signals to restore the saved context
            auto* task = ker::mod::sched::get_current_task();
            if (task != nullptr) {
                task->do_sigreturn = true;
            }
            return 0;
        }

        // --- POSIX credential syscalls ---
        case abi::process::procmgmt_ops::GETUID: {
            auto* task = ker::mod::sched::get_current_task();
            return (task != nullptr) ? task->uid : 0;
        }
        case abi::process::procmgmt_ops::GETEUID: {
            auto* task = ker::mod::sched::get_current_task();
            return (task != nullptr) ? task->euid : 0;
        }
        case abi::process::procmgmt_ops::GETGID: {
            auto* task = ker::mod::sched::get_current_task();
            return (task != nullptr) ? task->gid : 0;
        }
        case abi::process::procmgmt_ops::GETEGID: {
            auto* task = ker::mod::sched::get_current_task();
            return (task != nullptr) ? task->egid : 0;
        }
        case abi::process::procmgmt_ops::GETGROUPS: {
            return wos_proc_getgroups(static_cast<size_t>(a2), reinterpret_cast<uint32_t*>(a3));
        }
        case abi::process::procmgmt_ops::SETUID: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto new_uid = static_cast<uint32_t>(a2);
            if (task->euid == 0) {
                // Privileged: set all three
                task->uid = new_uid;
                task->euid = new_uid;
                task->suid = new_uid;
            } else if (new_uid == task->uid || new_uid == task->suid) {
                task->euid = new_uid;
            } else {
                return static_cast<uint64_t>(-EPERM);
            }
            return 0;
        }
        case abi::process::procmgmt_ops::SETGID: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto new_gid = static_cast<uint32_t>(a2);
            if (task->euid == 0) {
                task->gid = new_gid;
                task->egid = new_gid;
                task->sgid = new_gid;
            } else if (new_gid == task->gid || new_gid == task->sgid) {
                task->egid = new_gid;
            } else {
                return static_cast<uint64_t>(-EPERM);
            }
            return 0;
        }
        case abi::process::procmgmt_ops::SETEUID: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto new_euid = static_cast<uint32_t>(a2);
            if (task->euid == 0 || new_euid == task->uid || new_euid == task->suid) {
                task->euid = new_euid;
                return 0;
            }
            return static_cast<uint64_t>(-EPERM);
        }
        case abi::process::procmgmt_ops::SETEGID: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto new_egid = static_cast<uint32_t>(a2);
            if (task->euid == 0 || new_egid == task->gid || new_egid == task->sgid) {
                task->egid = new_egid;
                return 0;
            }
            return static_cast<uint64_t>(-EPERM);
        }
        case abi::process::procmgmt_ops::SETGROUPS: {
            return wos_proc_setgroups(static_cast<size_t>(a2), reinterpret_cast<const uint32_t*>(a3));
        }
        case abi::process::procmgmt_ops::GETUMASK: {
            auto* task = ker::mod::sched::get_current_task();
            return (task != nullptr) ? task->umask : 022;
        }
        case abi::process::procmgmt_ops::SETUMASK: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto old_umask = task->umask;
            task->umask = static_cast<uint32_t>(a2) & 0777;
            return old_umask;
        }

        case abi::process::procmgmt_ops::SETSID: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            // POSIX: setsid fails if the caller is already a process group leader
            if (task->pgid == task->pid && task->session_id != 0) {
                return static_cast<uint64_t>(-EPERM);
            }
            task->session_id = task->pid;
            task->pgid = task->pid;
            task->controlling_tty = -1;  // POSIX: setsid detaches from controlling terminal
            return task->pid;
        }
        case abi::process::procmgmt_ops::GETSID: {
            auto pid = static_cast<int64_t>(a2);
            if (pid == 0) {
                auto* task = ker::mod::sched::get_current_task();
                if (task == nullptr) {
                    return static_cast<uint64_t>(-ESRCH);
                }
                return task->session_id;
            }
            // Look up another task by pid
            auto* target = ker::mod::sched::find_task_by_pid_safe(static_cast<uint64_t>(pid));
            if (target == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto sid = target->session_id;
            target->release();
            return sid;
        }
        case abi::process::procmgmt_ops::SETPGID: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto pid = a2;
            auto new_pgid = a3;
            // pid==0 means current process
            if (pid == 0) {
                pid = task->pid;
            }
            // pgid==0 means use pid as pgid
            if (new_pgid == 0) {
                new_pgid = pid;
            }
            if (pid == task->pid) {
                task->pgid = new_pgid;
                return 0;
            }
            // Setting pgid for another process (must be a child, same session)
            auto* target = ker::mod::sched::find_task_by_pid_safe(pid);
            if (target == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (target->parent_pid != task->pid || target->session_id != task->session_id) {
                target->release();
                return static_cast<uint64_t>(-EPERM);
            }
            target->pgid = new_pgid;
            target->release();
            return 0;
        }
        case abi::process::procmgmt_ops::GETPGID: {
            auto pid = static_cast<int64_t>(a2);
            if (pid == 0) {
                auto* task = ker::mod::sched::get_current_task();
                if (task == nullptr) {
                    return static_cast<uint64_t>(-ESRCH);
                }
                return task->pgid;
            }
            auto* target = ker::mod::sched::find_task_by_pid_safe(static_cast<uint64_t>(pid));
            if (target == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto pgid = target->pgid;
            target->release();
            return pgid;
        }
        case abi::process::procmgmt_ops::EXECVE: {
            // POSIX replace-process execve
            return wos_proc_execve(reinterpret_cast<const char*>(a2), reinterpret_cast<const char* const*>(a3),
                                   reinterpret_cast<const char* const*>(a4), gpr);
        }
        case abi::process::procmgmt_ops::GETHOSTNAME: {
            auto* buf = reinterpret_cast<char*>(a2);
            auto bufsize = static_cast<size_t>(a3);
            if (buf == nullptr) {
                return static_cast<uint64_t>(-EFAULT);
            }
            const char* name = ker::util::hostname::get();
            size_t const LEN = std::strlen(name);
            if (LEN + 1 > bufsize) {
                return static_cast<uint64_t>(-ENAMETOOLONG);
            }
            std::memcpy(buf, name, LEN + 1);
            return 0;
        }
        case abi::process::procmgmt_ops::SETHOSTNAME: {
            const auto* name = reinterpret_cast<const char*>(a2);
            auto len = static_cast<size_t>(a3);
            if (name == nullptr) {
                return static_cast<uint64_t>(-EFAULT);
            }
            int const R = ker::util::hostname::set(name, len);
            return (R < 0) ? static_cast<uint64_t>(R) : 0;
        }
        case abi::process::procmgmt_ops::SETPRIORITY: {
            return wos_proc_setpriority(static_cast<int>(a2), static_cast<int64_t>(a3), static_cast<int>(a4));
        }
        case abi::process::procmgmt_ops::SETWKITARGET: {
            return wos_proc_setwkitarget(reinterpret_cast<const char*>(a2), static_cast<size_t>(a3), static_cast<uint32_t>(a4));
        }
        case abi::process::procmgmt_ops::GETWKITARGET: {
            return wos_proc_getwkitarget(reinterpret_cast<char*>(a2), static_cast<size_t>(a3), reinterpret_cast<uint32_t*>(a4));
        }
        case abi::process::procmgmt_ops::PTRACE: {
            return ker::mod::debug::ptrace::sys_ptrace(static_cast<abi::ptrace::request>(a2), a3, a4, a5, gpr);
        }

        default:
            process_log::warn("unknown op %llu", static_cast<unsigned long long>(op));
            return static_cast<uint64_t>(ENOSYS);
    }
}
}  // namespace ker::syscall::process
