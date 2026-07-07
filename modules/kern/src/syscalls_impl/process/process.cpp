#include "process.hpp"

#include <bits/posix/posix_string.h>

#include <array>
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
#include "platform/mm/paging.hpp"
#include "platform/mm/phys.hpp"
#include "platform/mm/virt.hpp"
#include "platform/perf/perf_events.hpp"
#include "platform/power/power.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "platform/sched/threading.hpp"
#include "platform/sys/context_switch.hpp"
#include "platform/sys/signal.hpp"
#include "platform/sys/usercopy.hpp"
#include "release.hpp"
#include "syscalls_impl/process/exec.hpp"
#include "syscalls_impl/process/exit.hpp"
#include "syscalls_impl/process/getpid.hpp"
#include "syscalls_impl/process/getppid.hpp"
#include "syscalls_impl/process/waitpid.hpp"
#include "syscalls_impl/shm/shm.hpp"
#include "syscalls_impl/vmem/sys_vmem.hpp"
#include "util/hostname.hpp"
#include "util/smallvec.hpp"
#include "vfs/file.hpp"
#include "vfs/vfs.hpp"

// Signal constants (matching Linux ABI from abi-bits/signal.h)
static constexpr int WOS_SIGKILL = 9;
static constexpr int WOS_SIGSTOP = 19;
static constexpr uint64_t WOS_SA_RESTORER = 0x04000000;
static constexpr uint64_t WOS_CLONE_VM = 0x00000100;
static constexpr uint64_t WOS_CLONE_FS = 0x00000200;
static constexpr uint64_t WOS_CLONE_FILES = 0x00000400;
static constexpr uint64_t WOS_CLONE_UNTRACED = 0x00800000;
static constexpr uint64_t WOS_PERSONALITY_QUERY = 0xffffffff;
static constexpr uint64_t WOS_PERSONALITY_QUERY_ALL_BITS = ~0ULL;
static constexpr int WOS_PRIO_PROCESS = 0;
static constexpr int WOS_PRIO_KERNEL_ENCODE_BIAS = 20;
static constexpr int WOS_PR_SET_PDEATHSIG = 1;
static constexpr int WOS_PR_GET_PDEATHSIG = 2;
static constexpr int WOS_PR_GET_DUMPABLE = 3;
static constexpr int WOS_PR_SET_DUMPABLE = 4;
static constexpr int WOS_PR_SET_NAME = 15;
static constexpr int WOS_PR_GET_NAME = 16;
static constexpr int WOS_PR_SET_PTRACER = 0x59616d61;
static constexpr int WOS_PR_SET_VMA = 0x53564d41;
static constexpr int WOS_PR_SET_VMA_ANON_NAME = 0;
static constexpr int WOS_ARCH_SET_GS = 0x1001;
static constexpr int WOS_ARCH_SET_FS = 0x1002;
static constexpr int WOS_ARCH_GET_FS = 0x1003;
static constexpr int WOS_ARCH_GET_GS = 0x1004;
static constexpr uint32_t WOS_SS_ONSTACK = 1;
static constexpr uint32_t WOS_SS_DISABLE = 2;
static constexpr size_t WOS_MINSIGSTKSZ = 2048;

namespace ker::syscall::process {

namespace {
using fork_log = ker::mod::dbg::logger<"fork">;
using process_log = ker::mod::dbg::logger<"process">;
constexpr uint32_t FORK_GC_NO_PROGRESS_YIELD_LIMIT = 64;
constexpr uint64_t FORK_GC_FREE_PAGE_LOW_WATERMARK_DIVISOR = 32;
constexpr uint64_t FORK_GC_FREE_PAGE_LOW_WATERMARK_MIN = 4096;

#ifdef WOS_SELFTEST
std::atomic<bool> g_process_selftest_force_fd_clone_insert_failure{false};
#endif

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

auto process_pid_for_task(const ker::mod::sched::task::Task& task) -> uint64_t { return ker::mod::sched::task::process_pid(task); }

void release_cloned_fd_table_refs(ker::mod::sched::task::Task* task) {
    if (task == nullptr) {
        return;
    }
    task->fd_table.for_each([](uint64_t /*key*/, void* val) {
        if (val != nullptr) {
            static_cast<ker::vfs::File*>(val)->refcount.fetch_sub(1, std::memory_order_relaxed);
        }
    });
}

auto clone_fd_table_shared_checked(ker::mod::sched::task::Task* parent, ker::mod::sched::task::Task* child) -> bool {
    if (parent == nullptr || child == nullptr) {
        return false;
    }

    bool ok = true;
    uint64_t const IRQF = parent->fd_table_lock.lock_irqsave();
    parent->fd_table.for_each([&](uint64_t key, void* val) {
        if (!ok || val == nullptr) {
            return;
        }

        auto* file = static_cast<ker::vfs::File*>(val);
        file->refcount.fetch_add(1, std::memory_order_relaxed);
#ifdef WOS_SELFTEST
        if (g_process_selftest_force_fd_clone_insert_failure.load(std::memory_order_relaxed)) {
            file->refcount.fetch_sub(1, std::memory_order_relaxed);
            ok = false;
            return;
        }
#endif
        if (!child->fd_table.insert(key, file)) {
            file->refcount.fetch_sub(1, std::memory_order_relaxed);
            ok = false;
        }
    });
    parent->fd_table_lock.unlock_irqrestore(IRQF);
    return ok;
}

auto find_process_leader_safe(uint64_t pid) -> ker::mod::sched::task::Task* {
    auto* task = ker::mod::sched::find_task_by_pid_safe(pid);
    if (task == nullptr) {
        return nullptr;
    }

    uint64_t const PROCESS_PID = process_pid_for_task(*task);
    if (PROCESS_PID == task->pid) {
        return task;
    }

    task->release();
    return ker::mod::sched::find_task_by_pid_safe(PROCESS_PID);
}

void update_thread_group_job_control(uint64_t process_pid, uint64_t session_id, uint64_t pgid, bool detach_tty) {
    uint32_t const TASK_COUNT = ker::mod::sched::get_active_task_count();
    for (uint32_t i = 0; i < TASK_COUNT; ++i) {
        auto* task = ker::mod::sched::get_active_task_at_safe(i);
        if (task == nullptr) {
            continue;
        }
        if (!ker::mod::sched::task::same_thread_group(*task, process_pid)) {
            task->release();
            continue;
        }
        task->session_id = session_id;
        task->pgid = pgid;
        if (detach_tty) {
            task->controlling_tty = -1;
        }
        task->release();
    }
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

    auto const LIST_ADDR = reinterpret_cast<uint64_t>(list);
    for (size_t i = 0; i < COUNT; ++i) {
        uint32_t const GROUP = task->supplementary_groups.at(i);
        if (!ker::mod::sys::usercopy::copy_value_to_task(*task, LIST_ADDR + (i * sizeof(uint32_t)), GROUP)) {
            return static_cast<uint64_t>(-EFAULT);
        }
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
    auto const LIST_ADDR = reinterpret_cast<uint64_t>(list);
    for (size_t i = 0; i < size; ++i) {
        uint32_t group = 0;
        if (!ker::mod::sys::usercopy::copy_value_from_task(*task, LIST_ADDR + (i * sizeof(uint32_t)), group)) {
            return static_cast<uint64_t>(-EFAULT);
        }
        if (!groups.push_back(group)) {
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

auto alloc_fork_kernel_stack_with_reclaim() -> uint64_t {
    return reinterpret_cast<uint64_t>(ker::mod::mm::phys::page_alloc_with_reclaim(ker::mod::mm::KERNEL_STACK_SIZE, "fork_kstack"));
}

auto fork_free_page_low_watermark() -> uint64_t {
    uint64_t const TOTAL_PAGES = ker::mod::mm::phys::get_total_mem_bytes() / ker::mod::mm::paging::PAGE_SIZE;
    uint64_t const SCALED_WATERMARK = TOTAL_PAGES / FORK_GC_FREE_PAGE_LOW_WATERMARK_DIVISOR;
    return SCALED_WATERMARK > FORK_GC_FREE_PAGE_LOW_WATERMARK_MIN ? SCALED_WATERMARK : FORK_GC_FREE_PAGE_LOW_WATERMARK_MIN;
}

auto fork_should_reclaim_for_pressure(uint64_t free_pages) -> bool { return free_pages < fork_free_page_low_watermark(); }

auto fork_pressure_has_emergency_headroom(uint64_t free_pages) -> bool { return free_pages >= fork_free_page_low_watermark(); }

auto throttle_fork_for_reclaim_pressure(uint64_t callsite) -> bool {
    if (!ker::mod::sched::has_run_queues() || ker::mod::sched::preempt_count() != 0 || !ker::mod::sched::interrupts_enabled()) {
        return true;
    }

    uint32_t no_progress_yields = 0;
    for (;;) {
        uint64_t const FREE_PAGES = ker::mod::mm::phys::get_free_mem_pages();
        if (!fork_should_reclaim_for_pressure(FREE_PAGES)) {
            return true;
        }

        if (ker::mod::sched::reclaim_memory_pressure() == 0) {
            ker::mod::sched::request_gc_memory_pressure();
            ker::mod::sched::kern_yield_impl(callsite);
            ++no_progress_yields;
            if (no_progress_yields >= FORK_GC_NO_PROGRESS_YIELD_LIMIT) {
                return fork_pressure_has_emergency_headroom(ker::mod::mm::phys::get_free_mem_pages());
            }
        } else {
            no_progress_yields = 0;
        }
    }
}

void snapshot_fpu_state_for_fork(ker::mod::sched::task::Task* parent, ker::mod::sched::task::Task* child) {
    ker::mod::sys::context_switch::save_fpu_state(parent);
    std::memcpy(child->fx_state.aligned(), parent->fx_state.aligned(), ker::mod::sched::task::FxState::XSAVE_AREA_SIZE);
    child->fx_state.saved = parent->fx_state.saved;
    child->fx_state.live_saved = false;
    child->fx_state.initialized = parent->fx_state.initialized;
}

auto wos_proc_fork(ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    using namespace ker::mod;

    auto* parent = sched::get_current_task();
    if (parent == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (power::shutdown_in_progress()) {
        return static_cast<uint64_t>(-ESHUTDOWN);
    }

    LocalProcStage const FORK_STAGE = begin_local_proc_stage(parent, perf::WkiPerfLocalProcOp::FORK, 0, WOS_PERF_CALLSITE());
    auto finish_fork = [&](int64_t result, uint64_t bytes = 0) -> uint64_t {
        int32_t const STATUS = result < 0 ? static_cast<int32_t>(result) : 0;
        end_local_proc_stage(parent, perf::WkiPerfLocalProcOp::FORK, FORK_STAGE, STATUS, bytes, WOS_PERF_CALLSITE());
        return static_cast<uint64_t>(result);
    };

    if (!throttle_fork_for_reclaim_pressure(WOS_PERF_CALLSITE())) {
        return finish_fork(-ENOMEM);
    }

    // --- Allocate child kernel stack ---
    auto const KERNEL_STACK_BASE = alloc_fork_kernel_stack_with_reclaim();
    if (KERNEL_STACK_BASE == 0) {
        return finish_fork(-ENOMEM);
    }
    uint64_t const KERNEL_RSP = KERNEL_STACK_BASE + ker::mod::mm::KERNEL_STACK_SIZE;

    // Save parent's register context after any pressure-reclaim yield; this
    // snapshot is copied into the child as the fork return frame.
    parent->context.regs = gpr;

    // --- Allocate child Task without the ELF-loading constructor ---
    auto* child = new sched::task::Task{};
    if (child == nullptr) {
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        return finish_fork(-ENOMEM);
    }
    snapshot_fpu_state_for_fork(parent, child);

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
    child->exit_in_progress = false;
    child->has_exited = false;
    child->exit_notify_ready.store(false, std::memory_order_relaxed);
    sched::task::task_clear_waited_on(*child);
    child->zombie_resources_reclaiming.store(false, std::memory_order_relaxed);
    child->zombie_resources_reclaimed.store(false, std::memory_order_relaxed);
    child->deferred_task_switch = false;
    child->yield_switch = false;
    child->set_voluntary_blocked(false);
    child->kthread_entry = nullptr;
    child->elf_buffer = nullptr;
    child->elf_buffer_size = 0;
    child->is_elf_buffer_shared = false;
    child->mmap_next.store(parent->mmap_next.load(std::memory_order_relaxed), std::memory_order_relaxed);
    child->waiting_for_pid = 0;
    child->wait_options = 0;
    child->wait_status_phys_addr = 0;
    child->jobctl_stopped.store(false, std::memory_order_relaxed);
    child->jobctl_stop_pending.store(false, std::memory_order_relaxed);
    child->jobctl_stop_signal = 0;

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
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ENOMEM);
    }
    if (!sched::task::clone_lazy_vmem_ranges(*child, *parent)) {
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
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
    child->personality = parent->personality;
    if (!child->supplementary_groups.clone_from(parent->supplementary_groups)) {
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ENOMEM);
    }

    // Copy session, process group, and controlling terminal
    child->session_id = parent->session_id;
    child->pgid = (parent->pgid != 0) ? parent->pgid : parent->pid;  // POSIX: pgid must never be 0 for user processes
    child->controlling_tty = parent->controlling_tty;

    // Copy signal dispositions from parent (fork inherits signal handlers)
    child->signal_pending_store(0, std::memory_order_relaxed);  // Pending signals are NOT inherited
    child->signal_mask_store(parent->signal_mask_bits(), std::memory_order_relaxed);
    child->sigsuspend_saved_mask = 0;
    child->sigaltstack_sp = parent->sigaltstack_sp;
    child->sigaltstack_size = parent->sigaltstack_size;
    child->sigaltstack_flags = parent->sigaltstack_flags;
    child->parent_death_signal = 0;
    child->dumpable = parent->dumpable;
    child->sigsuspend_active = false;
    child->in_signal_handler = false;
    child->do_sigreturn = false;
    child->sig_handlers = parent->sig_handlers;

    // --- Create child pagemap with COW ---
    child->pagemap = mm::virt::create_pagemap();
    if (child->pagemap == nullptr) {
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
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
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ENOMEM);
    }

    if (!ker::syscall::shm::shm_clone_for_fork(parent, child)) {
        mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-shm-clone-fail");
        mm::phys::page_free(child->pagemap);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
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
            mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
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
        ker::mod::sys::signal::sync_task_signal_mask_cache(child);
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
    if (!clone_fd_table_shared_checked(parent, child)) {
        release_cloned_fd_table_refs(child);
        delete child->thread;
        delete reinterpret_cast<cpu::PerCpu*>(child->context.syscall_scratch_area);
        ker::syscall::shm::shm_cleanup_for_task(child);
        mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-fd-clone-fail");
        mm::phys::page_free(child->pagemap);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ENOMEM);
    }

    child->fd_cloexec = parent->fd_cloexec;

    if (!ker::syscall::vmem::clone_file_mmap_ranges_for_pagemap(parent->pagemap, child->pagemap)) {
        release_cloned_fd_table_refs(child);
        delete child->thread;
        delete reinterpret_cast<cpu::PerCpu*>(child->context.syscall_scratch_area);
        ker::syscall::shm::shm_cleanup_for_task(child);
        mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-mmap-clone-fail");
        mm::phys::page_free(child->pagemap);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        delete child;
        return finish_fork(-ENOMEM);
    }

    // --- Enqueue child ---
    if (!sched::post_task_balanced(child)) {
        // Undo FD refcount increments
        release_cloned_fd_table_refs(child);

        delete child->thread;
        delete reinterpret_cast<cpu::PerCpu*>(child->context.syscall_scratch_area);
        ker::syscall::shm::shm_cleanup_for_task(child);
        ker::syscall::vmem::release_file_mmap_ranges_for_pagemap(child->pagemap);
        mm::virt::destroy_user_space(child->pagemap, child->pid, child->name, "fork-post-task-fail");
        mm::phys::page_free(child->pagemap);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
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

struct KernelStackT {
    void* ss_sp;
    int ss_flags;
    size_t ss_size;
};

struct KernelUtsname {
    char sysname[65];
    char nodename[65];
    char release[65];
    char version[65];
    char machine[65];
    char domainname[65];
};

struct CloneVmArgs {
    uint64_t fn;
    uint64_t child_stack;
    uint64_t flags;
    uint64_t arg;
    uint64_t parent_tidptr;
    uint64_t newtls;
    uint64_t child_tidptr;
};

void copy_cstr_field(char* dst, size_t cap, const char* src) {
    if (dst == nullptr || cap == 0) {
        return;
    }
    if (src == nullptr) {
        dst[0] = '\0';
        return;
    }
    size_t len = 0;
    while (len + 1 < cap && src[len] != '\0') {
        dst[len] = src[len];
        ++len;
    }
    dst[len] = '\0';
}

auto wos_proc_uname(KernelUtsname* buf) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (buf == nullptr) {
        return static_cast<uint64_t>(-EFAULT);
    }

    KernelUtsname uts{};
    copy_cstr_field(uts.sysname, sizeof(uts.sysname), ker::release::NAME);
    copy_cstr_field(uts.nodename, sizeof(uts.nodename), ker::util::hostname::get());
    copy_cstr_field(uts.release, sizeof(uts.release), ker::release::VERSION);
    copy_cstr_field(uts.version, sizeof(uts.version), ker::release::COMPILER);
    copy_cstr_field(uts.machine, sizeof(uts.machine), "x86_64");
    copy_cstr_field(uts.domainname, sizeof(uts.domainname), "localdomain");
    return ker::mod::sys::usercopy::copy_value_to_task(*task, reinterpret_cast<uint64_t>(buf), uts) ? 0 : static_cast<uint64_t>(-EFAULT);
}

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

    KernelSigaction act{};
    if (act_ptr != 0 && !ker::mod::sys::usercopy::copy_value_from_task(*task, act_ptr, act)) {
        return static_cast<uint64_t>(-EFAULT);
    }

    // Return old handler if requested
    if (oldact_ptr != 0) {
        const auto& handler = signal_handler_slot(*task, IDX);
        KernelSigaction old{};
        old.handler = handler.handler;
        old.flags = handler.flags;
        old.restorer = handler.restorer;
        old.mask = handler.mask;
        if (!ker::mod::sys::usercopy::copy_value_to_task(*task, oldact_ptr, old)) {
            return static_cast<uint64_t>(-EFAULT);
        }
    }

    // Set new handler if provided
    if (act_ptr != 0) {
        auto& handler = signal_handler_slot(*task, IDX);
        handler.handler = act.handler;
        handler.flags = act.flags;
        handler.mask = act.mask;
        // Store restorer if SA_RESTORER flag is set
        if ((act.flags & WOS_SA_RESTORER) != 0U) {
            handler.restorer = act.restorer;
        }
    }

    return 0;
}

auto wos_proc_sigaltstack(const KernelStackT* ss, KernelStackT* old_ss, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    (void)gpr;
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    KernelStackT new_ss{};
    if (ss != nullptr && !ker::mod::sys::usercopy::copy_value_from_task(*task, reinterpret_cast<uint64_t>(ss), new_ss)) {
        return static_cast<uint64_t>(-EFAULT);
    }

    uint64_t user_rsp = 0;
    asm volatile("movq %%gs:0x08, %0" : "=r"(user_rsp));
    bool const ON_STACK = (task->sigaltstack_flags & WOS_SS_DISABLE) == 0 && task->sigaltstack_sp != 0 &&
                          user_rsp >= task->sigaltstack_sp && user_rsp < task->sigaltstack_sp + task->sigaltstack_size;

    if (old_ss != nullptr) {
        KernelStackT old{};
        old.ss_sp = reinterpret_cast<void*>(task->sigaltstack_sp);
        old.ss_size = task->sigaltstack_size;
        old.ss_flags = ON_STACK ? WOS_SS_ONSTACK : static_cast<int>(task->sigaltstack_flags);
        if (!ker::mod::sys::usercopy::copy_value_to_task(*task, reinterpret_cast<uint64_t>(old_ss), old)) {
            return static_cast<uint64_t>(-EFAULT);
        }
    }

    if (ss == nullptr) {
        return 0;
    }
    if (ON_STACK) {
        return static_cast<uint64_t>(-EPERM);
    }
    auto const FLAGS = static_cast<uint32_t>(new_ss.ss_flags);
    if ((FLAGS & ~WOS_SS_DISABLE) != 0) {
        return static_cast<uint64_t>(-EINVAL);
    }
    if ((FLAGS & WOS_SS_DISABLE) == 0 && new_ss.ss_size < WOS_MINSIGSTKSZ) {
        return static_cast<uint64_t>(-ENOMEM);
    }

    if ((FLAGS & WOS_SS_DISABLE) != 0) {
        task->sigaltstack_sp = 0;
        task->sigaltstack_size = 0;
        task->sigaltstack_flags = WOS_SS_DISABLE;
        return 0;
    }

    task->sigaltstack_sp = reinterpret_cast<uint64_t>(new_ss.ss_sp);
    task->sigaltstack_size = new_ss.ss_size;
    task->sigaltstack_flags = 0;
    return 0;
}

auto wos_proc_sigprocmask(int how, uint64_t set_ptr, uint64_t oldset_ptr) -> uint64_t {
    using namespace ker::mod;

    auto* task = sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    uint64_t set = 0;
    if (set_ptr != 0 && !ker::mod::sys::usercopy::copy_value_from_task(*task, set_ptr, set)) {
        return static_cast<uint64_t>(-EFAULT);
    }

    // Return old mask if requested (sigset_t first word)
    if (oldset_ptr != 0) {
        uint64_t const OLD_MASK = task->signal_mask_bits();
        if (!ker::mod::sys::usercopy::copy_value_to_task(*task, oldset_ptr, OLD_MASK)) {
            return static_cast<uint64_t>(-EFAULT);
        }
    }

    // Apply new mask if provided
    if (set_ptr != 0) {
        // SIGKILL and SIGSTOP can never be blocked
        uint64_t const UNBLOCKABLE = (1ULL << (WOS_SIGKILL - 1)) | (1ULL << (WOS_SIGSTOP - 1));
        set &= ~UNBLOCKABLE;

        uint64_t new_mask = task->signal_mask_bits();
        switch (how) {
            case 0:  // SIG_BLOCK
                new_mask |= set;
                break;
            case 1:  // SIG_UNBLOCK
                new_mask &= ~set;
                break;
            case 2:  // SIG_SETMASK
                new_mask = set;
                break;
            default:
                return static_cast<uint64_t>(-EINVAL);
        }
        if (new_mask != task->signal_mask_bits()) {
            task->signal_mask_store(new_mask);
            ker::mod::sys::signal::sync_task_signal_mask_cache(task);
        }
    }

    return 0;
}

auto wos_proc_sigpending(uint64_t set_ptr) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (set_ptr == 0) {
        return static_cast<uint64_t>(-EFAULT);
    }

    std::array<uint64_t, 16> set{};
    set.at(0) = task->signal_pending_bits() & task->signal_mask_bits();
    bool const COPIED = ker::mod::sys::usercopy::copy_to_task(*task, set_ptr, set.data(), set.size() * sizeof(set.at(0)));
    return COPIED ? 0 : static_cast<uint64_t>(-EFAULT);
}

auto wos_proc_personality(uint64_t persona) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    uint64_t const OLD_PERSONALITY = task->personality;
    if (persona != WOS_PERSONALITY_QUERY && persona != WOS_PERSONALITY_QUERY_ALL_BITS) {
        task->personality = persona;
    }
    return OLD_PERSONALITY;
}

auto wos_proc_sigsuspend(uint64_t set_ptr, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    using namespace ker::mod;

    auto* task = sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }
    if (set_ptr == 0) {
        return static_cast<uint64_t>(-EFAULT);
    }

    uint64_t set = 0;
    if (!ker::mod::sys::usercopy::copy_value_from_task(*task, set_ptr, set)) {
        return static_cast<uint64_t>(-EFAULT);
    }
    uint64_t const UNBLOCKABLE = (1ULL << (WOS_SIGKILL - 1)) | (1ULL << (WOS_SIGSTOP - 1));
    set &= ~UNBLOCKABLE;

    task->context.regs = gpr;
    task->sigsuspend_saved_mask = task->signal_mask_bits();
    task->sigsuspend_active = true;
    task->signal_mask_store(set);
    ker::mod::sys::signal::sync_task_signal_mask_cache(task);

    if (task->signal_deliverable_bits() != 0) {
        return static_cast<uint64_t>(-EINTR);
    }

    task->set_wait_channel("sigsuspend", ker::mod::sched::task::WaitChannelKind::SIGSUSPEND);
    task->deferred_task_switch = true;
    return 0;
}

auto wos_proc_clone_vm(uint64_t args_addr) -> uint64_t {
    using namespace ker::mod;

    auto* parent = sched::get_current_task();
    if (args_addr == 0) {
        return static_cast<uint64_t>(-EFAULT);
    }
    if (parent == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    CloneVmArgs args{};
    if (!sys::usercopy::copy_value_from_task(*parent, args_addr, args)) {
        return static_cast<uint64_t>(-EFAULT);
    }
    if (power::shutdown_in_progress()) {
        return static_cast<uint64_t>(-ESHUTDOWN);
    }
    if (args.fn == 0 || args.child_stack == 0 || (args.flags & WOS_CLONE_VM) == 0) {
        return static_cast<uint64_t>(-EINVAL);
    }

    uint64_t const SUPPORTED_FLAGS = WOS_CLONE_VM | WOS_CLONE_FS | WOS_CLONE_FILES | WOS_CLONE_UNTRACED | 0xFF;
    if ((args.flags & ~SUPPORTED_FLAGS) != 0) {
        return static_cast<uint64_t>(-EINVAL);
    }
    if (args.parent_tidptr != 0 && !sys::usercopy::ensure_writable(*parent, args.parent_tidptr, sizeof(int))) {
        return static_cast<uint64_t>(-EFAULT);
    }
    if (args.child_tidptr != 0 && !sys::usercopy::ensure_writable(*parent, args.child_tidptr, sizeof(int))) {
        return static_cast<uint64_t>(-EFAULT);
    }

    auto const KERNEL_STACK_BASE = alloc_fork_kernel_stack_with_reclaim();
    if (KERNEL_STACK_BASE == 0) {
        return static_cast<uint64_t>(-ENOMEM);
    }
    uint64_t const KERNEL_RSP = KERNEL_STACK_BASE + ker::mod::mm::KERNEL_STACK_SIZE;

    auto* child = new sched::task::Task{};
    if (child == nullptr) {
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        return static_cast<uint64_t>(-ENOMEM);
    }

    auto cleanup_child = [&]() {
        release_cloned_fd_table_refs(child);
        delete child->thread;
        delete reinterpret_cast<cpu::PerCpu*>(child->context.syscall_scratch_area);
        delete[] child->name;
        mm::phys::page_free(reinterpret_cast<void*>(KERNEL_STACK_BASE));
        child->pagemap = nullptr;
        delete child;
    };

    if (parent->name != nullptr) {
        size_t const NAME_LEN = std::strlen(parent->name);
        char* name_copy = new char[NAME_LEN + 1];
        if (name_copy == nullptr) {
            cleanup_child();
            return static_cast<uint64_t>(-ENOMEM);
        }
        std::memcpy(name_copy, parent->name, NAME_LEN + 1);
        child->name = name_copy;
    }

    child->pid = sched::task::get_next_pid();
    child->parent_pid = parent->pid;
    child->type = sched::task::TaskType::PROCESS;
    child->cpu = cpu::current_cpu();
    child->pagemap = parent->pagemap;
    child->entry = parent->entry;
    child->program_header_addr = parent->program_header_addr;
    child->elf_header_addr = parent->elf_header_addr;
    child->program_header_count = parent->program_header_count;
    child->program_header_ent_size = parent->program_header_ent_size;
    child->interp_base = parent->interp_base;
    child->mmap_next.store(parent->mmap_next.load(std::memory_order_relaxed), std::memory_order_relaxed);
    child->sched_weight = parent->sched_weight;
    child->sched_nice = parent->sched_nice;
    child->slice_ns = parent->slice_ns;
    child->heap_index = -1;
    child->sched_queue = sched::task::Task::sched_queue::NONE;
    child->cwd = parent->cwd;
    child->root = parent->root;
    child->exe_path = parent->exe_path;
    child->session_id = parent->session_id;
    child->pgid = (parent->pgid != 0) ? parent->pgid : parent->pid;
    child->controlling_tty = parent->controlling_tty;
    child->uid = parent->uid;
    child->gid = parent->gid;
    child->euid = parent->euid;
    child->egid = parent->egid;
    child->suid = parent->suid;
    child->sgid = parent->sgid;
    child->umask = parent->umask;
    child->personality = parent->personality;
    child->signal_pending_store(0, std::memory_order_relaxed);
    child->signal_mask_store(parent->signal_mask_bits(), std::memory_order_relaxed);
    child->sig_handlers = parent->sig_handlers;
    child->sigaltstack_sp = parent->sigaltstack_sp;
    child->sigaltstack_size = parent->sigaltstack_size;
    child->sigaltstack_flags = parent->sigaltstack_flags;
    child->dumpable = parent->dumpable;
    child->wki_skip_legacy_placement = true;
    child->wki_target_flags = sched::task::Task::WKI_TARGET_FLAG_LOCAL;

    if (!child->supplementary_groups.clone_from(parent->supplementary_groups) || !child->wki_vfs_rules.clone_from(parent->wki_vfs_rules) ||
        !sched::task::clone_lazy_vmem_ranges(*child, *parent)) {
        cleanup_child();
        return static_cast<uint64_t>(-ENOMEM);
    }

    auto* child_thread = new sched::threading::Thread();
    if (child_thread == nullptr) {
        cleanup_child();
        return static_cast<uint64_t>(-ENOMEM);
    }
    if (parent->thread != nullptr) {
        *child_thread = *parent->thread;
        child_thread->tls_phys_ptr = 0;
        child_thread->stack_phys_ptr = 0;
    }
    uint64_t fsbase = 0;
    if (parent->thread != nullptr) {
        fsbase = parent->thread->fsbase;
    }
    if (args.newtls != 0) {
        fsbase = args.newtls;
    }
    child_thread->fsbase = fsbase;
    child_thread->stack = args.child_stack;
    child_thread->stack_size = 0;
    child_thread->stack_base_virt = 0;
    child_thread->stack_lowest_backed = 0;
    child->thread = child_thread;
    ker::mod::sys::signal::sync_task_signal_mask_cache(child);

    child->context.syscall_kernel_stack = KERNEL_RSP;
    auto* per_cpu = new cpu::PerCpu();
    if (per_cpu == nullptr) {
        cleanup_child();
        return static_cast<uint64_t>(-ENOMEM);
    }
    per_cpu->syscall_stack = KERNEL_RSP;
    per_cpu->cpu_id = cpu::current_cpu();
    per_cpu->user_rsp = args.child_stack;
    per_cpu->syscall_ret_rip = args.fn;
    per_cpu->syscall_ret_flags = 0x202;
    child->context.syscall_scratch_area = reinterpret_cast<uint64_t>(per_cpu);
    child_thread->gsbase = reinterpret_cast<uint64_t>(per_cpu);

    child->context.regs = {};
    child->context.regs.rdi = args.arg;
    child->context.frame.rip = args.fn;
    child->context.frame.rsp = args.child_stack;
    child->context.frame.flags = 0x202;
    child->context.frame.cs = desc::gdt::GDT_USER_CS;
    child->context.frame.ss = desc::gdt::GDT_USER_DS;

    if (!clone_fd_table_shared_checked(parent, child)) {
        cleanup_child();
        return static_cast<uint64_t>(-ENOMEM);
    }
    child->fd_cloexec = parent->fd_cloexec;

    if (!sched::post_task_balanced(child)) {
        cleanup_child();
        return static_cast<uint64_t>(-ENOMEM);
    }

    if (args.parent_tidptr != 0) {
        int const CHILD_PID = static_cast<int>(child->pid);
        if (!sys::usercopy::copy_value_to_task(*parent, args.parent_tidptr, CHILD_PID)) {
            return static_cast<uint64_t>(-EFAULT);
        }
    }
    if (args.child_tidptr != 0) {
        int const CHILD_PID = static_cast<int>(child->pid);
        if (!sys::usercopy::copy_value_to_task(*parent, args.child_tidptr, CHILD_PID)) {
            return static_cast<uint64_t>(-EFAULT);
        }
    }

    return child->pid;
}

auto wos_proc_prctl(int option, uint64_t arg2, uint64_t arg3, uint64_t arg4, uint64_t arg5) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    switch (option) {
        case WOS_PR_SET_PDEATHSIG:
            if (arg2 > ker::mod::sched::task::Task::MAX_SIGNALS) {
                return static_cast<uint64_t>(-EINVAL);
            }
            task->parent_death_signal = static_cast<uint32_t>(arg2);
            return 0;
        case WOS_PR_GET_PDEATHSIG:
            if (arg2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            {
                int const SIGNAL = static_cast<int>(task->parent_death_signal);
                return ker::mod::sys::usercopy::copy_value_to_task(*task, arg2, SIGNAL) ? 0 : static_cast<uint64_t>(-EFAULT);
            }
        case WOS_PR_GET_DUMPABLE:
            return static_cast<uint64_t>(task->dumpable);
        case WOS_PR_SET_DUMPABLE:
            if (arg2 > 1) {
                return static_cast<uint64_t>(-EINVAL);
            }
            task->dumpable = static_cast<int32_t>(arg2);
            return 0;
        case WOS_PR_SET_NAME:
            if (arg2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            {
                constexpr size_t TASK_NAME_MAX = 16;
                auto* name = new char[TASK_NAME_MAX];
                if (name == nullptr) {
                    return static_cast<uint64_t>(-ENOMEM);
                }
                if (!ker::mod::sys::usercopy::copy_cstring_from_task(*task, arg2, name, TASK_NAME_MAX)) {
                    delete[] name;
                    return static_cast<uint64_t>(-EFAULT);
                }
                delete[] task->name;
                task->name = name;
            }
            return 0;
        case WOS_PR_GET_NAME:
            if (arg2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            {
                std::array<char, 16> name{};
                copy_cstr_field(name.data(), name.size(), task->name != nullptr ? task->name : "");
                return ker::mod::sys::usercopy::copy_to_task(*task, arg2, name.data(), name.size()) ? 0 : static_cast<uint64_t>(-EFAULT);
            }
        case WOS_PR_SET_PTRACER:
            task->ptrace_tracer_pid = arg2;
            return 0;
        case WOS_PR_SET_VMA:
            return arg2 == WOS_PR_SET_VMA_ANON_NAME ? 0 : static_cast<uint64_t>(-EINVAL);
        default:
            (void)arg3;
            (void)arg4;
            (void)arg5;
            return static_cast<uint64_t>(-EINVAL);
    }
}

auto wos_proc_arch_prctl(int option, uint64_t arg2) -> uint64_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr || task->thread == nullptr) {
        return static_cast<uint64_t>(-ESRCH);
    }

    switch (option) {
        case WOS_ARCH_SET_FS:
            task->thread->fsbase = arg2;
            ker::mod::cpu::wrfsbase(arg2);
            ker::mod::sys::signal::sync_task_signal_mask_cache(task);
            return 0;
        case WOS_ARCH_GET_FS:
            if (arg2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return ker::mod::sys::usercopy::copy_value_to_task(*task, arg2, task->thread->fsbase) ? 0 : static_cast<uint64_t>(-EFAULT);
        case WOS_ARCH_SET_GS:
            (void)arg2;
            return 0;
        case WOS_ARCH_GET_GS:
            if (arg2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            {
                uint64_t const GS_BASE = 0;
                return ker::mod::sys::usercopy::copy_value_to_task(*task, arg2, GS_BASE) ? 0 : static_cast<uint64_t>(-EFAULT);
            }
        default:
            return static_cast<uint64_t>(-EINVAL);
    }
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
        target = ker::net::wki::wki_proxy_task_find_by_remote_pid_safe(static_cast<uint64_t>(pid));
    }
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
        target->signal_add_pending_mask(1ULL << (sig - 1));

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

auto wos_proc_getpriority(int which, int64_t who) -> uint64_t {
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

    int const ENCODED_NICE = static_cast<int>(target->sched_nice) + WOS_PRIO_KERNEL_ENCODE_BIAS;
    target->release();
    return static_cast<uint64_t>(ENCODED_NICE);
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

    if (!ker::mod::sys::usercopy::copy_from_task(*task, reinterpret_cast<uint64_t>(hostname), task->wki_target_hostname.data(), len)) {
        return static_cast<uint64_t>(-EFAULT);
    }
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
        if (!ker::mod::sys::usercopy::copy_to_task(*task, reinterpret_cast<uint64_t>(hostname_out), task->wki_target_hostname.data(),
                                                   LEN + 1)) {
            return static_cast<uint64_t>(-EFAULT);
        }
    }

    if (flags_out != nullptr) {
        if (!ker::mod::sys::usercopy::copy_value_to_task(*task, reinterpret_cast<uint64_t>(flags_out), task->wki_target_flags)) {
            return static_cast<uint64_t>(-EFAULT);
        }
    }

    return LEN;
}
}  // namespace

#ifdef WOS_SELFTEST
auto process_selftest_fd_clone_failure_releases_refs() -> bool {
    ker::mod::sched::task::Task parent{};
    ker::mod::sched::task::Task child{};
    ker::vfs::File file{};
    file.refcount.store(1, std::memory_order_relaxed);

    constexpr uint64_t FD = 17;
    if (!parent.fd_table.insert(FD, &file)) {
        return false;
    }

    g_process_selftest_force_fd_clone_insert_failure.store(true, std::memory_order_relaxed);
    bool const CLONED = clone_fd_table_shared_checked(&parent, &child);
    g_process_selftest_force_fd_clone_insert_failure.store(false, std::memory_order_relaxed);

    bool ok = !CLONED && file.refcount.load(std::memory_order_relaxed) == 1 && child.fd_table.lookup(FD) == nullptr;
    parent.fd_table.remove(FD);
    return ok;
}
#endif

auto personality(uint64_t persona) -> uint64_t { return wos_proc_personality(persona); }

auto process(abi::process::procmgmt_ops op, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5, uint64_t a6, ker::mod::cpu::GPRegs& gpr)
    -> uint64_t {
    switch (op) {
        case abi::process::procmgmt_ops::EXIT:
            wos_proc_exit(static_cast<int>(a2));
            __builtin_unreachable();
        case abi::process::procmgmt_ops::EXEC: {
            return wos_proc_exec(reinterpret_cast<const char*>(a2), reinterpret_cast<const char* const*>(a3),
                                 reinterpret_cast<const char* const*>(a4));
        }
        case abi::process::procmgmt_ops::SPAWN: {
            return wos_proc_spawn(reinterpret_cast<const char*>(a2), reinterpret_cast<const char* const*>(a3),
                                  reinterpret_cast<const char* const*>(a4), reinterpret_cast<const abi::process::SpawnOptions*>(a5));
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
        case abi::process::procmgmt_ops::SIGPENDING: {
            return wos_proc_sigpending(a2);
        }
        case abi::process::procmgmt_ops::SIGSUSPEND: {
            return wos_proc_sigsuspend(a2, gpr);
        }
        case abi::process::procmgmt_ops::UNAME: {
            return wos_proc_uname(reinterpret_cast<KernelUtsname*>(a2));
        }
        case abi::process::procmgmt_ops::CLONE_VM_PROC: {
            return wos_proc_clone_vm(a2);
        }
        case abi::process::procmgmt_ops::PRCTL: {
            return wos_proc_prctl(static_cast<int>(a2), a3, a4, a5, a6);
        }
        case abi::process::procmgmt_ops::ARCH_PRCTL: {
            return wos_proc_arch_prctl(static_cast<int>(a2), a3);
        }
        case abi::process::procmgmt_ops::SIGALTSTACK: {
            return wos_proc_sigaltstack(reinterpret_cast<const KernelStackT*>(a2), reinterpret_cast<KernelStackT*>(a3), gpr);
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
        case abi::process::procmgmt_ops::GETRESUID: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (a2 == 0 || a3 == 0 || a4 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            if (!ker::mod::sys::usercopy::copy_value_to_task(*task, a2, task->uid) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a3, task->euid) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a4, task->suid)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return 0;
        }
        case abi::process::procmgmt_ops::GETRESGID: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (a2 == 0 || a3 == 0 || a4 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            if (!ker::mod::sys::usercopy::copy_value_to_task(*task, a2, task->gid) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a3, task->egid) ||
                !ker::mod::sys::usercopy::copy_value_to_task(*task, a4, task->sgid)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            return 0;
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
            uint64_t const PROCESS_PID = process_pid_for_task(*task);
            // POSIX: setsid fails if the caller is already a process group leader
            if (task->pgid == PROCESS_PID && task->session_id != 0) {
                return static_cast<uint64_t>(-EPERM);
            }
            update_thread_group_job_control(PROCESS_PID, PROCESS_PID, PROCESS_PID, true);
            return PROCESS_PID;
        }
        case abi::process::procmgmt_ops::GETSID: {
            auto pid = static_cast<int64_t>(a2);
            if (pid == 0) {
                auto* task = ker::mod::sched::get_current_task();
                if (task == nullptr) {
                    return static_cast<uint64_t>(-ESRCH);
                }
                uint64_t const PROCESS_PID = process_pid_for_task(*task);
                return task->session_id != 0 ? task->session_id : PROCESS_PID;
            }
            auto* target = find_process_leader_safe(static_cast<uint64_t>(pid));
            if (target == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            auto sid = target->session_id != 0 ? target->session_id : process_pid_for_task(*target);
            target->release();
            return sid;
        }
        case abi::process::procmgmt_ops::SETPGID: {
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            uint64_t const CALLER_PID = process_pid_for_task(*task);
            auto pid = a2;
            auto new_pgid = a3;
            // pid==0 means current process
            if (pid == 0) {
                pid = CALLER_PID;
            }
            // pgid==0 means use pid as pgid
            if (new_pgid == 0) {
                new_pgid = pid;
            }
            auto* target = find_process_leader_safe(pid);
            if (target == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            uint64_t const TARGET_PID = process_pid_for_task(*target);
            if (TARGET_PID == CALLER_PID) {
                update_thread_group_job_control(TARGET_PID, target->session_id, new_pgid, false);
                target->release();
                return 0;
            }
            // Setting pgid for another process (must be a child, same session)
            if (target->parent_pid != CALLER_PID || target->session_id != task->session_id) {
                target->release();
                return static_cast<uint64_t>(-EPERM);
            }
            update_thread_group_job_control(TARGET_PID, target->session_id, new_pgid, false);
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
                uint64_t const PROCESS_PID = process_pid_for_task(*task);
                return task->pgid != 0 ? task->pgid : PROCESS_PID;
            }
            auto* target = find_process_leader_safe(static_cast<uint64_t>(pid));
            if (target == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            uint64_t const PROCESS_PID = process_pid_for_task(*target);
            auto pgid = target->pgid != 0 ? target->pgid : PROCESS_PID;
            target->release();
            return pgid;
        }
        case abi::process::procmgmt_ops::EXECVE: {
            // POSIX replace-process execve
            return wos_proc_execve(reinterpret_cast<const char*>(a2), reinterpret_cast<const char* const*>(a3),
                                   reinterpret_cast<const char* const*>(a4), gpr);
        }
        case abi::process::procmgmt_ops::GETHOSTNAME: {
            auto bufsize = static_cast<size_t>(a3);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (a2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            const char* name = ker::util::hostname::get();
            size_t const LEN = std::strlen(name);
            if (LEN + 1 > bufsize) {
                return static_cast<uint64_t>(-ENAMETOOLONG);
            }
            return ker::mod::sys::usercopy::copy_to_task(*task, a2, name, LEN + 1) ? 0 : static_cast<uint64_t>(-EFAULT);
        }
        case abi::process::procmgmt_ops::SETHOSTNAME: {
            auto len = static_cast<size_t>(a3);
            auto* task = ker::mod::sched::get_current_task();
            if (task == nullptr) {
                return static_cast<uint64_t>(-ESRCH);
            }
            if (a2 == 0) {
                return static_cast<uint64_t>(-EFAULT);
            }
            if (len == 0 || len >= ker::util::hostname::HOSTNAME_MAX) {
                return static_cast<uint64_t>(-EINVAL);
            }
            std::array<char, ker::util::hostname::HOSTNAME_MAX> name{};
            if (!ker::mod::sys::usercopy::copy_from_task(*task, a2, name.data(), len)) {
                return static_cast<uint64_t>(-EFAULT);
            }
            int const R = ker::util::hostname::set(name.data(), len);
            return (R < 0) ? static_cast<uint64_t>(R) : 0;
        }
        case abi::process::procmgmt_ops::SETPRIORITY: {
            return wos_proc_setpriority(static_cast<int>(a2), static_cast<int64_t>(a3), static_cast<int>(a4));
        }
        case abi::process::procmgmt_ops::GETPRIORITY: {
            return wos_proc_getpriority(static_cast<int>(a2), static_cast<int64_t>(a3));
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
