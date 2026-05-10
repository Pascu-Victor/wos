#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <defines/defines.hpp>
#include <platform/asm/cpu.hpp>
#include <platform/interrupt/gdt.hpp>
#include <platform/mm/paging.hpp>
// #include <platform/sys/context_switch.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/sched/threading.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/radix_tree.hpp>
#include <util/smallvec.hpp>
// Avoid pulling in in-tree std headers into kernel translation units from here.

namespace ker::mod::sched::task {

enum class WkiVfsRoute : uint8_t {
    LOCAL = 0,
    HOST = 1,
};

struct WkiVfsRule {
    static constexpr unsigned PREFIX_MAX = 256;

    char prefix[PREFIX_MAX] = {};
    uint16_t prefix_len = 0;
    uint8_t route = static_cast<uint8_t>(WkiVfsRoute::LOCAL);
    uint8_t reserved = 0;
};

enum class TaskType {
    DAEMON,
    PROCESS,
    IDLE,
};

// Task lifecycle state for lock-free scheduling
// Transitions: ACTIVE -> EXITING -> DEAD
enum class TaskState : uint32_t {
    ACTIVE = 0,   // Task is runnable/running
    EXITING = 1,  // Task is in exit process, resources being freed
    DEAD = 2,     // Task fully dead, safe to reclaim memory after epoch grace period
};

struct Context {
    uint64_t syscall_kernel_stack;
    uint64_t syscall_scratch_area;  // Small scratch area for syscall handler (RIP, RSP, RFLAGS, DS, ES)

    cpu::GPRegs regs;

    uint64_t int_no;
    uint64_t error_code;

    gates::InterruptFrame frame;
} __attribute__((packed));

// FPU/SSE/AVX state saved by xsave or fxsave (must be 64-byte aligned for xsave).
// We over-allocate by 63 bytes so we can always find a 64-byte-aligned region
// inside the buffer, regardless of the Task allocation's alignment.
struct FxState {
    uint8_t raw[2688 + 63] = {};
    bool saved = false;  // true after first save_fpu_state - guards xrstor on zeroed buffer

    // Return a pointer to the 64-byte-aligned region within raw[].
    uint8_t* aligned() {
        auto addr = reinterpret_cast<uintptr_t>(raw);
        addr = (addr + 63) & ~static_cast<uintptr_t>(63);
        return reinterpret_cast<uint8_t*>(addr);
    }
    [[nodiscard]] auto aligned() const -> const uint8_t* {
        auto addr = reinterpret_cast<uintptr_t>(raw);
        addr = (addr + 63) & ~static_cast<uintptr_t>(63);
        return reinterpret_cast<const uint8_t*>(addr);
    }
};

struct Task {
    Task(Task&&) = delete;
    auto operator=(const Task&) -> Task& = delete;
    auto operator=(Task&&) -> Task& = delete;
    Task(const char* name, uint64_t elf_start, uint64_t kernel_rsp, TaskType type);

    // Factory for kernel threads (DAEMON tasks).
    // entryFunc: [[noreturn]] void func() - the kernel thread body.
    static auto create_kernel_thread(const char* name, void (*entry_func)()) -> Task*;

    // Factory for userspace threads (PROCESS tasks that share the parent's pagemap).
    // tcbVaddr: virtual address of the mlibc TCB (becomes FS base / fsbase).
    // userSp:   prepared stack pointer (sys_prepare_stack pushed entry+user_arg below it).
    // enterThreadVa: virtual address of __mlibc_enter_thread in the process image.
    static auto create_user_thread(Task* parent, uint64_t tcb_vaddr, uint64_t user_sp, uint64_t enter_thread_va) -> Task*;

    Task(const Task& task) = delete;

    // Default constructor - leaves all fields zero/default-initialized.
    // Used only by createUserThread; callers must fill in all required fields.
    Task() = default;

    struct SigHandler {
        uint64_t handler;   // SIG_DFL=0, SIG_IGN=1, or function pointer
        uint64_t flags;     // SA_SIGINFO, SA_RESTART, etc.
        uint64_t restorer;  // Signal return trampoline (sa_restorer)
        uint64_t mask;      // Additional signals to block during handler
    };
    static constexpr unsigned MAX_SIGNALS = 64;

    // Which scheduling queue the task is logically in.
    enum class sched_queue : uint8_t { NONE = 0, RUNNABLE = 1, WAITING = 2, DEAD_GC = 3 };
    static constexpr unsigned FD_TABLE_SIZE = 256;
    static constexpr unsigned CWD_MAX = 256;
    static constexpr unsigned EXE_PATH_MAX = 256;
    static constexpr unsigned WKI_TARGET_HOSTNAME_MAX = 64;
    static constexpr uint32_t WKI_TARGET_FLAG_STRICT = 1U << 0;
    static constexpr uint32_t WKI_TARGET_FLAG_LOCAL = 1U << 1;      // pin task to local node (skip remote placement)
    static constexpr uint32_t WKI_TARGET_FLAG_NOINHERIT = 1U << 2;  // don't propagate wki_target to child processes
    static constexpr uint32_t WKI_TARGET_FLAG_REMOTE = 1U << 3;     // prefer a remote node, falling back locally unless strict
    static constexpr uint32_t WKI_TARGET_FLAGS_ALL =
        WKI_TARGET_FLAG_STRICT | WKI_TARGET_FLAG_LOCAL | WKI_TARGET_FLAG_NOINHERIT | WKI_TARGET_FLAG_REMOTE;

    // Lock-free lifecycle management (epoch-based reclamation).
    alignas(8) std::atomic<TaskState> state{TaskState::ACTIVE};

    TaskType type;
    mm::paging::PageTable* pagemap{};
    uint64_t entry{};
    void (*kthread_entry)(){};  // Kernel thread entry (DAEMON only), nullptr otherwise
    const char* name{};
    uint64_t cpu{};

    // CPU domain affinity (Phase 1 - CPU domain infrastructure).
    // domain_id: which domain this task belongs to (0 = ROOT, any CPU allowed)
    // domain_mask: bitmask of allowed CPUs (all-ones = unrestricted)
    // domain_hard: when true, task NEVER runs outside domain_mask
    uint64_t domain_mask = ~0ULL;
    threading::Thread* thread{};
    uint64_t pid{};
    uint64_t parent_pid{};  // Parent process ID (0 for orphaned/init processes)

    // ELF buffer for cleanup and auxv metadata.
    uint8_t* elf_buffer{};
    size_t elf_buffer_size{};
    uint64_t program_header_addr{};  // Virtual address of program headers (AT_PHDR)
    uint64_t elf_header_addr{};      // Virtual address of ELF header (AT_EHDR)
    uint64_t interp_base = 0;        // Load base of dynamic linker (AT_BASE), 0 if statically linked

    // PID of the process that owns this thread (set by create_user_thread).
    // 0 for regular processes.
    uint64_t owner_pid = 0;

    // WKI: PID visible on the remote execution host. For receiver-side tasks
    // this matches pid; for submitter-side proxy tasks it is filled in once the
    // remote launch is accepted.
    uint64_t wki_remote_pid = 0;

    // Session and process group IDs (for setsid/setpgid/POSIX job control).
    uint64_t session_id = 0;  // Session ID (0 = inherit from parent, set by setsid)
    uint64_t pgid = 0;        // Process group ID (0 = same as pid, set by setpgid)

    // Plain spinlocks disable task preemption without masking interrupts. This
    // keeps preemptible DAEMON/NAPI workers from being switched out while they
    // hold locks that other CPUs may spin on.
    uint64_t preempt_disable_start_us = 0;
    uint64_t preempt_disable_max_us = 0;
    uint64_t preempt_disable_owner = 0;

    // Waitpid state: when this task is waiting for another task to exit.
    uint64_t waiting_for_pid{};            // PID we're waiting for (for waitpid return value)
    uint64_t wait_status_user_addr{};      // Userspace virtual address of status variable (for waitpid)
    uint64_t wait_status_phys_addr{};      // Last translated physical address (debug; may change after COW)
    uint64_t wait_rusage_user_addr{};      // Userspace virtual address of rusage struct (for wait3/wait4)
    uint64_t wait_rusage_phys_addr{};      // Last translated physical address (debug; may change after COW)
    uint64_t wait_resume_rip_user_addr{};  // Userspace RIP expected when returning from waitpid
    uint64_t wait_resume_rip_phys_addr{};  // Last translated physical address for wait_resume_rip_user_addr
    uint64_t wait_resume_rsp_user_addr{};  // Userspace RSP expected when returning from waitpid
    uint64_t wait_resume_rsp_phys_addr{};  // Last translated physical address for wait_resume_rsp_user_addr (debug; may change after COW)

    // Signal state.
    uint64_t sig_pending{};  // Bit N = signal N+1 is pending, signals 1-64
    uint64_t sig_mask{};     // Bitmask of blocked signals

    // Process time accounting (microseconds).
    uint64_t start_time_us{};   // Wall-clock time when task was created
    uint64_t user_time_us{};    // Cumulative user-mode CPU time
    uint64_t system_time_us{};  // Cumulative kernel-mode CPU time

    // ITIMER_REAL state (microseconds, 0 = not armed).
    uint64_t itimer_real_expire_us{};    // Absolute HPET time when SIGALRM fires (0 = disarmed)
    uint64_t itimer_real_interval_us{};  // Reload interval after expiry (0 = one-shot)

    // EEVDF scheduling fields.
    int64_t vruntime{};   // Cumulative weighted CPU time consumed
    int64_t vdeadline{};  // vruntime + (slice_ns * 1024 / weight)
    uint64_t wake_at_us{};
    uint64_t poll_wait_deadline_us = 0;
    uint64_t last_sleep_start_us = 0;
    uint64_t last_wake_us = 0;
    uint64_t perf_wait_callsite = 0;

    // Wait channel: human-readable string describing why the task is blocked.
    const char* wait_channel = nullptr;

    // Owned futex waiter node while this task is blocked in futex_wait().
    // Wake and exit cleanup atomically exchange this pointer to guarantee the
    // waiter is detached and freed exactly once.
    std::atomic<void*> futex_waiter{nullptr};

    // Intrusive list pointer for wait queue and dead list (zero allocations).
    Task* sched_next{};
    alignas(8) std::atomic<uint64_t> death_epoch{0};  // Epoch when task became DEAD

    // File descriptor table for the task (per-process model).
    // Dynamic radix tree: no fixed upper bound on FD count.
    // FD_TABLE_SIZE retained as a soft limit for dup2/fcntl bounds checking.
    ker::util::RadixTree<void*> fd_table;

    // Per-fd close-on-exec bitmap (POSIX FD_CLOEXEC is per-fd, not per-file).
    uint64_t fd_cloexec[FD_TABLE_SIZE / 64] = {};

    // List of task IDs waiting for this task to exit. When this task exits,
    // all tasks in this list will be rescheduled on their respective CPUs.
    ker::util::SmallVec<uint64_t, 4> awaitee_on_exit;

    // WKI: explicit task-local VFS rules layered over defaults from /etc/vfstab.
    ker::util::SmallVec<WkiVfsRule, 4> wki_vfs_rules;

    // Signal handler entry (matches Linux ABI struct sigaction layout).
    SigHandler sig_handlers[MAX_SIGNALS]{};

    uint32_t domain_id = 0;
    uint32_t wki_target_flags = 0;

    // WKI: when non-zero, this task is a proxy for a remote task.
    // The proxy stays in WAITING state until the remote task completes.
    uint32_t wki_proxy_task_id = 0;

    // POSIX credential model.
    uint32_t uid = 0;      // Real user ID
    uint32_t gid = 0;      // Real group ID
    uint32_t euid = 0;     // Effective user ID
    uint32_t egid = 0;     // Effective group ID
    uint32_t suid = 0;     // Saved set-user-ID
    uint32_t sgid = 0;     // Saved set-group-ID
    uint32_t umask = 022;  // File creation mask (default: rw-r--r-- for files)

    // Controlling terminal: index into PTY pool, or -1 if none.
    int controlling_tty = -1;

    // Exit status of this process (set when process exits, used by waitpid).
    int exit_status{};
    uint32_t preempt_disable_depth = 0;
    uint32_t sched_weight{};   // 1024 = default (nice 0)
    uint32_t slice_ns{};       // Time slice in nanoseconds
    uint32_t slice_used_ns{};  // Accumulated runtime within current slice
    int32_t heap_index{};      // -1 if not in any per-CPU RunHeap
    uint32_t last_run_us = 0;
    uint32_t last_sleep_us = 0;

    alignas(4) std::atomic<uint32_t> ref_count{1};  // Scheduler owns initial reference
    mod::sys::Spinlock fd_table_lock;
    mod::sys::Spinlock exit_waiters_lock;

    uint16_t program_header_count = 0;     // Number of program headers (AT_PHNUM)
    uint16_t program_header_ent_size = 0;  // Size of each program header entry (AT_PHENT)

    bool cpu_pinned = false;  // When true, scheduler will not migrate this task to another CPU
    bool domain_hard = false;
    bool has_run{};                     // True once the task has run at least once
    bool is_elf_buffer_shared = false;  // True if buffer is from shared cache, don't delete[]

    // True when this Task is a userspace thread (shares pagemap with owning process).
    // Thread exits must NOT free the pagemap or close shared FDs.
    bool is_thread = false;

    // WKI: prefer inline delivery for remote compute placement (V2 A6.4).
    bool wki_prefer_inline = false;

    // WKI: set by richer spawn paths after they already attempted remote placement.
    // The legacy scheduler hook must skip those tasks to avoid losing exec context.
    bool wki_skip_legacy_placement = false;

    bool has_exited{};
    bool waited_on{};             // Set to true when parent retrieves exit status via waitpid
    bool deferred_task_switch{};  // Move to wait queue after syscall returns
    bool yield_switch{};          // Put task in expired queue instead of wait queue

    // Set by reschedule_task_for_cpu when a wakeup fires while this task is
    // still currentTask (about to block via deferred_task_switch). Checked
    // under the RQ lock in deferred_task_switch to avoid lost wakeups.
    std::atomic<bool> wakeup_pending{false};

    // When true, a PROCESS task is at a safe voluntary blocking point (e.g. sti;hlt
    // in a syscall wait loop). The scheduler may preempt it as if it were a
    // DAEMON, saving and restoring kernel-mode context. Set before hlt, cleared after wake.
    volatile bool voluntary_block = false;

    bool preempt_pending = false;
    bool in_signal_handler{};  // Set to true when a signal handler frame is being delivered
    bool do_sigreturn{};       // Set by sigreturn syscall; check_pending_signals restores context
    int8_t sched_nice = 0;     // POSIX nice value backing sched_weight for PROCESS tasks
    sched_queue sched_queue;
    bool wants_block{};

    // Set when a task transitions from the wait list to the runnable heap
    // (timer expiry or kern_wake). process_tasks uses this to enforce a
    // minimum run-time granularity before a freshly-woken I/O task is allowed
    // to preempt the currently running compute task (wakeup-preemption guard).
    // Cleared on the first timer tick that fires while the task is running.
    bool just_woke = false;

    std::atomic<bool> gc_queued{false};  // True once the task has been handed to dead-list GC

    // WKI: optional explicit remote target hostname for spawned processes.
    // Empty means use automatic load-based placement.
    char wki_target_hostname[WKI_TARGET_HOSTNAME_MAX] = "";

    // WKI: hostname of the logical submitter host whose filesystem should be
    // treated as /wki/host for this task.
    char wki_submitter_hostname[WKI_TARGET_HOSTNAME_MAX] = "";

    Context context{};

    // Current working directory (absolute path, "/" by default).
    char cwd[CWD_MAX] = "/";

    // Per-process root directory (for pivot_root / chroot).
    // Path resolution prepends this to absolute paths when it differs from "/".
    char root[CWD_MAX] = "/";

    // Executable path (set by exec, used by procfs /proc/self/exe).
    char exe_path[EXE_PATH_MAX] = "";

    FxState fx_state;  // FPU/SSE register state (fxsave/fxrstor on context switch)

    void set_fd_cloexec(unsigned fd) {
        if (fd < FD_TABLE_SIZE) {
            fd_cloexec[fd / 64] |= (1ULL << (fd % 64));
        }
    }
    void clear_fd_cloexec(unsigned fd) {
        if (fd < FD_TABLE_SIZE) {
            fd_cloexec[fd / 64] &= ~(1ULL << (fd % 64));
        }
    }
    [[nodiscard]] bool get_fd_cloexec(unsigned fd) const {
        if (fd >= FD_TABLE_SIZE) {
            return false;
        }
        return (fd_cloexec[fd / 64] & (1ULL << (fd % 64))) != 0;
    }

    void load_context(cpu::GPRegs* gpr);
    void save_context(cpu::GPRegs* gpr) const;

    // Try to acquire a reference to this task.
    // Returns false if task is EXITING or DEAD.
    // Caller MUST call release() when done with the task pointer.
    bool try_acquire() {
        uint32_t count = ref_count.load(std::memory_order_acquire);
        while (count > 0) {
            if (ref_count.compare_exchange_weak(count, count + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                // Double-check state after acquiring
                TaskState const S = state.load(std::memory_order_acquire);
                if (S == TaskState::DEAD || S == TaskState::EXITING) {
                    ref_count.fetch_sub(1, std::memory_order_release);
                    return false;
                }
                return true;
            }
        }
        return false;
    }

    // Release a reference to this task
    void release() { ref_count.fetch_sub(1, std::memory_order_acq_rel); }

    // Atomically transition task state. Returns true if transition succeeded.
    bool transition_state(TaskState from, TaskState to) {
        return state.compare_exchange_strong(from, to, std::memory_order_acq_rel, std::memory_order_acquire);
    }
};  // Removed __attribute__((packed)) - was causing misalignment of atomic fields and potential corruption

auto get_next_pid() -> uint64_t;
}  // namespace ker::mod::sched::task
