#pragma once

#include <abi/ptrace.hpp>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <defines/defines.hpp>
#include <iterator>
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

namespace ker::vfs {
struct File;
}  // namespace ker::vfs

namespace ker::mod::sched::task {

enum class WkiVfsRoute : uint8_t {
    LOCAL = 0,
    HOST = 1,
};

struct WkiVfsRule {
    static constexpr unsigned PREFIX_MAX = 256;
    using PrefixBuffer = std::array<char, PREFIX_MAX>;

    PrefixBuffer prefix{};
    uint16_t prefix_len = 0;
    uint8_t route = static_cast<uint8_t>(WkiVfsRoute::LOCAL);
    uint8_t reserved = 0;
};

enum class LazyVmemKind : uint8_t {
    ANONYMOUS,
    FILE_BACKED,
};

struct LazyVmemRange {
    uint64_t start = 0;
    uint64_t end = 0;
    uint64_t prot = 0;
    uint64_t flags = 0;
    LazyVmemKind kind = LazyVmemKind::ANONYMOUS;
    ker::vfs::File* file = nullptr;
    uint64_t file_offset = 0;
    uint64_t file_dev = 0;
    uint64_t file_ino = 0;
    uint64_t file_size = 0;
    int64_t file_mtime_sec = 0;
    int64_t file_mtime_nsec = 0;
    int64_t file_ctime_sec = 0;
    int64_t file_ctime_nsec = 0;
};
using LazyVmemRangeVec = ker::util::SmallVec<LazyVmemRange, 8>;

enum class TaskType : uint8_t {
    DAEMON,
    PROCESS,
    IDLE,
};

// Task lifecycle state for lock-free scheduling
// Transitions: ACTIVE -> EXITING -> DEAD
enum class TaskState : uint8_t {
    ACTIVE = 0,   // Task is runnable/running
    EXITING = 1,  // Task is in exit process, resources being freed
    DEAD = 2,     // Task fully dead, safe to reclaim memory after epoch grace period
};

enum class WaitChannelKind : uint8_t {
    NONE,
    GENERIC,
    LOCAL_PIPE,
    LOCAL_PTY,
    WAITPID,
    FUTEX,
    SIGSUSPEND,
    WKI_EXECVE_PROXY,
    PTRACE,
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
    static constexpr size_t XSAVE_AREA_SIZE = 2688;
    static constexpr size_t ALIGNMENT_SLACK = 63;
    using RawBuffer = std::array<uint8_t, XSAVE_AREA_SIZE + ALIGNMENT_SLACK>;

    RawBuffer raw{};
    bool saved = false;  // true after first save_fpu_state - guards xrstor on zeroed buffer

    // Return a pointer to the 64-byte-aligned region within raw[].
    uint8_t* aligned() {
        auto addr = reinterpret_cast<uintptr_t>(raw.data());
        addr = (addr + 63) & ~static_cast<uintptr_t>(63);
        return reinterpret_cast<uint8_t*>(addr);
    }
    [[nodiscard]] auto aligned() const -> const uint8_t* {
        auto addr = reinterpret_cast<uintptr_t>(raw.data());
        addr = (addr + 63) & ~static_cast<uintptr_t>(63);
        return reinterpret_cast<const uint8_t*>(addr);
    }
};

// Keep related scheduler/process fields grouped; optimal packing order makes the
// lifecycle-heavy type harder to audit for little memory win.
// NOLINTNEXTLINE(clang-analyzer-optin.performance.Padding)
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
    static constexpr unsigned FD_CLOEXEC_WORDS = FD_TABLE_SIZE / 64;
    static constexpr unsigned CWD_MAX = 256;
    static constexpr unsigned EXE_PATH_MAX = 256;
    static constexpr unsigned SUPPLEMENTARY_GROUPS_MAX = 8;
    static constexpr unsigned WKI_TARGET_HOSTNAME_MAX = 64;
    using FdCloexecBitmap = std::array<uint64_t, FD_CLOEXEC_WORDS>;
    using SignalHandlerTable = std::array<SigHandler, MAX_SIGNALS>;
    using PathBuffer = std::array<char, CWD_MAX>;
    using ExePathBuffer = std::array<char, EXE_PATH_MAX>;
    using HostnameBuffer = std::array<char, WKI_TARGET_HOSTNAME_MAX>;
    static constexpr uint32_t WKI_TARGET_FLAG_STRICT = 1U << 0;
    static constexpr uint32_t WKI_TARGET_FLAG_LOCAL = 1U << 1;      // pin task to local node (skip remote placement)
    static constexpr uint32_t WKI_TARGET_FLAG_NOINHERIT = 1U << 2;  // don't propagate wki_target to child processes
    static constexpr uint32_t WKI_TARGET_FLAG_REMOTE = 1U << 3;     // prefer a remote node, falling back locally unless strict
    static constexpr uint32_t WKI_TARGET_FLAGS_ALL =
        WKI_TARGET_FLAG_STRICT | WKI_TARGET_FLAG_LOCAL | WKI_TARGET_FLAG_NOINHERIT | WKI_TARGET_FLAG_REMOTE;
    static constexpr uint64_t PERSONALITY_ADDR_NO_RANDOMIZE = 0x0040000;
    static constexpr uint64_t DEFAULT_PERSONALITY = PERSONALITY_ADDR_NO_RANDOMIZE;

    // Lock-free lifecycle management (epoch-based reclamation).
    alignas(8) std::atomic<TaskState> state{TaskState::ACTIVE};

    TaskType type{TaskType::DAEMON};
    mm::paging::PageTable* pagemap{};
    uint64_t entry{};
    void (*kthread_entry)(){};  // Kernel thread entry (DAEMON only), nullptr otherwise
    const char* name{};
    // Authoritative scheduler owner while ACTIVE: for RUNNABLE/WAITING tasks
    // this names the runqueue holding the task; for current/handoff tasks it
    // names the CPU whose stack is executing or about to execute it. Updates
    // happen under the owning runqueue lock or during task construction before
    // publication.
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
    uint64_t program_header_addr{};      // Virtual address of program headers (AT_PHDR)
    uint64_t elf_header_addr{};          // Virtual address of ELF header (AT_EHDR)
    uint64_t interp_base = 0;            // Load base of dynamic linker (AT_BASE), 0 if statically linked
    std::atomic<uint64_t> mmap_next{0};  // Next preferred mmap search address; zero means use syscall default.
    LazyVmemRangeVec lazy_vmem_ranges;
    mod::sys::Spinlock lazy_vmem_lock;

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
    uint64_t personality = DEFAULT_PERSONALITY;

    // Plain spinlocks disable task preemption without masking interrupts. This
    // keeps preemptible DAEMON/NAPI workers from being switched out while they
    // hold locks that other CPUs may spin on.
    uint64_t preempt_disable_start_us = 0;
    uint64_t preempt_disable_max_us = 0;
    uint64_t preempt_disable_owner = 0;

    // Waitpid state: when this task is waiting for another task to exit.
    uint64_t waiting_for_pid{};            // PID we're waiting for (for waitpid return value)
    int32_t wait_options{};                // waitpid option bits active for the current block
    uint64_t wait_status_user_addr{};      // Userspace virtual address of status variable (for waitpid)
    uint64_t wait_status_phys_addr{};      // Last translated physical address (debug; may change after COW)
    uint64_t wait_rusage_user_addr{};      // Userspace virtual address of rusage struct (for wait3/wait4)
    uint64_t wait_rusage_phys_addr{};      // Last translated physical address (debug; may change after COW)
    uint64_t wait_resume_rip_user_addr{};  // Userspace RIP expected when returning from waitpid
    uint64_t wait_resume_rip_phys_addr{};  // Last translated physical address for wait_resume_rip_user_addr
    uint64_t wait_resume_rsp_user_addr{};  // Userspace RSP expected when returning from waitpid
    uint64_t wait_resume_rsp_phys_addr{};  // Last translated physical address for wait_resume_rsp_user_addr (debug; may change after COW)

    // Signal state.
    uint64_t sig_pending{};            // Bit N = signal N+1 is pending, signals 1-64
    uint64_t sig_mask{};               // Bitmask of blocked signals
    uint64_t sig_mask_seq{};           // Monotonic version mirrored into the userspace TCB cache.
    uint64_t sigsuspend_saved_mask{};  // Mask to restore after a sigsuspend wake.
    uint64_t sigaltstack_sp{};         // Base address for sigaltstack(2), or 0 when disabled.
    uint64_t sigaltstack_size{};       // Size of the alternate signal stack.
    uint32_t sigaltstack_flags = 2;    // SS_DISABLE by default.
    uint32_t parent_death_signal{};    // PR_SET_PDEATHSIG state.
    int32_t dumpable = 1;              // PR_GET/SET_DUMPABLE state.
    bool sigsuspend_active{};          // Current signal mask is temporary for sigsuspend.

    auto signal_frame_saved_mask() -> uint64_t {
        if (!sigsuspend_active) {
            return sig_mask;
        }
        uint64_t const SAVED = sigsuspend_saved_mask;
        sigsuspend_active = false;
        sigsuspend_saved_mask = 0;
        return SAVED;
    }

    auto has_interrupting_signal_pending() -> bool {
        for (;;) {
            uint64_t const DELIVERABLE = sig_pending & ~sig_mask;
            if (DELIVERABLE == 0) {
                return false;
            }

            int const SIGNO = __builtin_ctzll(DELIVERABLE) + 1;
            auto const IDX = static_cast<unsigned>(SIGNO - 1);
            auto const HANDLER = sig_handlers.at(IDX).handler;
            bool const DEFAULT_IGNORED = SIGNO == 17 || SIGNO == 23 || SIGNO == 28 || SIGNO == 18;
            bool const IGNORE = (HANDLER == 0 && DEFAULT_IGNORED) || HANDLER == 1;
            if (!IGNORE) {
                return true;
            }

            sig_pending &= ~(1ULL << IDX);
            if (sigsuspend_active) {
                sig_mask = signal_frame_saved_mask();
            }
        }
    }

    // Ptrace/debugging state. The syscall ABI is node-local; WKI proxy routing
    // is represented through RemoteInfo and handled by userland debug brokers.
    uint64_t ptrace_tracer_pid = 0;
    uint64_t ptrace_options = 0;
    uint64_t ptrace_event_msg = 0;
    uint64_t ptrace_stop_address = 0;
    std::array<uint64_t, 4> ptrace_dr_addr{};
    uint64_t ptrace_dr6 = 0;
    uint64_t ptrace_dr7 = 0;
    ker::abi::ptrace::stop_reason ptrace_stop_reason = ker::abi::ptrace::stop_reason::NONE;
    uint32_t ptrace_stop_signal = 0;
    bool ptrace_traced = false;
    bool ptrace_stopped = false;
    bool ptrace_stop_pending = false;
    bool ptrace_single_step = false;
    bool ptrace_syscall_trace = false;
    bool ptrace_syscall_in_stop = false;
    bool ptrace_stop_uses_syscall_snapshot = false;
    cpu::GPRegs ptrace_syscall_regs{};
    gates::InterruptFrame ptrace_syscall_frame{};

    // Process time accounting (microseconds).
    uint64_t start_time_us{};               // Wall-clock time when task was created
    uint64_t user_time_us{};                // Cumulative user-mode CPU time
    uint64_t system_time_us{};              // Cumulative kernel-mode CPU time
    uint64_t child_user_time_us{};          // User-mode CPU time from children reaped by waitpid
    uint64_t child_system_time_us{};        // Kernel-mode CPU time from children reaped by waitpid
    uint64_t syscall_account_start_us{};    // Non-zero while this task is executing a syscall
    uint64_t precharged_syscall_time_us{};  // Syscall time already charged before the next scheduler tick

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

    // Wait channel: human-readable string for diagnostics plus a typed policy key.
    const char* wait_channel = nullptr;
    WaitChannelKind wait_channel_kind{WaitChannelKind::NONE};

    void set_wait_channel(const char* channel, WaitChannelKind kind = WaitChannelKind::GENERIC) {
        wait_channel = channel;
        wait_channel_kind = channel != nullptr ? kind : WaitChannelKind::NONE;
    }

    void clear_wait_channel() { set_wait_channel(nullptr, WaitChannelKind::NONE); }

    [[nodiscard]] auto wait_channel_is(WaitChannelKind kind) const -> bool { return wait_channel != nullptr && wait_channel_kind == kind; }

    // Published futex waiter node while this task is blocked in futex_wait().
    // Wake and abort/exit cleanup atomically clear this pointer; the path that
    // removes the node from the futex table owns the final delete.
    std::atomic<void*> futex_waiter{nullptr};

    // Intrusive list pointer for wait queue and dead list (zero allocations).
    Task* sched_next{};
    alignas(8) std::atomic<uint64_t> death_epoch{0};  // Epoch when task became DEAD

    // File descriptor table for the task (per-process model).
    // Sparse radix storage backs ordinary fd allocation, which is capped at
    // FD_TABLE_SIZE so per-fd CLOEXEC state remains representable.
    ker::util::RadixTree<void*> fd_table;

    // Per-fd close-on-exec bitmap (POSIX FD_CLOEXEC is per-fd, not per-file).
    FdCloexecBitmap fd_cloexec{};

    // List of task IDs waiting for this task to exit. When this task exits,
    // all tasks in this list will be rescheduled on their respective CPUs.
    ker::util::SmallVec<uint64_t, 4> awaitee_on_exit;

    // WKI: explicit task-local VFS rules layered over defaults from /etc/vfstab.
    ker::util::SmallVec<WkiVfsRule, 4> wki_vfs_rules;

    // Signal handler entry (matches Linux ABI struct sigaction layout).
    SignalHandlerTable sig_handlers{};

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
    ker::util::SmallVec<uint32_t, SUPPLEMENTARY_GROUPS_MAX> supplementary_groups;

    // Controlling terminal: index into PTY pool, or -1 if none.
    int controlling_tty = -1;

    // Exit status of this process (set when process exits, used by waitpid).
    int exit_status{};
    std::atomic<bool> jobctl_stopped{false};
    std::atomic<bool> jobctl_stop_pending{false};
    uint32_t jobctl_stop_signal = 0;
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

    // WKI: this task is a local wait/forwarding proxy for a remotely executed task.
    // It keeps its task PID for waitpid, signals, ptrace, and WKI routing, but is
    // not a user-visible process row in /proc root.
    bool wki_proxy_task = false;

    bool has_exited{};
    // Set after exit cleanup finishes; waitpid may reap only after this.
    std::atomic<bool> exit_notify_ready{false};
    std::atomic<bool> waited_on{false};  // Set when waitpid atomically claims the exit status.
    std::atomic<bool> zombie_resources_reclaiming{false};
    std::atomic<bool> zombie_resources_reclaimed{false};
    // True while waitpid has begun publishing wait metadata but deferred_task_switch
    // has not yet saved the syscall return context. Exit notification may wake
    // the task in this window, but must not write saved registers directly.
    std::atomic<bool> waitpid_publish_pending{false};
    bool deferred_task_switch{};  // Move to wait queue after syscall returns
    bool yield_switch{};          // Put task in expired queue instead of wait queue

    // Cooperative kernel-thread shutdown. Set centrally by the power
    // subsystem; DAEMON tasks observe it at scheduler block/sleep/yield points
    // and exit through the normal task-exit path.
    std::atomic<bool> kernel_shutdown_requested{false};

    // Set by reschedule_task_for_cpu when a wakeup fires while this task is
    // still currentTask (about to block via deferred_task_switch). Checked
    // under the RQ lock in deferred_task_switch to avoid lost wakeups.
    std::atomic<bool> wakeup_pending{false};

    // When true, a PROCESS task is at a safe voluntary blocking point (e.g. sti;hlt
    // in a syscall wait loop). The scheduler may preempt it as if it were a
    // DAEMON, saving and restoring kernel-mode context. Set before hlt, cleared after wake.
    std::atomic<bool> voluntary_block{false};

    [[nodiscard]] auto is_voluntary_blocked() const -> bool { return voluntary_block.load(std::memory_order_acquire); }

    void set_voluntary_blocked(bool value) { voluntary_block.store(value, std::memory_order_release); }

    bool preempt_pending = false;
    bool in_signal_handler{};  // Set to true when a signal handler frame is being delivered
    bool do_sigreturn{};       // Set by sigreturn syscall; check_pending_signals restores context
    int8_t sched_nice = 0;     // POSIX nice value backing sched_weight for PROCESS tasks
    sched_queue sched_queue{sched_queue::NONE};
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
    HostnameBuffer wki_target_hostname{};

    // WKI: hostname of the logical submitter host whose filesystem should be
    // treated as /wki/host for this task.
    HostnameBuffer wki_submitter_hostname{};

    Context context{};

    // Current working directory (absolute path, "/" by default).
    PathBuffer cwd{'/', '\0'};

    // Per-process root directory (for pivot_root / chroot).
    // Path resolution prepends this to absolute paths when it differs from "/".
    PathBuffer root{'/', '\0'};

    // Executable path (set by exec, used by procfs /proc/self/exe).
    ExePathBuffer exe_path{};

    FxState fx_state;  // FPU/SSE register state (fxsave/fxrstor on context switch)

    void set_fd_cloexec(unsigned fd) {
        if (fd < FD_TABLE_SIZE) {
            auto* word = std::next(fd_cloexec.data(), static_cast<ptrdiff_t>(fd / 64));
            *word |= (1ULL << (fd % 64));
        }
    }
    void clear_fd_cloexec(unsigned fd) {
        if (fd < FD_TABLE_SIZE) {
            auto* word = std::next(fd_cloexec.data(), static_cast<ptrdiff_t>(fd / 64));
            *word &= ~(1ULL << (fd % 64));
        }
    }
    [[nodiscard]] bool get_fd_cloexec(unsigned fd) const {
        if (fd >= FD_TABLE_SIZE) {
            return false;
        }
        const auto* word = std::next(fd_cloexec.data(), static_cast<ptrdiff_t>(fd / 64));
        return (*word & (1ULL << (fd % 64))) != 0;
    }

    [[nodiscard]] bool has_group(uint32_t group) const { return egid == group || supplementary_groups.contains(group); }

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

    // Acquire a lifetime reference while the caller already owns a registry or
    // dead-list lock. Unlike try_acquire(), this intentionally allows EXITING
    // and DEAD tasks so waitpid can consume zombies before scheduler GC.
    bool try_acquire_lifetime_ref() {
        uint32_t count = ref_count.load(std::memory_order_acquire);
        while (count > 0) {
            if (ref_count.compare_exchange_weak(count, count + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
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
};

[[nodiscard]] inline auto process_pid(const Task& task) -> uint64_t { return task.owner_pid != 0 ? task.owner_pid : task.pid; }

[[nodiscard]] inline auto same_thread_group(const Task& lhs, const Task& rhs) -> bool { return process_pid(lhs) == process_pid(rhs); }

[[nodiscard]] inline auto same_thread_group(const Task& task, uint64_t process_pid_value) -> bool {
    return process_pid(task) == process_pid_value;
}

[[nodiscard]] inline auto process_visible(const Task& task) -> bool { return !task.is_thread && !task.wki_proxy_task; }

auto get_next_pid() -> uint64_t;
void destroy_unpublished_user_thread(Task* task);
[[nodiscard]] auto clone_lazy_vmem_ranges(Task& dst, Task& src) -> bool;
void release_lazy_vmem_ranges(Task& task);

[[nodiscard]] inline auto task_waited_on(const Task& task) -> bool { return task.waited_on.load(std::memory_order_acquire); }

inline void task_clear_waited_on(Task& task) { task.waited_on.store(false, std::memory_order_relaxed); }

[[nodiscard]] inline auto task_try_mark_waited_on(Task& task) -> bool {
    bool expected = false;
    return task.waited_on.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire);
}

inline void task_accumulate_waited_child_times(Task& parent, const Task& child) {
    if (child.parent_pid != parent.pid) {
        return;
    }

    parent.child_user_time_us += child.user_time_us + child.child_user_time_us;
    parent.child_system_time_us += child.system_time_us + child.child_system_time_us;
}

[[nodiscard]] inline auto task_rusage_user_time_us(const Task& task) -> uint64_t { return task.user_time_us + task.child_user_time_us; }

[[nodiscard]] inline auto task_rusage_system_time_us(const Task& task) -> uint64_t {
    return task.system_time_us + task.child_system_time_us;
}

inline void task_clear_waitpid_block_state(Task& task) {
    task.waiting_for_pid = 0;
    task.wait_options = 0;
    task.wait_status_user_addr = 0;
    task.wait_status_phys_addr = 0;
    task.wait_rusage_user_addr = 0;
    task.wait_rusage_phys_addr = 0;
    task.wait_resume_rip_user_addr = 0;
    task.wait_resume_rip_phys_addr = 0;
    task.wait_resume_rsp_user_addr = 0;
    task.wait_resume_rsp_phys_addr = 0;
    task.clear_wait_channel();
}

#ifdef WOS_SELFTEST
auto task_selftest_fd_clone_failure_releases_refs() -> bool;
auto task_selftest_destroy_unpublished_user_thread_releases_refs() -> bool;
auto task_selftest_waited_on_claim_is_single_winner() -> bool;
auto task_selftest_waitpid_block_state_clear_resets_fields() -> bool;
#endif
}  // namespace ker::mod::sched::task
