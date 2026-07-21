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

namespace ker::loader::elf {
struct ElfFileView;
struct ElfLoadOptions;
}  // namespace ker::loader::elf

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

template <size_t Capacity>
class FixedFdTable {
   public:
    FixedFdTable() = default;

    FixedFdTable(const FixedFdTable&) = delete;
    auto operator=(const FixedFdTable&) -> FixedFdTable& = delete;

    FixedFdTable(FixedFdTable&&) = delete;
    auto operator=(FixedFdTable&&) -> FixedFdTable& = delete;

    [[nodiscard]] auto insert(uint64_t key, void* value) -> bool {
        if (key >= Capacity) {
            return false;
        }

        void*& slot = slot_at(static_cast<size_t>(key));
        if (slot == nullptr && value != nullptr) {
            ++m_size;
        } else if (slot != nullptr && value == nullptr) {
            --m_size;
        }
        slot = value;
        return true;
    }

    auto remove(uint64_t key) -> void* {
        if (key >= Capacity) {
            return nullptr;
        }

        void*& slot = slot_at(static_cast<size_t>(key));
        void* const old = slot;
        if (old != nullptr) {
            slot = nullptr;
            --m_size;
        }
        return old;
    }

    [[nodiscard]] auto lookup(uint64_t key) const -> void* {
        if (key >= Capacity) {
            return nullptr;
        }
        return slot_at(static_cast<size_t>(key));
    }

    [[nodiscard]] auto find_first_unset_below(uint64_t start_key, uint64_t limit) const -> uint64_t {
        if (start_key >= limit || start_key >= Capacity) {
            return UINT64_MAX;
        }

        uint64_t const SEARCH_LIMIT = limit < Capacity ? limit : Capacity;
        for (uint64_t key = start_key; key < SEARCH_LIMIT; ++key) {
            if (slot_at(static_cast<size_t>(key)) == nullptr) {
                return key;
            }
        }
        return UINT64_MAX;
    }

    template <typename Fn>
    void for_each(const Fn& fn) const {
        for (size_t i = 0; i < Capacity; ++i) {
            void* const value = slot_at(i);
            if (value != nullptr) {
                fn(static_cast<uint64_t>(i), value);
            }
        }
    }

    [[nodiscard]] auto size() const -> size_t { return m_size; }
    [[nodiscard]] auto empty() const -> bool { return m_size == 0; }

   private:
    [[nodiscard]] auto slot_at(size_t index) -> void*& {
        // Capacity-bound callers validate keys before indexing the table.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        return m_slots[index];
    }

    [[nodiscard]] auto slot_at(size_t index) const -> void* {
        // Capacity-bound callers validate keys before indexing the table.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        return m_slots[index];
    }

    std::array<void*, Capacity> m_slots{};
    size_t m_size{};
};

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
    static constexpr size_t XSAVE_AREA_SIZE = cpu::XSAVE_STATIC_AREA_SIZE;
    static constexpr size_t ALIGNMENT_SLACK = 63;
    using RawBuffer = std::array<uint8_t, XSAVE_AREA_SIZE + ALIGNMENT_SLACK>;

    RawBuffer raw{};
    bool saved = false;        // true once raw[] contains a valid xsave/fxsave image
    bool live_saved = false;   // true while raw[] already protects the task's current user-visible state
    bool initialized = false;  // true once hardware FPU state has been initialized for this task

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
    auto initialize_process_image(const ker::loader::elf::ElfFileView& elf, const ker::loader::elf::ElfLoadOptions& options) -> bool;

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
    using InterpPathBuffer = std::array<char, 256>;
    using HostnameBuffer = std::array<char, WKI_TARGET_HOSTNAME_MAX>;
    using FdTable = FixedFdTable<FD_TABLE_SIZE>;
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
    // Large dynamic executables retain the opened file instead of a
    // contiguous full-file buffer. elf_buffer then contains only bounded ELF
    // header/program-header metadata for ptrace and procfs.
    ker::vfs::File* exec_image_file{};
    uint64_t exec_image_size{};
    uint64_t program_header_addr{};  // Virtual address of program headers (AT_PHDR)
    uint64_t elf_header_addr{};      // Virtual address of ELF header (AT_EHDR)
    uint64_t interp_base = 0;        // Load base of dynamic linker (AT_BASE), 0 if statically linked
    // Task construction records PT_INTERP without doing VFS I/O. Runtime
    // process creators load it only after publishing fatal-exit ownership.
    InterpPathBuffer pending_interp_path{};
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
    uint64_t waiting_for_pid{};            // Encoded waitpid selector: direct PID, any-child sentinel, or process group
    int32_t wait_options{};                // waitpid option bits active for the current block
    uint64_t wait_status_user_addr{};      // Userspace virtual address of status variable (for waitpid)
    uint64_t wait_status_phys_addr{};      // Last translated physical address (debug; may change after COW)
    uint64_t wait_rusage_user_addr{};      // Userspace virtual address of rusage struct (for wait3/wait4)
    uint64_t wait_rusage_phys_addr{};      // Last translated physical address (debug; may change after COW)
    uint64_t wait_resume_rip_user_addr{};  // Userspace RIP expected when returning from waitpid
    uint64_t wait_resume_rip_phys_addr{};  // Last translated physical address for wait_resume_rip_user_addr
    uint64_t wait_resume_rsp_user_addr{};  // Userspace RSP expected when returning from waitpid
    uint64_t wait_resume_rsp_phys_addr{};  // Last translated physical address for wait_resume_rsp_user_addr (debug; may change after COW)
    uint64_t waitpid_last_repair_us{};     // Last fallback waitpid repair attempt while still blocked.
    std::atomic<bool> waitpid_completion_claimed{false};  // One scheduler/exit completion per blocked waitpid.

    // Signal state.
    std::atomic<uint64_t> sig_pending{0};   // Bit N = signal N+1 is pending, signals 1-64
    std::atomic<uint64_t> sig_mask{0};      // Bitmask of blocked signals
    std::atomic<uint64_t> sig_mask_seq{0};  // Monotonic version mirrored into the userspace TCB cache.
    uint64_t sigsuspend_saved_mask{};       // Mask to restore after a sigsuspend wake.
    uint64_t sigaltstack_sp{};              // Base address for sigaltstack(2), or 0 when disabled.
    uint64_t sigaltstack_size{};            // Size of the alternate signal stack.
    uint32_t sigaltstack_flags = 2;         // SS_DISABLE by default.
    uint32_t parent_death_signal{};         // PR_SET_PDEATHSIG state.
    int32_t dumpable = 1;                   // PR_GET/SET_DUMPABLE state.
    bool sigsuspend_active{};               // Current signal mask is temporary for sigsuspend.

    [[nodiscard]] auto signal_pending_bits(std::memory_order order = std::memory_order_acquire) const -> uint64_t {
        return sig_pending.load(order);
    }

    [[nodiscard]] auto signal_mask_bits(std::memory_order order = std::memory_order_acquire) const -> uint64_t {
        return sig_mask.load(order);
    }

    [[nodiscard]] auto signal_deliverable_bits(std::memory_order order = std::memory_order_acquire) const -> uint64_t {
        return signal_pending_bits(order) & ~signal_mask_bits(order);
    }

    void signal_pending_store(uint64_t bits, std::memory_order order = std::memory_order_release) { sig_pending.store(bits, order); }

    void signal_mask_store(uint64_t bits, std::memory_order order = std::memory_order_release) { sig_mask.store(bits, order); }

    void signal_add_pending_mask(uint64_t mask, std::memory_order order = std::memory_order_acq_rel) { sig_pending.fetch_or(mask, order); }

    void signal_clear_pending_mask(uint64_t mask, std::memory_order order = std::memory_order_acq_rel) {
        sig_pending.fetch_and(~mask, order);
    }

    void signal_add_mask_bits(uint64_t mask, std::memory_order order = std::memory_order_acq_rel) { sig_mask.fetch_or(mask, order); }

    [[nodiscard]] auto signal_mask_next_seq(std::memory_order order = std::memory_order_acq_rel) -> uint64_t {
        return sig_mask_seq.fetch_add(1, order) + 1;
    }

    auto signal_frame_saved_mask() -> uint64_t {
        if (!sigsuspend_active) {
            return signal_mask_bits();
        }
        uint64_t const SAVED = sigsuspend_saved_mask;
        sigsuspend_active = false;
        sigsuspend_saved_mask = 0;
        return SAVED;
    }

    auto has_interrupting_signal_pending() -> bool {
        if (process_exit_requested.load(std::memory_order_acquire)) {
            return true;
        }

        for (;;) {
            uint64_t const DELIVERABLE = signal_deliverable_bits();
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

            signal_clear_pending_mask(1ULL << IDX);
            if (sigsuspend_active) {
                signal_mask_store(signal_frame_saved_mask());
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
    // Fixed descriptor storage keeps hot open/close paths allocation-free.
    FdTable fd_table;

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
    // Monotonic publication fence for teardown decisions. Once true, the Task
    // has been visible through scheduler registries/runqueues and must never
    // use an unpublished-process destruction path, even after it is detached.
    std::atomic<bool> scheduler_published{false};
    mod::sys::Spinlock fd_table_lock;
    mod::sys::Spinlock exit_waiters_lock;

    uint16_t program_header_count = 0;     // Number of program headers (AT_PHNUM)
    uint16_t program_header_ent_size = 0;  // Size of each program header entry (AT_PHENT)

    bool cpu_pinned = false;  // When true, scheduler will not migrate this task to another CPU
    bool domain_hard = false;
    bool has_run{};                     // True once the task has run at least once
    bool is_elf_buffer_shared = false;  // True if buffer is from shared cache, don't delete[]
    bool elf_buffer_complete = true;    // False when elf_buffer is bounded metadata only

    // True when this Task is a userspace thread (shares pagemap with owning process).
    // Thread exits must NOT free the pagemap or close shared FDs.
    bool is_thread = false;

    // Monotonic marker set before publishing any task that shares this task's
    // user pagemap. Shared-VM publication keeps false negatives impossible;
    // retaining a true value after the last sibling exits is conservative.
    std::atomic<bool> shares_user_pagemap{false};

    // WKI: prefer inline delivery for remote compute placement (V2 A6.4).
    bool wki_prefer_inline = false;

    // WKI: set by richer spawn paths after they already attempted remote placement.
    // The legacy scheduler hook must skip those tasks to avoid losing exec context.
    bool wki_skip_legacy_placement = false;

    // WKI: this task is a local wait/forwarding proxy for a remotely executed task.
    // It keeps its task PID for waitpid, signals, ptrace, and WKI routing, but is
    // not a user-visible process row in /proc root.
    bool wki_proxy_task = false;

    // A process-creation syscall keeps its not-yet-published child here while
    // kernel waits may reschedule the creator. Fatal exit does not unwind the
    // syscall stack, so exit cleanup must be able to recover this ownership.
    // The slot owns one lifetime reference in addition to the creator's initial
    // reference until publication or explicit destruction clears the slot.
    std::atomic<Task*> owned_unpublished_process{nullptr};
    // Synchronous child teardown can block in VFS/address-space cleanup. Fatal
    // and group-exit handoffs defer while this is set, leaving their pending
    // request intact until the owner slot and child are fully retired.
    std::atomic<bool> unpublished_teardown_in_progress{false};

    bool exit_in_progress{};
    bool has_exited{};
    std::atomic<bool> process_exit_requested{false};
    std::atomic<int32_t> requested_process_exit_status{0};
    std::atomic<int32_t> requested_process_exit_wait_status{0};
    // Set after user-visible exit state is published: descriptors are closed,
    // status/accounting are stable, and waitpid may reap while later memory
    // cleanup continues.
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

    // Coalesce concurrent cross-CPU reschedule requests behind one queue
    // transition leader. A task may be woken simultaneously by unrelated
    // event producers targeting different CPUs; distinct target runqueue
    // locks alone cannot serialize that ownerless remove/insert window.
    std::atomic<uint64_t> reschedule_requested_cpu{0};
    std::atomic<bool> reschedule_pending{false};
    std::atomic<bool> reschedule_in_progress{false};

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
    uint16_t cwd_len = 1;

    // Per-process root directory (for pivot_root / chroot).
    // Path resolution prepends this to absolute paths when it differs from "/".
    PathBuffer root{'/', '\0'};
    uint16_t root_len = 1;

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
[[nodiscard]] auto claim_unpublished_process(Task* owner, Task* child) -> bool;
[[nodiscard]] auto release_unpublished_process(Task* owner, Task* child) -> bool;
// Transfers the slot-owned lifetime reference to the caller. The returned
// pointer must be passed to destroy_unpublished_process(), which consumes it.
[[nodiscard]] auto take_unpublished_process(Task* owner) -> Task*;
[[nodiscard]] auto destroy_unpublished_process(Task* task) -> bool;
// Destroy an exclusively owned child while keeping it discoverable through
// owner until all potentially blocking cleanup has completed.
[[nodiscard]] auto destroy_owned_unpublished_process(Task* owner, Task* child) -> bool;
// Complete the PT_INTERP stage deliberately omitted from Task's constructor.
// Runtime callers must first transfer the ELF/kernel stack to the Task and
// claim an unpublished-process owner that survives any voluntary VFS wait.
[[nodiscard]] auto complete_unpublished_process_construction(Task* task) -> bool;
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
    task.waitpid_last_repair_us = 0;
    task.clear_wait_channel();
    task.waitpid_completion_claimed.store(false, std::memory_order_release);
}

[[nodiscard]] inline auto task_try_claim_waitpid_completion(Task& task) -> bool {
    bool expected = false;
    return task.waitpid_completion_claimed.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire);
}

inline void task_release_waitpid_completion_claim(Task& task) { task.waitpid_completion_claimed.store(false, std::memory_order_release); }

#ifdef WOS_SELFTEST
auto task_selftest_fd_clone_failure_releases_refs() -> bool;
auto task_selftest_destroy_unpublished_user_thread_releases_refs() -> bool;
auto task_selftest_unpublished_process_owner_releases_resources() -> bool;
auto task_selftest_owned_unpublished_process_teardown_releases_resources() -> bool;
auto task_selftest_published_process_refuses_unpublished_teardown() -> bool;
auto task_selftest_waited_on_claim_is_single_winner() -> bool;
auto task_selftest_waitpid_block_state_clear_resets_fields() -> bool;
#endif
}  // namespace ker::mod::sched::task
