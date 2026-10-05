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
#include <platform/ktime/interval_timer_model.hpp>
#include <platform/ktime/timer_queue.hpp>
#include <platform/sched/fd_table.hpp>
#include <platform/sched/frame_class.hpp>
#include <platform/sched/job_control.hpp>
#include <platform/sched/migration_guard.hpp>
#include <platform/sched/preemption_diagnostics.hpp>
#include <platform/sched/resource_lifetime.hpp>
#include <platform/sched/resource_model.hpp>
#include <platform/sched/resource_process.hpp>
#include <platform/sched/service_resource.hpp>
#include <platform/sched/session.hpp>
#include <platform/sched/shutdown_ownership.hpp>
#include <platform/sched/signal_pending.hpp>
#include <platform/sched/threading.hpp>
#include <platform/security/credential_store.hpp>
#include <platform/security/terminal.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/radix_tree.hpp>
#include <util/smallvec.hpp>
#include <vfs/directory_anchor.hpp>
// Avoid pulling in in-tree std headers into kernel translation units from here.

namespace ker::vfs {
struct File;
class FdReservation;
}  // namespace ker::vfs

namespace ker::net::wki {
class IpcFdSnapshot;
class WkiIpcSubmissionGuard;
class TaskSubmitBuffer;
class TaskFilesystemPreparation;
}  // namespace ker::net::wki

namespace ker::loader::elf {
struct ElfFileView;
struct ElfLoadOptions;
}  // namespace ker::loader::elf

namespace ker::syscall::process {
class PendingSignalTarget;
class PreparedNativeExecImage;
class ExecRequestArguments;
struct WorkerExecHandoff;
}  // namespace ker::syscall::process

namespace ker::mod::sched {
struct IntrusiveTaskList;
}

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

struct Task;

// Storage for the process-owned parent/child registry.  These nodes live in
// Task so exit, signal, and trap paths can publish lifecycle changes without
// allocating.  All non-atomic fields are protected by child_events.cpp's
// lifecycle lock; scheduler code may only inspect child_membership_state.
enum class ChildMembershipState : uint8_t {
    UNLINKED,
    PUBLISHING,
    LIVE,
    ZOMBIE,
};

enum class ChildEventKind : uint8_t {
    EXIT,
    PTRACE_STOP,
    JOB_CONTROL_STOP,
    CONTINUED,
};

enum class ChildEventAudience : uint8_t {
    PARENT,
    TRACER,
};

struct ChildWaitSelector {
    uint64_t value{};
    session::Origin group_origin{};

    // Existing PID/any selectors and explicitly local group selectors have no
    // foreign origin. Group resolution supplies the complete identity.
    constexpr ChildWaitSelector(uint64_t encoded = 0, session::Origin origin = {}) : value(encoded), group_origin(origin) {}
    constexpr auto operator==(const ChildWaitSelector&) const -> bool = default;
};

struct ChildEvent;

// Stable intrusive claim storage. External users must retain this record until
// explicit cancellation/commit; local users embed it in the retained waiter.
// Links are protected by the child lifecycle lock, never by the executor Task.
struct ChildEventClaim {
    explicit ChildEventClaim(Task* local_waiter = nullptr) : waiter(local_waiter) {}
    ChildEventClaim(const ChildEventClaim&) = delete;
    auto operator=(const ChildEventClaim&) -> ChildEventClaim& = delete;
    ChildEventClaim(ChildEventClaim&&) = delete;
    auto operator=(ChildEventClaim&&) -> ChildEventClaim& = delete;

    ChildEvent* event{};
    Task* const waiter;
};

struct ChildEvent {
    Task* queue_owner{};
    Task* subject{};
    ChildEvent* next{};
    ChildEventClaim* claimed_by{};
    uint64_t sequence{};
    uint64_t claim_cookie{};
    uint64_t subject_pid{};
    uint64_t process_group{};
    session::Origin group_origin{};
    uint64_t user_time_us{};
    uint64_t system_time_us{};
    int32_t status{};
    ChildEventKind kind{ChildEventKind::EXIT};
    ChildEventAudience audience{ChildEventAudience::PARENT};
    bool queued{};
};

struct ChildWaitRegistration {
    Task* queue_owner{};
    Task* waiter{};
    ChildWaitRegistration* next{};
    ChildWaitSelector selector{};
    uint64_t generation{};
    int32_t options{};
    bool registered{};
    bool notified{};
};

struct Context {
    uint64_t syscall_kernel_stack;
    uint64_t syscall_scratch_area;  // Small scratch area for syscall handler (RIP, RSP, RFLAGS, DS, ES)

    cpu::GPRegs regs;

    uint64_t int_no;
    uint64_t error_code;

    gates::InterruptFrame frame;

    // Producer-time classification for frame.  This is deliberately stored
    // rather than reconstructed from mutable state such as voluntary_block.
    SavedFrameClass saved_frame_class{SavedFrameClass::INVALID};
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
    Task(const char* name, uint64_t elf_start, size_t elf_size, uint64_t kernel_rsp, TaskType type);
    auto initialize_process_image(const uint8_t* elf_data, size_t elf_size) -> bool;
    auto initialize_process_image(const ker::loader::elf::ElfFileView& elf, const ker::loader::elf::ElfLoadOptions& options) -> bool;

    // Factory for kernel threads (DAEMON tasks).
    // entryFunc: [[noreturn]] void func() - the kernel thread body.
    static auto create_kernel_thread(const char* name, void (*entry_func)()) -> Task*;

    // Factory for userspace threads (PROCESS tasks that share the parent's pagemap).
    // tcbVaddr: virtual address of the mlibc TCB (becomes FS base / fsbase).
    // userSp:   prepared stack pointer (sys_prepare_stack pushed entry+user_arg below it).
    // enterThreadVa: virtual address of __mlibc_enter_thread in the process image.
    static auto create_user_thread(Task* parent, uint64_t tcb_vaddr, uint64_t user_sp, uint64_t enter_thread_va, uint64_t entry_va,
                                   uint64_t user_arg_va) -> Task*;

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
    struct FilesystemContext {
        // Null preserves the global namespace view and its compatibility root
        // prefix. Non-null retains an exact native root; never serialize it as
        // a pathname or install it without root-aware VFS admission.
        ker::vfs::DirectoryAnchorRef root_anchor;
        ker::vfs::DirectoryAnchorRef cwd_anchor;
        PathBuffer cwd{'/', '\0'};
        PathBuffer root{'/', '\0'};
        uint16_t cwd_len = 1;
        uint16_t root_len = 1;
    };
    using ExePathBuffer = std::array<char, EXE_PATH_MAX>;
    using InterpPathBuffer = std::array<char, 256>;
    using HostnameBuffer = std::array<char, WKI_TARGET_HOSTNAME_MAX>;
    using FdTable = FixedFdTable<FD_TABLE_SIZE>;
    static constexpr uint32_t WKI_TARGET_FLAG_STRICT = 1U << 0;
    static constexpr uint32_t WKI_TARGET_FLAG_LOCAL = 1U << 1;      // pin task to local node (skip remote placement)
    static constexpr uint32_t WKI_TARGET_FLAG_NOINHERIT = 1U << 2;  // don't propagate wki_target to child processes
    static constexpr uint32_t WKI_TARGET_FLAG_REMOTE = 1U << 3;     // prefer a remote node, falling back locally unless strict
    static constexpr uint32_t WKI_TARGET_FLAG_BALANCED = 1U << 4;   // balance across the local node and connected peers
    static constexpr uint32_t WKI_TARGET_FLAG_ONESHOT = 1U << 5;    // consume placement after the next successful exec attempt
    static constexpr uint32_t WKI_TARGET_FLAGS_ALL = WKI_TARGET_FLAG_STRICT | WKI_TARGET_FLAG_LOCAL | WKI_TARGET_FLAG_NOINHERIT |
                                                     WKI_TARGET_FLAG_REMOTE | WKI_TARGET_FLAG_BALANCED | WKI_TARGET_FLAG_ONESHOT;
    static constexpr uint64_t PERSONALITY_ADDR_NO_RANDOMIZE = 0x0040000;
    // Production process placement is always randomized.  Keep the Linux ABI
    // value above for personality(2) compatibility, but never make a
    // deterministic layout the default (or a production fallback).
    static constexpr uint64_t DEFAULT_PERSONALITY = 0;

    // Lock-free lifecycle management (epoch-based reclamation).
    alignas(8) std::atomic<TaskState> state{TaskState::ACTIVE};

    TaskType type{TaskType::DAEMON};
    mm::paging::PageTable* pagemap{};
    // Usercopy acquires a short-lived access before faulting or pinning one
    // page. Pagemap replacement/teardown closes the gate and waits for those
    // accesses before publishing a new root or freeing the old hierarchy.
    std::atomic<uint32_t> usercopy_pagemap_accesses{0};
    std::atomic<bool> usercopy_pagemap_closing{false};
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

    // Authoritative process-lifecycle topology.  parent_pid remains the public
    // scalar ABI/cache; these links determine waitability and reparenting.
    std::atomic<ChildMembershipState> child_membership_state{ChildMembershipState::UNLINKED};
    // Monotonic for one publication attempt.  This lets the creator commit a
    // child that ran, exited, and was reaped between scheduler visibility and
    // the caller-side commit without treating successful publication as a
    // rollback failure.
    bool child_publication_established{};
    Task* child_parent{};
    Task* child_sibling_prev{};
    Task* child_sibling_next{};
    Task* child_list_head{};

    // Events and waiters owned by this process.  Each producer has dedicated
    // storage, so an unsafe-context publication never allocates or overwrites
    // an unconsumed event.  Stop/continue transitions apply backpressure when
    // their corresponding node is still queued.
    ChildEvent* child_event_head{};
    ChildEvent* child_event_tail{};
    // Lifecycle-lock protected. UINT64_MAX is exhausted, never reused; only
    // versioned distributed selection stops, ordinary local waits still work.
    uint64_t child_queue_version{1};
    ChildWaitRegistration* child_waiter_head{};
    ChildWaitRegistration child_wait_registration{};
    ChildEventClaim child_claim{this};
    ChildEvent child_parent_exit_event{};
    ChildEvent child_tracer_exit_event{};
    ChildEvent child_ptrace_stop_event{};
    ChildEvent child_job_stop_event{};
    ChildEvent child_continued_event{};
    // At most one status per audience/owner may be in the claim/usercopy
    // transaction.  Keeping this on the subject makes matching linear while
    // still allowing distinct parent and tracer owners to consume safely.
    ChildEvent* child_parent_claimed_event{};
    ChildEvent* child_tracer_claimed_event{};

    // Explicit ptrace ownership mirrors parent/child membership and lets
    // waitpid determine whether a tracer has potential events without a global
    // task scan.
    Task* child_tracer{};
    Task* child_tracee_prev{};
    Task* child_tracee_next{};
    Task* child_tracee_head{};

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
    uint64_t image_load_base{};      // Authoritative main-image runtime load bias
    uint64_t image_vaddr_start{};    // Authoritative main PT_LOAD extent [start,end)
    uint64_t image_vaddr_end{};
    uint64_t interp_base = 0;       // Load base of dynamic linker (AT_BASE), 0 if statically linked
    uint64_t interp_vaddr_start{};  // Authoritative interpreter PT_LOAD extent [start,end)
    uint64_t interp_vaddr_end{};
    // Userspace address of the protected 16-byte AT_RANDOM payload.  This is
    // internal metadata used to redact the bytes from coredumps; the value is
    // never part of a public task or coredump ABI.
    uint64_t at_random_addr{};
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

    uint64_t personality = DEFAULT_PERSONALITY;

    // Plain spinlocks disable task preemption without masking interrupts. This
    // keeps preemptible DAEMON/NAPI workers from being switched out while they
    // hold locks that other CPUs may spin on.
    // Hold timestamps/maxima are sampled only with WOS_PREEMPT_GUARD_TIMING;
    // zero denotes unavailable timing otherwise. Owner/depth stay unconditional.
    uint64_t preempt_disable_start_us = 0;
    uint64_t preempt_disable_max_us = 0;
    uint64_t preempt_disable_owner = 0;

    // Depth and owner CPU are published as one word so cross-CPU selectors
    // cannot observe a disabled task without its mandatory resume CPU.
    std::atomic<uint64_t> migration_guard_state{
        encode_migration_guard_state(MigrationGuardState{.depth = 0, .owner_cpu = MIGRATION_CPU_INVALID})};
    std::atomic<uint64_t> migration_disable_start_us{0};
    std::atomic<uint64_t> migration_disable_max_us{0};
    std::atomic<uint64_t> migration_disable_owner{0};

    // Coherent, allocation-free last-decision records. Timer and placement
    // paths publish one packed scalar; procfs/debug readers decode it later.
    mutable std::atomic<uint64_t> preemption_diagnostic{encode_preemption_diagnostic({})};
    mutable std::atomic<uint64_t> migration_diagnostic{encode_migration_diagnostic({})};

    [[nodiscard]] auto migration_state(std::memory_order order = std::memory_order_acquire) const -> MigrationGuardState {
        return decode_migration_guard_state(migration_guard_state.load(order));
    }

    [[nodiscard]] auto is_migration_disabled(std::memory_order order = std::memory_order_acquire) const -> bool {
        return migration_guard_disabled(migration_state(order));
    }

    // Signal state.
    std::atomic<uint64_t> sig_pending{0};  // Bit N = signal N+1 is pending, signals 1-64
    std::atomic<uint64_t> sig_pending_claimed{0};
    // Process-directed interval signals live on the retained canonical owner,
    // including while its main thread is alive but masks the signal.
    std::atomic<uint64_t> timer_sig_pending{0};
    std::atomic<uint64_t> timer_sig_claimed{0};
    static constexpr uint64_t TIMER_SIGNALS = (uint64_t{1} << 13) | (uint64_t{1} << 25) | (uint64_t{1} << 26);
    std::atomic<bool> main_thread_retired{false};
    std::atomic<uint64_t> sig_mask{0};             // Bitmask of blocked signals
    std::atomic<uint64_t> sig_mask_seq{0};         // Monotonic version mirrored into the userspace TCB cache.
    uint64_t sigsuspend_saved_mask{};              // Mask to restore after a sigsuspend wake.
    uint64_t sigaltstack_sp{};                     // Base address for sigaltstack(2), or 0 when disabled.
    uint64_t sigaltstack_size{};                   // Size of the alternate signal stack.
    uint32_t sigaltstack_flags = 2;                // SS_DISABLE by default.
    std::atomic<uint32_t> parent_death_signal{0};  // Child-owned PR_SET_PDEATHSIG state.
    bool sigsuspend_active{};                      // Current signal mask is temporary for sigsuspend.

    [[nodiscard]] auto signal_pending_bits(std::memory_order order = std::memory_order_acquire) const -> uint64_t {
        return sig_pending.load(order);
    }

    [[nodiscard]] auto signal_mask_bits(std::memory_order order = std::memory_order_acquire) const -> uint64_t {
        return sig_mask.load(order);
    }

    static constexpr uint64_t RETIRED_SHARED_SIGNALS = ~((uint64_t{1} << 8) | (uint64_t{1} << 18));

    [[nodiscard]] auto retired_signal_owner() const -> const Task* {
        auto* owner = credential_owner();
        return is_thread && owner != nullptr && owner->main_thread_retired.load(std::memory_order_acquire) ? owner : nullptr;
    }

    [[nodiscard]] auto signal_delivery_pending_bits() const -> uint64_t {
        auto* owner = retired_signal_owner();
        auto* timer_owner = credential_owner();
        return signal_pending_bits() | (owner != nullptr ? owner->signal_pending_bits() & RETIRED_SHARED_SIGNALS : 0) |
               (timer_owner != nullptr ? timer_owner->timer_sig_pending.load(std::memory_order_acquire) & TIMER_SIGNALS : 0);
    }

    [[nodiscard]] auto signal_deliverable_bits(std::memory_order order = std::memory_order_acquire) const -> uint64_t {
        uint64_t pending = signal_pending_bits(order) & ~sig_pending_claimed.load(order);
        if (auto* owner = retired_signal_owner(); owner != nullptr) {
            pending |= owner->signal_pending_bits(order) & ~owner->sig_pending_claimed.load(order) & RETIRED_SHARED_SIGNALS;
        }
        if (auto* owner = credential_owner(); owner != nullptr) {
            pending |= owner->timer_sig_pending.load(order) & ~owner->timer_sig_claimed.load(order) & TIMER_SIGNALS;
        }
        return pending & ~signal_mask_bits(order);
    }

    [[nodiscard]] auto claim_signal_for_delivery(uint64_t mask) -> SignalPendingClaim {
        auto local = SignalPendingClaim::acquire(sig_pending, sig_pending_claimed, mask);
        if (local) {
            return local;
        }
        auto* owner = credential_owner();
        if (retired_signal_owner() != nullptr && (mask & RETIRED_SHARED_SIGNALS) != 0) {
            auto retired = SignalPendingClaim::acquire(owner->sig_pending, owner->sig_pending_claimed, mask);
            if (retired) return retired;
        }
        if (owner != nullptr && (mask & TIMER_SIGNALS) != 0) {
            return SignalPendingClaim::acquire(owner->timer_sig_pending, owner->timer_sig_claimed, mask);
        }
        return {};
    }

    [[nodiscard]] auto signal_claim_matches(const SignalPendingClaim& claim, uint64_t mask) const -> bool {
        return claim.is_for(sig_pending, mask);
    }

    [[nodiscard]] auto consume_signal_for_delivery(uint64_t mask) -> bool {
        auto claim = claim_signal_for_delivery(mask);
        return claim.commit();
    }

    void signal_pending_store(uint64_t bits, std::memory_order order = std::memory_order_release) { sig_pending.store(bits, order); }

    void signal_mask_store(uint64_t bits, std::memory_order order = std::memory_order_release) { sig_mask.store(bits, order); }
    [[nodiscard]] auto signal_take_pending_mask(uint64_t bits) -> uint64_t {
        return sig_pending.fetch_and(~bits, std::memory_order_acq_rel) & bits;
    }

    // New signal generation cancels opposing job-control signals atomically.
    // Storage/retry below intentionally does not repeat generation semantics.
    void signal_generate_pending_mask(uint64_t mask);
    // Controller-only storage: opposing-bit cancellation without fanout.
    void signal_generate_pending_mask_local(uint64_t mask, bool enqueue = true);
    void signal_restore_job_control_pending(uint64_t bits);

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

    [[nodiscard]] auto exec_quiescence_requested() const -> bool {
        auto const* owner = credential_owner();
        return is_thread && owner != nullptr && owner->resource_exec_quiescence_epoch.load(std::memory_order_acquire) != 0;
    }

    auto has_interrupting_signal_pending() -> bool {
        if (process_exit_requested.load(std::memory_order_acquire) || pending_exec_handoff.load(std::memory_order_acquire) != nullptr ||
            exec_quiescence_requested()) {
            return true;
        }

        for (;;) {
            uint64_t const DELIVERABLE = signal_deliverable_bits();
            if (DELIVERABLE == 0) {
                return false;
            }

            int const SIGNO = __builtin_ctzll(DELIVERABLE) + 1;
            auto const IDX = static_cast<unsigned>(SIGNO - 1);
            auto const HANDLER = signal_handler_snapshot(IDX).handler;
            bool const DEFAULT_IGNORED = SIGNO == 17 || SIGNO == 23 || SIGNO == 28 || SIGNO == 18;
            bool const IGNORE = (HANDLER == 0 && DEFAULT_IGNORED) || HANDLER == 1;
            if (!IGNORE) {
                return true;
            }

            bool const CONSUMED = consume_signal_for_delivery(1ULL << IDX);
            if (CONSUMED && sigsuspend_active) {
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
    std::atomic<bool> ptrace_stopped{false};
    bool ptrace_stop_pending = false;
    bool ptrace_single_step = false;
    bool ptrace_syscall_trace = false;
    bool ptrace_syscall_in_stop = false;
    bool ptrace_stop_uses_syscall_snapshot = false;
    cpu::GPRegs ptrace_syscall_regs{};
    gates::InterruptFrame ptrace_syscall_frame{};

    [[nodiscard]] auto ptrace_termination_pending() const -> bool {
        constexpr uint64_t KILL_MASK = uint64_t{1} << 8;
        return (signal_pending_bits() & KILL_MASK) != 0 || process_exit_requested.load(std::memory_order_acquire) ||
               exit_in_progress.load(std::memory_order_acquire);
    }

    // Stop-event ownership is independent of execution admission. Fatal delivery
    // must not wait for a tracer to consume even an already claimed stop event.
    [[nodiscard]] auto ptrace_stop_blocks_execution() const -> bool {
        return ptrace_stopped.load(std::memory_order_acquire) && !ptrace_termination_pending();
    }

    // Process time accounting (microseconds).
    std::atomic<bool> cpu_accounting_finalized{false};
    uint64_t start_time_us{};         // Wall-clock time when task was created
    uint64_t user_time_us{};          // Cumulative user-mode CPU time
    uint64_t system_time_us{};        // Cumulative kernel-mode CPU time
    uint64_t child_user_time_us{};    // User-mode CPU time from children reaped by waitpid
    uint64_t child_system_time_us{};  // Kernel-mode CPU time from children reaped by waitpid
    // Visible observations already charged by an earlier execution/source queue.
    // Never include these in remote-completion segment deltas.
    uint64_t imported_child_user_time_us{};
    uint64_t imported_child_system_time_us{};
    uint64_t syscall_account_start_us{};    // Non-zero while this task is executing a syscall
    uint64_t precharged_syscall_time_us{};  // Syscall time already charged before the next scheduler tick

    // ITIMER_REAL state (microseconds, 0 = not armed).

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
    Task* sched_previous{};
    std::atomic<IntrusiveTaskList*> sched_list_owner{nullptr};
    time::TimerNode wait_timer{};
    alignas(8) std::atomic<uint64_t> death_epoch{0};  // Epoch when task became DEAD

    // File descriptor table for the task (per-process model).
    // Fixed descriptor storage keeps hot open/close paths allocation-free.
    FdTable fd_table;

    // Per-fd close-on-exec bitmap (POSIX FD_CLOEXEC is per-fd, not per-file).
    FdCloexecBitmap fd_cloexec{};

    // WKI: explicit task-local VFS rules layered over defaults from /etc/vfstab.
    ker::util::SmallVec<WkiVfsRule, 4> wki_vfs_rules;

    uint32_t domain_id = 0;
    uint32_t wki_target_flags = 0;

    // WKI: when non-zero, this task is a proxy for a remote task.
    // The proxy stays in WAITING state until the remote task completes.
    uint32_t wki_proxy_task_id = 0;

   private:
    friend struct ker::mod::sched::resource::CredentialAccess;
    // Trusted kernel/boot construction is explicit. Userspace children replace
    // this before publication; threads resolve the retained process store.
    security::CredentialStore m_credentials{security::Snapshot::bootstrap_root()};
    // Process-wide file creation mask; threads use their retained owner.
    std::atomic<uint32_t> m_creation_mask{022};

   public:
    [[nodiscard]] auto credential_owner() -> Task* { return is_thread ? resource_owner.get() : this; }
    [[nodiscard]] auto credential_owner() const -> const Task* { return is_thread ? resource_owner.get() : this; }
    [[nodiscard]] auto credential_observation() const -> security::CredentialStore::Observation {
        auto* owner = credential_owner();
        return owner != nullptr ? owner->m_credentials.read() : security::CredentialStore::Observation{};
    }
    [[nodiscard]] auto credentials() const -> security::Snapshot { return credential_observation().credentials; }
    [[nodiscard]] auto creation_mask() const -> uint32_t {
        auto* owner = credential_owner();
        return owner != nullptr ? owner->m_creation_mask.load(std::memory_order_relaxed) : 0777U;
    }
    [[nodiscard]] auto exchange_creation_mask(uint32_t mask) -> uint32_t {
        auto* owner = credential_owner();
        // The mask publishes no other state. An exchange supplies the exact
        // previous value even when two process threads update it concurrently.
        return owner != nullptr ? owner->m_creation_mask.exchange(mask & 0777U, std::memory_order_relaxed) : 0777U;
    }

   private:
    mutable mod::sys::Spinlock m_signal_handlers_lock;
    SignalHandlerTable m_signal_handlers{};
    mod::sys::Mutex m_signal_action_mutex;

   public:
    // Syscall writers and exec reset serialize outside the short store lock.
    // The caller's retained resource owner keeps both locks and the store alive.
    [[nodiscard]] auto signal_action_mutex() -> mod::sys::Mutex& { return credential_owner()->m_signal_action_mutex; }
    [[nodiscard]] auto signal_handler_snapshot(unsigned index) const -> SigHandler {
        auto* owner = credential_owner();
        if (owner == nullptr) {
            return {};
        }
        uint64_t const IRQF = owner->m_signal_handlers_lock.lock_irqsave();
        auto const HANDLER = owner->m_signal_handlers.at(index);
        owner->m_signal_handlers_lock.unlock_irqrestore(IRQF);
        return HANDLER;
    }
    [[nodiscard]] auto signal_handlers_snapshot() const -> SignalHandlerTable {
        auto* owner = credential_owner();
        if (owner == nullptr) {
            return {};
        }
        uint64_t const IRQF = owner->m_signal_handlers_lock.lock_irqsave();
        auto const HANDLERS = owner->m_signal_handlers;
        owner->m_signal_handlers_lock.unlock_irqrestore(IRQF);
        return HANDLERS;
    }
    // Only unpublished process construction or final owner teardown; never a
    // worker-local copy. Thread readers always resolve the retained owner.
    void initialize_signal_handlers(const SignalHandlerTable& handlers) {
        uint64_t const IRQF = m_signal_handlers_lock.lock_irqsave();
        m_signal_handlers = handlers;
        m_signal_handlers_lock.unlock_irqrestore(IRQF);
    }
    // Runtime writers hold signal_action_mutex(); readers need only a snapshot.
    void publish_signal_handler(unsigned index, SigHandler handler) {
        auto* owner = credential_owner();
        uint64_t const IRQF = owner->m_signal_handlers_lock.lock_irqsave();
        owner->m_signal_handlers.at(index) = handler;
        owner->m_signal_handlers_lock.unlock_irqrestore(IRQF);
    }
    // Successful exec only, with signal_action_mutex() held. Pending signals
    // and the current blocked mask survive image replacement. No old handler,
    // restorer or alternate-stack address may be used by the new image.
    void reset_signal_state_for_exec() {
        auto* owner = credential_owner();
        uint64_t const IRQF = owner->m_signal_handlers_lock.lock_irqsave();
        for (auto& handler : owner->m_signal_handlers) {
            constexpr uint64_t IGNORED_HANDLER = 1;
            handler = {.handler = handler.handler == IGNORED_HANDLER ? IGNORED_HANDLER : 0, .flags = 0, .restorer = 0, .mask = 0};
        }
        owner->m_signal_handlers_lock.unlock_irqrestore(IRQF);
        in_signal_handler = false;
        do_sigreturn = false;
        sigsuspend_active = false;
        sigsuspend_saved_mask = 0;
        sigaltstack_sp = 0;
        sigaltstack_size = 0;
        sigaltstack_flags = 2;  // SS_DISABLE
    }

   private:
    mutable mod::sys::Spinlock m_job_identity_lock;
    session::Identity m_job_identity{};

   public:
    [[nodiscard]] auto job_identity() const -> session::Identity {
        auto* owner = credential_owner();
        if (owner == nullptr) return {};
        uint64_t const IRQF = owner->m_job_identity_lock.lock_irqsave();
        auto result = owner->m_job_identity;
        if (result.session_id == 0) {
            result.session_id = owner->pid;
            result.origin = {};
        }
        if (result.pgid == 0) {
            result.pgid = owner->pid;
            result.group_origin = {};
        }
        owner->m_job_identity_lock.unlock_irqrestore(IRQF);
        return result;
    }
    // Only for unpublished construction. Threads subsequently resolve their
    // retained process owner, so later process mutations need no sibling scan.
    void initialize_job_identity(session::Identity identity) {
        uint64_t const IRQF = m_job_identity_lock.lock_irqsave();
        m_job_identity = identity;
        m_job_identity_lock.unlock_irqrestore(IRQF);
    }
    // Runtime callers serialize through session coordination; group commits
    // also hold the child-event lifecycle lock before taking this store lock.
    void publish_job_identity(session::Identity identity) {
        auto* owner = credential_owner();
        if (owner != nullptr) owner->initialize_job_identity(identity);
    }
    void mark_exec_completed() {
        auto* owner = credential_owner();
        if (owner == nullptr) return;
        uint64_t const IRQF = owner->m_job_identity_lock.lock_irqsave();
        owner->m_job_identity.exec_completed = true;
        owner->m_job_identity_lock.unlock_irqrestore(IRQF);
    }
    [[nodiscard]] auto controlling_terminal() const -> security::TerminalIdentity { return job_identity().terminal; }
    void set_controlling_terminal(security::TerminalIdentity identity, uint64_t generation = 0) {
        auto* owner = credential_owner();
        if (owner == nullptr) return;
        uint64_t const IRQF = owner->m_job_identity_lock.lock_irqsave();
        owner->m_job_identity.terminal = identity;
        owner->m_job_identity.terminal_generation = identity.value != 0 ? generation : 0;
        owner->m_job_identity_lock.unlock_irqrestore(IRQF);
    }
    auto clear_controlling_terminal_if(security::TerminalIdentity identity, uint64_t generation = 0) -> bool {
        auto* owner = credential_owner();
        if (owner == nullptr) return false;
        uint64_t const IRQF = owner->m_job_identity_lock.lock_irqsave();
        bool const MATCH = owner->m_job_identity.terminal == identity && owner->m_job_identity.terminal_generation == generation;
        if (MATCH) {
            owner->m_job_identity.terminal = {};
            owner->m_job_identity.terminal_generation = 0;
        }
        owner->m_job_identity_lock.unlock_irqrestore(IRQF);
        return MATCH;
    }

    // Exit status of this process (set when process exits, used by waitpid).
    int exit_status{};
    std::atomic<bool> jobctl_stopped{false};
    std::atomic<bool> jobctl_stop_pending{false};
    std::atomic<bool> jobctl_stop_publish_deferred{false};
    uint32_t jobctl_stop_signal = 0;
    uint32_t preempt_disable_depth = 0;
    uint32_t sched_weight{};   // 1024 = default (nice 0)
    uint32_t slice_ns{};       // Time slice in nanoseconds
    uint32_t slice_used_ns{};  // Accumulated runtime within current slice
    int32_t heap_index{};      // -1 if not in any per-CPU RunHeap
    static constexpr uint32_t ACTIVE_REGISTRY_INDEX_INVALID = UINT32_MAX;
    // Dense active-task registry slot. The registry lock protects this field
    // and updates it whenever swap-removal moves another task into the slot.
    uint32_t active_registry_index = ACTIVE_REGISTRY_INDEX_INVALID;
    uint32_t last_run_us = 0;
    uint32_t last_sleep_us = 0;

    alignas(4) std::atomic<uint32_t> ref_count{1};  // Scheduler owns initial reference
    // Monotonic publication fence for teardown decisions. Once true, the Task
    // has been visible through scheduler registries/runqueues and must never
    // use an unpublished-process destruction path, even after it is detached.
    std::atomic<bool> scheduler_published{false};
    mod::sys::Spinlock fd_table_lock;
    // Process-wide policy; userspace threads access the retained owner task.
    resource::State resources{FD_TABLE_SIZE};
    mod::sys::Spinlock resource_lock;
    // Canonical process timer state uses resource_lock. Queue links below use
    // the timer service lock; every queued/in-flight item retains this owner.
    time::interval::ProcessTimers interval_timers;
    time::TimerNode interval_real_timer;
    Task* interval_wake_prev = nullptr;
    Task* interval_wake_next = nullptr;
    bool interval_wake_queued = false;
    std::atomic<uint32_t> interval_work_refs{0};
    resource::LifetimeOwner resource_owner;
    // resource_lock protects the process barrier. Destruction order matters:
    // detach membership before destroying the barrier or releasing the owner.
    job_control::StopBarrier jobctl_barrier;
    job_control::Control jobctl_control;
    job_control::Membership jobctl_membership;
    // Protected by the controller work-queue lock, not resource_lock.
    Task* jobctl_work_next = nullptr;
    bool jobctl_work_queued = false;
    std::atomic<uint64_t> resource_thread_owners{0};
    std::atomic<uint64_t> resource_cpu_threads{0};
    bool resource_cpu_admission_closed = false;   // resource_lock
    bool resource_exec_admission_closed = false;  // resource_lock; temporary native exec commit/retirement gate
    std::atomic<uint64_t> resource_exec_quiescence_epoch{0};
    uint64_t resource_exec_quiescence_sequence = 0;  // resource_lock; never reused
    uint64_t resource_exec_quiescent_threads = 0;    // resource_lock
    bool resource_exec_safe_wait = false;            // credential owner's resource_lock
    std::atomic<uint64_t> resource_exec_nudged_epoch{0};
    ker::net::wki::IpcFdSnapshot* pending_wki_fd_snapshots = nullptr;
    ker::net::wki::WkiIpcSubmissionGuard* pending_wki_submissions = nullptr;
    ker::syscall::process::PendingSignalTarget* pending_signal_targets = nullptr;
    ker::net::wki::TaskSubmitBuffer* pending_wki_submit_buffers = nullptr;
    ker::net::wki::TaskFilesystemPreparation* pending_wki_filesystems = nullptr;
    resource::ProcessCharge process_charge;
    resource::ServiceMember service_member;
    uint64_t selected_child_service = ker::abi::service_resource::NO_SELECTION;
    resource::ProcessReservation* pending_process_reservations = nullptr;
    // CPU exhaustion is delivered only at a safe user-return boundary, never
    // while resuming an interrupted syscall that may still hold kernel locks.
    std::atomic<uint64_t> pending_cpu_signals{0};
    // Stack-backed reservation nodes, canceled before fatal syscall exit
    // abandons the stack. Only this task's execution context mutates the list.
    ker::vfs::FdReservation* pending_fd_reservations = nullptr;
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
    // Intrusive deferred proxy retirement; protected by the compute exit queue
    // lock. Queue ownership retains this Task until lifecycle publication.
    Task* wki_proxy_exit_next = nullptr;
    // Deferred standard SIGCHLD work. Protected by the WKI child-signal queue
    // lock; queued storage owns one lifetime reference, including across retry.
    Task* wki_child_signal_next = nullptr;
    uint16_t wki_child_signal_hops = 0;
    bool wki_child_signal_queued = false;

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

    // Sole owner of a complete native exec preparation after stack staging.
    // Executor adopts/releases it, or transfers it through a claimed worker
    // handoff. Abort cleanup may block while ACTIVE.
    std::atomic<ker::syscall::process::PreparedNativeExecImage*> owned_exec_preparation{nullptr};
    // Published before native exec snapshots faulting user pointers.
    std::atomic<ker::syscall::process::ExecRequestArguments*> owned_exec_arguments{nullptr};
    std::atomic<bool> exec_preparation_teardown_in_progress{false};
    // Queued worker owns this request until the owner acknowledges it. Claim,
    // cancellation and preparation-slot transfer use the owner's resource_lock.
    std::atomic<ker::syscall::process::WorkerExecHandoff*> pending_exec_handoff{nullptr};
    std::atomic<ker::syscall::process::WorkerExecHandoff*> active_exec_handoff{nullptr};

    std::atomic<bool> exit_in_progress{false};
    bool has_exited{};
    std::atomic<bool> process_exit_requested{false};
    std::atomic<int32_t> requested_process_exit_status{0};
    std::atomic<int32_t> requested_process_exit_wait_status{0};
    // Set after user-visible exit state is published: descriptors are closed,
    // status/accounting are stable, and waitpid may reap while later memory
    // cleanup continues.
    std::atomic<bool> exit_notify_ready{false};
    // Accepted asynchronous core output can publish CORE/FSIZE observations
    // after waitability. Each contributor also retains this Task's lifetime.
    std::atomic<uint32_t> resource_deferred_observations{0};
    std::atomic<bool> zombie_resources_reclaiming{false};
    std::atomic<bool> zombie_resources_reclaimed{false};
    // True while deferred_task_switch owns the live syscall stack and is
    // publishing its replacement return state. A timer may record pending
    // work, but must not switch this task until the transition is complete.
    std::atomic<bool> scheduler_transition_active{false};
    bool deferred_task_switch{};  // Move to wait queue after syscall returns
    bool yield_switch{};          // Put task in expired queue instead of wait queue

    // Cooperative kernel-thread shutdown. Set centrally by the power
    // subsystem; DAEMON tasks observe it at scheduler block/sleep/yield points
    // and exit through the normal task-exit path.
    std::atomic<bool> kernel_shutdown_requested{false};
    ShutdownOwnership kernel_shutdown_ownership;

    // Set by reschedule_task_for_cpu when a wakeup fires while this task is
    // still currentTask (about to block via deferred_task_switch). Checked
    // under the RQ lock in deferred_task_switch to avoid lost wakeups.
    std::atomic<bool> wakeup_pending{false};

    // Intrusive, allocation-free anonymous-swap fault waiter. The VM side
    // publishes/removes these fields under its page-state lock. Completion is
    // task-owned so a worker may retire the page-state record before the
    // scheduler resolves the completion-before-park race.
    void* anonymous_swap_wait_mapping{};
    Task* anonymous_swap_wait_next{};
    uint64_t anonymous_swap_wait_generation{};
    std::atomic<uint64_t> anonymous_swap_completion_generation{};

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
    mutable mod::sys::Spinlock path_metadata_lock;
    // Threads use their retained process owner for filesystem state.
    [[nodiscard]] auto filesystem_owner() -> Task* { return credential_owner(); }
    [[nodiscard]] auto filesystem_owner() const -> const Task* { return credential_owner(); }
    // Captures one canonical owner's paths and directory ownership together.
    // Replacing output may perform a final File close after the path lock is
    // released: call without publication/spinlocks, in a sleepable context.
    // A missing thread owner leaves output untouched.
    [[nodiscard]] auto capture_filesystem_context(FilesystemContext& output) const -> bool;
    // Owned root/cwd are detached together during ACTIVE process cleanup.
    // Threads borrow the canonical owner; no public root installation exists.
    ker::vfs::DirectoryAnchorRef root_anchor;
    ker::vfs::DirectoryAnchorRef cwd_anchor;
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

    [[nodiscard]] bool has_group(uint32_t group) const { return credentials().has_group(group); }

   private:
    // Accepted exec owns signal routing before scheduler proxy cleanup is
    // ready. Sticky until Task destruction; retiring a remote row cannot
    // revive authority over the old local image. Protected by path_metadata_lock.
    uint32_t m_accepted_signal_task_id = 0;
    std::atomic<uint32_t> m_local_signal_admissions{0};
    mutable uint64_t m_signal_home_serial = 0;
    mutable uint64_t m_signal_receiver_serial = 0;
    session::Origin m_signal_home_origin{};
    bool m_signal_home_anchor = true;
    bool m_group_signal_visible = true;

   public:
    // Observation only: this copy does not authorize remote operations.
    struct WkiExecutionIdentity {
        HostnameBuffer submitter_hostname{};
        uint64_t remote_pid{};
        uint32_t proxy_task_id{};
        uint32_t signal_task_id{};
    };
    [[nodiscard]] auto wki_execution_identity() const -> WkiExecutionIdentity;
    void set_wki_remote_pid(uint64_t remote_pid);
    void set_wki_proxy_task_id(uint32_t proxy_task_id);
    void publish_accepted_signal_route(uint32_t task_id, uint64_t remote_pid);

    struct SignalHomeIdentity {
        session::Origin origin{};
        uint64_t serial = 0;
    };
    // Serial zero means allocation exhausted; never reuse a process identity.
    [[nodiscard]] auto signal_home_identity(bool initialize = true) const -> SignalHomeIdentity;
    [[nodiscard]] auto signal_receiver_serial() const -> uint64_t;
    [[nodiscard]] auto is_signal_home_anchor() const -> bool;
    // Receiver construction only, before any publication or group lookup.
    [[nodiscard]] auto adopt_signal_home(SignalHomeIdentity identity) -> bool;
    void hide_group_signals_until_publication();
    void publish_group_signal_visibility();
    [[nodiscard]] auto group_signal_visible() const -> bool;
    // Generate before the locked visibility observation: either the publisher
    // observes the pending value or this caller owns the scheduler wake.
    [[nodiscard]] auto generate_group_signal(uint64_t mask) -> bool;

    struct SignalAdmissionIdentity {
        security::Snapshot credentials;
        session::Identity job;
        bool remote = false;
        bool local_lease = false;
    };
    // Caller holds VM publication for current credential/job admission. This
    // additionally serializes the value capture with RX route publication.
    [[nodiscard]] auto signal_admission_identity() const -> SignalAdmissionIdentity;
    [[nodiscard]] auto begin_local_signal_admission() -> SignalAdmissionIdentity;
    void end_local_signal_admission();
    [[nodiscard]] auto local_signal_admissions_drained() const -> bool;

    void load_context(cpu::GPRegs* gpr);
    void save_context(cpu::GPRegs* gpr) const;

    [[nodiscard]] auto try_acquire_usercopy_pagemap(mm::paging::PageTable*& out) -> bool;
    void release_usercopy_pagemap();
    void quiesce_usercopy_pagemap();
    [[nodiscard]] auto detach_pagemap_after_usercopy_quiescence() -> mm::paging::PageTable*;
    [[nodiscard]] auto replace_pagemap_after_usercopy_quiescence(mm::paging::PageTable* replacement) -> mm::paging::PageTable*;

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

    // Caller-owned state, appended to preserve all existing member offsets.
    // IRQ wake notifications bypass batching and never modify these fields.
    uint32_t local_wake_batch_depth{};
    bool local_wake_batch_pending{};
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
// Optional output reports interpreter execute-admission denial. Other legacy
// construction failures leave it zero; the child remains unpublished.
[[nodiscard]] auto complete_unpublished_process_construction(Task* task, int* interpreter_error = nullptr) -> bool;
[[nodiscard]] auto clone_lazy_vmem_ranges(Task& dst, Task& src) -> bool;
void release_lazy_vmem_ranges(Task& task);

inline void task_accumulate_waited_child_times(Task& parent, const Task& child) {
    if (child.parent_pid != parent.pid) {
        return;
    }

    parent.child_user_time_us = resource::saturating_cpu_add(
        parent.child_user_time_us,
        resource::saturating_cpu_add(child.user_time_us,
                                     resource::saturating_cpu_add(child.child_user_time_us, child.imported_child_user_time_us)));
    parent.child_system_time_us = resource::saturating_cpu_add(
        parent.child_system_time_us,
        resource::saturating_cpu_add(child.system_time_us,
                                     resource::saturating_cpu_add(child.child_system_time_us, child.imported_child_system_time_us)));
}

[[nodiscard]] inline auto task_rusage_user_time_us(const Task& task) -> uint64_t {
    return resource::saturating_cpu_add(task.user_time_us,
                                        resource::saturating_cpu_add(task.child_user_time_us, task.imported_child_user_time_us));
}

[[nodiscard]] inline auto task_rusage_system_time_us(const Task& task) -> uint64_t {
    return resource::saturating_cpu_add(task.system_time_us,
                                        resource::saturating_cpu_add(task.child_system_time_us, task.imported_child_system_time_us));
}

#ifdef WOS_SELFTEST
auto task_selftest_fd_clone_failure_releases_refs() -> bool;
auto task_selftest_destroy_unpublished_user_thread_releases_refs() -> bool;
auto task_selftest_unpublished_process_owner_releases_resources() -> bool;
auto task_selftest_signal_home_and_visibility() -> bool;
auto task_selftest_owned_unpublished_process_teardown_releases_resources() -> bool;
auto task_selftest_published_process_refuses_unpublished_teardown() -> bool;
#endif
}  // namespace ker::mod::sched::task
