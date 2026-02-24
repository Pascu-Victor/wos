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
#include <util/list.hpp>
// Avoid pulling in in-tree std headers into kernel translation units from here.

namespace ker::mod::sched::task {

enum TaskType {
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
    uint64_t syscallKernelStack;
    uint64_t syscallScratchArea;  // Small scratch area for syscall handler (RIP, RSP, RFLAGS, DS, ES)

    cpu::GPRegs regs;

    uint64_t intNo;
    uint64_t errorCode;

    gates::interruptFrame frame;
} __attribute__((packed));

struct Task {
    Task(Task&&) = delete;
    auto operator=(const Task&) -> Task& = delete;
    auto operator=(Task&&) -> Task& = delete;
    Task(const char* name, uint64_t elfStart, uint64_t kernelRsp, TaskType type);

    // Factory for kernel threads (DAEMON tasks).
    // entryFunc: [[noreturn]] void func() — the kernel thread body.
    static Task* createKernelThread(const char* name, void (*entryFunc)());

    Task(const Task& task) = delete;

    mm::paging::PageTable* pagemap;
    Context context;
    uint64_t entry;
    void (*kthreadEntry)();  // Kernel thread entry (DAEMON only), nullptr otherwise

    const char* name;
    TaskType type;
    uint64_t cpu;
    threading::Thread* thread;
    uint64_t pid;
    uint64_t parentPid;  // Parent process ID (0 for orphaned/init processes)

    // has the task run at least once
    bool hasRun;

    // ELF buffer for cleanup
    uint8_t* elfBuffer;
    size_t elfBufferSize;

    // ELF metadata for auxv setup
    uint64_t programHeaderAddr;  // Virtual address of program headers (AT_PHDR)
    uint64_t elfHeaderAddr;      // Virtual address of ELF header (AT_EHDR)

    // File descriptor table for the task (per-process model planned).
    // Small fixed-size table for now; will be moved/made dynamic when process struct is available.
    static constexpr unsigned FD_TABLE_SIZE = 256;
    void* fds[FD_TABLE_SIZE];  // opaque pointers to kernel file descriptor objects (ker::vfs::File)

    // Current working directory (absolute path, "/" by default)
    static constexpr unsigned CWD_MAX = 256;
    char cwd[CWD_MAX] = "/";

    // Executable path (set by exec, used by procfs /proc/self/exe)
    static constexpr unsigned EXE_PATH_MAX = 256;
    char exe_path[EXE_PATH_MAX] = "";

    // POSIX credential model
    uint32_t uid = 0;      // Real user ID
    uint32_t gid = 0;      // Real group ID
    uint32_t euid = 0;     // Effective user ID
    uint32_t egid = 0;     // Effective group ID
    uint32_t suid = 0;     // Saved set-user-ID
    uint32_t sgid = 0;     // Saved set-group-ID
    uint32_t umask = 022;  // File creation mask (default: rw-r--r-- for files)

    // Exit status of this process (set when process exits, used by waitpid)
    int exitStatus;
    bool hasExited;
    bool waitedOn;  // Set to true when parent retrieves exit status via waitpid

    // List of task IDs waiting for this task to exit
    // When this task exits, all tasks in this list will be rescheduled on their respective CPUs
    static constexpr unsigned MAX_AWAITEE_COUNT = 512;
    uint64_t awaitee_on_exit[MAX_AWAITEE_COUNT];
    uint64_t awaitee_on_exit_count;

    // Flag indicating that this task should be moved to wait queue after syscall returns
    bool deferredTaskSwitch;
    // When true, deferredTaskSwitch puts task in expired queue (yield) instead of wait queue (block)
    bool yieldSwitch;

    // When true, a PROCESS task is at a safe voluntary blocking point (e.g. sti;hlt
    // in a syscall wait loop).  The scheduler may preempt it as if it were a DAEMON,
    // saving and restoring kernel-mode context.  Set before hlt, cleared after wake.
    volatile bool voluntaryBlock = false;

    // Waitpid state: when this task is waiting for another task to exit
    uint64_t waitingForPid;       // PID we're waiting for (for waitpid return value)
    uint64_t waitStatusPhysAddr;  // Physical address of status variable (for waitpid)

    // --- Signal infrastructure ---
    // Bitmask of pending signals (bit N = signal N+1 is pending, signals 1-64)
    uint64_t sigPending;
    // Bitmask of blocked signals
    uint64_t sigMask;

    // Signal handler entry (matches Linux ABI struct sigaction layout)
    struct SigHandler {
        uint64_t handler;   // SIG_DFL=0, SIG_IGN=1, or function pointer
        uint64_t flags;     // SA_SIGINFO, SA_RESTART, etc.
        uint64_t restorer;  // Signal return trampoline (sa_restorer)
        uint64_t mask;      // Additional signals to block during handler
    };
    static constexpr unsigned MAX_SIGNALS = 64;
    SigHandler sigHandlers[MAX_SIGNALS];

    // Set to true when a signal handler frame is being delivered
    bool inSignalHandler;

    // Set to true by sigreturn syscall; check_pending_signals will restore context
    bool doSigreturn;

    // === EEVDF scheduling fields ===
    // Virtual runtime: cumulative weighted CPU time consumed.
    // Advances by (actual_ns * 1024 / weight) each tick. Signed for wrap-safe comparison.
    int64_t vruntime;

    // Virtual deadline: vruntime + (sliceNs * 1024 / weight).
    // Tasks with earlier deadlines are scheduled first among eligible tasks.
    int64_t vdeadline;

    // Task weight (nice-level analog). 1024 = default (nice 0).
    uint32_t schedWeight;

    // Time slice in nanoseconds. Default 10ms.
    uint32_t sliceNs;

    // Accumulated wall-clock runtime within current slice (ns).
    uint32_t sliceUsedNs;

    // Index into the per-CPU RunHeap array. -1 if not in any heap.
    int32_t heapIndex;

    // Which scheduling queue the task is logically in.
    enum class SchedQueue : uint8_t { NONE = 0, RUNNABLE = 1, WAITING = 2, DEAD_GC = 3 };
    SchedQueue schedQueue;

    // Intrusive list pointer for wait queue and dead list (zero allocations).
    Task* schedNext;

    // Lock-free lifecycle management (epoch-based reclamation)
    // These must be aligned for atomic operations
    alignas(8) std::atomic<TaskState> state{TaskState::ACTIVE};
    alignas(4) std::atomic<uint32_t> refCount{1};    // Scheduler owns initial reference
    alignas(8) std::atomic<uint64_t> deathEpoch{0};  // Epoch when task became DEAD

    void loadContext(cpu::GPRegs* gpr);
    void saveContext(cpu::GPRegs* gpr);

    // Try to acquire a reference to this task.
    // Returns false if task is EXITING or DEAD.
    // Caller MUST call release() when done with the task pointer.
    bool tryAcquire() {
        uint32_t count = refCount.load(std::memory_order_acquire);
        while (count > 0) {
            if (refCount.compare_exchange_weak(count, count + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
                // Double-check state after acquiring
                TaskState s = state.load(std::memory_order_acquire);
                if (s == TaskState::DEAD || s == TaskState::EXITING) {
                    refCount.fetch_sub(1, std::memory_order_release);
                    return false;
                }
                return true;
            }
        }
        return false;
    }

    // Release a reference to this task
    void release() { refCount.fetch_sub(1, std::memory_order_acq_rel); }

    // Atomically transition task state. Returns true if transition succeeded.
    bool transitionState(TaskState from, TaskState to) {
        return state.compare_exchange_strong(from, to, std::memory_order_acq_rel, std::memory_order_acquire);
    }
};  // Removed __attribute__((packed)) - was causing misalignment of atomic fields and potential corruption

auto getNextPid() -> uint64_t;
}  // namespace ker::mod::sched::task
