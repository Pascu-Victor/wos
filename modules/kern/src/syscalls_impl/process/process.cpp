#include "process.hpp"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "abi/callnums/process.h"
#include "platform/asm/cpu.hpp"
#include "platform/mm/mm.hpp"
#include "platform/mm/phys.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/scheduler.hpp"
#include "platform/sched/task.hpp"
#include "syscalls_impl/process/exec.hpp"
#include "syscalls_impl/process/exit.hpp"
#include "syscalls_impl/process/getpid.hpp"
#include "syscalls_impl/process/getppid.hpp"
#include "syscalls_impl/process/waitpid.hpp"
#include "vfs/file.hpp"
#include "vfs/vfs.hpp"

// Signal constants (matching Linux ABI from abi-bits/signal.h)
static constexpr uint64_t WOS_SIG_DFL = 0;
static constexpr uint64_t WOS_SIG_IGN = 1;
static constexpr int WOS_SIGKILL = 9;
static constexpr int WOS_SIGSTOP = 19;
static constexpr uint64_t WOS_SA_RESTORER = 0x04000000;

namespace ker::syscall::process {

static auto wos_proc_fork(ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    using namespace ker::mod;

    auto* parent = sched::get_current_task();
    if (parent == nullptr) return static_cast<uint64_t>(-ESRCH);

    // Save parent's register context (will be copied to child)
    parent->context.regs = gpr;

    // --- Allocate child kernel stack ---
    auto kernelStackBase = (uint64_t)mm::phys::pageAlloc(KERNEL_STACK_SIZE);
    if (kernelStackBase == 0) return static_cast<uint64_t>(-ENOMEM);
    uint64_t kernelRsp = kernelStackBase + KERNEL_STACK_SIZE;

    // --- Allocate child Task manually (skip ELF-loading constructor) ---
    auto* child = static_cast<sched::task::Task*>(::operator new(sizeof(sched::task::Task)));
    if (child == nullptr) {
        mm::phys::pageFree(reinterpret_cast<void*>(kernelStackBase));
        return static_cast<uint64_t>(-ENOMEM);
    }

    // Zero-initialize
    memset(child, 0, sizeof(sched::task::Task));

    // --- Initialize child task fields ---
    // Copy name
    if (parent->name != nullptr) {
        size_t nameLen = strlen(parent->name);
        char* nameCopy = new char[nameLen + 1];
        memcpy(nameCopy, parent->name, nameLen + 1);
        child->name = nameCopy;
    }

    child->pid = sched::task::getNextPid();
    child->parentPid = parent->pid;
    child->type = sched::task::TaskType::PROCESS;
    child->cpu = cpu::currentCpu();
    child->hasRun = false;
    child->exitStatus = 0;
    child->hasExited = false;
    child->waitedOn = false;
    child->awaitee_on_exit_count = 0;
    child->deferredTaskSwitch = false;
    child->yieldSwitch = false;
    child->voluntaryBlock = false;
    child->kthreadEntry = nullptr;
    child->elfBuffer = nullptr;
    child->elfBufferSize = 0;
    child->waitingForPid = 0;
    child->waitStatusPhysAddr = 0;

    // EEVDF scheduling fields — start fresh
    child->vruntime = 0;
    child->vdeadline = 0;
    child->schedWeight = parent->schedWeight;
    child->sliceNs = parent->sliceNs;
    child->sliceUsedNs = 0;
    child->heapIndex = -1;
    child->schedQueue = sched::task::Task::SchedQueue::NONE;
    child->schedNext = nullptr;

    // Lock-free lifecycle management
    new (&child->state) std::atomic<sched::task::TaskState>(sched::task::TaskState::ACTIVE);
    new (&child->refCount) std::atomic<uint32_t>(1);
    new (&child->deathEpoch) std::atomic<uint64_t>(0);

    // Copy CWD
    memcpy(child->cwd, parent->cwd, sched::task::Task::CWD_MAX);

    // Copy executable path
    memcpy(child->exe_path, parent->exe_path, sched::task::Task::EXE_PATH_MAX);

    // Copy POSIX credentials
    child->uid = parent->uid;
    child->gid = parent->gid;
    child->euid = parent->euid;
    child->egid = parent->egid;
    child->suid = parent->suid;
    child->sgid = parent->sgid;
    child->umask = parent->umask;

    // Copy signal dispositions from parent (fork inherits signal handlers)
    child->sigPending = 0;  // Pending signals are NOT inherited
    child->sigMask = parent->sigMask;
    child->inSignalHandler = false;
    child->doSigreturn = false;
    memcpy(child->sigHandlers, parent->sigHandlers, sizeof(parent->sigHandlers));

    // --- Create child pagemap with COW ---
    child->pagemap = mm::virt::createPagemap();
    if (child->pagemap == nullptr) {
        delete[] child->name;
        mm::phys::pageFree(reinterpret_cast<void*>(kernelStackBase));
        ::operator delete(child);
        return static_cast<uint64_t>(-ENOMEM);
    }

    // Copy kernel mappings
    mm::virt::copyKernelMappings(child);

    // Deep-copy user pages with COW
    if (!mm::virt::deepCopyUserPagemapCOW(parent->pagemap, child->pagemap)) {
        mm::virt::destroyUserSpace(child->pagemap);
        mm::phys::pageFree(child->pagemap);
        delete[] child->name;
        mm::phys::pageFree(reinterpret_cast<void*>(kernelStackBase));
        ::operator delete(child);
        return static_cast<uint64_t>(-ENOMEM);
    }

    // --- Clone thread metadata ---
    // The child shares the same user-space layout (stack, TLS) via COW.
    // Allocate a Thread struct for the child that mirrors the parent's.
    if (parent->thread != nullptr) {
        auto* childThread = new sched::threading::Thread();
        if (childThread == nullptr) {
            mm::virt::destroyUserSpace(child->pagemap);
            mm::phys::pageFree(child->pagemap);
            delete[] child->name;
            mm::phys::pageFree(reinterpret_cast<void*>(kernelStackBase));
            ::operator delete(child);
            return static_cast<uint64_t>(-ENOMEM);
        }
        // Copy all fields — virtual addresses are the same (same address space layout via COW)
        *childThread = *parent->thread;
        // The physical pointers are now shared via COW so the child shouldn't free them
        // on thread destroy — we zero them to prevent double-free
        childThread->tlsPhysPtr = 0;
        childThread->stackPhysPtr = 0;
        child->thread = childThread;
    } else {
        child->thread = nullptr;
    }

    // --- Set up child context ---
    // Child's kernel stack and per-CPU scratch area
    child->context.syscallKernelStack = kernelRsp;

    auto* perCpu = new cpu::PerCpu();
    perCpu->syscallStack = kernelRsp;
    perCpu->cpuId = cpu::currentCpu();
    child->context.syscallScratchArea = (uint64_t)perCpu;

    // Copy parent's register context — child will resume at the same RIP
    child->context.regs = parent->context.regs;
    child->context.frame = parent->context.frame;
    child->context.intNo = 0;
    child->context.errorCode = 0;

    // Child returns 0 from fork
    child->context.regs.rax = 0;

    // Copy entry and ELF metadata pointers
    child->entry = parent->entry;
    child->programHeaderAddr = parent->programHeaderAddr;
    child->elfHeaderAddr = parent->elfHeaderAddr;

    // --- Clone file descriptors ---
    for (unsigned i = 0; i < sched::task::Task::FD_TABLE_SIZE; ++i) {
        if (parent->fds[i] != nullptr) {
            auto* file = static_cast<ker::vfs::File*>(parent->fds[i]);
            file->refcount++;
            child->fds[i] = file;
        }
    }

    // --- Enqueue child ---
    if (!sched::post_task_balanced(child)) {
        // Undo FD refcount increments
        for (unsigned i = 0; i < sched::task::Task::FD_TABLE_SIZE; ++i) {
            if (child->fds[i] != nullptr) {
                auto* file = static_cast<ker::vfs::File*>(child->fds[i]);
                file->refcount--;
                child->fds[i] = nullptr;
            }
        }
        if (child->thread) delete child->thread;
        delete (cpu::PerCpu*)child->context.syscallScratchArea;
        mm::virt::destroyUserSpace(child->pagemap);
        mm::phys::pageFree(child->pagemap);
        delete[] child->name;
        mm::phys::pageFree(reinterpret_cast<void*>(kernelStackBase));
        ::operator delete(child);
        return static_cast<uint64_t>(-ENOMEM);
    }

    // Return child PID to parent
    return child->pid;
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

static auto wos_proc_sigaction(int signum, uint64_t act_ptr, uint64_t oldact_ptr) -> uint64_t {
    using namespace ker::mod;

    auto* task = sched::get_current_task();
    if (task == nullptr) return static_cast<uint64_t>(-ESRCH);

    // Signal numbers are 1-based, array is 0-based
    if (signum < 1 || signum > static_cast<int>(sched::task::Task::MAX_SIGNALS)) return static_cast<uint64_t>(-EINVAL);

    // SIGKILL and SIGSTOP cannot have their handlers changed
    if (signum == WOS_SIGKILL || signum == WOS_SIGSTOP) return static_cast<uint64_t>(-EINVAL);

    unsigned idx = static_cast<unsigned>(signum - 1);

    // Return old handler if requested
    if (oldact_ptr != 0) {
        auto* old = reinterpret_cast<KernelSigaction*>(oldact_ptr);
        old->handler = task->sigHandlers[idx].handler;
        old->flags = task->sigHandlers[idx].flags;
        old->restorer = task->sigHandlers[idx].restorer;
        old->mask = task->sigHandlers[idx].mask;
    }

    // Set new handler if provided
    if (act_ptr != 0) {
        auto* act = reinterpret_cast<const KernelSigaction*>(act_ptr);
        task->sigHandlers[idx].handler = act->handler;
        task->sigHandlers[idx].flags = act->flags;
        task->sigHandlers[idx].mask = act->mask;
        // Store restorer if SA_RESTORER flag is set
        if (act->flags & WOS_SA_RESTORER) {
            task->sigHandlers[idx].restorer = act->restorer;
        }
    }

    return 0;
}

static auto wos_proc_sigprocmask(int how, uint64_t set_ptr, uint64_t oldset_ptr) -> uint64_t {
    using namespace ker::mod;

    auto* task = sched::get_current_task();
    if (task == nullptr) return static_cast<uint64_t>(-ESRCH);

    // Return old mask if requested (sigset_t first word)
    if (oldset_ptr != 0) {
        auto* oldset = reinterpret_cast<uint64_t*>(oldset_ptr);
        *oldset = task->sigMask;
    }

    // Apply new mask if provided
    if (set_ptr != 0) {
        auto* setp = reinterpret_cast<const uint64_t*>(set_ptr);
        uint64_t set = *setp;

        // SIGKILL and SIGSTOP can never be blocked
        uint64_t unblockable = (1ULL << (WOS_SIGKILL - 1)) | (1ULL << (WOS_SIGSTOP - 1));
        set &= ~unblockable;

        switch (how) {
            case 0:  // SIG_BLOCK
                task->sigMask |= set;
                break;
            case 1:  // SIG_UNBLOCK
                task->sigMask &= ~set;
                break;
            case 2:  // SIG_SETMASK
                task->sigMask = set;
                break;
            default:
                return static_cast<uint64_t>(-EINVAL);
        }
    }

    return 0;
}

static auto wos_proc_kill(int64_t pid, int sig) -> uint64_t {
    using namespace ker::mod;

    if (sig < 0 || sig > static_cast<int>(sched::task::Task::MAX_SIGNALS)) return static_cast<uint64_t>(-EINVAL);

    // sig == 0 is used to check if process exists (no signal sent)
    if (sig == 0) {
        if (pid <= 0) return 0;  // simplified
        auto* target = sched::find_task_by_pid_safe(static_cast<uint64_t>(pid));
        if (target == nullptr) return static_cast<uint64_t>(-ESRCH);
        target->release();
        return 0;
    }

    if (pid <= 0) {
        // pid==0 or pid==-1 means "all processes in group" — simplified for now
        return static_cast<uint64_t>(-ESRCH);
    }

    auto* target = sched::find_task_by_pid_safe(static_cast<uint64_t>(pid));
    if (target == nullptr) return static_cast<uint64_t>(-ESRCH);

    // Set the signal pending bit (signal N is bit N-1)
    target->sigPending |= (1ULL << (sig - 1));

    // If the target is blocked (waiting), wake it up so it can handle the signal
    auto state = target->state.load(std::memory_order_acquire);
    if (state == sched::task::TaskState::ACTIVE) {
        // Task may be in a wait queue — try to reschedule it
        // Only reschedule if it's actually blocked (not currently running)
        if (target->deferredTaskSwitch || target->voluntaryBlock) {
            uint64_t cpu = sched::get_least_loaded_cpu();
            sched::reschedule_task_for_cpu(cpu, target);
        }
    }

    target->release();
    return 0;
}

auto process(abi::process::procmgmt_ops op, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5, ker::mod::cpu::GPRegs& gpr) -> uint64_t {
    switch (op) {
        case abi::process::procmgmt_ops::exit:
            wos_proc_exit(static_cast<int>(a2));
            return 0;  // Should not reach here
        case abi::process::procmgmt_ops::exec: {
            return wos_proc_exec(reinterpret_cast<const char*>(a2), reinterpret_cast<const char* const*>(a3),
                                 reinterpret_cast<const char* const*>(a4));
        }
        case abi::process::procmgmt_ops::waitpid: {
            return wos_proc_waitpid(static_cast<int64_t>(a2), reinterpret_cast<int32_t*>(a3), static_cast<int32_t>(a4), gpr);
        }
        case abi::process::procmgmt_ops::getpid: {
            return wos_proc_getpid();
        }
        case abi::process::procmgmt_ops::getppid: {
            return wos_proc_getppid();
        }
        case abi::process::procmgmt_ops::fork: {
            return wos_proc_fork(gpr);
        }
        case abi::process::procmgmt_ops::sigaction: {
            return wos_proc_sigaction(static_cast<int>(a2), a3, a4);
        }
        case abi::process::procmgmt_ops::sigprocmask: {
            return wos_proc_sigprocmask(static_cast<int>(a2), a3, a4);
        }
        case abi::process::procmgmt_ops::kill: {
            return wos_proc_kill(static_cast<int64_t>(a2), static_cast<int>(a3));
        }
        case abi::process::procmgmt_ops::sigreturn: {
            // Signal the asm-level check_pending_signals to restore the saved context
            auto* task = ker::mod::sched::get_current_task();
            if (task) task->doSigreturn = true;
            return 0;
        }

        // --- POSIX credential syscalls ---
        case abi::process::procmgmt_ops::getuid: {
            auto* task = ker::mod::sched::get_current_task();
            return task ? task->uid : 0;
        }
        case abi::process::procmgmt_ops::geteuid: {
            auto* task = ker::mod::sched::get_current_task();
            return task ? task->euid : 0;
        }
        case abi::process::procmgmt_ops::getgid: {
            auto* task = ker::mod::sched::get_current_task();
            return task ? task->gid : 0;
        }
        case abi::process::procmgmt_ops::getegid: {
            auto* task = ker::mod::sched::get_current_task();
            return task ? task->egid : 0;
        }
        case abi::process::procmgmt_ops::setuid: {
            auto* task = ker::mod::sched::get_current_task();
            if (!task) return static_cast<uint64_t>(-ESRCH);
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
        case abi::process::procmgmt_ops::setgid: {
            auto* task = ker::mod::sched::get_current_task();
            if (!task) return static_cast<uint64_t>(-ESRCH);
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
        case abi::process::procmgmt_ops::seteuid: {
            auto* task = ker::mod::sched::get_current_task();
            if (!task) return static_cast<uint64_t>(-ESRCH);
            auto new_euid = static_cast<uint32_t>(a2);
            if (task->euid == 0 || new_euid == task->uid || new_euid == task->suid) {
                task->euid = new_euid;
                return 0;
            }
            return static_cast<uint64_t>(-EPERM);
        }
        case abi::process::procmgmt_ops::setegid: {
            auto* task = ker::mod::sched::get_current_task();
            if (!task) return static_cast<uint64_t>(-ESRCH);
            auto new_egid = static_cast<uint32_t>(a2);
            if (task->euid == 0 || new_egid == task->gid || new_egid == task->sgid) {
                task->egid = new_egid;
                return 0;
            }
            return static_cast<uint64_t>(-EPERM);
        }
        case abi::process::procmgmt_ops::getumask: {
            auto* task = ker::mod::sched::get_current_task();
            return task ? task->umask : 022;
        }
        case abi::process::procmgmt_ops::setumask: {
            auto* task = ker::mod::sched::get_current_task();
            if (!task) return static_cast<uint64_t>(-ESRCH);
            auto old_umask = task->umask;
            task->umask = static_cast<uint32_t>(a2) & 0777;
            return old_umask;
        }

        default:
            mod::io::serial::write("sys_process: unknown op\n");
            return static_cast<uint64_t>(ENOSYS);
    }
}
}  // namespace ker::syscall::process
