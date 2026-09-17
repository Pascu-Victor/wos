#include "session.hpp"

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <dev/pty.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/terminal.hpp>
#include <platform/sys/mutex.hpp>
#include <syscalls_impl/process/child_events.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>

namespace ker::mod::sched::session {
namespace {
sys::Mutex mutation_lock;

// Lock order: VM publication -> mutation mutex -> lifecycle/pair -> store.
// Registry lookups retain references and release the registry lock immediately.
struct Mutation {
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    sys::MutexGuard serial{mutation_lock};
};

auto live_process(const task::Task& subject) -> bool {
    return subject.type == task::TaskType::PROCESS && !subject.is_thread &&
           subject.state.load(std::memory_order_acquire) == task::TaskState::ACTIVE && !subject.has_exited;
}

auto resolve_group(const Identity& identity, uint64_t pgid, Origin& origin) -> bool {
    bool found = false;
    bool ambiguous = false;
    bool inherited = false;
    Origin selected{};
    uint32_t const COUNT = get_active_task_count();
    for (uint32_t i = 0; i < COUNT; ++i) {
        auto* candidate = get_active_task_at_safe(i);
        if (candidate == nullptr) {
            continue;
        }
        auto const JOB = candidate->job_identity();
        bool const MATCH = live_process(*candidate) && same_session(JOB, identity) && JOB.pgid == pgid;
        candidate->release();
        if (MATCH) {
            // A numeric group names this kernel's group before a foreign
            // alias, just as a positive PID prefers a local task lookup.
            if (JOB.group_origin.local()) {
                origin = {};
                return true;
            }
            if (found && selected != JOB.group_origin) ambiguous = true;
            if (!found) selected = JOB.group_origin;
            found = true;
            inherited |= identity.pgid == pgid && identity.group_origin == JOB.group_origin;
        }
    }
    if (inherited) {
        origin = identity.group_origin;
        return true;
    }
    if (!found || ambiguous) return false;
    origin = selected;
    return true;
}
// Caller holds mutation_lock; neither this nor cleanup callbacks take VM
// publication, since CLOEXEC teardown may already hold that nonrecursive lock.
void revoke_terminal(ker::dev::pty::PtyPair& pair) {
    uint64_t const IRQF = pair.lock.lock_irqsave();
    auto const TERMINAL = pair.identity;
    uint64_t const SESSION = pair.session_id;
    uint64_t const GENERATION = pair.attachment_generation;
    uint64_t const FOREGROUND = pair.foreground_pgrp;
    auto const FOREGROUND_ORIGIN = pair.foreground_origin;
    pair.session_id = 0;
    pair.foreground_pgrp = 0;
    pair.foreground_origin = {};
    pair.lock.unlock_irqrestore(IRQF);
    if (SESSION != 0 && GENERATION != 0) {
        revoke_terminal_attachment(TERMINAL, SESSION, GENERATION, FOREGROUND, FOREGROUND_ORIGIN);
    }
}

}  // namespace

auto create(task::Task& caller) -> int64_t {
    Mutation const MUTATION;
    auto* owner = caller.credential_owner();
    if (owner == nullptr || caller.type != task::TaskType::PROCESS) {
        return -ESRCH;
    }
    auto identity = owner->job_identity();
    if (group_leader(identity, owner->pid)) {
        return -EPERM;
    }
    identity.session_id = owner->pid;
    identity.origin = {};
    identity.pgid = owner->pid;
    identity.group_origin = {};
    identity.terminal = {};
    identity.terminal_generation = 0;
    ker::syscall::process::child_events::update_job_identity(*owner, identity);
    return static_cast<int64_t>(owner->pid);
}

auto set_group(task::Task& caller, int64_t pid, int64_t pgid) -> int {
    if (pid < 0 || pgid < 0) {
        return -EINVAL;
    }
    Mutation const MUTATION;
    auto* owner = caller.credential_owner();
    if (owner == nullptr || caller.type != task::TaskType::PROCESS) {
        return -ESRCH;
    }
    uint64_t const PID = pid != 0 ? static_cast<uint64_t>(pid) : owner->pid;
    uint64_t const PGID = pgid != 0 ? static_cast<uint64_t>(pgid) : PID;
    auto* target = find_task_by_pid_safe(PID);
    if (target == nullptr) {
        return -ESRCH;
    }
    int result = -ESRCH;
    if (live_process(*target)) {
        auto identity = target->job_identity();
        result = security::authorize_process_group_change(owner->pid, owner->job_identity().session_id,
                                                          {.pid = target->pid,
                                                           .session = identity.session_id,
                                                           .parent = target->parent_pid,
                                                           .exec_completed = identity.exec_completed,
                                                           .local_session = identity.origin.local()});
        if (result == 0 && !same_session(owner->job_identity(), identity)) result = -EPERM;
        Origin group_origin{};
        if (result == 0 && PGID != target->pid && !resolve_group(identity, PGID, group_origin)) {
            result = -EPERM;
        }
        if (result == 0) {
            identity.pgid = PGID;
            identity.group_origin = group_origin;
            ker::syscall::process::child_events::update_job_identity(*target, identity);
        }
    }
    target->release();
    return result;
}

auto prepare_spawn_group(task::Task& child, int64_t pgid) -> int {
    if (pgid < 0) {
        return -EINVAL;
    }
    sys::MutexGuard const SERIAL(mutation_lock);
    auto identity = child.job_identity();
    uint64_t const PGID = pgid != 0 ? static_cast<uint64_t>(pgid) : child.pid;
    Origin group_origin{};
    if (PGID != child.pid && !resolve_group(identity, PGID, group_origin)) {
        return -EPERM;
    }
    identity.pgid = PGID;
    identity.group_origin = group_origin;
    child.initialize_job_identity(identity);
    return 0;
}

auto claim_terminal(task::Task& caller, ker::dev::pty::PtyPair& pair) -> int {
    if (caller.type != task::TaskType::PROCESS) {
        return -ENOTTY;
    }
    Mutation const MUTATION;
    auto* owner = caller.credential_owner();
    if (owner == nullptr) {
        return -ESRCH;
    }
    auto identity = owner->job_identity();
    // A receiver-local terminal belongs to a local session. Imported numeric
    // SIDs cannot accidentally make a remote process its session leader.
    if (!identity.origin.local()) return -EPERM;
    if (identity.terminal.value != 0 &&
        !ker::dev::pty::pty_binding_is_live(identity.terminal, identity.session_id, identity.terminal_generation)) {
        identity.terminal = {};
        identity.terminal_generation = 0;
    }
    uint64_t const IRQF = pair.lock.lock_irqsave();
    int result =
        pair.freeing || !pair.allocated || pair.master_opened <= 0
            ? -EIO
            : security::authorize_terminal_claim(owner->pid, identity.session_id, identity.terminal, pair.identity, pair.session_id);
    if (result == 0 && pair.session_id == 0 && pair.attachment_generation == UINT64_MAX) {
        result = -EOVERFLOW;
    }
    if (result == 0) {
        bool const FIRST_CLAIM = pair.session_id == 0;
        if (FIRST_CLAIM) {
            ++pair.attachment_generation;
        }
        identity.terminal = pair.identity;
        identity.terminal_generation = pair.attachment_generation;
        owner->publish_job_identity(identity);
        pair.session_id = identity.session_id;
        if (FIRST_CLAIM) {
            pair.foreground_pgrp = identity.pgid;
            pair.foreground_origin = identity.group_origin;
        }
    }
    pair.lock.unlock_irqrestore(IRQF);
    return result;
}

auto set_foreground(task::Task& caller, ker::dev::pty::PtyPair& pair, int64_t pgid) -> int {
    if (pgid <= 0) {
        return -EINVAL;
    }
    if (caller.type != task::TaskType::PROCESS) {
        return -ENOTTY;
    }
    Mutation const MUTATION;
    auto const IDENTITY = caller.job_identity();
    uint64_t irqf = pair.lock.lock_irqsave();
    int result = security::authorize_terminal_control(IDENTITY.session_id, IDENTITY.terminal, pair.identity, pair.session_id,
                                                      IDENTITY.terminal_generation, pair.attachment_generation);
    pair.lock.unlock_irqrestore(irqf);
    if (result != 0) {
        return result;
    }
    Origin group_origin{};
    if (!resolve_group(IDENTITY, static_cast<uint64_t>(pgid), group_origin)) {
        return -EPERM;
    }
    irqf = pair.lock.lock_irqsave();
    pair.foreground_pgrp = static_cast<uint64_t>(pgid);
    pair.foreground_origin = group_origin;
    pair.lock.unlock_irqrestore(irqf);
    return 0;
}

auto detach_terminal(task::Task& caller, ker::dev::pty::PtyPair& pair) -> int {
    if (caller.type != task::TaskType::PROCESS) {
        return -ENOTTY;
    }
    Mutation const MUTATION;
    auto* owner = caller.credential_owner();
    if (owner == nullptr) {
        return -ESRCH;
    }
    auto const IDENTITY = owner->job_identity();
    uint64_t irqf = pair.lock.lock_irqsave();
    int const RESULT = security::authorize_terminal_control(IDENTITY.session_id, IDENTITY.terminal, pair.identity, pair.session_id,
                                                            IDENTITY.terminal_generation, pair.attachment_generation);
    pair.lock.unlock_irqrestore(irqf);
    if (RESULT != 0) {
        return RESULT;
    }
    if (!session_leader(IDENTITY, owner->pid)) {
        owner->set_controlling_terminal({});
        return 0;
    }
    revoke_terminal(pair);
    static_cast<void>(owner->clear_controlling_terminal_if(IDENTITY.terminal, IDENTITY.terminal_generation));
    return 0;
}

void process_exit(task::Task& owner) {
    if (owner.is_thread || owner.type != task::TaskType::PROCESS) {
        return;
    }
    sys::MutexGuard const SERIAL(mutation_lock);
    auto const JOB = owner.job_identity();
    if (JOB.terminal.value == 0) {
        return;
    }
    if (session_leader(JOB, owner.pid)) {
        auto* pair = ker::dev::pty::pty_get(JOB.terminal.index());
        if (pair != nullptr) {
            uint64_t const IRQF = pair->lock.lock_irqsave();
            bool const MATCH = security::authorize_terminal_control(JOB.session_id, JOB.terminal, pair->identity, pair->session_id,
                                                                    JOB.terminal_generation, pair->attachment_generation) == 0;
            pair->lock.unlock_irqrestore(IRQF);
            if (MATCH) {
                revoke_terminal(*pair);
            }
            ker::dev::pty::pty_put(pair);
        }
    }
    static_cast<void>(owner.clear_controlling_terminal_if(JOB.terminal, JOB.terminal_generation));
}

void hangup_terminal(ker::dev::pty::PtyPair& pair) {
    sys::MutexGuard const SERIAL(mutation_lock);
    uint64_t const IRQF = pair.lock.lock_irqsave();
    bool const LAST_MASTER = pair.master_opened == 0;
    pair.lock.unlock_irqrestore(IRQF);
    if (LAST_MASTER) {
        revoke_terminal(pair);
    }
}

}  // namespace ker::mod::sched::session
