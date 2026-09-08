#include "fd_reservation.hpp"

#include <abi/resource.hpp>
#include <cerrno>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <vfs/file.hpp>

namespace ker::vfs {
namespace {
constexpr unsigned NOFILE = static_cast<unsigned>(abi::resource::Id::NOFILE);
}

auto fd_limit_locked(mod::sched::task::Task& task) -> uint64_t {
    uint64_t const FLAGS = task.resource_lock.lock_irqsave();
    uint64_t const LIMIT = task.resources.entries[NOFILE].limit.soft;
    task.resource_lock.unlock_irqrestore(FLAGS);
    return LIMIT;
}

void fd_usage_changed_locked(mod::sched::task::Task& task) {
    uint64_t const FLAGS = task.resource_lock.lock_irqsave();
    auto& usage = task.resources.entries[NOFILE];
    usage.current = task.fd_table.size() + task.fd_table.reserved_count();
    if (usage.current > usage.peak) {
        usage.peak = usage.current;
    }
    task.resource_lock.unlock_irqrestore(FLAGS);
}

void fd_limit_denied_locked(mod::sched::task::Task& task) {
    uint64_t const FLAGS = task.resource_lock.lock_irqsave();
    task.resources.deny(NOFILE, abi::resource::Denial::SOFT_LIMIT);
    task.resource_lock.unlock_irqrestore(FLAGS);
}

FdReservation::FdReservation(mod::sched::task::Task* target, uint64_t minimum, bool dupfd) {
    mod::sched::resource::Owner owner(target);
    auto* table = owner.get();
    if (table == nullptr || !table->try_acquire()) {
        m_result = -ESRCH;
        return;
    }
    uint64_t const FLAGS = table->fd_table_lock.lock_irqsave();
    uint64_t const LIMIT = fd_limit_locked(*table);
    uint64_t const SLOT = table->fd_table.reserve(minimum, LIMIT, this);
    if (SLOT == UINT64_MAX) {
        fd_limit_denied_locked(*table);
        m_result = dupfd && minimum >= LIMIT ? -EINVAL : -EMFILE;
    } else {
        m_table = table;
        m_result = static_cast<int>(SLOT);
        m_executor = mod::sched::can_query_current_task() ? mod::sched::get_current_task() : nullptr;
        if (m_executor == nullptr) {
            m_executor = target;
        }
        m_next = m_executor->pending_fd_reservations;
        if (m_next != nullptr) {
            m_next->m_previous = this;
        }
        m_executor->pending_fd_reservations = this;
        fd_usage_changed_locked(*table);
    }
    table->fd_table_lock.unlock_irqrestore(FLAGS);
    if (SLOT == UINT64_MAX) {
        table->release();
    }
}

FdReservation::~FdReservation() { cancel(); }

void FdReservation::cancel() {
    if (m_table == nullptr) {
        return;
    }
    auto* table = m_table;
    uint64_t const FLAGS = table->fd_table_lock.lock_irqsave();
    static_cast<void>(table->fd_table.cancel(static_cast<uint64_t>(m_result), this));
    if (m_previous != nullptr) {
        m_previous->m_next = m_next;
    } else {
        m_executor->pending_fd_reservations = m_next;
    }
    if (m_next != nullptr) {
        m_next->m_previous = m_previous;
    }
    m_table = nullptr;
    m_result = -ECANCELED;
    fd_usage_changed_locked(*table);
    table->fd_table_lock.unlock_irqrestore(FLAGS);
    table->release();
}

auto FdReservation::install(File* file, bool cloexec) -> int {
    if (m_table == nullptr || file == nullptr) {
        return file == nullptr ? -EINVAL : m_result;
    }
    int const FD = m_result;
    uint64_t const PID = m_table->pid;
    uint64_t const FLAGS = m_table->fd_table_lock.lock_irqsave();
    bool const COMMITTED = m_table->fd_table.commit(static_cast<uint64_t>(FD), this, file);
    if (COMMITTED) {
        if (cloexec) {
            m_table->set_fd_cloexec(static_cast<unsigned>(FD));
        } else {
            m_table->clear_fd_cloexec(static_cast<unsigned>(FD));
        }
        file->fd = FD;
    }
    size_t const FD_COUNT = m_table->fd_table.size();
    m_table->fd_table_lock.unlock_irqrestore(FLAGS);
    cancel();
    if (COMMITTED) {
        mod::perf::record_container_stat(0, PID, mod::perf::PerfSubsystem::FD_TABLE, 0, mod::perf::PERF_FLAG_CT_INSERT,
                                         static_cast<int64_t>(FD_COUNT), 0, reinterpret_cast<uint64_t>(__builtin_return_address(0)));
    }
    return COMMITTED ? FD : -ECANCELED;
}

auto FdReservation::install_pair(FdReservation& second, File* first_file, File* second_file, bool cloexec) -> int {
    if (m_table == nullptr || m_table != second.m_table || this == &second || first_file == nullptr || second_file == nullptr) {
        return -EINVAL;
    }
    auto* table = m_table;
    uint64_t const PID = table->pid;
    uint64_t const FIRST = static_cast<uint64_t>(m_result);
    uint64_t const SECOND = static_cast<uint64_t>(second.m_result);
    uint64_t const FLAGS = table->fd_table_lock.lock_irqsave();
    bool const OWNED = table->fd_table.owned_by(FIRST, this) && table->fd_table.owned_by(SECOND, &second);
    if (OWNED) {
        // Both slots were validated under the same lock. Fixed-table commits
        // cannot allocate or fail after validation, so neither end is exposed
        // before the other is ready.
        static_cast<void>(table->fd_table.commit(FIRST, this, first_file));
        static_cast<void>(table->fd_table.commit(SECOND, &second, second_file));
        first_file->fd = static_cast<int>(FIRST);
        second_file->fd = static_cast<int>(SECOND);
        for (uint64_t slot : {FIRST, SECOND}) {
            if (cloexec) {
                table->set_fd_cloexec(static_cast<unsigned>(slot));
            } else {
                table->clear_fd_cloexec(static_cast<unsigned>(slot));
            }
        }
    }
    size_t const FD_COUNT = table->fd_table.size();
    table->fd_table_lock.unlock_irqrestore(FLAGS);
    second.cancel();
    cancel();
    if (OWNED) {
        mod::perf::record_container_stat(0, PID, mod::perf::PerfSubsystem::FD_TABLE, 0, mod::perf::PERF_FLAG_CT_INSERT,
                                         static_cast<int64_t>(FD_COUNT), 0, reinterpret_cast<uint64_t>(__builtin_return_address(0)));
    }
    return OWNED ? 0 : -ECANCELED;
}

void cancel_fd_reservations(mod::sched::task::Task& executor) {
    while (executor.pending_fd_reservations != nullptr) {
        executor.pending_fd_reservations->cancel();
    }
}
}  // namespace ker::vfs
