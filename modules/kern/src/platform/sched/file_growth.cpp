#include "file_growth.hpp"

#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>

namespace ker::mod::sched::resource {
namespace {
constexpr unsigned FSIZE = static_cast<unsigned>(Id::FSIZE);
constexpr unsigned SIGXFSZ_NUMBER = 25;
}  // namespace

FileGrowth::FileGrowth() : FileGrowth(has_run_queues() ? get_current_task() : nullptr) {}

FileGrowth::FileGrowth(task::Task* caller) : m_caller(caller), m_limit(INFINITY_VALUE) {
    Owner owner(m_caller);
    if (auto* process = owner.get(); process != nullptr) {
        uint64_t const FLAGS = process->resource_lock.lock_irqsave();
        m_limit = process->resources.entries[FSIZE].limit.soft;
        process->resource_lock.unlock_irqrestore(FLAGS);
    } else if (m_caller != nullptr) {
        // A thread whose owner has disappeared cannot gain an unlimited policy.
        m_limit = 0;
    }
}

FileGrowth::~FileGrowth() {
    if (!m_denied && !m_committed) {
        return;
    }
    {
        Owner owner(m_caller);
        if (auto* process = owner.get(); process != nullptr) {
            uint64_t const FLAGS = process->resource_lock.lock_irqsave();
            auto& entry = process->resources.entries[FSIZE];
            if (m_denied) {
                process->resources.deny(FSIZE, Denial::SOFT_LIMIT);
            }
            if (m_committed) {
                // FSIZE is a per-file bound, not a sum of bytes across files.
                // Current is the last committed file-size observation.
                entry.current = m_size;
                if (m_size > entry.peak) {
                    entry.peak = m_size;
                }
            }
            process->resource_lock.unlock_irqrestore(FLAGS);
        }
    }
    if (m_signal && m_caller != nullptr) {
        m_caller->signal_add_pending_mask(uint64_t{1} << (SIGXFSZ_NUMBER - 1));
    }
}

auto FileGrowth::bound_write(uint64_t offset, size_t& count) -> bool {
    size_t const ALLOWED = static_cast<size_t>(file_write_budget(offset, count, m_limit));
    if (ALLOWED < count) {
        m_denied = true;
        m_signal = ALLOWED == 0;
        count = ALLOWED;
        return !m_signal;
    }
    return true;
}

auto FileGrowth::allow_resize(uint64_t length) -> bool {
    if (length > m_limit) {
        m_denied = true;
        m_signal = true;
        return false;
    }
    return true;
}

void FileGrowth::committed(uint64_t file_size) {
    m_size = file_size;
    m_committed = true;
}
}  // namespace ker::mod::sched::resource
