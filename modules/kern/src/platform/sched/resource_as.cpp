#include "resource_as.hpp"

#include <algorithm>
#include <platform/mm/virt.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>

namespace ker::mod::sched::resource {
namespace {
constexpr uint64_t USER_END = 0x0000800000000000ULL;
constexpr unsigned AS = static_cast<unsigned>(Id::AS);
}  // namespace

auto count_address_space(task::Task& subject, mm::paging::PageTable* root, uint64_t start, uint64_t end, uint64_t& bytes) -> bool {
    bytes = 0;
    if (root == nullptr || start > end || end > USER_END || ((start | end) & (mm::paging::PAGE_SIZE - 1)) != 0) {
        return false;
    }
    if (root != subject.pagemap) {
        // Exec admission walks a private staged root, not the old image's mmap
        // metadata. ELF and automatic layouts carry actual reserved PTEs.
        return mm::virt::count_user_virtual_bytes(root, start, end, bytes);
    }
    // Lazy VM ranges are disjoint but not sorted. Walk their union and only
    // inspect PTEs in the gaps. A sibling fault can materialize any lazy page
    // during this walk without changing the count or being counted twice.
    // The caller's publication guard excludes metadata writers; task teardown
    // cannot discard the current task's metadata from another execution stack.
    uint64_t cursor = start;
    while (cursor < end) {
        uint64_t next_start = end;
        uint64_t next_end = end;
        for (auto const& range : subject.lazy_vmem_ranges) {
            if (range.start > range.end || range.end > USER_END || ((range.start | range.end) & (mm::paging::PAGE_SIZE - 1)) != 0) {
                return false;
            }
            if (range.end > cursor && range.start < next_start) {
                next_start = std::max(cursor, range.start);
                next_end = std::min(end, range.end);
            }
        }
        uint64_t gap_bytes = 0;
        if (!mm::virt::count_user_virtual_bytes(root, cursor, next_start, gap_bytes)) {
            return false;
        }
        bytes += gap_bytes + next_end - next_start;
        cursor = next_end;
    }
    return true;
}

auto admit_address_space(task::Task* subject, mm::paging::PageTable* root, uint64_t added, uint64_t replaced_start, uint64_t replaced_size)
    -> bool {
    // Loader selftests and pre-process boot helpers have no policy owner.
    if (subject == nullptr) {
        return true;
    }
    uint64_t current = 0;
    uint64_t removed = 0;
    bool const VALID = replaced_start <= USER_END && replaced_size <= USER_END - replaced_start &&
                       count_address_space(*subject, root, 0, USER_END, current) &&
                       count_address_space(*subject, root, replaced_start, replaced_start + replaced_size, removed);
    if (!VALID) {
        return false;
    }
    Owner owner(subject);
    if (owner.get() == nullptr) {
        return false;
    }
    auto& process = *owner.get();
    uint64_t const FLAGS = process.resource_lock.lock_irqsave();
    bool const ADMITTED = process.resources.admit_address_space(current, removed, added);
    process.resource_lock.unlock_irqrestore(FLAGS);
    return ADMITTED;
}

auto observe_address_space(task::Task& subject) -> bool {
    // No allocation/blocking while the short-lived Task/root pins are held.
    PreemptGuard guard;
    mm::paging::PageTable* root = nullptr;
    if (!subject.try_acquire_usercopy_pagemap(root)) {
        return false;
    }
    uint64_t bytes = 0;
    bool const VALID = count_address_space(subject, root, 0, USER_END, bytes);
    if (VALID) {
        record_address_space_usage(&subject, root, bytes);
    }
    subject.release_usercopy_pagemap();
    return VALID;
}

void copy_address_space_observation(task::Task& subject, abi::resource::Usage& usage) {
    uint64_t const FLAGS = subject.resource_lock.lock_irqsave();
    usage.current = subject.resources.entries[AS].current;
    usage.peak = subject.resources.entries[AS].peak;
    subject.resource_lock.unlock_irqrestore(FLAGS);
}

AddressSpaceObservation::~AddressSpaceObservation() {
    if (m_subject != nullptr) {
        (void)observe_address_space(*m_subject);
    }
}
}  // namespace ker::mod::sched::resource
