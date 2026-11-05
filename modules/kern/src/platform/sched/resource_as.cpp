#include "resource_as.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <platform/asm/cpu.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/virt.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/scheduler.hpp>

namespace ker::mod::sched::resource {
namespace {
constexpr uint64_t USER_END = 0x0000800000000000ULL;
constexpr unsigned AS = static_cast<unsigned>(Id::AS);
}  // namespace

namespace {
auto query_address_space_count(task::Task& subject, mm::paging::PageTable* root, uint64_t start, uint64_t end, uint64_t& bytes)
    -> mm::virt::UserVirtualCountStatus {
    using Status = mm::virt::UserVirtualCountStatus;
    bytes = 0;
    if (root == nullptr || start > end || end > USER_END || ((start | end) & (mm::paging::PAGE_SIZE - 1)) != 0) {
        return Status::Invalid;
    }
    if (root != subject.pagemap) {
        // Exec admission walks a private staged root, not the old image's mmap
        // metadata. ELF and automatic layouts carry actual reserved PTEs.
        return mm::virt::query_user_virtual_bytes(root, start, end, bytes);
    }
    // Lazy VM ranges are disjoint but not sorted. Walk their union and only
    // inspect PTEs in the gaps. A sibling fault can materialize any lazy page
    // during this walk without changing the count or being counted twice.
    // The caller's publication guard excludes metadata writers; task teardown
    // cannot discard the current task's metadata from another execution stack.
    uint64_t cursor = start;
    uint64_t total = 0;
    while (cursor < end) {
        uint64_t next_start = end;
        uint64_t next_end = end;
        for (auto const& range : subject.lazy_vmem_ranges) {
            if (range.start > range.end || range.end > USER_END || ((range.start | range.end) & (mm::paging::PAGE_SIZE - 1)) != 0) {
                return Status::Invalid;
            }
            if (range.end > cursor && range.start < next_start) {
                next_start = std::max(cursor, range.start);
                next_end = std::min(end, range.end);
            }
        }
        uint64_t gap_bytes = 0;
        auto const STATUS = mm::virt::query_user_virtual_bytes(root, cursor, next_start, gap_bytes);
        if (STATUS != Status::Counted) return STATUS;
        total += gap_bytes + next_end - next_start;
        cursor = next_end;
    }
    bytes = total;
    return Status::Counted;
}
}  // namespace

auto count_address_space(task::Task& subject, mm::paging::PageTable* root, uint64_t start, uint64_t end, uint64_t& bytes) -> bool {
    return query_address_space_count(subject, root, start, end, bytes) == mm::virt::UserVirtualCountStatus::Counted;
}

namespace {
auto can_reuse_mapping_count(task::Task& subject, mm::paging::PageTable* root) -> bool {
    return root != nullptr && root == subject.pagemap && &subject == get_current_task() && subject.type == task::TaskType::PROCESS &&
           !subject.is_thread && !subject.shares_user_pagemap.load(std::memory_order_acquire);
}

auto try_admit_mapping_count(task::Task* subject, mm::paging::PageTable* root, uint64_t added, uint64_t replaced_start,
                             uint64_t replaced_size, uint64_t* admitted_bytes) -> MappingAdmission {
    if (admitted_bytes != nullptr) *admitted_bytes = 0;
    // Loader selftests and pre-process boot helpers have no policy owner.
    if (subject == nullptr) {
        return MappingAdmission::Admitted;
    }
    uint64_t current = 0;
    uint64_t removed = 0;
    if (replaced_start > USER_END || replaced_size > USER_END - replaced_start) return MappingAdmission::Denied;
    auto const CURRENT = query_address_space_count(*subject, root, 0, USER_END, current);
    if (CURRENT == mm::virt::UserVirtualCountStatus::GateBusy) return MappingAdmission::GateBusy;
    if (CURRENT != mm::virt::UserVirtualCountStatus::Counted) return MappingAdmission::Denied;
    auto const REMOVED = query_address_space_count(*subject, root, replaced_start, replaced_start + replaced_size, removed);
    if (REMOVED == mm::virt::UserVirtualCountStatus::GateBusy) return MappingAdmission::GateBusy;
    if (REMOVED != mm::virt::UserVirtualCountStatus::Counted) return MappingAdmission::Denied;
    Owner owner(subject);
    if (owner.get() == nullptr) {
        return MappingAdmission::Denied;
    }
    auto& process = *owner.get();
    uint64_t const FLAGS = process.resource_lock.lock_irqsave();
    bool const ADMITTED = process.resources.admit_address_space(current, removed, added);
    process.resource_lock.unlock_irqrestore(FLAGS);
    if (ADMITTED && admitted_bytes != nullptr) {
        // The policy has checked removed <= current and addition overflow.
        *admitted_bytes = current - removed + added;
    }
    return ADMITTED ? MappingAdmission::Admitted : MappingAdmission::Denied;
}

auto observe_mapping_count(task::Task& subject, mm::paging::PageTable* admitted_root, uint64_t admitted_bytes) -> bool {
    // No allocation/blocking while the short-lived Task/root pins are held.
    PreemptGuard guard;
    mm::paging::PageTable* root = nullptr;
    if (!subject.try_acquire_usercopy_pagemap(root)) {
        return false;
    }
    uint64_t bytes = 0;
    bool const REUSE = root == admitted_root && can_reuse_mapping_count(subject, root);
    bool const VALID = REUSE ? mm::virt::validate_user_pagemap_access(root) : count_address_space(subject, root, 0, USER_END, bytes);
    if (VALID) {
        record_address_space_usage(&subject, root, REUSE ? admitted_bytes : bytes);
    }
    subject.release_usercopy_pagemap();
    return VALID;
}
}  // namespace

auto admit_address_space(task::Task* subject, mm::paging::PageTable* root, uint64_t added, uint64_t replaced_start, uint64_t replaced_size)
    -> bool {
    return try_admit_mapping_count(subject, root, added, replaced_start, replaced_size, nullptr) == MappingAdmission::Admitted;
}

auto observe_address_space(task::Task& subject) -> bool { return observe_mapping_count(subject, nullptr, 0); }

auto AddressSpaceObservation::try_admit_mapping(mm::paging::PageTable* root, uint64_t added, uint64_t replaced_start,
                                                uint64_t replaced_size) -> MappingAdmission {
    bool const FIRST = !m_admission_attempted;
    m_admission_attempted = true;
    m_admitted_root = nullptr;
    m_committed = false;
    auto const ADMISSION = try_admit_mapping_count(m_subject, root, added, replaced_start, replaced_size, &m_admitted_bytes);
    if (ADMISSION == MappingAdmission::Admitted && FIRST && m_subject != nullptr && can_reuse_mapping_count(*m_subject, root)) {
        m_admitted_root = root;
    }
    return ADMISSION;
}

auto AddressSpaceObservation::admit_mapping(mm::paging::PageTable* root, uint64_t added, uint64_t replaced_start, uint64_t replaced_size)
    -> bool {
    return try_admit_mapping(root, added, replaced_start, replaced_size) == MappingAdmission::Admitted;
}

void AddressSpaceObservation::commit_mapping() {
    if (m_committed) {
        m_admitted_root = nullptr;
    }
    m_committed = true;
}

auto AddressSpaceObservation::observe() -> bool {
    return observe_mapping_count(*m_subject, m_committed ? m_admitted_root : nullptr, m_admitted_bytes);
}

void copy_address_space_observation(task::Task& subject, abi::resource::Usage& usage) {
    uint64_t const FLAGS = subject.resource_lock.lock_irqsave();
    usage.current = subject.resources.entries[AS].current;
    usage.peak = subject.resources.entries[AS].peak;
    subject.resource_lock.unlock_irqrestore(FLAGS);
}

AddressSpaceObservation::~AddressSpaceObservation() {
    if (m_subject != nullptr) {
        constexpr auto SCOPE = perf::WkiPerfScope::LOCAL_VMEM;
        constexpr auto OP = static_cast<uint8_t>(perf::WkiPerfLocalVmemOp::VAS_OBSERVE);
        uint64_t const SESSION = perf::current_wki_capture_session(SCOPE, OP);
        bool const TRACE = SESSION != perf::PERF_ANY_SESSION;
        uint64_t const START = TRACE ? time::get_us() : 0;
        bool const OBSERVED = observe();
        if (TRACE) {
            uint64_t const NOW = time::get_us();
            uint32_t const ELAPSED = static_cast<uint32_t>(std::min<uint64_t>(UINT32_MAX, NOW >= START ? NOW - START : 0));
            int32_t const STATUS = OBSERVED ? 0 : -EFAULT;
            perf::record_wki_event(static_cast<uint32_t>(cpu::current_cpu()), m_subject->pid, SCOPE, OP, perf::WkiPerfPhase::END, 0, 0,
                                   perf::next_wki_trace_correlation(), STATUS, ELAPSED, 0, SESSION);
            perf::record_wki_summary(SCOPE, OP, 0, 0, STATUS, ELAPSED, true, 0, 0, SESSION);
        }
    }
}
}  // namespace ker::mod::sched::resource
