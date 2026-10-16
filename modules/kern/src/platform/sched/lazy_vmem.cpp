#ifdef WOS_SELFTEST
#include <test/numa_lifecycle_mm_debt.hpp>
#endif
#include <cerrno>
#include <platform/mm/virt.hpp>
#include <platform/sched/lazy_vmem.hpp>
#include <platform/sched/scheduler.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <utility>
#include <vfs/vfs.hpp>

namespace ker::mod::sched::task {
#ifdef WOS_SELFTEST
namespace debt = ::ker::test::numa_lifecycle::mm_debt;
using DebtField = ::ker::test::numa_lifecycle::CounterField;
static_assert(static_cast<uint8_t>(LazyVmemKind::ANONYMOUS) == 0 && static_cast<uint8_t>(LazyVmemKind::FILE_BACKED) == 1 &&
              static_cast<uint8_t>(LazyVmemKind::NATIVE_FILE_BACKED) == 2 && static_cast<uint8_t>(LazyVmemKind::TMPFS_READ_SHARED) == 3 &&
              static_cast<uint8_t>(LazyVmemKind::TMPFS_WRITE_SHARED) == 4);
#endif

auto try_retain_lazy_vmem_range(const LazyVmemRange& range) noexcept -> bool {
#ifdef WOS_SELFTEST
    uint8_t const KIND = static_cast<uint8_t>(range.kind);
    debt::begin_change();
    bool const RETAINED =
        lazy_vmem_has_lifetime(range)
            ? range.lifetime != nullptr && range.lifetime->try_acquire(mm::file_mapping::FamilyOwner::LAZY)
            : range.kind != LazyVmemKind::FILE_BACKED || range.file == nullptr || ker::vfs::vfs_try_retain_file(range.file);
    if (RETAINED) debt::add_lazy_row(KIND);
    debt::end_change();
    return RETAINED;
#else
    if (lazy_vmem_has_lifetime(range)) return range.lifetime != nullptr && range.lifetime->try_acquire(mm::file_mapping::FamilyOwner::LAZY);
    return range.kind != LazyVmemKind::FILE_BACKED || range.file == nullptr || ker::vfs::vfs_try_retain_file(range.file);
#endif
}

void release_lazy_vmem_range(const LazyVmemRange& range) noexcept {
#ifdef WOS_SELFTEST
    uint8_t const KIND = static_cast<uint8_t>(range.kind);
    debt::begin_change();
    debt::add(DebtField::LAZY_RETURNS);
    debt::end_change();
#endif
    if (lazy_vmem_has_lifetime(range)) {
        if (range.lifetime != nullptr) range.lifetime->release(mm::file_mapping::FamilyOwner::LAZY);
    } else if (range.kind == LazyVmemKind::FILE_BACKED && range.file != nullptr) {
        ker::vfs::vfs_put_file(range.file);
    }
#ifdef WOS_SELFTEST
    debt::begin_change();
    debt::drop_lazy_row(KIND);
    debt::drop(DebtField::LAZY_RETURNS);
    debt::end_change();
#endif
}

void release_lazy_vmem_range_refs(LazyVmemRangeVec& ranges) noexcept {
    for (auto const& range : ranges) release_lazy_vmem_range(range);
    ranges.clear();
}

auto lazy_vmem_ranges_equal(const LazyVmemRange& left, const LazyVmemRange& right) noexcept -> bool {
    if (left.kind != right.kind) return false;
    bool const SAME_OWNER = lazy_vmem_has_lifetime(left) ? left.lifetime == right.lifetime : left.file == right.file;
    return SAME_OWNER && left.start == right.start && left.end == right.end && left.prot == right.prot && left.flags == right.flags &&
           left.file_offset == right.file_offset && left.file_dev == right.file_dev && left.file_ino == right.file_ino &&
           left.file_size == right.file_size && left.file_mtime_sec == right.file_mtime_sec &&
           left.file_mtime_nsec == right.file_mtime_nsec && left.file_ctime_sec == right.file_ctime_sec &&
           left.file_ctime_nsec == right.file_ctime_nsec;
}

LazyVmemClone::~LazyVmemClone() { reset(); }

void LazyVmemClone::reset() noexcept {
    prepared_ = false;
    root_ = nullptr;
    root_incarnation_ = 0;
    release_lazy_vmem_range_refs(rows_);
    auto* source = std::exchange(source_, nullptr);
    if (source != nullptr) source->release();
}

auto LazyVmemClone::prepare(Task& source) -> int {
    if (source_ != nullptr || prepared_ || !rows_.empty()) return -EBUSY;
    if (!sched::interrupts_enabled() || sched::preempt_count() != 0) return -EWOULDBLOCK;
    if (!source.try_acquire_lifetime_ref()) return -ESRCH;
    source_ = &source;
    int result = -EAGAIN;
    // Growth/cohort churn never forces an allocation while guards are held.
    for (unsigned attempt = 0; attempt < 4; ++attempt) {
        size_t count = 0;
        bool eligible = false;
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            uint64_t const FLAGS = source.lazy_vmem_lock.lock_irqsave();
            eligible = source.state.load(std::memory_order_acquire) == TaskState::ACTIVE &&
                       !source.usercopy_pagemap_closing.load(std::memory_order_acquire) && source.pagemap != nullptr &&
                       source.native_file_mapping_retirement.load(std::memory_order_acquire) == nullptr;
            if (eligible) count = source.lazy_vmem_ranges.size();
            source.lazy_vmem_lock.unlock_irqrestore(FLAGS);
        }
        if (!eligible) break;
        if (!rows_.reserve(count)) {
            result = -ENOMEM;
            break;
        }
        bool retry = false;
        {
            ker::syscall::vmem::SharedVmemPublicationGuard publication;
            uint64_t const FLAGS = source.lazy_vmem_lock.lock_irqsave();
            auto* root = source.pagemap;
            uint64_t incarnation = 0;
            eligible = source.state.load(std::memory_order_acquire) == TaskState::ACTIVE &&
                       !source.usercopy_pagemap_closing.load(std::memory_order_acquire) && root != nullptr &&
                       source.native_file_mapping_retirement.load(std::memory_order_acquire) == nullptr &&
                       mm::virt::capture_user_root_incarnation(root, incarnation);
            if (eligible && source.lazy_vmem_ranges.size() > rows_.capacity()) {
                retry = true;
            } else if (eligible) {
                result = 0;
                for (auto const& range : source.lazy_vmem_ranges) {
                    if (range.kind == LazyVmemKind::NATIVE_FILE_BACKED) {
                        result = -EBUSY;
                        break;
                    }
                    if (!rows_.push_back_no_grow(range)) __builtin_trap();
                    if (!try_retain_lazy_vmem_range(range)) {
                        rows_.pop_back();  // This row never acquired an owner.
                        result = -EOVERFLOW;
                        break;
                    }
                }
                if (result == 0) {
                    root_ = root;
                    root_incarnation_ = incarnation;
                    prepared_ = true;
                }
            }
            source.lazy_vmem_lock.unlock_irqrestore(FLAGS);
        }
        if (prepared_) return 0;
        if (!retry) break;
    }
    reset();  // Prefix releases and possible final closes occur after guards.
    return result;
}

auto LazyVmemClone::matches(Task& source) const -> bool {
    if (!prepared_ || source_ != &source) return false;
    uint64_t const FLAGS = source.lazy_vmem_lock.lock_irqsave();
    uint64_t incarnation = 0;
    bool match = source.state.load(std::memory_order_acquire) == TaskState::ACTIVE &&
                 !source.usercopy_pagemap_closing.load(std::memory_order_acquire) && source.pagemap == root_ &&
                 source.native_file_mapping_retirement.load(std::memory_order_acquire) == nullptr &&
                 mm::virt::capture_user_root_incarnation(root_, incarnation) && incarnation == root_incarnation_ &&
                 source.lazy_vmem_ranges.size() == rows_.size();
    for (size_t i = 0; match && i < rows_.size(); ++i) match = lazy_vmem_ranges_equal(rows_.at(i), source.lazy_vmem_ranges.at(i));
    source.lazy_vmem_lock.unlock_irqrestore(FLAGS);
    return match;
}

auto LazyVmemClone::consume(Task& destination) -> bool {
    if (!prepared_ || &destination == source_ || !matches(*source_)) return false;
    uint64_t const FLAGS = destination.lazy_vmem_lock.lock_irqsave();
    bool const EMPTY = destination.lazy_vmem_ranges.empty();
    if (EMPTY) {
        // Empty alone does not imply heap-free. Swap it into rows_ so no old
        // storage is deleted until this object's outside-guard destruction.
        LazyVmemRangeVec empty_storage(std::move(destination.lazy_vmem_ranges));
        destination.lazy_vmem_ranges = std::move(rows_);
        rows_ = std::move(empty_storage);
        prepared_ = false;
    }
    destination.lazy_vmem_lock.unlock_irqrestore(FLAGS);
    return EMPTY;
}

}  // namespace ker::mod::sched::task
