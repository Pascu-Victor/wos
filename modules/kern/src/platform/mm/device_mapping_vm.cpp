#include <algorithm>
#include <cerrno>
#include <new>
#include <platform/mm/addr.hpp>
#include <platform/mm/device_mapping_vm.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sys/syscall_preparation.hpp>
#include <platform/sys/usercopy.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <utility>

namespace ker::mod::mm::device_vm {
namespace {
constexpr uint64_t PAGE = paging::PAGE_SIZE;
constexpr uint64_t USER_END = uint64_t{1} << 47;
constexpr uint64_t PROTECTIONS = 3;

auto ordinary() noexcept -> bool {
    return phys::can_wait_for_reclaim() && !sys::usercopy::access_window_active() &&
           !file_mapping::LifetimeRetirementExecutor::is_current_worker();
}
auto interval(uint64_t start, uint64_t end) noexcept -> bool {
    return start >= PAGE && start < end && end <= USER_END && ((start | end) & (PAGE - 1)) == 0;
}
auto checked_count(uint64_t value, size_t element) noexcept -> bool { return value <= SIZE_MAX / element; }
auto view_valid(const file_mapping::FileMappingLifetime& family, uint64_t offset, uint64_t length, uint64_t protections) noexcept -> bool {
    auto const* object = family.device_object();
    return family.is_device_backed() && object != nullptr && family.device_generation() == object->generation() &&
           family.device_object_owner().role() == ker::dev::mapping::ObjectRole::binding && length != 0 &&
           ((offset | length) & (PAGE - 1)) == 0 && offset >= family.device_window_base() && offset < family.device_window_end() &&
           length <= family.device_window_end() - offset && family.device_window_end() <= object->bytes() &&
           (protections & ~PROTECTIONS) == 0 && (protections & ~family.device_max_protections()) == 0;
}
void release_removed(paging::PageTableEntry* rows, size_t count) noexcept {
    for (size_t i = 0; i < count; ++i) {
        auto const& row = rows[i];
        if (!row.present || row.frame == 0) __builtin_trap();
        auto const physical = static_cast<uint64_t>(row.frame) << paging::PAGE_SHIFT;
        phys::page_ref_dec(reinterpret_cast<void*>(addr::get_virt_pointer(physical)));
    }
}
auto collect_current(sched::task::Task& actor, Limits limits, std::unique_ptr<file_mapping::FileBindingSnapshot[]>& rows, size_t& count,
                     uint64_t& serial) -> int {
    size_t capacity = 0;
    uint64_t expected = 0;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        int const error = detail::count_current_bindings(actor, publication, capacity, expected);
        if (error != 0) return error;
    }
    if (capacity > limits.max_bindings || !checked_count(capacity, sizeof(file_mapping::FileBindingSnapshot))) return -E2BIG;
    if (capacity != 0) {
        rows.reset(new (std::nothrow) file_mapping::FileBindingSnapshot[capacity]);
        if (!rows) return -ENOMEM;
    }
    int error = 0;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        error = detail::capture_current_bindings(actor, publication, {rows.get(), capacity}, count, serial);
    }
    return error != 0 ? error : (serial == expected ? 0 : -EAGAIN);
}
}  // namespace

auto detail::BindingAccess::get(const file_mapping::FileBindingSnapshot& snapshot) noexcept -> const file_mapping::FileMappingBinding* {
    return snapshot.binding_;
}

PreparedMap::PreparedMap() noexcept = default;
PreparedMap::~PreparedMap() { reset(); }
void PreparedMap::reset() noexcept {
    if (!storage_) return;
    auto storage = std::move(storage_);
    if (storage->binding) storage->binding->release(file_mapping::BindingOwner::PREPARED);
    for (size_t i = 0; i < storage->table_count; ++i)
        if (storage->tables[i] != nullptr) huge::detail::release_table(storage->tables[i]);
}
auto PreparedMap::prepare(file_mapping::FileMappingLifetime& family, uint64_t start, uint64_t object_offset, uint64_t length,
                          uint64_t protections, Limits limits) -> int {
    if (storage_) return -EBUSY;
    if (!ordinary()) return -EWOULDBLOCK;
    if (length > UINT64_MAX - start || !interval(start, start + length) || !view_valid(family, object_offset, length, protections))
        return -EINVAL;
    uint64_t const pages = length / PAGE;
    uint64_t tables = 0;
    for (unsigned shift : {21U, 30U, 39U}) tables += ((start + length - 1) >> shift) - (start >> shift) + 1;
    if (pages > limits.max_pages || !checked_count(pages, sizeof(ker::dev::mapping::PageLoan)) ||
        !checked_count(tables, sizeof(paging::PageTable*)) || !checked_count(tables, sizeof(uint64_t)))
        return -E2BIG;
    storage_.reset(new (std::nothrow) Storage{});
    if (!storage_) return -ENOMEM;
    auto& s = *storage_;
    s.start = start;
    s.end = start + length;
    s.offset = object_offset;
    s.protections = protections;
    s.pages = static_cast<size_t>(pages);
    s.loans.reset(new (std::nothrow) ker::dev::mapping::PageLoan[s.pages]);
    s.tables.reset(new (std::nothrow) paging::PageTable*[static_cast<size_t>(tables)]{});
    s.table_physical.reset(new (std::nothrow) uint64_t[static_cast<size_t>(tables)]{});
    if (!s.loans || !s.tables || !s.table_physical) return -ENOMEM;
    s.table_count = static_cast<size_t>(tables);
    s.binding = file_mapping::FileMappingBinding::prepare_unbound(family);
    if (s.binding == nullptr) return -ENOMEM;
    for (size_t i = 0; i < s.pages; ++i)
        if (!family.device_object()->prepare_page(object_offset + i * PAGE, s.loans[i])) return -ENODEV;
    for (size_t i = 0; i < s.table_count; ++i) {
        s.tables[i] = huge::detail::allocate_table();
        if (s.tables[i] == nullptr) return -ENOMEM;
        s.table_physical[i] = reinterpret_cast<uint64_t>(addr::get_phys_pointer(reinterpret_cast<uint64_t>(s.tables[i])));
    }
    return 0;
}
auto PreparedMap::publish_current(sched::task::Task& actor, const sys::SyscallPreparationScope& preparation,
                                  const ker::syscall::vmem::SharedVmemPublicationGuard& publication) noexcept -> int {
    return detail::VmAccess::publish(*this, actor, preparation, publication);
}

PreparedChange::PreparedChange() noexcept = default;
PreparedChange::~PreparedChange() { reset(); }
void PreparedChange::reset() noexcept {
    if (!storage_) return;
    auto storage = std::move(storage_);
    release_removed(storage->removed.get(), storage->removed_count);
    storage->after_tlb.release_after_completed_tlb();
    for (size_t i = 0; i < storage->fragment_count; ++i)
        if (storage->fragments[i].binding) storage->fragments[i].binding->release(file_mapping::BindingOwner::PREPARED);
}
auto PreparedChange::prepare_current(sched::task::Task& actor, uint64_t start, uint64_t end, Change change, uint64_t protections,
                                     Limits limits) -> int {
    if (storage_) return -EBUSY;
    if (!ordinary()) return -EWOULDBLOCK;
    if (!interval(start, end) || (protections & ~PROTECTIONS) != 0 || (change == Change::Remove && protections != 0)) return -EINVAL;
    if ((end - start) / PAGE > limits.max_pages) return -E2BIG;
    storage_.reset(new (std::nothrow) Storage{});
    if (!storage_) return -ENOMEM;
    auto& s = *storage_;
    s.start = start;
    s.end = end;
    s.change = change;
    s.protections = protections;
    int const error = collect_current(actor, limits, s.old, s.count, s.serial);
    if (error != 0) return error;
    size_t kept = 0;
    for (size_t i = 0; i < s.count; ++i) {
        auto const* binding = detail::BindingAccess::get(s.old[i]);
        if (binding->lifetime()->is_device_backed() && binding->start() < end && start < binding->end()) {
            if (kept != i) s.old[kept] = std::move(s.old[i]);
            ++kept;
        } else {
            s.old[i].reset();
        }
    }
    s.count = kept;
    if (s.count == 0) return -ENOENT;
    std::sort(s.old.get(), s.old.get() + s.count,
              [](auto const& a, auto const& b) { return detail::BindingAccess::get(a)->start() < detail::BindingAccess::get(b)->start(); });
    if (!checked_count(s.count, 3 * sizeof(Fragment))) return -E2BIG;
    s.fragments.reset(new (std::nothrow) Fragment[s.count * 3]{});
    s.page_capacity = static_cast<size_t>((end - start) / PAGE);
    s.removed.reset(new (std::nothrow) paging::PageTableEntry[s.page_capacity]{});
    if (!s.fragments || !s.removed) return -ENOMEM;
    for (size_t i = 0; i < s.count; ++i) {
        auto const* old = detail::BindingAccess::get(s.old[i]);
        auto& family = *old->lifetime();
        uint64_t const first = std::max(start, old->start()), last = std::min(end, old->end());
        if (!view_valid(family, old->file_offset(), old->end() - old->start(), protections)) return -EACCES;
        auto fragment = [&](uint64_t a, uint64_t b) -> bool {
            if (a == b) return true;
            auto& next = s.fragments[s.fragment_count++];
            next.start = a;
            next.end = b;
            next.offset = old->file_offset() + (a - old->start());
            next.binding = file_mapping::FileMappingBinding::prepare_unbound(family);
            return next.binding != nullptr;
        };
        if (!fragment(old->start(), first) || (change == Change::Protect && !fragment(first, last)) || !fragment(last, old->end()))
            return -ENOMEM;
    }
    return 0;
}
auto PreparedChange::commit_current(sched::task::Task& actor, const sys::SyscallPreparationScope& preparation,
                                    const ker::syscall::vmem::SharedVmemPublicationGuard& publication) noexcept -> int {
    return detail::VmAccess::change(*this, actor, preparation, publication);
}

PreparedRevoke::PreparedRevoke() noexcept = default;
PreparedRevoke::~PreparedRevoke() { reset(); }
void PreparedRevoke::reset() noexcept {
    if (!storage_) return;
    auto storage = std::move(storage_);
    release_removed(storage->removed.get(), storage->removed_count);
    storage->after_tlb.release_after_completed_tlb();
}
auto PreparedRevoke::prepare(const file_mapping::FileBindingSnapshot& snapshot, Limits limits) -> int {
    if (storage_) return -EBUSY;
    if (!ordinary()) return -EWOULDBLOCK;
    auto const* binding = detail::BindingAccess::get(snapshot);
    if (binding == nullptr || !binding->lifetime()->is_device_backed() || !interval(binding->start(), binding->end())) return -EINVAL;
    uint64_t const pages = (binding->end() - binding->start()) / PAGE;
    if (pages > limits.max_pages || !checked_count(pages, sizeof(paging::PageTableEntry))) return -E2BIG;
    storage_.reset(new (std::nothrow) Storage{});
    if (!storage_) return -ENOMEM;
    storage_->snapshot = &snapshot;
    storage_->binding = binding;
    storage_->page_capacity = static_cast<size_t>(pages);
    storage_->removed.reset(new (std::nothrow) paging::PageTableEntry[storage_->page_capacity]{});
    return storage_->removed ? 0 : -ENOMEM;
}
auto PreparedRevoke::commit(virt::BindingRootMutationLease& lease,
                            const ker::syscall::vmem::SharedVmemPublicationGuard& publication) noexcept -> int {
    return detail::VmAccess::revoke(*this, lease, publication);
}

PreparedFork::PreparedFork() noexcept = default;
PreparedFork::~PreparedFork() { reset(); }
void PreparedFork::reset() noexcept {
    if (!storage_) return;
    auto storage = std::move(storage_);
    for (size_t i = 0; i < storage->count; ++i)
        if (storage->children && storage->children[i]) storage->children[i]->release(file_mapping::BindingOwner::PREPARED);
}
auto PreparedFork::prepare_current(sched::task::Task& actor, const sys::SyscallPreparationScope& preparation, Limits limits) -> int {
    if (storage_) return -EBUSY;
    if (!ordinary() || !preparation.ready()) return -EWOULDBLOCK;
    storage_.reset(new (std::nothrow) Storage{});
    if (!storage_) return -ENOMEM;
    auto& s = *storage_;
    int const error = collect_current(actor, limits, s.source, s.count, s.serial);
    if (error != 0) return error;
    size_t kept = 0;
    uint64_t pages = 0;
    for (size_t i = 0; i < s.count; ++i) {
        auto const* binding = detail::BindingAccess::get(s.source[i]);
        if (binding->lifetime()->is_device_backed()) {
            uint64_t const n = (binding->end() - binding->start()) / PAGE;
            if (n > limits.max_pages - std::min(pages, limits.max_pages)) return -E2BIG;
            pages += n;
            if (kept != i) s.source[kept] = std::move(s.source[i]);
            ++kept;
        } else {
            s.source[i].reset();
        }
    }
    s.count = kept;
    if (s.count == 0) return 0;
    if (!checked_count(s.count, sizeof(file_mapping::FileMappingBinding*))) return -E2BIG;
    s.children.reset(new (std::nothrow) file_mapping::FileMappingBinding*[s.count]{});
    if (!s.children) return -ENOMEM;
    for (size_t i = 0; i < s.count; ++i) {
        auto const* source = detail::BindingAccess::get(s.source[i]);
        s.children[i] = file_mapping::FileMappingBinding::prepare_unbound(*source->lifetime());
        if (s.children[i] == nullptr) return -ENOMEM;
    }
    return 0;
}
auto PreparedFork::matches_current(sched::task::Task& actor, const sys::SyscallPreparationScope& preparation,
                                   const ker::syscall::vmem::SharedVmemPublicationGuard& publication) noexcept -> bool {
    return detail::VmAccess::match_fork(*this, actor, preparation, publication);
}
auto PreparedFork::attach_before_copy(sched::task::Task& actor, sched::task::Task& child, paging::PageTable* root,
                                      const ker::syscall::vmem::SharedVmemPublicationGuard& publication) noexcept -> int {
    return detail::VmAccess::attach_fork(*this, actor, child, root, publication);
}

}  // namespace ker::mod::mm::device_vm
