#include "device_mapping_grants.hpp"

#include <abi/callnums/vmem.h>

#include <cerrno>
#include <dev/device_mapping_client.hpp>
#include <new>
#include <platform/mm/phys.hpp>
#include <utility>

namespace ker::dev::mapping {
namespace {
constinit detail::OffsetIntervals offsets;
constexpr uint64_t RIGHTS = ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_WRITE;

auto bounded_slice(uint64_t offset, uint64_t length, uint64_t bytes) noexcept -> bool {
    return length != 0 && offset % OBJECT_PAGE_SIZE == 0 && length % OBJECT_PAGE_SIZE == 0 && offset <= bytes && length <= bytes - offset;
}
auto readable(ClientAccess access) noexcept -> bool { return (access.open_flags & 3U) != 1U && (access.open_flags & 3U) != 3U; }
auto permitted(ClientAccess access) noexcept -> uint64_t {
    return readable(access)
               ? ker::abi::vmem::PROT_READ | (((access.open_flags & 3U) == 2U && !access.read_only) ? ker::abi::vmem::PROT_WRITE : 0U)
               : 0;
}
}  // namespace

auto detail::OffsetIntervals::reserve(uint64_t length, Grant& empty) noexcept -> bool {
    if (!empty.empty() || length == 0 || length % OBJECT_PAGE_SIZE != 0 || limit_ > LIMIT || limit_ % OBJECT_PAGE_SIZE != 0) return false;
    uint64_t base = next_.load(std::memory_order_relaxed);
    for (;;) {
        if (base == 0 || base % OBJECT_PAGE_SIZE != 0 || base > limit_ || OBJECT_PAGE_SIZE > limit_ - base ||
            length > limit_ - base - OBJECT_PAGE_SIZE)
            return false;
        uint64_t const end = base + length;
        if (next_.compare_exchange_weak(base, end + OBJECT_PAGE_SIZE, std::memory_order_relaxed)) {
            empty = {.generation = base / OBJECT_PAGE_SIZE, .base = base, .end = end};
            return true;
        }
    }
}

auto PreparedGrant::take_object() noexcept -> ObjectRef {
    offset_ = length_ = max_protections_ = generation_ = 0;
    return std::move(object_);
}
void PreparedGrant::reset() noexcept {
    object_.reset();
    offset_ = length_ = max_protections_ = generation_ = 0;
}

struct GrantTable::Entry {
    Grant grant{};
    ObjectRef object;
    uint64_t object_offset{}, length{}, max_protections{}, object_generation{};
};

auto GrantTable::create(size_t capacity, std::unique_ptr<GrantTable>& empty) -> int {
    if (empty || capacity == 0 || capacity > MAX_CAPACITY) return -EINVAL;
    if (!ker::mod::mm::phys::can_wait_for_reclaim()) return -EWOULDBLOCK;
    std::unique_ptr<GrantTable> table(new (std::nothrow) GrantTable);
    if (!table) return -ENOMEM;
    table->entries_.reset(new (std::nothrow) Entry[capacity]);
    if (!table->entries_) return -ENOMEM;
    table->capacity_ = capacity;
    empty = std::move(table);
    return 0;
}
GrantTable::~GrantTable() { close(); }

auto GrantTable::grant(ClientAccess access, const ObjectRef& object, uint64_t object_offset, uint64_t length, uint64_t maximum,
                       Grant& empty) -> int {
    if (!empty.empty() || !object || !bounded_slice(object_offset, length, object.get()->bytes()) || (maximum & ~RIGHTS) != 0)
        return -EINVAL;
    uint64_t const ceiling = maximum & object.get()->max_protections() & permitted(access);
    if ((ceiling & ker::abi::vmem::PROT_READ) == 0) return -EACCES;
    ObjectRef retained;
    if (!object.clone_into(retained, ObjectRole::grant)) return -ENODEV;
    ker::mod::sys::MutexGuard const guard(lock_);
    if (closed_) return -ENODEV;
    for (size_t index = 0; index < capacity_; ++index) {
        auto& entry = entries_[index];
        if (entry.object) continue;
        Grant issued;
        if (!offsets.reserve(length, issued)) return -ENOSPC;
        entry.object = std::move(retained);
        entry.grant = issued;
        entry.object_offset = object_offset;
        entry.length = length;
        entry.max_protections = ceiling;
        entry.object_generation = object.get()->generation();
        empty = issued;
        return 0;
    }
    return -ENOSPC;
}
auto GrantTable::lookup(ClientAccess access, uint64_t fake_offset, uint64_t length, uint64_t protections, uint64_t flags,
                        PreparedGrant& empty) -> int {
    constexpr uint64_t FLAGS = ker::abi::vmem::MAP_SHARED | ker::abi::vmem::MAP_FIXED | ker::abi::vmem::MAP_POPULATE;
    if (empty || fake_offset == 0 || fake_offset % OBJECT_PAGE_SIZE != 0 || length == 0 || length % OBJECT_PAGE_SIZE != 0 ||
        (protections & ~RIGHTS) != 0 || (flags & ~FLAGS) != 0 || (flags & ker::abi::vmem::MAP_SHARED) == 0)
        return -EINVAL;
    if (!readable(access)) return -EACCES;
    PreparedGrant prepared;
    ker::mod::sys::MutexGuard const guard(lock_);
    if (closed_) return -ENODEV;
    for (size_t index = 0; index < capacity_; ++index) {
        auto const& entry = entries_[index];
        if (!entry.object || fake_offset < entry.grant.base || fake_offset >= entry.grant.end) continue;
        uint64_t const delta = fake_offset - entry.grant.base;
        if (length > entry.length - delta) return -EINVAL;
        uint64_t const ceiling = entry.max_protections & permitted(access);
        if ((protections & ~ceiling) != 0) return -EACCES;
        if (!entry.object.clone_into(prepared.object_, ObjectRole::binding)) return -ENODEV;
        prepared.offset_ = entry.object_offset + delta;
        prepared.length_ = length;
        prepared.max_protections_ = ceiling;
        prepared.generation_ = entry.object_generation;
        empty = std::move(prepared);
        return 0;
    }
    return -ENOENT;
}
auto GrantTable::remove(Grant grant) -> int {
    if (grant.empty()) return -EINVAL;
    ObjectRef retired;
    {
        ker::mod::sys::MutexGuard const guard(lock_);
        for (size_t index = 0; index < capacity_; ++index) {
            auto& entry = entries_[index];
            if (!entry.object || entry.grant.base != grant.base || entry.grant.end != grant.end ||
                entry.grant.generation != grant.generation)
                continue;
            retired = std::move(entry.object);
            entry = Entry{};
            return 0;
        }
    }
    return -ENOENT;
}
void GrantTable::close() noexcept {
    {
        ker::mod::sys::MutexGuard const guard(lock_);
        closed_ = true;
    }
    for (size_t index = 0; index < capacity_; ++index) {
        ObjectRef retired;
        {
            ker::mod::sys::MutexGuard const guard(lock_);
            retired = std::move(entries_[index].object);
            entries_[index] = Entry{};
        }
    }
}

}  // namespace ker::dev::mapping
