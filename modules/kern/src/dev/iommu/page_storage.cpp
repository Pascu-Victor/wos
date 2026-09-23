#include "page_storage.hpp"

#include <new>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>

namespace ker::dev::iommu::vtd {
auto PageStorage::allocate(uint8_t level, uint64_t prefix) -> TablePage {
    if (!coherent_ && (cache_line_ < 8 || cache_line_ > PAGE_SIZE || (cache_line_ & (cache_line_ - 1)) != 0)) return {};
    for (auto& record : records_) {
        if (record.page.cpu != nullptr) continue;
        auto* storage = mod::mm::phys::page_alloc_may_fail(mod::mm::PhysicalPageOwner::IOMMU_TABLE, PAGE_SIZE, "iommu_second_stage");
        if (storage == nullptr) return {};
        record = {.page = {new (storage) TranslationTable{}, reinterpret_cast<uint64_t>(storage) - mod::mm::addr::get_hhdm_offset()},
                  .prefix = prefix,
                  .level = level};
        return record.page;
    }
    return {};
}

auto PageStorage::find(uint64_t physical, uint8_t level, uint64_t prefix) -> TranslationTable* {
    for (auto const& record : records_)
        if (record.page.cpu != nullptr && record.page.physical == physical && record.level == level && record.prefix == prefix)
            return record.page.cpu;
    return nullptr;
}

void PageStorage::publish(void* pointer, size_t bytes) {
    if (!coherent_) {
        // allocate refuses an invalid cache-line contract. Entries are naturally
        // aligned and at most one page; include the line containing the first byte.
        auto const START = reinterpret_cast<uintptr_t>(pointer) & ~(cache_line_ - 1);
        auto const END = reinterpret_cast<uintptr_t>(pointer) + bytes;
        for (auto address = START; address < END; address += cache_line_) asm volatile("clflush (%0)" ::"r"(address) : "memory");
    }
    asm volatile("mfence" ::: "memory");
}

auto PageStorage::invalidate() -> bool { return invalidate_ != nullptr && invalidate_(context_); }

void PageStorage::release_all() {
    for (auto& record : records_) {
        if (record.page.cpu == nullptr) continue;
        mod::mm::phys::page_free(record.page.cpu);
        record = {};
    }
}
}  // namespace ker::dev::iommu::vtd
