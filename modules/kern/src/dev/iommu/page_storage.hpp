#pragma once

#include <dev/iommu/page_table.hpp>
#include <span>

namespace ker::dev::iommu::vtd {
struct TableRecord {
    TablePage page{};
    uint64_t prefix = 0;
    uint8_t level = 0;
};

// Kernel table allocator/cache backend. Metadata must be private to one root,
// stable and initially empty. PageTable serializes all access. No destructor
// frees pages: exposed trees have kernel lifetime until platform revocation.
class PageStorage {
    std::span<TableRecord> records_;
    bool coherent_;
    size_t cache_line_;
    bool (*invalidate_)(void*);
    void* context_;

   public:
    PageStorage(std::span<TableRecord> records, bool coherent, size_t cache_line, bool (*invalidate)(void*) = nullptr,
                void* context = nullptr)
        : records_(records), coherent_(coherent), cache_line_(cache_line), invalidate_(invalidate), context_(context) {}
    PageStorage(const PageStorage&) = delete;
    auto operator=(const PageStorage&) -> PageStorage& = delete;
    auto allocate(uint8_t level, uint64_t prefix) -> TablePage;
    auto find(uint64_t physical, uint8_t level, uint64_t prefix) -> TranslationTable*;
    void publish(void* pointer, size_t bytes);
    auto invalidate() -> bool;
    // Only PageTable::destroy_unpublished may use this for a prepared root.
    void release_all();
};
}  // namespace ker::dev::iommu::vtd
