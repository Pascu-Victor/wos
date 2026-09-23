#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <dev/iommu/vtd_model.hpp>
#include <optional>

namespace ker::dev::iommu::vtd {

struct alignas(PAGE_SIZE) TranslationTable {
    std::array<uint64_t, 512> entries{};
};
static_assert(sizeof(TranslationTable) == PAGE_SIZE);
static_assert(std::atomic_ref<uint64_t>::is_always_lock_free);

struct TablePage {
    TranslationTable* cpu = nullptr;
    uint64_t physical = 0;
};
enum class TableError : uint8_t { none, busy, invalid, unavailable, allocation, overlap, mismatch, corrupt, quarantined };
struct TableResult {
    TableError error = TableError::none;
    // This operation may have exposed its data pages. A failed map with this
    // flag must pin its backing; false permits freeing that new backing only.
    bool retain_data = false;
};
struct TableSnapshot {
    const void* owner = nullptr;
    uint64_t generation = 0;
    uint64_t root = 0;
    uint64_t table_pages = 0;
    // Includes cleared leaves whose invalidation could not be proven.
    uint64_t owned_pages = 0;
    bool exposed = false;
    bool quarantined = false;
};

// Exact legacy second-stage 4-KiB mappings. No huge pages, ATS, PASID or A/D
// updates. Caller qualifies capabilities and owns data-buffer lifetimes.
// Backend owns every allocated table, including empty published branches:
//   allocate(level, prefix) -> TablePage; find(physical, level, prefix) -> table*;
//   publish(pointer, bytes); invalidate() -> bool; release_all().
// find must reject foreign pages and pages belonging to another level/subtree.
// publish performs cache writeback (when needed) and orders stores before MMIO.
// invalidate serializes the unit, drains reads/writes and verifies completion.
// Mutation admission is nonblocking, and holds no spin/IRQ lock across allocate.
// Kernel callers must hold cooperative shutdown ownership and use task/boot
// context. There is deliberately no destructor which frees hardware storage.
template <typename Backend, uint64_t GenerationLimit = UINT64_MAX>
class PageTable {
    static_assert(GenerationLimit != 0);
    static constexpr uint64_t MAX_MAPPING = 64 * 1024 * 1024;
    Backend& backend_;
    std::atomic_flag busy_ = ATOMIC_FLAG_INIT;
    TableSnapshot state_{};
    uint64_t generation_ = 0;
    uint8_t physical_bits_ = 0, iova_bits_ = 0, levels_ = 0;

    struct Guard {
        std::atomic_flag& flag;
        bool admitted;
        explicit Guard(std::atomic_flag& value) : flag(value), admitted(!flag.test_and_set(std::memory_order_acquire)) {}
        ~Guard() {
            if (admitted) flag.clear(std::memory_order_release);
        }
        Guard(const Guard&) = delete;
        auto operator=(const Guard&) -> Guard& = delete;
    };
    struct Slot {
        uint64_t* entry = nullptr;
        TableError error = TableError::none;
    };
    static auto prefix(uint64_t address, uint8_t level) -> uint64_t { return address & ~((uint64_t{1} << (12U + 9U * level)) - 1); }
    static auto load(uint64_t& entry) -> uint64_t { return std::atomic_ref<uint64_t>(entry).load(std::memory_order_acquire); }
    void store(uint64_t& entry, uint64_t value) {
        std::atomic_ref<uint64_t>(entry).store(value, std::memory_order_release);
        backend_.publish(&entry, sizeof(entry));
    }
    auto address_mask() const -> uint64_t { return ((uint64_t{1} << physical_bits_) - 1) & ~(PAGE_SIZE - 1); }
    auto valid_entry(uint64_t entry, bool leaf) const -> bool {
        return entry == 0 || ((entry & ~address_mask() & ~uint64_t{3}) == 0 && (leaf ? (entry & 3U) != 0 : (entry & 3U) == 3U));
    }
    auto valid_range(uint64_t iova, uint64_t physical, uint64_t bytes, Access access) const -> bool {
        auto const PERMISSIONS = static_cast<uint8_t>(access);
        return bytes != 0 && bytes <= MAX_MAPPING && ((iova | physical | bytes) & (PAGE_SIZE - 1)) == 0 && PERMISSIONS != 0 &&
               (PERMISSIONS & ~3U) == 0 && iova < (uint64_t{1} << iova_bits_) && bytes <= (uint64_t{1} << iova_bits_) - iova &&
               physical < (uint64_t{1} << physical_bits_) && bytes <= (uint64_t{1} << physical_bits_) - physical;
    }
    auto allocate(uint8_t level, uint64_t base) -> TablePage {
        auto page = backend_.allocate(level, base);
        if (page.cpu == nullptr) return {};
        // The backend retains even an invalid allocation until explicit cleanup.
        ++state_.table_pages;
        if (page.physical == 0 || !page_address_valid(page.physical, physical_bits_) ||
            (reinterpret_cast<uintptr_t>(page.cpu) & (PAGE_SIZE - 1)) != 0 || backend_.find(page.physical, level, base) != page.cpu) {
            state_.quarantined = true;
            return {};
        }
        page.cpu->entries.fill(0);
        backend_.publish(page.cpu, sizeof(TranslationTable));
        return page;
    }
    auto slot(uint64_t iova, bool create) -> Slot {
        uint64_t physical = state_.root;
        for (uint8_t level = levels_; level != 0; --level) {
            auto* table = backend_.find(physical, level, prefix(iova, level));
            if (table == nullptr) return {.error = TableError::corrupt};
            auto& entry = table->entries.at(static_cast<size_t>((iova >> (12U + 9U * (level - 1U))) & 511U));
            uint64_t value = load(entry);
            if (!valid_entry(value, level == 1)) return {.error = TableError::corrupt};
            if (level == 1) return {.entry = &entry};
            if (value == 0) {
                if (!create) return {};
                auto const PAGE = allocate(static_cast<uint8_t>(level - 1), prefix(iova, static_cast<uint8_t>(level - 1)));
                if (PAGE.cpu == nullptr) return {.error = state_.quarantined ? TableError::corrupt : TableError::allocation};
                // A fully zeroed and cache-published child precedes its link.
                value = PAGE.physical | 3U;
                store(entry, value);
            }
            physical = value & address_mask();
        }
        return {.error = TableError::corrupt};
    }
    auto failure(TableError error) -> TableResult {
        if (error == TableError::corrupt) state_.quarantined = true;
        return {.error = error, .retain_data = state_.quarantined};
    }
    auto synchronize() -> bool {
        if (!state_.exposed || backend_.invalidate()) return true;
        state_.quarantined = true;
        return false;
    }

   public:
    explicit PageTable(Backend& backend) : backend_(backend) {}
    PageTable(const PageTable&) = delete;
    auto operator=(const PageTable&) -> PageTable& = delete;

    auto initialize(uint8_t physical_bits, uint8_t iova_bits, uint8_t levels) -> TableError {
        Guard guard(busy_);
        if (!guard.admitted) return TableError::busy;
        if (state_.root != 0 || state_.table_pages != 0 || generation_ == GenerationLimit) return TableError::unavailable;
        if (physical_bits < 12 || physical_bits > 52 || levels < 3 || levels > 5 || iova_bits < 12 || iova_bits > 12U + 9U * levels)
            return TableError::invalid;
        physical_bits_ = physical_bits;
        iova_bits_ = iova_bits;
        levels_ = levels;
        auto const ROOT = allocate(levels, 0);
        if (ROOT.cpu == nullptr) return state_.quarantined ? TableError::corrupt : TableError::allocation;
        state_.owner = this;
        state_.generation = ++generation_;
        state_.root = ROOT.physical;
        return TableError::none;
    }
    // Must precede publication of the context/root pointer to hardware. The
    // token binds that pointer to this lifetime, including physical-address reuse.
    auto expose(const TableSnapshot& expected) -> bool {
        Guard guard(busy_);
        if (!guard.admitted || state_.root == 0 || state_.quarantined || expected.owner != this ||
            expected.generation != state_.generation || expected.root != state_.root)
            return false;
        state_.exposed = true;
        return true;
    }
    auto snapshot() -> std::optional<TableSnapshot> {
        Guard guard(busy_);
        return guard.admitted ? std::optional{state_} : std::nullopt;
    }
    auto map(uint64_t iova, uint64_t physical, uint64_t bytes, Access access) -> TableResult {
        Guard guard(busy_);
        if (!guard.admitted) return {.error = TableError::busy};
        if (state_.quarantined) return failure(TableError::quarantined);
        if (state_.root == 0) return {.error = TableError::unavailable};
        if (!valid_range(iova, physical, bytes, access)) return {.error = TableError::invalid};
        // Reject overlap across the WHOLE range before allocating or changing it.
        for (uint64_t offset = 0; offset < bytes; offset += PAGE_SIZE) {
            auto const SLOT = slot(iova + offset, false);
            if (SLOT.error != TableError::none) return failure(SLOT.error);
            if (SLOT.entry != nullptr && load(*SLOT.entry) != 0) return {.error = TableError::overlap};
        }
        // Failure here has exposed empty branches only, never this data buffer.
        for (uint64_t offset = 0; offset < bytes; offset += PAGE_SIZE) {
            auto const SLOT = slot(iova + offset, true);
            if (SLOT.error != TableError::none) return failure(SLOT.error);
        }
        for (uint64_t offset = 0; offset < bytes; offset += PAGE_SIZE) {
            auto const SLOT = slot(iova + offset, false);
            if (SLOT.error != TableError::none || SLOT.entry == nullptr) return failure(TableError::corrupt);
            store(*SLOT.entry, (physical + offset) | static_cast<uint8_t>(access));
            ++state_.owned_pages;
        }
        if (!synchronize()) return failure(TableError::quarantined);
        return {};
    }
    // The caller independently proved device completion and drained IRQ/readers.
    // Exact physical/access matching makes a wrong request non-destructive.
    auto unmap(uint64_t iova, uint64_t physical, uint64_t bytes, Access access) -> TableResult {
        Guard guard(busy_);
        if (!guard.admitted) return {.error = TableError::busy, .retain_data = true};
        if (state_.quarantined) return failure(TableError::quarantined);
        if (state_.root == 0) return {.error = TableError::unavailable, .retain_data = true};
        if (!valid_range(iova, physical, bytes, access)) return {.error = TableError::invalid, .retain_data = true};
        if (bytes / PAGE_SIZE > state_.owned_pages) return {.error = TableError::mismatch, .retain_data = true};
        for (uint64_t offset = 0; offset < bytes; offset += PAGE_SIZE) {
            auto const SLOT = slot(iova + offset, false);
            if (SLOT.error != TableError::none) return failure(SLOT.error);
            if (SLOT.entry == nullptr || load(*SLOT.entry) != ((physical + offset) | static_cast<uint8_t>(access)))
                return {.error = TableError::mismatch, .retain_data = true};
        }
        for (uint64_t offset = 0; offset < bytes; offset += PAGE_SIZE) {
            auto const SLOT = slot(iova + offset, false);
            if (SLOT.error != TableError::none || SLOT.entry == nullptr) return failure(TableError::corrupt);
            store(*SLOT.entry, 0);
        }
        if (!synchronize()) return failure(TableError::quarantined);
        state_.owned_pages -= bytes / PAGE_SIZE;
        return {};
    }
    // No exposed table is reclaimed, even when its leaf count is zero. Context
    // revocation and hardware walk retirement are separate platform operations.
    auto destroy_unpublished() -> bool {
        Guard guard(busy_);
        if (!guard.admitted || state_.exposed || state_.owned_pages != 0) return false;
        backend_.release_all();
        state_ = {};
        return true;
    }
    // Boot preparation rollback may contain leaves for retained firmware
    // backing. Release table storage only; referenced data is never freed by
    // this backend. An ever-exposed root cannot use this operation, even after
    // its leaves are removed. Generation stays monotonic across reuse.
    auto discard_unpublished() -> bool {
        Guard guard(busy_);
        if (!guard.admitted || state_.exposed) return false;
        backend_.release_all();
        state_ = {};
        return true;
    }
};
}  // namespace ker::dev::iommu::vtd
