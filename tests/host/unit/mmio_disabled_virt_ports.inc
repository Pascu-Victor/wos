// Explicit flag-absent ports for existing focused VM fixtures. These fixtures
// do not link the typed MMIO owner engine. Enabling its branch aborts rather
// than manufacturing a successful permission/physical-owner result. The actual
// typed engine and native gateway effects have a separate composed fixture.
#pragma once

#include <atomic>
#include <cstdint>
#include <cstdlib>

namespace ker::mod::mm::paging {
struct PageTable;
}
inline std::atomic<bool> typed_mmio_guards{};
inline void require_mmio_disabled_fixture() noexcept {
    if (typed_mmio_guards.load(std::memory_order_relaxed)) std::abort();
}
namespace ker::mod::mm::mmio::detail {
inline void note_topology_publication_locked() noexcept { require_mmio_disabled_fixture(); }
}  // namespace ker::mod::mm::mmio::detail
inline auto typed_mmio_table_allowed(const ker::mod::mm::paging::PageTable*) noexcept -> bool {
    require_mmio_disabled_fixture();
    return true;
}
inline void require_ordinary_page_table(const ker::mod::mm::paging::PageTable*) noexcept { require_mmio_disabled_fixture(); }
inline void require_ordinary_virtual_range(const ker::mod::mm::paging::PageTable*, uint64_t, uint64_t) noexcept {
    require_mmio_disabled_fixture();
}
inline void require_ordinary_mapping(const ker::mod::mm::paging::PageTable*, uint64_t, uint64_t, uint64_t, uint64_t) noexcept {
    require_mmio_disabled_fixture();
}
inline void require_ordinary_leaf_locked(const ker::mod::mm::paging::PageTable*, uint64_t, uint64_t, uint64_t, unsigned = 1) noexcept {
    require_mmio_disabled_fixture();
}
inline auto ordinary_physical_access(uint64_t, uint64_t) noexcept -> bool {
    require_mmio_disabled_fixture();
    return true;
}
