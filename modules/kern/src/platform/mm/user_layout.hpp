#pragma once

#include <cstdint>

#include "platform/mm/paging.hpp"

namespace ker::mod::mm::user_layout {

enum class ImageRole : uint8_t {
    MAIN,
    INTERPRETER,
};

struct AddressWindow {
    uint64_t begin;
    uint64_t end;
    uint64_t alignment;
};

// All windows are half-open and deliberately disjoint. Consequently, a lazy
// range owned by one image role cannot alias a placement from another role.
// The 2 MiB image granularity still supplies at least eight nominal bits in
// each image window; mmap and thread placement each expose well over 20 bits.
inline constexpr uint64_t IMAGE_ALIGNMENT = 2ULL * 1024 * 1024;
inline constexpr uint64_t MMAP_ALIGNMENT = IMAGE_ALIGNMENT;
inline constexpr uint64_t THREAD_ALIGNMENT = IMAGE_ALIGNMENT;

// Starts above WOS's fixed 0x04000000 signal-restorer mapping.
inline constexpr AddressWindow MAIN_IMAGE_WINDOW{0x0000000008000000ULL, 0x0000000030000000ULL, IMAGE_ALIGNMENT};
inline constexpr AddressWindow INTERPRETER_WINDOW{0x0000000030000000ULL, 0x0000000070000000ULL, IMAGE_ALIGNMENT};
inline constexpr AddressWindow MMAP_WINDOW{0x0000200000000000ULL, 0x0000600000000000ULL, MMAP_ALIGNMENT};
// Leaves the fixed ELF-debug metadata starting at 0x700000000000 untouched.
inline constexpr AddressWindow THREAD_WINDOW{0x0000740000000000ULL, 0x00007F0000000000ULL, THREAD_ALIGNMENT};

// True only when every page in [start, start + size) is neither mapped nor
// reserved. Publication/metadata locks remain the caller's responsibility.
[[nodiscard]] auto range_is_free(paging::PageTable* page_table, uint64_t start, uint64_t size) -> bool;

// image_min_vaddr and image_max_vaddr are page-aligned PT_LOAD extents, with
// image_max_vaddr exclusive. The result is an ELF load bias; the checked range
// is [base + image_min_vaddr, base + image_max_vaddr).
[[nodiscard]] auto choose_image_base(paging::PageTable* page_table, ImageRole role, uint64_t image_min_vaddr, uint64_t image_max_vaddr,
                                     uint64_t& base) -> bool;

// Choose the low address of a total_span-byte, page-aligned thread layout.
[[nodiscard]] auto choose_thread_region(paging::PageTable* page_table, uint64_t total_span, uint64_t& start) -> bool;

// Choose a randomized starting cursor for non-fixed anonymous/file mappings.
[[nodiscard]] auto choose_mmap_cursor(uint64_t& cursor) -> bool;

#ifdef WOS_SELFTEST
// Pure deterministic selector used by KTEST. It performs the same arithmetic
// validation as production placement without consulting entropy or page tables.
[[nodiscard]] auto selftest_choose_aligned(const AddressWindow& window, uint64_t span, uint64_t sample, uint64_t& start) -> bool;
#endif

}  // namespace ker::mod::mm::user_layout
