#pragma once

// Host shim: map physical page allocation to host heap allocation.

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string_view>

namespace ker::mod::mm::phys {

inline auto page_alloc(uint64_t size, std::string_view = "anonymous") -> void* {
    if (size == 0) {
        size = 1;
    }
    constexpr size_t ALIGNMENT = alignof(std::max_align_t);
    size = (size + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
    return std::aligned_alloc(alignof(std::max_align_t), size);
}

inline auto page_alloc_full_overwrite(uint64_t size, std::string_view name = "full_overwrite") -> void* { return page_alloc(size, name); }

inline void page_free(void* page) { std::free(page); }

inline auto get_free_mem_pages() -> uint64_t { return 1024 * 1024; }
inline auto get_total_mem_bytes() -> uint64_t { return get_free_mem_pages() * 4096ULL; }

}  // namespace ker::mod::mm::phys
