#pragma once

// Host shim: mirror the public mini-malloc constants/API shape that host tests
// compile against. The kmalloc shim still routes allocations to libc.

#include <cstddef>
#include <cstdint>

namespace ker::mod::mm::mini_malloc {

constexpr size_t SLAB_SIZE10 = 0x10;
constexpr size_t SLAB_SIZE20 = 0x20;
constexpr size_t SLAB_SIZE40 = 0x40;
constexpr size_t SLAB_SIZE80 = 0x80;
constexpr size_t SLAB_SIZE100 = 0x100;
constexpr size_t SLAB_SIZE200 = 0x200;
constexpr size_t SLAB_SIZE300 = 0x300;
constexpr size_t SLAB_SIZE400 = 0x400;
constexpr size_t SLAB_SIZE800 = 0x800;

struct MiniSlabStats {
    const char* name;
    uint64_t object_size;
    uint64_t slab_count;
    uint64_t total_blocks;
    uint64_t free_blocks;
    uint64_t page_bytes;
};

inline void mini_malloc_init() {}
inline auto mini_malloc(size_t /*size*/) -> void* { return nullptr; }
inline auto mini_free(void* /*address*/) -> void { return; }
inline void mini_dump_stats() {}
inline auto mini_get_total_slab_bytes() -> uint64_t { return 0; }
inline auto mini_collect_slab_stats(MiniSlabStats* /*out*/, size_t /*max_rows*/) -> size_t { return 0; }
inline auto mini_get_slab_size(void* /*ptr*/) -> size_t { return 0; }
inline void mini_iter_live_debug_slots(void* /*userdata*/, void (* /*fn*/)(void* ud, const void* user_ptr, size_t block_size, uintptr_t debug_ref)) {}

}  // namespace ker::mod::mm::mini_malloc
