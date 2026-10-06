#pragma once

// Physical allocation remains a host heap boundary. Optional probes observe
// real cache snapshot allocations while request ownership is still retained.
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <platform/mm/page_alloc.hpp>
#include <string_view>

namespace ker::mod::mm::phys {
namespace cache_test {
inline void (*allocation_probe)(size_t){};
inline void (*free_probe)(void*){};
inline void (*metadata_allocation_probe)(size_t){};
inline void (*metadata_free_probe)(void*, size_t){};
inline bool fail_metadata{};
inline size_t metadata_attempts{};
inline size_t metadata_frees{};
struct MetadataBacking {
    void* data{};
    size_t bytes{};
};
inline std::array<MetadataBacking, 32> metadata_owned{};
inline std::mutex metadata_mutex;
}  // namespace cache_test

inline auto host_backing(uint64_t size) -> void* {
    if (size == 0) size = 1;
    constexpr size_t ALIGNMENT = alignof(std::max_align_t);
    size = (size + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
    return std::aligned_alloc(ALIGNMENT, size);
}
inline auto page_alloc(uint64_t size, std::string_view = "anonymous") -> void* {
    if (cache_test::allocation_probe != nullptr) cache_test::allocation_probe(size);
    return host_backing(size);
}
inline auto page_alloc(PhysicalPageOwner, uint64_t size, std::string_view name = {}) -> void* { return page_alloc(size, name); }
inline auto page_alloc_full_overwrite(uint64_t size, std::string_view name = "full_overwrite") -> void* { return page_alloc(size, name); }
inline auto page_alloc_full_overwrite(PhysicalPageOwner owner, uint64_t size, std::string_view name = {}) -> void* {
    return page_alloc(owner, size, name);
}
inline auto page_alloc_full_overwrite_may_fail(uint64_t size, std::string_view name = "full_overwrite") -> void* {
    return page_alloc(size, name);
}
inline auto page_alloc_full_overwrite_may_fail(PhysicalPageOwner owner, uint64_t size, std::string_view name = {}) -> void* {
    if (owner == PhysicalPageOwner::BUFFER_CACHE_METADATA) {
        {
            std::lock_guard guard(cache_test::metadata_mutex);
            ++cache_test::metadata_attempts;
        }
        if (cache_test::metadata_allocation_probe != nullptr) cache_test::metadata_allocation_probe(size);
        if (cache_test::fail_metadata) return nullptr;
        void* data = host_backing(size);
        if (data == nullptr) return nullptr;
        std::memset(data, 0xa7, size);
        std::lock_guard guard(cache_test::metadata_mutex);
        for (auto& entry : cache_test::metadata_owned) {
            if (entry.data == nullptr) {
                entry = {data, size};
                return data;
            }
        }
        std::abort();
    }
    return page_alloc(owner, size, name);
}
inline void page_free(void* page) {
    bool metadata = false;
    size_t metadata_bytes{};
    {
        std::lock_guard guard(cache_test::metadata_mutex);
        for (auto& entry : cache_test::metadata_owned) {
            if (entry.data == page && page != nullptr) {
                metadata_bytes = entry.bytes;
                entry = {};
                ++cache_test::metadata_frees;
                metadata = true;
                break;
            }
        }
    }
    if (metadata) {
        if (cache_test::metadata_free_probe != nullptr) cache_test::metadata_free_probe(page, metadata_bytes);
        std::free(page);
        return;
    }
    if (cache_test::free_probe != nullptr) cache_test::free_probe(page);
    std::free(page);
}
inline auto page_reassign_owner(void*, PhysicalPageOwner) -> bool { return true; }
inline auto get_free_mem_pages() -> uint64_t { return 1024 * 1024; }
inline auto get_total_mem_bytes() -> uint64_t { return get_free_mem_pages() * 4096ULL; }
}  // namespace ker::mod::mm::phys
