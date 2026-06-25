#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::dev {
struct BlockDevice;
}

namespace ker::vfs {
struct File;
}

namespace ker::mod::mm::swap {

struct SwapSlot {
    uint32_t area = UINT32_MAX;
    uint64_t index = UINT64_MAX;
};

struct SwapStats {
    uint64_t total_bytes{};
    uint64_t free_bytes{};
    uint64_t used_bytes{};
    uint64_t cached_bytes{};
    uint64_t active_areas{};
};

struct SwapExtent {
    ker::dev::BlockDevice* device{};
    uint64_t start_block{};
    uint64_t page_count{};
};

constexpr auto invalid_slot() -> SwapSlot { return {}; }
constexpr auto slot_valid(SwapSlot slot) -> bool { return slot.area != UINT32_MAX && slot.index != UINT64_MAX; }

auto swapon_path(const char* path, int flags) -> int;
auto swapoff_path(const char* path) -> int;
auto swap_available() -> bool;
auto allocate_slot(SwapSlot* out) -> int;
auto free_slot(SwapSlot slot) -> int;
auto write_slot(SwapSlot slot, const void* page) -> int;
auto read_slot(SwapSlot slot, void* page) -> int;
void get_stats(SwapStats* out);

auto activate_block_device(ker::dev::BlockDevice* device, const char* name) -> int;
auto activate_extents(const char* name, const SwapExtent* extents, size_t extent_count) -> int;

}  // namespace ker::mod::mm::swap
