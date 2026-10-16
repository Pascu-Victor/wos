#pragma once

#include <cstddef>
#include <cstdint>
#include <dev/block_request.hpp>

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
    // Changes every time an index is allocated.  A stale handle from an
    // earlier allocation must never address the current contents of a reused
    // slot.
    uint64_t generation = 0;
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

// Consumers that retain SwapSlot handles must register before they can own a
// slot. swapoff invokes every registered migrator after preventing new
// allocations from the target area and without holding the swap mutex.
struct SwapConsumer {
    const char* name{};
    void* context{};
    auto (*migrate_area)(void* context, uint32_t area_id) -> int{};
};

constexpr auto invalid_slot() -> SwapSlot { return {}; }
constexpr auto slot_valid(SwapSlot slot) -> bool { return slot.area != UINT32_MAX && slot.index != UINT64_MAX && slot.generation != 0; }

auto swapon_path(const char* path, int flags) -> int;
auto swapoff_path(const char* path) -> int;
auto swap_available() -> bool;
auto allocate_slot(SwapSlot* out) -> int;
auto free_slot(SwapSlot slot) -> int;
auto write_slot(SwapSlot slot, const void* page) -> int;
auto read_slot(SwapSlot slot, void* page) -> int;
// Caller retains the descriptor and page until finish_slot_io. Admission is
// bounded and can return -EAGAIN without retaining either. The slot/area pin
// remains active until task-side CRC/status publication and request release.
struct SlotIo {
    ker::dev::BlockRequest request;
    SwapSlot slot{};
    void* page{};
    ker::dev::BlockDevice* device{};
    uint32_t checksum{};
    int backend_status{};
    bool write{};
    bool active{};
    bool asynchronous{};
};
auto submit_slot_io(SlotIo&, SwapSlot, void* page, bool write) -> int;
auto finish_slot_io(SlotIo&) -> int;
void get_stats(SwapStats* out);

auto activate_block_device(ker::dev::BlockDevice* device, const char* name) -> int;
auto activate_extents(const char* name, const SwapExtent* extents, size_t extent_count) -> int;
auto register_consumer(const SwapConsumer& consumer) -> bool;

#ifdef WOS_SELFTEST
// Exact lexical private-device expectation, not a retained area or ABA handle.
// One try-only mutex admission, no consumer migration; refusal leaves it live.
struct SelftestEmptyAreaExpectation {
    const char* name;
    ker::dev::BlockDevice* device;
    uint64_t start_block;
    uint64_t page_count;
};
[[nodiscard]] auto selftest_try_detach_empty_area(const SelftestEmptyAreaExpectation& expected) -> int;
#endif

}  // namespace ker::mod::mm::swap
