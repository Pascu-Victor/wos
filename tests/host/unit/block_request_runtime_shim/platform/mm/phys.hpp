#pragma once

#include <atomic>
#include <platform/sched/scheduler.hpp>

#include "../../../../kern_shim/platform/mm/phys.hpp"

namespace ker::mod::mm::phys {
// Only the physical-watermark/safe-context boundary is simulated. The reclaim
// coordinator, capability filtering and Task-bound NoIoScope are production.
inline std::atomic<uint64_t> block_test_reclaim_free_pages{1024 * 1024};
inline auto can_wait_for_reclaim() -> bool {
    auto* current = ker::mod::sched::get_current_task();
    return current != nullptr && current->preempt_disable_depth == 0;
}
struct ReclaimZoneSnapshot {
    uint64_t zone;
    uint64_t total_pages;
    uint64_t free_pages;
    int largest_free_order;
};
inline auto snapshot_reclaim_zones(ReclaimZoneSnapshot* output, size_t capacity) -> size_t {
    if (output == nullptr || capacity == 0) return 0;
    output[0] = {0, 1024 * 1024, block_test_reclaim_free_pages.load(std::memory_order_relaxed), 18};
    return 1;
}
}  // namespace ker::mod::mm::phys
