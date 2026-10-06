#pragma once

// The cache target uses the production API and a functional Task-depth seam.
// Production coordinator filtering/migration behavior is covered separately by
// reclaim_runtime_test; these probes verify the cache's guard lifetimes.
#include <cassert>

#include "../../../../../../modules/kern/src/platform/mm/reclaim.hpp"

namespace ker::mod::mm::reclaim {
namespace cache_test {
struct Task {
    uint32_t reclaim_no_io_depth{};
};
inline thread_local Task current_task{};
}  // namespace cache_test

inline NoIoScope::NoIoScope() : depth_(&cache_test::current_task.reclaim_no_io_depth) { ++*depth_; }
inline NoIoScope::~NoIoScope() {
    assert(depth_ != nullptr && *depth_ != 0);
    --*depth_;
}
}  // namespace ker::mod::mm::reclaim
