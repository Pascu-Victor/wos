#pragma once

// Only the buffer-cache target substitutes the request provider boundary.
// Descriptor/operation/limit types come from the production public API.
#include "../../../../../modules/kern/src/dev/block_request.hpp"

namespace ker::dev::cache_test {
struct Hooks {
    bool (*available)(const BlockDevice*){};
    BlockQueueLimits (*limits)(const BlockDevice*){};
    int (*submit)(BlockDevice*, BlockRequest&, BlockRequestOperation, uint64_t, size_t, std::span<const BlockSegment>){};
    int (*wait)(BlockRequest&){};
    int (*release)(BlockRequest&){};
    void (*service)(BlockDevice*){};
    int (*submit_for_wait)(BlockDevice*, BlockRequest&, BlockRequestOperation, uint64_t, size_t, std::span<const BlockSegment>){};
};
inline Hooks hooks{};
}  // namespace ker::dev::cache_test

namespace ker::dev {
inline auto block_queue_available(const BlockDevice* bdev) -> bool {
    return cache_test::hooks.available != nullptr && cache_test::hooks.available(bdev);
}
inline auto block_queue_limits(const BlockDevice* bdev) -> BlockQueueLimits {
    return cache_test::hooks.limits == nullptr ? BlockQueueLimits{} : cache_test::hooks.limits(bdev);
}
inline auto block_request_submit(BlockDevice* bdev, BlockRequest& request, BlockRequestOperation op, uint64_t lba, size_t count,
                                 std::span<const BlockSegment> segments, uint64_t, BlockCompletion, void*) -> int {
    return cache_test::hooks.submit == nullptr ? -EOPNOTSUPP : cache_test::hooks.submit(bdev, request, op, lba, count, segments);
}
inline auto block_request_wait(BlockRequest& request) -> int {
    return cache_test::hooks.wait == nullptr ? -EINVAL : cache_test::hooks.wait(request);
}
inline auto block_request_submit_for_wait(BlockDevice* bdev, BlockRequest& request, BlockRequestOperation op, uint64_t lba, size_t count,
                                          std::span<const BlockSegment> segments, uint64_t) -> int {
    if (cache_test::hooks.submit_for_wait != nullptr) {
        return cache_test::hooks.submit_for_wait(bdev, request, op, lba, count, segments);
    }
    return cache_test::hooks.submit == nullptr ? -EOPNOTSUPP : cache_test::hooks.submit(bdev, request, op, lba, count, segments);
}
inline auto block_request_release(BlockRequest& request) -> int {
    return cache_test::hooks.release == nullptr ? -EINVAL : cache_test::hooks.release(request);
}
inline void block_queue_service(BlockDevice* bdev) {
    if (cache_test::hooks.service != nullptr) {
        cache_test::hooks.service(bdev);
    }
}
inline void block_queue_wait_for_capacity(BlockDevice* bdev) { block_queue_service(bdev); }
}  // namespace ker::dev
