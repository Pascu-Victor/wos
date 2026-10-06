#pragma once

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace ker::dev {

inline constexpr size_t BLOCK_MODEL_MAX_REQUESTS = 64;
inline constexpr size_t BLOCK_MODEL_MAX_SG = 32;
inline constexpr size_t BLOCK_MODEL_MAX_REQUEST_BYTES = size_t{16} * 1024 * 1024;

enum class BlockRequestOperation : uint8_t { READ, WRITE, FLUSH };
enum class BlockRequestState : uint8_t { FREE, QUEUED, ISSUED, COMPLETING, TERMINAL };
enum class BlockCancelResult : uint8_t { REJECTED, CLAIMED, NEEDS_RETIREMENT };

struct BlockRequestToken {
    uint64_t queue_generation{};
    uint64_t request_generation{};
    uint16_t slot{};

    auto operator==(const BlockRequestToken& other) const -> bool {
        return queue_generation == other.queue_generation && request_generation == other.request_generation && slot == other.slot;
    }
};

struct BlockRequestDescription {
    BlockRequestOperation op{BlockRequestOperation::READ};
    uint64_t lba{};  // Relative to the physical root, after checked partition translation.
    size_t count{};
    size_t bytes{};
};

struct BlockRequestModel {
    BlockRequestState state{BlockRequestState::FREE};
    BlockRequestOperation op{BlockRequestOperation::READ};
    uint64_t lba{};
    size_t count{};
    size_t bytes{};
    uint64_t sequence{};
    BlockRequestToken token{};
    int cancel_reason{};
    int status{};
    size_t completed_bytes{};
};

struct BlockQueueModelLimits {
    size_t max_requests{BLOCK_MODEL_MAX_REQUESTS};
    size_t max_bytes{size_t{64} * 1024 * 1024};
    size_t max_request_bytes{BLOCK_MODEL_MAX_REQUEST_BYTES};
};

struct BlockQueueModelStats {
    uint64_t submitted{};
    uint64_t admission_rejections{};
    uint64_t issued{};
    uint64_t completion_claims{};
    uint64_t completed{};
    uint64_t released{};
    uint64_t cancellation_requests{};
    uint64_t timeouts{};
    uint64_t resets{};
    uint64_t stale_completions{};
    uint64_t invalid_completions{};
    uint64_t generation_exhaustions{};
    size_t request_high_water{};
    size_t byte_high_water{};
};

// No allocation, locks, callbacks, DMA, or scheduler operations. All callers
// serialize mutations. A physical-root device and all its partitions share
// one queue. TERMINAL means transport access AND callback execution retired;
// the owner's slot/byte budget remains charged until explicit release.
struct BlockQueueModel {
    std::array<BlockRequestModel, BLOCK_MODEL_MAX_REQUESTS> requests{};
    BlockQueueModelLimits limits{};
    BlockQueueModelStats stats{};
    uint64_t generation{1};
    uint64_t next_sequence{1};
    size_t owned_requests{};
    size_t owned_bytes{};
    bool admission_open{true};
    bool resetting{};
};

struct BlockScatterSegment {
    uint64_t address{};  // DMA address; mapping/pinning is a separate driver obligation.
    size_t bytes{};
};

struct BlockScatterLimits {
    size_t max_segments{BLOCK_MODEL_MAX_SG};
    size_t max_segment_bytes{BLOCK_MODEL_MAX_REQUEST_BYTES};
    size_t alignment{1};
    uint64_t dma_mask{UINT64_MAX};  // Contiguous low address bits, inclusive.
};

namespace block_model_detail {
inline void increment(uint64_t& counter) {
    if (counter != UINT64_MAX) {
        ++counter;
    }
}

inline auto unfinished(const BlockRequestModel& request) -> bool {
    return request.state == BlockRequestState::QUEUED || request.state == BlockRequestState::ISSUED ||
           request.state == BlockRequestState::COMPLETING;
}

inline auto normalize_status(int status) -> int {
    if (status == -1) {
        return -EIO;
    }
    return status > 0 ? -status : status;
}

inline auto lookup(BlockQueueModel& queue, BlockRequestToken token) -> BlockRequestModel* {
    if (token.slot >= queue.requests.size()) {
        return nullptr;
    }
    auto& request = queue.requests[token.slot];
    return request.state != BlockRequestState::FREE && request.token == token ? &request : nullptr;
}

inline auto overlaps(const BlockRequestModel& first, const BlockRequestModel& second) -> bool {
    // Admission rejects wrapping ranges. Equal/adjacent endpoints do not overlap.
    return first.lba < second.lba + second.count && second.lba < first.lba + first.count;
}

inline auto blocks(const BlockRequestModel& earlier, const BlockRequestModel& later) -> bool {
    if (!unfinished(earlier) || earlier.sequence >= later.sequence) {
        return false;
    }
    if (earlier.op == BlockRequestOperation::FLUSH || later.op == BlockRequestOperation::FLUSH) {
        return true;
    }
    return (earlier.op == BlockRequestOperation::WRITE || later.op == BlockRequestOperation::WRITE) && overlaps(earlier, later);
}
}  // namespace block_model_detail

// Subtraction-based device bounds and checked byte multiplication. The caller
// separately translates partitions with block_model_translate_partition().
inline auto block_model_validate_io(uint64_t lba, size_t count, size_t block_size, uint64_t total_blocks, size_t max_bytes,
                                    size_t& bytes_out) -> int {
    bytes_out = 0;
    if (block_size == 0 || count == 0 || max_bytes == 0 || max_bytes > BLOCK_MODEL_MAX_REQUEST_BYTES) {
        return -EINVAL;
    }
    if (lba > total_blocks || count > total_blocks - lba) {
        return -EINVAL;
    }
    if (count > std::numeric_limits<size_t>::max() / block_size) {
        return -EOVERFLOW;
    }
    size_t const BYTES = count * block_size;
    if (BYTES > max_bytes) {
        return -E2BIG;
    }
    bytes_out = BYTES;
    return 0;
}

inline auto block_model_translate_partition(uint64_t lba, size_t count, uint64_t partition_start, uint64_t partition_blocks,
                                            uint64_t parent_blocks, uint64_t& root_lba_out) -> int {
    root_lba_out = 0;
    if (count == 0 || partition_start > parent_blocks || partition_blocks > parent_blocks - partition_start || lba > partition_blocks ||
        count > partition_blocks - lba) {
        return -EINVAL;
    }
    root_lba_out = partition_start + lba;
    return 0;
}

inline auto block_model_validate_sg(const BlockScatterSegment* segments, size_t segment_count, size_t expected_bytes,
                                    const BlockScatterLimits& limits = {}) -> int {
    if (segments == nullptr || segment_count == 0 || limits.max_segments == 0 || limits.max_segments > BLOCK_MODEL_MAX_SG ||
        limits.max_segment_bytes == 0 || limits.alignment == 0 || expected_bytes == 0 || expected_bytes > BLOCK_MODEL_MAX_REQUEST_BYTES ||
        (limits.dma_mask != UINT64_MAX && (limits.dma_mask & (limits.dma_mask + 1)) != 0)) {
        return -EINVAL;
    }
    if (segment_count > limits.max_segments) {
        return -E2BIG;
    }
    size_t sum = 0;
    for (size_t i = 0; i < segment_count; ++i) {
        auto const& segment = segments[i];
        if (segment.bytes == 0 || segment.bytes > limits.max_segment_bytes || segment.address % limits.alignment != 0 ||
            segment.bytes % limits.alignment != 0) {
            return -EINVAL;
        }
        if (segment.bytes - 1 > UINT64_MAX - segment.address || segment.bytes > expected_bytes - sum) {
            return -EOVERFLOW;
        }
        uint64_t const LAST = segment.address + segment.bytes - 1;
        if (LAST > limits.dma_mask) {
            return -ERANGE;
        }
        sum += segment.bytes;
    }
    return sum == expected_bytes ? 0 : -EINVAL;
}

inline auto block_model_submit(BlockQueueModel& queue, const BlockRequestDescription& description, BlockRequestToken& token_out) -> int {
    auto reject = [&](int status) {
        block_model_detail::increment(queue.stats.admission_rejections);
        return status;
    };
    if (!queue.admission_open || queue.resetting) {
        return reject(-ENODEV);
    }
    if (queue.limits.max_requests == 0 || queue.limits.max_requests > BLOCK_MODEL_MAX_REQUESTS || queue.limits.max_bytes == 0 ||
        queue.limits.max_request_bytes == 0 || queue.limits.max_request_bytes > BLOCK_MODEL_MAX_REQUEST_BYTES) {
        return reject(-EINVAL);
    }
    if (description.op == BlockRequestOperation::FLUSH) {
        if (description.lba != 0 || description.count != 0 || description.bytes != 0) {
            return reject(-EINVAL);
        }
    } else if ((description.op != BlockRequestOperation::READ && description.op != BlockRequestOperation::WRITE) ||
               description.count == 0 || description.bytes == 0 || description.count > UINT64_MAX - description.lba) {
        return reject(-EINVAL);
    }
    if (description.bytes > queue.limits.max_request_bytes) {
        return reject(-E2BIG);
    }
    if (queue.owned_requests >= queue.limits.max_requests || queue.owned_bytes > queue.limits.max_bytes ||
        description.bytes > queue.limits.max_bytes - queue.owned_bytes) {
        return reject(-EAGAIN);
    }
    if (queue.generation == 0 || queue.next_sequence == 0 || queue.next_sequence == UINT64_MAX) {
        block_model_detail::increment(queue.stats.generation_exhaustions);
        return reject(-EOVERFLOW);
    }
    size_t slot = queue.requests.size();
    for (size_t i = 0; i < queue.requests.size(); ++i) {
        if (queue.requests[i].state == BlockRequestState::FREE && queue.requests[i].token.request_generation != UINT64_MAX) {
            slot = i;
            break;
        }
    }
    if (slot == queue.requests.size()) {
        block_model_detail::increment(queue.stats.generation_exhaustions);
        return reject(-EOVERFLOW);
    }
    auto& request = queue.requests[slot];
    uint64_t const REQUEST_GENERATION = request.token.request_generation + 1;
    request = {};
    request.state = BlockRequestState::QUEUED;
    request.op = description.op;
    request.lba = description.lba;
    request.count = description.count;
    request.bytes = description.bytes;
    request.sequence = queue.next_sequence++;
    request.token = {queue.generation, REQUEST_GENERATION, static_cast<uint16_t>(slot)};
    token_out = request.token;
    ++queue.owned_requests;
    queue.owned_bytes += request.bytes;
    block_model_detail::increment(queue.stats.submitted);
    if (queue.owned_requests > queue.stats.request_high_water) {
        queue.stats.request_high_water = queue.owned_requests;
    }
    if (queue.owned_bytes > queue.stats.byte_high_water) {
        queue.stats.byte_high_water = queue.owned_bytes;
    }
    return 0;
}

// Select the oldest eligible request and mark it ISSUED. A blocked overlapping
// transfer may be bypassed by a non-overlapping transfer, but never a flush.
inline auto block_model_next_issue(BlockQueueModel& queue, BlockRequestToken& token_out, size_t max_issued = BLOCK_MODEL_MAX_REQUESTS)
    -> bool {
    if (queue.resetting) {
        return false;
    }
    BlockRequestModel* selected = nullptr;
    size_t issued = 0;
#ifdef WOS_ASYNC_BLOCK_IO
    bool read_only = true;
#endif
    for (auto& request : queue.requests) {
        if (request.state == BlockRequestState::ISSUED) ++issued;
#ifdef WOS_ASYNC_BLOCK_IO
        if (!read_only) continue;
        if (block_model_detail::unfinished(request) && request.op != BlockRequestOperation::READ) {
            read_only = false;
            selected = nullptr;
            continue;
        }
        if (request.state == BlockRequestState::QUEUED && (selected == nullptr || request.sequence < selected->sequence)) {
            selected = &request;
        }
#endif
    }
    if (issued >= max_issued) return false;
#ifdef WOS_ASYNC_BLOCK_IO
    // READs never conflict with one another. The same guarded scan proves
    // that every unfinished owner is a READ and enforces hardware depth.
    // COMPLETING writes/flushes invalidate that proof until callbacks retire.
    if (!read_only) {
#endif
        for (auto& request : queue.requests) {
            if (request.state != BlockRequestState::QUEUED || (selected != nullptr && request.sequence >= selected->sequence)) {
                continue;
            }
            bool blocked = false;
            for (auto const& earlier : queue.requests) {
                if (block_model_detail::blocks(earlier, request)) {
                    blocked = true;
                    break;
                }
            }
            if (!blocked) {
                selected = &request;
            }
        }
#ifdef WOS_ASYNC_BLOCK_IO
    }
#endif
    if (selected == nullptr) {
        return false;
    }
    selected->state = BlockRequestState::ISSUED;
    token_out = selected->token;
    block_model_detail::increment(queue.stats.issued);
    return true;
}

// The sole successful hardware/transport completion claim. 'transport_retired'
// promises that no provider/DMA access to the request's buffers remains. A
// cancellation intention wins over success and credits no transferred bytes.
inline auto block_model_claim_completion(BlockQueueModel& queue, BlockRequestToken token, int status, size_t bytes,
                                         bool transport_retired = true) -> bool {
    auto* request = block_model_detail::lookup(queue, token);
    if (request == nullptr || request->state != BlockRequestState::ISSUED) {
        block_model_detail::increment(queue.stats.stale_completions);
        return false;
    }
    if (!transport_retired || bytes > request->bytes) {
        block_model_detail::increment(queue.stats.invalid_completions);
        return false;
    }
    status = block_model_detail::normalize_status(status);
    if (request->cancel_reason != 0) {
        status = request->cancel_reason;
        bytes = 0;
    } else if (status == 0 && bytes != request->bytes) {
        status = -EIO;
    }
    request->status = status;
    request->completed_bytes = bytes;
    request->state = BlockRequestState::COMPLETING;
    block_model_detail::increment(queue.stats.completion_claims);
    return true;
}

inline auto block_model_cancel(BlockQueueModel& queue, BlockRequestToken token, int reason = -ECANCELED) -> BlockCancelResult {
    auto* request = block_model_detail::lookup(queue, token);
    if (reason >= 0 || request == nullptr || (request->state != BlockRequestState::QUEUED && request->state != BlockRequestState::ISSUED)) {
        return BlockCancelResult::REJECTED;
    }
    if (request->cancel_reason == 0) {
        request->cancel_reason = block_model_detail::normalize_status(reason);
        block_model_detail::increment(queue.stats.cancellation_requests);
        if (reason == -ETIMEDOUT) {
            block_model_detail::increment(queue.stats.timeouts);
        }
    }
    if (request->state == BlockRequestState::ISSUED) {
        return BlockCancelResult::NEEDS_RETIREMENT;
    }
    request->status = request->cancel_reason;
    request->completed_bytes = 0;
    request->state = BlockRequestState::COMPLETING;
    block_model_detail::increment(queue.stats.completion_claims);
    return BlockCancelResult::CLAIMED;
}

// Provider rejected ownership with -EAGAIN and therefore never accessed the
// buffers. Restore the original ordering ticket, or claim a cancellation that
// raced the issue attempt. This is not valid for an accepted hardware command.
inline auto block_model_defer_issue(BlockQueueModel& queue, BlockRequestToken token) -> bool {
    auto* request = block_model_detail::lookup(queue, token);
    if (request == nullptr || request->state != BlockRequestState::ISSUED) {
        return false;
    }
    request->state = BlockRequestState::QUEUED;
    if (request->cancel_reason != 0) {
        static_cast<void>(block_model_cancel(queue, token, request->cancel_reason));
    }
    return true;
}

// Called only AFTER the claimed completion callback has returned. Runtime
// publishes/wakes once for the claim, retaining storage until this retirement.
inline auto block_model_finish_completion(BlockQueueModel& queue, BlockRequestToken token) -> bool {
    auto* request = block_model_detail::lookup(queue, token);
    if (request == nullptr || request->state != BlockRequestState::COMPLETING) {
        return false;
    }
    request->state = BlockRequestState::TERMINAL;
    block_model_detail::increment(queue.stats.completed);
    return true;
}

inline auto block_model_release(BlockQueueModel& queue, BlockRequestToken token) -> bool {
    auto* request = block_model_detail::lookup(queue, token);
    if (request == nullptr || request->state != BlockRequestState::TERMINAL) {
        return false;
    }
    --queue.owned_requests;
    queue.owned_bytes -= request->bytes;
    // Keep the per-slot generation even when the old queue epoch is retired.
    BlockRequestToken const PREVIOUS = request->token;
    *request = {};
    request->token = PREVIOUS;
    block_model_detail::increment(queue.stats.released);
    return true;
}

inline auto block_model_begin_reset(BlockQueueModel& queue, int reason = -EIO) -> int {
    if (reason >= 0) {
        return -EINVAL;
    }
    if (queue.resetting) {
        return -EALREADY;
    }
    queue.admission_open = false;
    queue.resetting = true;
    block_model_detail::increment(queue.stats.resets);
    for (auto& request : queue.requests) {
        if (request.state == BlockRequestState::QUEUED || request.state == BlockRequestState::ISSUED) {
            static_cast<void>(block_model_cancel(queue, request.token, reason));
        }
    }
    return 0;
}

// No epoch advance until all transport access AND callbacks have retired.
// Old TERMINAL owners remain releasable using their stored old-epoch tokens.
inline auto block_model_finish_reset(BlockQueueModel& queue, bool reopen = true) -> int {
    if (!queue.resetting) {
        return -EINVAL;
    }
    for (auto const& request : queue.requests) {
        if (block_model_detail::unfinished(request)) {
            return -EBUSY;
        }
    }
    if (queue.generation == UINT64_MAX) {
        block_model_detail::increment(queue.stats.generation_exhaustions);
        return -EOVERFLOW;
    }
    ++queue.generation;
    queue.resetting = false;
    queue.admission_open = reopen;
    return 0;
}

}  // namespace ker::dev
