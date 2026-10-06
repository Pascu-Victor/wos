#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/block_request_model.hpp>

using namespace ker::dev;

namespace {
void require(bool condition) {
    if (!condition) {
        __builtin_trap();
    }
}

auto pending(const BlockRequestModel& request) -> bool {
    return request.state == BlockRequestState::QUEUED || request.state == BlockRequestState::ISSUED ||
           request.state == BlockRequestState::COMPLETING;
}

// Independent inclusive-range oracle; only writes and barriers conflict.
auto blocks(const BlockRequestModel& earlier, const BlockRequestModel& later) -> bool {
    if (!pending(earlier) || earlier.sequence >= later.sequence) {
        return false;
    }
    if (earlier.op == BlockRequestOperation::FLUSH || later.op == BlockRequestOperation::FLUSH) {
        return true;
    }
    bool const OVERLAP = earlier.lba <= later.lba + later.count - 1 && later.lba <= earlier.lba + earlier.count - 1;
    return OVERLAP && (earlier.op == BlockRequestOperation::WRITE || later.op == BlockRequestOperation::WRITE);
}

void verify(const BlockQueueModel& queue, uint64_t claims, uint64_t finishes, uint64_t releases) {
    size_t requests = 0;
    size_t bytes = 0;
    size_t unfinished = 0;
    for (size_t i = 0; i < queue.requests.size(); ++i) {
        auto const& request = queue.requests[i];
        if (request.state == BlockRequestState::FREE) {
            continue;
        }
        ++requests;
        bytes += request.bytes;
        if (request.state == BlockRequestState::QUEUED || request.state == BlockRequestState::ISSUED ||
            request.state == BlockRequestState::COMPLETING) {
            ++unfinished;
        }
        require(request.token.slot == i && request.token.queue_generation != 0 && request.token.request_generation != 0);
        require(request.sequence != 0 && request.sequence < queue.next_sequence);
        require(request.bytes <= queue.limits.max_request_bytes);
        if (request.state == BlockRequestState::COMPLETING || request.state == BlockRequestState::TERMINAL) {
            require(request.status <= 0 && request.completed_bytes <= request.bytes);
            require(request.cancel_reason == 0 || (request.status == request.cancel_reason && request.completed_bytes == 0));
            require(request.status != 0 || request.completed_bytes == request.bytes);
        }
        for (auto const& earlier : queue.requests) {
            if (&earlier != &request && earlier.state != BlockRequestState::FREE) {
                require(earlier.sequence != request.sequence);
            }
            if (request.state == BlockRequestState::ISSUED) {
                require(!blocks(earlier, request));
            }
        }
    }
    require(queue.owned_requests == requests && queue.owned_bytes == bytes);
    require(requests <= queue.limits.max_requests && bytes <= queue.limits.max_bytes);
    require(queue.stats.completion_claims == claims && queue.stats.completed == finishes && queue.stats.released == releases);
    require(releases <= finishes && finishes <= claims && claims <= queue.stats.submitted);
    // Before saturation, only admission adds unfinished work and callback
    // retirement removes it. Retained TERMINAL owners still consume budgets.
    if (queue.stats.submitted != UINT64_MAX) {
        require(queue.stats.completed <= queue.stats.submitted);
        require(queue.stats.submitted - queue.stats.completed == unfinished);
    }
    require(!queue.resetting || !queue.admission_open);
}
}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 3) {
        return 0;
    }
    BlockQueueModel queue{};
    queue.limits.max_requests = 1 + data[0] % BLOCK_MODEL_MAX_REQUESTS;
    queue.limits.max_bytes = (1 + data[1] % 32U) * 512;
    std::array<BlockRequestToken, BLOCK_MODEL_MAX_REQUESTS> old_tokens{};
    uint64_t claims = 0;
    uint64_t finishes = 0;
    uint64_t releases = 0;
    size_t const END = std::min(size, size_t{3 + 512 * 4});
    for (size_t cursor = 3; cursor + 3 < END; cursor += 4) {
        size_t const SLOT = data[cursor + 1] % BLOCK_MODEL_MAX_REQUESTS;
        auto token = queue.requests[SLOT].token;
        uint8_t const ARG = data[cursor + 2];
        uint8_t const OTHER = data[cursor + 3];
        if (OTHER % 8 == 1) {
            token = old_tokens[SLOT];
        } else if (OTHER % 8 == 2) {
            ++token.queue_generation;
        }
        switch (data[cursor] % 10U) {
            case 0: {
                BlockRequestOperation const OP = static_cast<BlockRequestOperation>(OTHER % 3U);
                size_t const COUNT = OP == BlockRequestOperation::FLUSH ? 0 : 1 + ARG % 4U;
                BlockRequestDescription const DESCRIPTION{OP, OP == BlockRequestOperation::FLUSH ? 0U : ARG % 16U, COUNT, COUNT * 512};
                BlockRequestToken added{};
                if (block_model_submit(queue, DESCRIPTION, added) == 0) {
                    old_tokens[added.slot] = token;
                }
                break;
            }
            case 1: {
                const BlockRequestModel* expected = nullptr;
                if (!queue.resetting) {
                    for (auto const& candidate : queue.requests) {
                        if (candidate.state != BlockRequestState::QUEUED) {
                            continue;
                        }
                        bool eligible = true;
                        for (auto const& earlier : queue.requests) {
                            eligible = eligible && !blocks(earlier, candidate);
                        }
                        if (eligible && (expected == nullptr || candidate.sequence < expected->sequence)) {
                            expected = &candidate;
                        }
                    }
                }
                BlockRequestToken issued{};
                bool const RESULT = block_model_next_issue(queue, issued);
                require(RESULT == (expected != nullptr));
                require(!RESULT || issued == expected->token);
                break;
            }
            case 2:
                if (block_model_cancel(queue, token, ARG % 2 ? -ETIMEDOUT : -ECANCELED) == BlockCancelResult::CLAIMED) {
                    ++claims;
                }
                break;
            case 3: {
                size_t const BYTES = ARG % 3 == 0 ? queue.requests[SLOT].bytes : ARG;
                if (block_model_claim_completion(queue, token, OTHER % 2 ? 0 : -EIO, BYTES, ARG % 4 != 3)) {
                    ++claims;
                }
                break;
            }
            case 4:
                if (block_model_finish_completion(queue, token)) {
                    ++finishes;
                }
                break;
            case 5:
                if (block_model_release(queue, token)) {
                    ++releases;
                }
                break;
            case 6: {
                size_t queued = 0;
                for (auto const& request : queue.requests) {
                    queued += request.state == BlockRequestState::QUEUED ? 1 : 0;
                }
                if (block_model_begin_reset(queue) == 0) {
                    claims += queued;
                }
                break;
            }
            case 7:
                static_cast<void>(block_model_finish_reset(queue, true));
                break;
            case 8: {
                bool const CANCELLED = queue.requests[SLOT].cancel_reason != 0;
                if (block_model_defer_issue(queue, token) && CANCELLED) {
                    ++claims;
                }
                break;
            }
            case 9: {
                size_t bytes = 0;
                int const RESULT = block_model_validate_io(ARG, OTHER, 512, 256, BLOCK_MODEL_MAX_REQUEST_BYTES, bytes);
                require((RESULT == 0) == (OTHER != 0 && OTHER <= 256U - ARG));
                require(RESULT != 0 || bytes == size_t{OTHER} * 512);
                break;
            }
        }
        verify(queue, claims, finishes, releases);
    }
    return 0;
}
