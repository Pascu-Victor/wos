#include <dev/block_request_model.hpp>
#include <test/ktest.hpp>

using namespace ker::dev;

// Fixed stack traces use no devices, DMA, scheduler, allocation, or blocking.
KTEST(BlockRequestModel, BarrierWaitsForTransportAndCallbackRetirement) {
    BlockQueueModel queue{};
    BlockRequestToken first{}, second{}, flush{}, later{}, issued{};
    KREQUIRE_EQ(block_model_submit(queue, {BlockRequestOperation::WRITE, 0, 1, 512}, first), 0);
    KREQUIRE_EQ(block_model_submit(queue, {BlockRequestOperation::READ, 10, 1, 512}, second), 0);
    KREQUIRE_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH}, flush), 0);
    KREQUIRE_EQ(block_model_submit(queue, {BlockRequestOperation::READ, 20, 1, 512}, later), 0);
    KREQUIRE_TRUE(block_model_next_issue(queue, issued));
    KEXPECT_EQ(issued.slot, first.slot);
    KREQUIRE_TRUE(block_model_next_issue(queue, issued));
    KEXPECT_EQ(issued.slot, second.slot);
    KEXPECT_FALSE(block_model_next_issue(queue, issued));
    KREQUIRE_TRUE(block_model_claim_completion(queue, second, 0, 512));
    KREQUIRE_TRUE(block_model_finish_completion(queue, second));
    KREQUIRE_TRUE(block_model_claim_completion(queue, first, 0, 512));
    KEXPECT_FALSE(block_model_next_issue(queue, issued));
    KREQUIRE_TRUE(block_model_finish_completion(queue, first));
    KREQUIRE_TRUE(block_model_next_issue(queue, issued));
    KEXPECT_EQ(issued.slot, flush.slot);
    KREQUIRE_TRUE(block_model_claim_completion(queue, flush, -EOPNOTSUPP, 0));
    KEXPECT_FALSE(block_model_next_issue(queue, issued));
    KREQUIRE_TRUE(block_model_finish_completion(queue, flush));
    KREQUIRE_TRUE(block_model_next_issue(queue, issued));
    KEXPECT_EQ(issued.slot, later.slot);
    KEXPECT_EQ(queue.requests[flush.slot].status, -EOPNOTSUPP);
}

KTEST(BlockRequestModel, TimeoutCannotReleaseLiveTransportBuffers) {
    BlockQueueModel queue{};
    BlockRequestToken token{}, issued{};
    KREQUIRE_EQ(block_model_submit(queue, {BlockRequestOperation::WRITE, 0, 1, 512}, token), 0);
    KREQUIRE_TRUE(block_model_next_issue(queue, issued));
    KEXPECT_EQ(block_model_cancel(queue, token, -ETIMEDOUT), BlockCancelResult::NEEDS_RETIREMENT);
    KEXPECT_FALSE(block_model_claim_completion(queue, token, 0, 512, false));
    KEXPECT_FALSE(block_model_release(queue, token));
    KEXPECT_EQ(queue.owned_bytes, size_t{512});
    KREQUIRE_TRUE(block_model_claim_completion(queue, token, 0, 512));
    KEXPECT_EQ(queue.requests[token.slot].status, -ETIMEDOUT);
    KEXPECT_EQ(queue.requests[token.slot].completed_bytes, size_t{0});
    KEXPECT_FALSE(block_model_claim_completion(queue, token, 0, 512));
    KREQUIRE_TRUE(block_model_finish_completion(queue, token));
    KREQUIRE_TRUE(block_model_release(queue, token));
    KEXPECT_FALSE(block_model_release(queue, token));
    KEXPECT_EQ(queue.owned_bytes, size_t{0});
}

KTEST(BlockRequestModel, ResetRetiresBeforeEpochChangeAndKeepsOldOwnersReleasable) {
    BlockQueueModel queue{};
    BlockRequestToken active{}, queued{}, issued{}, current{};
    KREQUIRE_EQ(block_model_submit(queue, {BlockRequestOperation::WRITE, 0, 1, 512}, active), 0);
    KREQUIRE_EQ(block_model_submit(queue, {BlockRequestOperation::READ, 0, 1, 512}, queued), 0);
    KREQUIRE_TRUE(block_model_next_issue(queue, issued));
    KREQUIRE_EQ(block_model_begin_reset(queue), 0);
    KEXPECT_EQ(block_model_finish_reset(queue), -EBUSY);
    KREQUIRE_TRUE(block_model_finish_completion(queue, queued));
    KREQUIRE_TRUE(block_model_claim_completion(queue, active, 0, 512));
    KEXPECT_EQ(queue.requests[active.slot].status, -EIO);
    KEXPECT_EQ(block_model_finish_reset(queue), -EBUSY);
    KREQUIRE_TRUE(block_model_finish_completion(queue, active));
    KREQUIRE_EQ(block_model_finish_reset(queue), 0);
    KEXPECT_EQ(queue.generation, uint64_t{2});
    KREQUIRE_TRUE(block_model_release(queue, active));
    KREQUIRE_TRUE(block_model_release(queue, queued));
    KREQUIRE_EQ(block_model_submit(queue, {BlockRequestOperation::READ, 0, 1, 512}, current), 0);
    KREQUIRE_TRUE(block_model_next_issue(queue, issued));
    KEXPECT_FALSE(block_model_claim_completion(queue, active, 0, 512));
    KREQUIRE_TRUE(block_model_claim_completion(queue, current, 0, 512));
    KREQUIRE_TRUE(block_model_finish_completion(queue, current));
    KREQUIRE_TRUE(block_model_release(queue, current));
    KEXPECT_EQ(queue.stats.submitted, queue.stats.released);
}

KTEST(BlockRequestModel, QueueBudgetsAndRejectedProviderCancelAreBounded) {
    BlockQueueModel queue{};
    queue.limits.max_requests = 1;
    queue.limits.max_bytes = 512;
    BlockRequestToken token{}, other{}, issued{};
    KREQUIRE_EQ(block_model_submit(queue, {BlockRequestOperation::WRITE, 0, 1, 512}, token), 0);
    KEXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH}, other), -EAGAIN);
    KREQUIRE_TRUE(block_model_next_issue(queue, issued));
    KEXPECT_EQ(block_model_cancel(queue, token), BlockCancelResult::NEEDS_RETIREMENT);
    KREQUIRE_TRUE(block_model_defer_issue(queue, token));
    KEXPECT_EQ(queue.requests[token.slot].status, -ECANCELED);
    KREQUIRE_TRUE(block_model_finish_completion(queue, token));
    KEXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH}, other), -EAGAIN);
    KREQUIRE_TRUE(block_model_release(queue, token));
    KREQUIRE_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH}, other), 0);
    KEXPECT_FALSE(block_model_defer_issue(queue, token));
    KEXPECT_EQ(queue.owned_bytes, size_t{0});
}

KTEST(BlockRequestModel, GeometryScatterAndEpochCannotWrap) {
    size_t bytes = 0;
    KEXPECT_EQ(block_model_validate_io(UINT64_MAX, 1, 512, UINT64_MAX, BLOCK_MODEL_MAX_REQUEST_BYTES, bytes), -EINVAL);
    KEXPECT_EQ(block_model_validate_io(0, SIZE_MAX, 2, UINT64_MAX, BLOCK_MODEL_MAX_REQUEST_BYTES, bytes), -EOVERFLOW);
    uint64_t root_lba = 0;
    KEXPECT_EQ(block_model_translate_partition(1, 2, 10, 3, 20, root_lba), 0);
    KEXPECT_EQ(root_lba, uint64_t{11});
    KEXPECT_EQ(block_model_translate_partition(2, 2, 10, 3, 20, root_lba), -EINVAL);
    BlockScatterSegment segment{UINT64_MAX - 255, 512};
    KEXPECT_EQ(block_model_validate_sg(&segment, 1, 512), -EOVERFLOW);
    BlockQueueModel queue{};
    queue.generation = UINT64_MAX;
    KREQUIRE_EQ(block_model_begin_reset(queue), 0);
    KEXPECT_EQ(block_model_finish_reset(queue), -EOVERFLOW);
    KEXPECT_FALSE(queue.admission_open);
}
