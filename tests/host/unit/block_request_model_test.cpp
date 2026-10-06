#include <gtest/gtest.h>

#include <array>
#include <dev/block_request_model.hpp>
#include <limits>

using namespace ker::dev;

namespace {
auto transfer(BlockRequestOperation op, uint64_t lba = 0, size_t count = 1) -> BlockRequestDescription {
    return {op, lba, count, count * 512};
}

auto submit(BlockQueueModel& queue, BlockRequestDescription description) -> BlockRequestToken {
    BlockRequestToken token{};
    EXPECT_EQ(block_model_submit(queue, description, token), 0);
    return token;
}

void complete(BlockQueueModel& queue, BlockRequestToken token, int status = 0) {
    ASSERT_TRUE(block_model_claim_completion(queue, token, status, queue.requests[token.slot].bytes));
    ASSERT_TRUE(block_model_finish_completion(queue, token));
}
}  // namespace

TEST(BlockRequestModel, IndependentTransfersIssueConcurrentlyInSubmissionOrder) {
    BlockQueueModel queue{};
    auto const FIRST = submit(queue, transfer(BlockRequestOperation::WRITE, 0));
    auto const SECOND = submit(queue, transfer(BlockRequestOperation::READ, 10));
    auto const THIRD = submit(queue, transfer(BlockRequestOperation::WRITE, 20));
    BlockRequestToken issued{};
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, FIRST);
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, SECOND);
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, THIRD);
    EXPECT_FALSE(block_model_next_issue(queue, issued));
    complete(queue, THIRD);
    complete(queue, FIRST);
    complete(queue, SECOND);
    EXPECT_EQ(queue.stats.issued, 3U);
    EXPECT_EQ(queue.stats.completed, 3U);
}

TEST(BlockRequestModel, OverlappingWritesBlockThroughCallbackRetirement) {
    BlockQueueModel queue{};
    auto const FIRST = submit(queue, transfer(BlockRequestOperation::WRITE, 10, 2));
    auto const SECOND = submit(queue, transfer(BlockRequestOperation::READ, 11));
    auto const THIRD = submit(queue, transfer(BlockRequestOperation::WRITE, 30));
    BlockRequestToken issued{};
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, FIRST);
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, THIRD);
    EXPECT_FALSE(block_model_next_issue(queue, issued));
    ASSERT_TRUE(block_model_claim_completion(queue, FIRST, 0, 1024));
    EXPECT_FALSE(block_model_next_issue(queue, issued));
    ASSERT_TRUE(block_model_finish_completion(queue, FIRST));
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, SECOND);
    // The terminal owner still holds the slot/bytes, but no longer blocks I/O.
    EXPECT_EQ(queue.owned_requests, 3U);
    EXPECT_EQ(queue.owned_bytes, 2048U);
}

TEST(BlockRequestModel, OverlappingReadsAndAdjacentWritesAreIndependent) {
    BlockQueueModel queue{};
    submit(queue, transfer(BlockRequestOperation::READ, 1, 2));
    submit(queue, transfer(BlockRequestOperation::READ, 2));
    submit(queue, transfer(BlockRequestOperation::WRITE, 3));
    BlockRequestToken token{};
    EXPECT_TRUE(block_model_next_issue(queue, token));
    EXPECT_TRUE(block_model_next_issue(queue, token));
    EXPECT_TRUE(block_model_next_issue(queue, token));
}

TEST(BlockRequestModel, EarlierQueuedOverlappingTransferCannotBeBypassed) {
    BlockQueueModel queue{};
    auto const FIRST = submit(queue, transfer(BlockRequestOperation::READ, 0));
    auto const SECOND = submit(queue, transfer(BlockRequestOperation::WRITE, 0, 2));
    auto const THIRD = submit(queue, transfer(BlockRequestOperation::READ, 1));
    BlockRequestToken issued{};
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, FIRST);
    EXPECT_FALSE(block_model_next_issue(queue, issued));
    complete(queue, FIRST);
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, SECOND);
    EXPECT_FALSE(block_model_next_issue(queue, issued));
    complete(queue, SECOND);
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, THIRD);
}

TEST(BlockRequestModel, FlushJoinsAllEarlierRequestsAndBlocksAllLaterRequests) {
    BlockQueueModel queue{};
    auto const READ = submit(queue, transfer(BlockRequestOperation::READ, 10));
    auto const WRITE = submit(queue, transfer(BlockRequestOperation::WRITE, 20));
    auto const FLUSH = submit(queue, {BlockRequestOperation::FLUSH});
    auto const LATER = submit(queue, transfer(BlockRequestOperation::READ, 50));
    BlockRequestToken issued{};
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, READ);
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, WRITE);
    EXPECT_FALSE(block_model_next_issue(queue, issued));
    complete(queue, WRITE);
    EXPECT_FALSE(block_model_next_issue(queue, issued));
    complete(queue, READ);
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, FLUSH);
    EXPECT_FALSE(block_model_next_issue(queue, issued));
    ASSERT_TRUE(block_model_claim_completion(queue, FLUSH, -EOPNOTSUPP, 0));
    EXPECT_FALSE(block_model_next_issue(queue, issued));
    ASSERT_TRUE(block_model_finish_completion(queue, FLUSH));
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, LATER);
    EXPECT_EQ(queue.requests[FLUSH.slot].status, -EOPNOTSUPP);
}

TEST(BlockRequestModel, CompletionClaimAndOwnerReleaseAreExactlyOnce) {
    BlockQueueModel queue{};
    auto const TOKEN = submit(queue, transfer(BlockRequestOperation::READ));
    BlockRequestToken issued{};
    EXPECT_FALSE(block_model_claim_completion(queue, TOKEN, 0, 512));
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_FALSE(block_model_release(queue, TOKEN));
    ASSERT_TRUE(block_model_claim_completion(queue, TOKEN, 0, 512));
    EXPECT_FALSE(block_model_claim_completion(queue, TOKEN, -EIO, 0));
    EXPECT_FALSE(block_model_release(queue, TOKEN));
    ASSERT_TRUE(block_model_finish_completion(queue, TOKEN));
    EXPECT_FALSE(block_model_finish_completion(queue, TOKEN));
    ASSERT_TRUE(block_model_release(queue, TOKEN));
    EXPECT_FALSE(block_model_release(queue, TOKEN));
    EXPECT_EQ(queue.owned_requests, 0U);
    EXPECT_EQ(queue.owned_bytes, 0U);
    EXPECT_EQ(queue.stats.completion_claims, 1U);
    EXPECT_EQ(queue.stats.completed, 1U);
    EXPECT_EQ(queue.stats.released, 1U);
}

TEST(BlockRequestModel, QueuedCancellationClaimsWithoutTransportAccess) {
    BlockQueueModel queue{};
    auto const TOKEN = submit(queue, transfer(BlockRequestOperation::WRITE));
    EXPECT_EQ(block_model_cancel(queue, TOKEN), BlockCancelResult::CLAIMED);
    EXPECT_EQ(queue.requests[TOKEN.slot].state, BlockRequestState::COMPLETING);
    EXPECT_EQ(queue.requests[TOKEN.slot].status, -ECANCELED);
    EXPECT_EQ(queue.requests[TOKEN.slot].completed_bytes, 0U);
    BlockRequestToken issued{};
    EXPECT_FALSE(block_model_next_issue(queue, issued));
    EXPECT_FALSE(block_model_release(queue, TOKEN));
    ASSERT_TRUE(block_model_finish_completion(queue, TOKEN));
    ASSERT_TRUE(block_model_release(queue, TOKEN));
    EXPECT_EQ(queue.stats.issued, 0U);
}

TEST(BlockRequestModel, IssuedCancellationRetainsBuffersUntilTransportRetirement) {
    BlockQueueModel queue{};
    auto const TOKEN = submit(queue, transfer(BlockRequestOperation::WRITE));
    BlockRequestToken issued{};
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(block_model_cancel(queue, TOKEN, -ETIMEDOUT), BlockCancelResult::NEEDS_RETIREMENT);
    EXPECT_EQ(block_model_cancel(queue, TOKEN), BlockCancelResult::NEEDS_RETIREMENT);
    EXPECT_EQ(queue.requests[TOKEN.slot].state, BlockRequestState::ISSUED);
    EXPECT_FALSE(block_model_claim_completion(queue, TOKEN, 0, 512, false));
    EXPECT_FALSE(block_model_finish_completion(queue, TOKEN));
    EXPECT_FALSE(block_model_release(queue, TOKEN));
    EXPECT_EQ(queue.owned_bytes, 512U);
    ASSERT_TRUE(block_model_claim_completion(queue, TOKEN, 0, 512, true));
    EXPECT_EQ(queue.requests[TOKEN.slot].status, -ETIMEDOUT);
    EXPECT_EQ(queue.requests[TOKEN.slot].completed_bytes, 0U);
    EXPECT_EQ(queue.stats.cancellation_requests, 1U);
    EXPECT_EQ(queue.stats.timeouts, 1U);
    ASSERT_TRUE(block_model_finish_completion(queue, TOKEN));
    ASSERT_TRUE(block_model_release(queue, TOKEN));
}

TEST(BlockRequestModel, CompletionWinningCancellationRetainsItsResult) {
    BlockQueueModel queue{};
    auto const TOKEN = submit(queue, transfer(BlockRequestOperation::READ));
    BlockRequestToken issued{};
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    ASSERT_TRUE(block_model_claim_completion(queue, TOKEN, 0, 512));
    EXPECT_EQ(block_model_cancel(queue, TOKEN), BlockCancelResult::REJECTED);
    EXPECT_EQ(queue.requests[TOKEN.slot].status, 0);
    EXPECT_EQ(queue.requests[TOKEN.slot].completed_bytes, 512U);
}

TEST(BlockRequestModel, ShortSuccessIsErrorAndProviderErrorsAreNormalized) {
    BlockQueueModel queue{};
    for (int status : {0, -1, EACCES, -ENODEV}) {
        auto const TOKEN = submit(queue, transfer(BlockRequestOperation::READ));
        BlockRequestToken issued{};
        ASSERT_TRUE(block_model_next_issue(queue, issued));
        ASSERT_TRUE(block_model_claim_completion(queue, TOKEN, status, 100));
        int const EXPECTED = status == 0 || status == -1 ? -EIO : (status > 0 ? -status : status);
        EXPECT_EQ(queue.requests[TOKEN.slot].status, EXPECTED);
        EXPECT_EQ(queue.requests[TOKEN.slot].completed_bytes, 100U);
        ASSERT_TRUE(block_model_finish_completion(queue, TOKEN));
        ASSERT_TRUE(block_model_release(queue, TOKEN));
    }
}

TEST(BlockRequestModel, OverReportedBytesCannotClaimCompletion) {
    BlockQueueModel queue{};
    auto const TOKEN = submit(queue, transfer(BlockRequestOperation::READ));
    BlockRequestToken issued{};
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_FALSE(block_model_claim_completion(queue, TOKEN, 0, 513));
    EXPECT_EQ(queue.requests[TOKEN.slot].state, BlockRequestState::ISSUED);
    complete(queue, TOKEN);
}

TEST(BlockRequestModel, ResetWaitsForTransportAndCallbacksButNotTerminalOwners) {
    BlockQueueModel queue{};
    auto const ACTIVE = submit(queue, transfer(BlockRequestOperation::WRITE));
    auto const QUEUED = submit(queue, transfer(BlockRequestOperation::READ));
    BlockRequestToken issued{};
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    ASSERT_EQ(block_model_begin_reset(queue), 0);
    EXPECT_EQ(block_model_begin_reset(queue), -EALREADY);
    EXPECT_FALSE(block_model_next_issue(queue, issued));
    EXPECT_EQ(block_model_submit(queue, transfer(BlockRequestOperation::READ), issued), -ENODEV);
    EXPECT_EQ(block_model_finish_reset(queue), -EBUSY);
    ASSERT_TRUE(block_model_finish_completion(queue, QUEUED));
    EXPECT_EQ(block_model_finish_reset(queue), -EBUSY);
    ASSERT_TRUE(block_model_claim_completion(queue, ACTIVE, 0, 512));
    EXPECT_EQ(queue.requests[ACTIVE.slot].status, -EIO);
    EXPECT_EQ(queue.requests[ACTIVE.slot].completed_bytes, 0U);
    EXPECT_EQ(block_model_finish_reset(queue), -EBUSY);
    ASSERT_TRUE(block_model_finish_completion(queue, ACTIVE));
    ASSERT_EQ(block_model_finish_reset(queue), 0);
    EXPECT_EQ(queue.generation, 2U);
    EXPECT_TRUE(queue.admission_open);
    EXPECT_FALSE(block_model_claim_completion(queue, ACTIVE, 0, 512));
    EXPECT_TRUE(block_model_release(queue, ACTIVE));
    EXPECT_TRUE(block_model_release(queue, QUEUED));
    EXPECT_EQ(queue.owned_requests, 0U);
}

TEST(BlockRequestModel, SlotReuseRejectsOldTokenAndWrongQueueGeneration) {
    BlockQueueModel queue{};
    auto const OLD = submit(queue, transfer(BlockRequestOperation::READ));
    BlockRequestToken issued{};
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    complete(queue, OLD);
    ASSERT_TRUE(block_model_release(queue, OLD));
    auto const CURRENT = submit(queue, transfer(BlockRequestOperation::READ));
    EXPECT_EQ(CURRENT.slot, OLD.slot);
    EXPECT_GT(CURRENT.request_generation, OLD.request_generation);
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_FALSE(block_model_claim_completion(queue, OLD, 0, 512));
    auto wrong = CURRENT;
    ++wrong.queue_generation;
    EXPECT_FALSE(block_model_claim_completion(queue, wrong, 0, 512));
    EXPECT_FALSE(block_model_release(queue, OLD));
    complete(queue, CURRENT);
}

TEST(BlockRequestModel, RequestAndByteBudgetsIncludeTerminalOwners) {
    BlockQueueModel queue{};
    queue.limits.max_requests = 2;
    queue.limits.max_bytes = 512;
    auto const TRANSFER = submit(queue, transfer(BlockRequestOperation::READ));
    auto const FLUSH = submit(queue, {BlockRequestOperation::FLUSH});
    BlockRequestToken token{};
    EXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH}, token), -EAGAIN);
    ASSERT_TRUE(block_model_next_issue(queue, token));
    complete(queue, TRANSFER);
    EXPECT_EQ(block_model_submit(queue, transfer(BlockRequestOperation::READ), token), -EAGAIN);
    ASSERT_TRUE(block_model_release(queue, TRANSFER));
    EXPECT_EQ(block_model_submit(queue, transfer(BlockRequestOperation::READ, 1), token), 0);
    EXPECT_EQ(queue.stats.request_high_water, 2U);
    EXPECT_EQ(queue.stats.byte_high_water, 512U);
    EXPECT_EQ(queue.requests[FLUSH.slot].state, BlockRequestState::QUEUED);
}

TEST(BlockRequestModel, HardSlotLimitAndNoGenerationWrap) {
    BlockQueueModel queue{};
    BlockRequestToken token{};
    for (size_t i = 0; i < BLOCK_MODEL_MAX_REQUESTS; ++i) {
        EXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH}, token), 0);
    }
    EXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH}, token), -EAGAIN);
    EXPECT_EQ(queue.owned_requests, BLOCK_MODEL_MAX_REQUESTS);
    queue = {};
    for (auto& request : queue.requests) {
        request.token.request_generation = UINT64_MAX;
    }
    EXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH}, token), -EOVERFLOW);
    queue.requests[0].token.request_generation = UINT64_MAX - 1;
    EXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH}, token), 0);
    EXPECT_EQ(token.request_generation, UINT64_MAX);
    EXPECT_EQ(block_model_cancel(queue, token), BlockCancelResult::CLAIMED);
    ASSERT_TRUE(block_model_finish_completion(queue, token));
    ASSERT_TRUE(block_model_release(queue, token));
    EXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH}, token), -EOVERFLOW);
    queue = {};
    queue.next_sequence = UINT64_MAX;
    EXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH}, token), -EOVERFLOW);
    queue = {};
    queue.generation = UINT64_MAX;
    ASSERT_EQ(block_model_begin_reset(queue), 0);
    EXPECT_EQ(block_model_finish_reset(queue), -EOVERFLOW);
    EXPECT_FALSE(queue.admission_open);
    EXPECT_TRUE(queue.resetting);
}

TEST(BlockRequestModel, ClosedResetCanDrainWithoutReopeningAdmission) {
    BlockQueueModel queue{};
    ASSERT_EQ(block_model_begin_reset(queue), 0);
    ASSERT_EQ(block_model_finish_reset(queue, false), 0);
    EXPECT_FALSE(queue.admission_open);
    BlockRequestToken token{};
    EXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH}, token), -ENODEV);
}

TEST(BlockRequestModel, RejectedProviderOwnershipRestoresOrderingAndHonorsRacedCancel) {
    BlockQueueModel queue{};
    auto const FIRST = submit(queue, transfer(BlockRequestOperation::WRITE));
    auto const SECOND = submit(queue, transfer(BlockRequestOperation::READ));
    uint64_t const SEQUENCE = queue.requests[FIRST.slot].sequence;
    BlockRequestToken issued{};
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    ASSERT_TRUE(block_model_defer_issue(queue, FIRST));
    EXPECT_EQ(queue.requests[FIRST.slot].sequence, SEQUENCE);
    EXPECT_EQ(queue.requests[FIRST.slot].state, BlockRequestState::QUEUED);
    EXPECT_FALSE(block_model_defer_issue(queue, FIRST));
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, FIRST);
    EXPECT_EQ(block_model_cancel(queue, FIRST, -ETIMEDOUT), BlockCancelResult::NEEDS_RETIREMENT);
    ASSERT_TRUE(block_model_defer_issue(queue, FIRST));
    EXPECT_EQ(queue.requests[FIRST.slot].state, BlockRequestState::COMPLETING);
    EXPECT_EQ(queue.requests[FIRST.slot].status, -ETIMEDOUT);
    EXPECT_EQ(queue.stats.completion_claims, 1U);
    EXPECT_FALSE(block_model_next_issue(queue, issued));
    ASSERT_TRUE(block_model_finish_completion(queue, FIRST));
    ASSERT_TRUE(block_model_release(queue, FIRST));
    EXPECT_FALSE(block_model_defer_issue(queue, FIRST));
    ASSERT_TRUE(block_model_next_issue(queue, issued));
    EXPECT_EQ(issued, SECOND);
}

TEST(BlockRequestModel, InvalidDescriptionsDoNotConsumeResources) {
    BlockQueueModel queue{};
    BlockRequestToken token{};
    EXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::FLUSH, 1, 0, 0}, token), -EINVAL);
    EXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::READ, 0, 0, 1}, token), -EINVAL);
    EXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::WRITE, UINT64_MAX, 1, 512}, token), -EINVAL);
    EXPECT_EQ(block_model_submit(queue, {static_cast<BlockRequestOperation>(100), 0, 1, 512}, token), -EINVAL);
    EXPECT_EQ(block_model_submit(queue, {BlockRequestOperation::WRITE, 0, 1, BLOCK_MODEL_MAX_REQUEST_BYTES + 1}, token), -E2BIG);
    EXPECT_EQ(queue.owned_requests, 0U);
    EXPECT_EQ(queue.owned_bytes, 0U);
}

TEST(BlockRequestModel, GeometryAndPartitionTranslationRejectOverflowAndEscape) {
    size_t bytes = 100;
    EXPECT_EQ(block_model_validate_io(5, 3, 512, 8, BLOCK_MODEL_MAX_REQUEST_BYTES, bytes), 0);
    EXPECT_EQ(bytes, 1536U);
    EXPECT_EQ(block_model_validate_io(5, 4, 512, 8, BLOCK_MODEL_MAX_REQUEST_BYTES, bytes), -EINVAL);
    EXPECT_EQ(bytes, 0U);
    EXPECT_EQ(block_model_validate_io(UINT64_MAX, 1, 512, UINT64_MAX, BLOCK_MODEL_MAX_REQUEST_BYTES, bytes), -EINVAL);
    EXPECT_EQ(block_model_validate_io(0, std::numeric_limits<size_t>::max(), 2, UINT64_MAX, BLOCK_MODEL_MAX_REQUEST_BYTES, bytes),
              -EOVERFLOW);
    EXPECT_EQ(block_model_validate_io(0, 2, BLOCK_MODEL_MAX_REQUEST_BYTES, 2, BLOCK_MODEL_MAX_REQUEST_BYTES, bytes), -E2BIG);
    EXPECT_EQ(block_model_validate_io(0, 1, 0, 2, BLOCK_MODEL_MAX_REQUEST_BYTES, bytes), -EINVAL);
    uint64_t root_lba = 100;
    EXPECT_EQ(block_model_translate_partition(1, 2, 10, 3, 20, root_lba), 0);
    EXPECT_EQ(root_lba, 11U);
    EXPECT_EQ(block_model_translate_partition(2, 2, 10, 3, 20, root_lba), -EINVAL);
    EXPECT_EQ(root_lba, 0U);
    EXPECT_EQ(block_model_translate_partition(0, 1, UINT64_MAX, 2, UINT64_MAX, root_lba), -EINVAL);
}

TEST(BlockRequestModel, ScatterGatherChecksLimitsSumAlignmentAndDmaRange) {
    std::array<BlockScatterSegment, 2> segments{{{0x1000, 512}, {0x2000, 512}}};
    BlockScatterLimits limits{2, 512, 512, UINT32_MAX};
    EXPECT_EQ(block_model_validate_sg(segments.data(), segments.size(), 1024, limits), 0);
    EXPECT_EQ(block_model_validate_sg(segments.data(), 3, 1024, limits), -E2BIG);
    EXPECT_EQ(block_model_validate_sg(segments.data(), segments.size(), 512, limits), -EOVERFLOW);
    EXPECT_EQ(block_model_validate_sg(segments.data(), segments.size(), 1536, limits), -EINVAL);
    segments[1].address++;
    EXPECT_EQ(block_model_validate_sg(segments.data(), segments.size(), 1024, limits), -EINVAL);
    segments[1].address = UINT64_MAX - 255;
    limits.alignment = 1;
    EXPECT_EQ(block_model_validate_sg(segments.data(), segments.size(), 1024, limits), -EOVERFLOW);
    segments[1].address = UINT32_MAX - 255;
    EXPECT_EQ(block_model_validate_sg(segments.data(), segments.size(), 1024, limits), -ERANGE);
    limits.dma_mask = 5;
    EXPECT_EQ(block_model_validate_sg(segments.data(), segments.size(), 1024, limits), -EINVAL);
    EXPECT_EQ(block_model_validate_sg(nullptr, 1, 512), -EINVAL);
}
