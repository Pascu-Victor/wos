#include <gtest/gtest.h>

#include <array>
#include <dev/block_request_model.hpp>
#include <memory>
#include <test/fault_block_device_impl.hpp>

using namespace ker::dev;
using namespace ker::test;

namespace {
// The real bounded fault-media callbacks execute only when the controller is
// stepped. Completion delivery is independently withheld/reordered/repeated.
// This fake has no clock, threads, hardware, or alternate request state model.
class DelayedFaultController {
   public:
    BlockQueueModel queue{};
    std::unique_ptr<FaultBlockDevice> media{std::make_unique<FaultBlockDevice>(512, 32, 0x11)};

    auto submit(BlockRequestOperation operation, uint64_t lba, void* buffer, BlockRequestToken& token) -> int {
        size_t const COUNT = operation == BlockRequestOperation::FLUSH ? 0 : 1;
        size_t const BYTES = COUNT * 512;
        int const RESULT = block_model_submit(queue, {operation, lba, COUNT, BYTES}, token);
        if (RESULT == 0) {
            pending_[token.slot] = {token, buffer, 0, 0, false};
        }
        return RESULT;
    }

    auto execute(BlockRequestToken token) -> bool {
        if (token.slot >= pending_.size()) {
            return false;
        }
        auto& pending = pending_[token.slot];
        auto const& request = queue.requests[token.slot];
        if (!(pending.token == token) || !(request.token == token) || request.state != BlockRequestState::ISSUED || pending.executed) {
            return false;
        }
        auto* device = media->device();
        size_t const EVENTS_BEFORE = media->event_count();
        switch (request.op) {
            case BlockRequestOperation::READ:
                pending.status = device->read_blocks(device, request.lba, request.count, pending.buffer);
                break;
            case BlockRequestOperation::WRITE:
                pending.status = device->write_blocks(device, request.lba, request.count, pending.buffer);
                break;
            case BlockRequestOperation::FLUSH:
                pending.status = device->flush != nullptr ? device->flush(device) : -EOPNOTSUPP;
                break;
        }
        pending.bytes = pending.status == 0 ? request.bytes : 0;
        if (request.op != BlockRequestOperation::FLUSH && media->event_count() > EVENTS_BEFORE) {
            pending.bytes = media->event(media->event_count() - 1)->transferred_bytes;
        }
        pending.executed = true;
        return true;
    }

    auto deliver(BlockRequestToken token, bool retire_callback = true) -> bool {
        if (token.slot >= pending_.size()) {
            return false;
        }
        auto const& pending = pending_[token.slot];
        if (!(pending.token == token) || !pending.executed || !block_model_claim_completion(queue, token, pending.status, pending.bytes)) {
            return false;
        }
        return !retire_callback || block_model_finish_completion(queue, token);
    }

    // A reset step explicitly quiesces every held command before publishing
    // errors. No execution method can access those buffers after this step.
    auto quiesce_reset() -> int {
        if (!queue.resetting) {
            return -EINVAL;
        }
        for (auto& request : queue.requests) {
            if (request.state == BlockRequestState::ISSUED) {
                pending_[request.token.slot].executed = true;
                if (!block_model_claim_completion(queue, request.token, -EIO, 0, true)) {
                    return -EIO;
                }
            }
            if (request.state == BlockRequestState::COMPLETING && !block_model_finish_completion(queue, request.token)) {
                return -EIO;
            }
        }
        media->power_cut();
        return block_model_finish_reset(queue);
    }

   private:
    struct Pending {
        BlockRequestToken token{};
        void* buffer{};
        int status{};
        size_t bytes{};
        bool executed{};
    };
    std::array<Pending, BLOCK_MODEL_MAX_REQUESTS> pending_{};
};
}  // namespace

TEST(BlockRequestFault, ReorderedTransfersWaitForDeliveryBeforeDurabilityBarrier) {
    DelayedFaultController controller{};
    std::array<uint8_t, 512> first{};
    std::array<uint8_t, 512> second{};
    first.fill(0x42);
    second.fill(0x73);
    BlockRequestToken a{}, b{}, flush{}, later{}, issued{};
    ASSERT_EQ(controller.submit(BlockRequestOperation::WRITE, 0, first.data(), a), 0);
    ASSERT_EQ(controller.submit(BlockRequestOperation::WRITE, 1, second.data(), b), 0);
    ASSERT_EQ(controller.submit(BlockRequestOperation::FLUSH, 0, nullptr, flush), 0);
    ASSERT_EQ(controller.submit(BlockRequestOperation::WRITE, 2, first.data(), later), 0);
    ASSERT_TRUE(block_model_next_issue(controller.queue, issued));
    EXPECT_EQ(issued, a);
    ASSERT_TRUE(block_model_next_issue(controller.queue, issued));
    EXPECT_EQ(issued, b);
    ASSERT_TRUE(controller.execute(b));
    ASSERT_TRUE(controller.execute(a));
    EXPECT_EQ(controller.media->durable_data()[0], 0x11);
    EXPECT_EQ(controller.media->volatile_data()[0], 0x42);
    ASSERT_TRUE(controller.deliver(b));
    EXPECT_FALSE(block_model_next_issue(controller.queue, issued));
    ASSERT_TRUE(controller.deliver(a, false));
    EXPECT_FALSE(block_model_next_issue(controller.queue, issued));
    ASSERT_TRUE(block_model_finish_completion(controller.queue, a));
    ASSERT_TRUE(block_model_next_issue(controller.queue, issued));
    EXPECT_EQ(issued, flush);
    ASSERT_TRUE(controller.execute(flush));
    ASSERT_TRUE(controller.deliver(flush));
    EXPECT_FALSE(controller.deliver(flush));
    EXPECT_EQ(controller.media->operation_count(FaultBlockOperation::FLUSH), 1U);
    ASSERT_TRUE(block_model_next_issue(controller.queue, issued));
    EXPECT_EQ(issued, later);
    ASSERT_TRUE(controller.execute(later));
    ASSERT_TRUE(controller.deliver(later));
    controller.media->power_cut();
    EXPECT_EQ(controller.media->volatile_data()[0], 0x42);
    EXPECT_EQ(controller.media->volatile_data()[512], 0x73);
    EXPECT_EQ(controller.media->volatile_data()[1024], 0x11);
    for (auto token : {a, b, flush, later}) {
        EXPECT_TRUE(block_model_release(controller.queue, token));
    }
    EXPECT_EQ(controller.queue.owned_requests, 0U);
    EXPECT_EQ(controller.queue.owned_bytes, 0U);
}

TEST(BlockRequestFault, QueuedCancellationDoesNotTouchMedia) {
    DelayedFaultController controller{};
    std::array<uint8_t, 512> buffer{};
    buffer.fill(0xFF);
    BlockRequestToken token{}, issued{};
    ASSERT_EQ(controller.submit(BlockRequestOperation::WRITE, 0, buffer.data(), token), 0);
    EXPECT_EQ(block_model_cancel(controller.queue, token), BlockCancelResult::CLAIMED);
    EXPECT_FALSE(block_model_next_issue(controller.queue, issued));
    EXPECT_FALSE(controller.execute(token));
    EXPECT_EQ(controller.media->event_count(), 0U);
    EXPECT_EQ(controller.media->volatile_data()[0], 0x11);
    EXPECT_TRUE(block_model_finish_completion(controller.queue, token));
    EXPECT_TRUE(block_model_release(controller.queue, token));
}

TEST(BlockRequestFault, TimedOutIssuedWriteCannotReportSuccessOrReleaseBeforeQuiescence) {
    DelayedFaultController controller{};
    std::array<uint8_t, 512> buffer{};
    buffer.fill(0x99);
    BlockRequestToken token{}, issued{};
    ASSERT_EQ(controller.submit(BlockRequestOperation::WRITE, 0, buffer.data(), token), 0);
    ASSERT_TRUE(block_model_next_issue(controller.queue, issued));
    EXPECT_EQ(block_model_cancel(controller.queue, token, -ETIMEDOUT), BlockCancelResult::NEEDS_RETIREMENT);
    EXPECT_FALSE(block_model_release(controller.queue, token));
    ASSERT_TRUE(controller.execute(token));
    ASSERT_TRUE(controller.deliver(token));
    EXPECT_EQ(controller.queue.requests[token.slot].status, -ETIMEDOUT);
    EXPECT_EQ(controller.queue.requests[token.slot].completed_bytes, 0U);
    EXPECT_EQ(controller.media->volatile_data()[0], 0x99);
    controller.media->power_cut();
    EXPECT_EQ(controller.media->volatile_data()[0], 0x11);
    EXPECT_TRUE(block_model_release(controller.queue, token));
}

TEST(BlockRequestFault, ResetRejectsLateCompletionAfterSlotReuseAndBalancesOwners) {
    DelayedFaultController controller{};
    std::array<uint8_t, 512> buffer{};
    buffer.fill(0x66);
    BlockRequestToken old{}, held{}, issued{}, current{};
    ASSERT_EQ(controller.submit(BlockRequestOperation::WRITE, 0, buffer.data(), old), 0);
    ASSERT_EQ(controller.submit(BlockRequestOperation::READ, 0, buffer.data(), held), 0);
    ASSERT_TRUE(block_model_next_issue(controller.queue, issued));
    ASSERT_TRUE(controller.execute(old));  // Device changed volatile data, IRQ withheld.
    ASSERT_EQ(block_model_begin_reset(controller.queue), 0);
    EXPECT_EQ(block_model_finish_reset(controller.queue), -EBUSY);
    ASSERT_EQ(controller.quiesce_reset(), 0);
    EXPECT_EQ(controller.queue.requests[old.slot].status, -EIO);
    EXPECT_EQ(controller.queue.requests[old.slot].completed_bytes, 0U);
    EXPECT_EQ(controller.media->volatile_data()[0], 0x11);
    ASSERT_TRUE(block_model_release(controller.queue, old));
    ASSERT_TRUE(block_model_release(controller.queue, held));
    EXPECT_FALSE(controller.execute(old));
    EXPECT_FALSE(controller.deliver(old));
    ASSERT_EQ(controller.submit(BlockRequestOperation::READ, 0, buffer.data(), current), 0);
    EXPECT_EQ(current.slot, old.slot);
    EXPECT_NE(current.queue_generation, old.queue_generation);
    ASSERT_TRUE(block_model_next_issue(controller.queue, issued));
    EXPECT_FALSE(block_model_claim_completion(controller.queue, old, 0, 512));
    ASSERT_TRUE(controller.execute(current));
    ASSERT_TRUE(controller.deliver(current));
    EXPECT_EQ(buffer[0], 0x11);
    ASSERT_TRUE(block_model_release(controller.queue, current));
    EXPECT_EQ(controller.queue.stats.submitted, controller.queue.stats.completed);
    EXPECT_EQ(controller.queue.stats.completed, controller.queue.stats.released);
    EXPECT_EQ(controller.queue.owned_bytes, 0U);
}

TEST(BlockRequestFault, TornWriteAndFailedFlushPreserveDurableGeneration) {
    DelayedFaultController controller{};
    std::array<uint8_t, 512> buffer{};
    buffer.fill(0xCC);
    ASSERT_TRUE(controller.media->fail_operation(FaultBlockOperation::WRITE, 1, -EIO, FaultBlockWriteMode::TORN_PREFIX, 128));
    BlockRequestToken write{}, flush{}, issued{};
    ASSERT_EQ(controller.submit(BlockRequestOperation::WRITE, 0, buffer.data(), write), 0);
    ASSERT_EQ(controller.submit(BlockRequestOperation::FLUSH, 0, nullptr, flush), 0);
    ASSERT_TRUE(block_model_next_issue(controller.queue, issued));
    ASSERT_TRUE(controller.execute(write));
    ASSERT_TRUE(controller.deliver(write));
    EXPECT_EQ(controller.queue.requests[write.slot].status, -EIO);
    EXPECT_EQ(controller.queue.requests[write.slot].completed_bytes, 128U);
    ASSERT_TRUE(controller.media->fail_operation(FaultBlockOperation::FLUSH, 1));
    ASSERT_TRUE(block_model_next_issue(controller.queue, issued));
    ASSERT_TRUE(controller.execute(flush));
    ASSERT_TRUE(controller.deliver(flush));
    EXPECT_EQ(controller.queue.requests[flush.slot].status, -EIO);
    EXPECT_EQ(controller.media->durability_generation(), 0U);
    controller.media->power_cut();
    EXPECT_EQ(controller.media->volatile_data()[0], 0x11);
    EXPECT_EQ(controller.media->volatile_data()[127], 0x11);
}

TEST(BlockRequestFault, UnsupportedFlushNeverReportsDurabilitySuccess) {
    DelayedFaultController controller{};
    controller.media->device()->flush = nullptr;
    EXPECT_EQ(block_flush(controller.media->device()), -EOPNOTSUPP);
    BlockRequestToken flush{}, issued{};
    ASSERT_EQ(controller.submit(BlockRequestOperation::FLUSH, 0, nullptr, flush), 0);
    ASSERT_TRUE(block_model_next_issue(controller.queue, issued));
    ASSERT_TRUE(controller.execute(flush));
    ASSERT_TRUE(controller.deliver(flush));
    EXPECT_EQ(controller.queue.requests[flush.slot].status, -EOPNOTSUPP);
    EXPECT_EQ(controller.media->durability_generation(), 0U);
}

TEST(BlockRequestFault, PartialReadRemainsErrorWithRecordedBytes) {
    DelayedFaultController controller{};
    std::array<uint8_t, 512> buffer{};
    ASSERT_TRUE(controller.media->fail_read_prefix(1, 37));
    BlockRequestToken read{}, issued{};
    ASSERT_EQ(controller.submit(BlockRequestOperation::READ, 0, buffer.data(), read), 0);
    ASSERT_TRUE(block_model_next_issue(controller.queue, issued));
    ASSERT_TRUE(controller.execute(read));
    ASSERT_TRUE(controller.deliver(read));
    EXPECT_EQ(controller.queue.requests[read.slot].status, -EIO);
    EXPECT_EQ(controller.queue.requests[read.slot].completed_bytes, 37U);
    EXPECT_EQ(buffer[36], 0x11);
    EXPECT_EQ(buffer[37], 0U);
    EXPECT_FALSE(controller.deliver(read));
    EXPECT_EQ(controller.media->operation_count(FaultBlockOperation::READ), 1U);
}
