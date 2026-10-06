#include <gtest/gtest.h>
#include <sys/mman.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <dev/block_device.hpp>
#include <dev/block_request.hpp>
#include <functional>
#include <memory>
#include <mutex>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/spinlock.hpp>
#include <platform/sys/usercopy.hpp>
#include <string>
#include <thread>
#include <vector>

using namespace ker::dev;

// Registry arbitration is a provider boundary in this target. Its real
// locking/alias rules are exercised by block_device_registry_test; every
// request/ordering/completion operation here compiles production source.
namespace ker::dev {
auto block_device_is_read_only(const BlockDevice* device) -> bool { return device != nullptr && device->read_only; }
}  // namespace ker::dev

namespace {
struct Gate {
    std::mutex lock;
    std::condition_variable cv;
    bool entered{};
    bool released{};

    void enter_and_wait() {
        std::unique_lock guard(lock);
        entered = true;
        cv.notify_all();
        cv.wait(guard, [&] { return released; });
    }
    auto await_entered() -> bool {
        std::unique_lock guard(lock);
        return cv.wait_for(guard, std::chrono::seconds(5), [&] { return entered; });
    }
    void release() {
        std::lock_guard guard(lock);
        released = true;
        cv.notify_all();
    }
};

class NativeProvider {
   public:
    struct Pending {
        BlockRequest* owner{};
        BlockRequestToken token{};
    };
    BlockDevice device{};
    std::mutex lock;
    std::array<Pending, BLOCK_MODEL_MAX_REQUESTS> pending{};
    std::array<std::byte, 128 * 512> media{};
    std::vector<BlockRequestDescription> issued;
    std::vector<BlockRequestToken> cancelled;
    std::function<void(BlockRequest&)> reject_hook;
    std::function<void(size_t)> service_hook;
    std::function<void(size_t)> poll_hook;
    size_t service_calls{};
    size_t poll_calls{};
    bool poll_result{true};
    size_t reject_submissions{};
    int rejection{-EAGAIN};
    bool retire_cancel{};
    bool auto_complete{};
    bool service_completion{};

    NativeProvider() {
        std::strcpy(device.name.data(), "host-block-native");
        device.block_size = 512;
        device.total_blocks = 128;
        device.private_data = this;
    }

    void reset_controls() {
        std::lock_guard guard(lock);
        pending.fill({});
        media.fill(std::byte{0});
        issued.clear();
        cancelled.clear();
        reject_hook = {};
        service_hook = {};
        poll_hook = {};
        service_calls = 0;
        poll_calls = 0;
        poll_result = true;
        reject_submissions = 0;
        rejection = -EAGAIN;
        retire_cancel = false;
        auto_complete = false;
        service_completion = false;
        device.read_only = false;
    }

    auto retire(BlockRequestToken token, int status = 0, size_t bytes = SIZE_MAX, bool from_service = false) -> bool {
        BlockRequest* request = nullptr;
        {
            std::lock_guard guard(lock);
            if (token.slot >= pending.size() || pending[token.slot].owner == nullptr || !(pending[token.slot].token == token)) {
                return false;
            }
            request = pending[token.slot].owner;
            if (bytes == SIZE_MAX) bytes = status == 0 ? request->description.bytes : 0;
            if (status == 0 && request->description.op != BlockRequestOperation::FLUSH) {
                size_t offset = request->description.lba * 512;
                for (size_t i = 0; i < request->segment_count; ++i) {
                    auto const& segment = request->segments[i];
                    if (request->description.op == BlockRequestOperation::WRITE) {
                        std::memcpy(media.data() + offset, segment.data, segment.size);
                    } else {
                        std::memcpy(segment.data, media.data() + offset, segment.size);
                    }
                    offset += segment.size;
                }
            }
            pending[token.slot] = {};  // Transport owns no buffer after this point.
        }
        return from_service ? block_request_complete_from_service(&device, token, status, bytes)
                            : block_request_complete(&device, token, status, bytes);
    }

    static auto submit(BlockDevice* device, BlockRequest& request) -> int {
        auto& provider = *static_cast<NativeProvider*>(device->private_data);
        std::function<void(BlockRequest&)> hook;
        int rejection = -EAGAIN;
        {
            std::lock_guard guard(provider.lock);
            if (provider.reject_submissions != 0) {
                --provider.reject_submissions;
                hook = provider.reject_hook;
                rejection = provider.rejection;
            } else {
                provider.pending[request.token.slot] = {&request, request.token};
                provider.issued.push_back(request.description);
                return 0;
            }
        }
        if (hook) hook(request);
        return rejection;
    }
    static void service(BlockDevice* device) {
        auto& provider = *static_cast<NativeProvider*>(device->private_data);
        std::function<void(size_t)> hook;
        size_t calls = 0;
        {
            std::lock_guard guard(provider.lock);
            calls = ++provider.service_calls;
            hook = provider.service_hook;
        }
        if (hook) hook(calls);
        std::array<BlockRequestToken, BLOCK_MODEL_MAX_REQUESTS> tokens{};
        size_t count = 0;
        {
            std::lock_guard guard(provider.lock);
            if (!provider.auto_complete) return;
            for (auto const& pending : provider.pending) {
                if (pending.owner != nullptr) tokens[count++] = pending.token;
            }
        }
        for (size_t i = 0; i < count; ++i) static_cast<void>(provider.retire(tokens[i], 0, SIZE_MAX, provider.service_completion));
    }
    static auto poll_active(BlockDevice* device) -> bool {
        auto& provider = *static_cast<NativeProvider*>(device->private_data);
        auto* task = ker::mod::sched::get_current_task();
        EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
        EXPECT_NE(task, nullptr);
        if (task != nullptr) {
            EXPECT_GT(task->reclaim_no_io_depth, 0U);
            EXPECT_EQ(task->references.load(), 3U);  // Caller, waiter, service owner.
        }
        std::function<void(size_t)> hook;
        size_t calls = 0;
        bool result = false;
        {
            std::lock_guard guard(provider.lock);
            calls = ++provider.poll_calls;
            hook = provider.poll_hook;
            result = provider.poll_result;
        }
        // Hooks model external events at the provider boundary, not a native
        // poll implementation. The production AHCI poll is tested separately.
        if (hook) hook(calls);
        return result;
    }
    static void cancel(BlockDevice* device, BlockRequestToken token, int reason) {
        auto& provider = *static_cast<NativeProvider*>(device->private_data);
        bool retire = false;
        {
            std::lock_guard guard(provider.lock);
            provider.cancelled.push_back(token);
            retire = provider.retire_cancel;
        }
        if (retire) static_cast<void>(provider.retire(token, reason, 0));
    }
};

NativeProvider native;  // Permanent provider, matching the production queue contract.
NativeProvider closing_native;
NativeProvider racing_native;
NativeProvider callback_closed_native;
NativeProvider polled_native;
NativeProvider polled_closed_native;
NativeProvider arbiter_native;
BlockDevice legacy{};
std::array<std::byte, 128 * 512> legacy_media{};
size_t legacy_calls{};

auto legacy_read(BlockDevice*, uint64_t lba, size_t count, void* buffer) -> int {
    ++legacy_calls;
    std::memcpy(buffer, legacy_media.data() + lba * 512, count * 512);
    return 0;
}
auto legacy_write(BlockDevice*, uint64_t lba, size_t count, const void* buffer) -> int {
    ++legacy_calls;
    std::memcpy(legacy_media.data() + lba * 512, buffer, count * 512);
    return 0;
}

auto snapshot(BlockDevice& device) -> BlockQueueSnapshot {
    std::array<BlockQueueSnapshot, 64> snapshots{};
    size_t const COUNT = block_queue_snapshots(snapshots);
    for (size_t i = 0; i < COUNT; ++i) {
        if (snapshots[i].name == device.name) return snapshots[i];
    }
    ADD_FAILURE() << "configured queue missing from snapshot";
    return {};
}

class BlockRequestRuntime : public testing::Test {
   public:
    std::array<std::array<std::byte, 512>, BLOCK_MODEL_MAX_REQUESTS + 1> buffers{};
    std::vector<std::unique_ptr<BlockRequest>> owners;
    std::atomic<size_t> callbacks{};

    void SetUp() override {
        static bool configured = false;
        if (!configured) {
            static const BlockAsyncOps OPS{NativeProvider::submit, NativeProvider::service, NativeProvider::cancel};
            ASSERT_EQ(block_queue_configure(&native.device, {8, 512, 32, 64, 4096}, &OPS), 0);
            std::strcpy(closing_native.device.name.data(), "host-block-closing");
            std::strcpy(racing_native.device.name.data(), "host-block-racing");
            ASSERT_EQ(block_queue_configure(&closing_native.device, {8, 512, 32, 64, 4096}, &OPS), 0);
            ASSERT_EQ(block_queue_configure(&racing_native.device, {8, 512, 32, 64, 4096}, &OPS), 0);
            std::strcpy(callback_closed_native.device.name.data(), "host-block-callback-close");
            ASSERT_EQ(block_queue_configure(&callback_closed_native.device, {8, 512, 32, 64, 4096}, &OPS), 0);
            static const BlockAsyncOps POLLED_OPS{NativeProvider::submit, NativeProvider::service, NativeProvider::cancel,
                                                  NativeProvider::poll_active};
            std::strcpy(polled_native.device.name.data(), "host-block-polled");
            std::strcpy(polled_closed_native.device.name.data(), "host-block-polled-close");
            ASSERT_EQ(block_queue_configure(&polled_native.device, {8, 512, 32, 64, 4096}, &POLLED_OPS), 0);
            ASSERT_EQ(block_queue_configure(&polled_closed_native.device, {8, 512, 32, 64, 4096}, &POLLED_OPS), 0);
            std::strcpy(arbiter_native.device.name.data(), "host-block-arbiter");
            ASSERT_EQ(block_queue_configure(&arbiter_native.device, {8, 512, 32, 64, 64 * 512}, &OPS), 0);
            std::strcpy(legacy.name.data(), "host-block-legacy");
            legacy.block_size = 512;
            legacy.total_blocks = 128;
            legacy.read_blocks = legacy_read;
            legacy.write_blocks = legacy_write;
            ASSERT_EQ(block_queue_configure(&legacy, {1, 512, 32, 64, 4096}), 0);
            configured = true;
        }
        native.reset_controls();
        closing_native.reset_controls();
        racing_native.reset_controls();
        callback_closed_native.reset_controls();
        polled_native.reset_controls();
        polled_closed_native.reset_controls();
        arbiter_native.reset_controls();
        legacy_media.fill(std::byte{0});
        legacy_calls = 0;
        ker::mod::time::block_test_now_us.store(1000);
        ker::mod::mm::addr::block_test_hhdm_offset.store(0);
        ker::mod::sys::usercopy::block_test_fail_copy = false;
        ker::mod::sched::block_test_current_task = nullptr;
        ker::mod::sched::block_test_pause_hook = {};
        ker::mod::sched::block_test_parks.store(0);
        ker::mod::sched::block_test_yields.store(0);
        ker::mod::sched::block_test_shutdown.store(false);
        ker::mod::sched::block_test_run_queues.store(true);
        ker::mod::sys::block_test_irqsave_hook = {};
        EXPECT_EQ(ker::mod::sched::block_test_shutdown_holds.load(), 0U);
    }

    void TearDown() override {
        ker::mod::sys::block_test_irqsave_hook = {};
        ker::mod::sched::block_test_pause_hook = {};
        native.service_hook = {};
        for (auto* provider :
             {&native, &closing_native, &racing_native, &callback_closed_native, &polled_native, &polled_closed_native, &arbiter_native}) {
            provider->retire_cancel = true;
            static_cast<void>(block_queue_begin_reset(&provider->device));
            block_queue_service(&provider->device);
        }
        for (auto const& owner : owners) {
            if (owner->queue != nullptr) {
                EXPECT_TRUE(owner->completed.load(std::memory_order_acquire));
                EXPECT_EQ(block_request_release(*owner), 0);
            }
        }
        for (auto* provider :
             {&native, &closing_native, &racing_native, &callback_closed_native, &polled_native, &polled_closed_native, &arbiter_native}) {
            EXPECT_EQ(block_queue_finish_reset(&provider->device), 0);
            EXPECT_EQ(snapshot(provider->device).owned_requests, 0U);
            EXPECT_EQ(snapshot(provider->device).owned_bytes, 0U);
        }
        static_cast<void>(block_queue_begin_reset(&legacy));
        block_queue_service(&legacy);
        EXPECT_EQ(block_queue_finish_reset(&legacy), 0);
        ker::mod::mm::addr::block_test_hhdm_offset.store(0);
        ker::mod::sched::block_test_current_task = nullptr;
        EXPECT_EQ(ker::mod::sched::block_test_shutdown_holds.load(), 0U);
    }

    auto owner() -> BlockRequest& {
        owners.push_back(std::make_unique<BlockRequest>());
        return *owners.back();
    }

    static void callback(BlockRequest& request, void* arg) {
        auto& fixture = *static_cast<BlockRequestRuntime*>(arg);
        EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
        EXPECT_FALSE(request.completed.load(std::memory_order_acquire));
        ++fixture.callbacks;
    }

    auto submit(BlockRequest& request, BlockRequestOperation op = BlockRequestOperation::READ, uint64_t lba = 0,
                BlockDevice* device = &native.device, uint64_t timeout = 1000) -> int {
        if (op == BlockRequestOperation::FLUSH) {
            return block_request_submit(device, request, op, 0, 0, {}, timeout, callback, this);
        }
        size_t const INDEX = owners.size() - 1;
        BlockSegment segment{buffers[INDEX].data(), 512};
        return block_request_submit(device, request, op, lba, 1, {&segment, 1}, timeout, callback, this);
    }

    auto submit_for_wait(BlockRequest& request, BlockRequestOperation op = BlockRequestOperation::READ, uint64_t lba = 0,
                         BlockDevice* device = &native.device) -> int {
        if (op == BlockRequestOperation::FLUSH) return block_request_submit_for_wait(device, request, op, 0, 0, {});
        BlockSegment const segment{buffers[owners.size() - 1].data(), 512};
        return block_request_submit_for_wait(device, request, op, lba, 1, {&segment, 1});
    }

    auto wait_polled(BlockRequest& request, size_t& first_pause_calls, bool return_yield = false, NativeProvider& provider = native)
        -> int {
        namespace sched = ker::mod::sched;
        sched::task::Task task;
        task.syscall_account_start_us = 1;
        task.deferred_task_switch = return_yield;
        task.yield_switch = return_yield;
        sched::block_test_current_task = &task;
        sched::block_test_pause_hook = [&] {
            EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
            EXPECT_TRUE(request.queue != nullptr);
            EXPECT_EQ(task.references.load(), 2U);
            if (first_pause_calls == SIZE_MAX) first_pause_calls = provider.service_calls;
            provider.auto_complete = true;  // Safe cleanup even for saved-source negatives.
        };
        int const status = block_request_wait(request);
        sched::block_test_pause_hook = {};
        sched::block_test_current_task = nullptr;
        EXPECT_EQ(task.references.load(), 1U);
        EXPECT_EQ(task.deferred_task_switch, return_yield);
        EXPECT_EQ(task.yield_switch, return_yield);
        return status;
    }
};
}  // namespace

TEST_F(BlockRequestRuntime, ReadArbiterCountsLateIssuedSlotsAfterEarlierBlockedWrite) {
    namespace sched = ker::mod::sched;
    sched::task::Task task;
    sched::block_test_current_task = &task;
    struct ClearTask {
        ~ClearTask() {
            arbiter_native.service_hook = {};
            sched::block_test_current_task = nullptr;
        }
    } const CLEAR_TASK;
    arbiter_native.service_hook = [&](size_t) {
        EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
        EXPECT_EQ(task.references.load(), 2U);
        EXPECT_GT(task.reclaim_no_io_depth, 0U);
        EXPECT_GT(sched::block_test_shutdown_holds.load(), 0U);
    };
    // Retained terminal owners put the live depth well beyond an early slot
    // prefix. They retain storage but do not consume transport depth.
    arbiter_native.auto_complete = true;
    for (size_t i = 0; i < 54; ++i) {
        auto& request = owner();
        ASSERT_EQ(submit(request, BlockRequestOperation::READ, i, &arbiter_native.device), 0);
        block_queue_service(&arbiter_native.device);
        ASSERT_TRUE(request.completed.load(std::memory_order_acquire));
    }
    arbiter_native.auto_complete = false;
    std::array<BlockRequest*, 8> active{};
    for (size_t i = 0; i < active.size(); ++i) {
        active[i] = &owner();
        ASSERT_EQ(submit(*active[i], BlockRequestOperation::READ, i, &arbiter_native.device), 0);
        EXPECT_EQ(active[i]->token.slot, 54 + i);
    }
    block_queue_service(&arbiter_native.device);
    EXPECT_EQ(snapshot(arbiter_native.device).issued, 8U);
    ASSERT_TRUE(arbiter_native.retire(active.front()->token));
    block_queue_service(&arbiter_native.device);
    ASSERT_EQ(block_request_release(*active.front()), 0);

    auto& blocked = owner();
    ASSERT_EQ(submit(blocked, BlockRequestOperation::WRITE, 7, &arbiter_native.device), 0);
    ASSERT_EQ(blocked.token.slot, 54U);
    auto& replenishment = owner();
    ASSERT_EQ(submit(replenishment, BlockRequestOperation::READ, 90, &arbiter_native.device), 0);
    auto& later = owner();
    ASSERT_EQ(submit(later, BlockRequestOperation::READ, 91, &arbiter_native.device), 0);
    size_t const BEFORE = arbiter_native.issued.size();
    size_t const SERVICES = arbiter_native.service_calls;
    block_queue_service(&arbiter_native.device);
    ASSERT_EQ(arbiter_native.issued.size(), BEFORE + 1);
    EXPECT_EQ(arbiter_native.issued.back().lba, 90U);
    EXPECT_EQ(snapshot(arbiter_native.device).issued, 8U);
    EXPECT_EQ(snapshot(arbiter_native.device).queued, 2U);
    EXPECT_EQ(arbiter_native.service_calls, SERVICES + 2);  // Mandatory PRE/accepted POST.
    block_queue_service(&arbiter_native.device);
    EXPECT_EQ(arbiter_native.issued.size(), BEFORE + 1);
    EXPECT_EQ(arbiter_native.service_calls, SERVICES + 3);  // Healthy saturated PRE only.

    ASSERT_TRUE(arbiter_native.retire(active.back()->token));
    block_queue_service(&arbiter_native.device);
    ASSERT_EQ(arbiter_native.issued.size(), BEFORE + 2);
    EXPECT_EQ(arbiter_native.issued.back().op, BlockRequestOperation::WRITE);
    EXPECT_EQ(snapshot(arbiter_native.device).issued, 8U);
    EXPECT_EQ(snapshot(arbiter_native.device).queued, 1U);
    arbiter_native.service_hook = {};
    EXPECT_EQ(task.references.load(), 1U);
    EXPECT_EQ(task.reclaim_no_io_depth, 0U);
}

TEST_F(BlockRequestRuntime, ReadArbiterRetainsCompletingWriteAndFlushUntilFinalGather) {
    for (BlockRequestOperation const operation : {BlockRequestOperation::WRITE, BlockRequestOperation::FLUSH}) {
        auto& active = owner();
        ASSERT_EQ(submit(active, operation), 0);
        block_queue_service(&native.device);
        auto& later = owner();
        ASSERT_EQ(submit(later, BlockRequestOperation::READ), 0);
        size_t const ISSUED = native.issued.size();
        size_t const SERVICES = native.service_calls;
        size_t following_guards = 0;
        bool armed = true;
        bool injected = false;
        native.service_hook = [&](size_t) {
            if (!armed) return;
            armed = false;
            ker::mod::sys::block_test_irqsave_hook = [&] {
                if (injected || ++following_guards != 2) return;
                injected = true;
                // The first gather has ended; transport retirement immediately
                // before the issue guard leaves this owner COMPLETING.
                if (operation == BlockRequestOperation::WRITE) EXPECT_EQ(block_request_cancel(active), 0);
                EXPECT_TRUE(native.retire(active.token));
            };
        };
        block_queue_service(&native.device);
        ker::mod::sys::block_test_irqsave_hook = {};
        native.service_hook = {};
        ASSERT_TRUE(injected);
        EXPECT_EQ(native.issued.size(), ISSUED);
        EXPECT_TRUE(active.completed.load(std::memory_order_acquire));
        EXPECT_FALSE(later.completed.load(std::memory_order_acquire));
        EXPECT_EQ(snapshot(native.device).queued, 1U);
        EXPECT_EQ(active.status, operation == BlockRequestOperation::WRITE ? -ECANCELED : 0);
        EXPECT_EQ(native.service_calls, SERVICES + (operation == BlockRequestOperation::WRITE ? 2 : 1));
        block_queue_service(&native.device);
        ASSERT_EQ(native.issued.size(), ISSUED + 1);
        ASSERT_TRUE(native.retire(later.token));
        block_queue_service(&native.device);
        EXPECT_EQ(block_request_release(active), 0);
        EXPECT_EQ(block_request_release(later), 0);
    }
    EXPECT_EQ(callbacks.load(), 4U);
}

TEST_F(BlockRequestRuntime, ReadArbiterOrdersReusedSlotsAfterBackpressureThenDrainsReset) {
    auto const BEFORE = snapshot(native.device);
    std::array<BlockRequest*, 8> active{};
    for (size_t i = 0; i < active.size(); ++i) {
        active[i] = &owner();
        ASSERT_EQ(submit(*active[i], BlockRequestOperation::READ, i), 0);
    }
    block_queue_service(&native.device);
    ASSERT_TRUE(native.retire(active.front()->token));
    ASSERT_TRUE(native.retire(active.back()->token));
    block_queue_service(&native.device);
    ASSERT_EQ(block_request_release(*active.front()), 0);
    ASSERT_EQ(block_request_release(*active.back()), 0);
    auto& temporary = owner();
    ASSERT_EQ(submit(temporary, BlockRequestOperation::READ, 70), 0);
    auto& earlier = owner();
    ASSERT_EQ(submit(earlier, BlockRequestOperation::READ, 80), 0);
    EXPECT_EQ(temporary.token.slot, 0U);
    EXPECT_EQ(earlier.token.slot, 7U);
    ASSERT_EQ(block_request_cancel(temporary), 0);
    native.reject_submissions = 1;
    size_t const SERVICES = native.service_calls;
    block_queue_service(&native.device);
    EXPECT_EQ(native.service_calls, SERVICES + 2);  // EAGAIN still requires POST.
    EXPECT_EQ(native.issued.size(), 8U);
    EXPECT_EQ(snapshot(native.device).queued, 1U);
    EXPECT_EQ(temporary.status, -ECANCELED);
    ASSERT_EQ(block_request_release(temporary), 0);
    auto& later = owner();
    ASSERT_EQ(submit(later, BlockRequestOperation::READ, 81), 0);
    EXPECT_EQ(later.token.slot, 0U);
    block_queue_service(&native.device);
    ASSERT_EQ(native.issued.size(), 10U);
    EXPECT_EQ(native.issued[8].lba, 80U);
    EXPECT_EQ(native.issued[9].lba, 81U);
    EXPECT_EQ(snapshot(native.device).issued, 8U);

    native.retire_cancel = true;
    ASSERT_EQ(block_queue_begin_reset(&native.device), 0);
    EXPECT_EQ(block_queue_finish_reset(&native.device), -EBUSY);
    block_queue_service(&native.device);
    ASSERT_EQ(block_queue_finish_reset(&native.device), 0);
    EXPECT_EQ(native.cancelled.size(), 8U);
    EXPECT_EQ(earlier.status, -EIO);
    EXPECT_EQ(later.status, -EIO);
    for (auto const& request : owners) {
        if (request->queue != nullptr) EXPECT_EQ(block_request_release(*request), 0);
    }
    auto const AFTER = snapshot(native.device);
    EXPECT_EQ(AFTER.owned_requests, 0U);
    EXPECT_EQ(AFTER.owned_bytes, 0U);
    EXPECT_EQ(AFTER.stats.submitted - BEFORE.stats.submitted, 11U);
    EXPECT_EQ(AFTER.stats.completed - BEFORE.stats.completed, 11U);
    EXPECT_EQ(AFTER.stats.released - BEFORE.stats.released, 11U);
    EXPECT_EQ(callbacks.load(), 11U);
}

TEST_F(BlockRequestRuntime, ReadArbiterReclassifiesCallbackSubmittedWriteAndFlush) {
    auto& provider = arbiter_native;
    std::array<BlockRequest*, 8> active{};
    for (size_t i = 0; i < active.size(); ++i) {
        active[i] = &owner();
        ASSERT_EQ(submit(*active[i], BlockRequestOperation::READ, i, &provider.device), 0);
    }
    block_queue_service(&provider.device);
    auto& write = owner();
    auto& flush = owner();
    struct Context {
        BlockRequestRuntime* fixture;
        BlockRequest* write;
        BlockRequest* flush;
        BlockDevice* device;
    } context{this, &write, &flush, &provider.device};
    active.front()->callback = [](BlockRequest& request, void* arg) {
        auto& context = *static_cast<Context*>(arg);
        BlockRequestRuntime::callback(request, context.fixture);
        EXPECT_EQ(context.fixture->submit(*context.write, BlockRequestOperation::WRITE, 7, context.device), 0);
        EXPECT_EQ(context.fixture->submit(*context.flush, BlockRequestOperation::FLUSH, 0, context.device), 0);
    };
    active.front()->callback_arg = &context;
    ASSERT_TRUE(provider.retire(active.front()->token));
    block_queue_service(&provider.device);
    EXPECT_EQ(provider.issued.size(), 8U);
    EXPECT_EQ(snapshot(provider.device).queued, 2U);
    ASSERT_TRUE(provider.retire(active.back()->token));
    block_queue_service(&provider.device);
    ASSERT_EQ(provider.issued.size(), 9U);
    EXPECT_EQ(provider.issued.back().op, BlockRequestOperation::WRITE);
    for (size_t i = 1; i + 1 < active.size(); ++i) ASSERT_TRUE(provider.retire(active[i]->token));
    ASSERT_TRUE(provider.retire(write.token));
    block_queue_service(&provider.device);
    ASSERT_EQ(provider.issued.size(), 10U);
    EXPECT_EQ(provider.issued.back().op, BlockRequestOperation::FLUSH);
    ASSERT_TRUE(provider.retire(flush.token));
    block_queue_service(&provider.device);
    EXPECT_EQ(callbacks.load(), 10U);
}

TEST_F(BlockRequestRuntime, ForWaitBatchDrainsEveryAcceptedOwnerAfterCapacityAndLaterSubmissionError) {
    native.media.fill(std::byte{0xa5});
    auto const BEFORE = snapshot(native.device);
    std::array<BlockRequest*, 8> accepted{};
    for (size_t i = 0; i < accepted.size(); ++i) {
        auto& request = owner();
        ASSERT_EQ(submit_for_wait(request, BlockRequestOperation::READ, i), 0);
        EXPECT_EQ(request.callback, nullptr);
        accepted[i] = &request;
    }
    auto& rejected = owner();
    EXPECT_EQ(submit_for_wait(rejected), -EAGAIN);
    EXPECT_EQ(rejected.queue, nullptr);
    native.device.read_only = true;
    EXPECT_EQ(submit_for_wait(rejected, BlockRequestOperation::WRITE), -EROFS);
    EXPECT_EQ(rejected.queue, nullptr);
    EXPECT_EQ(snapshot(native.device).queued, 8U);
    EXPECT_TRUE(native.issued.empty());
    native.auto_complete = true;
    for (auto* request : accepted) {
        EXPECT_EQ(block_request_wait(*request), 0);
        EXPECT_EQ(request->bytes, 512U);
        EXPECT_EQ(block_request_release(*request), 0);
    }
    // One mandatory pass performs pre-service, admits all eight, then services
    // the retained batch. The rejected ninth entry never acquired ownership.
    EXPECT_EQ(native.service_calls, 2U);
    EXPECT_EQ(callbacks.load(), 0U);
    for (size_t i = 0; i < accepted.size(); ++i)
        EXPECT_TRUE(std::all_of(buffers[i].begin(), buffers[i].end(), [](std::byte value) { return value == std::byte{0xa5}; }));
    auto const AFTER = snapshot(native.device);
    EXPECT_EQ(AFTER.owned_requests, 0U);
    EXPECT_EQ(AFTER.owned_bytes, 0U);
    EXPECT_EQ(AFTER.stats.submitted - BEFORE.stats.submitted, 8U);
    EXPECT_EQ(AFTER.stats.completed - BEFORE.stats.completed, 8U);
    EXPECT_EQ(AFTER.stats.released - BEFORE.stats.released, 8U);
}

TEST_F(BlockRequestRuntime, ForWaitAndOrdinaryAdmissionShareOverlapAndFlushOrdering) {
    auto& first = owner();
    ASSERT_EQ(submit_for_wait(first, BlockRequestOperation::WRITE, 0), 0);
    auto& overlap = owner();
    ASSERT_EQ(submit(overlap, BlockRequestOperation::READ, 0), 0);
    auto& flush = owner();
    ASSERT_EQ(submit_for_wait(flush, BlockRequestOperation::FLUSH), 0);
    auto& later = owner();
    ASSERT_EQ(submit_for_wait(later, BlockRequestOperation::READ, 20), 0);
    block_queue_service(&native.device);
    ASSERT_EQ(native.issued.size(), 1U);
    ASSERT_TRUE(native.retire(first.token));
    block_queue_service(&native.device);
    ASSERT_EQ(native.issued.size(), 2U);
    EXPECT_EQ(native.issued.back().op, BlockRequestOperation::READ);
    EXPECT_EQ(native.issued.back().lba, 0U);
    ASSERT_TRUE(native.retire(overlap.token));
    block_queue_service(&native.device);
    ASSERT_EQ(native.issued.size(), 3U);
    EXPECT_EQ(native.issued.back().op, BlockRequestOperation::FLUSH);
    ASSERT_TRUE(native.retire(flush.token, -EIO));
    block_queue_service(&native.device);
    EXPECT_EQ(flush.status, -EIO);
    ASSERT_EQ(native.issued.size(), 4U);
    EXPECT_EQ(native.issued.back().lba, 20U);
    ASSERT_TRUE(native.retire(later.token));
    for (auto* request : {&first, &overlap, &flush, &later}) {
        EXPECT_EQ(block_request_wait(*request), request == &flush ? -EIO : 0);
        EXPECT_EQ(block_request_release(*request), 0);
    }
    EXPECT_EQ(callbacks.load(), 1U);  // Only ordinary admission has a callback.
    EXPECT_EQ(snapshot(native.device).owned_requests, 0U);
}

TEST_F(BlockRequestRuntime, ForWaitQueuedCancelAndIssuedResetRetireBeforeStorageRelease) {
    auto& canceled = owner();
    ASSERT_EQ(submit_for_wait(canceled), 0);
    ASSERT_EQ(block_request_cancel(canceled), 0);
    EXPECT_EQ(block_request_release(canceled), -EBUSY);
    EXPECT_EQ(block_request_wait(canceled), -ECANCELED);
    EXPECT_TRUE(native.issued.empty());
    EXPECT_EQ(block_request_release(canceled), 0);
    auto& active = owner();
    ASSERT_EQ(submit_for_wait(active), 0);
    auto const OLD = active.token;
    block_queue_service(&native.device);
    ASSERT_EQ(native.issued.size(), 1U);
    native.retire_cancel = true;
    ASSERT_EQ(block_queue_begin_reset(&native.device), 0);
    EXPECT_EQ(block_queue_finish_reset(&native.device), -EBUSY);
    EXPECT_EQ(block_request_release(active), -EBUSY);
    EXPECT_EQ(block_request_wait(active), -EIO);
    EXPECT_EQ(active.bytes, 0U);
    EXPECT_EQ(block_queue_finish_reset(&native.device), 0);
    EXPECT_EQ(block_request_release(active), 0);
    EXPECT_FALSE(block_request_complete(&native.device, OLD, 0, 512));
    EXPECT_EQ(callbacks.load(), 0U);
    EXPECT_EQ(snapshot(native.device).owned_requests, 0U);
    EXPECT_EQ(snapshot(native.device).owned_bytes, 0U);
}

TEST_F(BlockRequestRuntime, HealthyNoAttemptUsesOneProviderPassIncludingRetainedTerminalCancel) {
    block_queue_service(&native.device);
    EXPECT_EQ(native.service_calls, 1U);
    auto& canceled = owner();
    ASSERT_EQ(submit(canceled), 0);
    ASSERT_EQ(block_request_cancel(canceled), 0);
    block_queue_service(&native.device);
    EXPECT_EQ(native.service_calls, 3U);  // Entry cancellation retains POST.
    EXPECT_TRUE(canceled.completed.load(std::memory_order_acquire));
    EXPECT_EQ(canceled.status, -ECANCELED);
    block_queue_service(&native.device);
    EXPECT_EQ(native.service_calls, 4U);  // TERMINAL ownership is not an obligation.
    EXPECT_EQ(snapshot(native.device).owned_requests, 1U);
    EXPECT_EQ(block_request_release(canceled), 0);
}

TEST_F(BlockRequestRuntime, MandatoryEmptyTailSavesTwoGuardsAfterPreRetirementAndWithTerminalOwners) {
#ifndef WOS_TEST_EMPTY_TAIL_GUARDS
#ifdef WOS_ASYNC_BLOCK_IO
    constexpr size_t EMPTY_GUARDS = 5;
#else
    constexpr size_t EMPTY_GUARDS = 7;
#endif
#else
    constexpr size_t EMPTY_GUARDS = WOS_TEST_EMPTY_TAIL_GUARDS;
#endif
    ker::mod::sched::task::Task caller;
    ker::mod::sched::block_test_current_task = &caller;
    for (size_t const COUNT : {size_t{0}, size_t{1}, size_t{8}}) {
        std::vector<BlockRequest*> batch;
        for (size_t i = 0; i < COUNT; ++i) {
            auto& request = owner();
            ASSERT_EQ(submit(request, BlockRequestOperation::READ, i), 0);
            batch.push_back(&request);
        }
        block_queue_service(&native.device);
        auto const BEFORE = snapshot(native.device);
        size_t const CALLBACKS = callbacks.load();
        size_t const SERVICES = native.service_calls;
        native.auto_complete = true;
        native.service_completion = true;
        native.service_hook = [&](size_t) {
            EXPECT_EQ(caller.references.load(), 2U);
            EXPECT_EQ(caller.reclaim_no_io_depth, 1U);
            EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
            EXPECT_GT(ker::mod::sched::block_test_shutdown_holds.load(), 0U);
        };
        size_t guards = ker::mod::sys::block_test_irqsave_acquisitions;
        block_queue_service(&native.device);
        // Each real inline claim uses registry+claim guards, and publication
        // uses one owner guard. Empty-tail cost is independent of the batch.
        EXPECT_EQ(ker::mod::sys::block_test_irqsave_acquisitions - guards, EMPTY_GUARDS + 3 * COUNT);
        EXPECT_EQ(native.service_calls, SERVICES + 1);
        EXPECT_EQ(callbacks.load(), CALLBACKS + COUNT);
        auto const RETIRED = snapshot(native.device);
        EXPECT_EQ(RETIRED.stats.submitted, BEFORE.stats.submitted);
        EXPECT_EQ(RETIRED.stats.completed - BEFORE.stats.completed, COUNT);
        EXPECT_EQ(RETIRED.owned_requests, COUNT);
        EXPECT_EQ(RETIRED.owned_bytes, COUNT * 512);
        EXPECT_EQ(RETIRED.queued + RETIRED.issued + RETIRED.completing, 0U);
        guards = ker::mod::sys::block_test_irqsave_acquisitions;
        block_queue_service(&native.device);
        EXPECT_EQ(ker::mod::sys::block_test_irqsave_acquisitions - guards, EMPTY_GUARDS);
        EXPECT_EQ(callbacks.load(), CALLBACKS + COUNT);
        EXPECT_EQ(caller.references.load(), 1U);
        EXPECT_EQ(caller.reclaim_no_io_depth, 0U);
        EXPECT_EQ(ker::mod::sched::block_test_shutdown_holds.load(), 0U);
        for (auto* request : batch) {
            EXPECT_TRUE(request->completed.load(std::memory_order_acquire));
            EXPECT_EQ(block_request_release(*request), 0);
        }
        native.auto_complete = false;
        native.service_hook = {};
    }
    ker::mod::sched::block_test_current_task = nullptr;
}

TEST_F(BlockRequestRuntime, FirstGatherCallbackAdmissionKeepsIssueAndRequiredPost) {
    auto& first = owner();
    auto& admitted = owner();
    struct Context {
        BlockRequestRuntime* fixture;
        BlockRequest* admitted;
    } context{this, &admitted};
    BlockSegment const SEGMENT{buffers[0].data(), 512};
    ASSERT_EQ(block_request_submit(
                  &native.device, first, BlockRequestOperation::READ, 0, 1, {&SEGMENT, 1}, 1000,
                  [](BlockRequest& request, void* argument) {
                      auto& context = *static_cast<Context*>(argument);
                      callback(request, context.fixture);
                      native.auto_complete = false;
                      EXPECT_EQ(context.fixture->submit(*context.admitted, BlockRequestOperation::WRITE), 0);
                  },
                  &context),
              0);
    block_queue_service(&native.device);
    native.auto_complete = true;
    size_t const BEFORE = native.service_calls;
    block_queue_service(&native.device);
    EXPECT_EQ(native.service_calls - BEFORE, 2U);
    EXPECT_TRUE(first.completed.load());
    EXPECT_FALSE(admitted.completed.load());
    EXPECT_EQ(snapshot(native.device).issued, 1U);
    ASSERT_TRUE(native.retire(admitted.token));
    block_queue_service(&native.device);
    EXPECT_TRUE(admitted.completed.load());
    EXPECT_EQ(callbacks.load(), 2U);
    EXPECT_EQ(block_request_release(first), 0);
    EXPECT_EQ(block_request_release(admitted), 0);
}

TEST_F(BlockRequestRuntime, FirstGatherCallbackLateClaimRequiresFinalGatherBeforeRelease) {
    auto& first = owner();
    auto& late = owner();
    struct Context {
        BlockRequestRuntime* fixture;
        BlockRequest* late;
    } context{this, &late};
    BlockSegment const SEGMENT{buffers[0].data(), 512};
    ASSERT_EQ(block_request_submit(
                  &native.device, first, BlockRequestOperation::READ, 0, 1, {&SEGMENT, 1}, 1000,
                  [](BlockRequest& request, void* argument) {
                      auto& context = *static_cast<Context*>(argument);
                      callback(request, context.fixture);
                      EXPECT_TRUE(native.retire(context.late->token, 0, SIZE_MAX, true));
                      EXPECT_FALSE(context.late->completed.load());
                      EXPECT_EQ(block_request_release(*context.late), -EBUSY);
                  },
                  &context),
              0);
    ASSERT_EQ(submit(late, BlockRequestOperation::READ, 1), 0);
    block_queue_service(&native.device);
    ASSERT_TRUE(native.retire(first.token));
    size_t const BEFORE = native.service_calls;
    block_queue_service(&native.device);
    EXPECT_EQ(native.service_calls, BEFORE + 1);
    EXPECT_TRUE(first.completed.load());
    EXPECT_TRUE(late.completed.load());
    EXPECT_EQ(callbacks.load(), 2U);
    EXPECT_EQ(snapshot(native.device).completing, 0U);
    EXPECT_EQ(block_request_release(first), 0);
    EXPECT_EQ(block_request_release(late), 0);
}

TEST_F(BlockRequestRuntime, EveryActualSubmitAttemptRetainsPostIncludingRejection) {
    for (int const RESULT : {0, -EAGAIN, -EIO}) {
        auto& request = owner();
        ASSERT_EQ(submit(request), 0);
        native.reject_submissions = RESULT == 0 ? 0 : 1;
        native.rejection = RESULT;
        size_t const BEFORE = native.service_calls;
        block_queue_service(&native.device);
        EXPECT_EQ(native.service_calls - BEFORE, 2U);
        if (RESULT == -EIO) {
            EXPECT_TRUE(request.completed.load(std::memory_order_acquire));
            EXPECT_EQ(request.status, -EIO);
        } else {
            EXPECT_FALSE(request.completed.load(std::memory_order_acquire));
            if (RESULT == -EAGAIN) {
                EXPECT_EQ(snapshot(native.device).queued, 1U);
                native.rejection = -EAGAIN;
                block_queue_service(&native.device);
                EXPECT_EQ(native.service_calls - BEFORE, 4U);
            }
            ASSERT_TRUE(native.retire(request.token));
            block_queue_service(&native.device);
            EXPECT_TRUE(request.completed.load(std::memory_order_acquire));
            EXPECT_EQ(request.status, 0);
        }
        EXPECT_EQ(block_request_release(request), 0);
    }
    EXPECT_EQ(callbacks.load(), 3U);
}

TEST_F(BlockRequestRuntime, PreResetCompletionRetiresCallbackThenPostReopensAutomatically) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    block_queue_service(&native.device);
    auto const BEFORE = snapshot(native.device);
    size_t const PRE = native.service_calls + 1;
    unsigned post_reopens = 0;
    native.service_hook = [&](size_t calls) {
        if (calls == PRE) {
            EXPECT_EQ(block_queue_begin_reset(&native.device, -EIO), 0);
            EXPECT_TRUE(native.retire(request.token, -EIO, 0));
            EXPECT_EQ(block_queue_finish_reset(&native.device), -EBUSY);
            EXPECT_FALSE(request.completed.load(std::memory_order_acquire));
        } else if (calls == PRE + 1) {
            EXPECT_TRUE(request.completed.load(std::memory_order_acquire));
            EXPECT_EQ(callbacks.load(), 1U);
            EXPECT_EQ(block_queue_finish_reset(&native.device), 0);
            ++post_reopens;
        }
    };
    block_queue_service(&native.device);
    EXPECT_EQ(post_reopens, 1U);
    EXPECT_EQ(native.service_calls, PRE + 1);
    EXPECT_TRUE(snapshot(native.device).admission_open);
    EXPECT_EQ(snapshot(native.device).generation, BEFORE.generation + 1);
    EXPECT_EQ(request.status, -EIO);
    EXPECT_EQ(block_request_release(request), 0);
}

TEST_F(BlockRequestRuntime, CompletedResetInPreKeepsPostThroughGenerationChange) {
    auto const GENERATION = snapshot(native.device).generation;
    native.service_hook = [&](size_t calls) {
        if (calls == 1) {
            EXPECT_EQ(block_queue_begin_reset(&native.device), 0);
            EXPECT_EQ(block_queue_finish_reset(&native.device), 0);
        }
    };
    block_queue_service(&native.device);
    EXPECT_EQ(native.service_calls, 2U);
    EXPECT_TRUE(snapshot(native.device).admission_open);
    EXPECT_EQ(snapshot(native.device).generation, GENERATION + 1);
}

TEST_F(BlockRequestRuntime, CallbackResetKeepsPostUntilCallbackRetirementAndEpochReopen) {
    auto& request = owner();
    BlockSegment const SEGMENT{buffers[0].data(), 512};
    ASSERT_EQ(block_request_submit(&native.device, request, BlockRequestOperation::READ, 0, 1, {&SEGMENT, 1}, 1000,
                                   [](BlockRequest&, void*) {
                                       EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
                                       EXPECT_EQ(block_queue_begin_reset(&native.device), 0);
                                       EXPECT_EQ(block_queue_finish_reset(&native.device), -EBUSY);
                                   }),
              0);
    block_queue_service(&native.device);
    ASSERT_TRUE(native.retire(request.token));
    size_t const PRE = native.service_calls + 1;
    unsigned reopens = 0;
    native.service_hook = [&](size_t calls) {
        if (calls == PRE + 1) {
            EXPECT_TRUE(request.completed.load(std::memory_order_acquire));
            EXPECT_EQ(block_queue_finish_reset(&native.device), 0);
            ++reopens;
        }
    };
    block_queue_service(&native.device);
    EXPECT_EQ(reopens, 1U);
    EXPECT_EQ(native.service_calls, PRE + 1);
    EXPECT_TRUE(snapshot(native.device).admission_open);
    EXPECT_EQ(block_request_release(request), 0);
}

TEST_F(BlockRequestRuntime, CallbackCloseKeepsPostAndRetiredOwnerReleasable) {
    auto& provider = callback_closed_native;
    auto& request = owner();
    BlockSegment const SEGMENT{buffers[0].data(), 512};
    ASSERT_EQ(block_request_submit(
                  &provider.device, request, BlockRequestOperation::READ, 0, 1, {&SEGMENT, 1}, 1000,
                  [](BlockRequest&, void* disk) { block_queue_close(static_cast<BlockDevice*>(disk)); }, &provider.device),
              0);
    block_queue_service(&provider.device);
    ASSERT_TRUE(provider.retire(request.token));
    size_t const BEFORE = provider.service_calls;
    block_queue_service(&provider.device);
    EXPECT_EQ(provider.service_calls - BEFORE, 2U);
    EXPECT_TRUE(request.completed.load(std::memory_order_acquire));
    EXPECT_FALSE(snapshot(provider.device).admission_open);
    EXPECT_EQ(block_request_release(request), 0);
}

TEST_F(BlockRequestRuntime, CallbackCancelOfAnotherIssuedOwnerKeepsPostAndLaterRetiresOnce) {
    auto& first = owner();
    auto& canceled = owner();
    BlockSegment const SEGMENT{buffers[0].data(), 512};
    ASSERT_EQ(block_request_submit(
                  &native.device, first, BlockRequestOperation::READ, 0, 1, {&SEGMENT, 1}, 1000,
                  [](BlockRequest&, void* other) { EXPECT_EQ(block_request_cancel(*static_cast<BlockRequest*>(other)), 0); }, &canceled),
              0);
    ASSERT_EQ(submit(canceled, BlockRequestOperation::READ, 1), 0);
    block_queue_service(&native.device);
    ASSERT_TRUE(native.retire(first.token));
    size_t const BEFORE = native.service_calls;
    block_queue_service(&native.device);
    EXPECT_EQ(native.service_calls - BEFORE, 2U);
    EXPECT_TRUE(first.completed.load(std::memory_order_acquire));
    EXPECT_FALSE(canceled.completed.load(std::memory_order_acquire));
    native.retire_cancel = true;
    block_queue_service(&native.device);
    EXPECT_EQ(native.cancelled.size(), 1U);
    EXPECT_EQ(canceled.status, -ECANCELED);
    EXPECT_EQ(callbacks.load(), 1U);
    EXPECT_EQ(block_request_release(first), 0);
    EXPECT_EQ(block_request_release(canceled), 0);
}

TEST_F(BlockRequestRuntime, LateCompletionStillRetiresInFinalScanWithoutPost) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    block_queue_service(&native.device);
    unsigned guards = 0;
    native.service_hook = [&](size_t calls) {
        if (calls == 3)
            ker::mod::sys::block_test_irqsave_hook = [&] {
                // PRE gather, issue selection, final health guard, final gather.
                if (++guards == 4) {
                    auto const TOKEN = request.token;
                    ker::mod::sys::block_test_irqsave_hook = {};
                    EXPECT_TRUE(native.retire(TOKEN));
                }
            };
    };
    block_queue_service(&native.device);
    ker::mod::sys::block_test_irqsave_hook = {};
    EXPECT_EQ(native.service_calls, 3U);
    EXPECT_EQ(guards, 4U);
    EXPECT_TRUE(request.completed.load(std::memory_order_acquire));
    EXPECT_EQ(callbacks.load(), 1U);
    EXPECT_EQ(block_request_release(request), 0);
}

TEST_F(BlockRequestRuntime, PriorAcceptedBusyStageProgressesOnLaterMandatoryPre) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    native.service_hook = [&](size_t calls) {
        if (calls == 4) native.auto_complete = true;
    };
    block_queue_service(&native.device);
    EXPECT_EQ(native.service_calls, 2U);
    EXPECT_EQ(snapshot(native.device).issued, 1U);
    block_queue_service(&native.device);
    EXPECT_EQ(native.service_calls, 3U);
    EXPECT_FALSE(request.completed.load(std::memory_order_acquire));
    block_queue_service(&native.device);
    EXPECT_EQ(native.service_calls, 4U);
    EXPECT_TRUE(request.completed.load(std::memory_order_acquire));
    EXPECT_EQ(callbacks.load(), 1U);
    EXPECT_EQ(block_request_release(request), 0);
}

TEST_F(BlockRequestRuntime, NativeFastCompletionFinishesDuringExtraPassBeforePark) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    native.service_hook = [&](size_t calls) {
        if (calls == 3) native.auto_complete = true;
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause, true), 0);
    EXPECT_EQ(first_pause, SIZE_MAX);
    EXPECT_EQ(ker::mod::sched::block_test_parks.load(), 0U);
    EXPECT_EQ(native.service_calls, 3U);  // Initial PRE/POST and one healthy extra PRE.
    EXPECT_EQ(callbacks.load(), 1U);
    EXPECT_EQ(snapshot(native.device).issued, 0U);
}

TEST_F(BlockRequestRuntime, ActivePollTrueSkipsOnlyExtraPreAndKeeps32PassBound) {
    auto& first = owner();
    ASSERT_EQ(submit(first, BlockRequestOperation::READ, 0, &polled_native.device), 0);
    auto& second = owner();
    ASSERT_EQ(submit(second, BlockRequestOperation::READ, 1, &polled_native.device), 0);
    polled_native.poll_hook = [&](size_t) {
        EXPECT_EQ(polled_native.service_calls, 2U);  // Mandatory PRE and publication POST.
        EXPECT_FALSE(first.completed.load());
        EXPECT_FALSE(second.completed.load());
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(first, first_pause, true, polled_native), 0);
    EXPECT_EQ(first_pause, 2U);
    EXPECT_EQ(polled_native.poll_calls, 32U);
    EXPECT_EQ(polled_native.service_calls, 3U);  // Next mandatory PRE retires both.
    EXPECT_EQ(callbacks.load(), 2U);
    EXPECT_TRUE(second.completed.load());
    EXPECT_EQ(block_request_wait(second), 0);
}

TEST_F(BlockRequestRuntime, ActivePollFalseFallsBackToFreshFullService) {
    auto& request = owner();
    ASSERT_EQ(submit(request, BlockRequestOperation::READ, 0, &polled_native.device), 0);
    polled_native.poll_result = false;
    polled_native.service_hook = [&](size_t calls) {
        if (calls == 3) polled_native.auto_complete = true;
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause, false, polled_native), 0);
    EXPECT_EQ(first_pause, SIZE_MAX);
    EXPECT_EQ(polled_native.poll_calls, 1U);
    EXPECT_EQ(polled_native.service_calls, 3U);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, ActivePollNeverReplacesExplicitMandatoryService) {
    auto& request = owner();
    ASSERT_EQ(submit(request, BlockRequestOperation::READ, 0, &polled_native.device), 0);
    block_queue_service(&polled_native.device);
    block_queue_service(&polled_native.device);
    EXPECT_EQ(polled_native.service_calls, 3U);
    EXPECT_EQ(polled_native.poll_calls, 0U);
    polled_native.auto_complete = true;
    block_queue_service(&polled_native.device);
    EXPECT_TRUE(request.completed.load());
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, ActivePollQueuedSubmitStillRequiresFullPostForEveryResult) {
    for (int const result : {0, -EAGAIN, -EIO}) {
        polled_native.reset_controls();
        auto& first = owner();
        ASSERT_EQ(submit(first, BlockRequestOperation::READ, 0, &polled_native.device), 0);
        auto& queued = owner();
        polled_native.poll_hook = [&](size_t calls) {
            if (calls != 1) return;
            ASSERT_EQ(submit(queued, BlockRequestOperation::READ, 1, &polled_native.device), 0);
            polled_native.reject_submissions = result == 0 ? 0 : 1;
            polled_native.rejection = result;
        };
        polled_native.service_hook = [&](size_t calls) {
            if (calls == 3) polled_native.auto_complete = true;
        };
        size_t first_pause = SIZE_MAX;
        EXPECT_EQ(wait_polled(first, first_pause, false, polled_native), 0);
        EXPECT_EQ(first_pause, SIZE_MAX);
        EXPECT_EQ(polled_native.poll_calls, 1U);
        EXPECT_EQ(polled_native.service_calls, 3U);  // The submit attempt forced full POST.
        polled_native.poll_hook = {};
        EXPECT_EQ(block_request_wait(queued), result == -EIO ? -EIO : 0);
        EXPECT_EQ(block_request_release(first), 0);
        EXPECT_EQ(block_request_release(queued), 0);
    }
    EXPECT_EQ(callbacks.load(), 6U);
}

TEST_F(BlockRequestRuntime, ActivePollEntryCancelRetainsFullPreAndPost) {
    auto& first = owner();
    ASSERT_EQ(submit(first, BlockRequestOperation::READ, 0, &polled_native.device), 0);
    auto& canceled = owner();
    ASSERT_EQ(submit(canceled, BlockRequestOperation::READ, 1, &polled_native.device), 0);
    polled_native.retire_cancel = true;
    polled_native.service_hook = [&](size_t calls) {
        if (calls == 2) EXPECT_EQ(block_request_cancel(canceled), 0);
        if (calls == 4) polled_native.auto_complete = true;
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(first, first_pause, false, polled_native), 0);
    EXPECT_EQ(first_pause, SIZE_MAX);
    EXPECT_EQ(polled_native.poll_calls, 0U);
    EXPECT_EQ(polled_native.service_calls, 4U);
    EXPECT_EQ(canceled.status, -ECANCELED);
    EXPECT_TRUE(canceled.completed.load());
    EXPECT_EQ(callbacks.load(), 2U);
}

TEST_F(BlockRequestRuntime, ActivePollLifecycleRaceRetainsRequiredFullPost) {
    for (unsigned event = 0; event < 3; ++event) {
        auto& provider = event == 2 ? polled_closed_native : polled_native;
        provider.reset_controls();
        auto& request = owner();
        ASSERT_EQ(submit(request, BlockRequestOperation::READ, 0, &provider.device), 0);
        provider.poll_hook = [&](size_t calls) {
            if (calls != 1) return;
            std::thread external([&] {
                if (event == 0) EXPECT_EQ(block_request_cancel(request), 0);
                if (event == 1) EXPECT_EQ(block_queue_begin_reset(&provider.device), 0);
                if (event == 2) block_queue_close(&provider.device);
            });
            external.join();
        };
        provider.service_hook = [&](size_t calls) {
            if (calls == 3) provider.auto_complete = true;
        };
        size_t first_pause = SIZE_MAX;
        int const expected = event == 0 ? -ECANCELED : event == 1 ? -EIO : 0;
        EXPECT_EQ(wait_polled(request, first_pause, false, provider), expected);
        EXPECT_EQ(first_pause, SIZE_MAX);
        EXPECT_EQ(provider.poll_calls, 1U);
        EXPECT_EQ(provider.service_calls, 3U);
        if (event == 1) EXPECT_EQ(block_queue_finish_reset(&provider.device), 0);
        EXPECT_EQ(block_request_release(request), 0);
    }
    EXPECT_EQ(callbacks.load(), 3U);
}

TEST_F(BlockRequestRuntime, ActivePollKeepsBothGathersForExternalCompletion) {
    auto& first = owner();
    ASSERT_EQ(submit(first, BlockRequestOperation::READ, 0, &polled_native.device), 0);
    auto& late = owner();
    ASSERT_EQ(submit(late, BlockRequestOperation::READ, 1, &polled_native.device), 0);
    bool late_claimed = false;
    polled_native.poll_hook = [&](size_t calls) {
        if (calls != 1) return;
        std::thread external([&] { EXPECT_TRUE(polled_native.retire(first.token)); });
        external.join();
        ker::mod::sys::block_test_irqsave_hook = [&] {
            if (!first.completed.load() || late_claimed) return;
            late_claimed = true;
            std::thread later([&] { EXPECT_TRUE(polled_native.retire(late.token)); });
            later.join();
        };
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(first, first_pause, false, polled_native), 0);
    ker::mod::sys::block_test_irqsave_hook = {};
    EXPECT_TRUE(late_claimed);
    EXPECT_TRUE(late.completed.load());
    EXPECT_EQ(callbacks.load(), 2U);
    EXPECT_EQ(polled_native.poll_calls, 1U);
    EXPECT_EQ(polled_native.service_calls, 2U);
    EXPECT_EQ(first_pause, SIZE_MAX);
}

TEST_F(BlockRequestRuntime, ActivePollGenerationChangeAfterGatherRetainsFullPost) {
    auto& request = owner();
    ASSERT_EQ(submit(request, BlockRequestOperation::READ, 0, &polled_native.device), 0);
    uint64_t const generation = snapshot(polled_native.device).generation;
    bool advanced = false;
    polled_native.poll_hook = [&](size_t calls) {
        if (calls != 1) return;
        std::thread external([&] { EXPECT_TRUE(polled_native.retire(request.token)); });
        external.join();
        ker::mod::sys::block_test_irqsave_hook = [&] {
            if (!request.completed.load() || advanced) return;
            advanced = true;
            EXPECT_EQ(block_queue_begin_reset(&polled_native.device), 0);
            EXPECT_EQ(block_queue_finish_reset(&polled_native.device), 0);
        };
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause, false, polled_native), 0);
    ker::mod::sys::block_test_irqsave_hook = {};
    EXPECT_TRUE(advanced);
    EXPECT_EQ(snapshot(polled_native.device).generation, generation + 1);
    EXPECT_EQ(polled_native.poll_calls, 1U);
    EXPECT_EQ(polled_native.service_calls, 3U);
    EXPECT_EQ(first_pause, SIZE_MAX);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, FrozenClockStillCapsExtraFullPassesAt32) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause), 0);
    EXPECT_EQ(first_pause, 34U);  // Mandatory PRE/POST plus exactly32 healthy extra PREs.
    EXPECT_EQ(ker::mod::sched::block_test_parks.load(), 1U);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, ElapsedAdmissionBudgetIncludesInitialServiceAndStopsAt128us) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    native.service_hook = [&](size_t) { ker::mod::time::block_test_now_us.fetch_add(16); };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause), 0);
    EXPECT_EQ(first_pause, 8U);  // Initial32us plus six16us healthy extra passes.
    EXPECT_EQ(ker::mod::sched::block_test_parks.load(), 1U);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, SlowInitialServiceAdmitsNoExtraPass) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    native.service_hook = [&](size_t calls) {
        if (calls == 1) ker::mod::time::block_test_now_us.fetch_add(128);
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause), 0);
    EXPECT_EQ(first_pause, 2U);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, QualificationGuardDelayConsumesBudgetBeforeAnotherServiceStarts) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    unsigned guards_after_post_service = 0;
    native.service_hook = [&](size_t calls) {
        if (calls == 2) {
            ker::mod::sys::block_test_irqsave_hook = [&] {
                // First is the mandatory pass's empty completion gather.
                // Second is the actual per-request polling qualification.
                if (++guards_after_post_service == 2) ker::mod::time::block_test_now_us.fetch_add(128);
            };
        }
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause), 0);
    EXPECT_EQ(first_pause, 2U);
    EXPECT_GE(guards_after_post_service, 2U);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, ClockRegressionAboveOriginalStartStillStopsExtraPasses) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    native.service_hook = [&](size_t calls) {
        if (calls == 2) ker::mod::time::block_test_now_us.store(1050);
        if (calls == 4) ker::mod::time::block_test_now_us.store(1040);
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause), 0);
    EXPECT_EQ(first_pause, 4U);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, UnrelatedCompletionsDoNotRenewTheAdmissionBudget) {
    auto& target = owner();
    ASSERT_EQ(submit(target), 0);
    auto& unrelated = owner();
    ASSERT_EQ(submit(unrelated, BlockRequestOperation::READ, 1), 0);
    native.service_hook = [&](size_t calls) {
        ker::mod::time::block_test_now_us.fetch_add(16);
        if (calls == 3) EXPECT_TRUE(native.retire(unrelated.token));
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(target, first_pause), 0);
    EXPECT_EQ(first_pause, 8U);
    EXPECT_TRUE(unrelated.completed.load(std::memory_order_acquire));
    EXPECT_EQ(callbacks.load(), 2U);
}

TEST_F(BlockRequestRuntime, SignalArrivingDuringInitialServiceUsesOriginalSafeYieldImmediately) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    native.service_hook = [&](size_t calls) {
        if (calls == 2) ker::mod::sched::get_current_task()->interrupting_signal_pending = true;
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause, true), 0);
    EXPECT_EQ(first_pause, 2U);
    EXPECT_EQ(ker::mod::sched::block_test_parks.load(), 0U);
    EXPECT_EQ(ker::mod::sched::block_test_yields.load(), 1U);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, SignalArrivingDuringExtraPassStopsFurtherPollingAndKeepsReturnYield) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    native.service_hook = [&](size_t calls) {
        if (calls == 4) ker::mod::sched::get_current_task()->interrupting_signal_pending = true;
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause, true), 0);
    EXPECT_EQ(first_pause, 4U);
    EXPECT_EQ(ker::mod::sched::block_test_parks.load(), 0U);
    EXPECT_EQ(ker::mod::sched::block_test_yields.load(), 1U);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, CancelAndResetDuringExtraPassStopFurtherPollingWithoutEscapingOwnership) {
    for (bool reset : {false, true}) {
        auto& request = owner();
        ASSERT_EQ(submit(request), 0);
        native.retire_cancel = true;
        native.auto_complete = false;
        size_t const base = native.service_calls;
        native.service_hook = [&, base, reset](size_t calls) {
            if (calls == base + 4) {
                if (reset)
                    EXPECT_EQ(block_queue_begin_reset(&native.device), 0);
                else
                    EXPECT_EQ(block_request_cancel(request), 0);
            }
        };
        size_t first_pause = SIZE_MAX;
        EXPECT_EQ(wait_polled(request, first_pause), reset ? -EIO : -ECANCELED);
        EXPECT_EQ(first_pause, base + 5);  // The fresh cancellation/reset retains POST.
        EXPECT_TRUE(request.completed.load(std::memory_order_acquire));
    }
    EXPECT_EQ(native.cancelled.size(), 2U);
    EXPECT_EQ(callbacks.load(), 2U);
}

TEST_F(BlockRequestRuntime, ExpiredDeadlineSkipsExtraServiceAndRetiresThroughOriginalCancel) {
    auto& request = owner();
    ASSERT_EQ(submit(request, BlockRequestOperation::READ, 0, &native.device, 100), 0);
    native.retire_cancel = true;
    native.service_hook = [&](size_t calls) {
        if (calls == 2) ker::mod::time::block_test_now_us.store(request.deadline_us);
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause), -ETIMEDOUT);
    EXPECT_EQ(first_pause, 2U);
    EXPECT_EQ(native.cancelled.size(), 1U);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, CancellationDuringInitialServiceStopsExtrasAndKeepsTransportRetirement) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    native.retire_cancel = true;
    native.service_hook = [&](size_t calls) {
        if (calls == 2) EXPECT_EQ(block_request_cancel(request), 0);
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause), -ECANCELED);
    EXPECT_EQ(first_pause, 2U);
    EXPECT_EQ(native.cancelled.size(), 1U);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, ResetDuringInitialServiceStopsExtrasAndRetiresTransportOnce) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    native.retire_cancel = true;
    native.service_hook = [&](size_t calls) {
        if (calls == 2) EXPECT_EQ(block_queue_begin_reset(&native.device), 0);
    };
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause), -EIO);
    EXPECT_EQ(first_pause, 2U);
    EXPECT_EQ(native.cancelled.size(), 1U);
    EXPECT_EQ(callbacks.load(), 1U);
    EXPECT_FALSE(snapshot(native.device).admission_open);
}

TEST_F(BlockRequestRuntime, DeferredQueuedProviderAdmissionSkipsExtraService) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    native.reject_submissions = 1;
    size_t first_pause = SIZE_MAX;
    EXPECT_EQ(wait_polled(request, first_pause), 0);
    EXPECT_EQ(first_pause, 2U);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, CompletionGuardCostDoesNotScaleWithEmptySlots) {
    for (size_t const COUNT : {size_t{0}, size_t{1}, size_t{8}}) {
        std::vector<BlockRequest*> batch;
        size_t const CALLBACKS = callbacks.load();
        for (size_t i = 0; i < COUNT; ++i) {
            auto& request = owner();
            ASSERT_EQ(submit(request, BlockRequestOperation::READ, i), 0);
            batch.push_back(&request);
        }
        block_queue_service(&native.device);
        for (auto* request : batch) ASSERT_TRUE(native.retire(request->token));
        size_t const BEFORE = ker::mod::sys::block_test_irqsave_acquisitions;
        block_queue_service(&native.device);
        size_t const GUARDS = ker::mod::sys::block_test_irqsave_acquisitions - BEFORE;
        RecordProperty("guard_count_" + std::to_string(COUNT), static_cast<int>(GUARDS));
        // Count the entire actual service path, including registry, deadlines,
        // issue selection and completion publication. Empty capacity should
        // not incur a guard per slot; real completions keep their owner guard.
        EXPECT_LE(GUARDS, 8U + COUNT);
        EXPECT_EQ(callbacks.load(), CALLBACKS + COUNT);
        for (auto* request : batch) {
            EXPECT_TRUE(request->completed.load(std::memory_order_acquire));
            EXPECT_EQ(request->status, 0);
            EXPECT_EQ(request->bytes, 512U);
            EXPECT_EQ(block_request_release(*request), 0);
        }
        EXPECT_EQ(snapshot(native.device).owned_requests, 0U);
    }
}

TEST_F(BlockRequestRuntime, GatheredOwnersSurviveCallbackResetAndLateTransportCompletion) {
    Gate first_callback;
    struct Context {
        Gate* first;
        BlockRequest* first_request;
        std::vector<uint16_t> order;
    } context{&first_callback, nullptr, {}};
    std::array<BlockRequest*, 3> batch{};
    for (size_t i = 0; i < batch.size(); ++i) {
        auto& request = owner();
        batch[i] = &request;
        if (i == 0) context.first_request = &request;
        BlockSegment const SEGMENT{buffers[i].data(), 512};
        auto const CALLBACK = [](BlockRequest& request, void* argument) {
            auto& context = *static_cast<Context*>(argument);
            ASSERT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
            EXPECT_FALSE(request.completed.load(std::memory_order_acquire));
            context.order.push_back(request.token.slot);
            if (&request == context.first_request) context.first->enter_and_wait();
        };
        ASSERT_EQ(block_request_submit(&native.device, request, BlockRequestOperation::READ, i, 1, {&SEGMENT, 1}, 1000, CALLBACK, &context),
                  0);
    }
    block_queue_service(&native.device);
    ASSERT_TRUE(native.retire(batch[0]->token));
    ASSERT_TRUE(native.retire(batch[1]->token));
    std::thread service([] { block_queue_service(&native.device); });
    bool const ENTERED = first_callback.await_entered();
    EXPECT_TRUE(ENTERED);
    if (ENTERED) {
        EXPECT_EQ(block_request_release(*batch[0]), -EBUSY);
        EXPECT_EQ(block_request_release(*batch[1]), -EBUSY);
        EXPECT_EQ(block_queue_begin_reset(&native.device, -EIO), 0);
        // The transport retires after the bounded gather, while callback 0
        // owns the service mutex. The late claim must survive to the next pass.
        EXPECT_TRUE(native.retire(batch[2]->token));
        EXPECT_EQ(block_queue_finish_reset(&native.device), -EBUSY);
    }
    first_callback.release();
    service.join();
    if (!ENTERED) return;  // Fixture teardown safely cancels any issued owner.
    ASSERT_EQ(context.order.size(), batch.size());
    for (size_t i = 0; i < batch.size(); ++i) {
        EXPECT_EQ(context.order[i], batch[i]->token.slot);
        EXPECT_TRUE(batch[i]->completed.load(std::memory_order_acquire));
        EXPECT_EQ(batch[i]->status, i == 2 ? -EIO : 0);
        EXPECT_EQ(batch[i]->bytes, i == 2 ? 0U : 512U);
        EXPECT_EQ(block_request_release(*batch[i]), 0);
    }
    EXPECT_EQ(block_queue_finish_reset(&native.device), 0);
    EXPECT_EQ(snapshot(native.device).owned_requests, 0U);
}

TEST_F(BlockRequestRuntime, CallerDrivenCompletionSkipsOnlyItsCurrentWaiterWake) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    native.auto_complete = true;
    ker::mod::sched::task::Task task;
    ker::mod::sched::block_test_current_task = &task;
    unsigned const WAKES = ker::mod::sched::block_test_wakes.load();
    unsigned const SELF_WAKES = ker::mod::sched::block_test_self_wakes.load();
    EXPECT_EQ(block_request_wait(request), 0);
    unsigned const DELTA = ker::mod::sched::block_test_wakes.load() - WAKES;
    unsigned const SELF_DELTA = ker::mod::sched::block_test_self_wakes.load() - SELF_WAKES;
    RecordProperty("caller_completion_wakes", static_cast<int>(DELTA));
    RecordProperty("caller_completion_self_wakes", static_cast<int>(SELF_DELTA));
    EXPECT_EQ(DELTA, 0U);
    EXPECT_EQ(SELF_DELTA, 0U);
    EXPECT_EQ(task.references.load(), 1U);
    ker::mod::sched::block_test_current_task = nullptr;
    EXPECT_EQ(callbacks.load(), 1U);
    EXPECT_EQ(block_request_release(request), 0);
}

TEST_F(BlockRequestRuntime, PublishedGatheredOwnerCanBeDestroyedDuringNextCallback) {
    auto& first = owner();
    ASSERT_EQ(submit(first, BlockRequestOperation::READ, 0), 0);
    auto& second = owner();
    Gate second_callback;
    struct Context {
        Gate* gate;
        BlockRequestRuntime* fixture;
    } context{&second_callback, this};
    BlockSegment const SEGMENT{buffers[1].data(), 512};
    ASSERT_EQ(block_request_submit(
                  &native.device, second, BlockRequestOperation::READ, 1, 1, {&SEGMENT, 1}, 1000,
                  [](BlockRequest& request, void* argument) {
                      auto& context = *static_cast<Context*>(argument);
                      BlockRequestRuntime::callback(request, context.fixture);
                      context.gate->enter_and_wait();
                  },
                  &context),
              0);
    block_queue_service(&native.device);
    ASSERT_TRUE(native.retire(first.token));
    ASSERT_TRUE(native.retire(second.token));
    std::thread service([] { block_queue_service(&native.device); });
    bool const ENTERED = second_callback.await_entered();
    EXPECT_TRUE(ENTERED);
    if (ENTERED) {
        EXPECT_TRUE(first.completed.load(std::memory_order_acquire));
        int const RELEASE = block_request_release(first);
        EXPECT_EQ(RELEASE, 0);
        if (RELEASE == 0) owners.erase(owners.begin());  // ASan poisons the first gathered descriptor.
        EXPECT_FALSE(second.completed.load(std::memory_order_acquire));
        EXPECT_EQ(snapshot(native.device).owned_requests, 1U);
    }
    second_callback.release();
    service.join();
    EXPECT_TRUE(second.completed.load(std::memory_order_acquire));
    EXPECT_EQ(callbacks.load(), 2U);
    EXPECT_EQ(block_request_release(second), 0);
}

TEST_F(BlockRequestRuntime, BoundsAndRejectedSubmissionsTakeNoOwnership) {
    auto& request = owner();
    BlockSegment segment{buffers[0].data(), 512};
    EXPECT_EQ(block_request_submit(&native.device, request, BlockRequestOperation::READ, 128, 1, {&segment, 1}), -EINVAL);
    EXPECT_EQ(block_request_submit(&native.device, request, BlockRequestOperation::READ, 0, 2, {&segment, 1}), -E2BIG);
    EXPECT_EQ(block_request_submit(&native.device, request, BlockRequestOperation::READ, 0, 1, {}), -EINVAL);
    segment.size = 511;
    EXPECT_EQ(block_request_submit(&native.device, request, BlockRequestOperation::READ, 0, 1, {&segment, 1}), -EINVAL);
    segment = {nullptr, 512};
    EXPECT_EQ(block_request_submit(&native.device, request, BlockRequestOperation::READ, 0, 1, {&segment, 1}), -EFAULT);
    EXPECT_EQ(request.queue, nullptr);
    native.device.read_only = true;
    EXPECT_EQ(submit(request, BlockRequestOperation::WRITE), -EROFS);
    EXPECT_EQ(snapshot(native.device).owned_requests, 0U);
    EXPECT_TRUE(native.issued.empty());
}

TEST_F(BlockRequestRuntime, PartitionAliasesShareOrderingAndTranslatePhysicalLba) {
    BlockDevice first{};
    first.is_partition = true;
    first.parent_disk = &native.device;
    first.block_size = 512;
    first.total_blocks = 4;
    first.partition_start_lba = 10;
    BlockDevice second = first;
    second.partition_start_lba = 11;
    auto& write = owner();
    ASSERT_EQ(submit(write, BlockRequestOperation::WRITE, 1, &first), 0);
    auto& read = owner();
    ASSERT_EQ(submit(read, BlockRequestOperation::READ, 0, &second), 0);
    EXPECT_EQ(write.description.lba, 11U);
    EXPECT_EQ(read.description.lba, 11U);
    block_queue_service(&first);
    ASSERT_EQ(native.issued.size(), 1U);
    ASSERT_TRUE(native.retire(write.token));
    block_queue_service(&second);
    EXPECT_TRUE(write.completed.load(std::memory_order_acquire));
    ASSERT_EQ(native.issued.size(), 2U);
    ASSERT_TRUE(native.retire(read.token));
    block_queue_service(&native.device);
    EXPECT_EQ(callbacks.load(), 2U);
    auto& invalid = owner();
    EXPECT_EQ(submit(invalid, BlockRequestOperation::READ, 4, &first), -EINVAL);
    first.partition_start_lba = UINT64_MAX;
    EXPECT_EQ(submit(invalid, BlockRequestOperation::READ, 0, &first), -EINVAL);
}

TEST_F(BlockRequestRuntime, QueueDepthAndBudgetIncludeCompletedUnreleasedOwners) {
    for (size_t i = 0; i < 8; ++i) ASSERT_EQ(submit(owner(), BlockRequestOperation::READ, i), 0);
    auto& rejected = owner();
    EXPECT_EQ(submit(rejected), -EAGAIN);
    block_queue_service(&native.device);
    EXPECT_EQ(native.issued.size(), 8U);
    auto& first = *owners.front();
    ASSERT_TRUE(native.retire(first.token));
    block_queue_service(&native.device);
    EXPECT_EQ(submit(rejected), -EAGAIN);
    ASSERT_EQ(block_request_release(first), 0);
    EXPECT_EQ(submit(rejected), 0);
    auto state = snapshot(native.device);
    EXPECT_EQ(state.owned_requests, 8U);
    EXPECT_EQ(state.owned_bytes, 4096U);
}

TEST_F(BlockRequestRuntime, DuplicateCompletionPublishesOneCallbackAndOwnerRelease) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    block_queue_service(&native.device);
    EXPECT_EQ(block_request_release(request), -EBUSY);
    ASSERT_TRUE(native.retire(request.token));
    EXPECT_FALSE(request.completed.load(std::memory_order_acquire));
    EXPECT_FALSE(block_request_complete(&native.device, request.token, -EIO, 0));
    block_queue_service(&native.device);
    EXPECT_EQ(block_request_wait(request), 0);
    EXPECT_EQ(request.bytes, 512U);
    EXPECT_EQ(callbacks.load(), 1U);
    ASSERT_EQ(block_request_release(request), 0);
    EXPECT_EQ(block_request_release(request), -EINVAL);
}

TEST_F(BlockRequestRuntime, QueuedCancellationNeverCallsProvider) {
    auto& request = owner();
    ASSERT_EQ(submit(request, BlockRequestOperation::WRITE), 0);
    ASSERT_EQ(block_request_cancel(request), 0);
    EXPECT_EQ(block_request_release(request), -EBUSY);
    block_queue_service(&native.device);
    EXPECT_TRUE(native.issued.empty());
    EXPECT_EQ(block_request_wait(request), -ECANCELED);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, IssuedTimeoutRetainsBuffersUntilProviderRetires) {
    auto& request = owner();
    ASSERT_EQ(submit(request, BlockRequestOperation::WRITE, 0, &native.device, 100), 0);
    block_queue_service(&native.device);
    ker::mod::time::block_test_now_us.store(1100);
    block_queue_service(&native.device);
    ASSERT_EQ(native.cancelled.size(), 1U);
    EXPECT_FALSE(request.completed.load(std::memory_order_acquire));
    EXPECT_EQ(block_request_release(request), -EBUSY);
    EXPECT_EQ(snapshot(native.device).owned_bytes, 512U);
    block_queue_service(&native.device);
    EXPECT_EQ(native.cancelled.size(), 1U);
    ASSERT_TRUE(native.retire(request.token));
    block_queue_service(&native.device);
    EXPECT_EQ(block_request_wait(request), -ETIMEDOUT);
    EXPECT_EQ(request.bytes, 0U);
    EXPECT_EQ(callbacks.load(), 1U);
}

TEST_F(BlockRequestRuntime, ProviderBackpressureRequeuesAndRacedCancelClaimsSafely) {
    auto& first = owner();
    ASSERT_EQ(submit(first), 0);
    native.reject_submissions = 1;
    block_queue_service(&native.device);
    EXPECT_TRUE(native.issued.empty());
    EXPECT_EQ(snapshot(native.device).queued, 1U);
    block_queue_service(&native.device);
    ASSERT_EQ(native.issued.size(), 1U);
    ASSERT_TRUE(native.retire(first.token));
    block_queue_service(&native.device);
    auto& second = owner();
    ASSERT_EQ(submit(second), 0);
    native.reject_submissions = 1;
    native.reject_hook = [](BlockRequest& request) { EXPECT_EQ(block_request_cancel(request), 0); };
    block_queue_service(&native.device);
    EXPECT_TRUE(second.completed.load(std::memory_order_acquire));
    EXPECT_EQ(second.status, -ECANCELED);
    EXPECT_TRUE(native.cancelled.empty());
    EXPECT_EQ(native.issued.size(), 1U);
}

TEST_F(BlockRequestRuntime, FlushJoinsEarlierTransfersAndBlocksLaterIndependentWork) {
    auto& first = owner();
    ASSERT_EQ(submit(first, BlockRequestOperation::WRITE, 0), 0);
    auto& second = owner();
    ASSERT_EQ(submit(second, BlockRequestOperation::READ, 10), 0);
    auto& flush = owner();
    ASSERT_EQ(submit(flush, BlockRequestOperation::FLUSH), 0);
    auto& later = owner();
    ASSERT_EQ(submit(later, BlockRequestOperation::READ, 20), 0);
    block_queue_service(&native.device);
    ASSERT_EQ(native.issued.size(), 2U);
    ASSERT_TRUE(native.retire(second.token));
    block_queue_service(&native.device);
    EXPECT_EQ(native.issued.size(), 2U);
    ASSERT_TRUE(native.retire(first.token));
    block_queue_service(&native.device);
    ASSERT_EQ(native.issued.size(), 3U);
    EXPECT_EQ(native.issued.back().op, BlockRequestOperation::FLUSH);
    ASSERT_TRUE(native.retire(flush.token, -EIO));
    block_queue_service(&native.device);
    EXPECT_EQ(flush.status, -EIO);
    ASSERT_EQ(native.issued.size(), 4U);
    EXPECT_EQ(native.issued.back().lba, 20U);
}

TEST_F(BlockRequestRuntime, ResetRetiresCallbacksAndOldTerminalOwnersRemainReleasable) {
    auto& active = owner();
    ASSERT_EQ(submit(active, BlockRequestOperation::WRITE), 0);
    auto const OLD = active.token;
    auto& queued = owner();
    ASSERT_EQ(submit(queued), 0);
    block_queue_service(&native.device);
    ASSERT_EQ(block_queue_begin_reset(&native.device), 0);
    EXPECT_EQ(block_queue_finish_reset(&native.device), -EBUSY);
    block_queue_service(&native.device);
    EXPECT_TRUE(queued.completed.load(std::memory_order_acquire));
    EXPECT_FALSE(active.completed.load(std::memory_order_acquire));
    ASSERT_TRUE(native.retire(active.token));
    EXPECT_EQ(block_queue_finish_reset(&native.device), -EBUSY);
    block_queue_service(&native.device);
    ASSERT_EQ(block_queue_finish_reset(&native.device), 0);
    EXPECT_EQ(active.status, -EIO);
    EXPECT_EQ(active.bytes, 0U);
    ASSERT_EQ(block_request_release(active), 0);
    ASSERT_EQ(block_request_release(queued), 0);
    auto& current = owner();
    ASSERT_EQ(submit(current), 0);
    block_queue_service(&native.device);
    EXPECT_EQ(current.token.slot, OLD.slot);
    EXPECT_NE(current.token.queue_generation, OLD.queue_generation);
    EXPECT_FALSE(block_request_complete(&native.device, OLD, 0, 512));
    EXPECT_FALSE(current.completed.load(std::memory_order_acquire));
}

TEST_F(BlockRequestRuntime, CallbackRetirementBlocksDrainAndAdmissionRemainsClosed) {
    auto& request = owner();
    Gate gate;
    BlockSegment segment{buffers[0].data(), 512};
    ASSERT_EQ(block_request_submit(
                  &closing_native.device, request, BlockRequestOperation::READ, 0, 1, {&segment, 1}, 1000,
                  [](BlockRequest&, void* arg) { static_cast<Gate*>(arg)->enter_and_wait(); }, &gate),
              0);
    block_queue_service(&closing_native.device);
    ASSERT_TRUE(closing_native.retire(request.token));
    std::thread service([] { block_queue_service(&closing_native.device); });
    bool const ENTERED = gate.await_entered();
    EXPECT_TRUE(ENTERED);
    if (!ENTERED) {
        gate.release();
        service.join();
        return;
    }
    block_queue_close(&closing_native.device);
    EXPECT_FALSE(request.completed.load(std::memory_order_acquire));
    EXPECT_EQ(block_request_release(request), -EBUSY);
    auto& rejected = owner();
    EXPECT_EQ(submit(rejected, BlockRequestOperation::READ, 0, &closing_native.device), -ENODEV);
    std::atomic<bool> drained{false};
    std::thread drain([&] {
        EXPECT_EQ(block_queue_drain(&closing_native.device), 0);
        drained.store(true, std::memory_order_release);
    });
    EXPECT_FALSE(drained.load(std::memory_order_acquire));
    EXPECT_EQ(snapshot(closing_native.device).completing, 1U);
    gate.release();
    service.join();
    drain.join();
    EXPECT_TRUE(drained.load(std::memory_order_acquire));
    EXPECT_TRUE(request.completed.load(std::memory_order_acquire));
    ASSERT_EQ(block_queue_begin_reset(&closing_native.device), 0);
    ASSERT_EQ(block_queue_finish_reset(&closing_native.device), 0);
    EXPECT_EQ(submit(rejected, BlockRequestOperation::READ, 0, &closing_native.device), -ENODEV);
}

TEST_F(BlockRequestRuntime, ConcurrentCompletionCancelAndClosePublishOneResult) {
    for (size_t i = 0; i < 32; ++i) {
        auto& request = owner();
        ASSERT_EQ(submit(request, BlockRequestOperation::READ, 0, &racing_native.device), 0);
        block_queue_service(&racing_native.device);
        std::barrier start(3);
        std::thread complete([&] {
            start.arrive_and_wait();
            EXPECT_TRUE(racing_native.retire(request.token));
        });
        std::thread cancel([&] {
            start.arrive_and_wait();
            int const RESULT = block_request_cancel(request);
            EXPECT_TRUE(RESULT == 0 || RESULT == -EALREADY);
            if (i == 31) block_queue_close(&racing_native.device);
        });
        start.arrive_and_wait();
        complete.join();
        cancel.join();
        block_queue_service(&racing_native.device);
        EXPECT_TRUE(request.completed.load(std::memory_order_acquire));
        EXPECT_TRUE(request.status == 0 || request.status == -ECANCELED);
        EXPECT_EQ(request.bytes, request.status == 0 ? 512U : 0U);
        ASSERT_EQ(block_request_release(request), 0);
        EXPECT_FALSE(block_request_complete(&racing_native.device, request.token, 0, 512));
        ASSERT_EQ(block_queue_begin_reset(&racing_native.device), 0);
        ASSERT_EQ(block_queue_finish_reset(&racing_native.device), 0);
    }
    EXPECT_EQ(callbacks.load(), 32U);
}

TEST_F(BlockRequestRuntime, LegacyScatterGatherSyncChunkingAndUnsupportedFlush) {
    auto& write = owner();
    buffers[0].fill(std::byte{0x55});
    std::array<BlockSegment, 2> segments{{{buffers[0].data(), 128}, {buffers[0].data() + 128, 384}}};
    ASSERT_EQ(block_request_submit(&legacy, write, BlockRequestOperation::WRITE, 0, 1, segments), 0);
    EXPECT_EQ(block_request_wait(write), 0);
    EXPECT_EQ(legacy_media[511], std::byte{0x55});
    EXPECT_EQ(block_request_release(write), 0);
    std::array<std::byte, 1024> read{};
    EXPECT_EQ(block_request_sync(&legacy, BlockRequestOperation::READ, 0, 2, read.data()), 0);
    EXPECT_EQ(read[511], std::byte{0x55});
    EXPECT_EQ(legacy_calls, 3U);
    EXPECT_EQ(block_request_sync(&legacy, BlockRequestOperation::READ, 0, 0, read.data()), 0);
    EXPECT_EQ(legacy_calls, 3U);
    EXPECT_EQ(block_request_sync(&legacy, BlockRequestOperation::FLUSH, 0, 0, nullptr), -EOPNOTSUPP);
}

TEST_F(BlockRequestRuntime, WaiterTaskReferencesBalanceAcrossCompletion) {
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    native.auto_complete = true;
    ker::mod::sched::task::Task task{};
    ker::mod::sched::block_test_current_task = &task;
    EXPECT_EQ(block_request_wait(request), 0);
    ker::mod::sched::block_test_current_task = nullptr;
    EXPECT_EQ(task.references.load(), 1U);
    EXPECT_EQ(request.waiter, nullptr);
    EXPECT_EQ(block_request_release(request), 0);
}

TEST_F(BlockRequestRuntime, TaskServiceBatchRetainsOneOwnerThroughCallbacksAndRejectsDuplicateClaims) {
    namespace sched = ker::mod::sched;
    sched::task::Task task;
    sched::block_test_current_task = &task;
    for (size_t i = 0; i < 8; ++i) {
        auto& request = owner();
        ASSERT_EQ(submit(request, BlockRequestOperation::READ, i), 0);
    }
    native.service_hook = [&](size_t calls) {
        EXPECT_EQ(task.references.load(), 2U);
        EXPECT_EQ(task.reclaim_no_io_depth, 1U);
        EXPECT_EQ(ker::mod::sys::block_test_spinlock_depth, 0U);
        if (calls != 2) return;
        for (auto const& request : owners) {
            EXPECT_TRUE(native.retire(request->token, 0, SIZE_MAX, true));
            EXPECT_FALSE(request->completed.load());
            EXPECT_FALSE(block_request_complete_from_service(&native.device, request->token, 0, 512));
        }
    };
    auto const BEFORE = snapshot(native.device);
    block_queue_service(&native.device);
    EXPECT_EQ(callbacks.load(), 8U);
    EXPECT_EQ(task.lifetime_acquires.load(), 1U);
    EXPECT_EQ(task.references.load(), 1U);
    EXPECT_EQ(task.reclaim_no_io_depth, 0U);
    EXPECT_EQ(snapshot(native.device).stats.completion_claims - BEFORE.stats.completion_claims, 8U);
    for (auto const& request : owners) {
        EXPECT_TRUE(request->completed.load());
        EXPECT_EQ(block_request_release(*request), 0);
    }
    native.service_hook = {};
    sched::block_test_current_task = nullptr;
}

TEST_F(BlockRequestRuntime, TaskServiceContendedPassDoesNotAcquireOrReplaceProviderOwner) {
    namespace sched = ker::mod::sched;
    auto& request = owner();
    ASSERT_EQ(submit_for_wait(request), 0);
    Gate provider;
    sched::task::Task owner_task;
    native.service_hook = [&](size_t) { provider.enter_and_wait(); };
    std::thread service([&] {
        sched::block_test_current_task = &owner_task;
        block_queue_service(&native.device);
        sched::block_test_current_task = nullptr;
    });
    bool const ENTERED = provider.await_entered();
    EXPECT_TRUE(ENTERED);
    if (ENTERED) {
        EXPECT_EQ(owner_task.references.load(), 2U);
        sched::task::Task contender;
        sched::block_test_current_task = &contender;
        block_queue_service(&native.device);
        EXPECT_EQ(contender.references.load(), 1U);
        EXPECT_EQ(contender.lifetime_acquires.load(), 0U);
        EXPECT_EQ(contender.reclaim_no_io_depth, 0U);
        sched::block_test_current_task = nullptr;
    }
    provider.release();
    service.join();
    native.service_hook = {};
    EXPECT_EQ(owner_task.references.load(), 1U);
    native.auto_complete = true;
    native.service_completion = true;
    EXPECT_EQ(block_request_wait(request), 0);
    EXPECT_EQ(block_request_release(request), 0);
}

TEST_F(BlockRequestRuntime, TaskServiceCompletesForeignWaiterAfterRetirementWithBalancedReferences) {
    namespace sched = ker::mod::sched;
    auto& request = owner();
    ASSERT_EQ(submit(request), 0);
    block_queue_service(&native.device);
    Gate provider;
    Gate parked;
    sched::task::Task service_task;
    sched::task::Task waiter;
    waiter.syscall_account_start_us = 1;
    native.auto_complete = true;
    native.service_completion = true;
    native.service_hook = [&](size_t) { provider.enter_and_wait(); };
    std::thread service([&] {
        sched::block_test_current_task = &service_task;
        block_queue_service(&native.device);
        sched::block_test_current_task = nullptr;
    });
    bool const ENTERED = provider.await_entered();
    EXPECT_TRUE(ENTERED);
    if (!ENTERED) {
        provider.release();
        service.join();
        native.service_hook = {};
        return;
    }
    int status = -EINPROGRESS;
    std::thread waiting([&] {
        sched::block_test_current_task = &waiter;
        sched::block_test_pause_hook = [&] { parked.enter_and_wait(); };
        status = block_request_wait(request);
        sched::block_test_pause_hook = {};
        sched::block_test_current_task = nullptr;
    });
    bool const PARKED = parked.await_entered();
    EXPECT_TRUE(PARKED);
    unsigned const WAKES = sched::block_test_wakes.load();
    provider.release();
    service.join();
    EXPECT_TRUE(request.completed.load());
    EXPECT_EQ(callbacks.load(), 1U);
    if (PARKED) {
        EXPECT_EQ(sched::block_test_wakes.load(), WAKES + 1);  // Foreign waiter remains explicitly woken.
        EXPECT_EQ(waiter.references.load(), 2U);
    }
    parked.release();
    waiting.join();
    native.service_hook = {};
    EXPECT_EQ(status, 0);
    EXPECT_EQ(waiter.references.load(), 1U);
    EXPECT_EQ(service_task.references.load(), 1U);
    EXPECT_EQ(service_task.reclaim_no_io_depth, 0U);
    EXPECT_EQ(block_request_release(request), 0);
}

TEST_F(BlockRequestRuntime, SyncRejectsWholeInvalidRangeBeforeAnyProviderSideEffect) {
    std::array<std::byte, 1024> buffer{};
    buffer.fill(std::byte{0x42});
    EXPECT_EQ(block_request_sync(&legacy, BlockRequestOperation::WRITE, 127, 2, buffer.data()), -EINVAL);
    EXPECT_EQ(legacy_calls, 0U);
    EXPECT_EQ(legacy_media[127 * 512], std::byte{0});
    EXPECT_EQ(block_request_sync(&legacy, BlockRequestOperation::READ, 0, SIZE_MAX, buffer.data()), -EINVAL);
    EXPECT_EQ(legacy_calls, 0U);
}

TEST_F(BlockRequestRuntime, UserSyncStagingAndCopyFaultsPreserveProviderOwnership) {
    // A low mapping stands in for a user VA, while allocator-backed staging
    // remains above the shim's HHDM boundary. Only address/copy primitives are
    // substituted; production submit/sync/read/write/release run unchanged.
    void* mapping = mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_32BIT, -1, 0);
    ASSERT_NE(mapping, MAP_FAILED);
    struct MappingGuard {
        void* address;
        ~MappingGuard() { munmap(address, 4096); }
    } const MAPPING{mapping};
    ASSERT_LT(reinterpret_cast<uint64_t>(mapping), uint64_t{1} << 32);
    ker::mod::mm::addr::block_test_hhdm_offset.store(uint64_t{1} << 32);
    ker::mod::sched::task::Task task{};
    ker::mod::sched::block_test_current_task = &task;
    std::memset(mapping, 0xA5, 1024);
    auto& direct = owner();
    BlockSegment segment{mapping, 512};
    EXPECT_EQ(block_request_submit(&legacy, direct, BlockRequestOperation::WRITE, 0, 1, {&segment, 1}), -EFAULT);
    EXPECT_EQ(direct.queue, nullptr);
    EXPECT_EQ(block_request_sync(&legacy, BlockRequestOperation::WRITE, 0, 2, mapping), 0);
    EXPECT_EQ(legacy_calls, 2U);
    EXPECT_EQ(legacy_media[1023], std::byte{0xA5});
    std::memset(mapping, 0, 1024);
    EXPECT_EQ(block_request_sync(&legacy, BlockRequestOperation::READ, 0, 2, mapping), 0);
    EXPECT_EQ(static_cast<unsigned char*>(mapping)[1023], 0xA5);
    ker::mod::sys::usercopy::block_test_fail_copy = true;
    EXPECT_EQ(block_request_sync(&legacy, BlockRequestOperation::WRITE, 0, 1, mapping), -EFAULT);
    EXPECT_EQ(legacy_calls, 4U);
    EXPECT_EQ(block_request_sync(&legacy, BlockRequestOperation::READ, 0, 1, mapping), -EFAULT);
    EXPECT_EQ(legacy_calls, 5U);
    EXPECT_EQ(snapshot(legacy).owned_requests, 0U);
    EXPECT_EQ(task.references.load(), 1U);
    ker::mod::sched::block_test_current_task = nullptr;
    ker::mod::mm::addr::block_test_hhdm_offset.store(0);
}
