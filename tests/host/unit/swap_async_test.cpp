#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <platform/sched/scheduler.hpp>

#include "../../../modules/kern/src/test/swap_async_ktest.cpp"

// Platform/VFS/registry seams only; the production KTEST body, swap area/slot
// implementation, CRC routine, and block request runtime compile unchanged.
namespace {
const ker::dev::BlockDevice* handoff_on_write{};
unsigned actual_handoffs{};

template <class Function>
void isolated(Function function) {
    pid_t const CHILD = fork();
    ASSERT_GE(CHILD, 0);
    if (CHILD == 0) {
        function();
        _exit(testing::Test::HasFailure() ? 1 : 0);
    }
    int status = 0;
    pid_t waited = 0;
    do {
        waited = waitpid(CHILD, &status, 0);
    } while (waited == -1 && errno == EINTR);
    ASSERT_EQ(waited, CHILD);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}
}  // namespace

namespace ker::dev {
auto block_device_is_read_only(const BlockDevice* device) -> bool {
    if (handoff_on_write != nullptr && device == handoff_on_write) {
        handoff_on_write = nullptr;
        EXPECT_TRUE(block_queue_available(device));
        // This endpoint runs after SlotIo's availability snapshot and before
        // submit_request takes the queue lock. Real failed startup publishes
        // caller_driven; successful worker exit has separate runtime coverage.
        ker::mod::sched::task::block_test_create_worker = false;
        block_request_start_workers();
        ++actual_handoffs;
        EXPECT_FALSE(block_queue_available(device));
    }
    return device != nullptr && device->read_only;
}
auto block_device_find_by_name(const char*) -> BlockDevice* { return nullptr; }
auto block_read(BlockDevice* device, uint64_t lba, size_t count, void* buffer) -> int {
    return device->read_blocks(device, lba, count, buffer);
}
auto block_write(BlockDevice* device, uint64_t lba, size_t count, const void* buffer) -> int {
    return device->write_blocks(device, lba, count, buffer);
}
}  // namespace ker::dev

namespace ker::test {
int g_pass{};
int g_fail{};
void check_eq_log(const char* file, int line, uint64_t a, uint64_t b, const char* as, const char* bs) {
    ADD_FAILURE_AT(file, line) << as << "=" << a << ", " << bs << "=" << b;
}
void check_ne_log(const char* file, int line, uint64_t, uint64_t, const char* as, const char* bs) {
    ADD_FAILURE_AT(file, line) << as << " == " << bs;
}
void check_true_log(const char* file, int line, const char* expression) { ADD_FAILURE_AT(file, line) << expression; }
void check_null_log(const char* file, int line, const void*, const char* expression) { ADD_FAILURE_AT(file, line) << expression; }
}  // namespace ker::test

TEST(SwapAsync, ActualKernelTrace) {
    _kt_SwapAsync_DelayedIoPinsGenerationAndAreaUntilFinish();
    EXPECT_EQ(ker::test::g_fail, 0);
    EXPECT_GE(ker::test::g_pass, 40);
    RecordProperty("ktest_assertions", ker::test::g_pass);
}

TEST(SwapAsync, AvailabilityThenCallerDrivenRefusalPreservesStoredSlot) {
    isolated([] {
        constexpr const char* NAME = "host-swap-admission-handoff";
        static DelayedSwapDevice device;
        device.device.block_size = 512;
        device.device.total_blocks = device.media.size() / 512;
        device.device.private_data = &device;
        device.device.read_blocks = DelayedSwapDevice::read;
        device.device.write_blocks = DelayedSwapDevice::write;
        static const BlockAsyncOps OPS{DelayedSwapDevice::submit, DelayedSwapDevice::service, DelayedSwapDevice::cancel};
        swap::SwapStats baseline{};
        swap::get_stats(&baseline);
        ASSERT_EQ(block_queue_configure(&device.device, {1, PAGE_BYTES, 32, 1, PAGE_BYTES}, &OPS), 0);
        swap::SwapExtent const EXTENT{&device.device, 0, 2};
        ASSERT_EQ(swap::activate_extents(NAME, &EXTENT, 1), 0);
        swap::SwapSlot slot{};
        ASSERT_EQ(swap::allocate_slot(&slot), 0);
        Page original{};
        original.fill(0x6d);
        swap::SlotIo write;
        ASSERT_EQ(swap::submit_slot_io(write, slot, original.data(), true), 0);
        block_queue_service(&device.device);
        ASSERT_TRUE(device.retire(write.request.token));
        ASSERT_EQ(swap::finish_slot_io(write), 0);
        swap::SwapStats stored{};
        swap::get_stats(&stored);

        Page replacement{};
        replacement.fill(0xa2);
        swap::SlotIo rejected;
        handoff_on_write = &device.device;
        ASSERT_EQ(swap::submit_slot_io(rejected, slot, replacement.data(), true), -ESHUTDOWN);
        EXPECT_EQ(actual_handoffs, 1U);
        EXPECT_FALSE(rejected.active);
        EXPECT_EQ(rejected.request.queue, nullptr);
        EXPECT_EQ(device.pending, nullptr);
        EXPECT_TRUE(std::memcmp(device.media.data(), original.data(), PAGE_BYTES) == 0);
        swap::SwapStats after_refusal{};
        swap::get_stats(&after_refusal);
        EXPECT_EQ(after_refusal.total_bytes, stored.total_bytes);
        EXPECT_EQ(after_refusal.used_bytes, stored.used_bytes);
        EXPECT_EQ(after_refusal.free_bytes, stored.free_bytes);
        EXPECT_EQ(after_refusal.cached_bytes, stored.cached_bytes);
        EXPECT_EQ(after_refusal.active_areas, stored.active_areas);

        // Actual SlotIo requires the original STORED generation and checksum.
        // Its direct-device endpoint is the same explicit Host VFS seam above.
        Page restored{};
        swap::SlotIo read;
        ASSERT_EQ(swap::submit_slot_io(read, slot, restored.data(), false), 0);
        EXPECT_FALSE(read.asynchronous);
        ASSERT_EQ(swap::finish_slot_io(read), 0);
        EXPECT_EQ(restored, original);
        ASSERT_EQ(swap::free_slot(slot), 0);     // No leaked slot io_in_flight.
        ASSERT_EQ(swap::swapoff_path(NAME), 0);  // No leaked area io_in_flight.
        block_queue_close(&device.device);
        EXPECT_EQ(block_queue_drain(&device.device), 0);
        swap::SwapStats final{};
        swap::get_stats(&final);
        EXPECT_EQ(final.total_bytes, baseline.total_bytes);
        EXPECT_EQ(final.used_bytes, baseline.used_bytes);
        EXPECT_EQ(final.free_bytes, baseline.free_bytes);
        EXPECT_EQ(final.cached_bytes, baseline.cached_bytes);
        EXPECT_EQ(final.active_areas, baseline.active_areas);
    });
}
