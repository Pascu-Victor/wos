#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <dev/block_device.hpp>
#include <dev/block_request.hpp>
#include <limits>
#include <mutex>
#include <new>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/reclaim.hpp>
#include <thread>
#include <vector>
#include <vfs/buffer_cache.hpp>

namespace buffer_cache_heap_test {
inline thread_local bool enabled{};
inline thread_local bool fail{};
inline thread_local size_t allocations{};
}  // namespace buffer_cache_heap_test

// Observe real descriptor allocation, rather than a production-only test
// hook. Forward to ordinary new/delete so ASan keeps allocation provenance.
auto operator new(size_t bytes, const std::nothrow_t&) noexcept -> void* {
    if (buffer_cache_heap_test::enabled) {
        ++buffer_cache_heap_test::allocations;
        if (buffer_cache_heap_test::fail) return nullptr;
    }
    try {
        return ::operator new(bytes);
    } catch (...) {
        return nullptr;
    }
}
void operator delete(void* pointer, const std::nothrow_t&) noexcept { ::operator delete(pointer); }
auto operator new[](size_t bytes, const std::nothrow_t&) noexcept -> void* {
    if (buffer_cache_heap_test::enabled) {
        ++buffer_cache_heap_test::allocations;
        if (buffer_cache_heap_test::fail) return nullptr;
    }
    try {
        return ::operator new[](bytes);
    } catch (...) {
        return nullptr;
    }
}
void operator delete[](void* pointer, const std::nothrow_t&) noexcept { ::operator delete[](pointer); }

namespace {
using namespace ker::dev;
using namespace ker::vfs;
namespace reclaim = ker::mod::mm::reclaim;
namespace phys = ker::mod::mm::phys;

struct QueuedDisk {
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<BlockRequest*> pending;
    std::vector<uint64_t> completed_lbas;
    std::vector<size_t> transfer_bytes;
    std::array<uint8_t, 512 * 1024> media{};
    size_t peak{};
    size_t submissions{};
    size_t ordinary_write_attempts{};
    size_t immediate_write_attempts{};
    size_t releases{};
    size_t flushes{};
    size_t limit_bytes{512 * 1024};
    size_t inflight_limit_bytes{8 * 1024 * 1024};
    size_t fail_admissions{};
    size_t capacity_waits{};
    uint64_t capacity_advance_us{};
    uint64_t now_us{100};
    size_t descriptor_allocations{};
    size_t legacy_reads{};
    size_t legacy_writes{};
    int admission_status{};
    int release_status{};
    size_t allocation_probes{};
    size_t allocation_probes_with_owned_requests{};
    size_t metadata_probes{};
    size_t free_probes{};
    int completion_status{};
    bool short_completion{};
    bool blocked{};
    bool complete_on_submit{};
    bool reverse{true};
    bool observe_descriptor_heap{};
    bool fail_descriptor_heap{};
    BufHead* redirty{};
    uint8_t redirty_value{};
};

QueuedDisk* active_disk{};

void observe_snapshot_allocation(size_t) {
    EXPECT_GT(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    ++active_disk->allocation_probes;
    if (active_disk->submissions > active_disk->releases) ++active_disk->allocation_probes_with_owned_requests;
}
void observe_snapshot_free(void*) {
    EXPECT_GT(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    ++active_disk->free_probes;
}

void observe_metadata_allocation(size_t bytes) {
    EXPECT_GT(bytes, 16 * 1024U);
    if (active_disk->metadata_probes++ == 0) {
        EXPECT_EQ(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    } else {
        EXPECT_GT(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    }
}
void observe_metadata_free(void* backing, size_t bytes) {
    EXPECT_GT(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    std::lock_guard guard(active_disk->mutex);
    for (const auto* request : active_disk->pending) {
        auto const ADDRESS = reinterpret_cast<uintptr_t>(request);
        auto const BASE = reinterpret_cast<uintptr_t>(backing);
        EXPECT_FALSE(ADDRESS >= BASE && ADDRESS < BASE + bytes);
    }
}

auto fake_available(const BlockDevice* dev) -> bool {
    bool const AVAILABLE = dev->private_data == active_disk;
    if (AVAILABLE && active_disk->observe_descriptor_heap) {
        buffer_cache_heap_test::allocations = 0;
        buffer_cache_heap_test::fail = active_disk->fail_descriptor_heap;
        buffer_cache_heap_test::enabled = true;
    }
    return AVAILABLE;
}
auto fake_limits(const BlockDevice*) -> BlockQueueLimits {
    BlockQueueLimits result{};
    result.depth = 8;
    result.max_transfer_bytes = active_disk->limit_bytes;
    result.max_inflight_bytes = active_disk->inflight_limit_bytes;
    return result;
}
void complete_one(QueuedDisk& disk);
auto fake_submit_common(BlockDevice* dev, BlockRequest& request, BlockRequestOperation op, uint64_t lba, size_t count,
                        std::span<const BlockSegment> segments, bool immediate_wait) -> int {
    auto& disk = *static_cast<QueuedDisk*>(dev->private_data);
    if (disk.observe_descriptor_heap) {
        disk.descriptor_allocations = buffer_cache_heap_test::allocations;
        buffer_cache_heap_test::enabled = false;
    }
    std::lock_guard guard(disk.mutex);
    if (op == BlockRequestOperation::WRITE) {
        if (immediate_wait)
            ++disk.immediate_write_attempts;
        else
            ++disk.ordinary_write_attempts;
    }
    EXPECT_GT(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    if (disk.fail_admissions != 0) {
        --disk.fail_admissions;
        return -EAGAIN;
    }
    if (disk.admission_status != 0) return disk.admission_status;
    if (disk.pending.size() >= 8) {
        return -EAGAIN;
    }
    size_t pending_bytes = 0;
    for (const auto* pending : disk.pending) pending_bytes += pending->description.bytes;
    if (pending_bytes + count * dev->block_size > disk.inflight_limit_bytes) return -EAGAIN;
    EXPECT_TRUE(op == BlockRequestOperation::WRITE || op == BlockRequestOperation::READ);
    EXPECT_EQ(segments.size(), 1U);
    EXPECT_EQ(segments.front().size, count * dev->block_size);
    EXPECT_LE(segments.front().size, disk.limit_bytes);
    request.description = {op, lba, count, count * dev->block_size};
    request.segments[0] = segments.front();
    request.segment_count = 1;
    request.completed.store(false, std::memory_order_release);
    disk.pending.push_back(&request);
    disk.peak = std::max(disk.peak, disk.pending.size());
    ++disk.submissions;
    if (disk.complete_on_submit) complete_one(disk);
    disk.cv.notify_all();
    return 0;
}
auto fake_submit(BlockDevice* dev, BlockRequest& request, BlockRequestOperation op, uint64_t lba, size_t count,
                 std::span<const BlockSegment> segments) -> int {
    return fake_submit_common(dev, request, op, lba, count, segments, false);
}
auto fake_submit_for_wait(BlockDevice* dev, BlockRequest& request, BlockRequestOperation op, uint64_t lba, size_t count,
                          std::span<const BlockSegment> segments) -> int {
    return fake_submit_common(dev, request, op, lba, count, segments, true);
}
void complete_one(QueuedDisk& disk) {
    size_t const INDEX = disk.reverse ? disk.pending.size() - 1 : 0;
    BlockRequest* request = disk.pending.at(INDEX);
    disk.pending.erase(disk.pending.begin() + static_cast<ptrdiff_t>(INDEX));
    request->status = disk.completion_status;
    request->bytes = disk.short_completion ? request->description.bytes - 1 : request->description.bytes;
    if (request->status == 0 && request->description.lba * 512 + request->description.bytes <= disk.media.size()) {
        if (request->description.op == BlockRequestOperation::WRITE) {
            memcpy(disk.media.data() + request->description.lba * 512, request->segments[0].data, request->description.bytes);
        } else {
            memcpy(request->segments[0].data, disk.media.data() + request->description.lba * 512, request->description.bytes);
        }
    }
    disk.completed_lbas.push_back(request->description.lba);
    disk.transfer_bytes.push_back(request->description.bytes);
    request->completed.store(true, std::memory_order_release);
}
auto fake_wait(BlockRequest& request) -> int {
    EXPECT_GT(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    auto& disk = *active_disk;
    std::unique_lock guard(disk.mutex);
    disk.cv.wait(guard, [&disk] { return !disk.blocked; });
    if (disk.redirty != nullptr) {
        BufHead* bh = disk.redirty;
        disk.redirty = nullptr;
        guard.unlock();
        bh->data[0] = disk.redirty_value;
        bdirty(bh);
        guard.lock();
    }
    while (!request.completed.load(std::memory_order_acquire)) {
        complete_one(disk);
    }
    return request.status;
}
auto fake_release(BlockRequest& request) -> int {
    EXPECT_GT(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    EXPECT_TRUE(request.completed.load(std::memory_order_acquire));
    EXPECT_NE(request.segments[0].data, nullptr);
    ++active_disk->releases;
    return active_disk->release_status;
}
auto fake_read(BlockDevice* dev, uint64_t lba, size_t count, void* data) -> int {
    auto& disk = *static_cast<QueuedDisk*>(dev->private_data);
    if (disk.observe_descriptor_heap) {
        disk.descriptor_allocations = buffer_cache_heap_test::allocations;
        buffer_cache_heap_test::enabled = false;
    }
    ++disk.legacy_reads;
    memcpy(data, disk.media.data() + lba * 512, count * 512);
    return 0;
}
void fake_capacity_wait(BlockDevice*) {
    EXPECT_GT(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    ++active_disk->capacity_waits;
    active_disk->now_us += active_disk->capacity_advance_us;
}
auto fake_clock_us() -> uint64_t { return active_disk->now_us; }
auto fake_write(BlockDevice* dev, uint64_t lba, size_t count, const void* data) -> int {
    EXPECT_GT(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    auto& disk = *static_cast<QueuedDisk*>(dev->private_data);
    ++disk.legacy_writes;
    if (lba * 512 + count * 512 <= disk.media.size()) {
        memcpy(disk.media.data() + lba * 512, data, count * 512);
    }
    return 0;
}
auto fake_flush(BlockDevice* dev) -> int {
    auto& disk = *static_cast<QueuedDisk*>(dev->private_data);
    std::lock_guard guard(disk.mutex);
    EXPECT_TRUE(disk.pending.empty());
    EXPECT_EQ(disk.submissions, disk.releases);
    ++disk.flushes;
    return 0;
}

class BufferCacheAsync : public ::testing::Test {
   protected:
    QueuedDisk disk;
    BlockDevice dev{};
    void SetUp() override {
        phys::cache_test::fail_metadata = false;
        phys::cache_test::metadata_attempts = 0;
        phys::cache_test::metadata_frees = 0;
        buffer_cache_init();
        dev.block_size = 512;
        dev.total_blocks = disk.media.size() / 512;
        dev.private_data = &disk;
        dev.read_blocks = fake_read;
        dev.write_blocks = fake_write;
        dev.flush = fake_flush;
        active_disk = &disk;
        cache_test::hooks = {fake_available, fake_limits, fake_submit, fake_wait, fake_release, nullptr, fake_submit_for_wait};
    }
    void TearDown() override {
        buffer_cache_heap_test::enabled = false;
        ker::mod::ktime::cache_test::clock_us = nullptr;
        EXPECT_EQ(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
        phys::cache_test::allocation_probe = nullptr;
        phys::cache_test::free_probe = nullptr;
        phys::cache_test::metadata_allocation_probe = nullptr;
        phys::cache_test::metadata_free_probe = nullptr;
        phys::cache_test::fail_metadata = false;
        EXPECT_TRUE(std::ranges::all_of(phys::cache_test::metadata_owned, [](const auto& entry) { return entry.data == nullptr; }));
        EXPECT_TRUE(disk.pending.empty());
        EXPECT_EQ(disk.submissions, disk.releases);
        cache_test::hooks = {};
        invalidate_bdev(&dev);
        active_disk = nullptr;
    }
    auto dirty(uint64_t block, size_t count = 1, uint8_t value = 0x5a) -> BufHead* {
        BufHead* bh = bget_multi(&dev, block, count);
        EXPECT_NE(bh, nullptr);
        memset(bh->data, value, count * 512);
        bh->flags |= BH_VALID;
        bdirty(bh);
        return bh;
    }
};

TEST_F(BufferCacheAsync, SingleSliceDemandReadHasNoDescriptorHeapAllocation) {
    disk.observe_descriptor_heap = true;
    disk.media.fill(0x37);
    auto* span = bread_multi(&dev, 0, 256, BufferReadClass::FILE_DATA);
    ASSERT_NE(span, nullptr);
    EXPECT_EQ(disk.descriptor_allocations, 0U);
    EXPECT_EQ(disk.submissions, 1U);
    EXPECT_EQ(disk.releases, 1U);
    EXPECT_EQ(disk.legacy_reads, 0U);
    EXPECT_TRUE(std::all_of(span->data, span->data + span->size, [](uint8_t byte) { return byte == 0x37; }));
    brelse(span);
}

TEST_F(BufferCacheAsync, SectorAndProviderBoundaryReadsIgnoreDescriptorHeapFailure) {
    disk.observe_descriptor_heap = true;
    disk.fail_descriptor_heap = true;
    disk.media.fill(0x5a);
    for (size_t count : {size_t{1}, size_t{1024}}) {
        auto* span = bread_multi(&dev, 0, count, BufferReadClass::FILESYSTEM_METADATA);
        ASSERT_NE(span, nullptr);
        EXPECT_EQ(span->size, count * 512);
        EXPECT_EQ(disk.descriptor_allocations, 0U);
        EXPECT_EQ(disk.legacy_reads, 0U);
        EXPECT_EQ(disk.submissions, disk.releases);
        EXPECT_TRUE(std::all_of(span->data, span->data + span->size, [](uint8_t byte) { return byte == 0x5a; }));
        brelse(span);
    }
    EXPECT_EQ(disk.submissions, 2U);
    EXPECT_EQ(disk.peak, 1U);
}

TEST_F(BufferCacheAsync, SingleSliceBackpressureWaitsWithoutDescriptorHeapOwnership) {
    disk.observe_descriptor_heap = true;
    disk.fail_descriptor_heap = true;
    disk.fail_admissions = 3;
    cache_test::hooks.service = fake_capacity_wait;
    auto* span = bread_multi(&dev, 0, 8, BufferReadClass::FILE_DATA);
    ASSERT_NE(span, nullptr);
    EXPECT_EQ(disk.capacity_waits, 3U);
    EXPECT_EQ(disk.descriptor_allocations, 0U);
    EXPECT_EQ(disk.submissions, 1U);
    EXPECT_EQ(disk.releases, 1U);
    EXPECT_EQ(disk.legacy_reads, 0U);
    brelse(span);
}

TEST_F(BufferCacheAsync, SingleSliceAdmissionTimeoutRetainsNoRequestAndPublishesNoCacheEntry) {
    disk.observe_descriptor_heap = true;
    disk.fail_admissions = std::numeric_limits<size_t>::max();
    disk.capacity_advance_us = 30'000'000;
    cache_test::hooks.service = fake_capacity_wait;
    ker::mod::ktime::cache_test::clock_us = fake_clock_us;
    EXPECT_EQ(bread_multi(&dev, 0, 8, BufferReadClass::FILE_DATA), nullptr);
    EXPECT_EQ(disk.capacity_waits, 1U);
    EXPECT_EQ(disk.descriptor_allocations, 0U);
    EXPECT_EQ(disk.submissions, 0U);
    EXPECT_EQ(disk.releases, 0U);
    EXPECT_EQ(disk.legacy_reads, 0U);
    EXPECT_FALSE(has_cached_bdev_range(&dev, 0, 8));
    EXPECT_EQ(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
}

TEST_F(BufferCacheAsync, SingleSliceFatalAdmissionAndUnsupportedFallbackHaveNoRetainedRequest) {
    disk.observe_descriptor_heap = true;
    disk.admission_status = -ENOSPC;
    EXPECT_EQ(bread_multi(&dev, 0, 8, BufferReadClass::FILE_DATA), nullptr);
    EXPECT_FALSE(has_cached_bdev_range(&dev, 0, 8));
    EXPECT_EQ(disk.legacy_reads, 0U);
    disk.admission_status = -EOPNOTSUPP;
    disk.media.fill(0x6c);
    auto* span = bread_multi(&dev, 0, 8, BufferReadClass::FILE_DATA);
    ASSERT_NE(span, nullptr);
    EXPECT_EQ(disk.descriptor_allocations, 0U);
    EXPECT_EQ(disk.submissions, 0U);
    EXPECT_EQ(disk.releases, 0U);
    EXPECT_EQ(disk.legacy_reads, 1U);
    EXPECT_TRUE(std::all_of(span->data, span->data + span->size, [](uint8_t byte) { return byte == 0x6c; }));
    brelse(span);
}

TEST_F(BufferCacheAsync, SingleSliceShortErrorAndReleaseFailureRetireBeforeDiscardingDetachedBuffer) {
    disk.observe_descriptor_heap = true;
    for (int mode = 0; mode < 3; ++mode) {
        disk.short_completion = mode == 0;
        disk.completion_status = mode == 1 ? -ETIMEDOUT : 0;
        disk.release_status = mode == 2 ? -EIO : 0;
        EXPECT_EQ(bread_multi(&dev, 0, 8, BufferReadClass::FILE_DATA), nullptr);
        EXPECT_FALSE(has_cached_bdev_range(&dev, 0, 8));
        EXPECT_EQ(disk.descriptor_allocations, 0U);
        EXPECT_EQ(disk.submissions, static_cast<size_t>(mode + 1));
        EXPECT_EQ(disk.releases, disk.submissions);
        EXPECT_EQ(disk.legacy_reads, 0U);
        EXPECT_TRUE(disk.pending.empty());
        EXPECT_EQ(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    }
}

TEST_F(BufferCacheAsync, SingleSliceCannotPublishOrReleaseBeforeCompletionRetirement) {
    disk.observe_descriptor_heap = true;
    disk.blocked = true;
    disk.media.fill(0x74);
    std::atomic<BufHead*> published{};
    std::thread reader([&] {
        published.store(bread_multi(&dev, 0, 8, BufferReadClass::FILE_DATA));
        EXPECT_EQ(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    });
    {
        std::unique_lock guard(disk.mutex);
        ASSERT_TRUE(disk.cv.wait_for(guard, std::chrono::seconds(2), [&] { return disk.submissions == 1; }));
    }
    EXPECT_EQ(published.load(), nullptr);
    EXPECT_FALSE(has_cached_bdev_range(&dev, 0, 8));
    {
        std::lock_guard guard(disk.mutex);
        EXPECT_EQ(disk.releases, 0U);
        EXPECT_EQ(disk.pending.size(), 1U);
        EXPECT_FALSE(disk.pending.front()->completed.load(std::memory_order_acquire));
        disk.blocked = false;
        disk.cv.notify_all();
    }
    reader.join();
    auto* span = published.load();
    ASSERT_NE(span, nullptr);
    EXPECT_EQ(disk.descriptor_allocations, 0U);
    EXPECT_EQ(disk.releases, 1U);
    EXPECT_TRUE(std::all_of(span->data, span->data + span->size, [](uint8_t byte) { return byte == 0x74; }));
    brelse(span);
}

TEST_F(BufferCacheAsync, RoundedProviderLimitKeepsMultipleSlicesAndHeapFallback) {
    disk.observe_descriptor_heap = true;
    disk.limit_bytes = 4097;
    disk.media.fill(0x81);
    auto* span = bread_multi(&dev, 0, 9, BufferReadClass::FILE_DATA);
    ASSERT_NE(span, nullptr);
    EXPECT_EQ(disk.descriptor_allocations, 1U);
    EXPECT_EQ(disk.submissions, 2U);
    EXPECT_EQ(disk.releases, 2U);
    EXPECT_EQ(disk.peak, 2U);
    std::sort(disk.transfer_bytes.begin(), disk.transfer_bytes.end());
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{512, 4096}));
    EXPECT_TRUE(std::all_of(span->data, span->data + span->size, [](uint8_t byte) { return byte == 0x81; }));
    brelse(span);
    discard_bdev_range(&dev, 0, 9);
    disk.fail_descriptor_heap = true;
    span = bread_multi(&dev, 0, 9, BufferReadClass::FILE_DATA);
    ASSERT_NE(span, nullptr);
    EXPECT_EQ(disk.descriptor_allocations, 1U);
    EXPECT_EQ(disk.submissions, 2U);
    EXPECT_EQ(disk.releases, 2U);
    EXPECT_EQ(disk.legacy_reads, 1U);
    brelse(span);
}

TEST_F(BufferCacheAsync, SmallerInflightBudgetUsesTheExistingMultisliceOwnershipLoop) {
    disk.observe_descriptor_heap = true;
    disk.inflight_limit_bytes = 4097;
    disk.media.fill(0x29);
    auto* span = bread_multi(&dev, 0, 16, BufferReadClass::FILE_DATA);
    ASSERT_NE(span, nullptr);
    EXPECT_EQ(disk.descriptor_allocations, 1U);
    EXPECT_EQ(disk.peak, 1U);
    EXPECT_EQ(disk.submissions, 2U);
    EXPECT_EQ(disk.releases, 2U);
    EXPECT_EQ(disk.legacy_reads, 0U);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{4096, 4096}));
    EXPECT_TRUE(std::all_of(span->data, span->data + span->size, [](uint8_t byte) { return byte == 0x29; }));
    brelse(span);
}

TEST_F(BufferCacheAsync, AllocationReclaimGuardsPreparedSnapshotsAndRefillsUntilRelease) {
    std::array<BufHead*, 12> buffers{};
    for (size_t i = 0; i < buffers.size(); ++i) buffers[i] = dirty(i * 16, 8);
    phys::cache_test::allocation_probe = observe_snapshot_allocation;
    phys::cache_test::free_probe = observe_snapshot_free;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(disk.allocation_probes, buffers.size());
    EXPECT_GT(disk.allocation_probes_with_owned_requests, 0U);
    EXPECT_EQ(disk.free_probes, buffers.size());
    EXPECT_EQ(disk.submissions, disk.releases);
    EXPECT_EQ(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    phys::cache_test::allocation_probe = nullptr;
    phys::cache_test::free_probe = nullptr;
    for (auto* bh : buffers) brelse(bh);
}

TEST_F(BufferCacheAsync, SynchronousSnapshotKeepsReclaimGuardThroughWriteAndCleanup) {
    auto* bh = dirty(0, 8);
    phys::cache_test::allocation_probe = observe_snapshot_allocation;
    phys::cache_test::free_probe = observe_snapshot_free;
    EXPECT_EQ(bwrite(bh), 0);
    EXPECT_EQ(disk.allocation_probes, 1U);
    EXPECT_EQ(disk.free_probes, 1U);
    EXPECT_EQ(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    phys::cache_test::allocation_probe = nullptr;
    phys::cache_test::free_probe = nullptr;
    brelse(bh);
}

TEST_F(BufferCacheAsync, EightIndependentRunsRemainPinnedThroughReverseCompletionsAndFlush) {
    std::array<BufHead*, 12> buffers{};
    for (size_t i = 0; i < buffers.size(); ++i) {
        buffers[i] = dirty(i * 2);
    }
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(disk.peak, 8U);
    EXPECT_EQ(disk.submissions, 12U);
    EXPECT_EQ(disk.flushes, 1U);
    EXPECT_GT(disk.completed_lbas.front(), disk.completed_lbas.at(7));
    for (auto* bh : buffers) {
        EXPECT_EQ(bh->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
        EXPECT_EQ(bh->refcount.load(), 1U);
        EXPECT_EQ(disk.media.at(bh->block_no * 512), 0x5a);
        brelse(bh);
    }
}

TEST_F(BufferCacheAsync, LargeSnapshotSlicesHonorProviderLimit) {
    disk.limit_bytes = 4096;
    auto* bh = dirty(0, 32);
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(disk.submissions, 4U);
    EXPECT_EQ(disk.immediate_write_attempts, 4U);
    EXPECT_EQ(disk.ordinary_write_attempts, 0U);
    EXPECT_TRUE(std::ranges::all_of(disk.transfer_bytes, [](size_t bytes) { return bytes == 4096; }));
    EXPECT_EQ(bh->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
    brelse(bh);
}

TEST_F(BufferCacheAsync, RedirtyDuringCompletionPreservesNewGenerationAndStableSnapshot) {
    auto* bh = dirty(0, 1, 0x11);
    disk.redirty = bh;
    disk.redirty_value = 0x22;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_NE(bh->flags & BH_DIRTY, 0U);
    EXPECT_EQ(bh->flags & BH_WRITEBACK, 0U);
    EXPECT_EQ(disk.media[0], 0x11);
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(disk.media[0], 0x22);
    EXPECT_EQ(bh->flags & BH_DIRTY, 0U);
    brelse(bh);
}

TEST_F(BufferCacheAsync, ShortAndFailedCompletionsRetainDirtyGenerationAndReleaseEveryRequest) {
    auto* bh = dirty(0);
    disk.short_completion = true;
    EXPECT_EQ(sync_blockdev(&dev), -EIO);
    EXPECT_NE(bh->flags & BH_DIRTY, 0U);
    EXPECT_EQ(bh->flags & BH_WRITEBACK, 0U);
    disk.short_completion = false;
    disk.completion_status = -ENOSPC;
    EXPECT_EQ(sync_blockdev(&dev), -ENOSPC);
    EXPECT_NE(bh->flags & BH_DIRTY, 0U);
    disk.completion_status = 0;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(bh->flags & BH_DIRTY, 0U);
    brelse(bh);
}

TEST_F(BufferCacheAsync, TransientAdmissionFailureDoesNotLoseOwnershipOrProgress) {
    auto* bh = dirty(0);
    disk.fail_admissions = 3;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(disk.submissions, 1U);
    EXPECT_EQ(disk.releases, 1U);
    EXPECT_EQ(disk.immediate_write_attempts, 4U);
    EXPECT_EQ(disk.ordinary_write_attempts, 0U);
    brelse(bh);
}

TEST_F(BufferCacheAsync, ConcurrentSyncJoinsRedirtiedInflightGenerationBeforeFlush) {
    auto* bh = dirty(0);
    disk.blocked = true;
    std::atomic<int> first{-999};
    std::atomic<int> second{-999};
    std::thread worker([&] { first.store(sync_blockdev(&dev)); });
    {
        std::unique_lock guard(disk.mutex);
        ASSERT_TRUE(disk.cv.wait_for(guard, std::chrono::seconds(2), [&] { return disk.submissions == 1; }));
    }
    bh->data[0] = 0x77;
    bdirty(bh);
    std::thread joining([&] { second.store(sync_blockdev(&dev)); });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(second.load(), -999);
    {
        std::lock_guard guard(disk.mutex);
        EXPECT_EQ(disk.submissions, 1U);
        EXPECT_EQ(disk.flushes, 0U);
        disk.blocked = false;
        disk.cv.notify_all();
    }
    worker.join();
    joining.join();
    EXPECT_EQ(first.load(), 0);
    EXPECT_EQ(second.load(), 0);
    EXPECT_EQ(disk.media[0], 0x77);
    EXPECT_EQ(bh->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
    brelse(bh);
}

TEST_F(BufferCacheAsync, FileDataSyncPreservesJournalHeldMetadata) {
    auto* data = dirty(0);
    data->flags |= BH_FILE_DATA;
    auto* metadata = dirty(2);
    bjournal_hold(metadata);
    EXPECT_EQ(sync_blockdev_file_data(&dev), 0);
    EXPECT_EQ(data->flags & BH_DIRTY, 0U);
    EXPECT_NE(metadata->flags & BH_DIRTY, 0U);
    EXPECT_EQ(disk.submissions, 1U);
    bjournal_release(metadata);
    EXPECT_EQ(sync_blockdev(&dev), 0);
    brelse(data);
    brelse(metadata);
}

TEST_F(BufferCacheAsync, SynchronousWriteCannotOvertakePreparedNativeSnapshot) {
    auto* bh = dirty(0, 1, 0x11);
    disk.blocked = true;
    std::atomic<int> synchronous{-999};
    std::thread writeback([&] { EXPECT_EQ(sync_blockdev(&dev), 0); });
    {
        std::unique_lock guard(disk.mutex);
        ASSERT_TRUE(disk.cv.wait_for(guard, std::chrono::seconds(2), [&] { return disk.submissions == 1; }));
    }
    bh->data[0] = 0x99;
    bdirty(bh);
    std::thread direct([&] { synchronous.store(bwrite(bh)); });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(synchronous.load(), -999);
    {
        std::lock_guard guard(disk.mutex);
        disk.blocked = false;
        disk.cv.notify_all();
    }
    writeback.join();
    direct.join();
    EXPECT_EQ(synchronous.load(), 0);
    EXPECT_EQ(disk.media[0], 0x99);
    EXPECT_EQ(bh->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
    brelse(bh);
}

TEST_F(BufferCacheAsync, RangeBarrierPropagatesUnsupportedFlushEvenWithNoDirtyData) {
    dev.flush = nullptr;
    EXPECT_EQ(sync_bdev_range(&dev, 0, 8), -EOPNOTSUPP);
}

TEST_F(BufferCacheAsync, DemandReadSlicesCompleteBeforeCachePublicationAndOverlayDirtyAliases) {
    disk.observe_descriptor_heap = true;
    disk.limit_bytes = 4096;
    disk.media.fill(0x33);
    auto* alias = dirty(2, 1, 0x88);
    auto* span = bread_multi(&dev, 0, 64, BufferReadClass::FILE_DATA);
    ASSERT_NE(span, nullptr);
    EXPECT_EQ(disk.peak, 8U);
    EXPECT_EQ(disk.descriptor_allocations, 1U);
    EXPECT_EQ(disk.submissions, 8U);
    EXPECT_EQ(span->flags & BH_VALID, BH_VALID);
    EXPECT_EQ(span->data[0], 0x33);
    EXPECT_EQ(span->data[2 * 512], 0x88);
    EXPECT_EQ(span->data[3 * 512], 0x33);
    auto* hit = bread_multi(&dev, 0, 64, BufferReadClass::FILE_DATA);
    EXPECT_EQ(hit, span);
    EXPECT_EQ(disk.submissions, 8U);
    brelse(hit);
    brelse(span);
    brelse(alias);
}

TEST_F(BufferCacheAsync, FailedDemandReadRetiresEverySliceWithoutPublishingCacheEntry) {
    disk.limit_bytes = 4096;
    disk.short_completion = true;
    EXPECT_EQ(bread_multi(&dev, 0, 64, BufferReadClass::FILE_DATA), nullptr);
    EXPECT_EQ(disk.submissions, 8U);
    EXPECT_FALSE(has_cached_bdev_range(&dev, 0, 64));
    disk.short_completion = false;
    auto* good = bread_multi(&dev, 0, 64, BufferReadClass::FILE_DATA);
    ASSERT_NE(good, nullptr);
    EXPECT_EQ(disk.submissions, 16U);
    brelse(good);
}

TEST_F(BufferCacheAsync, EmptyWritebackDoesNotAttemptARejectedDescriptorAllocation) {
    phys::cache_test::fail_metadata = true;
    disk.observe_descriptor_heap = true;
    disk.fail_descriptor_heap = true;
    int const RC = sync_blockdev(&dev);
    size_t const ATTEMPTS = buffer_cache_heap_test::allocations;
    buffer_cache_heap_test::enabled = false;
    EXPECT_EQ(RC, 0);
    EXPECT_EQ(ATTEMPTS, 0U);
    EXPECT_EQ(phys::cache_test::metadata_attempts, 0U);
    EXPECT_EQ(disk.submissions, 0U);
    EXPECT_EQ(disk.flushes, 1U);
}

TEST_F(BufferCacheAsync, SoleFourKiBSnapshotAvoidsEmptyRefillAndImmediatelyJoins) {
    auto* bh = dirty(0, 8, 0x63);
    disk.observe_descriptor_heap = true;
    phys::cache_test::fail_metadata = true;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(disk.descriptor_allocations, 1U);
    EXPECT_EQ(phys::cache_test::metadata_attempts, 1U);
    EXPECT_EQ(disk.immediate_write_attempts, 1U);
    EXPECT_EQ(disk.ordinary_write_attempts, 0U);
    EXPECT_EQ(disk.submissions, 1U);
    EXPECT_EQ(disk.releases, 1U);
    EXPECT_EQ(disk.peak, 1U);
    EXPECT_EQ(disk.flushes, 1U);
    EXPECT_EQ(bh->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
    EXPECT_EQ(bh->refcount.load(), 1);
    EXPECT_TRUE(std::all_of(disk.media.begin(), disk.media.begin() + 4096, [](uint8_t byte) { return byte == 0x63; }));
    brelse(bh);
}

TEST_F(BufferCacheAsync, TwoPreparedRunsRetainOrdinaryWakesEvenWhenCompletedBeforeWait) {
    auto* first = dirty(0, 8, 0x61);
    auto* second = dirty(16, 8, 0x72);
    disk.complete_on_submit = true;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(disk.ordinary_write_attempts, 2U);
    EXPECT_EQ(disk.immediate_write_attempts, 0U);
    EXPECT_EQ(disk.submissions, 2U);
    EXPECT_EQ(disk.releases, 2U);
    EXPECT_EQ(first->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
    EXPECT_EQ(second->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
    EXPECT_EQ(first->refcount.load(), 1);
    EXPECT_EQ(second->refcount.load(), 1);
    EXPECT_EQ(disk.media[0], 0x61);
    EXPECT_EQ(disk.media[8192], 0x72);
    brelse(first);
    brelse(second);
}

TEST_F(BufferCacheAsync, SoleCompletedBeforeWaitStillReleasesRetainedSnapshot) {
    auto* bh = dirty(0, 8, 0x83);
    disk.complete_on_submit = true;
    phys::cache_test::free_probe = observe_snapshot_free;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(disk.immediate_write_attempts, 1U);
    EXPECT_EQ(disk.ordinary_write_attempts, 0U);
    EXPECT_EQ(disk.releases, 1U);
    EXPECT_EQ(disk.free_probes, 1U);
    EXPECT_EQ(bh->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
    EXPECT_EQ(bh->refcount.load(), 1);
    phys::cache_test::free_probe = nullptr;
    brelse(bh);
}

TEST_F(BufferCacheAsync, SoleRejectedAdmissionAndReleaseErrorRetainDirtyGeneration) {
    auto* bh = dirty(0, 8);
    disk.admission_status = -ESHUTDOWN;
    EXPECT_EQ(sync_blockdev(&dev), -ESHUTDOWN);
    EXPECT_EQ(disk.immediate_write_attempts, 1U);
    EXPECT_EQ(disk.submissions, 0U);
    EXPECT_EQ(disk.releases, 0U);
    EXPECT_NE(bh->flags & BH_DIRTY, 0U);
    EXPECT_EQ(bh->flags & BH_WRITEBACK, 0U);
    EXPECT_EQ(bh->refcount.load(), 1);
    disk.admission_status = 0;
    disk.release_status = -EIO;
    EXPECT_EQ(sync_blockdev(&dev), -EIO);
    EXPECT_EQ(disk.immediate_write_attempts, 2U);
    EXPECT_EQ(disk.submissions, 1U);
    EXPECT_EQ(disk.releases, 1U);
    EXPECT_NE(bh->flags & BH_DIRTY, 0U);
    EXPECT_EQ(bh->flags & BH_WRITEBACK, 0U);
    EXPECT_EQ(bh->refcount.load(), 1);
    disk.release_status = 0;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(bh->flags & BH_DIRTY, 0U);
    brelse(bh);
}

TEST_F(BufferCacheAsync, SoleBackpressureTimeoutTransfersNoRequestOwnership) {
    auto* bh = dirty(0, 8);
    disk.fail_admissions = std::numeric_limits<size_t>::max();
    disk.capacity_advance_us = 31'000'000;
    cache_test::hooks.service = fake_capacity_wait;
    ker::mod::ktime::cache_test::clock_us = fake_clock_us;
    EXPECT_EQ(sync_blockdev(&dev), -ETIMEDOUT);
    EXPECT_EQ(disk.immediate_write_attempts, 2U);
    EXPECT_EQ(disk.ordinary_write_attempts, 0U);
    EXPECT_EQ(disk.capacity_waits, 1U);
    EXPECT_EQ(disk.submissions, 0U);
    EXPECT_EQ(disk.releases, 0U);
    EXPECT_NE(bh->flags & BH_DIRTY, 0U);
    EXPECT_EQ(bh->flags & BH_WRITEBACK, 0U);
    EXPECT_EQ(bh->refcount.load(), 1);
    brelse(bh);
}

TEST_F(BufferCacheAsync, BatchAdmissionErrorsDoNotSwitchRemainingPreparedRunToNoWake) {
    auto* first = dirty(0, 8);
    auto* second = dirty(16, 8);
    disk.admission_status = -ESHUTDOWN;
    EXPECT_EQ(sync_blockdev(&dev), -ESHUTDOWN);
    EXPECT_EQ(disk.ordinary_write_attempts, 2U);
    EXPECT_EQ(disk.immediate_write_attempts, 0U);
    EXPECT_EQ(disk.submissions, 0U);
    EXPECT_EQ(disk.releases, 0U);
    EXPECT_EQ(first->flags & BH_WRITEBACK, 0U);
    EXPECT_EQ(second->flags & BH_WRITEBACK, 0U);
    EXPECT_NE(first->flags & BH_DIRTY, 0U);
    EXPECT_NE(second->flags & BH_DIRTY, 0U);
    brelse(first);
    brelse(second);
}

TEST_F(BufferCacheAsync, ForeignActiveRunStillJoinsWithoutAllocatingAnEmptyDescriptor) {
    auto* bh = dirty(0, 8);
    disk.blocked = true;
    std::atomic<int> first{-999};
    std::atomic<int> second{-999};
    std::atomic<size_t> joining_allocations{SIZE_MAX};
    size_t metadata_before_join{};
    std::thread worker([&] { first.store(sync_blockdev(&dev)); });
    {
        std::unique_lock guard(disk.mutex);
        bool const SUBMITTED = disk.cv.wait_for(guard, std::chrono::seconds(2), [&] { return disk.submissions == 1; });
        EXPECT_TRUE(SUBMITTED);
        if (!SUBMITTED) {
            disk.blocked = false;
            disk.cv.notify_all();
            guard.unlock();
            worker.join();
            brelse(bh);
            return;
        }
    }
    disk.observe_descriptor_heap = true;
    disk.fail_descriptor_heap = true;
    metadata_before_join = phys::cache_test::metadata_attempts;
    std::thread joining([&] {
        int const RC = sync_blockdev(&dev);
        joining_allocations.store(buffer_cache_heap_test::allocations);
        buffer_cache_heap_test::enabled = false;
        second.store(RC);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(second.load(), -999);
    {
        std::lock_guard guard(disk.mutex);
        EXPECT_EQ(disk.submissions, 1U);
        EXPECT_EQ(disk.flushes, 0U);
        disk.blocked = false;
        disk.cv.notify_all();
    }
    worker.join();
    joining.join();
    EXPECT_EQ(first.load(), 0);
    EXPECT_EQ(second.load(), 0);
    EXPECT_EQ(phys::cache_test::metadata_attempts, metadata_before_join);
    EXPECT_EQ(joining_allocations.load(), 0U);
    EXPECT_EQ(disk.immediate_write_attempts, 1U);
    EXPECT_EQ(disk.releases, 1U);
    EXPECT_EQ(disk.flushes, 2U);
    EXPECT_EQ(bh->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
    EXPECT_EQ(bh->refcount.load(), 1);
    brelse(bh);
}

TEST_F(BufferCacheAsync, JournalHeldWorkRemainsDirtyWithoutMarkerOrRequestAdmission) {
    auto* bh = dirty(0, 8);
    bjournal_hold(bh);
    disk.observe_descriptor_heap = true;
    disk.fail_descriptor_heap = true;
    int const RC = sync_blockdev(&dev);
    size_t const ATTEMPTS = buffer_cache_heap_test::allocations;
    buffer_cache_heap_test::enabled = false;
    EXPECT_EQ(RC, 0);
    EXPECT_EQ(ATTEMPTS, 0U);
    EXPECT_EQ(disk.submissions, 0U);
    EXPECT_NE(bh->flags & BH_DIRTY, 0U);
    EXPECT_EQ(bh->flags & BH_WRITEBACK, 0U);
    bjournal_release(bh);
    disk.observe_descriptor_heap = false;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(bh->flags & BH_DIRTY, 0U);
    brelse(bh);
}

TEST_F(BufferCacheAsync, OverlappingJournalAliasStillRequiresCollectorRecheck) {
    auto* data = dirty(0, 8, 0x45);
    data->flags |= BH_FILE_DATA;
    auto* metadata = dirty(2, 1, 0x92);
    bjournal_hold(metadata);
    disk.observe_descriptor_heap = true;
    phys::cache_test::fail_metadata = true;
    int const RC = sync_blockdev_file_data(&dev);
    size_t const ATTEMPTS = buffer_cache_heap_test::allocations;
    buffer_cache_heap_test::enabled = false;
    EXPECT_EQ(RC, 0);
    EXPECT_EQ(ATTEMPTS, 1U);
    EXPECT_EQ(disk.submissions, 0U);
    EXPECT_EQ(data->flags & BH_WRITEBACK, 0U);
    EXPECT_NE(data->flags & BH_DIRTY, 0U);
    EXPECT_NE(metadata->flags & BH_DIRTY, 0U);
    bjournal_release(metadata);
    disk.observe_descriptor_heap = false;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(disk.media[1024], 0x92);
    EXPECT_EQ(data->flags & BH_DIRTY, 0U);
    EXPECT_EQ(metadata->flags & BH_DIRTY, 0U);
    brelse(data);
    brelse(metadata);
}

TEST_F(BufferCacheAsync, DirtyIndexAllocationFailureRetainsHashFallbackSelectionAndJoin) {
    // Dirty states use ordinary new; their SmallVec growth is a fallible array
    // allocation. Prepare range-index state first, then reject only that dirty
    // vector growth. All selection and retirement still use the real cache.
    static std::array<BlockDevice, 65> isolated;
    std::array<BufHead*, isolated.size()> buffers{};
    for (size_t i = 0; i < isolated.size(); ++i) {
        isolated[i] = dev;
        buffers[i] = bget_multi(&isolated[i], 0, 8);
        ASSERT_NE(buffers[i], nullptr);
        memset(buffers[i]->data, 0x56, buffers[i]->size);
        buffers[i]->flags |= BH_VALID;
    }
    size_t target = isolated.size();
    for (size_t i = 0; i < isolated.size(); ++i) {
        buffer_cache_heap_test::allocations = 0;
        buffer_cache_heap_test::enabled = true;
        buffer_cache_heap_test::fail = true;
        bdirty(buffers[i]);
        buffer_cache_heap_test::enabled = false;
        if (buffer_cache_heap_test::allocations != 0) {
            target = i;
            break;
        }
    }
    EXPECT_LT(target, isolated.size());
    if (target == isolated.size()) target = isolated.size() - 1;
    auto* bh = buffers[target];
    EXPECT_EQ(sync_blockdev(&isolated[target]), 0);
    EXPECT_EQ(disk.immediate_write_attempts, 1U);
    EXPECT_EQ(disk.releases, 1U);
    EXPECT_EQ(disk.flushes, 1U);
    EXPECT_EQ(bh->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
    EXPECT_EQ(bh->refcount.load(), 1);
    EXPECT_EQ(disk.media[0], 0x56);
    for (size_t i = 0; i < isolated.size(); ++i) {
        EXPECT_EQ(sync_blockdev(&isolated[i]), 0);
        brelse(buffers[i]);
        invalidate_bdev(&isolated[i]);
    }
}

TEST_F(BufferCacheAsync, PhysicalDescriptorsRetireAcrossRefillsWithoutHeapAllocation) {
    std::array<BufHead*, 12> buffers{};
    for (size_t index = 0; index < buffers.size(); ++index) buffers[index] = dirty(index * 16, 8);
    disk.observe_descriptor_heap = true;
    phys::cache_test::metadata_allocation_probe = observe_metadata_allocation;
    phys::cache_test::metadata_free_probe = observe_metadata_free;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(phys::cache_test::metadata_attempts, buffers.size());
    EXPECT_EQ(phys::cache_test::metadata_frees, buffers.size());
    EXPECT_EQ(disk.metadata_probes, buffers.size());
    EXPECT_EQ(disk.descriptor_allocations, 0U);
    EXPECT_EQ(disk.peak, 8U);
    EXPECT_EQ(disk.submissions, buffers.size());
    EXPECT_EQ(disk.releases, buffers.size());
    for (auto* bh : buffers) {
        EXPECT_EQ(bh->refcount.load(), 1);
        EXPECT_EQ(bh->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
        EXPECT_EQ(disk.media[bh->block_no * 512], 0x5a);
        brelse(bh);
    }
}

TEST_F(BufferCacheAsync, PhysicalFailureUsesHeapAndRetiresFailedGeneration) {
    auto* bh = dirty(0, 8, 0x6b);
    phys::cache_test::fail_metadata = true;
    disk.observe_descriptor_heap = true;
    disk.short_completion = true;
    EXPECT_EQ(sync_blockdev(&dev), -EIO);
    EXPECT_EQ(phys::cache_test::metadata_attempts, 1U);
    EXPECT_EQ(phys::cache_test::metadata_frees, 0U);
    EXPECT_EQ(disk.descriptor_allocations, 1U);
    EXPECT_EQ(disk.submissions, disk.releases);
    EXPECT_NE(bh->flags & BH_DIRTY, 0U);
    EXPECT_EQ(bh->flags & BH_WRITEBACK, 0U);
    EXPECT_EQ(bh->refcount.load(), 1);
    disk.short_completion = false;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(phys::cache_test::metadata_attempts, 2U);
    EXPECT_EQ(bh->flags & BH_DIRTY, 0U);
    brelse(bh);
}

TEST_F(BufferCacheAsync, BothDescriptorAllocatorsFailBeforeSerialProgress) {
    auto* bh = dirty(0, 8, 0x4d);
    phys::cache_test::fail_metadata = true;
    disk.observe_descriptor_heap = true;
    disk.fail_descriptor_heap = true;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    size_t const HEAP_ATTEMPTS = buffer_cache_heap_test::allocations;
    buffer_cache_heap_test::enabled = false;
    EXPECT_EQ(HEAP_ATTEMPTS, 1U);
    EXPECT_EQ(phys::cache_test::metadata_attempts, 1U);
    EXPECT_EQ(phys::cache_test::metadata_frees, 0U);
    EXPECT_EQ(disk.submissions, 0U);
    EXPECT_EQ(disk.releases, 0U);
    EXPECT_EQ(disk.legacy_writes, 1U);
    EXPECT_EQ(disk.media[0], 0x4d);
    EXPECT_EQ(bh->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
    EXPECT_EQ(bh->refcount.load(), 1);
    brelse(bh);
}

TEST_F(BufferCacheAsync, PhysicalCollectorRejectionRetiresWithoutAdmission) {
    auto* data = dirty(0, 8, 0x45);
    data->flags |= BH_FILE_DATA;
    auto* metadata = dirty(2, 1, 0x92);
    bjournal_hold(metadata);
    disk.observe_descriptor_heap = true;
    phys::cache_test::metadata_free_probe = observe_metadata_free;
    EXPECT_EQ(sync_blockdev_file_data(&dev), 0);
    EXPECT_EQ(phys::cache_test::metadata_attempts, 1U);
    EXPECT_EQ(phys::cache_test::metadata_frees, 1U);
    EXPECT_EQ(buffer_cache_heap_test::allocations, 0U);
    buffer_cache_heap_test::enabled = false;
    EXPECT_EQ(disk.submissions, 0U);
    EXPECT_EQ(data->flags & BH_WRITEBACK, 0U);
    EXPECT_NE(data->flags & BH_DIRTY, 0U);
    EXPECT_EQ(data->refcount.load(), 1);
    bjournal_release(metadata);
    disk.observe_descriptor_heap = false;
    EXPECT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(disk.media[1024], 0x92);
    brelse(data);
    brelse(metadata);
}
}  // namespace
