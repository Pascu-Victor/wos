// The complete production read function is compiled below. The existing cache
// fixture supplies the real cache and request-retirement boundary; only XFS
// mapping, task context and mount ownership are hosted seams.
#include <mod/io/serial/serial.hpp>
#include <platform/mm/addr.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sched/scheduler.hpp>

#include "buffer_cache_async_test.cpp"

namespace xfs_read_test {
inline thread_local unsigned io_depth{};
inline size_t allocations{};
inline size_t reject_transfer_above{};
inline bool available{true};
inline unsigned depth{8};
inline size_t max_requests{64};
inline bool virtual_tail{};
inline std::vector<size_t> legacy_transfers;
inline bool fail_one_aggregate_slice{};
inline bool record_allocation_sizes{};
inline std::vector<size_t> allocation_sizes;
inline std::vector<size_t> pending_bytes_at_wait;
inline std::vector<uint8_t> virtual_written;
inline const void* dirty_live_data{};
inline size_t serial_writes{};
struct IoMutex {
    std::mutex mutex;
    void lock() {
        mutex.lock();
        ++io_depth;
    }
    void unlock() {
        --io_depth;
        mutex.unlock();
    }
};
void allocation_probe(size_t bytes) {
    ++allocations;
    if (record_allocation_sizes) allocation_sizes.push_back(bytes);
}
auto available_probe(const ker::dev::BlockDevice* dev) -> bool { return available && fake_available(dev); }
auto limits_probe(const ker::dev::BlockDevice* dev) -> ker::dev::BlockQueueLimits {
    auto result = fake_limits(dev);
    result.depth = depth;
    result.max_requests = max_requests;
    return result;
}
auto submit_probe(ker::dev::BlockDevice* dev, ker::dev::BlockRequest& request, ker::dev::BlockRequestOperation op, uint64_t lba,
                  size_t count, std::span<const ker::dev::BlockSegment> segments) -> int {
    EXPECT_GT(io_depth, 0U);
    if (reject_transfer_above != 0 && count * dev->block_size > reject_transfer_above) return -EIO;
    return fake_submit(dev, request, op, lba, count, segments);
}
auto wait_probe(ker::dev::BlockRequest& request) -> int {
    EXPECT_GT(io_depth, 0U);
    if (virtual_tail) {
        std::lock_guard guard(active_disk->mutex);
        for (auto* pending : active_disk->pending) {
            if (pending->description.op == ker::dev::BlockRequestOperation::READ &&
                pending->description.lba * 512 + pending->description.bytes > active_disk->media.size()) {
                std::memset(pending->segments[0].data, 0x5a, pending->description.bytes);
            }
        }
    }
    if (fail_one_aggregate_slice && active_disk->pending.size() == 8) {
        // Complete one future slice with EIO while the remaining accepted
        // requests remain live. The real cache must drain them before retry.
        std::lock_guard guard(active_disk->mutex);
        size_t bytes = 0;
        for (auto* pending : active_disk->pending) bytes += pending->description.bytes;
        pending_bytes_at_wait.push_back(bytes);
        active_disk->completion_status = -EIO;
        complete_one(*active_disk);
        active_disk->completion_status = 0;
        fail_one_aggregate_slice = false;
    } else {
        std::lock_guard guard(active_disk->mutex);
        size_t bytes = 0;
        for (auto* pending : active_disk->pending) bytes += pending->description.bytes;
        pending_bytes_at_wait.push_back(bytes);
    }
    return fake_wait(request);
}
auto release_probe(ker::dev::BlockRequest& request) -> int {
    EXPECT_GT(io_depth, 0U);
    return fake_release(request);
}
auto read_probe(ker::dev::BlockDevice* dev, uint64_t lba, size_t count, void* data) -> int {
    EXPECT_GT(io_depth, 0U);
    legacy_transfers.push_back(count * dev->block_size);
    return fake_read(dev, lba, count, data);
}
auto write_probe(ker::dev::BlockDevice* dev, uint64_t lba, size_t count, const void* data) -> int {
    EXPECT_GT(reclaim::cache_test::current_task.reclaim_no_io_depth, 0U);
    EXPECT_TRUE(active_disk->pending.empty());
    if (dirty_live_data != nullptr) EXPECT_NE(data, dirty_live_data);
    ++serial_writes;
    if (virtual_tail) {
        auto* bytes = static_cast<const uint8_t*>(data);
        virtual_written.assign(bytes, bytes + count * dev->block_size);
    }
    return fake_write(dev, lba, count, data);
}
}  // namespace xfs_read_test

namespace ker::mod::sys {
template <typename Mutex>
struct MutexGuard {
    Mutex& mutex;
    explicit MutexGuard(Mutex& value) : mutex(value) { mutex.lock(); }
    ~MutexGuard() { mutex.unlock(); }
};
}  // namespace ker::mod::sys

namespace ker::vfs {
constexpr unsigned FILE_ACCESS_SEARCH_ONLY = 3;
struct File {
    int open_flags{};
    void* private_data{};
};
namespace xfs {
using xfs_fileoff_t = uint64_t;
using xfs_fsblock_t = uint64_t;
using xfs_filblks_t = uint64_t;
using xfs_agnumber_t = uint32_t;
using xfs_agblock_t = uint32_t;
constexpr uint64_t NULLFSBLOCK = UINT64_MAX;
constexpr int XFS_DINODE_FMT_LOCAL = 1;
constexpr int XFS_DINODE_FMT_EXTENTS = 2;
struct XfsMountContext {
    ker::dev::BlockDevice* device{};
    uint32_t block_size{4096};
    uint32_t block_log{12};
    uint64_t total_blocks{128};
    uint32_t ag_count{1};
    uint32_t ag_blocks{128};
    uint32_t ag_blk_log{7};
};
struct XfsBmapResult {
    uint64_t startblock{};
    uint64_t blockcount{};
    bool is_hole{};
    bool unwritten{};
};
struct Mapping {
    uint64_t logical{};
    uint64_t physical{};
    uint64_t blocks{};
    bool hole{};
    bool unwritten{};
};
struct XfsInode {
    uint64_t ino{1};
    uint64_t size{512 * 1024};
    uint64_t nblocks{128};
    struct {
        int format{XFS_DINODE_FMT_EXTENTS};
        struct {
            uint8_t* data{};
        } local;
    } data_fork;
    xfs_read_test::IoMutex io_lock;
    bool directory{};
    bool regular{true};
    std::vector<Mapping> mappings{{0, 0, 128, false, false}};
    uint64_t fail_lookup{UINT64_MAX};
};
struct XfsFileData {
    XfsMountContext* mount{};
    XfsInode* inode{};
    uint64_t read_next_offset{};
    bool read_sequence_valid{};
};
auto xfs_inode_isdir(const XfsInode* inode) -> bool { return inode->directory; }
auto xfs_inode_isreg(const XfsInode* inode) -> bool { return inode->regular; }
auto xfs_ag_number(uint64_t block, unsigned shift) -> uint32_t { return static_cast<uint32_t>(block >> shift); }
auto xfs_ag_block(uint64_t block, unsigned shift) -> uint32_t { return static_cast<uint32_t>(block & ((uint64_t{1} << shift) - 1)); }
auto xfs_bmap_lookup(XfsInode* inode, uint64_t logical, XfsBmapResult* result) -> int {
    EXPECT_GT(xfs_read_test::io_depth, 0U);
    if (logical == inode->fail_lookup) return -EIO;
    for (const auto& map : inode->mappings) {
        if (logical >= map.logical && logical - map.logical < map.blocks) {
            uint64_t const displacement = logical - map.logical;
            *result = {map.hole ? NULLFSBLOCK : map.physical + displacement, map.blocks - displacement, map.hole, map.unwritten};
            return 0;
        }
    }
    *result = {};
    return 0;
}
auto xfs_buf_read_data(XfsMountContext* mount, uint64_t block) -> BufHead* {
    uint64_t const linear =
        static_cast<uint64_t>(xfs_ag_number(block, mount->ag_blk_log)) * mount->ag_blocks + xfs_ag_block(block, mount->ag_blk_log);
    return bread_multi(mount->device, linear * 8, 8, BufferReadClass::FILE_DATA);
}
auto perf_xfs_started_us(ker::mod::perf::WkiPerfLocalXfsOp) -> uint64_t { return 0; }
auto perf_elapsed_since_us(uint64_t) -> uint32_t { return 0; }
auto perf_record_xfs_stage_elapsed(ker::mod::perf::WkiPerfLocalXfsOp, uint64_t, int, uint64_t) -> uint32_t { return 0; }
void perf_record_xfs_slow_event(ker::mod::perf::WkiPerfLocalXfsOp, int, uint32_t, uint64_t, uint64_t) {}
namespace log {
template <typename... Args>
void warn(const char*, Args...) {}
}  // namespace log
#ifndef WOS_PERF_CALLSITE
#define WOS_PERF_CALLSITE() 0
#endif
#include "actual_xfs_read.inc"
}  // namespace xfs
}  // namespace ker::vfs

namespace {
namespace xfs = ker::vfs::xfs;
class XfsRead : public BufferCacheAsync {
   protected:
    xfs::XfsMountContext mount;
    xfs::XfsInode inode;
    xfs::XfsFileData data;
    ker::vfs::File file;
    std::vector<uint8_t> output = std::vector<uint8_t>(512 * 1024, 0xcc);
    void SetUp() override {
        BufferCacheAsync::SetUp();
        mount.device = &dev;
        data.mount = &mount;
        data.inode = &inode;
        file.private_data = &data;
        disk.media.fill(0x5a);
        ker::mod::sched::xfs_read_test::queryable = true;
        ker::mod::sched::xfs_read_test::runqueues = true;
        ker::mod::sched::xfs_read_test::shutdown = false;
        auto& task = ker::mod::sched::xfs_read_test::current;
        task.type = ker::mod::sched::task::TaskType::PROCESS;
        task.pid = 1;
        task.preempt_disable_depth = 0;
        task.syscall_account_start_us = 1;
        task.scheduler_transition_active = false;
        task.deferred_task_switch = task.yield_switch = task.wants_block = task.voluntary_blocked = false;
        task.wait_channel = nullptr;
        task.wait_channel_kind = ker::mod::sched::task::WaitChannelKind::NONE;
        task.wki_proxy_task_id = 0;
        task.interrupting_signal_pending = false;
        ker::mod::mm::addr::test_hhdm_offset = 4096;
        ker::mod::io::serial::test_panic = false;
        xfs_read_test::allocations = xfs_read_test::reject_transfer_above = 0;
        xfs_read_test::available = true;
        xfs_read_test::depth = 8;
        xfs_read_test::max_requests = 64;
        xfs_read_test::virtual_tail = false;
        xfs_read_test::fail_one_aggregate_slice = false;
        xfs_read_test::record_allocation_sizes = false;
        xfs_read_test::allocation_sizes.clear();
        xfs_read_test::pending_bytes_at_wait.clear();
        xfs_read_test::virtual_written.clear();
        xfs_read_test::dirty_live_data = nullptr;
        xfs_read_test::serial_writes = 0;
        xfs_read_test::legacy_transfers.clear();
        dev.read_blocks = xfs_read_test::read_probe;
        dev.write_blocks = xfs_read_test::write_probe;
        phys::cache_test::reject_size = 0;
        phys::cache_test::allocation_probe = xfs_read_test::allocation_probe;
        cache_test::hooks = {xfs_read_test::available_probe, xfs_read_test::limits_probe,  xfs_read_test::submit_probe,
                             xfs_read_test::wait_probe,      xfs_read_test::release_probe, nullptr};
    }
    void TearDown() override {
        EXPECT_EQ(xfs_read_test::io_depth, 0U);
        phys::cache_test::reject_size = 0;
        BufferCacheAsync::TearDown();
    }
    auto read(size_t bytes, size_t offset = 0) -> ssize_t { return xfs::xfs_vfs_read(&file, output.data(), bytes, offset); }
    auto cache(size_t bytes, uint64_t block = 0) -> BufHead* {
        ker::mod::sys::MutexGuard guard(inode.io_lock);
        return bread_multi(&dev, block, bytes / 512, BufferReadClass::FILE_DATA);
    }
    void virtual_extent(size_t bytes = 4 * 1024 * 1024 + 128 * 1024) {
        inode.size = bytes;
        inode.mappings = {{0, 0, bytes / 4096, false, false}};
        mount.ag_blocks = 16384;
        mount.ag_blk_log = 14;
        mount.total_blocks = 16384;
        dev.total_blocks = 16384 * 8;
        xfs_read_test::virtual_tail = true;
    }
    void expect_bytes(size_t bytes, uint8_t value = 0x5a) {
        EXPECT_TRUE(
            std::all_of(output.begin(), output.begin() + static_cast<ptrdiff_t>(bytes), [value](uint8_t byte) { return byte == value; }));
    }
};

TEST_F(XfsRead, InitialDemandReadsExactBytesAndRetires) {
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024}));
    EXPECT_EQ(disk.submissions, disk.releases);
}
TEST_F(XfsRead, WarmLargerAliasAvoidsAllocationAndIo) {
    BufHead* bh = cache(512 * 1024);
    ASSERT_NE(bh, nullptr);
    brelse(bh);
    auto const submissions = disk.submissions;
    xfs_read_test::allocations = 0;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.submissions, submissions);
#if defined(XFS_EXPECT_ORIGINAL) && !defined(XFS_EXPECT_NO_ALLOCATION)
    EXPECT_GT(xfs_read_test::allocations, 0U);
#else
    EXPECT_EQ(xfs_read_test::allocations, 0U);
#endif
}
TEST_F(XfsRead, PartialCacheCoverageOverwritesEntireDemand) {
    BufHead* bh = cache(4096);
    ASSERT_NE(bh, nullptr);
    brelse(bh);
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.transfer_bytes.back(), 128 * 1024);
}
TEST_F(XfsRead, DirtySubrangeRemainsAuthoritative) {
    BufHead* bh = cache(512 * 1024);
    ASSERT_NE(bh, nullptr);
    brelse(bh);
    BufHead* modified = dirty(16, 8, 0x7b);
    brelse(modified);
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    for (size_t index = 0; index < 128 * 1024; ++index) {
        ASSERT_EQ(output[index], index >= 8192 && index < 12288 ? 0x7b : 0x5a) << index;
    }
}
TEST_F(XfsRead, RetiredAliasCannotServeReusedExtent) {
    BufHead* held = cache(512 * 1024);
    ASSERT_NE(held, nullptr);
    retire_bdev_range(&dev, 0, 1024);
    disk.media.fill(0x6f);
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024, 0x6f);
    brelse(held);
}
TEST_F(XfsRead, HoleUnalignedAndEofPreserveByteBounds) {
    inode.mappings = {{0, 0, 1, true, false}, {1, 1, 127, false, false}};
    ASSERT_EQ(read(6000, 100), 6000);
    EXPECT_TRUE(std::all_of(output.begin(), output.begin() + 3996, [](uint8_t byte) { return byte == 0; }));
    EXPECT_TRUE(std::all_of(output.begin() + 3996, output.begin() + 6000, [](uint8_t byte) { return byte == 0x5a; }));
    inode.size = 4100;
    ASSERT_EQ(read(4096, 4096), 4);
    expect_bytes(4);
    EXPECT_EQ(read(10, 4100), 0);
}
TEST_F(XfsRead, LookupAndShortCompletionDoNotPublishUncertainBytes) {
    inode.fail_lookup = 0;
    EXPECT_EQ(read(128 * 1024), -EIO);
    EXPECT_EQ(disk.submissions, 0U);
    inode.fail_lookup = UINT64_MAX;
    disk.short_completion = true;
    EXPECT_EQ(read(128 * 1024), -EIO);
    EXPECT_EQ(disk.submissions, disk.releases);
}
TEST_F(XfsRead, EightSliceReadRetiresBeforePublication) {
    disk.limit_bytes = 32 * 1024;
    ASSERT_EQ(read(256 * 1024), 256 * 1024);
    expect_bytes(256 * 1024);
    EXPECT_EQ(disk.peak, 8U);
    EXPECT_EQ(disk.submissions, disk.releases);
}
TEST_F(XfsRead, PendingReadRetainsMappingLockUntilRequestRetirement) {
    disk.blocked = true;
    ssize_t result = -1;
    std::thread reader([&] { result = read(128 * 1024); });
    bool observed = false;
    {
        std::unique_lock guard(disk.mutex);
        observed = disk.cv.wait_for(guard, std::chrono::seconds(2), [&] { return !disk.pending.empty(); });
        if (observed) {
            EXPECT_EQ(disk.releases, 0U);
            EXPECT_EQ(output.front(), 0xcc);
            bool const acquired = inode.io_lock.mutex.try_lock();
            EXPECT_FALSE(acquired);
            if (acquired) inode.io_lock.mutex.unlock();
            for (auto* request : disk.pending) EXPECT_NE(request->segments[0].data, nullptr);
        }
        disk.blocked = false;
    }
    disk.cv.notify_all();
    reader.join();
    ASSERT_TRUE(observed);
    ASSERT_EQ(result, 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.submissions, disk.releases);
}
TEST_F(XfsRead, RetryableAdmissionDoesNotChargeRejectedRequestOwnership) {
    disk.fail_admissions = 2;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.submissions, 1U);
    EXPECT_EQ(disk.releases, 1U);
}
TEST_F(XfsRead, InvalidHandlesAndSearchOnlyAccessDoNotPerformIo) {
    EXPECT_EQ(xfs::xfs_vfs_read(nullptr, output.data(), 4096, 0), -EINVAL);
    EXPECT_EQ(xfs::xfs_vfs_read(&file, nullptr, 4096, 0), -EINVAL);
    file.open_flags = ker::vfs::FILE_ACCESS_SEARCH_ONLY;
    EXPECT_EQ(read(4096), -EBADF);
    file.open_flags = 0;
    inode.directory = true;
    EXPECT_EQ(read(4096), -EISDIR);
    inode.directory = false;
    file.private_data = nullptr;
    EXPECT_EQ(read(4096), -EBADF);
    EXPECT_EQ(disk.submissions, 0U);
}

#ifndef XFS_EXPECT_ORIGINAL
TEST_F(XfsRead, SequentialReadCachesBoundedFutureBytesUnderInodeLock) {
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 384 * 1024}));
    auto const submitted = disk.submissions;
    xfs_read_test::allocations = 0;
    ASSERT_EQ(read(128 * 1024, 256 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.submissions, submitted);
    EXPECT_EQ(xfs_read_test::allocations, 0U);
}
TEST_F(XfsRead, RandomReadHasNoSupplementalIo) {
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 256 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 128 * 1024}));
}
TEST_F(XfsRead, TinySequentialReadRemainsExact) {
    ASSERT_EQ(read(4096), 4096);
    ASSERT_EQ(read(4096, 4096), 4096);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{4096, 4096}));
}
TEST_F(XfsRead, SupplementalAllocationFailureRetriesExactDemand) {
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    phys::cache_test::reject_size = 384 * 1024;
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 128 * 1024}));
    EXPECT_EQ(disk.submissions, disk.releases);
}
TEST_F(XfsRead, SupplementalAdmissionFailureRetriesExactDemand) {
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    xfs_read_test::reject_transfer_above = 128 * 1024;
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 128 * 1024}));
    EXPECT_EQ(disk.submissions, disk.releases);
}
TEST_F(XfsRead, BothSupplementalAndDemandFailurePreserveErrorAndRetirement) {
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    disk.completion_status = -EIO;
    EXPECT_EQ(read(128 * 1024, 128 * 1024), -EIO);
    EXPECT_EQ(disk.submissions, disk.releases);
    EXPECT_TRUE(disk.pending.empty());
    EXPECT_FALSE(data.read_sequence_valid);
}
TEST_F(XfsRead, WholeBlockEofLimitsFutureIo) {
    inode.size = 300 * 1024 + 17;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    EXPECT_EQ(disk.transfer_bytes.back(), 172 * 1024);
    expect_bytes(128 * 1024);
}
TEST_F(XfsRead, CurrentExtentLimitsFutureIo) {
    inode.mappings = {{0, 0, 72, false, false}, {72, 90, 20, false, false}};
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    EXPECT_EQ(disk.transfer_bytes.back(), 160 * 1024);
}
TEST_F(XfsRead, SequentialHintCannotReusePreviousMappingAfterRetirement) {
    ASSERT_EQ(read(64 * 1024), 64 * 1024);
    {
        ker::mod::sys::MutexGuard guard(inode.io_lock);
        retire_bdev_range(&dev, 0, 1024);
        inode.mappings = {{0, 0, 16, false, false}, {16, 80, 16, false, false}};
        std::fill(disk.media.begin() + 80 * 4096, disk.media.begin() + 96 * 4096, 0x71);
    }
    ASSERT_EQ(read(64 * 1024, 64 * 1024), 64 * 1024);
    expect_bytes(64 * 1024, 0x71);
    EXPECT_EQ(disk.completed_lbas.back(), 640U);
    EXPECT_EQ(disk.transfer_bytes.back(), 64 * 1024);
}
TEST_F(XfsRead, DemandCoalescedAcrossMappingsIsNotTruncatedByNewPolicy) {
    inode.mappings = {{0, 0, 48, false, false}, {48, 48, 80, false, false}};
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    EXPECT_EQ(disk.transfer_bytes.back(), 128 * 1024);
    expect_bytes(128 * 1024);
}
TEST_F(XfsRead, SmallerLastAgLimitsFutureIoBeforeEncodedPadding) {
    mount.ag_count = 2;
    mount.ag_blocks = 64;
    mount.ag_blk_log = 6;
    mount.total_blocks = 104;
    inode.mappings = {{0, 64, 128, false, false}};
    ASSERT_EQ(read(64 * 1024), 64 * 1024);
    ASSERT_EQ(read(64 * 1024, 64 * 1024), 64 * 1024);
    EXPECT_EQ(disk.transfer_bytes.back(), 96 * 1024);
    EXPECT_EQ(disk.completed_lbas.back(), 640U);
    expect_bytes(64 * 1024);
}
TEST_F(XfsRead, ActualAgLengthLimitsEncodedNonPowerOfTwoGeometry) {
    mount.ag_blocks = 100;
    mount.ag_blk_log = 7;
    mount.total_blocks = 100;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    EXPECT_EQ(disk.transfer_bytes.back(), 272 * 1024);
}
TEST_F(XfsRead, ProviderTransferAndBudgetBoundsClipSupplementalIo) {
    disk.limit_bytes = 192 * 1024;
    disk.inflight_limit_bytes = 160 * 1024;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    EXPECT_EQ(disk.transfer_bytes.back(), 160 * 1024);
}
TEST_F(XfsRead, UnwrittenMappingsRetainExistingExactPhysicalReadBehavior) {
    inode.mappings.front().unwritten = true;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 128 * 1024}));
    expect_bytes(128 * 1024);
}
TEST_F(XfsRead, LowDestinationRemainsOnDemandPath) {
    ker::mod::mm::addr::test_hhdm_offset = UINT64_MAX;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 128 * 1024}));
}
TEST_F(XfsRead, SingleDepthProviderRemainsOnDemandPath) {
    xfs_read_test::depth = 1;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    EXPECT_EQ(xfs_read_test::legacy_transfers, (std::vector<size_t>{128 * 1024, 128 * 1024}));
}
TEST_F(XfsRead, LegacyProviderRemainsOnDemandPath) {
    xfs_read_test::available = false;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    EXPECT_EQ(disk.submissions, 0U);
    EXPECT_EQ(disk.legacy_reads, 2U);
    expect_bytes(128 * 1024);
}
TEST_F(XfsRead, UnsafeTaskContextsDoNotExpandDemand) {
    auto& task = ker::mod::sched::xfs_read_test::current;
    for (unsigned state = 0; state < 11; ++state) {
        SCOPED_TRACE(state);
        invalidate_bdev(&dev);
        disk.transfer_bytes.clear();
        data.read_sequence_valid = false;
        ker::mod::sched::xfs_read_test::queryable = state != 0;
        ker::mod::sched::xfs_read_test::runqueues = state != 1;
        ker::mod::sched::xfs_read_test::shutdown = state == 2;
        ker::mod::io::serial::test_panic = state == 3;
        task.type = state == 4 ? ker::mod::sched::task::TaskType::DAEMON : ker::mod::sched::task::TaskType::PROCESS;
        task.syscall_account_start_us = state == 5 ? 0 : 1;
        task.preempt_disable_depth = state == 6 ? 1 : 0;
        task.scheduler_transition_active = state == 7;
        task.deferred_task_switch = state == 8;
        task.wki_proxy_task_id = state == 9 ? 1 : 0;
        task.interrupting_signal_pending = state == 10;
        ASSERT_EQ(read(128 * 1024), 128 * 1024);
        ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
        EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 128 * 1024}));
    }
}
TEST_F(XfsRead, ChannelFreeReturnYieldPreservesFlagsAndAllowsBoundedRead) {
    auto& task = ker::mod::sched::xfs_read_test::current;
    task.deferred_task_switch = task.yield_switch = true;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    EXPECT_EQ(disk.transfer_bytes.back(), 384 * 1024);
    EXPECT_TRUE(task.deferred_task_switch);
    EXPECT_TRUE(task.yield_switch);
}
TEST_F(XfsRead, LargeDemandPreservesOriginalMultiSlicePipeline) {
    inode.size = 1024 * 1024;
    inode.mappings = {{0, 0, 256, false, false}};
    mount.ag_blocks = 256;
    mount.ag_blk_log = 8;
    mount.total_blocks = 256;
    dev.total_blocks = 2048;
    output.resize(1024 * 1024);
    xfs_read_test::virtual_tail = true;
    ASSERT_EQ(read(1024 * 1024), 1024 * 1024);
    expect_bytes(1024 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{512 * 1024, 512 * 1024}));
    EXPECT_EQ(disk.peak, 2U);
}
TEST_F(XfsRead, WholeDestinationBoundsRejectWrappedOrEmptySpan) {
    EXPECT_TRUE(xfs::xfs_read_kernel_destination_eligible(output.data(), output.size()));
    EXPECT_FALSE(xfs::xfs_read_kernel_destination_eligible(output.data(), 0));
    EXPECT_FALSE(xfs::xfs_read_kernel_destination_eligible(reinterpret_cast<void*>(UINT64_MAX - 7), 9));
    EXPECT_FALSE(xfs::xfs_read_kernel_destination_eligible(reinterpret_cast<void*>(4095), 32));
}
TEST_F(XfsRead, MalformedAgGeometryRetainsDemandInsteadOfSupplementing) {
    mount.ag_blocks = 129;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 128 * 1024}));
}
TEST_F(XfsRead, FourMiBAggregateUsesEightRetainedProviderSlices) {
    virtual_extent();
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    xfs_read_test::record_allocation_sizes = true;
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    ASSERT_EQ(disk.transfer_bytes.size(), 9U);
    EXPECT_EQ(disk.transfer_bytes.front(), 128 * 1024);
    EXPECT_TRUE(std::all_of(disk.transfer_bytes.begin() + 1, disk.transfer_bytes.end(), [](size_t bytes) { return bytes == 512 * 1024; }));
    EXPECT_EQ(disk.peak, 8U);
    EXPECT_EQ(disk.submissions, disk.releases);
    EXPECT_TRUE(disk.pending.empty());
    EXPECT_NE(std::find(xfs_read_test::allocation_sizes.begin(), xfs_read_test::allocation_sizes.end(), 4 * 1024 * 1024),
              xfs_read_test::allocation_sizes.end());
    EXPECT_TRUE(std::all_of(xfs_read_test::allocation_sizes.begin(), xfs_read_test::allocation_sizes.end(),
                            [](size_t bytes) { return bytes <= 4 * 1024 * 1024; }));
    auto const submitted = disk.submissions;
    xfs_read_test::allocations = 0;
    ASSERT_EQ(read(128 * 1024, 256 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.submissions, submitted);
    EXPECT_EQ(xfs_read_test::allocations, 0U);
}
TEST_F(XfsRead, FourMiBAggregateBackingFailureRetriesOnlyDemand) {
    virtual_extent();
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    phys::cache_test::reject_size = 4 * 1024 * 1024;
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 128 * 1024}));
    EXPECT_EQ(disk.submissions, disk.releases);
}
TEST_F(XfsRead, FullAggregateKeepsMappingLockAndDestinationUnpublishedUntilRetirement) {
    virtual_extent();
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    std::fill(output.begin(), output.end(), 0xcc);
    disk.blocked = true;
    ssize_t result = -1;
    std::thread reader([&] {
        auto& task = ker::mod::sched::xfs_read_test::current;
        task.type = ker::mod::sched::task::TaskType::PROCESS;
        task.syscall_account_start_us = 1;
        result = read(128 * 1024, 128 * 1024);
    });
    bool observed = false;
    {
        std::unique_lock guard(disk.mutex);
        observed = disk.cv.wait_for(guard, std::chrono::seconds(2), [&] { return disk.pending.size() == 8; });
        if (observed) {
            EXPECT_EQ(disk.submissions - disk.releases, 8U);
            size_t bytes = 0;
            for (auto* request : disk.pending) {
                EXPECT_NE(request->segments[0].data, nullptr);
                bytes += request->description.bytes;
            }
            EXPECT_EQ(bytes, 4 * 1024 * 1024);
            EXPECT_EQ(output.front(), 0xcc);
            bool const acquired = inode.io_lock.mutex.try_lock();
            EXPECT_FALSE(acquired);
            if (acquired) inode.io_lock.mutex.unlock();
        }
        disk.blocked = false;
    }
    disk.cv.notify_all();
    reader.join();
    ASSERT_TRUE(observed);
    ASSERT_EQ(result, 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.submissions, disk.releases);
}
TEST_F(XfsRead, FailedFutureSliceDrainsEntireAggregateBeforeExactRetry) {
    virtual_extent();
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    xfs_read_test::fail_one_aggregate_slice = true;
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    ASSERT_EQ(disk.transfer_bytes.size(), 10U);
    EXPECT_TRUE(
        std::all_of(disk.transfer_bytes.begin() + 1, disk.transfer_bytes.end() - 1, [](size_t bytes) { return bytes == 512 * 1024; }));
    EXPECT_EQ(disk.transfer_bytes.back(), 128 * 1024);
    EXPECT_EQ(disk.peak, 8U);
    EXPECT_EQ(disk.submissions, disk.releases);
    EXPECT_TRUE(disk.pending.empty());
    EXPECT_NE(std::find(xfs_read_test::pending_bytes_at_wait.begin(), xfs_read_test::pending_bytes_at_wait.end(), 4 * 1024 * 1024),
              xfs_read_test::pending_bytes_at_wait.end());
    EXPECT_EQ(xfs_read_test::pending_bytes_at_wait.back(), 128 * 1024);
}
TEST_F(XfsRead, AggregateRespectsCurrentProviderRequestSlotBudget) {
    virtual_extent();
    xfs_read_test::max_requests = 2;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    ASSERT_EQ(disk.transfer_bytes.size(), 3U);
    EXPECT_EQ(disk.transfer_bytes[1], 512 * 1024);
    EXPECT_EQ(disk.transfer_bytes[2], 512 * 1024);
    EXPECT_EQ(disk.peak, 2U);
}
TEST_F(XfsRead, AggregateRespectsCurrentProviderDepth) {
    virtual_extent();
    xfs_read_test::depth = 3;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    EXPECT_EQ(disk.transfer_bytes.size(), 4U);
    EXPECT_EQ(disk.peak, 3U);
}
TEST_F(XfsRead, AggregateTransferBudgetMultiplicationCannotWrapToZero) {
    virtual_extent();
    disk.limit_bytes = SIZE_MAX / 4 + 1;
    xfs_read_test::depth = 4;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 4 * 1024 * 1024}));
}
TEST_F(XfsRead, LongExtentStillKeepsFourMiBAggregateBackingPerCaller) {
    virtual_extent(32 * 1024 * 1024);
    disk.limit_bytes = 2 * 1024 * 1024;
    disk.inflight_limit_bytes = 32 * 1024 * 1024;
    xfs_read_test::depth = 64;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    xfs_read_test::record_allocation_sizes = true;
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 2 * 1024 * 1024, 2 * 1024 * 1024}));
    EXPECT_NE(std::find(xfs_read_test::allocation_sizes.begin(), xfs_read_test::allocation_sizes.end(), 4 * 1024 * 1024),
              xfs_read_test::allocation_sizes.end());
    EXPECT_TRUE(std::all_of(xfs_read_test::allocation_sizes.begin(), xfs_read_test::allocation_sizes.end(),
                            [](size_t bytes) { return bytes <= 4 * 1024 * 1024; }));
}
TEST_F(XfsRead, SequentialHalfMiBDemandDoesNotExpand) {
    virtual_extent();
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(512 * 1024, 128 * 1024), 512 * 1024);
    expect_bytes(512 * 1024);
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 512 * 1024}));
}
TEST_F(XfsRead, OriginalTwoMiBDemandBatchLimitRemainsUnchanged) {
    virtual_extent(8 * 1024 * 1024);
    output.resize(3 * 1024 * 1024);
    ASSERT_EQ(read(output.size()), static_cast<ssize_t>(output.size()));
    expect_bytes(output.size());
    EXPECT_EQ(disk.transfer_bytes.size(), 6U);
    EXPECT_TRUE(std::all_of(disk.transfer_bytes.begin(), disk.transfer_bytes.end(), [](size_t bytes) { return bytes == 512 * 1024; }));
    EXPECT_EQ(disk.peak, 4U);
}
TEST_F(XfsRead, LargerWarmDemandStillAllocatesReferencedSpan) {
    virtual_extent();
    BufHead* bh = cache(4 * 1024 * 1024);
    ASSERT_NE(bh, nullptr);
    brelse(bh);
    auto const submitted = disk.submissions;
    output.resize(1024 * 1024);
    xfs_read_test::allocations = 0;
    ASSERT_EQ(read(output.size()), static_cast<ssize_t>(output.size()));
    expect_bytes(output.size());
    EXPECT_EQ(disk.submissions, submitted);
    EXPECT_GT(xfs_read_test::allocations, 0U);
}
TEST_F(XfsRead, FourMiBCachedReadCanBeDirtiedAndSyncedWithJoinedSerialSnapshot) {
    virtual_extent();
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    BufHead* bh = bget_multi(&dev, 256, 8192, BufferReadClass::FILE_DATA);
    ASSERT_NE(bh, nullptr);
    ASSERT_EQ(bh->size, 4 * 1024 * 1024);
    {
        ker::mod::sys::MutexGuard guard(inode.io_lock);
        std::memset(bh->data, 0x7b, bh->size);
        bdirty(bh);
    }
    xfs_read_test::dirty_live_data = bh->data;
    brelse(bh);
    auto const submitted = disk.submissions;
    ASSERT_EQ(sync_blockdev(&dev), 0);
    EXPECT_EQ(xfs_read_test::serial_writes, 1U);
    ASSERT_EQ(xfs_read_test::virtual_written.size(), 4 * 1024 * 1024);
    EXPECT_TRUE(std::all_of(xfs_read_test::virtual_written.begin(), xfs_read_test::virtual_written.end(),
                            [](uint8_t byte) { return byte == 0x7b; }));
    EXPECT_EQ(disk.submissions, submitted);
    EXPECT_EQ(disk.submissions, disk.releases);
    EXPECT_EQ(disk.flushes, 1U);
    bh = bget_multi(&dev, 256, 8192, BufferReadClass::FILE_DATA);
    ASSERT_NE(bh, nullptr);
    EXPECT_EQ(bh->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
    brelse(bh);
}
#endif
#ifdef XFS_EXPECT_DISABLED
class XfsDisabled : public XfsRead {};
TEST_F(XfsDisabled, FullReadKeepsExactDemandIoAndNoAcceleration) {
    EXPECT_FALSE(xfs::xfs_read_kernel_destination_eligible(output.data(), output.size()));
    EXPECT_FALSE(xfs::xfs_read_cluster_context_eligible());
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    ASSERT_EQ(read(128 * 1024, 128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    // WOS_HOST_TEST keeps the cache's provider seam available independently
    // of the kernel build switch. Even with that capable provider, XFS OFF
    // must submit only the two demanded ranges.
    EXPECT_EQ(disk.transfer_bytes, (std::vector<size_t>{128 * 1024, 128 * 1024}));
}
TEST_F(XfsDisabled, WarmLargerAliasKeepsReferencedAllocationPath) {
    BufHead* bh = cache(512 * 1024);
    ASSERT_NE(bh, nullptr);
    brelse(bh);
    auto const legacy_reads = disk.legacy_reads;
    xfs_read_test::allocations = 0;
    ASSERT_EQ(read(128 * 1024), 128 * 1024);
    expect_bytes(128 * 1024);
    EXPECT_EQ(disk.legacy_reads, legacy_reads);
    EXPECT_GT(xfs_read_test::allocations, 0U);
}
#endif
}  // namespace
