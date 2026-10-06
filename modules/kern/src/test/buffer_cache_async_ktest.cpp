#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <dev/block_device.hpp>
#include <dev/block_request.hpp>
#include <test/ktest.hpp>
#include <vfs/buffer_cache.hpp>

namespace {
using namespace ker::dev;
using namespace ker::vfs;

// Queue/provider storage has kernel lifetime, matching the public API. The
// fake provider performs bounded task-context copies and reverse completions.
struct CacheAsyncDisk {
    BlockDevice device{};
    std::array<uint8_t, 64 * 1024> media{};
    std::array<BlockRequest*, 8> pending{};
    size_t count{};
    size_t peak{};
    size_t transfers{};
    int status{};
    bool short_transfer{};
    bool configured{};
};
CacheAsyncDisk disk{};

auto submit(BlockDevice*, BlockRequest& request) -> int {
    if (disk.count == disk.pending.size()) return -EAGAIN;
    disk.pending.at(disk.count++) = &request;
    disk.peak = std::max(disk.peak, disk.count);
    return 0;
}
void service(BlockDevice* device) {
    while (disk.count != 0) {
        BlockRequest& request = *disk.pending.at(--disk.count);
        auto const& description = request.description;
        size_t const OFFSET = description.lba * device->block_size;
        int status = disk.status;
        if (description.op != BlockRequestOperation::FLUSH &&
            (OFFSET > disk.media.size() || description.bytes > disk.media.size() - OFFSET))
            status = -EIO;
        if (status == 0 && description.op != BlockRequestOperation::FLUSH) {
            if (description.op == BlockRequestOperation::WRITE) {
                memcpy(disk.media.data() + OFFSET, request.segments.at(0).data, description.bytes);
            } else {
                memcpy(request.segments.at(0).data, disk.media.data() + OFFSET, description.bytes);
            }
        }
        size_t const BYTES = disk.short_transfer && description.bytes != 0 ? description.bytes - 1 : description.bytes;
        ++disk.transfers;
        static_cast<void>(block_request_complete(device, request.token, status, status == 0 ? BYTES : 0));
    }
}
void cancel(BlockDevice* device, BlockRequestToken token, int reason) {
    for (size_t i = 0; i < disk.count; ++i) {
        if (disk.pending.at(i)->token == token) {
            disk.pending.at(i) = disk.pending.at(--disk.count);
            static_cast<void>(block_request_complete(device, token, reason, 0));
            return;
        }
    }
}
auto fallback_read(BlockDevice*, uint64_t lba, size_t count, void* data) -> int {
    memcpy(data, disk.media.data() + lba * 512, count * 512);
    return 0;
}
auto fallback_write(BlockDevice*, uint64_t lba, size_t count, const void* data) -> int {
    memcpy(disk.media.data() + lba * 512, data, count * 512);
    return 0;
}
auto fallback_flush(BlockDevice*) -> int { return disk.count == 0 ? 0 : -EBUSY; }
BlockAsyncOps const OPS{submit, service, cancel};

auto prepare() -> bool {
    if (!disk.configured) {
        disk.device.block_size = 512;
        disk.device.total_blocks = disk.media.size() / 512;
        constexpr char NAME[] = "cache-async-test";
        std::copy(std::begin(NAME), std::end(NAME), disk.device.name.begin());
        disk.device.read_blocks = fallback_read;
        disk.device.write_blocks = fallback_write;
        disk.device.flush = fallback_flush;
        BlockQueueLimits limits{};
        limits.depth = 8;
        limits.max_transfer_bytes = 4096;
        if (block_queue_configure(&disk.device, limits, &OPS) != 0) return false;
        disk.configured = true;
    }
    invalidate_bdev(&disk.device);
    disk.media.fill(0);
    disk.peak = 0;
    disk.transfers = 0;
    disk.status = 0;
    disk.short_transfer = false;
    return true;
}
auto dirty(uint64_t block) -> BufHead* {
    auto* bh = bget(&disk.device, block);
    if (bh != nullptr) {
        memset(bh->data, 0x5a, bh->size);
        bh->flags |= BH_VALID;
        bdirty(bh);
    }
    return bh;
}
}  // namespace

KTEST(BufferCacheAsync, NativeQueueWritebackJoinsBeforeDurabilityBarrier) {
    KREQUIRE_TRUE(prepare());
    std::array<BufHead*, 12> buffers{};
    for (size_t i = 0; i < buffers.size(); ++i) {
        buffers.at(i) = dirty(i * 2);
        KREQUIRE_NE(buffers.at(i), nullptr);
    }
    KEXPECT_EQ(sync_blockdev(&disk.device), 0);
    KEXPECT_TRUE(disk.peak > 0 && disk.peak <= 8);
    KEXPECT_EQ(disk.count, 0U);
    for (auto* bh : buffers) {
        KEXPECT_EQ(bh->flags & (BH_DIRTY | BH_WRITEBACK), 0U);
        KEXPECT_EQ(bh->refcount.load(), 1);
        KEXPECT_EQ(disk.media.at(bh->block_no * 512), 0x5a);
        brelse(bh);
    }
    invalidate_bdev(&disk.device);
}

KTEST(BufferCacheAsync, NativeQueueShortWriteRemainsDirtyAndRetryable) {
    KREQUIRE_TRUE(prepare());
    auto* bh = dirty(0);
    KREQUIRE_NE(bh, nullptr);
    disk.short_transfer = true;
    KEXPECT_EQ(sync_blockdev(&disk.device), -EIO);
    KEXPECT_NE(bh->flags & BH_DIRTY, 0U);
    KEXPECT_EQ(bh->flags & BH_WRITEBACK, 0U);
    KEXPECT_EQ(disk.count, 0U);
    disk.short_transfer = false;
    KEXPECT_EQ(sync_blockdev(&disk.device), 0);
    KEXPECT_EQ(bh->flags & BH_DIRTY, 0U);
    brelse(bh);
    invalidate_bdev(&disk.device);
}

KTEST(BufferCacheAsync, NativeDemandReadRetiresAllSlicesBeforePublication) {
    KREQUIRE_TRUE(prepare());
    disk.media.fill(0x73);
    auto* bh = bread_multi(&disk.device, 0, 64, BufferReadClass::FILE_DATA);
    KREQUIRE_NE(bh, nullptr);
    KEXPECT_EQ(disk.count, 0U);
    KEXPECT_TRUE(disk.peak > 0 && disk.peak <= 8);
    KEXPECT_EQ(bh->flags & BH_VALID, BH_VALID);
    KEXPECT_EQ(bh->data[0], 0x73);
    KEXPECT_EQ(bh->data[bh->size - 1], 0x73);
    brelse(bh);
    invalidate_bdev(&disk.device);
}
