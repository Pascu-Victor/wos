#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <vector>

// Preserve the real BlockDevice descriptor even with common host shims first.
#include <util/crc32c.hpp>
#include <vfs/fs/xfs/xfs_log.hpp>
#include <vfs/fs/xfs/xfs_log_codec.hpp>

#include "../../../modules/kern/src/dev/block_device.hpp"

namespace {
struct Storage {
    static constexpr size_t SECTOR_BYTES = 512;
    static constexpr size_t SECTORS = 512;
    std::vector<uint8_t> visible = std::vector<uint8_t>(SECTOR_BYTES * SECTORS, 0x5a);
    std::vector<uint8_t> durable = visible;
    std::vector<char> operations;
    size_t fail_operation{};

    auto write(uint64_t block, size_t count, const void* data) -> int {
        operations.push_back('W');
        if (operations.size() == fail_operation) return -EIO;
        if (block > SECTORS || count > SECTORS - block || data == nullptr) return -EINVAL;
        std::memcpy(visible.data() + block * SECTOR_BYTES, data, count * SECTOR_BYTES);
        return 0;
    }

    auto flush() -> int {
        operations.push_back('F');
        if (operations.size() == fail_operation) return -EIO;
        durable = visible;
        return 0;
    }
};
}  // namespace

namespace ker::dev {
auto block_write(BlockDevice* device, uint64_t block, size_t count, const void* data) -> int {
    if (device == nullptr) return -EINVAL;
    return static_cast<Storage*>(device->private_data)->write(block, count, data);
}
}  // namespace ker::dev

namespace ker::vfs {
auto flush_blockdev(dev::BlockDevice* device) -> int {
    if (device == nullptr) return -EINVAL;
    return static_cast<Storage*>(device->private_data)->flush();
}
}  // namespace ker::vfs

#include "xfs_log_clean_runtime.inc"

namespace {
using namespace ker::vfs::xfs;

class XfsLogCleanRuntime : public testing::Test {
   protected:
    Storage storage;
    ker::dev::BlockDevice device{};
    XfsMountContext mount{};
    XfsLog log{};

    void SetUp() override {
        device.block_size = Storage::SECTOR_BYTES;
        device.total_blocks = Storage::SECTORS;
        device.private_data = &storage;
        mount.device = &device;
        mount.block_size = 4096;
        mount.ag_blocks = 64;
        mount.ag_blk_log = 6;
        mount.uuid.b.at(0) = 0x19;
        log.mount = &mount;
        log.log_start = 4;
        log.log_blocks = 32;
        log.sect_size = 512;
        log.head_cycle = 1;
        log.head_block = 20;
        log.tail_cycle = 1;
        log.tail_block = 18;
        log.previous_block = 18 * 8;
        log.active = true;
    }

    auto log_offset() const -> size_t { return log.log_start * mount.block_size; }

    auto header() const -> XlogRecHeader {
        XlogRecHeader result{};
        std::memcpy(&result, storage.durable.data() + log_offset(), sizeof(result));
        return result;
    }

    void expect_standard_record(uint32_t cycle) {
        auto const RECORD = header();
        EXPECT_EQ(RECORD.h_magicno.to_cpu(), XLOG_HEADER_MAGIC_NUM);
        EXPECT_EQ(RECORD.h_cycle.to_cpu(), cycle);
        EXPECT_EQ(RECORD.h_version.to_cpu(), XLOG_VERSION_2);
        EXPECT_EQ(RECORD.h_len.to_cpu(), 512U);
        EXPECT_EQ(RECORD.h_lsn.to_cpu(), static_cast<uint64_t>(cycle) << 32);
        EXPECT_EQ(RECORD.h_tail_lsn.to_cpu(), RECORD.h_lsn.to_cpu());
        EXPECT_EQ(RECORD.h_prev_block.to_cpu(), UINT32_MAX);
        EXPECT_EQ(RECORD.h_num_logops.to_cpu(), 1U);
        EXPECT_EQ(RECORD.h_fmt.to_cpu(), XLOG_FMT_LINUX_LE);
        EXPECT_EQ(RECORD.h_size.to_cpu(), 32768U);
        EXPECT_EQ(RECORD.h_fs_uuid.b, mount.uuid.b);
        auto const* body = storage.durable.data() + log_offset() + XLOG_HEADER_SIZE;
        EXPECT_NE(RECORD.h_crc, 0U);
        EXPECT_EQ(RECORD.h_crc, wos_log_record_crc(RECORD, body, 512));
        Be32 packed_cycle{};
        Be32 length{};
        uint16_t magic{};
        std::memcpy(&packed_cycle, body, sizeof(packed_cycle));
        std::memcpy(&length, body + 4, sizeof(length));
        std::memcpy(&magic, body + 12, sizeof(magic));
        EXPECT_EQ(packed_cycle.to_cpu(), cycle);
        EXPECT_EQ(length.to_cpu(), 8U);
        EXPECT_EQ(body[8], XFS_LOG);
        EXPECT_EQ(body[9], XLOG_UNMOUNT_TRANS);
        EXPECT_EQ(magic, XLOG_UNMOUNT_TYPE);
        EXPECT_EQ(RECORD.h_cycle_data.at(0).to_cpu(), 0U);
        EXPECT_TRUE(std::all_of(storage.durable.begin() + log_offset() + 1024,
                                storage.durable.begin() + log_offset() + log.log_blocks * mount.block_size,
                                [](uint8_t byte) { return byte == 0; }));
    }
};

TEST_F(XfsLogCleanRuntime, UntouchedAgiLaterLsnStaysValidAcrossCanonicalClean) {
    // Linux-seeded AGI: CRC-valid, same-cycle later LSN than a block-zero marker.
    constexpr size_t AGI_OFFSET = 2 * Storage::SECTOR_BYTES;
    constexpr uint64_t OLD_LSN = (uint64_t{1} << 32) | 6604;
    auto* agi = reinterpret_cast<XfsAgi*>(storage.visible.data() + AGI_OFFSET);
    std::memset(agi, 0, Storage::SECTOR_BYTES);
    agi->agi_magicnum = Be32::from_cpu(XFS_AGI_MAGIC);
    agi->agi_versionnum = Be32::from_cpu(1);
    agi->agi_seqno = Be32::from_cpu(2);
    agi->agi_lsn = Be64::from_cpu(OLD_LSN);
    uint32_t const CRC = ker::util::crc32c_block_with_cksum(agi, Storage::SECTOR_BYTES, XFS_AGI_CRC_OFF);
    std::memcpy(&agi->agi_crc, &CRC, sizeof(CRC));
    storage.durable = storage.visible;
    std::array<uint8_t, Storage::SECTOR_BYTES> original{};
    std::memcpy(original.data(), agi, original.size());

    ASSERT_EQ(xfs_log_clear_clean(&mount, &log), 0);
    auto const CLEAN = header();
    // Linux accepts null LSN or lexicographically older cycle/block. Its next
    // current position is one header BB plus the declared 512-byte payload.
    auto const CURRENT_LSN = (static_cast<uint64_t>(CLEAN.h_cycle.to_cpu()) << 32) | 2;
    EXPECT_LT(OLD_LSN, CURRENT_LSN);
    EXPECT_EQ(std::memcmp(storage.durable.data() + AGI_OFFSET, original.data(), original.size()), 0);
    EXPECT_EQ(agi->agi_lsn.to_cpu(), OLD_LSN);
    uint32_t saved_crc = 0;
    std::memcpy(&saved_crc, &agi->agi_crc, sizeof(saved_crc));
    EXPECT_EQ(saved_crc, ker::util::crc32c_block_with_cksum(agi, Storage::SECTOR_BYTES, XFS_AGI_CRC_OFF));
    EXPECT_TRUE(log.clean);
    expect_standard_record(2);
    EXPECT_EQ(storage.operations, (std::vector<char>{'W', 'W', 'F', 'W', 'F'}));
}

TEST_F(XfsLogCleanRuntime, RepeatedCleanRewindsNeverMoveEpochBackward) {
    for (uint32_t expected = 2; expected <= 5; ++expected) {
        log.clean = false;
        log.head_block = log.log_blocks - 1;
        log.tail_block = log.head_block;
        ASSERT_EQ(xfs_log_clear_clean(&mount, &log), 0);
        EXPECT_EQ(log.head_cycle, expected);
        EXPECT_EQ(log.tail_cycle, expected);
        EXPECT_EQ(log.head_block, 1U);
        EXPECT_EQ(log.tail_block, 1U);
        expect_standard_record(expected);
    }
}

TEST_F(XfsLogCleanRuntime, SkipsHeaderMagicCycleWhileKeepingPackedPayloadUnambiguous) {
    log.head_cycle = XLOG_HEADER_MAGIC_NUM - 1;
    ASSERT_EQ(xfs_log_clear_clean(&mount, &log), 0);
    EXPECT_EQ(log.head_cycle, XLOG_HEADER_MAGIC_NUM + 1);
    expect_standard_record(XLOG_HEADER_MAGIC_NUM + 1);
}

TEST_F(XfsLogCleanRuntime, MaximumValidNewEpochThenExhaustionPreservesDurableWal) {
    log.head_cycle = UINT32_MAX - 1;
    ASSERT_EQ(xfs_log_clear_clean(&mount, &log), 0);
    expect_standard_record(UINT32_MAX);

    // Create a real checksummed WOS buffer record in the last valid epoch.
    std::array<uint8_t, sizeof(WosLogBodyHeader) + sizeof(WosLogBufferItemHeader) + 8> body{};
    std::array<uint8_t, 8> bytes{1, 2, 3, 4, 5, 6, 7, 8};
    uint64_t const LSN = xfs_log_make_lsn(&log, log.head_cycle, log.head_block);
    ASSERT_EQ(wos_log_body_begin(body.data(), body.size(), 0, 1, LSN, body.size()), 0);
    size_t offset = sizeof(WosLogBodyHeader);
    ASSERT_EQ(wos_log_body_append_buffer(body.data(), body.size(), &offset, 2, 1, 0, bytes.data(), bytes.size()), 0);
    ASSERT_EQ(wos_log_body_finalize(body.data(), body.size(), offset), 0);
    ASSERT_EQ(xfs_log_write_record(&log, body.data(), body.size(), 1, false), 0);
    ASSERT_EQ(ker::vfs::flush_blockdev(&device), 0);
    auto const BEFORE = storage.durable;
    auto const HEAD = log.head_block;
    auto const TAIL = log.tail_block;
    auto const PREVIOUS = log.previous_block;
    storage.operations.clear();

    EXPECT_EQ(xfs_log_clear_clean(&mount, &log), -EOVERFLOW);
    EXPECT_TRUE(storage.operations.empty());
    EXPECT_EQ(storage.visible, BEFORE);
    EXPECT_EQ(storage.durable, BEFORE);
    EXPECT_FALSE(log.clean);
    EXPECT_EQ(log.head_cycle, UINT32_MAX);
    EXPECT_EQ(log.head_block, HEAD);
    EXPECT_EQ(log.tail_block, TAIL);
    EXPECT_EQ(log.previous_block, PREVIOUS);
}

TEST_F(XfsLogCleanRuntime, InvalidEpochsFailWithoutTouchingStorageOrState) {
    for (uint32_t invalid : {0U, XLOG_HEADER_MAGIC_NUM}) {
        log.head_cycle = invalid;
        auto const BEFORE = storage.durable;
        EXPECT_EQ(xfs_log_clear_clean(&mount, &log), -EUCLEAN);
        EXPECT_TRUE(storage.operations.empty());
        EXPECT_EQ(storage.visible, BEFORE);
        EXPECT_EQ(storage.durable, BEFORE);
        EXPECT_EQ(log.head_cycle, invalid);
        EXPECT_EQ(log.head_block, 20U);
    }
}

TEST_F(XfsLogCleanRuntime, EraseWriteFailuresNeverIssueMarkerOrFlush) {
    for (size_t failure : {size_t{1}, size_t{2}}) {
        storage.operations.clear();
        storage.fail_operation = failure;
        auto const BEFORE = storage.durable;
        EXPECT_EQ(xfs_log_clear_clean(&mount, &log), -EIO);
        EXPECT_EQ(storage.operations, std::vector<char>(failure, 'W'));
        EXPECT_EQ(storage.durable, BEFORE);
        EXPECT_FALSE(log.clean);
        EXPECT_EQ(log.head_cycle, 1U);
        EXPECT_EQ(log.head_block, 20U);
    }
}

TEST_F(XfsLogCleanRuntime, EraseBarrierFailureRetainsEpochUntilSuccessfulRetry) {
    storage.fail_operation = 3;
    EXPECT_EQ(xfs_log_clear_clean(&mount, &log), -EIO);
    EXPECT_EQ(storage.operations, (std::vector<char>{'W', 'W', 'F'}));
    EXPECT_EQ(log.head_cycle, 1U);
    EXPECT_EQ(log.head_block, 20U);
    EXPECT_FALSE(log.clean);
    storage.operations.clear();
    storage.fail_operation = 0;
    ASSERT_EQ(xfs_log_clear_clean(&mount, &log), 0);
    expect_standard_record(2);
}

TEST_F(XfsLogCleanRuntime, MarkerFailureNeverPublishesCleanAndRetryMovesForward) {
    storage.fail_operation = 4;
    EXPECT_EQ(xfs_log_clear_clean(&mount, &log), -EIO);
    EXPECT_EQ(storage.operations, (std::vector<char>{'W', 'W', 'F', 'W'}));
    EXPECT_EQ(log.head_cycle, 2U);
    EXPECT_EQ(log.head_block, 0U);
    EXPECT_FALSE(log.clean);
    storage.operations.clear();
    storage.fail_operation = 0;
    ASSERT_EQ(xfs_log_clear_clean(&mount, &log), 0);
    expect_standard_record(3);
}

TEST_F(XfsLogCleanRuntime, FinalBarrierFailureNeverPublishesClean) {
    storage.fail_operation = 5;
    EXPECT_EQ(xfs_log_clear_clean(&mount, &log), -EIO);
    EXPECT_EQ(storage.operations, (std::vector<char>{'W', 'W', 'F', 'W', 'F'}));
    EXPECT_EQ(log.head_cycle, 2U);
    EXPECT_EQ(log.head_block, 1U);
    EXPECT_FALSE(log.clean);
    auto const* durable_log = storage.durable.data() + log_offset();
    EXPECT_TRUE(std::all_of(durable_log, durable_log + log.log_blocks * mount.block_size, [](uint8_t byte) { return byte == 0; }));
    storage.operations.clear();
    storage.fail_operation = 0;
    ASSERT_EQ(xfs_log_clear_clean(&mount, &log), 0);
    expect_standard_record(3);
}
}  // namespace
