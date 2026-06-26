// XFS on-disk format and mount-path ktest.
// Two classes of tests:
//   (a) Pure struct-level checks — sizes, magic constants, endian wrappers.
//       No block device or kmalloc required.
//   (b) xfs_mount() error-path — a null-zeroed block device returns zeros for
//       sector 0, so sb_magicnum == 0 != XFS_SB_MAGIC and xfs_mount must
//       return a non-zero error code without leaking the ctx.

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <test/ktest.hpp>
#include <vfs/fs/xfs/xfs_bmap.hpp>
#include <vfs/fs/xfs/xfs_btree.hpp>
#include <vfs/fs/xfs/xfs_dir2.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_vfs.hpp>

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

auto xfs_null_read(ker::dev::BlockDevice* dev, uint64_t /*blk*/, size_t count, void* buf) -> int {
    std::memset(buf, 0, count * dev->block_size);
    return 0;
}

auto xfs_null_write(ker::dev::BlockDevice* /*dev*/, uint64_t /*blk*/, size_t /*count*/, const void* /*buf*/) -> int { return 0; }

struct XfsReadCounter {
    size_t calls = 0;
};

auto xfs_counting_read(ker::dev::BlockDevice* dev, uint64_t /*blk*/, size_t count, void* buf) -> int {
    auto* counter = static_cast<XfsReadCounter*>(dev->private_data);
    if (counter != nullptr) {
        counter->calls++;
    }
    std::memset(buf, 0, count * dev->block_size);
    return 0;
}

auto make_xfs_null_bdev() -> ker::dev::BlockDevice {
    ker::dev::BlockDevice d{};
    d.block_size = 512;
    d.total_blocks = 1024;
    d.read_blocks = xfs_null_read;
    d.write_blocks = xfs_null_write;
    return d;
}

auto make_xfs_counting_bdev(XfsReadCounter* counter) -> ker::dev::BlockDevice {
    ker::dev::BlockDevice d = make_xfs_null_bdev();
    d.read_blocks = xfs_counting_read;
    d.private_data = counter;
    return d;
}

}  // namespace

// ---------------------------------------------------------------------------
// Struct-level checks
// ---------------------------------------------------------------------------

KTEST(XFS, MagicConstant) {
    // 'XFSB' stored big-endian = 0x58 0x46 0x53 0x42 = 0x58465342
    KEXPECT_EQ(ker::vfs::xfs::XFS_SB_MAGIC, 0x58465342U);
}

KTEST(XFS, SuperblockStructSize) { KEXPECT_EQ(sizeof(ker::vfs::xfs::XfsDsb), static_cast<size_t>(264)); }

KTEST(XFS, InodeStructSize) { KEXPECT_EQ(sizeof(ker::vfs::xfs::XfsDinode), static_cast<size_t>(176)); }

KTEST(XFS, AgfStructSize) { KEXPECT_EQ(sizeof(ker::vfs::xfs::XfsAgf), static_cast<size_t>(224)); }

KTEST(XFS, BmbtRecSize) { KEXPECT_EQ(sizeof(ker::vfs::xfs::XfsBmbtRec), static_cast<size_t>(16)); }

KTEST(XFS, BmapInsertMergeCases) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_bmap_insert_merge_cases()); }

KTEST(XFS, BmapSyntheticBtreeLookup) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_bmap_synthetic_btree_lookup()); }

KTEST(XFS, BmapExtentPromotion) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_bmap_extent_promotion()); }

KTEST(XFS, SmallHoleWriteAllocatesOnlyNeededBlocks) {
    constexpr size_t BLOCK_SIZE = 4096;
    constexpr uint32_t BLOCK_LOG = 12;
    constexpr ker::vfs::xfs::xfs_filblks_t UNBOUNDED_HOLE = ~static_cast<ker::vfs::xfs::xfs_filblks_t>(0);

    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 12, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(1));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 4096, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(1));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 4097, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(2));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(4095, 2, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(2));
}

KTEST(XFS, SequentialAppendDefersSpeculativePreallocUntilStream) {
    constexpr size_t BLOCK_SIZE = 4096;
    constexpr uint32_t BLOCK_LOG = 12;
    constexpr size_t SMALL_APPEND_POS = size_t{16} * 1024;
    constexpr size_t STREAM_APPEND_POS = size_t{2} * 1024 * 1024;
    constexpr ker::vfs::xfs::xfs_filblks_t UNBOUNDED_HOLE = ~static_cast<ker::vfs::xfs::xfs_filblks_t>(0);

    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 4096, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG, SMALL_APPEND_POS, true),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(1));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 4096, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG, STREAM_APPEND_POS, true),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(1024));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 4096, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG, SMALL_APPEND_POS, false),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(1));
    KEXPECT_EQ(
        ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, 4096, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG, STREAM_APPEND_POS, false),
        static_cast<ker::vfs::xfs::xfs_extlen_t>(1));
}

KTEST(XFS, MappedAppendSkipsReadOnlyAtCleanBlockBoundary) {
    constexpr size_t BLOCK_SIZE = 4096;

    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_mapped_append_can_zero_without_read(0, 0, BLOCK_SIZE));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_mapped_append_can_zero_without_read(BLOCK_SIZE, BLOCK_SIZE, BLOCK_SIZE));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_mapped_append_can_zero_without_read(BLOCK_SIZE - 1, BLOCK_SIZE - 1, BLOCK_SIZE));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_mapped_append_can_zero_without_read(BLOCK_SIZE, BLOCK_SIZE - 1, BLOCK_SIZE));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_mapped_append_can_zero_without_read(BLOCK_SIZE, BLOCK_SIZE * 2, BLOCK_SIZE));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_mapped_append_can_zero_without_read(BLOCK_SIZE, BLOCK_SIZE, 0));
}

KTEST(XFS, DirectReadBatchIsBoundedAndBlockAligned) {
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_direct_read_batch_max_bytes(4096), static_cast<size_t>(16 * 1024 * 1024));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_direct_read_batch_max_bytes(512), static_cast<size_t>(16 * 1024 * 1024));
    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_direct_read_batch_max_bytes(3 * 1024 * 1024), static_cast<size_t>(3 * 1024 * 1024));
}

KTEST(XFS, MappedDirectOverwriteRequiresUncachedRange) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_mapped_direct_overwrite_requires_uncached_range());
}

KTEST(XFS, BufGetMultiSkipsDeviceRead) {
    XfsReadCounter counter{};
    ker::dev::BlockDevice dev = make_xfs_counting_bdev(&counter);
    ker::vfs::invalidate_bdev(&dev);

    ker::vfs::xfs::XfsMountContext ctx{};
    ctx.device = &dev;
    ctx.block_size = 4096;
    ctx.block_log = 12;
    ctx.ag_blocks = 1024;
    ctx.ag_blk_log = 10;

    constexpr size_t COUNT = 2;
    ker::vfs::BufHead* bh = ker::vfs::xfs::xfs_buf_get_multi(&ctx, ker::vfs::xfs::xfs_agbno_to_fsbno(0, 8, ctx.ag_blk_log), COUNT);
    KREQUIRE_NE(bh, nullptr);
    KEXPECT_EQ(counter.calls, static_cast<size_t>(0));
    KEXPECT_EQ(bh->size, COUNT * ctx.block_size);

    ker::vfs::brelse(bh);
    ker::vfs::invalidate_bdev(&dev);
}

KTEST(XFS, HoleWriteStillCapsLargeAllocations) {
    constexpr size_t BLOCK_SIZE = 4096;
    constexpr uint32_t BLOCK_LOG = 12;
    constexpr size_t BIG_WRITE_BYTES = size_t{512} * 1024 * 1024;
    constexpr ker::vfs::xfs::xfs_filblks_t UNBOUNDED_HOLE = ~static_cast<ker::vfs::xfs::xfs_filblks_t>(0);

    KEXPECT_EQ(ker::vfs::xfs::xfs_selftest_hole_write_alloc_blocks(0, BIG_WRITE_BYTES, UNBOUNDED_HOLE, BLOCK_SIZE, BLOCK_LOG),
               static_cast<ker::vfs::xfs::xfs_extlen_t>(65536));
}

KTEST(XFS, TruncateZeroResetsStaleDataFork) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_truncate_zero_resets_data(12, 1));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_truncate_zero_resets_data(0, 1024));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_truncate_zero_resets_data(0, 0));
}

KTEST(XFS, ReadOnlyCloseSkipsPreallocTrim) {
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(0));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(ker::vfs::O_CREAT));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(ker::vfs::O_TRUNC));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(1));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(2));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(ker::vfs::O_CREAT | 1, true, false));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_trim_prealloc(ker::vfs::O_CREAT | 1, true, true));
}

KTEST(XFS, CleanFreshCreateCloseSkipsCommit) {
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_close_should_commit_inode(false, ker::vfs::O_CREAT | 1, true, false));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_commit_inode(true, ker::vfs::O_CREAT | 1, true, false));
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_close_should_commit_inode(false, ker::vfs::O_CREAT | 1, true, true));
    KEXPECT_FALSE(ker::vfs::xfs::xfs_selftest_close_should_commit_inode(false, 0, false, false));
}

KTEST(XFS, CloseTrimDetectsActualEofPrealloc) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_inode_has_eof_prealloc()); }

KTEST(XFS, DirectFreshWriteDropsDirtyCacheAlias) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_direct_fresh_write_discards_cache()); }

KTEST(XFS, FreshFullWriteUsesBufferCache) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_fresh_full_write_uses_buffer_cache()); }

KTEST(XFS, DentryCacheHitsAndInvalidates) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_dentry_cache_shortform()); }

KTEST(XFS, DentryCacheInvalidationKeepsUnrelatedDirectoryHot) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_dentry_cache_keeps_unrelated_dir_hot());
}

KTEST(XFS, DentryCacheAddKeepsSiblingHot) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_dentry_cache_add_keeps_sibling_hot()); }

KTEST(XFS, DentryCacheRemoveKeepsSiblingHot) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_dentry_cache_remove_keeps_sibling_hot()); }

KTEST(XFS, ParentPathCacheHitsAndPurges) { KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_parent_path_cache()); }

KTEST(XFS, ShortformReaddirCookiesProgressAcrossDuplicateOffsets) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_shortform_readdir_cookies_are_monotonic());
}

KTEST(XFS, ShortformReaddirCookiesResumeAfterRemovals) {
    KEXPECT_TRUE(ker::vfs::xfs::xfs_selftest_shortform_readdir_resume_after_removals());
}

// ---------------------------------------------------------------------------
// xfs_mount() error path: corrupt magic (all zeros) must return != 0
// ---------------------------------------------------------------------------

KTEST(XFS, NullMagicReturnsError) {
    ker::dev::BlockDevice dev = make_xfs_null_bdev();
    ker::vfs::xfs::XfsMountContext* ctx = nullptr;
    int const RET = ker::vfs::xfs::xfs_mount(&dev, true, &ctx);
    KEXPECT_NE(RET, 0);
    // ctx must not have been allocated on error
    KEXPECT_EQ(ctx, nullptr);
}

KTEST(XFS, NullDeviceReturnsEINVAL) {
    ker::vfs::xfs::XfsMountContext* ctx = nullptr;
    int const RET = ker::vfs::xfs::xfs_mount(nullptr, true, &ctx);
    KEXPECT_EQ(RET, -EINVAL);
    KEXPECT_EQ(ctx, nullptr);
}

KTEST(XFS, NullCtxOutReturnsEINVAL) {
    ker::dev::BlockDevice dev = make_xfs_null_bdev();
    int const RET = ker::vfs::xfs::xfs_mount(&dev, true, nullptr);
    KEXPECT_EQ(RET, -EINVAL);
}

KTEST(XFS, BtreeLookupRejectsZeroDepth) {
    ker::vfs::xfs::XfsBtreeCursor<ker::vfs::xfs::XfsCntbtTraits> cur;
    ker::vfs::xfs::XfsCntbtTraits::IRec target{};

    int const RET = ker::vfs::xfs::xfs_btree_lookup(&cur, 0, 0, target, ker::vfs::xfs::XfsBtreeLookup::GE);

    KEXPECT_EQ(RET, -EINVAL);
}

KTEST(XFS, BtreeLookupRejectsOverMaxDepth) {
    ker::vfs::xfs::XfsBtreeCursor<ker::vfs::xfs::XfsCntbtTraits> cur;
    ker::vfs::xfs::XfsCntbtTraits::IRec target{};

    int const RET = ker::vfs::xfs::xfs_btree_lookup(&cur, 0, static_cast<uint8_t>(ker::vfs::xfs::XFS_BTREE_MAXLEVELS + 1), target,
                                                    ker::vfs::xfs::XfsBtreeLookup::GE);

    KEXPECT_EQ(RET, -EINVAL);
}
