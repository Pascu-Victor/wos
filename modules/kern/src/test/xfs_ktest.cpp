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
#include <vfs/fs/xfs/xfs_btree.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

namespace {

auto xfs_null_read(ker::dev::BlockDevice* dev, uint64_t /*blk*/, size_t count, void* buf) -> int {
    std::memset(buf, 0, count * dev->block_size);
    return 0;
}

auto xfs_null_write(ker::dev::BlockDevice* /*dev*/, uint64_t /*blk*/, size_t /*count*/, const void* /*buf*/) -> int { return 0; }

auto make_xfs_null_bdev() -> ker::dev::BlockDevice {
    ker::dev::BlockDevice d{};
    d.block_size = 512;
    d.total_blocks = 1024;
    d.read_blocks = xfs_null_read;
    d.write_blocks = xfs_null_write;
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
