// Unit tests for XFS on-disk format structures.
//
// Tests struct sizes, magic numbers, field packing, endian conversions,
// and the extent record pack/unpack round-trip. All tests operate on raw
// byte buffers — no disk I/O or kernel VFS required.

#include <gtest/gtest.h>

#include <cstring>
#include <vfs/fs/xfs/xfs_format.hpp>

using namespace ker::vfs::xfs;

// =============================================================================
// Struct Sizes (must match on-disk format exactly)
// =============================================================================

TEST(XfsFormat, SuperblockSize) { static_assert(sizeof(XfsDsb) == 264); }
TEST(XfsFormat, InodeSize) { static_assert(sizeof(XfsDinode) == 176); }
TEST(XfsFormat, AgfSize) { static_assert(sizeof(XfsAgf) == 224); }
TEST(XfsFormat, AgiSize) { static_assert(sizeof(XfsAgi) == 344); }
TEST(XfsFormat, BmbtRecSize) { static_assert(sizeof(XfsBmbtRec) == 16); }
TEST(XfsFormat, AllocRecSize) { static_assert(sizeof(XfsAllocRec) == 8); }

// =============================================================================
// Magic Numbers
// =============================================================================

TEST(XfsFormat, SuperblockMagic) { EXPECT_EQ(XFS_SB_MAGIC, 0x58465342u); }
TEST(XfsFormat, InodeMagic) { EXPECT_EQ(XFS_DINODE_MAGIC, 0x494Eu); }
TEST(XfsFormat, AgfMagic) { EXPECT_EQ(XFS_AGF_MAGIC, 0x58414746u); }
TEST(XfsFormat, AgiMagic) { EXPECT_EQ(XFS_AGI_MAGIC, 0x58414749u); }
TEST(XfsFormat, AgflMagic) { EXPECT_EQ(XFS_AGFL_MAGIC, 0x5841464Cu); }

// =============================================================================
// Big-Endian Type Wrappers
// =============================================================================

TEST(BigEndian, Be16RoundTrip) {
    auto be = Be16::from_cpu(0x1234);
    EXPECT_EQ(be.to_cpu(), 0x1234u);
}

TEST(BigEndian, Be32RoundTrip) {
    auto be = Be32::from_cpu(0xDEADBEEF);
    EXPECT_EQ(be.to_cpu(), 0xDEADBEEFu);
}

TEST(BigEndian, Be64RoundTrip) {
    auto be = Be64::from_cpu(0xCAFEBABE12345678ULL);
    EXPECT_EQ(be.to_cpu(), 0xCAFEBABE12345678ULL);
}

TEST(BigEndian, Be16ByteOrder) {
    auto be = Be16::from_cpu(0x0102);
    uint8_t bytes[2];
    memcpy(bytes, &be, 2);
    EXPECT_EQ(bytes[0], 0x01);
    EXPECT_EQ(bytes[1], 0x02);
}

TEST(BigEndian, Be32ByteOrder) {
    auto be = Be32::from_cpu(0x01020304);
    uint8_t bytes[4];
    memcpy(bytes, &be, 4);
    EXPECT_EQ(bytes[0], 0x01);
    EXPECT_EQ(bytes[1], 0x02);
    EXPECT_EQ(bytes[2], 0x03);
    EXPECT_EQ(bytes[3], 0x04);
}

// =============================================================================
// Superblock Field Access
// =============================================================================

TEST(XfsSuperblock, FieldAccess) {
    XfsDsb sb{};
    sb.sb_magicnum = Be32::from_cpu(XFS_SB_MAGIC);
    sb.sb_blocksize = Be32::from_cpu(4096);
    sb.sb_agcount = Be32::from_cpu(4);
    sb.sb_versionnum = Be16::from_cpu(XFS_SB_VERSION_5);

    EXPECT_EQ(sb.sb_magicnum.to_cpu(), XFS_SB_MAGIC);
    EXPECT_EQ(sb.sb_blocksize.to_cpu(), 4096u);
    EXPECT_EQ(sb.sb_agcount.to_cpu(), 4u);
    EXPECT_EQ(sb.sb_versionnum.to_cpu(), XFS_SB_VERSION_5);
}

TEST(XfsSuperblock, CrcOffset) {
    // CRC must be at a fixed offset for on-disk format compatibility
    EXPECT_EQ(XFS_SB_CRC_OFF, offsetof(XfsDsb, sb_crc));
    // CRC field should be after the v5-only fields start
    EXPECT_GT(XFS_SB_CRC_OFF, 200u);
}

TEST(XfsSuperblock, FeatureBits) {
    XfsDsb sb{};
    sb.sb_features_incompat = Be32::from_cpu(XFS_SB_FEAT_INCOMPAT_FTYPE | XFS_SB_FEAT_INCOMPAT_BIGTIME);
    uint32_t feats = sb.sb_features_incompat.to_cpu();
    EXPECT_TRUE(feats & XFS_SB_FEAT_INCOMPAT_FTYPE);
    EXPECT_TRUE(feats & XFS_SB_FEAT_INCOMPAT_BIGTIME);
    EXPECT_FALSE(feats & XFS_SB_FEAT_INCOMPAT_SPINODES);
}

// =============================================================================
// Inode Format
// =============================================================================

TEST(XfsInode, DiSizeVersions) {
    EXPECT_EQ(xfs_dinode_size(3), 176u);
    EXPECT_EQ(xfs_dinode_size(2), 100u);
    EXPECT_EQ(xfs_dinode_size(1), 100u);
}

TEST(XfsInode, AttrForkOffset) {
    XfsDinode inode{};
    inode.di_version = 3;
    inode.di_forkoff = 24;  // 24 * 8 = 192 bytes from inode start
    EXPECT_EQ(xfs_dinode_attr_fork_off(&inode), 176u + 192u);
}

TEST(XfsInode, AttrForkOffsetZero) {
    XfsDinode inode{};
    inode.di_version = 3;
    inode.di_forkoff = 0;  // no attr fork
    EXPECT_EQ(xfs_dinode_attr_fork_off(&inode), 176u);
}

TEST(XfsInode, FormatEnum) {
    EXPECT_EQ(static_cast<uint8_t>(XFS_DINODE_FMT_DEV), 0);
    EXPECT_EQ(static_cast<uint8_t>(XFS_DINODE_FMT_LOCAL), 1);
    EXPECT_EQ(static_cast<uint8_t>(XFS_DINODE_FMT_EXTENTS), 2);
    EXPECT_EQ(static_cast<uint8_t>(XFS_DINODE_FMT_BTREE), 3);
}

// =============================================================================
// Extent Record Pack / Unpack Round-Trip
// =============================================================================

TEST(XfsBmbt, PackUnpackRoundTrip) {
    XfsBmbtIrec orig{};
    orig.br_startoff = 12345;
    orig.br_startblock = 6789012;
    orig.br_blockcount = 42;
    orig.br_unwritten = false;

    XfsBmbtRec packed = xfs_bmbt_rec_pack(orig);
    XfsBmbtIrec decoded = xfs_bmbt_rec_unpack(&packed);

    EXPECT_EQ(decoded.br_startoff, orig.br_startoff);
    EXPECT_EQ(decoded.br_startblock, orig.br_startblock);
    EXPECT_EQ(decoded.br_blockcount, orig.br_blockcount);
    EXPECT_EQ(decoded.br_unwritten, orig.br_unwritten);
}

TEST(XfsBmbt, PackUnpackUnwritten) {
    XfsBmbtIrec orig{};
    orig.br_startoff = 100;
    orig.br_startblock = 200;
    orig.br_blockcount = 300;
    orig.br_unwritten = true;

    XfsBmbtRec packed = xfs_bmbt_rec_pack(orig);
    XfsBmbtIrec decoded = xfs_bmbt_rec_unpack(&packed);

    EXPECT_EQ(decoded.br_startoff, 100u);
    EXPECT_EQ(decoded.br_startblock, 200u);
    EXPECT_EQ(decoded.br_blockcount, 300u);
    EXPECT_TRUE(decoded.br_unwritten);
}

TEST(XfsBmbt, PackUnpackMaxValues) {
    XfsBmbtIrec orig{};
    // Max values for each bit field
    orig.br_startoff = (1ULL << 54) - 1;    // 54-bit max
    orig.br_startblock = (1ULL << 52) - 1;  // 52-bit max
    orig.br_blockcount = (1ULL << 21) - 1;  // 21-bit max
    orig.br_unwritten = true;

    XfsBmbtRec packed = xfs_bmbt_rec_pack(orig);
    XfsBmbtIrec decoded = xfs_bmbt_rec_unpack(&packed);

    EXPECT_EQ(decoded.br_startoff, orig.br_startoff);
    EXPECT_EQ(decoded.br_startblock, orig.br_startblock);
    EXPECT_EQ(decoded.br_blockcount, orig.br_blockcount);
    EXPECT_TRUE(decoded.br_unwritten);
}

TEST(XfsBmbt, PackUnpackZeros) {
    XfsBmbtIrec orig{};
    orig.br_startoff = 0;
    orig.br_startblock = 0;
    orig.br_blockcount = 0;
    orig.br_unwritten = false;

    XfsBmbtRec packed = xfs_bmbt_rec_pack(orig);
    XfsBmbtIrec decoded = xfs_bmbt_rec_unpack(&packed);

    EXPECT_EQ(decoded.br_startoff, 0u);
    EXPECT_EQ(decoded.br_startblock, 0u);
    EXPECT_EQ(decoded.br_blockcount, 0u);
    EXPECT_FALSE(decoded.br_unwritten);
}

// =============================================================================
// AG Header Structures
// =============================================================================

TEST(XfsAg, AgfFieldAccess) {
    XfsAgf agf{};
    agf.agf_magicnum = Be32::from_cpu(XFS_AGF_MAGIC);
    agf.agf_seqno = Be32::from_cpu(2);
    agf.agf_length = Be32::from_cpu(65536);
    agf.agf_freeblks = Be32::from_cpu(1000);

    EXPECT_EQ(agf.agf_magicnum.to_cpu(), XFS_AGF_MAGIC);
    EXPECT_EQ(agf.agf_seqno.to_cpu(), 2u);
    EXPECT_EQ(agf.agf_length.to_cpu(), 65536u);
    EXPECT_EQ(agf.agf_freeblks.to_cpu(), 1000u);
}

TEST(XfsAg, AgiFieldAccess) {
    XfsAgi agi{};
    agi.agi_magicnum = Be32::from_cpu(XFS_AGI_MAGIC);
    agi.agi_count = Be32::from_cpu(64);
    agi.agi_freecount = Be32::from_cpu(10);

    EXPECT_EQ(agi.agi_magicnum.to_cpu(), XFS_AGI_MAGIC);
    EXPECT_EQ(agi.agi_count.to_cpu(), 64u);
    EXPECT_EQ(agi.agi_freecount.to_cpu(), 10u);
}

// =============================================================================
// B+tree Magics
// =============================================================================

TEST(XfsBtree, MagicNumbers) {
    EXPECT_EQ(XFS_ABTB_CRC_MAGIC, 0x41423342u);
    EXPECT_EQ(XFS_ABTC_CRC_MAGIC, 0x41423343u);
    EXPECT_EQ(XFS_IBT_CRC_MAGIC, 0x49414233u);
    EXPECT_EQ(XFS_FIBT_CRC_MAGIC, 0x46494233u);
    EXPECT_EQ(XFS_BMAP_CRC_MAGIC, 0x424D4133u);
}

// =============================================================================
// Sentinel Values
// =============================================================================

TEST(XfsFormat, Sentinels) {
    EXPECT_EQ(NULLFSBLOCK, static_cast<xfs_fsblock_t>(-1));
    EXPECT_EQ(NULLAGBLOCK, static_cast<xfs_agblock_t>(-1));
    EXPECT_EQ(NULLAGNUMBER, static_cast<xfs_agnumber_t>(-1));
    EXPECT_EQ(NULLFSINO, static_cast<xfs_ino_t>(-1));
}
