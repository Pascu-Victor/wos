// LibFuzzer target for XFS on-disk format parsing.
//
// Feeds random bytes as XFS superblocks, inodes, and extent records to
// exercise struct field access, endian conversions, and the extent
// pack/unpack round-trip. Catches corruption in bit manipulation code.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vfs/fs/xfs/xfs_format.hpp>

using namespace ker::vfs::xfs;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    // --- Superblock parsing ---
    if (size >= sizeof(XfsDsb)) {
        XfsDsb sb;
        memcpy(&sb, data, sizeof(XfsDsb));

        // Exercise all endian conversions — these must never crash
        volatile uint32_t magic = sb.sb_magicnum.to_cpu();
        volatile uint32_t bsize = sb.sb_blocksize.to_cpu();
        volatile uint64_t dblocks = sb.sb_dblocks.to_cpu();
        volatile uint32_t agcount = sb.sb_agcount.to_cpu();
        volatile uint16_t version = sb.sb_versionnum.to_cpu();
        volatile uint16_t sectsize = sb.sb_sectsize.to_cpu();
        volatile uint16_t inodesize = sb.sb_inodesize.to_cpu();
        volatile uint64_t rootino = sb.sb_rootino.to_cpu();
        volatile uint64_t icount = sb.sb_icount.to_cpu();
        volatile uint64_t ifree = sb.sb_ifree.to_cpu();
        volatile uint64_t fdblocks = sb.sb_fdblocks.to_cpu();
        volatile uint32_t feat_incompat = sb.sb_features_incompat.to_cpu();
        volatile uint32_t feat_ro = sb.sb_features_ro_compat.to_cpu();
        (void)magic;
        (void)bsize;
        (void)dblocks;
        (void)agcount;
        (void)version;
        (void)sectsize;
        (void)inodesize;
        (void)rootino;
        (void)icount;
        (void)ifree;
        (void)fdblocks;
        (void)feat_incompat;
        (void)feat_ro;

        // Check magic (exercise branch)
        if (magic == XFS_SB_MAGIC) {
            volatile bool v5 = (version == XFS_SB_VERSION_5);
            (void)v5;
        }

        // Log fields
        volatile uint8_t blocklog = sb.sb_blocklog;
        volatile uint8_t sectlog = sb.sb_sectlog;
        volatile uint8_t inodelog = sb.sb_inodelog;
        (void)blocklog;
        (void)sectlog;
        (void)inodelog;
    }

    // --- Inode parsing ---
    if (size >= sizeof(XfsDinode)) {
        XfsDinode inode;
        memcpy(&inode, data, sizeof(XfsDinode));

        volatile uint16_t di_magic = inode.di_magic.to_cpu();
        volatile uint16_t di_mode = inode.di_mode.to_cpu();
        volatile uint64_t di_size = inode.di_size.to_cpu();
        volatile uint64_t di_nblocks = inode.di_nblocks.to_cpu();
        volatile uint32_t di_nextents = inode.di_nextents.to_cpu();
        volatile uint32_t di_uid = inode.di_uid.to_cpu();
        volatile uint32_t di_gid = inode.di_gid.to_cpu();
        volatile uint32_t di_nlink = inode.di_nlink.to_cpu();
        (void)di_magic;
        (void)di_mode;
        (void)di_size;
        (void)di_nblocks;
        (void)di_nextents;
        (void)di_uid;
        (void)di_gid;
        (void)di_nlink;

        // Version-dependent size helper
        volatile size_t dsize = xfs_dinode_size(inode.di_version);
        (void)dsize;

        // Attr fork offset (bounded by forkoff byte)
        if (inode.di_version >= 3) {
            volatile size_t fork_off = xfs_dinode_attr_fork_off(&inode);
            (void)fork_off;
        }

        // Format enum access
        volatile uint8_t fmt = inode.di_format;
        (void)fmt;
    }

    // --- Extent record pack/unpack round-trip ---
    if (size >= sizeof(XfsBmbtRec)) {
        XfsBmbtRec rec;
        memcpy(&rec, data, sizeof(XfsBmbtRec));

        // Unpack fuzzed on-disk record
        XfsBmbtIrec irec = xfs_bmbt_rec_unpack(&rec);

        // Re-pack and unpack again — must produce identical result
        XfsBmbtRec repacked = xfs_bmbt_rec_pack(irec);
        XfsBmbtIrec irec2 = xfs_bmbt_rec_unpack(&repacked);

        // Verify round-trip (these should be identical after going through
        // pack/unpack which masks to valid bit widths)
        if (irec.br_startoff != irec2.br_startoff || irec.br_startblock != irec2.br_startblock ||
            irec.br_blockcount != irec2.br_blockcount || irec.br_unwritten != irec2.br_unwritten) {
            __builtin_trap();  // Signal to fuzzer: round-trip mismatch
        }
    }

    // --- AGF header ---
    if (size >= sizeof(XfsAgf)) {
        XfsAgf agf;
        memcpy(&agf, data, sizeof(XfsAgf));
        volatile uint32_t agf_magic = agf.agf_magicnum.to_cpu();
        volatile uint32_t agf_freeblks = agf.agf_freeblks.to_cpu();
        volatile uint32_t agf_longest = agf.agf_longest.to_cpu();
        (void)agf_magic;
        (void)agf_freeblks;
        (void)agf_longest;
    }

    // --- AGI header ---
    if (size >= sizeof(XfsAgi)) {
        XfsAgi agi;
        memcpy(&agi, data, sizeof(XfsAgi));
        volatile uint32_t agi_magic = agi.agi_magicnum.to_cpu();
        volatile uint32_t agi_count = agi.agi_count.to_cpu();
        volatile uint32_t agi_freecount = agi.agi_freecount.to_cpu();
        (void)agi_magic;
        (void)agi_count;
        (void)agi_freecount;
    }

    // --- Free space record ---
    if (size >= sizeof(XfsAllocRec)) {
        XfsAllocRec arec;
        memcpy(&arec, data, sizeof(XfsAllocRec));
        volatile uint32_t start = arec.ar_startblock.to_cpu();
        volatile uint32_t count = arec.ar_blockcount.to_cpu();
        (void)start;
        (void)count;
    }

    return 0;
}
