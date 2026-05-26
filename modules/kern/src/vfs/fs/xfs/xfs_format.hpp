#pragma once

// XFS v5 On-Disk Format Definitions.
//
// Translated from the Linux XFS reference sources at reference/xfs/libxfs/.
// All on-disk structures use Be16/Be32/Be64 typed wrappers so endian
// conversions are explicit at every field access.
//
// Reference files:
//   reference/xfs/libxfs/xfs_format.h   - superblock, AG headers, inode, extent records
//   reference/xfs/libxfs/xfs_da_format.h - directory/attribute B+tree node format
//   reference/xfs/libxfs/xfs_types.h     - type aliases
//   reference/xfs/libxfs/xfs_sb.h        - superblock field definitions
//   reference/xfs/libxfs/xfs_log_format.h - journal log record format

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/endian.hpp>

// XFS structures mirror packed on-disk records. They are normally populated by
// block reads rather than constructors, so member-init warnings are noise here.
// NOLINTBEGIN(cppcoreguidelines-pro-type-member-init)
namespace ker::vfs::xfs {

// ============================================================================
// Type aliases (from xfs_types.h)
// ============================================================================

using xfs_ino_t = uint64_t;            // inode number
using xfs_agblock_t = uint32_t;        // block number within an AG
using xfs_agino_t = uint32_t;          // inode number within an AG
using xfs_extlen_t = uint32_t;         // extent length in blocks
using xfs_agnumber_t = uint32_t;       // allocation group number
using xfs_extnum_t = uint64_t;         // # of extents in a file
using xfs_aextnum_t = uint32_t;        // # extents in attribute fork
using xfs_fsize_t = int64_t;           // bytes in a file
using xfs_lsn_t = int64_t;             // log sequence number
using xfs_dablk_t = uint32_t;          // dir/attr block number (in file)
using xfs_dahash_t = uint32_t;         // dir/attr hash value
using xfs_fsblock_t = uint64_t;        // block number in filesystem (agno|agbno)
using xfs_rfsblock_t = uint64_t;       // raw filesystem block number
using xfs_fileoff_t = uint64_t;        // block number in a file
using xfs_filblks_t = uint64_t;        // number of blocks in a file
using xfs_dir2_data_off_t = uint16_t;  // byte offset in directory data block
using xfs_dir2_dataptr_t = uint32_t;   // offset in data space
using xfs_dir2_db_t = uint32_t;        // directory block number (logical)
using xfs_daddr_t = int64_t;           // disk address (512-byte sectors)

// Null/invalid sentinel values
constexpr xfs_fsblock_t NULLFSBLOCK = static_cast<xfs_fsblock_t>(-1);
constexpr xfs_agblock_t NULLAGBLOCK = static_cast<xfs_agblock_t>(-1);
constexpr xfs_agnumber_t NULLAGNUMBER = static_cast<xfs_agnumber_t>(-1);
constexpr xfs_ino_t NULLFSINO = static_cast<xfs_ino_t>(-1);

// ============================================================================
// UUID type (16 bytes)
// ============================================================================

struct XfsUuidT {
    std::array<uint8_t, 16> b;
} __attribute__((packed));
static_assert(sizeof(XfsUuidT) == 16);

// ============================================================================
// XFS Superblock - On-Disk (xfs_dsb)
// ============================================================================

constexpr uint32_t XFS_SB_MAGIC = 0x58465342;  // 'XFSB'
constexpr uint16_t XFS_SB_VERSION_5 = 5;

// Superblock feature bits
// Incompat features
constexpr uint32_t XFS_SB_FEAT_INCOMPAT_FTYPE = (1U << 0);
constexpr uint32_t XFS_SB_FEAT_INCOMPAT_SPINODES = (1U << 1);
constexpr uint32_t XFS_SB_FEAT_INCOMPAT_META_UUID = (1U << 2);
constexpr uint32_t XFS_SB_FEAT_INCOMPAT_BIGTIME = (1U << 3);
constexpr uint32_t XFS_SB_FEAT_INCOMPAT_NEEDSREPAIR = (1U << 4);
constexpr uint32_t XFS_SB_FEAT_INCOMPAT_NREXT64 = (1U << 5);
constexpr uint32_t XFS_SB_FEAT_INCOMPAT_EXCHRANGE = (1U << 6);
constexpr uint32_t XFS_SB_FEAT_INCOMPAT_PARENT = (1U << 7);

// RO-compat features
constexpr uint32_t XFS_SB_FEAT_RO_COMPAT_FINOBT = (1U << 0);
constexpr uint32_t XFS_SB_FEAT_RO_COMPAT_RMAPBT = (1U << 1);
constexpr uint32_t XFS_SB_FEAT_RO_COMPAT_REFLINK = (1U << 2);
constexpr uint32_t XFS_SB_FEAT_RO_COMPAT_INOBTCNT = (1U << 3);

struct XfsDsb {
    Be32 sb_magicnum;               // 0x58465342 ('XFSB')
    Be32 sb_blocksize;              // logical block size (bytes)
    Be64 sb_dblocks;                // number of data blocks
    Be64 sb_rblocks;                // number of realtime blocks
    Be64 sb_rextents;               // number of realtime extents
    XfsUuidT sb_uuid;               // user-visible filesystem UUID
    Be64 sb_logstart;               // starting block of log (if internal)
    Be64 sb_rootino;                // root inode number
    Be64 sb_rbmino;                 // bitmap inode for realtime extents
    Be64 sb_rsumino;                // summary inode for realtime bitmap
    Be32 sb_rextsize;               // realtime extent size (blocks)
    Be32 sb_agblocks;               // size of an AG (blocks)
    Be32 sb_agcount;                // number of AGs
    Be32 sb_rbmblocks;              // number of realtime bitmap blocks
    Be32 sb_logblocks;              // number of log blocks
    Be16 sb_versionnum;             // header version == 5 for v5
    Be16 sb_sectsize;               // volume sector size (bytes)
    Be16 sb_inodesize;              // inode size (bytes)
    Be16 sb_inopblock;              // inodes per block
    std::array<char, 12> sb_fname;  // filesystem name (not null-terminated)
    uint8_t sb_blocklog;            // log2(sb_blocksize)
    uint8_t sb_sectlog;             // log2(sb_sectsize)
    uint8_t sb_inodelog;            // log2(sb_inodesize)
    uint8_t sb_inopblog;            // log2(sb_inopblock)
    uint8_t sb_agblklog;            // log2(sb_agblocks) rounded up
    uint8_t sb_rextslog;            // log2(sb_rextents)
    uint8_t sb_inprogress;          // mkfs in progress
    uint8_t sb_imax_pct;            // max % of fs for inode space
    Be64 sb_icount;                 // allocated inodes
    Be64 sb_ifree;                  // free inodes
    Be64 sb_fdblocks;               // free data blocks
    Be64 sb_frextents;              // free realtime extents
    Be64 sb_uquotino;               // user quota inode
    Be64 sb_gquotino;               // group quota inode
    Be16 sb_qflags;                 // quota flags
    uint8_t sb_flags;               // misc flags
    uint8_t sb_shared_vn;           // shared version number
    Be32 sb_inoalignmt;             // inode chunk alignment (fsblocks)
    Be32 sb_unit;                   // stripe/raid unit
    Be32 sb_width;                  // stripe/raid width
    uint8_t sb_dirblklog;           // log2 of dir block size (fsblocks)
    uint8_t sb_logsectlog;          // log2 of log sector size
    Be16 sb_logsectsize;            // sector size for log (bytes)
    Be32 sb_logsunit;               // stripe unit size for log
    Be32 sb_features2;              // additional feature bits
    Be32 sb_bad_features2;          // legacy duplicate of features2

    // --- v5 superblock fields ---
    Be32 sb_features_compat;
    Be32 sb_features_ro_compat;
    Be32 sb_features_incompat;
    Be32 sb_features_log_incompat;
    uint32_t sb_crc;        // superblock CRC (note: LITTLE-endian on disk)
    Be32 sb_spino_align;    // sparse inode chunk alignment
    Be64 sb_pquotino;       // project quota inode
    Be64 sb_lsn;            // last write sequence
    XfsUuidT sb_meta_uuid;  // metadata filesystem UUID
} __attribute__((packed));
static_assert(sizeof(XfsDsb) == 264);

// CRC offset within xfs_dsb (for CRC32C verification)
constexpr size_t XFS_SB_CRC_OFF = offsetof(XfsDsb, sb_crc);

// ============================================================================
// On-Disk Inode (xfs_dinode)
// ============================================================================

constexpr uint16_t XFS_DINODE_MAGIC = 0x494E;  // 'IN'

// Inode data fork format (di_format / di_aformat)
// Kept unscoped because these constants are stored in on-disk one-byte fields and used in switch labels across the XFS parser.
// NOLINTNEXTLINE(cppcoreguidelines-use-enum-class)
enum xfs_dinode_fmt : uint8_t {
    XFS_DINODE_FMT_DEV = 0,      // device (special file)
    XFS_DINODE_FMT_LOCAL = 1,    // inline data (shortform)
    XFS_DINODE_FMT_EXTENTS = 2,  // extent list
    XFS_DINODE_FMT_BTREE = 3,    // B+tree
    XFS_DINODE_FMT_UUID = 4,     // UUID (not used in v5)
};

// Inode flags (di_flags)
constexpr uint16_t XFS_DIFLAG_REALTIME = (1U << 0);
constexpr uint16_t XFS_DIFLAG_PREALLOC = (1U << 1);
constexpr uint16_t XFS_DIFLAG_NEWRTBM = (1U << 2);
constexpr uint16_t XFS_DIFLAG_IMMUTABLE = (1U << 3);
constexpr uint16_t XFS_DIFLAG_APPEND = (1U << 4);
constexpr uint16_t XFS_DIFLAG_SYNC = (1U << 5);
constexpr uint16_t XFS_DIFLAG_NOATIME = (1U << 6);
constexpr uint16_t XFS_DIFLAG_NODUMP = (1U << 7);

// Inode flags2 (di_flags2) - v3 inodes only
constexpr uint64_t XFS_DIFLAG2_BIGTIME = (1ULL << 3);

// XFS timestamp (on-disk) - packed into 8 bytes
using xfs_timestamp_t = Be64;

struct XfsDinode {
    Be16 di_magic;                  // 0x494E ('IN')
    Be16 di_mode;                   // file mode & type (POSIX)
    uint8_t di_version;             // inode version (3 for v5)
    uint8_t di_format;              // xfs_dinode_fmt
    Be16 di_metatype;               // XFS_METAFILE_*; was di_onlink (v1)
    Be32 di_uid;                    // owner's user ID
    Be32 di_gid;                    // owner's group ID
    Be32 di_nlink;                  // number of hard links
    Be16 di_projid_lo;              // project ID (low 16 bits)
    Be16 di_projid_hi;              // project ID (high 16 bits)
    std::array<uint8_t, 8> di_pad;  // padding / union area
    xfs_timestamp_t di_atime;       // last access time
    xfs_timestamp_t di_mtime;       // last modification time
    xfs_timestamp_t di_ctime;       // inode change time
    Be64 di_size;                   // file size in bytes
    Be64 di_nblocks;                // total blocks used
    Be32 di_extsize;                // extent size hint
    Be32 di_nextents;               // number of data extents
    Be16 di_anextents;              // number of attr extents
    uint8_t di_forkoff;             // attr fork offset (x8)
    int8_t di_aformat;              // format of attr fork (xfs_dinode_fmt)
    Be32 di_dmevmask;               // DMIG event mask
    Be16 di_dmstate;                // DMIG state info
    Be16 di_flags;                  // XFS_DIFLAG_*
    Be32 di_gen;                    // generation number

    // --- for v1/v2, di_next_unlinked is the last field before data fork ---
    Be32 di_next_unlinked;  // AGI unlinked list pointer

    // --- v3 extended inode fields (CRC-enabled) ---
    uint32_t di_crc;                  // inode CRC (little-endian on disk)
    Be64 di_changecount;              // attribute change count
    Be64 di_lsn;                      // flush sequence
    Be64 di_flags2;                   // XFS_DIFLAG2_*
    Be32 di_cowextsize;               // CoW extent size hint
    std::array<uint8_t, 12> di_pad2;  // padding
    xfs_timestamp_t di_crtime;        // creation time
    Be64 di_ino;                      // inode number (self-reference)
    XfsUuidT di_uuid;                 // filesystem UUID
} __attribute__((packed));
static_assert(sizeof(XfsDinode) == 176);

// Inode sizes
constexpr size_t XFS_DINODE_SIZE_V2 = 100;                // v1/v2 core size
constexpr size_t XFS_DINODE_SIZE_V3 = sizeof(XfsDinode);  // v3 core size

// Data fork starts immediately after the inode core
inline auto xfs_dinode_size(uint8_t version) -> size_t { return (version >= 3) ? XFS_DINODE_SIZE_V3 : XFS_DINODE_SIZE_V2; }

// Attribute fork offset in bytes from start of inode
inline auto xfs_dinode_attr_fork_off(const XfsDinode* dip) -> size_t {
    return xfs_dinode_size(dip->di_version) + (static_cast<size_t>(dip->di_forkoff) << 3);
}

// CRC offset within xfs_dinode
constexpr size_t XFS_DINODE_CRC_OFF = offsetof(XfsDinode, di_crc);

// ============================================================================
// AG Free Space Header (xfs_agf)
// ============================================================================

constexpr uint32_t XFS_AGF_MAGIC = 0x58414746;  // 'XAGF'

struct XfsAgf {
    Be32 agf_magicnum;                 // 0x58414746
    Be32 agf_versionnum;               // 1
    Be32 agf_seqno;                    // AG sequence number (0-based)
    Be32 agf_length;                   // AG size in blocks
    Be32 agf_bno_root;                 // bnobt root block
    Be32 agf_cnt_root;                 // cntbt root block
    Be32 agf_rmap_root;                // rmapbt root block
    Be32 agf_bno_level;                // bnobt btree levels
    Be32 agf_cnt_level;                // cntbt btree levels
    Be32 agf_rmap_level;               // rmapbt btree levels
    Be32 agf_flfirst;                  // first freelist block index
    Be32 agf_fllast;                   // last freelist block index
    Be32 agf_flcount;                  // count of blocks in freelist
    Be32 agf_freeblks;                 // total free blocks in AG
    Be32 agf_longest;                  // longest free extent
    Be32 agf_btreeblks;                // # blocks held in AGF btrees
    XfsUuidT agf_uuid;                 // filesystem UUID
    Be32 agf_rmap_blocks;              // rmapbt blocks used
    Be32 agf_refcount_blocks;          // refcountbt blocks used
    Be32 agf_refcount_root;            // refcount tree root block
    Be32 agf_refcount_level;           // refcount btree levels
    std::array<Be64, 14> agf_spare64;  // reserved
    Be64 agf_lsn;                      // last write sequence
    Be32 agf_crc;                      // CRC of AGF sector
    Be32 agf_spare2;
} __attribute__((packed));
static_assert(sizeof(XfsAgf) == 224);

constexpr size_t XFS_AGF_CRC_OFF = offsetof(XfsAgf, agf_crc);

// ============================================================================
// AG Inode Header (xfs_agi)
// ============================================================================

constexpr uint32_t XFS_AGI_MAGIC = 0x58414749;  // 'XAGI'

struct XfsAgi {
    Be32 agi_magicnum;                  // 0x58414749
    Be32 agi_versionnum;                // 1
    Be32 agi_seqno;                     // AG sequence number
    Be32 agi_length;                    // AG size in blocks
    Be32 agi_count;                     // count of allocated inodes
    Be32 agi_root;                      // inobt root block
    Be32 agi_level;                     // inobt btree levels
    Be32 agi_freecount;                 // number of free inodes
    Be32 agi_newino;                    // new inode just allocated
    Be32 agi_dirino;                    // last directory inode chunk
    std::array<Be32, 64> agi_unlinked;  // hash table of unlinked inodes
    // --- v5 fields ---
    XfsUuidT agi_uuid;  // filesystem UUID
    Be32 agi_crc;       // CRC
    Be32 agi_pad32;
    Be64 agi_lsn;         // last write sequence
    Be32 agi_free_root;   // free inode btree root
    Be32 agi_free_level;  // free inode btree levels
    Be32 agi_iblocks;     // inobt blocks used
    Be32 agi_fblocks;     // finobt blocks used
} __attribute__((packed));
static_assert(sizeof(XfsAgi) == 344);

constexpr size_t XFS_AGI_CRC_OFF = offsetof(XfsAgi, agi_crc);

// ============================================================================
// AG Free List (xfs_agfl)
// ============================================================================

constexpr uint32_t XFS_AGFL_MAGIC = 0x5841464C;  // 'XAFL'

struct XfsAgfl {
    Be32 agfl_magicnum;
    Be32 agfl_seqno;
    XfsUuidT agfl_uuid;
    Be64 agfl_lsn;
    Be32 agfl_crc;
    // Followed by Be32 agfl_bno[] filling the rest of the sector
} __attribute__((packed));

constexpr size_t XFS_AGFL_CRC_OFF = offsetof(XfsAgfl, agfl_crc);

// ============================================================================
// B+Tree Block Headers
// ============================================================================

// Short-form B+tree header (AG-relative pointers) - used by bnobt, cntbt, inobt
struct XfsBtreeSblock {
    Be32 bb_magic;
    Be16 bb_level;     // 0 = leaf
    Be16 bb_numrecs;   // current number of records
    Be32 bb_leftsib;   // left sibling AG block (NULLAGBLOCK if none)
    Be32 bb_rightsib;  // right sibling AG block
    // --- v5 CRC fields ---
    Be64 bb_blkno;     // disk address of this block (sectors)
    Be64 bb_lsn;       // last write sequence
    XfsUuidT bb_uuid;  // filesystem UUID
    Be32 bb_owner;     // AG number
    uint32_t bb_crc;   // CRC (little-endian on disk)
} __attribute__((packed));

// Long-form B+tree header (filesystem-wide pointers) - used by bmbt
struct XfsBtreeLblock {
    Be32 bb_magic;
    Be16 bb_level;
    Be16 bb_numrecs;
    Be64 bb_leftsib;   // left sibling filesystem block (NULLFSBLOCK if none)
    Be64 bb_rightsib;  // right sibling filesystem block
    // --- v5 CRC fields ---
    Be64 bb_blkno;
    Be64 bb_lsn;
    XfsUuidT bb_uuid;
    Be64 bb_owner;    // inode number
    uint32_t bb_crc;  // CRC (little-endian on disk)
    Be32 bb_pad;
} __attribute__((packed));

// Header sizes for offset calculations
constexpr size_t XFS_BTREE_SBLOCK_CRC_LEN = sizeof(XfsBtreeSblock);  // 56 bytes
constexpr size_t XFS_BTREE_LBLOCK_CRC_LEN = sizeof(XfsBtreeLblock);  // 64 bytes

// B+tree magic numbers
constexpr uint32_t XFS_ABTB_CRC_MAGIC = 0x41423342;  // 'AB3B' - bnobt (free space by block)
constexpr uint32_t XFS_ABTC_CRC_MAGIC = 0x41423343;  // 'AB3C' - cntbt (free space by count)
constexpr uint32_t XFS_IBT_CRC_MAGIC = 0x49414233;   // 'IAB3' - inobt
constexpr uint32_t XFS_FIBT_CRC_MAGIC = 0x46494233;  // 'FIB3' - free inobt
constexpr uint32_t XFS_BMAP_CRC_MAGIC = 0x424D4133;  // 'BMA3' - bmbt (extent map)

// ============================================================================
// Extent B+tree Records (bmbt)
// ============================================================================

// BMDR block - root of extent B+tree embedded in inode data fork
struct XfsBmdrBlock {
    Be16 bb_level;    // 0 = leaf
    Be16 bb_numrecs;  // record count
} __attribute__((packed));

// Extent record - 16 bytes, packed as two big-endian 64-bit values
// Bit layout:
//   l0[63]        - extent flag (1 = unwritten/preallocated)
//   l0[62:9]      - startoff (54 bits, file block offset)
//   l0[8:0]+l1[63:21] - startblock (52 bits, filesystem block)
//   l1[20:0]      - blockcount (21 bits, max 2M-1 blocks)
struct XfsBmbtRec {
    Be64 l0;
    Be64 l1;
} __attribute__((packed));

// Decoded extent record (in CPU byte order)
struct XfsBmbtIrec {
    xfs_fileoff_t br_startoff;    // file block offset
    xfs_fsblock_t br_startblock;  // filesystem block
    xfs_filblks_t br_blockcount;  // block count
    bool br_unwritten;            // unwritten/preallocated extent
};

// Decode a packed on-disk extent record into CPU-order fields
inline auto xfs_bmbt_rec_unpack(const XfsBmbtRec* rec) -> XfsBmbtIrec {
    uint64_t const L0 = rec->l0.to_cpu();
    uint64_t const L1 = rec->l1.to_cpu();

    XfsBmbtIrec irec{};
    irec.br_unwritten = (L0 >> 63) != 0;
    irec.br_startoff = (L0 >> 9) & 0x3FFFFFFFFFFFFFULL;         // 54 bits
    irec.br_startblock = ((L0 & 0x1FFULL) << 43) | (L1 >> 21);  // 52 bits
    irec.br_blockcount = L1 & 0x1FFFFFULL;                      // 21 bits
    return irec;
}

// Encode a CPU-order extent record into packed on-disk format
inline auto xfs_bmbt_rec_pack(const XfsBmbtIrec& irec) -> XfsBmbtRec {
    uint64_t l0 = 0;
    uint64_t l1 = 0;

    l0 |= (irec.br_unwritten ? 1ULL : 0ULL) << 63;
    l0 |= (irec.br_startoff & 0x3FFFFFFFFFFFFFULL) << 9;
    l0 |= (irec.br_startblock >> 43) & 0x1FFULL;
    l1 |= (irec.br_startblock & 0x7FFFFFFFFFFULL) << 21;
    l1 |= irec.br_blockcount & 0x1FFFFFULL;

    XfsBmbtRec rec{};
    rec.l0 = Be64::from_cpu(l0);
    rec.l1 = Be64::from_cpu(l1);
    return rec;
}

// Extent key (for B+tree interior nodes)
struct XfsBmbtKey {
    Be64 br_startoff;
} __attribute__((packed));

// Extent pointer (for B+tree interior nodes) - filesystem block number
using xfs_bmbt_ptr_t = Be64;

// ============================================================================
// Free Space B+tree Records (bnobt / cntbt)
// ============================================================================

// Free space record - both bnobt and cntbt use the same record format
struct XfsAllocRec {
    Be32 ar_startblock;  // starting block number (AG-relative)
    Be32 ar_blockcount;  // count of free blocks
} __attribute__((packed));

// Key for bnobt (sorted by start block)
struct XfsAllocKey {
    Be32 ar_startblock;
    Be32 ar_blockcount;
} __attribute__((packed));

// Pointer for AG btrees - AG-relative block number
using xfs_alloc_ptr_t = Be32;

// ============================================================================
// Inode B+tree Records (inobt / finobt)
// ============================================================================

struct XfsInobtRec {
    Be32 ir_startino;  // starting inode number (AG-relative)
    union {
        struct {
            Be32 ir_freecount;  // count of free inodes
        } f;
        struct {
            Be16 ir_holemask;      // hole mask (sparse inodes)
            uint8_t ir_count;      // total inode count
            uint8_t ir_freecount;  // free inode count
        } sp;
    } ir_u;
    Be64 ir_free;  // free inode bitmask
} __attribute__((packed));

struct XfsInobtKey {
    Be32 ir_startino;
} __attribute__((packed));

using XfsInobtPtr_t = Be32;

// ============================================================================
// Directory Format Structures
// ============================================================================

// --- Directory data block magic numbers ---
constexpr uint32_t XFS_DIR3_BLOCK_MAGIC = 0x58444233;  // 'XDB3' - single-block dir
constexpr uint32_t XFS_DIR3_DATA_MAGIC = 0x58444433;   // 'XDD3' - multi-block data
constexpr uint32_t XFS_DIR3_FREE_MAGIC = 0x58444633;   // 'XDF3' - free space index
constexpr uint32_t XFS_DIR3_LEAF_MAGIC = 0x3DF1;       // leaf (v3) - stored as Be16 in da_blkinfo
constexpr uint32_t XFS_DIR3_LEAFN_MAGIC = 0x3FF1;      // leaf node (v3)
constexpr uint32_t XFS_DA3_NODE_MAGIC = 0xFEBE;        // DA btree node (v3)

// --- Directory entry file types (v3 / ftype feature) ---
constexpr uint8_t XFS_DIR3_FT_UNKNOWN = 0;
constexpr uint8_t XFS_DIR3_FT_REG_FILE = 1;
constexpr uint8_t XFS_DIR3_FT_DIR = 2;
constexpr uint8_t XFS_DIR3_FT_CHRDEV = 3;
constexpr uint8_t XFS_DIR3_FT_BLKDEV = 4;
constexpr uint8_t XFS_DIR3_FT_FIFO = 5;
constexpr uint8_t XFS_DIR3_FT_SOCK = 6;
constexpr uint8_t XFS_DIR3_FT_SYMLINK = 7;
constexpr uint8_t XFS_DIR3_FT_WHT = 8;  // whiteout

// --- Shortform directory (inline in inode data fork) ---
struct XfsDir2SfHdr {
    uint8_t count;                  // number of entries
    uint8_t i8count;                // count of 8-byte inode numbers
    std::array<uint8_t, 8> parent;  // parent inode number (4 or 8 bytes used)
} __attribute__((packed));

// Shortform directory entry (variable-length)
// Layout: namelen(1) + offset(2) + name(namelen) + [filetype(1)] + ino(4 or 8)
struct XfsDir2SfEntry {
    uint8_t namelen;                // name length
    std::array<uint8_t, 2> offset;  // saved offset (big-endian uint16)
    uint8_t name[];                 // name bytes (variable)  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
    // Followed by: uint8_t filetype (if ftype feature), then 4 or 8 byte inode
} __attribute__((packed));

inline auto xfs_dir2_sf_entry_name(XfsDir2SfEntry* e) -> uint8_t* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    return e->name;
}
inline auto xfs_dir2_sf_entry_name(const XfsDir2SfEntry* e) -> const uint8_t* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    return e->name;
}

// Helper: get size of inline inode number (4 or 8 bytes)
inline auto xfs_dir2_sf_inumber_size(const XfsDir2SfHdr* hdr) -> size_t { return (hdr->i8count != 0) ? 8 : 4; }

// Helper: get parent inode from shortform header
inline auto xfs_dir2_sf_get_parent(const XfsDir2SfHdr* hdr) -> xfs_ino_t {
    if (hdr->i8count != 0) {
        uint64_t val = 0;
        for (unsigned char const I : hdr->parent) {
            val = (val << 8) | I;
        }
        return val;
    }
    uint32_t val = 0;
    for (size_t i = 0; i < 4; i++) {
        val = (val << 8) | hdr->parent.at(i);
    }
    return val;
}

// Helper: get shortform header actual size
inline auto xfs_dir2_sf_hdr_size(const XfsDir2SfHdr* hdr) -> size_t {
    // 1(count) + 1(i8count) + ino_size(parent)
    return 2 + xfs_dir2_sf_inumber_size(hdr);
}

// --- Data block entry ---
struct XfsDir2DataEntry {
    Be64 inumber;     // inode number
    uint8_t namelen;  // name length
    uint8_t name[];   // name bytes (no null terminator)  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
    // Followed by: uint8_t filetype (if ftype), then Be16 tag (starting offset)
    // Padded to 8-byte alignment
} __attribute__((packed));

inline auto xfs_dir2_data_entry_name(XfsDir2DataEntry* e) -> uint8_t* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    return e->name;
}
inline auto xfs_dir2_data_entry_name(const XfsDir2DataEntry* e) -> const uint8_t* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    return e->name;
}

// Unused/free space entry in a data block
struct XfsDir2DataUnused {
    Be16 freetag;  // 0xFFFF
    Be16 length;   // total free space bytes
    Be16 tag;      // starting offset
} __attribute__((packed));

constexpr uint16_t XFS_DIR2_DATA_FREE_TAG = 0xFFFF;

// Free space info within a data block header
struct XfsDir2DataFree {
    Be16 offset;  // offset to free space
    Be16 length;  // length of free space
} __attribute__((packed));

// v3 (CRC) data block header
struct XfsDir3BlkHdr {
    Be32 magic;
    Be32 crc;
    Be64 blkno;
    Be64 lsn;
    XfsUuidT uuid;
    Be64 owner;
} __attribute__((packed));

struct XfsDir3DataHdr {
    XfsDir3BlkHdr hdr;
    std::array<XfsDir2DataFree, 3> best_free;  // top 3 free spaces
    Be32 pad;
} __attribute__((packed));
static_assert(sizeof(XfsDir3DataHdr) == 64);

// --- Leaf block structures ---

struct XfsDir2LeafEntry {
    Be32 hashval;  // hash value of name
    Be32 address;  // encoded data block offset
} __attribute__((packed));

// Block-format tail (at end of single-block directory)
struct XfsDir2BlockTail {
    Be32 count;  // number of leaf entries
    Be32 stale;  // number of stale (deleted) entries
} __attribute__((packed));

// --- DA btree node (for multi-level directory hash index) ---

struct XfsDaBlkinfo {
    Be32 forw;   // next block in list
    Be32 back;   // previous block in list
    Be16 magic;  // magic number
    Be16 pad;
} __attribute__((packed));

struct XfsDa3Blkinfo {
    XfsDaBlkinfo hdr;
    Be32 crc;
    Be64 blkno;
    Be64 lsn;
    XfsUuidT uuid;
    Be64 owner;
} __attribute__((packed));

struct XfsDa3NodeHdr {
    XfsDa3Blkinfo info;
    Be16 count;
    Be16 level;
    Be32 pad32;
} __attribute__((packed));

struct XfsDaNodeEntry {
    Be32 hashval;  // hash value
    Be32 before;   // btree block before this key
} __attribute__((packed));

// ============================================================================
// Extended Attribute Format Structures
// ============================================================================

// --- Attribute namespace flags (stored in xfs_attr_sf_entry::flags / leaf entry flags) ---
constexpr uint8_t XFS_ATTR_LOCAL = (1U << 0);       // attr value stored locally in leaf
constexpr uint8_t XFS_ATTR_ROOT = (1U << 1);        // attr is in the root (trusted) namespace
constexpr uint8_t XFS_ATTR_SECURE = (1U << 2);      // attr is in the security namespace
constexpr uint8_t XFS_ATTR_PARENT = (1U << 5);      // attr is a parent pointer
constexpr uint8_t XFS_ATTR_INCOMPLETE = (1U << 7);  // attr is being modified

// Mask for namespace bits only (excluding LOCAL and INCOMPLETE)
constexpr uint8_t XFS_ATTR_NSP_ONDISK_MASK = XFS_ATTR_ROOT | XFS_ATTR_SECURE | XFS_ATTR_PARENT;

// --- Shortform attribute header ---
struct XfsAttrSfHdr {
    Be16 totsize;   // total size of shortform attr data (including header)
    uint8_t count;  // number of attribute entries
    uint8_t padding;
} __attribute__((packed));
static_assert(sizeof(XfsAttrSfHdr) == 4);

// --- Shortform attribute entry (variable-length) ---
// Layout: namelen(1) + valuelen(1) + flags(1) + nameval[namelen + valuelen]
struct XfsAttrSfEntry {
    uint8_t namelen;    // length of name
    uint8_t valuelen;   // length of value
    uint8_t flags;      // XFS_ATTR_* namespace flags
    uint8_t nameval[];  // name followed by value  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
} __attribute__((packed));

// Helper: size of one shortform attr entry (header + name + value)
inline auto xfs_attr_sf_entry_size(const XfsAttrSfEntry* e) -> size_t { return sizeof(XfsAttrSfEntry) + e->namelen + e->valuelen; }

// Helper: get pointer to name bytes within a shortform attr entry.
inline auto xfs_attr_sf_entry_name(XfsAttrSfEntry* e) -> uint8_t* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    return e->nameval;
}
inline auto xfs_attr_sf_entry_name(const XfsAttrSfEntry* e) -> const uint8_t* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    return e->nameval;
}

// Helper: get pointer to value within a shortform attr entry
inline auto xfs_attr_sf_entry_value(XfsAttrSfEntry* e) -> uint8_t* { return xfs_attr_sf_entry_name(e) + e->namelen; }
inline auto xfs_attr_sf_entry_value(const XfsAttrSfEntry* e) -> const uint8_t* { return xfs_attr_sf_entry_name(e) + e->namelen; }

// --- Leaf attribute structures ---
constexpr uint16_t XFS_ATTR3_LEAF_MAGIC = 0x3BEE;
constexpr size_t XFS_ATTR_LEAF_MAPSIZE = 3;  // slots in the freespace map

// Run-length-encoded free region within a leaf block
struct XfsAttrLeafMap {
    Be16 base;  // base of free region
    Be16 size;  // length of free region
} __attribute__((packed));
static_assert(sizeof(XfsAttrLeafMap) == 4);

struct XfsAttr3LeafHdr {
    XfsDa3Blkinfo info;  // DA btree block info
    Be16 count;          // number of entries
    Be16 usedbytes;      // bytes used for name/value payloads
    Be16 firstused;      // first used byte in name area
    uint8_t holes;       // non-zero if compaction needed
    uint8_t pad1;
    // N largest free regions; raw array is part of the packed on-disk XFS leaf header.
    // NOLINTNEXTLINE(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
    XfsAttrLeafMap freemap[XFS_ATTR_LEAF_MAPSIZE];
    // Followed by XfsAttrLeafEntry[] array
} __attribute__((packed));
static_assert(sizeof(XfsAttr3LeafHdr) == 76);

// CRC field is at offsetof(XfsAttr3LeafHdr, info.crc) = sizeof(XfsDaBlkinfo) = 12
constexpr size_t XFS_ATTR3_LEAF_CRC_OFF = 12;

struct XfsAttrLeafEntry {
    Be32 hashval;   // hash of attr name
    Be16 nameidx;   // byte offset into leaf block of name/value
    uint8_t flags;  // XFS_ATTR_* flags
    uint8_t pad2;
} __attribute__((packed));
static_assert(sizeof(XfsAttrLeafEntry) == 8);

// Local attribute name+value in leaf block (pointed to by nameidx)
struct XfsAttrLeafNameLocal {
    Be16 valuelen;      // length of value
    uint8_t namelen;    // length of name
    uint8_t nameval[];  // name followed by value  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
} __attribute__((packed));

inline auto xfs_attr_leaf_name_local_name(XfsAttrLeafNameLocal* e) -> uint8_t* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    return e->nameval;
}
inline auto xfs_attr_leaf_name_local_name(const XfsAttrLeafNameLocal* e) -> const uint8_t* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    return e->nameval;
}
inline auto xfs_attr_leaf_name_local_value(XfsAttrLeafNameLocal* e) -> uint8_t* { return xfs_attr_leaf_name_local_name(e) + e->namelen; }
inline auto xfs_attr_leaf_name_local_value(const XfsAttrLeafNameLocal* e) -> const uint8_t* {
    return xfs_attr_leaf_name_local_name(e) + e->namelen;
}

// Remote attribute name in leaf block (value stored in separate block)
struct XfsAttrLeafNameRemote {
    Be32 valueblk;    // logical attr block number of value data
    Be32 valuelen;    // total length of value
    uint8_t namelen;  // length of name
    uint8_t name[];   // name bytes  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays)
} __attribute__((packed));

inline auto xfs_attr_leaf_name_remote_name(XfsAttrLeafNameRemote* e) -> uint8_t* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    return e->name;
}
inline auto xfs_attr_leaf_name_remote_name(const XfsAttrLeafNameRemote* e) -> const uint8_t* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    return e->name;
}

// --- Remote attribute value block header (XFS v5) ---
constexpr uint32_t XFS_ATTR3_RMT_MAGIC = 0x5841524d;  // 'XARM'
constexpr size_t XFS_ATTR3_RMT_CRC_OFF = 12;          // offsetof(XfsAttr3RmtHdr, rm_crc)

struct XfsAttr3RmtHdr {
    Be32 rm_magic;     // XFS_ATTR3_RMT_MAGIC
    Be32 rm_offset;    // byte offset of this block's data within the total value
    Be32 rm_bytes;     // data bytes stored in this block
    Be32 rm_crc;       // CRC of block (with this field set to zero during computation)
    Be64 rm_owner;     // owning inode number
    Be64 rm_blkno;     // device block number of this block
    Be64 rm_lsn;       // log sequence number
    XfsUuidT rm_uuid;  // filesystem UUID
} __attribute__((packed));
static_assert(sizeof(XfsAttr3RmtHdr) == 56);

// ============================================================================
// Parent Pointer Structures (XFS_SB_FEAT_INCOMPAT_PARENT)
// ============================================================================

// Parent pointer on-disk record - stored as the xattr name.
// The xattr value is the directory entry name (filename).
struct XfsParentRec {
    Be64 p_ino;  // parent inode number
    Be32 p_gen;  // parent inode generation
} __attribute__((packed));
static_assert(sizeof(XfsParentRec) == 12);

// ============================================================================
// Journal / Log Format
// ============================================================================

constexpr uint32_t XLOG_HEADER_MAGIC_NUM = 0xFEEDbabe;
constexpr size_t XLOG_HEADER_SIZE = 512;

// Log clients
constexpr uint8_t XFS_TRANSACTION = 0x69;
constexpr uint8_t XFS_LOG = 0xAA;

// Unmount magic
constexpr uint16_t XLOG_UNMOUNT_TYPE = 0x556E;

// LSN helpers: upper 32 bits = cycle, lower 32 bits = block offset
inline auto xlog_lsn_cycle(xfs_lsn_t lsn) -> uint32_t { return static_cast<uint32_t>(static_cast<uint64_t>(lsn) >> 32); }
inline auto xlog_lsn_block(xfs_lsn_t lsn) -> uint32_t { return static_cast<uint32_t>(lsn); }

// Log record header (512 bytes on disk)
struct XlogRecHeader {
    Be32 h_magicno;                      // XLOG_HEADER_MAGIC_NUM
    Be32 h_cycle;                        // log cycle of this record
    Be32 h_version;                      // log version (2)
    Be32 h_len;                          // length of the log body (bytes)
    Be64 h_lsn;                          // cycle.block of this record
    Be64 h_tail_lsn;                     // oldest active log record
    uint32_t h_crc;                      // CRC (little-endian)
    Be32 h_prev_block;                   // block of previous record
    Be32 h_num_logops;                   // number of log operations in body
    std::array<Be32, 246> h_cycle_data;  // cycle data from data blocks
                                         // (replaces first word of each block)
    Be32 h_fmt;                          // log format
    XfsUuidT h_fs_uuid;                  // filesystem UUID
    Be32 h_size;                         // iclog size
} __attribute__((packed));
static_assert(sizeof(XlogRecHeader) == 1052);

// Log item header (precedes each logged item in the log body)
struct XfsLogIovec {
    // In-memory representation; on-disk the items are stored sequentially
    void* i_addr;
    int i_len;
    uint32_t i_type;
};

// ============================================================================
// Helpers: AG number / block conversion
// ============================================================================

// Extract AG number from an absolute filesystem block
inline auto xfs_ag_number(xfs_fsblock_t fsbno, uint8_t agblklog) -> xfs_agnumber_t {
    return static_cast<xfs_agnumber_t>(fsbno >> agblklog);
}

// Extract AG-relative block from an absolute filesystem block
inline auto xfs_ag_block(xfs_fsblock_t fsbno, uint8_t agblklog) -> xfs_agblock_t {
    return static_cast<xfs_agblock_t>(fsbno & ((1ULL << agblklog) - 1));
}

// Build an absolute filesystem block from AG number and AG-relative block
inline auto xfs_agbno_to_fsbno(xfs_agnumber_t agno, xfs_agblock_t agbno, uint8_t agblklog) -> xfs_fsblock_t {
    return (static_cast<xfs_fsblock_t>(agno) << agblklog) | agbno;
}

// Extract AG-relative inode number from absolute inode number
inline auto xfs_ag_ino(xfs_ino_t ino, uint32_t agino_log) -> xfs_agino_t {
    return static_cast<xfs_agino_t>(ino & ((1ULL << agino_log) - 1));
}

// Extract AG number from absolute inode number
inline auto xfs_ino_ag(xfs_ino_t ino, uint32_t agino_log) -> xfs_agnumber_t { return static_cast<xfs_agnumber_t>(ino >> agino_log); }

}  // namespace ker::vfs::xfs
// NOLINTEND(cppcoreguidelines-pro-type-member-init)
