// XFS Extended Attribute subsystem - implementation.
//
// Handles shortform (inline in inode attr fork), leaf, and node attribute
// formats for get, list, set, and remove operations.
//
// Reference: reference/xfs/libxfs/xfs_attr.c, xfs_attr_sf.h, xfs_attr_leaf.c

#include "xfs_attr.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <util/crc32c.hpp>
#include <vfs/fs/xfs/xfs_alloc.hpp>
#include <vfs/fs/xfs/xfs_bmap.hpp>
#include <vfs/fs/xfs/xfs_btree.hpp>
#include <vfs/fs/xfs/xfs_dir2.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_inode.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

#include "net/endian.hpp"
#include "vfs/buffer_cache.hpp"

namespace ker::vfs::xfs {

using mod::dbg::log;

// ============================================================================
// ENOATTR - Linux uses ENODATA (61) for missing xattrs
// ============================================================================
constexpr int ENOATTR = 61;

// ============================================================================
// Shortform attribute helpers
// ============================================================================

namespace {

// Get the shortform header from the attr fork's local data.
auto sf_hdr(const XfsInode* ip) -> const XfsAttrSfHdr* {
    if (!ip->has_attr_fork || ip->attr_fork.format != XFS_DINODE_FMT_LOCAL) {
        return nullptr;
    }
    if (ip->attr_fork.local.data == nullptr || ip->attr_fork.local.size < sizeof(XfsAttrSfHdr)) {
        return nullptr;
    }
    return reinterpret_cast<const XfsAttrSfHdr*>(ip->attr_fork.local.data);
}

// Get mutable shortform header.
auto sf_hdr_mut(XfsInode* ip) -> XfsAttrSfHdr* {
    if (!ip->has_attr_fork || ip->attr_fork.format != XFS_DINODE_FMT_LOCAL) {
        return nullptr;
    }
    if (ip->attr_fork.local.data == nullptr || ip->attr_fork.local.size < sizeof(XfsAttrSfHdr)) {
        return nullptr;
    }
    return reinterpret_cast<XfsAttrSfHdr*>(ip->attr_fork.local.data);
}

// Compare an attribute name + flags against a search key.
auto name_match(const XfsAttrSfEntry* entry, const uint8_t* name, uint16_t namelen, uint8_t flags) -> bool {
    if (entry->namelen != namelen) {
        return false;
    }
    if ((entry->flags & XFS_ATTR_NSP_ONDISK_MASK) != (flags & XFS_ATTR_NSP_ONDISK_MASK)) {
        return false;
    }
    return __builtin_memcmp(xfs_attr_sf_entry_name(entry), name, namelen) == 0;
}

// ============================================================================
// Shortform: get
// ============================================================================

auto sf_get(const XfsInode* ip, const uint8_t* name, uint16_t namelen, uint8_t flags, void* value, uint32_t valuelen) -> int {
    const auto* hdr = sf_hdr(ip);
    if (hdr == nullptr) {
        return -ENOATTR;
    }

    const auto* base = reinterpret_cast<const uint8_t*>(hdr);
    size_t const TOTAL = hdr->totsize.to_cpu();
    size_t pos = sizeof(XfsAttrSfHdr);

    for (uint8_t i = 0; i < hdr->count; i++) {
        if (pos + sizeof(XfsAttrSfEntry) > TOTAL) {
            break;
        }
        const auto* entry = reinterpret_cast<const XfsAttrSfEntry*>(base + pos);
        size_t const ENTRY_SIZE = xfs_attr_sf_entry_size(entry);
        if (pos + ENTRY_SIZE > TOTAL) {
            break;
        }

        if (name_match(entry, name, namelen, flags)) {
            // Found it
            if (value == nullptr) {
                return static_cast<int>(entry->valuelen);
            }
            if (valuelen < entry->valuelen) {
                return -ERANGE;
            }
            __builtin_memcpy(value, xfs_attr_sf_entry_value(entry), entry->valuelen);
            return static_cast<int>(entry->valuelen);
        }

        pos += ENTRY_SIZE;
    }

    return -ENOATTR;
}

// ============================================================================
// Shortform: list
// ============================================================================

auto sf_list(const XfsInode* ip, XfsAttrIterFn fn, void* priv) -> int {
    const auto* hdr = sf_hdr(ip);
    if (hdr == nullptr) {
        return 0;
    }

    const auto* base = reinterpret_cast<const uint8_t*>(hdr);
    size_t const TOTAL = hdr->totsize.to_cpu();
    size_t pos = sizeof(XfsAttrSfHdr);

    for (uint8_t i = 0; i < hdr->count; i++) {
        if (pos + sizeof(XfsAttrSfEntry) > TOTAL) {
            break;
        }
        const auto* entry = reinterpret_cast<const XfsAttrSfEntry*>(base + pos);
        size_t const ENTRY_SIZE = xfs_attr_sf_entry_size(entry);
        if (pos + ENTRY_SIZE > TOTAL) {
            break;
        }

        XfsAttrEntry ae{};
        ae.name = xfs_attr_sf_entry_name(entry);
        ae.namelen = entry->namelen;
        ae.value = xfs_attr_sf_entry_value(entry);
        ae.valuelen = entry->valuelen;
        ae.flags = entry->flags;

        int const RC = fn(&ae, priv);
        if (RC != 0) {
            return RC;
        }

        pos += ENTRY_SIZE;
    }

    return 0;
}

// ============================================================================
// BTREE attr-fork extent enumeration
// ============================================================================

// Enumerate all extents from the attr-fork B+tree root.
// Returns a heap-allocated array of XfsBmbtIrec records and writes the count.
// The caller must delete[] the returned pointer (or it may be nullptr if the
// tree is empty or on error, with *out_count == 0).
auto btree_attr_list_extents(XfsInode* ip, uint32_t* out_count) -> XfsBmbtIrec* {
    *out_count = 0;
    const XfsIforkBtree& bt = ip->attr_fork.btree;
    if (bt.root == nullptr || bt.root_size < sizeof(XfsBmdrBlock)) {
        return nullptr;
    }

    const auto* bmdr = reinterpret_cast<const XfsBmdrBlock*>(bt.root);
    uint16_t const LEVEL = bmdr->bb_level.to_cpu();
    uint16_t const NUMRECS = bmdr->bb_numrecs.to_cpu();
    if (NUMRECS == 0) {
        return nullptr;
    }

    // Leftmost child pointer (first ptr in the root)
    const uint8_t* ptrs_base = bt.root + sizeof(XfsBmdrBlock) + (static_cast<size_t>(NUMRECS) * sizeof(XfsBmbtKey));
    Be64 ptr_val{};
    __builtin_memcpy(&ptr_val, ptrs_base, sizeof(Be64));
    uint64_t const CHILD_BLOCK = ptr_val.to_cpu();

    XfsBtreeCursor<XfsBmbtTraits> cur;
    cur.mount = ip->mount;

    XfsBmbtIrec target{};
    target.br_startoff = 0;

    int const RC = xfs_btree_lookup(&cur, CHILD_BLOCK, LEVEL, target, XfsBtreeLookup::GE);
    if (RC != 0) {
        return nullptr;
    }

    constexpr uint32_t MAX_ATTR_EXTENTS = 4096;
    auto* extents = new (std::nothrow) XfsBmbtIrec[MAX_ATTR_EXTENTS];
    if (extents == nullptr) {
        return nullptr;
    }

    uint32_t n = 0;
    while (n < MAX_ATTR_EXTENTS) {
        extents[n++] = xfs_btree_get_rec(&cur);
        if (xfs_btree_increment(&cur) != 0) {
            break;
        }
    }

    *out_count = n;
    return extents;
}

// ============================================================================
// Attr extent logical→physical block mapping
// ============================================================================

// Map a logical attr block number (dablk_t) to a physical fsblock using
// a pre-enumerated extent list.  Returns NULLFSBLOCK if not found.
auto attr_extents_map_logblk(const XfsBmbtIrec* extents, uint32_t count, xfs_dablk_t logblk) -> xfs_fsblock_t {
    for (uint32_t i = 0; i < count; i++) {
        xfs_fileoff_t const START = extents[i].br_startoff;
        xfs_fileoff_t const END = START + extents[i].br_blockcount;
        if (logblk >= START && logblk < END) {
            return extents[i].br_startblock + (logblk - START);
        }
    }
    return NULLFSBLOCK;
}

// ============================================================================
// Remote attribute value reads (Phase 2)
// ============================================================================

// Read a remote attribute value from consecutive attr-fork blocks.
// valueblk: logical attr block where the value starts
// valuelen: total value byte count
// buf/buflen: caller buffer (nullptr for size-only query)
// extents/ext_count: attr fork extent mapping
auto attr_read_remote_value(XfsMountContext* mount, const XfsBmbtIrec* extents, uint32_t ext_count, xfs_dablk_t valueblk, uint32_t valuelen,
                            void* buf, uint32_t buflen) -> int {
    if (buf == nullptr) {
        return static_cast<int>(valuelen);
    }
    if (buflen < valuelen) {
        return -ERANGE;
    }

    const size_t BLK_SIZE = mount->block_size;
    const size_t HDR_SIZE = sizeof(XfsAttr3RmtHdr);
    const size_t DATA_PER_BLK = BLK_SIZE - HDR_SIZE;

    uint32_t bytes_read = 0;
    xfs_dablk_t cur_logblk = valueblk;

    while (bytes_read < valuelen) {
        xfs_fsblock_t const PHYS = attr_extents_map_logblk(extents, ext_count, cur_logblk);
        if (PHYS == NULLFSBLOCK) {
            log("[xfs attr] remote value: no extent for logical block %u\n", cur_logblk);
            return -EIO;
        }

        BufHead* bh = xfs_buf_read(mount, PHYS);
        if (bh == nullptr) {
            return -EIO;
        }

        const auto* rmt = reinterpret_cast<const XfsAttr3RmtHdr*>(bh->data);
        if (rmt->rm_magic.to_cpu() != XFS_ATTR3_RMT_MAGIC) {
            log("[xfs attr] remote value block bad magic 0x%x\n", rmt->rm_magic.to_cpu());
            brelse(bh);
            return -EIO;
        }

        uint32_t const OFFSET = rmt->rm_offset.to_cpu();
        uint32_t const NBYTES = rmt->rm_bytes.to_cpu();

        if (OFFSET != bytes_read || NBYTES == 0 || NBYTES > DATA_PER_BLK) {
            log("[xfs attr] remote value block corrupt: offset=%u expected=%u bytes=%u\n", OFFSET, bytes_read, NBYTES);
            brelse(bh);
            return -EIO;
        }

        uint32_t const COPY_LEN = std::min(NBYTES, valuelen - bytes_read);
        __builtin_memcpy(static_cast<uint8_t*>(buf) + bytes_read, bh->data + HDR_SIZE, COPY_LEN);
        bytes_read += COPY_LEN;
        brelse(bh);
        cur_logblk++;
    }

    return static_cast<int>(valuelen);
}

// ============================================================================
// Leaf / Node attribute helpers
// ============================================================================

// Read a single leaf block and iterate its entries.
// Returns 0 immediately for non-leaf blocks (DA node blocks are skipped).
auto leaf_block_iterate(XfsInode* ip, xfs_fsblock_t blkno, XfsAttrIterFn fn, void* priv) -> int {
    XfsMountContext* mount = ip->mount;
    BufHead* bh = xfs_buf_read(mount, blkno);
    if (bh == nullptr) {
        return -EIO;
    }

    const auto* leaf = reinterpret_cast<const XfsAttr3LeafHdr*>(bh->data);

    // Skip non-leaf blocks (e.g. DA node blocks with XFS_DA3_NODE_MAGIC)
    if (leaf->info.hdr.magic.to_cpu() != XFS_ATTR3_LEAF_MAGIC) {
        brelse(bh);
        return 0;
    }

    uint16_t const COUNT = leaf->count.to_cpu();
    const auto* entries = reinterpret_cast<const XfsAttrLeafEntry*>(bh->data + sizeof(XfsAttr3LeafHdr));

    for (uint16_t i = 0; i < COUNT; i++) {
        uint16_t const NAMEIDX = entries[i].nameidx.to_cpu();
        uint8_t const EFLAGS = entries[i].flags;

        if ((EFLAGS & XFS_ATTR_INCOMPLETE) != 0) {
            continue;  // skip incomplete entries
        }

        if ((EFLAGS & XFS_ATTR_LOCAL) != 0) {
            // Local attribute - name and value are in the leaf block
            const auto* local = reinterpret_cast<const XfsAttrLeafNameLocal*>(bh->data + NAMEIDX);
            XfsAttrEntry ae{};
            ae.name = xfs_attr_leaf_name_local_name(local);
            ae.namelen = local->namelen;
            ae.value = xfs_attr_leaf_name_local_value(local);
            ae.valuelen = local->valuelen.to_cpu();
            ae.flags = EFLAGS;

            int const RC = fn(&ae, priv);
            if (RC != 0) {
                brelse(bh);
                return RC;
            }
        } else {
            // Remote attribute - report name and size; value must be fetched separately
            const auto* remote = reinterpret_cast<const XfsAttrLeafNameRemote*>(bh->data + NAMEIDX);
            XfsAttrEntry ae{};
            ae.name = xfs_attr_leaf_name_remote_name(remote);
            ae.namelen = remote->namelen;
            ae.value = nullptr;
            ae.valuelen = remote->valuelen.to_cpu();
            ae.flags = EFLAGS;

            int const RC = fn(&ae, priv);
            if (RC != 0) {
                brelse(bh);
                return RC;
            }
        }
    }

    brelse(bh);
    return 0;
}

// Look up a single attribute in a leaf block.
// extents/ext_count: attr fork extent map needed to resolve remote value blocks.
// Returns 0 immediately for non-leaf blocks (skips DA node blocks).
auto leaf_block_get(XfsInode* ip, xfs_fsblock_t blkno, const XfsBmbtIrec* extents, uint32_t ext_count, const uint8_t* name,
                    uint16_t namelen, uint8_t flags, void* value, uint32_t valuelen) -> int {
    XfsMountContext* mount = ip->mount;
    BufHead* bh = xfs_buf_read(mount, blkno);
    if (bh == nullptr) {
        return -EIO;
    }

    const auto* leaf = reinterpret_cast<const XfsAttr3LeafHdr*>(bh->data);

    // Skip non-leaf blocks (DA node blocks)
    if (leaf->info.hdr.magic.to_cpu() != XFS_ATTR3_LEAF_MAGIC) {
        brelse(bh);
        return -ENOATTR;
    }

    uint16_t const COUNT = leaf->count.to_cpu();
    const auto* entries = reinterpret_cast<const XfsAttrLeafEntry*>(bh->data + sizeof(XfsAttr3LeafHdr));

    for (uint16_t i = 0; i < COUNT; i++) {
        uint16_t const NAMEIDX = entries[i].nameidx.to_cpu();
        uint8_t const EFLAGS = entries[i].flags;

        if ((EFLAGS & XFS_ATTR_INCOMPLETE) != 0) {
            continue;
        }
        if ((EFLAGS & XFS_ATTR_NSP_ONDISK_MASK) != (flags & XFS_ATTR_NSP_ONDISK_MASK)) {
            continue;
        }

        if ((EFLAGS & XFS_ATTR_LOCAL) != 0) {
            const auto* local = reinterpret_cast<const XfsAttrLeafNameLocal*>(bh->data + NAMEIDX);
            if (local->namelen == namelen && __builtin_memcmp(xfs_attr_leaf_name_local_name(local), name, namelen) == 0) {
                uint32_t const VLEN = local->valuelen.to_cpu();
                if (value == nullptr) {
                    brelse(bh);
                    return static_cast<int>(VLEN);
                }
                if (valuelen < VLEN) {
                    brelse(bh);
                    return -ERANGE;
                }
                __builtin_memcpy(value, xfs_attr_leaf_name_local_value(local), VLEN);
                brelse(bh);
                return static_cast<int>(VLEN);
            }
        } else {
            const auto* remote = reinterpret_cast<const XfsAttrLeafNameRemote*>(bh->data + NAMEIDX);
            if (remote->namelen == namelen && __builtin_memcmp(xfs_attr_leaf_name_remote_name(remote), name, namelen) == 0) {
                uint32_t const VLEN = remote->valuelen.to_cpu();
                xfs_dablk_t const VBLK = remote->valueblk.to_cpu();
                brelse(bh);
                // The remote value block/length and caller buffer/length pairs intentionally share similar names.
                // NOLINTNEXTLINE(readability-suspicious-call-argument)
                return attr_read_remote_value(mount, extents, ext_count, VBLK, VLEN, value, valuelen);
            }
        }
    }

    brelse(bh);
    return -ENOATTR;
}

// ============================================================================
// EXTENTS format: iterate and get
// ============================================================================

auto extents_iterate(XfsInode* ip, XfsAttrIterFn fn, void* priv) -> int {
    if (ip->attr_fork.extents.count == 0) {
        return 0;
    }
    for (uint32_t i = 0; i < ip->attr_fork.extents.count; i++) {
        const XfsBmbtIrec& ext = ip->attr_fork.extents.list[i];
        for (xfs_extlen_t b = 0; b < ext.br_blockcount; b++) {
            int const RC = leaf_block_iterate(ip, ext.br_startblock + b, fn, priv);
            if (RC != 0) {
                return RC;
            }
        }
    }
    return 0;
}

auto extents_get(XfsInode* ip, const uint8_t* name, uint16_t namelen, uint8_t flags, void* value, uint32_t valuelen) -> int {
    if (ip->attr_fork.extents.count == 0) {
        return -ENOATTR;
    }
    const XfsBmbtIrec* extents = ip->attr_fork.extents.list;
    uint32_t const EXT_COUNT = ip->attr_fork.extents.count;

    for (uint32_t i = 0; i < EXT_COUNT; i++) {
        const XfsBmbtIrec& ext = extents[i];
        for (xfs_extlen_t b = 0; b < ext.br_blockcount; b++) {
            int const RC = leaf_block_get(ip, ext.br_startblock + b, extents, EXT_COUNT, name, namelen, flags, value, valuelen);
            if (RC != -ENOATTR) {
                return RC;
            }
        }
    }
    return -ENOATTR;
}

// ============================================================================
// BTREE attr-fork: iterate and get (Phase 1)
// ============================================================================

auto btree_iterate(XfsInode* ip, XfsAttrIterFn fn, void* priv) -> int {
    uint32_t ext_count = 0;
    XfsBmbtIrec const* extents = btree_attr_list_extents(ip, &ext_count);
    if (extents == nullptr || ext_count == 0) {
        delete[] extents;
        return 0;
    }

    int rc = 0;
    for (uint32_t i = 0; i < ext_count && rc == 0; i++) {
        for (xfs_extlen_t b = 0; b < extents[i].br_blockcount && rc == 0; b++) {
            rc = leaf_block_iterate(ip, extents[i].br_startblock + b, fn, priv);
        }
    }

    delete[] extents;
    return rc;
}

auto btree_get(XfsInode* ip, const uint8_t* name, uint16_t namelen, uint8_t flags, void* value, uint32_t valuelen) -> int {
    uint32_t ext_count = 0;
    XfsBmbtIrec const* extents = btree_attr_list_extents(ip, &ext_count);
    if (extents == nullptr || ext_count == 0) {
        delete[] extents;
        return -ENOATTR;
    }

    int rc = -ENOATTR;
    for (uint32_t i = 0; i < ext_count && rc == -ENOATTR; i++) {
        for (xfs_extlen_t b = 0; b < extents[i].br_blockcount && rc == -ENOATTR; b++) {
            rc = leaf_block_get(ip, extents[i].br_startblock + b, extents, ext_count, name, namelen, flags, value, valuelen);
        }
    }

    delete[] extents;
    return rc;
}

// ============================================================================
// Shortform → leaf conversion (Phase 3)
// ============================================================================

// Build an on-disk CRC for a leaf block (CRC field is at XFS_ATTR3_LEAF_CRC_OFF = 12).
void attr_leaf_compute_crc(uint8_t* block, size_t block_size) {
    // Zero the CRC field before computing
    __builtin_memset(block + XFS_ATTR3_LEAF_CRC_OFF, 0, 4);
    uint32_t const CRC = util::crc32c_block_with_cksum(block, block_size, XFS_ATTR3_LEAF_CRC_OFF);
    // XFS metadata checksums are stored little-endian, including the checksum
    // slot embedded in the otherwise big-endian DA header.
    __builtin_memcpy(block + XFS_ATTR3_LEAF_CRC_OFF, &CRC, sizeof(CRC));
}

struct AttrLeafBuildRec {
    xfs_dahash_t hash;
    const uint8_t* name_ptr;
    uint16_t namelen;
    const uint8_t* val_ptr;
    uint32_t valuelen;
    xfs_dablk_t valueblk;
    uint8_t flags;
    bool local;
};

auto attr_leaf_payload_size(const AttrLeafBuildRec& rec) -> size_t {
    if (rec.local) {
        return sizeof(XfsAttrLeafNameLocal) + rec.namelen + rec.valuelen;
    }
    return sizeof(XfsAttrLeafNameRemote) + rec.namelen;
}

auto attr_leaf_name_match(const AttrLeafBuildRec& rec, const uint8_t* name, uint16_t namelen, uint8_t flags) -> bool {
    if (rec.namelen != namelen) {
        return false;
    }
    if ((rec.flags & XFS_ATTR_NSP_ONDISK_MASK) != (flags & XFS_ATTR_NSP_ONDISK_MASK)) {
        return false;
    }
    return __builtin_memcmp(rec.name_ptr, name, namelen) == 0;
}

auto attr_leaf_rebuild(XfsInode* ip, XfsTransaction* tp, BufHead* bh, const uint8_t* name, uint16_t namelen, const uint8_t* value,
                       uint32_t valuelen, uint8_t flags, bool remove) -> int {
    if (namelen > UINT8_MAX) {
        return -EINVAL;
    }
    if (!remove && valuelen > UINT16_MAX) {
        return -E2BIG;
    }

    XfsMountContext* ctx = ip->mount;
    size_t const BLK_SIZE = ctx->block_size;
    const auto* leaf = reinterpret_cast<const XfsAttr3LeafHdr*>(bh->data);
    if (leaf->info.hdr.magic.to_cpu() != XFS_ATTR3_LEAF_MAGIC) {
        return -EOPNOTSUPP;
    }

    uint16_t const COUNT = leaf->count.to_cpu();
    size_t const ENTRY_ARRAY_END = sizeof(XfsAttr3LeafHdr) + (static_cast<size_t>(COUNT) * sizeof(XfsAttrLeafEntry));
    if (ENTRY_ARRAY_END > BLK_SIZE) {
        return -EINVAL;
    }

    uint32_t const MAX_RECS = static_cast<uint32_t>(COUNT) + (remove ? 0U : 1U);
    auto* recs = new (std::nothrow) AttrLeafBuildRec[MAX_RECS == 0 ? 1 : MAX_RECS];
    if (recs == nullptr) {
        return -ENOMEM;
    }

    const auto* entries = reinterpret_cast<const XfsAttrLeafEntry*>(bh->data + sizeof(XfsAttr3LeafHdr));
    uint32_t n = 0;
    bool found = false;

    for (uint16_t i = 0; i < COUNT; i++) {
        uint16_t const NAMEIDX = entries[i].nameidx.to_cpu();
        uint8_t const EFLAGS = entries[i].flags;

        if ((EFLAGS & XFS_ATTR_INCOMPLETE) != 0) {
            continue;
        }
        if (NAMEIDX >= BLK_SIZE) {
            delete[] recs;
            return -EINVAL;
        }

        AttrLeafBuildRec rec{};
        rec.hash = entries[i].hashval.to_cpu();
        rec.flags = EFLAGS;
        rec.local = (EFLAGS & XFS_ATTR_LOCAL) != 0;

        if (rec.local) {
            if (NAMEIDX + sizeof(XfsAttrLeafNameLocal) > BLK_SIZE) {
                delete[] recs;
                return -EINVAL;
            }
            const auto* local = reinterpret_cast<const XfsAttrLeafNameLocal*>(bh->data + NAMEIDX);
            rec.name_ptr = xfs_attr_leaf_name_local_name(local);
            rec.namelen = local->namelen;
            rec.val_ptr = xfs_attr_leaf_name_local_value(local);
            rec.valuelen = local->valuelen.to_cpu();
            if (NAMEIDX + attr_leaf_payload_size(rec) > BLK_SIZE) {
                delete[] recs;
                return -EINVAL;
            }
        } else {
            if (NAMEIDX + sizeof(XfsAttrLeafNameRemote) > BLK_SIZE) {
                delete[] recs;
                return -EINVAL;
            }
            const auto* remote = reinterpret_cast<const XfsAttrLeafNameRemote*>(bh->data + NAMEIDX);
            rec.name_ptr = xfs_attr_leaf_name_remote_name(remote);
            rec.namelen = remote->namelen;
            rec.val_ptr = nullptr;
            rec.valuelen = remote->valuelen.to_cpu();
            rec.valueblk = remote->valueblk.to_cpu();
            if (NAMEIDX + attr_leaf_payload_size(rec) > BLK_SIZE) {
                delete[] recs;
                return -EINVAL;
            }
        }

        if (attr_leaf_name_match(rec, name, namelen, flags)) {
            found = true;
            if (!rec.local) {
                delete[] recs;
                return -EOPNOTSUPP;
            }
            continue;
        }

        recs[n++] = rec;
    }

    if (remove) {
        if (!found) {
            delete[] recs;
            return -ENOATTR;
        }
    } else {
        recs[n++] = AttrLeafBuildRec{.hash = xfs_da_hashname(name, namelen),
                                     .name_ptr = name,
                                     .namelen = namelen,
                                     .val_ptr = value,
                                     .valuelen = valuelen,
                                     .valueblk = 0,
                                     .flags = static_cast<uint8_t>((flags & XFS_ATTR_NSP_ONDISK_MASK) | XFS_ATTR_LOCAL),
                                     .local = true};
    }

    for (uint32_t i = 1; i < n; i++) {
        AttrLeafBuildRec const TMP = recs[i];
        int j = static_cast<int>(i) - 1;
        while (j >= 0 && recs[static_cast<uint32_t>(j)].hash > TMP.hash) {
            recs[static_cast<uint32_t>(j + 1)] = recs[static_cast<uint32_t>(j)];
            j--;
        }
        recs[static_cast<uint32_t>(j + 1)] = TMP;
    }

    size_t payload_bytes = 0;
    for (uint32_t i = 0; i < n; i++) {
        payload_bytes += attr_leaf_payload_size(recs[i]);
    }
    size_t const TOTAL_BYTES = sizeof(XfsAttr3LeafHdr) + (static_cast<size_t>(n) * sizeof(XfsAttrLeafEntry)) + payload_bytes;
    if (TOTAL_BYTES > BLK_SIZE) {
        delete[] recs;
        return -ENOSPC;
    }

    auto* new_block = new (std::nothrow) uint8_t[BLK_SIZE];
    if (new_block == nullptr) {
        delete[] recs;
        return -ENOMEM;
    }
    __builtin_memset(new_block, 0, BLK_SIZE);
    __builtin_memcpy(new_block, bh->data, sizeof(XfsAttr3LeafHdr));

    auto* new_leaf = reinterpret_cast<XfsAttr3LeafHdr*>(new_block);
    auto* new_entries = reinterpret_cast<XfsAttrLeafEntry*>(new_block + sizeof(XfsAttr3LeafHdr));
    auto firstused = static_cast<uint16_t>(BLK_SIZE);
    uint16_t usedbytes = 0;

    for (uint32_t i = 0; i < n; i++) {
        auto const PAYLOAD = static_cast<uint16_t>(attr_leaf_payload_size(recs[i]));
        firstused -= PAYLOAD;

        if (recs[i].local) {
            auto* local = reinterpret_cast<XfsAttrLeafNameLocal*>(new_block + firstused);
            local->valuelen = Be16::from_cpu(static_cast<uint16_t>(recs[i].valuelen));
            local->namelen = static_cast<uint8_t>(recs[i].namelen);
            __builtin_memcpy(xfs_attr_leaf_name_local_name(local), recs[i].name_ptr, recs[i].namelen);
            if (recs[i].valuelen > 0) {
                __builtin_memcpy(xfs_attr_leaf_name_local_value(local), recs[i].val_ptr, recs[i].valuelen);
            }
        } else {
            auto* remote = reinterpret_cast<XfsAttrLeafNameRemote*>(new_block + firstused);
            remote->valueblk = Be32::from_cpu(recs[i].valueblk);
            remote->valuelen = Be32::from_cpu(recs[i].valuelen);
            remote->namelen = static_cast<uint8_t>(recs[i].namelen);
            __builtin_memcpy(xfs_attr_leaf_name_remote_name(remote), recs[i].name_ptr, recs[i].namelen);
        }

        new_entries[i].hashval = Be32::from_cpu(recs[i].hash);
        new_entries[i].nameidx = Be16::from_cpu(firstused);
        new_entries[i].flags =
            recs[i].local ? static_cast<uint8_t>(recs[i].flags | XFS_ATTR_LOCAL) : static_cast<uint8_t>(recs[i].flags & ~XFS_ATTR_LOCAL);
        new_entries[i].pad2 = 0;
        usedbytes += PAYLOAD;
    }

    new_leaf->count = Be16::from_cpu(static_cast<uint16_t>(n));
    new_leaf->usedbytes = Be16::from_cpu(usedbytes);
    new_leaf->firstused = Be16::from_cpu(firstused);
    new_leaf->holes = 0;
    new_leaf->pad1 = 0;

    auto const FREE_BASE = static_cast<uint16_t>(sizeof(XfsAttr3LeafHdr) + (n * sizeof(XfsAttrLeafEntry)));
    uint16_t const FREE_SIZE = (firstused > FREE_BASE) ? static_cast<uint16_t>(firstused - FREE_BASE) : 0;
    new_leaf->freemap[0].base = Be16::from_cpu(FREE_BASE);
    new_leaf->freemap[0].size = Be16::from_cpu(FREE_SIZE);
    for (size_t i = 1; i < XFS_ATTR_LEAF_MAPSIZE; i++) {
        new_leaf->freemap[i].base = Be16::from_cpu(0);
        new_leaf->freemap[i].size = Be16::from_cpu(0);
    }

    attr_leaf_compute_crc(new_block, BLK_SIZE);
    __builtin_memcpy(bh->data, new_block, BLK_SIZE);
    xfs_trans_log_buf_full(tp, bh);

    delete[] new_block;
    delete[] recs;

    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);
    return 0;
}

auto single_leaf_extent_block(XfsInode* ip, xfs_fsblock_t* out_block) -> int {
    if (ip->attr_fork.extents.list == nullptr || ip->attr_fork.extents.count != 1) {
        return -EOPNOTSUPP;
    }
    const XfsBmbtIrec& ext = ip->attr_fork.extents.list[0];
    if (ext.br_startoff != 0 || ext.br_blockcount != 1 || ext.br_unwritten) {
        return -EOPNOTSUPP;
    }
    *out_block = ext.br_startblock;
    return 0;
}

auto extents_set(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, const uint8_t* value, uint32_t valuelen,
                 uint8_t flags) -> int {
    xfs_fsblock_t leaf_block = NULLFSBLOCK;
    int rc = single_leaf_extent_block(ip, &leaf_block);
    if (rc != 0) {
        return rc;
    }

    BufHead* bh = xfs_buf_read(ip->mount, leaf_block);
    if (bh == nullptr) {
        return -EIO;
    }

    rc = attr_leaf_rebuild(ip, tp, bh, name, namelen, value, valuelen, flags, false);
    brelse(bh);
    return rc;
}

auto extents_remove(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, uint8_t flags) -> int {
    xfs_fsblock_t leaf_block = NULLFSBLOCK;
    int rc = single_leaf_extent_block(ip, &leaf_block);
    if (rc != 0) {
        return rc;
    }

    BufHead* bh = xfs_buf_read(ip->mount, leaf_block);
    if (bh == nullptr) {
        return -EIO;
    }

    rc = attr_leaf_rebuild(ip, tp, bh, name, namelen, nullptr, 0, flags, true);
    brelse(bh);
    return rc;
}

// Convert the inode attr fork from shortform (LOCAL) to leaf (EXTENTS) format.
// Called from sf_set when the new total exceeds the attr fork inline space.
// At call time ip->attr_fork.local still holds the existing shortform data
// (with the old duplicate entry already removed by sf_set if applicable).
// The new attr (name/namelen/val/valuelen/flags) must be appended.
auto sf_to_leaf_convert(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, const uint8_t* val, uint32_t valuelen,
                        uint8_t flags) -> int {
    XfsMountContext* ctx = ip->mount;
    const size_t BLK_SIZE = ctx->block_size;

    //  1. Snapshot existing shortform entries
    const auto* sf_base = ip->attr_fork.local.data;
    const auto* sf_hdrp = reinterpret_cast<const XfsAttrSfHdr*>(sf_base);
    uint8_t const SF_COUNT = (sf_hdrp != nullptr) ? sf_hdrp->count : 0;
    size_t const SF_TOTAL = (sf_hdrp != nullptr) ? sf_hdrp->totsize.to_cpu() : 0;

    // Upper bound on total entries: existing + new.
    uint32_t const TOTAL_ENTRIES = static_cast<uint32_t>(SF_COUNT) + 1;

    // Structures for sort-by-hash.
    struct AttrRec {
        xfs_dahash_t hash;
        const uint8_t* name_ptr;
        uint16_t namelen;
        const uint8_t* val_ptr;
        uint32_t valuelen;
        uint8_t flags;
    };

    auto* recs = new (std::nothrow) AttrRec[TOTAL_ENTRIES];
    if (recs == nullptr) {
        return -ENOMEM;
    }

    // Collect existing sf entries.
    uint32_t n = 0;
    if (sf_hdrp != nullptr && SF_TOTAL >= sizeof(XfsAttrSfHdr)) {
        size_t pos = sizeof(XfsAttrSfHdr);
        for (uint8_t i = 0; i < SF_COUNT && n < TOTAL_ENTRIES - 1; i++) {
            if (pos + sizeof(XfsAttrSfEntry) > SF_TOTAL) {
                break;
            }
            const auto* entry = reinterpret_cast<const XfsAttrSfEntry*>(sf_base + pos);
            size_t const ENTRY_SIZE = xfs_attr_sf_entry_size(entry);
            if (pos + ENTRY_SIZE > SF_TOTAL) {
                break;
            }
            recs[n].name_ptr = xfs_attr_sf_entry_name(entry);
            recs[n].namelen = entry->namelen;
            recs[n].val_ptr = xfs_attr_sf_entry_value(entry);
            recs[n].valuelen = entry->valuelen;
            recs[n].flags = entry->flags;
            recs[n].hash = xfs_da_hashname(xfs_attr_sf_entry_name(entry), entry->namelen);
            n++;
            pos += ENTRY_SIZE;
        }
    }

    // Append the new entry.
    recs[n].name_ptr = name;
    recs[n].namelen = namelen;
    recs[n].val_ptr = val;
    recs[n].valuelen = valuelen;
    recs[n].flags = flags;
    recs[n].hash = xfs_da_hashname(name, namelen);
    n++;

    // Sort by hash (simple insertion sort – n is small).
    for (uint32_t i = 1; i < n; i++) {
        AttrRec const TMP = recs[i];
        int j = static_cast<int>(i) - 1;
        while (j >= 0 && recs[j].hash > TMP.hash) {
            recs[j + 1] = recs[j];
            j--;
        }
        recs[j + 1] = TMP;
    }

    //  2. Validate fit in a single leaf block
    // All sf attrs have valuelen <= 255 (uint8_t field), so they are always
    // stored locally.  The new entry may be larger but still has to fit locally;
    // oversized values require remote value blocks.
    size_t const HEADER_BYTES = sizeof(XfsAttr3LeafHdr);
    size_t const ENTRIES_BYTES = static_cast<size_t>(n) * sizeof(XfsAttrLeafEntry);
    size_t payload_bytes = 0;
    for (uint32_t i = 0; i < n; i++) {
        payload_bytes += sizeof(XfsAttrLeafNameLocal) + recs[i].namelen + recs[i].valuelen;
    }
    size_t const TOTAL_BYTES = HEADER_BYTES + ENTRIES_BYTES + payload_bytes;

    if (TOTAL_BYTES > BLK_SIZE) {
        // Would not fit in a single leaf block.
        delete[] recs;
        log("[xfs attr] sf→leaf: total %zu > blk_size %zu, cannot convert\n", TOTAL_BYTES, BLK_SIZE);
        return -ENOSPC;
    }

    //  3. Allocate a filesystem block
    xfs_agnumber_t const PREF_AG = xfs_ino_ag(ip->ino, ctx->agino_log);
    XfsAllocReq req{};
    req.agno = PREF_AG;
    req.agbno = 0;
    req.minlen = 1;
    req.maxlen = 1;
    req.alignment = 0;

    XfsAllocResult alloc{};
    int const RC = xfs_alloc_extent(ctx, tp, req, &alloc);
    if (RC != 0) {
        delete[] recs;
        return RC;
    }

    xfs_fsblock_t const DISK_BLOCK = xfs_agbno_to_fsbno(alloc.agno, alloc.agbno, ctx->ag_blk_log);

    //  4. Build the leaf block
    BufHead* bh = xfs_buf_get(ctx, DISK_BLOCK);
    if (bh == nullptr) {
        delete[] recs;
        return -EIO;
    }

    uint8_t* block = bh->data;
    __builtin_memset(block, 0, BLK_SIZE);

    // 4a. Header
    auto* lhdr = reinterpret_cast<XfsAttr3LeafHdr*>(block);
    lhdr->info.hdr.magic = Be16::from_cpu(XFS_ATTR3_LEAF_MAGIC);
    lhdr->info.hdr.forw = Be32::from_cpu(0);
    lhdr->info.hdr.back = Be32::from_cpu(0);
    lhdr->info.owner = Be64::from_cpu(ip->ino);
    __builtin_memcpy(&lhdr->info.uuid, &ctx->uuid, sizeof(XfsUuidT));
    {
        // Compute device block number for blkno field
        uint64_t const LINEAR = (static_cast<uint64_t>(alloc.agno) * ctx->ag_blocks) + alloc.agbno;
        size_t const RATIO = BLK_SIZE / ctx->device->block_size;
        lhdr->info.blkno = Be64::from_cpu(LINEAR * RATIO);
    }

    // 4b. Build entries + name/value area from end of block backward.
    auto* leaf_entries = reinterpret_cast<XfsAttrLeafEntry*>(block + sizeof(XfsAttr3LeafHdr));
    auto firstused = static_cast<uint16_t>(BLK_SIZE);
    uint16_t usedbytes = 0;

    for (uint32_t i = 0; i < n; i++) {
        auto const PAYLOAD = static_cast<uint16_t>(sizeof(XfsAttrLeafNameLocal) + recs[i].namelen + recs[i].valuelen);
        firstused -= PAYLOAD;

        auto* local = reinterpret_cast<XfsAttrLeafNameLocal*>(block + firstused);
        local->valuelen = Be16::from_cpu(static_cast<uint16_t>(recs[i].valuelen));
        local->namelen = static_cast<uint8_t>(recs[i].namelen);
        __builtin_memcpy(xfs_attr_leaf_name_local_name(local), recs[i].name_ptr, recs[i].namelen);
        if (recs[i].valuelen > 0) {
            __builtin_memcpy(xfs_attr_leaf_name_local_value(local), recs[i].val_ptr, recs[i].valuelen);
        }

        leaf_entries[i].hashval = Be32::from_cpu(recs[i].hash);
        leaf_entries[i].nameidx = Be16::from_cpu(firstused);
        leaf_entries[i].flags = recs[i].flags | XFS_ATTR_LOCAL;
        leaf_entries[i].pad2 = 0;

        usedbytes += PAYLOAD;
    }

    lhdr->count = Be16::from_cpu(static_cast<uint16_t>(n));
    lhdr->usedbytes = Be16::from_cpu(usedbytes);
    lhdr->firstused = Be16::from_cpu(firstused);
    lhdr->holes = 0;

    // Freemap: single free region between end of entry array and firstused.
    auto const FREE_BASE = static_cast<uint16_t>(sizeof(XfsAttr3LeafHdr) + (n * sizeof(XfsAttrLeafEntry)));
    uint16_t const FREE_SIZE = (firstused > FREE_BASE) ? static_cast<uint16_t>(firstused - FREE_BASE) : 0;
    lhdr->freemap[0].base = Be16::from_cpu(FREE_BASE);
    lhdr->freemap[0].size = Be16::from_cpu(FREE_SIZE);
    lhdr->freemap[1].base = Be16::from_cpu(0);
    lhdr->freemap[1].size = Be16::from_cpu(0);
    lhdr->freemap[2].base = Be16::from_cpu(0);
    lhdr->freemap[2].size = Be16::from_cpu(0);

    // 4c. Compute and write CRC.
    attr_leaf_compute_crc(block, BLK_SIZE);

    bwrite(bh);
    brelse(bh);

    delete[] recs;

    //  5. Switch attr fork from LOCAL to EXTENTS
    delete[] ip->attr_fork.local.data;

    ip->attr_fork.format = XFS_DINODE_FMT_EXTENTS;
    ip->attr_fork.extents.list = new XfsBmbtIrec[1];
    ip->attr_fork.extents.list[0].br_startoff = 0;
    ip->attr_fork.extents.list[0].br_startblock = DISK_BLOCK;
    ip->attr_fork.extents.list[0].br_blockcount = 1;
    ip->attr_fork.extents.list[0].br_unwritten = false;
    ip->attr_fork.extents.count = 1;
    ip->attr_fork.extents.capacity = 1;
    ip->anextents = 1;
    ip->nblocks++;
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);

    return 0;
}

// ============================================================================
// Shortform: set (insert or replace)
// ============================================================================

auto sf_set(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, const uint8_t* val, uint32_t valuelen, uint8_t flags)
    -> int {
    size_t const NEW_ENTRY_SIZE = sizeof(XfsAttrSfEntry) + namelen + valuelen;

    // If there is no attr fork yet, create one (shortform with 0 entries)
    if (!ip->has_attr_fork || ip->attr_fork.format != XFS_DINODE_FMT_LOCAL || ip->attr_fork.local.data == nullptr) {
        // Allocate initial shortform: header + new entry
        size_t const ALLOC_SIZE = sizeof(XfsAttrSfHdr) + NEW_ENTRY_SIZE;
        auto* buf = new (std::nothrow) uint8_t[ALLOC_SIZE];
        if (buf == nullptr) {
            return -ENOMEM;
        }
        __builtin_memset(buf, 0, ALLOC_SIZE);

        auto* new_hdr = reinterpret_cast<XfsAttrSfHdr*>(buf);
        new_hdr->totsize = Be16::from_cpu(static_cast<uint16_t>(ALLOC_SIZE));
        new_hdr->count = 1;
        new_hdr->padding = 0;

        auto* entry = reinterpret_cast<XfsAttrSfEntry*>(buf + sizeof(XfsAttrSfHdr));
        entry->namelen = static_cast<uint8_t>(namelen);
        entry->valuelen = static_cast<uint8_t>(valuelen);
        entry->flags = flags;
        __builtin_memcpy(xfs_attr_sf_entry_name(entry), name, namelen);
        if (valuelen > 0) {
            __builtin_memcpy(xfs_attr_sf_entry_value(entry), val, valuelen);
        }

        // Install into inode
        if (ip->attr_fork.format == XFS_DINODE_FMT_LOCAL && ip->attr_fork.local.data != nullptr) {
            delete[] ip->attr_fork.local.data;
        }
        ip->attr_fork.format = XFS_DINODE_FMT_LOCAL;
        ip->attr_fork.local.data = buf;
        ip->attr_fork.local.size = ALLOC_SIZE;
        ip->has_attr_fork = true;
        ip->anextents = 0;

        // Set forkoff if not already set - place attr fork after data fork.
        // Default: split inode literal area in half if data fork permits it.
        if (ip->forkoff == 0) {
            size_t const INODE_CORE = 176;  // v3 core (XfsDinode size)
            size_t const LITERAL_AREA = ip->mount->inode_size - INODE_CORE;
            // Attr fork starts at forkoff * 8 bytes from data fork start.
            // Set forkoff so attr fork gets the minimum needed space.
            size_t const ATTR_NEEDED = ALLOC_SIZE;
            if (ATTR_NEEDED > LITERAL_AREA) {
                return -ENOSPC;
            }
            // forkoff is in 8-byte units from inode core end (data fork start)
            size_t data_fork_bytes = LITERAL_AREA - ATTR_NEEDED;
            // Round down to 8-byte boundary
            data_fork_bytes &= ~7ULL;
            ip->forkoff = static_cast<uint8_t>(data_fork_bytes >> 3);
        }

        ip->dirty = true;
        return 0;
    }

    auto* hdr = sf_hdr_mut(ip);
    if (hdr == nullptr) {
        return -EINVAL;
    }

    auto* base = ip->attr_fork.local.data;
    size_t total = hdr->totsize.to_cpu();
    size_t pos = sizeof(XfsAttrSfHdr);

    // Check if the attribute already exists - replace it
    for (uint8_t i = 0; i < hdr->count; i++) {
        if (pos + sizeof(XfsAttrSfEntry) > total) {
            break;
        }
        auto* entry = reinterpret_cast<XfsAttrSfEntry*>(base + pos);
        size_t const ENTRY_SIZE = xfs_attr_sf_entry_size(entry);
        if (pos + ENTRY_SIZE > total) {
            break;
        }

        if (name_match(entry, name, namelen, flags)) {
            // Replace in-place if same size
            if (entry->valuelen == valuelen) {
                if (valuelen > 0) {
                    __builtin_memcpy(xfs_attr_sf_entry_value(entry), val, valuelen);
                }
                ip->dirty = true;
                return 0;
            }

            // Different size - remove old entry, then fall through to insert
            size_t const TAIL_START = pos + ENTRY_SIZE;
            size_t const TAIL_LEN = total - TAIL_START;
            if (TAIL_LEN > 0) {
                __builtin_memmove(base + pos, base + TAIL_START, TAIL_LEN);
            }
            total -= ENTRY_SIZE;
            hdr->count--;
            hdr->totsize = Be16::from_cpu(static_cast<uint16_t>(total));
            break;
        }

        pos += ENTRY_SIZE;
    }

    // Insert new entry at end
    size_t const NEW_TOTAL = total + NEW_ENTRY_SIZE;

    // Check if it fits in the attr fork space
    size_t const INODE_CORE = 176;  // v3 core size
    size_t const DATA_FORK_BYTES = static_cast<size_t>(ip->forkoff) << 3;
    size_t const ATTR_FORK_SPACE = ip->mount->inode_size - INODE_CORE - DATA_FORK_BYTES;
    if (NEW_TOTAL > ATTR_FORK_SPACE) {
        // Convert shortform to leaf block format
        return sf_to_leaf_convert(ip, tp, name, namelen, val, valuelen, flags);
    }

    // Reallocate the buffer
    auto* new_buf = new uint8_t[NEW_TOTAL];
    if (new_buf == nullptr) {
        return -ENOMEM;
    }
    __builtin_memcpy(new_buf, base, total);

    // Append the new entry
    auto* new_entry = reinterpret_cast<XfsAttrSfEntry*>(new_buf + total);
    new_entry->namelen = static_cast<uint8_t>(namelen);
    new_entry->valuelen = static_cast<uint8_t>(valuelen);
    new_entry->flags = flags;
    __builtin_memcpy(xfs_attr_sf_entry_name(new_entry), name, namelen);
    if (valuelen > 0) {
        __builtin_memcpy(xfs_attr_sf_entry_value(new_entry), val, valuelen);
    }

    // Update header
    auto* new_hdr = reinterpret_cast<XfsAttrSfHdr*>(new_buf);
    new_hdr->count++;
    new_hdr->totsize = Be16::from_cpu(static_cast<uint16_t>(NEW_TOTAL));

    // Replace buffer in inode
    delete[] ip->attr_fork.local.data;
    ip->attr_fork.local.data = new_buf;
    ip->attr_fork.local.size = NEW_TOTAL;
    ip->dirty = true;

    return 0;
}

// ============================================================================
// Shortform: remove
// ============================================================================

auto sf_remove(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, uint8_t flags) -> int {
    (void)tp;

    auto* hdr = sf_hdr_mut(ip);
    if (hdr == nullptr) {
        return -ENOATTR;
    }

    auto* base = ip->attr_fork.local.data;
    size_t total = hdr->totsize.to_cpu();
    size_t pos = sizeof(XfsAttrSfHdr);

    for (uint8_t i = 0; i < hdr->count; i++) {
        if (pos + sizeof(XfsAttrSfEntry) > total) {
            break;
        }
        auto* entry = reinterpret_cast<XfsAttrSfEntry*>(base + pos);
        size_t const ENTRY_SIZE = xfs_attr_sf_entry_size(entry);
        if (pos + ENTRY_SIZE > total) {
            break;
        }

        if (name_match(entry, name, namelen, flags)) {
            // Found it - remove by shifting tail data
            size_t const TAIL_START = pos + ENTRY_SIZE;
            size_t const TAIL_LEN = total - TAIL_START;
            if (TAIL_LEN > 0) {
                __builtin_memmove(base + pos, base + TAIL_START, TAIL_LEN);
            }
            total -= ENTRY_SIZE;
            hdr->count--;
            hdr->totsize = Be16::from_cpu(static_cast<uint16_t>(total));
            ip->attr_fork.local.size = total;
            ip->dirty = true;
            return 0;
        }

        pos += ENTRY_SIZE;
    }

    return -ENOATTR;
}

}  // anonymous namespace

// ============================================================================
// Public API
// ============================================================================

auto xfs_attr_get(XfsInode* ip, const uint8_t* name, uint16_t namelen, uint8_t flags, void* value, uint32_t valuelen) -> int {
    if (ip == nullptr || name == nullptr || namelen == 0) {
        return -EINVAL;
    }

    if (!ip->has_attr_fork) {
        return -ENOATTR;
    }

    switch (ip->attr_fork.format) {
        case XFS_DINODE_FMT_LOCAL:
            return sf_get(ip, name, namelen, flags, value, valuelen);

        case XFS_DINODE_FMT_EXTENTS:
            return extents_get(ip, name, namelen, flags, value, valuelen);

        case XFS_DINODE_FMT_BTREE:
            return btree_get(ip, name, namelen, flags, value, valuelen);

        default:
            return -EINVAL;
    }
}

auto xfs_attr_list(XfsInode* ip, XfsAttrIterFn fn, void* private_data) -> int {
    if (ip == nullptr || fn == nullptr) {
        return -EINVAL;
    }

    if (!ip->has_attr_fork) {
        return 0;
    }

    switch (ip->attr_fork.format) {
        case XFS_DINODE_FMT_LOCAL:
            return sf_list(ip, fn, private_data);

        case XFS_DINODE_FMT_EXTENTS:
            return extents_iterate(ip, fn, private_data);

        case XFS_DINODE_FMT_BTREE:
            return btree_iterate(ip, fn, private_data);

        default:
            return 0;
    }
}

auto xfs_attr_set(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, const uint8_t* value, uint32_t valuelen,
                  uint8_t flags) -> int {
    if (ip == nullptr || name == nullptr || namelen == 0) {
        return -EINVAL;
    }
    if (valuelen > 0 && value == nullptr) {
        return -EINVAL;
    }

    if (!ip->has_attr_fork || ip->attr_fork.format == XFS_DINODE_FMT_LOCAL) {
        return sf_set(ip, tp, name, namelen, value, valuelen, flags);
    }

    if (ip->attr_fork.format == XFS_DINODE_FMT_EXTENTS) {
        return extents_set(ip, tp, name, namelen, value, valuelen, flags);
    }

    log("[xfs attr] set on btree attr fork requires DA btree mutation (format=%d)\n", ip->attr_fork.format);
    return -EOPNOTSUPP;
}

auto xfs_attr_remove(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, uint8_t flags) -> int {
    if (ip == nullptr || name == nullptr || namelen == 0) {
        return -EINVAL;
    }

    if (!ip->has_attr_fork) {
        return -ENOATTR;
    }

    if (ip->attr_fork.format == XFS_DINODE_FMT_LOCAL) {
        return sf_remove(ip, tp, name, namelen, flags);
    }
    if (ip->attr_fork.format == XFS_DINODE_FMT_EXTENTS) {
        return extents_remove(ip, tp, name, namelen, flags);
    }

    return -EOPNOTSUPP;
}

}  // namespace ker::vfs::xfs
