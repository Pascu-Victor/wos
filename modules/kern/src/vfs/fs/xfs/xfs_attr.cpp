// XFS Extended Attribute subsystem - implementation.
//
// Handles shortform (inline in inode attr fork), leaf, and node attribute
// formats for get, list, set, and remove operations.
//
// Reference: reference/xfs/libxfs/xfs_attr.c, xfs_attr_sf.h, xfs_attr_leaf.c

#include "xfs_attr.hpp"

#include <algorithm>
#include <cerrno>
#include <cstddef>
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

constexpr uint8_t XFS_ATTR_ONDISK_FLAGS = XFS_ATTR_LOCAL | XFS_ATTR_NSP_ONDISK_MASK | XFS_ATTR_INCOMPLETE;

#ifdef WOS_SELFTEST
bool selftest_fragment_mappings = false;
#endif

auto attr_device_block(const XfsMountContext* mount, xfs_fsblock_t fsblock, uint64_t* out) -> int {
    if (mount == nullptr || mount->device == nullptr || out == nullptr || mount->device->block_size == 0 ||
        mount->block_size < mount->device->block_size || mount->block_size % mount->device->block_size != 0) {
        return -EIO;
    }
    size_t const RATIO = mount->block_size / mount->device->block_size;
    if (fsblock > UINT64_MAX / RATIO) {
        return -EIO;
    }
    *out = fsblock * RATIO;
    return 0;
}

auto attr_uuid_matches(const XfsUuidT& lhs, const XfsUuidT& rhs) -> bool { return __builtin_memcmp(&lhs, &rhs, sizeof(XfsUuidT)) == 0; }

auto attr_compute_hash(uint8_t flags, const uint8_t* name, uint16_t namelen, const uint8_t* value, uint32_t valuelen, xfs_dahash_t* hash)
    -> int {
    if (name == nullptr || namelen == 0 || hash == nullptr) {
        return -EINVAL;
    }
    xfs_dahash_t result = xfs_da_hashname(name, namelen);
    if ((flags & XFS_ATTR_PARENT) != 0) {
        // Current standard parent attrs store the directory-entry name as the
        // attr name and XfsParentRec as its local value. Keep accepting WOS's
        // earlier reversed representation while existing files migrate.
        if (value == nullptr) {
            return -EOPNOTSUPP;
        }
        XfsParentRec parent{};
        if (valuelen == sizeof(XfsParentRec)) {
            __builtin_memcpy(&parent, value, sizeof(parent));
        } else if (namelen == sizeof(XfsParentRec) && valuelen != 0) {
            __builtin_memcpy(&parent, name, sizeof(parent));
            result = xfs_da_hashname(value, static_cast<int>(valuelen));
        } else {
            return -EOPNOTSUPP;
        }
        uint64_t const PARENT_INO = parent.p_ino.to_cpu();
        result ^= static_cast<uint32_t>(PARENT_INO >> 32U) ^ static_cast<uint32_t>(PARENT_INO);
    }
    *hash = result;
    return 0;
}

auto attr_crc_valid(const uint8_t* block, size_t block_size, size_t crc_offset) -> bool {
    if (block == nullptr || crc_offset > block_size || sizeof(uint32_t) > block_size - crc_offset) {
        return false;
    }
    uint32_t stored{};
    __builtin_memcpy(&stored, block + crc_offset, sizeof(stored));
    return util::crc32c_block_with_cksum(block, block_size, crc_offset) == stored;
}

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

auto sf_validate(const XfsInode* ip, const XfsAttrSfHdr** out_hdr) -> int {
    if (out_hdr == nullptr) {
        return -EINVAL;
    }
    *out_hdr = nullptr;
    const auto* hdr = sf_hdr(ip);
    if (hdr == nullptr) {
        return ip != nullptr && !ip->has_attr_fork ? -ENOATTR : -EIO;
    }
    size_t const TOTAL = hdr->totsize.to_cpu();
    if (TOTAL < sizeof(XfsAttrSfHdr) || TOTAL > ip->attr_fork.local.size) {
        return -EIO;
    }

    const auto* base = reinterpret_cast<const uint8_t*>(hdr);
    size_t pos = sizeof(XfsAttrSfHdr);
    for (uint8_t i = 0; i < hdr->count; ++i) {
        if (pos > TOTAL || sizeof(XfsAttrSfEntry) > TOTAL - pos) {
            return -EIO;
        }
        const auto* entry = reinterpret_cast<const XfsAttrSfEntry*>(base + pos);
        size_t const ENTRY_SIZE = xfs_attr_sf_entry_size(entry);
        if (entry->namelen == 0 || (entry->flags & ~XFS_ATTR_ONDISK_FLAGS) != 0 || (entry->flags & XFS_ATTR_LOCAL) != 0 ||
            ENTRY_SIZE > TOTAL - pos) {
            return -EIO;
        }
        pos += ENTRY_SIZE;
    }
    if (pos != TOTAL) {
        return -EIO;
    }
    *out_hdr = hdr;
    return 0;
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
    const XfsAttrSfHdr* hdr = nullptr;
    int const VALID_RC = sf_validate(ip, &hdr);
    if (VALID_RC != 0) {
        return VALID_RC;
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
    const XfsAttrSfHdr* hdr = nullptr;
    int const VALID_RC = sf_validate(ip, &hdr);
    if (VALID_RC == -ENOATTR) {
        return 0;
    }
    if (VALID_RC != 0) {
        return VALID_RC;
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
        int const HASH_RC = attr_compute_hash(ae.flags, ae.name, ae.namelen, ae.value, ae.valuelen, &ae.hash);
        if (HASH_RC != 0) {
            return HASH_RC;
        }

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
// Returns a heap-allocated exact extent list.  Errors are never collapsed into
// an empty list because callers must distinguish absence from corrupt mapping.
auto btree_attr_list_extents(XfsInode* ip, XfsBmbtIrec** out_extents, uint32_t* out_count) -> int {
    if (ip == nullptr || out_extents == nullptr || out_count == nullptr) {
        return -EINVAL;
    }
    *out_extents = nullptr;
    *out_count = 0;
    const XfsIforkBtree& bt = ip->attr_fork.btree;
    if (bt.root == nullptr || bt.root_size < sizeof(XfsBmdrBlock)) {
        return -EIO;
    }

    const auto* bmdr = reinterpret_cast<const XfsBmdrBlock*>(bt.root);
    uint16_t const LEVEL = bmdr->bb_level.to_cpu();
    uint16_t const NUMRECS = bmdr->bb_numrecs.to_cpu();
    size_t const MAXRECS = (bt.root_size - sizeof(XfsBmdrBlock)) / (sizeof(XfsBmbtKey) + sizeof(Be64));
    if (LEVEL == 0 || LEVEL > XFS_BTREE_MAXLEVELS || NUMRECS == 0 || NUMRECS > MAXRECS || ip->anextents == 0) {
        return -EIO;
    }

    // Leftmost child pointer (first ptr in the root)
    const uint8_t* ptrs_base = bt.root + sizeof(XfsBmdrBlock) + (MAXRECS * sizeof(XfsBmbtKey));
    if (ptrs_base > bt.root + bt.root_size || sizeof(Be64) > static_cast<size_t>((bt.root + bt.root_size) - ptrs_base)) {
        return -EIO;
    }
    Be64 ptr_val{};
    __builtin_memcpy(&ptr_val, ptrs_base, sizeof(Be64));
    uint64_t const CHILD_BLOCK = ptr_val.to_cpu();
    if (CHILD_BLOCK == NULLFSBLOCK) {
        return -EIO;
    }

    XfsBtreeCursor<XfsBmbtTraits> cur;
    cur.mount = ip->mount;
    cur.owner = ip->ino;

    XfsBmbtIrec target{};
    target.br_startoff = 0;

    int const RC = xfs_btree_lookup(&cur, CHILD_BLOCK, LEVEL, target, XfsBtreeLookup::GE);
    if (RC != 0) {
        return RC;
    }

    auto* extents = new (std::nothrow) XfsBmbtIrec[static_cast<uint32_t>(ip->anextents) + 1U];
    if (extents == nullptr) {
        return -ENOMEM;
    }

    uint32_t n = 0;
    while (n <= ip->anextents) {
        extents[n++] = xfs_btree_get_rec(&cur);
        int const NEXT_RC = xfs_btree_increment(&cur);
        if (NEXT_RC == -ENOENT) {
            break;
        }
        if (NEXT_RC != 0) {
            delete[] extents;
            return NEXT_RC;
        }
    }
    if (n != ip->anextents) {
        delete[] extents;
        return -EIO;
    }
    *out_extents = extents;
    *out_count = n;
    return 0;
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

auto attr_validate_extents(const XfsInode* ip, const XfsBmbtIrec* extents, uint32_t count) -> int {
    if (ip == nullptr || ip->mount == nullptr || (count != 0 && extents == nullptr)) {
        return -EIO;
    }
    xfs_fileoff_t previous_end = 0;
    for (uint32_t i = 0; i < count; ++i) {
        const XfsBmbtIrec& rec = extents[i];
        if (rec.br_unwritten || rec.br_blockcount == 0 || rec.br_startblock == NULLFSBLOCK ||
            rec.br_startoff > UINT64_MAX - rec.br_blockcount || rec.br_startblock > UINT64_MAX - rec.br_blockcount ||
            rec.br_startblock + rec.br_blockcount > ip->mount->total_blocks || (i != 0 && rec.br_startoff < previous_end)) {
            return -EIO;
        }
        previous_end = rec.br_startoff + rec.br_blockcount;
    }
    return 0;
}

// ============================================================================
// Remote attribute value reads (Phase 2)
// ============================================================================

// Read a remote attribute value from consecutive attr-fork blocks.
// valueblk: logical attr block where the value starts
// valuelen: total value byte count
// buf/buflen: caller buffer (nullptr for size-only query)
// extents/ext_count: attr fork extent mapping
auto attr_read_remote_value(XfsInode* ip, const XfsBmbtIrec* extents, uint32_t ext_count, xfs_dablk_t valueblk, uint32_t valuelen,
                            void* buf, uint32_t buflen) -> int {
    if (ip == nullptr || ip->mount == nullptr || extents == nullptr || ext_count == 0 || valuelen == 0) {
        return -EIO;
    }
    if (buf == nullptr) {
        return static_cast<int>(valuelen);
    }
    if (buflen < valuelen) {
        return -ERANGE;
    }

    XfsMountContext* mount = ip->mount;
    const size_t BLK_SIZE = mount->block_size;
    const size_t HDR_SIZE = sizeof(XfsAttr3RmtHdr);
    if (BLK_SIZE <= HDR_SIZE) {
        return -EIO;
    }
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
        uint64_t expected_dev_block = 0;
        int const DEV_RC = attr_device_block(mount, PHYS, &expected_dev_block);
        uint32_t const EXPECTED_BYTES = static_cast<uint32_t>(std::min<size_t>(DATA_PER_BLK, valuelen - bytes_read));
        if (DEV_RC != 0 || rmt->rm_magic.to_cpu() != XFS_ATTR3_RMT_MAGIC || !attr_crc_valid(bh->data, BLK_SIZE, XFS_ATTR3_RMT_CRC_OFF) ||
            !attr_uuid_matches(rmt->rm_uuid, mount->uuid) || rmt->rm_owner.to_cpu() != ip->ino ||
            rmt->rm_blkno.to_cpu() != expected_dev_block) {
            log("[xfs attr] remote value block bad magic 0x%x\n", rmt->rm_magic.to_cpu());
            brelse(bh);
            return -EIO;
        }

        uint32_t const OFFSET = rmt->rm_offset.to_cpu();
        uint32_t const NBYTES = rmt->rm_bytes.to_cpu();

        if (OFFSET != bytes_read || NBYTES != EXPECTED_BYTES || NBYTES > DATA_PER_BLK) {
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

struct ParsedLeafEntry {
    const uint8_t* name{};
    const uint8_t* value{};
    uint16_t namelen{};
    uint32_t valuelen{};
    xfs_dablk_t valueblk{};
    size_t start{};
    size_t storage_size{};
    uint8_t flags{};
    bool local{};
};

auto leaf_entries_have_incomplete(const XfsAttrLeafEntry* entries, size_t count) -> bool {
    if (entries == nullptr) {
        return false;
    }
    for (size_t i = 0; i < count; ++i) {
        if ((entries[i].flags & XFS_ATTR_INCOMPLETE) != 0) {
            return true;
        }
    }
    return false;
}

auto attr_leaf_hash_matches(uint8_t flags, xfs_dahash_t stored_hash, const uint8_t* name, uint16_t namelen) -> bool {
    // Parent-pointer attrs use the directory name plus parent inode number,
    // not the ordinary xattr-name hash. Their validated stored hash is the
    // ordering key and must survive unrelated public xattr rebuilds.
    return (flags & XFS_ATTR_PARENT) != 0 || stored_hash == xfs_da_hashname(name, namelen);
}

auto attr_validate_da_identity(XfsInode* ip, xfs_fsblock_t fsblock, const uint8_t* block, size_t block_size, const XfsDa3Blkinfo* info)
    -> int {
    uint64_t expected_dev_block = 0;
    if (ip == nullptr || ip->mount == nullptr || block == nullptr || info == nullptr ||
        attr_device_block(ip->mount, fsblock, &expected_dev_block) != 0 || !attr_crc_valid(block, block_size, XFS_ATTR3_LEAF_CRC_OFF) ||
        !attr_uuid_matches(info->uuid, ip->mount->uuid) || info->owner.to_cpu() != ip->ino || info->blkno.to_cpu() != expected_dev_block) {
        return -EIO;
    }
    return 0;
}

auto attr_parse_leaf_entry(const uint8_t* block, size_t block_size, const XfsAttrLeafEntry& entry, ParsedLeafEntry* parsed) -> int {
    if (block == nullptr || parsed == nullptr || (entry.flags & ~XFS_ATTR_ONDISK_FLAGS) != 0) {
        return -EIO;
    }
    size_t const NAMEIDX = entry.nameidx.to_cpu();
    if (NAMEIDX >= block_size || NAMEIDX % XFS_ATTR_LEAF_NAME_ALIGN != 0) {
        return -EIO;
    }

    parsed->start = NAMEIDX;
    parsed->flags = entry.flags;
    parsed->local = (entry.flags & XFS_ATTR_LOCAL) != 0;
    if (parsed->local) {
        if (offsetof(XfsAttrLeafNameLocal, nameval) > block_size - NAMEIDX) {
            return -EIO;
        }
        const auto* local = reinterpret_cast<const XfsAttrLeafNameLocal*>(block + NAMEIDX);
        parsed->namelen = local->namelen;
        parsed->valuelen = local->valuelen.to_cpu();
        parsed->storage_size = xfs_attr_leaf_entsize_local(parsed->namelen, parsed->valuelen);
        parsed->name = xfs_attr_leaf_name_local_name(local);
        parsed->value = xfs_attr_leaf_name_local_value(local);
    } else {
        if (offsetof(XfsAttrLeafNameRemote, name) > block_size - NAMEIDX) {
            return -EIO;
        }
        const auto* remote = reinterpret_cast<const XfsAttrLeafNameRemote*>(block + NAMEIDX);
        parsed->namelen = remote->namelen;
        parsed->valuelen = remote->valuelen.to_cpu();
        parsed->valueblk = remote->valueblk.to_cpu();
        parsed->storage_size = xfs_attr_leaf_entsize_remote(parsed->namelen);
        parsed->name = xfs_attr_leaf_name_remote_name(remote);
        parsed->value = nullptr;
    }
    if (parsed->namelen == 0 || parsed->storage_size > block_size - NAMEIDX ||
        (!parsed->local && (parsed->flags & XFS_ATTR_INCOMPLETE) == 0 && (parsed->valuelen == 0 || parsed->valueblk == 0))) {
        return -EIO;
    }
    return 0;
}

auto attr_validate_leaf(XfsInode* ip, xfs_fsblock_t fsblock, const uint8_t* block) -> int {
    size_t const BLOCK_SIZE = ip->mount->block_size;
    if (BLOCK_SIZE < sizeof(XfsAttr3LeafHdr)) {
        return -EIO;
    }
    const auto* leaf = reinterpret_cast<const XfsAttr3LeafHdr*>(block);
    if (leaf->info.hdr.magic.to_cpu() != XFS_ATTR3_LEAF_MAGIC ||
        attr_validate_da_identity(ip, fsblock, block, BLOCK_SIZE, &leaf->info) != 0) {
        return -EIO;
    }
    size_t const COUNT = leaf->count.to_cpu();
    size_t const CAPACITY = (BLOCK_SIZE - sizeof(XfsAttr3LeafHdr)) / sizeof(XfsAttrLeafEntry);
    size_t const ENTRIES_END = sizeof(XfsAttr3LeafHdr) + (COUNT * sizeof(XfsAttrLeafEntry));
    size_t const FIRST_USED = leaf->firstused.to_cpu() == 0 ? BLOCK_SIZE : leaf->firstused.to_cpu();
    if (COUNT > CAPACITY || ENTRIES_END > FIRST_USED || FIRST_USED > BLOCK_SIZE) {
        return -EIO;
    }

    const auto* entries = reinterpret_cast<const XfsAttrLeafEntry*>(block + sizeof(XfsAttr3LeafHdr));
    uint32_t previous_hash = 0;
    size_t usedbytes = 0;
    size_t lowest_start = BLOCK_SIZE;
    for (size_t i = 0; i < COUNT; ++i) {
        ParsedLeafEntry parsed{};
        if (attr_parse_leaf_entry(block, BLOCK_SIZE, entries[i], &parsed) != 0 || parsed.start < FIRST_USED || parsed.start < ENTRIES_END) {
            return -EIO;
        }
        uint32_t const HASH = entries[i].hashval.to_cpu();
        if ((i != 0 && HASH < previous_hash) || !attr_leaf_hash_matches(entries[i].flags, HASH, parsed.name, parsed.namelen)) {
            return -EIO;
        }
        previous_hash = HASH;
        usedbytes += parsed.storage_size;
        lowest_start = std::min(lowest_start, parsed.start);
        for (size_t j = 0; j < i; ++j) {
            ParsedLeafEntry earlier{};
            if (attr_parse_leaf_entry(block, BLOCK_SIZE, entries[j], &earlier) != 0) {
                return -EIO;
            }
            if (parsed.start < earlier.start + earlier.storage_size && earlier.start < parsed.start + parsed.storage_size) {
                return -EIO;
            }
        }
    }
    if (usedbytes != leaf->usedbytes.to_cpu() || (COUNT == 0 ? FIRST_USED != BLOCK_SIZE : lowest_start != FIRST_USED)) {
        return -EIO;
    }
    return 0;
}

auto attr_validate_node(XfsInode* ip, xfs_fsblock_t fsblock, const uint8_t* block) -> int {
    size_t const BLOCK_SIZE = ip->mount->block_size;
    if (BLOCK_SIZE < sizeof(XfsDa3NodeHdr)) {
        return -EIO;
    }
    const auto* node = reinterpret_cast<const XfsDa3NodeHdr*>(block);
    if (node->info.hdr.magic.to_cpu() != XFS_DA3_NODE_MAGIC || node->level.to_cpu() == 0 || node->level.to_cpu() > XFS_BTREE_MAXLEVELS ||
        attr_validate_da_identity(ip, fsblock, block, BLOCK_SIZE, &node->info) != 0) {
        return -EIO;
    }
    size_t const COUNT = node->count.to_cpu();
    size_t const CAPACITY = (BLOCK_SIZE - sizeof(XfsDa3NodeHdr)) / sizeof(XfsDaNodeEntry);
    if (COUNT == 0 || COUNT > CAPACITY) {
        return -EIO;
    }
    const auto* entries = reinterpret_cast<const XfsDaNodeEntry*>(block + sizeof(XfsDa3NodeHdr));
    uint32_t previous_hash = 0;
    for (size_t i = 0; i < COUNT; ++i) {
        uint32_t const HASH = entries[i].hashval.to_cpu();
        if ((i != 0 && HASH < previous_hash) || entries[i].before.to_cpu() == 0) {
            return -EIO;
        }
        previous_hash = HASH;
    }
    return 0;
}

auto attr_validate_remote_metadata(XfsInode* ip, xfs_fsblock_t fsblock, const uint8_t* block) -> int {
    size_t const BLOCK_SIZE = ip->mount->block_size;
    if (BLOCK_SIZE <= sizeof(XfsAttr3RmtHdr)) {
        return -EIO;
    }
    const auto* remote = reinterpret_cast<const XfsAttr3RmtHdr*>(block);
    uint64_t expected_dev_block = 0;
    uint32_t const BYTES = remote->rm_bytes.to_cpu();
    uint32_t const OFFSET = remote->rm_offset.to_cpu();
    if (remote->rm_magic.to_cpu() != XFS_ATTR3_RMT_MAGIC || attr_device_block(ip->mount, fsblock, &expected_dev_block) != 0 ||
        !attr_crc_valid(block, BLOCK_SIZE, XFS_ATTR3_RMT_CRC_OFF) || !attr_uuid_matches(remote->rm_uuid, ip->mount->uuid) ||
        remote->rm_owner.to_cpu() != ip->ino || remote->rm_blkno.to_cpu() != expected_dev_block || BYTES == 0 ||
        BYTES > BLOCK_SIZE - sizeof(XfsAttr3RmtHdr) || OFFSET > UINT32_MAX - BYTES) {
        return -EIO;
    }
    return 0;
}

auto attr_validate_or_skip_nonleaf(XfsInode* ip, xfs_fsblock_t fsblock, const uint8_t* block) -> int {
    Be32 magic32{};
    __builtin_memcpy(&magic32, block, sizeof(magic32));
    if (magic32.to_cpu() == XFS_ATTR3_RMT_MAGIC) {
        return attr_validate_remote_metadata(ip, fsblock, block) == 0 ? 1 : -EIO;
    }
    const auto* info = reinterpret_cast<const XfsDa3Blkinfo*>(block);
    uint16_t const MAGIC = info->hdr.magic.to_cpu();
    if (MAGIC == XFS_DA3_NODE_MAGIC) {
        return attr_validate_node(ip, fsblock, block) == 0 ? 1 : -EIO;
    }
    if (MAGIC != XFS_ATTR3_LEAF_MAGIC) {
        return -EIO;
    }
    return attr_validate_leaf(ip, fsblock, block);
}

// Read a single leaf block and iterate its entries.
// Returns 0 immediately for valid non-leaf attr blocks.
auto leaf_block_iterate(XfsInode* ip, xfs_fsblock_t blkno, XfsAttrIterFn fn, void* priv) -> int {
    XfsMountContext* mount = ip->mount;
    BufHead* bh = xfs_buf_read(mount, blkno);
    if (bh == nullptr) {
        return -EIO;
    }

    int const VALID_RC = attr_validate_or_skip_nonleaf(ip, blkno, bh->data);
    if (VALID_RC != 0) {
        brelse(bh);
        return VALID_RC > 0 ? 0 : VALID_RC;
    }

    const auto* leaf = reinterpret_cast<const XfsAttr3LeafHdr*>(bh->data);

    uint16_t const COUNT = leaf->count.to_cpu();
    const auto* entries = reinterpret_cast<const XfsAttrLeafEntry*>(bh->data + sizeof(XfsAttr3LeafHdr));

    for (uint16_t i = 0; i < COUNT; i++) {
        uint16_t const NAMEIDX = entries[i].nameidx.to_cpu();
        uint8_t const EFLAGS = entries[i].flags;

        if ((EFLAGS & XFS_ATTR_INCOMPLETE) != 0) {
            continue;  // skip incomplete entries
        }

        ParsedLeafEntry parsed{};
        if (attr_parse_leaf_entry(bh->data, mount->block_size, entries[i], &parsed) != 0) {
            brelse(bh);
            return -EIO;
        }

        if (parsed.local) {
            // Local attribute - name and value are in the leaf block
            const auto* local = reinterpret_cast<const XfsAttrLeafNameLocal*>(bh->data + NAMEIDX);
            XfsAttrEntry ae{};
            ae.name = xfs_attr_leaf_name_local_name(local);
            ae.namelen = local->namelen;
            ae.value = xfs_attr_leaf_name_local_value(local);
            ae.valuelen = local->valuelen.to_cpu();
            ae.flags = EFLAGS;
            ae.hash = entries[i].hashval.to_cpu();

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
            ae.hash = entries[i].hashval.to_cpu();

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

    int const VALID_RC = attr_validate_or_skip_nonleaf(ip, blkno, bh->data);
    if (VALID_RC != 0) {
        brelse(bh);
        return VALID_RC > 0 ? -ENOATTR : VALID_RC;
    }

    const auto* leaf = reinterpret_cast<const XfsAttr3LeafHdr*>(bh->data);

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
                return attr_read_remote_value(ip, extents, ext_count, VBLK, VLEN, value, valuelen);
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
    int const VALID_RC = attr_validate_extents(ip, ip->attr_fork.extents.list, ip->attr_fork.extents.count);
    if (VALID_RC != 0) {
        return VALID_RC;
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
    int const VALID_RC = attr_validate_extents(ip, extents, EXT_COUNT);
    if (VALID_RC != 0) {
        return VALID_RC;
    }

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
    XfsBmbtIrec* extents = nullptr;
    int rc = btree_attr_list_extents(ip, &extents, &ext_count);
    if (rc != 0) {
        return rc;
    }
    rc = attr_validate_extents(ip, extents, ext_count);
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
    XfsBmbtIrec* extents = nullptr;
    int rc = btree_attr_list_extents(ip, &extents, &ext_count);
    if (rc != 0) {
        return rc;
    }
    rc = attr_validate_extents(ip, extents, ext_count);
    if (rc == 0) {
        rc = -ENOATTR;
    }
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
    auto* hdr = reinterpret_cast<XfsAttr3LeafHdr*>(block);
    hdr->info.lsn = Be64{};
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
        return xfs_attr_leaf_entsize_local(rec.namelen, rec.valuelen);
    }
    return xfs_attr_leaf_entsize_remote(rec.namelen);
}

auto attr_leaf_local_max(size_t block_size) -> size_t { return (block_size >> 1U) + (block_size >> 2U); }

auto attr_leaf_name_match(const AttrLeafBuildRec& rec, const uint8_t* name, uint16_t namelen, uint8_t flags) -> bool {
    if (rec.namelen != namelen) {
        return false;
    }
    if ((rec.flags & XFS_ATTR_NSP_ONDISK_MASK) != (flags & XFS_ATTR_NSP_ONDISK_MASK)) {
        return false;
    }
    return __builtin_memcmp(rec.name_ptr, name, namelen) == 0;
}

auto attr_leaf_rebuild(XfsInode* ip, XfsTransaction* tp, BufHead* bh, xfs_fsblock_t leaf_block, const uint8_t* name, uint16_t namelen,
                       const uint8_t* value, uint32_t valuelen, uint8_t flags, bool remove, bool* empty_out = nullptr,
                       bool new_local = true, xfs_dablk_t new_valueblk = 0, xfs_dablk_t* old_valueblk = nullptr,
                       uint32_t* old_valuelen = nullptr) -> int {
    if (empty_out != nullptr) {
        *empty_out = false;
    }
    if (namelen > UINT8_MAX) {
        return -EINVAL;
    }
    if (!remove && new_local && valuelen > UINT16_MAX) {
        return -E2BIG;
    }
    if (old_valueblk != nullptr) {
        *old_valueblk = 0;
    }
    if (old_valuelen != nullptr) {
        *old_valuelen = 0;
    }

    XfsMountContext* ctx = ip->mount;
    size_t const BLK_SIZE = ctx->block_size;
    if (attr_validate_leaf(ip, leaf_block, bh->data) != 0) {
        return -EIO;
    }
    const auto* leaf = reinterpret_cast<const XfsAttr3LeafHdr*>(bh->data);

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
                if (old_valueblk != nullptr) {
                    *old_valueblk = rec.valueblk;
                }
                if (old_valuelen != nullptr) {
                    *old_valuelen = rec.valuelen;
                }
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
        xfs_dahash_t new_hash = 0;
        int const HASH_RC = attr_compute_hash(flags, name, namelen, value, valuelen, &new_hash);
        if (HASH_RC != 0) {
            delete[] recs;
            return HASH_RC;
        }
        recs[n++] = AttrLeafBuildRec{.hash = new_hash,
                                     .name_ptr = name,
                                     .namelen = namelen,
                                     .val_ptr = value,
                                     .valuelen = valuelen,
                                     .valueblk = new_valueblk,
                                     .flags = static_cast<uint8_t>((flags & XFS_ATTR_NSP_ONDISK_MASK) | (new_local ? XFS_ATTR_LOCAL : 0)),
                                     .local = new_local};
    }

    if (remove && n == 0) {
        delete[] recs;
        if (empty_out != nullptr) {
            *empty_out = true;
        }
        return 0;
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
    new_leaf->pad2 = Be32{};

    auto const FREE_BASE = static_cast<uint16_t>(sizeof(XfsAttr3LeafHdr) + (n * sizeof(XfsAttrLeafEntry)));
    uint16_t const FREE_SIZE = (firstused > FREE_BASE) ? static_cast<uint16_t>(firstused - FREE_BASE) : 0;
    new_leaf->freemap[0].base = Be16::from_cpu(FREE_BASE);
    new_leaf->freemap[0].size = Be16::from_cpu(FREE_SIZE);
    for (size_t i = 1; i < XFS_ATTR_LEAF_MAPSIZE; i++) {
        new_leaf->freemap[i].base = Be16::from_cpu(0);
        new_leaf->freemap[i].size = Be16::from_cpu(0);
    }

    attr_leaf_compute_crc(new_block, BLK_SIZE);
    int const CAPTURE_RC = xfs_trans_capture_buf(tp, bh);
    if (CAPTURE_RC != 0) {
        delete[] new_block;
        delete[] recs;
        return CAPTURE_RC;
    }
    __builtin_memcpy(bh->data, new_block, BLK_SIZE);
    xfs_trans_log_buf_full(tp, bh);

    delete[] new_block;
    delete[] recs;

    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);
    return 0;
}

auto attr_store_remote_value(XfsInode* ip, XfsTransaction* tp, const uint8_t* value, uint32_t valuelen, xfs_dablk_t* valueblk) -> int;
auto attr_free_remote_value(XfsInode* ip, XfsTransaction* tp, xfs_dablk_t valueblk, uint32_t valuelen) -> int;

auto first_leaf_extent_block(XfsInode* ip, xfs_fsblock_t* out_block) -> int {
    if (ip->attr_fork.extents.list == nullptr || ip->attr_fork.extents.count == 0 || out_block == nullptr) {
        return -EIO;
    }
    int const VALID_RC = attr_validate_extents(ip, ip->attr_fork.extents.list, ip->attr_fork.extents.count);
    if (VALID_RC != 0) {
        return VALID_RC;
    }
    *out_block = attr_extents_map_logblk(ip->attr_fork.extents.list, ip->attr_fork.extents.count, 0);
    return *out_block == NULLFSBLOCK ? -EIO : 0;
}

auto extents_set(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, const uint8_t* value, uint32_t valuelen,
                 uint8_t flags) -> int {
    xfs_fsblock_t leaf_block = NULLFSBLOCK;
    int rc = first_leaf_extent_block(ip, &leaf_block);
    if (rc != 0) {
        return rc;
    }

    BufHead* bh = xfs_buf_read(ip->mount, leaf_block);
    if (bh == nullptr) {
        return -EIO;
    }

    xfs_dablk_t old_valueblk = 0;
    uint32_t old_valuelen = 0;
    if (valuelen <= UINT16_MAX && xfs_attr_leaf_entsize_local(namelen, valuelen) <= attr_leaf_local_max(ip->mount->block_size)) {
        rc = attr_leaf_rebuild(ip, tp, bh, leaf_block, name, namelen, value, valuelen, flags, false, nullptr, true, 0, &old_valueblk,
                               &old_valuelen);
    } else {
        rc = -ENOSPC;
    }
    if (rc == -ENOSPC) {
        xfs_dablk_t new_valueblk = 0;
        rc = attr_store_remote_value(ip, tp, value, valuelen, &new_valueblk);
        if (rc == 0) {
            rc = attr_leaf_rebuild(ip, tp, bh, leaf_block, name, namelen, value, valuelen, flags, false, nullptr, false, new_valueblk,
                                   &old_valueblk, &old_valuelen);
        }
    }
    brelse(bh);
    if (rc == 0 && old_valueblk != 0) {
        rc = attr_free_remote_value(ip, tp, old_valueblk, old_valuelen);
    }
    return rc;
}

[[maybe_unused]] auto extents_remove(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, uint8_t flags) -> int {
    xfs_fsblock_t leaf_block = NULLFSBLOCK;
    int rc = first_leaf_extent_block(ip, &leaf_block);
    if (rc != 0) {
        return rc;
    }

    BufHead* bh = xfs_buf_read(ip->mount, leaf_block);
    if (bh == nullptr) {
        return -EIO;
    }

    bool empty = false;
    xfs_dablk_t old_valueblk = 0;
    uint32_t old_valuelen = 0;
    rc = attr_leaf_rebuild(ip, tp, bh, leaf_block, name, namelen, nullptr, 0, flags, true, &empty, true, 0, &old_valueblk, &old_valuelen);
    brelse(bh);
    if (rc == 0 && old_valueblk != 0) {
        rc = attr_free_remote_value(ip, tp, old_valueblk, old_valuelen);
    }
    if (rc != 0 || !empty) {
        return rc;
    }
    if (ip->nblocks == 0) {
        return -EIO;
    }

    XfsMountContext* mount = ip->mount;
    xfs_agnumber_t const AGNO = xfs_ag_number(leaf_block, mount->ag_blk_log);
    xfs_agblock_t const AGBNO = xfs_ag_block(leaf_block, mount->ag_blk_log);
    rc = xfs_alloc_ensure_freelist_headroom(mount, tp, AGNO);
    if (rc == 0) {
        rc = xfs_free_extent(mount, tp, AGNO, AGBNO, 1);
    }
    uint64_t device_block = 0;
    if (rc == 0) {
        rc = attr_device_block(mount, leaf_block, &device_block);
    }
    if (rc == 0) {
        rc = xfs_trans_retire_bdev_range(tp, device_block, mount->block_size / mount->device->block_size);
    }
    if (rc != 0) {
        return rc;
    }

    XfsBmbtIrec* old_list = ip->attr_fork.extents.list;
    bool const OLD_LIST_INLINE = xfs_ifork_extents_uses_inline(ip->attr_fork.extents);
    ip->attr_fork.format = XFS_DINODE_FMT_LOCAL;
    ip->attr_fork.local.data = nullptr;
    ip->attr_fork.local.size = 0;
    if (!OLD_LIST_INLINE) {
        delete[] old_list;
    }
    ip->has_attr_fork = false;
    ip->forkoff = 0;
    ip->anextents = 0;
    ip->nblocks--;
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);
    return rc;
}

auto attr_forkoff_for_space(const XfsInode* ip, size_t attr_bytes, uint8_t* forkoff) -> int {
    if (ip == nullptr || ip->mount == nullptr || forkoff == nullptr || ip->mount->inode_size <= sizeof(XfsDinode)) {
        return -EIO;
    }
    size_t data_bytes = 0;
    switch (ip->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL:
            data_bytes = ip->data_fork.local.size;
            break;
        case XFS_DINODE_FMT_EXTENTS:
            data_bytes = static_cast<size_t>(ip->data_fork.extents.count) * sizeof(XfsBmbtRec);
            break;
        case XFS_DINODE_FMT_BTREE:
            data_bytes = ip->data_fork.btree.root_size;
            break;
        default:
            return -EOPNOTSUPP;
    }
    size_t const LITERAL_BYTES = ip->mount->inode_size - sizeof(XfsDinode);
    data_bytes = std::max<size_t>(8, (data_bytes + 7) & ~size_t{7});
    if (data_bytes > LITERAL_BYTES || attr_bytes > LITERAL_BYTES - data_bytes || data_bytes / 8 > UINT8_MAX) {
        return -ENOSPC;
    }
    *forkoff = static_cast<uint8_t>(data_bytes / 8);
    return 0;
}

auto attr_fork_space(const XfsInode* ip) -> size_t {
    if (ip == nullptr || ip->mount == nullptr || ip->mount->inode_size <= sizeof(XfsDinode)) {
        return 0;
    }
    size_t const DATA_BYTES = static_cast<size_t>(ip->forkoff) << 3U;
    size_t const LITERAL_BYTES = ip->mount->inode_size - sizeof(XfsDinode);
    return DATA_BYTES <= LITERAL_BYTES ? LITERAL_BYTES - DATA_BYTES : 0;
}

auto attr_install_extent_list(XfsInode* ip, XfsTransaction* tp, const XfsBmbtIrec* records, uint32_t count) -> int {
    if (ip == nullptr || tp == nullptr || (count != 0 && records == nullptr) || count > UINT16_MAX ||
        static_cast<size_t>(count) * sizeof(XfsBmbtRec) > attr_fork_space(ip)) {
        return -ENOSPC;
    }
    XfsBmbtIrec* replacement = nullptr;
    uint32_t capacity = XFS_IFORK_INLINE_EXTENT_CAPACITY;
    if (count > XFS_IFORK_INLINE_EXTENT_CAPACITY) {
        replacement = new (std::nothrow) XfsBmbtIrec[count];
        if (replacement == nullptr) {
            return -ENOMEM;
        }
        capacity = count;
        for (uint32_t i = 0; i < count; ++i) {
            replacement[i] = records[i];
        }
    }

    XfsBmbtIrec* old_list = ip->attr_fork.extents.list;
    bool const OLD_INLINE = xfs_ifork_extents_uses_inline(ip->attr_fork.extents);
    if (replacement == nullptr) {
        replacement = xfs_ifork_extents_inline_data(ip->attr_fork.extents);
        for (uint32_t i = 0; i < count; ++i) {
            replacement[i] = records[i];
        }
    }
    if (!OLD_INLINE) {
        delete[] old_list;
    }
    ip->attr_fork.extents.list = replacement;
    ip->attr_fork.extents.count = count;
    ip->attr_fork.extents.capacity = capacity;
    ip->anextents = static_cast<uint16_t>(count);
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);
    return 0;
}

auto attr_append_mapping(XfsBmbtIrec* records, uint32_t* count, uint32_t capacity, xfs_fileoff_t logical, xfs_fsblock_t physical,
                         xfs_extlen_t blocks) -> int {
    if (records == nullptr || count == nullptr || blocks == 0) {
        return -EINVAL;
    }
    if (*count >= capacity) {
        return -EFBIG;
    }
    records[(*count)++] = {.br_startoff = logical, .br_startblock = physical, .br_blockcount = blocks, .br_unwritten = false};
    return 0;
}

void attr_remote_compute_crc(uint8_t* block, size_t block_size) {
    __builtin_memset(block + XFS_ATTR3_RMT_CRC_OFF, 0, sizeof(uint32_t));
    uint32_t const CRC = util::crc32c_block_with_cksum(block, block_size, XFS_ATTR3_RMT_CRC_OFF);
    __builtin_memcpy(block + XFS_ATTR3_RMT_CRC_OFF, &CRC, sizeof(CRC));
}

auto attr_store_remote_value(XfsInode* ip, XfsTransaction* tp, const uint8_t* value, uint32_t valuelen, xfs_dablk_t* valueblk) -> int {
    if (ip == nullptr || ip->mount == nullptr || tp == nullptr || value == nullptr || valuelen == 0 || valueblk == nullptr ||
        ip->attr_fork.format != XFS_DINODE_FMT_EXTENTS || ip->mount->block_size <= sizeof(XfsAttr3RmtHdr)) {
        return -EINVAL;
    }
    size_t const DATA_PER_BLOCK = ip->mount->block_size - sizeof(XfsAttr3RmtHdr);
    uint64_t const BLOCKS64 = (static_cast<uint64_t>(valuelen) + DATA_PER_BLOCK - 1) / DATA_PER_BLOCK;
    if (BLOCKS64 == 0 || BLOCKS64 > UINT32_MAX || ip->nblocks > UINT64_MAX - BLOCKS64) {
        return -E2BIG;
    }
    auto const BLOCKS = static_cast<uint32_t>(BLOCKS64);
    uint32_t const OLD_COUNT = ip->attr_fork.extents.count;
    if (OLD_COUNT > UINT32_MAX - BLOCKS - 1) {
        return -EFBIG;
    }
    uint32_t const CAPACITY = OLD_COUNT + BLOCKS + 1;
    auto* mappings = new (std::nothrow) XfsBmbtIrec[CAPACITY];
    if (mappings == nullptr) {
        return -ENOMEM;
    }
    uint32_t mapping_count = 0;
    xfs_fileoff_t logical = 1;
    for (uint32_t i = 0; i < OLD_COUNT; ++i) {
        mappings[mapping_count++] = ip->attr_fork.extents.list[i];
        if (mappings[i].br_startoff > UINT32_MAX - mappings[i].br_blockcount) {
            delete[] mappings;
            return -EFBIG;
        }
        logical = std::max(logical, mappings[i].br_startoff + mappings[i].br_blockcount);
    }
    if (logical == 0 || logical > UINT32_MAX || BLOCKS > UINT32_MAX - logical) {
        delete[] mappings;
        return -EFBIG;
    }
    *valueblk = static_cast<xfs_dablk_t>(logical);

    uint32_t allocated_blocks = 0;
    uint32_t copied = 0;
    while (allocated_blocks < BLOCKS) {
        uint32_t const WANT = BLOCKS - allocated_blocks;
        XfsAllocResult allocation{};
        int rc = -ENOSPC;
        xfs_agnumber_t const PREFERRED = xfs_ino_ag(ip->ino, ip->mount->agino_log);
        for (xfs_agnumber_t attempt = 0; attempt < ip->mount->ag_count && rc == -ENOSPC; ++attempt) {
            XfsAllocReq request{};
            request.agno = (PREFERRED + attempt) % ip->mount->ag_count;
            request.minlen = 1;
            request.maxlen = WANT;
            rc = xfs_alloc_extent(ip->mount, tp, request, &allocation);
        }
        if (rc != 0 || allocation.len == 0 || allocation.len > WANT) {
            delete[] mappings;
            return rc != 0 ? rc : -EIO;
        }
        xfs_fsblock_t const PHYSICAL = xfs_agbno_to_fsbno(allocation.agno, allocation.agbno, ip->mount->ag_blk_log);
        rc = attr_append_mapping(mappings, &mapping_count, CAPACITY, logical + allocated_blocks, PHYSICAL, allocation.len);
        if (rc != 0) {
            delete[] mappings;
            return rc;
        }

        for (xfs_extlen_t i = 0; i < allocation.len; ++i) {
            BufHead* bh = xfs_buf_get(ip->mount, PHYSICAL + i);
            if (bh == nullptr) {
                delete[] mappings;
                return -EIO;
            }
            rc = xfs_trans_capture_buf(tp, bh);
            if (rc != 0) {
                brelse(bh);
                delete[] mappings;
                return rc;
            }
            __builtin_memset(bh->data, 0, ip->mount->block_size);
            auto* header = reinterpret_cast<XfsAttr3RmtHdr*>(bh->data);
            uint32_t const BYTES = static_cast<uint32_t>(std::min<size_t>(DATA_PER_BLOCK, valuelen - copied));
            uint64_t device_block = 0;
            if (attr_device_block(ip->mount, PHYSICAL + i, &device_block) != 0) {
                brelse(bh);
                delete[] mappings;
                return -EIO;
            }
            header->rm_magic = Be32::from_cpu(XFS_ATTR3_RMT_MAGIC);
            header->rm_offset = Be32::from_cpu(copied);
            header->rm_bytes = Be32::from_cpu(BYTES);
            __builtin_memcpy(&header->rm_uuid, &ip->mount->uuid, sizeof(XfsUuidT));
            header->rm_owner = Be64::from_cpu(ip->ino);
            header->rm_blkno = Be64::from_cpu(device_block);
            header->rm_lsn = Be64{};
            __builtin_memcpy(bh->data + sizeof(XfsAttr3RmtHdr), value + copied, BYTES);
            attr_remote_compute_crc(bh->data, ip->mount->block_size);
            xfs_trans_log_buf_full(tp, bh);
            brelse(bh);
            copied += BYTES;
        }
        allocated_blocks += allocation.len;
    }
    if (copied != valuelen) {
        delete[] mappings;
        return -EIO;
    }
    int const INSTALL_RC = attr_install_extent_list(ip, tp, mappings, mapping_count);
    delete[] mappings;
    if (INSTALL_RC != 0) {
        return INSTALL_RC;
    }
    ip->nblocks += BLOCKS;
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);
    return 0;
}

auto attr_free_remote_value(XfsInode* ip, XfsTransaction* tp, xfs_dablk_t valueblk, uint32_t valuelen) -> int {
    if (ip == nullptr || ip->mount == nullptr || tp == nullptr || valueblk == 0 || valuelen == 0 ||
        ip->mount->block_size <= sizeof(XfsAttr3RmtHdr)) {
        return -EIO;
    }
    size_t const DATA_PER_BLOCK = ip->mount->block_size - sizeof(XfsAttr3RmtHdr);
    uint64_t const BLOCKS = (static_cast<uint64_t>(valuelen) + DATA_PER_BLOCK - 1) / DATA_PER_BLOCK;
    uint64_t const END = static_cast<uint64_t>(valueblk) + BLOCKS;
    if (END > UINT32_MAX + uint64_t{1} || BLOCKS > ip->nblocks) {
        return -EIO;
    }
    uint32_t const OLD_COUNT = ip->attr_fork.extents.count;
    auto* retained = new (std::nothrow) XfsBmbtIrec[OLD_COUNT + 1];
    if (retained == nullptr) {
        return -ENOMEM;
    }
    uint32_t retained_count = 0;
    uint64_t freed = 0;
    for (uint32_t i = 0; i < OLD_COUNT; ++i) {
        XfsBmbtIrec const& rec = ip->attr_fork.extents.list[i];
        uint64_t const REC_END = rec.br_startoff + rec.br_blockcount;
        uint64_t const CUT_START = std::max<uint64_t>(rec.br_startoff, valueblk);
        uint64_t const CUT_END = std::min<uint64_t>(REC_END, END);
        if (CUT_START >= CUT_END) {
            retained[retained_count++] = rec;
            continue;
        }
        uint64_t const PREFIX = CUT_START - rec.br_startoff;
        uint64_t const CUT_BLOCKS = CUT_END - CUT_START;
        if (PREFIX != 0) {
            retained[retained_count++] = {
                .br_startoff = rec.br_startoff, .br_startblock = rec.br_startblock, .br_blockcount = PREFIX, .br_unwritten = false};
        }
        xfs_fsblock_t const PHYSICAL = rec.br_startblock + PREFIX;
        xfs_agnumber_t const AGNO = xfs_ag_number(PHYSICAL, ip->mount->ag_blk_log);
        xfs_agblock_t const AGBNO = xfs_ag_block(PHYSICAL, ip->mount->ag_blk_log);
        int rc = xfs_alloc_ensure_freelist_headroom(ip->mount, tp, AGNO);
        if (rc == 0) {
            rc = xfs_free_extent(ip->mount, tp, AGNO, AGBNO, static_cast<xfs_extlen_t>(CUT_BLOCKS));
        }
        uint64_t device_block = 0;
        if (rc == 0) {
            rc = attr_device_block(ip->mount, PHYSICAL, &device_block);
        }
        if (rc == 0) {
            rc = xfs_trans_retire_bdev_range(tp, device_block, CUT_BLOCKS * (ip->mount->block_size / ip->mount->device->block_size));
        }
        if (rc != 0) {
            delete[] retained;
            return rc;
        }
        freed += CUT_BLOCKS;
        if (CUT_END < REC_END) {
            retained[retained_count++] = {.br_startoff = CUT_END,
                                          .br_startblock = rec.br_startblock + (CUT_END - rec.br_startoff),
                                          .br_blockcount = REC_END - CUT_END,
                                          .br_unwritten = false};
        }
    }
    if (freed != BLOCKS) {
        delete[] retained;
        return -EIO;
    }
    int const INSTALL_RC = attr_install_extent_list(ip, tp, retained, retained_count);
    delete[] retained;
    if (INSTALL_RC != 0) {
        return INSTALL_RC;
    }
    ip->nblocks -= freed;
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);
    return 0;
}

struct CowAttrRec {
    uint8_t* name{};
    uint8_t* value{};
    uint16_t namelen{};
    uint32_t valuelen{};
    uint8_t flags{};
    xfs_dahash_t hash{};
    xfs_dablk_t valueblk{};
    bool local{};
};

void cow_free_attrs(CowAttrRec* attrs, uint32_t count) {
    if (attrs == nullptr) {
        return;
    }
    for (uint32_t i = 0; i < count; ++i) {
        delete[] attrs[i].name;
        delete[] attrs[i].value;
    }
    delete[] attrs;
}

struct CowCountContext {
    uint32_t count{};
};

auto cow_count_attr(const XfsAttrEntry* /*entry*/, void* private_data) -> int {
    auto* context = static_cast<CowCountContext*>(private_data);
    if (context->count == UINT32_MAX) {
        return -EFBIG;
    }
    context->count++;
    return 0;
}

struct CowCollectContext {
    XfsInode* ip{};
    CowAttrRec* attrs{};
    uint32_t capacity{};
    uint32_t count{};
};

auto attr_fork_has_incomplete(XfsInode* ip) -> int;

auto cow_collect_attr(const XfsAttrEntry* entry, void* private_data) -> int {
    auto* context = static_cast<CowCollectContext*>(private_data);
    if (entry == nullptr || context == nullptr || context->count >= context->capacity || entry->namelen == 0 ||
        entry->namelen > UINT8_MAX || entry->valuelen > 65536) {
        return -EIO;
    }
    CowAttrRec& target = context->attrs[context->count];
    target.name = new (std::nothrow) uint8_t[entry->namelen];
    target.value = entry->valuelen == 0 ? nullptr : new (std::nothrow) uint8_t[entry->valuelen];
    if (target.name == nullptr || (entry->valuelen != 0 && target.value == nullptr)) {
        return -ENOMEM;
    }
    __builtin_memcpy(target.name, entry->name, entry->namelen);
    if (entry->valuelen != 0) {
        if (entry->value != nullptr) {
            __builtin_memcpy(target.value, entry->value, entry->valuelen);
        } else {
            int const GET_RC = xfs_attr_get(context->ip, entry->name, entry->namelen, entry->flags, target.value, entry->valuelen);
            if (GET_RC < 0 || GET_RC != static_cast<int>(entry->valuelen)) {
                return GET_RC < 0 ? GET_RC : -EIO;
            }
        }
    }
    target.namelen = entry->namelen;
    target.valuelen = entry->valuelen;
    target.flags = entry->flags & XFS_ATTR_NSP_ONDISK_MASK;
    target.hash = entry->hash;
    context->count++;
    return 0;
}

auto cow_copy_attr(const CowAttrRec& source, CowAttrRec* target) -> int {
    target->name = new (std::nothrow) uint8_t[source.namelen];
    target->value = source.valuelen == 0 ? nullptr : new (std::nothrow) uint8_t[source.valuelen];
    if (target->name == nullptr || (source.valuelen != 0 && target->value == nullptr)) {
        return -ENOMEM;
    }
    __builtin_memcpy(target->name, source.name, source.namelen);
    if (source.valuelen != 0) {
        __builtin_memcpy(target->value, source.value, source.valuelen);
    }
    target->namelen = source.namelen;
    target->valuelen = source.valuelen;
    target->flags = source.flags;
    target->hash = source.hash;
    return 0;
}

auto cow_collect_mutation(XfsInode* ip, const uint8_t* name, uint16_t namelen, const uint8_t* value, uint32_t valuelen, uint8_t flags,
                          bool remove, CowAttrRec** out_attrs, uint32_t* out_count) -> int {
    int const INCOMPLETE = attr_fork_has_incomplete(ip);
    if (INCOMPLETE != 0) {
        return INCOMPLETE < 0 ? INCOMPLETE : -EAGAIN;
    }
    CowCountContext count_context{};
    int rc = xfs_attr_list(ip, cow_count_attr, &count_context);
    if (rc != 0) {
        return rc;
    }
    CowAttrRec* old_attrs = count_context.count == 0 ? nullptr : new (std::nothrow) CowAttrRec[count_context.count];
    if (count_context.count != 0 && old_attrs == nullptr) {
        return -ENOMEM;
    }
    CowCollectContext collect{.ip = ip, .attrs = old_attrs, .capacity = count_context.count};
    rc = xfs_attr_list(ip, cow_collect_attr, &collect);
    if (rc != 0 || collect.count != count_context.count) {
        cow_free_attrs(old_attrs, count_context.count);
        return rc != 0 ? rc : -EIO;
    }

    int32_t match = -1;
    for (uint32_t i = 0; i < count_context.count; ++i) {
        if (old_attrs[i].namelen == namelen && old_attrs[i].flags == (flags & XFS_ATTR_NSP_ONDISK_MASK) &&
            __builtin_memcmp(old_attrs[i].name, name, namelen) == 0) {
            match = static_cast<int32_t>(i);
            break;
        }
    }
    if (remove && match < 0) {
        cow_free_attrs(old_attrs, count_context.count);
        return -ENOATTR;
    }
    uint32_t new_count = count_context.count;
    if (remove) {
        new_count--;
    } else if (match < 0) {
        new_count++;
    }
    uint32_t const NEW_COUNT = new_count;
    CowAttrRec* attrs = NEW_COUNT == 0 ? nullptr : new (std::nothrow) CowAttrRec[NEW_COUNT];
    if (NEW_COUNT != 0 && attrs == nullptr) {
        cow_free_attrs(old_attrs, count_context.count);
        return -ENOMEM;
    }
    uint32_t out = 0;
    for (uint32_t i = 0; i < count_context.count; ++i) {
        if (match >= 0 && i == static_cast<uint32_t>(match)) {
            continue;
        }
        rc = cow_copy_attr(old_attrs[i], &attrs[out]);
        if (rc != 0) {
            cow_free_attrs(attrs, NEW_COUNT);
            cow_free_attrs(old_attrs, count_context.count);
            return rc;
        }
        out++;
    }
    if (!remove) {
        CowAttrRec& target = attrs[out++];
        target.name = new (std::nothrow) uint8_t[namelen];
        target.value = valuelen == 0 ? nullptr : new (std::nothrow) uint8_t[valuelen];
        if (target.name == nullptr || (valuelen != 0 && target.value == nullptr)) {
            cow_free_attrs(attrs, NEW_COUNT);
            cow_free_attrs(old_attrs, count_context.count);
            return -ENOMEM;
        }
        __builtin_memcpy(target.name, name, namelen);
        if (valuelen != 0) {
            __builtin_memcpy(target.value, value, valuelen);
        }
        target.namelen = namelen;
        target.valuelen = valuelen;
        target.flags = flags & XFS_ATTR_NSP_ONDISK_MASK;
        rc = attr_compute_hash(flags, name, namelen, value, valuelen, &target.hash);
        if (rc != 0) {
            cow_free_attrs(attrs, NEW_COUNT);
            cow_free_attrs(old_attrs, count_context.count);
            return rc;
        }
    }
    cow_free_attrs(old_attrs, count_context.count);
    for (uint32_t i = 1; i < NEW_COUNT; ++i) {
        CowAttrRec temporary = attrs[i];
        uint32_t j = i;
        while (j != 0 &&
               (attrs[j - 1].hash > temporary.hash || (attrs[j - 1].hash == temporary.hash && attrs[j - 1].flags > temporary.flags))) {
            attrs[j] = attrs[j - 1];
            --j;
        }
        attrs[j] = temporary;
    }
    *out_attrs = attrs;
    *out_count = NEW_COUNT;
    return 0;
}

void cow_free_ifork_memory(XfsIfork* fork, bool extents_were_inline = false) {
    if (fork == nullptr) {
        return;
    }
    if (fork->format == XFS_DINODE_FMT_LOCAL) {
        delete[] fork->local.data;
    } else if (fork->format == XFS_DINODE_FMT_EXTENTS && !extents_were_inline) {
        delete[] fork->extents.list;
    } else if (fork->format == XFS_DINODE_FMT_BTREE) {
        delete[] fork->btree.root;
    }
}

auto cow_old_mappings(XfsInode* ip, XfsBmbtIrec** mappings, uint32_t* count) -> int {
    *mappings = nullptr;
    *count = 0;
    if (!ip->has_attr_fork || ip->attr_fork.format == XFS_DINODE_FMT_LOCAL) {
        return 0;
    }
    if (ip->attr_fork.format == XFS_DINODE_FMT_BTREE) {
        return btree_attr_list_extents(ip, mappings, count);
    }
    if (ip->attr_fork.format != XFS_DINODE_FMT_EXTENTS || (ip->attr_fork.extents.count != 0 && ip->attr_fork.extents.list == nullptr)) {
        return -EIO;
    }
    *count = ip->attr_fork.extents.count;
    if (*count == 0) {
        return 0;
    }
    *mappings = new (std::nothrow) XfsBmbtIrec[*count];
    if (*mappings == nullptr) {
        return -ENOMEM;
    }
    for (uint32_t i = 0; i < *count; ++i) {
        (*mappings)[i] = ip->attr_fork.extents.list[i];
    }
    return 0;
}

auto attr_fork_has_incomplete(XfsInode* ip) -> int {
    if (ip == nullptr || !ip->has_attr_fork) {
        return 0;
    }
    if (ip->attr_fork.format == XFS_DINODE_FMT_LOCAL) {
        const XfsAttrSfHdr* header = nullptr;
        int const RC = sf_validate(ip, &header);
        if (RC != 0) {
            return RC == -ENOATTR ? 0 : RC;
        }
        const auto* base = reinterpret_cast<const uint8_t*>(header);
        size_t offset = sizeof(XfsAttrSfHdr);
        for (uint8_t i = 0; i < header->count; ++i) {
            const auto* entry = reinterpret_cast<const XfsAttrSfEntry*>(base + offset);
            if ((entry->flags & XFS_ATTR_INCOMPLETE) != 0) {
                return 1;
            }
            offset += xfs_attr_sf_entry_size(entry);
        }
        return 0;
    }

    XfsBmbtIrec* mappings = nullptr;
    uint32_t mapping_count = 0;
    int rc = cow_old_mappings(ip, &mappings, &mapping_count);
    for (uint32_t i = 0; i < mapping_count && rc == 0; ++i) {
        for (xfs_extlen_t block = 0; block < mappings[i].br_blockcount && rc == 0; ++block) {
            xfs_fsblock_t const PHYSICAL = mappings[i].br_startblock + block;
            BufHead* bh = xfs_buf_read(ip->mount, PHYSICAL);
            if (bh == nullptr) {
                rc = -EIO;
                break;
            }
            int const VALID = attr_validate_or_skip_nonleaf(ip, PHYSICAL, bh->data);
            if (VALID < 0) {
                rc = VALID;
            } else if (VALID == 0) {
                const auto* header = reinterpret_cast<const XfsAttr3LeafHdr*>(bh->data);
                const auto* entries = reinterpret_cast<const XfsAttrLeafEntry*>(bh->data + sizeof(XfsAttr3LeafHdr));
                if (leaf_entries_have_incomplete(entries, header->count.to_cpu())) {
                    rc = 1;
                }
            }
            brelse(bh);
        }
    }
    delete[] mappings;
    return rc;
}

auto cow_free_physical_range(XfsInode* ip, XfsTransaction* tp, xfs_fsblock_t start, xfs_filblks_t blocks) -> int {
    while (blocks != 0) {
        xfs_agnumber_t const AGNO = xfs_ag_number(start, ip->mount->ag_blk_log);
        xfs_agblock_t const AGBNO = xfs_ag_block(start, ip->mount->ag_blk_log);
        if (AGNO >= ip->mount->ag_count || AGBNO >= ip->mount->ag_blocks) {
            return -EIO;
        }
        xfs_filblks_t const SPAN = std::min<xfs_filblks_t>(blocks, ip->mount->ag_blocks - AGBNO);
        int rc = xfs_alloc_ensure_freelist_headroom(ip->mount, tp, AGNO);
        if (rc == 0) {
            rc = xfs_free_extent(ip->mount, tp, AGNO, AGBNO, static_cast<xfs_extlen_t>(SPAN));
            if (rc != 0) {
                log("[xfs attr] old-fork extent free failed ag=%u agbno=%u len=%lu rc=%d\n", AGNO, AGBNO, static_cast<unsigned long>(SPAN),
                    rc);
            }
        } else {
            log("[xfs attr] old-fork freelist headroom failed ag=%u rc=%d\n", AGNO, rc);
        }
        uint64_t device_block = 0;
        if (rc == 0) {
            rc = attr_device_block(ip->mount, start, &device_block);
        }
        if (rc == 0) {
            rc = xfs_trans_retire_bdev_range(tp, device_block, SPAN * (ip->mount->block_size / ip->mount->device->block_size));
            if (rc != 0) {
                log("[xfs attr] old-fork cache retirement failed block=%lu count=%lu rc=%d\n", static_cast<unsigned long>(device_block),
                    static_cast<unsigned long>(SPAN * (ip->mount->block_size / ip->mount->device->block_size)), rc);
            }
        }
        if (rc != 0) {
            return rc;
        }
        start += SPAN;
        blocks -= SPAN;
    }
    return 0;
}

auto cow_retire_old_fork(XfsInode* ip, XfsTransaction* tp, const XfsIfork& old_fork, const XfsBmbtIrec* mappings, uint32_t mapping_count,
                         uint64_t* freed_blocks) -> int {
    *freed_blocks = 0;
    for (uint32_t i = 0; i < mapping_count; ++i) {
        int const RC = cow_free_physical_range(ip, tp, mappings[i].br_startblock, mappings[i].br_blockcount);
        if (RC != 0) {
            log("[xfs attr] old-fork mapping retirement failed index=%u start=%lu len=%lu rc=%d\n", i,
                static_cast<unsigned long>(mappings[i].br_startblock), static_cast<unsigned long>(mappings[i].br_blockcount), RC);
            return RC;
        }
        *freed_blocks += mappings[i].br_blockcount;
    }
    if (old_fork.format == XFS_DINODE_FMT_BTREE) {
        uint32_t metadata = 0;
        int const RC = xfs_bmap_free_fork_btree(ip, tp, old_fork.btree, &metadata);
        if (RC != 0) {
            log("[xfs attr] old-fork BMBT retirement failed root_size=%lu rc=%d\n", static_cast<unsigned long>(old_fork.btree.root_size),
                RC);
            return RC;
        }
        *freed_blocks += metadata;
    }
    return 0;
}

auto cow_build_shortform(XfsInode* ip, CowAttrRec* attrs, uint32_t count, uint8_t** data, size_t* size, uint8_t* forkoff) -> int {
    if (count > UINT8_MAX) {
        return -ENOSPC;
    }
    size_t total = sizeof(XfsAttrSfHdr);
    for (uint32_t i = 0; i < count; ++i) {
        if (attrs[i].namelen > UINT8_MAX || attrs[i].valuelen > UINT8_MAX ||
            total > UINT16_MAX - sizeof(XfsAttrSfEntry) - attrs[i].namelen - attrs[i].valuelen) {
            return -ENOSPC;
        }
        total += sizeof(XfsAttrSfEntry) + attrs[i].namelen + attrs[i].valuelen;
    }
    int const SPACE_RC = attr_forkoff_for_space(ip, total, forkoff);
    if (SPACE_RC != 0) {
        return -ENOSPC;
    }
    auto* buffer = new (std::nothrow) uint8_t[total];
    if (buffer == nullptr) {
        return -ENOMEM;
    }
    __builtin_memset(buffer, 0, total);
    auto* header = reinterpret_cast<XfsAttrSfHdr*>(buffer);
    header->totsize = Be16::from_cpu(static_cast<uint16_t>(total));
    header->count = static_cast<uint8_t>(count);
    size_t offset = sizeof(XfsAttrSfHdr);
    for (uint32_t i = 0; i < count; ++i) {
        auto* entry = reinterpret_cast<XfsAttrSfEntry*>(buffer + offset);
        entry->namelen = static_cast<uint8_t>(attrs[i].namelen);
        entry->valuelen = static_cast<uint8_t>(attrs[i].valuelen);
        entry->flags = attrs[i].flags;
        __builtin_memcpy(xfs_attr_sf_entry_name(entry), attrs[i].name, attrs[i].namelen);
        if (attrs[i].valuelen != 0) {
            __builtin_memcpy(xfs_attr_sf_entry_value(entry), attrs[i].value, attrs[i].valuelen);
        }
        offset += xfs_attr_sf_entry_size(entry);
    }
    *data = buffer;
    *size = total;
    return 0;
}

struct CowDaDesc {
    bool leaf{};
    uint16_t level{};
    uint32_t item_start{};
    uint32_t item_count{};
    uint32_t max_hash{};
    xfs_dablk_t logical{};
};

void cow_node_siblings(const CowDaDesc* descs, uint32_t count, uint32_t index, xfs_dablk_t* previous, xfs_dablk_t* next) {
    *previous = 0;
    *next = 0;
    if (descs == nullptr || index >= count || descs[index].leaf) {
        return;
    }
    if (index != 0 && !descs[index - 1].leaf && descs[index - 1].level == descs[index].level) {
        *previous = descs[index - 1].logical;
    }
    if (index + 1 < count && !descs[index + 1].leaf && descs[index + 1].level == descs[index].level) {
        *next = descs[index + 1].logical;
    }
}

auto cow_allocate_blocks(XfsInode* ip, XfsTransaction* tp, xfs_dablk_t logical_start, uint32_t block_count, xfs_fsblock_t** physical_out,
                         XfsBmbtIrec** mappings_out, uint32_t* mapping_count_out) -> int {
    auto* physical = new (std::nothrow) xfs_fsblock_t[block_count];
    auto* mappings = new (std::nothrow) XfsBmbtIrec[block_count];
    if (physical == nullptr || mappings == nullptr) {
        delete[] physical;
        delete[] mappings;
        return -ENOMEM;
    }
    uint32_t allocated = 0;
    uint32_t mapping_count = 0;
    while (allocated < block_count) {
        uint32_t const WANT = block_count - allocated;
        XfsAllocResult allocation{};
        int rc = -ENOSPC;
        xfs_agnumber_t const PREFERRED = xfs_ino_ag(ip->ino, ip->mount->agino_log);
        for (xfs_agnumber_t attempt = 0; attempt < ip->mount->ag_count && rc == -ENOSPC; ++attempt) {
            XfsAllocReq request{};
            request.agno = (PREFERRED + attempt) % ip->mount->ag_count;
            request.minlen = 1;
            request.maxlen = WANT;
            rc = xfs_alloc_extent(ip->mount, tp, request, &allocation);
        }
        if (rc != 0 || allocation.len == 0 || allocation.len > WANT) {
            delete[] physical;
            delete[] mappings;
            return rc != 0 ? rc : -EIO;
        }
        xfs_fsblock_t const START = xfs_agbno_to_fsbno(allocation.agno, allocation.agbno, ip->mount->ag_blk_log);
        bool fragment_mapping = false;
#ifdef WOS_SELFTEST
        fragment_mapping = selftest_fragment_mappings;
#endif
        if (fragment_mapping) {
            for (xfs_extlen_t i = 0; i < allocation.len; ++i) {
                mappings[mapping_count++] = {.br_startoff = static_cast<xfs_fileoff_t>(logical_start) + allocated + i,
                                             .br_startblock = START + i,
                                             .br_blockcount = 1,
                                             .br_unwritten = false};
            }
        } else {
            mappings[mapping_count++] = {.br_startoff = static_cast<xfs_fileoff_t>(logical_start) + allocated,
                                         .br_startblock = START,
                                         .br_blockcount = allocation.len,
                                         .br_unwritten = false};
        }
        for (xfs_extlen_t i = 0; i < allocation.len; ++i) {
            physical[allocated + i] = START + i;
        }
        allocated += allocation.len;
    }
    *physical_out = physical;
    *mappings_out = mappings;
    *mapping_count_out = mapping_count;
    return 0;
}

auto cow_init_da_info(XfsInode* ip, xfs_fsblock_t physical, XfsDa3Blkinfo* info, uint16_t magic) -> int {
    uint64_t device_block = 0;
    int const RC = attr_device_block(ip->mount, physical, &device_block);
    if (RC != 0) {
        return RC;
    }
    info->hdr.forw = Be32{};
    info->hdr.back = Be32{};
    info->hdr.magic = Be16::from_cpu(magic);
    info->hdr.pad = Be16{};
    info->crc = Be32{};
    info->blkno = Be64::from_cpu(device_block);
    info->lsn = Be64{};
    __builtin_memcpy(&info->uuid, &ip->mount->uuid, sizeof(XfsUuidT));
    info->owner = Be64::from_cpu(ip->ino);
    return 0;
}

auto cow_write_leaf(XfsInode* ip, XfsTransaction* tp, CowAttrRec* attrs, const CowDaDesc& desc, xfs_fsblock_t physical,
                    xfs_dablk_t previous, xfs_dablk_t next) -> int {
    BufHead* bh = xfs_buf_get(ip->mount, physical);
    if (bh == nullptr) {
        return -EIO;
    }
    int rc = xfs_trans_capture_buf(tp, bh);
    if (rc != 0) {
        brelse(bh);
        return rc;
    }
    __builtin_memset(bh->data, 0, ip->mount->block_size);
    auto* header = reinterpret_cast<XfsAttr3LeafHdr*>(bh->data);
    rc = cow_init_da_info(ip, physical, &header->info, XFS_ATTR3_LEAF_MAGIC);
    if (rc != 0) {
        brelse(bh);
        return rc;
    }
    header->info.hdr.back = Be32::from_cpu(previous);
    header->info.hdr.forw = Be32::from_cpu(next);
    auto* entries = reinterpret_cast<XfsAttrLeafEntry*>(bh->data + sizeof(XfsAttr3LeafHdr));
    auto firstused = static_cast<uint16_t>(ip->mount->block_size);
    uint16_t used = 0;
    for (uint32_t i = 0; i < desc.item_count; ++i) {
        CowAttrRec& attr = attrs[desc.item_start + i];
        size_t const PAYLOAD =
            attr.local ? xfs_attr_leaf_entsize_local(attr.namelen, attr.valuelen) : xfs_attr_leaf_entsize_remote(attr.namelen);
        firstused -= static_cast<uint16_t>(PAYLOAD);
        if (attr.local) {
            auto* local = reinterpret_cast<XfsAttrLeafNameLocal*>(bh->data + firstused);
            local->valuelen = Be16::from_cpu(static_cast<uint16_t>(attr.valuelen));
            local->namelen = static_cast<uint8_t>(attr.namelen);
            __builtin_memcpy(xfs_attr_leaf_name_local_name(local), attr.name, attr.namelen);
            if (attr.valuelen != 0) {
                __builtin_memcpy(xfs_attr_leaf_name_local_value(local), attr.value, attr.valuelen);
            }
        } else {
            auto* remote = reinterpret_cast<XfsAttrLeafNameRemote*>(bh->data + firstused);
            remote->valueblk = Be32::from_cpu(attr.valueblk);
            remote->valuelen = Be32::from_cpu(attr.valuelen);
            remote->namelen = static_cast<uint8_t>(attr.namelen);
            __builtin_memcpy(xfs_attr_leaf_name_remote_name(remote), attr.name, attr.namelen);
        }
        entries[i].hashval = Be32::from_cpu(attr.hash);
        entries[i].nameidx = Be16::from_cpu(firstused);
        entries[i].flags = attr.flags | (attr.local ? XFS_ATTR_LOCAL : 0);
        used += static_cast<uint16_t>(PAYLOAD);
    }
    header->count = Be16::from_cpu(static_cast<uint16_t>(desc.item_count));
    header->usedbytes = Be16::from_cpu(used);
    header->firstused = Be16::from_cpu(firstused);
    auto const FREE_BASE = static_cast<uint16_t>(sizeof(XfsAttr3LeafHdr) + (desc.item_count * sizeof(XfsAttrLeafEntry)));
    header->freemap[0].base = Be16::from_cpu(FREE_BASE);
    header->freemap[0].size = Be16::from_cpu(firstused - FREE_BASE);
    attr_leaf_compute_crc(bh->data, ip->mount->block_size);
    xfs_trans_log_buf_full(tp, bh);
    brelse(bh);
    return 0;
}

auto cow_write_node(XfsInode* ip, XfsTransaction* tp, const CowDaDesc* descs, const CowDaDesc& desc, const xfs_fsblock_t* physical,
                    xfs_dablk_t previous, xfs_dablk_t next) -> int {
    BufHead* bh = xfs_buf_get(ip->mount, physical[desc.logical]);
    if (bh == nullptr) {
        return -EIO;
    }
    int rc = xfs_trans_capture_buf(tp, bh);
    if (rc != 0) {
        brelse(bh);
        return rc;
    }
    __builtin_memset(bh->data, 0, ip->mount->block_size);
    auto* header = reinterpret_cast<XfsDa3NodeHdr*>(bh->data);
    rc = cow_init_da_info(ip, physical[desc.logical], &header->info, XFS_DA3_NODE_MAGIC);
    if (rc != 0) {
        brelse(bh);
        return rc;
    }
    header->info.hdr.back = Be32::from_cpu(previous);
    header->info.hdr.forw = Be32::from_cpu(next);
    header->count = Be16::from_cpu(static_cast<uint16_t>(desc.item_count));
    header->level = Be16::from_cpu(desc.level);
    auto* entries = reinterpret_cast<XfsDaNodeEntry*>(bh->data + sizeof(XfsDa3NodeHdr));
    for (uint32_t i = 0; i < desc.item_count; ++i) {
        CowDaDesc const& child = descs[desc.item_start + i];
        entries[i].hashval = Be32::from_cpu(child.max_hash);
        entries[i].before = Be32::from_cpu(child.logical);
    }
    attr_leaf_compute_crc(bh->data, ip->mount->block_size);
    xfs_trans_log_buf_full(tp, bh);
    brelse(bh);
    return 0;
}

auto cow_write_remote(XfsInode* ip, XfsTransaction* tp, CowAttrRec* attrs, uint32_t count, xfs_dablk_t logical_base,
                      const xfs_fsblock_t* physical, uint32_t physical_count) -> int {
    size_t const SPACE = xfs_attr3_rmt_buf_space(ip->mount->block_size);
    for (uint32_t a = 0; a < count; ++a) {
        CowAttrRec& attr = attrs[a];
        if (attr.local) {
            continue;
        }
        uint32_t copied = 0;
        size_t const BLOCKS = xfs_attr3_rmt_blocks(ip->mount->block_size, attr.valuelen);
        for (size_t b = 0; b < BLOCKS; ++b) {
            xfs_dablk_t const LOGICAL = attr.valueblk + b;
            if (LOGICAL < logical_base || LOGICAL - logical_base >= physical_count) {
                return -EIO;
            }
            size_t const PHYSICAL_INDEX = LOGICAL - logical_base;
            BufHead* bh = xfs_buf_get(ip->mount, physical[PHYSICAL_INDEX]);
            if (bh == nullptr) {
                return -EIO;
            }
            int rc = xfs_trans_capture_buf(tp, bh);
            if (rc != 0) {
                brelse(bh);
                return rc;
            }
            __builtin_memset(bh->data, 0, ip->mount->block_size);
            auto* header = reinterpret_cast<XfsAttr3RmtHdr*>(bh->data);
            uint32_t const BYTES = static_cast<uint32_t>(std::min<size_t>(SPACE, attr.valuelen - copied));
            uint64_t device_block = 0;
            rc = attr_device_block(ip->mount, physical[PHYSICAL_INDEX], &device_block);
            if (rc != 0) {
                brelse(bh);
                return rc;
            }
            header->rm_magic = Be32::from_cpu(XFS_ATTR3_RMT_MAGIC);
            header->rm_offset = Be32::from_cpu(copied);
            header->rm_bytes = Be32::from_cpu(BYTES);
            __builtin_memcpy(&header->rm_uuid, &ip->mount->uuid, sizeof(XfsUuidT));
            header->rm_owner = Be64::from_cpu(ip->ino);
            header->rm_blkno = Be64::from_cpu(device_block);
            __builtin_memcpy(bh->data + sizeof(XfsAttr3RmtHdr), attr.value + copied, BYTES);
            attr_remote_compute_crc(bh->data, ip->mount->block_size);
            xfs_trans_log_buf_full(tp, bh);
            brelse(bh);
            copied += BYTES;
        }
        if (copied != attr.valuelen) {
            return -EIO;
        }
    }
    return 0;
}

auto cow_build_block_fork(XfsInode* ip, XfsTransaction* tp, CowAttrRec* attrs, uint32_t count, XfsIfork* new_fork, uint8_t* new_forkoff,
                          uint16_t* new_anextents, uint64_t* new_blocks) -> int {
    size_t const BLOCK_SIZE = ip->mount->block_size;
    if (count == 0 || BLOCK_SIZE > UINT16_MAX || BLOCK_SIZE < sizeof(XfsDa3NodeHdr)) {
        return -EIO;
    }
    auto const NODE_CAPACITY = static_cast<uint32_t>((BLOCK_SIZE - sizeof(XfsDa3NodeHdr)) / sizeof(XfsDaNodeEntry));
    if (NODE_CAPACITY < 2) {
        return -EIO;
    }
    for (uint32_t i = 0; i < count; ++i) {
        attrs[i].local = attrs[i].valuelen <= UINT16_MAX &&
                         xfs_attr_leaf_entsize_local(attrs[i].namelen, attrs[i].valuelen) <= attr_leaf_local_max(BLOCK_SIZE);
    }

    auto* leaf_starts = new (std::nothrow) uint32_t[count];
    auto* leaf_counts = new (std::nothrow) uint32_t[count];
    if (leaf_starts == nullptr || leaf_counts == nullptr) {
        delete[] leaf_starts;
        delete[] leaf_counts;
        return -ENOMEM;
    }
    uint32_t leaf_count = 0;
    uint32_t attr_index = 0;
    while (attr_index < count) {
        uint32_t const START = attr_index;
        size_t bytes = sizeof(XfsAttr3LeafHdr);
        while (attr_index < count) {
            size_t const PAYLOAD = attrs[attr_index].local
                                       ? xfs_attr_leaf_entsize_local(attrs[attr_index].namelen, attrs[attr_index].valuelen)
                                       : xfs_attr_leaf_entsize_remote(attrs[attr_index].namelen);
            if (bytes + sizeof(XfsAttrLeafEntry) + PAYLOAD > BLOCK_SIZE) {
                break;
            }
            bytes += sizeof(XfsAttrLeafEntry) + PAYLOAD;
            attr_index++;
        }
        if (attr_index == START) {
            delete[] leaf_starts;
            delete[] leaf_counts;
            return -E2BIG;
        }
        leaf_starts[leaf_count] = START;
        leaf_counts[leaf_count] = attr_index - START;
        leaf_count++;
    }

    uint32_t max_descs = leaf_count;
    uint32_t level_items = leaf_count;
    while (level_items > 1) {
        level_items = 1U + ((level_items - 1U) / NODE_CAPACITY);
        if (max_descs > UINT32_MAX - level_items) {
            delete[] leaf_starts;
            delete[] leaf_counts;
            return -EFBIG;
        }
        max_descs += level_items;
    }
    auto* descs = new (std::nothrow) CowDaDesc[max_descs];
    if (descs == nullptr) {
        delete[] leaf_starts;
        delete[] leaf_counts;
        return -ENOMEM;
    }
    uint32_t desc_count = leaf_count;
    for (uint32_t i = 0; i < leaf_count; ++i) {
        descs[i] = {.leaf = true,
                    .level = 0,
                    .item_start = leaf_starts[i],
                    .item_count = leaf_counts[i],
                    .max_hash = attrs[leaf_starts[i] + leaf_counts[i] - 1].hash};
    }
    delete[] leaf_starts;
    delete[] leaf_counts;

    uint32_t child_start = 0;
    uint32_t child_count = leaf_count;
    uint16_t level = 1;
    while (child_count > 1) {
        uint32_t const PARENT_COUNT = 1U + ((child_count - 1U) / NODE_CAPACITY);
        uint32_t consumed = 0;
        for (uint32_t parent = 0; parent < PARENT_COUNT; ++parent) {
            uint32_t const LEFT = child_count - consumed;
            uint32_t const CHILDREN = std::min(NODE_CAPACITY, LEFT);
            descs[desc_count++] = {.leaf = false,
                                   .level = level,
                                   .item_start = child_start + consumed,
                                   .item_count = CHILDREN,
                                   .max_hash = descs[child_start + consumed + CHILDREN - 1].max_hash};
            consumed += CHILDREN;
        }
        child_start += child_count;
        child_count = PARENT_COUNT;
        level++;
    }
    if (desc_count != max_descs || level > XFS_BTREE_MAXLEVELS) {
        delete[] descs;
        return -EFBIG;
    }
    uint32_t const ROOT_INDEX = desc_count - 1;
    descs[ROOT_INDEX].logical = 0;
    xfs_dablk_t next_logical = 1;
    for (uint32_t i = 0; i < desc_count; ++i) {
        if (i != ROOT_INDEX) {
            descs[i].logical = next_logical++;
        }
    }
    uint64_t remote_blocks = 0;
    for (uint32_t i = 0; i < count; ++i) {
        if (!attrs[i].local) {
            size_t const BLOCKS = xfs_attr3_rmt_blocks(BLOCK_SIZE, attrs[i].valuelen);
            if (BLOCKS == 0 || desc_count > UINT32_MAX - remote_blocks || BLOCKS > UINT32_MAX - desc_count - remote_blocks) {
                delete[] descs;
                return -EFBIG;
            }
            attrs[i].valueblk = static_cast<xfs_dablk_t>(desc_count + remote_blocks);
            remote_blocks += BLOCKS;
        }
    }

    xfs_fsblock_t* da_physical = nullptr;
    XfsBmbtIrec* da_mappings = nullptr;
    uint32_t da_mapping_count = 0;
    int rc = cow_allocate_blocks(ip, tp, 0, desc_count, &da_physical, &da_mappings, &da_mapping_count);
    if (rc != 0) {
        delete[] descs;
        return rc;
    }
    xfs_fsblock_t* remote_physical = nullptr;
    XfsBmbtIrec* remote_mappings = nullptr;
    uint32_t remote_mapping_count = 0;
    if (remote_blocks != 0) {
        rc = cow_allocate_blocks(ip, tp, desc_count, static_cast<uint32_t>(remote_blocks), &remote_physical, &remote_mappings,
                                 &remote_mapping_count);
    }
    uint32_t const MAPPING_COUNT = da_mapping_count + remote_mapping_count;
    auto* mappings = rc == 0 ? new (std::nothrow) XfsBmbtIrec[MAPPING_COUNT] : nullptr;
    if (rc == 0 && mappings == nullptr) {
        rc = -ENOMEM;
    }
    for (uint32_t i = 0; i < da_mapping_count && rc == 0; ++i) {
        mappings[i] = da_mappings[i];
    }
    for (uint32_t i = 0; i < remote_mapping_count && rc == 0; ++i) {
        mappings[da_mapping_count + i] = remote_mappings[i];
    }
    delete[] da_mappings;
    delete[] remote_mappings;
    for (uint32_t i = 0; i < leaf_count && rc == 0; ++i) {
        xfs_dablk_t const PREVIOUS = i == 0 ? 0 : descs[i - 1].logical;
        xfs_dablk_t const NEXT = i + 1 == leaf_count ? 0 : descs[i + 1].logical;
        rc = cow_write_leaf(ip, tp, attrs, descs[i], da_physical[descs[i].logical], PREVIOUS, NEXT);
    }
    for (uint32_t i = leaf_count; i < desc_count && rc == 0; ++i) {
        xfs_dablk_t previous = 0;
        xfs_dablk_t next = 0;
        cow_node_siblings(descs, desc_count, i, &previous, &next);
        rc = cow_write_node(ip, tp, descs, descs[i], da_physical, previous, next);
    }
    if (rc == 0) {
        rc = cow_write_remote(ip, tp, attrs, count, desc_count, remote_physical, static_cast<uint32_t>(remote_blocks));
    }
    delete[] da_physical;
    delete[] remote_physical;
    delete[] descs;
    if (rc != 0) {
        delete[] mappings;
        return rc;
    }

    constexpr size_t MIN_BMDR_BYTES = sizeof(XfsBmdrBlock) + sizeof(XfsBmbtKey) + sizeof(Be64);
    rc = attr_forkoff_for_space(ip, std::max(MIN_BMDR_BYTES, sizeof(XfsBmbtRec)), new_forkoff);
    if (rc != 0) {
        delete[] mappings;
        return rc;
    }
    size_t const LITERAL = ip->mount->inode_size - sizeof(XfsDinode);
    size_t const FORK_SPACE = LITERAL - (static_cast<size_t>(*new_forkoff) << 3U);
    uint32_t metadata_blocks = 0;
    if (static_cast<size_t>(MAPPING_COUNT) * sizeof(XfsBmbtRec) <= FORK_SPACE) {
        new_fork->format = XFS_DINODE_FMT_EXTENTS;
        new_fork->extents.count = MAPPING_COUNT;
        new_fork->extents.capacity = MAPPING_COUNT > XFS_IFORK_INLINE_EXTENT_CAPACITY ? MAPPING_COUNT : XFS_IFORK_INLINE_EXTENT_CAPACITY;
        if (MAPPING_COUNT > XFS_IFORK_INLINE_EXTENT_CAPACITY) {
            new_fork->extents.list = mappings;
            mappings = nullptr;
        } else {
            new_fork->extents.list = xfs_ifork_extents_inline_data(new_fork->extents);
            for (uint32_t i = 0; i < MAPPING_COUNT; ++i) {
                new_fork->extents.list[i] = mappings[i];
            }
        }
    } else {
        new_fork->format = XFS_DINODE_FMT_BTREE;
        rc = xfs_bmap_build_fork_btree(ip, tp, mappings, MAPPING_COUNT, FORK_SPACE, &new_fork->btree, &metadata_blocks);
        if (rc != 0) {
            delete[] mappings;
            return rc;
        }
    }
    delete[] mappings;
    if (MAPPING_COUNT > UINT16_MAX) {
        cow_free_ifork_memory(new_fork, new_fork->format == XFS_DINODE_FMT_EXTENTS && xfs_ifork_extents_uses_inline(new_fork->extents));
        return -EFBIG;
    }
    *new_anextents = static_cast<uint16_t>(MAPPING_COUNT);
    *new_blocks = desc_count + remote_blocks + metadata_blocks;
    return 0;
}

auto cow_attr_mutate(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, const uint8_t* value, uint32_t valuelen,
                     uint8_t flags, bool remove) -> int {
    CowAttrRec* attrs = nullptr;
    uint32_t count = 0;
    int rc = cow_collect_mutation(ip, name, namelen, value, valuelen, flags, remove, &attrs, &count);
    if (rc != 0) {
        log("[xfs attr] COW mutation collect failed ino=%lu remove=%d rc=%d\n", static_cast<unsigned long>(ip->ino), remove, rc);
        return rc;
    }
    XfsBmbtIrec* old_mappings = nullptr;
    uint32_t old_mapping_count = 0;
    rc = cow_old_mappings(ip, &old_mappings, &old_mapping_count);
    if (rc != 0) {
        log("[xfs attr] COW old-mapping collection failed ino=%lu format=%u anextents=%u rc=%d\n", static_cast<unsigned long>(ip->ino),
            ip->attr_fork.format, ip->anextents, rc);
        cow_free_attrs(attrs, count);
        return rc;
    }

    bool const OLD_HAS_FORK = ip->has_attr_fork;
    XfsIfork old_fork{};
    bool old_extents_inline = false;
    if (OLD_HAS_FORK) {
        old_fork = ip->attr_fork;
        old_extents_inline = old_fork.format == XFS_DINODE_FMT_EXTENTS && xfs_ifork_extents_uses_inline(ip->attr_fork.extents);
    }
    uint64_t const OLD_NBLOCKS = ip->nblocks;

    XfsIfork new_fork{};
    uint8_t new_forkoff = 0;
    uint16_t new_anextents = 0;
    uint64_t new_blocks = 0;
    bool new_has_fork = count != 0;
    if (count != 0) {
        uint8_t* sf_data = nullptr;
        size_t sf_size = 0;
        rc = cow_build_shortform(ip, attrs, count, &sf_data, &sf_size, &new_forkoff);
        if (rc == 0) {
            new_fork.format = XFS_DINODE_FMT_LOCAL;
            new_fork.local.data = sf_data;
            new_fork.local.size = sf_size;
        } else if (rc == -ENOSPC) {
            rc = cow_build_block_fork(ip, tp, attrs, count, &new_fork, &new_forkoff, &new_anextents, &new_blocks);
        }
        if (rc != 0) {
            log("[xfs attr] COW replacement build failed ino=%lu attrs=%u rc=%d\n", static_cast<unsigned long>(ip->ino), count, rc);
            delete[] old_mappings;
            cow_free_attrs(attrs, count);
            return rc;
        }
    } else {
        new_fork.format = XFS_DINODE_FMT_LOCAL;
        new_fork.local.data = nullptr;
        new_fork.local.size = 0;
    }

    ip->attr_fork = new_fork;
    if (new_fork.format == XFS_DINODE_FMT_EXTENTS && xfs_ifork_extents_uses_inline(new_fork.extents)) {
        ip->attr_fork.extents.list = xfs_ifork_extents_inline_data(ip->attr_fork.extents);
    }
    ip->has_attr_fork = new_has_fork;
    ip->forkoff = new_has_fork ? new_forkoff : 0;
    ip->anextents = new_anextents;

    uint64_t freed_blocks = 0;
    if (OLD_HAS_FORK) {
        rc = cow_retire_old_fork(ip, tp, old_fork, old_mappings, old_mapping_count, &freed_blocks);
    }
    delete[] old_mappings;
    if (OLD_HAS_FORK) {
        cow_free_ifork_memory(&old_fork, old_extents_inline);
    }
    cow_free_attrs(attrs, count);
    if (rc != 0) {
        log("[xfs attr] COW old-fork retirement failed ino=%lu mappings=%u rc=%d\n", static_cast<unsigned long>(ip->ino), old_mapping_count,
            rc);
        return rc;
    }
    if (freed_blocks > OLD_NBLOCKS || OLD_NBLOCKS - freed_blocks > UINT64_MAX - new_blocks) {
        return -EIO;
    }
    ip->nblocks = OLD_NBLOCKS - freed_blocks + new_blocks;
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);
    return 0;
}

// Convert shortform to one logged leaf.  The old fork remains untouched until
// every fallible allocation and buffer capture succeeds.
auto sf_to_leaf_convert(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, const uint8_t* val, uint32_t valuelen,
                        uint8_t flags) -> int {
    XfsMountContext* ctx = ip->mount;
    const size_t BLK_SIZE = ctx->block_size;
    if (valuelen > UINT16_MAX) {
        return -E2BIG;
    }

    const XfsAttrSfHdr* sf_hdrp = nullptr;
    bool const HAS_SF = ip->has_attr_fork && ip->attr_fork.format == XFS_DINODE_FMT_LOCAL;
    if (HAS_SF && sf_validate(ip, &sf_hdrp) != 0) {
        return -EIO;
    }
    const auto* sf_base = HAS_SF ? ip->attr_fork.local.data : nullptr;
    uint8_t const SF_COUNT = sf_hdrp != nullptr ? sf_hdrp->count : 0;

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

    uint32_t n = 0;
    if (sf_hdrp != nullptr) {
        size_t pos = sizeof(XfsAttrSfHdr);
        for (uint8_t i = 0; i < SF_COUNT; i++) {
            const auto* entry = reinterpret_cast<const XfsAttrSfEntry*>(sf_base + pos);
            size_t const ENTRY_SIZE = xfs_attr_sf_entry_size(entry);
            if (name_match(entry, name, namelen, flags)) {
                pos += ENTRY_SIZE;
                continue;
            }
            recs[n].name_ptr = xfs_attr_sf_entry_name(entry);
            recs[n].namelen = entry->namelen;
            recs[n].val_ptr = xfs_attr_sf_entry_value(entry);
            recs[n].valuelen = entry->valuelen;
            recs[n].flags = entry->flags;
            int const HASH_RC =
                attr_compute_hash(recs[n].flags, recs[n].name_ptr, recs[n].namelen, recs[n].val_ptr, recs[n].valuelen, &recs[n].hash);
            if (HASH_RC != 0) {
                delete[] recs;
                return HASH_RC;
            }
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
    int const NEW_HASH_RC = attr_compute_hash(flags, name, namelen, val, valuelen, &recs[n].hash);
    if (NEW_HASH_RC != 0) {
        delete[] recs;
        return NEW_HASH_RC;
    }
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

    size_t const HEADER_BYTES = sizeof(XfsAttr3LeafHdr);
    size_t const ENTRIES_BYTES = static_cast<size_t>(n) * sizeof(XfsAttrLeafEntry);
    size_t payload_bytes = 0;
    for (uint32_t i = 0; i < n; i++) {
        payload_bytes += xfs_attr_leaf_entsize_local(recs[i].namelen, recs[i].valuelen);
    }
    size_t const TOTAL_BYTES = HEADER_BYTES + ENTRIES_BYTES + payload_bytes;

    if (TOTAL_BYTES > BLK_SIZE) {
        // Would not fit in a single leaf block.
        delete[] recs;
        log("[xfs attr] sf→leaf: total %zu > blk_size %zu, cannot convert\n", TOTAL_BYTES, BLK_SIZE);
        return -ENOSPC;
    }

    uint8_t new_forkoff = ip->forkoff;
    int rc = attr_forkoff_for_space(ip, sizeof(XfsBmbtRec), &new_forkoff);
    if (rc != 0) {
        delete[] recs;
        return rc;
    }

    xfs_agnumber_t const PREF_AG = xfs_ino_ag(ip->ino, ctx->agino_log);
    XfsAllocReq req{};
    req.agno = PREF_AG;
    req.agbno = 0;
    req.minlen = 1;
    req.maxlen = 1;
    req.alignment = 0;

    XfsAllocResult alloc{};
    rc = xfs_alloc_extent(ctx, tp, req, &alloc);
    if (rc != 0) {
        delete[] recs;
        return rc;
    }

    xfs_fsblock_t const DISK_BLOCK = xfs_agbno_to_fsbno(alloc.agno, alloc.agbno, ctx->ag_blk_log);

    //  4. Build the leaf block
    BufHead* bh = xfs_buf_get(ctx, DISK_BLOCK);
    if (bh == nullptr) {
        delete[] recs;
        return -EIO;
    }

    rc = xfs_trans_capture_buf(tp, bh);
    if (rc != 0) {
        brelse(bh);
        delete[] recs;
        return rc;
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
        uint64_t device_block = 0;
        if (attr_device_block(ctx, DISK_BLOCK, &device_block) != 0) {
            brelse(bh);
            delete[] recs;
            return -EIO;
        }
        lhdr->info.blkno = Be64::from_cpu(device_block);
    }

    // 4b. Build entries + name/value area from end of block backward.
    auto* leaf_entries = reinterpret_cast<XfsAttrLeafEntry*>(block + sizeof(XfsAttr3LeafHdr));
    auto firstused = static_cast<uint16_t>(BLK_SIZE);
    uint16_t usedbytes = 0;

    for (uint32_t i = 0; i < n; i++) {
        auto const PAYLOAD = static_cast<uint16_t>(xfs_attr_leaf_entsize_local(recs[i].namelen, recs[i].valuelen));
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
    lhdr->pad1 = 0;
    lhdr->pad2 = Be32{};

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

    xfs_trans_log_buf_full(tp, bh);
    brelse(bh);

    delete[] recs;

    uint8_t* old_local = HAS_SF ? ip->attr_fork.local.data : nullptr;

    ip->attr_fork.format = XFS_DINODE_FMT_EXTENTS;
    ip->attr_fork.extents.list = xfs_ifork_extents_inline_data(ip->attr_fork.extents);
    ip->attr_fork.extents.list[0].br_startoff = 0;
    ip->attr_fork.extents.list[0].br_startblock = DISK_BLOCK;
    ip->attr_fork.extents.list[0].br_blockcount = 1;
    ip->attr_fork.extents.list[0].br_unwritten = false;
    ip->attr_fork.extents.count = 1;
    ip->attr_fork.extents.capacity = XFS_IFORK_INLINE_EXTENT_CAPACITY;
    delete[] old_local;
    ip->has_attr_fork = true;
    ip->forkoff = new_forkoff;
    ip->anextents = 1;
    ip->nblocks++;
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);

    return 0;
}

// ============================================================================
// Shortform: set (insert or replace)
// ============================================================================

auto sf_convert_and_set(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, const uint8_t* val, uint32_t valuelen,
                        uint8_t flags) -> int {
    if (valuelen <= UINT16_MAX && xfs_attr_leaf_entsize_local(namelen, valuelen) <= attr_leaf_local_max(ip->mount->block_size)) {
        int const LOCAL_RC = sf_to_leaf_convert(ip, tp, name, namelen, val, valuelen, flags);
        if (LOCAL_RC != -ENOSPC) {
            return LOCAL_RC;
        }
    }
    int const CONVERT_RC = sf_to_leaf_convert(ip, tp, name, namelen, nullptr, 0, flags);
    if (CONVERT_RC != 0) {
        return CONVERT_RC;
    }
    return extents_set(ip, tp, name, namelen, val, valuelen, flags);
}

auto sf_set(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, const uint8_t* val, uint32_t valuelen, uint8_t flags)
    -> int {
    constexpr uint32_t XATTR_SIZE_MAX = 65536;
    if (namelen > UINT8_MAX || valuelen > XATTR_SIZE_MAX) {
        return -E2BIG;
    }
    size_t const NEW_ENTRY_SIZE = sizeof(XfsAttrSfEntry) + namelen + valuelen;

    if (!ip->has_attr_fork) {
        if (valuelen > UINT8_MAX) {
            return sf_convert_and_set(ip, tp, name, namelen, val, valuelen, flags);
        }
        size_t const ALLOC_SIZE = sizeof(XfsAttrSfHdr) + NEW_ENTRY_SIZE;
        uint8_t new_forkoff = 0;
        int const SPACE_RC = attr_forkoff_for_space(ip, ALLOC_SIZE, &new_forkoff);
        if (SPACE_RC != 0) {
            return sf_convert_and_set(ip, tp, name, namelen, val, valuelen, flags);
        }
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

        ip->attr_fork.format = XFS_DINODE_FMT_LOCAL;
        ip->attr_fork.local.data = buf;
        ip->attr_fork.local.size = ALLOC_SIZE;
        ip->has_attr_fork = true;
        ip->anextents = 0;
        ip->forkoff = new_forkoff;

        ip->dirty = true;
        xfs_trans_log_inode(tp, ip);
        return 0;
    }

    const XfsAttrSfHdr* hdr = nullptr;
    int const VALID_RC = sf_validate(ip, &hdr);
    if (VALID_RC != 0) {
        return VALID_RC;
    }

    const auto* base = ip->attr_fork.local.data;
    size_t total = hdr->totsize.to_cpu();
    uint8_t const OLD_COUNT = hdr->count;
    size_t pos = sizeof(XfsAttrSfHdr);
    size_t old_pos = total;
    size_t old_size = 0;

    for (uint8_t i = 0; i < hdr->count; i++) {
        const auto* entry = reinterpret_cast<const XfsAttrSfEntry*>(base + pos);
        size_t const ENTRY_SIZE = xfs_attr_sf_entry_size(entry);
        if (name_match(entry, name, namelen, flags)) {
            old_pos = pos;
            old_size = ENTRY_SIZE;
            break;
        }
        pos += ENTRY_SIZE;
    }

    size_t const RETAINED_TOTAL = total - old_size;
    size_t const NEW_TOTAL = RETAINED_TOTAL + NEW_ENTRY_SIZE;
    if (NEW_TOTAL < sizeof(XfsAttrSfHdr)) {
        return -EIO;
    }
    if (valuelen > UINT8_MAX || NEW_TOTAL > UINT16_MAX || NEW_TOTAL > attr_fork_space(ip)) {
        return sf_convert_and_set(ip, tp, name, namelen, val, valuelen, flags);
    }

    auto* new_buf = new (std::nothrow) uint8_t[NEW_TOTAL];
    if (new_buf == nullptr) {
        return -ENOMEM;
    }
    __builtin_memcpy(new_buf, base, old_pos);
    if (old_size != 0) {
        __builtin_memcpy(new_buf + old_pos, base + old_pos + old_size, total - old_pos - old_size);
    }

    auto* new_entry = reinterpret_cast<XfsAttrSfEntry*>(new_buf + RETAINED_TOTAL);
    new_entry->namelen = static_cast<uint8_t>(namelen);
    new_entry->valuelen = static_cast<uint8_t>(valuelen);
    new_entry->flags = flags;
    __builtin_memcpy(xfs_attr_sf_entry_name(new_entry), name, namelen);
    if (valuelen > 0) {
        __builtin_memcpy(xfs_attr_sf_entry_value(new_entry), val, valuelen);
    }

    // Update header
    auto* new_hdr = reinterpret_cast<XfsAttrSfHdr*>(new_buf);
    new_hdr->count = static_cast<uint8_t>(OLD_COUNT + (old_size == 0 ? 1 : 0));
    new_hdr->totsize = Be16::from_cpu(static_cast<uint16_t>(NEW_TOTAL));

    // Replace buffer in inode
    delete[] ip->attr_fork.local.data;
    ip->attr_fork.local.data = new_buf;
    ip->attr_fork.local.size = NEW_TOTAL;
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);

    return 0;
}

// ============================================================================
// Shortform: remove
// ============================================================================

auto sf_remove(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, uint8_t flags) -> int {
    const XfsAttrSfHdr* checked = nullptr;
    int const VALID_RC = sf_validate(ip, &checked);
    if (VALID_RC != 0) {
        return VALID_RC;
    }
    auto* hdr = sf_hdr_mut(ip);

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
            if (hdr->count == 0) {
                delete[] ip->attr_fork.local.data;
                ip->attr_fork.format = XFS_DINODE_FMT_LOCAL;
                ip->attr_fork.local.data = nullptr;
                ip->attr_fork.local.size = 0;
                ip->has_attr_fork = false;
                ip->forkoff = 0;
            }
            ip->dirty = true;
            xfs_trans_log_inode(tp, ip);
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
    if (ip == nullptr || ip->mount == nullptr || tp == nullptr || tp->mount != ip->mount || name == nullptr || namelen == 0 ||
        namelen > UINT8_MAX || (flags & ~XFS_ATTR_NSP_ONDISK_MASK) != 0) {
        return -EINVAL;
    }
    if (valuelen > 0 && value == nullptr) {
        return -EINVAL;
    }
    if (valuelen > 65536) {
        return -E2BIG;
    }

    int const CAPTURE_RC = xfs_trans_capture_inode(tp, ip);
    if (CAPTURE_RC != 0) {
        return CAPTURE_RC;
    }

    if (!ip->has_attr_fork || ip->attr_fork.format == XFS_DINODE_FMT_LOCAL) {
        return sf_set(ip, tp, name, namelen, value, valuelen, flags);
    }

    if (ip->attr_fork.format == XFS_DINODE_FMT_EXTENTS || ip->attr_fork.format == XFS_DINODE_FMT_BTREE) {
        return cow_attr_mutate(ip, tp, name, namelen, value, valuelen, flags, false);
    }

    log("[xfs attr] set on btree attr fork requires DA btree mutation (format=%d)\n", ip->attr_fork.format);
    return -EOPNOTSUPP;
}

auto xfs_attr_remove(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, uint8_t flags) -> int {
    if (ip == nullptr || ip->mount == nullptr || tp == nullptr || tp->mount != ip->mount || name == nullptr || namelen == 0 ||
        namelen > UINT8_MAX || (flags & ~XFS_ATTR_NSP_ONDISK_MASK) != 0) {
        return -EINVAL;
    }

    if (!ip->has_attr_fork) {
        return -ENOATTR;
    }

    int const CAPTURE_RC = xfs_trans_capture_inode(tp, ip);
    if (CAPTURE_RC != 0) {
        return CAPTURE_RC;
    }

    if (ip->attr_fork.format == XFS_DINODE_FMT_LOCAL) {
        return sf_remove(ip, tp, name, namelen, flags);
    }
    if (ip->attr_fork.format == XFS_DINODE_FMT_EXTENTS || ip->attr_fork.format == XFS_DINODE_FMT_BTREE) {
        return cow_attr_mutate(ip, tp, name, namelen, nullptr, 0, flags, true);
    }

    return -EOPNOTSUPP;
}

auto xfs_attr_teardown(XfsInode* ip, XfsTransaction* tp) -> int {
    if (ip == nullptr || tp == nullptr || tp->mount != ip->mount) {
        return -EINVAL;
    }
    if (!ip->has_attr_fork) {
        return 0;
    }
    int const CAPTURE_RC = xfs_trans_capture_inode(tp, ip);
    if (CAPTURE_RC != 0) {
        return CAPTURE_RC;
    }
    XfsBmbtIrec* mappings = nullptr;
    uint32_t mapping_count = 0;
    int rc = cow_old_mappings(ip, &mappings, &mapping_count);
    if (rc != 0) {
        return rc;
    }
    XfsIfork old_fork = ip->attr_fork;
    bool const OLD_INLINE = old_fork.format == XFS_DINODE_FMT_EXTENTS && xfs_ifork_extents_uses_inline(ip->attr_fork.extents);
    uint64_t freed = 0;
    rc = cow_retire_old_fork(ip, tp, old_fork, mappings, mapping_count, &freed);
    delete[] mappings;
    if (rc != 0) {
        return rc;
    }
    if (freed > ip->nblocks) {
        return -EIO;
    }
    cow_free_ifork_memory(&old_fork, OLD_INLINE);
    ip->attr_fork.format = XFS_DINODE_FMT_LOCAL;
    ip->attr_fork.local.data = nullptr;
    ip->attr_fork.local.size = 0;
    ip->has_attr_fork = false;
    ip->forkoff = 0;
    ip->anextents = 0;
    ip->nblocks -= freed;
    ip->dirty = true;
    xfs_trans_log_inode(tp, ip);
    return 0;
}

#ifdef WOS_SELFTEST
void xfs_selftest_attr_fragment_mappings(bool enabled) { selftest_fragment_mappings = enabled; }

auto xfs_selftest_attr_parent_hash_preserved() -> bool {
    constexpr uint8_t NAME[]{0x11, 0x22, 0x33};
    constexpr uint8_t VALUE[]{0x44, 0x55};
    constexpr xfs_dahash_t STORED_HASH = 0xA1B2C3D4;
    XfsAttrEntry entry{
        .name = NAME, .namelen = sizeof(NAME), .value = VALUE, .valuelen = sizeof(VALUE), .flags = XFS_ATTR_PARENT, .hash = STORED_HASH};
    CowAttrRec collected{};
    CowCollectContext context{.attrs = &collected, .capacity = 1};
    int const RC = cow_collect_attr(&entry, &context);
    XfsParentRec parent{};
    parent.p_ino = Be64::from_cpu(0x1122334455667788ULL);
    xfs_dahash_t computed_hash = 0;
    int const STANDARD_RC =
        attr_compute_hash(XFS_ATTR_PARENT, NAME, sizeof(NAME), reinterpret_cast<const uint8_t*>(&parent), sizeof(parent), &computed_hash);
    xfs_dahash_t const EXPECTED_HASH = xfs_da_hashname(NAME, sizeof(NAME)) ^ 0x11223344U ^ 0x55667788U;
    xfs_dahash_t legacy_hash = 0;
    int const LEGACY_RC =
        attr_compute_hash(XFS_ATTR_PARENT, reinterpret_cast<const uint8_t*>(&parent), sizeof(parent), NAME, sizeof(NAME), &legacy_hash);
    bool const OK = RC == 0 && context.count == 1 && collected.hash == STORED_HASH && collected.flags == XFS_ATTR_PARENT &&
                    attr_leaf_hash_matches(XFS_ATTR_PARENT, STORED_HASH, NAME, sizeof(NAME)) &&
                    !attr_leaf_hash_matches(0, STORED_HASH, NAME, sizeof(NAME)) && STANDARD_RC == 0 && computed_hash == EXPECTED_HASH &&
                    LEGACY_RC == 0 && legacy_hash == EXPECTED_HASH &&
                    attr_compute_hash(XFS_ATTR_PARENT, NAME, sizeof(NAME), VALUE, sizeof(VALUE), &computed_hash) == -EOPNOTSUPP;
    delete[] collected.name;
    delete[] collected.value;
    return OK;
}

auto xfs_selftest_attr_incomplete_detected() -> bool {
    XfsAttrLeafEntry entries[2]{};
    entries[1].flags = XFS_ATTR_INCOMPLETE;
    return leaf_entries_have_incomplete(entries, 2) && !leaf_entries_have_incomplete(entries, 1);
}

auto xfs_selftest_attr_node_sibling_links() -> bool {
    CowDaDesc descs[4]{};
    descs[0] = {.leaf = false, .level = 1, .logical = 10};
    descs[1] = {.leaf = false, .level = 1, .logical = 11};
    descs[2] = {.leaf = false, .level = 1, .logical = 12};
    descs[3] = {.leaf = false, .level = 2, .logical = 0};
    xfs_dablk_t previous = 0;
    xfs_dablk_t next = 0;
    cow_node_siblings(descs, 4, 0, &previous, &next);
    bool ok = previous == 0 && next == 11;
    cow_node_siblings(descs, 4, 1, &previous, &next);
    ok = ok && previous == 10 && next == 12;
    cow_node_siblings(descs, 4, 2, &previous, &next);
    ok = ok && previous == 11 && next == 0;
    cow_node_siblings(descs, 4, 3, &previous, &next);
    return ok && previous == 0 && next == 0;
}
#endif

}  // namespace ker::vfs::xfs
