// XFS Directory operations implementation.
//
// Handles all four directory formats: shortform, block, leaf, and node.
// Currently implements read-only operations (lookup and iterate).
//
// Reference: reference/xfs/libxfs/xfs_dir2_sf.c, xfs_dir2_block.c,
//            reference/xfs/libxfs/xfs_dir2_leaf.c, xfs_dir2_node.c,
//            reference/xfs/libxfs/xfs_da_btree.c

#include "xfs_dir2.hpp"

#include <array>
#include <cerrno>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <util/crc32c.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_alloc.hpp>
#include <vfs/fs/xfs/xfs_bmap.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

namespace ker::vfs::xfs {

// ============================================================================
// Hash function
// ============================================================================

namespace {

inline auto rol32(uint32_t val, int shift) -> uint32_t { return (val << shift) | (val >> (32 - shift)); }

}  // anonymous namespace

auto xfs_da_hashname(const uint8_t* name, int namelen) -> xfs_dahash_t {
    xfs_dahash_t hash = 0;

    // Process 4 bytes at a time
    while (namelen >= 4) {
        hash = (static_cast<uint32_t>(name[0]) << 21) ^ (static_cast<uint32_t>(name[1]) << 14) ^ (static_cast<uint32_t>(name[2]) << 7) ^
               (static_cast<uint32_t>(name[3]) << 0) ^ rol32(hash, 7 * 4);
        name += 4;
        namelen -= 4;
    }

    // Remaining bytes
    switch (namelen) {
        case 3:
            return (static_cast<uint32_t>(name[0]) << 14) ^ (static_cast<uint32_t>(name[1]) << 7) ^ (static_cast<uint32_t>(name[2]) << 0) ^
                   rol32(hash, 7 * 3);
        case 2:
            return (static_cast<uint32_t>(name[0]) << 7) ^ (static_cast<uint32_t>(name[1]) << 0) ^ rol32(hash, 7 * 2);
        case 1:
            return (static_cast<uint32_t>(name[0]) << 0) ^ rol32(hash, 7 * 1);
        default:
            return hash;
    }
}

// ============================================================================
// Directory entry size helpers
// ============================================================================

namespace {

// Compute on-disk size of a data entry (aligned to 8 bytes)
auto dir2_data_entsize(const XfsMountContext* ctx, uint8_t namelen) -> size_t {
    // inumber(8) + namelen(1) + name(N) + ftype(0 or 1) + tag(2), padded to 8
    size_t len = 8 + 1 + namelen + sizeof(xfs_dir2_data_off_t);
    if (xfs_has_ftype(ctx)) {
        len += 1;  // ftype byte
    }
    return (len + XFS_DIR2_DATA_ALIGN - 1) & ~(XFS_DIR2_DATA_ALIGN - 1);
}

// Read the inode number from a data entry
auto dir2_data_entry_ino(const XfsDir2DataEntry* dep) -> xfs_ino_t { return dep->inumber.to_cpu(); }

// Read the file type from a data entry (ftype byte after the name)
auto dir2_data_entry_ftype(const XfsMountContext* ctx, const XfsDir2DataEntry* dep) -> uint8_t {
    if (xfs_has_ftype(ctx)) {
        return dep->name[dep->namelen];
    }
    return XFS_DIR3_FT_UNKNOWN;
}

// Fill an XfsDirEntry from a data entry
void fill_dir_entry(const XfsMountContext* ctx, const XfsDir2DataEntry* dep, XfsDirEntry* entry) {
    entry->ino = dir2_data_entry_ino(dep);
    entry->ftype = dir2_data_entry_ftype(ctx, dep);
    entry->namelen = dep->namelen;
    __builtin_memcpy(entry->name.data(), dep->name, dep->namelen);
    entry->name[dep->namelen] = '\0';
}

// Get directory block number from dataptr
auto dir2_dataptr_to_db(const XfsMountContext* ctx, xfs_dir2_dataptr_t dp) -> xfs_dir2_db_t {
    uint64_t byte_off = static_cast<uint64_t>(dp) << XFS_DIR2_DATA_ALIGN_LOG;
    return static_cast<xfs_dir2_db_t>(byte_off >> (ctx->block_log + ctx->dir_blk_log));
}

// Get byte offset within directory block from dataptr
auto dir2_dataptr_to_off(const XfsMountContext* ctx, xfs_dir2_dataptr_t dp) -> xfs_dir2_data_off_t {
    uint64_t byte_off = static_cast<uint64_t>(dp) << XFS_DIR2_DATA_ALIGN_LOG;
    return static_cast<xfs_dir2_data_off_t>(byte_off & (ctx->dir_blk_size - 1));
}

// Convert directory block number to file offset (in filesystem blocks)
auto dir2_db_to_fsbno(const XfsMountContext* ctx, xfs_dir2_db_t db) -> xfs_fileoff_t {
    return static_cast<xfs_fileoff_t>(db) << ctx->dir_blk_log;
}

// Read a directory block (may span multiple fs blocks if dir_blk_log > 0)
auto dir2_read_block(XfsInode* dp, xfs_dir2_db_t db, BufHead** bhp) -> int {
    XfsMountContext* ctx = dp->mount;
    xfs_fileoff_t file_block = dir2_db_to_fsbno(ctx, db);

    XfsBmapResult bmap{};
    int rc = xfs_bmap_lookup(dp, file_block, &bmap);
    if (rc != 0) {
#ifdef XFS_DEBUG
        mod::dbg::log("[xfs] dir2_read_block: bmap_lookup failed ino=%lu db=%u rc=%d\n", (unsigned long)dp->ino, db, rc);
#endif
        return rc;
    }
    if (bmap.is_hole) {
#ifdef XFS_DEBUG
        mod::dbg::log("[xfs] dir2_read_block: HOLE ino=%lu db=%u fmt=%d ext_count=%u\n", (unsigned long)dp->ino, db, dp->data_fork.format,
                      dp->data_fork.extents.count);
#endif
        return -EINVAL;
    }
#ifdef XFS_DEBUG
    mod::dbg::log("[xfs] dir2_read_block: ino=%lu db=%u blk=%lu\n", (unsigned long)dp->ino, db, (unsigned long)bmap.startblock);
#endif

    uint32_t fbs = 1U << ctx->dir_blk_log;  // fs blocks per dir block
    if (fbs == 1) {
        *bhp = xfs_buf_read(ctx, bmap.startblock);
    } else {
        *bhp = xfs_buf_read_multi(ctx, bmap.startblock, fbs);
    }

    return (*bhp != nullptr) ? 0 : -EIO;
}

// ============================================================================
// Shortform directory operations
// ============================================================================

// Read an inode number from the shortform entry data
auto sf_get_ino(const XfsDir2SfHdr* hdr, const uint8_t* ptr) -> xfs_ino_t {
    if (hdr->i8count != 0) {
        uint64_t val = 0;
        for (int i = 0; i < 8; i++) {
            val = (val << 8) | ptr[i];
        }
        return val;
    }
    uint32_t val = 0;
    for (int i = 0; i < 4; i++) {
        val = (val << 8) | ptr[i];
    }
    return val;
}

auto dir2_sf_lookup(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry) -> int {
    // Data fork must be LOCAL
    if (dp->data_fork.format != XFS_DINODE_FMT_LOCAL) {
        return -EINVAL;
    }

    const uint8_t* data = dp->data_fork.local.data;
    size_t data_size = dp->data_fork.local.size;
    if (data == nullptr || data_size < sizeof(XfsDir2SfHdr)) {
        return -EINVAL;
    }

    const auto* hdr = reinterpret_cast<const XfsDir2SfHdr*>(data);
    XfsMountContext* ctx = dp->mount;

    // Check for "."
    if (namelen == 1 && name[0] == '.') {
        entry->ino = dp->ino;
        entry->ftype = XFS_DIR3_FT_DIR;
        entry->namelen = 1;
        entry->name[0] = '.';
        entry->name[1] = '\0';
        return 0;
    }

    // Check for ".."
    if (namelen == 2 && name[0] == '.' && name[1] == '.') {
        entry->ino = xfs_dir2_sf_get_parent(hdr);
        entry->ftype = XFS_DIR3_FT_DIR;
        entry->namelen = 2;
        entry->name[0] = '.';
        entry->name[1] = '.';
        entry->name[2] = '\0';
        return 0;
    }

    // Linear scan
    size_t hdr_size = xfs_dir2_sf_hdr_size(hdr);
    size_t ino_size = xfs_dir2_sf_inumber_size(hdr);
    bool has_ftype = xfs_has_ftype(ctx);
    const uint8_t* ptr = data + hdr_size;
    uint8_t count = hdr->count;

    for (uint8_t i = 0; i < count; i++) {
        if (ptr >= data + data_size) {
            break;
        }

        const auto* sfep = reinterpret_cast<const XfsDir2SfEntry*>(ptr);
        uint8_t entry_namelen = sfep->namelen;

        // Inode number is at: sfep->name + namelen [+ 1 if ftype]
        const uint8_t* ino_ptr = sfep->name + entry_namelen;
        uint8_t ftype = XFS_DIR3_FT_UNKNOWN;
        if (has_ftype) {
            ftype = *ino_ptr;
            ino_ptr++;
        }

        if (entry_namelen == namelen && __builtin_memcmp(sfep->name, name, namelen) == 0) {
            entry->ino = sf_get_ino(hdr, ino_ptr);
            entry->ftype = ftype;
            entry->namelen = namelen;
            __builtin_memcpy(entry->name.data(), name, namelen);
            entry->name[namelen] = '\0';
            return 0;
        }

        // Advance to next entry
        size_t entry_size = sizeof(uint8_t) +  // namelen
                            2 +                // offset
                            entry_namelen + (has_ftype ? 1 : 0) + ino_size;
        ptr += entry_size;
    }

    return -ENOENT;
}

auto dir2_sf_iterate(XfsInode* dp, XfsDirIterFn fn, void* user_ctx) -> int {
    if (dp->data_fork.format != XFS_DINODE_FMT_LOCAL) {
        return -EINVAL;
    }

    const uint8_t* data = dp->data_fork.local.data;
    size_t data_size = dp->data_fork.local.size;
    if (data == nullptr || data_size < sizeof(XfsDir2SfHdr)) {
        return -EINVAL;
    }

    const auto* hdr = reinterpret_cast<const XfsDir2SfHdr*>(data);
    XfsMountContext* ctx = dp->mount;

    XfsDirEntry entry{};

    // Emit "."
    entry.ino = dp->ino;
    entry.ftype = XFS_DIR3_FT_DIR;
    entry.namelen = 1;
    entry.name[0] = '.';
    entry.name[1] = '\0';
    int rc = fn(&entry, user_ctx);
    if (rc != 0) {
        return 0;
    }

    // Emit ".."
    entry.ino = xfs_dir2_sf_get_parent(hdr);
    entry.ftype = XFS_DIR3_FT_DIR;
    entry.namelen = 2;
    entry.name[0] = '.';
    entry.name[1] = '.';
    entry.name[2] = '\0';
    rc = fn(&entry, user_ctx);
    if (rc != 0) {
        return 0;
    }

    // Iterate entries
    size_t hdr_size = xfs_dir2_sf_hdr_size(hdr);
    size_t ino_size = xfs_dir2_sf_inumber_size(hdr);
    bool has_ftype = xfs_has_ftype(ctx);
    const uint8_t* ptr = data + hdr_size;
    uint8_t count = hdr->count;

    for (uint8_t i = 0; i < count; i++) {
        if (ptr >= data + data_size) {
            break;
        }

        const auto* sfep = reinterpret_cast<const XfsDir2SfEntry*>(ptr);
        uint8_t entry_namelen = sfep->namelen;

        const uint8_t* ino_ptr = sfep->name + entry_namelen;
        uint8_t ftype = XFS_DIR3_FT_UNKNOWN;
        if (has_ftype) {
            ftype = *ino_ptr;
            ino_ptr++;
        }

        entry.ino = sf_get_ino(hdr, ino_ptr);
        entry.ftype = ftype;
        entry.namelen = entry_namelen;
        __builtin_memcpy(entry.name.data(), sfep->name, entry_namelen);
        entry.name[entry_namelen] = '\0';

        rc = fn(&entry, user_ctx);
        if (rc != 0) {
            return 0;
        }

        size_t entry_size = sizeof(uint8_t) + 2 + entry_namelen + (has_ftype ? 1 : 0) + ino_size;
        ptr += entry_size;
    }

    return 0;
}

// ============================================================================
// Block-format directory operations
// ============================================================================

auto dir2_block_lookup(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry) -> int {
    XfsMountContext* ctx = dp->mount;

    // Read the single directory block
    BufHead* bh = nullptr;
    int rc = dir2_read_block(dp, 0, &bh);
    if (rc != 0) return rc;

    const uint8_t* block = bh->data;
    size_t blksize = ctx->dir_blk_size;

    // Validate magic
    const auto* hdr = reinterpret_cast<const XfsDir3DataHdr*>(block);
    uint32_t magic = hdr->hdr.magic.to_cpu();
    if (magic != XFS_DIR3_BLOCK_MAGIC) {
        mod::dbg::log("[xfs dir] block: bad magic 0x%x\n", magic);
        brelse(bh);
        return -EINVAL;
    }

    // Block tail is at the very end of the block
    const auto* btp = reinterpret_cast<const XfsDir2BlockTail*>(block + blksize - sizeof(XfsDir2BlockTail));
    uint32_t leaf_count = btp->count.to_cpu();
    (void)btp->stale;  // stale count unused in read-only lookup

    // Leaf entries are just before the tail
    const auto* blp = reinterpret_cast<const XfsDir2LeafEntry*>(reinterpret_cast<const uint8_t*>(btp) -
                                                                (static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry)));

    // Hash the name and binary search the leaf array
    xfs_dahash_t hash = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);

    int lo = 0;
    int hi = static_cast<int>(leaf_count) - 1;
    int mid = -1;
    bool found = false;

    while (lo <= hi) {
        mid = (lo + hi) / 2;
        uint32_t entry_hash = blp[mid].hashval.to_cpu();

        if (hash < entry_hash) {
            hi = mid - 1;
        } else if (hash > entry_hash) {
            lo = mid + 1;
        } else {
            found = true;
            break;
        }
    }

    if (!found) {
        brelse(bh);
        return -ENOENT;
    }

    // Back up to the first entry with this hash
    while (mid > 0 && blp[mid - 1].hashval.to_cpu() == hash) {
        mid--;
    }

    // Scan all entries with matching hash
    for (int i = mid; i < static_cast<int>(leaf_count); i++) {
        if (blp[i].hashval.to_cpu() != hash) {
            break;
        }

        xfs_dir2_dataptr_t addr = blp[i].address.to_cpu();
        if (addr == XFS_DIR2_NULL_DATAPTR) {
            continue;  // stale
        }

        uint32_t off = dir2_dataptr_to_off(ctx, addr);
        if (off + sizeof(XfsDir2DataEntry) > blksize) {
            continue;
        }

        const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + off);

        if (dep->namelen == namelen && __builtin_memcmp(dep->name, name, namelen) == 0) {
            fill_dir_entry(ctx, dep, entry);
            brelse(bh);
            return 0;
        }
    }

    brelse(bh);
    return -ENOENT;
}

auto dir2_block_iterate(XfsInode* dp, XfsDirIterFn fn, void* user_ctx) -> int {
    XfsMountContext* ctx = dp->mount;

    BufHead* bh = nullptr;
    int rc = dir2_read_block(dp, 0, &bh);
    if (rc != 0) {
#ifdef XFS_DEBUG
        mod::dbg::log("[xfs] dir2_block_iterate: read_block failed ino=%lu rc=%d\n", (unsigned long)dp->ino, rc);
#endif
        return rc;
    }

    const uint8_t* block = bh->data;
    size_t blksize = ctx->dir_blk_size;

    // Block tail
    const auto* btp = reinterpret_cast<const XfsDir2BlockTail*>(block + blksize - sizeof(XfsDir2BlockTail));
    uint32_t leaf_count = btp->count.to_cpu();

#ifdef XFS_DEBUG
    const auto* dbg_hdr = reinterpret_cast<const XfsDir3DataHdr*>(block);
    mod::dbg::log("[xfs] dir2_block_iterate: ino=%lu magic=0x%x leaf_count=%u\n", (unsigned long)dp->ino, dbg_hdr->hdr.magic.to_cpu(),
                  leaf_count);
#endif
    // Data entries start after the v3 header
    size_t data_start = sizeof(XfsDir3DataHdr);
    // Data entries end before the leaf entries
    const uint8_t* leaf_start = reinterpret_cast<const uint8_t*>(btp) - (static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry));
    auto data_end = static_cast<size_t>(leaf_start - block);

    size_t offset = data_start;
    XfsDirEntry entry{};

    while (offset < data_end) {
        // Check for free space entry
        const auto* unused = reinterpret_cast<const XfsDir2DataUnused*>(block + offset);
        if (unused->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
            uint16_t free_len = unused->length.to_cpu();
            if (free_len == 0 || free_len > data_end - offset) {
                break;
            }
            offset += free_len;
            continue;
        }

        const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + offset);
        fill_dir_entry(ctx, dep, &entry);

        rc = fn(&entry, user_ctx);
        if (rc != 0) {
            brelse(bh);
            return 0;
        }

        offset += dir2_data_entsize(ctx, dep->namelen);
    }

    brelse(bh);
    return 0;
}

// ============================================================================
// Leaf/Node directory — data block scanning
// ============================================================================

// Iterate over a single data block calling fn for each entry
auto dir2_scan_data_block(XfsInode* dp, xfs_dir2_db_t db, XfsDirIterFn fn, void* user_ctx) -> int {
    XfsMountContext* ctx = dp->mount;

    BufHead* bh = nullptr;
    int rc = dir2_read_block(dp, db, &bh);
    if (rc != 0) {
        return rc;
    }

    const uint8_t* block = bh->data;
    size_t blksize = ctx->dir_blk_size;

    // v3 data header
    size_t offset = sizeof(XfsDir3DataHdr);
    XfsDirEntry entry{};

    while (offset + sizeof(XfsDir2DataUnused) <= blksize) {
        const auto* unused = reinterpret_cast<const XfsDir2DataUnused*>(block + offset);
        if (unused->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
            uint16_t free_len = unused->length.to_cpu();
            if (free_len == 0 || offset + free_len > blksize) {
                break;
            }
            offset += free_len;
            continue;
        }

        const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + offset);
        if (dep->namelen == 0 || offset + dir2_data_entsize(ctx, dep->namelen) > blksize) {
            break;  // corrupt or past end
        }

        fill_dir_entry(ctx, dep, &entry);
        rc = fn(&entry, user_ctx);
        if (rc != 0) {
            brelse(bh);
            return 1;  // caller requested stop
        }

        offset += dir2_data_entsize(ctx, dep->namelen);
    }

    brelse(bh);
    return 0;
}

// Lookup in leaf/node format: find the name by scanning data blocks
// using the bmap to resolve block addresses.
auto dir2_leaf_node_lookup(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry) -> int {
    XfsMountContext* ctx = dp->mount;

    // Compute hash
    xfs_dahash_t hash = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);

    // For leaf/node directories, we need to read the leaf block(s) to find
    // the data block containing the matching hash.  The leaf block is at
    // directory block number = XFS_DIR2_LEAF_OFFSET >> (blklog).
    xfs_fileoff_t leaf_fsbno = XFS_DIR2_LEAF_OFFSET >> ctx->block_log;

    XfsBmapResult bmap{};
    int rc = xfs_bmap_lookup(dp, leaf_fsbno, &bmap);
    if (rc != 0 || bmap.is_hole) {
        // No leaf block — might be single-block or corrupt
        // Fall back to linear scan of data blocks
        goto linear_scan;
    }

    {
        // Read the leaf block
        uint32_t fbs = 1u << ctx->dir_blk_log;
        BufHead* leaf_bh = nullptr;
        if (fbs == 1) {
            leaf_bh = xfs_buf_read(ctx, bmap.startblock);
        } else {
            leaf_bh = xfs_buf_read_multi(ctx, bmap.startblock, fbs);
        }
        if (leaf_bh == nullptr) {
            goto linear_scan;
        }

        const uint8_t* leaf_data = leaf_bh->data;

        // Check magic — leaf block starts with xfs_da3_blkinfo
        const auto* info = reinterpret_cast<const XfsDa3Blkinfo*>(leaf_data);
        uint16_t leaf_magic = info->hdr.magic.to_cpu();

        if (leaf_magic != XFS_DIR3_LEAF_MAGIC && leaf_magic != XFS_DIR3_LEAFN_MAGIC) {
            brelse(leaf_bh);
            goto linear_scan;
        }

        // Leaf entries start after the leaf header.
        // Leaf header: xfs_da3_blkinfo + __be16 count + __be16 stale + __be32 pad
        size_t leaf_hdr_size = sizeof(XfsDa3Blkinfo) + 2 + 2 + 4;
        const uint8_t* leaf_entries_base = leaf_data + leaf_hdr_size;

        // Read count from the leaf header
        uint16_t leaf_count = 0;
        __builtin_memcpy(&leaf_count, leaf_data + sizeof(XfsDa3Blkinfo), 2);
        // It's big-endian
        leaf_count = (static_cast<uint16_t>(leaf_data[sizeof(XfsDa3Blkinfo)]) << 8) | leaf_data[sizeof(XfsDa3Blkinfo) + 1];

        const auto* lep = reinterpret_cast<const XfsDir2LeafEntry*>(leaf_entries_base);

        // Binary search for hash
        int lo = 0;
        int hi = leaf_count - 1;
        int mid = -1;
        bool found = false;

        while (lo <= hi) {
            mid = (lo + hi) / 2;
            uint32_t lhash = lep[mid].hashval.to_cpu();
            if (hash < lhash) {
                hi = mid - 1;
            } else if (hash > lhash) {
                lo = mid + 1;
            } else {
                found = true;
                break;
            }
        }

        if (!found) {
            brelse(leaf_bh);
            return -ENOENT;
        }

        // Back up to first with this hash
        while (mid > 0 && lep[mid - 1].hashval.to_cpu() == hash) {
            mid--;
        }

        // Check all matching hashes
        for (int i = mid; i < static_cast<int>(leaf_count); i++) {
            if (lep[i].hashval.to_cpu() != hash) {
                break;
            }

            xfs_dir2_dataptr_t addr = lep[i].address.to_cpu();
            if (addr == XFS_DIR2_NULL_DATAPTR) {
                continue;
            }

            xfs_dir2_db_t db = dir2_dataptr_to_db(ctx, addr);
            uint32_t off = dir2_dataptr_to_off(ctx, addr);

            // Read the data block
            BufHead* data_bh = nullptr;
            rc = dir2_read_block(dp, db, &data_bh);
            if (rc != 0) {
                continue;
            }

            if (off + sizeof(XfsDir2DataEntry) <= ctx->dir_blk_size) {
                const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(data_bh->data + off);
                if (dep->namelen == namelen && __builtin_memcmp(dep->name, name, namelen) == 0) {
                    fill_dir_entry(ctx, dep, entry);
                    brelse(data_bh);
                    brelse(leaf_bh);
                    return 0;
                }
            }
            brelse(data_bh);
        }

        brelse(leaf_bh);
        return -ENOENT;
    }

linear_scan:
    // Fall back to scanning data blocks sequentially
    {
        // Estimate number of data blocks from file size
        uint64_t nblocks = dp->size >> (ctx->block_log + ctx->dir_blk_log);
        if (nblocks == 0) {
            nblocks = 1;
        }

        for (xfs_dir2_db_t db = 0; db < nblocks; db++) {
            BufHead* data_bh = nullptr;
            rc = dir2_read_block(dp, db, &data_bh);
            if (rc != 0) {
                continue;
            }

            const uint8_t* block = data_bh->data;
            size_t blksize = ctx->dir_blk_size;
            size_t offset = sizeof(XfsDir3DataHdr);

            while (offset + sizeof(XfsDir2DataUnused) <= blksize) {
                const auto* unused = reinterpret_cast<const XfsDir2DataUnused*>(block + offset);
                if (unused->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
                    uint16_t free_len = unused->length.to_cpu();
                    if (free_len == 0 || offset + free_len > blksize) {
                        break;
                    }
                    offset += free_len;
                    continue;
                }

                const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + offset);
                if (dep->namelen == 0) {
                    break;
                }

                if (dep->namelen == namelen && __builtin_memcmp(dep->name, name, namelen) == 0) {
                    fill_dir_entry(ctx, dep, entry);
                    brelse(data_bh);
                    return 0;
                }

                offset += dir2_data_entsize(ctx, dep->namelen);
            }
            brelse(data_bh);
        }
    }

    return -ENOENT;
}

// Iterate all data blocks for leaf/node format
auto dir2_leaf_node_iterate(XfsInode* dp, XfsDirIterFn fn, void* user_ctx) -> int {
    XfsMountContext* ctx = dp->mount;

    // Number of data blocks (approximate from file size)
    uint64_t nblocks = dp->size >> (ctx->block_log + ctx->dir_blk_log);
    if (nblocks == 0) {
        nblocks = 1;
    }

    for (xfs_dir2_db_t db = 0; db < nblocks; db++) {
        // Check if this data block exists (not a hole)
        xfs_fileoff_t fbo = dir2_db_to_fsbno(ctx, db);
        XfsBmapResult bmap{};
        int rc = xfs_bmap_lookup(dp, fbo, &bmap);
        if (rc != 0 || bmap.is_hole) {
            continue;
        }

        rc = dir2_scan_data_block(dp, db, fn, user_ctx);
        if (rc != 0) {
            return 0;  // iterator requested stop
        }
    }

    return 0;
}

}  // anonymous namespace

// ============================================================================
// Public API
// ============================================================================

auto xfs_dir_lookup(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry) -> int {
    if (dp == nullptr || name == nullptr || entry == nullptr) {
        return -EINVAL;
    }
    if (!xfs_inode_isdir(dp)) {
        return -ENOTDIR;
    }
#ifdef XFS_DEBUG
    mod::dbg::log("[xfs] dir_lookup: ino=%lu fmt=%d size=%lu name=%.*s\n", (unsigned long)dp->ino, dp->data_fork.format,
                  (unsigned long)dp->size, (int)namelen, name);
#endif
    switch (dp->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL:
            return dir2_sf_lookup(dp, name, namelen, entry);

        case XFS_DINODE_FMT_EXTENTS:
        case XFS_DINODE_FMT_BTREE: {
            // Check if it's block format (single block) or leaf/node
            // Block format: directory size <= dir_blk_size
            if (dp->size <= dp->mount->dir_blk_size) {
                return dir2_block_lookup(dp, name, namelen, entry);
            }
            return dir2_leaf_node_lookup(dp, name, namelen, entry);
        }

        default:
            return -EINVAL;
    }
}

auto xfs_dir_iterate(XfsInode* dp, XfsDirIterFn fn, void* ctx) -> int {
    if (dp == nullptr || fn == nullptr) {
        return -EINVAL;
    }
    if (!xfs_inode_isdir(dp)) {
        return -ENOTDIR;
    }

    switch (dp->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL:
            return dir2_sf_iterate(dp, fn, ctx);

        case XFS_DINODE_FMT_EXTENTS:
        case XFS_DINODE_FMT_BTREE: {
            if (dp->size <= dp->mount->dir_blk_size) {
                return dir2_block_iterate(dp, fn, ctx);
            }
            return dir2_leaf_node_iterate(dp, fn, ctx);
        }

        default:
            return -EINVAL;
    }
}

// ============================================================================
// Directory add-name — add a new entry to a directory
// ============================================================================

namespace {

// Add a name to a shortform directory (inline in inode data fork).
// The new entry is appended after the existing entries.
auto dir2_sf_addname(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype, XfsTransaction* tp) -> int {
    XfsMountContext* ctx = dp->mount;
    bool has_ftype = xfs_has_ftype(ctx);

    const uint8_t* old_data = dp->data_fork.local.data;
    size_t old_size = dp->data_fork.local.size;

    if (old_data == nullptr || old_size < sizeof(XfsDir2SfHdr)) {
        return -EINVAL;
    }

    const auto* old_hdr = reinterpret_cast<const XfsDir2SfHdr*>(old_data);
    size_t ino_size = xfs_dir2_sf_inumber_size(old_hdr);

    // Compute the size of the new entry:
    // namelen(1) + offset(2) + name(namelen) + [ftype(1)] + ino(4 or 8)
    size_t new_entry_size = 1 + 2 + namelen + (has_ftype ? 1 : 0) + ino_size;
    size_t new_size = old_size + new_entry_size;

    // Check if 8-byte inode numbers are needed
    bool need_i8 = (old_hdr->i8count != 0) || (ino > 0xFFFFFFFFULL);

    // If we need to upgrade from 4-byte to 8-byte inodes, the calculation
    // changes significantly.  For simplicity, just handle the common case
    // where the format stays the same.
    if (need_i8 && old_hdr->i8count == 0) {
        // Would need format conversion — fall through to block format
        return -E2BIG;
    }

    // Check if the shortform still fits in the inode literal area
    // (inode_size - dinode header - attr fork space)
    size_t max_inline = ctx->inode_size - 176;  // XfsDinode is 176 bytes
    if (dp->forkoff != 0) {
        max_inline = static_cast<size_t>(dp->forkoff) << 3;
    }
    if (new_size > max_inline) {
        return -E2BIG;  // need to convert to block format
    }

    // Build the new data fork with the entry appended
    auto* new_data = new uint8_t[new_size];
    if (new_data == nullptr) {
        return -ENOMEM;
    }

    // Copy existing data
    __builtin_memcpy(new_data, old_data, old_size);

    // Update the header: increment count
    auto* new_hdr = reinterpret_cast<XfsDir2SfHdr*>(new_data);
    new_hdr->count++;

    // Append entry at old_size offset
    uint8_t* entry_ptr = new_data + old_size;

    // namelen
    entry_ptr[0] = static_cast<uint8_t>(namelen);
    // offset — use a simple sequential offset (count * XFS_DIR2_DATA_ALIGN works as a tag)
    uint16_t off_val = static_cast<uint16_t>(new_hdr->count);
    entry_ptr[1] = static_cast<uint8_t>(off_val >> 8);
    entry_ptr[2] = static_cast<uint8_t>(off_val & 0xFF);
    // name
    __builtin_memcpy(entry_ptr + 3, name, namelen);

    size_t p = 3 + namelen;
    // ftype
    if (has_ftype) {
        entry_ptr[p++] = ftype;
    }
    // inode number (big-endian)
    if (ino_size == 8) {
        for (int i = 7; i >= 0; i--) {
            entry_ptr[p++] = static_cast<uint8_t>((ino >> (i * 8)) & 0xFF);
        }
    } else {
        auto ino32 = static_cast<uint32_t>(ino);
        for (int i = 3; i >= 0; i--) {
            entry_ptr[p++] = static_cast<uint8_t>((ino32 >> (i * 8)) & 0xFF);
        }
    }

    // Replace the data fork
    delete[] dp->data_fork.local.data;
    dp->data_fork.local.data = new_data;
    dp->data_fork.local.size = new_size;
    dp->size = new_size;
    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);

    return 0;
}

// Convert a shortform directory to block format.
// Allocates a disk block, builds a complete XFS_DIR3_BLOCK directory from the
// existing SF entries, and switches the inode data fork from LOCAL to EXTENTS.
auto dir2_sf_to_block(XfsInode* dp, XfsTransaction* tp) -> int {
    XfsMountContext* ctx = dp->mount;

    if (dp->data_fork.format != XFS_DINODE_FMT_LOCAL) {
        return -EINVAL;
    }

    const uint8_t* sf_data = dp->data_fork.local.data;
    size_t sf_size = dp->data_fork.local.size;
    if (sf_data == nullptr || sf_size < sizeof(XfsDir2SfHdr)) {
        return -EINVAL;
    }

    const auto* sf_hdr = reinterpret_cast<const XfsDir2SfHdr*>(sf_data);
    bool has_ftype = xfs_has_ftype(ctx);
    size_t ino_size = xfs_dir2_sf_inumber_size(sf_hdr);
    size_t blksize = ctx->dir_blk_size;

    // --- 1. Collect all shortform entries into a temporary array ---
    struct SfRec {
        char name[256];
        uint8_t namelen;
        xfs_ino_t ino;
        uint8_t ftype;
    };

    uint8_t sf_count = sf_hdr->count;
    auto* recs = new SfRec[sf_count + 2];  // +2 for dot/dotdot
    if (recs == nullptr) return -ENOMEM;

    xfs_ino_t parent_ino = xfs_dir2_sf_get_parent(sf_hdr);

    // "." entry
    recs[0].namelen = 1;
    recs[0].name[0] = '.';
    recs[0].name[1] = '\0';
    recs[0].ino = dp->ino;
    recs[0].ftype = XFS_DIR3_FT_DIR;

    // ".." entry
    recs[1].namelen = 2;
    recs[1].name[0] = '.';
    recs[1].name[1] = '.';
    recs[1].name[2] = '\0';
    recs[1].ino = parent_ino;
    recs[1].ftype = XFS_DIR3_FT_DIR;

    // Parse SF entries
    size_t hdr_size = xfs_dir2_sf_hdr_size(sf_hdr);
    const uint8_t* ptr = sf_data + hdr_size;
    int total_entries = 2;  // dot and dotdot

    for (uint8_t i = 0; i < sf_count; i++) {
        if (ptr >= sf_data + sf_size) break;
        const auto* sfep = reinterpret_cast<const XfsDir2SfEntry*>(ptr);
        uint8_t entry_namelen = sfep->namelen;

        const uint8_t* ino_ptr = sfep->name + entry_namelen;
        uint8_t ftype = XFS_DIR3_FT_UNKNOWN;
        if (has_ftype) {
            ftype = *ino_ptr;
            ino_ptr++;
        }

        recs[total_entries].namelen = entry_namelen;
        __builtin_memcpy(recs[total_entries].name, sfep->name, entry_namelen);
        recs[total_entries].name[entry_namelen] = '\0';
        recs[total_entries].ino = sf_get_ino(sf_hdr, ino_ptr);
        recs[total_entries].ftype = ftype;
        total_entries++;

        size_t entry_size = sizeof(uint8_t) + 2 + entry_namelen + (has_ftype ? 1 : 0) + ino_size;
        ptr += entry_size;
    }

    // --- 2. Allocate a disk block for the directory ---
    xfs_agnumber_t pref_ag = xfs_ino_ag(dp->ino, ctx->agino_log);

    XfsAllocReq req{};
    req.agno = pref_ag;
    req.agbno = 0;
    uint32_t fbs = 1U << ctx->dir_blk_log;  // fs blocks per dir block
    req.minlen = fbs;
    req.maxlen = fbs;
    req.alignment = 0;

    XfsAllocResult alloc_result{};
    int rc = xfs_alloc_extent(ctx, tp, req, &alloc_result);
    if (rc != 0) {
        delete[] recs;
        return rc;
    }

    xfs_fsblock_t disk_block = xfs_agbno_to_fsbno(alloc_result.agno, alloc_result.agbno, ctx->ag_blk_log);

    // Read the block (to get a buffer to write into)
    BufHead* bh = nullptr;
    if (fbs == 1) {
        bh = xfs_buf_read(ctx, disk_block);
    } else {
        bh = xfs_buf_read_multi(ctx, disk_block, fbs);
    }
    if (bh == nullptr) {
        delete[] recs;
        return -EIO;
    }

    uint8_t* block = bh->data;
    __builtin_memset(block, 0, blksize);

    // --- 3. Build the block-format directory ---

    // 3a. Header
    auto* hdr3 = reinterpret_cast<XfsDir3DataHdr*>(block);
    hdr3->hdr.magic = __be32::from_cpu(XFS_DIR3_BLOCK_MAGIC);
    hdr3->hdr.owner = __be64::from_cpu(dp->ino);
    // Compute disk address for blkno field
    {
        auto agno = static_cast<xfs_agnumber_t>(disk_block >> ctx->ag_blk_log);
        auto agbno = static_cast<xfs_agblock_t>(disk_block & ((1ULL << ctx->ag_blk_log) - 1));
        uint64_t linear = (static_cast<uint64_t>(agno) * ctx->ag_blocks) + agbno;
        size_t ratio = ctx->block_size / ctx->device->block_size;
        hdr3->hdr.blkno = __be64::from_cpu(linear * ratio);
    }
    __builtin_memcpy(&hdr3->hdr.uuid, &ctx->uuid, sizeof(XfsUuidT));

    // 3b. Write data entries after the header
    size_t data_offset = sizeof(XfsDir3DataHdr);

    // We also need to build leaf entries for later
    struct LeafRec {
        xfs_dahash_t hash;
        uint32_t address;
    };
    auto* leaves = new LeafRec[total_entries];
    int leaf_count = 0;

    for (int i = 0; i < total_entries; i++) {
        size_t entry_size = dir2_data_entsize(ctx, recs[i].namelen);

        auto* dep = reinterpret_cast<XfsDir2DataEntry*>(block + data_offset);
        dep->inumber = __be64::from_cpu(recs[i].ino);
        dep->namelen = recs[i].namelen;
        __builtin_memcpy(dep->name, recs[i].name, recs[i].namelen);

        // ftype
        if (has_ftype) {
            dep->name[recs[i].namelen] = recs[i].ftype;
        }

        // Zero pad
        size_t used = 8 + 1 + recs[i].namelen + (has_ftype ? 1 : 0);
        size_t pad_start = data_offset + used;
        size_t pad_end = data_offset + entry_size - sizeof(__be16);
        if (pad_end > pad_start) {
            __builtin_memset(block + pad_start, 0, pad_end - pad_start);
        }

        // Tag (self offset within the block, at end of entry)
        auto* tag = reinterpret_cast<__be16*>(block + data_offset + entry_size - sizeof(__be16));
        *tag = __be16::from_cpu(static_cast<uint16_t>(data_offset));

        // Build leaf record
        leaves[leaf_count].hash = xfs_da_hashname(reinterpret_cast<const uint8_t*>(recs[i].name), recs[i].namelen);
        leaves[leaf_count].address = static_cast<uint32_t>(data_offset >> XFS_DIR2_DATA_ALIGN_LOG);
        leaf_count++;

        data_offset += entry_size;
    }

    // 3c. Block tail at the very end
    auto* btp = reinterpret_cast<XfsDir2BlockTail*>(block + blksize - sizeof(XfsDir2BlockTail));
    btp->count = __be32::from_cpu(static_cast<uint32_t>(leaf_count));
    btp->stale = __be32::from_cpu(0);

    // 3d. Leaf entries right before the tail (sorted by hash)
    // Simple insertion sort
    for (int i = 1; i < leaf_count; i++) {
        LeafRec tmp = leaves[i];
        int j = i - 1;
        while (j >= 0 && leaves[j].hash > tmp.hash) {
            leaves[j + 1] = leaves[j];
            j--;
        }
        leaves[j + 1] = tmp;
    }

    auto* blp = reinterpret_cast<XfsDir2LeafEntry*>(block + blksize - sizeof(XfsDir2BlockTail) -
                                                    (static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry)));

    for (int i = 0; i < leaf_count; i++) {
        blp[i].hashval = __be32::from_cpu(leaves[i].hash);
        blp[i].address = __be32::from_cpu(leaves[i].address);
    }

    // 3e. Free space between last data entry and leaf entries
    size_t leaf_area_start = blksize - sizeof(XfsDir2BlockTail) - (static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry));
    if (data_offset < leaf_area_start) {
        size_t free_len = leaf_area_start - data_offset;
        auto* unused = reinterpret_cast<XfsDir2DataUnused*>(block + data_offset);
        unused->freetag = __be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
        unused->length = __be16::from_cpu(static_cast<uint16_t>(free_len));
        // Unused tail tag
        auto* unused_tag = reinterpret_cast<__be16*>(block + data_offset + free_len - sizeof(__be16));
        *unused_tag = __be16::from_cpu(static_cast<uint16_t>(data_offset));

        // Update best_free in the header
        hdr3->best_free[0].offset = __be16::from_cpu(static_cast<uint16_t>(data_offset));
        hdr3->best_free[0].length = __be16::from_cpu(static_cast<uint16_t>(free_len));
    }

    // 3f. Compute CRC over the block
    hdr3->hdr.crc = __be32{0};
    uint32_t crc = util::crc32c_block_with_cksum(block, blksize, 4);  // crc at offset 4 in XfsDir3BlkHdr
    hdr3->hdr.crc = __be32::from_cpu(crc);

    // Write the block to disk IMMEDIATELY.  dir2_block_addname (which runs
    // in the same transaction, before commit) will re-read this block from
    // disk.  Since multi-block buffers aren't cached, logging the buffer in
    // the transaction is insufficient — we must ensure the data is on disk
    // before the addname read.
    bwrite(bh);
    brelse(bh);

    delete[] leaves;
    delete[] recs;

    // --- 4. Switch the inode from LOCAL to EXTENTS ---
    // Free old inline data
    delete[] dp->data_fork.local.data;

    dp->data_fork.format = XFS_DINODE_FMT_EXTENTS;
    dp->data_fork.extents.list = new XfsBmbtIrec[1];
    dp->data_fork.extents.list[0].br_startoff = 0;
    dp->data_fork.extents.list[0].br_startblock = disk_block;
    dp->data_fork.extents.list[0].br_blockcount = fbs;
    dp->data_fork.extents.list[0].br_unwritten = false;
    dp->data_fork.extents.count = 1;
    dp->nextents = 1;
    dp->nblocks = fbs;
    dp->size = blksize;
    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);
#ifdef XFS_DEBUG
    mod::dbg::log("[xfs] dir sf->block conversion complete: ino=%lu blk=%lu entries=%d\n", (unsigned long)dp->ino,
                  (unsigned long)disk_block, total_entries);
#endif
    return 0;
}

// Add a name to a block-format directory (single directory block).
auto dir2_block_addname(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype, XfsTransaction* tp) -> int {
    XfsMountContext* ctx = dp->mount;

    // Read the single directory block
    BufHead* bh = nullptr;
    int rc = dir2_read_block(dp, 0, &bh);
    if (rc != 0) return rc;

    uint8_t* block = bh->data;
    size_t blksize = ctx->dir_blk_size;

    // Validate magic
    const auto* hdr = reinterpret_cast<const XfsDir3DataHdr*>(block);
    uint32_t magic = hdr->hdr.magic.to_cpu();
    if (magic != XFS_DIR3_BLOCK_MAGIC) {
        brelse(bh);
        return -EINVAL;
    }

    // Compute the entry size needed
    bool has_ftype_flag = xfs_has_ftype(ctx);
    size_t need_len = dir2_data_entsize(ctx, namelen);

    // Block tail is at the very end of the block
    auto* btp = reinterpret_cast<XfsDir2BlockTail*>(block + blksize - sizeof(XfsDir2BlockTail));
    uint32_t leaf_count = btp->count.to_cpu();

    // Leaf entries are just before the tail
    uint8_t* leaf_start = block + blksize - sizeof(XfsDir2BlockTail) - (static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry));

    // Scan data area for a free space entry large enough
    size_t data_start = sizeof(XfsDir3DataHdr);
    size_t data_end = static_cast<size_t>(leaf_start - block);

    size_t offset = data_start;
    size_t found_offset = 0;
    bool found_free = false;

    while (offset < data_end) {
        const auto* unused = reinterpret_cast<const XfsDir2DataUnused*>(block + offset);
        if (unused->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
            uint16_t free_len = unused->length.to_cpu();
            if (free_len == 0 || offset + free_len > data_end) {
                break;
            }
            if (free_len >= need_len) {
                found_offset = offset;
                found_free = true;
                break;
            }
            offset += free_len;
            continue;
        }

        const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + offset);
        if (dep->namelen == 0) break;
        offset += dir2_data_entsize(ctx, dep->namelen);
    }

    if (!found_free) {
        brelse(bh);
        return -ENOSPC;  // block full — would need conversion to leaf format
    }

    // Write the new data entry at found_offset
    const auto* old_unused = reinterpret_cast<const XfsDir2DataUnused*>(block + found_offset);
    uint16_t old_free_len = old_unused->length.to_cpu();

    auto* dep = reinterpret_cast<XfsDir2DataEntry*>(block + found_offset);
    dep->inumber = __be64::from_cpu(ino);
    dep->namelen = static_cast<uint8_t>(namelen);
    __builtin_memcpy(dep->name, name, namelen);

    // ftype + tag
    size_t tag_off = 8 + 1 + namelen;  // inumber(8) + namelen(1) + name
    if (has_ftype_flag) {
        dep->name[namelen] = ftype;
        tag_off++;
    }
    // Tag (starting offset within the block, stored as __be16)
    auto tag_val = static_cast<uint16_t>(found_offset);
    // Pad to 8-byte alignment before writing tag
    // The tag is at the end of the padded entry, at entry_end - 2
    size_t entry_end = found_offset + need_len;
    auto* tag_loc = reinterpret_cast<__be16*>(block + entry_end - sizeof(__be16));
    *tag_loc = __be16::from_cpu(tag_val);

    // Zero pad the entry between ftype/name end and tag
    size_t used_bytes = tag_off;
    size_t pad_start = found_offset + used_bytes;
    size_t pad_end = entry_end - sizeof(__be16);
    if (pad_end > pad_start) {
        __builtin_memset(block + pad_start, 0, pad_end - pad_start);
    }

    // If there's remaining free space after our entry, create a new unused entry
    if (old_free_len > need_len) {
        size_t remaining = old_free_len - need_len;
        auto* new_unused = reinterpret_cast<XfsDir2DataUnused*>(block + entry_end);
        new_unused->freetag = __be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
        new_unused->length = __be16::from_cpu(static_cast<uint16_t>(remaining));
        // Tag for unused entry: at the very end
        auto* unused_tag = reinterpret_cast<__be16*>(block + entry_end + remaining - sizeof(__be16));
        *unused_tag = __be16::from_cpu(static_cast<uint16_t>(entry_end));
    }

    // Add a leaf entry for the new name
    xfs_dahash_t hash = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);

    // Compute the dataptr for this entry
    // dataptr = (byte_offset_in_dir_block) >> XFS_DIR2_DATA_ALIGN_LOG
    auto dataptr = static_cast<uint32_t>(found_offset >> XFS_DIR2_DATA_ALIGN_LOG);

    // We need to insert a new leaf entry into the sorted leaf array.
    // First, check if we can reclaim a stale entry.
    auto* blp = reinterpret_cast<XfsDir2LeafEntry*>(leaf_start);
    uint32_t stale_count = btp->stale.to_cpu();

    if (stale_count > 0) {
        // Find a stale entry to reuse — find one nearest the correct sorted position
        int insert_pos = 0;
        for (int i = 0; i < static_cast<int>(leaf_count); i++) {
            if (blp[i].address.to_cpu() != XFS_DIR2_NULL_DATAPTR && blp[i].hashval.to_cpu() <= hash) {
                insert_pos = i + 1;
            }
        }

        // Find nearest stale entry
        int stale_idx = -1;
        int best_dist = static_cast<int>(leaf_count);
        for (int i = 0; i < static_cast<int>(leaf_count); i++) {
            if (blp[i].address.to_cpu() == XFS_DIR2_NULL_DATAPTR) {
                int dist = (i >= insert_pos) ? (i - insert_pos) : (insert_pos - i);
                if (dist < best_dist) {
                    best_dist = dist;
                    stale_idx = i;
                }
            }
        }

        if (stale_idx >= 0) {
            // If the stale entry is before our insert position, shift entries down
            if (stale_idx < insert_pos) {
                insert_pos--;
                for (int i = stale_idx; i < insert_pos; i++) {
                    blp[i] = blp[i + 1];
                }
            } else if (stale_idx > insert_pos) {
                // Shift entries up
                for (int i = stale_idx; i > insert_pos; i--) {
                    blp[i] = blp[i - 1];
                }
            }
            blp[insert_pos].hashval = __be32::from_cpu(hash);
            blp[insert_pos].address = __be32::from_cpu(dataptr);
            btp->stale = __be32::from_cpu(stale_count - 1);
        }
    } else {
        // No stale entries — grow the leaf area by shifting it down one slot.
        // This consumes sizeof(XfsDir2LeafEntry) bytes from the free space.
        size_t new_leaf_bytes = (static_cast<size_t>(leaf_count) + 1) * sizeof(XfsDir2LeafEntry);
        size_t new_leaf_start = blksize - sizeof(XfsDir2BlockTail) - new_leaf_bytes;

        // Verify there's enough free space between data entries and the new leaf area
        if (found_offset + need_len > new_leaf_start) {
            // Not enough room for both the data entry and the expanded leaf array
            brelse(bh);
            return -ENOSPC;
        }

        // Move the existing leaf array down by one entry
        auto* new_blp = reinterpret_cast<XfsDir2LeafEntry*>(block + new_leaf_start);
        __builtin_memmove(new_blp, blp, static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry));
        blp = new_blp;

        // Find sorted insertion position
        int insert_pos = 0;
        for (int i = 0; i < static_cast<int>(leaf_count); i++) {
            if (blp[i].hashval.to_cpu() <= hash) {
                insert_pos = i + 1;
            }
        }

        // Shift entries after insert_pos to make room
        for (int i = static_cast<int>(leaf_count); i > insert_pos; i--) {
            blp[i] = blp[i - 1];
        }

        blp[insert_pos].hashval = __be32::from_cpu(hash);
        blp[insert_pos].address = __be32::from_cpu(dataptr);
        leaf_count++;
        btp->count = __be32::from_cpu(leaf_count);
    }

    // Update best_free in the header to reflect the remaining free space.
    // After inserting the data entry (consuming need_len bytes from the old
    // free region), there may be a leftover free region.
    auto* mutable_hdr = reinterpret_cast<XfsDir3DataHdr*>(block);
    {
        size_t entry_end = found_offset + need_len;
        // Compute where the leaf area now starts
        size_t current_leaf_start = blksize - sizeof(XfsDir2BlockTail) - (static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry));
        // The remaining free space is between entry_end and current_leaf_start
        // (only if a free-space marker was written there earlier, check it)
        mutable_hdr->best_free[0].offset = __be16{0};
        mutable_hdr->best_free[0].length = __be16{0};
        mutable_hdr->best_free[1].offset = __be16{0};
        mutable_hdr->best_free[1].length = __be16{0};
        mutable_hdr->best_free[2].offset = __be16{0};
        mutable_hdr->best_free[2].length = __be16{0};
        // Check the remainder area for a free-tag marker
        if (entry_end < current_leaf_start) {
            const auto* rem = reinterpret_cast<const XfsDir2DataUnused*>(block + entry_end);
            if (rem->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
                uint16_t rem_len = rem->length.to_cpu();
                // The free region may need its length truncated if the leaf
                // array grew into it (leaf area shifted down).
                size_t max_free = current_leaf_start - entry_end;
                if (rem_len > max_free) {
                    // Adjust free region length and re-write its tail tag
                    auto* adj_unused = reinterpret_cast<XfsDir2DataUnused*>(block + entry_end);
                    adj_unused->length = __be16::from_cpu(static_cast<uint16_t>(max_free));
                    auto* adj_tag = reinterpret_cast<__be16*>(block + entry_end + max_free - sizeof(__be16));
                    *adj_tag = __be16::from_cpu(static_cast<uint16_t>(entry_end));
                    rem_len = static_cast<uint16_t>(max_free);
                }
                mutable_hdr->best_free[0].offset = __be16::from_cpu(static_cast<uint16_t>(entry_end));
                mutable_hdr->best_free[0].length = __be16::from_cpu(rem_len);
            }
        }
    }

    // Recompute CRC over the entire block
    mutable_hdr->hdr.crc = __be32{0};
    uint32_t crc = util::crc32c_block_with_cksum(block, blksize, 4);
    mutable_hdr->hdr.crc = __be32::from_cpu(crc);

    xfs_trans_log_buf_full(tp, bh);
    brelse(bh);

    // Update directory inode metadata
    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);

    return 0;
}

// ============================================================================
// Directory remove-name — remove an entry from a directory
// ============================================================================

// Remove a name from a shortform directory (inline in inode data fork).
// Finds the entry and removes it, compacting the remaining entries.
auto dir2_sf_removename(XfsInode* dp, const char* name, uint16_t namelen, XfsTransaction* tp) -> int {
    XfsMountContext* ctx = dp->mount;
    bool has_ftype = xfs_has_ftype(ctx);

    const uint8_t* old_data = dp->data_fork.local.data;
    size_t old_size = dp->data_fork.local.size;

    if (old_data == nullptr || old_size < sizeof(XfsDir2SfHdr)) {
        return -EINVAL;
    }

    const auto* old_hdr = reinterpret_cast<const XfsDir2SfHdr*>(old_data);
    size_t ino_size = xfs_dir2_sf_inumber_size(old_hdr);
    size_t hdr_size = xfs_dir2_sf_hdr_size(old_hdr);

    // Scan to find the entry to remove
    const uint8_t* ptr = old_data + hdr_size;
    uint8_t count = old_hdr->count;
    size_t entry_offset_start = 0;
    size_t entry_size = 0;
    bool found = false;

    for (uint8_t i = 0; i < count; i++) {
        if (ptr >= old_data + old_size) {
            break;
        }

        entry_offset_start = ptr - old_data;
        const auto* sfep = reinterpret_cast<const XfsDir2SfEntry*>(ptr);
        uint8_t entry_namelen = sfep->namelen;

        if (entry_namelen == namelen && __builtin_memcmp(sfep->name, name, namelen) == 0) {
            found = true;
            entry_size = sizeof(uint8_t) + 2 + entry_namelen + (has_ftype ? 1 : 0) + ino_size;
            break;
        }

        entry_size = sizeof(uint8_t) + 2 + entry_namelen + (has_ftype ? 1 : 0) + ino_size;
        ptr += entry_size;
    }

    if (!found) {
        return -ENOENT;
    }

    // Build new data fork without this entry
    size_t new_size = old_size - entry_size;
    auto* new_data = new uint8_t[new_size];
    if (new_data == nullptr) {
        return -ENOMEM;
    }

    // Copy header
    __builtin_memcpy(new_data, old_data, hdr_size);

    // Update header: decrement count
    auto* new_hdr = reinterpret_cast<XfsDir2SfHdr*>(new_data);
    new_hdr->count--;

    // Copy entries before the removed entry
    if (entry_offset_start > hdr_size) {
        __builtin_memcpy(new_data + hdr_size, old_data + hdr_size, entry_offset_start - hdr_size);
    }

    // Copy entries after the removed entry
    size_t after_offset = entry_offset_start + entry_size;
    if (after_offset < old_size) {
        __builtin_memcpy(new_data + entry_offset_start, old_data + after_offset, old_size - after_offset);
    }

    // Replace the data fork
    delete[] dp->data_fork.local.data;
    dp->data_fork.local.data = new_data;
    dp->data_fork.local.size = new_size;
    dp->size = new_size;
    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);

    return 0;
}

// Remove a name from a block-format directory (single directory block).
auto dir2_block_removename(XfsInode* dp, const char* name, uint16_t namelen, XfsTransaction* tp) -> int {
    XfsMountContext* ctx = dp->mount;

    // Read the single directory block
    BufHead* bh = nullptr;
    int rc = dir2_read_block(dp, 0, &bh);
    if (rc != 0) return rc;

    uint8_t* block = bh->data;
    size_t blksize = ctx->dir_blk_size;

    // Validate magic
    auto* hdr = reinterpret_cast<XfsDir3DataHdr*>(block);
    uint32_t magic = hdr->hdr.magic.to_cpu();
    if (magic != XFS_DIR3_BLOCK_MAGIC) {
        brelse(bh);
        return -EINVAL;
    }

    // Block tail is at the very end of the block
    auto* btp = reinterpret_cast<XfsDir2BlockTail*>(block + blksize - sizeof(XfsDir2BlockTail));
    uint32_t leaf_count = btp->count.to_cpu();

    size_t leaf_bytes = static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry);
    if (leaf_bytes > blksize - sizeof(XfsDir3DataHdr) - sizeof(XfsDir2BlockTail)) {
        brelse(bh);
        return -EINVAL;
    }

    size_t data_start = sizeof(XfsDir3DataHdr);
    size_t data_end = blksize - sizeof(XfsDir2BlockTail) - leaf_bytes;
    if (data_end <= data_start) {
        brelse(bh);
        return -EINVAL;
    }

    // Leaf entries are just before the tail
    auto* blp = reinterpret_cast<XfsDir2LeafEntry*>(block + data_end);

    // Hash the name and find matching leaf entry
    xfs_dahash_t hash = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);

    int lo = 0;
    int hi = static_cast<int>(leaf_count) - 1;
    int mid = -1;
    int match_idx = -1;
    uint32_t entry_off = 0;
    size_t entry_size = 0;

    // Binary search for hash
    while (lo <= hi) {
        mid = (lo + hi) / 2;
        uint32_t entry_hash = blp[mid].hashval.to_cpu();

        if (hash < entry_hash) {
            hi = mid - 1;
        } else if (hash > entry_hash) {
            lo = mid + 1;
        } else {
            // Back up to first with this hash
            while (mid > 0 && blp[mid - 1].hashval.to_cpu() == hash) {
                mid--;
            }
            break;
        }
    }

    // Scan all entries with matching hash to find the name
    if (lo <= hi || (mid >= 0 && mid < static_cast<int>(leaf_count) && blp[mid].hashval.to_cpu() == hash)) {
        int start_idx = (mid >= 0) ? mid : lo;
        for (int i = start_idx; i < static_cast<int>(leaf_count); i++) {
            if (blp[i].hashval.to_cpu() != hash) {
                break;
            }

            xfs_dir2_dataptr_t addr = blp[i].address.to_cpu();
            if (addr == XFS_DIR2_NULL_DATAPTR) {
                continue;  // stale
            }

            uint32_t off = dir2_dataptr_to_off(ctx, addr);
            if (off < data_start || off + sizeof(XfsDir2DataEntry) > data_end) {
                continue;
            }

            const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + off);
            size_t dep_size = dir2_data_entsize(ctx, dep->namelen);
            if (dep->namelen == 0 || dep_size == 0 || off + dep_size > data_end) {
                continue;
            }

            if (dep->namelen == namelen && __builtin_memcmp(dep->name, name, namelen) == 0) {
                match_idx = i;
                entry_off = off;
                entry_size = dep_size;
                break;
            }
        }
    }

    if (match_idx < 0) {
        brelse(bh);
        return -ENOENT;
    }

    // Mark the leaf entry as stale (address = XFS_DIR2_NULL_DATAPTR)
    blp[match_idx].address = __be32::from_cpu(XFS_DIR2_NULL_DATAPTR);
    uint32_t stale_count = btp->stale.to_cpu();
    btp->stale = __be32::from_cpu(stale_count + 1);

    // Convert the data entry to free space
    auto* unused = reinterpret_cast<XfsDir2DataUnused*>(block + entry_off);
    unused->freetag = __be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
    unused->length = __be16::from_cpu(static_cast<uint16_t>(entry_size));

    // Write tag at end of free space
    auto* tag = reinterpret_cast<__be16*>(block + entry_off + entry_size - sizeof(__be16));
    *tag = __be16::from_cpu(static_cast<uint16_t>(entry_off));

    // Find immediate neighboring free regions by safely walking the data area.
    size_t prev_free_off = 0;
    size_t prev_free_len = 0;
    bool prev_found = false;
    size_t next_free_len = 0;
    bool next_found = false;

    {
        size_t off = data_start;
        while (off < data_end) {
            if (off + sizeof(XfsDir2DataUnused) > data_end) {
                break;
            }

            const auto* ent = reinterpret_cast<const XfsDir2DataUnused*>(block + off);
            if (ent->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
                uint16_t free_len = ent->length.to_cpu();
                if (free_len == 0 || off + free_len > data_end) {
                    break;
                }

                if (off + free_len == entry_off) {
                    prev_free_off = off;
                    prev_free_len = free_len;
                    prev_found = true;
                } else if (off == entry_off + entry_size) {
                    next_free_len = free_len;
                    next_found = true;
                }

                off += free_len;
                continue;
            }

            const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + off);
            size_t dep_size = dir2_data_entsize(ctx, dep->namelen);
            if (dep->namelen == 0 || dep_size == 0 || off + dep_size > data_end) {
                break;
            }
            off += dep_size;
        }
    }

    // Coalesce adjacent free regions into one contiguous record.
    size_t merged_off = entry_off;
    size_t merged_len = entry_size;
    if (prev_found) {
        merged_off = prev_free_off;
        merged_len += prev_free_len;
    }
    if (next_found) {
        merged_len += next_free_len;
    }

    auto* merged = reinterpret_cast<XfsDir2DataUnused*>(block + merged_off);
    merged->freetag = __be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
    merged->length = __be16::from_cpu(static_cast<uint16_t>(merged_len));
    auto* merged_tag = reinterpret_cast<__be16*>(block + merged_off + merged_len - sizeof(__be16));
    *merged_tag = __be16::from_cpu(static_cast<uint16_t>(merged_off));

    // Rebuild top-3 best free regions from a validated linear walk.
    struct BestFreeSlot {
        uint16_t off;
        uint16_t len;
    };
    std::array<BestFreeSlot, 3> best{{{0, 0}, {0, 0}, {0, 0}}};

    {
        size_t off = data_start;
        while (off < data_end) {
            if (off + sizeof(XfsDir2DataUnused) > data_end) {
                break;
            }

            const auto* ent = reinterpret_cast<const XfsDir2DataUnused*>(block + off);
            if (ent->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
                uint16_t free_len = ent->length.to_cpu();
                if (free_len == 0 || off + free_len > data_end) {
                    break;
                }

                BestFreeSlot cur{static_cast<uint16_t>(off), free_len};
                for (int idx = 0; idx < 3; idx++) {
                    if (cur.len > best[idx].len) {
                        for (int j = 2; j > idx; j--) {
                            best[j] = best[j - 1];
                        }
                        best[idx] = cur;
                        break;
                    }
                }

                off += free_len;
                continue;
            }

            const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + off);
            size_t dep_size = dir2_data_entsize(ctx, dep->namelen);
            if (dep->namelen == 0 || dep_size == 0 || off + dep_size > data_end) {
                break;
            }
            off += dep_size;
        }
    }

    for (int i = 0; i < 3; i++) {
        hdr->best_free[i].offset = __be16::from_cpu(best[i].off);
        hdr->best_free[i].length = __be16::from_cpu(best[i].len);
    }

    // Update CRC over the block
    hdr->hdr.crc = __be32{0};
    uint32_t crc = util::crc32c_block_with_cksum(block, blksize, 4);
    hdr->hdr.crc = __be32::from_cpu(crc);

    xfs_trans_log_buf_full(tp, bh);
    brelse(bh);

    return 0;
}

}  // anonymous namespace

auto xfs_dir_addname(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype, XfsTransaction* tp) -> int {
    if (dp == nullptr || name == nullptr || namelen == 0 || tp == nullptr) {
        return -EINVAL;
    }
    if (!xfs_inode_isdir(dp)) {
        return -ENOTDIR;
    }

    // Check that the name doesn't already exist
    XfsDirEntry existing{};
    int rc = xfs_dir_lookup(dp, name, namelen, &existing);
    if (rc == 0) {
        return -EEXIST;
    }

    switch (dp->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL: {
            rc = dir2_sf_addname(dp, name, namelen, ino, ftype, tp);
            if (rc == -E2BIG) {
// Shortform is full — convert to block format, then add there.
#ifdef XFS_DEBUG
                mod::dbg::log("[xfs] dir_addname: shortform dir full, converting to block format\n");
#endif
                rc = dir2_sf_to_block(dp, tp);
                if (rc != 0) {
#ifdef XFS_DEBUG
                    mod::dbg::log("[xfs] dir_addname: sf->block conversion failed: %d\n", rc);
#endif
                    return rc;
                }
                // Now it's a block-format directory; add the new entry there.
                return dir2_block_addname(dp, name, namelen, ino, ftype, tp);
            }
            return rc;
        }

        case XFS_DINODE_FMT_EXTENTS:
        case XFS_DINODE_FMT_BTREE: {
            if (dp->size <= dp->mount->dir_blk_size) {
                return dir2_block_addname(dp, name, namelen, ino, ftype, tp);
            }
            // Leaf/node format add not yet implemented
            mod::dbg::log("[xfs] dir_addname: leaf/node dir add not yet supported\n");
            return -ENOSPC;
        }

        default:
            return -EINVAL;
    }
}

// Remove a name from a directory.  Dispatches to the appropriate format handler.
auto xfs_dir_removename(XfsInode* dp, const char* name, uint16_t namelen, XfsTransaction* tp) -> int {
    if (dp == nullptr || name == nullptr || namelen == 0 || tp == nullptr) {
        return -EINVAL;
    }
    if (!xfs_inode_isdir(dp)) {
        return -ENOTDIR;
    }

    switch (dp->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL: {
            return dir2_sf_removename(dp, name, namelen, tp);
        }

        case XFS_DINODE_FMT_EXTENTS:
        case XFS_DINODE_FMT_BTREE: {
            if (dp->size <= dp->mount->dir_blk_size) {
                return dir2_block_removename(dp, name, namelen, tp);
            }
            // Leaf/node format remove not yet implemented
            mod::dbg::log("[xfs] dir_removename: leaf/node dir remove not yet supported\n");
            return -ENOSYS;
        }

        default:
            return -EINVAL;
    }
}

}  // namespace ker::vfs::xfs
