// XFS Directory operations implementation.
//
// Handles all four directory formats: shortform, block, leaf, and node.
// Lookup/iterate support all formats.  Mutating operations support shortform,
// block-format, and existing-space leaf/node updates.
//
// Reference: reference/xfs/libxfs/xfs_dir2_sf.c, xfs_dir2_block.c,
//            reference/xfs/libxfs/xfs_dir2_leaf.c, xfs_dir2_node.c,
//            reference/xfs/libxfs/xfs_da_btree.c

#include "xfs_dir2.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/crc32c.hpp>
#include <utility>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_alloc.hpp>
#include <vfs/fs/xfs/xfs_bmap.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

#include "net/endian.hpp"
#include "vfs/fs/xfs/xfs_format.hpp"
#include "vfs/fs/xfs/xfs_inode.hpp"
#include "vfs/fs/xfs/xfs_mount.hpp"

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
        return xfs_dir2_data_entry_name(dep)[dep->namelen];
    }
    return XFS_DIR3_FT_UNKNOWN;
}

auto dir2_sf_header_size_if_valid(const uint8_t* data, size_t data_size, size_t* hdr_size) -> bool {
    if (data == nullptr || data_size < 2) {
        return false;
    }

    const auto* hdr = reinterpret_cast<const XfsDir2SfHdr*>(data);
    size_t const HDR_SIZE = xfs_dir2_sf_hdr_size(hdr);
    if (HDR_SIZE > data_size) {
        return false;
    }

    if (hdr_size != nullptr) {
        *hdr_size = HDR_SIZE;
    }
    return true;
}

auto sf_entry_stored_offset(const XfsDir2SfEntry* sfep) -> uint16_t {
    return (static_cast<uint16_t>(sfep->offset.at(0)) << 8U) | static_cast<uint16_t>(sfep->offset.at(1));
}

void sf_entry_store_offset(XfsDir2SfEntry* sfep, uint16_t offset) {
    sfep->offset.at(0) = static_cast<uint8_t>(offset >> 8U);
    sfep->offset.at(1) = static_cast<uint8_t>(offset & 0xffU);
}

auto sf_entry_iteration_cookie(const XfsDir2SfEntry* sfep, uint64_t ordinal, uint64_t last_cookie) -> uint64_t {
    constexpr uint64_t SF_OFFSET_COOKIE_SHIFT = 16;
    uint64_t const STORED_OFFSET = sf_entry_stored_offset(sfep);
    uint64_t candidate = XFS_READDIR_COOKIE_BASE + (STORED_OFFSET << SF_OFFSET_COOKIE_SHIFT) + ordinal;
    if (candidate <= last_cookie) {
        candidate = last_cookie + 1;
    }
    return candidate;
}

auto sf_entry_size(const XfsDir2SfHdr* hdr, const XfsDir2SfEntry* sfep, bool has_ftype) -> size_t {
    return sizeof(uint8_t) + 2 + sfep->namelen + (has_ftype ? 1 : 0) + xfs_dir2_sf_inumber_size(hdr);
}

auto sf_repair_offset_tags(uint8_t* data, size_t data_size, const XfsMountContext* ctx) -> bool {
    size_t hdr_size = 0;
    if (data == nullptr || ctx == nullptr || !dir2_sf_header_size_if_valid(data, data_size, &hdr_size)) {
        return false;
    }

    auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data);
    bool const HAS_FTYPE = xfs_has_ftype(ctx);
    uint8_t* ptr = data + hdr_size;
    uint16_t previous = 0;
    bool changed = false;

    for (uint8_t i = 0; i < hdr->count; i++) {
        if (ptr >= data + data_size) {
            break;
        }

        auto* sfep = reinterpret_cast<XfsDir2SfEntry*>(ptr);
        size_t const ENTRY_SIZE = sf_entry_size(hdr, sfep, HAS_FTYPE);
        if (ENTRY_SIZE == 0 || ptr + ENTRY_SIZE > data + data_size) {
            break;
        }

        uint16_t stored = sf_entry_stored_offset(sfep);
        if (stored <= previous) {
            if (previous == UINT16_MAX) {
                break;
            }
            stored = static_cast<uint16_t>(previous + 1U);
            sf_entry_store_offset(sfep, stored);
            changed = true;
        }
        previous = stored;
        ptr += ENTRY_SIZE;
    }

    return changed;
}

auto sf_next_offset_tag(const uint8_t* data, size_t data_size, const XfsMountContext* ctx) -> uint16_t {
    size_t hdr_size = 0;
    if (data == nullptr || ctx == nullptr || !dir2_sf_header_size_if_valid(data, data_size, &hdr_size)) {
        return 1;
    }

    const auto* hdr = reinterpret_cast<const XfsDir2SfHdr*>(data);
    bool const HAS_FTYPE = xfs_has_ftype(ctx);
    const uint8_t* ptr = data + hdr_size;
    uint16_t max_offset = 0;

    for (uint8_t i = 0; i < hdr->count; i++) {
        if (ptr >= data + data_size) {
            break;
        }

        const auto* sfep = reinterpret_cast<const XfsDir2SfEntry*>(ptr);
        size_t const ENTRY_SIZE = sf_entry_size(hdr, sfep, HAS_FTYPE);
        if (ENTRY_SIZE == 0 || ptr + ENTRY_SIZE > data + data_size) {
            break;
        }

        max_offset = std::max(max_offset, sf_entry_stored_offset(sfep));
        ptr += ENTRY_SIZE;
    }

    return max_offset < UINT16_MAX ? static_cast<uint16_t>(max_offset + 1U) : static_cast<uint16_t>(hdr->count + 1U);
}

auto data_entry_cookie(const XfsMountContext* ctx, xfs_dir2_db_t db, size_t offset) -> uint64_t {
    uint64_t const BYTE_OFF = (static_cast<uint64_t>(db) << (ctx->block_log + ctx->dir_blk_log)) + offset;
    return XFS_READDIR_COOKIE_BASE + (BYTE_OFF >> XFS_DIR2_DATA_ALIGN_LOG);
}

// Fill an XfsDirEntry from a data entry.
void fill_dir_entry(const XfsMountContext* ctx, const XfsDir2DataEntry* dep, XfsDirEntry* entry, uint64_t cookie = 0) {
    entry->ino = dir2_data_entry_ino(dep);
    entry->ftype = dir2_data_entry_ftype(ctx, dep);
    entry->namelen = dep->namelen;
    entry->cookie = cookie;
    __builtin_memcpy(entry->name.data(), xfs_dir2_data_entry_name(dep), dep->namelen);
    entry->name.at(dep->namelen) = '\0';
}

// Large source checkouts and compiler tree scans revisit far more than 4096
// directory names. Keep this fixed-size and bounded while reducing set churn
// in hot XFS path walks.
constexpr size_t XFS_DENTRY_CACHE_SET_COUNT = 4096;
constexpr size_t XFS_DENTRY_CACHE_WAYS = 4;
constexpr size_t XFS_DENTRY_GENERATION_SET_COUNT = 4096;
constexpr size_t XFS_DENTRY_GENERATION_WAYS = 4;
static_assert((XFS_DENTRY_CACHE_SET_COUNT & (XFS_DENTRY_CACHE_SET_COUNT - 1)) == 0);
static_assert((XFS_DENTRY_GENERATION_SET_COUNT & (XFS_DENTRY_GENERATION_SET_COUNT - 1)) == 0);

struct XfsDentryCacheEntry {
    std::array<char, 256> name{};
    XfsDirEntry entry{};
    XfsMountContext* mount{};
    xfs_ino_t parent_ino{};
    uint64_t hash{};
    uint64_t dir_generation{};
    uint64_t last_used{};
    uint16_t namelen{};
    int result{};
    bool valid{};
};

struct XfsDentryCacheSet {
    std::array<XfsDentryCacheEntry, XFS_DENTRY_CACHE_WAYS> ways{};
};

struct XfsDentryGenerationEntry {
    XfsMountContext* mount{};
    xfs_ino_t parent_ino{};
    uint64_t hash{};
    uint64_t generation{};
    uint64_t last_used{};
    bool valid{};
};

struct XfsDentryGenerationSet {
    std::array<XfsDentryGenerationEntry, XFS_DENTRY_GENERATION_WAYS> ways{};
};

std::array<XfsDentryCacheSet, XFS_DENTRY_CACHE_SET_COUNT> g_xfs_dentry_cache{};
std::array<XfsDentryGenerationSet, XFS_DENTRY_GENERATION_SET_COUNT> g_xfs_dentry_generations{};
ker::mod::sys::Spinlock g_xfs_dentry_cache_lock;
std::atomic<uint64_t> g_xfs_dentry_cache_clock{0};
std::atomic<uint64_t> g_xfs_dentry_generation_clock{0};
std::atomic<uint64_t> g_xfs_dentry_next_generation{1};
std::atomic<uint64_t> g_xfs_dentry_hits{0};
std::atomic<uint64_t> g_xfs_dentry_misses{0};
std::atomic<uint64_t> g_xfs_dentry_stores{0};
std::atomic<uint64_t> g_xfs_dentry_invalidations{0};

auto xfs_dentry_hash_dir(XfsMountContext* mount, xfs_ino_t parent_ino) -> uint64_t {
    uint64_t hash = 1469598103934665603ULL;
    auto const MOUNT_VALUE = reinterpret_cast<uintptr_t>(mount);
    for (int shift = 0; shift < 64; shift += 8) {
        hash ^= static_cast<uint8_t>((MOUNT_VALUE >> shift) & 0xffU);
        hash *= 1099511628211ULL;
    }
    for (int shift = 0; shift < 64; shift += 8) {
        hash ^= static_cast<uint8_t>((parent_ino >> shift) & 0xffU);
        hash *= 1099511628211ULL;
    }
    return hash == 0 ? 1 : hash;
}

auto xfs_dentry_hash_name(XfsMountContext* mount, xfs_ino_t parent_ino, const char* name, uint16_t namelen) -> uint64_t {
    uint64_t hash = xfs_dentry_hash_dir(mount, parent_ino);
    for (uint16_t i = 0; i < namelen; ++i) {
        hash ^= static_cast<unsigned char>(name[i]);
        hash *= 1099511628211ULL;
    }
    hash ^= namelen;
    hash *= 1099511628211ULL;
    return hash == 0 ? 1 : hash;
}

auto xfs_dentry_next_generation_value() -> uint64_t { return g_xfs_dentry_next_generation.fetch_add(1, std::memory_order_relaxed) + 1; }

auto xfs_dentry_cache_dir_generation_locked(XfsMountContext* mount, xfs_ino_t parent_ino) -> uint64_t {
    uint64_t const HASH = xfs_dentry_hash_dir(mount, parent_ino);
    uint64_t const USE_STAMP = g_xfs_dentry_generation_clock.fetch_add(1, std::memory_order_relaxed) + 1;
    auto& set = g_xfs_dentry_generations.at(HASH & (XFS_DENTRY_GENERATION_SET_COUNT - 1));

    XfsDentryGenerationEntry* victim = &set.ways.front();
    for (auto& candidate : set.ways) {
        if (candidate.valid && candidate.hash == HASH && candidate.mount == mount && candidate.parent_ino == parent_ino) {
            candidate.last_used = USE_STAMP;
            return candidate.generation;
        }
        if (!candidate.valid) {
            victim = &candidate;
            break;
        }
        if (candidate.last_used < victim->last_used) {
            victim = &candidate;
        }
    }

    uint64_t const GENERATION = xfs_dentry_next_generation_value();
    victim->mount = mount;
    victim->parent_ino = parent_ino;
    victim->hash = HASH;
    victim->generation = GENERATION;
    victim->last_used = USE_STAMP;
    victim->valid = true;
    return GENERATION;
}

auto xfs_dentry_cache_lookup(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry, int* result) -> bool {
    if (dp == nullptr || dp->mount == nullptr || name == nullptr || entry == nullptr || result == nullptr || namelen >= 256) {
        return false;
    }

    uint64_t const HASH = xfs_dentry_hash_name(dp->mount, dp->ino, name, namelen);
    auto& set = g_xfs_dentry_cache.at(HASH & (XFS_DENTRY_CACHE_SET_COUNT - 1));

    uint64_t const IRQF = g_xfs_dentry_cache_lock.lock_irqsave();
    uint64_t const DIR_GENERATION = xfs_dentry_cache_dir_generation_locked(dp->mount, dp->ino);
    for (auto& candidate : set.ways) {
        if (!candidate.valid || candidate.hash != HASH || candidate.mount != dp->mount || candidate.parent_ino != dp->ino ||
            candidate.namelen != namelen || candidate.dir_generation != DIR_GENERATION) {
            continue;
        }
        if (std::memcmp(candidate.name.data(), name, namelen) != 0) {
            continue;
        }

        candidate.last_used = g_xfs_dentry_cache_clock.fetch_add(1, std::memory_order_relaxed) + 1;
        *result = candidate.result;
        if (candidate.result == 0) {
            *entry = candidate.entry;
        }
        g_xfs_dentry_cache_lock.unlock_irqrestore(IRQF);
        g_xfs_dentry_hits.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    g_xfs_dentry_cache_lock.unlock_irqrestore(IRQF);
    g_xfs_dentry_misses.fetch_add(1, std::memory_order_relaxed);
    return false;
}

void xfs_dentry_cache_store(XfsInode* dp, const char* name, uint16_t namelen, int result, const XfsDirEntry* entry) {
    if (dp == nullptr || dp->mount == nullptr || name == nullptr || namelen >= 256 || (result != 0 && result != -ENOENT)) {
        return;
    }
    if (result == 0 && entry == nullptr) {
        return;
    }

    uint64_t const HASH = xfs_dentry_hash_name(dp->mount, dp->ino, name, namelen);
    uint64_t const USE_STAMP = g_xfs_dentry_cache_clock.fetch_add(1, std::memory_order_relaxed) + 1;
    auto& set = g_xfs_dentry_cache.at(HASH & (XFS_DENTRY_CACHE_SET_COUNT - 1));

    uint64_t const IRQF = g_xfs_dentry_cache_lock.lock_irqsave();
    uint64_t const DIR_GENERATION = xfs_dentry_cache_dir_generation_locked(dp->mount, dp->ino);
    XfsDentryCacheEntry* victim = &set.ways.front();
    for (auto& candidate : set.ways) {
        if (candidate.valid && candidate.hash == HASH && candidate.mount == dp->mount && candidate.parent_ino == dp->ino &&
            candidate.namelen == namelen && candidate.dir_generation == DIR_GENERATION &&
            std::memcmp(candidate.name.data(), name, namelen) == 0) {
            victim = &candidate;
            break;
        }
        if (!candidate.valid ||
            (candidate.mount == dp->mount && candidate.parent_ino == dp->ino && candidate.dir_generation != DIR_GENERATION)) {
            victim = &candidate;
            break;
        }
        if (candidate.last_used < victim->last_used) {
            victim = &candidate;
        }
    }

    victim->name.fill('\0');
    std::memcpy(victim->name.data(), name, namelen);
    victim->mount = dp->mount;
    victim->parent_ino = dp->ino;
    victim->hash = HASH;
    victim->dir_generation = DIR_GENERATION;
    victim->last_used = USE_STAMP;
    victim->namelen = namelen;
    victim->result = result;
    victim->entry = (result == 0) ? *entry : XfsDirEntry{};
    victim->valid = true;
    g_xfs_dentry_cache_lock.unlock_irqrestore(IRQF);
    g_xfs_dentry_stores.fetch_add(1, std::memory_order_relaxed);
}

void xfs_dentry_cache_store_added_name(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype) {
    XfsDirEntry entry{};
    entry.ino = ino;
    entry.ftype = ftype;
    entry.namelen = namelen;
    entry.cookie = 0;
    std::memcpy(entry.name.data(), name, namelen);
    entry.name.at(namelen) = '\0';
    xfs_dentry_cache_store(dp, name, namelen, 0, &entry);
}

void xfs_dentry_cache_invalidate_dir(XfsInode* dp) {
    if (dp == nullptr || dp->mount == nullptr) {
        return;
    }

    uint64_t const HASH = xfs_dentry_hash_dir(dp->mount, dp->ino);
    uint64_t const USE_STAMP = g_xfs_dentry_generation_clock.fetch_add(1, std::memory_order_relaxed) + 1;
    uint64_t const IRQF = g_xfs_dentry_cache_lock.lock_irqsave();
    auto& set = g_xfs_dentry_generations.at(HASH & (XFS_DENTRY_GENERATION_SET_COUNT - 1));

    XfsDentryGenerationEntry* victim = &set.ways.front();
    for (auto& candidate : set.ways) {
        if (candidate.valid && candidate.hash == HASH && candidate.mount == dp->mount && candidate.parent_ino == dp->ino) {
            candidate.generation = xfs_dentry_next_generation_value();
            candidate.last_used = USE_STAMP;
            g_xfs_dentry_cache_lock.unlock_irqrestore(IRQF);
            g_xfs_dentry_invalidations.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        if (!candidate.valid) {
            victim = &candidate;
            break;
        }
        if (candidate.last_used < victim->last_used) {
            victim = &candidate;
        }
    }

    victim->mount = dp->mount;
    victim->parent_ino = dp->ino;
    victim->hash = HASH;
    victim->generation = xfs_dentry_next_generation_value();
    victim->last_used = USE_STAMP;
    victim->valid = true;
    g_xfs_dentry_cache_lock.unlock_irqrestore(IRQF);
    g_xfs_dentry_invalidations.fetch_add(1, std::memory_order_relaxed);
}

// Get directory block number from dataptr
auto dir2_dataptr_to_db(const XfsMountContext* ctx, xfs_dir2_dataptr_t dp) -> xfs_dir2_db_t {
    uint64_t const BYTE_OFF = static_cast<uint64_t>(dp) << XFS_DIR2_DATA_ALIGN_LOG;
    return static_cast<xfs_dir2_db_t>(BYTE_OFF >> (ctx->block_log + ctx->dir_blk_log));
}

// Get byte offset within directory block from dataptr
auto dir2_dataptr_to_off(const XfsMountContext* ctx, xfs_dir2_dataptr_t dp) -> xfs_dir2_data_off_t {
    uint64_t const BYTE_OFF = static_cast<uint64_t>(dp) << XFS_DIR2_DATA_ALIGN_LOG;
    return static_cast<xfs_dir2_data_off_t>(BYTE_OFF & (ctx->dir_blk_size - 1));
}

// Convert directory block number to file offset (in filesystem blocks)
auto dir2_db_to_fsbno(const XfsMountContext* ctx, xfs_dir2_db_t db) -> xfs_fileoff_t {
    return static_cast<xfs_fileoff_t>(db) << ctx->dir_blk_log;
}

// Read a directory block (may span multiple fs blocks if dir_blk_log > 0)
auto dir2_read_block(XfsInode* dp, xfs_dir2_db_t db, BufHead** bhp) -> int {
    XfsMountContext* ctx = dp->mount;
    xfs_fileoff_t const FILE_BLOCK = dir2_db_to_fsbno(ctx, db);

    XfsBmapResult bmap{};
    int const RC = xfs_bmap_lookup(dp, FILE_BLOCK, &bmap);
    if (RC != 0) {
#ifdef XFS_DEBUG
        mod::dbg::log("[xfs] dir2_read_block: bmap_lookup failed ino=%lu db=%u rc=%d", static_cast<unsigned long>(dp->ino), db, RC);
#endif
        return RC;
    }
    if (bmap.is_hole) {
#ifdef XFS_DEBUG
        mod::dbg::log("[xfs] dir2_read_block: HOLE ino=%lu db=%u fmt=%d ext_count=%u", static_cast<unsigned long>(dp->ino), db,
                      dp->data_fork.format, dp->data_fork.extents.count);
#endif
        return -EINVAL;
    }
#ifdef XFS_DEBUG
    mod::dbg::log("[xfs] dir2_read_block: ino=%lu db=%u blk=%lu", static_cast<unsigned long>(dp->ino), db,
                  static_cast<unsigned long>(bmap.startblock));
#endif

    uint32_t const FBS = 1U << ctx->dir_blk_log;  // fs blocks per dir block
    if (FBS == 1) {
        *bhp = xfs_buf_read(ctx, bmap.startblock);
    } else {
        *bhp = xfs_buf_read_multi(ctx, bmap.startblock, FBS);
    }

    return (*bhp != nullptr) ? 0 : -EIO;
}

auto dir2_is_single_block_dir(XfsInode* dp) -> bool {
    if (dp == nullptr || dp->mount == nullptr) {
        return false;
    }

    BufHead* bh = nullptr;
    int const RC = dir2_read_block(dp, 0, &bh);
    if (RC != 0 || bh == nullptr) {
        return dp->size <= dp->mount->dir_blk_size;
    }

    const auto* hdr = reinterpret_cast<const XfsDir3DataHdr*>(bh->data);
    uint32_t const MAGIC = hdr->hdr.magic.to_cpu();
    brelse(bh);

    if (MAGIC == XFS_DIR3_BLOCK_MAGIC) {
        return true;
    }
    if (MAGIC == XFS_DIR3_DATA_MAGIC) {
        return false;
    }

    mod::dbg::logger<"xfs">::error("dir format detect: unexpected magic 0x%x", MAGIC);
    return dp->size <= dp->mount->dir_blk_size;
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
    size_t const DATA_SIZE = dp->data_fork.local.size;
    size_t hdr_size = 0;
    if (!dir2_sf_header_size_if_valid(data, DATA_SIZE, &hdr_size)) {
        return -EINVAL;
    }

    const auto* hdr = reinterpret_cast<const XfsDir2SfHdr*>(data);
    XfsMountContext const* ctx = dp->mount;

    // Check for "."
    if (namelen == 1 && name[0] == '.') {
        entry->ino = dp->ino;
        entry->ftype = XFS_DIR3_FT_DIR;
        entry->namelen = 1;
        entry->name.at(0) = '.';
        entry->name.at(1) = '\0';
        return 0;
    }

    // Check for ".."
    if (namelen == 2 && name[0] == '.' && name[1] == '.') {
        entry->ino = xfs_dir2_sf_get_parent(hdr);
        entry->ftype = XFS_DIR3_FT_DIR;
        entry->namelen = 2;
        entry->name.at(0) = '.';
        entry->name.at(1) = '.';
        entry->name.at(2) = '\0';
        return 0;
    }

    // Linear scan
    bool const HAS_FTYPE = xfs_has_ftype(ctx);
    const uint8_t* ptr = data + hdr_size;
    uint8_t const COUNT = hdr->count;

    for (uint8_t i = 0; i < COUNT; i++) {
        if (ptr >= data + DATA_SIZE) {
            break;
        }

        const auto* sfep = reinterpret_cast<const XfsDir2SfEntry*>(ptr);
        uint8_t const ENTRY_NAMELEN = sfep->namelen;

        // Inode number is at: sfep->name + namelen [+ 1 if ftype]
        const uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + ENTRY_NAMELEN;
        uint8_t ftype = XFS_DIR3_FT_UNKNOWN;
        if (HAS_FTYPE) {
            ftype = *ino_ptr;
            ino_ptr++;
        }

        if (ENTRY_NAMELEN == namelen && __builtin_memcmp(xfs_dir2_sf_entry_name(sfep), name, namelen) == 0) {
            entry->ino = sf_get_ino(hdr, ino_ptr);
            entry->ftype = ftype;
            entry->namelen = namelen;
            __builtin_memcpy(entry->name.data(), name, namelen);
            entry->name.at(namelen) = '\0';
            return 0;
        }

        // Advance to next entry
        size_t const ENTRY_SIZE = sf_entry_size(hdr, sfep, HAS_FTYPE);
        ptr += ENTRY_SIZE;
    }

    return -ENOENT;
}

auto dir2_sf_iterate(XfsInode* dp, XfsDirIterFn fn, void* user_ctx) -> int {
    if (dp->data_fork.format != XFS_DINODE_FMT_LOCAL) {
        return -EINVAL;
    }

    const uint8_t* data = dp->data_fork.local.data;
    size_t const DATA_SIZE = dp->data_fork.local.size;
    size_t hdr_size = 0;
    if (!dir2_sf_header_size_if_valid(data, DATA_SIZE, &hdr_size)) {
        return -EINVAL;
    }

    const auto* hdr = reinterpret_cast<const XfsDir2SfHdr*>(data);
    XfsMountContext const* ctx = dp->mount;

    XfsDirEntry entry{};

    // Emit "."
    entry.ino = dp->ino;
    entry.ftype = XFS_DIR3_FT_DIR;
    entry.namelen = 1;
    entry.cookie = 0;
    entry.name.at(0) = '.';
    entry.name.at(1) = '\0';
    int rc = fn(&entry, user_ctx);
    if (rc != 0) {
        return 0;
    }

    // Emit ".."
    entry.ino = xfs_dir2_sf_get_parent(hdr);
    entry.ftype = XFS_DIR3_FT_DIR;
    entry.namelen = 2;
    entry.cookie = 1;
    entry.name.at(0) = '.';
    entry.name.at(1) = '.';
    entry.name.at(2) = '\0';
    rc = fn(&entry, user_ctx);
    if (rc != 0) {
        return 0;
    }

    // Iterate entries
    bool const HAS_FTYPE = xfs_has_ftype(ctx);
    const uint8_t* ptr = data + hdr_size;
    uint8_t const COUNT = hdr->count;
    uint64_t last_cookie = XFS_READDIR_COOKIE_BASE - 1;
    uint64_t ordinal = 0;

    for (uint8_t i = 0; i < COUNT; i++) {
        if (ptr >= data + DATA_SIZE) {
            break;
        }

        const auto* sfep = reinterpret_cast<const XfsDir2SfEntry*>(ptr);
        uint8_t const ENTRY_NAMELEN = sfep->namelen;

        const uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + ENTRY_NAMELEN;
        uint8_t ftype = XFS_DIR3_FT_UNKNOWN;
        if (HAS_FTYPE) {
            ftype = *ino_ptr;
            ino_ptr++;
        }

        entry.ino = sf_get_ino(hdr, ino_ptr);
        entry.ftype = ftype;
        entry.namelen = ENTRY_NAMELEN;
        // Prefer the stored shortform offset so cookies survive compaction when
        // earlier entries are removed. Repair duplicate/nonmonotonic tags with
        // the current ordinal so one getdents walk still advances strictly.
        entry.cookie = sf_entry_iteration_cookie(sfep, ordinal, last_cookie);
        last_cookie = entry.cookie;
        ordinal++;
        __builtin_memcpy(entry.name.data(), xfs_dir2_sf_entry_name(sfep), ENTRY_NAMELEN);
        entry.name.at(ENTRY_NAMELEN) = '\0';

        rc = fn(&entry, user_ctx);
        if (rc != 0) {
            return 0;
        }

        size_t const ENTRY_SIZE = sf_entry_size(hdr, sfep, HAS_FTYPE);
        ptr += ENTRY_SIZE;
    }

    return 0;
}

// ============================================================================
// Block-format directory operations
// ============================================================================

auto dir2_block_lookup(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry) -> int {
    XfsMountContext const* ctx = dp->mount;

    // Read the single directory block
    BufHead* bh = nullptr;
    int const RC = dir2_read_block(dp, 0, &bh);
    if (RC != 0) {
        return RC;
    }

    const uint8_t* block = bh->data;
    size_t const BLKSIZE = ctx->dir_blk_size;

    // Validate magic
    const auto* hdr = reinterpret_cast<const XfsDir3DataHdr*>(block);
    uint32_t const MAGIC = hdr->hdr.magic.to_cpu();
    if (MAGIC != XFS_DIR3_BLOCK_MAGIC) {
        mod::dbg::logger<"xfs">::error("dir block: bad magic 0x%x", MAGIC);
        brelse(bh);
        return -EINVAL;
    }

    // Block tail is at the very end of the block
    const auto* btp = reinterpret_cast<const XfsDir2BlockTail*>(block + BLKSIZE - sizeof(XfsDir2BlockTail));
    uint32_t const LEAF_COUNT = btp->count.to_cpu();
    (void)btp->stale;  // stale count unused in read-only lookup

    // Leaf entries are just before the tail
    const auto* blp = reinterpret_cast<const XfsDir2LeafEntry*>(reinterpret_cast<const uint8_t*>(btp) -
                                                                (static_cast<size_t>(LEAF_COUNT) * sizeof(XfsDir2LeafEntry)));

    // Hash the name and binary search the leaf array
    xfs_dahash_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);

    int lo = 0;
    int hi = static_cast<int>(LEAF_COUNT) - 1;
    int mid = -1;
    bool found = false;

    while (lo <= hi) {
        mid = (lo + hi) / 2;
        uint32_t const ENTRY_HASH = blp[mid].hashval.to_cpu();

        if (HASH < ENTRY_HASH) {
            hi = mid - 1;
        } else if (HASH > ENTRY_HASH) {
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
    while (mid > 0 && blp[mid - 1].hashval.to_cpu() == HASH) {
        mid--;
    }

    // Scan all entries with matching hash
    for (int i = mid; std::cmp_less(i, LEAF_COUNT); i++) {
        if (blp[i].hashval.to_cpu() != HASH) {
            break;
        }

        xfs_dir2_dataptr_t const ADDR = blp[i].address.to_cpu();
        if (ADDR == XFS_DIR2_NULL_DATAPTR) {
            continue;  // stale
        }

        uint32_t const OFF = dir2_dataptr_to_off(ctx, ADDR);
        if (OFF + sizeof(XfsDir2DataEntry) > BLKSIZE) {
            continue;
        }

        const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + OFF);

        if (dep->namelen == namelen && __builtin_memcmp(xfs_dir2_data_entry_name(dep), name, namelen) == 0) {
            fill_dir_entry(ctx, dep, entry);
            brelse(bh);
            return 0;
        }
    }

    brelse(bh);
    return -ENOENT;
}

auto dir2_block_iterate(XfsInode* dp, XfsDirIterFn fn, void* user_ctx) -> int {
    XfsMountContext const* ctx = dp->mount;

    BufHead* bh = nullptr;
    int rc = dir2_read_block(dp, 0, &bh);
    if (rc != 0) {
#ifdef XFS_DEBUG
        mod::dbg::log("[xfs] dir2_block_iterate: read_block failed ino=%lu rc=%d", static_cast<unsigned long>(dp->ino), rc);
#endif
        return rc;
    }

    const uint8_t* block = bh->data;
    size_t const BLKSIZE = ctx->dir_blk_size;

    // Block tail
    const auto* btp = reinterpret_cast<const XfsDir2BlockTail*>(block + BLKSIZE - sizeof(XfsDir2BlockTail));
    uint32_t const LEAF_COUNT = btp->count.to_cpu();

#ifdef XFS_DEBUG
    const auto* dbg_hdr = reinterpret_cast<const XfsDir3DataHdr*>(block);
    mod::dbg::log("[xfs] dir2_block_iterate: ino=%lu magic=0x%x leaf_count=%u", static_cast<unsigned long>(dp->ino),
                  dbg_hdr->hdr.magic.to_cpu(), LEAF_COUNT);
#endif
    // Data entries start after the v3 header
    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    // Data entries end before the leaf entries
    const uint8_t* leaf_start = reinterpret_cast<const uint8_t*>(btp) - (static_cast<size_t>(LEAF_COUNT) * sizeof(XfsDir2LeafEntry));
    auto data_end = static_cast<size_t>(leaf_start - block);

    size_t offset = DATA_START;
    XfsDirEntry entry{};

    while (offset < data_end) {
        // Check for free space entry
        const auto* unused = reinterpret_cast<const XfsDir2DataUnused*>(block + offset);
        if (unused->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
            uint16_t const FREE_LEN = unused->length.to_cpu();
            if (FREE_LEN == 0 || FREE_LEN > data_end - offset) {
                break;
            }
            offset += FREE_LEN;
            continue;
        }

        const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + offset);
        fill_dir_entry(ctx, dep, &entry, data_entry_cookie(ctx, 0, offset));

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
// Leaf/Node directory - data block scanning
// ============================================================================

// Iterate over a single data block calling fn for each entry
auto dir2_scan_data_block(XfsInode* dp, xfs_dir2_db_t db, XfsDirIterFn fn, void* user_ctx) -> int {
    XfsMountContext const* ctx = dp->mount;

    BufHead* bh = nullptr;
    int rc = dir2_read_block(dp, db, &bh);
    if (rc != 0) {
        return rc;
    }

    const uint8_t* block = bh->data;
    size_t const BLKSIZE = ctx->dir_blk_size;

    const auto* hdr = reinterpret_cast<const XfsDir3DataHdr*>(block);
    uint32_t const MAGIC = hdr->hdr.magic.to_cpu();
    if (MAGIC != XFS_DIR3_DATA_MAGIC && MAGIC != XFS_DIR3_BLOCK_MAGIC) {
        mod::dbg::logger<"xfs">::error("dir data block: bad magic 0x%x", MAGIC);
        brelse(bh);
        return -EINVAL;
    }

    // v3 data header
    size_t offset = sizeof(XfsDir3DataHdr);
    XfsDirEntry entry{};

    while (offset + sizeof(XfsDir2DataUnused) <= BLKSIZE) {
        const auto* unused = reinterpret_cast<const XfsDir2DataUnused*>(block + offset);
        if (unused->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
            uint16_t const FREE_LEN = unused->length.to_cpu();
            if (FREE_LEN == 0 || offset + FREE_LEN > BLKSIZE) {
                break;
            }
            offset += FREE_LEN;
            continue;
        }

        const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + offset);
        if (dep->namelen == 0 || offset + dir2_data_entsize(ctx, dep->namelen) > BLKSIZE) {
            break;  // corrupt or past end
        }

        fill_dir_entry(ctx, dep, &entry, data_entry_cookie(ctx, db, offset));
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
    xfs_dahash_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);

    // For leaf/node directories, we need to read the leaf block(s) to find
    // the data block containing the matching hash.  The leaf block is at
    // directory block number = XFS_DIR2_LEAF_OFFSET >> (blklog).
    xfs_fileoff_t const LEAF_FSBNO = XFS_DIR2_LEAF_OFFSET >> ctx->block_log;

    XfsBmapResult bmap{};
    int rc = xfs_bmap_lookup(dp, LEAF_FSBNO, &bmap);
    if (rc != 0 || bmap.is_hole) {
        // No leaf block - might be single-block or corrupt
        // Fall back to linear scan of data blocks
        goto linear_scan;
    }

    {
        // Read the leaf block
        uint32_t const FBS = 1U << ctx->dir_blk_log;
        BufHead* leaf_bh = nullptr;
        if (FBS == 1) {
            leaf_bh = xfs_buf_read(ctx, bmap.startblock);
        } else {
            leaf_bh = xfs_buf_read_multi(ctx, bmap.startblock, FBS);
        }
        if (leaf_bh == nullptr) {
            goto linear_scan;
        }

        const uint8_t* leaf_data = leaf_bh->data;

        // Check magic - leaf block starts with xfs_da3_blkinfo
        const auto* info = reinterpret_cast<const XfsDa3Blkinfo*>(leaf_data);
        uint16_t const LEAF_MAGIC = info->hdr.magic.to_cpu();

        if (LEAF_MAGIC != XFS_DIR3_LEAF_MAGIC && LEAF_MAGIC != XFS_DIR3_LEAFN_MAGIC) {
            brelse(leaf_bh);
            goto linear_scan;
        }

        // Leaf entries start after the leaf header.
        // Leaf header: xfs_da3_blkinfo + Be16 count + Be16 stale + Be32 pad
        size_t const LEAF_HDR_SIZE = sizeof(XfsDa3Blkinfo) + 2 + 2 + 4;
        const uint8_t* leaf_entries_base = leaf_data + LEAF_HDR_SIZE;

        // Read count from the leaf header
        uint16_t leaf_count = 0;
        __builtin_memcpy(&leaf_count, leaf_data + sizeof(XfsDa3Blkinfo), 2);
        // It's big-endian
        leaf_count = (static_cast<uint16_t>(leaf_data[sizeof(XfsDa3Blkinfo)]) << 8) | leaf_data[sizeof(XfsDa3Blkinfo) + 1];

        size_t const LEAF_CAPACITY = (ctx->dir_blk_size - LEAF_HDR_SIZE) / sizeof(XfsDir2LeafEntry);
        if (leaf_count > LEAF_CAPACITY) {
            brelse(leaf_bh);
            goto linear_scan;
        }
        bool const LEAF_INDEX_FULL = leaf_count >= LEAF_CAPACITY;

        const auto* lep = reinterpret_cast<const XfsDir2LeafEntry*>(leaf_entries_base);

        // Binary search for hash
        int lo = 0;
        int hi = leaf_count - 1;
        int mid = -1;
        bool found = false;

        while (lo <= hi) {
            mid = (lo + hi) / 2;
            uint32_t const LHASH = lep[mid].hashval.to_cpu();
            if (HASH < LHASH) {
                hi = mid - 1;
            } else if (HASH > LHASH) {
                lo = mid + 1;
            } else {
                found = true;
                break;
            }
        }

        if (!found) {
            brelse(leaf_bh);
            if (LEAF_INDEX_FULL) {
                goto linear_scan;
            }
            return -ENOENT;
        }

        // Back up to first with this hash
        while (mid > 0 && lep[mid - 1].hashval.to_cpu() == HASH) {
            mid--;
        }

        // Check all matching hashes
        for (int i = mid; std::cmp_less(i, leaf_count); i++) {
            if (lep[i].hashval.to_cpu() != HASH) {
                break;
            }

            xfs_dir2_dataptr_t const ADDR = lep[i].address.to_cpu();
            if (ADDR == XFS_DIR2_NULL_DATAPTR) {
                continue;
            }

            xfs_dir2_db_t const DB = dir2_dataptr_to_db(ctx, ADDR);
            uint32_t const OFF = dir2_dataptr_to_off(ctx, ADDR);

            // Read the data block
            BufHead* data_bh = nullptr;
            rc = dir2_read_block(dp, DB, &data_bh);
            if (rc != 0) {
                continue;
            }

            if (OFF + sizeof(XfsDir2DataEntry) <= ctx->dir_blk_size) {
                const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(data_bh->data + OFF);
                if (dep->namelen == namelen && __builtin_memcmp(xfs_dir2_data_entry_name(dep), name, namelen) == 0) {
                    fill_dir_entry(ctx, dep, entry);
                    brelse(data_bh);
                    brelse(leaf_bh);
                    return 0;
                }
            }
            brelse(data_bh);
        }

        brelse(leaf_bh);
        if (LEAF_INDEX_FULL) {
            goto linear_scan;
        }
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
            size_t const BLKSIZE = ctx->dir_blk_size;
            size_t offset = sizeof(XfsDir3DataHdr);

            while (offset + sizeof(XfsDir2DataUnused) <= BLKSIZE) {
                const auto* unused = reinterpret_cast<const XfsDir2DataUnused*>(block + offset);
                if (unused->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
                    uint16_t const FREE_LEN = unused->length.to_cpu();
                    if (FREE_LEN == 0 || offset + FREE_LEN > BLKSIZE) {
                        break;
                    }
                    offset += FREE_LEN;
                    continue;
                }

                const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + offset);
                if (dep->namelen == 0) {
                    break;
                }

                if (dep->namelen == namelen && __builtin_memcmp(xfs_dir2_data_entry_name(dep), name, namelen) == 0) {
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
    XfsMountContext const* ctx = dp->mount;

    // Number of data blocks (approximate from file size)
    uint64_t nblocks = dp->size >> (ctx->block_log + ctx->dir_blk_log);
    if (nblocks == 0) {
        nblocks = 1;
    }

    for (xfs_dir2_db_t db = 0; db < nblocks; db++) {
        // Check if this data block exists (not a hole)
        xfs_fileoff_t const FBO = dir2_db_to_fsbno(ctx, db);
        XfsBmapResult bmap{};
        int rc = xfs_bmap_lookup(dp, FBO, &bmap);
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
    mod::dbg::log("[xfs] dir_lookup: ino=%lu fmt=%d size=%lu name=%.*s", static_cast<unsigned long>(dp->ino), dp->data_fork.format,
                  static_cast<unsigned long>(dp->size), static_cast<int>(namelen), name);
#endif
    int cached_result = 0;
    if (xfs_dentry_cache_lookup(dp, name, namelen, entry, &cached_result)) {
        return cached_result;
    }

    int result = 0;
    switch (dp->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL:
            result = dir2_sf_lookup(dp, name, namelen, entry);
            break;

        case XFS_DINODE_FMT_EXTENTS:
        case XFS_DINODE_FMT_BTREE: {
            if (dir2_is_single_block_dir(dp)) {
                result = dir2_block_lookup(dp, name, namelen, entry);
            } else {
                result = dir2_leaf_node_lookup(dp, name, namelen, entry);
            }
            break;
        }

        default:
            result = -EINVAL;
            break;
    }
    xfs_dentry_cache_store(dp, name, namelen, result, entry);
    return result;
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
            if (dir2_is_single_block_dir(dp)) {
                return dir2_block_iterate(dp, fn, ctx);
            }
            return dir2_leaf_node_iterate(dp, fn, ctx);
        }

        default:
            return -EINVAL;
    }
}

// ============================================================================
// Directory add-name - add a new entry to a directory
// ============================================================================

namespace {

struct XfsDir3LeafHdr {
    XfsDa3Blkinfo info;
    Be16 count;
    Be16 stale;
    Be32 pad32;
} __attribute__((packed));

static_assert(sizeof(XfsDir3LeafHdr) == 64);

constexpr size_t XFS_DA3_CRC_OFF = __builtin_offsetof(XfsDa3Blkinfo, crc);

auto dir2_buf_read_dir_block(XfsMountContext* ctx, xfs_fsblock_t disk_block) -> BufHead* {
    uint32_t const FBS = 1U << ctx->dir_blk_log;
    return (FBS == 1) ? xfs_buf_read(ctx, disk_block) : xfs_buf_read_multi(ctx, disk_block, FBS);
}

auto dir2_device_blkno(const XfsMountContext* ctx, xfs_fsblock_t disk_block) -> uint64_t {
    auto const AGNO = xfs_ag_number(disk_block, ctx->ag_blk_log);
    auto const AGBNO = xfs_ag_block(disk_block, ctx->ag_blk_log);
    uint64_t const LINEAR = (static_cast<uint64_t>(AGNO) * ctx->ag_blocks) + AGBNO;
    size_t const RATIO = ctx->block_size / ctx->device->block_size;
    return LINEAR * RATIO;
}

void dir2_set_sequential_alloc_hint(XfsInode* dp, xfs_fileoff_t file_block, XfsAllocReq* req) {
    if (dp == nullptr || dp->mount == nullptr || req == nullptr || dp->data_fork.format != XFS_DINODE_FMT_EXTENTS) {
        return;
    }

    XfsIforkExtents const& extents = dp->data_fork.extents;
    if (extents.list == nullptr || extents.count == 0) {
        return;
    }

    XfsMountContext const* ctx = dp->mount;
    for (uint32_t i = extents.count; i > 0; --i) {
        XfsBmbtIrec const& ext = extents.list[i - 1];
        if (ext.br_startoff + ext.br_blockcount != file_block) {
            continue;
        }

        xfs_fsblock_t const NEXT_FSB = ext.br_startblock + ext.br_blockcount;
        xfs_agnumber_t const AGNO = xfs_ag_number(NEXT_FSB, ctx->ag_blk_log);
        xfs_agblock_t const AGBNO = xfs_ag_block(NEXT_FSB, ctx->ag_blk_log);
        if (AGNO < ctx->ag_count && AGBNO < ctx->ag_blocks) {
            req->agno = AGNO;
            req->agbno = AGBNO;
        }
        return;
    }
}

void dir2_init_data_header(XfsInode* dp, xfs_fsblock_t disk_block, uint32_t magic, uint8_t* block) {
    auto* hdr = reinterpret_cast<XfsDir3DataHdr*>(block);
    hdr->hdr.magic = Be32::from_cpu(magic);
    hdr->hdr.blkno = Be64::from_cpu(dir2_device_blkno(dp->mount, disk_block));
    hdr->hdr.owner = Be64::from_cpu(dp->ino);
    __builtin_memcpy(&hdr->hdr.uuid, &dp->mount->uuid, sizeof(XfsUuidT));
}

void dir2_init_leaf_header(XfsInode* dp, xfs_fsblock_t disk_block, XfsDir3LeafHdr* hdr, size_t count, size_t stale) {
    hdr->info.hdr.forw = Be32::from_cpu(0);
    hdr->info.hdr.back = Be32::from_cpu(0);
    hdr->info.hdr.magic = Be16::from_cpu(static_cast<uint16_t>(XFS_DIR3_LEAF_MAGIC));
    hdr->info.hdr.pad = Be16{0};
    hdr->info.blkno = Be64::from_cpu(dir2_device_blkno(dp->mount, disk_block));
    hdr->info.owner = Be64::from_cpu(dp->ino);
    __builtin_memcpy(&hdr->info.uuid, &dp->mount->uuid, sizeof(XfsUuidT));
    hdr->count = Be16::from_cpu(static_cast<uint16_t>(count));
    hdr->stale = Be16::from_cpu(static_cast<uint16_t>(stale));
    hdr->pad32 = Be32{0};
}

auto dir2_alloc_mapped_dir_block(XfsInode* dp, XfsTransaction* tp, xfs_fileoff_t file_block, xfs_fsblock_t* disk_block_out) -> int {
    XfsMountContext* ctx = dp->mount;
    xfs_agnumber_t const PREF_AG = xfs_ino_ag(dp->ino, ctx->agino_log);
    uint32_t const FBS = 1U << ctx->dir_blk_log;

    XfsAllocReq req{};
    req.agno = PREF_AG;
    req.agbno = 0;
    req.minlen = FBS;
    req.maxlen = FBS;
    req.alignment = 0;
    dir2_set_sequential_alloc_hint(dp, file_block, &req);

    XfsAllocResult alloc_result{};
    int rc = xfs_alloc_extent(ctx, tp, req, &alloc_result);
    if (rc != 0) {
        return rc;
    }

    xfs_fsblock_t const DISK_BLOCK = xfs_agbno_to_fsbno(alloc_result.agno, alloc_result.agbno, ctx->ag_blk_log);
    XfsBmbtIrec new_extent{};
    new_extent.br_startoff = file_block;
    new_extent.br_startblock = DISK_BLOCK;
    new_extent.br_blockcount = alloc_result.len;
    new_extent.br_unwritten = false;

    rc = xfs_bmap_add_extent(dp, tp, new_extent);
    if (rc != 0) {
        return rc;
    }

    dp->nblocks += alloc_result.len;
    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);
    *disk_block_out = DISK_BLOCK;
    return 0;
}

auto dir2_data_block_count(const XfsInode* dp) -> uint64_t {
    XfsMountContext const* ctx = dp->mount;
    uint64_t nblocks = dp->size >> (ctx->block_log + ctx->dir_blk_log);
    if (nblocks == 0) {
        nblocks = 1;
    }
    return nblocks;
}

auto dir2_db_off_to_dataptr(const XfsMountContext* ctx, xfs_dir2_db_t db, size_t off) -> xfs_dir2_dataptr_t {
    uint64_t const BYTE_OFF = (static_cast<uint64_t>(db) << (ctx->block_log + ctx->dir_blk_log)) + off;
    return static_cast<xfs_dir2_dataptr_t>(BYTE_OFF >> XFS_DIR2_DATA_ALIGN_LOG);
}

auto dir2_leaf_entries(XfsDir3LeafHdr* hdr) -> XfsDir2LeafEntry* {
    return reinterpret_cast<XfsDir2LeafEntry*>(reinterpret_cast<uint8_t*>(hdr) + sizeof(XfsDir3LeafHdr));
}

auto dir2_leaf_entries(const XfsDir3LeafHdr* hdr) -> const XfsDir2LeafEntry* {
    return reinterpret_cast<const XfsDir2LeafEntry*>(reinterpret_cast<const uint8_t*>(hdr) + sizeof(XfsDir3LeafHdr));
}

auto dir2_read_leaf_block(XfsInode* dp, BufHead** bhp) -> int {
    if (bhp == nullptr) {
        return -EINVAL;
    }
    *bhp = nullptr;

    XfsMountContext* ctx = dp->mount;
    xfs_fileoff_t const LEAF_FSBNO = XFS_DIR2_LEAF_OFFSET >> ctx->block_log;

    XfsBmapResult bmap{};
    int const RC = xfs_bmap_lookup(dp, LEAF_FSBNO, &bmap);
    if (RC != 0) {
        return RC;
    }
    if (bmap.is_hole) {
        return -ENOSYS;
    }

    uint32_t const FBS = 1U << ctx->dir_blk_log;
    BufHead* bh = (FBS == 1) ? xfs_buf_read(ctx, bmap.startblock) : xfs_buf_read_multi(ctx, bmap.startblock, FBS);
    if (bh == nullptr) {
        return -EIO;
    }

    auto* hdr = reinterpret_cast<XfsDir3LeafHdr*>(bh->data);
    uint16_t const MAGIC = hdr->info.hdr.magic.to_cpu();
    if (MAGIC != XFS_DIR3_LEAF_MAGIC && MAGIC != XFS_DIR3_LEAFN_MAGIC) {
        brelse(bh);
        return -ENOSYS;
    }

    size_t const CAPACITY = (ctx->dir_blk_size - sizeof(XfsDir3LeafHdr)) / sizeof(XfsDir2LeafEntry);
    if (hdr->count.to_cpu() > CAPACITY) {
        brelse(bh);
        return -EINVAL;
    }

    *bhp = bh;
    return 0;
}

void dir2_recompute_leaf_crc(uint8_t* block, size_t blksize) {
    auto* hdr = reinterpret_cast<XfsDir3LeafHdr*>(block);
    hdr->info.crc = Be32{0};
    uint32_t const CRC = util::crc32c_block_with_cksum(block, blksize, XFS_DA3_CRC_OFF);
    hdr->info.crc = Be32::from_cpu(CRC);
}

void dir2_recompute_data_crc(uint8_t* block, size_t blksize) {
    auto* hdr = reinterpret_cast<XfsDir3DataHdr*>(block);
    hdr->hdr.crc = Be32{0};
    uint32_t const CRC = util::crc32c_block_with_cksum(block, blksize, 4);
    hdr->hdr.crc = Be32::from_cpu(CRC);
}

auto dir2_validate_leaf_data_block(const uint8_t* block) -> int {
    const auto* hdr = reinterpret_cast<const XfsDir3DataHdr*>(block);
    uint32_t const MAGIC = hdr->hdr.magic.to_cpu();
    return (MAGIC == XFS_DIR3_DATA_MAGIC) ? 0 : -EINVAL;
}

struct BestFreeSlot {
    uint16_t off;
    uint16_t len;
};

void dir2_rebuild_data_bestfree(const XfsMountContext* ctx, uint8_t* block, size_t data_start, size_t data_end) {
    auto* hdr = reinterpret_cast<XfsDir3DataHdr*>(block);
    std::array<BestFreeSlot, 3> best{{{.off = 0, .len = 0}, {.off = 0, .len = 0}, {.off = 0, .len = 0}}};

    size_t off = data_start;
    while (off < data_end) {
        if (off + sizeof(XfsDir2DataUnused) > data_end) {
            break;
        }

        const auto* unused = reinterpret_cast<const XfsDir2DataUnused*>(block + off);
        if (unused->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
            uint16_t const FREE_LEN = unused->length.to_cpu();
            if (FREE_LEN == 0 || off + FREE_LEN > data_end) {
                break;
            }

            BestFreeSlot const CUR{.off = static_cast<uint16_t>(off), .len = FREE_LEN};
            for (int idx = 0; idx < 3; idx++) {
                if (CUR.len > best.at(static_cast<size_t>(idx)).len) {
                    for (int j = 2; j > idx; j--) {
                        best.at(static_cast<size_t>(j)) = best.at(static_cast<size_t>(j - 1));
                    }
                    best.at(static_cast<size_t>(idx)) = CUR;
                    break;
                }
            }

            off += FREE_LEN;
            continue;
        }

        const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + off);
        size_t const DEP_SIZE = dir2_data_entsize(ctx, dep->namelen);
        if (dep->namelen == 0 || DEP_SIZE == 0 || off + DEP_SIZE > data_end) {
            break;
        }
        off += DEP_SIZE;
    }

    for (int i = 0; i < 3; i++) {
        hdr->best_free.at(static_cast<size_t>(i)).offset = Be16::from_cpu(best.at(static_cast<size_t>(i)).off);
        hdr->best_free.at(static_cast<size_t>(i)).length = Be16::from_cpu(best.at(static_cast<size_t>(i)).len);
    }
}

void dir2_make_data_free(const XfsMountContext* ctx, uint8_t* block, size_t data_start, size_t data_end, size_t entry_off,
                         size_t entry_size) {
    auto* unused = reinterpret_cast<XfsDir2DataUnused*>(block + entry_off);
    unused->freetag = Be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
    unused->length = Be16::from_cpu(static_cast<uint16_t>(entry_size));
    auto* tag = reinterpret_cast<Be16*>(block + entry_off + entry_size - sizeof(Be16));
    *tag = Be16::from_cpu(static_cast<uint16_t>(entry_off));

    size_t prev_free_off = 0;
    size_t prev_free_len = 0;
    bool prev_found = false;
    size_t next_free_len = 0;
    bool next_found = false;

    size_t off = data_start;
    while (off < data_end) {
        if (off + sizeof(XfsDir2DataUnused) > data_end) {
            break;
        }

        const auto* ent = reinterpret_cast<const XfsDir2DataUnused*>(block + off);
        if (ent->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
            uint16_t const FREE_LEN = ent->length.to_cpu();
            if (FREE_LEN == 0 || off + FREE_LEN > data_end) {
                break;
            }

            if (off + FREE_LEN == entry_off) {
                prev_free_off = off;
                prev_free_len = FREE_LEN;
                prev_found = true;
            } else if (off == entry_off + entry_size) {
                next_free_len = FREE_LEN;
                next_found = true;
            }

            off += FREE_LEN;
            continue;
        }

        const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + off);
        size_t const DEP_SIZE = dir2_data_entsize(ctx, dep->namelen);
        if (dep->namelen == 0 || DEP_SIZE == 0 || off + DEP_SIZE > data_end) {
            break;
        }
        off += DEP_SIZE;
    }

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
    merged->freetag = Be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
    merged->length = Be16::from_cpu(static_cast<uint16_t>(merged_len));
    auto* merged_tag = reinterpret_cast<Be16*>(block + merged_off + merged_len - sizeof(Be16));
    *merged_tag = Be16::from_cpu(static_cast<uint16_t>(merged_off));

    dir2_rebuild_data_bestfree(ctx, block, data_start, data_end);
}

auto dir2_find_free_region(const XfsMountContext* ctx, const uint8_t* block, size_t data_start, size_t data_end, size_t need_len,
                           size_t* free_off, size_t* free_len) -> bool {
    size_t off = data_start;
    while (off < data_end) {
        if (off + sizeof(XfsDir2DataUnused) > data_end) {
            break;
        }

        const auto* unused = reinterpret_cast<const XfsDir2DataUnused*>(block + off);
        if (unused->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
            uint16_t const FREE_LEN = unused->length.to_cpu();
            if (FREE_LEN == 0 || off + FREE_LEN > data_end) {
                break;
            }
            if (FREE_LEN >= need_len) {
                *free_off = off;
                *free_len = FREE_LEN;
                return true;
            }
            off += FREE_LEN;
            continue;
        }

        const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + off);
        size_t const DEP_SIZE = dir2_data_entsize(ctx, dep->namelen);
        if (dep->namelen == 0 || DEP_SIZE == 0 || off + DEP_SIZE > data_end) {
            break;
        }
        off += DEP_SIZE;
    }

    return false;
}

void dir2_write_data_entry(const XfsMountContext* ctx, uint8_t* block, size_t data_start, size_t data_end, size_t entry_off,
                           size_t free_len, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype) {
    bool const HAS_FTYPE_FLAG = xfs_has_ftype(ctx);
    size_t const NEED_LEN = dir2_data_entsize(ctx, static_cast<uint8_t>(namelen));

    auto* dep = reinterpret_cast<XfsDir2DataEntry*>(block + entry_off);
    dep->inumber = Be64::from_cpu(ino);
    dep->namelen = static_cast<uint8_t>(namelen);
    __builtin_memcpy(xfs_dir2_data_entry_name(dep), name, namelen);

    size_t tag_off = 8 + 1 + namelen;
    if (HAS_FTYPE_FLAG) {
        xfs_dir2_data_entry_name(dep)[namelen] = ftype;
        tag_off++;
    }

    size_t const ENTRY_END = entry_off + NEED_LEN;
    auto* tag_loc = reinterpret_cast<Be16*>(block + ENTRY_END - sizeof(Be16));
    *tag_loc = Be16::from_cpu(static_cast<uint16_t>(entry_off));

    size_t const PAD_START = entry_off + tag_off;
    size_t const PAD_END = ENTRY_END - sizeof(Be16);
    if (PAD_END > PAD_START) {
        __builtin_memset(block + PAD_START, 0, PAD_END - PAD_START);
    }

    if (free_len > NEED_LEN) {
        size_t const REMAINING = free_len - NEED_LEN;
        auto* new_unused = reinterpret_cast<XfsDir2DataUnused*>(block + ENTRY_END);
        new_unused->freetag = Be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
        new_unused->length = Be16::from_cpu(static_cast<uint16_t>(REMAINING));
        auto* unused_tag = reinterpret_cast<Be16*>(block + ENTRY_END + REMAINING - sizeof(Be16));
        *unused_tag = Be16::from_cpu(static_cast<uint16_t>(ENTRY_END));
    }

    dir2_rebuild_data_bestfree(ctx, block, data_start, data_end);
}

auto dir2_leaf_find_dataptr(const XfsDir3LeafHdr* hdr, xfs_dahash_t hash, xfs_dir2_dataptr_t dataptr, int* index_out) -> int {
    uint16_t const LEAF_COUNT = hdr->count.to_cpu();
    const auto* lep = dir2_leaf_entries(hdr);
    for (int i = 0; std::cmp_less(i, LEAF_COUNT); i++) {
        if (lep[i].address.to_cpu() == dataptr && lep[i].hashval.to_cpu() == hash) {
            *index_out = i;
            return 0;
        }
    }
    return -ENOENT;
}

auto dir2_leaf_prepare_stale_insert(const XfsDir3LeafHdr* hdr, xfs_dahash_t hash, int* stale_idx_out, int* insert_pos_out) -> int {
    uint16_t const LEAF_COUNT = hdr->count.to_cpu();
    uint16_t const STALE_COUNT = hdr->stale.to_cpu();
    if (STALE_COUNT == 0) {
        return -ENOSPC;
    }

    const auto* lep = dir2_leaf_entries(hdr);
    int insert_pos = 0;
    for (int i = 0; std::cmp_less(i, LEAF_COUNT); i++) {
        if (lep[i].address.to_cpu() != XFS_DIR2_NULL_DATAPTR && lep[i].hashval.to_cpu() <= hash) {
            insert_pos = i + 1;
        }
    }

    int stale_idx = -1;
    int best_dist = static_cast<int>(LEAF_COUNT) + 1;
    for (int i = 0; std::cmp_less(i, LEAF_COUNT); i++) {
        if (lep[i].address.to_cpu() == XFS_DIR2_NULL_DATAPTR) {
            int const DIST = (i >= insert_pos) ? (i - insert_pos) : (insert_pos - i);
            if (DIST < best_dist) {
                best_dist = DIST;
                stale_idx = i;
            }
        }
    }

    if (stale_idx < 0) {
        return -EINVAL;
    }

    *stale_idx_out = stale_idx;
    *insert_pos_out = insert_pos;
    return 0;
}

auto dir2_leaf_ensure_stale_slot(XfsMountContext const* ctx, XfsDir3LeafHdr* hdr) -> int {
    uint16_t const STALE_COUNT = hdr->stale.to_cpu();
    if (STALE_COUNT != 0) {
        return 0;
    }

    uint16_t const LEAF_COUNT = hdr->count.to_cpu();
    size_t const CAPACITY = (ctx->dir_blk_size - sizeof(XfsDir3LeafHdr)) / sizeof(XfsDir2LeafEntry);
    if (LEAF_COUNT >= CAPACITY || CAPACITY > UINT16_MAX) {
        return -ENOSPC;
    }

    auto* lep = dir2_leaf_entries(hdr);
    lep[LEAF_COUNT].hashval = Be32::from_cpu(UINT32_MAX);
    lep[LEAF_COUNT].address = Be32::from_cpu(XFS_DIR2_NULL_DATAPTR);
    hdr->count = Be16::from_cpu(static_cast<uint16_t>(LEAF_COUNT + 1));
    hdr->stale = Be16::from_cpu(1);
    return 0;
}

auto dir2_leaf_can_index_new_entry(XfsMountContext const* ctx, const XfsDir3LeafHdr* hdr) -> bool {
    if (hdr->stale.to_cpu() != 0) {
        return true;
    }

    uint16_t const LEAF_COUNT = hdr->count.to_cpu();
    size_t const CAPACITY = (ctx->dir_blk_size - sizeof(XfsDir3LeafHdr)) / sizeof(XfsDir2LeafEntry);
    return CAPACITY <= UINT16_MAX && LEAF_COUNT < CAPACITY;
}

void dir2_leaf_reuse_stale_entry(XfsDir3LeafHdr* hdr, xfs_dahash_t hash, xfs_dir2_dataptr_t dataptr, int stale_idx, int insert_pos) {
    auto* lep = dir2_leaf_entries(hdr);
    uint16_t const STALE_COUNT = hdr->stale.to_cpu();

    if (stale_idx < insert_pos) {
        insert_pos--;
        for (int i = stale_idx; i < insert_pos; i++) {
            lep[i] = lep[i + 1];
        }
    } else if (stale_idx > insert_pos) {
        for (int i = stale_idx; i > insert_pos; i--) {
            lep[i] = lep[i - 1];
        }
    }

    lep[insert_pos].hashval = Be32::from_cpu(hash);
    lep[insert_pos].address = Be32::from_cpu(dataptr);
    hdr->stale = Be16::from_cpu(static_cast<uint16_t>(STALE_COUNT - 1));
}

auto dir2_leaf_node_find_data_entry(XfsInode* dp, const char* name, uint16_t namelen, BufHead** data_bhp, xfs_dir2_db_t* dbp,
                                    size_t* entry_offp, size_t* entry_sizep) -> int {
    XfsMountContext const* ctx = dp->mount;
    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    size_t const DATA_END = ctx->dir_blk_size;

    uint64_t const NBLOCKS = dir2_data_block_count(dp);
    for (xfs_dir2_db_t db = 0; db < NBLOCKS; db++) {
        xfs_fileoff_t const FBO = dir2_db_to_fsbno(ctx, db);
        XfsBmapResult bmap{};
        int rc = xfs_bmap_lookup(dp, FBO, &bmap);
        if (rc != 0 || bmap.is_hole) {
            continue;
        }

        BufHead* bh = nullptr;
        rc = dir2_read_block(dp, db, &bh);
        if (rc != 0) {
            continue;
        }

        uint8_t* block = bh->data;
        rc = dir2_validate_leaf_data_block(block);
        if (rc != 0) {
            brelse(bh);
            return rc;
        }

        size_t off = DATA_START;
        while (off + sizeof(XfsDir2DataUnused) <= DATA_END) {
            const auto* unused = reinterpret_cast<const XfsDir2DataUnused*>(block + off);
            if (unused->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
                uint16_t const FREE_LEN = unused->length.to_cpu();
                if (FREE_LEN == 0 || off + FREE_LEN > DATA_END) {
                    break;
                }
                off += FREE_LEN;
                continue;
            }

            const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + off);
            size_t const DEP_SIZE = dir2_data_entsize(ctx, dep->namelen);
            if (dep->namelen == 0 || DEP_SIZE == 0 || off + DEP_SIZE > DATA_END) {
                break;
            }

            if (dep->namelen == namelen && __builtin_memcmp(xfs_dir2_data_entry_name(dep), name, namelen) == 0) {
                *data_bhp = bh;
                *dbp = db;
                *entry_offp = off;
                *entry_sizep = DEP_SIZE;
                return 0;
            }

            off += DEP_SIZE;
        }

        brelse(bh);
    }

    return -ENOENT;
}

auto dir2_leaf_node_find_free_region(XfsInode* dp, size_t need_len, BufHead** data_bhp, xfs_dir2_db_t* dbp, size_t* free_offp,
                                     size_t* free_lenp) -> int {
    XfsMountContext const* ctx = dp->mount;
    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    size_t const DATA_END = ctx->dir_blk_size;

    uint64_t const NBLOCKS = dir2_data_block_count(dp);
    for (xfs_dir2_db_t db = 0; db < NBLOCKS; db++) {
        xfs_fileoff_t const FBO = dir2_db_to_fsbno(ctx, db);
        XfsBmapResult bmap{};
        int rc = xfs_bmap_lookup(dp, FBO, &bmap);
        if (rc != 0 || bmap.is_hole) {
            continue;
        }

        BufHead* bh = nullptr;
        rc = dir2_read_block(dp, db, &bh);
        if (rc != 0) {
            continue;
        }

        uint8_t* block = bh->data;
        rc = dir2_validate_leaf_data_block(block);
        if (rc != 0) {
            brelse(bh);
            return rc;
        }

        size_t free_off = 0;
        size_t free_len = 0;
        if (dir2_find_free_region(ctx, block, DATA_START, DATA_END, need_len, &free_off, &free_len)) {
            *data_bhp = bh;
            *dbp = db;
            *free_offp = free_off;
            *free_lenp = free_len;
            return 0;
        }

        brelse(bh);
    }

    return -ENOSPC;
}

auto dir2_leaf_alloc_data_block(XfsInode* dp, XfsTransaction* tp, BufHead** data_bhp, xfs_dir2_db_t* dbp, size_t* free_offp,
                                size_t* free_lenp) -> int {
    XfsMountContext* ctx = dp->mount;
    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    size_t const DATA_END = ctx->dir_blk_size;
    auto const NEW_DB = static_cast<xfs_dir2_db_t>(dir2_data_block_count(dp));

    xfs_fsblock_t disk_block = 0;
    int const RC = dir2_alloc_mapped_dir_block(dp, tp, dir2_db_to_fsbno(ctx, NEW_DB), &disk_block);
    if (RC != 0) {
        return RC;
    }

    BufHead* bh = dir2_buf_read_dir_block(ctx, disk_block);
    if (bh == nullptr) {
        return -EIO;
    }

    uint8_t* block = bh->data;
    __builtin_memset(block, 0, DATA_END);
    dir2_init_data_header(dp, disk_block, XFS_DIR3_DATA_MAGIC, block);
    dir2_make_data_free(ctx, block, DATA_START, DATA_END, DATA_START, DATA_END - DATA_START);
    dir2_recompute_data_crc(block, DATA_END);
    xfs_trans_log_buf_full(tp, bh);

    dp->size = (static_cast<uint64_t>(NEW_DB) + 1ULL) * static_cast<uint64_t>(ctx->dir_blk_size);
    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);

    *data_bhp = bh;
    *dbp = NEW_DB;
    *free_offp = DATA_START;
    *free_lenp = DATA_END - DATA_START;
    return 0;
}

// Add a name to a shortform directory (inline in inode data fork).
// The new entry is appended after the existing entries.
auto dir2_sf_addname(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype, XfsTransaction* tp) -> int {
    XfsMountContext const* ctx = dp->mount;
    bool const HAS_FTYPE = xfs_has_ftype(ctx);

    const uint8_t* old_data = dp->data_fork.local.data;
    size_t const OLD_SIZE = dp->data_fork.local.size;

    if (!dir2_sf_header_size_if_valid(old_data, OLD_SIZE, nullptr)) {
        return -EINVAL;
    }

    const auto* old_hdr = reinterpret_cast<const XfsDir2SfHdr*>(old_data);

    // Compute the size of the new entry:
    // namelen(1) + offset(2) + name(namelen) + [ftype(1)] + ino(4 or 8)
    size_t const INO_SIZE = xfs_dir2_sf_inumber_size(old_hdr);
    size_t const NEW_ENTRY_SIZE = 1 + 2 + namelen + (HAS_FTYPE ? 1 : 0) + INO_SIZE;
    size_t const NEW_SIZE = OLD_SIZE + NEW_ENTRY_SIZE;

    // Check if 8-byte inode numbers are needed
    bool const NEED_I8 = (old_hdr->i8count != 0) || (ino > 0xFFFFFFFFULL);

    // If we need to upgrade from 4-byte to 8-byte inodes, the calculation
    // changes significantly.  For simplicity, just handle the common case
    // where the format stays the same.
    if (NEED_I8 && old_hdr->i8count == 0) {
        // Would need format conversion - fall through to block format
        return -E2BIG;
    }

    // Check if the shortform still fits in the inode literal area
    // (inode_size - dinode header - attr fork space)
    size_t max_inline = ctx->inode_size - 176;  // XfsDinode is 176 bytes
    if (dp->forkoff != 0) {
        max_inline = static_cast<size_t>(dp->forkoff) << 3;
    }
    if (NEW_SIZE > max_inline) {
        return -E2BIG;  // need to convert to block format
    }

    // Build the new data fork with the entry appended
    auto* new_data = new uint8_t[NEW_SIZE];
    if (new_data == nullptr) {
        return -ENOMEM;
    }

    // Copy existing data
    __builtin_memcpy(new_data, old_data, OLD_SIZE);
    sf_repair_offset_tags(new_data, OLD_SIZE, ctx);
    auto const OFF_VAL = sf_next_offset_tag(new_data, OLD_SIZE, ctx);

    // Update the header: increment count
    auto* new_hdr = reinterpret_cast<XfsDir2SfHdr*>(new_data);
    new_hdr->count++;

    // Append entry at old_size offset
    uint8_t* entry_ptr = new_data + OLD_SIZE;

    // namelen
    entry_ptr[0] = static_cast<uint8_t>(namelen);
    // Offset tags back readdir cookies.  Keep them monotonic across remove/add
    // churn so callers can resume after deleting previously returned names.
    entry_ptr[1] = static_cast<uint8_t>(OFF_VAL >> 8U);
    entry_ptr[2] = static_cast<uint8_t>(OFF_VAL & 0xffU);
    // name
    __builtin_memcpy(entry_ptr + 3, name, namelen);

    size_t p = 3 + namelen;
    // ftype
    if (HAS_FTYPE) {
        entry_ptr[p++] = ftype;
    }
    // inode number (big-endian)
    if (INO_SIZE == 8) {
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
    dp->data_fork.local.size = NEW_SIZE;
    dp->size = NEW_SIZE;
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
    size_t const SF_SIZE = dp->data_fork.local.size;
    size_t sf_hdr_size = 0;
    if (!dir2_sf_header_size_if_valid(sf_data, SF_SIZE, &sf_hdr_size)) {
        return -EINVAL;
    }

    const auto* sf_hdr = reinterpret_cast<const XfsDir2SfHdr*>(sf_data);
    bool const HAS_FTYPE = xfs_has_ftype(ctx);
    size_t const BLKSIZE = ctx->dir_blk_size;

    // --- 1. Collect all shortform entries into a temporary array ---
    struct SfRec {
        std::array<char, 256> name{};
        uint8_t namelen{};
        xfs_ino_t ino{};
        uint8_t ftype{};
    };

    uint8_t const SF_COUNT = sf_hdr->count;
    auto* recs = new SfRec[SF_COUNT + 2];  // +2 for dot/dotdot
    if (recs == nullptr) {
        return -ENOMEM;
    }

    xfs_ino_t const PARENT_INO = xfs_dir2_sf_get_parent(sf_hdr);

    // "." entry
    recs[0].namelen = 1;
    recs[0].name.at(0) = '.';
    recs[0].name.at(1) = '\0';
    recs[0].ino = dp->ino;
    recs[0].ftype = XFS_DIR3_FT_DIR;

    // ".." entry
    recs[1].namelen = 2;
    recs[1].name.at(0) = '.';
    recs[1].name.at(1) = '.';
    recs[1].name.at(2) = '\0';
    recs[1].ino = PARENT_INO;
    recs[1].ftype = XFS_DIR3_FT_DIR;

    // Parse SF entries
    size_t const HDR_SIZE = sf_hdr_size;
    const uint8_t* ptr = sf_data + HDR_SIZE;
    int total_entries = 2;  // dot and dotdot

    for (uint8_t i = 0; i < SF_COUNT; i++) {
        if (ptr >= sf_data + SF_SIZE) {
            break;
        }
        const auto* sfep = reinterpret_cast<const XfsDir2SfEntry*>(ptr);
        uint8_t const ENTRY_NAMELEN = sfep->namelen;

        const uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + ENTRY_NAMELEN;
        uint8_t ftype = XFS_DIR3_FT_UNKNOWN;
        if (HAS_FTYPE) {
            ftype = *ino_ptr;
            ino_ptr++;
        }

        recs[total_entries].namelen = ENTRY_NAMELEN;
        __builtin_memcpy(recs[total_entries].name.data(), xfs_dir2_sf_entry_name(sfep), ENTRY_NAMELEN);
        recs[total_entries].name.at(ENTRY_NAMELEN) = '\0';
        recs[total_entries].ino = sf_get_ino(sf_hdr, ino_ptr);
        recs[total_entries].ftype = ftype;
        total_entries++;

        size_t const ENTRY_SIZE = sf_entry_size(sf_hdr, sfep, HAS_FTYPE);
        ptr += ENTRY_SIZE;
    }

    // --- 2. Allocate a disk block for the directory ---
    xfs_agnumber_t const PREF_AG = xfs_ino_ag(dp->ino, ctx->agino_log);

    XfsAllocReq req{};
    req.agno = PREF_AG;
    req.agbno = 0;
    uint32_t const FBS = 1U << ctx->dir_blk_log;  // fs blocks per dir block
    req.minlen = FBS;
    req.maxlen = FBS;
    req.alignment = 0;

    XfsAllocResult alloc_result{};
    int const RC = xfs_alloc_extent(ctx, tp, req, &alloc_result);
    if (RC != 0) {
        delete[] recs;
        return RC;
    }

    xfs_fsblock_t const DISK_BLOCK = xfs_agbno_to_fsbno(alloc_result.agno, alloc_result.agbno, ctx->ag_blk_log);

    // Read the block (to get a buffer to write into)
    BufHead* bh = nullptr;
    if (FBS == 1) {
        bh = xfs_buf_read(ctx, DISK_BLOCK);
    } else {
        bh = xfs_buf_read_multi(ctx, DISK_BLOCK, FBS);
    }
    if (bh == nullptr) {
        delete[] recs;
        return -EIO;
    }

    uint8_t* block = bh->data;
    __builtin_memset(block, 0, BLKSIZE);

    // --- 3. Build the block-format directory ---

    // 3a. Header
    auto* hdr3 = reinterpret_cast<XfsDir3DataHdr*>(block);
    hdr3->hdr.magic = Be32::from_cpu(XFS_DIR3_BLOCK_MAGIC);
    hdr3->hdr.owner = Be64::from_cpu(dp->ino);
    // Compute disk address for blkno field
    {
        auto agno = static_cast<xfs_agnumber_t>(DISK_BLOCK >> ctx->ag_blk_log);
        auto agbno = static_cast<xfs_agblock_t>(DISK_BLOCK & ((1ULL << ctx->ag_blk_log) - 1));
        uint64_t const LINEAR = (static_cast<uint64_t>(agno) * ctx->ag_blocks) + agbno;
        size_t const RATIO = ctx->block_size / ctx->device->block_size;
        hdr3->hdr.blkno = Be64::from_cpu(LINEAR * RATIO);
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
        size_t const ENTRY_SIZE = dir2_data_entsize(ctx, recs[i].namelen);

        auto* dep = reinterpret_cast<XfsDir2DataEntry*>(block + data_offset);
        dep->inumber = Be64::from_cpu(recs[i].ino);
        dep->namelen = recs[i].namelen;
        __builtin_memcpy(xfs_dir2_data_entry_name(dep), recs[i].name.data(), recs[i].namelen);

        // ftype
        if (HAS_FTYPE) {
            xfs_dir2_data_entry_name(dep)[recs[i].namelen] = recs[i].ftype;
        }

        // Zero pad
        size_t const USED = 8 + 1 + recs[i].namelen + (HAS_FTYPE ? 1 : 0);
        size_t const PAD_START = data_offset + USED;
        size_t const PAD_END = data_offset + ENTRY_SIZE - sizeof(Be16);
        if (PAD_END > PAD_START) {
            __builtin_memset(block + PAD_START, 0, PAD_END - PAD_START);
        }

        // Tag (self offset within the block, at end of entry)
        auto* tag = reinterpret_cast<Be16*>(block + data_offset + ENTRY_SIZE - sizeof(Be16));
        *tag = Be16::from_cpu(static_cast<uint16_t>(data_offset));

        // Build leaf record
        leaves[leaf_count].hash = xfs_da_hashname(reinterpret_cast<const uint8_t*>(recs[i].name.data()), recs[i].namelen);
        leaves[leaf_count].address = static_cast<uint32_t>(data_offset >> XFS_DIR2_DATA_ALIGN_LOG);
        leaf_count++;

        data_offset += ENTRY_SIZE;
    }

    // 3c. Block tail at the very end
    auto* btp = reinterpret_cast<XfsDir2BlockTail*>(block + BLKSIZE - sizeof(XfsDir2BlockTail));
    btp->count = Be32::from_cpu(static_cast<uint32_t>(leaf_count));
    btp->stale = Be32::from_cpu(0);

    // 3d. Leaf entries right before the tail (sorted by hash)
    // Simple insertion sort
    for (int i = 1; i < leaf_count; i++) {
        LeafRec const TMP = leaves[i];
        int j = i - 1;
        while (j >= 0 && leaves[j].hash > TMP.hash) {
            leaves[j + 1] = leaves[j];
            j--;
        }
        leaves[j + 1] = TMP;
    }

    auto* blp = reinterpret_cast<XfsDir2LeafEntry*>(block + BLKSIZE - sizeof(XfsDir2BlockTail) -
                                                    (static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry)));

    for (int i = 0; i < leaf_count; i++) {
        blp[i].hashval = Be32::from_cpu(leaves[i].hash);
        blp[i].address = Be32::from_cpu(leaves[i].address);
    }

    // 3e. Free space between last data entry and leaf entries
    size_t const LEAF_AREA_START = BLKSIZE - sizeof(XfsDir2BlockTail) - (static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry));
    if (data_offset < LEAF_AREA_START) {
        size_t const FREE_LEN = LEAF_AREA_START - data_offset;
        auto* unused = reinterpret_cast<XfsDir2DataUnused*>(block + data_offset);
        unused->freetag = Be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
        unused->length = Be16::from_cpu(static_cast<uint16_t>(FREE_LEN));
        // Unused tail tag
        auto* unused_tag = reinterpret_cast<Be16*>(block + data_offset + FREE_LEN - sizeof(Be16));
        *unused_tag = Be16::from_cpu(static_cast<uint16_t>(data_offset));

        // Update best_free in the header
        hdr3->best_free.at(0).offset = Be16::from_cpu(static_cast<uint16_t>(data_offset));
        hdr3->best_free.at(0).length = Be16::from_cpu(static_cast<uint16_t>(FREE_LEN));
    }

    // 3f. Compute CRC over the block
    hdr3->hdr.crc = Be32{0};
    uint32_t const CRC = util::crc32c_block_with_cksum(block, BLKSIZE, 4);  // crc at offset 4 in XfsDir3BlkHdr
    hdr3->hdr.crc = Be32::from_cpu(CRC);

    // Write the block to disk IMMEDIATELY.  dir2_block_addname (which runs
    // in the same transaction, before commit) will re-read this block from
    // disk.  Since multi-block buffers aren't cached, logging the buffer in
    // the transaction is insufficient - we must ensure the data is on disk
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
    dp->data_fork.extents.list[0].br_startblock = DISK_BLOCK;
    dp->data_fork.extents.list[0].br_blockcount = FBS;
    dp->data_fork.extents.list[0].br_unwritten = false;
    dp->data_fork.extents.count = 1;
    dp->data_fork.extents.capacity = 1;
    dp->nextents = 1;
    dp->nblocks = FBS;
    dp->size = BLKSIZE;
    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);
#ifdef XFS_DEBUG
    mod::dbg::log("[xfs] dir sf->block conversion complete: ino=%lu blk=%lu entries=%d", static_cast<unsigned long>(dp->ino),
                  static_cast<unsigned long>(disk_block), total_entries);
#endif
    return 0;
}

auto dir2_block_to_leaf(XfsInode* dp, XfsTransaction* tp) -> int {
    XfsMountContext* ctx = dp->mount;
    size_t const BLKSIZE = ctx->dir_blk_size;
    size_t const DATA_START = sizeof(XfsDir3DataHdr);

    BufHead* block_bh = nullptr;
    int rc = dir2_read_block(dp, 0, &block_bh);
    if (rc != 0) {
        return rc;
    }

    uint8_t* block = block_bh->data;
    auto* data_hdr = reinterpret_cast<XfsDir3DataHdr*>(block);
    if (data_hdr->hdr.magic.to_cpu() != XFS_DIR3_BLOCK_MAGIC) {
        brelse(block_bh);
        return -EINVAL;
    }

    auto* btp = reinterpret_cast<XfsDir2BlockTail*>(block + BLKSIZE - sizeof(XfsDir2BlockTail));
    uint32_t const LEAF_COUNT = btp->count.to_cpu();
    uint32_t const STALE_COUNT = btp->stale.to_cpu();
    size_t const LEAF_BYTES = static_cast<size_t>(LEAF_COUNT) * sizeof(XfsDir2LeafEntry);
    if (STALE_COUNT > LEAF_COUNT) {
        brelse(block_bh);
        return -EINVAL;
    }
    if (LEAF_BYTES > BLKSIZE - DATA_START - sizeof(XfsDir2BlockTail)) {
        brelse(block_bh);
        return -EINVAL;
    }

    size_t const OLD_LEAF_START = BLKSIZE - sizeof(XfsDir2BlockTail) - LEAF_BYTES;
    if (OLD_LEAF_START < DATA_START) {
        brelse(block_bh);
        return -EINVAL;
    }

    size_t const LEAF_CAPACITY = (BLKSIZE - sizeof(XfsDir3LeafHdr)) / sizeof(XfsDir2LeafEntry);
    if (LEAF_COUNT > LEAF_CAPACITY || LEAF_CAPACITY > UINT16_MAX) {
        brelse(block_bh);
        return -ENOSPC;
    }

    auto* leaf_copy = new (std::nothrow) XfsDir2LeafEntry[LEAF_CAPACITY];
    if (leaf_copy == nullptr) {
        brelse(block_bh);
        return -ENOMEM;
    }

    const auto* old_leaf = reinterpret_cast<const XfsDir2LeafEntry*>(block + OLD_LEAF_START);
    for (uint32_t i = 0; i < LEAF_COUNT; i++) {
        leaf_copy[i] = old_leaf[i];
    }

    xfs_fsblock_t new_data_disk = 0;
    rc = dir2_alloc_mapped_dir_block(dp, tp, dir2_db_to_fsbno(ctx, 1), &new_data_disk);
    if (rc != 0) {
        delete[] leaf_copy;
        brelse(block_bh);
        return rc;
    }

    BufHead* new_data_bh = dir2_buf_read_dir_block(ctx, new_data_disk);
    if (new_data_bh == nullptr) {
        delete[] leaf_copy;
        brelse(block_bh);
        return -EIO;
    }

    uint8_t* new_data = new_data_bh->data;
    __builtin_memset(new_data, 0, BLKSIZE);
    dir2_init_data_header(dp, new_data_disk, XFS_DIR3_DATA_MAGIC, new_data);
    dir2_make_data_free(ctx, new_data, DATA_START, BLKSIZE, DATA_START, BLKSIZE - DATA_START);
    dir2_recompute_data_crc(new_data, BLKSIZE);
    xfs_trans_log_buf_full(tp, new_data_bh);
    bwrite(new_data_bh);
    brelse(new_data_bh);

    xfs_fileoff_t const LEAF_FSBNO = XFS_DIR2_LEAF_OFFSET >> ctx->block_log;
    xfs_fsblock_t leaf_disk = 0;
    rc = dir2_alloc_mapped_dir_block(dp, tp, LEAF_FSBNO, &leaf_disk);
    if (rc != 0) {
        delete[] leaf_copy;
        brelse(block_bh);
        return rc;
    }

    BufHead* leaf_bh = dir2_buf_read_dir_block(ctx, leaf_disk);
    if (leaf_bh == nullptr) {
        delete[] leaf_copy;
        brelse(block_bh);
        return -EIO;
    }

    uint8_t* leaf_block = leaf_bh->data;
    __builtin_memset(leaf_block, 0, BLKSIZE);
    auto* leaf_hdr = reinterpret_cast<XfsDir3LeafHdr*>(leaf_block);
    dir2_init_leaf_header(dp, leaf_disk, leaf_hdr, LEAF_COUNT, STALE_COUNT);
    __builtin_memcpy(dir2_leaf_entries(leaf_hdr), leaf_copy, LEAF_BYTES);
    dir2_recompute_leaf_crc(leaf_block, BLKSIZE);
    xfs_trans_log_buf_full(tp, leaf_bh);
    bwrite(leaf_bh);
    brelse(leaf_bh);

    data_hdr->hdr.magic = Be32::from_cpu(XFS_DIR3_DATA_MAGIC);
    dir2_make_data_free(ctx, block, DATA_START, BLKSIZE, OLD_LEAF_START, BLKSIZE - OLD_LEAF_START);
    dir2_recompute_data_crc(block, BLKSIZE);
    xfs_trans_log_buf_full(tp, block_bh);
    bwrite(block_bh);
    brelse(block_bh);

    dp->size = 2ULL * static_cast<uint64_t>(ctx->dir_blk_size);
    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);

    delete[] leaf_copy;
    return 0;
}

// Add a name to a block-format directory (single directory block).
auto dir2_block_addname(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype, XfsTransaction* tp) -> int {
    XfsMountContext const* ctx = dp->mount;

    // Read the single directory block
    BufHead* bh = nullptr;
    int const RC = dir2_read_block(dp, 0, &bh);
    if (RC != 0) {
        return RC;
    }

    uint8_t* block = bh->data;
    size_t const BLKSIZE = ctx->dir_blk_size;

    // Validate magic
    const auto* hdr = reinterpret_cast<const XfsDir3DataHdr*>(block);
    uint32_t const MAGIC = hdr->hdr.magic.to_cpu();
    if (MAGIC != XFS_DIR3_BLOCK_MAGIC) {
        brelse(bh);
        return -EINVAL;
    }

    // Compute the entry size needed
    bool const HAS_FTYPE_FLAG = xfs_has_ftype(ctx);
    size_t const NEED_LEN = dir2_data_entsize(ctx, namelen);

    // Block tail is at the very end of the block
    auto* btp = reinterpret_cast<XfsDir2BlockTail*>(block + BLKSIZE - sizeof(XfsDir2BlockTail));
    uint32_t leaf_count = btp->count.to_cpu();

    // Leaf entries are just before the tail
    uint8_t* leaf_start = block + BLKSIZE - sizeof(XfsDir2BlockTail) - (static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry));

    // Scan data area for a free space entry large enough
    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    auto const DATA_END = static_cast<size_t>(leaf_start - block);

    size_t offset = DATA_START;
    size_t found_offset = 0;
    bool found_free = false;

    while (offset < DATA_END) {
        const auto* unused = reinterpret_cast<const XfsDir2DataUnused*>(block + offset);
        if (unused->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
            uint16_t const FREE_LEN = unused->length.to_cpu();
            if (FREE_LEN == 0 || offset + FREE_LEN > DATA_END) {
                break;
            }
            if (FREE_LEN >= NEED_LEN) {
                found_offset = offset;
                found_free = true;
                break;
            }
            offset += FREE_LEN;
            continue;
        }

        const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + offset);
        if (dep->namelen == 0) {
            break;
        }
        offset += dir2_data_entsize(ctx, dep->namelen);
    }

    if (!found_free) {
        brelse(bh);
        return -ENOSPC;  // block full - would need conversion to leaf format
    }

    // Write the new data entry at found_offset
    const auto* old_unused = reinterpret_cast<const XfsDir2DataUnused*>(block + found_offset);
    uint16_t const OLD_FREE_LEN = old_unused->length.to_cpu();

    auto* dep = reinterpret_cast<XfsDir2DataEntry*>(block + found_offset);
    dep->inumber = Be64::from_cpu(ino);
    dep->namelen = static_cast<uint8_t>(namelen);
    __builtin_memcpy(xfs_dir2_data_entry_name(dep), name, namelen);

    // ftype + tag
    size_t tag_off = 8 + 1 + namelen;  // inumber(8) + namelen(1) + name
    if (HAS_FTYPE_FLAG) {
        xfs_dir2_data_entry_name(dep)[namelen] = ftype;
        tag_off++;
    }
    // Tag (starting offset within the block, stored as Be16)
    auto tag_val = static_cast<uint16_t>(found_offset);
    // Pad to 8-byte alignment before writing tag
    // The tag is at the end of the padded entry, at entry_end - 2
    size_t const ENTRY_END = found_offset + NEED_LEN;
    auto* tag_loc = reinterpret_cast<Be16*>(block + ENTRY_END - sizeof(Be16));
    *tag_loc = Be16::from_cpu(tag_val);

    // Zero pad the entry between ftype/name end and tag
    size_t const USED_BYTES = tag_off;
    size_t const PAD_START = found_offset + USED_BYTES;
    size_t const PAD_END = ENTRY_END - sizeof(Be16);
    if (PAD_END > PAD_START) {
        __builtin_memset(block + PAD_START, 0, PAD_END - PAD_START);
    }

    // If there's remaining free space after our entry, create a new unused entry
    if (OLD_FREE_LEN > NEED_LEN) {
        size_t const REMAINING = OLD_FREE_LEN - NEED_LEN;
        auto* new_unused = reinterpret_cast<XfsDir2DataUnused*>(block + ENTRY_END);
        new_unused->freetag = Be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
        new_unused->length = Be16::from_cpu(static_cast<uint16_t>(REMAINING));
        // Tag for unused entry: at the very end
        auto* unused_tag = reinterpret_cast<Be16*>(block + ENTRY_END + REMAINING - sizeof(Be16));
        *unused_tag = Be16::from_cpu(static_cast<uint16_t>(ENTRY_END));
    }

    // Add a leaf entry for the new name
    xfs_dahash_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);

    // Compute the dataptr for this entry
    // dataptr = (byte_offset_in_dir_block) >> XFS_DIR2_DATA_ALIGN_LOG
    auto dataptr = static_cast<uint32_t>(found_offset >> XFS_DIR2_DATA_ALIGN_LOG);

    // We need to insert a new leaf entry into the sorted leaf array.
    // First, check if we can reclaim a stale entry.
    auto* blp = reinterpret_cast<XfsDir2LeafEntry*>(leaf_start);
    uint32_t const STALE_COUNT = btp->stale.to_cpu();

    if (STALE_COUNT > 0) {
        // Find a stale entry to reuse - find one nearest the correct sorted position
        int insert_pos = 0;
        for (int i = 0; std::cmp_less(i, leaf_count); i++) {
            if (blp[i].address.to_cpu() != XFS_DIR2_NULL_DATAPTR && blp[i].hashval.to_cpu() <= HASH) {
                insert_pos = i + 1;
            }
        }

        // Find nearest stale entry
        int stale_idx = -1;
        int best_dist = static_cast<int>(leaf_count);
        for (int i = 0; std::cmp_less(i, leaf_count); i++) {
            if (blp[i].address.to_cpu() == XFS_DIR2_NULL_DATAPTR) {
                int const DIST = (i >= insert_pos) ? (i - insert_pos) : (insert_pos - i);
                if (DIST < best_dist) {
                    best_dist = DIST;
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
            blp[insert_pos].hashval = Be32::from_cpu(HASH);
            blp[insert_pos].address = Be32::from_cpu(dataptr);
            btp->stale = Be32::from_cpu(STALE_COUNT - 1);
        }
    } else {
        // No stale entries - grow the leaf area by shifting it down one slot.
        // This consumes sizeof(XfsDir2LeafEntry) bytes from the free space.
        size_t const NEW_LEAF_BYTES = (static_cast<size_t>(leaf_count) + 1) * sizeof(XfsDir2LeafEntry);
        size_t const NEW_LEAF_START = BLKSIZE - sizeof(XfsDir2BlockTail) - NEW_LEAF_BYTES;

        // Verify there's enough free space between data entries and the new leaf area
        if (found_offset + NEED_LEN > NEW_LEAF_START) {
            // Not enough room for both the data entry and the expanded leaf array
            brelse(bh);
            return -ENOSPC;
        }

        // Move the existing leaf array down by one entry
        auto* new_blp = reinterpret_cast<XfsDir2LeafEntry*>(block + NEW_LEAF_START);
        __builtin_memmove(new_blp, blp, static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry));
        blp = new_blp;

        // Find sorted insertion position
        int insert_pos = 0;
        for (int i = 0; std::cmp_less(i, leaf_count); i++) {
            if (blp[i].hashval.to_cpu() <= HASH) {
                insert_pos = i + 1;
            }
        }

        // Shift entries after insert_pos to make room
        for (int i = static_cast<int>(leaf_count); i > insert_pos; i--) {
            blp[i] = blp[i - 1];
        }

        blp[insert_pos].hashval = Be32::from_cpu(HASH);
        blp[insert_pos].address = Be32::from_cpu(dataptr);
        leaf_count++;
        btp->count = Be32::from_cpu(leaf_count);
    }

    // Update best_free in the header to reflect the remaining free space.
    // After inserting the data entry (consuming need_len bytes from the old
    // free region), there may be a leftover free region.
    auto* mutable_hdr = reinterpret_cast<XfsDir3DataHdr*>(block);
    {
        size_t const ENTRY_END = found_offset + NEED_LEN;
        // Compute where the leaf area now starts
        size_t const CURRENT_LEAF_START = BLKSIZE - sizeof(XfsDir2BlockTail) - (static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry));
        // The remaining free space is between entry_end and current_leaf_start
        // (only if a free-space marker was written there earlier, check it)
        mutable_hdr->best_free.at(0).offset = Be16{0};
        mutable_hdr->best_free.at(0).length = Be16{0};
        mutable_hdr->best_free.at(1).offset = Be16{0};
        mutable_hdr->best_free.at(1).length = Be16{0};
        mutable_hdr->best_free.at(2).offset = Be16{0};
        mutable_hdr->best_free.at(2).length = Be16{0};
        // Check the remainder area for a free-tag marker
        if (ENTRY_END < CURRENT_LEAF_START) {
            const auto* rem = reinterpret_cast<const XfsDir2DataUnused*>(block + ENTRY_END);
            if (rem->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
                uint16_t rem_len = rem->length.to_cpu();
                // The free region may need its length truncated if the leaf
                // array grew into it (leaf area shifted down).
                size_t const MAX_FREE = CURRENT_LEAF_START - ENTRY_END;
                if (rem_len > MAX_FREE) {
                    // Adjust free region length and re-write its tail tag
                    auto* adj_unused = reinterpret_cast<XfsDir2DataUnused*>(block + ENTRY_END);
                    adj_unused->length = Be16::from_cpu(static_cast<uint16_t>(MAX_FREE));
                    auto* adj_tag = reinterpret_cast<Be16*>(block + ENTRY_END + MAX_FREE - sizeof(Be16));
                    *adj_tag = Be16::from_cpu(static_cast<uint16_t>(ENTRY_END));
                    rem_len = static_cast<uint16_t>(MAX_FREE);
                }
                mutable_hdr->best_free.at(0).offset = Be16::from_cpu(static_cast<uint16_t>(ENTRY_END));
                mutable_hdr->best_free.at(0).length = Be16::from_cpu(rem_len);
            }
        }
    }

    // Recompute CRC over the entire block
    mutable_hdr->hdr.crc = Be32{0};
    uint32_t const CRC = util::crc32c_block_with_cksum(block, BLKSIZE, 4);
    mutable_hdr->hdr.crc = Be32::from_cpu(CRC);

    xfs_trans_log_buf_full(tp, bh);
    brelse(bh);

    // Update directory inode metadata
    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);

    return 0;
}

auto dir2_leaf_node_addname(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype, XfsTransaction* tp) -> int {
    XfsMountContext const* ctx = dp->mount;
    size_t const NEED_LEN = dir2_data_entsize(ctx, static_cast<uint8_t>(namelen));
    xfs_dahash_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);

    BufHead* leaf_bh = nullptr;
    int rc = dir2_read_leaf_block(dp, &leaf_bh);
    if (rc != 0) {
        return (rc == -ENOSYS) ? -ENOSPC : rc;
    }

    auto* leaf_hdr = reinterpret_cast<XfsDir3LeafHdr*>(leaf_bh->data);
    int stale_idx = -1;
    int insert_pos = 0;
    bool const UPDATE_LEAF = dir2_leaf_can_index_new_entry(ctx, leaf_hdr);

    BufHead* data_bh = nullptr;
    xfs_dir2_db_t db = 0;
    size_t free_off = 0;
    size_t free_len = 0;
    rc = dir2_leaf_node_find_free_region(dp, NEED_LEN, &data_bh, &db, &free_off, &free_len);
    if (rc == -ENOSPC) {
        rc = dir2_leaf_alloc_data_block(dp, tp, &data_bh, &db, &free_off, &free_len);
    }
    if (rc != 0) {
        brelse(leaf_bh);
        return rc;
    }

    xfs_dir2_dataptr_t const DATAPTR = dir2_db_off_to_dataptr(ctx, db, free_off);
    if (UPDATE_LEAF) {
        rc = dir2_leaf_ensure_stale_slot(ctx, leaf_hdr);
        if (rc == 0) {
            rc = dir2_leaf_prepare_stale_insert(leaf_hdr, HASH, &stale_idx, &insert_pos);
        }
        if (rc != 0) {
            brelse(data_bh);
            brelse(leaf_bh);
            return rc;
        }
    }

    uint8_t* data_block = data_bh->data;
    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    size_t const DATA_END = ctx->dir_blk_size;
    dir2_write_data_entry(ctx, data_block, DATA_START, DATA_END, free_off, free_len, name, namelen, ino, ftype);
    dir2_recompute_data_crc(data_block, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, data_bh);

    if (UPDATE_LEAF) {
        dir2_leaf_reuse_stale_entry(leaf_hdr, HASH, DATAPTR, stale_idx, insert_pos);
        dir2_recompute_leaf_crc(leaf_bh->data, ctx->dir_blk_size);
        xfs_trans_log_buf_full(tp, leaf_bh);
    }

    brelse(data_bh);
    brelse(leaf_bh);

    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);
    return 0;
}

// ============================================================================
// Directory remove-name - remove an entry from a directory
// ============================================================================

// Remove a name from a shortform directory (inline in inode data fork).
// Finds the entry and removes it, compacting the remaining entries.
auto dir2_sf_removename(XfsInode* dp, const char* name, uint16_t namelen, XfsTransaction* tp) -> int {
    XfsMountContext const* ctx = dp->mount;
    bool const HAS_FTYPE = xfs_has_ftype(ctx);

    const uint8_t* old_data = dp->data_fork.local.data;
    size_t const OLD_SIZE = dp->data_fork.local.size;

    size_t old_hdr_size = 0;
    if (!dir2_sf_header_size_if_valid(old_data, OLD_SIZE, &old_hdr_size)) {
        return -EINVAL;
    }

    const auto* old_hdr = reinterpret_cast<const XfsDir2SfHdr*>(old_data);
    size_t const HDR_SIZE = old_hdr_size;

    // Scan to find the entry to remove
    const uint8_t* ptr = old_data + HDR_SIZE;
    uint8_t const COUNT = old_hdr->count;
    size_t entry_offset_start = 0;
    size_t entry_size = 0;
    bool found = false;

    for (uint8_t i = 0; i < COUNT; i++) {
        if (ptr >= old_data + OLD_SIZE) {
            break;
        }

        entry_offset_start = ptr - old_data;
        const auto* sfep = reinterpret_cast<const XfsDir2SfEntry*>(ptr);
        uint8_t const ENTRY_NAMELEN = sfep->namelen;

        if (ENTRY_NAMELEN == namelen && __builtin_memcmp(xfs_dir2_sf_entry_name(sfep), name, namelen) == 0) {
            found = true;
            entry_size = sf_entry_size(old_hdr, sfep, HAS_FTYPE);
            break;
        }

        entry_size = sf_entry_size(old_hdr, sfep, HAS_FTYPE);
        ptr += entry_size;
    }

    if (!found) {
        return -ENOENT;
    }

    // Build new data fork without this entry
    size_t const NEW_SIZE = OLD_SIZE - entry_size;
    auto* new_data = new uint8_t[NEW_SIZE];
    if (new_data == nullptr) {
        return -ENOMEM;
    }

    // Copy header
    __builtin_memcpy(new_data, old_data, HDR_SIZE);

    // Update header: decrement count
    auto* new_hdr = reinterpret_cast<XfsDir2SfHdr*>(new_data);
    new_hdr->count--;

    // Copy entries before the removed entry
    if (entry_offset_start > HDR_SIZE) {
        __builtin_memcpy(new_data + HDR_SIZE, old_data + HDR_SIZE, entry_offset_start - HDR_SIZE);
    }

    // Copy entries after the removed entry
    size_t const AFTER_OFFSET = entry_offset_start + entry_size;
    if (AFTER_OFFSET < OLD_SIZE) {
        __builtin_memcpy(new_data + entry_offset_start, old_data + AFTER_OFFSET, OLD_SIZE - AFTER_OFFSET);
    }
    sf_repair_offset_tags(new_data, NEW_SIZE, ctx);

    // Replace the data fork
    delete[] dp->data_fork.local.data;
    dp->data_fork.local.data = new_data;
    dp->data_fork.local.size = NEW_SIZE;
    dp->size = NEW_SIZE;
    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);

    return 0;
}

// Remove a name from a block-format directory (single directory block).
auto dir2_block_removename(XfsInode* dp, const char* name, uint16_t namelen, XfsTransaction* tp) -> int {
    XfsMountContext const* ctx = dp->mount;

    // Read the single directory block
    BufHead* bh = nullptr;
    int const RC = dir2_read_block(dp, 0, &bh);
    if (RC != 0) {
        return RC;
    }

    uint8_t* block = bh->data;
    size_t const BLKSIZE = ctx->dir_blk_size;

    // Validate magic
    auto* hdr = reinterpret_cast<XfsDir3DataHdr*>(block);
    uint32_t const MAGIC = hdr->hdr.magic.to_cpu();
    if (MAGIC != XFS_DIR3_BLOCK_MAGIC) {
        brelse(bh);
        return -EINVAL;
    }

    // Block tail is at the very end of the block
    auto* btp = reinterpret_cast<XfsDir2BlockTail*>(block + BLKSIZE - sizeof(XfsDir2BlockTail));
    uint32_t const LEAF_COUNT = btp->count.to_cpu();

    size_t const LEAF_BYTES = static_cast<size_t>(LEAF_COUNT) * sizeof(XfsDir2LeafEntry);
    if (LEAF_BYTES > BLKSIZE - sizeof(XfsDir3DataHdr) - sizeof(XfsDir2BlockTail)) {
        brelse(bh);
        return -EINVAL;
    }

    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    size_t const DATA_END = BLKSIZE - sizeof(XfsDir2BlockTail) - LEAF_BYTES;
    if (DATA_END <= DATA_START) {
        brelse(bh);
        return -EINVAL;
    }

    // Leaf entries are just before the tail
    auto* blp = reinterpret_cast<XfsDir2LeafEntry*>(block + DATA_END);

    // Hash the name and find matching leaf entry
    xfs_dahash_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);

    int lo = 0;
    int hi = static_cast<int>(LEAF_COUNT) - 1;
    int mid = -1;
    int match_idx = -1;
    uint32_t entry_off = 0;
    size_t entry_size = 0;

    // Binary search for hash
    while (lo <= hi) {
        mid = (lo + hi) / 2;
        uint32_t const ENTRY_HASH = blp[mid].hashval.to_cpu();

        if (HASH < ENTRY_HASH) {
            hi = mid - 1;
        } else if (HASH > ENTRY_HASH) {
            lo = mid + 1;
        } else {
            // Back up to first with this hash
            while (mid > 0 && blp[mid - 1].hashval.to_cpu() == HASH) {
                mid--;
            }
            break;
        }
    }

    // Scan all entries with matching hash to find the name
    if (lo <= hi || (mid >= 0 && std::cmp_less(mid, LEAF_COUNT) && blp[mid].hashval.to_cpu() == HASH)) {
        int const START_IDX = (mid >= 0) ? mid : lo;
        for (int i = START_IDX; std::cmp_less(i, LEAF_COUNT); i++) {
            if (blp[i].hashval.to_cpu() != HASH) {
                break;
            }

            xfs_dir2_dataptr_t const ADDR = blp[i].address.to_cpu();
            if (ADDR == XFS_DIR2_NULL_DATAPTR) {
                continue;  // stale
            }

            uint32_t const OFF = dir2_dataptr_to_off(ctx, ADDR);
            if (OFF < DATA_START || OFF + sizeof(XfsDir2DataEntry) > DATA_END) {
                continue;
            }

            const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + OFF);
            size_t const DEP_SIZE = dir2_data_entsize(ctx, dep->namelen);
            if (dep->namelen == 0 || DEP_SIZE == 0 || OFF + DEP_SIZE > DATA_END) {
                continue;
            }

            if (dep->namelen == namelen && __builtin_memcmp(xfs_dir2_data_entry_name(dep), name, namelen) == 0) {
                match_idx = i;
                entry_off = OFF;
                entry_size = DEP_SIZE;
                break;
            }
        }
    }

    if (match_idx < 0) {
        brelse(bh);
        return -ENOENT;
    }

    // Mark the leaf entry as stale (address = XFS_DIR2_NULL_DATAPTR)
    blp[match_idx].address = Be32::from_cpu(XFS_DIR2_NULL_DATAPTR);
    uint32_t const STALE_COUNT = btp->stale.to_cpu();
    btp->stale = Be32::from_cpu(STALE_COUNT + 1);

    // Convert the data entry to free space
    auto* unused = reinterpret_cast<XfsDir2DataUnused*>(block + entry_off);
    unused->freetag = Be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
    unused->length = Be16::from_cpu(static_cast<uint16_t>(entry_size));

    // Write tag at end of free space
    auto* tag = reinterpret_cast<Be16*>(block + entry_off + entry_size - sizeof(Be16));
    *tag = Be16::from_cpu(static_cast<uint16_t>(entry_off));

    // Find immediate neighboring free regions by safely walking the data area.
    size_t prev_free_off = 0;
    size_t prev_free_len = 0;
    bool prev_found = false;
    size_t next_free_len = 0;
    bool next_found = false;

    {
        size_t off = DATA_START;
        while (off < DATA_END) {
            if (off + sizeof(XfsDir2DataUnused) > DATA_END) {
                break;
            }

            const auto* ent = reinterpret_cast<const XfsDir2DataUnused*>(block + off);
            if (ent->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
                uint16_t const FREE_LEN = ent->length.to_cpu();
                if (FREE_LEN == 0 || off + FREE_LEN > DATA_END) {
                    break;
                }

                if (off + FREE_LEN == entry_off) {
                    prev_free_off = off;
                    prev_free_len = FREE_LEN;
                    prev_found = true;
                } else if (off == entry_off + entry_size) {
                    next_free_len = FREE_LEN;
                    next_found = true;
                }

                off += FREE_LEN;
                continue;
            }

            const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + off);
            size_t const DEP_SIZE = dir2_data_entsize(ctx, dep->namelen);
            if (dep->namelen == 0 || DEP_SIZE == 0 || off + DEP_SIZE > DATA_END) {
                break;
            }
            off += DEP_SIZE;
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
    merged->freetag = Be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
    merged->length = Be16::from_cpu(static_cast<uint16_t>(merged_len));
    auto* merged_tag = reinterpret_cast<Be16*>(block + merged_off + merged_len - sizeof(Be16));
    *merged_tag = Be16::from_cpu(static_cast<uint16_t>(merged_off));

    // Rebuild top-3 best free regions from a validated linear walk.
    struct BestFreeSlot {
        uint16_t off;
        uint16_t len;
    };
    std::array<BestFreeSlot, 3> best{{{.off = 0, .len = 0}, {.off = 0, .len = 0}, {.off = 0, .len = 0}}};

    {
        size_t off = DATA_START;
        while (off < DATA_END) {
            if (off + sizeof(XfsDir2DataUnused) > DATA_END) {
                break;
            }

            const auto* ent = reinterpret_cast<const XfsDir2DataUnused*>(block + off);
            if (ent->freetag.to_cpu() == XFS_DIR2_DATA_FREE_TAG) {
                uint16_t const FREE_LEN = ent->length.to_cpu();
                if (FREE_LEN == 0 || off + FREE_LEN > DATA_END) {
                    break;
                }

                BestFreeSlot const CUR{.off = static_cast<uint16_t>(off), .len = FREE_LEN};
                for (int idx = 0; idx < 3; idx++) {
                    if (CUR.len > best.at(static_cast<size_t>(idx)).len) {
                        for (int j = 2; j > idx; j--) {
                            best.at(static_cast<size_t>(j)) = best.at(static_cast<size_t>(j - 1));
                        }
                        best.at(static_cast<size_t>(idx)) = CUR;
                        break;
                    }
                }

                off += FREE_LEN;
                continue;
            }

            const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + off);
            size_t const DEP_SIZE = dir2_data_entsize(ctx, dep->namelen);
            if (dep->namelen == 0 || DEP_SIZE == 0 || off + DEP_SIZE > DATA_END) {
                break;
            }
            off += DEP_SIZE;
        }
    }

    for (int i = 0; i < 3; i++) {
        hdr->best_free.at(static_cast<size_t>(i)).offset = Be16::from_cpu(best.at(static_cast<size_t>(i)).off);
        hdr->best_free.at(static_cast<size_t>(i)).length = Be16::from_cpu(best.at(static_cast<size_t>(i)).len);
    }

    // Update CRC over the block
    hdr->hdr.crc = Be32{0};
    uint32_t const CRC = util::crc32c_block_with_cksum(block, BLKSIZE, 4);
    hdr->hdr.crc = Be32::from_cpu(CRC);

    xfs_trans_log_buf_full(tp, bh);
    brelse(bh);

    return 0;
}

auto dir2_leaf_node_removename(XfsInode* dp, const char* name, uint16_t namelen, XfsTransaction* tp) -> int {
    XfsMountContext const* ctx = dp->mount;

    BufHead* data_bh = nullptr;
    xfs_dir2_db_t db = 0;
    size_t entry_off = 0;
    size_t entry_size = 0;
    int rc = dir2_leaf_node_find_data_entry(dp, name, namelen, &data_bh, &db, &entry_off, &entry_size);
    if (rc != 0) {
        return rc;
    }

    BufHead* leaf_bh = nullptr;
    rc = dir2_read_leaf_block(dp, &leaf_bh);
    if (rc != 0) {
        brelse(data_bh);
        return rc;
    }

    xfs_dahash_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);
    xfs_dir2_dataptr_t const DATAPTR = dir2_db_off_to_dataptr(ctx, db, entry_off);
    auto* leaf_hdr = reinterpret_cast<XfsDir3LeafHdr*>(leaf_bh->data);
    int leaf_idx = -1;
    rc = dir2_leaf_find_dataptr(leaf_hdr, HASH, DATAPTR, &leaf_idx);
    if (rc == 0) {
        auto* lep = dir2_leaf_entries(leaf_hdr);
        lep[leaf_idx].address = Be32::from_cpu(XFS_DIR2_NULL_DATAPTR);
        uint16_t const STALE_COUNT = leaf_hdr->stale.to_cpu();
        leaf_hdr->stale = Be16::from_cpu(static_cast<uint16_t>(STALE_COUNT + 1));
        dir2_recompute_leaf_crc(leaf_bh->data, ctx->dir_blk_size);
        xfs_trans_log_buf_full(tp, leaf_bh);
    }

    uint8_t* data_block = data_bh->data;
    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    size_t const DATA_END = ctx->dir_blk_size;
    dir2_make_data_free(ctx, data_block, DATA_START, DATA_END, entry_off, entry_size);
    dir2_recompute_data_crc(data_block, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, data_bh);

    brelse(leaf_bh);
    brelse(data_bh);
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
// Shortform is full - convert to block format, then add there.
#ifdef XFS_DEBUG
                mod::dbg::log("[xfs] dir_addname: shortform dir full, converting to block format");
#endif
                rc = dir2_sf_to_block(dp, tp);
                if (rc != 0) {
#ifdef XFS_DEBUG
                    mod::dbg::log("[xfs] dir_addname: sf->block conversion failed: %d", rc);
#endif
                    return rc;
                }
                // Now it's a block-format directory; add the new entry there.
                rc = dir2_block_addname(dp, name, namelen, ino, ftype, tp);
            }
            break;
        }

        case XFS_DINODE_FMT_EXTENTS:
        case XFS_DINODE_FMT_BTREE: {
            if (dir2_is_single_block_dir(dp)) {
                rc = dir2_block_addname(dp, name, namelen, ino, ftype, tp);
                if (rc == -ENOSPC) {
                    rc = dir2_block_to_leaf(dp, tp);
                    if (rc == 0) {
                        rc = dir2_leaf_node_addname(dp, name, namelen, ino, ftype, tp);
                    }
                }
            } else {
                rc = dir2_leaf_node_addname(dp, name, namelen, ino, ftype, tp);
            }
            break;
        }

        default:
            return -EINVAL;
    }
    if (rc == 0) {
        xfs_dentry_cache_store_added_name(dp, name, namelen, ino, ftype);
    }
    return rc;
}

// Remove a name from a directory.  Dispatches to the appropriate format handler.
auto xfs_dir_removename(XfsInode* dp, const char* name, uint16_t namelen, XfsTransaction* tp) -> int {
    if (dp == nullptr || name == nullptr || namelen == 0 || tp == nullptr) {
        return -EINVAL;
    }
    if (!xfs_inode_isdir(dp)) {
        return -ENOTDIR;
    }

    int rc = 0;
    switch (dp->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL: {
            rc = dir2_sf_removename(dp, name, namelen, tp);
            break;
        }

        case XFS_DINODE_FMT_EXTENTS:
        case XFS_DINODE_FMT_BTREE: {
            if (dir2_is_single_block_dir(dp)) {
                rc = dir2_block_removename(dp, name, namelen, tp);
            } else {
                rc = dir2_leaf_node_removename(dp, name, namelen, tp);
            }
            break;
        }

        default:
            return -EINVAL;
    }
    if (rc == 0) {
        xfs_dentry_cache_invalidate_dir(dp);
    }
    return rc;
}

void xfs_dentry_cache_stats(XfsDentryCacheStats& out) {
    out.hits = g_xfs_dentry_hits.load(std::memory_order_relaxed);
    out.misses = g_xfs_dentry_misses.load(std::memory_order_relaxed);
    out.stores = g_xfs_dentry_stores.load(std::memory_order_relaxed);
    out.invalidations = g_xfs_dentry_invalidations.load(std::memory_order_relaxed);
}

void xfs_dentry_cache_purge_mount(XfsMountContext* mount) {
    if (mount == nullptr) {
        return;
    }

    uint64_t const IRQF = g_xfs_dentry_cache_lock.lock_irqsave();
    for (auto& set : g_xfs_dentry_cache) {
        for (auto& candidate : set.ways) {
            if (candidate.valid && candidate.mount == mount) {
                candidate.valid = false;
            }
        }
    }
    for (auto& set : g_xfs_dentry_generations) {
        for (auto& candidate : set.ways) {
            if (candidate.valid && candidate.mount == mount) {
                candidate.valid = false;
            }
        }
    }
    g_xfs_dentry_cache_lock.unlock_irqrestore(IRQF);
    g_xfs_dentry_invalidations.fetch_add(1, std::memory_order_relaxed);
}

#ifdef WOS_SELFTEST
void xfs_selftest_make_shortform_one_entry_dir(XfsMountContext* mount, XfsInode* dir, std::array<uint8_t, 32>& data, xfs_ino_t dir_ino,
                                               xfs_ino_t child_ino) {
    data.fill(0);
    auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data.data());
    hdr->count = 1;
    hdr->i8count = 0;
    hdr->parent.at(3) = 7;

    uint8_t* p = data.data() + xfs_dir2_sf_hdr_size(hdr);
    auto* sfep = reinterpret_cast<XfsDir2SfEntry*>(p);
    sfep->namelen = 3;
    sfep->offset.at(0) = 0;
    sfep->offset.at(1) = 1;
    std::memcpy(xfs_dir2_sf_entry_name(sfep), "foo", 3);
    uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + 3;
    *ino_ptr++ = XFS_DIR3_FT_REG_FILE;
    auto child_ino32 = static_cast<uint32_t>(child_ino);
    ino_ptr[0] = static_cast<uint8_t>((child_ino32 >> 24U) & 0xffU);
    ino_ptr[1] = static_cast<uint8_t>((child_ino32 >> 16U) & 0xffU);
    ino_ptr[2] = static_cast<uint8_t>((child_ino32 >> 8U) & 0xffU);
    ino_ptr[3] = static_cast<uint8_t>(child_ino32 & 0xffU);

    dir->ino = dir_ino;
    dir->mount = mount;
    dir->mode = 0040755;
    dir->data_fork.format = XFS_DINODE_FMT_LOCAL;
    dir->data_fork.local.data = data.data();
    dir->data_fork.local.size = static_cast<size_t>((ino_ptr + 4) - data.data());
    dir->size = dir->data_fork.local.size;
}

auto xfs_selftest_dentry_cache_shortform() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    xfs_dentry_cache_purge_mount(&mount);

    std::array<uint8_t, 32> data{};
    XfsInode dir{};
    xfs_selftest_make_shortform_one_entry_dir(&mount, &dir, data, 100, 42);

    XfsDentryCacheStats before{};
    xfs_dentry_cache_stats(before);

    XfsDirEntry entry{};
    if (xfs_dir_lookup(&dir, "foo", 3, &entry) != 0 || entry.ino != 42 || entry.ftype != XFS_DIR3_FT_REG_FILE) {
        return false;
    }
    if (xfs_dir_lookup(&dir, "foo", 3, &entry) != 0 || entry.ino != 42) {
        return false;
    }

    XfsDentryCacheStats after_hit{};
    xfs_dentry_cache_stats(after_hit);
    if (after_hit.hits <= before.hits || after_hit.stores <= before.stores) {
        return false;
    }

    xfs_dentry_cache_invalidate_dir(&dir);
    if (xfs_dir_lookup(&dir, "foo", 3, &entry) != 0) {
        return false;
    }

    XfsDentryCacheStats after_invalidate{};
    xfs_dentry_cache_stats(after_invalidate);
    return after_invalidate.misses > after_hit.misses && after_invalidate.invalidations > after_hit.invalidations;
}

auto xfs_selftest_dentry_cache_keeps_unrelated_dir_hot() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    xfs_dentry_cache_purge_mount(&mount);

    std::array<uint8_t, 32> dir_a_data{};
    std::array<uint8_t, 32> dir_b_data{};
    XfsInode dir_a{};
    XfsInode dir_b{};
    xfs_selftest_make_shortform_one_entry_dir(&mount, &dir_a, dir_a_data, 100, 42);
    xfs_selftest_make_shortform_one_entry_dir(&mount, &dir_b, dir_b_data, 200, 84);

    XfsDirEntry entry{};
    if (xfs_dir_lookup(&dir_a, "foo", 3, &entry) != 0 || entry.ino != 42) {
        return false;
    }
    if (xfs_dir_lookup(&dir_b, "foo", 3, &entry) != 0 || entry.ino != 84) {
        return false;
    }

    XfsDentryCacheStats before_invalidate{};
    xfs_dentry_cache_stats(before_invalidate);
    xfs_dentry_cache_invalidate_dir(&dir_a);

    if (xfs_dir_lookup(&dir_b, "foo", 3, &entry) != 0 || entry.ino != 84) {
        return false;
    }
    XfsDentryCacheStats after_unrelated_lookup{};
    xfs_dentry_cache_stats(after_unrelated_lookup);
    if (after_unrelated_lookup.hits <= before_invalidate.hits) {
        return false;
    }

    if (xfs_dir_lookup(&dir_a, "foo", 3, &entry) != 0 || entry.ino != 42) {
        return false;
    }
    XfsDentryCacheStats after_mutated_lookup{};
    xfs_dentry_cache_stats(after_mutated_lookup);
    return after_mutated_lookup.misses > after_unrelated_lookup.misses;
}

auto xfs_selftest_dentry_cache_add_keeps_sibling_hot() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    xfs_dentry_cache_purge_mount(&mount);

    std::array<uint8_t, 32> data{};
    XfsInode dir{};
    xfs_selftest_make_shortform_one_entry_dir(&mount, &dir, data, 100, 42);

    auto* heap_data = new uint8_t[dir.data_fork.local.size];
    if (heap_data == nullptr) {
        return false;
    }
    std::memcpy(heap_data, dir.data_fork.local.data, dir.data_fork.local.size);
    dir.data_fork.local.data = heap_data;

    auto cleanup = [&dir]() {
        delete[] dir.data_fork.local.data;
        dir.data_fork.local.data = nullptr;
    };

    XfsDirEntry entry{};
    if (xfs_dir_lookup(&dir, "foo", 3, &entry) != 0 || entry.ino != 42) {
        cleanup();
        return false;
    }
    if (xfs_dir_lookup(&dir, "foo", 3, &entry) != 0 || entry.ino != 42) {
        cleanup();
        return false;
    }

    XfsDentryCacheStats before_add{};
    xfs_dentry_cache_stats(before_add);

    XfsTransaction tp{};
    tp.mount = &mount;
    if (xfs_dir_addname(&dir, "bar", 3, 84, XFS_DIR3_FT_REG_FILE, &tp) != 0) {
        cleanup();
        return false;
    }

    XfsDentryCacheStats after_add{};
    xfs_dentry_cache_stats(after_add);
    if (after_add.invalidations != before_add.invalidations || after_add.stores < before_add.stores + 2) {
        cleanup();
        return false;
    }

    if (xfs_dir_lookup(&dir, "foo", 3, &entry) != 0 || entry.ino != 42) {
        cleanup();
        return false;
    }
    XfsDentryCacheStats after_sibling_lookup{};
    xfs_dentry_cache_stats(after_sibling_lookup);
    if (after_sibling_lookup.hits <= after_add.hits) {
        cleanup();
        return false;
    }

    if (xfs_dir_lookup(&dir, "bar", 3, &entry) != 0 || entry.ino != 84 || entry.ftype != XFS_DIR3_FT_REG_FILE) {
        cleanup();
        return false;
    }
    XfsDentryCacheStats after_added_lookup{};
    xfs_dentry_cache_stats(after_added_lookup);
    bool const OK = after_added_lookup.hits > after_sibling_lookup.hits;
    cleanup();
    return OK;
}

auto xfs_selftest_shortform_readdir_cookies_are_monotonic() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;

    std::array<uint8_t, 192> data{};
    auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data.data());
    hdr->count = 4;
    hdr->i8count = 0;
    hdr->parent.at(3) = 7;

    uint8_t* p = data.data() + xfs_dir2_sf_hdr_size(hdr);
    auto append_entry = [&](const char* name, uint16_t stored_offset, uint32_t ino) {
        auto const name_len = static_cast<uint8_t>(std::strlen(name));
        p[0] = name_len;
        p[1] = static_cast<uint8_t>(stored_offset >> 8U);
        p[2] = static_cast<uint8_t>(stored_offset & 0xffU);
        std::memcpy(p + 3, name, name_len);
        size_t cursor = 3 + name_len;
        p[cursor++] = XFS_DIR3_FT_REG_FILE;
        p[cursor++] = static_cast<uint8_t>((ino >> 24U) & 0xffU);
        p[cursor++] = static_cast<uint8_t>((ino >> 16U) & 0xffU);
        p[cursor++] = static_cast<uint8_t>((ino >> 8U) & 0xffU);
        p[cursor++] = static_cast<uint8_t>(ino & 0xffU);
        p += cursor;
    };

    // A rename sequence like Git index-pack's tmp_pack/tmp_rev/tmp_idx
    // finalization can leave duplicate shortform offset tags while the entries
    // are still ordered correctly in the in-memory data fork.
    append_entry("pack.keep", 4, 41);
    append_entry("pack.pack", 5, 42);
    append_entry("pack.rev", 5, 43);
    append_entry("pack.idx", 5, 44);

    XfsInode dir{};
    dir.ino = 100;
    dir.mount = &mount;
    dir.mode = 0040755;
    dir.data_fork.format = XFS_DINODE_FMT_LOCAL;
    dir.data_fork.local.data = data.data();
    dir.data_fork.local.size = static_cast<size_t>(p - data.data());
    dir.size = dir.data_fork.local.size;

    struct CookieCheck {
        uint64_t last_cookie = 0;
        size_t real_entries = 0;
        bool ok = true;
    } check{};

    auto callback = [](const XfsDirEntry* entry, void* raw) -> int {
        auto* state = static_cast<CookieCheck*>(raw);
        if ((entry->namelen == 1 && std::strcmp(entry->name.data(), ".") == 0) ||
            (entry->namelen == 2 && std::strcmp(entry->name.data(), "..") == 0)) {
            return 0;
        }
        if (entry->cookie <= state->last_cookie) {
            state->ok = false;
            return 1;
        }
        state->last_cookie = entry->cookie;
        state->real_entries++;
        return 0;
    };

    int const RC = xfs_dir_iterate(&dir, callback, &check);
    return RC == 0 && check.ok && check.real_entries == 4;
}

auto xfs_selftest_shortform_readdir_resume_after_removals() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;

    constexpr uint8_t ENTRY_COUNT = 8;
    constexpr uint16_t DUPLICATE_OFFSET = 5;
    auto* data = new uint8_t[160];
    if (data == nullptr) {
        return false;
    }
    std::memset(data, 0, 160);

    auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data);
    hdr->count = ENTRY_COUNT;
    hdr->i8count = 0;
    hdr->parent.at(3) = 7;

    uint8_t* p = data + xfs_dir2_sf_hdr_size(hdr);
    for (uint8_t i = 0; i < ENTRY_COUNT; ++i) {
        auto* sfep = reinterpret_cast<XfsDir2SfEntry*>(p);
        sfep->namelen = 2;
        sf_entry_store_offset(sfep, DUPLICATE_OFFSET);
        xfs_dir2_sf_entry_name(sfep)[0] = 'f';
        xfs_dir2_sf_entry_name(sfep)[1] = static_cast<uint8_t>('0' + i);
        uint8_t* ino_ptr = xfs_dir2_sf_entry_name(sfep) + 2;
        *ino_ptr++ = XFS_DIR3_FT_REG_FILE;
        uint32_t const INO = static_cast<uint32_t>(40 + i);
        ino_ptr[0] = static_cast<uint8_t>((INO >> 24U) & 0xffU);
        ino_ptr[1] = static_cast<uint8_t>((INO >> 16U) & 0xffU);
        ino_ptr[2] = static_cast<uint8_t>((INO >> 8U) & 0xffU);
        ino_ptr[3] = static_cast<uint8_t>(INO & 0xffU);
        p = ino_ptr + 4;
    }

    XfsInode dir{};
    dir.ino = 100;
    dir.mount = &mount;
    dir.mode = 0040755;
    dir.data_fork.format = XFS_DINODE_FMT_LOCAL;
    dir.data_fork.local.data = data;
    dir.data_fork.local.size = static_cast<size_t>(p - data);
    dir.size = dir.data_fork.local.size;

    struct FirstBatch {
        uint64_t target_cookie = 0;
        size_t real_seen = 0;
    } first_batch{};

    auto first_callback = [](const XfsDirEntry* entry, void* raw) -> int {
        auto* state = static_cast<FirstBatch*>(raw);
        bool const IS_DOT = entry->name.at(0) == '.' && (entry->namelen == 1 || (entry->namelen == 2 && entry->name.at(1) == '.'));
        if (IS_DOT) {
            return 0;
        }
        if (state->real_seen == 4) {
            state->target_cookie = entry->cookie + 1;
            state->real_seen++;
            return 1;
        }
        state->real_seen++;
        return 0;
    };

    if (xfs_dir_iterate(&dir, first_callback, &first_batch) != 0 || first_batch.real_seen != 5 || first_batch.target_cookie == 0) {
        delete[] dir.data_fork.local.data;
        return false;
    }

    XfsTransaction tp{};
    tp.mount = &mount;
    for (uint8_t i = 0; i < 5; ++i) {
        char name[] = {'f', static_cast<char>('0' + i), '\0'};
        if (dir2_sf_removename(&dir, name, 2, &tp) != 0) {
            delete[] dir.data_fork.local.data;
            return false;
        }
    }

    struct ResumeCheck {
        uint64_t target_cookie = 0;
        bool found = false;
        bool found_expected = false;
    } resume{.target_cookie = first_batch.target_cookie};

    auto resume_callback = [](const XfsDirEntry* entry, void* raw) -> int {
        auto* state = static_cast<ResumeCheck*>(raw);
        bool const IS_DOT = entry->name.at(0) == '.' && (entry->namelen == 1 || (entry->namelen == 2 && entry->name.at(1) == '.'));
        if (IS_DOT || entry->cookie < state->target_cookie) {
            return 0;
        }
        state->found = true;
        state->found_expected = entry->namelen == 2 && entry->name.at(0) == 'f' && entry->name.at(1) == '5';
        return 1;
    };

    bool ok = xfs_dir_iterate(&dir, resume_callback, &resume) == 0 && resume.found && resume.found_expected;

    for (uint8_t i = 5; ok && i < ENTRY_COUNT; ++i) {
        char name[] = {'f', static_cast<char>('0' + i), '\0'};
        ok = dir2_sf_removename(&dir, name, 2, &tp) == 0;
    }

    if (ok) {
        int real_entries = 0;
        auto count_callback = [](const XfsDirEntry* entry, void* raw) -> int {
            auto* count = static_cast<int*>(raw);
            bool const IS_DOT = entry->name.at(0) == '.' && (entry->namelen == 1 || (entry->namelen == 2 && entry->name.at(1) == '.'));
            if (!IS_DOT) {
                (*count)++;
            }
            return 0;
        };
        ok = xfs_dir_iterate(&dir, count_callback, &real_entries) == 0 && real_entries == 0;
    }

    delete[] dir.data_fork.local.data;
    return ok;
}
#endif

}  // namespace ker::vfs::xfs
