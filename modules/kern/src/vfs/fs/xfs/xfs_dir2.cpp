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

namespace {

auto dir2_name_filter_second_hash(uint32_t hash) -> uint32_t {
    hash ^= hash >> 16U;
    hash *= 0x7feb352dU;
    hash ^= hash >> 15U;
    return hash;
}

void dir2_name_filter_add(XfsInode* dp, const char* name, uint16_t namelen) {
    if (dp == nullptr || name == nullptr || namelen == 0) {
        return;
    }

    constexpr size_t FILTER_BITS = XFS_DIR_NAME_FILTER_WORDS * 64;
    uint32_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);
    size_t const FIRST_BIT = HASH & (FILTER_BITS - 1);
    size_t const SECOND_BIT = dir2_name_filter_second_hash(HASH) & (FILTER_BITS - 1);
    dp->dir_name_filter[FIRST_BIT >> 6U] |= 1ULL << (FIRST_BIT & 63U);
    dp->dir_name_filter[SECOND_BIT >> 6U] |= 1ULL << (SECOND_BIT & 63U);
}

}  // anonymous namespace

void xfs_dir_name_filter_init_empty(XfsInode* dp) {
    if (dp == nullptr || !xfs_inode_isdir(dp)) {
        return;
    }

    for (auto& word : dp->dir_name_filter) {
        word = 0;
    }
    dp->dir_name_filter_complete = true;
    dir2_name_filter_add(dp, ".", 1);
    dir2_name_filter_add(dp, "..", 2);
}

auto xfs_dir_name_filter_known_absent(const XfsInode* dp, const char* name, uint16_t namelen) -> bool {
    if (dp == nullptr || name == nullptr || namelen == 0 || !xfs_inode_isdir(dp) || !dp->dir_name_filter_complete) {
        return false;
    }

    constexpr size_t FILTER_BITS = XFS_DIR_NAME_FILTER_WORDS * 64;
    uint32_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);
    size_t const FIRST_BIT = HASH & (FILTER_BITS - 1);
    size_t const SECOND_BIT = dir2_name_filter_second_hash(HASH) & (FILTER_BITS - 1);
    uint64_t const FIRST_MASK = 1ULL << (FIRST_BIT & 63U);
    uint64_t const SECOND_MASK = 1ULL << (SECOND_BIT & 63U);
    return (dp->dir_name_filter[FIRST_BIT >> 6U] & FIRST_MASK) == 0 || (dp->dir_name_filter[SECOND_BIT >> 6U] & SECOND_MASK) == 0;
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

auto dir2_data_first_offset(const XfsMountContext* ctx) -> size_t {
    return sizeof(XfsDir3DataHdr) + dir2_data_entsize(ctx, 1) + dir2_data_entsize(ctx, 2);
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

auto sf_repair_offset_tags(uint8_t* data, size_t data_size, const XfsMountContext* ctx) -> int {
    size_t hdr_size = 0;
    if (data == nullptr || ctx == nullptr || !dir2_sf_header_size_if_valid(data, data_size, &hdr_size)) {
        return -EINVAL;
    }

    auto* hdr = reinterpret_cast<XfsDir2SfHdr*>(data);
    bool const HAS_FTYPE = xfs_has_ftype(ctx);
    uint8_t* ptr = data + hdr_size;
    size_t next_offset = dir2_data_first_offset(ctx);

    for (uint8_t i = 0; i < hdr->count; i++) {
        if (ptr >= data + data_size) {
            return -EINVAL;
        }

        auto* sfep = reinterpret_cast<XfsDir2SfEntry*>(ptr);
        size_t const ENTRY_SIZE = sf_entry_size(hdr, sfep, HAS_FTYPE);
        if (sfep->namelen == 0 || ENTRY_SIZE == 0 || ptr + ENTRY_SIZE > data + data_size || next_offset > UINT16_MAX) {
            return -EINVAL;
        }

        size_t stored = sf_entry_stored_offset(sfep);
        if (stored < next_offset) {
            stored = next_offset;
            sf_entry_store_offset(sfep, static_cast<uint16_t>(stored));
        }
        next_offset = stored + dir2_data_entsize(ctx, sfep->namelen);
        if (next_offset > UINT16_MAX) {
            return -E2BIG;
        }
        ptr += ENTRY_SIZE;
    }

    return ptr == data + data_size ? 0 : -EINVAL;
}

auto sf_next_offset_tag(const uint8_t* data, size_t data_size, const XfsMountContext* ctx, uint16_t* offset_out) -> int {
    size_t hdr_size = 0;
    if (data == nullptr || ctx == nullptr || offset_out == nullptr || !dir2_sf_header_size_if_valid(data, data_size, &hdr_size)) {
        return -EINVAL;
    }

    const auto* hdr = reinterpret_cast<const XfsDir2SfHdr*>(data);
    bool const HAS_FTYPE = xfs_has_ftype(ctx);
    const uint8_t* ptr = data + hdr_size;
    size_t next_offset = dir2_data_first_offset(ctx);

    for (uint8_t i = 0; i < hdr->count; i++) {
        if (ptr >= data + data_size) {
            return -EINVAL;
        }

        const auto* sfep = reinterpret_cast<const XfsDir2SfEntry*>(ptr);
        size_t const ENTRY_SIZE = sf_entry_size(hdr, sfep, HAS_FTYPE);
        if (sfep->namelen == 0 || ENTRY_SIZE == 0 || ptr + ENTRY_SIZE > data + data_size) {
            return -EINVAL;
        }

        size_t const STORED = sf_entry_stored_offset(sfep);
        if (STORED < next_offset) {
            return -EINVAL;
        }
        next_offset = STORED + dir2_data_entsize(ctx, sfep->namelen);
        if (next_offset > UINT16_MAX) {
            return -E2BIG;
        }
        ptr += ENTRY_SIZE;
    }

    if (ptr != data + data_size) {
        return -EINVAL;
    }
    *offset_out = static_cast<uint16_t>(next_offset);
    return 0;
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

auto dir2_data_entry_at_if_valid(const XfsMountContext* ctx, const uint8_t* block, size_t offset, size_t data_end,
                                 const XfsDir2DataEntry** dep_out, size_t* dep_size_out) -> bool {
    if (ctx == nullptr || block == nullptr || offset > UINT16_MAX || offset + sizeof(XfsDir2DataEntry) > data_end) {
        return false;
    }

    const auto* dep = reinterpret_cast<const XfsDir2DataEntry*>(block + offset);
    size_t const DEP_SIZE = dir2_data_entsize(ctx, dep->namelen);
    if (dep->namelen == 0 || DEP_SIZE == 0 || offset + DEP_SIZE > data_end) {
        return false;
    }

    const auto* tag = reinterpret_cast<const Be16*>(block + offset + DEP_SIZE - sizeof(Be16));
    if (tag->to_cpu() != static_cast<uint16_t>(offset)) {
        return false;
    }

    if (dep_out != nullptr) {
        *dep_out = dep;
    }
    if (dep_size_out != nullptr) {
        *dep_size_out = DEP_SIZE;
    }
    return true;
}

auto dir2_data_unused_at_if_valid(const uint8_t* block, size_t offset, size_t data_end, uint16_t* free_len_out) -> bool {
    if (block == nullptr || offset > UINT16_MAX || offset + sizeof(XfsDir2DataUnused) > data_end) {
        return false;
    }

    const auto* unused = reinterpret_cast<const XfsDir2DataUnused*>(block + offset);
    uint16_t const FREE_LEN = unused->length.to_cpu();
    if (unused->freetag.to_cpu() != XFS_DIR2_DATA_FREE_TAG || FREE_LEN < sizeof(XfsDir2DataUnused) ||
        (FREE_LEN & (XFS_DIR2_DATA_ALIGN - 1)) != 0 || offset + FREE_LEN > data_end) {
        return false;
    }

    const auto* tag = reinterpret_cast<const Be16*>(block + offset + FREE_LEN - sizeof(Be16));
    if (tag->to_cpu() != static_cast<uint16_t>(offset)) {
        return false;
    }

    if (free_len_out != nullptr) {
        *free_len_out = FREE_LEN;
    }
    return true;
}

// Large source checkouts and compiler tree scans revisit far more directory
// names than small fixed tables can retain. Keep this static and bounded while
// reducing set churn in hot XFS path walks.
constexpr size_t XFS_DENTRY_CACHE_SET_COUNT = 65536;
constexpr size_t XFS_DENTRY_CACHE_WAYS = 4;
constexpr size_t XFS_DENTRY_GENERATION_SET_COUNT = 16384;
constexpr size_t XFS_DENTRY_GENERATION_WAYS = 4;
static_assert((XFS_DENTRY_CACHE_SET_COUNT & (XFS_DENTRY_CACHE_SET_COUNT - 1)) == 0);
static_assert((XFS_DENTRY_GENERATION_SET_COUNT & (XFS_DENTRY_GENERATION_SET_COUNT - 1)) == 0);

struct XfsDentryCacheEntry {
    std::array<char, 256> name{};
    XfsMountContext* mount{};
    xfs_ino_t parent_ino{};
    xfs_ino_t ino{};
    uint64_t hash{};
    uint64_t dir_generation{};
    uint64_t last_used{};
    uint16_t namelen{};
    int result{};
    uint8_t ftype{};
    bool valid{};
};

struct XfsDentryCacheSet {
    ker::mod::sys::Spinlock lock;
    std::array<XfsDentryCacheEntry, XFS_DENTRY_CACHE_WAYS> ways{};
    uint64_t clock{};
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
    ker::mod::sys::Spinlock lock;
    std::array<XfsDentryGenerationEntry, XFS_DENTRY_GENERATION_WAYS> ways{};
    uint64_t clock{};
};

std::array<XfsDentryCacheSet, XFS_DENTRY_CACHE_SET_COUNT> g_xfs_dentry_cache{};
std::array<XfsDentryGenerationSet, XFS_DENTRY_GENERATION_SET_COUNT> g_xfs_dentry_generations{};
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

auto xfs_dentry_cache_dir_generation_locked(XfsDentryGenerationSet& set, XfsMountContext* mount, xfs_ino_t parent_ino, uint64_t hash)
    -> uint64_t {
    uint64_t const USE_STAMP = ++set.clock;
    XfsDentryGenerationEntry* victim = &set.ways.front();
    for (auto& candidate : set.ways) {
        if (candidate.valid && candidate.hash == hash && candidate.mount == mount && candidate.parent_ino == parent_ino) {
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
    victim->hash = hash;
    victim->generation = GENERATION;
    victim->last_used = USE_STAMP;
    victim->valid = true;
    return GENERATION;
}

auto xfs_dentry_cache_lookup_impl(XfsMountContext* mount, xfs_ino_t parent_ino, const char* name, uint16_t namelen, XfsDirEntry* entry,
                                  int* result) -> bool {
    if (mount == nullptr || parent_ino == NULLFSINO || name == nullptr || entry == nullptr || result == nullptr || namelen >= 256) {
        return false;
    }

    uint64_t const HASH = xfs_dentry_hash_name(mount, parent_ino, name, namelen);
    uint64_t const DIR_HASH = xfs_dentry_hash_dir(mount, parent_ino);
    auto& generation_set = g_xfs_dentry_generations.at(DIR_HASH & (XFS_DENTRY_GENERATION_SET_COUNT - 1));
    auto& set = g_xfs_dentry_cache.at(HASH & (XFS_DENTRY_CACHE_SET_COUNT - 1));

    uint64_t const GENERATION_IRQF = generation_set.lock.lock_irqsave();
    uint64_t const DIR_GENERATION = xfs_dentry_cache_dir_generation_locked(generation_set, mount, parent_ino, DIR_HASH);
    uint64_t const CACHE_IRQF = set.lock.lock_irqsave();
    for (auto& candidate : set.ways) {
        if (!candidate.valid || candidate.hash != HASH || candidate.mount != mount || candidate.parent_ino != parent_ino ||
            candidate.namelen != namelen || candidate.dir_generation != DIR_GENERATION) {
            continue;
        }
        if (std::memcmp(candidate.name.data(), name, namelen) != 0) {
            continue;
        }

        candidate.last_used = ++set.clock;
        *result = candidate.result;
        if (candidate.result == 0) {
            entry->ino = candidate.ino;
            entry->ftype = candidate.ftype;
            entry->namelen = candidate.namelen;
            entry->cookie = 0;
            std::memcpy(entry->name.data(), candidate.name.data(), candidate.namelen + 1);
        }
        set.lock.unlock_irqrestore(CACHE_IRQF);
        generation_set.lock.unlock_irqrestore(GENERATION_IRQF);
        g_xfs_dentry_hits.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    set.lock.unlock_irqrestore(CACHE_IRQF);
    generation_set.lock.unlock_irqrestore(GENERATION_IRQF);
    g_xfs_dentry_misses.fetch_add(1, std::memory_order_relaxed);
    return false;
}

auto xfs_dentry_cache_lookup(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry, int* result) -> bool {
    if (dp == nullptr) {
        return false;
    }
    return xfs_dentry_cache_lookup_impl(dp->mount, dp->ino, name, namelen, entry, result);
}

void xfs_dentry_cache_store(XfsInode* dp, const char* name, uint16_t namelen, int result, const XfsDirEntry* entry) {
    if (dp == nullptr || dp->mount == nullptr || name == nullptr || namelen >= 256 || (result != 0 && result != -ENOENT)) {
        return;
    }
    if (result == 0 && entry == nullptr) {
        return;
    }

    uint64_t const HASH = xfs_dentry_hash_name(dp->mount, dp->ino, name, namelen);
    uint64_t const DIR_HASH = xfs_dentry_hash_dir(dp->mount, dp->ino);
    auto& generation_set = g_xfs_dentry_generations.at(DIR_HASH & (XFS_DENTRY_GENERATION_SET_COUNT - 1));
    auto& set = g_xfs_dentry_cache.at(HASH & (XFS_DENTRY_CACHE_SET_COUNT - 1));

    uint64_t const GENERATION_IRQF = generation_set.lock.lock_irqsave();
    uint64_t const DIR_GENERATION = xfs_dentry_cache_dir_generation_locked(generation_set, dp->mount, dp->ino, DIR_HASH);
    uint64_t const CACHE_IRQF = set.lock.lock_irqsave();
    uint64_t const USE_STAMP = ++set.clock;
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

    std::memcpy(victim->name.data(), name, namelen);
    victim->name.at(namelen) = '\0';
    victim->mount = dp->mount;
    victim->parent_ino = dp->ino;
    victim->ino = result == 0 ? entry->ino : NULLFSINO;
    victim->hash = HASH;
    victim->dir_generation = DIR_GENERATION;
    victim->last_used = USE_STAMP;
    victim->namelen = namelen;
    victim->result = result;
    victim->ftype = result == 0 ? entry->ftype : XFS_DIR3_FT_UNKNOWN;
    victim->valid = true;
    set.lock.unlock_irqrestore(CACHE_IRQF);
    generation_set.lock.unlock_irqrestore(GENERATION_IRQF);
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

void xfs_dentry_cache_invalidate_dir_impl(XfsInode* dp) {
    if (dp == nullptr || dp->mount == nullptr) {
        return;
    }

    uint64_t const HASH = xfs_dentry_hash_dir(dp->mount, dp->ino);
    auto& set = g_xfs_dentry_generations.at(HASH & (XFS_DENTRY_GENERATION_SET_COUNT - 1));

    uint64_t const IRQF = set.lock.lock_irqsave();
    uint64_t const USE_STAMP = ++set.clock;
    XfsDentryGenerationEntry* victim = &set.ways.front();
    for (auto& candidate : set.ways) {
        if (candidate.valid && candidate.hash == HASH && candidate.mount == dp->mount && candidate.parent_ino == dp->ino) {
            candidate.generation = xfs_dentry_next_generation_value();
            candidate.last_used = USE_STAMP;
            set.lock.unlock_irqrestore(IRQF);
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
    set.lock.unlock_irqrestore(IRQF);
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

void dir2_log_bad_magic(const char* where, XfsInode* dp, xfs_dir2_db_t db, BufHead* bh, uint32_t magic) {
    if (where == nullptr || dp == nullptr || dp->mount == nullptr) {
        mod::dbg::logger<"xfs">::error("dir bad magic: where=%s magic=0x%x", where != nullptr ? where : "<null>", magic);
        return;
    }

    XfsMountContext* ctx = dp->mount;
    xfs_fileoff_t const FILE_BLOCK = dir2_db_to_fsbno(ctx, db);
    XfsBmapResult bmap{};
    int const BMAP_RC = xfs_bmap_lookup(dp, FILE_BLOCK, &bmap);
    uint32_t const FORK_EXTENTS = dp->data_fork.format == XFS_DINODE_FMT_EXTENTS ? dp->data_fork.extents.count : 0;

    uint8_t b0 = 0;
    uint8_t b1 = 0;
    uint8_t b2 = 0;
    uint8_t b3 = 0;
    uint8_t b4 = 0;
    uint8_t b5 = 0;
    uint8_t b6 = 0;
    uint8_t b7 = 0;
    if (bh != nullptr && bh->data != nullptr && bh->size >= 8) {
        b0 = bh->data[0];
        b1 = bh->data[1];
        b2 = bh->data[2];
        b3 = bh->data[3];
        b4 = bh->data[4];
        b5 = bh->data[5];
        b6 = bh->data[6];
        b7 = bh->data[7];
    }

    mod::dbg::logger<"xfs">::error(
        "dir bad magic: where=%s magic=0x%x ino=%lu db=%u file_block=%lu bmap_rc=%d hole=%d start=%lu len=%lu bh_dev_block=%lu "
        "bh_size=%lu fmt=%d nextents=%u fork_extents=%u nblocks=%lu isize=%lu bytes=%02x %02x %02x %02x %02x %02x %02x %02x",
        where, magic, static_cast<unsigned long>(dp->ino), static_cast<unsigned int>(db), static_cast<unsigned long>(FILE_BLOCK), BMAP_RC,
        BMAP_RC == 0 && bmap.is_hole ? 1 : 0, BMAP_RC == 0 ? static_cast<unsigned long>(bmap.startblock) : 0UL,
        BMAP_RC == 0 ? static_cast<unsigned long>(bmap.blockcount) : 0UL, bh != nullptr ? static_cast<unsigned long>(bh->block_no) : 0UL,
        bh != nullptr ? static_cast<unsigned long>(bh->size) : 0UL, static_cast<int>(dp->data_fork.format), dp->nextents, FORK_EXTENTS,
        static_cast<unsigned long>(dp->nblocks), static_cast<unsigned long>(dp->size), b0, b1, b2, b3, b4, b5, b6, b7);
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

    if (MAGIC == XFS_DIR3_BLOCK_MAGIC) {
        brelse(bh);
        return true;
    }
    if (MAGIC == XFS_DIR3_DATA_MAGIC) {
        brelse(bh);
        return false;
    }

    dir2_log_bad_magic("format-detect", dp, 0, bh, MAGIC);
    brelse(bh);
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

auto dir2_block_lookup_loaded(XfsInode* dp, BufHead* bh, const char* name, uint16_t namelen, XfsDirEntry* entry, bool log_bad_magic)
    -> int {
    XfsMountContext const* ctx = dp->mount;
    if (bh == nullptr || bh->data == nullptr) {
        return -EIO;
    }

    const uint8_t* block = bh->data;
    size_t const BLKSIZE = ctx->dir_blk_size;

    // Validate magic
    const auto* hdr = reinterpret_cast<const XfsDir3DataHdr*>(block);
    uint32_t const MAGIC = hdr->hdr.magic.to_cpu();
    if (MAGIC != XFS_DIR3_BLOCK_MAGIC) {
        if (log_bad_magic) {
            dir2_log_bad_magic("block-lookup", dp, 0, bh, MAGIC);
        }
        return -EAGAIN;
    }

    // Block tail is at the very end of the block
    const auto* btp = reinterpret_cast<const XfsDir2BlockTail*>(block + BLKSIZE - sizeof(XfsDir2BlockTail));
    uint32_t const LEAF_COUNT = btp->count.to_cpu();
    (void)btp->stale;  // stale count unused in read-only lookup

    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    size_t const LEAF_BYTES = static_cast<size_t>(LEAF_COUNT) * sizeof(XfsDir2LeafEntry);
    if (LEAF_BYTES > BLKSIZE - DATA_START - sizeof(XfsDir2BlockTail)) {
        return -EINVAL;
    }
    size_t const DATA_END = BLKSIZE - sizeof(XfsDir2BlockTail) - LEAF_BYTES;
    if (DATA_END <= DATA_START) {
        return -EINVAL;
    }

    // Leaf entries are just before the tail
    const auto* blp = reinterpret_cast<const XfsDir2LeafEntry*>(block + DATA_END);

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

        size_t const OFF = dir2_dataptr_to_off(ctx, ADDR);
        const XfsDir2DataEntry* dep = nullptr;
        if (OFF < DATA_START || !dir2_data_entry_at_if_valid(ctx, block, OFF, DATA_END, &dep, nullptr)) {
            continue;
        }

        if (dep->namelen == namelen && __builtin_memcmp(xfs_dir2_data_entry_name(dep), name, namelen) == 0) {
            fill_dir_entry(ctx, dep, entry);
            return 0;
        }
    }

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

    const auto* hdr = reinterpret_cast<const XfsDir3DataHdr*>(block);
    uint32_t const MAGIC = hdr->hdr.magic.to_cpu();
    if (MAGIC != XFS_DIR3_BLOCK_MAGIC) {
        brelse(bh);
        return -EINVAL;
    }

    // Block tail
    const auto* btp = reinterpret_cast<const XfsDir2BlockTail*>(block + BLKSIZE - sizeof(XfsDir2BlockTail));
    uint32_t const LEAF_COUNT = btp->count.to_cpu();

#ifdef XFS_DEBUG
    const auto* dbg_hdr = reinterpret_cast<const XfsDir3DataHdr*>(block);
    mod::dbg::log("[xfs] dir2_block_iterate: ino=%lu magic=0x%x leaf_count=%u", static_cast<unsigned long>(dp->ino),
                  dbg_hdr->hdr.magic.to_cpu(), LEAF_COUNT);
#endif
    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    size_t const LEAF_BYTES = static_cast<size_t>(LEAF_COUNT) * sizeof(XfsDir2LeafEntry);
    if (LEAF_BYTES > BLKSIZE - DATA_START - sizeof(XfsDir2BlockTail)) {
        brelse(bh);
        return -EINVAL;
    }
    size_t const DATA_END = BLKSIZE - sizeof(XfsDir2BlockTail) - LEAF_BYTES;

    size_t offset = DATA_START;
    XfsDirEntry entry{};

    while (offset < DATA_END) {
        // Check for free space entry
        uint16_t free_len = 0;
        if (dir2_data_unused_at_if_valid(block, offset, DATA_END, &free_len)) {
            offset += free_len;
            continue;
        }

        const XfsDir2DataEntry* dep = nullptr;
        size_t dep_size = 0;
        if (!dir2_data_entry_at_if_valid(ctx, block, offset, DATA_END, &dep, &dep_size)) {
            break;
        }

        fill_dir_entry(ctx, dep, &entry, data_entry_cookie(ctx, 0, offset));

        rc = fn(&entry, user_ctx);
        if (rc != 0) {
            brelse(bh);
            return 0;
        }

        offset += dep_size;
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
        dir2_log_bad_magic("data-scan", dp, db, bh, MAGIC);
        brelse(bh);
        return -EINVAL;
    }

    // v3 data header
    size_t offset = sizeof(XfsDir3DataHdr);
    XfsDirEntry entry{};

    while (offset + sizeof(XfsDir2DataUnused) <= BLKSIZE) {
        uint16_t free_len = 0;
        if (dir2_data_unused_at_if_valid(block, offset, BLKSIZE, &free_len)) {
            offset += free_len;
            continue;
        }

        const XfsDir2DataEntry* dep = nullptr;
        size_t dep_size = 0;
        if (!dir2_data_entry_at_if_valid(ctx, block, offset, BLKSIZE, &dep, &dep_size)) {
            break;
        }

        fill_dir_entry(ctx, dep, &entry, data_entry_cookie(ctx, db, offset));
        rc = fn(&entry, user_ctx);
        if (rc != 0) {
            brelse(bh);
            return 1;  // caller requested stop
        }

        offset += dep_size;
    }

    brelse(bh);
    return 0;
}

auto dir2_leaf_index_known_complete(const XfsInode* dp) -> bool {
    return dp != nullptr && dp->dir_leaf_index_complete && dp->dir_leaf_index_complete_generation == dp->dir_generation;
}

void dir2_leaf_index_note_complete(XfsInode* dp) {
    if (dp == nullptr) {
        return;
    }
    dp->dir_leaf_index_complete_generation = dp->dir_generation;
    dp->dir_leaf_index_complete = true;
}

void dir2_leaf_index_note_unknown(XfsInode* dp) {
    if (dp == nullptr) {
        return;
    }
    dp->dir_leaf_index_complete = false;
    dp->dir_leaf_index_complete_generation = 0;
}

auto dir2_leaf_entries_contain_dataptr(const XfsDir2LeafEntry* entries, size_t count, xfs_dahash_t hash, xfs_dir2_dataptr_t dataptr)
    -> bool {
    if (entries == nullptr || count == 0 || dataptr == XFS_DIR2_NULL_DATAPTR) {
        return false;
    }

    int lo = 0;
    int hi = static_cast<int>(count) - 1;
    int mid = -1;
    bool found = false;
    while (lo <= hi) {
        mid = (lo + hi) / 2;
        uint32_t const ENTRY_HASH = entries[mid].hashval.to_cpu();
        if (hash < ENTRY_HASH) {
            hi = mid - 1;
        } else if (hash > ENTRY_HASH) {
            lo = mid + 1;
        } else {
            found = true;
            break;
        }
    }
    if (!found) {
        return false;
    }

    while (mid > 0 && entries[mid - 1].hashval.to_cpu() == hash) {
        mid--;
    }
    for (int i = mid; std::cmp_less(i, count); ++i) {
        if (entries[i].hashval.to_cpu() != hash) {
            break;
        }
        if (entries[i].address.to_cpu() == dataptr) {
            return true;
        }
    }
    return false;
}

auto dir2_db_off_to_dataptr(const XfsMountContext* ctx, xfs_dir2_db_t db, size_t off) -> xfs_dir2_dataptr_t;
auto dir2_validate_leaf_data_block(const uint8_t* block) -> int;
auto dir2_read_mapped_dir_block(XfsInode* dp, xfs_fileoff_t file_block, BufHead** bhp, xfs_fsblock_t* disk_block_out = nullptr) -> int;
auto dir2_validate_node_root(const XfsMountContext* ctx, const XfsDa3NodeHdr* hdr) -> int;
struct XfsDir3LeafHdr;
auto dir2_validate_leafn(const XfsMountContext* ctx, const XfsDir3LeafHdr* hdr, size_t* live_count_out = nullptr) -> int;
auto dir2_node_entries(const XfsDa3NodeHdr* hdr) -> const XfsDaNodeEntry*;
auto dir2_leaf_entries(const XfsDir3LeafHdr* hdr) -> const XfsDir2LeafEntry*;

auto dir2_lookup_leafn_hash(XfsInode* dp, const XfsDir3LeafHdr* leaf_hdr, xfs_dahash_t hash, const char* name, uint16_t namelen,
                            XfsDirEntry* entry) -> int;

auto dir2_leaf_node_linear_scan(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry,
                                const XfsDir2LeafEntry* leaf_entries = nullptr, size_t leaf_count = 0,
                                bool prove_leaf_index_complete = false) -> int {
    XfsMountContext* ctx = dp->mount;
    bool leaf_index_complete = prove_leaf_index_complete && leaf_entries != nullptr;

    uint64_t nblocks = dp->size >> (ctx->block_log + ctx->dir_blk_log);
    if (nblocks == 0) {
        nblocks = 1;
    }

    for (xfs_dir2_db_t db = 0; db < nblocks; db++) {
        BufHead* data_bh = nullptr;
        int const RC = dir2_read_block(dp, db, &data_bh);
        if (RC != 0) {
            return RC;
        }

        const uint8_t* block = data_bh->data;
        int const VALID_RC = dir2_validate_leaf_data_block(block);
        if (VALID_RC != 0) {
            brelse(data_bh);
            return VALID_RC;
        }
        size_t const BLKSIZE = ctx->dir_blk_size;
        size_t offset = sizeof(XfsDir3DataHdr);

        while (offset + sizeof(XfsDir2DataUnused) <= BLKSIZE) {
            uint16_t free_len = 0;
            if (dir2_data_unused_at_if_valid(block, offset, BLKSIZE, &free_len)) {
                offset += free_len;
                continue;
            }

            const XfsDir2DataEntry* dep = nullptr;
            size_t dep_size = 0;
            if (!dir2_data_entry_at_if_valid(ctx, block, offset, BLKSIZE, &dep, &dep_size)) {
                brelse(data_bh);
                return -EINVAL;
            }

            if (leaf_index_complete) {
                xfs_dahash_t const ENTRY_HASH = xfs_da_hashname(xfs_dir2_data_entry_name(dep), dep->namelen);
                xfs_dir2_dataptr_t const DATAPTR = dir2_db_off_to_dataptr(ctx, db, offset);
                if (!dir2_leaf_entries_contain_dataptr(leaf_entries, leaf_count, ENTRY_HASH, DATAPTR)) {
                    leaf_index_complete = false;
                }
            }

            if (dep->namelen == namelen && __builtin_memcmp(xfs_dir2_data_entry_name(dep), name, namelen) == 0) {
                fill_dir_entry(ctx, dep, entry);
                brelse(data_bh);
                return 0;
            }

            offset += dep_size;
        }
        brelse(data_bh);
    }

    if (leaf_index_complete) {
        dir2_leaf_index_note_complete(dp);
    }
    return -ENOENT;
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
    if (rc != 0) {
        return rc;
    }
    if (bmap.is_hole) {
        return -EIO;
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
            return -EIO;
        }

        const uint8_t* leaf_data = leaf_bh->data;

        // Check magic - leaf block starts with xfs_da3_blkinfo
        const auto* info = reinterpret_cast<const XfsDa3Blkinfo*>(leaf_data);
        uint16_t const LEAF_MAGIC = info->hdr.magic.to_cpu();

        if (LEAF_MAGIC == XFS_DA3_NODE_MAGIC) {
            const auto* node_hdr = reinterpret_cast<const XfsDa3NodeHdr*>(leaf_data);
            int const NODE_RC = dir2_validate_node_root(ctx, node_hdr);
            if (NODE_RC != 0) {
                brelse(leaf_bh);
                return NODE_RC;
            }

            const auto* node_entries = dir2_node_entries(node_hdr);
            size_t const NODE_COUNT = node_hdr->count.to_cpu();
            for (size_t i = 0; i < NODE_COUNT; i++) {
                uint32_t const MAX_HASH = node_entries[i].hashval.to_cpu();
                if (HASH > MAX_HASH) {
                    continue;
                }

                BufHead* child_bh = nullptr;
                int const READ_RC = dir2_read_mapped_dir_block(dp, node_entries[i].before.to_cpu(), &child_bh);
                if (READ_RC != 0) {
                    brelse(leaf_bh);
                    return READ_RC;
                }
                int const LOOKUP_RC =
                    dir2_lookup_leafn_hash(dp, reinterpret_cast<const XfsDir3LeafHdr*>(child_bh->data), HASH, name, namelen, entry);
                brelse(child_bh);
                if (LOOKUP_RC == 0) {
                    brelse(leaf_bh);
                    return 0;
                }
                if (LOOKUP_RC != -ENOENT) {
                    brelse(leaf_bh);
                    return LOOKUP_RC;
                }
                // Equal hashes can straddle adjacent children. Once a child
                // maximum exceeds the target, later children cannot match.
                if (MAX_HASH > HASH) {
                    break;
                }
            }
            brelse(leaf_bh);
            return -ENOENT;
        }

        if (LEAF_MAGIC != XFS_DIR3_LEAF_MAGIC && LEAF_MAGIC != XFS_DIR3_LEAFN_MAGIC) {
            brelse(leaf_bh);
            return -EINVAL;
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
            return -EINVAL;
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
            if (LEAF_INDEX_FULL) {
                if (dir2_leaf_index_known_complete(dp)) {
                    brelse(leaf_bh);
                    return -ENOENT;
                }
                int const SCAN_RET = dir2_leaf_node_linear_scan(dp, name, namelen, entry, lep, leaf_count, true);
                brelse(leaf_bh);
                return SCAN_RET;
            }
            brelse(leaf_bh);
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

            const XfsDir2DataEntry* dep = nullptr;
            if (dir2_data_entry_at_if_valid(ctx, data_bh->data, OFF, ctx->dir_blk_size, &dep, nullptr)) {
                if (dep->namelen == namelen && __builtin_memcmp(xfs_dir2_data_entry_name(dep), name, namelen) == 0) {
                    fill_dir_entry(ctx, dep, entry);
                    brelse(data_bh);
                    brelse(leaf_bh);
                    return 0;
                }
            }
            brelse(data_bh);
        }

        if (LEAF_INDEX_FULL) {
            if (dir2_leaf_index_known_complete(dp)) {
                brelse(leaf_bh);
                return -ENOENT;
            }
            int const SCAN_RET = dir2_leaf_node_linear_scan(dp, name, namelen, entry, lep, leaf_count, true);
            brelse(leaf_bh);
            return SCAN_RET;
        }
        brelse(leaf_bh);
        return -ENOENT;
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
        if (rc != 0) {
            return rc;
        }
        if (bmap.is_hole) {
            return -EIO;
        }

        rc = dir2_scan_data_block(dp, db, fn, user_ctx);
        if (rc != 0) {
            return 0;  // iterator requested stop
        }
    }

    return 0;
}

auto dir2_extent_or_btree_lookup(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry) -> int {
    XfsMountContext const* ctx = dp->mount;

    BufHead* bh = nullptr;
    int const RC = dir2_read_block(dp, 0, &bh);
    if (RC != 0 || bh == nullptr || bh->data == nullptr) {
        if (bh != nullptr) {
            brelse(bh);
        }
        if (dp->size <= ctx->dir_blk_size) {
            return RC != 0 ? RC : -EIO;
        }
        return dir2_leaf_node_lookup(dp, name, namelen, entry);
    }

    const auto* hdr = reinterpret_cast<const XfsDir3DataHdr*>(bh->data);
    uint32_t const MAGIC = hdr->hdr.magic.to_cpu();
    if (MAGIC == XFS_DIR3_BLOCK_MAGIC) {
        int const RESULT = dir2_block_lookup_loaded(dp, bh, name, namelen, entry, false);
        brelse(bh);
        return RESULT == -EAGAIN ? -EINVAL : RESULT;
    }
    if (MAGIC == XFS_DIR3_DATA_MAGIC) {
        brelse(bh);
        return dir2_leaf_node_lookup(dp, name, namelen, entry);
    }

    dir2_log_bad_magic("format-detect", dp, 0, bh, MAGIC);
    brelse(bh);
    if (dp->size <= ctx->dir_blk_size) {
        return -EINVAL;
    }
    return dir2_leaf_node_lookup(dp, name, namelen, entry);
}

}  // anonymous namespace

// ============================================================================
// Public API
// ============================================================================

void xfs_dentry_cache_invalidate_dir(XfsInode* dp) { xfs_dentry_cache_invalidate_dir_impl(dp); }

auto xfs_dentry_cache_lookup_parent(XfsMountContext* mount, xfs_ino_t parent_ino, const char* name, uint16_t namelen, XfsDirEntry* entry,
                                    int* result) -> bool {
    return xfs_dentry_cache_lookup_impl(mount, parent_ino, name, namelen, entry, result);
}

namespace {

auto xfs_dir_lookup_impl(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry, bool allow_cache) -> int {
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
    if (allow_cache) {
        int cached_result = 0;
        if (xfs_dentry_cache_lookup(dp, name, namelen, entry, &cached_result)) {
            return cached_result;
        }
    }

    int result = 0;
    switch (dp->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL:
            result = dir2_sf_lookup(dp, name, namelen, entry);
            break;

        case XFS_DINODE_FMT_EXTENTS:
        case XFS_DINODE_FMT_BTREE: {
            result = dir2_extent_or_btree_lookup(dp, name, namelen, entry);
            break;
        }

        default:
            result = -EINVAL;
            break;
    }
    xfs_dentry_cache_store(dp, name, namelen, result, entry);
    return result;
}

}  // namespace

auto xfs_dir_lookup(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry) -> int {
    return xfs_dir_lookup_impl(dp, name, namelen, entry, true);
}

auto xfs_dir_lookup_authoritative(XfsInode* dp, const char* name, uint16_t namelen, XfsDirEntry* entry) -> int {
    return xfs_dir_lookup_impl(dp, name, namelen, entry, false);
}

namespace {

auto dir_entry_index_membership(const XfsDirEntry* observed, int lookup_result, const XfsDirEntry* indexed) -> int {
    if (observed == nullptr) {
        return -EINVAL;
    }
    if (lookup_result == -ENOENT) {
        return 0;
    }
    if (lookup_result != 0) {
        return lookup_result;
    }
    if (indexed == nullptr || indexed->ino != observed->ino || indexed->ftype != observed->ftype) {
        return -EIO;
    }
    return 1;
}

}  // namespace

auto xfs_dir_entry_is_indexed(XfsInode* dp, const XfsDirEntry* observed) -> int {
    if (dp == nullptr || observed == nullptr || observed->namelen == 0 || observed->namelen > observed->name.size()) {
        return -EINVAL;
    }

    XfsDirEntry indexed{};
    int const LOOKUP_RET = xfs_dir_lookup_authoritative(dp, observed->name.data(), observed->namelen, &indexed);
    return dir_entry_index_membership(observed, LOOKUP_RET, &indexed);
}

void xfs_dir_observe_entry(XfsInode* dp, const XfsDirEntry* entry) {
    if (dp == nullptr || entry == nullptr || entry->namelen == 0) {
        return;
    }
    xfs_dentry_cache_store(dp, entry->name.data(), entry->namelen, 0, entry);
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

auto dir2_lookup_leafn_hash(XfsInode* dp, const XfsDir3LeafHdr* leaf_hdr, xfs_dahash_t hash, const char* name, uint16_t namelen,
                            XfsDirEntry* entry) -> int {
    XfsMountContext const* ctx = dp->mount;
    int const VALID_RC = dir2_validate_leafn(ctx, leaf_hdr);
    if (VALID_RC != 0) {
        return VALID_RC;
    }

    const auto* entries = dir2_leaf_entries(leaf_hdr);
    size_t const COUNT = leaf_hdr->count.to_cpu();
    size_t first = 0;
    while (first < COUNT && entries[first].hashval.to_cpu() < hash) {
        first++;
    }
    for (size_t i = first; i < COUNT && entries[i].hashval.to_cpu() == hash; i++) {
        xfs_dir2_dataptr_t const ADDR = entries[i].address.to_cpu();
        if (ADDR == XFS_DIR2_NULL_DATAPTR) {
            continue;
        }
        BufHead* data_bh = nullptr;
        int const READ_RC = dir2_read_block(dp, dir2_dataptr_to_db(ctx, ADDR), &data_bh);
        if (READ_RC != 0) {
            return READ_RC;
        }
        const XfsDir2DataEntry* dep = nullptr;
        uint32_t const OFF = dir2_dataptr_to_off(ctx, ADDR);
        if (!dir2_data_entry_at_if_valid(ctx, data_bh->data, OFF, ctx->dir_blk_size, &dep, nullptr)) {
            brelse(data_bh);
            return -EINVAL;
        }
        if (dep->namelen == namelen && __builtin_memcmp(xfs_dir2_data_entry_name(dep), name, namelen) == 0) {
            fill_dir_entry(ctx, dep, entry);
            brelse(data_bh);
            return 0;
        }
        brelse(data_bh);
    }
    return -ENOENT;
}

struct XfsDir2LeafTail {
    Be32 bestcount;
} __attribute__((packed));

static_assert(sizeof(XfsDir2LeafTail) == 4);

constexpr size_t XFS_DA3_CRC_OFF = __builtin_offsetof(XfsDa3Blkinfo, crc);

auto dir2_buf_get_dir_block(XfsMountContext* ctx, xfs_fsblock_t disk_block) -> BufHead* {
    uint32_t const FBS = 1U << ctx->dir_blk_log;
    return (FBS == 1) ? xfs_buf_get(ctx, disk_block) : xfs_buf_get_multi(ctx, disk_block, FBS);
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

auto dir2_leaf_layout(const XfsMountContext* ctx, const XfsDir3LeafHdr* hdr, size_t bestcount_override, size_t* bestcount_out,
                      size_t* capacity_out) -> bool {
    uint16_t const MAGIC = hdr->info.hdr.magic.to_cpu();
    size_t bestcount = 0;
    size_t trailer_size = 0;
    if (MAGIC == XFS_DIR3_LEAF_MAGIC) {
        const auto* tail =
            reinterpret_cast<const XfsDir2LeafTail*>(reinterpret_cast<const uint8_t*>(hdr) + ctx->dir_blk_size - sizeof(XfsDir2LeafTail));
        bestcount = bestcount_override == SIZE_MAX ? tail->bestcount.to_cpu() : bestcount_override;
        if (bestcount > (ctx->dir_blk_size - sizeof(XfsDir3LeafHdr) - sizeof(XfsDir2LeafTail)) / sizeof(Be16)) {
            return false;
        }
        trailer_size = sizeof(XfsDir2LeafTail) + (bestcount * sizeof(Be16));
    } else if (MAGIC != XFS_DIR3_LEAFN_MAGIC) {
        return false;
    }

    if (sizeof(XfsDir3LeafHdr) + trailer_size > ctx->dir_blk_size) {
        return false;
    }
    *bestcount_out = bestcount;
    *capacity_out = (ctx->dir_blk_size - sizeof(XfsDir3LeafHdr) - trailer_size) / sizeof(XfsDir2LeafEntry);
    return true;
}

auto dir2_leaf_bests(const XfsMountContext* ctx, XfsDir3LeafHdr* hdr) -> Be16* {
    auto* tail = reinterpret_cast<XfsDir2LeafTail*>(reinterpret_cast<uint8_t*>(hdr) + ctx->dir_blk_size - sizeof(XfsDir2LeafTail));
    return reinterpret_cast<Be16*>(tail) - tail->bestcount.to_cpu();
}

auto dir2_leaf_set_best(const XfsMountContext* ctx, XfsDir3LeafHdr* hdr, xfs_dir2_db_t db, uint16_t best) -> int {
    size_t bestcount = 0;
    size_t capacity = 0;
    if (!dir2_leaf_layout(ctx, hdr, SIZE_MAX, &bestcount, &capacity) || db >= bestcount || hdr->count.to_cpu() > capacity) {
        return -EINVAL;
    }
    dir2_leaf_bests(ctx, hdr)[db] = Be16::from_cpu(best);
    return 0;
}

auto dir2_leaf_extend_bests(const XfsMountContext* ctx, XfsDir3LeafHdr* hdr, uint16_t best) -> int {
    size_t old_bestcount = 0;
    size_t old_capacity = 0;
    if (!dir2_leaf_layout(ctx, hdr, SIZE_MAX, &old_bestcount, &old_capacity) || old_bestcount >= UINT32_MAX) {
        return -EINVAL;
    }

    size_t new_bestcount = old_bestcount + 1;
    size_t new_capacity = 0;
    size_t ignored = 0;
    if (!dir2_leaf_layout(ctx, hdr, new_bestcount, &ignored, &new_capacity) || hdr->count.to_cpu() > new_capacity) {
        return -ENOSPC;
    }

    auto* tail = reinterpret_cast<XfsDir2LeafTail*>(reinterpret_cast<uint8_t*>(hdr) + ctx->dir_blk_size - sizeof(XfsDir2LeafTail));
    auto* old_bests = reinterpret_cast<Be16*>(tail) - old_bestcount;
    auto* new_bests = reinterpret_cast<Be16*>(tail) - new_bestcount;
    __builtin_memmove(new_bests, old_bests, old_bestcount * sizeof(Be16));
    new_bests[old_bestcount] = Be16::from_cpu(best);
    tail->bestcount = Be32::from_cpu(static_cast<uint32_t>(new_bestcount));
    return 0;
}

auto dir2_count_stale_leaf_entries(const XfsDir2LeafEntry* entries, size_t count) -> size_t {
    size_t stale = 0;
    for (size_t i = 0; i < count; i++) {
        if (entries[i].address.to_cpu() == XFS_DIR2_NULL_DATAPTR) {
            stale++;
        }
    }
    return stale;
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

    size_t bestcount = 0;
    size_t capacity = 0;
    if (!dir2_leaf_layout(ctx, hdr, SIZE_MAX, &bestcount, &capacity) || hdr->count.to_cpu() > capacity ||
        (hdr->info.hdr.magic.to_cpu() == XFS_DIR3_LEAF_MAGIC && bestcount != dir2_data_block_count(dp))) {
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
    // XFS checksum fields are stored little-endian even when the surrounding
    // metadata structure declares the slot as __be32.
    __builtin_memcpy(&hdr->info.crc, &CRC, sizeof(CRC));
}

void dir2_recompute_data_crc(uint8_t* block, size_t blksize) {
    auto* hdr = reinterpret_cast<XfsDir3DataHdr*>(block);
    hdr->hdr.crc = Be32{0};
    uint32_t const CRC = util::crc32c_block_with_cksum(block, blksize, 4);
    __builtin_memcpy(&hdr->hdr.crc, &CRC, sizeof(CRC));
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

        uint16_t free_len = 0;
        if (dir2_data_unused_at_if_valid(block, off, data_end, &free_len)) {
            BestFreeSlot const CUR{.off = static_cast<uint16_t>(off), .len = free_len};
            for (int idx = 0; idx < 3; idx++) {
                if (CUR.len > best.at(static_cast<size_t>(idx)).len) {
                    for (int j = 2; j > idx; j--) {
                        best.at(static_cast<size_t>(j)) = best.at(static_cast<size_t>(j - 1));
                    }
                    best.at(static_cast<size_t>(idx)) = CUR;
                    break;
                }
            }

            off += free_len;
            continue;
        }

        const XfsDir2DataEntry* dep = nullptr;
        size_t dep_size = 0;
        if (!dir2_data_entry_at_if_valid(ctx, block, off, data_end, &dep, &dep_size)) {
            break;
        }
        off += dep_size;
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

        uint16_t free_len = 0;
        if (dir2_data_unused_at_if_valid(block, off, data_end, &free_len)) {
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

        const XfsDir2DataEntry* dep = nullptr;
        size_t dep_size = 0;
        if (!dir2_data_entry_at_if_valid(ctx, block, off, data_end, &dep, &dep_size)) {
            break;
        }
        off += dep_size;
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
                           size_t* free_off, size_t* free_len) -> int {
    size_t off = data_start;
    while (off < data_end) {
        if (off + sizeof(XfsDir2DataUnused) > data_end) {
            break;
        }

        uint16_t candidate_free_len = 0;
        if (dir2_data_unused_at_if_valid(block, off, data_end, &candidate_free_len)) {
            if (candidate_free_len >= need_len) {
                *free_off = off;
                *free_len = candidate_free_len;
                return 0;
            }
            off += candidate_free_len;
            continue;
        }

        const XfsDir2DataEntry* dep = nullptr;
        size_t dep_size = 0;
        if (!dir2_data_entry_at_if_valid(ctx, block, off, data_end, &dep, &dep_size)) {
            return -EINVAL;
        }
        off += dep_size;
    }

    return off == data_end ? -ENOSPC : -EINVAL;
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

auto dir2_leaf_find_dataptr(const XfsMountContext* ctx, const XfsDir3LeafHdr* hdr, xfs_dahash_t hash, xfs_dir2_dataptr_t dataptr,
                            int* index_out) -> int {
    uint16_t const MAGIC = hdr->info.hdr.magic.to_cpu();
    if (MAGIC == XFS_DIR3_LEAFN_MAGIC) {
        int const VALID_RC = dir2_validate_leafn(ctx, hdr);
        if (VALID_RC != 0) {
            return VALID_RC;
        }
    } else if (MAGIC == XFS_DIR3_LEAF_MAGIC) {
        size_t bestcount = 0;
        size_t capacity = 0;
        if (!dir2_leaf_layout(ctx, hdr, SIZE_MAX, &bestcount, &capacity) || hdr->count.to_cpu() > capacity ||
            dir2_count_stale_leaf_entries(dir2_leaf_entries(hdr), hdr->count.to_cpu()) != hdr->stale.to_cpu()) {
            return -EINVAL;
        }
    } else {
        return -EINVAL;
    }
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

auto dir2_leaf_preflight_index_slot(XfsMountContext const* ctx, const XfsDir3LeafHdr* hdr, size_t bestcount_override = SIZE_MAX) -> int {
    uint16_t const LEAF_COUNT = hdr->count.to_cpu();
    uint16_t const STALE_COUNT = hdr->stale.to_cpu();
    size_t bestcount = 0;
    size_t capacity = 0;
    if (!dir2_leaf_layout(ctx, hdr, bestcount_override, &bestcount, &capacity) || capacity > UINT16_MAX) {
        return -EINVAL;
    }
    if (LEAF_COUNT > capacity) {
        return bestcount_override == SIZE_MAX ? -EINVAL : -ENOSPC;
    }

    const auto* lep = dir2_leaf_entries(hdr);
    size_t const ACTUAL_STALE = dir2_count_stale_leaf_entries(lep, LEAF_COUNT);
    if (ACTUAL_STALE != static_cast<size_t>(STALE_COUNT)) {
        return -EINVAL;
    }
    if (ACTUAL_STALE != 0 || LEAF_COUNT < capacity) {
        return 0;
    }
    return -ENOSPC;
}

auto dir2_leaf_ensure_stale_slot(XfsMountContext const* ctx, XfsDir3LeafHdr* hdr) -> int {
    int const PREFLIGHT = dir2_leaf_preflight_index_slot(ctx, hdr);
    if (PREFLIGHT != 0) {
        return PREFLIGHT;
    }

    uint16_t const STALE_COUNT = hdr->stale.to_cpu();
    if (STALE_COUNT != 0) {
        return 0;
    }

    uint16_t const LEAF_COUNT = hdr->count.to_cpu();

    auto* lep = dir2_leaf_entries(hdr);
    lep[LEAF_COUNT].hashval = Be32::from_cpu(UINT32_MAX);
    lep[LEAF_COUNT].address = Be32::from_cpu(XFS_DIR2_NULL_DATAPTR);
    hdr->count = Be16::from_cpu(static_cast<uint16_t>(LEAF_COUNT + 1));
    hdr->stale = Be16::from_cpu(1);
    return 0;
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
            return rc;
        }

        uint8_t* block = bh->data;
        rc = dir2_validate_leaf_data_block(block);
        if (rc != 0) {
            brelse(bh);
            return rc;
        }

        size_t off = DATA_START;
        while (off + sizeof(XfsDir2DataUnused) <= DATA_END) {
            uint16_t free_len = 0;
            if (dir2_data_unused_at_if_valid(block, off, DATA_END, &free_len)) {
                off += free_len;
                continue;
            }

            const XfsDir2DataEntry* dep = nullptr;
            size_t dep_size = 0;
            if (!dir2_data_entry_at_if_valid(ctx, block, off, DATA_END, &dep, &dep_size)) {
                brelse(bh);
                return -EINVAL;
            }

            if (dep->namelen == namelen && __builtin_memcmp(xfs_dir2_data_entry_name(dep), name, namelen) == 0) {
                *data_bhp = bh;
                *dbp = db;
                *entry_offp = off;
                *entry_sizep = dep_size;
                return 0;
            }

            off += dep_size;
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
        if (rc != 0) {
            return rc;
        }
        if (bmap.is_hole) {
            return -EIO;
        }

        BufHead* bh = nullptr;
        rc = dir2_read_block(dp, db, &bh);
        if (rc != 0) {
            return rc;
        }

        uint8_t* block = bh->data;
        rc = dir2_validate_leaf_data_block(block);
        if (rc != 0) {
            brelse(bh);
            return rc;
        }

        size_t free_off = 0;
        size_t free_len = 0;
        int const FREE_RC = dir2_find_free_region(ctx, block, DATA_START, DATA_END, need_len, &free_off, &free_len);
        if (FREE_RC == 0) {
            *data_bhp = bh;
            *dbp = db;
            *free_offp = free_off;
            *free_lenp = free_len;
            return 0;
        }
        if (FREE_RC != -ENOSPC) {
            brelse(bh);
            return FREE_RC;
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

    BufHead* bh = dir2_buf_get_dir_block(ctx, disk_block);
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

constexpr uint16_t XFS_DIR2_NULL_DATAOFF = UINT16_MAX;

auto dir2_read_mapped_dir_block(XfsInode* dp, xfs_fileoff_t file_block, BufHead** bhp, xfs_fsblock_t* disk_block_out) -> int {
    if (dp == nullptr || bhp == nullptr) {
        return -EINVAL;
    }
    *bhp = nullptr;

    XfsMountContext* ctx = dp->mount;
    XfsBmapResult bmap{};
    int const RC = xfs_bmap_lookup(dp, file_block, &bmap);
    uint32_t const FBS = 1U << ctx->dir_blk_log;
    if (RC != 0) {
        return RC;
    }
    if (bmap.is_hole || bmap.blockcount < FBS) {
        return -ENOENT;
    }

    BufHead* bh = FBS == 1 ? xfs_buf_read(ctx, bmap.startblock) : xfs_buf_read_multi(ctx, bmap.startblock, FBS);
    if (bh == nullptr) {
        return -EIO;
    }
    if (disk_block_out != nullptr) {
        *disk_block_out = bmap.startblock;
    }
    *bhp = bh;
    return 0;
}

auto dir2_mapped_range_is_hole(XfsInode* dp, xfs_fileoff_t file_block) -> bool {
    XfsBmapResult bmap{};
    int const RC = xfs_bmap_lookup(dp, file_block, &bmap);
    return RC == 0 && bmap.is_hole && bmap.blockcount >= (1U << dp->mount->dir_blk_log);
}

auto dir2_find_free_leaf_block(XfsInode* dp, xfs_fileoff_t* file_block_out) -> int {
    XfsMountContext const* ctx = dp->mount;
    uint32_t const FBS = 1U << ctx->dir_blk_log;
    xfs_fileoff_t const LEAF_ROOT = XFS_DIR2_LEAF_OFFSET >> ctx->block_log;
    xfs_fileoff_t const FREE_ROOT = XFS_DIR2_FREE_OFFSET >> ctx->block_log;
    for (xfs_fileoff_t candidate = LEAF_ROOT + FBS; candidate < FREE_ROOT; candidate += FBS) {
        if (dir2_mapped_range_is_hole(dp, candidate)) {
            *file_block_out = candidate;
            return 0;
        }
    }
    return -ENOSPC;
}

auto dir2_node_entries(XfsDa3NodeHdr* hdr) -> XfsDaNodeEntry* {
    return reinterpret_cast<XfsDaNodeEntry*>(reinterpret_cast<uint8_t*>(hdr) + sizeof(XfsDa3NodeHdr));
}

auto dir2_node_entries(const XfsDa3NodeHdr* hdr) -> const XfsDaNodeEntry* {
    return reinterpret_cast<const XfsDaNodeEntry*>(reinterpret_cast<const uint8_t*>(hdr) + sizeof(XfsDa3NodeHdr));
}

auto dir2_free_bests(XfsDir3FreeHdr* hdr) -> Be16* {
    return reinterpret_cast<Be16*>(reinterpret_cast<uint8_t*>(hdr) + sizeof(XfsDir3FreeHdr));
}

auto dir2_free_bests(const XfsDir3FreeHdr* hdr) -> const Be16* {
    return reinterpret_cast<const Be16*>(reinterpret_cast<const uint8_t*>(hdr) + sizeof(XfsDir3FreeHdr));
}

auto dir2_node_capacity(const XfsMountContext* ctx) -> size_t {
    return (ctx->dir_blk_size - sizeof(XfsDa3NodeHdr)) / sizeof(XfsDaNodeEntry);
}

auto dir2_leafn_capacity(const XfsMountContext* ctx) -> size_t {
    return (ctx->dir_blk_size - sizeof(XfsDir3LeafHdr)) / sizeof(XfsDir2LeafEntry);
}

auto dir2_free_capacity(const XfsMountContext* ctx) -> size_t { return (ctx->dir_blk_size - sizeof(XfsDir3FreeHdr)) / sizeof(Be16); }

void dir2_recompute_free_crc(uint8_t* block, size_t blksize) {
    auto* hdr = reinterpret_cast<XfsDir3FreeHdr*>(block);
    hdr->hdr.crc = Be32{0};
    uint32_t const CRC = util::crc32c_block_with_cksum(block, blksize, __builtin_offsetof(XfsDir3BlkHdr, crc));
    __builtin_memcpy(&hdr->hdr.crc, &CRC, sizeof(CRC));
}

auto dir2_validate_node_root(const XfsMountContext* ctx, const XfsDa3NodeHdr* hdr) -> int {
    if (hdr->info.hdr.magic.to_cpu() != XFS_DA3_NODE_MAGIC || hdr->level.to_cpu() != 1) {
        return -EOPNOTSUPP;
    }
    size_t const COUNT = hdr->count.to_cpu();
    size_t const CAPACITY = dir2_node_capacity(ctx);
    if (COUNT == 0 || COUNT > CAPACITY) {
        return -EINVAL;
    }

    xfs_fileoff_t const LEAF_ROOT = XFS_DIR2_LEAF_OFFSET >> ctx->block_log;
    xfs_fileoff_t const FREE_ROOT = XFS_DIR2_FREE_OFFSET >> ctx->block_log;
    uint32_t const FBS = 1U << ctx->dir_blk_log;
    const auto* entries = dir2_node_entries(hdr);
    uint32_t previous_hash = 0;
    for (size_t i = 0; i < COUNT; i++) {
        uint32_t const HASH = entries[i].hashval.to_cpu();
        xfs_fileoff_t const BEFORE = entries[i].before.to_cpu();
        if ((i != 0 && HASH < previous_hash) || BEFORE <= LEAF_ROOT || BEFORE >= FREE_ROOT || (BEFORE - LEAF_ROOT) % FBS != 0) {
            return -EINVAL;
        }
        previous_hash = HASH;
    }
    return 0;
}

auto dir2_validate_leafn(const XfsMountContext* ctx, const XfsDir3LeafHdr* hdr, size_t* live_count_out) -> int {
    if (hdr->info.hdr.magic.to_cpu() != XFS_DIR3_LEAFN_MAGIC) {
        return -EINVAL;
    }
    size_t const COUNT = hdr->count.to_cpu();
    size_t const STALE = hdr->stale.to_cpu();
    if (COUNT > dir2_leafn_capacity(ctx) || STALE > COUNT) {
        return -EINVAL;
    }
    const auto* entries = dir2_leaf_entries(hdr);
    size_t actual_stale = 0;
    uint32_t previous_hash = 0;
    for (size_t i = 0; i < COUNT; i++) {
        if (i != 0 && entries[i].hashval.to_cpu() < previous_hash) {
            return -EINVAL;
        }
        previous_hash = entries[i].hashval.to_cpu();
        if (entries[i].address.to_cpu() == XFS_DIR2_NULL_DATAPTR) {
            actual_stale++;
        }
    }
    if (actual_stale != STALE) {
        return -EINVAL;
    }
    if (live_count_out != nullptr) {
        *live_count_out = COUNT - STALE;
    }
    return 0;
}

auto dir2_validate_free_block(const XfsMountContext* ctx, const XfsDir3FreeHdr* hdr, size_t minimum_valid) -> int {
    if (hdr->hdr.magic.to_cpu() != XFS_DIR3_FREE_MAGIC || hdr->firstdb.to_cpu() != 0) {
        return -EINVAL;
    }
    size_t const NVALID = hdr->nvalid.to_cpu();
    size_t const NUSED = hdr->nused.to_cpu();
    if (NVALID < minimum_valid || NVALID > dir2_free_capacity(ctx) || NUSED > NVALID) {
        return -EINVAL;
    }
    size_t actual_used = 0;
    const auto* bests = dir2_free_bests(hdr);
    for (size_t i = 0; i < NVALID; i++) {
        if (bests[i].to_cpu() != XFS_DIR2_NULL_DATAOFF) {
            actual_used++;
        }
    }
    return actual_used == NUSED ? 0 : -EINVAL;
}

void dir2_init_leafn_header(XfsInode* dp, xfs_fsblock_t disk_block, XfsDir3LeafHdr* hdr, xfs_fileoff_t back, xfs_fileoff_t forw,
                            size_t count) {
    __builtin_memset(hdr, 0, sizeof(*hdr));
    hdr->info.hdr.forw = Be32::from_cpu(static_cast<uint32_t>(forw));
    hdr->info.hdr.back = Be32::from_cpu(static_cast<uint32_t>(back));
    hdr->info.hdr.magic = Be16::from_cpu(static_cast<uint16_t>(XFS_DIR3_LEAFN_MAGIC));
    hdr->info.blkno = Be64::from_cpu(dir2_device_blkno(dp->mount, disk_block));
    hdr->info.owner = Be64::from_cpu(dp->ino);
    __builtin_memcpy(&hdr->info.uuid, &dp->mount->uuid, sizeof(XfsUuidT));
    hdr->count = Be16::from_cpu(static_cast<uint16_t>(count));
}

void dir2_init_node_header(XfsInode* dp, xfs_fsblock_t disk_block, XfsDa3NodeHdr* hdr, size_t count) {
    __builtin_memset(hdr, 0, sizeof(*hdr));
    hdr->info.hdr.magic = Be16::from_cpu(static_cast<uint16_t>(XFS_DA3_NODE_MAGIC));
    hdr->info.blkno = Be64::from_cpu(dir2_device_blkno(dp->mount, disk_block));
    hdr->info.owner = Be64::from_cpu(dp->ino);
    __builtin_memcpy(&hdr->info.uuid, &dp->mount->uuid, sizeof(XfsUuidT));
    hdr->count = Be16::from_cpu(static_cast<uint16_t>(count));
    hdr->level = Be16::from_cpu(1);
}

void dir2_init_free_header(XfsInode* dp, xfs_fsblock_t disk_block, XfsDir3FreeHdr* hdr, size_t nvalid, size_t nused) {
    __builtin_memset(hdr, 0, sizeof(*hdr));
    hdr->hdr.magic = Be32::from_cpu(XFS_DIR3_FREE_MAGIC);
    hdr->hdr.blkno = Be64::from_cpu(dir2_device_blkno(dp->mount, disk_block));
    hdr->hdr.owner = Be64::from_cpu(dp->ino);
    __builtin_memcpy(&hdr->hdr.uuid, &dp->mount->uuid, sizeof(XfsUuidT));
    hdr->firstdb = Be32::from_cpu(0);
    hdr->nvalid = Be32::from_cpu(static_cast<uint32_t>(nvalid));
    hdr->nused = Be32::from_cpu(static_cast<uint32_t>(nused));
}

auto dir2_copy_live_leaf_entries(const XfsDir3LeafHdr* hdr, XfsDir2LeafEntry* out, size_t out_capacity, size_t* count_out) -> int {
    size_t out_count = 0;
    const auto* entries = dir2_leaf_entries(hdr);
    for (size_t i = 0; i < hdr->count.to_cpu(); i++) {
        if (entries[i].address.to_cpu() == XFS_DIR2_NULL_DATAPTR) {
            continue;
        }
        if (out_count >= out_capacity) {
            return -ENOSPC;
        }
        out[out_count++] = entries[i];
    }
    *count_out = out_count;
    return 0;
}

auto dir2_insert_sorted_leaf_entry(XfsDir2LeafEntry* entries, size_t* count, size_t capacity, xfs_dahash_t hash, xfs_dir2_dataptr_t dataptr)
    -> int {
    if (*count >= capacity) {
        return -ENOSPC;
    }
    size_t pos = 0;
    while (pos < *count && entries[pos].hashval.to_cpu() <= hash) {
        pos++;
    }
    __builtin_memmove(entries + pos + 1, entries + pos, (*count - pos) * sizeof(*entries));
    entries[pos].hashval = Be32::from_cpu(hash);
    entries[pos].address = Be32::from_cpu(dataptr);
    (*count)++;
    return 0;
}

auto dir2_node_replacement_order_valid(const XfsDaNodeEntry* root_entries, size_t root_count, size_t child_index, uint32_t left_max,
                                       uint32_t right_max) -> bool {
    if (root_entries == nullptr || child_index >= root_count || left_max > right_max) {
        return false;
    }
    if (child_index != 0 && left_max < root_entries[child_index - 1].hashval.to_cpu()) {
        return false;
    }
    return child_index + 1 >= root_count || right_max <= root_entries[child_index + 1].hashval.to_cpu();
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
    int rc = sf_repair_offset_tags(new_data, OLD_SIZE, ctx);
    uint16_t off_val = 0;
    if (rc == 0) {
        rc = sf_next_offset_tag(new_data, OLD_SIZE, ctx, &off_val);
    }
    if (rc != 0) {
        delete[] new_data;
        return rc;
    }

    // Update the header: increment count
    auto* new_hdr = reinterpret_cast<XfsDir2SfHdr*>(new_data);
    new_hdr->count++;

    // Append entry at old_size offset
    uint8_t* entry_ptr = new_data + OLD_SIZE;

    // namelen
    entry_ptr[0] = static_cast<uint8_t>(namelen);
    // Offset tags back readdir cookies.  Keep them monotonic across remove/add
    // churn so callers can resume after deleting previously returned names.
    entry_ptr[1] = static_cast<uint8_t>(off_val >> 8U);
    entry_ptr[2] = static_cast<uint8_t>(off_val & 0xffU);
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

    BufHead* bh = (FBS == 1) ? xfs_buf_get(ctx, DISK_BLOCK) : xfs_buf_get_multi(ctx, DISK_BLOCK, FBS);
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
    __builtin_memcpy(&hdr3->hdr.crc, &CRC, sizeof(CRC));

    // The transaction owns a reference until commit, so the follow-up
    // dir2_block_addname read in this same transaction can reuse the cached
    // exact-span buffer. Commit dirties it only after the log write.
    xfs_trans_log_buf_full(tp, bh);
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

    constexpr size_t INITIAL_BESTCOUNT = 2;
    size_t const LEAF_CAPACITY =
        (BLKSIZE - sizeof(XfsDir3LeafHdr) - sizeof(XfsDir2LeafTail) - (INITIAL_BESTCOUNT * sizeof(Be16))) / sizeof(XfsDir2LeafEntry);
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

    rc = xfs_trans_capture_buf(tp, block_bh);
    if (rc != 0) {
        delete[] leaf_copy;
        brelse(block_bh);
        return rc;
    }

    xfs_fsblock_t new_data_disk = 0;
    rc = dir2_alloc_mapped_dir_block(dp, tp, dir2_db_to_fsbno(ctx, 1), &new_data_disk);
    if (rc != 0) {
        delete[] leaf_copy;
        brelse(block_bh);
        return rc;
    }

    BufHead* new_data_bh = dir2_buf_get_dir_block(ctx, new_data_disk);
    if (new_data_bh == nullptr) {
        delete[] leaf_copy;
        brelse(block_bh);
        return -EIO;
    }

    uint8_t* new_data = new_data_bh->data;
    __builtin_memset(new_data, 0, BLKSIZE);
    dir2_init_data_header(dp, new_data_disk, XFS_DIR3_DATA_MAGIC, new_data);
    dir2_make_data_free(ctx, new_data, DATA_START, BLKSIZE, DATA_START, BLKSIZE - DATA_START);
    uint16_t const NEW_DATA_BEST = reinterpret_cast<XfsDir3DataHdr*>(new_data)->best_free.at(0).length.to_cpu();
    dir2_recompute_data_crc(new_data, BLKSIZE);
    xfs_trans_log_buf_full(tp, new_data_bh);
    brelse(new_data_bh);

    xfs_fileoff_t const LEAF_FSBNO = XFS_DIR2_LEAF_OFFSET >> ctx->block_log;
    xfs_fsblock_t leaf_disk = 0;
    rc = dir2_alloc_mapped_dir_block(dp, tp, LEAF_FSBNO, &leaf_disk);
    if (rc != 0) {
        delete[] leaf_copy;
        brelse(block_bh);
        return rc;
    }

    BufHead* leaf_bh = dir2_buf_get_dir_block(ctx, leaf_disk);
    if (leaf_bh == nullptr) {
        delete[] leaf_copy;
        brelse(block_bh);
        return -EIO;
    }

    data_hdr->hdr.magic = Be32::from_cpu(XFS_DIR3_DATA_MAGIC);
    dir2_make_data_free(ctx, block, DATA_START, BLKSIZE, OLD_LEAF_START, BLKSIZE - OLD_LEAF_START);
    uint16_t const OLD_DATA_BEST = data_hdr->best_free.at(0).length.to_cpu();
    dir2_recompute_data_crc(block, BLKSIZE);
    xfs_trans_log_buf_full(tp, block_bh);
    brelse(block_bh);

    uint8_t* leaf_block = leaf_bh->data;
    __builtin_memset(leaf_block, 0, BLKSIZE);
    auto* leaf_hdr = reinterpret_cast<XfsDir3LeafHdr*>(leaf_block);
    dir2_init_leaf_header(dp, leaf_disk, leaf_hdr, LEAF_COUNT, STALE_COUNT);
    __builtin_memcpy(dir2_leaf_entries(leaf_hdr), leaf_copy, LEAF_BYTES);
    auto* leaf_tail = reinterpret_cast<XfsDir2LeafTail*>(leaf_block + BLKSIZE - sizeof(XfsDir2LeafTail));
    leaf_tail->bestcount = Be32::from_cpu(INITIAL_BESTCOUNT);
    auto* bests = dir2_leaf_bests(ctx, leaf_hdr);
    bests[0] = Be16::from_cpu(OLD_DATA_BEST);
    bests[1] = Be16::from_cpu(NEW_DATA_BEST);
    dir2_recompute_leaf_crc(leaf_block, BLKSIZE);
    xfs_trans_log_buf_full(tp, leaf_bh);
    brelse(leaf_bh);

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
    size_t const LEAF_BYTES = static_cast<size_t>(leaf_count) * sizeof(XfsDir2LeafEntry);
    if (LEAF_BYTES > BLKSIZE - sizeof(XfsDir3DataHdr) - sizeof(XfsDir2BlockTail)) {
        brelse(bh);
        return -EINVAL;
    }

    // Leaf entries are just before the tail
    uint8_t* leaf_start = block + BLKSIZE - sizeof(XfsDir2BlockTail) - LEAF_BYTES;

    auto* blp = reinterpret_cast<XfsDir2LeafEntry*>(leaf_start);
    uint32_t const STALE_COUNT = btp->stale.to_cpu();
    size_t const ACTUAL_STALE = dir2_count_stale_leaf_entries(blp, leaf_count);
    if (static_cast<size_t>(STALE_COUNT) != ACTUAL_STALE) {
        brelse(bh);
        return -EINVAL;
    }

    // Scan data area for a free space entry large enough
    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    auto const DATA_END = static_cast<size_t>(leaf_start - block);
    constexpr size_t LEAF_SLOT_SIZE = sizeof(XfsDir2LeafEntry);

    size_t offset = DATA_START;
    size_t found_offset = 0;
    uint16_t found_free_len = 0;
    bool found_free = false;
    size_t tail_free_offset = 0;
    uint16_t tail_free_len = 0;
    bool found_tail_free = false;

    while (offset < DATA_END) {
        uint16_t free_len = 0;
        if (dir2_data_unused_at_if_valid(block, offset, DATA_END, &free_len)) {
            bool const IS_TAIL_FREE = offset + free_len == DATA_END;
            if (IS_TAIL_FREE) {
                tail_free_offset = offset;
                tail_free_len = free_len;
                found_tail_free = true;
            }

            size_t usable_len = free_len;
            if (STALE_COUNT == 0 && IS_TAIL_FREE) {
                usable_len = usable_len >= LEAF_SLOT_SIZE ? usable_len - LEAF_SLOT_SIZE : 0;
            }
            if (!found_free && usable_len >= NEED_LEN) {
                found_offset = offset;
                found_free_len = static_cast<uint16_t>(usable_len);
                found_free = true;
            }
            offset += free_len;
            continue;
        }

        const XfsDir2DataEntry* dep = nullptr;
        size_t dep_size = 0;
        if (!dir2_data_entry_at_if_valid(ctx, block, offset, DATA_END, &dep, &dep_size)) {
            brelse(bh);
            return -EINVAL;
        }
        offset += dep_size;
    }

    if (offset != DATA_END) {
        brelse(bh);
        return -EINVAL;
    }
    if (!found_free || (STALE_COUNT == 0 && (!found_tail_free || static_cast<size_t>(tail_free_len) < LEAF_SLOT_SIZE))) {
        brelse(bh);
        return -ENOSPC;  // block full - would need conversion to leaf format
    }

    // Add a leaf entry for the new name.  Precompute the sorted slot before
    // mutating the data area so readdir cannot expose an unindexed entry.
    xfs_dahash_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);
    int stale_idx = -1;
    int insert_pos = 0;
    if (STALE_COUNT != 0) {
        for (int i = 0; std::cmp_less(i, leaf_count); i++) {
            if (blp[i].address.to_cpu() != XFS_DIR2_NULL_DATAPTR && blp[i].hashval.to_cpu() <= HASH) {
                insert_pos = i + 1;
            }
        }

        int best_dist = static_cast<int>(leaf_count) + 1;
        for (int i = 0; std::cmp_less(i, leaf_count); i++) {
            if (blp[i].address.to_cpu() == XFS_DIR2_NULL_DATAPTR) {
                int const DIST = (i >= insert_pos) ? (i - insert_pos) : (insert_pos - i);
                if (DIST < best_dist) {
                    best_dist = DIST;
                    stale_idx = i;
                }
            }
        }

        if (stale_idx < 0) {
            brelse(bh);
            return -EINVAL;
        }
    }

    int const CAPTURE_RC = xfs_trans_capture_buf(tp, bh);
    if (CAPTURE_RC != 0) {
        brelse(bh);
        return CAPTURE_RC;
    }

    // Reserve the independently-owned leaf slot before writing either area.
    // When the selected data region is also the trailing free record, its
    // usable length was already reduced during preflight.
    uint16_t old_free_len = found_free_len;
    size_t current_data_end = DATA_END;
    if (STALE_COUNT == 0) {
        current_data_end -= LEAF_SLOT_SIZE;
        size_t const REMAINING_TAIL = static_cast<size_t>(tail_free_len) - LEAF_SLOT_SIZE;
        if (REMAINING_TAIL != 0) {
            auto* tail_unused = reinterpret_cast<XfsDir2DataUnused*>(block + tail_free_offset);
            tail_unused->freetag = Be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
            tail_unused->length = Be16::from_cpu(static_cast<uint16_t>(REMAINING_TAIL));
            auto* tail_tag = reinterpret_cast<Be16*>(block + current_data_end - sizeof(Be16));
            *tail_tag = Be16::from_cpu(static_cast<uint16_t>(tail_free_offset));
        }
        if (found_offset == tail_free_offset) {
            old_free_len = static_cast<uint16_t>(REMAINING_TAIL);
        }
    }

    // Write the new data entry at found_offset.

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
    if (old_free_len > NEED_LEN) {
        size_t const REMAINING = old_free_len - NEED_LEN;
        auto* new_unused = reinterpret_cast<XfsDir2DataUnused*>(block + ENTRY_END);
        new_unused->freetag = Be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
        new_unused->length = Be16::from_cpu(static_cast<uint16_t>(REMAINING));
        // Tag for unused entry: at the very end
        auto* unused_tag = reinterpret_cast<Be16*>(block + ENTRY_END + REMAINING - sizeof(Be16));
        *unused_tag = Be16::from_cpu(static_cast<uint16_t>(ENTRY_END));
    }

    // Compute the dataptr for this entry
    // dataptr = (byte_offset_in_dir_block) >> XFS_DIR2_DATA_ALIGN_LOG
    auto dataptr = static_cast<uint32_t>(found_offset >> XFS_DIR2_DATA_ALIGN_LOG);

    // We need to insert a new leaf entry into the sorted leaf array.
    // First, check if we can reclaim a stale entry.
    if (STALE_COUNT > 0) {
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
    } else {
        // No stale entries - grow the leaf area by shifting it down one slot.
        // This consumes sizeof(XfsDir2LeafEntry) bytes from the free space.
        size_t const NEW_LEAF_BYTES = (static_cast<size_t>(leaf_count) + 1) * sizeof(XfsDir2LeafEntry);
        size_t const NEW_LEAF_START = BLKSIZE - sizeof(XfsDir2BlockTail) - NEW_LEAF_BYTES;

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

    // Rebuild bestfree over the exact post-growth data span.  This records
    // both an interior insertion remainder and the independently-shortened
    // trailing free record.
    auto* mutable_hdr = reinterpret_cast<XfsDir3DataHdr*>(block);
    dir2_rebuild_data_bestfree(ctx, block, DATA_START, current_data_end);

    // Recompute CRC over the entire block
    mutable_hdr->hdr.crc = Be32{0};
    uint32_t const CRC = util::crc32c_block_with_cksum(block, BLKSIZE, 4);
    __builtin_memcpy(&mutable_hdr->hdr.crc, &CRC, sizeof(CRC));

    xfs_trans_log_buf_full(tp, bh);
    brelse(bh);

    // Update directory inode metadata
    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);

    return 0;
}

auto dir2_leaf1_to_node_addname(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype, XfsTransaction* tp) -> int {
    XfsMountContext* ctx = dp->mount;
    size_t const LEAFN_CAPACITY = dir2_leafn_capacity(ctx);
    size_t const NEED_LEN = dir2_data_entsize(ctx, static_cast<uint8_t>(namelen));
    xfs_dahash_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);
    xfs_fileoff_t const ROOT_FSBNO = XFS_DIR2_LEAF_OFFSET >> ctx->block_log;
    xfs_fileoff_t const FREE_FSBNO = XFS_DIR2_FREE_OFFSET >> ctx->block_log;
    uint32_t const FBS = 1U << ctx->dir_blk_log;
    xfs_fileoff_t const LEFT_FSBNO = ROOT_FSBNO + FBS;
    xfs_fileoff_t const RIGHT_FSBNO = LEFT_FSBNO + FBS;
    if (RIGHT_FSBNO >= FREE_FSBNO) {
        return -EFBIG;
    }

    auto* merged = new (std::nothrow) XfsDir2LeafEntry[LEAFN_CAPACITY + 1];
    if (merged == nullptr) {
        return -ENOMEM;
    }

    BufHead* root_bh = nullptr;
    xfs_fsblock_t root_disk = 0;
    int rc = dir2_read_mapped_dir_block(dp, ROOT_FSBNO, &root_bh, &root_disk);
    if (rc != 0) {
        delete[] merged;
        return rc;
    }
    auto* old_leaf = reinterpret_cast<XfsDir3LeafHdr*>(root_bh->data);
    size_t old_bestcount = 0;
    size_t old_capacity = 0;
    if (old_leaf->info.hdr.magic.to_cpu() != XFS_DIR3_LEAF_MAGIC ||
        !dir2_leaf_layout(ctx, old_leaf, SIZE_MAX, &old_bestcount, &old_capacity) || old_leaf->count.to_cpu() > old_capacity ||
        old_bestcount != dir2_data_block_count(dp)) {
        brelse(root_bh);
        delete[] merged;
        return -EINVAL;
    }

    size_t merged_count = 0;
    rc = dir2_copy_live_leaf_entries(old_leaf, merged, LEAFN_CAPACITY + 1, &merged_count);
    if (rc != 0 || merged_count >= LEAFN_CAPACITY) {
        brelse(root_bh);
        delete[] merged;
        return rc != 0 ? rc : -ENOSPC;
    }

    BufHead* data_bh = nullptr;
    xfs_dir2_db_t db = 0;
    size_t free_off = 0;
    size_t free_len = 0;
    bool grow_data = false;
    rc = dir2_leaf_node_find_free_region(dp, NEED_LEN, &data_bh, &db, &free_off, &free_len);
    if (rc == -ENOSPC) {
        grow_data = true;
        db = static_cast<xfs_dir2_db_t>(dir2_data_block_count(dp));
        free_off = sizeof(XfsDir3DataHdr);
        free_len = ctx->dir_blk_size - free_off;
    } else if (rc != 0) {
        brelse(root_bh);
        delete[] merged;
        return rc;
    }

    size_t const NEW_BESTCOUNT = old_bestcount + (grow_data ? 1 : 0);
    auto* bests_copy = new (std::nothrow) Be16[NEW_BESTCOUNT];
    if (bests_copy == nullptr || NEW_BESTCOUNT > dir2_free_capacity(ctx)) {
        delete[] bests_copy;
        if (data_bh != nullptr) {
            brelse(data_bh);
        }
        brelse(root_bh);
        delete[] merged;
        return bests_copy == nullptr ? -ENOMEM : -EFBIG;
    }
    const Be16* old_bests = dir2_leaf_bests(ctx, old_leaf);
    for (size_t i = 0; i < old_bestcount; i++) {
        bests_copy[i] = old_bests[i];
    }

    xfs_dir2_dataptr_t const DATAPTR = dir2_db_off_to_dataptr(ctx, db, free_off);
    rc = dir2_insert_sorted_leaf_entry(merged, &merged_count, LEAFN_CAPACITY + 1, HASH, DATAPTR);
    if (rc != 0 || merged_count > LEAFN_CAPACITY) {
        delete[] bests_copy;
        if (data_bh != nullptr) {
            brelse(data_bh);
        }
        brelse(root_bh);
        delete[] merged;
        return rc != 0 ? rc : -ENOSPC;
    }
    if (!dir2_mapped_range_is_hole(dp, LEFT_FSBNO) || !dir2_mapped_range_is_hole(dp, RIGHT_FSBNO) ||
        !dir2_mapped_range_is_hole(dp, FREE_FSBNO) || (grow_data && !dir2_mapped_range_is_hole(dp, dir2_db_to_fsbno(ctx, db)))) {
        delete[] bests_copy;
        if (data_bh != nullptr) {
            brelse(data_bh);
        }
        brelse(root_bh);
        delete[] merged;
        return -EEXIST;
    }

    rc = xfs_trans_capture_buf(tp, root_bh);
    if (rc == 0 && data_bh != nullptr) {
        rc = xfs_trans_capture_buf(tp, data_bh);
    }
    if (rc != 0) {
        delete[] bests_copy;
        if (data_bh != nullptr) {
            brelse(data_bh);
        }
        brelse(root_bh);
        delete[] merged;
        return rc;
    }

    xfs_fsblock_t left_disk = 0;
    xfs_fsblock_t right_disk = 0;
    xfs_fsblock_t free_disk = 0;
    xfs_fsblock_t data_disk = 0;
    BufHead* left_bh = nullptr;
    BufHead* right_bh = nullptr;
    BufHead* free_bh = nullptr;

    auto release_all = [&]() {
        if (free_bh != nullptr) {
            brelse(free_bh);
        }
        if (right_bh != nullptr) {
            brelse(right_bh);
        }
        if (left_bh != nullptr) {
            brelse(left_bh);
        }
        if (data_bh != nullptr) {
            brelse(data_bh);
        }
        brelse(root_bh);
        delete[] bests_copy;
        delete[] merged;
    };

    rc = dir2_alloc_mapped_dir_block(dp, tp, LEFT_FSBNO, &left_disk);
    if (rc == 0) {
        rc = dir2_alloc_mapped_dir_block(dp, tp, RIGHT_FSBNO, &right_disk);
    }
    if (rc == 0) {
        rc = dir2_alloc_mapped_dir_block(dp, tp, FREE_FSBNO, &free_disk);
    }
    if (rc == 0 && grow_data) {
        rc = dir2_alloc_mapped_dir_block(dp, tp, dir2_db_to_fsbno(ctx, db), &data_disk);
    }
    if (rc != 0) {
        release_all();
        return rc;
    }

    left_bh = dir2_buf_get_dir_block(ctx, left_disk);
    right_bh = dir2_buf_get_dir_block(ctx, right_disk);
    free_bh = dir2_buf_get_dir_block(ctx, free_disk);
    if (grow_data) {
        data_bh = dir2_buf_get_dir_block(ctx, data_disk);
    }
    if (left_bh == nullptr || right_bh == nullptr || free_bh == nullptr || data_bh == nullptr) {
        release_all();
        return -EIO;
    }

    if (grow_data) {
        __builtin_memset(data_bh->data, 0, ctx->dir_blk_size);
        dir2_init_data_header(dp, data_disk, XFS_DIR3_DATA_MAGIC, data_bh->data);
        dir2_make_data_free(ctx, data_bh->data, sizeof(XfsDir3DataHdr), ctx->dir_blk_size, sizeof(XfsDir3DataHdr),
                            ctx->dir_blk_size - sizeof(XfsDir3DataHdr));
        dp->size = (static_cast<uint64_t>(db) + 1ULL) * static_cast<uint64_t>(ctx->dir_blk_size);
    }

    dir2_write_data_entry(ctx, data_bh->data, sizeof(XfsDir3DataHdr), ctx->dir_blk_size, free_off, free_len, name, namelen, ino, ftype);
    dir2_recompute_data_crc(data_bh->data, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, data_bh);
    auto const* updated_data_hdr = reinterpret_cast<const XfsDir3DataHdr*>(data_bh->data);
    bests_copy[db] = updated_data_hdr->best_free.at(0).length;

    size_t const LEFT_COUNT = merged_count / 2;
    size_t const RIGHT_COUNT = merged_count - LEFT_COUNT;
    __builtin_memset(left_bh->data, 0, ctx->dir_blk_size);
    __builtin_memset(right_bh->data, 0, ctx->dir_blk_size);
    auto* left_hdr = reinterpret_cast<XfsDir3LeafHdr*>(left_bh->data);
    auto* right_hdr = reinterpret_cast<XfsDir3LeafHdr*>(right_bh->data);
    dir2_init_leafn_header(dp, left_disk, left_hdr, 0, RIGHT_FSBNO, LEFT_COUNT);
    dir2_init_leafn_header(dp, right_disk, right_hdr, LEFT_FSBNO, 0, RIGHT_COUNT);
    __builtin_memcpy(dir2_leaf_entries(left_hdr), merged, LEFT_COUNT * sizeof(*merged));
    __builtin_memcpy(dir2_leaf_entries(right_hdr), merged + LEFT_COUNT, RIGHT_COUNT * sizeof(*merged));
    dir2_recompute_leaf_crc(left_bh->data, ctx->dir_blk_size);
    dir2_recompute_leaf_crc(right_bh->data, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, left_bh);
    xfs_trans_log_buf_full(tp, right_bh);

    __builtin_memset(free_bh->data, 0, ctx->dir_blk_size);
    auto* free_hdr = reinterpret_cast<XfsDir3FreeHdr*>(free_bh->data);
    size_t used = 0;
    for (size_t i = 0; i < NEW_BESTCOUNT; i++) {
        if (bests_copy[i].to_cpu() != XFS_DIR2_NULL_DATAOFF) {
            used++;
        }
    }
    dir2_init_free_header(dp, free_disk, free_hdr, NEW_BESTCOUNT, used);
    __builtin_memcpy(dir2_free_bests(free_hdr), bests_copy, NEW_BESTCOUNT * sizeof(Be16));
    dir2_recompute_free_crc(free_bh->data, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, free_bh);

    __builtin_memset(root_bh->data, 0, ctx->dir_blk_size);
    auto* root_hdr = reinterpret_cast<XfsDa3NodeHdr*>(root_bh->data);
    dir2_init_node_header(dp, root_disk, root_hdr, 2);
    auto* root_entries = dir2_node_entries(root_hdr);
    root_entries[0].hashval = merged[LEFT_COUNT - 1].hashval;
    root_entries[0].before = Be32::from_cpu(static_cast<uint32_t>(LEFT_FSBNO));
    root_entries[1].hashval = merged[merged_count - 1].hashval;
    root_entries[1].before = Be32::from_cpu(static_cast<uint32_t>(RIGHT_FSBNO));
    dir2_recompute_leaf_crc(root_bh->data, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, root_bh);

    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);
    release_all();
    return 0;
}

auto dir2_node_addname(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype, XfsTransaction* tp) -> int {
    XfsMountContext* ctx = dp->mount;
    size_t const LEAFN_CAPACITY = dir2_leafn_capacity(ctx);
    size_t const NEED_LEN = dir2_data_entsize(ctx, static_cast<uint8_t>(namelen));
    xfs_dahash_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);
    xfs_fileoff_t const ROOT_FSBNO = XFS_DIR2_LEAF_OFFSET >> ctx->block_log;
    xfs_fileoff_t const FREE_FSBNO = XFS_DIR2_FREE_OFFSET >> ctx->block_log;

    auto* merged = new (std::nothrow) XfsDir2LeafEntry[LEAFN_CAPACITY + 1];
    if (merged == nullptr) {
        return -ENOMEM;
    }

    BufHead* root_bh = nullptr;
    int rc = dir2_read_mapped_dir_block(dp, ROOT_FSBNO, &root_bh);
    if (rc != 0) {
        delete[] merged;
        return rc;
    }
    auto* root_hdr = reinterpret_cast<XfsDa3NodeHdr*>(root_bh->data);
    rc = dir2_validate_node_root(ctx, root_hdr);
    if (rc != 0) {
        brelse(root_bh);
        delete[] merged;
        return rc;
    }

    auto* root_entries = dir2_node_entries(root_hdr);
    size_t const ROOT_COUNT = root_hdr->count.to_cpu();
    size_t child_index = ROOT_COUNT - 1;
    for (size_t i = 0; i < ROOT_COUNT; i++) {
        if (HASH <= root_entries[i].hashval.to_cpu()) {
            child_index = i;
            break;
        }
    }
    xfs_fileoff_t const CHILD_FSBNO = root_entries[child_index].before.to_cpu();
    BufHead* child_bh = nullptr;
    int child_rc = dir2_read_mapped_dir_block(dp, CHILD_FSBNO, &child_bh);
    if (child_rc != 0) {
        brelse(root_bh);
        delete[] merged;
        return child_rc;
    }
    auto* child_hdr = reinterpret_cast<XfsDir3LeafHdr*>(child_bh->data);
    size_t live_count = 0;
    rc = dir2_validate_leafn(ctx, child_hdr, &live_count);
    if (rc == 0) {
        rc = dir2_copy_live_leaf_entries(child_hdr, merged, LEAFN_CAPACITY + 1, &live_count);
    }
    bool const SPLIT = live_count >= LEAFN_CAPACITY;
    if (rc != 0 || (SPLIT && ROOT_COUNT >= dir2_node_capacity(ctx))) {
        brelse(child_bh);
        brelse(root_bh);
        delete[] merged;
        return rc != 0 ? rc : -ENOSPC;
    }

    BufHead* free_bh = nullptr;
    rc = dir2_read_mapped_dir_block(dp, FREE_FSBNO, &free_bh);
    if (rc != 0) {
        brelse(child_bh);
        brelse(root_bh);
        delete[] merged;
        return rc;
    }
    auto* free_hdr = reinterpret_cast<XfsDir3FreeHdr*>(free_bh->data);
    size_t const DATA_BLOCKS = dir2_data_block_count(dp);
    rc = dir2_validate_free_block(ctx, free_hdr, DATA_BLOCKS);
    if (rc != 0) {
        brelse(free_bh);
        brelse(child_bh);
        brelse(root_bh);
        delete[] merged;
        return rc;
    }

    BufHead* data_bh = nullptr;
    xfs_dir2_db_t db = 0;
    size_t free_off = 0;
    size_t free_len = 0;
    bool grow_data = false;
    rc = dir2_leaf_node_find_free_region(dp, NEED_LEN, &data_bh, &db, &free_off, &free_len);
    if (rc == -ENOSPC) {
        grow_data = true;
        db = static_cast<xfs_dir2_db_t>(DATA_BLOCKS);
        free_off = sizeof(XfsDir3DataHdr);
        free_len = ctx->dir_blk_size - free_off;
        if (db >= dir2_free_capacity(ctx) || free_hdr->nvalid.to_cpu() != db) {
            rc = -EFBIG;
        } else {
            rc = 0;
        }
    }
    if (rc != 0) {
        brelse(free_bh);
        brelse(child_bh);
        brelse(root_bh);
        delete[] merged;
        return rc;
    }

    xfs_dir2_dataptr_t const DATAPTR = dir2_db_off_to_dataptr(ctx, db, free_off);
    rc = dir2_insert_sorted_leaf_entry(merged, &live_count, LEAFN_CAPACITY + 1, HASH, DATAPTR);
    if (rc != 0) {
        if (data_bh != nullptr) {
            brelse(data_bh);
        }
        brelse(free_bh);
        brelse(child_bh);
        brelse(root_bh);
        delete[] merged;
        return rc;
    }
    uint32_t const LEFT_MAX = merged[(SPLIT ? live_count / 2 : live_count) - 1].hashval.to_cpu();
    uint32_t const RIGHT_MAX = merged[live_count - 1].hashval.to_cpu();
    if (!dir2_node_replacement_order_valid(root_entries, ROOT_COUNT, child_index, LEFT_MAX, RIGHT_MAX)) {
        if (data_bh != nullptr) {
            brelse(data_bh);
        }
        brelse(free_bh);
        brelse(child_bh);
        brelse(root_bh);
        delete[] merged;
        return -EINVAL;
    }

    rc = xfs_trans_capture_buf(tp, root_bh);
    if (rc == 0) {
        rc = xfs_trans_capture_buf(tp, child_bh);
    }
    if (rc == 0) {
        rc = xfs_trans_capture_buf(tp, free_bh);
    }
    if (rc == 0 && data_bh != nullptr) {
        rc = xfs_trans_capture_buf(tp, data_bh);
    }
    if (rc != 0) {
        if (data_bh != nullptr) {
            brelse(data_bh);
        }
        brelse(free_bh);
        brelse(child_bh);
        brelse(root_bh);
        delete[] merged;
        return rc;
    }

    xfs_fileoff_t new_leaf_fsbno = 0;
    xfs_fsblock_t new_leaf_disk = 0;
    xfs_fsblock_t new_data_disk = 0;
    BufHead* new_leaf_bh = nullptr;
    BufHead* next_leaf_bh = nullptr;
    if (SPLIT) {
        rc = dir2_find_free_leaf_block(dp, &new_leaf_fsbno);
        if (rc == 0) {
            rc = dir2_alloc_mapped_dir_block(dp, tp, new_leaf_fsbno, &new_leaf_disk);
        }
        if (rc == 0) {
            new_leaf_bh = dir2_buf_get_dir_block(ctx, new_leaf_disk);
            if (new_leaf_bh == nullptr) {
                rc = -EIO;
            }
        }
        xfs_fileoff_t const OLD_FORW = child_hdr->info.hdr.forw.to_cpu();
        if (rc == 0 && OLD_FORW != 0) {
            rc = dir2_read_mapped_dir_block(dp, OLD_FORW, &next_leaf_bh);
            if (rc == 0) {
                rc = dir2_validate_leafn(ctx, reinterpret_cast<XfsDir3LeafHdr*>(next_leaf_bh->data));
            }
            if (rc == 0) {
                rc = xfs_trans_capture_buf(tp, next_leaf_bh);
            }
        }
    }
    if (rc == 0 && grow_data) {
        rc = dir2_alloc_mapped_dir_block(dp, tp, dir2_db_to_fsbno(ctx, db), &new_data_disk);
        if (rc == 0) {
            data_bh = dir2_buf_get_dir_block(ctx, new_data_disk);
            if (data_bh == nullptr) {
                rc = -EIO;
            }
        }
    }
    auto release_all = [&]() {
        if (next_leaf_bh != nullptr) {
            brelse(next_leaf_bh);
        }
        if (new_leaf_bh != nullptr) {
            brelse(new_leaf_bh);
        }
        if (data_bh != nullptr) {
            brelse(data_bh);
        }
        brelse(free_bh);
        brelse(child_bh);
        brelse(root_bh);
        delete[] merged;
    };
    if (rc != 0) {
        release_all();
        return rc;
    }

    if (grow_data) {
        __builtin_memset(data_bh->data, 0, ctx->dir_blk_size);
        dir2_init_data_header(dp, new_data_disk, XFS_DIR3_DATA_MAGIC, data_bh->data);
        dir2_make_data_free(ctx, data_bh->data, sizeof(XfsDir3DataHdr), ctx->dir_blk_size, sizeof(XfsDir3DataHdr),
                            ctx->dir_blk_size - sizeof(XfsDir3DataHdr));
        dp->size = (static_cast<uint64_t>(db) + 1ULL) * static_cast<uint64_t>(ctx->dir_blk_size);
    }
    dir2_write_data_entry(ctx, data_bh->data, sizeof(XfsDir3DataHdr), ctx->dir_blk_size, free_off, free_len, name, namelen, ino, ftype);
    dir2_recompute_data_crc(data_bh->data, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, data_bh);

    Be16* free_bests = dir2_free_bests(free_hdr);
    if (grow_data) {
        free_hdr->nvalid = Be32::from_cpu(static_cast<uint32_t>(db + 1));
        free_hdr->nused = Be32::from_cpu(free_hdr->nused.to_cpu() + 1);
    }
    auto const* data_hdr = reinterpret_cast<const XfsDir3DataHdr*>(data_bh->data);
    free_bests[db] = data_hdr->best_free.at(0).length;
    dir2_recompute_free_crc(free_bh->data, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, free_bh);

    if (!SPLIT) {
        child_hdr->count = Be16::from_cpu(static_cast<uint16_t>(live_count));
        child_hdr->stale = Be16::from_cpu(0);
        __builtin_memcpy(dir2_leaf_entries(child_hdr), merged, live_count * sizeof(*merged));
        root_entries[child_index].hashval = merged[live_count - 1].hashval;
        dir2_recompute_leaf_crc(child_bh->data, ctx->dir_blk_size);
        xfs_trans_log_buf_full(tp, child_bh);
    } else {
        size_t const LEFT_COUNT = live_count / 2;
        size_t const RIGHT_COUNT = live_count - LEFT_COUNT;
        xfs_fileoff_t const OLD_FORW = child_hdr->info.hdr.forw.to_cpu();
        child_hdr->info.hdr.forw = Be32::from_cpu(static_cast<uint32_t>(new_leaf_fsbno));
        child_hdr->count = Be16::from_cpu(static_cast<uint16_t>(LEFT_COUNT));
        child_hdr->stale = Be16::from_cpu(0);
        __builtin_memcpy(dir2_leaf_entries(child_hdr), merged, LEFT_COUNT * sizeof(*merged));
        dir2_recompute_leaf_crc(child_bh->data, ctx->dir_blk_size);
        xfs_trans_log_buf_full(tp, child_bh);

        __builtin_memset(new_leaf_bh->data, 0, ctx->dir_blk_size);
        auto* new_leaf_hdr = reinterpret_cast<XfsDir3LeafHdr*>(new_leaf_bh->data);
        dir2_init_leafn_header(dp, new_leaf_disk, new_leaf_hdr, CHILD_FSBNO, OLD_FORW, RIGHT_COUNT);
        __builtin_memcpy(dir2_leaf_entries(new_leaf_hdr), merged + LEFT_COUNT, RIGHT_COUNT * sizeof(*merged));
        dir2_recompute_leaf_crc(new_leaf_bh->data, ctx->dir_blk_size);
        xfs_trans_log_buf_full(tp, new_leaf_bh);

        if (next_leaf_bh != nullptr) {
            auto* next_hdr = reinterpret_cast<XfsDir3LeafHdr*>(next_leaf_bh->data);
            next_hdr->info.hdr.back = Be32::from_cpu(static_cast<uint32_t>(new_leaf_fsbno));
            dir2_recompute_leaf_crc(next_leaf_bh->data, ctx->dir_blk_size);
            xfs_trans_log_buf_full(tp, next_leaf_bh);
        }

        __builtin_memmove(root_entries + child_index + 2, root_entries + child_index + 1,
                          (ROOT_COUNT - child_index - 1) * sizeof(*root_entries));
        root_entries[child_index].hashval = merged[LEFT_COUNT - 1].hashval;
        root_entries[child_index + 1].hashval = merged[live_count - 1].hashval;
        root_entries[child_index + 1].before = Be32::from_cpu(static_cast<uint32_t>(new_leaf_fsbno));
        root_hdr->count = Be16::from_cpu(static_cast<uint16_t>(ROOT_COUNT + 1));
    }
    dir2_recompute_leaf_crc(root_bh->data, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, root_bh);
    dp->dirty = true;
    xfs_trans_log_inode(tp, dp);
    release_all();
    return 0;
}

auto dir2_leaf_node_addname(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype, XfsTransaction* tp) -> int {
    XfsMountContext const* ctx = dp->mount;
    size_t const NEED_LEN = dir2_data_entsize(ctx, static_cast<uint8_t>(namelen));
    xfs_dahash_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);

    BufHead* index_bh = nullptr;
    int rc = dir2_read_mapped_dir_block(dp, XFS_DIR2_LEAF_OFFSET >> ctx->block_log, &index_bh);
    if (rc != 0) {
        return rc;
    }
    uint16_t const INDEX_MAGIC = reinterpret_cast<XfsDa3Blkinfo*>(index_bh->data)->hdr.magic.to_cpu();
    brelse(index_bh);
    if (INDEX_MAGIC == XFS_DA3_NODE_MAGIC) {
        return dir2_node_addname(dp, name, namelen, ino, ftype, tp);
    }

    BufHead* leaf_bh = nullptr;
    rc = dir2_read_leaf_block(dp, &leaf_bh);
    if (rc != 0) {
        return (rc == -ENOSYS) ? -ENOSPC : rc;
    }

    auto* leaf_hdr = reinterpret_cast<XfsDir3LeafHdr*>(leaf_bh->data);
    int stale_idx = -1;
    int insert_pos = 0;
    rc = dir2_leaf_preflight_index_slot(ctx, leaf_hdr);
    if (rc == -ENOSPC && leaf_hdr->info.hdr.magic.to_cpu() == XFS_DIR3_LEAF_MAGIC) {
        brelse(leaf_bh);
        return dir2_leaf1_to_node_addname(dp, name, namelen, ino, ftype, tp);
    }
    if (rc != 0) {
        brelse(leaf_bh);
        return rc;
    }

    rc = xfs_trans_capture_buf(tp, leaf_bh);
    if (rc != 0) {
        brelse(leaf_bh);
        return rc;
    }

    BufHead* data_bh = nullptr;
    xfs_dir2_db_t db = 0;
    size_t free_off = 0;
    size_t free_len = 0;
    rc = dir2_leaf_node_find_free_region(dp, NEED_LEN, &data_bh, &db, &free_off, &free_len);
    if (rc == -ENOSPC) {
        size_t bestcount = 0;
        size_t capacity = 0;
        if (!dir2_leaf_layout(ctx, leaf_hdr, SIZE_MAX, &bestcount, &capacity)) {
            brelse(leaf_bh);
            return -EINVAL;
        }
        rc = dir2_leaf_preflight_index_slot(ctx, leaf_hdr, bestcount + 1);
        if (rc == -ENOSPC && leaf_hdr->info.hdr.magic.to_cpu() == XFS_DIR3_LEAF_MAGIC) {
            brelse(leaf_bh);
            return dir2_leaf1_to_node_addname(dp, name, namelen, ino, ftype, tp);
        }
        if (rc != 0) {
            brelse(leaf_bh);
            return rc;
        }
        rc = dir2_leaf_alloc_data_block(dp, tp, &data_bh, &db, &free_off, &free_len);
        if (rc == 0) {
            auto const* new_data_hdr = reinterpret_cast<const XfsDir3DataHdr*>(data_bh->data);
            rc = dir2_leaf_extend_bests(ctx, leaf_hdr, new_data_hdr->best_free.at(0).length.to_cpu());
        }
    }
    if (rc != 0) {
        if (data_bh != nullptr) {
            brelse(data_bh);
        }
        brelse(leaf_bh);
        return rc;
    }

    rc = xfs_trans_capture_buf(tp, data_bh);
    if (rc != 0) {
        brelse(data_bh);
        brelse(leaf_bh);
        return rc;
    }

    xfs_dir2_dataptr_t const DATAPTR = dir2_db_off_to_dataptr(ctx, db, free_off);
    rc = dir2_leaf_ensure_stale_slot(ctx, leaf_hdr);
    if (rc == 0) {
        rc = dir2_leaf_prepare_stale_insert(leaf_hdr, HASH, &stale_idx, &insert_pos);
    }
    if (rc != 0) {
        brelse(data_bh);
        brelse(leaf_bh);
        return rc;
    }

    uint8_t* data_block = data_bh->data;
    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    size_t const DATA_END = ctx->dir_blk_size;
    dir2_write_data_entry(ctx, data_block, DATA_START, DATA_END, free_off, free_len, name, namelen, ino, ftype);
    dir2_recompute_data_crc(data_block, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, data_bh);

    auto const* updated_data_hdr = reinterpret_cast<const XfsDir3DataHdr*>(data_block);
    rc = dir2_leaf_set_best(ctx, leaf_hdr, db, updated_data_hdr->best_free.at(0).length.to_cpu());
    if (rc != 0) {
        brelse(data_bh);
        brelse(leaf_bh);
        return rc;
    }
    dir2_leaf_reuse_stale_entry(leaf_hdr, HASH, DATAPTR, stale_idx, insert_pos);
    dir2_recompute_leaf_crc(leaf_bh->data, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, leaf_bh);

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
    int const REPAIR_RC = sf_repair_offset_tags(new_data, NEW_SIZE, ctx);
    if (REPAIR_RC != 0) {
        delete[] new_data;
        return REPAIR_RC;
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

            size_t const OFF = dir2_dataptr_to_off(ctx, ADDR);
            const XfsDir2DataEntry* dep = nullptr;
            size_t dep_size = 0;
            if (OFF < DATA_START || !dir2_data_entry_at_if_valid(ctx, block, OFF, DATA_END, &dep, &dep_size)) {
                continue;
            }

            if (dep->namelen == namelen && __builtin_memcmp(xfs_dir2_data_entry_name(dep), name, namelen) == 0) {
                match_idx = i;
                entry_off = OFF;
                entry_size = dep_size;
                break;
            }
        }
    }

    if (match_idx < 0) {
        brelse(bh);
        return -ENOENT;
    }

    int const CAPTURE_RC = xfs_trans_capture_buf(tp, bh);
    if (CAPTURE_RC != 0) {
        brelse(bh);
        return CAPTURE_RC;
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

            uint16_t free_len = 0;
            if (dir2_data_unused_at_if_valid(block, off, DATA_END, &free_len)) {
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

            const XfsDir2DataEntry* dep = nullptr;
            size_t dep_size = 0;
            if (!dir2_data_entry_at_if_valid(ctx, block, off, DATA_END, &dep, &dep_size)) {
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

            uint16_t free_len = 0;
            if (dir2_data_unused_at_if_valid(block, off, DATA_END, &free_len)) {
                BestFreeSlot const CUR{.off = static_cast<uint16_t>(off), .len = free_len};
                for (int idx = 0; idx < 3; idx++) {
                    if (CUR.len > best.at(static_cast<size_t>(idx)).len) {
                        for (int j = 2; j > idx; j--) {
                            best.at(static_cast<size_t>(j)) = best.at(static_cast<size_t>(j - 1));
                        }
                        best.at(static_cast<size_t>(idx)) = CUR;
                        break;
                    }
                }

                off += free_len;
                continue;
            }

            const XfsDir2DataEntry* dep = nullptr;
            size_t dep_size = 0;
            if (!dir2_data_entry_at_if_valid(ctx, block, off, DATA_END, &dep, &dep_size)) {
                break;
            }
            off += dep_size;
        }
    }

    for (int i = 0; i < 3; i++) {
        hdr->best_free.at(static_cast<size_t>(i)).offset = Be16::from_cpu(best.at(static_cast<size_t>(i)).off);
        hdr->best_free.at(static_cast<size_t>(i)).length = Be16::from_cpu(best.at(static_cast<size_t>(i)).len);
    }

    // Update CRC over the block
    hdr->hdr.crc = Be32{0};
    uint32_t const CRC = util::crc32c_block_with_cksum(block, BLKSIZE, 4);
    __builtin_memcpy(&hdr->hdr.crc, &CRC, sizeof(CRC));

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

    xfs_dahash_t const HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>(name), namelen);
    xfs_dir2_dataptr_t const DATAPTR = dir2_db_off_to_dataptr(ctx, db, entry_off);
    BufHead* root_bh = nullptr;
    rc = dir2_read_mapped_dir_block(dp, XFS_DIR2_LEAF_OFFSET >> ctx->block_log, &root_bh);
    if (rc != 0) {
        brelse(data_bh);
        return rc;
    }

    BufHead* leaf_bh = nullptr;
    int leaf_idx = -1;
    uint16_t const ROOT_MAGIC = reinterpret_cast<XfsDa3Blkinfo*>(root_bh->data)->hdr.magic.to_cpu();
    if (ROOT_MAGIC == XFS_DA3_NODE_MAGIC) {
        auto* node_hdr = reinterpret_cast<XfsDa3NodeHdr*>(root_bh->data);
        rc = dir2_validate_node_root(ctx, node_hdr);
        if (rc == 0) {
            const auto* node_entries = dir2_node_entries(node_hdr);
            rc = -ENOENT;
            for (size_t i = 0; i < node_hdr->count.to_cpu(); i++) {
                if (HASH > node_entries[i].hashval.to_cpu()) {
                    continue;
                }
                BufHead* candidate = nullptr;
                int const READ_RC = dir2_read_mapped_dir_block(dp, node_entries[i].before.to_cpu(), &candidate);
                if (READ_RC != 0) {
                    rc = READ_RC;
                    break;
                }
                auto* candidate_hdr = reinterpret_cast<XfsDir3LeafHdr*>(candidate->data);
                int const VALID_RC = dir2_validate_leafn(ctx, candidate_hdr);
                if (VALID_RC != 0) {
                    brelse(candidate);
                    rc = VALID_RC;
                    break;
                }
                int const FIND_RC = dir2_leaf_find_dataptr(ctx, candidate_hdr, HASH, DATAPTR, &leaf_idx);
                if (FIND_RC == 0) {
                    leaf_bh = candidate;
                    rc = 0;
                    break;
                }
                brelse(candidate);
                if (node_entries[i].hashval.to_cpu() > HASH) {
                    break;
                }
            }
        }
        brelse(root_bh);
    } else {
        leaf_bh = root_bh;
        rc = dir2_leaf_find_dataptr(ctx, reinterpret_cast<XfsDir3LeafHdr*>(leaf_bh->data), HASH, DATAPTR, &leaf_idx);
    }
    if (rc != 0) {
        if (leaf_bh != nullptr) {
            brelse(leaf_bh);
        }
        brelse(data_bh);
        return rc;
    }
    rc = xfs_trans_capture_buf(tp, data_bh);
    if (rc == 0) {
        rc = xfs_trans_capture_buf(tp, leaf_bh);
    }
    if (rc != 0) {
        brelse(leaf_bh);
        brelse(data_bh);
        return rc;
    }
    auto* leaf_hdr = reinterpret_cast<XfsDir3LeafHdr*>(leaf_bh->data);
    auto* lep = dir2_leaf_entries(leaf_hdr);
    lep[leaf_idx].address = Be32::from_cpu(XFS_DIR2_NULL_DATAPTR);
    uint16_t const STALE_COUNT = leaf_hdr->stale.to_cpu();
    leaf_hdr->stale = Be16::from_cpu(static_cast<uint16_t>(STALE_COUNT + 1));

    uint8_t* data_block = data_bh->data;
    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    size_t const DATA_END = ctx->dir_blk_size;
    dir2_make_data_free(ctx, data_block, DATA_START, DATA_END, entry_off, entry_size);
    dir2_recompute_data_crc(data_block, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, data_bh);

    const auto* updated_data_hdr = reinterpret_cast<const XfsDir3DataHdr*>(data_block);
    if (leaf_hdr->info.hdr.magic.to_cpu() == XFS_DIR3_LEAF_MAGIC) {
        int const BEST_RC = dir2_leaf_set_best(ctx, leaf_hdr, db, updated_data_hdr->best_free.at(0).length.to_cpu());
        if (BEST_RC != 0) {
            brelse(leaf_bh);
            brelse(data_bh);
            return BEST_RC;
        }
    } else {
        BufHead* free_bh = nullptr;
        int const FREE_RC = dir2_read_mapped_dir_block(dp, XFS_DIR2_FREE_OFFSET >> ctx->block_log, &free_bh);
        if (FREE_RC != 0) {
            brelse(leaf_bh);
            brelse(data_bh);
            return FREE_RC;
        }
        auto* free_hdr = reinterpret_cast<XfsDir3FreeHdr*>(free_bh->data);
        int const VALID_RC = dir2_validate_free_block(ctx, free_hdr, dir2_data_block_count(dp));
        if (VALID_RC != 0 || db >= free_hdr->nvalid.to_cpu()) {
            brelse(free_bh);
            brelse(leaf_bh);
            brelse(data_bh);
            return VALID_RC != 0 ? VALID_RC : -EINVAL;
        }
        int const CAPTURE_RC = xfs_trans_capture_buf(tp, free_bh);
        if (CAPTURE_RC != 0) {
            brelse(free_bh);
            brelse(leaf_bh);
            brelse(data_bh);
            return CAPTURE_RC;
        }
        dir2_free_bests(free_hdr)[db] = updated_data_hdr->best_free.at(0).length;
        dir2_recompute_free_crc(free_bh->data, ctx->dir_blk_size);
        xfs_trans_log_buf_full(tp, free_bh);
        brelse(free_bh);
    }
    dir2_recompute_leaf_crc(leaf_bh->data, ctx->dir_blk_size);
    xfs_trans_log_buf_full(tp, leaf_bh);

    brelse(leaf_bh);
    brelse(data_bh);
    return 0;
}

}  // anonymous namespace

auto xfs_dir_addname(XfsInode* dp, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype, XfsTransaction* tp,
                     bool name_known_absent) -> int {
    if (dp == nullptr || name == nullptr || namelen == 0 || tp == nullptr) {
        return -EINVAL;
    }
    if (!xfs_inode_isdir(dp)) {
        return -ENOTDIR;
    }

    int rc = 0;
    if (!name_known_absent && !xfs_dir_name_filter_known_absent(dp, name, namelen)) {
        // Check that the name doesn't already exist.
        XfsDirEntry existing{};
        rc = xfs_dir_lookup(dp, name, namelen, &existing);
        if (rc == 0) {
            return -EEXIST;
        }
        if (rc != -ENOENT) {
            return rc;
        }
    }

    rc = xfs_trans_capture_inode(tp, dp);
    if (rc != 0) {
        return rc;
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
        dp->dir_generation++;
        dir2_leaf_index_note_unknown(dp);
        if (dp->dir_name_filter_complete) {
            dir2_name_filter_add(dp, name, namelen);
        }
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

    int rc = xfs_trans_capture_inode(tp, dp);
    if (rc != 0) {
        return rc;
    }

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
        dp->dir_generation++;
        dir2_leaf_index_note_unknown(dp);
        xfs_dentry_cache_store(dp, name, namelen, -ENOENT, nullptr);
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

    for (auto& set : g_xfs_dentry_cache) {
        uint64_t const IRQF = set.lock.lock_irqsave();
        for (auto& candidate : set.ways) {
            if (candidate.valid && candidate.mount == mount) {
                candidate.valid = false;
            }
        }
        set.lock.unlock_irqrestore(IRQF);
    }
    for (auto& set : g_xfs_dentry_generations) {
        uint64_t const IRQF = set.lock.lock_irqsave();
        for (auto& candidate : set.ways) {
            if (candidate.valid && candidate.mount == mount) {
                candidate.valid = false;
            }
        }
        set.lock.unlock_irqrestore(IRQF);
    }
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

auto xfs_selftest_authoritative_lookup_repairs_stale_negative() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;
    xfs_dentry_cache_purge_mount(&mount);

    std::array<uint8_t, 32> data{};
    XfsInode dir{};
    xfs_selftest_make_shortform_one_entry_dir(&mount, &dir, data, 101, 43);
    xfs_dentry_cache_store(&dir, "foo", 3, -ENOENT, nullptr);

    XfsDirEntry entry{};
    bool const STALE_NEGATIVE_OBSERVED = xfs_dir_lookup(&dir, "foo", 3, &entry) == -ENOENT;
    bool const AUTHORITATIVE_FOUND = xfs_dir_lookup_authoritative(&dir, "foo", 3, &entry) == 0 && entry.ino == 43;
    bool const CACHE_REPAIRED = xfs_dir_lookup(&dir, "foo", 3, &entry) == 0 && entry.ino == 43;
    xfs_dentry_cache_purge_mount(&mount);
    return STALE_NEGATIVE_OBSERVED && AUTHORITATIVE_FOUND && CACHE_REPAIRED;
}

auto xfs_selftest_directory_entry_index_membership() -> bool {
    XfsDirEntry observed{};
    observed.ino = 43;
    observed.ftype = XFS_DIR3_FT_REG_FILE;
    observed.namelen = 3;
    std::memcpy(observed.name.data(), "foo", observed.namelen);

    XfsDirEntry indexed = observed;
    bool const MATCHED = dir_entry_index_membership(&observed, 0, &indexed) == 1;
    bool const UNINDEXED = dir_entry_index_membership(&observed, -ENOENT, &indexed) == 0;

    indexed.ino++;
    bool const MISMATCH_REJECTED = dir_entry_index_membership(&observed, 0, &indexed) == -EIO;
    bool const IO_ERROR_PROPAGATED = dir_entry_index_membership(&observed, -EIO, nullptr) == -EIO;
    return MATCHED && UNINDEXED && MISMATCH_REJECTED && IO_ERROR_PROPAGATED;
}

auto xfs_selftest_block_lookup_uses_leaf_index_for_misses() -> bool {
    XfsMountContext mount{};
    mount.block_size = 4096;
    mount.block_log = 12;
    mount.dir_blk_size = 4096;
    mount.dir_blk_log = 0;
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;

    XfsInode dir{};
    dir.ino = 100;
    dir.mount = &mount;
    dir.mode = 0040755;

    std::array<uint8_t, 4096> block{};
    auto* hdr = reinterpret_cast<XfsDir3DataHdr*>(block.data());
    hdr->hdr.magic = Be32::from_cpu(XFS_DIR3_BLOCK_MAGIC);
    hdr->hdr.owner = Be64::from_cpu(dir.ino);

    constexpr size_t LEAF_COUNT = 1;
    size_t const DATA_START = sizeof(XfsDir3DataHdr);
    size_t const LEAF_BYTES = LEAF_COUNT * sizeof(XfsDir2LeafEntry);
    size_t const DATA_END = mount.dir_blk_size - sizeof(XfsDir2BlockTail) - LEAF_BYTES;

    auto write_entry = [&mount, &block](size_t off, const char* name, uint16_t namelen, xfs_ino_t ino, uint8_t ftype) -> size_t {
        size_t const ENTRY_SIZE = dir2_data_entsize(&mount, static_cast<uint8_t>(namelen));
        auto* dep = reinterpret_cast<XfsDir2DataEntry*>(block.data() + off);
        dep->inumber = Be64::from_cpu(ino);
        dep->namelen = static_cast<uint8_t>(namelen);
        std::memcpy(xfs_dir2_data_entry_name(dep), name, namelen);
        xfs_dir2_data_entry_name(dep)[namelen] = ftype;

        size_t const USED = 8 + 1 + namelen + 1;
        size_t const PAD_START = off + USED;
        size_t const PAD_END = off + ENTRY_SIZE - sizeof(Be16);
        if (PAD_END > PAD_START) {
            std::memset(block.data() + PAD_START, 0, PAD_END - PAD_START);
        }

        auto* tag = reinterpret_cast<Be16*>(block.data() + off + ENTRY_SIZE - sizeof(Be16));
        *tag = Be16::from_cpu(static_cast<uint16_t>(off));
        return ENTRY_SIZE;
    };

    size_t const ALPHA_OFF = DATA_START;
    size_t const ALPHA_SIZE = write_entry(ALPHA_OFF, "alpha", 5, 42, XFS_DIR3_FT_REG_FILE);
    size_t const BETA_OFF = ALPHA_OFF + ALPHA_SIZE;
    size_t const BETA_SIZE = write_entry(BETA_OFF, "beta", 4, 84, XFS_DIR3_FT_REG_FILE);
    size_t const FREE_OFF = BETA_OFF + BETA_SIZE;
    if (FREE_OFF >= DATA_END || (FREE_OFF & (XFS_DIR2_DATA_ALIGN - 1)) != 0) {
        return false;
    }

    auto* unused = reinterpret_cast<XfsDir2DataUnused*>(block.data() + FREE_OFF);
    size_t const FREE_LEN = DATA_END - FREE_OFF;
    unused->freetag = Be16::from_cpu(XFS_DIR2_DATA_FREE_TAG);
    unused->length = Be16::from_cpu(static_cast<uint16_t>(FREE_LEN));
    auto* unused_tag = reinterpret_cast<Be16*>(block.data() + DATA_END - sizeof(Be16));
    *unused_tag = Be16::from_cpu(static_cast<uint16_t>(FREE_OFF));

    auto* blp = reinterpret_cast<XfsDir2LeafEntry*>(block.data() + DATA_END);
    blp[0].hashval = Be32::from_cpu(xfs_da_hashname(reinterpret_cast<const uint8_t*>("alpha"), 5));
    blp[0].address = Be32::from_cpu(dir2_db_off_to_dataptr(&mount, 0, ALPHA_OFF));

    auto* btp = reinterpret_cast<XfsDir2BlockTail*>(block.data() + mount.dir_blk_size - sizeof(XfsDir2BlockTail));
    btp->count = Be32::from_cpu(static_cast<uint32_t>(LEAF_COUNT));
    btp->stale = Be32::from_cpu(0);

    BufHead bh{};
    bh.data = block.data();
    bh.size = block.size();

    XfsDirEntry entry{};
    if (dir2_block_lookup_loaded(&dir, &bh, "alpha", 5, &entry, false) != 0 || entry.ino != 42) {
        return false;
    }

    entry = {};
    if (dir2_block_lookup_loaded(&dir, &bh, "beta", 4, &entry, false) != -ENOENT) {
        return false;
    }

    entry = {};
    return dir2_block_lookup_loaded(&dir, &bh, "gamma", 5, &entry, false) == -ENOENT;
}

auto xfs_selftest_leaf_index_complete_marker() -> bool {
    XfsInode dir{};
    dir.dir_generation = 7;
    bool ok = !dir2_leaf_index_known_complete(&dir);
    dir2_leaf_index_note_complete(&dir);
    ok = ok && dir2_leaf_index_known_complete(&dir);
    dir.dir_generation++;
    ok = ok && !dir2_leaf_index_known_complete(&dir);
    dir2_leaf_index_note_complete(&dir);
    dir2_leaf_index_note_unknown(&dir);
    ok = ok && !dir2_leaf_index_known_complete(&dir);

    std::array<XfsDir2LeafEntry, 2> entries{};
    xfs_dahash_t const ALPHA_HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>("alpha"), 5);
    xfs_dahash_t const BETA_HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>("beta"), 4);
    constexpr xfs_dir2_dataptr_t ALPHA_DATAPTR = 8;
    constexpr xfs_dir2_dataptr_t BETA_DATAPTR = 16;
    if (ALPHA_HASH <= BETA_HASH) {
        entries.at(0).hashval = Be32::from_cpu(ALPHA_HASH);
        entries.at(0).address = Be32::from_cpu(ALPHA_DATAPTR);
        entries.at(1).hashval = Be32::from_cpu(BETA_HASH);
        entries.at(1).address = Be32::from_cpu(BETA_DATAPTR);
    } else {
        entries.at(0).hashval = Be32::from_cpu(BETA_HASH);
        entries.at(0).address = Be32::from_cpu(BETA_DATAPTR);
        entries.at(1).hashval = Be32::from_cpu(ALPHA_HASH);
        entries.at(1).address = Be32::from_cpu(ALPHA_DATAPTR);
    }
    ok = ok && dir2_leaf_entries_contain_dataptr(entries.data(), entries.size(), ALPHA_HASH, ALPHA_DATAPTR);
    ok = ok && dir2_leaf_entries_contain_dataptr(entries.data(), entries.size(), BETA_HASH, BETA_DATAPTR);
    ok = ok && !dir2_leaf_entries_contain_dataptr(entries.data(), entries.size(), ALPHA_HASH, BETA_DATAPTR);
    xfs_dahash_t const GAMMA_HASH = xfs_da_hashname(reinterpret_cast<const uint8_t*>("gamma"), 5);
    ok = ok && !dir2_leaf_entries_contain_dataptr(entries.data(), entries.size(), GAMMA_HASH, ALPHA_DATAPTR);
    return ok;
}

auto xfs_selftest_directory_name_filter() -> bool {
    XfsInode dir{};
    dir.mode = 0040000;
    if (xfs_dir_name_filter_known_absent(&dir, "alpha", 5)) {
        return false;
    }

    xfs_dir_name_filter_init_empty(&dir);
    if (!xfs_dir_name_filter_known_absent(&dir, "alpha", 5) || xfs_dir_name_filter_known_absent(&dir, ".", 1) ||
        xfs_dir_name_filter_known_absent(&dir, "..", 2)) {
        return false;
    }

    dir2_name_filter_add(&dir, "alpha", 5);
    if (xfs_dir_name_filter_known_absent(&dir, "alpha", 5)) {
        return false;
    }

    bool const PROVES_ANOTHER_MISS = xfs_dir_name_filter_known_absent(&dir, "beta", 4) ||
                                     xfs_dir_name_filter_known_absent(&dir, "gamma", 5) ||
                                     xfs_dir_name_filter_known_absent(&dir, "delta", 5);
    dir.dir_name_filter_complete = false;
    return PROVES_ANOTHER_MISS && !xfs_dir_name_filter_known_absent(&dir, "beta", 4);
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

auto xfs_selftest_dentry_cache_remove_keeps_sibling_hot() -> bool {
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

    XfsTransaction add_tp{};
    add_tp.mount = &mount;
    if (xfs_dir_addname(&dir, "bar", 3, 84, XFS_DIR3_FT_REG_FILE, &add_tp) != 0) {
        cleanup();
        return false;
    }
    if (xfs_dir_lookup(&dir, "bar", 3, &entry) != 0 || entry.ino != 84) {
        cleanup();
        return false;
    }

    XfsDentryCacheStats before_remove{};
    xfs_dentry_cache_stats(before_remove);

    XfsTransaction remove_tp{};
    remove_tp.mount = &mount;
    if (xfs_dir_removename(&dir, "bar", 3, &remove_tp) != 0) {
        cleanup();
        return false;
    }

    XfsDentryCacheStats after_remove{};
    xfs_dentry_cache_stats(after_remove);
    if (after_remove.invalidations != before_remove.invalidations || after_remove.stores <= before_remove.stores) {
        cleanup();
        return false;
    }

    if (xfs_dir_lookup(&dir, "foo", 3, &entry) != 0 || entry.ino != 42) {
        cleanup();
        return false;
    }
    XfsDentryCacheStats after_sibling_lookup{};
    xfs_dentry_cache_stats(after_sibling_lookup);
    if (after_sibling_lookup.hits <= after_remove.hits) {
        cleanup();
        return false;
    }

    int const REMOVED_LOOKUP = xfs_dir_lookup(&dir, "bar", 3, &entry);
    XfsDentryCacheStats after_removed_lookup{};
    xfs_dentry_cache_stats(after_removed_lookup);
    bool const OK = REMOVED_LOOKUP == -ENOENT && after_removed_lookup.hits > after_sibling_lookup.hits;
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

auto xfs_selftest_shortform_offsets_match_data_layout() -> bool {
    XfsMountContext mount{};
    mount.inode_size = 512;
    mount.feat_incompat = XFS_SB_FEAT_INCOMPAT_FTYPE;

    std::array<uint8_t, 32> data{};
    XfsInode dir{};
    xfs_selftest_make_shortform_one_entry_dir(&mount, &dir, data, 100, 42);

    auto* heap_data = new uint8_t[dir.data_fork.local.size];
    if (heap_data == nullptr) {
        return false;
    }
    std::memcpy(heap_data, dir.data_fork.local.data, dir.data_fork.local.size);
    dir.data_fork.local.data = heap_data;

    XfsTransaction tp{};
    tp.mount = &mount;
    if (xfs_dir_addname(&dir, "bar", 3, 84, XFS_DIR3_FT_REG_FILE, &tp) != 0) {
        delete[] dir.data_fork.local.data;
        return false;
    }

    const auto* hdr = reinterpret_cast<const XfsDir2SfHdr*>(dir.data_fork.local.data);
    const auto* first = reinterpret_cast<const XfsDir2SfEntry*>(dir.data_fork.local.data + xfs_dir2_sf_hdr_size(hdr));
    const auto* second =
        reinterpret_cast<const XfsDir2SfEntry*>(reinterpret_cast<const uint8_t*>(first) + sf_entry_size(hdr, first, xfs_has_ftype(&mount)));
    size_t const FIRST_OFFSET = dir2_data_first_offset(&mount);
    bool const OK = hdr->count == 2 && sf_entry_stored_offset(first) == FIRST_OFFSET &&
                    sf_entry_stored_offset(second) == FIRST_OFFSET + dir2_data_entsize(&mount, first->namelen);
    delete[] dir.data_fork.local.data;
    return OK;
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

auto xfs_selftest_node_directory_growth_layout() -> bool {
    XfsMountContext mount{};
    mount.block_log = 12;
    mount.dir_blk_log = 0;
    mount.dir_blk_size = 4096;

    constexpr size_t TARGET_RECORDS = 6000;
    constexpr size_t MAX_TEST_LEAVES = 32;
    size_t const LEAF_CAPACITY = dir2_leafn_capacity(&mount);
    size_t const ROOT_CAPACITY = dir2_node_capacity(&mount);
    if (LEAF_CAPACITY != 504 || ROOT_CAPACITY != 504) {
        return false;
    }

    struct TestLeaf {
        std::array<XfsDir2LeafEntry, 505> entries{};
        size_t count{};
    };
    auto* leaves = new (std::nothrow) TestLeaf[MAX_TEST_LEAVES];
    if (leaves == nullptr) {
        return false;
    }

    size_t leaf_count = 2;
    for (size_t i = 0; i < 504; i++) {
        size_t const LEAF = i < 252 ? 0 : 1;
        auto& entry = leaves[LEAF].entries.at(leaves[LEAF].count++);
        entry.hashval = Be32::from_cpu(static_cast<uint32_t>(i));
        entry.address = Be32::from_cpu(static_cast<uint32_t>(i + 1));
    }

    bool ok = true;
    for (size_t value = 504; ok && value < TARGET_RECORDS; value++) {
        TestLeaf& leaf = leaves[leaf_count - 1];
        if (leaf.count == LEAF_CAPACITY) {
            if (leaf_count >= MAX_TEST_LEAVES || leaf_count >= ROOT_CAPACITY) {
                ok = false;
                break;
            }
            TestLeaf& right = leaves[leaf_count++];
            size_t const LEFT_COUNT = leaf.count / 2;
            right.count = leaf.count - LEFT_COUNT;
            __builtin_memcpy(right.entries.data(), leaf.entries.data() + LEFT_COUNT, right.count * sizeof(XfsDir2LeafEntry));
            leaf.count = LEFT_COUNT;
        }
        TestLeaf& target = leaves[leaf_count - 1];
        size_t count = target.count;
        ok = dir2_insert_sorted_leaf_entry(target.entries.data(), &count, target.entries.size(), static_cast<uint32_t>(value),
                                           static_cast<uint32_t>(value + 1)) == 0;
        target.count = count;
    }

    size_t records = 0;
    uint32_t previous = 0;
    for (size_t leaf = 0; ok && leaf < leaf_count; leaf++) {
        if (leaves[leaf].count == 0 || leaves[leaf].count > LEAF_CAPACITY) {
            ok = false;
            break;
        }
        for (size_t i = 0; i < leaves[leaf].count; i++) {
            uint32_t const HASH = leaves[leaf].entries.at(i).hashval.to_cpu();
            if (records != 0 && HASH < previous) {
                ok = false;
                break;
            }
            previous = HASH;
            records++;
        }
    }
    ok = ok && records == TARGET_RECORDS && leaf_count > 2 && leaf_count < ROOT_CAPACITY;
    delete[] leaves;
    return ok;
}

auto xfs_selftest_node_directory_stale_compaction() -> bool {
    XfsMountContext mount{};
    mount.dir_blk_size = 4096;
    std::array<uint8_t, 4096> block{};
    auto* hdr = reinterpret_cast<XfsDir3LeafHdr*>(block.data());
    hdr->info.hdr.magic = Be16::from_cpu(static_cast<uint16_t>(XFS_DIR3_LEAFN_MAGIC));
    hdr->count = Be16::from_cpu(504);
    hdr->stale = Be16::from_cpu(104);
    auto* entries = dir2_leaf_entries(hdr);
    for (size_t i = 0; i < 504; i++) {
        entries[i].hashval = Be32::from_cpu(static_cast<uint32_t>(i));
        entries[i].address = Be32::from_cpu(i % 5 == 0 ? XFS_DIR2_NULL_DATAPTR : static_cast<uint32_t>(i + 1));
    }
    // Four extra stale records make the declared total exactly 104.
    for (size_t i = 1; i <= 4; i++) {
        entries[504 - i].address = Be32::from_cpu(XFS_DIR2_NULL_DATAPTR);
    }

    size_t live = 0;
    if (dir2_validate_leafn(&mount, hdr, &live) != 0 || live != 400) {
        return false;
    }
    std::array<XfsDir2LeafEntry, 505> compacted{};
    if (dir2_copy_live_leaf_entries(hdr, compacted.data(), compacted.size(), &live) != 0 || live != 400) {
        return false;
    }
    if (dir2_insert_sorted_leaf_entry(compacted.data(), &live, compacted.size(), 250, 0xfeed) != 0 || live != 401) {
        return false;
    }

    std::array<XfsDaNodeEntry, 3> root_entries{};
    root_entries[0].hashval = Be32::from_cpu(249);
    root_entries[1].hashval = Be32::from_cpu(503);
    root_entries[2].hashval = Be32::from_cpu(600);
    uint32_t const NEW_MAX = compacted[live - 1].hashval.to_cpu();
    return dir2_node_replacement_order_valid(root_entries.data(), root_entries.size(), 1, NEW_MAX, NEW_MAX) &&
           !dir2_node_replacement_order_valid(root_entries.data(), root_entries.size(), 1, 200, 200);
}

auto xfs_selftest_node_directory_free_layout() -> bool {
    XfsMountContext mount{};
    mount.dir_blk_size = 4096;
    std::array<uint8_t, 4096> block{};
    auto* hdr = reinterpret_cast<XfsDir3FreeHdr*>(block.data());
    hdr->hdr.magic = Be32::from_cpu(XFS_DIR3_FREE_MAGIC);
    hdr->firstdb = Be32::from_cpu(0);
    hdr->nvalid = Be32::from_cpu(3);
    hdr->nused = Be32::from_cpu(3);
    auto* bests = dir2_free_bests(hdr);
    bests[0] = Be16::from_cpu(320);
    bests[1] = Be16::from_cpu(128);
    bests[2] = Be16::from_cpu(64);
    if (dir2_free_capacity(&mount) != 2016 || dir2_validate_free_block(&mount, hdr, 3) != 0) {
        return false;
    }
    dir2_recompute_free_crc(block.data(), block.size());
    uint32_t stored = 0;
    __builtin_memcpy(&stored, &hdr->hdr.crc, sizeof(stored));
    uint32_t const computed = util::crc32c_block_with_cksum(block.data(), block.size(), __builtin_offsetof(XfsDir3BlkHdr, crc));
    return stored != 0 && stored == computed;
}
#endif

}  // namespace ker::vfs::xfs
