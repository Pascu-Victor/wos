// XFS Inode read / cache implementation.
//
// Reads inodes from disk via the buffer cache, validates CRC, parses the
// xfs_dinode into the in-memory XfsInode struct, and caches them in a hash
// table for reuse.
//
// Reference: reference/xfs/libxfs/xfs_inode_buf.c, reference/xfs/xfs_icache.c

#include "xfs_inode.hpp"

#include <cerrno>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/sys/spinlock.hpp>
#include <util/crc32c.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

namespace ker::vfs::xfs {

// ============================================================================
// Inode cache — simple hash table
// ============================================================================

namespace {

constexpr size_t ICACHE_BUCKETS = 1024;
constexpr size_t ICACHE_HASH_MASK = ICACHE_BUCKETS - 1;

struct IcacheBucket {
    XfsInode* head{};
    mod::sys::Spinlock lock;
};

std::array<IcacheBucket, ICACHE_BUCKETS> icache;
bool icache_inited = false;

auto icache_hash(xfs_ino_t ino) -> size_t {
    // Simple multiplicative hash
    return static_cast<size_t>((ino * 2654435761ULL) & ICACHE_HASH_MASK);
}

// Look up an inode in the cache.  Returns with bucket locked and refcount
// incremented if found, or nullptr with bucket locked if not found.
auto icache_lookup_locked(xfs_ino_t ino, size_t bucket) -> XfsInode* {
    XfsInode* ip = icache[bucket].head;
    while (ip != nullptr) {
        if (ip->ino == ino) {
            ip->refcount++;
            return ip;
        }
        ip = ip->hash_next;
    }
    return nullptr;
}

// Insert an inode into the cache.  Caller must hold bucket lock.
void icache_insert_locked(XfsInode* ip, size_t bucket) {
    ip->hash_next = icache[bucket].head;
    icache[bucket].head = ip;
}

// Remove an inode from the cache.  Caller must hold bucket lock.
void icache_remove_locked(XfsInode* ip, size_t bucket) {
    XfsInode** pp = &icache[bucket].head;
    while (*pp != nullptr) {
        if (*pp == ip) {
            *pp = ip->hash_next;
            ip->hash_next = nullptr;
            return;
        }
        pp = &(*pp)->hash_next;
    }
}

// Free the fork data for an inode
void free_ifork(XfsIfork* fork) {
    switch (fork->format) {
        case XFS_DINODE_FMT_LOCAL:
            if (fork->local.data != nullptr) {
                mod::mm::dyn::kmalloc::free(fork->local.data);
                fork->local.data = nullptr;
            }
            break;
        case XFS_DINODE_FMT_EXTENTS:
            if (fork->extents.list != nullptr) {
                mod::mm::dyn::kmalloc::free(fork->extents.list);
                fork->extents.list = nullptr;
            }
            break;
        case XFS_DINODE_FMT_BTREE:
            if (fork->btree.root != nullptr) {
                mod::mm::dyn::kmalloc::free(fork->btree.root);
                fork->btree.root = nullptr;
            }
            break;
        default:
            break;
    }
}

// Free an XfsInode and all its data
void free_inode(XfsInode* ip) {
    free_ifork(&ip->data_fork);
    if (ip->has_attr_fork) {
        free_ifork(&ip->attr_fork);
    }
    delete ip;
}

// Parse a fork from the on-disk inode.  data_ptr points to the start of the
// fork data within the inode, data_size is the available space.
auto parse_ifork(XfsIfork* fork, uint8_t fmt, const uint8_t* data_ptr, size_t data_size, uint32_t nextents) -> int {
    auto format = static_cast<xfs_dinode_fmt>(fmt);
    fork->format = format;

    switch (format) {
        case XFS_DINODE_FMT_LOCAL: {
            fork->local.size = data_size;
            fork->local.data = static_cast<uint8_t*>(mod::mm::dyn::kmalloc::malloc(data_size));
            if (fork->local.data == nullptr) {
                return -ENOMEM;
            }
            __builtin_memcpy(fork->local.data, data_ptr, data_size);
            break;
        }

        case XFS_DINODE_FMT_EXTENTS: {
            fork->extents.count = nextents;
            if (nextents == 0) {
                fork->extents.list = nullptr;
                break;
            }
            size_t list_size = sizeof(XfsBmbtIrec) * nextents;
            fork->extents.list = static_cast<XfsBmbtIrec*>(mod::mm::dyn::kmalloc::malloc(list_size));
            if (fork->extents.list == nullptr) {
                return -ENOMEM;
            }

            // Decode the on-disk extent records
            const auto* recs = reinterpret_cast<const XfsBmbtRec*>(data_ptr);
            for (uint32_t i = 0; i < nextents; i++) {
                fork->extents.list[i] = xfs_bmbt_rec_unpack(&recs[i]);
            }
            break;
        }

        case XFS_DINODE_FMT_BTREE: {
            // The fork contains a bmdr_block header + keys + pointers
            if (data_size < sizeof(XfsBmdrBlock)) {
                return -EINVAL;
            }
            const auto* bmdr = reinterpret_cast<const XfsBmdrBlock*>(data_ptr);
            fork->btree.level = bmdr->bb_level.to_cpu();
            fork->btree.numrecs = bmdr->bb_numrecs.to_cpu();
            // Copy the entire fork data (header + keys + ptrs) for later traversal
            fork->btree.root_size = data_size;
            fork->btree.root = static_cast<uint8_t*>(mod::mm::dyn::kmalloc::malloc(data_size));
            if (fork->btree.root == nullptr) {
                return -ENOMEM;
            }
            __builtin_memcpy(fork->btree.root, data_ptr, data_size);
            break;
        }

        case XFS_DINODE_FMT_DEV:
            // Device inodes: store the 4-byte dev_t in the local data
            fork->local.size = data_size < 4 ? data_size : 4;
            fork->local.data = static_cast<uint8_t*>(mod::mm::dyn::kmalloc::malloc(4));
            if (fork->local.data == nullptr) {
                return -ENOMEM;
            }
            __builtin_memcpy(fork->local.data, data_ptr, fork->local.size);
            break;

        default:
            mod::dbg::log("[xfs] unknown fork format %d\n", fmt);
            return -EINVAL;
    }

    return 0;
}

}  // anonymous namespace

// ============================================================================
// Public API
// ============================================================================

void xfs_icache_init() {
    if (icache_inited) {
        return;
    }
    for (auto& i : icache) {
        i.head = nullptr;
    }
    icache_inited = true;
}

void xfs_icache_purge(XfsMountContext* mount) {
    for (auto& i : icache) {
        uint64_t flags = i.lock.lock_irqsave();
        XfsInode** pp = &i.head;
        while (*pp != nullptr) {
            XfsInode* ip = *pp;
            if (ip->mount == mount) {
                *pp = ip->hash_next;
                ip->hash_next = nullptr;
                free_inode(ip);
            } else {
                pp = &ip->hash_next;
            }
        }
        i.lock.unlock_irqrestore(flags);
    }
}

auto xfs_inode_read(XfsMountContext* mount, xfs_ino_t ino) -> XfsInode* {
    if (mount == nullptr || ino == NULLFSINO) {
        return nullptr;
    }

    xfs_icache_init();

    size_t bucket = icache_hash(ino);

    // Check cache first
    uint64_t flags = icache[bucket].lock.lock_irqsave();
    XfsInode* ip = icache_lookup_locked(ino, bucket);
    if (ip != nullptr) {
        icache[bucket].lock.unlock_irqrestore(flags);
        return ip;
    }
    icache[bucket].lock.unlock_irqrestore(flags);

    // Not in cache — read from disk
    xfs_fsblock_t block = xfs_inode_block(mount, ino);
    size_t offset = xfs_inode_offset(mount, ino);

    BufHead* bh = xfs_buf_read(mount, block);
    if (bh == nullptr) {
        mod::dbg::log("[xfs] failed to read inode %lu block %lu\n", (unsigned long)ino, (unsigned long)block);
        return nullptr;
    }

    if (offset + mount->inode_size > bh->size) {
        mod::dbg::log("[xfs] inode %lu: offset %lu + size %u exceeds block size %lu\n", (unsigned long)ino, (unsigned long)offset,
                      mount->inode_size, (unsigned long)bh->size);
        brelse(bh);
        return nullptr;
    }

    const auto* dip = reinterpret_cast<const XfsDinode*>(bh->data + offset);

    // Validate magic
    if (dip->di_magic.to_cpu() != XFS_DINODE_MAGIC) {
        mod::dbg::log("[xfs] inode %lu: bad magic 0x%x\n", (unsigned long)ino, dip->di_magic.to_cpu());
        brelse(bh);
        return nullptr;
    }

    // Verify version
    if (dip->di_version != 3) {
        mod::dbg::log("[xfs] inode %lu: unsupported version %u\n", (unsigned long)ino, dip->di_version);
        brelse(bh);
        return nullptr;
    }

    // Verify CRC
    uint32_t computed = util::crc32c_block_with_cksum(dip, mount->inode_size, XFS_DINODE_CRC_OFF);
    if (computed != dip->di_crc) {
        mod::dbg::log("[xfs] inode %lu: CRC mismatch (computed 0x%x, on-disk 0x%x)\n", (unsigned long)ino, computed, dip->di_crc);
        brelse(bh);
        return nullptr;
    }

    // Allocate and parse the in-memory inode
    ip = new XfsInode{};
    ip->ino = ino;
    ip->mount = mount;
    ip->agno = xfs_ino_ag(ino, mount->agino_log);
    ip->agino = xfs_ag_ino(ino, mount->agino_log);

    // Core fields
    ip->mode = dip->di_mode.to_cpu();
    ip->uid = dip->di_uid.to_cpu();
    ip->gid = dip->di_gid.to_cpu();
    ip->nlink = dip->di_nlink.to_cpu();
    ip->size = dip->di_size.to_cpu();
    ip->nblocks = dip->di_nblocks.to_cpu();
    ip->gen = dip->di_gen.to_cpu();
    ip->flags = dip->di_flags.to_cpu();
    ip->flags2 = dip->di_flags2.to_cpu();

    ip->atime = dip->di_atime.to_cpu();
    ip->mtime = dip->di_mtime.to_cpu();
    ip->ctime = dip->di_ctime.to_cpu();
    ip->crtime = dip->di_crtime.to_cpu();

    // NREXT64: extent counts are stored in different fields
    if (xfs_has_nrext64(mount)) {
        // Data extent count in di_pad[0..7] as __be64 (di_big_nextents)
        __be64 big_nextents_be{};
        __builtin_memcpy(&big_nextents_be, dip->di_pad.data(), sizeof(__be64));
        ip->nextents = static_cast<uint32_t>(big_nextents_be.to_cpu());
        // Attr extent count in di_nextents as __be32 (di_big_anextents)
        ip->anextents = dip->di_nextents.to_cpu();
    } else {
        ip->nextents = dip->di_nextents.to_cpu();
        ip->anextents = dip->di_anextents.to_cpu();
    }
    ip->forkoff = dip->di_forkoff;

    // Compute fork sizes
    size_t inode_core_size = xfs_dinode_size(dip->di_version);
    size_t inode_total = mount->inode_size;
    size_t data_fork_start = inode_core_size;
    size_t attr_fork_start = (dip->di_forkoff != 0) ? xfs_dinode_attr_fork_off(dip) : inode_total;
    size_t data_fork_size = attr_fork_start - data_fork_start;
    size_t attr_fork_size = inode_total - attr_fork_start;

    const uint8_t* inode_data = bh->data + offset;

    // Parse data fork
    int rc = parse_ifork(&ip->data_fork, dip->di_format, inode_data + data_fork_start, data_fork_size, ip->nextents);
    if (rc != 0) {
        mod::dbg::log("[xfs] inode %lu: failed to parse data fork (%d)\n", (unsigned long)ino, rc);
        brelse(bh);
        delete ip;
        return nullptr;
    }

    // Parse attribute fork (if present)
    ip->has_attr_fork = (dip->di_forkoff != 0);
    if (ip->has_attr_fork && attr_fork_size > 0) {
        rc =
            parse_ifork(&ip->attr_fork, static_cast<uint8_t>(dip->di_aformat), inode_data + attr_fork_start, attr_fork_size, ip->anextents);
        if (rc != 0) {
            mod::dbg::log("[xfs] inode %lu: failed to parse attr fork (%d)\n", (unsigned long)ino, rc);
            free_ifork(&ip->data_fork);
            brelse(bh);
            delete ip;
            return nullptr;
        }
    } else {
        ip->attr_fork.format = XFS_DINODE_FMT_LOCAL;
        ip->attr_fork.local.data = nullptr;
        ip->attr_fork.local.size = 0;
    }

    brelse(bh);

    // Insert into cache
    ip->refcount = 1;
    ip->hash_next = nullptr;
    ip->dirty = false;

    flags = icache[bucket].lock.lock_irqsave();
    // Check for a race — another thread might have loaded the same inode
    XfsInode* existing = icache_lookup_locked(ino, bucket);
    if (existing != nullptr) {
        // Another thread beat us; free our copy and use theirs
        icache[bucket].lock.unlock_irqrestore(flags);
        free_inode(ip);
        return existing;
    }
    icache_insert_locked(ip, bucket);
    icache[bucket].lock.unlock_irqrestore(flags);

    return ip;
}

void xfs_inode_release(XfsInode* ip) {
    if (ip == nullptr) {
        return;
    }

    size_t bucket = icache_hash(ip->ino);
    uint64_t flags = icache[bucket].lock.lock_irqsave();

    ip->refcount--;
    if (ip->refcount <= 0) {
        // Inode is no longer referenced — remove from cache and free
        icache_remove_locked(ip, bucket);
        icache[bucket].lock.unlock_irqrestore(flags);
        free_inode(ip);
        return;
    }

    icache[bucket].lock.unlock_irqrestore(flags);
}

auto xfs_inode_write(XfsInode* ip, XfsTransaction* tp) -> int {
    if (ip == nullptr || ip->mount == nullptr) {
        return -EINVAL;
    }

    XfsMountContext* mount = ip->mount;
    xfs_fsblock_t block = xfs_inode_block(mount, ip->ino);
    size_t offset = xfs_inode_offset(mount, ip->ino);

    BufHead* bh = xfs_buf_read(mount, block);
    if (bh == nullptr) {
        mod::dbg::log("[xfs] inode write: failed to read block %lu\n", (unsigned long)block);
        return -EIO;
    }

    if (offset + mount->inode_size > bh->size) {
        brelse(bh);
        return -EIO;
    }

    auto* dip = reinterpret_cast<XfsDinode*>(bh->data + offset);

    // Serialize core fields back to on-disk format
    dip->di_mode = __be16::from_cpu(ip->mode);
    dip->di_uid = __be32::from_cpu(ip->uid);
    dip->di_gid = __be32::from_cpu(ip->gid);
    dip->di_nlink = __be32::from_cpu(ip->nlink);
    dip->di_size = __be64::from_cpu(ip->size);
    dip->di_nblocks = __be64::from_cpu(ip->nblocks);
    dip->di_gen = __be32::from_cpu(ip->gen);
    dip->di_flags = __be16::from_cpu(ip->flags);
    dip->di_flags2 = __be64::from_cpu(ip->flags2);

    dip->di_atime = xfs_timestamp_t::from_cpu(ip->atime);
    dip->di_mtime = xfs_timestamp_t::from_cpu(ip->mtime);
    dip->di_ctime = xfs_timestamp_t::from_cpu(ip->ctime);
    dip->di_crtime = xfs_timestamp_t::from_cpu(ip->crtime);

    // NREXT64: extent counts are stored in different fields (mirrors read path)
    if (xfs_has_nrext64(mount)) {
        // Data extent count → di_pad[0..7] as __be64
        __be64 big_nextents_be = __be64::from_cpu(static_cast<uint64_t>(ip->nextents));
        __builtin_memcpy(dip->di_pad.data(), &big_nextents_be, sizeof(__be64));
        // Attr extent count → di_nextents as __be32
        dip->di_nextents = __be32::from_cpu(static_cast<uint32_t>(ip->anextents));
    } else {
        dip->di_nextents = __be32::from_cpu(ip->nextents);
        dip->di_anextents = __be16::from_cpu(ip->anextents);
    }
    dip->di_forkoff = ip->forkoff;
    dip->di_format = static_cast<uint8_t>(ip->data_fork.format);

    // Serialize data fork
    size_t inode_core_size = xfs_dinode_size(dip->di_version);
    uint8_t* fork_data = bh->data + offset + inode_core_size;

    switch (ip->data_fork.format) {
        case XFS_DINODE_FMT_LOCAL:
            if (ip->data_fork.local.data != nullptr && ip->data_fork.local.size > 0) {
                __builtin_memcpy(fork_data, ip->data_fork.local.data, ip->data_fork.local.size);
            }
            break;

        case XFS_DINODE_FMT_EXTENTS: {
            auto* recs = reinterpret_cast<XfsBmbtRec*>(fork_data);
            for (uint32_t i = 0; i < ip->data_fork.extents.count; i++) {
                recs[i] = xfs_bmbt_rec_pack(ip->data_fork.extents.list[i]);
            }
            break;
        }

        case XFS_DINODE_FMT_BTREE:
            // Copy the btree root data back
            if (ip->data_fork.btree.root != nullptr && ip->data_fork.btree.root_size > 0) {
                __builtin_memcpy(fork_data, ip->data_fork.btree.root, ip->data_fork.btree.root_size);
            }
            break;

        default:
            break;
    }

    // Serialize attribute fork (if present)
    if (ip->has_attr_fork && ip->forkoff > 0) {
        dip->di_aformat = static_cast<int8_t>(ip->attr_fork.format);
        size_t attr_fork_offset = static_cast<size_t>(ip->forkoff) << 3;
        uint8_t* attr_data = fork_data + attr_fork_offset;

        switch (ip->attr_fork.format) {
            case XFS_DINODE_FMT_LOCAL:
                if (ip->attr_fork.local.data != nullptr && ip->attr_fork.local.size > 0) {
                    __builtin_memcpy(attr_data, ip->attr_fork.local.data, ip->attr_fork.local.size);
                }
                break;

            case XFS_DINODE_FMT_EXTENTS: {
                auto* recs = reinterpret_cast<XfsBmbtRec*>(attr_data);
                for (uint32_t i = 0; i < ip->attr_fork.extents.count; i++) {
                    recs[i] = xfs_bmbt_rec_pack(ip->attr_fork.extents.list[i]);
                }
                break;
            }

            case XFS_DINODE_FMT_BTREE:
                if (ip->attr_fork.btree.root != nullptr && ip->attr_fork.btree.root_size > 0) {
                    __builtin_memcpy(attr_data, ip->attr_fork.btree.root, ip->attr_fork.btree.root_size);
                }
                break;

            default:
                break;
        }
    }

    // Recompute the CRC over the entire inode
    dip->di_crc = 0;
    uint32_t crc = util::crc32c_block_with_cksum(dip, mount->inode_size, XFS_DINODE_CRC_OFF);
    dip->di_crc = crc;  // di_crc is stored little-endian on disk

    // Log the inode buffer through the transaction
    if (tp != nullptr) {
        xfs_trans_log_buf(tp, bh, static_cast<uint32_t>(offset), static_cast<uint32_t>(mount->inode_size));
    }
    brelse(bh);

    ip->dirty = false;

    return 0;
}

}  // namespace ker::vfs::xfs
