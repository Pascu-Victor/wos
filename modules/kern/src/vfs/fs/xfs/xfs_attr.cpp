// XFS Extended Attribute subsystem — implementation.
//
// Handles shortform (inline in inode attr fork), leaf, and node attribute
// formats for get, list, set, and remove operations.
//
// Reference: reference/xfs/libxfs/xfs_attr.c, xfs_attr_sf.h, xfs_attr_leaf.c

#include "xfs_attr.hpp"

#include <cerrno>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_bmap.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_inode.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

namespace ker::vfs::xfs {

using mod::dbg::log;

// ============================================================================
// ENOATTR — Linux uses ENODATA (61) for missing xattrs
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
    return __builtin_memcmp(entry->nameval, name, namelen) == 0;
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
    size_t total = hdr->totsize.to_cpu();
    size_t pos = sizeof(XfsAttrSfHdr);

    for (uint8_t i = 0; i < hdr->count; i++) {
        if (pos + sizeof(XfsAttrSfEntry) > total) {
            break;
        }
        const auto* entry = reinterpret_cast<const XfsAttrSfEntry*>(base + pos);
        size_t entry_size = xfs_attr_sf_entry_size(entry);
        if (pos + entry_size > total) {
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

        pos += entry_size;
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
    size_t total = hdr->totsize.to_cpu();
    size_t pos = sizeof(XfsAttrSfHdr);

    for (uint8_t i = 0; i < hdr->count; i++) {
        if (pos + sizeof(XfsAttrSfEntry) > total) {
            break;
        }
        const auto* entry = reinterpret_cast<const XfsAttrSfEntry*>(base + pos);
        size_t entry_size = xfs_attr_sf_entry_size(entry);
        if (pos + entry_size > total) {
            break;
        }

        XfsAttrEntry ae{};
        ae.name = entry->nameval;
        ae.namelen = entry->namelen;
        ae.value = xfs_attr_sf_entry_value(entry);
        ae.valuelen = entry->valuelen;
        ae.flags = entry->flags;

        int rc = fn(&ae, priv);
        if (rc != 0) {
            return rc;
        }

        pos += entry_size;
    }

    return 0;
}

// ============================================================================
// Shortform: set (insert or replace)
// ============================================================================

auto sf_set(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, const uint8_t* val, uint32_t valuelen, uint8_t flags)
    -> int {
    (void)tp;

    size_t new_entry_size = sizeof(XfsAttrSfEntry) + namelen + valuelen;

    // If there is no attr fork yet, create one (shortform with 0 entries)
    if (!ip->has_attr_fork || ip->attr_fork.format != XFS_DINODE_FMT_LOCAL || ip->attr_fork.local.data == nullptr) {
        // Allocate initial shortform: header + new entry
        size_t alloc_size = sizeof(XfsAttrSfHdr) + new_entry_size;
        auto* buf = static_cast<uint8_t*>(mod::mm::dyn::kmalloc::malloc(alloc_size));
        if (buf == nullptr) {
            return -ENOMEM;
        }
        __builtin_memset(buf, 0, alloc_size);

        auto* new_hdr = reinterpret_cast<XfsAttrSfHdr*>(buf);
        new_hdr->totsize = __be16::from_cpu(static_cast<uint16_t>(alloc_size));
        new_hdr->count = 1;
        new_hdr->padding = 0;

        auto* entry = reinterpret_cast<XfsAttrSfEntry*>(buf + sizeof(XfsAttrSfHdr));
        entry->namelen = static_cast<uint8_t>(namelen);
        entry->valuelen = static_cast<uint8_t>(valuelen);
        entry->flags = flags;
        __builtin_memcpy(entry->nameval, name, namelen);
        __builtin_memcpy(entry->nameval + namelen, val, valuelen);

        // Install into inode
        if (ip->attr_fork.format == XFS_DINODE_FMT_LOCAL && ip->attr_fork.local.data != nullptr) {
            mod::mm::dyn::kmalloc::free(ip->attr_fork.local.data);
        }
        ip->attr_fork.format = XFS_DINODE_FMT_LOCAL;
        ip->attr_fork.local.data = buf;
        ip->attr_fork.local.size = alloc_size;
        ip->has_attr_fork = true;
        ip->anextents = 0;

        // Set forkoff if not already set — place attr fork after data fork.
        // Default: split inode literal area in half if data fork permits it.
        if (ip->forkoff == 0) {
            size_t inode_core = (ip->mount != nullptr) ? 176 : 176;  // v3 core
            size_t literal_area = ip->mount->inode_size - inode_core;
            // Attr fork starts at forkoff * 8 bytes from data fork start.
            // Set forkoff so attr fork gets the minimum needed space.
            size_t attr_needed = alloc_size;
            if (attr_needed > literal_area) {
                return -ENOSPC;
            }
            // forkoff is in 8-byte units from inode core end (data fork start)
            size_t data_fork_bytes = literal_area - attr_needed;
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

    // Check if the attribute already exists — replace it
    for (uint8_t i = 0; i < hdr->count; i++) {
        if (pos + sizeof(XfsAttrSfEntry) > total) {
            break;
        }
        auto* entry = reinterpret_cast<XfsAttrSfEntry*>(base + pos);
        size_t entry_size = xfs_attr_sf_entry_size(entry);
        if (pos + entry_size > total) {
            break;
        }

        if (name_match(entry, name, namelen, flags)) {
            // Replace in-place if same size
            if (entry->valuelen == valuelen) {
                __builtin_memcpy(entry->nameval + namelen, val, valuelen);
                ip->dirty = true;
                return 0;
            }

            // Different size — remove old entry, then fall through to insert
            size_t tail_start = pos + entry_size;
            size_t tail_len = total - tail_start;
            if (tail_len > 0) {
                __builtin_memmove(base + pos, base + tail_start, tail_len);
            }
            total -= entry_size;
            hdr->count--;
            hdr->totsize = __be16::from_cpu(static_cast<uint16_t>(total));
            break;
        }

        pos += entry_size;
    }

    // Insert new entry at end
    size_t new_total = total + new_entry_size;

    // Check if it fits in the attr fork space
    size_t inode_core = 176;  // v3 core size
    size_t data_fork_bytes = static_cast<size_t>(ip->forkoff) << 3;
    size_t attr_fork_space = ip->mount->inode_size - inode_core - data_fork_bytes;
    if (new_total > attr_fork_space) {
        // TODO: convert to leaf format
        return -ENOSPC;
    }

    // Reallocate the buffer
    auto* new_buf = static_cast<uint8_t*>(mod::mm::dyn::kmalloc::malloc(new_total));
    if (new_buf == nullptr) {
        return -ENOMEM;
    }
    __builtin_memcpy(new_buf, base, total);

    // Append the new entry
    auto* new_entry = reinterpret_cast<XfsAttrSfEntry*>(new_buf + total);
    new_entry->namelen = static_cast<uint8_t>(namelen);
    new_entry->valuelen = static_cast<uint8_t>(valuelen);
    new_entry->flags = flags;
    __builtin_memcpy(new_entry->nameval, name, namelen);
    __builtin_memcpy(new_entry->nameval + namelen, val, valuelen);

    // Update header
    auto* new_hdr = reinterpret_cast<XfsAttrSfHdr*>(new_buf);
    new_hdr->count++;
    new_hdr->totsize = __be16::from_cpu(static_cast<uint16_t>(new_total));

    // Replace buffer in inode
    mod::mm::dyn::kmalloc::free(ip->attr_fork.local.data);
    ip->attr_fork.local.data = new_buf;
    ip->attr_fork.local.size = new_total;
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
        size_t entry_size = xfs_attr_sf_entry_size(entry);
        if (pos + entry_size > total) {
            break;
        }

        if (name_match(entry, name, namelen, flags)) {
            // Found it — remove by shifting tail data
            size_t tail_start = pos + entry_size;
            size_t tail_len = total - tail_start;
            if (tail_len > 0) {
                __builtin_memmove(base + pos, base + tail_start, tail_len);
            }
            total -= entry_size;
            hdr->count--;
            hdr->totsize = __be16::from_cpu(static_cast<uint16_t>(total));
            ip->attr_fork.local.size = total;
            ip->dirty = true;
            return 0;
        }

        pos += entry_size;
    }

    return -ENOATTR;
}

// ============================================================================
// Leaf / Node attribute helpers
// ============================================================================

// Read a single leaf block and iterate its entries.
auto leaf_block_iterate(XfsInode* ip, xfs_fsblock_t blkno, XfsAttrIterFn fn, void* priv) -> int {
    XfsMountContext* mount = ip->mount;
    BufHead* bh = xfs_buf_read(mount, blkno);
    if (bh == nullptr) {
        return -EIO;
    }

    const auto* leaf = reinterpret_cast<const XfsAttr3LeafHdr*>(bh->data);
    uint16_t count = leaf->count.to_cpu();
    const auto* entries = reinterpret_cast<const XfsAttrLeafEntry*>(bh->data + sizeof(XfsAttr3LeafHdr));

    for (uint16_t i = 0; i < count; i++) {
        uint16_t nameidx = entries[i].nameidx.to_cpu();
        uint8_t eflags = entries[i].flags;

        if ((eflags & XFS_ATTR_INCOMPLETE) != 0) {
            continue;  // skip incomplete entries
        }

        if ((eflags & XFS_ATTR_LOCAL) != 0) {
            // Local attribute — name and value are in the leaf block
            const auto* local = reinterpret_cast<const XfsAttrLeafNameLocal*>(bh->data + nameidx);
            XfsAttrEntry ae{};
            ae.name = local->nameval;
            ae.namelen = local->namelen;
            ae.value = local->nameval + local->namelen;
            ae.valuelen = local->valuelen.to_cpu();
            ae.flags = eflags;

            int rc = fn(&ae, priv);
            if (rc != 0) {
                brelse(bh);
                return rc;
            }
        } else {
            // Remote attribute — only return name, value must be fetched separately
            const auto* remote = reinterpret_cast<const XfsAttrLeafNameRemote*>(bh->data + nameidx);
            XfsAttrEntry ae{};
            ae.name = remote->name;
            ae.namelen = remote->namelen;
            ae.value = nullptr;
            ae.valuelen = remote->valuelen.to_cpu();
            ae.flags = eflags;

            int rc = fn(&ae, priv);
            if (rc != 0) {
                brelse(bh);
                return rc;
            }
        }
    }

    brelse(bh);
    return 0;
}

// Look up a single attribute in a leaf block.
auto leaf_block_get(XfsInode* ip, xfs_fsblock_t blkno, const uint8_t* name, uint16_t namelen, uint8_t flags, void* value, uint32_t valuelen)
    -> int {
    XfsMountContext* mount = ip->mount;
    BufHead* bh = xfs_buf_read(mount, blkno);
    if (bh == nullptr) {
        return -EIO;
    }

    const auto* leaf = reinterpret_cast<const XfsAttr3LeafHdr*>(bh->data);
    uint16_t count = leaf->count.to_cpu();
    const auto* entries = reinterpret_cast<const XfsAttrLeafEntry*>(bh->data + sizeof(XfsAttr3LeafHdr));

    for (uint16_t i = 0; i < count; i++) {
        uint16_t nameidx = entries[i].nameidx.to_cpu();
        uint8_t eflags = entries[i].flags;

        if ((eflags & XFS_ATTR_INCOMPLETE) != 0) {
            continue;
        }
        if ((eflags & XFS_ATTR_NSP_ONDISK_MASK) != (flags & XFS_ATTR_NSP_ONDISK_MASK)) {
            continue;
        }

        if ((eflags & XFS_ATTR_LOCAL) != 0) {
            const auto* local = reinterpret_cast<const XfsAttrLeafNameLocal*>(bh->data + nameidx);
            if (local->namelen == namelen && __builtin_memcmp(local->nameval, name, namelen) == 0) {
                uint32_t vlen = local->valuelen.to_cpu();
                if (value == nullptr) {
                    brelse(bh);
                    return static_cast<int>(vlen);
                }
                if (valuelen < vlen) {
                    brelse(bh);
                    return -ERANGE;
                }
                __builtin_memcpy(value, local->nameval + local->namelen, vlen);
                brelse(bh);
                return static_cast<int>(vlen);
            }
        } else {
            const auto* remote = reinterpret_cast<const XfsAttrLeafNameRemote*>(bh->data + nameidx);
            if (remote->namelen == namelen && __builtin_memcmp(remote->name, name, namelen) == 0) {
                // TODO: fetch remote value from remote->valueblk
                brelse(bh);
                return -EOPNOTSUPP;
            }
        }
    }

    brelse(bh);
    return -ENOATTR;
}

// Walk extents-format attr fork to find leaf blocks.
auto extents_iterate(XfsInode* ip, XfsAttrIterFn fn, void* priv) -> int {
    if (ip->attr_fork.extents.count == 0) {
        return 0;
    }
    for (uint32_t i = 0; i < ip->attr_fork.extents.count; i++) {
        const XfsBmbtIrec& ext = ip->attr_fork.extents.list[i];
        for (xfs_extlen_t b = 0; b < ext.br_blockcount; b++) {
            int rc = leaf_block_iterate(ip, ext.br_startblock + b, fn, priv);
            if (rc != 0) {
                return rc;
            }
        }
    }
    return 0;
}

auto extents_get(XfsInode* ip, const uint8_t* name, uint16_t namelen, uint8_t flags, void* value, uint32_t valuelen) -> int {
    if (ip->attr_fork.extents.count == 0) {
        return -ENOATTR;
    }
    for (uint32_t i = 0; i < ip->attr_fork.extents.count; i++) {
        const XfsBmbtIrec& ext = ip->attr_fork.extents.list[i];
        for (xfs_extlen_t b = 0; b < ext.br_blockcount; b++) {
            int rc = leaf_block_get(ip, ext.br_startblock + b, name, namelen, flags, value, valuelen);
            if (rc != -ENOATTR) {
                return rc;
            }
        }
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
            // TODO: implement btree attr lookup
            log("[xfs attr] btree attr lookup not implemented\n");
            return -EOPNOTSUPP;

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
            log("[xfs attr] btree attr iterate not implemented\n");
            return -EOPNOTSUPP;

        default:
            return 0;
    }
}

auto xfs_attr_set(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, const uint8_t* value, uint32_t valuelen,
                  uint8_t flags) -> int {
    if (ip == nullptr || name == nullptr || namelen == 0) {
        return -EINVAL;
    }

    // Currently only shortform set is implemented.
    // If the attr fork is in extents or btree format, we cannot modify it.
    if (ip->has_attr_fork && ip->attr_fork.format != XFS_DINODE_FMT_LOCAL) {
        log("[xfs attr] set on non-shortform attr fork not implemented (format=%d)\n", ip->attr_fork.format);
        return -EOPNOTSUPP;
    }

    return sf_set(ip, tp, name, namelen, value, valuelen, flags);
}

auto xfs_attr_remove(XfsInode* ip, XfsTransaction* tp, const uint8_t* name, uint16_t namelen, uint8_t flags) -> int {
    if (ip == nullptr || name == nullptr || namelen == 0) {
        return -EINVAL;
    }

    if (!ip->has_attr_fork || ip->attr_fork.format != XFS_DINODE_FMT_LOCAL) {
        return -ENOATTR;
    }

    return sf_remove(ip, tp, name, namelen, flags);
}

}  // namespace ker::vfs::xfs
