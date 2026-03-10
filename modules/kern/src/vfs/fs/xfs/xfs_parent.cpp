// XFS Parent Pointer Operations — implementation.
//
// Parent pointers are extended attributes with the XFS_ATTR_PARENT namespace
// flag.  The xattr "name" is a 12-byte XfsParentRec (big-endian parent inode
// number + generation), and the "value" is the filename under which the child
// appears in the parent directory.
//
// Reference: reference/xfs/libxfs/xfs_parent.c

#include "xfs_parent.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <vfs/fs/xfs/xfs_attr.hpp>
#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_inode.hpp>

namespace ker::vfs::xfs {

// ============================================================================
// xfs_parent_get — retrieve the first parent pointer
// ============================================================================

namespace {

struct ParentGetCtx {
    xfs_ino_t* out_ino;
    uint32_t* out_gen;
    uint8_t* name_buf;
    uint32_t name_buflen;
    int result;  // positive = name length, negative = error
    bool found;
};

int parent_get_cb(const XfsAttrEntry* entry, void* priv) {
    auto* ctx = static_cast<ParentGetCtx*>(priv);

    // Only consider parent-namespace attributes
    if ((entry->flags & XFS_ATTR_PARENT) == 0) {
        return 0;  // continue iterating
    }

    // The "name" is really a 12-byte XfsParentRec
    if (entry->namelen != sizeof(XfsParentRec)) {
        return 0;  // skip malformed entries
    }

    const auto* prec = reinterpret_cast<const XfsParentRec*>(entry->name);
    *ctx->out_ino = prec->p_ino.to_cpu();
    *ctx->out_gen = prec->p_gen.to_cpu();

    // Copy the directory entry name (the value)
    uint32_t copylen = entry->valuelen;
    copylen = std::min(copylen, ctx->name_buflen);
    if (ctx->name_buf != nullptr && entry->value != nullptr && copylen > 0) {
        __builtin_memcpy(ctx->name_buf, entry->value, copylen);
    }

    ctx->result = static_cast<int>(entry->valuelen);
    ctx->found = true;
    return 1;  // stop iterating — we only want the first one
}

}  // anonymous namespace

auto xfs_parent_get(XfsInode* ip, xfs_ino_t* parent_ino, uint32_t* parent_gen, uint8_t* name_buf, uint32_t name_buflen) -> int {
    if (ip == nullptr || parent_ino == nullptr || parent_gen == nullptr) {
        return -EINVAL;
    }

    ParentGetCtx ctx{};
    ctx.out_ino = parent_ino;
    ctx.out_gen = parent_gen;
    ctx.name_buf = name_buf;
    ctx.name_buflen = name_buflen;
    ctx.result = 0;
    ctx.found = false;

    int rc = xfs_attr_list(ip, parent_get_cb, &ctx);
    // xfs_attr_list returns the callback's non-zero return (1) on early stop
    if (ctx.found) {
        return ctx.result;
    }
    if (rc < 0) {
        return rc;
    }
    return -61;  // ENOATTR / ENODATA
}

// ============================================================================
// xfs_parent_add — add a parent pointer
// ============================================================================

auto xfs_parent_add(XfsInode* child, XfsTransaction* tp, xfs_ino_t parent_ino, uint32_t parent_gen, const uint8_t* name, uint16_t namelen)
    -> int {
    if (child == nullptr || name == nullptr || namelen == 0) {
        return -EINVAL;
    }

    XfsParentRec prec{};
    prec.p_ino = __be64::from_cpu(parent_ino);
    prec.p_gen = __be32::from_cpu(parent_gen);

    return xfs_attr_set(child, tp, reinterpret_cast<const uint8_t*>(&prec), sizeof(XfsParentRec), name, namelen, XFS_ATTR_PARENT);
}

// ============================================================================
// xfs_parent_remove — remove a parent pointer
// ============================================================================

auto xfs_parent_remove(XfsInode* child, XfsTransaction* tp, xfs_ino_t parent_ino, uint32_t parent_gen, const uint8_t* name,
                       uint16_t namelen) -> int {
    (void)name;
    (void)namelen;

    if (child == nullptr) {
        return -EINVAL;
    }

    XfsParentRec prec{};
    prec.p_ino = __be64::from_cpu(parent_ino);
    prec.p_gen = __be32::from_cpu(parent_gen);

    return xfs_attr_remove(child, tp, reinterpret_cast<const uint8_t*>(&prec), sizeof(XfsParentRec), XFS_ATTR_PARENT);
}

// ============================================================================
// xfs_parent_replace — rename: remove old pointer, add new
// ============================================================================

auto xfs_parent_replace(XfsInode* child, XfsTransaction* tp, xfs_ino_t old_pino, uint32_t old_pgen, const uint8_t* old_name,
                        uint16_t old_namelen, xfs_ino_t new_pino, uint32_t new_pgen, const uint8_t* new_name, uint16_t new_namelen) -> int {
    int rc = xfs_parent_remove(child, tp, old_pino, old_pgen, old_name, old_namelen);
    if (rc < 0 && rc != -61 /* ENOATTR */) {
        return rc;
    }

    return xfs_parent_add(child, tp, new_pino, new_pgen, new_name, new_namelen);
}

}  // namespace ker::vfs::xfs
