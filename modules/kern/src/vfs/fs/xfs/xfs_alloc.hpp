#pragma once

// XFS Free Space Allocation — allocate and free blocks from AG free space
// B+trees (bnobt/cntbt).
//
// Reference: reference/xfs/libxfs/xfs_alloc.h, reference/xfs/libxfs/xfs_alloc.c

#include <vfs/fs/xfs/xfs_format.hpp>
#include <vfs/fs/xfs/xfs_mount.hpp>
#include <vfs/fs/xfs/xfs_trans.hpp>

namespace ker::vfs::xfs {

// Allocation request — specifies what to allocate
struct XfsAllocReq {
    xfs_agnumber_t agno;     // preferred AG (or NULLAGNUMBER for any)
    xfs_agblock_t agbno;     // preferred starting block (hint)
    xfs_extlen_t minlen;     // minimum length
    xfs_extlen_t maxlen;     // maximum length (requested)
    xfs_extlen_t alignment;  // alignment requirement (0 or 1 = none)
};

// Allocation result
struct XfsAllocResult {
    xfs_agnumber_t agno;  // AG where blocks were allocated
    xfs_agblock_t agbno;  // starting block within AG
    xfs_extlen_t len;     // actual length allocated
};

// Allocate contiguous free blocks from the filesystem.
// Returns 0 on success (result filled), -ENOSPC if no space, or negative errno.
auto xfs_alloc_extent(XfsMountContext* mount, XfsTransaction* tp, const XfsAllocReq& req, XfsAllocResult* result) -> int;

// Free a range of blocks back to the AG free space trees.
// Returns 0 on success.
auto xfs_free_extent(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, xfs_agblock_t agbno, xfs_extlen_t len) -> int;

// Number of block slots in the AGFL bno array for this filesystem.
inline auto xfs_agfl_size(const XfsMountContext* ctx) -> uint32_t {
    return static_cast<uint32_t>((ctx->sect_size - sizeof(XfsAgfl)) / sizeof(uint32_t));
}

// Take one block from the AGFL for use as a btree node.
// Returns 0 and sets *out_bno on success, -ENOSPC if the AGFL is empty.
auto xfs_alloc_get_freelist(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, xfs_agblock_t* out_bno) -> int;

// Return one block to the AGFL (e.g. an emptied btree leaf).
// Falls back to xfs_free_extent if the AGFL is already full.
// Returns 0 on success.
auto xfs_alloc_put_freelist(XfsMountContext* mount, XfsTransaction* tp, xfs_agnumber_t agno, xfs_agblock_t bno) -> int;

}  // namespace ker::vfs::xfs
