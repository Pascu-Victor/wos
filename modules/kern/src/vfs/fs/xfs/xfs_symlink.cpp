// XFS Symlink implementation - read symlink targets.
//
// Short symlinks: target stored inline in inode data fork (LOCAL format).
//   Just copy from fork data.
//
// Long symlinks: target stored in data blocks.  XFS v5 wraps each block
//   with a symlink remote header (xfs_dsymlink_hdr) containing CRC.
//
// Reference: reference/xfs/xfs_symlink.c, reference/xfs/libxfs/xfs_symlink_remote.c

#include "xfs_symlink.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <vfs/buffer_cache.hpp>
#include <vfs/fs/xfs/xfs_bmap.hpp>

#include "net/endian.hpp"
#include "vfs/fs/xfs/xfs_format.hpp"
#include "vfs/fs/xfs/xfs_inode.hpp"
#include "vfs/fs/xfs/xfs_mount.hpp"

namespace ker::vfs::xfs {

// Remote symlink header (v5/CRC)
struct XfsDsymlinkHdr {
    Be32 sl_magic;     // 0x58534C4D ('XSLM')
    Be32 sl_offset;    // offset of this data in the symlink
    Be32 sl_bytes;     // bytes of link data in this block
    Be32 sl_crc;       // CRC32c
    Be64 sl_owner;     // owning inode number
    Be64 sl_blkno;     // disk address
    Be64 sl_lsn;       // log sequence
    XfsUuidT sl_uuid;  // filesystem UUID
} __attribute__((packed));

constexpr uint32_t XFS_SYMLINK_MAGIC = 0x58534C4D;  // 'XSLM'

auto xfs_readlink(XfsInode* ip, char* buf, size_t buflen) -> int {
    if (ip == nullptr || buf == nullptr || buflen == 0) {
        return -EINVAL;
    }
    if (!xfs_inode_islnk(ip)) {
        return -EINVAL;
    }

    uint64_t target_len = ip->size;
    if (target_len == 0) {
        buf[0] = '\0';
        return 0;
    }
    if (target_len >= buflen) {
        target_len = buflen - 1;
    }

    if (ip->data_fork.format == XFS_DINODE_FMT_LOCAL) {
        // Inline symlink - target is in the fork data
        if (ip->data_fork.local.data == nullptr) {
            return -EIO;
        }
        size_t copy_len = target_len;
        copy_len = std::min(copy_len, ip->data_fork.local.size);
        __builtin_memcpy(buf, ip->data_fork.local.data, copy_len);
        buf[copy_len] = '\0';
        return static_cast<int>(copy_len);
    }

    // Remote symlink - target is in data blocks
    XfsMountContext* ctx = ip->mount;
    size_t copied = 0;

    // May span multiple blocks
    xfs_fileoff_t fblock = 0;
    while (copied < target_len) {
        XfsBmapResult bmap{};
        int const RC = xfs_bmap_lookup(ip, fblock, &bmap);
        if (RC != 0 || bmap.is_hole) {
            mod::dbg::log("[xfs symlink] bmap failed or hole at block %lu\n", static_cast<unsigned long>(fblock));
            buf[copied] = '\0';
            return (copied > 0) ? static_cast<int>(copied) : -EIO;
        }

        BufHead* bh = xfs_buf_read(ctx, bmap.startblock);
        if (bh == nullptr) {
            buf[copied] = '\0';
            return (copied > 0) ? static_cast<int>(copied) : -EIO;
        }

        const uint8_t* data = bh->data;
        size_t data_off = 0;
        size_t avail = bh->size;

        // Check for v5 symlink header
        const auto* hdr = reinterpret_cast<const XfsDsymlinkHdr*>(data);
        if (hdr->sl_magic.to_cpu() == XFS_SYMLINK_MAGIC) {
            data_off = sizeof(XfsDsymlinkHdr);
            uint32_t const SL_BYTES = hdr->sl_bytes.to_cpu();
            avail = (SL_BYTES < avail - data_off) ? SL_BYTES : avail - data_off;
        }

        size_t to_copy = target_len - copied;
        to_copy = std::min(to_copy, avail);

        __builtin_memcpy(buf + copied, data + data_off, to_copy);
        copied += to_copy;

        brelse(bh);
        fblock++;
    }

    buf[copied] = '\0';
    return static_cast<int>(copied);
}

}  // namespace ker::vfs::xfs
