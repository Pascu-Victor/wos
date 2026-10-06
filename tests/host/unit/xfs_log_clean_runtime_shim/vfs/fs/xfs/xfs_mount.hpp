#pragma once

#include <cstdint>
#include <vfs/fs/xfs/xfs_format.hpp>

namespace ker::dev {
struct BlockDevice;
}

namespace ker::vfs::xfs {
// Mount geometry is the seam; log state, disk formats and CRC remain production.
struct XfsMountContext {
    dev::BlockDevice* device{};
    uint32_t block_size{};
    uint32_t ag_blocks{};
    uint8_t ag_blk_log{};
    XfsUuidT uuid{};
};
}  // namespace ker::vfs::xfs
