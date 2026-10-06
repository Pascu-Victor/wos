#pragma once
#include <dev/block_device.hpp>
namespace ker::vfs::devfs {
inline auto devfs_resolve_block_device(const char*) -> ker::dev::BlockDevice* { return nullptr; }
}  // namespace ker::vfs::devfs
