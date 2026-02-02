#pragma once

#include <cstddef>

namespace ker::vfs::initramfs {

// Unpack a CPIO newc archive into the tmpfs root filesystem.
// data: pointer to raw CPIO archive bytes
// size: size of the archive in bytes
// Returns number of entries unpacked, or -1 on error.
auto unpack_initramfs(const void* data, size_t size) -> int;

}  // namespace ker::vfs::initramfs
