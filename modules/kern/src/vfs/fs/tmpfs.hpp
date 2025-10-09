#pragma once

#include <cstddef>

#include "../vfs.hpp"
#include "bits/ssize_t.h"

namespace ker::vfs::tmpfs {
void register_tmpfs();
auto create_root_file() -> ker::vfs::File*;
auto tmpfs_read(ker::vfs::File* f, void* buf, std::size_t count, std::size_t offset) -> ssize_t;
auto tmpfs_write(ker::vfs::File* f, const void* buf, std::size_t count, std::size_t offset) -> ssize_t;
auto tmpfs_get_size(ker::vfs::File* f) -> std::size_t;
// Lookup or create a node by simple absolute path ("/" or "/name").
auto tmpfs_open_path(const char* path, int flags, int mode) -> ker::vfs::File*;
}  // namespace ker::vfs::tmpfs
