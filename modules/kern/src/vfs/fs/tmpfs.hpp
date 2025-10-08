#pragma once

#include "../vfs.hpp"

namespace ker::vfs::tmpfs {
void register_tmpfs();
ker::vfs::File* create_root_file();
ssize_t tmpfs_read(ker::vfs::File* f, void* buf, size_t count, size_t offset);
ssize_t tmpfs_write(ker::vfs::File* f, const void* buf, size_t count, size_t offset);
size_t tmpfs_get_size(ker::vfs::File* f);
// Lookup or create a node by simple absolute path ("/" or "/name").
ker::vfs::File* tmpfs_open_path(const char* path, int flags, int mode);
}  // namespace ker::vfs::tmpfs
