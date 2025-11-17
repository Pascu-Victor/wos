#pragma once

#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>

namespace ker::vfs::devfs {

// Register devfs as a virtual filesystem
void register_devfs();

// Get devfs file operations
FileOperations* get_devfs_fops();

// Open a file in devfs (device lookup)
File* devfs_open_path(const char* path, int flags, int mode);

// Initialize devfs
void devfs_init();

}  // namespace ker::vfs::devfs
