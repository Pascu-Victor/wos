#pragma once

#include <cstddef>
#include <cstdint>
#include <mod/io/serial/serial.hpp>
#include <string_view>

#include "bits/off_t.h"
#include "bits/ssize_t.h"

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::vfs {

// VFS logging control - define VFS_DEBUG to enable debug logging
// Helper inline functions for logging (optimizes away when VFS_DEBUG is not defined)
inline void vfs_debug_log(const char* msg) {
#ifdef VFS_DEBUG
    ker::mod::io::serial::write(msg);
#else
    (void)msg;
#endif
}

inline void vfs_debug_log_hex(uint64_t value) {
#ifdef VFS_DEBUG
    ker::mod::io::serial::writeHex(value);
#else
    (void)value;
#endif
}

enum class vfs_node_type : uint8_t { file, directory, device, socket, symlink };

struct File;

struct VNode {
    const char* name;
    vfs_node_type type;
    void* private_data;
};

// Open a path and return a file descriptor-like opaque pointer
auto vfs_open(std::string_view path, int flags, int mode) -> int;
auto vfs_close(int fd) -> int;
auto vfs_read(int fd, void* buf, size_t count, size_t* actual_size = nullptr) -> ssize_t;
auto vfs_write(int fd, const void* buf, size_t count, size_t* actual_size = nullptr) -> ssize_t;
auto vfs_lseek(int fd, off_t offset, int whence) -> off_t;
auto vfs_isatty(int fd) -> bool;
auto vfs_read_dir_entries(int fd, void* buffer, std::size_t max_size) -> ssize_t;

// FD helpers used by Task
auto vfs_alloc_fd(ker::mod::sched::task::Task* task, struct File* file) -> int;
auto vfs_get_file(ker::mod::sched::task::Task* task, int fd) -> struct File*;
auto vfs_release_fd(ker::mod::sched::task::Task* task, int fd) -> int;

// Initialize VFS (register tmpfs, devfs, etc.)
void init();

}  // namespace ker::vfs
