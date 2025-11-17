#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "bits/off_t.h"
#include "bits/ssize_t.h"

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::vfs {

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
auto vfs_read(int fd, void* buf, std::size_t count) -> ssize_t;
auto vfs_write(int fd, const void* buf, std::size_t count) -> ssize_t;
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
