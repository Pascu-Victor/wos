#pragma once

#include <cstddef>
#include <cstdint>

// Minimal freestanding typedefs for ssize_t and off_t when not provided by
// the environment. Use guards to avoid redefinition.
#if !defined(__ssize_t_defined) && !defined(_SSIZE_T_DEFINED)
typedef long ssize_t;
#define __ssize_t_defined
#define _SSIZE_T_DEFINED
#endif

#if !defined(__off_t_defined) && !defined(_OFF_T_DEFINED)
typedef long off_t;
#define __off_t_defined
#define _OFF_T_DEFINED
#endif

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
int vfs_open(const char* path, int flags, int mode);
int vfs_close(int fd);
ssize_t vfs_read(int fd, void* buf, size_t count);
ssize_t vfs_write(int fd, const void* buf, size_t count);
off_t vfs_lseek(int fd, off_t offset, int whence);

// FD helpers used by Task
int vfs_alloc_fd(ker::mod::sched::task::Task* task, struct File* file);
struct File* vfs_get_file(ker::mod::sched::task::Task* task, int fd);
int vfs_release_fd(ker::mod::sched::task::Task* task, int fd);

// Initialize VFS (register tmpfs, devfs, etc.)
void init();

}  // namespace ker::vfs
