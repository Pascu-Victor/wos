#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <dev/block_device.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <string_view>
#include <vfs/fs/devfs.hpp>
#include <vfs/fs/fat32.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/mount.hpp>
#include <vfs/stat.hpp>

#include "file.hpp"
#include "fs/devfs.hpp"
#include "fs/fat32.hpp"
#include "fs/tmpfs.hpp"
#include "platform/mm/dyn/kmalloc.hpp"
#include "vfs.hpp"

namespace ker::vfs {

namespace {
constexpr size_t MAX_PATH_LEN = 512;
constexpr int MAX_SYMLINK_DEPTH = 8;
constexpr size_t MAX_COMPONENTS = 64;

// Canonicalize a path in place: resolve ".", "..", and collapse "//".
// The path must be absolute (start with "/").
// Returns 0 on success, -ENAMETOOLONG if the path is too long.
auto canonicalize_path(char* path, size_t bufsize) -> int {
    if (path == nullptr || bufsize == 0 || path[0] != '/') {
        return -1;
    }

    // Split into components, resolving . and ..
    const char* components[MAX_COMPONENTS];  // NOLINT
    size_t num_components = 0;

    char* p = path + 1;  // skip leading /
    while (*p != '\0') {
        // Skip slashes
        while (*p == '/') {
            p++;
        }
        if (*p == '\0') {
            break;
        }

        // Find end of component
        char* comp_start = p;
        while (*p != '\0' && *p != '/') {
            p++;
        }

        // Null-terminate this component in the buffer
        char saved = *p;
        *p = '\0';

        if (comp_start[0] == '.' && comp_start[1] == '\0') {
            // "." - skip
        } else if (comp_start[0] == '.' && comp_start[1] == '.' && comp_start[2] == '\0') {
            // ".." - pop last component
            if (num_components > 0) {
                num_components--;
            }
        } else {
            if (num_components >= MAX_COMPONENTS) {
                return -ENAMETOOLONG;
            }
            components[num_components++] = comp_start;
        }

        // Keep the null terminator in place — the component pointers
        // stored in components[] depend on it for correct strlen/memcpy
        // during reconstruction.  Parsing still works because we advance
        // p past the '\0' below.
        if (saved == '/') {
            p++;
        }
    }

    // Reconstruct canonical path
    char result[MAX_PATH_LEN];  // NOLINT
    size_t pos = 0;
    result[pos++] = '/';

    for (size_t i = 0; i < num_components; ++i) {
        if (i > 0) {
            if (pos >= MAX_PATH_LEN - 1) {
                return -ENAMETOOLONG;
            }
            result[pos++] = '/';
        }
        size_t comp_len = std::strlen(components[i]);
        if (pos + comp_len >= MAX_PATH_LEN) {
            return -ENAMETOOLONG;
        }
        std::memcpy(static_cast<char*>(result) + pos, components[i], comp_len);
        pos += comp_len;
    }
    result[pos] = '\0';

    if (pos >= bufsize) {
        return -ENAMETOOLONG;
    }
    std::memcpy(path, static_cast<const char*>(result), pos + 1);
    return 0;
}

// Resolve symlinks in a path. The resolved path is written to resolved_buf.
// Returns 0 on success, -ELOOP on too many symlinks, -1 on other errors.
auto resolve_symlinks(const char* path, char* resolved_buf, size_t bufsize) -> int {
    if (path == nullptr || resolved_buf == nullptr || bufsize == 0) {
        return -1;
    }

    // Copy initial path to working buffer
    size_t path_len = 0;
    while (path[path_len] != '\0' && path_len < bufsize - 1) {
        resolved_buf[path_len] = path[path_len];
        path_len++;
    }
    resolved_buf[path_len] = '\0';

    for (int depth = 0; depth < MAX_SYMLINK_DEPTH; ++depth) {
        MountPoint* mount = find_mount_point(resolved_buf);
        if (mount == nullptr) {
            return 0;
        }

        // Only tmpfs supports symlinks currently
        if (mount->fs_type != FSType::TMPFS) {
            return 0;
        }

        // Strip mount prefix to get fs-relative path
        size_t mount_len = 0;
        while (mount->path[mount_len] != '\0') {
            mount_len++;
        }

        const char* fs_path = resolved_buf;
        if (mount_len == 1 && mount->path[0] == '/') {
            fs_path = resolved_buf + 1;
        } else if (resolved_buf[mount_len] == '/') {
            fs_path = resolved_buf + mount_len + 1;
        } else if (resolved_buf[mount_len] == '\0') {
            fs_path = "";
        } else {
            fs_path = resolved_buf + mount_len;
        }

        // Walk the tmpfs path to find the node
        auto* node = ker::vfs::tmpfs::tmpfs_walk_path(fs_path, false);
        if (node == nullptr) {
            return 0;  // Path doesn't exist yet (might be created with O_CREAT)
        }

        if (node->type != ker::vfs::tmpfs::TmpNodeType::SYMLINK) {
            return 0;  // Not a symlink, resolution complete
        }

        if (node->symlink_target == nullptr) {
            return -1;
        }

        // Build the new path
        char new_path[MAX_PATH_LEN];  // NOLINT
        size_t target_len = 0;
        while (node->symlink_target[target_len] != '\0') {
            target_len++;
        }

        if (node->symlink_target[0] == '/') {
            // Absolute symlink target - replace entire path
            if (target_len >= bufsize) {
                return -1;
            }
            memcpy(resolved_buf, node->symlink_target, target_len + 1);
        } else {
            // Relative symlink target - replace last component
            size_t last_slash = 0;
            bool found_slash = false;
            for (size_t i = 0; resolved_buf[i] != '\0'; ++i) {
                if (resolved_buf[i] == '/') {
                    last_slash = i;
                    found_slash = true;
                }
            }

            size_t prefix_len = found_slash ? last_slash + 1 : 0;
            if (prefix_len + target_len >= bufsize) {
                return -1;
            }
            memcpy(new_path, resolved_buf, prefix_len);                       // NOLINT
            memcpy(new_path + prefix_len, node->symlink_target, target_len);  // NOLINT
            new_path[prefix_len + target_len] = '\0';                         // NOLINT
            memcpy(resolved_buf, new_path, prefix_len + target_len + 1);
        }
    }

    return -ELOOP;
}
}  // namespace

auto vfs_open(std::string_view path, int flags, int mode) -> int {
    vfs_debug_log("vfs_open: opening file\n");

    // Convert string_view to null-terminated string
    char pathBuffer[MAX_PATH_LEN];  // NOLINT
    if (path.size() >= MAX_PATH_LEN) {
        return -ENAMETOOLONG;
    }
    std::memcpy(pathBuffer, path.data(), path.size());
    pathBuffer[path.size()] = '\0';

    // Canonicalize path (resolve ".", "..", collapse "//")
    canonicalize_path(static_cast<char*>(pathBuffer), MAX_PATH_LEN);

    // Resolve symlinks in the path
    char resolved[MAX_PATH_LEN];  // NOLINT
    int resolve_ret = resolve_symlinks(pathBuffer, resolved, MAX_PATH_LEN);
    if (resolve_ret == -ELOOP) {
        ker::mod::io::serial::write("vfs_open: too many symlink levels\n");
        return -1;
    }
    if (resolve_ret == 0) {
        // Use the resolved path
        std::memcpy(pathBuffer, resolved, MAX_PATH_LEN);
    }

    auto* current = ker::mod::sched::getCurrentTask();
    if (current == nullptr) {
        vfs_debug_log("vfs_open: no current task\n");
        return -1;
    }

    // Find the mount point for this path
    MountPoint* mount = find_mount_point(pathBuffer);
    if (mount == nullptr) {
        vfs_debug_log("vfs_open: no mount point found for path\n");
        ker::mod::io::serial::write("vfs_open: no mount point found for path: ");
        ker::mod::io::serial::write(pathBuffer);
        ker::mod::io::serial::write("\n");
        return -1;
    }

    // Strip the mount point prefix to get the filesystem-relative path
    const char* fs_relative_path = pathBuffer;
    size_t mount_len = 0;
    while (mount->path[mount_len] != '\0') {
        mount_len++;
    }

    // Skip past the mount point prefix
    if (mount_len > 0 && pathBuffer[mount_len - 1] == '/' && mount_len == 1) {
        // Root mount "/" - skip the leading /
        fs_relative_path = pathBuffer + 1;
    } else if (pathBuffer[mount_len] == '/') {
        // Path continues after mount point with /
        fs_relative_path = pathBuffer + mount_len + 1;
    } else if (pathBuffer[mount_len] == '\0') {
        // Path exactly matches mount point - opening mount root
        fs_relative_path = "";
    } else {
        // Path doesn't match mount point properly (shouldn't happen)
        fs_relative_path = pathBuffer + mount_len;
    }

    ker::vfs::File* f = nullptr;

    // Route to the appropriate filesystem driver based on mount point
    switch (mount->fs_type) {
        case FSType::DEVFS:
            f = ker::vfs::devfs::devfs_open_path(fs_relative_path, flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::devfs::get_devfs_fops();
                f->fs_type = FSType::DEVFS;
            }
            break;
        case FSType::FAT32:
            f = ker::vfs::fat32::fat32_open_path(fs_relative_path, flags, mode,
                                                 static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
            if (f != nullptr) {
                f->fops = ker::vfs::fat32::get_fat32_fops();
                f->fs_type = FSType::FAT32;
            } else {
                ker::mod::io::serial::write("vfs_open: fat32_open_path failed for '");
                ker::mod::io::serial::write(fs_relative_path);
                ker::mod::io::serial::write("' (mount='");
                ker::mod::io::serial::write(mount->path);
                ker::mod::io::serial::write("', original path='");
                ker::mod::io::serial::write(pathBuffer);
                ker::mod::io::serial::write("')\n");
            }
            break;
        case FSType::TMPFS:
            f = ker::vfs::tmpfs::tmpfs_open_path(fs_relative_path, flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::tmpfs::get_tmpfs_fops();
                f->fs_type = FSType::TMPFS;
            }
            break;
        default:
            vfs_debug_log("vfs_open: unknown filesystem type\n");
            return -1;
    }

    if (f == nullptr) {
        vfs_debug_log("vfs_open: failed to open file\n");
        return -1;
    }

    int fd = vfs_alloc_fd(current, f);
    if (fd < 0) {
        return -1;
    }
    return fd;
}

auto vfs_close(int fd) -> int {
    // Release FD from current task
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (t == nullptr) {
        return -1;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -1;
    }

    // Decrement reference count
    f->refcount--;

    // Release the FD from the task's file descriptor table
    vfs_release_fd(t, fd);

    // Only call close and free if no more references
    if (f->refcount <= 0) {
        if ((f->fops != nullptr) && (f->fops->vfs_close != nullptr)) {
            f->fops->vfs_close(f);
        }
        // Free the File descriptor object (just the handle/wrapper)
        // but keep the underlying fs node (f->private_data) intact
        // so the file can be reopened later
        ker::mod::mm::dyn::kmalloc::free((void*)f);
    }

    return 0;
}

auto vfs_read(int fd, void* buf, size_t count, size_t* actual_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (t == nullptr) {
        return -1;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -1;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_read == nullptr)) {
        return -1;
    }
    ssize_t r = f->fops->vfs_read(f, buf, count, (size_t)f->pos);
    if (r >= 0) {
        f->pos += r;
        if (actual_size != nullptr) {
            *actual_size = static_cast<size_t>(r);
        }
        return r;
    }
    return r;
}

auto vfs_write(int fd, const void* buf, size_t count, size_t* actual_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (t == nullptr) {
        return -1;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -1;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_write == nullptr)) {
        return -1;
    }
    ssize_t r = f->fops->vfs_write(f, buf, count, (size_t)f->pos);
    if (r >= 0) {
        f->pos += r;
        if (actual_size != nullptr) {
            *actual_size = static_cast<size_t>(r);
        }
        return r;
    }
    return r;
}

auto vfs_lseek(int fd, off_t offset, int whence) -> off_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (t == nullptr) {
        return -1;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -1;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_lseek == nullptr)) {
        return -1;
    }
    return f->fops->vfs_lseek(f, offset, whence);
}

auto vfs_alloc_fd(ker::mod::sched::task::Task* task, struct File* file) -> int {
    if ((task == nullptr) || (file == nullptr)) {
        return -1;
    }
    for (unsigned i = 0; i < ker::mod::sched::task::Task::FD_TABLE_SIZE; ++i) {
        if (task->fds[i] == nullptr) {
            task->fds[i] = file;
            file->fd = (int)i;
            return (int)i;
        }
    }
    return -1;  // EMFILE (too many open files)
}

auto vfs_get_file(ker::mod::sched::task::Task* task, int fd) -> struct File* {
    if (task == nullptr) {
        return nullptr;
    }
    if (fd < 0 || (unsigned)fd >= ker::mod::sched::task::Task::FD_TABLE_SIZE) {
        return nullptr;
    }
    return reinterpret_cast<struct File*>(task->fds[fd]);
}

auto vfs_release_fd(ker::mod::sched::task::Task* task, int fd) -> int {
    if (task == nullptr) {
        return -1;
    }
    if (fd < 0 || (unsigned)fd >= ker::mod::sched::task::Task::FD_TABLE_SIZE) {
        return -1;
    }
    task->fds[fd] = nullptr;
    return 0;
}

auto vfs_isatty(int fd) -> bool {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (t == nullptr) {
        return false;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return false;
    }
    if ((f->fops == nullptr) || (f->fops->vfs_isatty == nullptr)) {
        return false;
    }
    return f->fops->vfs_isatty(f);
}

auto vfs_read_dir_entries(int fd, void* buffer, size_t max_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::getCurrentTask();
    if (t == nullptr) {
        return -1;
    }
    ker::vfs::File* f = vfs_get_file(t, fd);
    if (f == nullptr) {
        return -1;
    }

    // Check if this is a directory
    if (!f->is_directory) {
        return -ENOTDIR;
    }

    if ((f->fops == nullptr) || (f->fops->vfs_readdir == nullptr)) {
        return -ENOSYS;
    }

    // Buffer must be large enough for at least one DirEntry
    if (buffer == nullptr || max_size < sizeof(DirEntry)) {
        return -EINVAL;
    }

    auto* entries = static_cast<DirEntry*>(buffer);
    size_t max_entries = max_size / sizeof(DirEntry);
    size_t entries_read = 0;

    // Read directory entries using the current position as index
    size_t start_index = static_cast<size_t>(f->pos);

    for (size_t i = 0; i < max_entries; ++i) {
        int ret = f->fops->vfs_readdir(f, &entries[i], start_index + i);
        if (ret != 0) {
            // End of directory or error
            break;
        }
        entries_read++;
    }

    // Update file position
    f->pos += static_cast<off_t>(entries_read);

    return static_cast<ssize_t>(entries_read * sizeof(DirEntry));
}

// --- Symlink / mkdir / mount operations ---

auto vfs_symlink(const char* target, const char* linkpath) -> int {
    if (target == nullptr || linkpath == nullptr) {
        return -EINVAL;
    }

    // Find mount point for the linkpath
    MountPoint* mount = find_mount_point(linkpath);
    if (mount == nullptr) {
        return -ENOENT;
    }

    // Only tmpfs supports symlinks
    if (mount->fs_type != FSType::TMPFS) {
        return -ENOSYS;
    }

    // Strip mount prefix
    size_t mount_len = 0;
    while (mount->path[mount_len] != '\0') {
        mount_len++;
    }

    const char* fs_path = linkpath;
    if (mount_len == 1 && mount->path[0] == '/') {
        fs_path = linkpath + 1;
    } else if (linkpath[mount_len] == '/') {
        fs_path = linkpath + mount_len + 1;
    } else {
        fs_path = linkpath + mount_len;
    }

    // Split into parent path and link name
    const char* last_slash = nullptr;
    for (const char* p = fs_path; *p != '\0'; ++p) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    ker::vfs::tmpfs::TmpNode* parent = nullptr;
    const char* link_name = nullptr;

    if (last_slash == nullptr) {
        parent = ker::vfs::tmpfs::get_root_node();
        link_name = fs_path;
    } else {
        std::array<char, MAX_PATH_LEN> parent_path{};
        auto parent_len = static_cast<size_t>(last_slash - fs_path);
        if (parent_len >= MAX_PATH_LEN) {
            return -ENAMETOOLONG;
        }
        memcpy(parent_path.data(), fs_path, parent_len);
        parent_path[parent_len] = '\0';
        parent = ker::vfs::tmpfs::tmpfs_walk_path(parent_path.data(), true);
        link_name = last_slash + 1;
    }

    if (parent == nullptr || link_name == nullptr || *link_name == '\0') {
        return -ENOENT;
    }

    auto* node = ker::vfs::tmpfs::tmpfs_create_symlink(parent, link_name, target);
    return (node != nullptr) ? 0 : -1;
}

auto vfs_readlink(const char* path, char* buf, size_t bufsize) -> ssize_t {
    if (path == nullptr || buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }

    MountPoint* mount = find_mount_point(path);
    if (mount == nullptr) {
        return -ENOENT;
    }

    if (mount->fs_type != FSType::TMPFS) {
        return -ENOSYS;
    }

    // Strip mount prefix
    size_t mount_len = 0;
    while (mount->path[mount_len] != '\0') {
        mount_len++;
    }

    const char* fs_path = path;
    if (mount_len == 1 && mount->path[0] == '/') {
        fs_path = path + 1;
    } else if (path[mount_len] == '/') {
        fs_path = path + mount_len + 1;
    } else {
        fs_path = path + mount_len;
    }

    auto* node = ker::vfs::tmpfs::tmpfs_walk_path(fs_path, false);
    if (node == nullptr) {
        return -ENOENT;
    }

    if (node->type != ker::vfs::tmpfs::TmpNodeType::SYMLINK || node->symlink_target == nullptr) {
        return -EINVAL;
    }

    size_t len = 0;
    while (node->symlink_target[len] != '\0') {
        len++;
    }
    size_t to_copy = (len < bufsize) ? len : bufsize;
    memcpy(buf, node->symlink_target, to_copy);
    return static_cast<ssize_t>(to_copy);
}

auto vfs_mkdir(const char* path, int mode) -> int {
    (void)mode;
    if (path == nullptr) {
        return -EINVAL;
    }

    MountPoint* mount = find_mount_point(path);
    if (mount == nullptr) {
        return -ENOENT;
    }

    if (mount->fs_type != FSType::TMPFS) {
        return -ENOSYS;  // Only tmpfs supports mkdir for now
    }

    // Strip mount prefix
    size_t mount_len = 0;
    while (mount->path[mount_len] != '\0') {
        mount_len++;
    }

    const char* fs_path = path;
    if (mount_len == 1 && mount->path[0] == '/') {
        fs_path = path + 1;
    } else if (path[mount_len] == '/') {
        fs_path = path + mount_len + 1;
    } else {
        fs_path = path + mount_len;
    }

    auto* node = ker::vfs::tmpfs::tmpfs_walk_path(fs_path, true);
    return (node != nullptr) ? 0 : -1;
}

auto vfs_stat(const char* path, stat* statbuf) -> int {
    if (path == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }

    // Canonicalize path
    char pathBuffer[MAX_PATH_LEN];  // NOLINT
    size_t path_len = 0;
    while (path[path_len] != '\0' && path_len < MAX_PATH_LEN - 1) {
        pathBuffer[path_len] = path[path_len];
        path_len++;
    }
    pathBuffer[path_len] = '\0';

    canonicalize_path(pathBuffer, MAX_PATH_LEN);

    // Find mount point
    MountPoint* mount = find_mount_point(pathBuffer);
    if (mount == nullptr) {
        return -ENOENT;
    }

    // Strip mount prefix
    size_t mount_len = 0;
    while (mount->path[mount_len] != '\0') {
        mount_len++;
    }

    const char* fs_path = pathBuffer;
    if (mount_len == 1 && mount->path[0] == '/') {
        fs_path = pathBuffer + 1;
    } else if (pathBuffer[mount_len] == '/') {
        fs_path = pathBuffer + mount_len + 1;
    } else if (pathBuffer[mount_len] == '\0') {
        fs_path = "";
    } else {
        fs_path = pathBuffer + mount_len;
    }

    // Initialize stat buffer
    memset(statbuf, 0, sizeof(stat));

    switch (mount->fs_type) {
        case FSType::TMPFS: {
            auto* node = ker::vfs::tmpfs::tmpfs_walk_path(fs_path, false);
            if (node == nullptr) {
                return -ENOENT;
            }
            statbuf->st_dev = 0;
            statbuf->st_ino = reinterpret_cast<ino_t>(node);  // Use node address as inode
            statbuf->st_nlink = 1;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = static_cast<off_t>(node->size);
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = (node->size + 511) / 512;
            switch (node->type) {
                case ker::vfs::tmpfs::TmpNodeType::FILE:
                    statbuf->st_mode = S_IFREG | 0644;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::DIRECTORY:
                    statbuf->st_mode = S_IFDIR | 0755;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::SYMLINK:
                    statbuf->st_mode = S_IFLNK | 0777;
                    break;
            }
            return 0;
        }
        case FSType::FAT32: {
            return ker::vfs::fat32::fat32_stat(fs_path, statbuf, static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
        }
        case FSType::DEVFS: {
            // Device files - report as character devices
            statbuf->st_dev = 0;
            statbuf->st_ino = 1;
            statbuf->st_nlink = 1;
            statbuf->st_mode = S_IFCHR | 0666;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;
            return 0;
        }
        default:
            return -ENOSYS;
    }
}

auto vfs_fstat(int fd, stat* statbuf) -> int {
    if (statbuf == nullptr) {
        return -EINVAL;
    }

    auto* task = ker::mod::sched::getCurrentTask();
    if (task == nullptr) {
        return -ESRCH;
    }

    auto* file = vfs_get_file(task, fd);
    if (file == nullptr) {
        return -EBADF;
    }

    // Initialize stat buffer
    memset(statbuf, 0, sizeof(stat));

    switch (file->fs_type) {
        case FSType::TMPFS: {
            auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(file->private_data);
            if (node == nullptr) {
                return -EBADF;
            }
            statbuf->st_dev = 0;
            statbuf->st_ino = reinterpret_cast<ino_t>(node);
            statbuf->st_nlink = 1;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = static_cast<off_t>(node->size);
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = (node->size + 511) / 512;
            switch (node->type) {
                case ker::vfs::tmpfs::TmpNodeType::FILE:
                    statbuf->st_mode = S_IFREG | 0644;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::DIRECTORY:
                    statbuf->st_mode = S_IFDIR | 0755;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::SYMLINK:
                    statbuf->st_mode = S_IFLNK | 0777;
                    break;
            }
            return 0;
        }
        case FSType::FAT32: {
            return ker::vfs::fat32::fat32_fstat(file, statbuf);
        }
        case FSType::DEVFS: {
            statbuf->st_dev = 0;
            statbuf->st_ino = 1;
            statbuf->st_nlink = 1;
            statbuf->st_mode = S_IFCHR | 0666;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;
            return 0;
        }
        case FSType::SOCKET: {
            statbuf->st_dev = 0;
            statbuf->st_ino = 1;
            statbuf->st_nlink = 1;
            statbuf->st_mode = S_IFSOCK | 0666;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;
            return 0;
        }
        default:
            return -ENOSYS;
    }
}

auto vfs_mount(const char* source, const char* target, const char* fstype) -> int {
    if (target == nullptr || fstype == nullptr) {
        return -EINVAL;
    }

    ker::dev::BlockDevice* bdev = nullptr;

    if (source != nullptr) {
        // Check for PARTUUID= prefix
        constexpr size_t PARTUUID_PREFIX_LEN = 9;  // "PARTUUID="
        bool is_partuuid = (source[0] == 'P' && source[1] == 'A' && source[2] == 'R' && source[3] == 'T' && source[4] == 'U' &&
                            source[5] == 'U' && source[6] == 'I' && source[7] == 'D' && source[8] == '=');

        if (is_partuuid) {
            bdev = ker::dev::block_device_find_by_partuuid(source + PARTUUID_PREFIX_LEN);
            if (bdev == nullptr) {
                ker::mod::io::serial::write("vfs_mount: PARTUUID not found: ");
                ker::mod::io::serial::write(source + PARTUUID_PREFIX_LEN);
                ker::mod::io::serial::write("\n");
                return -ENOENT;
            }
        } else if (source[0] == '/' && source[1] == 'd' && source[2] == 'e' && source[3] == 'v' && source[4] == '/') {
            // /dev/XXX - lookup by device name
            bdev = ker::dev::block_device_find_by_name(source + 5);
            if (bdev == nullptr) {
                ker::mod::io::serial::write("vfs_mount: device not found: ");
                ker::mod::io::serial::write(source);
                ker::mod::io::serial::write("\n");
                return -ENOENT;
            }
        }
    }

    // Create mount point directory in tmpfs if needed
    vfs_mkdir(target, 0755);

    return mount_filesystem(target, fstype, bdev);
}

void init() {
    vfs_debug_log("vfs: init\n");
    // Register tmpfs as a minimal root filesystem
    ker::vfs::tmpfs::register_tmpfs();
    // Mount tmpfs at root
    mount_filesystem("/", "tmpfs", nullptr);

    // Register FAT32 driver (will be mounted when a disk is available)
    ker::vfs::fat32::register_fat32();

    // Register and mount devfs for device files
    ker::vfs::devfs::devfs_init();
    mount_filesystem("/dev", "devfs", nullptr);
}

auto vfs_sendfile(int outfd, int infd, off_t* offset, size_t count) -> ssize_t {
    // Get the current task
    auto* task = mod::sched::getCurrentTask();
    if (task == nullptr) {
        return -ESRCH;
    }

    // Get input file
    auto* infile = vfs_get_file(task, infd);
    if (infile == nullptr) {
        return -EBADF;
    }

    // Get output file
    auto* outfile = vfs_get_file(task, outfd);
    if (outfile == nullptr) {
        return -EBADF;
    }

    // Allocate buffer for reading/writing
    constexpr size_t BUF_SIZE = 65536;  // 64KB buffer
    auto* buffer = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(BUF_SIZE));
    if (buffer == nullptr) {
        return -ENOMEM;
    }

    ssize_t total_sent = 0;

    while (total_sent < static_cast<ssize_t>(count)) {
        size_t to_read = (count - total_sent) > BUF_SIZE ? BUF_SIZE : (count - total_sent);

        // If offset is provided, seek to that position first
        if (offset != nullptr) {
            off_t seek_result = vfs_lseek(infd, *offset, SEEK_SET);
            if (seek_result < 0) {
                ker::mod::mm::dyn::kmalloc::free(buffer);
                return seek_result;
            }
        }

        // Read from input file
        size_t bytes_read = 0;
        ssize_t read_result = vfs_read(infd, buffer, to_read, &bytes_read);
        if (read_result < 0) {
            ker::mod::mm::dyn::kmalloc::free(buffer);
            return read_result;
        }

        // If we read 0 bytes, we're at EOF
        if (bytes_read == 0) {
            break;
        }

        // Write to output file
        size_t bytes_written = 0;
        ssize_t write_result = vfs_write(outfd, buffer, bytes_read, &bytes_written);
        if (write_result < 0) {
            ker::mod::mm::dyn::kmalloc::free(buffer);
            return write_result;
        }

        // Update offset if provided
        if (offset != nullptr) {
            *offset += bytes_written;
        }

        total_sent += bytes_written;

        // If we wrote less than we read, the output device might be full
        if (bytes_written < bytes_read) {
            break;
        }
    }

    ker::mod::mm::dyn::kmalloc::free(buffer);
    return total_sent;
}

}  // namespace ker::vfs
