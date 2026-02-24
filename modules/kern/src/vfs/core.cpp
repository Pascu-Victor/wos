#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <dev/block_device.hpp>
#include <dev/device.hpp>
#include <mod/io/serial/serial.hpp>
#include <net/wki/remote_vfs.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <string_view>
#include <vfs/fs/devfs.hpp>
#include <vfs/fs/fat32.hpp>
#include <vfs/fs/procfs.hpp>
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

    // Apply umask on creation
    if (flags & ker::vfs::O_CREAT) {
        auto* task = ker::mod::sched::get_current_task();
        if (task != nullptr) {
            mode = mode & ~static_cast<int>(task->umask);
        }
    }

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

    auto* current = ker::mod::sched::get_current_task();
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
        case FSType::REMOTE:
            f = ker::net::wki::wki_remote_vfs_open_path(fs_relative_path, flags, mode, mount->private_data);
            break;
        case FSType::PROCFS:
            f = ker::vfs::procfs::procfs_open_path(fs_relative_path, flags, mode);
            if (f != nullptr) {
                f->fops = ker::vfs::procfs::get_procfs_fops();
                f->fs_type = FSType::PROCFS;
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

    // Store the absolute VFS path for mount-overlay directory listing
    size_t path_len = std::strlen(pathBuffer);
    auto* path_copy = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(path_len + 1));
    if (path_copy != nullptr) {
        std::memcpy(path_copy, pathBuffer, path_len + 1);
        f->vfs_path = path_copy;
    } else {
        f->vfs_path = nullptr;
    }
    f->dir_fs_count = static_cast<size_t>(-1);
    f->open_flags = flags;
    f->fd_flags = (flags & ker::vfs::O_CLOEXEC) ? ker::vfs::FD_CLOEXEC : 0;

    // Permission check: verify R/W access based on open flags
    // Build required access bits from open flags
    int required_access = 0;
    int accmode = flags & 3;                                 // O_RDONLY=0, O_WRONLY=1, O_RDWR=2
    if (accmode == 0 || accmode == 2) required_access |= 4;  // R_OK
    if (accmode == 1 || accmode == 2) required_access |= 2;  // W_OK

    // Get the file's mode/uid/gid for permission check
    if (required_access != 0 && f->fs_type == FSType::TMPFS) {
        auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(f->private_data);
        if (node != nullptr) {
            int perm_ret = vfs_check_permission(node->mode, node->uid, node->gid, required_access);
            if (perm_ret < 0) {
                // Permission denied — clean up and return
                if (f->vfs_path != nullptr) ker::mod::mm::dyn::kmalloc::free(const_cast<char*>(f->vfs_path));
                ker::mod::mm::dyn::kmalloc::free(f);
                return perm_ret;
            }
        }
    }

    int fd = vfs_alloc_fd(current, f);
    if (fd < 0) {
        return -1;
    }
    return fd;
}

auto vfs_close(int fd) -> int {
    // Release FD from current task
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
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
        // Free the VFS path string if allocated
        if (f->vfs_path != nullptr) {
            ker::mod::mm::dyn::kmalloc::free(const_cast<char*>(f->vfs_path));
        }
        // Free the File descriptor object (just the handle/wrapper)
        // but keep the underlying fs node (f->private_data) intact
        // so the file can be reopened later
        ker::mod::mm::dyn::kmalloc::free((void*)f);
    }

    return 0;
}

auto vfs_read(int fd, void* buf, size_t count, size_t* actual_size) -> ssize_t {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
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
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
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
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
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

auto vfs_resolve_dirfd(ker::mod::sched::task::Task* task, int dirfd, const char* pathname, char* resolved, size_t resolved_size) -> int {
    if (task == nullptr || pathname == nullptr || resolved == nullptr || resolved_size == 0) {
        return -EINVAL;
    }

    // Absolute pathnames ignore dirfd entirely
    if (pathname[0] == '/') {
        size_t len = strlen(pathname);
        if (len >= resolved_size) return -ENAMETOOLONG;
        memcpy(resolved, pathname, len + 1);
        return 0;
    }

    // Determine the base directory path
    const char* base = nullptr;
    if (dirfd == AT_FDCWD) {
        base = task->cwd;
    } else {
        auto* file = vfs_get_file(task, dirfd);
        if (file == nullptr) return -EBADF;
        if (!file->is_directory) return -ENOTDIR;
        base = file->vfs_path;
        if (base == nullptr) return -EBADF;
    }

    // Concatenate base + "/" + pathname
    size_t base_len = strlen(base);
    size_t path_len = strlen(pathname);

    // Strip trailing slash from base
    while (base_len > 1 && base[base_len - 1] == '/') {
        base_len--;
    }

    // Need: base + "/" + pathname + '\0'
    if (base_len + 1 + path_len + 1 > resolved_size) {
        return -ENAMETOOLONG;
    }

    memcpy(resolved, base, base_len);
    resolved[base_len] = '/';
    memcpy(resolved + base_len + 1, pathname, path_len + 1);
    return 0;
}

auto vfs_isatty(int fd) -> bool {
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
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
    ker::mod::sched::task::Task* t = ker::mod::sched::get_current_task();
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

    // Buffer must be large enough for at least one DirEntry
    if (buffer == nullptr || max_size < sizeof(DirEntry)) {
        return -EINVAL;
    }

    // We allow vfs_readdir to be null — the directory may contain only mount children
    bool has_fs_readdir = (f->fops != nullptr) && (f->fops->vfs_readdir != nullptr);

    auto* entries = static_cast<DirEntry*>(buffer);
    size_t max_entries = max_size / sizeof(DirEntry);
    size_t entries_read = 0;

    // Read directory entries using the current position as index
    size_t start_index = static_cast<size_t>(f->pos);

    for (size_t i = 0; i < max_entries; ++i) {
        size_t actual_index = start_index + i;

        // Phase 1: try filesystem readdir
        if (has_fs_readdir && (f->dir_fs_count == static_cast<size_t>(-1) || actual_index < f->dir_fs_count)) {
            int ret = f->fops->vfs_readdir(f, &entries[entries_read], actual_index);
            if (ret == 0) {
                entries_read++;
                continue;
            }
            // FS entries exhausted at this index
            f->dir_fs_count = actual_index;
        }

        // Phase 2: inject mount-point children as synthetic directory entries.
        // For each mount whose path starts with vfs_path, extract the first
        // path component after vfs_path as a child directory name.
        // Deduplicate against FS entries and against earlier mounts that
        // yield the same child component.
        bool found_mount_child = false;
        if (f->vfs_path != nullptr) {
            size_t fs_count = has_fs_readdir ? f->dir_fs_count : 0;
            size_t mount_idx = actual_index - fs_count;

            size_t dir_len = std::strlen(f->vfs_path);
            size_t child_count = 0;

            for (size_t mi = 0; mi < get_mount_count(); ++mi) {
                MountPoint* mp = get_mount_at(mi);
                if (mp == nullptr || mp->path == nullptr) continue;

                size_t mp_len = std::strlen(mp->path);
                const char* child_start = nullptr;
                size_t child_len = 0;

                if (dir_len == 1 && f->vfs_path[0] == '/') {
                    // Root directory: child is first component of "/xxx[/...]"
                    if (mp_len > 1 && mp->path[0] == '/') {
                        child_start = mp->path + 1;
                    }
                } else {
                    // Non-root: mount must start with dir_path + "/"
                    if (mp_len > dir_len && std::memcmp(mp->path, f->vfs_path, dir_len) == 0 && mp->path[dir_len] == '/') {
                        child_start = mp->path + dir_len + 1;
                    }
                }

                if (child_start == nullptr || *child_start == '\0') continue;

                // Extract only the first path component
                const char* p = child_start;
                while (*p != '\0' && *p != '/') p++;
                child_len = static_cast<size_t>(p - child_start);
                if (child_len == 0) continue;

                // Dedup against earlier mounts that yield the same child name
                bool dup_mount = false;
                for (size_t mj = 0; mj < mi; ++mj) {
                    MountPoint* mp2 = get_mount_at(mj);
                    if (mp2 == nullptr || mp2->path == nullptr) continue;
                    size_t mp2_len = std::strlen(mp2->path);
                    const char* c2 = nullptr;

                    if (dir_len == 1 && f->vfs_path[0] == '/') {
                        if (mp2_len > 1 && mp2->path[0] == '/') c2 = mp2->path + 1;
                    } else {
                        if (mp2_len > dir_len && std::memcmp(mp2->path, f->vfs_path, dir_len) == 0 && mp2->path[dir_len] == '/') {
                            c2 = mp2->path + dir_len + 1;
                        }
                    }
                    if (c2 == nullptr || *c2 == '\0') continue;

                    const char* p2 = c2;
                    while (*p2 != '\0' && *p2 != '/') p2++;
                    size_t c2_len = static_cast<size_t>(p2 - c2);

                    if (c2_len == child_len && std::memcmp(child_start, c2, child_len) == 0) {
                        dup_mount = true;
                        break;
                    }
                }
                if (dup_mount) continue;

                // Dedup against FS readdir entries
                if (has_fs_readdir && fs_count > 0) {
                    bool already_in_fs = false;
                    DirEntry probe = {};
                    for (size_t pi = 0; pi < fs_count; ++pi) {
                        int pret = f->fops->vfs_readdir(f, &probe, pi);
                        if (pret != 0) break;
                        size_t dn_len = std::strlen(probe.d_name.data());
                        if (dn_len == child_len && std::memcmp(probe.d_name.data(), child_start, child_len) == 0) {
                            already_in_fs = true;
                            break;
                        }
                    }
                    if (already_in_fs) continue;
                }

                if (child_count == mount_idx) {
                    // Fill the synthetic DirEntry
                    entries[entries_read].d_ino = reinterpret_cast<uint64_t>(mp);
                    entries[entries_read].d_off = actual_index + 1;
                    entries[entries_read].d_reclen = sizeof(DirEntry);
                    entries[entries_read].d_type = DT_DIR;

                    size_t copy_len = child_len < DIRENT_NAME_MAX - 1 ? child_len : DIRENT_NAME_MAX - 1;
                    std::memcpy(entries[entries_read].d_name.data(), child_start, copy_len);
                    entries[entries_read].d_name[copy_len] = '\0';

                    entries_read++;
                    found_mount_child = true;
                    break;
                }
                child_count++;
            }
        }

        if (found_mount_child) {
            continue;
        }

        // No more entries from either FS or mount children
        break;
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

    if (mount->fs_type == FSType::PROCFS) {
        // Strip mount prefix for procfs readlink
        size_t ml = 0;
        while (mount->path[ml] != '\0') ml++;
        const char* fsp = path;
        if (ml == 1 && mount->path[0] == '/')
            fsp = path + 1;
        else if (path[ml] == '/')
            fsp = path + ml + 1;
        else
            fsp = path + ml;

        auto* f = ker::vfs::procfs::procfs_open_path(fsp, 0, 0);
        if (f == nullptr) return -ENOENT;
        ssize_t ret = ker::vfs::procfs::get_procfs_fops()->vfs_readlink(f, buf, bufsize);
        ker::vfs::procfs::get_procfs_fops()->vfs_close(f);
        ker::mod::mm::dyn::kmalloc::free(f);
        return ret;
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
            statbuf->st_uid = node->uid;
            statbuf->st_gid = node->gid;
            statbuf->st_rdev = 0;
            statbuf->st_size = static_cast<off_t>(node->size);
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = (node->size + 511) / 512;
            switch (node->type) {
                case ker::vfs::tmpfs::TmpNodeType::FILE:
                    statbuf->st_mode = S_IFREG | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::DIRECTORY:
                    statbuf->st_mode = S_IFDIR | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::SYMLINK:
                    statbuf->st_mode = S_IFLNK | node->mode;
                    break;
            }
            return 0;
        }
        case FSType::FAT32: {
            return ker::vfs::fat32::fat32_stat(fs_path, statbuf, static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data));
        }
        case FSType::DEVFS: {
            // Walk devfs tree to determine if directory or device
            auto* node = ker::vfs::devfs::devfs_walk_path(fs_path);
            if (node == nullptr) {
                return -ENOENT;
            }
            statbuf->st_dev = 0;
            statbuf->st_ino = reinterpret_cast<ino_t>(node);
            statbuf->st_nlink = 1;
            statbuf->st_uid = node->uid;
            statbuf->st_gid = node->gid;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;

            // Set mode based on node type
            if (node->type == ker::vfs::devfs::DevFSNodeType::DIRECTORY) {
                statbuf->st_mode = S_IFDIR | node->mode;
            } else if (node->type == ker::vfs::devfs::DevFSNodeType::SYMLINK) {
                statbuf->st_mode = S_IFLNK | 0777;
            } else if (node->device != nullptr && node->device->type == ker::dev::DeviceType::BLOCK) {
                statbuf->st_mode = S_IFBLK | node->mode;
            } else {
                statbuf->st_mode = S_IFCHR | node->mode;
            }
            return 0;
        }
        case FSType::REMOTE: {
            return ker::net::wki::wki_remote_vfs_stat(mount->private_data, fs_path, statbuf);
        }
        case FSType::PROCFS: {
            // procfs: open the path, check if it exists
            auto* f = ker::vfs::procfs::procfs_open_path(fs_path, 0, 0);
            if (f == nullptr) return -ENOENT;
            statbuf->st_dev = 0;
            statbuf->st_ino = 1;
            statbuf->st_nlink = 1;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;
            if (f->is_directory) {
                statbuf->st_mode = S_IFDIR | 0555;
            } else {
                auto* pfd = static_cast<ker::vfs::procfs::ProcFileData*>(f->private_data);
                if (pfd != nullptr && (pfd->node.type == ker::vfs::procfs::ProcNodeType::EXE_LINK ||
                                       pfd->node.type == ker::vfs::procfs::ProcNodeType::SELF_LINK)) {
                    statbuf->st_mode = S_IFLNK | 0777;
                } else {
                    statbuf->st_mode = S_IFREG | 0444;
                }
            }
            // Clean up temporary file
            ker::vfs::procfs::get_procfs_fops()->vfs_close(f);
            ker::mod::mm::dyn::kmalloc::free(f);
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

    auto* task = ker::mod::sched::get_current_task();
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
            statbuf->st_uid = node->uid;
            statbuf->st_gid = node->gid;
            statbuf->st_rdev = 0;
            statbuf->st_size = static_cast<off_t>(node->size);
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = (node->size + 511) / 512;
            switch (node->type) {
                case ker::vfs::tmpfs::TmpNodeType::FILE:
                    statbuf->st_mode = S_IFREG | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::DIRECTORY:
                    statbuf->st_mode = S_IFDIR | node->mode;
                    break;
                case ker::vfs::tmpfs::TmpNodeType::SYMLINK:
                    statbuf->st_mode = S_IFLNK | node->mode;
                    break;
            }
            return 0;
        }
        case FSType::FAT32: {
            return ker::vfs::fat32::fat32_fstat(file, statbuf);
        }
        case FSType::DEVFS: {
            auto* node = static_cast<ker::vfs::devfs::DevFSNode*>(file->private_data);
            statbuf->st_dev = 0;
            statbuf->st_ino = node ? reinterpret_cast<ino_t>(node) : 1;
            statbuf->st_nlink = 1;
            statbuf->st_uid = node ? node->uid : 0;
            statbuf->st_gid = node ? node->gid : 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = 0;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;

            // Set mode based on whether this is a directory or device
            if (file->is_directory) {
                statbuf->st_mode = S_IFDIR | (node ? node->mode : 0755);
            } else {
                statbuf->st_mode = S_IFCHR | (node ? node->mode : 0666);
            }
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
        case FSType::REMOTE: {
            // Remote files — report basic file info from position
            statbuf->st_dev = 0;
            statbuf->st_ino = 1;
            statbuf->st_nlink = 1;
            statbuf->st_mode = S_IFREG | 0644;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = file->pos;
            statbuf->st_blksize = 4096;
            statbuf->st_blocks = 0;
            return 0;
        }
        default:
            return -ENOSYS;
    }
}

// --- umount ---
auto vfs_umount(const char* target) -> int { return unmount_filesystem(target); }

// --- dup / dup2 ---
auto vfs_dup(int oldfd) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, oldfd);
    if (f == nullptr) return -EBADF;
    f->refcount++;
    int newfd = vfs_alloc_fd(task, f);
    if (newfd < 0) {
        f->refcount--;
        return -EMFILE;
    }
    return newfd;
}

auto vfs_dup2(int oldfd, int newfd) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    if (newfd < 0 || static_cast<unsigned>(newfd) >= ker::mod::sched::task::Task::FD_TABLE_SIZE) return -EBADF;
    auto* f = vfs_get_file(task, oldfd);
    if (f == nullptr) return -EBADF;
    if (oldfd == newfd) return newfd;
    // Close newfd if it's open
    auto* existing = vfs_get_file(task, newfd);
    if (existing != nullptr) {
        vfs_close(newfd);
    }
    f->refcount++;
    task->fds[newfd] = f;
    return newfd;
}

// --- getcwd / chdir ---
auto vfs_getcwd(char* buf, size_t size) -> int {
    if (buf == nullptr || size == 0) return -EINVAL;
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    size_t len = std::strlen(task->cwd);
    if (len + 1 > size) return -ERANGE;
    std::memcpy(buf, task->cwd, len + 1);
    return 0;
}

auto vfs_chdir(const char* path) -> int {
    if (path == nullptr) return -EINVAL;
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;

    // Canonicalize the path
    char resolved[MAX_PATH_LEN];
    size_t plen = std::strlen(path);

    if (path[0] != '/') {
        // Relative path — prepend cwd
        size_t cwdlen = std::strlen(task->cwd);
        if (cwdlen + 1 + plen + 1 > MAX_PATH_LEN) return -ENAMETOOLONG;
        std::memcpy(resolved, task->cwd, cwdlen);
        if (cwdlen > 1) resolved[cwdlen++] = '/';  // don't double-/ after root
        std::memcpy(resolved + cwdlen, path, plen + 1);
    } else {
        if (plen + 1 > MAX_PATH_LEN) return -ENAMETOOLONG;
        std::memcpy(resolved, path, plen + 1);
    }

    canonicalize_path(resolved, MAX_PATH_LEN);

    // Verify the directory exists via stat
    ker::vfs::stat st{};
    int ret = vfs_stat(resolved, &st);
    if (ret < 0) return -ENOENT;
    if ((st.st_mode & S_IFDIR) == 0) return -ENOTDIR;

    // Copy to task cwd
    size_t rlen = std::strlen(resolved);
    if (rlen + 1 > ker::mod::sched::task::Task::CWD_MAX) return -ENAMETOOLONG;
    std::memcpy(task->cwd, resolved, rlen + 1);
    return 0;
}

// --- access ---
// R_OK=4, W_OK=2, X_OK=1, F_OK=0
auto vfs_check_permission(uint32_t file_mode, uint32_t file_uid, uint32_t file_gid, int access_bits) -> int {
    if (access_bits == 0) return 0;  // F_OK — existence only

    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;

    // Root can do anything (except execute if no execute bit set anywhere)
    if (task->euid == 0) {
        if ((access_bits & 1) && !(file_mode & 0111)) {
            return -EACCES;  // No execute bit set at all
        }
        return 0;
    }

    uint32_t perm_bits;
    if (task->euid == file_uid) {
        perm_bits = (file_mode >> 6) & 7;  // Owner bits
    } else if (task->egid == file_gid) {
        perm_bits = (file_mode >> 3) & 7;  // Group bits
    } else {
        perm_bits = file_mode & 7;  // Other bits
    }

    if ((access_bits & 4) && !(perm_bits & 4)) return -EACCES;  // R_OK
    if ((access_bits & 2) && !(perm_bits & 2)) return -EACCES;  // W_OK
    if ((access_bits & 1) && !(perm_bits & 1)) return -EACCES;  // X_OK
    return 0;
}

auto vfs_access(const char* path, int mode) -> int {
    if (path == nullptr) {
        return -EINVAL;
    }

    // Check existence first
    ker::vfs::stat st{};
    int ret = vfs_stat(path, &st);
    if (ret < 0) {
        return ret;
    }

    if (mode == 0) {
        return 0;  // F_OK — just existence check
    }

    // st_mode already has the full mode bits from stat
    return vfs_check_permission(st.st_mode & 07777, st.st_uid, st.st_gid, mode);
}

// --- pread / pwrite ---
auto vfs_pread(int fd, void* buf, size_t count, off_t offset) -> ssize_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, fd);
    if (f == nullptr) return -EBADF;
    if (f->fops == nullptr || f->fops->vfs_read == nullptr) return -ENOSYS;
    // Read at given offset without modifying file position
    return f->fops->vfs_read(f, buf, count, static_cast<size_t>(offset));
}

auto vfs_pwrite(int fd, const void* buf, size_t count, off_t offset) -> ssize_t {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, fd);
    if (f == nullptr) return -EBADF;
    if (f->fops == nullptr || f->fops->vfs_write == nullptr) return -ENOSYS;
    return f->fops->vfs_write(f, buf, count, static_cast<size_t>(offset));
}

// --- unlink ---
auto vfs_unlink(const char* path) -> int {
    if (path == nullptr) return -EINVAL;

    // Canonicalize
    char pathBuf[MAX_PATH_LEN];
    size_t pl = std::strlen(path);
    if (pl + 1 > MAX_PATH_LEN) return -ENAMETOOLONG;
    std::memcpy(pathBuf, path, pl + 1);
    canonicalize_path(pathBuf, MAX_PATH_LEN);

    MountPoint* mount = find_mount_point(pathBuf);
    if (mount == nullptr) return -ENOENT;

    if (mount->fs_type == FSType::FAT32) {
        // Strip mount prefix for FAT32
        size_t mount_len = std::strlen(mount->path);
        const char* fs_path = pathBuf;
        if (mount_len == 1 && mount->path[0] == '/')
            fs_path = pathBuf + 1;
        else if (pathBuf[mount_len] == '/')
            fs_path = pathBuf + mount_len + 1;
        else
            fs_path = pathBuf + mount_len;
        return ker::vfs::fat32::fat32_unlink_path(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data), fs_path);
    }

    if (mount->fs_type != FSType::TMPFS) return -ENOSYS;

    // Strip mount prefix
    size_t mount_len = std::strlen(mount->path);
    const char* fs_path = pathBuf;
    if (mount_len == 1 && mount->path[0] == '/') {
        fs_path = pathBuf + 1;
    } else if (pathBuf[mount_len] == '/') {
        fs_path = pathBuf + mount_len + 1;
    } else {
        fs_path = pathBuf + mount_len;
    }

    // Walk to parent, then find child
    const char* last_slash = nullptr;
    for (const char* p = fs_path; *p; ++p) {
        if (*p == '/') last_slash = p;
    }

    ker::vfs::tmpfs::TmpNode* parent = nullptr;
    const char* name = nullptr;

    if (last_slash == nullptr) {
        parent = ker::vfs::tmpfs::get_root_node();
        name = fs_path;
    } else {
        char parent_path[MAX_PATH_LEN];
        auto paren_len = static_cast<size_t>(last_slash - fs_path);
        if (paren_len >= MAX_PATH_LEN) return -ENAMETOOLONG;
        std::memcpy(parent_path, fs_path, paren_len);
        parent_path[paren_len] = '\0';
        parent = ker::vfs::tmpfs::tmpfs_walk_path(parent_path, false);
        name = last_slash + 1;
    }

    if (parent == nullptr || name == nullptr || *name == '\0') return -ENOENT;

    auto* child = ker::vfs::tmpfs::tmpfs_lookup(parent, name);
    if (child == nullptr) return -ENOENT;
    if (child->type == ker::vfs::tmpfs::TmpNodeType::DIRECTORY) return -EISDIR;

    // Remove child from parent's children array
    for (size_t i = 0; i < parent->children_count; ++i) {
        if (parent->children[i] == child) {
            // Shift remaining children down
            for (size_t j = i; j + 1 < parent->children_count; ++j) {
                parent->children[j] = parent->children[j + 1];
            }
            parent->children_count--;
            parent->children[parent->children_count] = nullptr;
            // Free node data
            delete[] child->data;
            delete[] child->symlink_target;
            delete child;
            return 0;
        }
    }
    return -ENOENT;
}

// --- rmdir ---
auto vfs_rmdir(const char* path) -> int {
    if (path == nullptr) return -EINVAL;

    char pathBuf[MAX_PATH_LEN];
    size_t pl = std::strlen(path);
    if (pl + 1 > MAX_PATH_LEN) return -ENAMETOOLONG;
    std::memcpy(pathBuf, path, pl + 1);
    canonicalize_path(pathBuf, MAX_PATH_LEN);

    MountPoint* mount = find_mount_point(pathBuf);
    if (mount == nullptr) return -ENOENT;

    if (mount->fs_type == FSType::FAT32) {
        size_t mount_len = std::strlen(mount->path);
        const char* fs_path = pathBuf;
        if (mount_len == 1 && mount->path[0] == '/')
            fs_path = pathBuf + 1;
        else if (pathBuf[mount_len] == '/')
            fs_path = pathBuf + mount_len + 1;
        else
            fs_path = pathBuf + mount_len;
        return ker::vfs::fat32::fat32_rmdir_path(static_cast<ker::vfs::fat32::FAT32MountContext*>(mount->private_data), fs_path);
    }

    if (mount->fs_type != FSType::TMPFS) return -ENOSYS;

    size_t mount_len = std::strlen(mount->path);
    const char* fs_path = pathBuf;
    if (mount_len == 1 && mount->path[0] == '/') {
        fs_path = pathBuf + 1;
    } else if (pathBuf[mount_len] == '/') {
        fs_path = pathBuf + mount_len + 1;
    } else {
        fs_path = pathBuf + mount_len;
    }

    const char* last_slash = nullptr;
    for (const char* p = fs_path; *p; ++p) {
        if (*p == '/') last_slash = p;
    }

    ker::vfs::tmpfs::TmpNode* parent = nullptr;
    const char* name = nullptr;

    if (last_slash == nullptr) {
        parent = ker::vfs::tmpfs::get_root_node();
        name = fs_path;
    } else {
        char parent_path[MAX_PATH_LEN];
        auto paren_len = static_cast<size_t>(last_slash - fs_path);
        if (paren_len >= MAX_PATH_LEN) return -ENAMETOOLONG;
        std::memcpy(parent_path, fs_path, paren_len);
        parent_path[paren_len] = '\0';
        parent = ker::vfs::tmpfs::tmpfs_walk_path(parent_path, false);
        name = last_slash + 1;
    }

    if (parent == nullptr || name == nullptr || *name == '\0') return -ENOENT;

    auto* child = ker::vfs::tmpfs::tmpfs_lookup(parent, name);
    if (child == nullptr) return -ENOENT;
    if (child->type != ker::vfs::tmpfs::TmpNodeType::DIRECTORY) return -ENOTDIR;
    if (child->children_count > 0) return -ENOTEMPTY;

    for (size_t i = 0; i < parent->children_count; ++i) {
        if (parent->children[i] == child) {
            for (size_t j = i; j + 1 < parent->children_count; ++j) {
                parent->children[j] = parent->children[j + 1];
            }
            parent->children_count--;
            parent->children[parent->children_count] = nullptr;
            delete[] child->children;
            delete child;
            return 0;
        }
    }
    return -ENOENT;
}

// --- rename ---
auto vfs_rename(const char* oldpath, const char* newpath) -> int {
    if (oldpath == nullptr || newpath == nullptr) return -EINVAL;

    // Both paths must be on tmpfs for now
    char oldBuf[MAX_PATH_LEN], newBuf[MAX_PATH_LEN];
    size_t ol = std::strlen(oldpath), nl = std::strlen(newpath);
    if (ol + 1 > MAX_PATH_LEN || nl + 1 > MAX_PATH_LEN) return -ENAMETOOLONG;
    std::memcpy(oldBuf, oldpath, ol + 1);
    std::memcpy(newBuf, newpath, nl + 1);
    canonicalize_path(oldBuf, MAX_PATH_LEN);
    canonicalize_path(newBuf, MAX_PATH_LEN);

    MountPoint* oldMount = find_mount_point(oldBuf);
    MountPoint* newMount = find_mount_point(newBuf);
    if (!oldMount || !newMount) return -ENOENT;

    if (oldMount->fs_type == FSType::FAT32 && newMount->fs_type == FSType::FAT32 && oldMount == newMount) {
        auto strip_mount_fat = [](const char* buf, MountPoint* m) -> const char* {
            size_t ml = std::strlen(m->path);
            if (ml == 1 && m->path[0] == '/') return buf + 1;
            if (buf[ml] == '/') return buf + ml + 1;
            return buf + ml;
        };
        return ker::vfs::fat32::fat32_rename_path(static_cast<ker::vfs::fat32::FAT32MountContext*>(oldMount->private_data),
                                                  strip_mount_fat(oldBuf, oldMount), strip_mount_fat(newBuf, newMount));
    }

    if (oldMount->fs_type != FSType::TMPFS || newMount->fs_type != FSType::TMPFS) return -ENOSYS;

    // Helper lambda to strip mount prefix
    auto strip_mount = [](const char* buf, MountPoint* m) -> const char* {
        size_t ml = std::strlen(m->path);
        if (ml == 1 && m->path[0] == '/') return buf + 1;
        if (buf[ml] == '/') return buf + ml + 1;
        return buf + ml;
    };

    const char* oldFs = strip_mount(oldBuf, oldMount);
    const char* newFs = strip_mount(newBuf, newMount);

    // Lookup old node
    auto* oldNode = ker::vfs::tmpfs::tmpfs_walk_path(oldFs, false);
    if (oldNode == nullptr) return -ENOENT;

    // Find old parent
    auto* oldParent = oldNode->parent;
    if (oldParent == nullptr) return -EINVAL;  // Can't rename root

    // Walk to new parent, extract new name
    const char* newLastSlash = nullptr;
    for (const char* p = newFs; *p; ++p) {
        if (*p == '/') newLastSlash = p;
    }

    ker::vfs::tmpfs::TmpNode* newParent = nullptr;
    const char* newName = nullptr;

    if (newLastSlash == nullptr) {
        newParent = ker::vfs::tmpfs::get_root_node();
        newName = newFs;
    } else {
        char parentPath[MAX_PATH_LEN];
        auto plen = static_cast<size_t>(newLastSlash - newFs);
        if (plen >= MAX_PATH_LEN) return -ENAMETOOLONG;
        std::memcpy(parentPath, newFs, plen);
        parentPath[plen] = '\0';
        newParent = ker::vfs::tmpfs::tmpfs_walk_path(parentPath, false);
        newName = newLastSlash + 1;
    }

    if (newParent == nullptr || newName == nullptr || *newName == '\0') return -ENOENT;

    // If destination exists, remove it
    auto* existing = ker::vfs::tmpfs::tmpfs_lookup(newParent, newName);
    if (existing != nullptr) {
        // Remove existing from parent
        for (size_t i = 0; i < newParent->children_count; ++i) {
            if (newParent->children[i] == existing) {
                for (size_t j = i; j + 1 < newParent->children_count; ++j) {
                    newParent->children[j] = newParent->children[j + 1];
                }
                newParent->children_count--;
                newParent->children[newParent->children_count] = nullptr;
                delete[] existing->data;
                delete[] existing->symlink_target;
                delete[] existing->children;
                delete existing;
                break;
            }
        }
    }

    // Remove old node from old parent
    for (size_t i = 0; i < oldParent->children_count; ++i) {
        if (oldParent->children[i] == oldNode) {
            for (size_t j = i; j + 1 < oldParent->children_count; ++j) {
                oldParent->children[j] = oldParent->children[j + 1];
            }
            oldParent->children_count--;
            oldParent->children[oldParent->children_count] = nullptr;
            break;
        }
    }

    // Rename and reparent
    size_t nn_len = std::strlen(newName);
    size_t copy_len = nn_len < ker::vfs::tmpfs::TMPFS_NAME_MAX - 1 ? nn_len : ker::vfs::tmpfs::TMPFS_NAME_MAX - 1;
    std::memcpy(oldNode->name.data(), newName, copy_len);
    oldNode->name[copy_len] = '\0';

    // Add to new parent (inline — avoid circular include of tmpfs internal helpers)
    // Grow children array if needed
    if (newParent->children_count >= newParent->children_capacity) {
        size_t new_cap = (newParent->children_capacity == 0) ? 8 : newParent->children_capacity * 2;
        auto** new_arr = new ker::vfs::tmpfs::TmpNode*[new_cap];
        for (size_t i = 0; i < newParent->children_count; ++i) new_arr[i] = newParent->children[i];
        for (size_t i = newParent->children_count; i < new_cap; ++i) new_arr[i] = nullptr;
        delete[] newParent->children;
        newParent->children = new_arr;
        newParent->children_capacity = new_cap;
    }
    newParent->children[newParent->children_count++] = oldNode;
    oldNode->parent = newParent;

    return 0;
}

// --- chmod (stub) ---
auto vfs_chmod(const char* path, int mode) -> int {
    if (path == nullptr) return -EINVAL;

    // Resolve mount point
    char pathBuffer[512];
    size_t len = strlen(path);
    if (len >= sizeof(pathBuffer)) return -ENAMETOOLONG;
    memcpy(pathBuffer, path, len + 1);

    auto* mount = find_mount_point(pathBuffer);
    if (mount == nullptr) return -ENOENT;

    size_t mount_len = strlen(mount->path);
    const char* fs_path = "";
    if (mount_len == 1 && mount->path[0] == '/') {
        fs_path = pathBuffer + 1;
    } else if (pathBuffer[mount_len] == '/') {
        fs_path = pathBuffer + mount_len + 1;
    } else if (pathBuffer[mount_len] == '\0') {
        fs_path = "";
    } else {
        fs_path = pathBuffer + mount_len;
    }

    switch (mount->fs_type) {
        case FSType::TMPFS: {
            auto* node = ker::vfs::tmpfs::tmpfs_walk_path(fs_path, false);
            if (node == nullptr) return -ENOENT;
            node->mode = static_cast<uint32_t>(mode) & 07777;
            return 0;
        }
        case FSType::DEVFS: {
            auto* node = ker::vfs::devfs::devfs_walk_path(fs_path);
            if (node == nullptr) return -ENOENT;
            node->mode = static_cast<uint32_t>(mode) & 07777;
            return 0;
        }
        case FSType::FAT32:
            // FAT32 has no on-disk permission model; accept silently
            return 0;
        default:
            return -ENOSYS;
    }
}

auto vfs_fchmod(int fd, int mode) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, fd);
    if (f == nullptr) return -EBADF;

    switch (f->fs_type) {
        case FSType::TMPFS: {
            auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(f->private_data);
            if (node == nullptr) return -EBADF;
            node->mode = static_cast<uint32_t>(mode) & 07777;
            return 0;
        }
        case FSType::DEVFS:
        case FSType::FAT32:
            return 0;  // Accept silently
        default:
            return -ENOSYS;
    }
}

auto vfs_chown(const char* path, uint32_t owner, uint32_t group) -> int {
    if (path == nullptr) return -EINVAL;

    char pathBuffer[512];
    size_t len = strlen(path);
    if (len >= sizeof(pathBuffer)) return -ENAMETOOLONG;
    memcpy(pathBuffer, path, len + 1);

    auto* mount = find_mount_point(pathBuffer);
    if (mount == nullptr) return -ENOENT;

    size_t mount_len = strlen(mount->path);
    const char* fs_path = "";
    if (mount_len == 1 && mount->path[0] == '/') {
        fs_path = pathBuffer + 1;
    } else if (pathBuffer[mount_len] == '/') {
        fs_path = pathBuffer + mount_len + 1;
    } else if (pathBuffer[mount_len] == '\0') {
        fs_path = "";
    } else {
        fs_path = pathBuffer + mount_len;
    }

    switch (mount->fs_type) {
        case FSType::TMPFS: {
            auto* node = ker::vfs::tmpfs::tmpfs_walk_path(fs_path, false);
            if (node == nullptr) return -ENOENT;
            if (owner != static_cast<uint32_t>(-1)) node->uid = owner;
            if (group != static_cast<uint32_t>(-1)) node->gid = group;
            return 0;
        }
        case FSType::DEVFS: {
            auto* node = ker::vfs::devfs::devfs_walk_path(fs_path);
            if (node == nullptr) return -ENOENT;
            if (owner != static_cast<uint32_t>(-1)) node->uid = owner;
            if (group != static_cast<uint32_t>(-1)) node->gid = group;
            return 0;
        }
        case FSType::FAT32:
            return 0;  // Accept silently
        default:
            return -ENOSYS;
    }
}

auto vfs_fchown(int fd, uint32_t owner, uint32_t group) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, fd);
    if (f == nullptr) return -EBADF;

    switch (f->fs_type) {
        case FSType::TMPFS: {
            auto* node = static_cast<ker::vfs::tmpfs::TmpNode*>(f->private_data);
            if (node == nullptr) return -EBADF;
            if (owner != static_cast<uint32_t>(-1)) node->uid = owner;
            if (group != static_cast<uint32_t>(-1)) node->gid = group;
            return 0;
        }
        case FSType::DEVFS:
        case FSType::FAT32:
            return 0;  // Accept silently
        default:
            return -ENOSYS;
    }
}

// --- ftruncate ---
auto vfs_ftruncate(int fd, off_t length) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, fd);
    if (f == nullptr) return -EBADF;
    if (f->fops == nullptr || f->fops->vfs_truncate == nullptr) return -ENOSYS;
    return f->fops->vfs_truncate(f, length);
}

// --- fcntl ---
auto vfs_fcntl(int fd, int cmd, uint64_t arg) -> int {
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;
    auto* f = vfs_get_file(task, fd);
    if (f == nullptr) return -EBADF;

    // F_DUPFD=0, F_GETFD=1, F_SETFD=2, F_GETFL=3, F_SETFL=4 (Linux values)
    switch (cmd) {
        case 0: {  // F_DUPFD — dup to fd >= arg
            f->refcount++;
            for (unsigned i = static_cast<unsigned>(arg); i < ker::mod::sched::task::Task::FD_TABLE_SIZE; ++i) {
                if (task->fds[i] == nullptr) {
                    task->fds[i] = f;
                    return static_cast<int>(i);
                }
            }
            f->refcount--;
            return -EMFILE;
        }
        case 1:  // F_GETFD
            return f->fd_flags;
        case 2:  // F_SETFD
            f->fd_flags = static_cast<int>(arg);
            return 0;
        case 3:  // F_GETFL
            return f->open_flags;
        case 4:  // F_SETFL
            f->open_flags = static_cast<int>(arg);
            return 0;
        default:
            return -EINVAL;
    }
}

// --- pipe ---

// PipeState is shared between both ends. It includes wait queues for blocking.
struct PipeState {
    char* buf;
    size_t capacity;
    size_t head;   // write position
    size_t tail;   // read position
    size_t count;  // bytes in buffer
    bool write_closed;
    bool read_closed;

    // Wait queues for blocking pipe I/O
    static constexpr unsigned MAX_WAITERS = 16;
    uint64_t readers_waiting[MAX_WAITERS];  // PIDs of tasks blocked on read
    uint64_t readers_count;
    uint64_t writers_waiting[MAX_WAITERS];  // PIDs of tasks blocked on write
    uint64_t writers_count;

    // Physical addresses of user buffers + counts for deferred completion
    struct WaiterInfo {
        uint64_t bufPhysAddr;  // Physical address of user buffer
        size_t requested;      // Bytes the user requested
    };
    WaiterInfo reader_info[MAX_WAITERS];
    WaiterInfo writer_info[MAX_WAITERS];
};

// Wake all waiting readers and let them read from the buffer
static void pipe_wake_readers(PipeState* st) {
    for (uint64_t i = 0; i < st->readers_count; i++) {
        auto* waiter = ker::mod::sched::find_task_by_pid_safe(st->readers_waiting[i]);
        if (waiter == nullptr) continue;

        // Copy data into the waiter's buffer
        size_t to_read = st->reader_info[i].requested;
        if (to_read > st->count) to_read = st->count;

        if (to_read > 0 && st->reader_info[i].bufPhysAddr != 0) {
            auto* dst = reinterpret_cast<char*>(ker::mod::mm::addr::getVirtPointer(st->reader_info[i].bufPhysAddr));
            for (size_t j = 0; j < to_read; j++) {
                dst[j] = st->buf[st->tail];
                st->tail = (st->tail + 1) % st->capacity;
            }
            st->count -= to_read;
        }

        // Set return value: bytes read (or 0 for EOF)
        waiter->context.regs.rax = to_read;

        uint64_t targetCpu = ker::mod::sched::get_least_loaded_cpu();
        ker::mod::sched::reschedule_task_for_cpu(targetCpu, waiter);
        waiter->release();
    }
    st->readers_count = 0;
}

// Wake all waiting writers and let them write into the buffer
static void pipe_wake_writers(PipeState* st) {
    for (uint64_t i = 0; i < st->writers_count; i++) {
        auto* waiter = ker::mod::sched::find_task_by_pid_safe(st->writers_waiting[i]);
        if (waiter == nullptr) continue;

        size_t avail = st->capacity - st->count;
        size_t to_write = st->writer_info[i].requested;
        if (to_write > avail) to_write = avail;

        if (to_write > 0 && st->writer_info[i].bufPhysAddr != 0) {
            auto* src = reinterpret_cast<const char*>(ker::mod::mm::addr::getVirtPointer(st->writer_info[i].bufPhysAddr));
            for (size_t j = 0; j < to_write; j++) {
                st->buf[st->head] = src[j];
                st->head = (st->head + 1) % st->capacity;
            }
            st->count += to_write;
        }

        waiter->context.regs.rax = to_write;

        uint64_t targetCpu = ker::mod::sched::get_least_loaded_cpu();
        ker::mod::sched::reschedule_task_for_cpu(targetCpu, waiter);
        waiter->release();
    }
    st->writers_count = 0;
}

auto vfs_pipe(int pipefd[2]) -> int {
    if (pipefd == nullptr) return -EINVAL;
    auto* task = ker::mod::sched::get_current_task();
    if (task == nullptr) return -ESRCH;

    // Allocate pipe buffer
    constexpr size_t PIPE_BUF_SIZE = 4096;
    auto* pipe_buf = new char[PIPE_BUF_SIZE];

    auto* ps = new PipeState{};
    ps->buf = pipe_buf;
    ps->capacity = PIPE_BUF_SIZE;
    ps->head = 0;
    ps->tail = 0;
    ps->count = 0;
    ps->write_closed = false;
    ps->read_closed = false;
    ps->readers_count = 0;
    ps->writers_count = 0;

    // Pipe fops — static lambdas converted to function pointers
    static auto pipe_read = [](File* f, void* buf, size_t count, size_t /*offset*/) -> ssize_t {
        auto* st = static_cast<PipeState*>(f->private_data);
        if (st == nullptr) return -EBADF;

        if (st->count > 0) {
            // Data available — read immediately
            size_t to_read = count < st->count ? count : st->count;
            auto* dst = static_cast<char*>(buf);
            for (size_t i = 0; i < to_read; ++i) {
                dst[i] = st->buf[st->tail];
                st->tail = (st->tail + 1) % st->capacity;
            }
            st->count -= to_read;

            // Wake any blocked writers now that there's buffer space
            if (st->writers_count > 0) {
                pipe_wake_writers(st);
            }

            return static_cast<ssize_t>(to_read);
        }

        // Buffer empty
        if (st->write_closed) return 0;  // EOF — write end closed

        // Block: add current task to readers wait queue
        auto* currentTask = ker::mod::sched::get_current_task();
        if (currentTask == nullptr) return -ESRCH;

        if (st->readers_count < PipeState::MAX_WAITERS) {
            uint64_t idx = st->readers_count++;
            st->readers_waiting[idx] = currentTask->pid;
            // Translate user buffer to physical address for deferred copy
            st->reader_info[idx].bufPhysAddr = ker::mod::mm::virt::translate(currentTask->pagemap, (uint64_t)buf);
            st->reader_info[idx].requested = count;

            currentTask->deferredTaskSwitch = true;
            return 0;  // Will be overwritten when woken
        }

        // Wait queue full — return EAGAIN
        return -EAGAIN;
    };

    static auto pipe_write = [](File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
        auto* st = static_cast<PipeState*>(f->private_data);
        if (st == nullptr) return -EBADF;
        if (st->read_closed) {
            // Send SIGPIPE to the writing process (signal 13)
            auto* task = ker::mod::sched::get_current_task();
            if (task) task->sigPending |= (1ULL << (13 - 1));
            return -EPIPE;
        }

        size_t avail = st->capacity - st->count;
        if (avail > 0) {
            size_t to_write = count < avail ? count : avail;
            auto* src = static_cast<const char*>(buf);
            for (size_t i = 0; i < to_write; ++i) {
                st->buf[st->head] = src[i];
                st->head = (st->head + 1) % st->capacity;
            }
            st->count += to_write;

            // Wake any blocked readers now that there's data
            if (st->readers_count > 0) {
                pipe_wake_readers(st);
            }

            return static_cast<ssize_t>(to_write);
        }

        // Buffer full — block
        auto* currentTask = ker::mod::sched::get_current_task();
        if (currentTask == nullptr) return -ESRCH;

        if (st->writers_count < PipeState::MAX_WAITERS) {
            uint64_t idx = st->writers_count++;
            st->writers_waiting[idx] = currentTask->pid;
            st->writer_info[idx].bufPhysAddr = ker::mod::mm::virt::translate(currentTask->pagemap, (uint64_t)buf);
            st->writer_info[idx].requested = count;

            currentTask->deferredTaskSwitch = true;
            return 0;
        }

        return -EAGAIN;
    };

    static auto pipe_close_read = [](File* f) -> int {
        auto* st = static_cast<PipeState*>(f->private_data);
        if (st != nullptr) {
            st->read_closed = true;
            // Wake blocked writers — they'll get EPIPE on next attempt
            if (st->writers_count > 0) {
                for (uint64_t i = 0; i < st->writers_count; i++) {
                    auto* waiter = ker::mod::sched::find_task_by_pid_safe(st->writers_waiting[i]);
                    if (waiter != nullptr) {
                        waiter->context.regs.rax = static_cast<uint64_t>(-EPIPE);
                        // Send SIGPIPE to the blocked writer
                        waiter->sigPending |= (1ULL << (13 - 1));
                        uint64_t targetCpu = ker::mod::sched::get_least_loaded_cpu();
                        ker::mod::sched::reschedule_task_for_cpu(targetCpu, waiter);
                        waiter->release();
                    }
                }
                st->writers_count = 0;
            }
        }
        // Free if both ends closed
        if (st != nullptr && st->write_closed) {
            delete[] st->buf;
            delete st;
        }
        return 0;
    };

    static auto pipe_close_write = [](File* f) -> int {
        auto* st = static_cast<PipeState*>(f->private_data);
        if (st != nullptr) {
            st->write_closed = true;
            // Wake blocked readers — they'll get EOF (0)
            if (st->readers_count > 0) {
                for (uint64_t i = 0; i < st->readers_count; i++) {
                    auto* waiter = ker::mod::sched::find_task_by_pid_safe(st->readers_waiting[i]);
                    if (waiter != nullptr) {
                        waiter->context.regs.rax = 0;  // EOF
                        uint64_t targetCpu = ker::mod::sched::get_least_loaded_cpu();
                        ker::mod::sched::reschedule_task_for_cpu(targetCpu, waiter);
                        waiter->release();
                    }
                }
                st->readers_count = 0;
            }
        }
        if (st != nullptr && st->read_closed) {
            delete[] st->buf;
            delete st;
        }
        return 0;
    };

    static FileOperations pipe_read_fops = {
        .vfs_open = nullptr,
        .vfs_close = pipe_close_read,
        .vfs_read = pipe_read,
        .vfs_write = nullptr,
        .vfs_lseek = nullptr,
        .vfs_isatty = nullptr,
        .vfs_readdir = nullptr,
        .vfs_readlink = nullptr,
        .vfs_truncate = nullptr,
        .vfs_poll_check = [](File* f, int events) -> int {
            auto* st = static_cast<PipeState*>(f->private_data);
            if (st == nullptr) return 0;
            int ready = 0;
            if ((events & 0x0001) && (st->count > 0 || st->write_closed))  // POLLIN
                ready |= 0x0001;
            if (st->write_closed && st->count == 0)  // POLLHUP
                ready |= 0x0010;
            return ready;
        },
    };

    static FileOperations pipe_write_fops = {
        .vfs_open = nullptr,
        .vfs_close = pipe_close_write,
        .vfs_read = nullptr,
        .vfs_write = pipe_write,
        .vfs_lseek = nullptr,
        .vfs_isatty = nullptr,
        .vfs_readdir = nullptr,
        .vfs_readlink = nullptr,
        .vfs_truncate = nullptr,
        .vfs_poll_check = [](File* f, int events) -> int {
            auto* st = static_cast<PipeState*>(f->private_data);
            if (st == nullptr) return 0;
            int ready = 0;
            if ((events & 0x0004) && (st->count < st->capacity || st->read_closed))  // POLLOUT
                ready |= 0x0004;
            if (st->read_closed)  // POLLERR (broken pipe)
                ready |= 0x0008;
            return ready;
        },
    };

    // Create read-end File
    auto* rf = new File;
    rf->private_data = ps;
    rf->fops = &pipe_read_fops;
    rf->pos = 0;
    rf->is_directory = false;
    rf->fs_type = FSType::TMPFS;  // pseudo-type
    rf->refcount = 1;
    rf->open_flags = 0;  // O_RDONLY
    rf->fd_flags = 0;
    rf->vfs_path = nullptr;
    rf->dir_fs_count = 0;

    // Create write-end File
    auto* wf = new File;
    wf->private_data = ps;
    wf->fops = &pipe_write_fops;
    wf->pos = 0;
    wf->is_directory = false;
    wf->fs_type = FSType::TMPFS;
    wf->refcount = 1;
    wf->open_flags = 1;  // O_WRONLY
    wf->fd_flags = 0;
    wf->vfs_path = nullptr;
    wf->dir_fs_count = 0;

    int rfd = vfs_alloc_fd(task, rf);
    if (rfd < 0) {
        delete rf;
        delete wf;
        delete[] pipe_buf;
        delete ps;
        return -EMFILE;
    }
    int wfd = vfs_alloc_fd(task, wf);
    if (wfd < 0) {
        vfs_release_fd(task, rfd);
        delete rf;
        delete wf;
        delete[] pipe_buf;
        delete ps;
        return -EMFILE;
    }

    pipefd[0] = rfd;
    pipefd[1] = wfd;
    return 0;
}

auto vfs_mount(const char* source, const char* target, const char* fstype) -> int {
    if (target == nullptr) {
        return -EINVAL;
    }

    // Default fstype to "fat32" when not specified (auto-detect for block devices)
    const char* effective_fstype = fstype;
    if (effective_fstype == nullptr || effective_fstype[0] == '\0') {
        effective_fstype = "fat32";
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
                // Walk devfs tree — handles subdirectory paths like wki/block/<name>
                // and triggers WKI proxy attach for remote block devices
                bdev = ker::vfs::devfs::devfs_resolve_block_device(source + 5);
            }
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

    return mount_filesystem(target, effective_fstype, bdev);
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

    // Register and mount procfs for process information
    ker::vfs::procfs::procfs_init();
}

auto vfs_open_file(const char* path, int flags, int mode) -> File* {
    if (path == nullptr) {
        return nullptr;
    }

    // Canonicalize path
    char pathBuffer[MAX_PATH_LEN];  // NOLINT
    size_t path_len = 0;
    while (path[path_len] != '\0' && path_len < MAX_PATH_LEN - 1) {
        pathBuffer[path_len] = path[path_len];
        path_len++;
    }
    pathBuffer[path_len] = '\0';

    canonicalize_path(static_cast<char*>(pathBuffer), MAX_PATH_LEN);

    // Resolve symlinks
    char resolved[MAX_PATH_LEN];  // NOLINT
    int resolve_ret = resolve_symlinks(pathBuffer, resolved, MAX_PATH_LEN);
    if (resolve_ret == 0) {
        std::memcpy(pathBuffer, resolved, MAX_PATH_LEN);
    }

    // Find mount point
    MountPoint* mount = find_mount_point(pathBuffer);
    if (mount == nullptr) {
        return nullptr;
    }

    // Strip mount prefix
    const char* fs_relative_path = pathBuffer;
    size_t mount_len = 0;
    while (mount->path[mount_len] != '\0') {
        mount_len++;
    }

    if (mount_len > 0 && pathBuffer[mount_len - 1] == '/' && mount_len == 1) {
        fs_relative_path = pathBuffer + 1;
    } else if (pathBuffer[mount_len] == '/') {
        fs_relative_path = pathBuffer + mount_len + 1;
    } else if (pathBuffer[mount_len] == '\0') {
        fs_relative_path = "";
    } else {
        fs_relative_path = pathBuffer + mount_len;
    }

    File* f = nullptr;

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
            return nullptr;
    }

    // Store the absolute VFS path for mount-overlay directory listing
    if (f != nullptr) {
        size_t pl = std::strlen(pathBuffer);
        auto* pc = static_cast<char*>(ker::mod::mm::dyn::kmalloc::malloc(pl + 1));
        if (pc != nullptr) {
            std::memcpy(pc, pathBuffer, pl + 1);
            f->vfs_path = pc;
        } else {
            f->vfs_path = nullptr;
        }
        f->dir_fs_count = static_cast<size_t>(-1);
    }

    return f;
}

auto vfs_sendfile(int outfd, int infd, off_t* offset, size_t count) -> ssize_t {
    // Get the current task
    auto* task = mod::sched::get_current_task();
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
