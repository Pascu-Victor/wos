#include "devfs.hpp"

#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <dev/block_device.hpp>
#include <dev/device.hpp>
#include <net/netdevice.hpp>
#include <net/netif.hpp>
#include <net/wki/dev_proxy.hpp>
#include <net/wki/remotable.hpp>
#include <net/wki/wki.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/dbg/journal.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/spinlock.hpp>
#include <string_view>
#include <utility>

#include "net/wki/wire.hpp"
#include "vfs/file.hpp"
#include "vfs/file_operations.hpp"
#include "vfs/vfs.hpp"

// DevFS exposes kernel pseudo-files and WKI resource names through fixed-size
// directory, symlink, and stats buffers. The indexed writes below are bounded
// by local capacity checks and keep this filesystem allocation-light.
// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access, cppcoreguidelines-interfaces-global-init,
// readability-suspicious-call-argument)
namespace ker::vfs::devfs {

namespace {

constexpr uint32_t DEVFS_FILE_MAGIC = 0xDEADBEEF;
constexpr uint64_t NS_PER_SEC = 1000000000ULL;
using log = ker::mod::dbg::logger<"devfs">;
ker::mod::sys::Spinlock g_wki_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

class OptionalWkiLock {
   public:
    explicit OptionalWkiLock(bool enabled) : locked(enabled) {
        if (locked) {
            g_wki_lock.lock();
        }
    }
    OptionalWkiLock(const OptionalWkiLock&) = delete;
    auto operator=(const OptionalWkiLock&) -> OptionalWkiLock& = delete;
    ~OptionalWkiLock() {
        if (locked) {
            g_wki_lock.unlock();
        }
    }

   private:
    bool locked;
};

void wki_release_node_ref(DevFSNode* node);

auto is_wki_devfs_path(const char* path) -> bool {
    if (path == nullptr) {
        return false;
    }
    const char* rel_path = (*path == '/') ? path + 1 : path;
    return std::strncmp(rel_path, "wki", 3) == 0 && (rel_path[3] == '\0' || rel_path[3] == '/');
}

auto is_valid_kernel_pointer(const void* ptr) -> bool {
    auto addr = reinterpret_cast<uintptr_t>(ptr);
    bool const IN_HHDM = (addr >= 0xffff800000000000ULL && addr < 0xffff900000000000ULL);
    bool const IN_KERNEL_STATIC = (addr >= 0xffffffff80000000ULL && addr < 0xffffffffc0000000ULL);
    return IN_HHDM || IN_KERNEL_STATIC;
}

// -- DevFSNode tree root ----------------------------------------------

DevFSNode root_node;

constexpr size_t INITIAL_CHILDREN_CAPACITY = 16;

// -- Tree helpers -----------------------------------------------------

auto current_timespec() -> Timespec {
    uint64_t const NOW_NS = ker::mod::time::get_epoch_ns();
    return Timespec{
        .tv_sec = static_cast<int64_t>(NOW_NS / NS_PER_SEC),
        .tv_nsec = static_cast<int64_t>(NOW_NS % NS_PER_SEC),
    };
}

void stamp_new_node(DevFSNode* node) {
    if (node == nullptr) {
        return;
    }
    Timespec const NOW = current_timespec();
    node->atime = NOW;
    node->mtime = NOW;
    node->ctime = NOW;
}

void touch_node_modified(DevFSNode* node) {
    if (node == nullptr) {
        return;
    }
    Timespec const NOW = current_timespec();
    node->mtime = NOW;
    node->ctime = NOW;
}

void ensure_children_capacity(DevFSNode* node) {
    if (node->children_count < node->children_capacity) {
        return;
    }
    size_t const NEW_CAP = (node->children_capacity == 0) ? INITIAL_CHILDREN_CAPACITY : node->children_capacity * 2;
    auto** new_arr = new DevFSNode*[NEW_CAP];
    if (new_arr == nullptr) {
        return;
    }
    if (node->children != nullptr && node->children_count > 0) {
        std::memcpy(static_cast<void*>(new_arr), static_cast<const void*>(node->children), node->children_count * sizeof(DevFSNode*));
    }
    node->children = new_arr;
    node->children_capacity = NEW_CAP;
}

auto find_child(DevFSNode* dir, const char* name) -> DevFSNode* {
    if (dir == nullptr) {
        return nullptr;
    }
    for (size_t i = 0; i < dir->children_count; i++) {
        if (std::strcmp(dir->children[i]->name.data(), name) == 0) {
            return dir->children[i];
        }
    }
    return nullptr;
}

void add_child(DevFSNode* parent, DevFSNode* child) {
    if (parent == nullptr || child == nullptr) {
        return;
    }
    ensure_children_capacity(parent);
    if (parent->children_count >= parent->children_capacity) {
        return;  // allocation failed
    }
    child->parent = parent;
    parent->children[parent->children_count++] = child;
    touch_node_modified(parent);
}

void remove_child(DevFSNode* parent, DevFSNode* child) {
    if (parent == nullptr || child == nullptr) {
        return;
    }
    for (size_t i = 0; i < parent->children_count; i++) {
        if (parent->children[i] == child) {
            // Shift remaining children left
            for (size_t j = i; j + 1 < parent->children_count; j++) {
                parent->children[j] = parent->children[j + 1];
            }
            parent->children_count--;
            child->parent = nullptr;
            touch_node_modified(parent);
            return;
        }
    }
}

auto create_node(const char* name, DevFSNodeType type) -> DevFSNode* {
    auto* node = new DevFSNode();
    if (node == nullptr) {
        return nullptr;
    }
    size_t len = std::strlen(name);
    if (len >= DEVFS_NAME_MAX) {
        len = DEVFS_NAME_MAX - 1;
    }
    std::memcpy(node->name.data(), name, len);
    node->name[len] = '\0';
    node->type = type;
    // Set default POSIX mode based on node type
    switch (type) {
        case DevFSNodeType::DIRECTORY:
            node->mode = 0755;
            break;
        case DevFSNodeType::DEVICE:
            node->mode = 0666;
            break;
        case DevFSNodeType::SYMLINK:
            node->mode = 0777;
            break;
    }
    stamp_new_node(node);
    return node;
}

// Walk a path relative to /dev root.
// If create_intermediate is true, missing directories are created.
auto walk_path(const char* path, bool create_intermediate = false) -> DevFSNode* {
    if (path == nullptr || path[0] == '\0') {
        return &root_node;
    }

    const char* p = path;
    if (*p == '/') {
        p++;
    }

    DevFSNode* current = &root_node;

    while (*p != '\0') {
        if (*p == '/') {
            p++;
            continue;
        }

        // Extract component
        const char* end = p;
        while (*end != '\0' && *end != '/') {
            end++;
        }
        auto comp_len = static_cast<size_t>(end - p);

        std::array<char, DEVFS_NAME_MAX> component{};
        if (comp_len >= DEVFS_NAME_MAX) {
            comp_len = DEVFS_NAME_MAX - 1;
        }
        std::memcpy(component.data(), p, comp_len);
        component[comp_len] = '\0';

        DevFSNode* child = find_child(current, component.data());
        if (child == nullptr) {
            if (!create_intermediate) {
                return nullptr;
            }
            child = create_node(component.data(), DevFSNodeType::DIRECTORY);
            if (child == nullptr) {
                return nullptr;
            }
            add_child(current, child);
        }

        current = child;
        p = (*end == '/') ? end + 1 : end;
    }

    return current;
}

// -- DevFSFile wrapper (stored in File->private_data) -----------------

struct DevFSFile {
    DevFSNode* node = nullptr;
    ker::dev::Device* device = nullptr;
    uint32_t magic = DEVFS_FILE_MAGIC;
    bool owns_wki_node_ref = false;
};

auto validate_devfs_file(File* f, const char* op) -> DevFSFile* {
    if (f == nullptr || f->private_data == nullptr) {
        return nullptr;
    }

    auto* devfs_file = static_cast<DevFSFile*>(f->private_data);
    if (!is_valid_kernel_pointer(devfs_file)) {
        log::error("%s: private_data %p outside valid kernel range", op, devfs_file);
        return nullptr;
    }

    if (devfs_file->magic != DEVFS_FILE_MAGIC) {
        log::error("%s: private_data %p has invalid magic 0x%x", op, devfs_file, devfs_file->magic);
        return nullptr;
    }

    if (devfs_file->node != nullptr && !is_valid_kernel_pointer(devfs_file->node)) {
        log::error("%s: node pointer %p invalid (df=%p)", op, devfs_file->node, devfs_file);
        return nullptr;
    }

    if (devfs_file->device != nullptr && !is_valid_kernel_pointer(devfs_file->device)) {
        log::error("%s: device pointer %p invalid (df=%p)", op, devfs_file->device, devfs_file);
        return nullptr;
    }

    if (devfs_file->device != nullptr && devfs_file->device->char_ops != nullptr &&
        !is_valid_kernel_pointer(devfs_file->device->char_ops)) {
        log::error("%s: char_ops pointer %p invalid (device=%p df=%p)", op, devfs_file->device->char_ops, devfs_file->device, devfs_file);
        return nullptr;
    }

    return devfs_file;
}

// -- File operations --------------------------------------------------

auto devfs_close(File* f) -> int {
    if (f == nullptr) {
        vfs_debug_log("devfs_close: file is null\n");
        return -EBADF;
    }
    if (f->private_data == nullptr) {
        vfs_debug_log("devfs_close: no private_data\n");
        return 0;
    }

    auto* devfs_file = static_cast<DevFSFile*>(f->private_data);

    // Validate pointer is in valid kernel memory range
    auto df_addr = reinterpret_cast<uintptr_t>(devfs_file);
    bool const IN_HHDM = (df_addr >= 0xffff800000000000ULL && df_addr < 0xffff900000000000ULL);
    bool const IN_KERNEL_STATIC = (df_addr >= 0xffffffff80000000ULL && df_addr < 0xffffffffc0000000ULL);
    if (!IN_HHDM && !IN_KERNEL_STATIC) {
        log::error("close: devfs_file %p out of valid kernel range; skipping delete", devfs_file);
        f->private_data = nullptr;
        return 0;
    }

    if (devfs_file->magic != DEVFS_FILE_MAGIC) {
        log::error("close: devfs_file %p has invalid magic 0x%x; skipping delete", devfs_file, devfs_file->magic);
        f->private_data = nullptr;
        return 0;
    }

    // Call device close if available
    if (devfs_file->device != nullptr && is_valid_kernel_pointer(devfs_file->device) && devfs_file->device->char_ops != nullptr &&
        is_valid_kernel_pointer(devfs_file->device->char_ops) && devfs_file->device->char_ops->close != nullptr) {
        devfs_file->device->char_ops->close(f);
    }

    if (devfs_file->owns_wki_node_ref) {
        wki_release_node_ref(devfs_file->node);
    }

    devfs_file->magic = 0;
    delete devfs_file;
    f->private_data = nullptr;
    return 0;
}

auto devfs_read(File* f, void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* devfs_file = validate_devfs_file(f, "read");
    if (devfs_file == nullptr) {
        return -EBADF;
    }
    if (devfs_file->device != nullptr && devfs_file->device->char_ops != nullptr && devfs_file->device->char_ops->read != nullptr) {
        return devfs_file->device->char_ops->read(f, buf, count);
    }
    return -ENOSYS;
}

auto devfs_write(File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    auto* devfs_file = validate_devfs_file(f, "write");
    if (devfs_file == nullptr) {
        return -EBADF;
    }
    if (devfs_file->device != nullptr && devfs_file->device->char_ops != nullptr && devfs_file->device->char_ops->write != nullptr) {
        return devfs_file->device->char_ops->write(f, buf, count);
    }
    return -ENOSYS;
}

auto devfs_lseek(File* f, off_t offset, int whence) -> off_t {
    auto* devfs_file = validate_devfs_file(f, "lseek");
    if (devfs_file == nullptr) {
        return -EBADF;
    }
    if (devfs_file->device != nullptr && devfs_file->device->char_ops != nullptr && devfs_file->device->char_ops->lseek != nullptr) {
        return devfs_file->device->char_ops->lseek(f, offset, whence);
    }
    return -ESPIPE;
}

auto devfs_isatty(File* f) -> bool {
    auto* devfs_file = validate_devfs_file(f, "isatty");
    if (devfs_file == nullptr) {
        return false;
    }
    if (devfs_file->device != nullptr && devfs_file->device->char_ops != nullptr && devfs_file->device->char_ops->isatty != nullptr) {
        return devfs_file->device->char_ops->isatty(f);
    }
    return false;
}

auto devfs_readdir(File* f, DirEntry* entry, size_t index) -> int {
    if (f == nullptr || entry == nullptr || !f->is_directory) {
        return -EINVAL;
    }
    auto* devfs_file = validate_devfs_file(f, "readdir");
    if (devfs_file == nullptr) {
        return -EBADF;
    }
    OptionalWkiLock const WKI_GUARD(devfs_file->owns_wki_node_ref);
    auto* dir_node = devfs_file->node;
    if (dir_node == nullptr || dir_node->type != DevFSNodeType::DIRECTORY) {
        return -ENOTDIR;
    }

    // Indices 0 and 1 are synthetic "." and ".." entries
    if (index == 0) {
        entry->d_ino = reinterpret_cast<uint64_t>(dir_node);
        entry->d_off = 1;
        entry->d_reclen = sizeof(DirEntry);
        entry->d_type = DT_DIR;
        entry->d_name[0] = '.';
        entry->d_name[1] = '\0';
        return 0;
    }
    if (index == 1) {
        auto* parent = (dir_node->parent != nullptr) ? dir_node->parent : dir_node;
        entry->d_ino = reinterpret_cast<uint64_t>(parent);
        entry->d_off = 2;
        entry->d_reclen = sizeof(DirEntry);
        entry->d_type = DT_DIR;
        entry->d_name[0] = '.';
        entry->d_name[1] = '.';
        entry->d_name[2] = '\0';
        return 0;
    }

    size_t const CHILD_INDEX = index - 2;
    if (CHILD_INDEX >= dir_node->children_count) {
        return -ENOENT;
    }

    auto* child = dir_node->children[CHILD_INDEX];

    entry->d_ino = index + 1;
    entry->d_off = index;
    entry->d_reclen = sizeof(DirEntry);

    switch (child->type) {
        case DevFSNodeType::DIRECTORY:
            entry->d_type = DT_DIR;
            break;
        case DevFSNodeType::DEVICE:
            if (child->device != nullptr && child->device->type == ker::dev::DeviceType::CHAR) {
                entry->d_type = DT_CHR;
            } else {
                entry->d_type = DT_BLK;
            }
            break;
        case DevFSNodeType::SYMLINK:
            entry->d_type = DT_LNK;
            break;
    }

    size_t name_len = std::strlen(child->name.data());
    if (name_len >= DIRENT_NAME_MAX) {
        name_len = DIRENT_NAME_MAX - 1;
    }
    std::memcpy(entry->d_name.data(), child->name.data(), name_len);
    entry->d_name[name_len] = '\0';

    return 0;
}

auto devfs_fops_readlink(File* f, char* buf, size_t bufsize) -> ssize_t {
    if (buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }

    auto* devfs_file = validate_devfs_file(f, "readlink");
    if (devfs_file == nullptr) {
        return -EBADF;
    }
    OptionalWkiLock const WKI_GUARD(devfs_file->owns_wki_node_ref);
    auto* node = devfs_file->node;
    if (node == nullptr || node->type != DevFSNodeType::SYMLINK) {
        return -EINVAL;
    }

    size_t const TARGET_LEN = std::strlen(node->symlink_target.data());
    size_t const COPY_LEN = (TARGET_LEN < bufsize) ? TARGET_LEN : bufsize;
    std::memcpy(buf, node->symlink_target.data(), COPY_LEN);
    return static_cast<ssize_t>(COPY_LEN);
}

auto devfs_poll_check(File* f, int events) -> int {
    auto* devfs_file = validate_devfs_file(f, "poll_check");
    if (devfs_file == nullptr) {
        return events & (0x001 | 0x004);  // EPOLLIN | EPOLLOUT - always ready fallback
    }
    if (devfs_file->device != nullptr && devfs_file->device->char_ops != nullptr && devfs_file->device->char_ops->poll_check != nullptr) {
        return devfs_file->device->char_ops->poll_check(f, events);
    }
    // Default: report requested events as ready (non-blocking device)
    return events & (0x001 | 0x004);  // EPOLLIN | EPOLLOUT
}

auto devfs_poll_register_waiter(File* f, uint64_t pid) -> bool {
    auto* devfs_file = validate_devfs_file(f, "poll_register_waiter");
    if (devfs_file == nullptr) {
        return false;
    }
    if (devfs_file->device == nullptr || devfs_file->device->char_ops == nullptr ||
        devfs_file->device->char_ops->poll_register_waiter == nullptr) {
        return false;
    }
    return devfs_file->device->char_ops->poll_register_waiter(f, pid);
}

auto devfs_poll_wait_kind(File* f) -> ker::mod::sched::task::WaitChannelKind {
    auto* devfs_file = validate_devfs_file(f, "poll_wait_kind");
    if (devfs_file == nullptr || devfs_file->device == nullptr || devfs_file->device->char_ops == nullptr ||
        devfs_file->device->char_ops->poll_wait_kind == nullptr) {
        return ker::mod::sched::task::WaitChannelKind::GENERIC;
    }
    return devfs_file->device->char_ops->poll_wait_kind(f);
}

FileOperations devfs_fops = {
    .vfs_open = nullptr,
    .vfs_close = devfs_close,
    .vfs_read = devfs_read,
    .vfs_write = devfs_write,
    .vfs_lseek = devfs_lseek,
    .vfs_isatty = devfs_isatty,
    .vfs_readdir = devfs_readdir,
    .vfs_readlink = devfs_fops_readlink,
    .vfs_truncate = nullptr,
    .vfs_poll_check = devfs_poll_check,
    .vfs_poll_register_waiter = devfs_poll_register_waiter,
    .vfs_poll_wait_kind = devfs_poll_wait_kind,
    .vfs_ioctl = devfs_ioctl,
};

}  // anonymous namespace

// -- Public interface -------------------------------------------------

auto get_devfs_fops() -> FileOperations* { return &devfs_fops; }

auto devfs_ioctl(File* f, unsigned long cmd, unsigned long arg) -> int {
    auto* devfs_file = validate_devfs_file(f, "ioctl");
    if (devfs_file == nullptr) {
        return -EBADF;
    }
    if (devfs_file->device != nullptr && devfs_file->device->char_ops != nullptr && devfs_file->device->char_ops->ioctl != nullptr) {
        return devfs_file->device->char_ops->ioctl(f, cmd, arg);
    }
    return -ENOTTY;
}

auto DevFSNodeRef::operator=(DevFSNodeRef&& other) noexcept -> DevFSNodeRef& {
    if (this != &other) {
        reset();
        node_ = other.node_;
        owns_wki_node_ref_ = other.owns_wki_node_ref_;
        other.node_ = nullptr;
        other.owns_wki_node_ref_ = false;
    }
    return *this;
}

DevFSNodeRef::~DevFSNodeRef() { reset(); }

void DevFSNodeRef::reset() {
    if (owns_wki_node_ref_) {
        wki_release_node_ref(node_);
    }
    node_ = nullptr;
    owns_wki_node_ref_ = false;
}

auto devfs_acquire_path(const char* path) -> DevFSNodeRef {
    bool const WKI_PATH = is_wki_devfs_path(path);
    OptionalWkiLock const WKI_GUARD(WKI_PATH);
    DevFSNode* node = walk_path(path, false);
    if (node != nullptr && WKI_PATH) {
        node->wki_open_refs++;
    }
    return DevFSNodeRef(node, node != nullptr && WKI_PATH);
}

auto devfs_file_node(File* file) -> DevFSNode* {
    auto* devfs_file = validate_devfs_file(file, "file_node");
    return (devfs_file != nullptr) ? devfs_file->node : nullptr;
}

auto devfs_open_path(const char* path, int flags, int /*mode*/) -> File* {
    if (path == nullptr) {
        return nullptr;
    }

    // Determine path relative to /dev root
    const char* rel_path = path;
    if (path[0] == '/' && path[1] == 'd' && path[2] == 'e' && path[3] == 'v') {
        if (path[4] == '\0' || (path[4] == '/' && path[5] == '\0')) {
            rel_path = "";  // /dev itself
        } else if (path[4] == '/') {
            rel_path = path + 5;  // strip "/dev/"
        }
    }

    // Allocate wrappers before taking the WKI tree lock; allocator paths must
    // not run under this IRQ-safe mutation lock.
    auto* file = new File();
    if (file == nullptr) {
        return nullptr;
    }

    auto* devfs_file = new DevFSFile();
    if (devfs_file == nullptr) {
        delete file;
        return nullptr;
    }

    bool const WKI_PATH = is_wki_devfs_path(rel_path);
    DevFSNode* node = nullptr;
    {
        OptionalWkiLock const WKI_GUARD(WKI_PATH);
        node = walk_path(rel_path);
        if (node != nullptr && WKI_PATH) {
            node->wki_open_refs++;
            devfs_file->owns_wki_node_ref = true;
        }
    }
    if (node == nullptr) {
        delete devfs_file;
        delete file;
        vfs_debug_log("devfs: path not found: ");
        vfs_debug_log(path);
        vfs_debug_log("\n");
        return nullptr;
    }
    devfs_file->node = node;

    file->fd = -1;
    file->fops = &devfs_fops;
    file->pos = 0;
    file->fs_type = FSType::DEVFS;
    file->refcount = 1;
    file->open_flags = flags;
    file->fd_flags = 0;

    if (node->type == DevFSNodeType::DIRECTORY) {
        file->is_directory = true;
        file->private_data = devfs_file;
        vfs_debug_log("devfs: opened directory\n");
    } else if (node->type == DevFSNodeType::DEVICE) {
        file->is_directory = false;
        devfs_file->device = node->device;
        file->private_data = devfs_file;

        // Call device open if char device with open handler
        if (node->device != nullptr && node->device->char_ops != nullptr && node->device->char_ops->open != nullptr) {
            if (node->device->char_ops->open(file) != 0) {
                if (devfs_file->owns_wki_node_ref) {
                    wki_release_node_ref(node);
                }
                delete devfs_file;
                delete file;
                return nullptr;
            }
        }

        vfs_debug_log("devfs: opened device: ");
        vfs_debug_log(node->name.data());
        vfs_debug_log("\n");
    } else if (node->type == DevFSNodeType::SYMLINK) {
        // Return a File representing the symlink (for readlink)
        file->is_directory = false;
        file->private_data = devfs_file;
        vfs_debug_log("devfs: opened symlink: ");
        vfs_debug_log(node->name.data());
        vfs_debug_log("\n");
    } else {
        if (devfs_file->owns_wki_node_ref) {
            wki_release_node_ref(node);
        }
        delete devfs_file;
        delete file;
        return nullptr;
    }

    return file;
}

auto devfs_create_directory(const char* path) -> DevFSNode* { return walk_path(path, true); }

auto devfs_create_symlink(const char* path, const char* target) -> DevFSNode* {
    if (path == nullptr || target == nullptr) {
        return nullptr;
    }

    // Split path into parent directory + leaf name
    const char* last_slash = nullptr;
    for (const char* p = path; *p != '\0'; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    DevFSNode* parent = &root_node;
    const char* symlink_name = path;

    if (last_slash != nullptr) {
        auto parent_len = static_cast<size_t>(last_slash - path);
        std::array<char, DEVFS_NAME_MAX> parent_path{};
        if (parent_len >= DEVFS_NAME_MAX) {
            parent_len = DEVFS_NAME_MAX - 1;
        }
        std::memcpy(parent_path.data(), path, parent_len);
        parent_path[parent_len] = '\0';

        parent = walk_path(parent_path.data(), true);
        if (parent == nullptr) {
            return nullptr;
        }
        symlink_name = last_slash + 1;
    }

    auto* node = create_node(symlink_name, DevFSNodeType::SYMLINK);
    if (node == nullptr) {
        return nullptr;
    }

    size_t target_len = std::strlen(target);
    if (target_len >= DEVFS_SYMLINK_MAX) {
        target_len = DEVFS_SYMLINK_MAX - 1;
    }
    std::memcpy(node->symlink_target.data(), target, target_len);
    node->symlink_target[target_len] = '\0';

    add_child(parent, node);

    vfs_debug_log("devfs: created symlink ");
    vfs_debug_log(path);
    vfs_debug_log(" -> ");
    vfs_debug_log(target);
    vfs_debug_log("\n");

    return node;
}

auto devfs_add_device_node(const char* name, ker::dev::Device* dev) -> DevFSNode* {
    if (name == nullptr) {
        return nullptr;
    }

    std::array<char, DEVFS_NAME_MAX> name_buf{};
    size_t name_len = std::strlen(name);
    if (name_len >= name_buf.size()) {
        name_len = name_buf.size() - 1;
    }
    std::memcpy(name_buf.data(), name, name_len);
    name_buf[name_len] = '\0';  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    return devfs_add_device_node(name_buf, dev);
}

auto devfs_add_device_node(const std::array<char, DEVFS_NAME_MAX>& name, ker::dev::Device* dev) -> DevFSNode* {
    // If the name contains '/', walk to the parent directory first.
    // e.g. "pts/0" => find (or create) the "pts" dir, then add node "0" there.
    const char* slash = nullptr;
    for (const char* p = name.data(); *p != '\0'; p++) {
        if (*p == '/') {
            slash = p;
        }
    }

    DevFSNode* parent = &root_node;
    const char* leaf_name = name.data();

    if (slash != nullptr) {
        // Extract the directory portion and walk/create it
        auto dir_len = static_cast<size_t>(slash - name.data());
        std::array<char, DEVFS_NAME_MAX> dir_path{};
        if (dir_len >= DEVFS_NAME_MAX) {
            dir_len = DEVFS_NAME_MAX - 1;
        }
        std::memcpy(dir_path.data(), name.data(), dir_len);
        dir_path[dir_len] = '\0';

        parent = walk_path(dir_path.data(), true);  // create intermediate dirs if needed
        if (parent == nullptr) {
            return nullptr;
        }
        leaf_name = slash + 1;
    }

    if (auto* existing = find_child(parent, leaf_name); existing != nullptr) {
        existing->type = DevFSNodeType::DEVICE;
        existing->device = dev;
        existing->mode = 0666;
        touch_node_modified(existing);
        return existing;
    }

    auto* node = create_node(leaf_name, DevFSNodeType::DEVICE);
    if (node == nullptr) {
        return nullptr;
    }
    node->device = dev;
    add_child(parent, node);
    return node;
}

auto devfs_remove_node(const char* path) -> bool {
    auto* node = walk_path(path, false);
    if (node == nullptr || node == &root_node || node->parent == nullptr) {
        return false;
    }
    if (node->type == DevFSNodeType::DIRECTORY && node->children_count != 0) {
        return false;
    }

    remove_child(node->parent, node);
    delete node;
    return true;
}

void devfs_populate_partition_symlinks() {
    // Create /dev/disk/by-partuuid/ directory hierarchy
    walk_path("disk/by-partuuid", true);

    size_t const COUNT = ker::dev::block_device_count();
    for (size_t i = 0; i < COUNT; i++) {
        auto* bdev = ker::dev::block_device_at(i);
        if (bdev == nullptr) {
            continue;
        }

        // Add block device node at /dev root (e.g. sda, sda1)
        if (find_child(&root_node, bdev->name.data()) == nullptr) {
            auto* dnode = create_node(bdev->name.data(), DevFSNodeType::DEVICE);
            if (dnode != nullptr) {
                dnode->device = nullptr;  // block devices have no char_ops
                add_child(&root_node, dnode);
            }
        }

        // Create PARTUUID symlink for partition devices
        if (bdev->is_partition && bdev->partuuid_str[0] != '\0') {
            // symlink path: disk/by-partuuid/<UUID>
            std::array<char, DEVFS_SYMLINK_MAX> symlink_path{};
            const char* prefix = "disk/by-partuuid/";
            size_t const PREFIX_LEN = std::strlen(prefix);
            std::memcpy(symlink_path.data(), prefix, PREFIX_LEN);
            size_t const UUID_LEN = std::strlen(bdev->partuuid_str.data());
            std::memcpy(symlink_path.data() + PREFIX_LEN, bdev->partuuid_str.data(), UUID_LEN);
            symlink_path[PREFIX_LEN + UUID_LEN] = '\0';

            // target: /dev/<name>
            std::array<char, DEVFS_SYMLINK_MAX> target_path{};
            const char* dev_prefix = "/dev/";
            size_t const DEV_PREFIX_LEN = std::strlen(dev_prefix);
            std::memcpy(target_path.data(), dev_prefix, DEV_PREFIX_LEN);
            size_t const NAME_LEN = std::strlen(bdev->name.data());
            std::memcpy(target_path.data() + DEV_PREFIX_LEN, bdev->name.data(), NAME_LEN);
            target_path[DEV_PREFIX_LEN + NAME_LEN] = '\0';

            devfs_create_symlink(symlink_path.data(), target_path.data());
        }
    }

    vfs_debug_log("devfs: partition symlinks populated\n");
}

void register_devfs() { vfs_debug_log("devfs: registered\n"); }

void devfs_init() {
    vfs_debug_log("devfs: initializing\n");

    // Initialize root node
    root_node.name[0] = '\0';
    root_node.type = DevFSNodeType::DIRECTORY;
    stamp_new_node(&root_node);

    // Populate with registered character devices
    size_t const DEV_COUNT = ker::dev::dev_get_count();
    for (size_t i = 0; i < DEV_COUNT; i++) {
        auto* dev = ker::dev::dev_get_at_index(i);
        if (dev != nullptr) {
            devfs_add_device_node(dev->name, dev);
        }
    }

    devfs_create_symlink("fd", "/proc/self/fd");
    devfs_create_symlink("stdin", "/proc/self/fd/0");
    devfs_create_symlink("stdout", "/proc/self/fd/1");
    devfs_create_symlink("stderr", "/proc/self/fd/2");

    register_devfs();
    ker::mod::dbg::journal::mark_devfs_ready();
    vfs_debug_log("devfs: initialized with ");
    vfs_debug_log_hex(root_node.children_count);
    vfs_debug_log(" device nodes\n");
}

void devfs_populate_net_nodes() {
    // Create /dev/net/ directory
    walk_path("net", true);
    auto* net_dir = walk_path("net");
    if (net_dir == nullptr) {
        return;
    }

    size_t const COUNT = ker::net::netdev_count();
    for (size_t i = 0; i < COUNT; i++) {
        auto* netdev = ker::net::netdev_at(i);
        if (netdev == nullptr) {
            continue;
        }

        // Skip if node already exists
        if (find_child(net_dir, netdev->name.data()) != nullptr) {
            continue;
        }

        // Create device node (not a real char device, just stores netdev pointer)
        auto* node = create_node(netdev->name.data(), DevFSNodeType::DEVICE);
        if (node == nullptr) {
            continue;
        }

        // Allocate a Device struct to hold the netdev pointer
        auto* dev = new ker::dev::Device();
        if (dev == nullptr) {
            delete node;
            continue;
        }

        // Net stats read handler
        static ker::dev::CharDeviceOps net_stats_ops = {
            .open = nullptr,
            .close = nullptr,
            .read = [](ker::vfs::File* f, void* buf, size_t count) -> ssize_t {
                if (f == nullptr || f->private_data == nullptr || buf == nullptr) {
                    return -EINVAL;
                }
                auto* df = static_cast<DevFSFile*>(f->private_data);
                if (df->device == nullptr || df->device->private_data == nullptr) {
                    return -EINVAL;
                }
                auto* nd = static_cast<ker::net::NetDevice*>(df->device->private_data);

                // Format stats into a temporary buffer
                std::array<char, 512> stats{};
                size_t pos = 0;

                // Helper to append string
                auto append_str = [&](const char* s) {
                    while (*s != '\0' && pos < stats.size() - 1) {
                        stats[pos++] = *s++;
                    }
                };
                // Helper to append uint64 as decimal
                auto append_u64 = [&](uint64_t val) {
                    if (val == 0) {
                        if (pos < stats.size() - 1) {
                            stats[pos++] = '0';
                        }
                        return;
                    }
                    std::array<char, 21> tmp{};
                    int ti = 0;
                    while (val > 0) {
                        tmp[ti++] = '0' + static_cast<char>(val % 10);
                        val /= 10;
                    }
                    for (int j = ti - 1; j >= 0 && pos < stats.size() - 1; j--) {
                        stats[pos++] = tmp[j];
                    }
                };
                // Helper to append hex byte
                auto append_hex_byte = [&](uint8_t b) {
                    constexpr std::string_view HEX{"0123456789abcdef"};
                    if (pos < stats.size() - 2) {
                        stats[pos++] = HEX[b >> 4];
                        stats[pos++] = HEX[b & 0xF];
                    }
                };

                append_str("name: ");
                append_str(nd->name.data());
                append_str("\nstate: ");
                append_str(nd->state != 0 ? "up" : "down");
                append_str("\nmac: ");
                for (int m = 0; m < 6; m++) {
                    if (m > 0) {
                        append_str(":");
                    }
                    append_hex_byte(nd->mac[m]);
                }
                append_str("\nmtu: ");
                append_u64(nd->mtu);

                // Show IPv4 addresses if configured
                auto* nif = ker::net::netif_find_by_dev(nd);
                if (nif != nullptr && nif->ipv4_addr_count > 0) {
                    append_str("\nipv4: ");
                    uint32_t const ADDR = nif->ipv4_addrs[0].addr;
                    append_u64((ADDR >> 24) & 0xFF);
                    append_str(".");
                    append_u64((ADDR >> 16) & 0xFF);
                    append_str(".");
                    append_u64((ADDR >> 8) & 0xFF);
                    append_str(".");
                    append_u64(ADDR & 0xFF);
                    append_str("/");
                    // Count prefix length from netmask
                    uint32_t const MASK = nif->ipv4_addrs[0].netmask;
                    int prefix = 0;
                    for (int b = 31; b >= 0; b--) {
                        if ((MASK >> b) & 1) {
                            prefix++;
                        } else {
                            break;
                        }
                    }
                    append_u64(static_cast<uint64_t>(prefix));
                }

                append_str("\nrx_packets: ");
                append_u64(nd->rx_packets);
                append_str("\ntx_packets: ");
                append_u64(nd->tx_packets);
                append_str("\nrx_bytes: ");
                append_u64(nd->rx_bytes);
                append_str("\ntx_bytes: ");
                append_u64(nd->tx_bytes);
                append_str("\n");
                stats[pos] = '\0';

                // Apply file position offset
                auto offset = static_cast<size_t>(f->pos);
                if (offset >= pos) {
                    return 0;  // EOF
                }
                size_t const AVAILABLE = pos - offset;
                size_t const TO_COPY = (count < AVAILABLE) ? count : AVAILABLE;
                std::memcpy(buf, stats.data() + offset, TO_COPY);
                f->pos += static_cast<off_t>(TO_COPY);
                return static_cast<ssize_t>(TO_COPY);
            },
            .write = nullptr,
            .isatty = nullptr,
            .ioctl = nullptr,
            .poll_check = nullptr,
            .poll_register_waiter = nullptr,
        };

        dev->major = 10;
        dev->minor = static_cast<unsigned>(200 + i);
        dev->name = netdev->name.data();
        dev->type = ker::dev::DeviceType::CHAR;
        dev->private_data = netdev;
        dev->char_ops = &net_stats_ops;

        node->device = dev;
        add_child(net_dir, node);
    }

    vfs_debug_log("devfs: net nodes populated (");
    vfs_debug_log_hex(COUNT);
    vfs_debug_log(" devices)\n");
}

// -----------------------------------------------------------------------------
// WKI remotable resource nodes - /dev/wki/
// -----------------------------------------------------------------------------

namespace {

// Protects all g_wki_* state and DevFSNode tree mutations in devfs_wki_* functions.
// Multiple CPUs run backlog handler threads concurrently; devfs_wki_* are called
// after s_remotable_lock is dropped, so they need their own lock.
struct WkiAutoLock {
    WkiAutoLock() { g_wki_lock.lock(); }
    ~WkiAutoLock() { g_wki_lock.unlock(); }
};

// Persistent directory pointers (lazily initialized)
DevFSNode* g_wki_dir = nullptr;                 // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
DevFSNode* g_wki_by_zone = nullptr;             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
DevFSNode* g_wki_by_peer = nullptr;             // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
DevFSNode* g_wki_by_peer_id = nullptr;          // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
DevFSNode* g_wki_by_peer_host = nullptr;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
std::array<uint32_t, 7> g_wki_type_counters{};  // indexed by ResourceType (1-6), slot 0 unused // NOLINT
size_t g_wki_total = 0;                         // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

// ResourceType => directory name
auto wki_type_dir(ker::net::wki::ResourceType type) -> const char* {
    using RT = ker::net::wki::ResourceType;
    switch (type) {
        case RT::BLOCK:
            return "block";
        case RT::CHAR:
            return "char";
        case RT::NET:
            return "net";
        case RT::VFS:
            return "vfs";
        case RT::COMPUTE:
            return "compute";
        case RT::CUSTOM:
            return "custom";
        default:
            mod::dbg::logger<"devfs_wki">::error("invalid WKI resource type: %d", static_cast<int>(type));
            break;
    }
    return "unknown";
}

// ResourceType => device name prefix
auto wki_type_prefix(ker::net::wki::ResourceType type) -> const char* {
    using RT = ker::net::wki::ResourceType;
    switch (type) {
        case RT::BLOCK:
            return "blk";
        case RT::CHAR:
            return "chr";
        case RT::NET:
            return "eth";
        case RT::VFS:
            return "vfs";
        case RT::COMPUTE:
            return "cmp";
        case RT::CUSTOM:
            return "cst";
        default:
            mod::dbg::logger<"devfs_wki">::error("invalid WKI resource type: %d", static_cast<int>(type));
            break;
    }
    return "unk";
}

auto wki_type_name(ker::net::wki::ResourceType type) -> const char* {
    using RT = ker::net::wki::ResourceType;
    switch (type) {
        case RT::BLOCK:
            return "block";
        case RT::CHAR:
            return "char";
        case RT::NET:
            return "net";
        case RT::VFS:
            return "vfs";
        case RT::COMPUTE:
            return "compute";
        case RT::CUSTOM:
            return "custom";
        default:
            mod::dbg::logger<"devfs_wki">::error("invalid WKI resource type: %d", static_cast<int>(type));
            break;
    }
    return "unknown";
}

// Per-resource context stored in Device::private_data
struct WkiDevfsCtx {
    ker::net::wki::ResourceType resource_type{ker::net::wki::ResourceType::CUSTOM};
    uint16_t peer_node_id{};
    uint16_t rdma_zone_id{};
    uint32_t resource_id{};
    uint64_t resource_generation{};
    uint8_t flags{};
    std::array<char, 64> remote_name{};
    std::array<char, 64> dev_name{};  // persistent name backing Device::name
    std::array<char, 64> display_name{};
    std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> peer_hostname{};
};

void wki_destroy_unlinked_node(DevFSNode* node) {
    if (node == nullptr) {
        return;
    }
    if (node->device != nullptr) {
        delete static_cast<WkiDevfsCtx*>(node->device->private_data);
        delete node->device;
        node->device = nullptr;
    }
    delete node;
}

void wki_unlink_node(DevFSNode* parent, DevFSNode* node) {
    if (node == nullptr) {
        return;
    }
    remove_child(parent, node);
    node->wki_unlinked = true;
    if (node->wki_open_refs == 0) {
        wki_destroy_unlinked_node(node);
    }
}

void wki_release_node_ref(DevFSNode* node) {
    if (node == nullptr) {
        return;
    }

    WkiAutoLock const GUARD;
    if (node->wki_open_refs == 0) {
        ker::mod::dbg::panic_handler("devfs: unbalanced WKI node reference");
        return;
    }
    node->wki_open_refs--;
    if (node->wki_open_refs == 0 && node->wki_unlinked) {
        wki_destroy_unlinked_node(node);
    }
}

// Read handler for WKI device nodes
ker::dev::CharDeviceOps wki_resource_ops = {
    .open = nullptr,
    .close = nullptr,
    .read = [](ker::vfs::File* f, void* buf, size_t count) -> ssize_t {
        if (f == nullptr || f->private_data == nullptr || buf == nullptr) {
            return -EINVAL;
        }
        WkiAutoLock const GUARD;
        auto* df = static_cast<DevFSFile*>(f->private_data);
        if (df->device == nullptr || df->device->private_data == nullptr) {
            return -EINVAL;
        }
        auto* ctx = static_cast<WkiDevfsCtx*>(df->device->private_data);

        std::array<char, 512> stats{};
        size_t pos = 0;

        auto append_str = [&](const char* s) -> void {
            while (*s != '\0' && pos < stats.size() - 1) {
                stats[pos++] = *s++;
            }
        };
        auto append_u64 = [&](uint64_t val) {
            if (val == 0) {
                if (pos < stats.size() - 1) {
                    stats[pos++] = '0';
                }
                return;
            }
            std::array<char, 21> tmp{};
            int ti = 0;
            while (val > 0) {
                tmp[ti++] = '0' + static_cast<char>(val % 10);
                val /= 10;
            }
            for (int j = ti - 1; j >= 0 && pos < stats.size() - 1; j--) {
                stats[pos++] = tmp[j];
            }
        };

        append_str("type: ");
        append_str(wki_type_name(ctx->resource_type));
        append_str("\npeer: ");
        append_u64(ctx->peer_node_id);
        append_str("\nzone: ");
        append_u64(ctx->rdma_zone_id);
        append_str("\nresource_id: ");
        append_u64(ctx->resource_id);
        append_str("\nflags:");
        if (ctx->flags & ker::net::wki::RESOURCE_FLAG_SHAREABLE) {
            append_str(" shareable");
        }
        if (ctx->flags & ker::net::wki::RESOURCE_FLAG_PASSTHROUGH_CAPABLE) {
            append_str(" passthrough");
        }
        if (ctx->flags & ker::net::wki::RESOURCE_FLAG_READABLE) {
            append_str(" readable");
        }
        if (ctx->flags & ker::net::wki::RESOURCE_FLAG_WRITABLE) {
            append_str(" writable");
        }
        if (ctx->flags & ker::net::wki::RESOURCE_FLAG_OCCUPIED) {
            append_str(" occupied");
        }
        if (ctx->flags == 0) {
            append_str(" none");
        }
        append_str("\nremote_name: ");
        append_str(ctx->remote_name.data());
        append_str("\n");
        stats[pos] = '\0';

        auto offset = static_cast<size_t>(f->pos);
        if (offset >= pos) {
            return 0;
        }
        size_t const AVAILABLE = pos - offset;
        size_t const TO_COPY = (count < AVAILABLE) ? count : AVAILABLE;
        std::memcpy(buf, stats.data() + offset, TO_COPY);
        f->pos += static_cast<off_t>(TO_COPY);
        return static_cast<ssize_t>(TO_COPY);
    },
    .write = nullptr,
    .isatty = nullptr,
    .ioctl = nullptr,
    .poll_check = nullptr,
    .poll_register_waiter = nullptr,
};

// Format a uint32 into a char buffer, returns bytes written
auto fmt_u32(char* buf, size_t cap, uint32_t val) -> size_t {
    if (val == 0) {
        if (cap > 0) {
            buf[0] = '0';
        }
        return 1;
    }
    std::array<char, 11> tmp{};
    int ti = 0;
    while (val > 0) {
        tmp[ti++] = '0' + static_cast<char>(val % 10);
        val /= 10;
    }
    size_t w = 0;
    for (int j = ti - 1; j >= 0 && w < cap; j--) {
        buf[w++] = tmp[j];
    }
    return w;
}

// Format a uint16 as 4-character lowercase hex into buf, returns 4
auto fmt_u16_hex4(char* buf, size_t cap, uint16_t val) -> size_t {
    constexpr std::string_view HEX{"0123456789abcdef"};
    if (cap < 4) {
        return 0;
    }
    buf[0] = HEX[(val >> 12) & 0xF];
    buf[1] = HEX[(val >> 8) & 0xF];
    buf[2] = HEX[(val >> 4) & 0xF];
    buf[3] = HEX[val & 0xF];
    return 4;
}

// Build a WKI device name into buf: rz{zone_hex4}p{peer_hex4}{prefix}{counter}
void wki_build_dev_name(char* buf, size_t cap, uint16_t zone_id, uint16_t peer_id, ker::net::wki::ResourceType type, uint32_t local_num) {
    size_t p = 0;
    auto app_str = [&](const char* s) {
        while (*s != '\0' && p < cap - 1) {
            buf[p++] = *s++;
        }
    };
    auto app_num = [&](uint32_t val) { p += fmt_u32(buf + p, cap - 1 - p, val); };
    auto app_hex4 = [&](uint16_t val) { p += fmt_u16_hex4(buf + p, cap - 1 - p, val); };
    app_str("rz");
    app_hex4(zone_id);
    app_str("p");
    app_hex4(peer_id);
    app_str(wki_type_prefix(type));
    app_num(local_num);
    buf[p] = '\0';
}

void wki_build_display_name(char* buf, size_t cap, ker::net::wki::ResourceType type, uint32_t local_num) {
    if (buf == nullptr || cap == 0) {
        return;
    }
    std::snprintf(buf, cap, "%s%u", wki_type_prefix(type), local_num);
}

void wki_copy_peer_hostname(char* buf, size_t cap, uint16_t peer_id) {
    if (buf == nullptr || cap == 0) {
        return;
    }

    const char* hostname = ker::net::wki::wki_peer_get_hostname(peer_id);
    if (hostname != nullptr && hostname[0] != '\0') {
        std::snprintf(buf, cap, "%s", hostname);
    } else {
        std::snprintf(buf, cap, "node-%04x", peer_id);
    }
}

// Ensure the /dev/wki/ directory hierarchy exists. Returns false on failure.
auto wki_ensure_dirs() -> bool {
    if (g_wki_dir == nullptr) {
        g_wki_dir = walk_path("wki", true);
    }
    if (g_wki_dir == nullptr) {
        return false;
    }
    if (g_wki_by_zone == nullptr) {
        g_wki_by_zone = walk_path("wki/by-zone", true);
    }
    if (g_wki_by_zone == nullptr) {
        return false;
    }
    if (g_wki_by_peer == nullptr) {
        g_wki_by_peer = walk_path("wki/by-peer", true);
    }
    if (g_wki_by_peer == nullptr) {
        return false;
    }
    if (g_wki_by_peer_id == nullptr) {
        g_wki_by_peer_id = walk_path("wki/by-peer-id", true);
    }
    if (g_wki_by_peer_id == nullptr) {
        return false;
    }
    if (g_wki_by_peer_host == nullptr) {
        g_wki_by_peer_host = walk_path("wki/by-peer-host", true);
    }
    return g_wki_by_peer_host != nullptr;
}

// Add a symlink named `name` under `parent_dir`, pointing to `target`.
void wki_add_symlink(DevFSNode* parent_dir, std::string_view name, std::string_view target) {
    if (parent_dir == nullptr) {
        return;
    }

    std::array<char, DEVFS_NAME_MAX> name_buf{};
    size_t const NLEN = (name.size() < DEVFS_NAME_MAX - 1) ? name.size() : DEVFS_NAME_MAX - 1;
    std::memcpy(name_buf.data(), name.data(), NLEN);
    name_buf[NLEN] = '\0';

    if (auto* existing = find_child(parent_dir, name_buf.data()); existing != nullptr) {
        if (existing->type != DevFSNodeType::SYMLINK) {
            return;
        }
        size_t const TLEN = (target.size() < DEVFS_SYMLINK_MAX - 1) ? target.size() : DEVFS_SYMLINK_MAX - 1;
        std::memcpy(existing->symlink_target.data(), target.data(), TLEN);
        existing->symlink_target[TLEN] = '\0';
        touch_node_modified(existing);
        return;
    }

    auto* link = create_node(name_buf.data(), DevFSNodeType::SYMLINK);
    if (link == nullptr) {
        return;
    }
    size_t const TLEN = (target.size() < DEVFS_SYMLINK_MAX - 1) ? target.size() : DEVFS_SYMLINK_MAX - 1;
    std::memcpy(link->symlink_target.data(), target.data(), TLEN);
    link->symlink_target[TLEN] = '\0';
    add_child(parent_dir, link);
}

// Find or create a hex-named subdirectory (e.g. "0000", "0003") under parent.
auto wki_ensure_hex_subdir(DevFSNode* parent, uint16_t num) -> DevFSNode* {
    std::array<char, 8> name{};
    fmt_u16_hex4(name.data(), name.size() - 1, num);
    name[4] = '\0';
    DevFSNode* sub = find_child(parent, name.data());
    if (sub == nullptr) {
        sub = create_node(name.data(), DevFSNodeType::DIRECTORY);
        if (sub != nullptr) {
            add_child(parent, sub);
        }
    }
    return sub;
}

// Find or create a named subdirectory under parent.
auto wki_ensure_named_subdir(DevFSNode* parent, const char* name) -> DevFSNode* {
    if (parent == nullptr || name == nullptr || name[0] == '\0') {
        return nullptr;
    }

    DevFSNode* sub = find_child(parent, name);
    if (sub == nullptr) {
        sub = create_node(name, DevFSNodeType::DIRECTORY);
        if (sub != nullptr) {
            add_child(parent, sub);
        }
    }
    return sub;
}

// Remove only the alias still owned by the resource whose target we captured.
// BLOCK remote names are not globally unique (for example, multiple peers may
// advertise "vda" in one zone), so name-only removal can unlink another
// resource's first-writer-wins alias.
void wki_remove_named_symlink_if_target(DevFSNode* dir, const char* name, std::string_view expected_target) {
    if (dir == nullptr || name == nullptr) {
        return;
    }
    DevFSNode* child = find_child(dir, name);
    if (child == nullptr || child->type != DevFSNodeType::SYMLINK || std::string_view{child->symlink_target.data()} != expected_target) {
        return;
    }
    wki_unlink_node(dir, child);
}

void wki_remove_named_symlink_if_distinct(DevFSNode* dir, const char* preferred_name, const char* fallback_name,
                                          std::string_view expected_target) {
    wki_remove_named_symlink_if_target(dir, preferred_name, expected_target);
    if (std::strcmp(preferred_name, fallback_name) != 0) {
        wki_remove_named_symlink_if_target(dir, fallback_name, expected_target);
    }
}

void wki_add_symlink_if_absent(DevFSNode* parent_dir, std::string_view name, std::string_view target) {
    if (parent_dir == nullptr || name.empty()) {
        return;
    }

    std::array<char, DEVFS_NAME_MAX> name_buf{};
    size_t const NLEN = (name.size() < DEVFS_NAME_MAX - 1) ? name.size() : DEVFS_NAME_MAX - 1;
    std::memcpy(name_buf.data(), name.data(), NLEN);
    name_buf[NLEN] = '\0';

    if (find_child(parent_dir, name_buf.data()) != nullptr) {
        return;
    }

    wki_add_symlink(parent_dir, name, target);
}

auto wki_block_remote_alias(const WkiDevfsCtx* ctx) -> const char* {
    if (ctx == nullptr || ctx->resource_type != ker::net::wki::ResourceType::BLOCK || ctx->remote_name[0] == '\0') {
        return nullptr;
    }
    return ctx->remote_name.data();
}

void wki_add_block_remote_alias(DevFSNode* dir, const WkiDevfsCtx* ctx, std::string_view target) {
    const char* alias = wki_block_remote_alias(ctx);
    if (alias == nullptr) {
        return;
    }
    if (std::strcmp(alias, ctx->display_name.data()) == 0 || std::strcmp(alias, ctx->dev_name.data()) == 0) {
        return;
    }

    wki_add_symlink_if_absent(dir, alias, target);
}

void wki_build_resource_symlink_target(const char* type_dir_name, const WkiDevfsCtx* ctx, std::array<char, DEVFS_SYMLINK_MAX>* target) {
    if (type_dir_name == nullptr || ctx == nullptr || target == nullptr) {
        return;
    }

    size_t pos = 0;
    auto append = [&](std::string_view text) {
        size_t const LEN = std::min(text.size(), target->size() - 1 - pos);
        std::memcpy(target->data() + pos, text.data(), LEN);
        pos += LEN;
    };
    append("/dev/wki/");
    append(type_dir_name);
    append("/");
    append(ctx->dev_name.data());
    target->at(pos) = '\0';
}

void wki_remove_wki_resource_aliases(DevFSNode* dir, const WkiDevfsCtx* ctx, std::string_view expected_target) {
    wki_remove_named_symlink_if_distinct(dir, ctx->display_name.data(), ctx->dev_name.data(), expected_target);

    const char* alias = wki_block_remote_alias(ctx);
    if (alias == nullptr) {
        return;
    }
    if (std::strcmp(alias, ctx->display_name.data()) == 0 || std::strcmp(alias, ctx->dev_name.data()) == 0) {
        return;
    }
    wki_remove_named_symlink_if_target(dir, alias, expected_target);
}

// Find the WKI device node matching (node_id, type, resource_id) in a type directory.
// Returns the node, or nullptr if not found.
auto wki_find_device_in_type_dir(DevFSNode* type_dir, uint16_t node_id, ker::net::wki::ResourceType res_type, uint32_t resource_id,
                                 const DevFSNode* exclude = nullptr) -> DevFSNode* {
    if (type_dir == nullptr) {
        return nullptr;
    }
    for (size_t i = 0; i < type_dir->children_count; i++) {
        auto* child = type_dir->children[i];
        if (child == exclude) {
            continue;
        }
        if (child->type != DevFSNodeType::DEVICE || child->device == nullptr) {
            continue;
        }
        auto* ctx = static_cast<WkiDevfsCtx*>(child->device->private_data);
        if (ctx == nullptr) {
            continue;
        }
        if (ctx->peer_node_id == node_id && ctx->resource_type == res_type && ctx->resource_id == resource_id) {
            return child;
        }
    }
    return nullptr;
}

void wki_remove_resource_symlinks(const WkiDevfsCtx* ctx) {
    if (ctx == nullptr) {
        return;
    }

    std::array<char, DEVFS_SYMLINK_MAX> target{};
    wki_build_resource_symlink_target(wki_type_dir(ctx->resource_type), ctx, &target);
    std::string_view const TARGET{target.data()};

    // Remove symlink from by-zone subdirectory
    {
        std::array<char, 8> zone_str{};
        fmt_u16_hex4(zone_str.data(), zone_str.size() - 1, ctx->rdma_zone_id);
        zone_str[4] = '\0';
        DevFSNode* zone_sub = find_child(g_wki_by_zone, zone_str.data());
        if (zone_sub != nullptr) {
            wki_remove_wki_resource_aliases(zone_sub, ctx, TARGET);
        }
    }

    // Remove symlink from legacy by-peer subdirectory
    {
        std::array<char, 8> peer_str{};
        fmt_u16_hex4(peer_str.data(), peer_str.size() - 1, ctx->peer_node_id);
        peer_str[4] = '\0';
        DevFSNode* peer_sub = find_child(g_wki_by_peer, peer_str.data());
        if (peer_sub != nullptr) {
            wki_remove_wki_resource_aliases(peer_sub, ctx, TARGET);
        }
    }

    // Remove symlink from by-peer-id subdirectory
    {
        std::array<char, 8> peer_str{};
        fmt_u16_hex4(peer_str.data(), peer_str.size() - 1, ctx->peer_node_id);
        peer_str[4] = '\0';
        DevFSNode* peer_sub = find_child(g_wki_by_peer_id, peer_str.data());
        if (peer_sub != nullptr) {
            wki_remove_wki_resource_aliases(peer_sub, ctx, TARGET);
        }
    }

    // Remove symlink from by-peer-host subdirectory
    if (ctx->peer_hostname[0] != '\0') {
        DevFSNode* peer_host_sub = find_child(g_wki_by_peer_host, ctx->peer_hostname.data());
        if (peer_host_sub != nullptr) {
            wki_remove_wki_resource_aliases(peer_host_sub, ctx, TARGET);
        }
    }
}

void wki_add_resource_symlinks(const char* type_dir_name, const WkiDevfsCtx* ctx) {
    if (type_dir_name == nullptr || ctx == nullptr) {
        return;
    }

    std::array<char, DEVFS_SYMLINK_MAX> target{};
    wki_build_resource_symlink_target(type_dir_name, ctx, &target);

    std::string_view const DISPLAY_NAME{ctx->display_name.data()};
    std::string_view const TARGET{target.data()};
    DevFSNode* zone_sub = wki_ensure_hex_subdir(g_wki_by_zone, ctx->rdma_zone_id);
    if (zone_sub != nullptr) {
        wki_add_symlink(zone_sub, DISPLAY_NAME, TARGET);
        wki_add_block_remote_alias(zone_sub, ctx, TARGET);
    }

    DevFSNode* peer_sub = wki_ensure_hex_subdir(g_wki_by_peer, ctx->peer_node_id);
    if (peer_sub != nullptr) {
        wki_add_symlink(peer_sub, DISPLAY_NAME, TARGET);
        wki_add_block_remote_alias(peer_sub, ctx, TARGET);
    }

    DevFSNode* peer_id_sub = wki_ensure_hex_subdir(g_wki_by_peer_id, ctx->peer_node_id);
    if (peer_id_sub != nullptr) {
        wki_add_symlink(peer_id_sub, DISPLAY_NAME, TARGET);
        wki_add_block_remote_alias(peer_id_sub, ctx, TARGET);
    }

    DevFSNode* peer_host_sub = wki_ensure_named_subdir(g_wki_by_peer_host, ctx->peer_hostname.data());
    if (peer_host_sub != nullptr) {
        wki_add_symlink(peer_host_sub, DISPLAY_NAME, TARGET);
        wki_add_block_remote_alias(peer_host_sub, ctx, TARGET);
    }
}

void wki_copy_resource_name(WkiDevfsCtx* ctx, const char* name) {
    if (ctx == nullptr || name == nullptr) {
        return;
    }

    size_t const NAME_LEN = std::min(std::strlen(name), ctx->remote_name.size() - 1);
    std::memcpy(ctx->remote_name.data(), name, NAME_LEN);
    ctx->remote_name.at(NAME_LEN) = '\0';
}

// Remove a device node and its by-zone/by-peer symlinks given the device name and zone/peer IDs.
void wki_remove_device_and_symlinks(DevFSNode* type_dir, DevFSNode* device_node, const WkiDevfsCtx* ctx) {
    wki_remove_resource_symlinks(ctx);

    // Unlink immediately, then defer backing-object reclamation until the last
    // opened devfs File drops its WKI node pin.
    wki_unlink_node(type_dir, device_node);
}

}  // namespace

void devfs_wki_add_resource(uint16_t node_id, uint16_t resource_type, uint32_t resource_id, uint64_t resource_generation, uint8_t flags,
                            const char* name) {
    WkiAutoLock const GUARD;
    if (name == nullptr || !wki_ensure_dirs()) {
        return;
    }

    auto type = static_cast<ker::net::wki::ResourceType>(resource_type);
    auto type_idx = static_cast<uint16_t>(type);
    if (type_idx == 0 || type_idx > 6) {
        return;
    }

    // Look up peer to get rdma_zone_id
    auto* peer = ker::net::wki::wki_peer_find(node_id);
    uint16_t const ZONE_ID = (peer != nullptr) ? peer->rdma_zone_id : 0;
    std::array<char, ker::net::wki::WKI_HOSTNAME_MAX> peer_hostname{};
    wki_copy_peer_hostname(peer_hostname.data(), peer_hostname.size(), node_id);

    // Ensure type subdirectory exists under wki/
    const char* type_dir_name = wki_type_dir(type);
    DevFSNode* type_dir = find_child(g_wki_dir, type_dir_name);
    if (type_dir == nullptr) {
        type_dir = create_node(type_dir_name, DevFSNodeType::DIRECTORY);
        if (type_dir == nullptr) {
            return;
        }
        add_child(g_wki_dir, type_dir);
    }

    // Preserve the device/node/context allocation across identity upserts:
    // opened devfs files keep raw views of those objects. Exact repetitions are
    // a true no-op; generation/content changes update metadata and aliases in
    // place while retaining the stable device name and object lifetime.
    if (auto* existing = wki_find_device_in_type_dir(type_dir, node_id, type, resource_id); existing != nullptr) {
        auto* existing_ctx = static_cast<WkiDevfsCtx*>(existing->device->private_data);
        if (existing_ctx != nullptr && existing_ctx->resource_generation == resource_generation && existing_ctx->flags == flags &&
            existing_ctx->rdma_zone_id == ZONE_ID &&
            std::strncmp(existing_ctx->remote_name.data(), name, existing_ctx->remote_name.size()) == 0 &&
            existing_ctx->peer_hostname == peer_hostname) {
            return;
        }

        if (existing_ctx != nullptr) {
            wki_remove_resource_symlinks(existing_ctx);
            existing_ctx->rdma_zone_id = ZONE_ID;
            existing_ctx->resource_generation = resource_generation;
            existing_ctx->flags = flags;
            existing_ctx->peer_hostname = peer_hostname;
            wki_copy_resource_name(existing_ctx, name);
            touch_node_modified(existing);

            while (auto* duplicate = wki_find_device_in_type_dir(type_dir, node_id, type, resource_id, existing)) {
                auto* duplicate_ctx = static_cast<WkiDevfsCtx*>(duplicate->device->private_data);
                wki_remove_device_and_symlinks(type_dir, duplicate, duplicate_ctx);
            }
            wki_add_resource_symlinks(type_dir_name, existing_ctx);
            return;
        }

        wki_remove_device_and_symlinks(type_dir, existing, existing_ctx);
    }

    // Clean up any legacy duplicate/corrupt rows before first publication.
    while (auto* existing = wki_find_device_in_type_dir(type_dir, node_id, type, resource_id)) {
        auto* existing_ctx = static_cast<WkiDevfsCtx*>(existing->device->private_data);
        wki_remove_device_and_symlinks(type_dir, existing, existing_ctx);
    }

    // Assign local counter
    uint32_t const LOCAL_NUM = g_wki_type_counters[type_idx]++;  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)

    // Allocate context (also holds persistent dev_name for Device::name)
    auto* rctx = new WkiDevfsCtx();
    if (rctx == nullptr) {
        return;
    }
    rctx->resource_type = type;
    rctx->peer_node_id = node_id;
    rctx->rdma_zone_id = ZONE_ID;
    rctx->resource_id = resource_id;
    rctx->resource_generation = resource_generation;
    rctx->flags = flags;
    wki_copy_resource_name(rctx, name);
    wki_build_dev_name(rctx->dev_name.data(), rctx->dev_name.size(), ZONE_ID, node_id, type, LOCAL_NUM);
    wki_build_display_name(rctx->display_name.data(), rctx->display_name.size(), type, LOCAL_NUM);
    rctx->peer_hostname = peer_hostname;

    // Allocate Device struct
    auto* dev = new ker::dev::Device();
    if (dev == nullptr) {
        delete rctx;
        return;
    }
    dev->major = 11;
    dev->minor = static_cast<unsigned>(g_wki_total);
    dev->name = rctx->dev_name.data();  // points into heap-allocated ctx
    dev->type = (type == ker::net::wki::ResourceType::BLOCK) ? ker::dev::DeviceType::BLOCK : ker::dev::DeviceType::CHAR;
    dev->private_data = rctx;
    dev->char_ops = &wki_resource_ops;

    // Create device node under type directory
    auto* node = create_node(rctx->dev_name.data(), DevFSNodeType::DEVICE);
    if (node == nullptr) {
        delete dev;
        delete rctx;
        return;
    }
    node->device = dev;
    add_child(type_dir, node);

    wki_add_resource_symlinks(type_dir_name, rctx);

    g_wki_total++;
}

void devfs_wki_remove_resource(uint16_t node_id, uint16_t resource_type, uint32_t resource_id, uint64_t expected_generation) {
    WkiAutoLock const GUARD;
    if (g_wki_dir == nullptr) {
        return;
    }

    auto type = static_cast<ker::net::wki::ResourceType>(resource_type);
    const char* type_dir_name = wki_type_dir(type);
    DevFSNode* type_dir = find_child(g_wki_dir, type_dir_name);
    if (type_dir == nullptr) {
        return;
    }

    while (auto* device_node = wki_find_device_in_type_dir(type_dir, node_id, type, resource_id)) {
        auto* ctx = static_cast<WkiDevfsCtx*>(device_node->device->private_data);
        if (expected_generation != 0 && (ctx == nullptr || ctx->resource_generation != expected_generation)) {
            return;
        }
        wki_remove_device_and_symlinks(type_dir, device_node, ctx);
    }
}

void devfs_wki_remove_peer_resources(uint16_t node_id) {
    WkiAutoLock const GUARD;
    if (g_wki_dir == nullptr) {
        return;
    }

    // Scan all type directories under wki/. Remove one match at a time so peer
    // invalidation is complete even when a type exposes more than a fixed
    // temporary batch of resources.
    for (size_t d = 0; d < g_wki_dir->children_count; d++) {
        auto* type_dir = g_wki_dir->children[d];
        if (type_dir->type != DevFSNodeType::DIRECTORY) {
            continue;
        }
        // Skip WKI metadata view directories
        if (std::strcmp(type_dir->name.data(), "by-zone") == 0) {
            continue;
        }
        if (std::strcmp(type_dir->name.data(), "by-peer") == 0) {
            continue;
        }
        if (std::strcmp(type_dir->name.data(), "by-peer-id") == 0) {
            continue;
        }
        if (std::strcmp(type_dir->name.data(), "by-peer-host") == 0) {
            continue;
        }

        for (;;) {
            DevFSNode* matching_node = nullptr;
            for (size_t i = 0; i < type_dir->children_count; i++) {
                auto* child = type_dir->children[i];
                if (child->type != DevFSNodeType::DEVICE || child->device == nullptr) {
                    continue;
                }
                auto* ctx = static_cast<WkiDevfsCtx*>(child->device->private_data);
                if (ctx != nullptr && ctx->peer_node_id == node_id) {
                    matching_node = child;
                    break;
                }
            }
            if (matching_node == nullptr) {
                break;
            }

            auto* ctx = static_cast<WkiDevfsCtx*>(matching_node->device->private_data);
            wki_remove_device_and_symlinks(type_dir, matching_node, ctx);
        }
    }
}

void devfs_populate_wki() {
    if (!wki_ensure_dirs()) {
        return;
    }

    // Populate from current discovered resources
    ker::net::wki::wki_resource_foreach(
        [](const ker::net::wki::DiscoveredResource& res, void* /*ctx*/) {
            devfs_wki_add_resource(res.node_id, static_cast<uint16_t>(res.resource_type), res.resource_id, res.generation, res.flags,
                                   static_cast<const char*>(res.name));
        },
        nullptr);

    vfs_debug_log("devfs: wki nodes populated (");
    vfs_debug_log_hex(g_wki_total);
    vfs_debug_log(" resources)\n");
}

auto devfs_resolve_block_device(const char* path) -> ker::dev::BlockDevice* {
    auto node_ref = devfs_acquire_path(path);
    auto* node = node_ref.get();
    bool const WKI_PATH = is_wki_devfs_path(path);
    std::array<char, ker::dev::BLOCK_NAME_SIZE> device_name{};
    uint16_t owner_node = ker::net::wki::WKI_NODE_INVALID;
    uint32_t resource_id = 0;
    uint64_t resource_generation = 0;
    ker::net::wki::ResourceIncarnationToken owner_incarnation = {};
    bool remote_block = false;
    {
        // A WKI pin keeps the backing allocation alive, while this lock makes
        // the identity snapshot coherent with in-place generation upserts.
        OptionalWkiLock const WKI_GUARD(WKI_PATH);
        if (node == nullptr || (WKI_PATH && node->wki_unlinked) || node->type != DevFSNodeType::DEVICE || node->device == nullptr ||
            node->device->type != ker::dev::DeviceType::BLOCK || node->device->name == nullptr) {
            return nullptr;
        }

        std::snprintf(device_name.data(), device_name.size(), "%s", node->device->name);
        if (node->device->major == 11 && node->device->private_data != nullptr) {
            auto* ctx = static_cast<WkiDevfsCtx*>(node->device->private_data);
            if (ctx->resource_type == ker::net::wki::ResourceType::BLOCK) {
                owner_node = ctx->peer_node_id;
                resource_id = ctx->resource_id;
                resource_generation = ctx->resource_generation;
                remote_block = true;
            }
        }
    }

    // Nothing below needs the devfs node. Drop its pin before registry lookup
    // and the potentially blocking remote attach handshake.
    node_ref.reset();

    if (remote_block && !ker::net::wki::wki_resource_observation_snapshot(owner_node, ker::net::wki::ResourceType::BLOCK, resource_id,
                                                                          resource_generation, &owner_incarnation)) {
        return nullptr;
    }

    // Check if already registered as a block device.
    auto* existing = ker::dev::block_device_find_by_name(device_name.data());
    if (existing != nullptr) {
        return existing;
    }

    // WKI block resource - trigger proxy attach using only the stable snapshot.
    if (remote_block) {
        return ker::net::wki::wki_dev_proxy_attach_block(owner_node, resource_id, resource_generation, owner_incarnation,
                                                         device_name.data());
    }

    return nullptr;
}

// =============================================================================
// V2: /dev/nodes/ hierarchy - Node identity
// =============================================================================

namespace {

// Context for each devfs file under /dev/nodes/<hostname>/
struct NodeDevfsCtx {
    uint16_t node_id{};               // WKI node ID (0 = self)
    uint8_t file_type{};              // 0 = id, 1 = state, 2 = load
    std::array<char, 64> hostname{};  // Copy of hostname for lookup
};

// Separate CharDeviceOps for id, state, and load files
ker::dev::CharDeviceOps s_node_id_ops = {
    .open = nullptr,
    .close = nullptr,
    .read = [](ker::vfs::File* f, void* buf, size_t count) -> ssize_t {
        auto* df = static_cast<DevFSFile*>(f->private_data);
        if (df == nullptr || df->device == nullptr || df->device->private_data == nullptr) {
            return 0;
        }
        auto* ctx = static_cast<NodeDevfsCtx*>(df->device->private_data);

        std::array<char, 16> data = {};
        int const LEN = snprintf(data.data(), data.size(), "0x%04x\n", ctx->node_id);

        auto offset = static_cast<size_t>(f->pos);
        if (std::cmp_greater_equal(offset, LEN)) {
            return 0;
        }
        size_t const AVAIL = static_cast<size_t>(LEN) - offset;
        size_t const TO_COPY = (count < AVAIL) ? count : AVAIL;
        std::memcpy(buf, data.data() + offset, TO_COPY);
        f->pos += static_cast<off_t>(TO_COPY);
        return static_cast<ssize_t>(TO_COPY);
    },
    .write = nullptr,
    .isatty = nullptr,
    .ioctl = nullptr,
    .poll_check = nullptr,
    .poll_register_waiter = nullptr,
};

ker::dev::CharDeviceOps s_node_state_ops = {
    .open = nullptr,
    .close = nullptr,
    .read = [](ker::vfs::File* f, void* buf, size_t count) -> ssize_t {
        auto* df = static_cast<DevFSFile*>(f->private_data);
        if (df == nullptr || df->device == nullptr || df->device->private_data == nullptr) {
            return 0;
        }
        auto* ctx = static_cast<NodeDevfsCtx*>(df->device->private_data);

        const char* state_str = "UNKNOWN\n";

        if (ctx->node_id == ker::net::wki::g_wki.my_node_id) {
            state_str = "SELF\n";
        } else {
            auto* peer = ker::net::wki::wki_peer_find(ctx->node_id);
            if (peer != nullptr) {
                switch (peer->state) {
                    case ker::net::wki::PeerState::CONNECTED:
                        state_str = "CONNECTED\n";
                        break;
                    case ker::net::wki::PeerState::FENCED:
                        state_str = "FENCED\n";
                        break;
                    case ker::net::wki::PeerState::HELLO_SENT:
                        state_str = "HELLO_SENT\n";
                        break;
                    case ker::net::wki::PeerState::RECONNECTING:
                        state_str = "RECONNECTING\n";
                        break;
                    default:
                        state_str = "UNKNOWN\n";
                        break;
                }
            }
        }

        size_t const LEN = std::strlen(state_str);
        auto offset = static_cast<size_t>(f->pos);
        if (offset >= LEN) {
            return 0;
        }
        size_t const AVAIL = LEN - offset;
        size_t const TO_COPY = (count < AVAIL) ? count : AVAIL;
        std::memcpy(buf, state_str + offset, TO_COPY);
        f->pos += static_cast<off_t>(TO_COPY);
        return static_cast<ssize_t>(TO_COPY);
    },
    .write = nullptr,
    .isatty = nullptr,
    .ioctl = nullptr,
    .poll_check = nullptr,
    .poll_register_waiter = nullptr,
};

ker::dev::CharDeviceOps s_node_load_ops = {
    .open = nullptr,
    .close = nullptr,
    .read = [](ker::vfs::File* f, void* buf, size_t count) -> ssize_t {
        auto* df = static_cast<DevFSFile*>(f->private_data);
        if (df == nullptr || df->device == nullptr || df->device->private_data == nullptr) {
            return 0;
        }
        auto* ctx = static_cast<NodeDevfsCtx*>(df->device->private_data);

        std::array<char, 128> data = {};
        int len = 0;

        if (ctx->node_id == ker::net::wki::g_wki.my_node_id) {
            len = snprintf(data.data(), data.size(), "cpu=0 mem=0 tasks=0\n");
        } else {
            auto* peer = ker::net::wki::wki_peer_find(ctx->node_id);
            if (peer != nullptr) {
                len = snprintf(data.data(), data.size(), "rtt=%u\n", peer->rtt_us);
            } else {
                len = snprintf(data.data(), data.size(), "unavailable\n");
            }
        }

        auto offset = static_cast<size_t>(f->pos);
        if (std::cmp_greater_equal(offset, len)) {
            return 0;
        }
        size_t const AVAIL = static_cast<size_t>(len) - offset;
        size_t const TO_COPY = (count < AVAIL) ? count : AVAIL;
        std::memcpy(buf, data.data() + offset, TO_COPY);
        f->pos += static_cast<off_t>(TO_COPY);
        return static_cast<ssize_t>(TO_COPY);
    },
    .write = nullptr,
    .isatty = nullptr,
    .ioctl = nullptr,
    .poll_check = nullptr,
    .poll_register_waiter = nullptr,
};

unsigned s_node_minor_counter = 0;

auto make_node_device(const char* hostname, uint16_t node_id, uint8_t file_type, ker::dev::CharDeviceOps* ops) -> ker::dev::Device* {
    auto* dev = new ker::dev::Device();
    if (dev == nullptr) {
        return nullptr;
    }

    auto* ctx = new NodeDevfsCtx();
    if (ctx == nullptr) {
        delete dev;
        return nullptr;
    }

    ctx->node_id = node_id;
    ctx->file_type = file_type;
    size_t host_len = std::strlen(hostname);
    if (host_len >= ctx->hostname.size()) {
        host_len = ctx->hostname.size() - 1;
    }
    std::memcpy(ctx->hostname.data(), hostname, host_len);
    ctx->hostname[host_len] = '\0';  // NOLINT(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)

    dev->major = 10;
    dev->minor = 300 + s_node_minor_counter++;
    dev->name = ctx->hostname.data();  // devfs doesn't use this for path
    dev->type = ker::dev::DeviceType::CHAR;
    dev->private_data = ctx;
    dev->char_ops = ops;

    return dev;
}

void destroy_node_device(ker::dev::Device* dev) {
    if (dev == nullptr) {
        return;
    }
    delete static_cast<NodeDevfsCtx*>(dev->private_data);
    delete dev;
}

}  // namespace

void devfs_nodes_init() {
    walk_path("nodes", true);  // creates /dev/nodes/
}

void devfs_nodes_add_peer(const char* hostname, uint16_t node_id) {
    if (hostname == nullptr || hostname[0] == '\0') {
        return;
    }

    // Build directory path: "nodes/<hostname>"
    std::array<char, 128> dir_path = {};
    snprintf(dir_path.data(), dir_path.size(), "nodes/%s", hostname);
    walk_path(dir_path.data(), true);  // creates /dev/nodes/<hostname>/

    // Create id file
    std::array<char, 160> path = {};
    snprintf(path.data(), path.size(), "nodes/%s/id", hostname);
    auto* id_dev = make_node_device(hostname, node_id, 0, &s_node_id_ops);
    if (id_dev != nullptr) {
        if (devfs_add_device_node(path.data(), id_dev) == nullptr) {
            destroy_node_device(id_dev);
        }
    }

    // Create state file
    snprintf(path.data(), path.size(), "nodes/%s/state", hostname);
    auto* state_dev = make_node_device(hostname, node_id, 1, &s_node_state_ops);
    if (state_dev != nullptr) {
        if (devfs_add_device_node(path.data(), state_dev) == nullptr) {
            destroy_node_device(state_dev);
        }
    }

    // Create load file
    snprintf(path.data(), path.size(), "nodes/%s/load", hostname);
    auto* load_dev = make_node_device(hostname, node_id, 2, &s_node_load_ops);
    if (load_dev != nullptr) {
        if (devfs_add_device_node(path.data(), load_dev) == nullptr) {
            destroy_node_device(load_dev);
        }
    }
}

void devfs_nodes_update_state(const char* /*hostname*/, const char* /*state_str*/) {
    // State is read live from WkiPeer::state via the s_node_state_ops read handler.
    // No cached state to update - this function exists for future use if caching is added.
}

void devfs_nodes_remove_peer(const char* hostname) {
    if (hostname == nullptr || hostname[0] == '\0') {
        return;
    }

    std::array<char, 128> dir_path = {};
    snprintf(dir_path.data(), dir_path.size(), "nodes/%s", hostname);
    auto* dir = walk_path(dir_path.data());
    if (dir == nullptr) {
        return;
    }

    // Remove all children (id, state, load files)
    while (dir->children_count > 0) {
        remove_child(dir, dir->children[0]);
    }

    // Remove the directory itself from /dev/nodes/
    auto* nodes_dir = walk_path("nodes");
    if (nodes_dir != nullptr) {
        remove_child(nodes_dir, dir);
    }
}

}  // namespace ker::vfs::devfs
// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access, cppcoreguidelines-interfaces-global-init,
// readability-suspicious-call-argument)
