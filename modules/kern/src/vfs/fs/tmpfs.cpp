#include "tmpfs.hpp"

#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
#include <vfs/file.hpp>

#include "vfs/file_operations.hpp"
#include "vfs/vfs.hpp"

namespace {
auto kstrcmp(const char* a, const char* b) -> int {
    if ((a == nullptr) || (b == nullptr)) {
        if (a == b) {
            return 0;
        }
        if (a != nullptr) {
            return 1;
        }
        return -1;
    }
    while ((*a != 0) && (*b != 0)) {
        if (*a != *b) {
            return static_cast<unsigned char>(*a) - static_cast<unsigned char>(*b);
        }
        ++a;
        ++b;
    }
    return static_cast<unsigned char>(*a) - static_cast<unsigned char>(*b);
}

// Copy a string into a fixed-size array, ensuring null termination
void copy_name(std::array<char, ker::vfs::tmpfs::TMPFS_NAME_MAX>& dst, const char* src) {
    size_t i = 0;
    if (src != nullptr) {
        while (src[i] != '\0' && i < ker::vfs::tmpfs::TMPFS_NAME_MAX - 1) {
            dst.at(i) = src[i];
            i++;
        }
    }
    dst.at(i) = '\0';
}
}  // namespace

namespace ker::vfs::tmpfs {

constexpr size_t DEFAULT_TMPFS_BLOCK_SIZE = 4096;
constexpr size_t INITIAL_CHILDREN_CAPACITY = 8;
constexpr int O_CREAT = 0100;  // octal = 64 decimal = 0x40 hex

namespace {
TmpNode* root_node = nullptr;        // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
ker::mod::sys::Spinlock tmpfs_lock;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
}  // namespace

// --- Internal helpers ---

namespace {
auto tmpfs_capacity_for_size(size_t size) -> size_t {
    if (size == 0) {
        return 0;
    }
    size_t cap = DEFAULT_TMPFS_BLOCK_SIZE;
    while (cap < size) {
        cap *= 2;
    }
    return cap;
}

// Grow the children slot array of a directory node if needed.
void ensure_children_capacity(TmpNode* dir, size_t slot) {
    if (slot < dir->children_capacity) {
        return;
    }
    size_t new_cap = (dir->children_capacity == 0) ? INITIAL_CHILDREN_CAPACITY : dir->children_capacity * 2;
    while (slot >= new_cap) {
        new_cap *= 2;
    }
    auto** new_arr = new TmpNode*[new_cap];
    for (size_t i = 0; i < dir->children_count; ++i) {
        new_arr[i] = dir->children[i];
    }
    for (size_t i = dir->children_count; i < new_cap; ++i) {
        new_arr[i] = nullptr;
    }
    delete[] dir->children;
    dir->children = new_arr;
    dir->children_capacity = new_cap;
}

void add_child(TmpNode* parent, TmpNode* child) {
    size_t slot = parent->children_count;
    if (parent->open_count.load(std::memory_order_acquire) == 0) {
        for (size_t i = 0; i < parent->children_count; ++i) {
            if (parent->children[i] == nullptr) {
                slot = i;
                break;
            }
        }
    }

    ensure_children_capacity(parent, slot);
    parent->children[slot] = child;
    if (slot == parent->children_count) {
        parent->children_count++;
    }
    parent->children_live_count++;
    child->parent = parent;
}

auto tmpfs_write_locked(TmpNode* n, const void* buf, size_t count, size_t offset) -> ssize_t {
    if (count == 0) {
        return 0;
    }
    size_t const NEED = offset + count;
    if (NEED > n->capacity) {
        size_t const NEWCAP = tmpfs_capacity_for_size(NEED);
        char* nd = new char[NEWCAP];
        if (n->data != nullptr) {
            std::memcpy(nd, n->data, n->size);
        }
        delete[] n->data;
        n->data = nd;
        n->capacity = NEWCAP;
    }
    std::memcpy(n->data + offset, buf, count);
    n->size = std::max(NEED, n->size);
    return static_cast<ssize_t>(count);
}

auto tmpfs_resize_locked(TmpNode* n, size_t new_size) -> int {
    size_t const NEWCAP = tmpfs_capacity_for_size(new_size);
    if (NEWCAP != n->capacity) {
        if (NEWCAP == 0) {
            delete[] n->data;
            n->data = nullptr;
            n->capacity = 0;
            n->size = 0;
            return 0;
        }
        char* nd = new char[NEWCAP];
        if (n->data != nullptr) {
            std::memcpy(nd, n->data, std::min(n->size, new_size));
        }
        delete[] n->data;
        n->data = nd;
        n->capacity = NEWCAP;
    }
    if (new_size > n->size) {
        std::memset(n->data + n->size, 0, new_size - n->size);
    }
    n->size = new_size;
    return 0;
}

auto create_root_node_internal() -> TmpNode* {
    auto* node = new TmpNode;
    if (node == nullptr) {
        return nullptr;
    }
    copy_name(node->name, "/");
    node->type = TmpNodeType::DIRECTORY;
    node->mode = 0755;
    return node;
}
}  // namespace

void tmpfs_free_node(TmpNode* node) {
    if (node == nullptr) {
        return;
    }
    delete[] node->data;
    delete[] node->symlink_target;
    delete[] node->children;
    delete node;
}

void tmpfs_lock_tree() { tmpfs_lock.lock(); }
void tmpfs_unlock_tree() { tmpfs_lock.unlock(); }

// --- Node operations ---

auto tmpfs_lookup(TmpNode* dir, const char* name) -> TmpNode* {
    if (dir == nullptr || name == nullptr || dir->type != TmpNodeType::DIRECTORY) {
        return nullptr;
    }
    for (size_t i = 0; i < dir->children_count; ++i) {
        if (dir->children[i] != nullptr && kstrcmp(dir->children[i]->name.data(), name) == 0) {
            return dir->children[i];
        }
    }
    return nullptr;
}

auto tmpfs_mkdir(TmpNode* parent, const char* name) -> TmpNode* {
    if (parent == nullptr || name == nullptr || parent->type != TmpNodeType::DIRECTORY) {
        return nullptr;
    }
    // Check if it already exists
    TmpNode* existing = tmpfs_lookup(parent, name);
    if (existing != nullptr) {
        if (existing->type == TmpNodeType::DIRECTORY) {
            return existing;  // Already exists as directory
        }
        return nullptr;  // Exists as non-directory
    }
    auto* node = new TmpNode;
    copy_name(node->name, name);
    node->type = TmpNodeType::DIRECTORY;
    node->mode = 0755;
    add_child(parent, node);
    return node;
}

auto tmpfs_create_file(TmpNode* parent, const char* name, uint32_t create_mode) -> TmpNode* {
    if (parent == nullptr || name == nullptr || parent->type != TmpNodeType::DIRECTORY) {
        return nullptr;
    }
    TmpNode* existing = tmpfs_lookup(parent, name);
    if (existing != nullptr) {
        return existing;  // Return existing node
    }
    auto* node = new TmpNode;
    copy_name(node->name, name);
    node->type = TmpNodeType::FILE;
    node->mode = create_mode & 07777;
    add_child(parent, node);
    return node;
}

auto tmpfs_create_symlink(TmpNode* parent, const char* name, const char* target) -> TmpNode* {
    if (parent == nullptr || name == nullptr || target == nullptr || parent->type != TmpNodeType::DIRECTORY) {
        return nullptr;
    }
    TmpNode const* existing = tmpfs_lookup(parent, name);
    if (existing != nullptr) {
        return nullptr;  // Already exists
    }
    auto* node = new TmpNode;
    copy_name(node->name, name);
    node->type = TmpNodeType::SYMLINK;
    node->mode = 0777;
    // Allocate and copy the symlink target
    size_t target_len = 0;
    while (target[target_len] != '\0') {
        target_len++;
    }
    node->symlink_target = new char[target_len + 1];
    std::memcpy(node->symlink_target, target, target_len + 1);
    add_child(parent, node);
    return node;
}

auto tmpfs_attach_child(TmpNode* parent, TmpNode* child) -> bool {
    if (parent == nullptr || child == nullptr || parent->type != TmpNodeType::DIRECTORY) {
        return false;
    }
    add_child(parent, child);
    return true;
}

auto tmpfs_detach_child(TmpNode* parent, TmpNode* child) -> bool {
    if (parent == nullptr || child == nullptr || parent->type != TmpNodeType::DIRECTORY) {
        return false;
    }

    for (size_t i = 0; i < parent->children_count; ++i) {
        if (parent->children[i] != child) {
            continue;
        }

        parent->children[i] = nullptr;
        if (parent->children_live_count > 0) {
            parent->children_live_count--;
        }
        child->parent = nullptr;

        if (parent->open_count.load(std::memory_order_acquire) == 0) {
            while (parent->children_count > 0 && parent->children[parent->children_count - 1] == nullptr) {
                parent->children_count--;
            }
        }
        return true;
    }

    return false;
}

auto tmpfs_directory_is_empty(const TmpNode* dir) -> bool {
    return dir != nullptr && dir->type == TmpNodeType::DIRECTORY && dir->children_live_count == 0;
}

namespace {

// Internal unlocked version - caller must hold tmpfs_lock
auto tmpfs_walk_path_unlocked(TmpNode* root, const char* path, bool create_intermediate) -> TmpNode* {
    if (root == nullptr) {
        return nullptr;
    }

    // Skip leading slashes
    while (*path == '/') {
        path++;
    }

    // Empty path means root
    if (*path == '\0') {
        return root;
    }

    TmpNode* current = root;

    // Parse path component by component
    while (*path != '\0') {
        // Skip consecutive slashes
        while (*path == '/') {
            path++;
        }
        if (*path == '\0') {
            break;
        }

        // Extract the next component
        std::array<char, TMPFS_NAME_MAX> component{};
        size_t comp_len = 0;
        while (path[comp_len] != '\0' && path[comp_len] != '/' && comp_len < TMPFS_NAME_MAX - 1) {
            component.at(comp_len) = path[comp_len];
            comp_len++;
        }
        component.at(comp_len) = '\0';
        path += comp_len;

        // Handle "." and ".."
        if (kstrcmp(component.data(), ".") == 0) {
            continue;
        }
        if (kstrcmp(component.data(), "..") == 0) {
            if (current->parent != nullptr) {
                current = current->parent;
            }
            continue;
        }

        // Current node must be a directory to descend into
        if (current->type != TmpNodeType::DIRECTORY) {
            return nullptr;
        }

        TmpNode* child = tmpfs_lookup(current, component.data());
        if (child == nullptr) {
            if (!create_intermediate) {
                return nullptr;
            }
            // All missing components are created as directories;
            // the caller can convert the final node to a different type if needed
            child = tmpfs_mkdir(current, component.data());
            if (child == nullptr) {
                return nullptr;
            }
        }

        current = child;
    }

    return current;
}

}  // namespace

auto tmpfs_walk_path(TmpNode* root, const char* path, bool create_intermediate) -> TmpNode* {
    if (path == nullptr || root == nullptr) {
        return nullptr;
    }
    tmpfs_lock.lock();
    TmpNode* result = tmpfs_walk_path_unlocked(root, path, create_intermediate);
    tmpfs_lock.unlock();
    return result;
}

auto tmpfs_walk_path(const char* path, bool create_intermediate) -> TmpNode* {
    return tmpfs_walk_path(root_node, path, create_intermediate);
}

// --- Initialization ---

void register_tmpfs() {
    vfs_debug_log("tmpfs: register_tmpfs called\n");
    if (root_node == nullptr) {
        root_node = create_root_node_internal();
    }
}

auto create_root_node() -> TmpNode* { return create_root_node_internal(); }

auto get_root_node() -> TmpNode* { return root_node; }

// --- File-level operations ---

auto create_root_file(TmpNode* root) -> ker::vfs::File* {
    if (root == nullptr) {
        return nullptr;
    }
    root->open_count.fetch_add(1, std::memory_order_relaxed);
    auto* f = new File;
    f->private_data = root;
    f->fd = -1;
    f->pos = 0;
    f->is_directory = true;
    f->fs_type = FSType::TMPFS;
    f->refcount = 1;
    return f;
}

auto create_root_file() -> ker::vfs::File* { return create_root_file(root_node); }

auto tmpfs_open_path(TmpNode* root, const char* path, int flags, int mode) -> ker::vfs::File* {
    // mode is now used for O_CREAT
    if (path == nullptr || root == nullptr) {
        return nullptr;
    }

    // Handle root path
    if (path[0] == '\0' || (path[0] == '/' && path[1] == '\0')) {
        return create_root_file(root);
    }

    // Skip leading slash for walk_path
    const char* rel_path = path;
    if (rel_path[0] == '/') {
        rel_path++;
    }
    if (rel_path[0] == '\0') {
        return create_root_file(root);
    }

    // Split path into parent path and final component
    // Find the last '/' to separate parent from name
    const char* last_slash = nullptr;
    for (const char* p = rel_path; *p != '\0'; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    TmpNode* node = nullptr;

    tmpfs_lock.lock();
    if (last_slash == nullptr) {
        // Single component path (e.g., "file.txt")
        node = tmpfs_lookup(root, rel_path);
        if (node == nullptr && (flags & O_CREAT) != 0) {
            node = tmpfs_create_file(root, rel_path, static_cast<uint32_t>(mode) & 07777);
        }
    } else {
        // Multi-component path (e.g., "etc/fstab")
        // Walk to the parent directory
        constexpr size_t MAX_PATH_LEN = 512;
        auto parent_len = static_cast<size_t>(last_slash - rel_path);
        std::array<char, MAX_PATH_LEN> parent_path{};
        if (parent_len >= MAX_PATH_LEN) {
            tmpfs_lock.unlock();
            return nullptr;
        }
        std::memcpy(parent_path.data(), rel_path, parent_len);
        parent_path.at(parent_len) = '\0';

        const char* final_name = last_slash + 1;
        if (*final_name == '\0') {
            // Path ends with '/' - open the directory itself
            node = tmpfs_walk_path_unlocked(root, parent_path.data(), (flags & O_CREAT) != 0);
        } else {
            // Walk to parent, then lookup/create the final component
            TmpNode* parent = tmpfs_walk_path_unlocked(root, parent_path.data(), (flags & O_CREAT) != 0);
            if (parent == nullptr) {
                tmpfs_lock.unlock();
                return nullptr;
            }
            node = tmpfs_lookup(parent, final_name);
            if (node == nullptr && (flags & O_CREAT) != 0) {
                node = tmpfs_create_file(parent, final_name, static_cast<uint32_t>(mode) & 07777);
            }
        }
    }
    if (node != nullptr) {
        node->open_count.fetch_add(1, std::memory_order_relaxed);
    }
    tmpfs_lock.unlock();

    if (node == nullptr) {
        return nullptr;
    }

    if ((flags & ker::vfs::O_TRUNC) != 0 && node->type == TmpNodeType::FILE) {
        ker::mod::sys::MutexGuard guard(node->io_lock);
        int const TRUNCATE_RET = tmpfs_resize_locked(node, 0);
        if (TRUNCATE_RET < 0) {
            uint32_t const PREV = node->open_count.fetch_sub(1, std::memory_order_acq_rel);
            if (PREV == 1 && node->unlinked) {
                tmpfs_free_node(node);
            }
            return nullptr;
        }
    }

    auto* f = new File;
    f->private_data = node;
    f->fd = -1;
    f->pos = 0;
    f->is_directory = (node->type == TmpNodeType::DIRECTORY);
    f->fs_type = FSType::TMPFS;
    f->refcount = 1;
    return f;
}

auto tmpfs_open_path(const char* path, int flags, int mode) -> ker::vfs::File* { return tmpfs_open_path(root_node, path, flags, mode); }

auto tmpfs_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -EBADF;
    }
    auto* n = static_cast<TmpNode*>(f->private_data);
    ker::mod::sys::MutexGuard guard(n->io_lock);
    if (offset >= n->size) {
        return 0;
    }
    size_t to_read = n->size - offset;
    to_read = std::min(to_read, count);
    std::memcpy(buf, n->data + offset, to_read);
    return static_cast<ssize_t>(to_read);
}

auto tmpfs_write(ker::vfs::File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -EBADF;
    }
    auto* n = static_cast<TmpNode*>(f->private_data);
    ker::mod::sys::MutexGuard guard(n->io_lock);
    return tmpfs_write_locked(n, buf, count, offset);
}

auto tmpfs_write_append(ker::vfs::File* f, const void* buf, size_t count, size_t* offset_out) -> ssize_t {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -EBADF;
    }
    auto* n = static_cast<TmpNode*>(f->private_data);
    ker::mod::sys::MutexGuard guard(n->io_lock);
    size_t const OFFSET = n->size;
    ssize_t const RET = tmpfs_write_locked(n, buf, count, OFFSET);
    if (RET >= 0 && offset_out != nullptr) {
        *offset_out = OFFSET;
    }
    return RET;
}

auto tmpfs_get_size(ker::vfs::File* f) -> size_t {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return 0;
    }
    auto* n = static_cast<TmpNode*>(f->private_data);
    ker::mod::sys::MutexGuard guard(n->io_lock);
    return n->size;
}

// --- FileOperations callbacks ---

auto tmpfs_fops_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t { return tmpfs_read(f, buf, count, offset); }

auto tmpfs_fops_write(ker::vfs::File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    return tmpfs_write(f, buf, count, offset);
}

auto tmpfs_fops_close(ker::vfs::File* f) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return 0;
    }
    auto* node = static_cast<TmpNode*>(f->private_data);
    uint32_t const PREV = node->open_count.fetch_sub(1, std::memory_order_acq_rel);
    if (PREV == 1 && node->unlinked) {
        // Last close of an unlinked node — free it now
        tmpfs_free_node(node);
    }
    f->private_data = nullptr;
    return 0;
}

auto tmpfs_fops_lseek(ker::vfs::File* f, off_t offset, int whence) -> off_t {
    if (f == nullptr) {
        return -EBADF;
    }

    size_t const FILE_SIZE = tmpfs_get_size(f);
    off_t newpos = 0;

    switch (whence) {
        case 0:  // SEEK_SET
            newpos = offset;
            break;
        case 1:  // SEEK_CUR
            newpos = f->pos + offset;
            break;
        case 2:  // SEEK_END
            newpos = static_cast<off_t>(FILE_SIZE) + offset;
            break;
        default:
            return -EINVAL;
    }

    if (newpos < 0) {
        return -EINVAL;
    }
    f->pos = newpos;
    return f->pos;
}

auto tmpfs_fops_isatty(ker::vfs::File* f) -> bool {
    (void)f;
    return false;
}

namespace {
auto tmpfs_fops_readdir(ker::vfs::File* f, DirEntry* entry, size_t index) -> int {
    if (entry == nullptr) {
        return -EINVAL;
    }
    if (f == nullptr || f->private_data == nullptr) {
        return -EBADF;
    }

    auto* n = static_cast<TmpNode*>(f->private_data);

    if (n->type != TmpNodeType::DIRECTORY) {
        return -ENOTDIR;
    }

    // Indices 0 and 1 are synthetic "." and ".." entries
    if (index == 0) {
        entry->d_ino = reinterpret_cast<uint64_t>(n);
        entry->d_off = 1;
        entry->d_reclen = sizeof(DirEntry);
        entry->d_type = DT_DIR;
        // DirEntry is a public ABI-style record with a raw d_name buffer.
        // NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        entry->d_name[0] = '.';
        entry->d_name[1] = '\0';
        // NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        return 0;
    }
    if (index == 1) {
        TmpNode const* parent = (n->parent != nullptr) ? n->parent : n;
        entry->d_ino = reinterpret_cast<uint64_t>(parent);
        entry->d_off = 2;
        entry->d_reclen = sizeof(DirEntry);
        entry->d_type = DT_DIR;
        // DirEntry is a public ABI-style record with a raw d_name buffer.
        // NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        entry->d_name[0] = '.';
        entry->d_name[1] = '.';
        entry->d_name[2] = '\0';
        // NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        return 0;
    }

    // Real children start at index 2.  The child array is sparse so open
    // directory streams keep stable offsets while entries are removed.
    size_t child_index = index - 2;

    while (child_index < n->children_count && n->children[child_index] == nullptr) {
        child_index++;
    }

    if (child_index >= n->children_count) {
        return -ENOENT;
    }

    TmpNode* child = n->children[child_index];
    entry->d_ino = reinterpret_cast<uint64_t>(child);
    entry->d_off = child_index + 3;
    entry->d_reclen = sizeof(DirEntry);

    switch (child->type) {
        case TmpNodeType::DIRECTORY:
            entry->d_type = DT_DIR;
            break;
        case TmpNodeType::SYMLINK:
            entry->d_type = DT_LNK;
            break;
        default:
            entry->d_type = DT_REG;
            break;
    }

    size_t name_len = 0;
    while (child->name.at(name_len) != '\0' && name_len < DIRENT_NAME_MAX - 1) {
        // DirEntry is a public ABI-style record with a raw d_name buffer.
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        entry->d_name[name_len] = child->name.at(name_len);
        name_len++;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
    entry->d_name[name_len] = '\0';

    return 0;
}

auto tmpfs_fops_readlink(ker::vfs::File* f, char* buf, size_t bufsize) -> ssize_t {
    if (buf == nullptr || bufsize == 0) {
        return -EINVAL;
    }
    if (f == nullptr || f->private_data == nullptr) {
        return -EBADF;
    }
    auto* n = static_cast<TmpNode*>(f->private_data);
    if (n->type != TmpNodeType::SYMLINK || n->symlink_target == nullptr) {
        return -EINVAL;
    }
    size_t len = 0;
    while (n->symlink_target[len] != '\0') {
        len++;
    }
    size_t const TO_COPY = (len < bufsize) ? len : bufsize;
    std::memcpy(buf, n->symlink_target, TO_COPY);
    return static_cast<ssize_t>(TO_COPY);
}

// --- FileOperations instance ---

auto tmpfs_fops_truncate(ker::vfs::File* f, off_t length) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -EBADF;
    }
    auto* n = static_cast<TmpNode*>(f->private_data);
    if (n->type != TmpNodeType::FILE) {
        return -EISDIR;
    }
    if (length < 0) {
        return -EINVAL;
    }
    ker::mod::sys::MutexGuard guard(n->io_lock);
    auto const NEW_SIZE = static_cast<size_t>(length);
    return tmpfs_resize_locked(n, NEW_SIZE);
}

ker::vfs::FileOperations tmpfs_fops_instance = {
    .vfs_open = nullptr,
    .vfs_close = tmpfs_fops_close,
    .vfs_read = tmpfs_fops_read,
    .vfs_write = tmpfs_fops_write,
    .vfs_lseek = tmpfs_fops_lseek,
    .vfs_isatty = tmpfs_fops_isatty,
    .vfs_readdir = tmpfs_fops_readdir,
    .vfs_readlink = tmpfs_fops_readlink,
    .vfs_truncate = tmpfs_fops_truncate,
    .vfs_poll_check = nullptr,
    .vfs_ioctl = nullptr,
    .vfs_poll_register_waiter = nullptr,
};
}  // namespace

auto get_tmpfs_fops() -> ker::vfs::FileOperations* { return &tmpfs_fops_instance; }

}  // namespace ker::vfs::tmpfs
