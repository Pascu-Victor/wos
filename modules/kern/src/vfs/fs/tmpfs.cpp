#include "tmpfs.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <vfs/file.hpp>

#include "vfs/file_operations.hpp"

namespace {
auto _kstrcmp(const char* a, const char* b) -> int {
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
            return (unsigned char)*a - (unsigned char)*b;
        }
        ++a;
        ++b;
    }
    return (unsigned char)*a - (unsigned char)*b;
}

// Copy a string into a fixed-size array, ensuring null termination
void copy_name(std::array<char, ker::vfs::tmpfs::TMPFS_NAME_MAX>& dst, const char* src) {
    size_t i = 0;
    if (src != nullptr) {
        while (src[i] != '\0' && i < ker::vfs::tmpfs::TMPFS_NAME_MAX - 1) {
            dst[i] = src[i];
            i++;
        }
    }
    dst[i] = '\0';
}
}  // namespace

namespace ker::vfs::tmpfs {

constexpr size_t DEFAULT_TMPFS_BLOCK_SIZE = 4096;
constexpr size_t INITIAL_CHILDREN_CAPACITY = 8;
constexpr int O_CREAT = 0100;  // octal = 64 decimal = 0x40 hex

namespace {
TmpNode* rootNode = nullptr;
}  // namespace

// --- Internal helpers ---

namespace {
// Grow the children array of a directory node if needed
void ensure_children_capacity(TmpNode* dir) {
    if (dir->children_count < dir->children_capacity) {
        return;
    }
    size_t new_cap = (dir->children_capacity == 0) ? INITIAL_CHILDREN_CAPACITY : dir->children_capacity * 2;
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
    ensure_children_capacity(parent);
    parent->children[parent->children_count] = child;
    parent->children_count++;
    child->parent = parent;
}
}  // namespace

// --- Node operations ---

auto tmpfs_lookup(TmpNode* dir, const char* name) -> TmpNode* {
    if (dir == nullptr || name == nullptr || dir->type != TmpNodeType::DIRECTORY) {
        return nullptr;
    }
    for (size_t i = 0; i < dir->children_count; ++i) {
        if (dir->children[i] != nullptr && _kstrcmp(dir->children[i]->name.data(), name) == 0) {
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
    TmpNode* existing = tmpfs_lookup(parent, name);
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
    memcpy(node->symlink_target, target, target_len + 1);
    add_child(parent, node);
    return node;
}

auto tmpfs_walk_path(const char* path, bool create_intermediate) -> TmpNode* {
    if (path == nullptr || rootNode == nullptr) {
        return nullptr;
    }

    // Skip leading slashes
    while (*path == '/') {
        path++;
    }

    // Empty path means root
    if (*path == '\0') {
        return rootNode;
    }

    TmpNode* current = rootNode;

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
            component[comp_len] = path[comp_len];
            comp_len++;
        }
        component[comp_len] = '\0';
        path += comp_len;

        // Handle "." and ".."
        if (_kstrcmp(component.data(), ".") == 0) {
            continue;
        }
        if (_kstrcmp(component.data(), "..") == 0) {
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

// --- Initialization ---

void register_tmpfs() {
    vfs_debug_log("tmpfs: register_tmpfs called\n");
    if (rootNode == nullptr) {
        rootNode = new TmpNode;
        copy_name(rootNode->name, "/");
        rootNode->type = TmpNodeType::DIRECTORY;
        rootNode->mode = 0755;
    }
}

auto get_root_node() -> TmpNode* { return rootNode; }

// --- File-level operations ---

auto create_root_file() -> ker::vfs::File* {
    auto* f = new File;
    f->private_data = rootNode;
    f->fd = -1;
    f->pos = 0;
    f->is_directory = true;
    f->fs_type = FSType::TMPFS;
    f->refcount = 1;
    return f;
}

auto tmpfs_open_path(const char* path, int flags, int mode) -> ker::vfs::File* {
    // mode is now used for O_CREAT
    if (path == nullptr) {
        return nullptr;
    }

    // Handle root path
    if (path[0] == '\0' || (path[0] == '/' && path[1] == '\0')) {
        return create_root_file();
    }

    // Skip leading slash for walk_path
    const char* rel_path = path;
    if (rel_path[0] == '/') {
        rel_path++;
    }
    if (rel_path[0] == '\0') {
        return create_root_file();
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

    if (last_slash == nullptr) {
        // Single component path (e.g., "file.txt")
        node = tmpfs_lookup(rootNode, rel_path);
        if (node == nullptr && (flags & O_CREAT) != 0) {
            node = tmpfs_create_file(rootNode, rel_path, static_cast<uint32_t>(mode) & 07777);
        }
    } else {
        // Multi-component path (e.g., "etc/fstab")
        // Walk to the parent directory
        constexpr size_t MAX_PATH_LEN = 512;
        auto parent_len = static_cast<size_t>(last_slash - rel_path);
        std::array<char, MAX_PATH_LEN> parent_path{};
        if (parent_len >= MAX_PATH_LEN) {
            return nullptr;
        }
        memcpy(parent_path.data(), rel_path, parent_len);
        parent_path[parent_len] = '\0';

        const char* final_name = last_slash + 1;
        if (*final_name == '\0') {
            // Path ends with '/' - open the directory itself
            node = tmpfs_walk_path(parent_path.data(), (flags & O_CREAT) != 0);
        } else {
            // Walk to parent, then lookup/create the final component
            TmpNode* parent = tmpfs_walk_path(parent_path.data(), (flags & O_CREAT) != 0);
            if (parent == nullptr) {
                return nullptr;
            }
            node = tmpfs_lookup(parent, final_name);
            if (node == nullptr && (flags & O_CREAT) != 0) {
                node = tmpfs_create_file(parent, final_name, static_cast<uint32_t>(mode) & 07777);
            }
        }
    }

    if (node == nullptr) {
        return nullptr;
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

auto tmpfs_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -1;
    }
    auto* n = static_cast<TmpNode*>(f->private_data);
    if (offset >= n->size) {
        return 0;
    }
    size_t toRead = n->size - offset;
    toRead = std::min(toRead, count);
    memcpy(buf, n->data + offset, toRead);
    return static_cast<ssize_t>(toRead);
}

auto tmpfs_write(ker::vfs::File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -1;
    }
    auto* n = static_cast<TmpNode*>(f->private_data);
    size_t need = offset + count;
    if (need > n->capacity) {
        size_t newcap = (n->capacity == 0) ? DEFAULT_TMPFS_BLOCK_SIZE : n->capacity;
        while (newcap < need) {
            newcap *= 2;
        }
        char* nd = new char[newcap];
        if (n->data != nullptr) {
            memcpy(nd, n->data, n->size);
        }
        delete[] n->data;
        n->data = nd;
        n->capacity = newcap;
    }
    memcpy(n->data + offset, buf, count);
    n->size = std::max(need, n->size);
    return static_cast<ssize_t>(count);
}

auto tmpfs_get_size(ker::vfs::File* f) -> size_t {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return 0;
    }
    auto* n = static_cast<TmpNode*>(f->private_data);
    return n->size;
}

// --- FileOperations callbacks ---

auto tmpfs_fops_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t { return tmpfs_read(f, buf, count, offset); }

auto tmpfs_fops_write(ker::vfs::File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    return tmpfs_write(f, buf, count, offset);
}

auto tmpfs_fops_close(ker::vfs::File* f) -> int {
    (void)f;
    return 0;
}

auto tmpfs_fops_lseek(ker::vfs::File* f, off_t offset, int whence) -> off_t {
    if (f == nullptr) {
        return -1;
    }

    size_t file_size = tmpfs_get_size(f);
    off_t newpos = f->pos;

    switch (whence) {
        case 0:  // SEEK_SET
            newpos = offset;
            break;
        case 1:  // SEEK_CUR
            newpos = f->pos + offset;
            break;
        case 2:  // SEEK_END
            newpos = static_cast<off_t>(file_size) + offset;
            break;
        default:
            return -1;
    }

    if (newpos < 0) {
        return -1;
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
    if (f == nullptr || f->private_data == nullptr || entry == nullptr) {
        return -1;
    }

    auto* n = static_cast<TmpNode*>(f->private_data);

    if (n->type != TmpNodeType::DIRECTORY) {
        return -1;
    }

    // Indices 0 and 1 are synthetic "." and ".." entries
    if (index == 0) {
        entry->d_ino = reinterpret_cast<uint64_t>(n);
        entry->d_off = 1;
        entry->d_reclen = sizeof(DirEntry);
        entry->d_type = DT_DIR;
        entry->d_name[0] = '.';
        entry->d_name[1] = '\0';
        return 0;
    }
    if (index == 1) {
        TmpNode* parent = (n->parent != nullptr) ? n->parent : n;
        entry->d_ino = reinterpret_cast<uint64_t>(parent);
        entry->d_off = 2;
        entry->d_reclen = sizeof(DirEntry);
        entry->d_type = DT_DIR;
        entry->d_name[0] = '.';
        entry->d_name[1] = '.';
        entry->d_name[2] = '\0';
        return 0;
    }

    // Real children start at index 2
    size_t child_index = index - 2;

    if (child_index >= n->children_count) {
        return -1;
    }

    TmpNode* child = n->children[child_index];
    if (child == nullptr) {
        return -1;
    }

    entry->d_ino = reinterpret_cast<uint64_t>(child);
    entry->d_off = index + 1;
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
    while (child->name[name_len] != '\0' && name_len < DIRENT_NAME_MAX - 1) {
        entry->d_name[name_len] = child->name[name_len];
        name_len++;
    }
    entry->d_name[name_len] = '\0';

    return 0;
}

auto tmpfs_fops_readlink(ker::vfs::File* f, char* buf, size_t bufsize) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr || buf == nullptr || bufsize == 0) {
        return -1;
    }
    auto* n = static_cast<TmpNode*>(f->private_data);
    if (n->type != TmpNodeType::SYMLINK || n->symlink_target == nullptr) {
        return -1;
    }
    size_t len = 0;
    while (n->symlink_target[len] != '\0') {
        len++;
    }
    size_t to_copy = (len < bufsize) ? len : bufsize;
    memcpy(buf, n->symlink_target, to_copy);
    return static_cast<ssize_t>(to_copy);
}

// --- FileOperations instance ---

auto tmpfs_fops_truncate(ker::vfs::File* f, off_t length) -> int {
    if (f == nullptr || f->private_data == nullptr) return -1;
    auto* n = static_cast<TmpNode*>(f->private_data);
    if (n->type != TmpNodeType::FILE) return -EISDIR;
    auto new_size = static_cast<size_t>(length);
    if (new_size > n->capacity) {
        size_t newcap = (n->capacity == 0) ? 4096 : n->capacity;
        while (newcap < new_size) newcap *= 2;
        char* nd = new char[newcap];
        if (n->data != nullptr) {
            std::memcpy(nd, n->data, n->size);
        }
        delete[] n->data;
        n->data = nd;
        n->capacity = newcap;
    }
    if (new_size > n->size) {
        std::memset(n->data + n->size, 0, new_size - n->size);
    }
    n->size = new_size;
    return 0;
}

ker::vfs::FileOperations tmpfs_fops_instance = {.vfs_open = nullptr,
                                                .vfs_close = tmpfs_fops_close,
                                                .vfs_read = tmpfs_fops_read,
                                                .vfs_write = tmpfs_fops_write,
                                                .vfs_lseek = tmpfs_fops_lseek,
                                                .vfs_isatty = tmpfs_fops_isatty,
                                                .vfs_readdir = tmpfs_fops_readdir,
                                                .vfs_readlink = tmpfs_fops_readlink,
                                                .vfs_truncate = tmpfs_fops_truncate,
                                                .vfs_poll_check = nullptr};
}  // namespace

auto get_tmpfs_fops() -> ker::vfs::FileOperations* { return &tmpfs_fops_instance; }

}  // namespace ker::vfs::tmpfs
