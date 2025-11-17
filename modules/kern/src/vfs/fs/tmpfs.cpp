#include "tmpfs.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <vfs/file.hpp>

#include "vfs/file_operations.hpp"

namespace {
// Local strcmp helper to avoid linking against libc strcmp in kernel
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

}  // namespace

namespace ker::vfs::tmpfs {

constexpr size_t DEFAULT_TMPFS_BLOCK_SIZE = 4096;

enum class TmpNodeType : uint8_t { FILE, DIRECTORY };

struct TmpNode {
    char* data;
    size_t size;
    size_t capacity;
    const char* name;
    TmpNodeType type;
    TmpNode** children;  // For directories
    size_t children_count;
};
namespace {

// single root file for now
TmpNode* rootNode = nullptr;
// simple array of children (root-level only) for now
TmpNode** children = nullptr;
size_t children_count = 0;

auto create_node_for_name(const char* name, TmpNodeType type = TmpNodeType::FILE) -> TmpNode* {
    auto* n = new TmpNode;
    n->data = nullptr;
    n->size = 0;
    n->capacity = 0;
    n->name = name;
    n->type = type;
    n->children = nullptr;
    n->children_count = 0;
    // append to children list
    size_t newcount = children_count + 1;
    auto** newarr = new TmpNode*;
    for (size_t i = 0; i < children_count; ++i) {
        newarr[i] = children[i];
    }
    newarr[children_count] = n;
    children = newarr;
    children_count = newcount;
    return n;
}

auto find_child_by_name(const char* name) -> TmpNode* {
    if (children == nullptr) {
        return nullptr;
    }
    for (size_t i = 0; i < children_count; ++i) {
        // simple pointer compare or strcmp if needed
        const char* nname = children[i]->name;
        if (nname == nullptr) {
            continue;
        }
        // compare by string
        if (_kstrcmp(nname, name) == 0) {
            return children[i];
        }
    }
    return nullptr;
}
}  // namespace

void register_tmpfs() {
    mod::io::serial::write("tmpfs: register_tmpfs called\n");
    if (rootNode == nullptr) {
        rootNode = new TmpNode;
        rootNode->data = nullptr;
        rootNode->size = 0;
        rootNode->capacity = 0;
        rootNode->name = "/";
        rootNode->type = TmpNodeType::DIRECTORY;
        rootNode->children = nullptr;
        rootNode->children_count = 0;
    }
}

// Create a File object representing root
auto create_root_file() -> ker::vfs::File* {
    File* f = new File;
    f->private_data = rootNode;
    f->fd = -1;
    f->pos = 0;
    f->is_directory = true;
    f->fs_type = FSType::TMPFS;
    f->refcount = 1;
    return f;
}

auto tmpfs_open_path(const char* path, int flags, int mode) -> ker::vfs::File* {
    if (path == nullptr) {
        return nullptr;
    }
    if (_kstrcmp(path, "/") == 0) {
        return create_root_file();
    }
    // accept paths like "/name"
    if (path[0] != '/') {
        return nullptr;
    }
    const char* name = path + 1;
    if (name[0] == '\0') {
        return create_root_file();
    }
    TmpNode* n = find_child_by_name(name);
    if (n == nullptr) {
        // create node by name - default to FILE type
        n = create_node_for_name(name, TmpNodeType::FILE);
    }
    File* f = new File;
    f->private_data = n;
    f->fd = -1;
    f->pos = 0;
    f->is_directory = (n->type == TmpNodeType::DIRECTORY);
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
    return (ssize_t)toRead;
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
        delete n->data;
        n->data = nd;
        n->capacity = newcap;
    }
    memcpy(n->data + offset, buf, count);
    n->size = std::max(need, n->size);
    return (ssize_t)count;
}

auto tmpfs_get_size(ker::vfs::File* f) -> size_t {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return 0;
    }
    auto* n = static_cast<TmpNode*>(f->private_data);
    return n->size;
}

// FileOperations callback implementations
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
            newpos = (off_t)file_size + offset;
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

auto tmpfs_fops_readdir(ker::vfs::File* f, DirEntry* entry, size_t index) -> int {
    if (f == nullptr || f->private_data == nullptr || entry == nullptr) {
        return -1;
    }

    auto* n = static_cast<TmpNode*>(f->private_data);

    // Check if this is a directory
    if (n->type != TmpNodeType::DIRECTORY) {
        return -1;
    }

    // Check if index is valid
    if (index >= n->children_count) {
        return -1;  // No more entries
    }

    // Get the child at this index
    TmpNode* child = n->children[index];
    if (child == nullptr) {
        return -1;
    }

    // Fill in the dirent structure
    entry->d_ino = reinterpret_cast<uint64_t>(child);  // Use pointer as inode
    entry->d_off = index + 1;
    entry->d_reclen = sizeof(DirEntry);
    entry->d_type = (child->type == TmpNodeType::DIRECTORY) ? DT_DIR : DT_REG;

    // Copy filename
    size_t name_len = 0;
    if (child->name != nullptr) {
        while (child->name[name_len] != '\0' && name_len < DIRENT_NAME_MAX - 1) {
            entry->d_name[name_len] = child->name[name_len];
            name_len++;
        }
    }
    entry->d_name[name_len] = '\0';

    return 0;
}

// Static storage for tmpfs FileOperations
namespace {
ker::vfs::FileOperations tmpfs_fops_instance = {.vfs_open = nullptr,  // vfs_open - will be set by register_tmpfs
                                                .vfs_close = tmpfs_fops_close,
                                                .vfs_read = tmpfs_fops_read,
                                                .vfs_write = tmpfs_fops_write,
                                                .vfs_lseek = tmpfs_fops_lseek,
                                                .vfs_isatty = tmpfs_fops_isatty,
                                                .vfs_readdir = tmpfs_fops_readdir};
}

auto get_tmpfs_fops() -> ker::vfs::FileOperations* { return &tmpfs_fops_instance; }

}  // namespace ker::vfs::tmpfs
