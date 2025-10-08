#include "tmpfs.hpp"

#include <string.h>

#include <mod/io/serial/serial.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <vfs/file.hpp>

// Local strcmp helper to avoid linking against libc strcmp in kernel
static int _kstrcmp(const char* a, const char* b) {
    if (!a || !b) return (a == b) ? 0 : (a ? 1 : -1);
    while (*a && *b) {
        if (*a != *b) return (unsigned char)*a - (unsigned char)*b;
        ++a;
        ++b;
    }
    return (unsigned char)*a - (unsigned char)*b;
}

namespace ker::vfs::tmpfs {

struct TmpNode {
    char* data;
    size_t size;
    size_t capacity;
    const char* name;
};

// single root file for now
static TmpNode* rootNode = nullptr;
// simple array of children (root-level only) for now
static TmpNode** children = nullptr;
static size_t children_count = 0;

void register_tmpfs() {
    mod::io::serial::write("tmpfs: register_tmpfs called\n");
    if (!rootNode) {
        rootNode = (TmpNode*)ker::mod::mm::dyn::kmalloc::malloc(sizeof(TmpNode));
        rootNode->data = nullptr;
        rootNode->size = 0;
        rootNode->capacity = 0;
        rootNode->name = "/";
    }
}

// Create a File object representing root
ker::vfs::File* create_root_file() {
    ker::vfs::File* f = (ker::vfs::File*)ker::mod::mm::dyn::kmalloc::malloc(sizeof(ker::vfs::File));
    f->private_data = rootNode;
    f->fd = -1;
    f->pos = 0;
    return f;
}

static TmpNode* create_node_for_name(const char* name) {
    TmpNode* n = (TmpNode*)ker::mod::mm::dyn::kmalloc::malloc(sizeof(TmpNode));
    n->data = nullptr;
    n->size = 0;
    n->capacity = 0;
    n->name = name;
    // append to children list
    size_t newcount = children_count + 1;
    TmpNode** newarr = (TmpNode**)ker::mod::mm::dyn::kmalloc::malloc(sizeof(TmpNode*) * newcount);
    for (size_t i = 0; i < children_count; ++i) newarr[i] = children[i];
    newarr[children_count] = n;
    children = newarr;
    children_count = newcount;
    return n;
}

static TmpNode* find_child_by_name(const char* name) {
    if (!children) return nullptr;
    for (size_t i = 0; i < children_count; ++i) {
        // simple pointer compare or strcmp if needed
        const char* nname = children[i]->name;
        if (!nname) continue;
        // compare by string
        if (_kstrcmp(nname, name) == 0) return children[i];
    }
    return nullptr;
}

ker::vfs::File* tmpfs_open_path(const char* path, int flags, int mode) {
    if (!path) return nullptr;
    if (_kstrcmp(path, "/") == 0) {
        return create_root_file();
    }
    // accept paths like "/name"
    if (path[0] != '/') return nullptr;
    const char* name = path + 1;
    if (name[0] == '\0') return create_root_file();
    TmpNode* n = find_child_by_name(name);
    if (!n) {
        // create node by name
        n = create_node_for_name(name);
    }
    ker::vfs::File* f = (ker::vfs::File*)ker::mod::mm::dyn::kmalloc::malloc(sizeof(ker::vfs::File));
    f->private_data = n;
    f->fd = -1;
    f->pos = 0;
    return f;
}

ssize_t tmpfs_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) {
    if (!f || !f->private_data) return -1;
    TmpNode* n = (TmpNode*)f->private_data;
    if (offset >= n->size) return 0;
    size_t toread = n->size - offset;
    if (toread > count) toread = count;
    memcpy(buf, n->data + offset, toread);
    return (ssize_t)toread;
}

ssize_t tmpfs_write(ker::vfs::File* f, const void* buf, size_t count, size_t offset) {
    if (!f || !f->private_data) return -1;
    TmpNode* n = (TmpNode*)f->private_data;
    size_t need = offset + count;
    if (need > n->capacity) {
        size_t newcap = (n->capacity == 0) ? 4096 : n->capacity;
        while (newcap < need) newcap *= 2;
        char* nd = (char*)ker::mod::mm::dyn::kmalloc::malloc(newcap);
        if (n->data) memcpy(nd, n->data, n->size);
        n->data = nd;
        n->capacity = newcap;
    }
    memcpy(n->data + offset, buf, count);
    if (need > n->size) n->size = need;
    return (ssize_t)count;
}

size_t tmpfs_get_size(ker::vfs::File* f) {
    if (!f || !f->private_data) return 0;
    TmpNode* n = (TmpNode*)f->private_data;
    return n->size;
}

}  // namespace ker::vfs::tmpfs
