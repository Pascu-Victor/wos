#include "initramfs.hpp"

#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <vfs/fs/tmpfs.hpp>

namespace ker::vfs::initramfs {

namespace {

constexpr size_t CPIO_HEADER_SIZE = 110;
constexpr size_t PATH_BUF_SIZE = 512;

// Unix file type mask and constants
constexpr uint64_t S_IFMT = 0170000;
constexpr uint64_t S_IFDIR = 0040000;
constexpr uint64_t S_IFREG = 0100000;
constexpr uint64_t S_IFLNK = 0120000;

auto parse_hex8(const char* s) -> uint64_t {
    uint64_t val = 0;
    for (int i = 0; i < 8; i++) {
        val <<= 4;
        char c = s[i];
        if (c >= '0' && c <= '9') {
            val |= static_cast<uint64_t>(c - '0');
        } else if (c >= 'a' && c <= 'f') {
            val |= static_cast<uint64_t>(c - 'a' + 10);
        } else if (c >= 'A' && c <= 'F') {
            val |= static_cast<uint64_t>(c - 'A' + 10);
        }
    }
    return val;
}

auto align_up(size_t val, size_t alignment) -> size_t {
    return (val + alignment - 1) & ~(alignment - 1);
}

// Strip leading "./" or "/" from CPIO path.
// Returns nullptr for root directory entry (".").
auto strip_path(const char* name) -> const char* {
    if (name[0] == '.' && name[1] == '/') {
        return name + 2;
    }
    if (name[0] == '/') {
        return name + 1;
    }
    if (name[0] == '.' && name[1] == '\0') {
        return nullptr;  // root dir entry
    }
    return name;
}

// Split a path into parent + leaf, creating parent directories.
// Writes the leaf name into leaf_out.
// Returns the parent TmpNode, or nullptr on failure.
auto split_and_create_parents(const char* path, char* leaf_out,
                              size_t leaf_out_size) -> tmpfs::TmpNode* {
    char path_copy[PATH_BUF_SIZE]{};
    size_t plen = std::strlen(path);
    if (plen >= PATH_BUF_SIZE) {
        plen = PATH_BUF_SIZE - 1;
    }
    std::memcpy(path_copy, path, plen);
    path_copy[plen] = '\0';

    // Find last slash
    char* last_slash = nullptr;
    for (char* p = path_copy; *p != '\0'; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    tmpfs::TmpNode* parent = nullptr;
    const char* leaf = path_copy;

    if (last_slash != nullptr) {
        *last_slash = '\0';
        parent = tmpfs::tmpfs_walk_path(path_copy, true);
        leaf = last_slash + 1;
    } else {
        parent = tmpfs::get_root_node();
    }

    size_t leaf_len = std::strlen(leaf);
    if (leaf_len >= leaf_out_size) {
        leaf_len = leaf_out_size - 1;
    }
    std::memcpy(leaf_out, leaf, leaf_len);
    leaf_out[leaf_len] = '\0';

    return parent;
}

}  // namespace

auto unpack_initramfs(const void* data, size_t size) -> int {
    auto* buf = static_cast<const uint8_t*>(data);
    size_t offset = 0;
    int entry_count = 0;

    ker::mod::dbg::log("initramfs: unpacking CPIO archive (%u bytes)",
                       static_cast<unsigned>(size));

    while (offset + CPIO_HEADER_SIZE <= size) {
        auto* hdr = reinterpret_cast<const char*>(buf + offset);

        // Validate magic "070701"
        if (hdr[0] != '0' || hdr[1] != '7' || hdr[2] != '0' ||
            hdr[3] != '7' || hdr[4] != '0' || hdr[5] != '1') {
            ker::mod::dbg::log("initramfs: invalid magic at offset 0x%x",
                               static_cast<unsigned>(offset));
            return -1;
        }

        // Parse header fields
        // Offsets: magic(0,6) ino(6,8) mode(14,8) uid(22,8) gid(30,8)
        //          nlink(38,8) mtime(46,8) filesize(54,8) devmajor(62,8)
        //          devminor(70,8) rdevmajor(78,8) rdevminor(86,8)
        //          namesize(94,8) check(102,8)
        uint64_t mode = parse_hex8(hdr + 14);
        uint64_t filesize = parse_hex8(hdr + 54);
        uint64_t namesize = parse_hex8(hdr + 94);

        // Filename starts after header
        size_t name_offset = offset + CPIO_HEADER_SIZE;
        auto* name = reinterpret_cast<const char*>(buf + name_offset);

        // Data starts after padded (header + filename)
        size_t data_offset = align_up(offset + CPIO_HEADER_SIZE + namesize, 4);
        auto* file_data = buf + data_offset;

        // Next entry after padded data
        size_t next_offset = align_up(data_offset + filesize, 4);

        // Check for TRAILER
        if (namesize >= 11 &&
            std::strncmp(name, "TRAILER!!!", 10) == 0) {
            break;
        }

        // Strip leading "./" or "/"
        const char* stripped = strip_path(name);
        if (stripped != nullptr && stripped[0] != '\0') {
            uint64_t file_type = mode & S_IFMT;

            if (file_type == S_IFLNK) {
                // Symlink: CPIO data is the target path
                char target[PATH_BUF_SIZE]{};
                size_t tlen = (filesize < PATH_BUF_SIZE - 1)
                                  ? static_cast<size_t>(filesize)
                                  : PATH_BUF_SIZE - 1;
                std::memcpy(target, file_data, tlen);
                target[tlen] = '\0';

                char leaf[tmpfs::TMPFS_NAME_MAX]{};
                auto* parent = split_and_create_parents(stripped, leaf,
                                                        tmpfs::TMPFS_NAME_MAX);
                if (parent != nullptr) {
                    tmpfs::tmpfs_create_symlink(parent, leaf, target);
                    ker::mod::dbg::log("initramfs: symlink %s -> %s",
                                       stripped, target);
                }
                entry_count++;

            } else if (file_type == S_IFDIR) {
                tmpfs::tmpfs_walk_path(stripped, true);
                entry_count++;

            } else if (file_type == S_IFREG) {
                char leaf[tmpfs::TMPFS_NAME_MAX]{};
                auto* parent = split_and_create_parents(stripped, leaf,
                                                        tmpfs::TMPFS_NAME_MAX);
                if (parent != nullptr) {
                    auto* fnode = tmpfs::tmpfs_create_file(parent, leaf);
                    if (fnode != nullptr && filesize > 0) {
                        fnode->data = static_cast<char*>(
                            ker::mod::mm::dyn::kmalloc::malloc(
                                static_cast<size_t>(filesize)));
                        if (fnode->data != nullptr) {
                            std::memcpy(fnode->data, file_data,
                                        static_cast<size_t>(filesize));
                            fnode->size = static_cast<size_t>(filesize);
                            fnode->capacity = static_cast<size_t>(filesize);
                        }
                    }
                    entry_count++;
                    ker::mod::dbg::log("initramfs: file %s (%u bytes)",
                                       stripped,
                                       static_cast<unsigned>(filesize));
                }
            }
        }

        offset = next_offset;
    }

    ker::mod::dbg::log("initramfs: unpacked %d entries", entry_count);
    return entry_count;
}

}  // namespace ker::vfs::initramfs
