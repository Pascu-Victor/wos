#include "initramfs.hpp"

#include <array>
#include <cstdint>
#include <cstring>
#include <platform/dbg/dbg.hpp>
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

using log = ker::mod::dbg::logger<"initramfs">;

auto parse_hex8(const char* s) -> uint64_t {
    uint64_t val = 0;
    for (int i = 0; i < 8; i++) {
        val <<= 4;
        char const C = s[i];
        if (C >= '0' && C <= '9') {
            val |= static_cast<uint64_t>(C - '0');
        } else if (C >= 'a' && C <= 'f') {
            val |= static_cast<uint64_t>(C - 'a' + 10);
        } else if (C >= 'A' && C <= 'F') {
            val |= static_cast<uint64_t>(C - 'A' + 10);
        }
    }
    return val;
}

auto align_up(size_t val, size_t alignment) -> size_t { return (val + alignment - 1) & ~(alignment - 1); }

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
auto split_and_create_parents(const char* path, char* leaf_out, size_t leaf_out_size) -> tmpfs::TmpNode* {
    std::array<char, PATH_BUF_SIZE> path_copy{};
    size_t plen = std::strlen(path);
    if (plen >= PATH_BUF_SIZE) {
        plen = PATH_BUF_SIZE - 1;
    }
    std::memcpy(path_copy.data(), path, plen);
    path_copy.at(plen) = '\0';

    // Find last slash
    char* last_slash = nullptr;
    for (char* p = path_copy.data(); *p != '\0'; p++) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    tmpfs::TmpNode* parent = nullptr;
    const char* leaf = path_copy.data();

    if (last_slash != nullptr) {
        *last_slash = '\0';
        parent = tmpfs::tmpfs_walk_path(path_copy.data(), true);
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
    const auto* buf = static_cast<const uint8_t*>(data);
    size_t offset = 0;
    int entry_count = 0;

    log::info("unpacking CPIO archive (%u bytes)", static_cast<unsigned>(size));

    while (offset + CPIO_HEADER_SIZE <= size) {
        const auto* hdr = reinterpret_cast<const char*>(buf + offset);

        // Validate magic "070701"
        if (hdr[0] != '0' || hdr[1] != '7' || hdr[2] != '0' || hdr[3] != '7' || hdr[4] != '0' || hdr[5] != '1') {
            log::error("invalid magic at offset 0x%x", static_cast<unsigned>(offset));
            return -EINVAL;
        }

        // Parse header fields
        // Offsets: magic(0,6) ino(6,8) mode(14,8) uid(22,8) gid(30,8)
        //          nlink(38,8) mtime(46,8) filesize(54,8) devmajor(62,8)
        //          devminor(70,8) rdevmajor(78,8) rdevminor(86,8)
        //          namesize(94,8) check(102,8)
        uint64_t const MODE = parse_hex8(hdr + 14);
        uint64_t const FILESIZE = parse_hex8(hdr + 54);
        uint64_t const NAMESIZE = parse_hex8(hdr + 94);

        // Filename starts after header
        size_t const NAME_OFFSET = offset + CPIO_HEADER_SIZE;
        const auto* name = reinterpret_cast<const char*>(buf + NAME_OFFSET);

        // Data starts after padded (header + filename)
        size_t const DATA_OFFSET = align_up(offset + CPIO_HEADER_SIZE + NAMESIZE, 4);
        const auto* file_data = buf + DATA_OFFSET;

        // Next entry after padded data
        size_t const NEXT_OFFSET = align_up(DATA_OFFSET + FILESIZE, 4);

        // Check for TRAILER
        if (NAMESIZE >= 11 && std::strncmp(name, "TRAILER!!!", 10) == 0) {
            break;
        }

        // Strip leading "./" or "/"
        const char* stripped = strip_path(name);
        if (stripped != nullptr && stripped[0] != '\0') {
            uint64_t const FILE_TYPE = MODE & S_IFMT;

            if (FILE_TYPE == S_IFLNK) {
                // Symlink: CPIO data is the target path
                std::array<char, PATH_BUF_SIZE> target{};
                size_t const TLEN = (FILESIZE < PATH_BUF_SIZE - 1) ? static_cast<size_t>(FILESIZE) : PATH_BUF_SIZE - 1;
                std::memcpy(target.data(), file_data, TLEN);
                target.at(TLEN) = '\0';

                std::array<char, tmpfs::TMPFS_NAME_MAX> leaf{};
                auto* parent = split_and_create_parents(stripped, leaf.data(), leaf.size());
                if (parent != nullptr) {
                    tmpfs::tmpfs_create_symlink(parent, leaf.data(), target.data());
                    log::debug("symlink %s -> %s", stripped, target.data());
                }
                entry_count++;

            } else if (FILE_TYPE == S_IFDIR) {
                tmpfs::tmpfs_walk_path(stripped, true);
                entry_count++;

            } else if (FILE_TYPE == S_IFREG) {
                std::array<char, tmpfs::TMPFS_NAME_MAX> leaf{};
                auto* parent = split_and_create_parents(stripped, leaf.data(), leaf.size());
                if (parent != nullptr) {
                    auto* fnode = tmpfs::tmpfs_create_file(parent, leaf.data(), static_cast<uint32_t>(MODE) & 07777);
                    if (fnode != nullptr && FILESIZE > 0) {
                        fnode->data = new char[FILESIZE];
                        if (fnode->data != nullptr) {
                            std::memcpy(fnode->data, file_data, static_cast<size_t>(FILESIZE));
                            fnode->size = static_cast<size_t>(FILESIZE);
                            fnode->capacity = static_cast<size_t>(FILESIZE);
                        }
                    }
                    entry_count++;
                    log::debug("file %s (%u bytes)", stripped, static_cast<unsigned>(FILESIZE));
                }
            }
        }

        offset = NEXT_OFFSET;
    }

    log::info("unpacked %d entries", entry_count);
    return entry_count;
}

}  // namespace ker::vfs::initramfs
