#pragma once

#include <stddef.h>
#include <stdint.h>

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace sftpserver {

constexpr uint32_t MAX_PACKET_SIZE = 16U * 1024U * 1024U;
constexpr uint32_t MAX_READ_SIZE = 256U * 1024U;
constexpr uint32_t READDIR_BATCH = 64;
constexpr uint64_t WRITE_RESERVE_CHUNK = 8ULL * 1024ULL * 1024ULL;

enum PacketType : uint8_t {
    SSH_FXP_INIT = 1,
    SSH_FXP_VERSION = 2,
    SSH_FXP_OPEN = 3,
    SSH_FXP_CLOSE = 4,
    SSH_FXP_READ = 5,
    SSH_FXP_WRITE = 6,
    SSH_FXP_LSTAT = 7,
    SSH_FXP_FSTAT = 8,
    SSH_FXP_SETSTAT = 9,
    SSH_FXP_FSETSTAT = 10,
    SSH_FXP_OPENDIR = 11,
    SSH_FXP_READDIR = 12,
    SSH_FXP_REMOVE = 13,
    SSH_FXP_MKDIR = 14,
    SSH_FXP_RMDIR = 15,
    SSH_FXP_REALPATH = 16,
    SSH_FXP_STAT = 17,
    SSH_FXP_RENAME = 18,
    SSH_FXP_READLINK = 19,
    SSH_FXP_SYMLINK = 20,
    SSH_FXP_STATUS = 101,
    SSH_FXP_HANDLE = 102,
    SSH_FXP_DATA = 103,
    SSH_FXP_NAME = 104,
    SSH_FXP_ATTRS = 105,
    SSH_FXP_EXTENDED = 200,
};

enum StatusCode : uint32_t {
    SSH_FX_OK = 0,
    SSH_FX_EOF = 1,
    SSH_FX_NO_SUCH_FILE = 2,
    SSH_FX_PERMISSION_DENIED = 3,
    SSH_FX_FAILURE = 4,
    SSH_FX_BAD_MESSAGE = 5,
    SSH_FX_OP_UNSUPPORTED = 8,
};

enum AttrFlags : uint32_t {
    SSH_FILEXFER_ATTR_SIZE = 0x00000001,
    SSH_FILEXFER_ATTR_UIDGID = 0x00000002,
    SSH_FILEXFER_ATTR_PERMISSIONS = 0x00000004,
    SSH_FILEXFER_ATTR_ACMODTIME = 0x00000008,
    SSH_FILEXFER_ATTR_EXTENDED = 0x80000000,
};

enum OpenFlags : uint32_t {
    SSH_FXF_READ = 0x00000001,
    SSH_FXF_WRITE = 0x00000002,
    SSH_FXF_APPEND = 0x00000004,
    SSH_FXF_CREAT = 0x00000008,
    SSH_FXF_TRUNC = 0x00000010,
    SSH_FXF_EXCL = 0x00000020,
};

struct Attributes {
    uint32_t flags = 0;
    uint64_t size = 0;
    uint32_t uid = 0;
    uint32_t gid = 0;
    uint32_t permissions = 0;
    uint32_t atime = 0;
    uint32_t mtime = 0;
};

struct DirEntry {
    std::string name;
    std::string longname;
    Attributes attrs;
};

struct PacketReader {
    std::span<const uint8_t> data;
    size_t pos = 0;
    bool ok = true;

    auto u8() -> uint8_t;
    auto u32() -> uint32_t;
    auto u64() -> uint64_t;
    auto byte_span() -> std::span<const uint8_t>;
    auto bytes() -> std::vector<uint8_t>;
    auto string() -> std::string;
    auto attrs() -> Attributes;
};

struct PacketWriter {
    std::vector<uint8_t> body;

    explicit PacketWriter(uint8_t type);

    void u8(uint8_t value);
    void u32(uint32_t value);
    void u64(uint64_t value);
    void string(std::string_view value);
    void bytes(std::span<const uint8_t> value);
    void attrs(const Attributes& value);
};

auto status_from_errno(int err) -> uint32_t;
auto status_text(uint32_t status) -> std::string_view;

}  // namespace sftpserver
