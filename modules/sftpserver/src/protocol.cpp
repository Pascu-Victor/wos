#include "protocol.hpp"

#include <errno.h>

namespace sftpserver {

auto PacketReader::u8() -> uint8_t {
    if (pos + 1 > data.size()) {
        ok = false;
        return 0;
    }
    return data[pos++];
}

auto PacketReader::u32() -> uint32_t {
    if (pos + 4 > data.size()) {
        ok = false;
        return 0;
    }
    uint32_t value = (static_cast<uint32_t>(data[pos]) << 24) | (static_cast<uint32_t>(data[pos + 1]) << 16) |
                     (static_cast<uint32_t>(data[pos + 2]) << 8) | static_cast<uint32_t>(data[pos + 3]);
    pos += 4;
    return value;
}

auto PacketReader::u64() -> uint64_t {
    uint64_t high = u32();
    uint64_t low = u32();
    return (high << 32) | low;
}

auto PacketReader::byte_span() -> std::span<const uint8_t> {
    uint32_t len = u32();
    if (!ok || pos + len > data.size()) {
        ok = false;
        return {};
    }
    std::span<const uint8_t> out = data.subspan(pos, len);
    pos += len;
    return out;
}

auto PacketReader::bytes() -> std::vector<uint8_t> {
    std::span<const uint8_t> raw = byte_span();
    return {raw.begin(), raw.end()};
}

auto PacketReader::string() -> std::string {
    std::span<const uint8_t> raw = byte_span();
    if (raw.empty()) {
        return {};
    }
    return std::string(reinterpret_cast<const char*>(raw.data()), raw.size());
}

auto PacketReader::attrs() -> Attributes {
    Attributes out{};
    out.flags = u32();
    if ((out.flags & SSH_FILEXFER_ATTR_SIZE) != 0) {
        out.size = u64();
    }
    if ((out.flags & SSH_FILEXFER_ATTR_UIDGID) != 0) {
        out.uid = u32();
        out.gid = u32();
    }
    if ((out.flags & SSH_FILEXFER_ATTR_PERMISSIONS) != 0) {
        out.permissions = u32();
    }
    if ((out.flags & SSH_FILEXFER_ATTR_ACMODTIME) != 0) {
        out.atime = u32();
        out.mtime = u32();
    }
    if ((out.flags & SSH_FILEXFER_ATTR_EXTENDED) != 0) {
        uint32_t count = u32();
        for (uint32_t i = 0; ok && i < count; ++i) {
            (void)string();
            (void)string();
        }
    }
    return out;
}

PacketWriter::PacketWriter(uint8_t type) { u8(type); }

void PacketWriter::u8(uint8_t value) { body.push_back(value); }

void PacketWriter::u32(uint32_t value) {
    body.push_back(static_cast<uint8_t>((value >> 24) & 0xffU));
    body.push_back(static_cast<uint8_t>((value >> 16) & 0xffU));
    body.push_back(static_cast<uint8_t>((value >> 8) & 0xffU));
    body.push_back(static_cast<uint8_t>(value & 0xffU));
}

void PacketWriter::u64(uint64_t value) {
    u32(static_cast<uint32_t>(value >> 32));
    u32(static_cast<uint32_t>(value & 0xffffffffULL));
}

void PacketWriter::string(std::string_view value) {
    u32(static_cast<uint32_t>(value.size()));
    body.insert(body.end(), value.begin(), value.end());
}

void PacketWriter::bytes(std::span<const uint8_t> value) {
    u32(static_cast<uint32_t>(value.size()));
    body.insert(body.end(), value.begin(), value.end());
}

void PacketWriter::attrs(const Attributes& value) {
    u32(value.flags & ~SSH_FILEXFER_ATTR_EXTENDED);
    if ((value.flags & SSH_FILEXFER_ATTR_SIZE) != 0) {
        u64(value.size);
    }
    if ((value.flags & SSH_FILEXFER_ATTR_UIDGID) != 0) {
        u32(value.uid);
        u32(value.gid);
    }
    if ((value.flags & SSH_FILEXFER_ATTR_PERMISSIONS) != 0) {
        u32(value.permissions);
    }
    if ((value.flags & SSH_FILEXFER_ATTR_ACMODTIME) != 0) {
        u32(value.atime);
        u32(value.mtime);
    }
}

auto status_from_errno(int err) -> uint32_t {
    switch (err) {
        case 0:
            return SSH_FX_OK;
        case ENOENT:
        case ENOTDIR:
            return SSH_FX_NO_SUCH_FILE;
        case EACCES:
        case EPERM:
            return SSH_FX_PERMISSION_DENIED;
        default:
            return SSH_FX_FAILURE;
    }
}

auto status_text(uint32_t status) -> std::string_view {
    switch (status) {
        case SSH_FX_OK:
            return "OK";
        case SSH_FX_EOF:
            return "End of file";
        case SSH_FX_NO_SUCH_FILE:
            return "No such file";
        case SSH_FX_PERMISSION_DENIED:
            return "Permission denied";
        case SSH_FX_BAD_MESSAGE:
            return "Bad message";
        case SSH_FX_OP_UNSUPPORTED:
            return "Operation unsupported";
        default:
            return "Failure";
    }
}

}  // namespace sftpserver
