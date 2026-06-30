#include "server.hpp"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "filesystem.hpp"
#include "transport.hpp"

namespace sftpserver {
namespace {

void send_status(uint32_t id, uint32_t status) {
    PacketWriter out(SSH_FXP_STATUS);
    out.u32(id);
    out.u32(status);
    out.string(status_text(status));
    out.string("");
    (void)send_packet(out);
}

void send_errno(uint32_t id, int err) { send_status(id, status_from_errno(err)); }

void send_handle(uint32_t id, char kind, uint32_t handle_id) {
    PacketWriter out(SSH_FXP_HANDLE);
    out.u32(id);
    std::string encoded;
    encoded.push_back(kind);
    encoded += std::to_string(handle_id);
    out.string(encoded);
    (void)send_packet(out);
}

auto decode_handle(std::string_view encoded, char expected_kind, uint32_t& id) -> bool {
    if (encoded.size() < 2 || encoded.front() != expected_kind) {
        return false;
    }
    uint32_t value = 0;
    for (size_t i = 1; i < encoded.size(); ++i) {
        char ch = encoded[i];
        if (ch < '0' || ch > '9') {
            return false;
        }
        uint32_t digit = static_cast<uint32_t>(ch - '0');
        if (value > (UINT32_MAX - digit) / 10U) {
            return false;
        }
        value = value * 10U + digit;
    }
    id = value;
    return true;
}

void send_attrs(uint32_t id, const Attributes& attrs) {
    PacketWriter out(SSH_FXP_ATTRS);
    out.u32(id);
    out.attrs(attrs);
    (void)send_packet(out);
}

void send_name(uint32_t id, std::span<const DirEntry> entries) {
    PacketWriter out(SSH_FXP_NAME);
    out.u32(id);
    out.u32(static_cast<uint32_t>(entries.size()));
    for (const auto& entry : entries) {
        out.string(entry.name);
        out.string(entry.longname);
        out.attrs(entry.attrs);
    }
    (void)send_packet(out);
}

}  // namespace

Server::~Server() { close_all_handles(); }

void Server::run() {
    std::vector<uint8_t> packet;
    if (!read_packet(packet)) {
        return;
    }

    PacketReader init{std::span<const uint8_t>(packet.data(), packet.size())};
    if (init.u8() != SSH_FXP_INIT || !init.ok) {
        return;
    }

    PacketWriter version(SSH_FXP_VERSION);
    version.u32(3);
    if (!send_packet(version)) {
        return;
    }

    while (read_packet(packet)) {
        handle_packet(packet);
    }
}

auto Server::add_handle(Handle handle) -> uint32_t {
    uint32_t id = next_handle_++;
    if (next_handle_ == 0) {
        next_handle_ = 1;
    }
    handles_.emplace(id, std::move(handle));
    return id;
}

auto Server::find_file(std::string_view encoded) -> FileHandle* {
    uint32_t id = 0;
    if (!decode_handle(encoded, 'F', id)) {
        return nullptr;
    }
    auto it = handles_.find(id);
    if (it == handles_.end()) {
        return nullptr;
    }
    return std::get_if<FileHandle>(&it->second);
}

auto Server::find_dir(std::string_view encoded) -> DirectoryHandle* {
    uint32_t id = 0;
    if (!decode_handle(encoded, 'D', id)) {
        return nullptr;
    }
    auto it = handles_.find(id);
    if (it == handles_.end()) {
        return nullptr;
    }
    return std::get_if<DirectoryHandle>(&it->second);
}

auto Server::close_handle(std::string_view encoded) -> bool {
    if (encoded.size() < 2) {
        return false;
    }
    uint32_t id = 0;
    char kind = encoded.front();
    if ((kind != 'F' && kind != 'D') || !decode_handle(encoded, kind, id)) {
        return false;
    }
    auto it = handles_.find(id);
    if (it == handles_.end()) {
        return false;
    }
    if (auto* file = std::get_if<FileHandle>(&it->second); file != nullptr) {
        if (!close_file(*file)) {
            return false;
        }
    }
    handles_.erase(it);
    return true;
}

auto Server::close_file(FileHandle& file) -> bool {
    if (file.fd < 0) {
        return true;
    }
    bool ok = true;
    if (file.reserve_writes && file.reserved_size != file.logical_size && ftruncate(file.fd, static_cast<off_t>(file.logical_size)) != 0) {
        ok = false;
    }
    if (close(file.fd) != 0) {
        ok = false;
    }
    file.fd = -1;
    return ok;
}

void Server::close_all_handles() {
    for (auto& [id, handle] : handles_) {
        (void)id;
        if (auto* file = std::get_if<FileHandle>(&handle); file != nullptr) {
            (void)close_file(*file);
        }
    }
    handles_.clear();
}

auto Server::reserve_for_write(FileHandle& file, uint64_t end_offset) -> int {
    if (!file.reserve_writes || end_offset <= file.reserved_size) {
        return 0;
    }

    uint64_t const TARGET = round_up(end_offset, WRITE_RESERVE_CHUNK);
    if (TARGET < end_offset || TARGET > static_cast<uint64_t>(LLONG_MAX)) {
        return EFBIG;
    }
    if (ftruncate(file.fd, static_cast<off_t>(TARGET)) != 0) {
        return errno;
    }
    file.reserved_size = TARGET;
    return 0;
}

void Server::handle_packet(const std::vector<uint8_t>& packet) {
    PacketReader reader{std::span<const uint8_t>(packet.data(), packet.size())};
    uint8_t type = reader.u8();
    uint32_t id = reader.u32();
    if (!reader.ok) {
        send_status(0, SSH_FX_BAD_MESSAGE);
        return;
    }

    switch (type) {
        case SSH_FXP_OPEN:
            handle_open(id, reader);
            break;
        case SSH_FXP_CLOSE:
            handle_close(id, reader);
            break;
        case SSH_FXP_READ:
            handle_read(id, reader);
            break;
        case SSH_FXP_WRITE:
            handle_write(id, reader);
            break;
        case SSH_FXP_LSTAT:
            handle_stat(id, reader, false);
            break;
        case SSH_FXP_STAT:
            handle_stat(id, reader, true);
            break;
        case SSH_FXP_FSTAT:
            handle_fstat(id, reader);
            break;
        case SSH_FXP_SETSTAT:
            handle_setstat(id, reader);
            break;
        case SSH_FXP_FSETSTAT:
            handle_fsetstat(id, reader);
            break;
        case SSH_FXP_OPENDIR:
            handle_opendir(id, reader);
            break;
        case SSH_FXP_READDIR:
            handle_readdir(id, reader);
            break;
        case SSH_FXP_REMOVE:
            handle_remove(id, reader);
            break;
        case SSH_FXP_MKDIR:
            handle_mkdir(id, reader);
            break;
        case SSH_FXP_RMDIR:
            handle_rmdir(id, reader);
            break;
        case SSH_FXP_REALPATH:
            handle_realpath(id, reader);
            break;
        case SSH_FXP_RENAME:
            handle_rename(id, reader);
            break;
        case SSH_FXP_READLINK:
            handle_readlink(id, reader);
            break;
        case SSH_FXP_SYMLINK:
            handle_symlink(id, reader);
            break;
        case SSH_FXP_EXTENDED:
            handle_extended(id, reader);
            break;
        default:
            send_status(id, SSH_FX_OP_UNSUPPORTED);
            break;
    }
}

void Server::handle_open(uint32_t id, PacketReader& reader) {
    std::string path = reader.string();
    uint32_t pflags = reader.u32();
    Attributes attrs = reader.attrs();
    if (!reader.ok || has_nul(path)) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }

    int flags = 0;
    bool want_read = (pflags & SSH_FXF_READ) != 0;
    bool want_write = (pflags & SSH_FXF_WRITE) != 0;
    if (want_read && want_write) {
        flags |= O_RDWR;
    } else if (want_write) {
        flags |= O_WRONLY;
    } else {
        flags |= O_RDONLY;
    }
    if ((pflags & SSH_FXF_APPEND) != 0) {
        flags |= O_APPEND;
    }
    if ((pflags & SSH_FXF_CREAT) != 0) {
        flags |= O_CREAT;
    }
    if ((pflags & SSH_FXF_TRUNC) != 0) {
        flags |= O_TRUNC;
    }
    if ((pflags & SSH_FXF_EXCL) != 0) {
        flags |= O_EXCL;
    }

    mode_t mode = (attrs.flags & SSH_FILEXFER_ATTR_PERMISSIONS) != 0 ? static_cast<mode_t>(attrs.permissions) : 0644;
    int fd = open(path.c_str(), flags, mode);
    if (fd < 0) {
        send_errno(id, errno);
        return;
    }
    uint64_t logical_size = 0;
    bool sendfile_reads = false;
    struct stat st{};
    if (fstat(fd, &st) == 0) {
        sendfile_reads = want_read && !want_write && S_ISREG(st.st_mode);
        if (st.st_size > 0) {
            logical_size = static_cast<uint64_t>(st.st_size);
        }
    }
    if (want_write && (attrs.flags & SSH_FILEXFER_ATTR_SIZE) != 0) {
        if (attrs.size > static_cast<uint64_t>(LLONG_MAX) || ftruncate(fd, static_cast<off_t>(attrs.size)) != 0) {
            int const SAVED_ERRNO = attrs.size > static_cast<uint64_t>(LLONG_MAX) ? EFBIG : errno;
            close(fd);
            send_errno(id, SAVED_ERRNO);
            return;
        }
        logical_size = attrs.size;
    }
    bool const RESERVE_WRITES = want_write && !want_read && (pflags & SSH_FXF_APPEND) == 0;
    send_handle(id, 'F',
                add_handle(FileHandle{
                    .fd = fd,
                    .logical_size = logical_size,
                    .reserved_size = logical_size,
                    .reserve_writes = RESERVE_WRITES,
                    .sendfile_reads = sendfile_reads,
                }));
}

void Server::handle_close(uint32_t id, PacketReader& reader) {
    std::string handle = reader.string();
    if (!reader.ok || !close_handle(handle)) {
        send_status(id, SSH_FX_FAILURE);
        return;
    }
    send_status(id, SSH_FX_OK);
}

void Server::handle_read(uint32_t id, PacketReader& reader) {
    std::string handle = reader.string();
    uint64_t offset = reader.u64();
    uint32_t len = reader.u32();
    if (!reader.ok) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    FileHandle* file = find_file(handle);
    if (file == nullptr) {
        send_status(id, SSH_FX_FAILURE);
        return;
    }
    len = std::min(len, MAX_READ_SIZE);
    if (file->sendfile_reads) {
        struct stat st{};
        if (fstat(file->fd, &st) != 0) {
            send_errno(id, errno);
            return;
        }
        if (st.st_size <= 0) {
            send_status(id, SSH_FX_EOF);
            return;
        }
        auto const FILE_SIZE = static_cast<uint64_t>(st.st_size);
        if (offset >= FILE_SIZE) {
            send_status(id, SSH_FX_EOF);
            return;
        }
        uint64_t const AVAILABLE = FILE_SIZE - offset;
        auto const COUNT = static_cast<size_t>(std::min<uint64_t>(len, AVAILABLE));
        (void)send_file_data_packet(id, file->fd, offset, COUNT);
        return;
    }
    read_buffer_.resize(len);
    ssize_t count = pread(file->fd, read_buffer_.data(), read_buffer_.size(), static_cast<off_t>(offset));
    if (count < 0) {
        send_errno(id, errno);
        return;
    }
    if (count == 0) {
        send_status(id, SSH_FX_EOF);
        return;
    }
    (void)send_data_packet(id, std::span<const uint8_t>(read_buffer_.data(), static_cast<size_t>(count)));
}

void Server::handle_write(uint32_t id, PacketReader& reader) {
    std::string handle = reader.string();
    uint64_t offset = reader.u64();
    std::span<const uint8_t> data = reader.byte_span();
    if (!reader.ok) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    FileHandle* file = find_file(handle);
    if (file == nullptr) {
        send_status(id, SSH_FX_FAILURE);
        return;
    }
    uint64_t const END_OFFSET = offset + data.size();
    if (END_OFFSET < offset) {
        send_status(id, SSH_FX_FAILURE);
        return;
    }
    int const RESERVE_ERR = reserve_for_write(*file, END_OFFSET);
    if (RESERVE_ERR != 0) {
        send_errno(id, RESERVE_ERR);
        return;
    }

    size_t done = 0;
    while (done < data.size()) {
        ssize_t written = pwrite(file->fd, data.data() + done, data.size() - done, static_cast<off_t>(offset + done));
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            send_errno(id, errno);
            return;
        }
        if (written == 0) {
            send_status(id, SSH_FX_FAILURE);
            return;
        }
        done += static_cast<size_t>(written);
    }
    file->logical_size = std::max(file->logical_size, END_OFFSET);
    send_status(id, SSH_FX_OK);
}

void Server::handle_stat(uint32_t id, PacketReader& reader, bool follow) {
    std::string path = reader.string();
    if (!reader.ok || has_nul(path)) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    struct stat st{};
    int rc = follow ? stat(path.c_str(), &st) : lstat(path.c_str(), &st);
    if (rc != 0) {
        send_errno(id, errno);
        return;
    }
    send_attrs(id, attrs_from_stat(st));
}

void Server::handle_fstat(uint32_t id, PacketReader& reader) {
    std::string handle = reader.string();
    if (!reader.ok) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    FileHandle* file = find_file(handle);
    if (file == nullptr) {
        send_status(id, SSH_FX_FAILURE);
        return;
    }
    struct stat st{};
    if (fstat(file->fd, &st) != 0) {
        send_errno(id, errno);
        return;
    }
    Attributes attrs = attrs_from_stat(st);
    if (file->reserve_writes && file->reserved_size != file->logical_size) {
        attrs.size = file->logical_size;
    }
    send_attrs(id, attrs);
}

void Server::handle_setstat(uint32_t id, PacketReader& reader) {
    std::string path = reader.string();
    Attributes attrs = reader.attrs();
    if (!reader.ok || has_nul(path)) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    int err = apply_path_attrs(path, attrs);
    if (err != 0) {
        send_errno(id, err);
        return;
    }
    send_status(id, SSH_FX_OK);
}

void Server::handle_fsetstat(uint32_t id, PacketReader& reader) {
    std::string handle = reader.string();
    Attributes attrs = reader.attrs();
    if (!reader.ok) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    FileHandle* file = find_file(handle);
    if (file == nullptr) {
        send_status(id, SSH_FX_FAILURE);
        return;
    }
    int err = apply_fd_attrs(file->fd, attrs);
    if (err != 0) {
        send_errno(id, err);
        return;
    }
    if ((attrs.flags & SSH_FILEXFER_ATTR_SIZE) != 0) {
        file->logical_size = attrs.size;
        file->reserved_size = attrs.size;
    }
    send_status(id, SSH_FX_OK);
}

void Server::handle_opendir(uint32_t id, PacketReader& reader) {
    std::string path = reader.string();
    if (!reader.ok || has_nul(path)) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    DIR* dir = opendir(path.c_str());
    if (dir == nullptr) {
        send_errno(id, errno);
        return;
    }

    DirectoryHandle handle{};
    while (dirent* ent = readdir(dir)) {
        handle.entries.push_back(stat_entry(path, ent->d_name));
    }
    closedir(dir);
    send_handle(id, 'D', add_handle(std::move(handle)));
}

void Server::handle_readdir(uint32_t id, PacketReader& reader) {
    std::string handle = reader.string();
    if (!reader.ok) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    DirectoryHandle* dir = find_dir(handle);
    if (dir == nullptr) {
        send_status(id, SSH_FX_FAILURE);
        return;
    }
    if (dir->index >= dir->entries.size()) {
        send_status(id, SSH_FX_EOF);
        return;
    }
    size_t begin = dir->index;
    size_t end = std::min(dir->entries.size(), begin + READDIR_BATCH);
    dir->index = end;
    send_name(id, std::span<const DirEntry>(dir->entries.data() + begin, end - begin));
}

void Server::handle_remove(uint32_t id, PacketReader& reader) {
    std::string path = reader.string();
    if (!reader.ok || has_nul(path)) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    if (unlink(path.c_str()) != 0) {
        send_errno(id, errno);
        return;
    }
    send_status(id, SSH_FX_OK);
}

void Server::handle_mkdir(uint32_t id, PacketReader& reader) {
    std::string path = reader.string();
    Attributes attrs = reader.attrs();
    if (!reader.ok || has_nul(path)) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    mode_t mode = (attrs.flags & SSH_FILEXFER_ATTR_PERMISSIONS) != 0 ? static_cast<mode_t>(attrs.permissions) : 0755;
    if (mkdir(path.c_str(), mode) != 0) {
        send_errno(id, errno);
        return;
    }
    send_status(id, SSH_FX_OK);
}

void Server::handle_rmdir(uint32_t id, PacketReader& reader) {
    std::string path = reader.string();
    if (!reader.ok || has_nul(path)) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    if (rmdir(path.c_str()) != 0) {
        send_errno(id, errno);
        return;
    }
    send_status(id, SSH_FX_OK);
}

void Server::handle_realpath(uint32_t id, PacketReader& reader) {
    std::string path = reader.string();
    if (!reader.ok || has_nul(path)) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    std::string normalized = path_for_client(normalize_path(path));
    DirEntry entry{};
    entry.name = normalized;
    struct stat st{};
    if (stat(normalized.c_str(), &st) == 0) {
        entry.attrs = attrs_from_stat(st);
        entry.longname = make_longname(normalized, st);
    } else {
        entry.longname = normalized;
    }
    std::array<DirEntry, 1> entries = {std::move(entry)};
    send_name(id, std::span<const DirEntry>(entries.data(), entries.size()));
}

void Server::handle_rename(uint32_t id, PacketReader& reader) {
    std::string old_path = reader.string();
    std::string new_path = reader.string();
    if (!reader.ok || has_nul(old_path) || has_nul(new_path)) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    if (rename(old_path.c_str(), new_path.c_str()) != 0) {
        int err = retry_rename_over_symlink(old_path, new_path, errno);
        if (err != 0) {
            send_errno(id, err);
            return;
        }
        send_status(id, SSH_FX_OK);
        return;
    }
    send_status(id, SSH_FX_OK);
}

void Server::handle_readlink(uint32_t id, PacketReader& reader) {
    std::string path = reader.string();
    if (!reader.ok || has_nul(path)) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }
    std::array<char, PATH_MAX> target{};
    ssize_t len = readlink(path.c_str(), target.data(), target.size() - 1);
    if (len < 0) {
        send_errno(id, errno);
        return;
    }
    std::string target_path(target.data(), static_cast<size_t>(len));
    DirEntry entry{};
    entry.name = target_path;
    entry.longname = target_path;
    std::array<DirEntry, 1> entries = {std::move(entry)};
    send_name(id, std::span<const DirEntry>(entries.data(), entries.size()));
}

void Server::handle_symlink(uint32_t id, PacketReader& reader) {
    std::string target_path = reader.string();
    std::string link_path = reader.string();
    if (!reader.ok || has_nul(target_path) || has_nul(link_path)) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }

    if (symlink(target_path.c_str(), link_path.c_str()) != 0) {
        send_errno(id, errno);
        return;
    }
    send_status(id, SSH_FX_OK);
}

void Server::handle_extended(uint32_t id, PacketReader& reader) {
    std::string request = reader.string();
    if (!reader.ok) {
        send_status(id, SSH_FX_BAD_MESSAGE);
        return;
    }

    if (request == "posix-rename@openssh.com") {
        handle_rename(id, reader);
        return;
    }
    if (request == "expand-path@openssh.com") {
        handle_realpath(id, reader);
        return;
    }
    if (request == "fsync@openssh.com") {
        std::string handle = reader.string();
        if (!reader.ok) {
            send_status(id, SSH_FX_BAD_MESSAGE);
            return;
        }
        FileHandle* file = find_file(handle);
        if (file == nullptr) {
            send_status(id, SSH_FX_FAILURE);
            return;
        }
        if (fsync(file->fd) != 0) {
            send_errno(id, errno);
            return;
        }
        send_status(id, SSH_FX_OK);
        return;
    }

    send_status(id, SSH_FX_OP_UNSUPPORTED);
}

}  // namespace sftpserver
