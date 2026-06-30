#pragma once

#include <stdint.h>

#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

#include "protocol.hpp"

namespace sftpserver {

struct FileHandle {
    int fd = -1;
    uint64_t logical_size = 0;
    uint64_t reserved_size = 0;
    bool reserve_writes = false;
    bool sendfile_reads = false;
};

struct DirectoryHandle {
    std::vector<DirEntry> entries;
    size_t index = 0;
};

using Handle = std::variant<FileHandle, DirectoryHandle>;

class Server {
   public:
    ~Server();

    void run();

   private:
    std::unordered_map<uint32_t, Handle> handles_;
    std::vector<uint8_t> read_buffer_;
    uint32_t next_handle_ = 1;

    auto add_handle(Handle handle) -> uint32_t;
    auto find_file(std::string_view encoded) -> FileHandle*;
    auto find_dir(std::string_view encoded) -> DirectoryHandle*;
    auto close_handle(std::string_view encoded) -> bool;
    static auto close_file(FileHandle& file) -> bool;
    void close_all_handles();
    static auto reserve_for_write(FileHandle& file, uint64_t end_offset) -> int;

    void handle_packet(const std::vector<uint8_t>& packet);
    void handle_open(uint32_t id, PacketReader& reader);
    void handle_close(uint32_t id, PacketReader& reader);
    void handle_read(uint32_t id, PacketReader& reader);
    void handle_write(uint32_t id, PacketReader& reader);
    void handle_stat(uint32_t id, PacketReader& reader, bool follow);
    void handle_fstat(uint32_t id, PacketReader& reader);
    void handle_setstat(uint32_t id, PacketReader& reader);
    void handle_fsetstat(uint32_t id, PacketReader& reader);
    void handle_opendir(uint32_t id, PacketReader& reader);
    void handle_readdir(uint32_t id, PacketReader& reader);
    void handle_remove(uint32_t id, PacketReader& reader);
    void handle_mkdir(uint32_t id, PacketReader& reader);
    void handle_rmdir(uint32_t id, PacketReader& reader);
    void handle_realpath(uint32_t id, PacketReader& reader);
    void handle_rename(uint32_t id, PacketReader& reader);
    void handle_readlink(uint32_t id, PacketReader& reader);
    void handle_symlink(uint32_t id, PacketReader& reader);
    void handle_extended(uint32_t id, PacketReader& reader);
};

}  // namespace sftpserver
