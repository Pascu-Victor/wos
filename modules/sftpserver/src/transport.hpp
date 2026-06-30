#pragma once

#include <stddef.h>
#include <stdint.h>

#include <span>
#include <vector>

#include "protocol.hpp"

namespace sftpserver {

auto read_packet(std::vector<uint8_t>& out) -> bool;
auto send_packet(const PacketWriter& packet) -> bool;
auto send_data_packet(uint32_t id, std::span<const uint8_t> data) -> bool;
auto send_file_data_packet(uint32_t id, int fd, uint64_t offset, size_t size) -> bool;

}  // namespace sftpserver
