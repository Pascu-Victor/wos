#pragma once
// Host shim for dev/block_device.hpp — minimal BlockDevice stub for buffer_cache tests.

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::net::wki { struct RemotableOps; }

namespace ker::dev {

struct BlockDevice;

using block_read_fn  = int (*)(BlockDevice* dev, uint64_t block, size_t count, void* buffer);
using block_write_fn = int (*)(BlockDevice* dev, uint64_t block, size_t count, const void* buffer);
using block_flush_fn = int (*)(BlockDevice* dev);

constexpr size_t BLOCK_NAME_SIZE      = 256;
constexpr size_t PARTUUID_STRING_SIZE = 37;

struct BlockDevice {
    unsigned major = 0;
    unsigned minor = 0;
    std::array<char, BLOCK_NAME_SIZE> name{};

    size_t   block_size   = 0;
    uint64_t total_blocks = 0;

    block_read_fn  read_blocks  = nullptr;
    block_write_fn write_blocks = nullptr;
    block_flush_fn flush        = nullptr;

    void* private_data = nullptr;
    uint32_t capabilities = 0;
    ker::net::wki::RemotableOps const* remotable = nullptr;

    bool is_partition = false;
    std::array<uint8_t, 16> partuuid{};
    std::array<char, PARTUUID_STRING_SIZE> partuuid_str{};
    BlockDevice* parent_disk     = nullptr;
    uint64_t partition_start_lba = 0;
    uint64_t partition_end_lba   = 0;
};

// No-op stubs for registration functions (unused in host tests)
inline auto block_device_register(BlockDevice*) -> int   { return 0; }
inline auto block_device_unregister(BlockDevice*) -> int { return 0; }
inline auto block_device_find(unsigned, unsigned) -> BlockDevice* { return nullptr; }
inline auto block_device_find_by_name(const char*) -> BlockDevice* { return nullptr; }

}  // namespace ker::dev
