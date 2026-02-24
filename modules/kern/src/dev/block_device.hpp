#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::net::wki {
struct RemotableOps;
}

namespace ker::dev {

// Forward declaration for function pointer types
struct BlockDevice;

// Function pointer types for block device operations
// Note: device parameter allows drivers to access device-specific data (io_base, etc)
using block_read_fn = int (*)(BlockDevice* dev, uint64_t block, size_t count, void* buffer);
using block_write_fn = int (*)(BlockDevice* dev, uint64_t block, size_t count, const void* buffer);
using block_flush_fn = int (*)(BlockDevice* dev);

constexpr size_t BLOCK_NAME_SIZE = 256;
constexpr size_t PARTUUID_STRING_SIZE = 37;  // "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" + NUL

// Block device interface for filesystem drivers
struct BlockDevice {
    // Device identifiers
    unsigned major = 0;
    unsigned minor = 0;
    std::array<char, BLOCK_NAME_SIZE> name{};

    // Device characteristics
    size_t block_size = 0;      // Typically 512 or 4096 bytes
    uint64_t total_blocks = 0;  // Total number of blocks

    // Device operations using function pointers
    // These should be implemented by the actual driver (ATA, AHCI, virtio, etc.)
    block_read_fn read_blocks = nullptr;
    block_write_fn write_blocks = nullptr;
    block_flush_fn flush = nullptr;

    // Private driver data
    void* private_data = nullptr;

    // WKI remotable trait — set by drivers that support remote access
    ker::net::wki::RemotableOps const* remotable = nullptr;

    // Partition info (set for partition block devices, not whole disks)
    bool is_partition = false;
    std::array<uint8_t, 16> partuuid{};
    std::array<char, PARTUUID_STRING_SIZE> partuuid_str{};
    BlockDevice* parent_disk = nullptr;
    uint64_t partition_start_lba = 0;
    uint64_t partition_end_lba = 0;
};

// Block device registration and management
auto block_device_register(BlockDevice* bdev) -> int;
auto block_device_unregister(BlockDevice* bdev) -> int;
auto block_device_find(unsigned major, unsigned minor) -> BlockDevice*;
auto block_device_find_by_name(const char* name) -> BlockDevice*;

// Helper functions for block I/O
auto block_read(BlockDevice* bdev, uint64_t block, size_t count, void* buffer) -> int;
auto block_write(BlockDevice* bdev, uint64_t block, size_t count, const void* buffer) -> int;
auto block_flush(BlockDevice* bdev) -> int;

// Partition block device management
// Creates a partition block device that delegates I/O to parent_disk with an LBA offset.
// The new device is named "<parent_name><1-based partition_index>" (e.g. "sda1").
auto block_device_create_partition(BlockDevice* parent_disk, uint64_t start_lba, uint64_t end_lba,
                                   const uint8_t* partuuid, uint32_t partition_index) -> BlockDevice*;

// Find a registered block device by PARTUUID string (lowercase, hyphenated)
auto block_device_find_by_partuuid(const char* uuid_str) -> BlockDevice*;

// Returns the number of registered block devices
auto block_device_count() -> size_t;

// Returns block device at index (for enumeration)
auto block_device_at(size_t index) -> BlockDevice*;

// Initializes block devices and enumerates GPT partitions
auto block_device_init() -> void;

}  // namespace ker::dev
