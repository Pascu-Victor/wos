#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

namespace ker::dev {

// Forward declaration for function pointer types
struct BlockDevice;

// Function pointer types for block device operations
// Note: device parameter allows drivers to access device-specific data (io_base, etc)
using block_read_fn = int (*)(BlockDevice* dev, uint64_t block, size_t count, void* buffer);
using block_write_fn = int (*)(BlockDevice* dev, uint64_t block, size_t count, const void* buffer);
using block_flush_fn = int (*)(BlockDevice* dev);

constexpr size_t BLOCK_NAME_SIZE = 256;

// Block device interface for filesystem drivers
struct BlockDevice {
    // Device identifiers
    unsigned major;
    unsigned minor;
    std::array<char, BLOCK_NAME_SIZE> name;

    // Device characteristics
    size_t block_size;      // Typically 512 or 4096 bytes
    uint64_t total_blocks;  // Total number of blocks

    // Device operations using function pointers
    // These should be implemented by the actual driver (ATA, AHCI, virtio, etc.)
    block_read_fn read_blocks;
    block_write_fn write_blocks;
    block_flush_fn flush;

    // Private driver data
    void* private_data;
};

// Block device registration and management
auto block_device_register(BlockDevice* bdev) -> int;
auto block_device_find(unsigned major, unsigned minor) -> BlockDevice*;
auto block_device_find_by_name(const char* name) -> BlockDevice*;

// Helper functions for block I/O
auto block_read(BlockDevice* bdev, uint64_t block, size_t count, void* buffer) -> int;
auto block_write(BlockDevice* bdev, uint64_t block, size_t count, const void* buffer) -> int;
auto block_flush(BlockDevice* bdev) -> int;

// Initializes block devices and mounts FAT32 filesystem
auto block_device_init() -> void;

}  // namespace ker::dev
