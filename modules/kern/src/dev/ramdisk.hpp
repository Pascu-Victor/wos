#pragma once

#include <cstddef>
#include <cstdint>

#include "block_device.hpp"

namespace ker::dev::ramdisk {

// Create a RAM disk device with the given size (in bytes)
// The device will allocate memory for the disk content
// Returns a BlockDevice* that can be used with filesystems
auto ramdisk_create(size_t size_bytes) -> BlockDevice*;

// Destroy a RAM disk device and free its memory
auto ramdisk_destroy(BlockDevice* disk) -> int;

// Get the memory buffer of a RAM disk (for initializing with data)
auto ramdisk_get_buffer(BlockDevice* disk) -> void*;

}  // namespace ker::dev::ramdisk
