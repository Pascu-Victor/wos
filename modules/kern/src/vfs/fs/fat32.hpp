#pragma once

#include <cstddef>
#include <cstdint>

#include "../file_operations.hpp"
#include "../vfs.hpp"
#include "bits/ssize_t.h"

namespace ker::dev {
struct BlockDevice;
}

namespace ker::vfs::fat32 {

// FAT32 Boot Sector Structure (simplified)
struct __attribute__((packed)) FAT32BootSector {
    uint8_t jump_boot[3];
    char oem_name[8];
    uint16_t bytes_per_sector;
    uint8_t sectors_per_cluster;
    uint16_t reserved_sectors;
    uint8_t num_fats;
    uint16_t root_entry_count;
    uint16_t total_sectors_16;
    uint8_t media_descriptor;
    uint16_t sectors_per_fat_16;
    uint16_t sectors_per_track;
    uint16_t num_heads;
    uint32_t hidden_sectors;
    uint32_t total_sectors_32;
    uint32_t sectors_per_fat_32;
    uint16_t flags;
    uint16_t version;
    uint32_t root_cluster;
    uint16_t fsinfo_sector;
    uint16_t boot_sector_copy;
    uint8_t reserved[12];
    uint8_t drive_number;
    uint8_t reserved_byte;
    uint8_t boot_signature;
    uint32_t volume_serial;
    char volume_label[11];
    char file_system_type[8];
};

// FAT32 Directory Entry (32 bytes)
struct __attribute__((packed)) FAT32DirectoryEntry {
    char name[11];
    uint8_t attributes;
    uint8_t reserved;
    uint8_t create_time_tenths;
    uint16_t create_time;
    uint16_t create_date;
    uint16_t access_date;
    uint16_t cluster_high;
    uint16_t modify_time;
    uint16_t modify_date;
    uint16_t cluster_low;
    uint32_t file_size;
};

// Fs limits
constexpr uint64_t FAT32_NAME_PART_LEN = 9;
constexpr uint64_t FAT32_EXT_PART_LEN = 4;

// Attributes
constexpr uint8_t FAT32_ATTR_READ_ONLY = 0x01;
constexpr uint8_t FAT32_ATTR_HIDDEN = 0x02;
constexpr uint8_t FAT32_ATTR_SYSTEM = 0x04;
constexpr uint8_t FAT32_ATTR_VOLUME_ID = 0x08;
constexpr uint8_t FAT32_ATTR_DIRECTORY = 0x10;
constexpr uint8_t FAT32_ATTR_ARCHIVE = 0x20;
constexpr uint8_t FAT32_ATTR_DEVICE = 0x40;
constexpr uint8_t FAT32_ATTR_LONG_NAME = 0x0F;

// FAT32 specific end-of-chain marker
constexpr uint32_t FAT32_EOC = 0x0FFFFFFF;
constexpr uint32_t FAT32_BAD_CLUSTER = 0x0FFFFFF7;

// Initialize FAT32 driver
auto fat32_init(void* disk_buffer, size_t disk_size) -> int;

// Initialize FAT32 with a block device
auto fat32_init_device(ker::dev::BlockDevice* device, uint64_t partition_start_lba = 0) -> int;

// FAT32 filesystem operations
auto fat32_open_path(const char* path, int flags, int mode) -> File*;

// FAT32 I/O operations
auto fat32_read(File* f, void* buf, size_t count, size_t offset) -> ssize_t;
auto fat32_write(File* f, const void* buf, size_t count, size_t offset) -> ssize_t;
auto fat32_lseek(File* f, off_t offset, int whence) -> off_t;
auto fat32_close(File* f) -> int;

// Get FAT32 FileOperations structure
auto get_fat32_fops() -> FileOperations*;

// Register FAT32 driver
void register_fat32();

}  // namespace ker::vfs::fat32
