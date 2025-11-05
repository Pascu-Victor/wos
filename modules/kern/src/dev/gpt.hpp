#pragma once

#include <cstdint>
#include <dev/block_device.hpp>

namespace ker::dev::gpt {

// GPT Partition Type GUIDs
constexpr uint8_t EFI_SYSTEM_PARTITION_GUID[16] = {0xC1, 0x2A, 0x73, 0x28, 0xF8, 0x1F, 0x11, 0xD2,
                                                   0xBA, 0x4B, 0x00, 0xA0, 0xC9, 0x3E, 0xC9, 0x3B};

constexpr uint8_t FAT32_PARTITION_GUID[16] = {0xEB, 0x3B, 0xA1, 0x3D, 0xB6, 0x10, 0xA7, 0x46,
                                              0xBB, 0x38, 0x25, 0x25, 0x83, 0x13, 0xB5, 0x78};

// Microsoft Basic Data Partition (commonly used for FAT32)
constexpr uint8_t BASIC_DATA_PARTITION_GUID[16] = {0xEB, 0xD0, 0xA0, 0xA2, 0xB9, 0xE5, 0x44, 0x33,
                                                   0x87, 0xC0, 0x68, 0xB6, 0xB7, 0x26, 0x99, 0xC7};

// Linux filesystem data partition (used by guestfish)
constexpr uint8_t LINUX_DATA_PARTITION_GUID[16] = {0xAF, 0x3D, 0xC6, 0x0F, 0x83, 0x84, 0x72, 0x47,
                                                   0x8E, 0x79, 0x3D, 0x69, 0xD8, 0x47, 0x7D, 0xE4};

// GPT Header structure (simplified, only essential fields)
struct GPTHeader {
    uint64_t signature;                // "EFI PART" = 0x5452415020494645
    uint32_t revision;                 // Revision 1.0 = 0x00010000
    uint32_t header_size;              // Usually 92 bytes
    uint32_t header_crc32;             // CRC32 of header (not including this field)
    uint32_t reserved;                 // Must be zero
    uint64_t current_lba;              // LBA of this header (1 for primary)
    uint64_t backup_lba;               // LBA of backup header
    uint64_t first_usable_lba;         // First LBA usable for partitions
    uint64_t last_usable_lba;          // Last LBA usable for partitions
    uint8_t disk_guid[16];             // Disk GUID
    uint64_t partition_entries_lba;    // Starting LBA of partition entries
    uint32_t num_partition_entries;    // Number of partition entries
    uint32_t partition_entry_size;     // Size of each partition entry (usually 128)
    uint32_t partition_entries_crc32;  // CRC32 of partition entries
} __attribute__((packed));

// GPT Partition Entry structure
struct GPTPartitionEntry {
    uint8_t partition_type_guid[16];    // Partition type GUID
    uint8_t unique_partition_guid[16];  // Unique partition GUID
    uint64_t starting_lba;              // Starting LBA of partition
    uint64_t ending_lba;                // Ending LBA of partition
    uint64_t attributes;                // Partition attributes
    uint16_t partition_name[36];        // Partition name (UTF-16LE)
} __attribute__((packed));

// Find FAT32 partition on a GPT-partitioned disk
// Returns the starting LBA of the FAT32 partition, or 0 if not found
uint64_t gpt_find_fat32_partition(BlockDevice* device);

}  // namespace ker::dev::gpt
