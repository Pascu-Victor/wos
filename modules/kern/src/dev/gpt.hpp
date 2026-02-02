#pragma once

#include <array>
#include <cstdint>
#include <dev/block_device.hpp>
#include <mod/io/serial/serial.hpp>

namespace ker::dev::gpt {

// GPT logging control - define GPT_DEBUG to enable debug logging
// Helper inline functions for logging (optimizes away when GPT_DEBUG is not defined)
inline void gpt_log(const char* msg) {
#ifdef GPT_DEBUG
    ker::mod::io::serial::write(msg);
#else
    (void)msg;
#endif
}

inline void gpt_log_hex(uint64_t value) {
#ifdef GPT_DEBUG
    ker::mod::io::serial::writeHex(value);
#else
    (void)value;
#endif
}

// GPT Partition Type GUIDs (C-style arrays required for packed struct compatibility)
constexpr uint8_t EFI_SYSTEM_PARTITION_GUID[16] = {0xC1, 0x2A, 0x73, 0x28, 0xF8, 0x1F, 0x11, 0xD2,  // NOLINT
                                                   0xBA, 0x4B, 0x00, 0xA0, 0xC9, 0x3E, 0xC9, 0x3B};

constexpr uint8_t FAT32_PARTITION_GUID[16] = {0xEB, 0x3B, 0xA1, 0x3D, 0xB6, 0x10, 0xA7, 0x46,  // NOLINT
                                              0xBB, 0x38, 0x25, 0x25, 0x83, 0x13, 0xB5, 0x78};

// Microsoft Basic Data Partition (commonly used for FAT32)
constexpr uint8_t BASIC_DATA_PARTITION_GUID[16] = {0xEB, 0xD0, 0xA0, 0xA2, 0xB9, 0xE5, 0x44, 0x33,  // NOLINT
                                                   0x87, 0xC0, 0x68, 0xB6, 0xB7, 0x26, 0x99, 0xC7};

// Linux filesystem data partition (used by guestfish)
constexpr uint8_t LINUX_DATA_PARTITION_GUID[16] = {0xAF, 0x3D, 0xC6, 0x0F, 0x83, 0x84, 0x72, 0x47,  // NOLINT
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
    uint8_t disk_guid[16];             // Disk GUID  NOLINT
    uint64_t partition_entries_lba;    // Starting LBA of partition entries
    uint32_t num_partition_entries;    // Number of partition entries
    uint32_t partition_entry_size;     // Size of each partition entry (usually 128)
    uint32_t partition_entries_crc32;  // CRC32 of partition entries
} __attribute__((packed));

// GPT Partition Entry structure
struct GPTPartitionEntry {
    uint8_t partition_type_guid[16];    // Partition type GUID  NOLINT
    uint8_t unique_partition_guid[16];  // Unique partition GUID  NOLINT
    uint64_t starting_lba;              // Starting LBA of partition
    uint64_t ending_lba;                // Ending LBA of partition
    uint64_t attributes;                // Partition attributes
    uint16_t partition_name[36];        // Partition name (UTF-16LE)  NOLINT
} __attribute__((packed));

// --- Partition enumeration ---

constexpr size_t MAX_GPT_PARTITIONS = 128;
constexpr size_t GUID_SIZE = 16;
constexpr size_t GUID_STRING_SIZE = 37;  // "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" + NUL

struct GPTPartitionInfo {
    std::array<uint8_t, GUID_SIZE> partition_type_guid{};
    std::array<uint8_t, GUID_SIZE> unique_partition_guid{};  // PARTUUID
    uint64_t starting_lba = 0;
    uint64_t ending_lba = 0;
    uint32_t partition_index = 0;
};

struct GPTDiskInfo {
    std::array<uint8_t, GUID_SIZE> disk_guid{};
    std::array<GPTPartitionInfo, MAX_GPT_PARTITIONS> partitions{};
    uint32_t partition_count = 0;
};

// Convert a 16-byte mixed-endian GPT GUID to a lowercase string
// Output format: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" (36 chars + NUL)
void guid_to_string(const uint8_t* guid, char* out);

// Enumerate all partitions on a GPT-partitioned disk.
// Populates disk_info with disk GUID and per-partition info.
// Returns 0 on success, -1 on failure.
auto gpt_enumerate_partitions(BlockDevice* device, GPTDiskInfo* disk_info) -> int;

// Find FAT32 partition on a GPT-partitioned disk
// Returns the starting LBA of the FAT32 partition, or 0 if not found
uint64_t gpt_find_fat32_partition(BlockDevice* device);

}  // namespace ker::dev::gpt
