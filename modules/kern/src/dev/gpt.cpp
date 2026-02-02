#include "gpt.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

#include "dev/block_device.hpp"

namespace ker::dev::gpt {

namespace {
// Compare two GUIDs as byte arrays
auto guid_compare(const uint8_t* guid1, const uint8_t* guid2) -> bool {
    for (size_t i = 0; i < GUID_SIZE; i++) {
        if (guid1[i] != guid2[i]) {
            return false;
        }
    }
    return true;
}

// Check if a GPT partition entry is empty (all-zero type GUID)
auto is_entry_empty(const GPTPartitionEntry* entry) -> bool {
    for (size_t j = 0; j < GUID_SIZE; ++j) {
        if (entry->partition_type_guid[j] != 0) {
            return false;
        }
    }
    return true;
}

constexpr char HEX_CHARS[] = "0123456789abcdef";  // NOLINT

void byte_to_hex(uint8_t byte, char* out) {
    out[0] = HEX_CHARS[(byte >> 4) & 0x0F];
    out[1] = HEX_CHARS[byte & 0x0F];
}
}  // namespace

// Convert a 16-byte mixed-endian GPT GUID to lowercase string
// GPT GUIDs are stored in mixed endian: first 3 groups are little-endian, last 2 are big-endian
// Output: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
void guid_to_string(const uint8_t* guid, char* out) {
    // Group 1: bytes 0-3, little-endian (reverse order)
    byte_to_hex(guid[3], out + 0);
    byte_to_hex(guid[2], out + 2);
    byte_to_hex(guid[1], out + 4);
    byte_to_hex(guid[0], out + 6);
    out[8] = '-';
    // Group 2: bytes 4-5, little-endian
    byte_to_hex(guid[5], out + 9);
    byte_to_hex(guid[4], out + 11);
    out[13] = '-';
    // Group 3: bytes 6-7, little-endian
    byte_to_hex(guid[7], out + 14);
    byte_to_hex(guid[6], out + 16);
    out[18] = '-';
    // Group 4: bytes 8-9, big-endian
    byte_to_hex(guid[8], out + 19);
    byte_to_hex(guid[9], out + 21);
    out[23] = '-';
    // Group 5: bytes 10-15, big-endian
    byte_to_hex(guid[10], out + 24);
    byte_to_hex(guid[11], out + 26);
    byte_to_hex(guid[12], out + 28);
    byte_to_hex(guid[13], out + 30);
    byte_to_hex(guid[14], out + 32);
    byte_to_hex(guid[15], out + 34);
    out[36] = '\0';
}

auto gpt_enumerate_partitions(BlockDevice* device, GPTDiskInfo* disk_info) -> int {
    if (device == nullptr || disk_info == nullptr) {
        gpt_log("gpt_enumerate: invalid arguments\n");
        return -1;
    }

    disk_info->partition_count = 0;

    // Allocate buffer for one sector
    auto* sector_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(device->block_size));
    if (sector_buf == nullptr) {
        gpt_log("gpt_enumerate: memory allocation failed\n");
        return -1;
    }

    // Read GPT header from LBA 1
    if (ker::dev::block_read(device, 1, 1, sector_buf) != 0) {
        gpt_log("gpt_enumerate: failed to read GPT header\n");
        return -1;
    }

    auto* hdr = reinterpret_cast<GPTHeader*>(sector_buf);

    // Validate GPT signature "EFI PART"
    if (hdr->signature != 0x5452415020494645ULL) {
        gpt_log("gpt_enumerate: invalid GPT signature\n");
        return -1;
    }

    // Copy disk GUID
    for (size_t i = 0; i < GUID_SIZE; ++i) {
        disk_info->disk_guid[i] = hdr->disk_guid[i];
    }

    uint64_t entries_lba = hdr->partition_entries_lba;
    uint32_t num_entries = hdr->num_partition_entries;
    uint32_t entry_size = hdr->partition_entry_size;
    uint32_t entries_per_sector = device->block_size / entry_size;
    uint32_t num_sectors = (num_entries + entries_per_sector - 1) / entries_per_sector;

    gpt_log("gpt_enumerate: scanning ");
    gpt_log_hex(num_entries);
    gpt_log(" partition entries\n");

    for (uint32_t sector = 0; sector < num_sectors; ++sector) {
        if (ker::dev::block_read(device, entries_lba + sector, 1, sector_buf) != 0) {
            gpt_log("gpt_enumerate: failed to read partition entries sector\n");
            return -1;
        }

        uint32_t entries_in_sector = entries_per_sector;
        if (sector == num_sectors - 1) {
            uint32_t remaining = num_entries - (sector * entries_per_sector);
            entries_in_sector = remaining < entries_per_sector ? remaining : entries_per_sector;
        }

        for (uint32_t i = 0; i < entries_in_sector; ++i) {
            auto* entry = reinterpret_cast<GPTPartitionEntry*>(
                sector_buf + (static_cast<size_t>(i) * entry_size));

            if (is_entry_empty(entry)) {
                continue;
            }

            if (disk_info->partition_count >= MAX_GPT_PARTITIONS) {
                gpt_log("gpt_enumerate: too many partitions\n");
                break;
            }

            uint32_t idx = disk_info->partition_count;
            auto& part = disk_info->partitions[idx];

            for (size_t b = 0; b < GUID_SIZE; ++b) {
                part.partition_type_guid[b] = entry->partition_type_guid[b];
                part.unique_partition_guid[b] = entry->unique_partition_guid[b];
            }
            part.starting_lba = entry->starting_lba;
            part.ending_lba = entry->ending_lba;
            part.partition_index = (sector * entries_per_sector) + i;

            disk_info->partition_count++;
        }
    }

    gpt_log("gpt_enumerate: found ");
    gpt_log_hex(disk_info->partition_count);
    gpt_log(" partitions\n");

    return 0;
}

// Find FAT32 partition on a GPT-partitioned disk
auto gpt_find_fat32_partition(BlockDevice* device) -> uint64_t {
    if (device == nullptr) {
        gpt_log("gpt_find_fat32_partition: Invalid device\n");
        return 0;
    }

    // Allocate buffer for one sector
    auto* sector_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(device->block_size));
    if (sector_buf == nullptr) {
        gpt_log("gpt_find_fat32_partition: Memory allocation failed\n");
        return 0;
    }

    // Read GPT header from LBA 1 (primary GPT header)
    if (ker::dev::block_read(device, 1, 1, sector_buf) != 0) {
        gpt_log("gpt_find_fat32_partition: Failed to read GPT header\n");
        // Note: Not freeing to avoid kmalloc::free() issues
        return 0;
    }

    auto* gpt_header = reinterpret_cast<GPTHeader*>(sector_buf);

    // Validate GPT signature "EFI PART"
    if (gpt_header->signature != 0x5452415020494645ULL) {
        gpt_log("gpt_find_fat32_partition: Invalid GPT signature\n");
        // Note: Not freeing to avoid kmalloc::free() issues
        return 0;
    }

    gpt_log("gpt_find_fat32_partition: Valid GPT found\n");
    gpt_log("gpt_find_fat32_partition: Looking for FAT32 GUID: ");
    for (size_t j = 0; j < GUID_SIZE; ++j) {
        gpt_log_hex(FAT32_PARTITION_GUID[j]);
        if (j < GUID_SIZE - 1) {
            gpt_log(" ");
        }
    }
    gpt_log("\n");
    gpt_log("gpt_find_fat32_partition: Partition entries at LBA 0x");
    gpt_log_hex(gpt_header->partition_entries_lba);
    gpt_log(", count: ");
    gpt_log_hex(gpt_header->num_partition_entries);
    gpt_log(", entry size: ");
    gpt_log_hex(gpt_header->partition_entry_size);
    gpt_log("\n");

    // Read partition entries one sector at a time to avoid large allocations
    uint32_t entries_per_sector = device->block_size / gpt_header->partition_entry_size;
    uint32_t num_sectors_needed = (gpt_header->num_partition_entries + entries_per_sector - 1) / entries_per_sector;

    gpt_log("gpt_find_fat32_partition: Reading ");
    gpt_log_hex(num_sectors_needed);
    gpt_log(" sectors of partition entries\n");

    // Reuse sector_buf to read one sector at a time
    uint64_t fat32_start_lba = 0;
    bool found = false;

    for (uint32_t sector = 0; sector < num_sectors_needed && !found; ++sector) {
        // Read one sector of partition entries
        if (ker::dev::block_read(device, gpt_header->partition_entries_lba + sector, 1, sector_buf) != 0) {
            gpt_log("gpt_find_fat32_partition: Failed to read partition entries sector ");
            gpt_log_hex(sector);
            gpt_log("\n");
            // Note: Intentionally not freeing memory to avoid kmalloc::free() issues
            return 0;
        }

        // Check entries in this sector
        uint32_t entries_in_sector = entries_per_sector;
        if (sector == num_sectors_needed - 1) {
            // Last sector might have fewer entries
            uint32_t remaining = gpt_header->num_partition_entries - (sector * entries_per_sector);
            entries_in_sector = remaining < entries_per_sector ? remaining : entries_per_sector;
        }

        for (uint32_t i = 0; i < entries_in_sector && !found; i++) {
            auto* entry = reinterpret_cast<GPTPartitionEntry*>(
                sector_buf + (static_cast<size_t>(i) * gpt_header->partition_entry_size));

            if (is_entry_empty(entry)) {
                continue;
            }

            // Print the GUID we found
            gpt_log("gpt: Partition ");
            gpt_log_hex((sector * entries_per_sector) + i);
            gpt_log(" GUID: ");
            for (size_t j = 0; j < GUID_SIZE; ++j) {
                gpt_log_hex(entry->partition_type_guid[j]);
                if (j < GUID_SIZE - 1) {
                    gpt_log(" ");
                }
            }
            gpt_log("\n");

            // Check if partition type matches FAT32
            const auto* type_guid = static_cast<const uint8_t*>(entry->partition_type_guid);
            if (guid_compare(type_guid, static_cast<const uint8_t*>(FAT32_PARTITION_GUID))) {
                fat32_start_lba = entry->starting_lba;
                gpt_log("gpt_find_fat32_partition: Found FAT32 partition at LBA 0x");
                gpt_log_hex(fat32_start_lba);
                gpt_log("\n");
                found = true;
            }

            // Also check for Microsoft Basic Data partition (commonly used for FAT32)
            if (!found && guid_compare(type_guid, static_cast<const uint8_t*>(BASIC_DATA_PARTITION_GUID))) {
                fat32_start_lba = entry->starting_lba;
                gpt_log("gpt_find_fat32_partition: Found Basic Data partition at LBA 0x");
                gpt_log_hex(fat32_start_lba);
                gpt_log("\n");
                found = true;
            }

            // Also check for Linux filesystem data partition (used by guestfish)
            if (!found && guid_compare(type_guid, static_cast<const uint8_t*>(LINUX_DATA_PARTITION_GUID))) {
                fat32_start_lba = entry->starting_lba;
                gpt_log("gpt_find_fat32_partition: Found Linux data partition at LBA 0x");
                gpt_log_hex(fat32_start_lba);
                gpt_log("\n");
                found = true;
            }
        }
    }

    // Note: Intentionally not freeing sector_buf to avoid kmalloc::free() issues during boot
    // This is acceptable as GPT parsing happens once during initialization

    if (fat32_start_lba == 0) {
        gpt_log("gpt_find_fat32_partition: No FAT32 partition found\n");
    }

    return fat32_start_lba;
}

}  // namespace ker::dev::gpt
