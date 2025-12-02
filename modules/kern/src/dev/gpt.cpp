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
constexpr size_t FAT32_PARTITION_GUID_SIZE = 16;
auto guid_compare(const uint8_t* guid1, const uint8_t* guid2) -> bool {
    for (size_t i = 0; i < FAT32_PARTITION_GUID_SIZE; i++) {
        if (guid1[i] != guid2[i]) {
            return false;
        }
    }
    return true;
}
}  // namespace

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
    for (int j = 0; j < 16; ++j) {
        gpt_log_hex(FAT32_PARTITION_GUID[j]);
        if (j < 15) gpt_log(" ");
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
            auto* entry = reinterpret_cast<GPTPartitionEntry*>(sector_buf + (i * gpt_header->partition_entry_size));

            // Check if entry is not all zeros (empty entry)
            bool is_empty = true;
            for (int j = 0; j < 16; ++j) {
                if (entry->partition_type_guid[j] != 0) {
                    is_empty = false;
                    break;
                }
            }

            if (!is_empty) {
                // Print the GUID we found
                gpt_log("gpt: Partition ");
                gpt_log_hex(sector * entries_per_sector + i);
                gpt_log(" GUID: ");
                for (int j = 0; j < 16; ++j) {
                    gpt_log_hex(entry->partition_type_guid[j]);
                    if (j < 15) gpt_log(" ");
                }
                gpt_log("\n");

                // Check if partition type matches FAT32
                if (guid_compare(entry->partition_type_guid, FAT32_PARTITION_GUID)) {
                    fat32_start_lba = entry->starting_lba;
                    gpt_log("gpt_find_fat32_partition: Found FAT32 partition at LBA 0x");
                    gpt_log_hex(fat32_start_lba);
                    gpt_log("\n");
                    found = true;
                }

                // Also check for Microsoft Basic Data partition (commonly used for FAT32)
                if (!found && guid_compare(entry->partition_type_guid, BASIC_DATA_PARTITION_GUID)) {
                    fat32_start_lba = entry->starting_lba;
                    gpt_log("gpt_find_fat32_partition: Found Basic Data partition at LBA 0x");
                    gpt_log_hex(fat32_start_lba);
                    gpt_log("\n");
                    found = true;
                }

                // Also check for Linux filesystem data partition (used by guestfish)
                if (!found && guid_compare(entry->partition_type_guid, LINUX_DATA_PARTITION_GUID)) {
                    fat32_start_lba = entry->starting_lba;
                    gpt_log("gpt_find_fat32_partition: Found Linux data partition at LBA 0x");
                    gpt_log_hex(fat32_start_lba);
                    gpt_log("\n");
                    found = true;
                }
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
