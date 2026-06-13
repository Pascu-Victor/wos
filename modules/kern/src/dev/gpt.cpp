#include "gpt.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <span>

#include "dev/block_device.hpp"

namespace ker::dev::gpt {

namespace {
// Check if a GPT partition entry is empty (all-zero type GUID)
auto is_entry_empty(const GPTPartitionEntry* entry) -> bool {
    std::span<const uint8_t, GUID_SIZE> const TYPE_GUID{entry->partition_type_guid};
    return std::ranges::all_of(TYPE_GUID, [](uint8_t value) { return value == 0; });
}

auto read_le16(const uint8_t* data, size_t offset) -> uint16_t {
    return static_cast<uint16_t>(data[offset]) | (static_cast<uint16_t>(data[offset + 1]) << 8);
}

auto read_le32(const uint8_t* data, size_t offset) -> uint32_t {
    return static_cast<uint32_t>(data[offset]) | (static_cast<uint32_t>(data[offset + 1]) << 8) |
           (static_cast<uint32_t>(data[offset + 2]) << 16) | (static_cast<uint32_t>(data[offset + 3]) << 24);
}

auto is_power_of_two(uint32_t value) -> bool { return value != 0 && (value & (value - 1)) == 0; }

using SectorProbe = bool (*)(const uint8_t*, size_t);

auto gpt_find_partition_by_content(BlockDevice* device, SectorProbe probe, const char* label) -> uint64_t {
    if (device == nullptr || probe == nullptr || device->block_size < 512) {
        return 0;
    }

    auto* entries_buf = new (std::nothrow) uint8_t[device->block_size];
    auto* probe_buf = new (std::nothrow) uint8_t[device->block_size];
    if (entries_buf == nullptr || probe_buf == nullptr) {
        delete[] entries_buf;
        delete[] probe_buf;
        return 0;
    }

    if (ker::dev::block_read(device, 1, 1, entries_buf) != 0) {
        delete[] entries_buf;
        delete[] probe_buf;
        return 0;
    }

    auto* raw_header = reinterpret_cast<GPTHeader*>(entries_buf);
    if (raw_header->signature != 0x5452415020494645ULL || raw_header->partition_entry_size < sizeof(GPTPartitionEntry) ||
        raw_header->partition_entry_size > device->block_size) {
        delete[] entries_buf;
        delete[] probe_buf;
        return 0;
    }

    GPTHeader const HEADER = *raw_header;
    uint32_t const ENTRIES_PER_SECTOR = device->block_size / HEADER.partition_entry_size;
    if (ENTRIES_PER_SECTOR == 0) {
        delete[] entries_buf;
        delete[] probe_buf;
        return 0;
    }

    uint32_t const NUM_SECTORS_NEEDED = (HEADER.num_partition_entries + ENTRIES_PER_SECTOR - 1) / ENTRIES_PER_SECTOR;
    for (uint32_t sector = 0; sector < NUM_SECTORS_NEEDED; ++sector) {
        if (ker::dev::block_read(device, HEADER.partition_entries_lba + sector, 1, entries_buf) != 0) {
            delete[] entries_buf;
            delete[] probe_buf;
            return 0;
        }

        uint32_t entries_in_sector = ENTRIES_PER_SECTOR;
        if (sector == NUM_SECTORS_NEEDED - 1) {
            uint32_t const REMAINING = HEADER.num_partition_entries - (sector * ENTRIES_PER_SECTOR);
            entries_in_sector = REMAINING < ENTRIES_PER_SECTOR ? REMAINING : ENTRIES_PER_SECTOR;
        }

        for (uint32_t i = 0; i < entries_in_sector; i++) {
            auto* entry = reinterpret_cast<GPTPartitionEntry*>(entries_buf + (static_cast<size_t>(i) * HEADER.partition_entry_size));
            GPTPartitionEntry const ENTRY = *entry;
            if (is_entry_empty(&ENTRY) || ENTRY.starting_lba >= device->total_blocks) {
                continue;
            }

            if (ker::dev::block_read(device, ENTRY.starting_lba, 1, probe_buf) != 0) {
                continue;
            }

            if (probe(probe_buf, device->block_size)) {
                gpt_log("gpt_find_partition_by_content: Found ");
                gpt_log(label);
                gpt_log(" partition at LBA 0x");
                gpt_log_hex(ENTRY.starting_lba);
                gpt_log("\n");
                delete[] entries_buf;
                delete[] probe_buf;
                return ENTRY.starting_lba;
            }
        }
    }

    delete[] entries_buf;
    delete[] probe_buf;
    return 0;
}

constexpr std::array HEX_CHARS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

auto byte_to_hex(uint8_t byte, char* out) -> void {
    out[0] = HEX_CHARS.at((byte >> 4) & 0x0F);
    out[1] = HEX_CHARS.at(byte & 0x0F);
}
}  // namespace

// Convert a 16-byte mixed-endian GPT GUID to lowercase string
// GPT GUIDs are stored in mixed endian: first 3 groups are little-endian, last 2 are big-endian
// Output: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
auto guid_to_string(const uint8_t* guid, char* out) -> void {
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

auto sector_looks_like_fat32_boot(const uint8_t* sector, size_t size) -> bool {
    if (sector == nullptr || size < 512) {
        return false;
    }

    uint16_t const SIGNATURE = read_le16(sector, 510);
    if (SIGNATURE != 0xAA55) {
        return false;
    }

    uint16_t const BYTES_PER_SECTOR = read_le16(sector, 11);
    bool const VALID_SECTOR_SIZE =
        BYTES_PER_SECTOR == 512 || BYTES_PER_SECTOR == 1024 || BYTES_PER_SECTOR == 2048 || BYTES_PER_SECTOR == 4096;
    if (!VALID_SECTOR_SIZE) {
        return false;
    }

    uint8_t const SECTORS_PER_CLUSTER = sector[13];
    uint16_t const RESERVED_SECTORS = read_le16(sector, 14);
    uint8_t const NUM_FATS = sector[16];
    uint16_t const TOTAL_SECTORS_16 = read_le16(sector, 19);
    uint32_t const TOTAL_SECTORS_32 = read_le32(sector, 32);
    uint32_t const SECTORS_PER_FAT_32 = read_le32(sector, 36);
    uint32_t const ROOT_CLUSTER = read_le32(sector, 44);

    return is_power_of_two(SECTORS_PER_CLUSTER) && SECTORS_PER_CLUSTER <= 128 && RESERVED_SECTORS != 0 && NUM_FATS != 0 && NUM_FATS <= 2 &&
           TOTAL_SECTORS_16 == 0 && TOTAL_SECTORS_32 != 0 && SECTORS_PER_FAT_32 != 0 && ROOT_CLUSTER >= 2;
}

auto sector_looks_like_xfs_superblock(const uint8_t* sector, size_t size) -> bool {
    return sector != nullptr && size >= 4 && sector[0] == 'X' && sector[1] == 'F' && sector[2] == 'S' && sector[3] == 'B';
}

auto gpt_enumerate_partitions(BlockDevice* device, GPTDiskInfo* disk_info) -> int {
    if (device == nullptr || disk_info == nullptr) {
        gpt_log("gpt_enumerate: invalid arguments\n");
        return -1;
    }

    disk_info->partition_count = 0;

    // Allocate buffer for one sector
    auto* sector_buf = new (std::nothrow) uint8_t[device->block_size];
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
    std::span<const uint8_t, GUID_SIZE> const DISK_GUID{hdr->disk_guid};
    std::copy_n(DISK_GUID.begin(), DISK_GUID.size(), disk_info->disk_guid.begin());

    uint64_t const ENTRIES_LBA = hdr->partition_entries_lba;
    uint32_t const NUM_ENTRIES = hdr->num_partition_entries;
    uint32_t const ENTRY_SIZE = hdr->partition_entry_size;
    uint32_t const ENTRIES_PER_SECTOR = device->block_size / ENTRY_SIZE;
    uint32_t const NUM_SECTORS = (NUM_ENTRIES + ENTRIES_PER_SECTOR - 1) / ENTRIES_PER_SECTOR;

    gpt_log("gpt_enumerate: scanning ");
    gpt_log_hex(NUM_ENTRIES);
    gpt_log(" partition entries\n");

    for (uint32_t sector = 0; sector < NUM_SECTORS; ++sector) {
        if (ker::dev::block_read(device, ENTRIES_LBA + sector, 1, sector_buf) != 0) {
            gpt_log("gpt_enumerate: failed to read partition entries sector\n");
            return -1;
        }

        uint32_t entries_in_sector = ENTRIES_PER_SECTOR;
        if (sector == NUM_SECTORS - 1) {
            uint32_t const REMAINING = NUM_ENTRIES - (sector * ENTRIES_PER_SECTOR);
            entries_in_sector = REMAINING < ENTRIES_PER_SECTOR ? REMAINING : ENTRIES_PER_SECTOR;
        }

        for (uint32_t i = 0; i < entries_in_sector; ++i) {
            auto* entry = reinterpret_cast<GPTPartitionEntry*>(sector_buf + (static_cast<size_t>(i) * ENTRY_SIZE));

            if (is_entry_empty(entry)) {
                continue;
            }

            if (disk_info->partition_count >= MAX_GPT_PARTITIONS) {
                gpt_log("gpt_enumerate: too many partitions\n");
                break;
            }

            uint32_t const IDX = disk_info->partition_count;
            auto& part = disk_info->partitions.at(IDX);

            std::span<const uint8_t, GUID_SIZE> const PARTITION_TYPE_GUID{entry->partition_type_guid};
            std::span<const uint8_t, GUID_SIZE> const UNIQUE_PARTITION_GUID{entry->unique_partition_guid};
            std::copy_n(PARTITION_TYPE_GUID.begin(), PARTITION_TYPE_GUID.size(), part.partition_type_guid.begin());
            std::copy_n(UNIQUE_PARTITION_GUID.begin(), UNIQUE_PARTITION_GUID.size(), part.unique_partition_guid.begin());
            part.starting_lba = entry->starting_lba;
            part.ending_lba = entry->ending_lba;
            part.partition_index = (sector * ENTRIES_PER_SECTOR) + i;

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
    return gpt_find_partition_by_content(device, sector_looks_like_fat32_boot, "FAT32");
}

}  // namespace ker::dev::gpt
