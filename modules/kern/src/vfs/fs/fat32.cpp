#include "fat32.hpp"

#include <bits/off_t.h>
#include <bits/ssize_t.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <net/wki/blk_ring.hpp>
#include <net/wki/dev_proxy.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>
#include <vfs/stat.hpp>

#include "platform/dbg/dbg.hpp"

namespace ker::vfs::fat32 {

using log = ker::mod::dbg::logger<"fat32">;

// FAT32 parsing/serialization operates directly on packed on-disk records and
// public DirEntry name buffers. Keep those raw extents visible and constrained
// by the surrounding FAT checks instead of wrapping them in layout-changing types.
// This file still exposes many file-local helpers between public VFS entry
// points, so keep internal linkage explicit until the FAT32 implementation is
// split into smaller translation units.
// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access,cppcoreguidelines-pro-bounds-array-to-pointer-decay,misc-use-anonymous-namespace,misc-use-internal-linkage)

// Forward declarations for helpers defined later in this translation unit.
auto flush_fat_table(const FAT32MountContext* ctx) -> int;

// FAT32 filesystem context
namespace {

// Keep this in sync with the userspace fcntl.h values (Linux-compatible octal).
constexpr int O_CREAT = 0100;  // octal = 64 decimal = 0x40 hex

// Simple file node for FAT32
struct FAT32Node {
    FAT32MountContext* context{};  // Pointer to mount context - place first to avoid alignment issues
    uint32_t start_cluster{};
    uint32_t file_size{};
    char* name{};
    uint8_t attributes{};
    bool is_directory{};
    // Track directory entry location for updating on write
    uint32_t dir_entry_cluster{};  // Which cluster contains the directory entry
    uint32_t dir_entry_offset{};   // Offset within that cluster (in bytes)

    // POSIX permission model (runtime-only, not persisted to FAT32 disk)
    uint32_t mode = 0;  // Permission bits (synthesized from FAT32 attributes)
    uint32_t uid = 0;   // Owner user ID
    uint32_t gid = 0;   // Owner group ID
};

// Long File Name entry (FAT32 VFAT). Packed on-disk.
// NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays): fixed FAT32 on-disk layout.
struct __attribute__((packed)) FAT32LongNameEntry {
    uint8_t order;
    uint16_t name1[5];
    uint8_t attr;
    uint8_t type;
    uint8_t checksum;
    uint16_t name2[6];
    uint16_t first_cluster_low;
    uint16_t name3[2];
};
// NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
static_assert(sizeof(FAT32LongNameEntry) == sizeof(FAT32DirectoryEntry));

constexpr uint8_t FAT32_LFN_ATTR = 0x0F;

auto lfn_checksum_83(const std::array<char, 11>& short_name) -> uint8_t {
    uint8_t sum = 0;
    for (int i = 0; i < 11; ++i) {
        sum = static_cast<uint8_t>((((sum & 1) != 0) ? 0x80 : 0) + (sum >> 1) + static_cast<uint8_t>(short_name.at(i)));
    }
    return sum;
}

auto hex_digit(uint8_t v) -> char {
    v &= 0xF;
    return (v < 10) ? static_cast<char>('0' + v) : static_cast<char>('A' + (v - 10));
}

auto ascii_upper(char c) -> char { return (c >= 'a' && c <= 'z') ? static_cast<char>(c - ('a' - 'A')) : c; }

auto ascii_lower(char c) -> char { return (c >= 'A' && c <= 'Z') ? static_cast<char>(c + ('a' - 'A')) : c; }

// Case-insensitive string comparison (ASCII only)
auto strcasecmp_local(const char* s1, const char* s2) -> int {
    while (*s1 != '\0' && *s2 != '\0') {
        char c1 = *s1;
        char c2 = *s2;
        // Convert to lowercase
        if (c1 >= 'A' && c1 <= 'Z') {
            c1 = static_cast<char>(c1 + ('a' - 'A'));
        }
        if (c2 >= 'A' && c2 <= 'Z') {
            c2 = static_cast<char>(c2 + ('a' - 'A'));
        }
        if (c1 != c2) {
            return static_cast<int>(c1) - static_cast<int>(c2);
        }
        s1++;
        s2++;
    }
    return static_cast<int>(static_cast<unsigned char>(*s1)) - static_cast<int>(static_cast<unsigned char>(*s2));
}

auto hash32_djb2(const char* s) -> uint32_t {
    uint32_t h = 5381U;
    if (s == nullptr) {
        return h;
    }
    for (size_t i = 0; s[i] != '\0'; ++i) {
        h = ((h << 5) + h) + static_cast<uint8_t>(s[i]);
    }
    return h;
}

inline auto decode_dirent_cluster(const FAT32DirectoryEntry& e) -> uint32_t {
    uint32_t const RAW = (static_cast<uint32_t>(e.cluster_high) << 16) | e.cluster_low;
    return RAW & FAT32_CLUSTER_MASK;
}

auto make_short_alias_83(const char* long_name, std::array<char, 11>& out11) -> void {
    // Deterministic alias: "CD" + 6 hex digits, extension "BIN".
    // This is sufficient for our coredump use-case and avoids needing full collision handling.
    uint32_t const H = hash32_djb2(long_name);
    uint32_t const V = H & 0xFFFFFFU;

    out11.at(0) = 'C';
    out11.at(1) = 'D';
    out11.at(2) = hex_digit((V >> 20) & 0xF);
    out11.at(3) = hex_digit((V >> 16) & 0xF);
    out11.at(4) = hex_digit((V >> 12) & 0xF);
    out11.at(5) = hex_digit((V >> 8) & 0xF);
    out11.at(6) = hex_digit((V >> 4) & 0xF);
    out11.at(7) = hex_digit(V & 0xF);
    out11.at(8) = 'B';
    out11.at(9) = 'I';
    out11.at(10) = 'N';
}

auto write_lfn_entries(FAT32DirectoryEntry* entry_table, size_t start_index, size_t lfn_count, const char* long_name, uint8_t checksum)
    -> void {
    // Writes entries in on-disk order: (lfnCount|0x40), ..., 1.
    // Each LFN entry stores up to 13 UTF-16 code units. We only support ASCII here.
    size_t name_len = 0;
    while (long_name[name_len] != '\0') {
        ++name_len;
    }

    // Emit from the end of the name backward in 13-char chunks.
    for (size_t idx = 0; idx < lfn_count; ++idx) {
        size_t const SEQ = lfn_count - idx;  // lfnCount..1
        auto* lfn = reinterpret_cast<FAT32LongNameEntry*>(&entry_table[start_index + idx]);

        memset(lfn, 0, sizeof(FAT32LongNameEntry));
        auto order = static_cast<uint8_t>(SEQ);
        if (SEQ == lfn_count) {
            order |= 0x40;  // last entry flag
        }
        lfn->order = order;
        lfn->attr = FAT32_LFN_ATTR;
        lfn->type = 0;
        lfn->checksum = checksum;
        lfn->first_cluster_low = 0;

        // Fill all chars with 0xFFFF by default.
        for (unsigned short& i : lfn->name1) {
            i = 0xFFFF;
        }
        for (unsigned short& i : lfn->name2) {
            i = 0xFFFF;
        }
        for (unsigned short& i : lfn->name3) {
            i = 0xFFFF;
        }

        // Determine the slice for this entry.
        size_t const CHUNK_START = (SEQ - 1) * 13;
        auto put = [&](size_t pos_in_chunk, uint16_t val) {
            if (pos_in_chunk < 5) {
                lfn->name1[pos_in_chunk] = val;
            } else if (pos_in_chunk < 11) {
                lfn->name2[pos_in_chunk - 5] = val;
            } else {
                lfn->name3[pos_in_chunk - 11] = val;
            }
        };

        for (size_t p = 0; p < 13; ++p) {
            size_t const SRC_INDEX = CHUNK_START + p;
            if (SRC_INDEX < name_len) {
                put(p, static_cast<uint16_t>(static_cast<uint8_t>(long_name[SRC_INDEX])));
            } else if (SRC_INDEX == name_len) {
                put(p, 0x0000);  // terminator
                break;
            } else {
                // Keep 0xFFFF padding
            }
        }
    }
}

// Extract long filename from collected LFN entries
// lfn_entries: array of LFN entries in on-disk order (seq N|0x40, ..., seq 1)
// lfn_count: number of LFN entries collected
// out_name: buffer to write the extracted name (should be at least 256 bytes)
// Returns length of extracted name, or 0 on failure
auto extract_lfn_name(const FAT32LongNameEntry* lfn_entries, size_t lfn_count, char* out_name, size_t out_size) -> size_t {
    if (lfn_count == 0 || lfn_entries == nullptr || out_name == nullptr || out_size == 0) {
        return 0;
    }

    size_t out_pos = 0;

    // LFN entries are stored in reverse order on disk: highest sequence first (with 0x40 flag), down to seq 1
    // Process from seq 1 to highest to build the name in correct order
    for (size_t i = lfn_count; i > 0; --i) {
        const auto* lfn = &lfn_entries[i - 1];

        // Verify this is actually an LFN entry
        if (lfn->attr != FAT32_LFN_ATTR) {
            continue;
        }

        // Extract chars from name1[5], name2[6], name3[2] = 13 chars per entry
        auto extract_char = [&](uint16_t c) -> bool {
            if (c == 0x0000) {
                // Null terminator
                return false;
            }
            if (c == 0xFFFF) {
                // Padding
                return false;
            }
            if (out_pos < out_size - 1) {
                // We only support ASCII for now
                out_name[out_pos++] = (c < 128) ? static_cast<char>(c) : '?';
            }
            return true;
        };

        bool cont = true;
        for (int j = 0; j < 5 && cont; ++j) {
            cont = extract_char(lfn->name1[j]);
        }
        for (int j = 0; j < 6 && cont; ++j) {
            cont = extract_char(lfn->name2[j]);
        }
        for (int j = 0; j < 2 && cont; ++j) {
            cont = extract_char(lfn->name3[j]);
        }
    }

    out_name[out_pos] = '\0';
    return out_pos;
}

}  // namespace

// Initialize FAT32 from a block device and return mount context
auto fat32_init_device(ker::dev::BlockDevice* device, uint64_t partition_start_lba) -> FAT32MountContext* {
    if (device == nullptr) {
        log::warn("fat32_init_device: invalid device");
        return nullptr;
    }

#ifdef FAT32_DEBUG
    log::debug("fat32_init_device: reading boot sector from LBA 0x%llx", partition_start_lba);
#endif

    // Allocate mount context
    auto* context = new FAT32MountContext{};
    if (context == nullptr) {
        log::warn("fat32_init_device: failed to allocate mount context");
        return nullptr;
    }

    // Initialize context
    context->device = device;
    context->partition_offset = partition_start_lba;
    context->fat_table = nullptr;
    context->bytes_per_sector = 0;
    context->sectors_per_cluster = 0;
    context->reserved_sectors = 0;
    context->sectors_per_fat = 0;
    context->num_fats = 0;
    context->data_start_sector = 0;
    context->total_sectors = 0;
    context->root_cluster = 0;

    // Allocate buffer for boot sector
    auto* boot_buf = new uint8_t[device->block_size];
    if (boot_buf == nullptr) {
        log::warn("fat32_init_device: failed to allocate boot sector buffer");
        delete context;
        return nullptr;
    }

    // Read boot sector from device at partition offset
    if (ker::dev::block_read(device, partition_start_lba, 1, boot_buf) != 0) {
        log::warn("fat32_init_device: failed to read boot sector");
        delete[] boot_buf;
        delete context;
        return nullptr;
    }

    auto* boot_sector = reinterpret_cast<FAT32BootSector*>(boot_buf);

#ifdef FAT32_DEBUG
    auto const* spt_ptr = reinterpret_cast<uint32_t*>(boot_buf + 0x24);
    log::trace("Boot sector signature: 0x%x", *reinterpret_cast<uint16_t*>(boot_buf + 510));
    log::trace("Raw bytes at offset 0x24 (sectors_per_fat_32): 0x%x", *spt_ptr);
#endif

    context->bytes_per_sector = boot_sector->bytes_per_sector;
    context->sectors_per_cluster = boot_sector->sectors_per_cluster;
    context->reserved_sectors = boot_sector->reserved_sectors;
    context->total_sectors = boot_sector->total_sectors_32;
    context->root_cluster = boot_sector->root_cluster;

    // Calculate data start sector (relative to partition start)
    context->sectors_per_fat = boot_sector->sectors_per_fat_32;
    context->num_fats = boot_sector->num_fats;

    context->data_start_sector = context->reserved_sectors + (static_cast<uint32_t>(context->sectors_per_fat * context->num_fats));
    delete[] boot_buf;

    // Allocate and read FAT table
    size_t const FAT_SIZE = static_cast<size_t>(context->sectors_per_fat) * context->bytes_per_sector;
#ifdef FAT32_DEBUG
    log::trace("bytes_per_sector: 0x%x", context->bytes_per_sector);
    log::trace("sectors_per_cluster: 0x%x", context->sectors_per_cluster);
    log::trace("reserved_sectors: 0x%x", context->reserved_sectors);
    log::trace("sectors_per_fat: 0x%x", context->sectors_per_fat);
    log::trace("num_fats: 0x%x", context->num_fats);
    log::trace("FAT size to allocate: 0x%zx bytes", FAT_SIZE);
#endif

    // Validate boot sector values before allocation
    constexpr size_t MAX_FAT_BYTES = 64ULL * 1024ULL * 1024ULL;
    if (context->bytes_per_sector == 0 || context->bytes_per_sector > 4096 || context->sectors_per_fat == 0 ||
        context->sectors_per_fat > 0xFFFF || FAT_SIZE == 0 || FAT_SIZE > MAX_FAT_BYTES) {  // Sanity check: max 64MB FAT
        log::warn("fat32_init_device: invalid boot sector values");
        delete context;
        return nullptr;
    }

    context->fat_table = new uint32_t[FAT_SIZE / sizeof(uint32_t)];

    if (context->fat_table == nullptr) {
        log::warn("fat32_init_device: failed to allocate FAT table");
        delete context;
        return nullptr;
    }

    // Read FAT from device (adjusted for partition offset)
    size_t const FAT_SECTORS_TO_READ = (FAT_SIZE + device->block_size - 1) / device->block_size;
    if (ker::dev::block_read(device, partition_start_lba + context->reserved_sectors, FAT_SECTORS_TO_READ, context->fat_table) != 0) {
        log::warn("fat32_init_device: failed to read FAT");
        delete[] context->fat_table;
        context->fat_table = nullptr;
        delete context;
        return nullptr;
    }

#ifdef FAT32_DEBUG
    log::debug("fat32_init_device: initialized successfully");
#endif
    return context;
}

// Helper to get next cluster in chain
static auto get_next_cluster(const FAT32MountContext* ctx, uint32_t cluster) -> uint32_t {
    if (ctx == nullptr || ctx->fat_table == nullptr) {
        log::warn("get_next_cluster: context or fat_table is null");
        return 0;
    }
    if (cluster < 2) {
        return 0;
    }
    if (cluster >= FAT32_EOC_MIN) {
        return 0;
    }

    auto const FAT_ENTRIES = static_cast<uint32_t>((static_cast<size_t>(ctx->sectors_per_fat) * ctx->bytes_per_sector) / sizeof(uint32_t));
    if (cluster >= FAT_ENTRIES) {
        return 0;
    }

    uint32_t const NEXT = ctx->fat_table[cluster] & FAT32_CLUSTER_MASK;

    if (NEXT < 2 || NEXT >= FAT32_EOC_MIN) {
        return 0;
    }
    if (NEXT >= FAT_ENTRIES) {
        return 0;
    }
    return NEXT;
}

// Helper function to read a cluster from the block device
static auto read_cluster(const FAT32MountContext* ctx, uint32_t cluster, void* buffer) -> int {
    if (ctx == nullptr || ctx->device == nullptr || buffer == nullptr) {
        return -EINVAL;
    }

    cluster &= FAT32_CLUSTER_MASK;
    if (cluster < 2 || cluster >= FAT32_EOC_MIN) {
        return -EINVAL;
    }

    uint32_t data_clusters = 0;
    if (ctx->sectors_per_cluster != 0 && ctx->total_sectors > ctx->data_start_sector) {
        data_clusters = (ctx->total_sectors - ctx->data_start_sector) / ctx->sectors_per_cluster;
    }
    if (data_clusters == 0) {
        return -EINVAL;
    }
    if (cluster - 2 >= data_clusters) {
        return -EINVAL;
    }

    // Calculate the LBA of the cluster
    uint64_t const CLUSTER_LBA =
        ctx->partition_offset + ctx->data_start_sector + ((static_cast<uint64_t>(cluster) - 2) * ctx->sectors_per_cluster);
    uint64_t const PARTITION_END_LBA = ctx->partition_offset + ctx->total_sectors;
    if (CLUSTER_LBA + ctx->sectors_per_cluster > PARTITION_END_LBA) {
        return -EINVAL;
    }

    // Read the cluster
    return ker::dev::block_read(ctx->device, CLUSTER_LBA, ctx->sectors_per_cluster, buffer) == 0 ? 0 : -EIO;
}

// Helper function to compare FAT32 filenames (8.3 format with spaces)
static auto compare_fat32_name(const char* dir_name, const char* search_name) -> bool {
    // dir_name is from directory entry (11 chars, space-padded, no dot)
    // search_name is the filename we're looking for (e.g., "testprog" or "hello.txt")

#ifdef FAT32_DEBUG
    log::trace("compare_fat32_name: dir_name='%.11s' search='%s'", dir_name, search_name);
#endif

    std::array<char, FAT32_NAME_PART_LEN> name_part = {0};  // 8 chars + null
    std::array<char, FAT32_EXT_PART_LEN> ext_part = {0};

    // Split search_name into name and extension
    const char* dot = nullptr;
    for (const char* p = search_name; (*p) != 0; ++p) {
        if (*p == '.') {
            dot = p;
            break;
        }
    }

    if (dot != nullptr) {
        // Has extension
        size_t name_len = dot - search_name;
        name_len = std::min<size_t>(name_len, FAT32_NAME_PART_LEN - 1);
        for (size_t i = 0; i < name_len; ++i) {
            name_part[i] = ascii_upper(search_name[i]);
        }

        size_t ext_len = 0;
        for (const char* p = dot + 1; ((*p) != 0) && ext_len < 3; ++p, ++ext_len) {
            ext_part[ext_len] = ascii_upper(*p);
        }
    } else {
        // No extension
        size_t name_len = 0;
        for (const char* p = search_name; ((*p) != 0) && name_len < 8; ++p, ++name_len) {
            name_part[name_len] = ascii_upper(*p);
        }
    }

    // Compare with directory entry name (space-padded)
    for (int i = 0; i < 8; ++i) {
        char const EXPECTED = (name_part[i] != 0) ? name_part[i] : ' ';
        if (dir_name[i] != EXPECTED) {
            return false;
        }
    }

    for (int i = 0; i < 3; ++i) {
        char const EXPECTED = (ext_part[i] != 0) ? ext_part[i] : ' ';
        if (dir_name[8 + i] != EXPECTED) {
            return false;
        }
    }

    return true;
}

// Helper function to create a new file in a directory
// Returns a File* with start_cluster=0 (first write will allocate)
// parent_cluster is the cluster of the directory to create the file in
static auto create_file_in_directory(FAT32MountContext* ctx, uint32_t parent_cluster, const char* filename) -> ker::vfs::File* {
    if (ctx == nullptr || filename == nullptr || filename[0] == '\0') {
        log::warn("create_file_in_directory: invalid arguments");
        return nullptr;
    }

#ifdef FAT32_DEBUG
    log::debug("create_file_in_directory: creating '%s' in cluster 0x%x", filename, parent_cluster);
#endif

    size_t const CLUSTER_SIZE = static_cast<size_t>(ctx->bytes_per_sector) * ctx->sectors_per_cluster;
    auto* cluster_buf = new uint8_t[CLUSTER_SIZE];
    if (cluster_buf == nullptr) {
        log::warn("create_file_in_directory: failed to allocate cluster buffer");
        return nullptr;
    }

    // Calculate how many LFN entries we need
    size_t name_len = 0;
    while (filename[name_len] != '\0') {
        ++name_len;
    }
    size_t const LFN_COUNT = (name_len + 12) / 13;      // Ceil division for 13 chars per LFN entry
    size_t const TOTAL_ENTRIES_NEEDED = LFN_COUNT + 1;  // LFN entries + 1 SFN entry

    // Search for a contiguous block of free entries
    uint32_t current_cluster = parent_cluster;
    size_t const ENTRIES_PER_CLUSTER = CLUSTER_SIZE / sizeof(FAT32DirectoryEntry);

    uint32_t found_cluster = 0;
    uint32_t found_start_index = 0;
    bool found_slot = false;

    while (current_cluster >= 2 && current_cluster < FAT32_EOC) {
        if (read_cluster(ctx, current_cluster, cluster_buf) != 0) {
            log::warn("create_file_in_directory: failed to read cluster");
            delete[] cluster_buf;
            return nullptr;
        }

        auto* entries = reinterpret_cast<FAT32DirectoryEntry*>(cluster_buf);

        // Look for contiguous free entries or end of directory
        size_t consecutive_free = 0;
        size_t free_start = 0;

        for (size_t i = 0; i < ENTRIES_PER_CLUSTER; ++i) {
            bool const IS_FREE = (entries[i].name[0] == 0x00) || (static_cast<uint8_t>(entries[i].name[0]) == 0xE5);
            bool const IS_END = (entries[i].name[0] == 0x00);

            if (IS_FREE) {
                if (consecutive_free == 0) {
                    free_start = i;
                }
                consecutive_free++;

                if (consecutive_free >= TOTAL_ENTRIES_NEEDED) {
                    found_cluster = current_cluster;
                    found_start_index = free_start;
                    found_slot = true;
                    break;
                }

                // If we hit end of directory, we can use it
                if (IS_END && consecutive_free >= TOTAL_ENTRIES_NEEDED) {
                    found_cluster = current_cluster;
                    found_start_index = free_start;
                    found_slot = true;
                    break;
                }
            } else {
                consecutive_free = 0;
            }
        }

        if (found_slot) {
            break;
        }

        current_cluster = get_next_cluster(ctx, current_cluster);
    }

    if (!found_slot) {
        log::warn("create_file_in_directory: no free directory entries");
        delete[] cluster_buf;
        return nullptr;
    }

    // Re-read the cluster where we'll write
    if (read_cluster(ctx, found_cluster, cluster_buf) != 0) {
        log::warn("create_file_in_directory: failed to re-read cluster");
        delete[] cluster_buf;
        return nullptr;
    }

    auto* entries = reinterpret_cast<FAT32DirectoryEntry*>(cluster_buf);

    // Create short name (8.3 format)
    std::array<char, 11> short_name{};
    make_short_alias_83(filename, short_name);
    uint8_t const CHECKSUM = lfn_checksum_83(short_name);

    // Write LFN entries
    write_lfn_entries(entries, found_start_index, LFN_COUNT, filename, CHECKSUM);

    // Write the short name entry
    auto* sfn_entry = &entries[found_start_index + LFN_COUNT];
    memset(sfn_entry, 0, sizeof(FAT32DirectoryEntry));
    std::memcpy(sfn_entry->name, short_name.data(), short_name.size());
    sfn_entry->attributes = 0x20;  // Archive bit set
    sfn_entry->cluster_high = 0;
    sfn_entry->cluster_low = 0;
    sfn_entry->file_size = 0;
    // Time/date fields left as 0

    // Mark the next entry as end-of-directory if needed
    size_t const NEXT_ENTRY_IDX = found_start_index + LFN_COUNT + 1;
    if (NEXT_ENTRY_IDX < ENTRIES_PER_CLUSTER) {
        // Check if original was end-of-directory
        if (entries[NEXT_ENTRY_IDX].name[0] != 0x00 && static_cast<uint8_t>(entries[NEXT_ENTRY_IDX].name[0]) != 0xE5) {
            // There's already an entry here, don't overwrite
        } else if (entries[found_start_index + LFN_COUNT].name[0] == 0x00) {
            // We're extending into unused space, mark next as end
            entries[NEXT_ENTRY_IDX].name[0] = 0x00;
        }
    }

    // Write the cluster back to disk
    uint64_t const CLUSTER_LBA =
        ctx->partition_offset + ctx->data_start_sector + ((static_cast<uint64_t>(found_cluster) - 2) * ctx->sectors_per_cluster);
    if (ker::dev::block_write(ctx->device, CLUSTER_LBA, ctx->sectors_per_cluster, cluster_buf) != 0) {
        log::warn("create_file_in_directory: failed to write cluster");
        delete[] cluster_buf;
        return nullptr;
    }

    // Flush device
    ker::dev::block_flush(ctx->device);

#ifdef FAT32_DEBUG
    log::debug("create_file_in_directory: created file entry successfully");
#endif

    // Create file node
    auto* node = new FAT32Node;
    node->start_cluster = 0;  // Will be allocated on first write
    node->file_size = 0;
    node->attributes = 0x20;
    node->name = nullptr;
    node->is_directory = false;
    node->context = ctx;
    node->dir_entry_cluster = found_cluster;
    node->dir_entry_offset = static_cast<uint32_t>((found_start_index + LFN_COUNT) * sizeof(FAT32DirectoryEntry));
    node->mode = 0644;  // Default mode for new files
    node->uid = 0;
    node->gid = 0;

    auto* f = new File;
    f->private_data = node;
    f->fd = -1;
    f->pos = 0;
    f->fops = nullptr;
    f->is_directory = false;
    f->fs_type = FSType::FAT32;
    f->refcount = 1;
    f->open_flags = 0;
    f->fd_flags = 0;
    f->vfs_path = nullptr;
    f->dir_fs_count = static_cast<size_t>(-1);
    f->stream_cache_attachment = nullptr;

    delete[] cluster_buf;
    return f;
}

// Open a file by path
auto fat32_open_path(const char* path, int flags, int /*mode*/, FAT32MountContext* ctx) -> ker::vfs::File* {
#ifdef FAT32_DEBUG
    log::trace("fat32_open_path: path='%s'", path);
#endif
    if (ctx == nullptr) {
        log::warn("fat32_open_path: ctx is null");
        return nullptr;
    }

    // The path is now filesystem-relative (mount point prefix already stripped by VFS layer)
    // Empty path means opening the root directory
    const char* remaining_path = path;
    if (remaining_path[0] == '/') {
        remaining_path++;
    }

#ifdef FAT32_DEBUG
    log::trace("fat32_open_path: searching for '%s'", remaining_path);
#endif

    // If path is empty, we're opening the root directory
    if (remaining_path[0] == '\0') {
#ifdef FAT32_DEBUG
        log::trace("fat32_open_path: opening root directory");
#endif

        auto* node = new FAT32Node;
        node->start_cluster = ctx->root_cluster;
        node->file_size = 0;  // Directories don't have a meaningful size
        node->is_directory = true;
        node->context = ctx;  // Store mount context
        node->mode = 0755;    // Default mode for root directory
        node->uid = 0;
        node->gid = 0;

        auto* file = new File;
        file->fd = -1;
        file->private_data = node;
        file->fops = nullptr;  // Will be set by caller
        file->pos = 0;
        file->is_directory = true;
        file->fs_type = FSType::FAT32;
        file->refcount = 1;
        file->open_flags = 0;
        file->fd_flags = 0;
        file->vfs_path = nullptr;
        file->dir_fs_count = static_cast<size_t>(-1);
        file->stream_cache_attachment = nullptr;

#ifdef FAT32_DEBUG
        log::trace("fat32_open_path: root directory opened");
#endif
        return file;
    }

    // Walk the path component by component
    uint32_t current_cluster = ctx->root_cluster;
    size_t const CLUSTER_SIZE = static_cast<size_t>(ctx->bytes_per_sector) * ctx->sectors_per_cluster;
    auto* cluster_buf = new uint8_t[CLUSTER_SIZE];
    if (cluster_buf == nullptr) {
        log::warn("fat32_open_path: failed to allocate cluster buffer");
        return nullptr;
    }

    FAT32DirectoryEntry final_entry{};
    uint32_t final_entry_cluster = 0;
    uint32_t final_entry_offset = 0;
    bool found_final = false;

    while (*remaining_path != '\0') {
        // Extract the next path component
        std::array<char, 256> component{};
        size_t comp_len = 0;
        while (remaining_path[comp_len] != '\0' && remaining_path[comp_len] != '/') {
            component[comp_len] = remaining_path[comp_len];
            comp_len++;
        }
        component[comp_len] = '\0';
        remaining_path += comp_len;
        if (*remaining_path == '/') {
            remaining_path++;
        }

        // Skip empty components (from double slashes)
        if (comp_len == 0) {
            continue;
        }

        // Current-directory components should not require a directory lookup.
        // Userspace frequently opens/stats "." while inside mounted FAT32 paths.
        if (comp_len == 1 && component[0] == '.') {
            continue;
        }

#ifdef FAT32_DEBUG
        log::trace("fat32_open_path: looking for component '%s'", component.data());
#endif

        // Search current directory for this component
        bool found = false;
        FAT32DirectoryEntry found_entry{};
        uint32_t found_cluster = 0;
        uint32_t found_offset = 0;
        uint32_t search_cluster = current_cluster;

#ifdef FAT32_DEBUG
        log::trace("fat32_open_path: searching in cluster 0x%x for '%s'", search_cluster, component.data());
#endif
        while (search_cluster >= 2 && search_cluster < FAT32_EOC_MIN) {
            if (read_cluster(ctx, search_cluster, cluster_buf) != 0) {
                log::warn("fat32_open_path: failed to read cluster");
                delete[] cluster_buf;
                return nullptr;
            }

            auto* entries = reinterpret_cast<FAT32DirectoryEntry*>(cluster_buf);
            size_t const NUM_ENTRIES = CLUSTER_SIZE / sizeof(FAT32DirectoryEntry);

            // LFN collection: max 20 entries (260 chars / 13 chars per entry)
            std::array<FAT32LongNameEntry, 20> lfn_buffer{};
            size_t lfn_count = 0;

            for (size_t i = 0; i < NUM_ENTRIES; ++i) {
                auto* entry = &entries[i];

                // End of directory
                if (entry->name[0] == 0x00) {
                    goto component_search_done;
                }

                // Deleted entry - reset LFN collection
                if (entry->name[0] == static_cast<char>(0xE5)) {
                    lfn_count = 0;
                    continue;
                }

                // Long filename entry - collect it
                if ((entry->attributes & FAT32_ATTR_LONG_NAME) == FAT32_ATTR_LONG_NAME) {
                    const auto* lfn = reinterpret_cast<const FAT32LongNameEntry*>(entry);
                    if (lfn_count < 20) {
                        lfn_buffer[lfn_count++] = *lfn;
                    }
                    continue;
                }

                // Volume ID - skip and reset LFN
                if ((entry->attributes & FAT32_ATTR_VOLUME_ID) != 0) {
                    lfn_count = 0;
                    continue;
                }

                // Extract LFN if we have one
                std::array<char, 256> lfn_name{};
                size_t lfn_len = 0;
                if (lfn_count > 0) {
                    lfn_len = extract_lfn_name(lfn_buffer.data(), lfn_count, lfn_name.data(), lfn_name.size());
                }

#ifdef FAT32_DEBUG
                log::trace("  entry[%zu]: name='%.11s' attr=0x%x%s%s", i, entry->name, entry->attributes, lfn_len > 0 ? " lfn='" : "",
                           lfn_len > 0 ? lfn_name.data() : "");
#endif
                // Compare filename: check LFN first (if present), then 8.3 short name
                bool name_match = false;
                if (lfn_len > 0) {
                    // Case-insensitive comparison with LFN
                    name_match = (strcasecmp_local(lfn_name.data(), component.data()) == 0);
                }
                if (!name_match) {
                    // Try 8.3 short name comparison
                    name_match = compare_fat32_name(entry->name, component.data());
                }

                // Reset LFN collection for next entry
                lfn_count = 0;

                if (name_match) {
                    found = true;
                    found_entry = *entry;
                    found_cluster = search_cluster;
                    found_offset = i * sizeof(FAT32DirectoryEntry);
                    goto component_search_done;
                }
            }

            search_cluster = get_next_cluster(ctx, search_cluster);
        }

    component_search_done:
        if (!found) {
#ifdef FAT32_DEBUG
            log::debug("fat32_open_path: component not found: '%s'", component.data());
#endif

            // If O_CREAT and this is the last component, create the file
            if ((flags & O_CREAT) != 0 && *remaining_path == '\0') {
#ifdef FAT32_DEBUG
                log::debug("fat32_open_path: O_CREAT - creating file in parent cluster 0x%x", current_cluster);
#endif

                delete[] cluster_buf;
                return create_file_in_directory(ctx, current_cluster, component.data());
            }

            delete[] cluster_buf;
            return nullptr;
        }

        // If there are more path components, this must be a directory
        if (*remaining_path != '\0') {
            if ((found_entry.attributes & FAT32_ATTR_DIRECTORY) == 0) {
                log::warn("fat32_open_path: path component is not a directory");
                delete[] cluster_buf;
                return nullptr;
            }
            // Move to this directory
            current_cluster = decode_dirent_cluster(found_entry);
        } else {
            // This is the final component
            final_entry = found_entry;
            final_entry_cluster = found_cluster;
            final_entry_offset = found_offset;
            found_final = true;
        }
    }

    delete[] cluster_buf;

    if (!found_final) {
#ifdef FAT32_DEBUG
        log::debug("fat32_open_path: file not found");
#endif
        return nullptr;
    }

    // Create file node
    auto* node = new FAT32Node;
    node->start_cluster = decode_dirent_cluster(final_entry);
    node->file_size = final_entry.file_size;
    node->attributes = final_entry.attributes;
    node->name = nullptr;
    node->is_directory = (final_entry.attributes & FAT32_ATTR_DIRECTORY) != 0;
    node->context = ctx;
    node->dir_entry_cluster = final_entry_cluster;
    node->dir_entry_offset = final_entry_offset;
    // Synthesize POSIX mode from FAT32 attributes
    if (node->is_directory) {
        node->mode = 0755;
    } else {
        node->mode = (final_entry.attributes & 0x01) != 0 ? 0444U : 0644U;  // read-only attr -> no write
    }
    node->uid = 0;
    node->gid = 0;

    auto* f = new File;
    f->private_data = node;
    f->fd = -1;
    f->pos = 0;
    f->fops = nullptr;  // Will be set by vfs_open
    f->is_directory = node->is_directory;
    f->fs_type = FSType::FAT32;
    f->refcount = 1;
    f->open_flags = 0;
    f->fd_flags = 0;
    f->vfs_path = nullptr;
    f->dir_fs_count = static_cast<size_t>(-1);
    f->stream_cache_attachment = nullptr;

#ifdef FAT32_DEBUG
    log::debug("fat32_open_path: opened %s", node->is_directory ? "directory" : "file");
#endif

    return f;
}

// Read from a FAT32 file
auto fat32_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t {
#ifdef FAT32_DEBUG
    log::trace("fat32_read: called, count=0x%zx, offset=0x%zx", count, offset);
#endif

    if ((f == nullptr) || (f->private_data == nullptr)) {
        log::warn("fat32_read: invalid file or private_data");
        return -EBADF;
    }

    auto* node = static_cast<FAT32Node*>(f->private_data);
    if (node->context == nullptr) {
        log::warn("fat32_read: mount context is null");
        return -EIO;
    }

    FAT32MountContext* ctx = node->context;

    // Lock the mount context for the duration of the read
    ctx->lock.lock();

#ifdef FAT32_DEBUG
    log::trace("fat32_read: file_size=0x%x, start_cluster=0x%x", node->file_size, node->start_cluster);
#endif

    if ((buf == nullptr) || count == 0) {
        ctx->lock.unlock();
        return 0;
    }

    // Simple read implementation: traverse cluster chain
    uint32_t const BYTES_AVAILABLE = node->file_size - std::min(offset, static_cast<size_t>(node->file_size));
    size_t const TO_READ = std::min(count, static_cast<size_t>(BYTES_AVAILABLE));

#ifdef FAT32_DEBUG
    log::trace("fat32_read: to_read=0x%zx", to_read);
#endif

    if (TO_READ == 0) {
        ctx->lock.unlock();
        return 0;
    }

    // Calculate starting cluster and offset within cluster

    size_t const CLUSTER_SIZE = static_cast<size_t>(ctx->bytes_per_sector) * ctx->sectors_per_cluster;

#ifdef FAT32_DEBUG
    log::trace("fat32_read: cluster_size=0x%zx", cluster_size);
#endif

    uint32_t const CLUSTER_OFFSET = offset / CLUSTER_SIZE;
    uint32_t byte_offset = offset % CLUSTER_SIZE;
    uint32_t current_cluster = node->start_cluster;

#ifdef FAT32_DEBUG
    log::trace("fat32_read: cluster_offset=0x%x, byte_offset=0x%x, current_cluster=0x%x", cluster_offset, byte_offset, current_cluster);
#endif

    // Skip to the correct cluster
    for (uint32_t i = 0; i < CLUSTER_OFFSET; ++i) {
        current_cluster = get_next_cluster(ctx, current_cluster);
        if (current_cluster == 0) {
            ctx->lock.unlock();
            return -EIO;
        }
    }

    // Allocate buffer for reading clusters
    auto* cluster_buf = new uint8_t[CLUSTER_SIZE];

    if (cluster_buf == nullptr) {
        log::warn("fat32_read: failed to allocate cluster buffer");
        ctx->lock.unlock();
        return -ENOMEM;
    }

    // Read data
    size_t bytes_read = 0;
    auto* dest = reinterpret_cast<uint8_t*>(buf);

    // Check if the underlying block device supports streaming bulk RDMA transfer.
    // If so, we detect contiguous cluster chains and issue a single bulk_read()
    // for large extents (> BLK_RING_BULK_THRESHOLD bytes), which bypasses the
    // per-slot SQ/CQ pipeline and transfers data via direct RDMA.
    bool const BULK_CAPABLE = (ctx->device != nullptr) && ((ctx->device->capabilities & ker::dev::BDEV_CAP_BULK_RDMA) != 0);

    while (bytes_read < TO_READ && current_cluster >= 2 && current_cluster < FAT32_EOC) {
        // -- Contiguous extent detection -------------------------------------
        // Walk the FAT chain to find the longest run of physically contiguous
        // clusters starting at current_cluster.
        uint32_t const EXTENT_START = current_cluster;
        uint32_t extent_len = 1;  // cluster count in the contiguous run
        if (BULK_CAPABLE && byte_offset == 0) {
            uint32_t probe = current_cluster;
            while (extent_len < 4096) {  // safety cap
                uint32_t const NEXT = get_next_cluster(ctx, probe);
                if (NEXT != probe + 1) {
                    break;  // chain is not contiguous
                }
                extent_len++;
                probe = NEXT;
            }
        }

        // Calculate how many bytes the contiguous extent covers
        size_t const EXTENT_BYTES = static_cast<size_t>(extent_len) * CLUSTER_SIZE;
        size_t const REMAINING = TO_READ - bytes_read;

        // -- Bulk path: contiguous extent exceeds threshold ------------------
        if (BULK_CAPABLE && byte_offset == 0 && EXTENT_BYTES >= ker::net::wki::BLK_RING_BULK_THRESHOLD && REMAINING >= EXTENT_BYTES) {
            // Compute the LBA range for the contiguous cluster run
            uint64_t const EXTENT_LBA =
                ctx->partition_offset + ctx->data_start_sector + (static_cast<uint64_t>(EXTENT_START - 2) * ctx->sectors_per_cluster);
            uint32_t const EXTENT_SECTORS = extent_len * ctx->sectors_per_cluster;

            int const RET = ker::net::wki::wki_dev_proxy_bulk_read(ctx->device, EXTENT_LBA, EXTENT_SECTORS, dest);
            if (RET == 0) {
                size_t const BULK_BYTES = std::min(REMAINING, EXTENT_BYTES);
                bytes_read += BULK_BYTES;
                dest += BULK_BYTES;

                // Verify that we land on a valid cluster (the next one after extent)
                uint32_t const NEXT_AFTER = get_next_cluster(ctx, EXTENT_START + extent_len - 1);
                if (NEXT_AFTER >= 2 && NEXT_AFTER < FAT32_EOC) {
                    current_cluster = NEXT_AFTER;
                } else {
                    current_cluster = 0;  // end of chain
                }
                continue;
            }
            // Bulk read failed - fall through to per-cluster path
        }

        // -- Standard per-cluster path ---------------------------------------
        // Read the cluster from disk
        if (read_cluster(ctx, current_cluster, cluster_buf) != 0) {
            log::warn("fat32_read: failed to read cluster");
            break;
        }

        size_t const BYTES_IN_CLUSTER = std::min(TO_READ - bytes_read, CLUSTER_SIZE - byte_offset);
#ifdef FAT32_DEBUG
        log::trace("fat32_read: copying 0x%zx bytes", bytes_in_cluster);
#endif
        memcpy(dest, cluster_buf + byte_offset, BYTES_IN_CLUSTER);

        bytes_read += BYTES_IN_CLUSTER;
        dest += BYTES_IN_CLUSTER;
        byte_offset = 0;
        current_cluster = get_next_cluster(ctx, current_cluster);
    }

    // Free the cluster buffer
    delete[] cluster_buf;

    ctx->lock.unlock();
    return static_cast<ssize_t>(bytes_read);
}

// Flush FAT table to disk
auto flush_fat_table(const FAT32MountContext* ctx) -> int {
    if (ctx == nullptr || ctx->device == nullptr || ctx->fat_table == nullptr) {
        log::warn("flush_fat_table: invalid context");
        return -EINVAL;
    }

    // Calculate how many sectors to write
    size_t const FAT_SIZE_BYTES = static_cast<size_t>(ctx->sectors_per_fat) * ctx->bytes_per_sector;
    size_t const FAT_SECTORS = (FAT_SIZE_BYTES + ctx->bytes_per_sector - 1) / ctx->bytes_per_sector;

#ifdef FAT32_DEBUG
    log::trace("flush_fat_table: writing FAT table, %zu sectors", fat_sectors);
#endif

    // Write FAT table to both FAT1 and FAT2
    int result = ker::dev::block_write(ctx->device, ctx->partition_offset + ctx->reserved_sectors, static_cast<uint32_t>(FAT_SECTORS),
                                       ctx->fat_table);

    if (result != 0) {
        log::warn("flush_fat_table: failed to write FAT1");
        return -EIO;
    }

    // Write to FAT2 as well (backup FAT)
    result = ker::dev::block_write(ctx->device, ctx->partition_offset + ctx->reserved_sectors + ctx->sectors_per_fat,
                                   static_cast<uint32_t>(FAT_SECTORS), ctx->fat_table);

    if (result != 0) {
        log::warn("flush_fat_table: failed to write FAT2");
        return -EIO;
    }

#ifdef FAT32_DEBUG
    log::trace("flush_fat_table: FAT table flushed successfully");
#endif
    return 0;
}

// Sync file data to disk
auto fat32_fsync(File* f) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* node = static_cast<FAT32Node*>(f->private_data);
    if (node->context == nullptr) {
        return -EINVAL;
    }

    int result = flush_fat_table(node->context);
    if (result != 0) {
        return result;
    }

    result = ker::dev::block_flush(node->context->device);
    return result;
}

// Update file's directory entry on disk
static auto update_directory_entry(const FAT32Node* node, uint32_t new_size) -> int {
    if (node == nullptr || node->context == nullptr || node->context->device == nullptr) {
        log::warn("update_directory_entry: invalid node or context");
        return -EINVAL;
    }

    const FAT32MountContext* ctx = node->context;
    uint32_t const CLUSTER_SIZE = static_cast<uint32_t>(ctx->bytes_per_sector) * ctx->sectors_per_cluster;

#ifdef FAT32_DEBUG
    log::trace("update_directory_entry: updating entry at cluster 0x%x, offset 0x%x, new size 0x%x", node->dir_entry_cluster,
               node->dir_entry_offset, new_size);
#endif

    // Allocate buffer for the cluster
    auto* cluster_buf = new uint8_t[CLUSTER_SIZE];
    if (cluster_buf == nullptr) {
        log::warn("update_directory_entry: failed to allocate cluster buffer");
        return -ENOMEM;
    }

    // Read the cluster containing the directory entry
    uint64_t const CLUSTER_LBA =
        ctx->partition_offset + ctx->data_start_sector + ((static_cast<uint64_t>(node->dir_entry_cluster) - 2) * ctx->sectors_per_cluster);
    if (ker::dev::block_read(ctx->device, CLUSTER_LBA, ctx->sectors_per_cluster, cluster_buf) != 0) {
        log::warn("update_directory_entry: failed to read directory cluster");
        delete[] cluster_buf;
        return -EIO;
    }

    // Get the directory entry
    auto* entry = reinterpret_cast<FAT32DirectoryEntry*>(cluster_buf + node->dir_entry_offset);

    // Update the file size
    entry->file_size = new_size;

    // Also persist the file's start cluster. Newly created files are emitted with
    // cluster_low/high = 0 and the first cluster is allocated on first write.
    // If we don't write it back, the file will not be readable after reboot.
    entry->cluster_low = static_cast<uint16_t>(node->start_cluster & 0xFFFFU);
    entry->cluster_high = static_cast<uint16_t>((node->start_cluster >> 16) & 0xFFFFU);

#ifdef FAT32_DEBUG
    log::trace("update_directory_entry: writing cluster back");
#endif

    // Write the cluster back to disk
    if (ker::dev::block_write(ctx->device, CLUSTER_LBA, ctx->sectors_per_cluster, cluster_buf) != 0) {
        log::warn("update_directory_entry: failed to write directory cluster");
        return -EIO;
    }

#ifdef FAT32_DEBUG
    log::trace("update_directory_entry: directory entry updated successfully");
#endif
    return 0;
}

// Write to a FAT32 file
auto fat32_write(File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    if ((f == nullptr) || (f->private_data == nullptr) || (buf == nullptr)) {
        return (buf == nullptr) ? -EINVAL : -EBADF;
    }

    auto* node = static_cast<FAT32Node*>(f->private_data);
    if (node->context == nullptr) {
        log::warn("fat32_write: no mount context available");
        return -EIO;
    }

    const FAT32MountContext* ctx = node->context;
    if (ctx->device == nullptr) {
        log::warn("fat32_write: no block device available");
        return -EIO;
    }

    // Calculate starting cluster and offset within cluster
    size_t const CLUSTER_SIZE = static_cast<size_t>(ctx->bytes_per_sector) * ctx->sectors_per_cluster;
    uint32_t cluster_offset = offset / CLUSTER_SIZE;
    uint32_t byte_offset = offset % CLUSTER_SIZE;
    uint32_t current_cluster = node->start_cluster;

    // If starting from cluster 0 (new file), allocate first cluster
    if (current_cluster == 0) {
        // Simple cluster allocation: find first free cluster
        // In a real implementation, use a free cluster bitmap
        for (uint32_t i = 2; i < FAT32_EOC; ++i) {
            if ((ctx->fat_table[i] & FAT32_EOC) == 0) {
                current_cluster = i;
                ctx->fat_table[i] = FAT32_EOC;
                node->start_cluster = i;
                break;
            }
        }
        if (current_cluster == 0) {
            log::warn("fat32_write: no free clusters");
            return -ENOSPC;
        }
        byte_offset = 0;
        cluster_offset = 0;
    }

    // Skip to the correct cluster
    for (uint32_t i = 0; i < cluster_offset; ++i) {
        uint32_t const NEXT = ctx->fat_table[current_cluster] & FAT32_EOC;
        if (NEXT >= FAT32_EOC) {
            // Need to allocate new cluster
            for (uint32_t j = 2; j < FAT32_EOC; ++j) {
                if ((ctx->fat_table[j] & FAT32_EOC) == 0) {
                    ctx->fat_table[current_cluster] = j;
                    ctx->fat_table[j] = FAT32_EOC;
                    current_cluster = j;
                    break;
                }
            }
        } else {
            current_cluster = NEXT;
        }
    }

    // Write data
    size_t bytes_written = 0;
    const auto* src = reinterpret_cast<const uint8_t*>(buf);
    auto* cluster_buf = new uint8_t[CLUSTER_SIZE];

    while (bytes_written < count && current_cluster != 0) {
        // Read current cluster first
        uint64_t const CLUSTER_SECTOR =
            ctx->partition_offset + ctx->data_start_sector + ((static_cast<uint64_t>(current_cluster) - 2) * ctx->sectors_per_cluster);
        if (ker::dev::block_read(ctx->device, CLUSTER_SECTOR, ctx->sectors_per_cluster, cluster_buf) != 0) {
            log::warn("fat32_write: failed to read cluster");
            delete[] cluster_buf;
            return -EIO;
        }

        // Write data into cluster buffer
        size_t const BYTES_IN_CLUSTER = std::min(count - bytes_written, CLUSTER_SIZE - byte_offset);
        memcpy(cluster_buf + byte_offset, src, BYTES_IN_CLUSTER);

        // Write cluster back to device
        if (ker::dev::block_write(ctx->device, CLUSTER_SECTOR, ctx->sectors_per_cluster, cluster_buf) != 0) {
            log::warn("fat32_write: failed to write cluster");
            delete[] cluster_buf;
            return -EIO;
        }

        bytes_written += BYTES_IN_CLUSTER;
        src += BYTES_IN_CLUSTER;
        byte_offset = 0;

        // Update file size
        node->file_size = std::max<size_t>(offset + bytes_written, node->file_size);

        // Get next cluster
        uint32_t const NEXT = ctx->fat_table[current_cluster] & FAT32_EOC;
        if (NEXT >= FAT32_EOC) {
            if (bytes_written < count) {
                // Allocate new cluster
                for (uint32_t j = 2; j < FAT32_EOC; ++j) {
                    if ((ctx->fat_table[j] & FAT32_EOC) == 0) {
                        ctx->fat_table[current_cluster] = j;
                        ctx->fat_table[j] = FAT32_EOC;
                        current_cluster = j;
                        break;
                    }
                }
            }
        } else {
            current_cluster = NEXT;
        }
    }

    delete[] cluster_buf;

    // Update the file node with the new size
    node->file_size = offset + bytes_written;

    // Flush FAT table to disk
    if (flush_fat_table(ctx) != 0) {
        log::warn("fat32_write: failed to flush FAT table");
        return -EIO;
    }

    // Update the directory entry on disk with the new file size
    if (update_directory_entry(node, node->file_size) != 0) {
        log::warn("fat32_write: failed to update directory entry");
        return -EIO;
    }

    // Flush block device
    if (ctx->device != nullptr && ker::dev::block_flush(ctx->device) != 0) {
        log::warn("fat32_write: failed to flush device");
        return -EIO;
    }

#ifdef FAT32_DEBUG
    log::debug("fat32_write: wrote %zu bytes, new file size: 0x%x", bytes_written, node->file_size);
#endif

    return static_cast<ssize_t>(bytes_written);
}

// Seek in a FAT32 file
auto fat32_lseek(ker::vfs::File* f, off_t offset, int whence) -> off_t {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -EBADF;
    }

    auto* node = static_cast<FAT32Node*>(f->private_data);
    off_t newpos = 0;

    switch (whence) {
        case 0:  // SEEK_SET
            newpos = offset;
            break;
        case 1:  // SEEK_CUR
            newpos = f->pos + offset;
            break;
        case 2:  // SEEK_END
            newpos = static_cast<off_t>(node->file_size) + offset;
            break;
        default:
            return -EINVAL;
    }

    if (newpos < 0) {
        return -EINVAL;
    }
    f->pos = newpos;
    return f->pos;
}

// Close a FAT32 file
auto fat32_close(ker::vfs::File* f) -> int {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -EBADF;
    }

    auto* node = static_cast<FAT32Node*>(f->private_data);
    // Note: Do NOT delete node->name or node->context
    // node->name is always nullptr in current implementation
    // node->context points to mount's private_data and must NOT be freed here
    // The mount context is managed by the mount point and freed on unmount
    delete node;
    return 0;
}

constexpr auto fat32_isatty(ker::vfs::File* f) -> bool {
    (void)f;
    return false;
}

// Read directory entry at given index
static auto fat32_readdir(ker::vfs::File* f, DirEntry* entry, size_t index) -> int {
    if (f == nullptr || f->private_data == nullptr || entry == nullptr) {
        return -EINVAL;
    }

    auto* node = static_cast<FAT32Node*>(f->private_data);

    // Check if this is a directory
    if (!node->is_directory) {
        log::warn("fat32_readdir: not a directory");
        return -ENOTDIR;
    }

    // Read directory entries from the directory's cluster chain
    if (node->context == nullptr) {
        log::warn("fat32_readdir: no mount context");
        return -EIO;
    }

    FAT32MountContext* ctx = node->context;

    // Synthesize "." and ".." entries at indices 0 and 1
    if (index == 0) {
        entry->d_ino = node->start_cluster;
        entry->d_off = 1;
        entry->d_reclen = sizeof(DirEntry);
        entry->d_type = DT_DIR;
        entry->d_name[0] = '.';
        entry->d_name[1] = '\0';
        return 0;
    }
    if (index == 1) {
        entry->d_ino = 0;  // Parent cluster not tracked in FAT32Node
        entry->d_off = 2;
        entry->d_reclen = sizeof(DirEntry);
        entry->d_type = DT_DIR;
        entry->d_name[0] = '.';
        entry->d_name[1] = '.';
        entry->d_name[2] = '\0';
        return 0;
    }

    // Real entries start at index 2
    size_t const REAL_INDEX = index - 2;

    // Lock the mount context for the duration of the readdir
    ctx->lock.lock();

    size_t const CLUSTER_SIZE = static_cast<size_t>(ctx->bytes_per_sector) * ctx->sectors_per_cluster;
    auto* cluster_buf = new uint8_t[CLUSTER_SIZE];
    if (cluster_buf == nullptr) {
        ctx->lock.unlock();
        return -ENOMEM;
    }

    uint32_t current_cluster = node->start_cluster;
    size_t entries_seen = 0;
    bool found = false;

    // LFN accumulator: collect LFN entries preceding each 8.3 entry so we can
    // return the original mixed-case long filename instead of the uppercase 8.3 name.
    constexpr size_t MAX_LFN = 20;
    std::array<FAT32LongNameEntry, MAX_LFN> lfn_entries{};
    int lfn_count = 0;

    while (current_cluster >= 2 && current_cluster < FAT32_EOC_MIN) {
        // Read the cluster
        if (read_cluster(ctx, current_cluster, cluster_buf) != 0) {
            delete[] cluster_buf;
            ctx->lock.unlock();
            return -EIO;
        }

        // Parse directory entries
        size_t const NUM_ENTRIES = CLUSTER_SIZE / sizeof(FAT32DirectoryEntry);
        auto* entries = reinterpret_cast<FAT32DirectoryEntry*>(cluster_buf);

        for (size_t i = 0; i < NUM_ENTRIES; ++i) {
            auto* dir_entry = &entries[i];

            // End of directory
            if (dir_entry->name[0] == 0x00) {
                delete[] cluster_buf;
                ctx->lock.unlock();
                return -ENOENT;  // No more entries
            }

            // Deleted entry - reset LFN accumulator
            if (static_cast<uint8_t>(dir_entry->name[0]) == 0xE5) {
                lfn_count = 0;
                continue;
            }

            // LFN entry - accumulate it
            if (dir_entry->attributes == FAT32_ATTR_LONG_NAME) {
                auto* lfn = reinterpret_cast<FAT32LongNameEntry*>(dir_entry);
                if ((lfn->order & 0x40) != 0) {
                    lfn_count = 0;  // first (highest-seq) LFN entry, start fresh
                }
                if (static_cast<size_t>(lfn_count) < lfn_entries.size()) {
                    lfn_entries[static_cast<size_t>(lfn_count++)] = *lfn;
                }
                continue;
            }

            // Volume ID - skip and reset LFN accumulator
            if ((dir_entry->attributes & FAT32_ATTR_VOLUME_ID) != 0) {
                lfn_count = 0;
                continue;
            }

            // Skip . and .. entries from FAT32 (we synthesize our own)
            if (dir_entry->name[0] == '.' && (dir_entry->name[1] == ' ' || dir_entry->name[1] == '.')) {
                lfn_count = 0;
                continue;
            }

            // Check if this is the entry we're looking for (offset by 2 for synthetic . and ..)
            if (entries_seen == REAL_INDEX) {
                // Fill in the dirent structure
                entry->d_ino = (static_cast<uint32_t>(dir_entry->cluster_high) << 16) | dir_entry->cluster_low;
                entry->d_off = index + 1;
                entry->d_reclen = sizeof(DirEntry);
                entry->d_type = (dir_entry->attributes & FAT32_ATTR_DIRECTORY) != 0 ? DT_DIR : DT_REG;

                // Prefer the LFN name (preserves original case) over the 8.3 uppercase name
                if (lfn_count > 0) {
                    extract_lfn_name(lfn_entries.data(), static_cast<size_t>(lfn_count), entry->d_name.data(), entry->d_name.size());
                } else {
                    // Fall back to 8.3 short name (uppercase)
                    size_t name_idx = 0;
                    for (int j = 0; j < 8 && dir_entry->name[j] != ' '; ++j) {
                        entry->d_name[name_idx++] = dir_entry->name[j];
                    }
                    bool has_ext = false;
                    for (int j = 8; j < 11; ++j) {
                        if (dir_entry->name[j] != ' ') {
                            has_ext = true;
                            break;
                        }
                    }
                    if (has_ext) {
                        entry->d_name[name_idx++] = '.';
                        for (int j = 8; j < 11 && dir_entry->name[j] != ' '; ++j) {
                            entry->d_name[name_idx++] = dir_entry->name[j];
                        }
                    }
                    entry->d_name[name_idx] = '\0';
                }

                found = true;
                break;
            }
            entries_seen++;
            lfn_count = 0;  // reset accumulator after consuming each 8.3 entry
        }

        if (found) {
            break;
        }

        // Get next cluster
        current_cluster = get_next_cluster(ctx, current_cluster);
    }

    delete[] cluster_buf;
    ctx->lock.unlock();
    return found ? 0 : -1;
}

auto fat32_stat(const char* path, ker::vfs::Stat* statbuf, FAT32MountContext* ctx) -> int {
    if (path == nullptr || statbuf == nullptr || ctx == nullptr) {
        return -EINVAL;
    }

    // Handle root directory
    if (path[0] == '\0' || (path[0] == '/' && path[1] == '\0')) {
        statbuf->st_dev = 0;
        statbuf->st_ino = ctx->root_cluster;
        statbuf->st_nlink = 1;
        statbuf->st_mode = ker::vfs::S_IFDIR | 0755;
        statbuf->st_uid = 0;
        statbuf->st_gid = 0;
        statbuf->st_rdev = 0;
        statbuf->st_size = 0;
        statbuf->st_blksize = static_cast<blksize_t>(static_cast<size_t>(ctx->bytes_per_sector) * ctx->sectors_per_cluster);
        statbuf->st_blocks = 0;
        return 0;
    }

    // Find the entry using our existing directory traversal
    ctx->lock.lock();

    uint32_t current_cluster = ctx->root_cluster;
    const char* remaining_path = path;
    if (remaining_path[0] == '/') {
        remaining_path++;
    }

    // Walk path components
    while (*remaining_path != '\0') {
        // Extract component
        std::array<char, 256> component{};
        size_t comp_len = 0;
        while (remaining_path[comp_len] != '\0' && remaining_path[comp_len] != '/') {
            component[comp_len] = remaining_path[comp_len];
            comp_len++;
        }
        component[comp_len] = '\0';
        remaining_path += comp_len;
        if (*remaining_path == '/') {
            remaining_path++;
        }

        // Treat "." as a no-op path component.
        if (comp_len == 1 && component[0] == '.') {
            continue;
        }

        // Search directory for component
        uint32_t const BYTES_PER_CLUSTER = ctx->bytes_per_sector * ctx->sectors_per_cluster;
        auto* cluster_buf = new uint8_t[BYTES_PER_CLUSTER];
        bool found = false;
        uint32_t found_cluster = 0;
        uint32_t found_size = 0;
        uint8_t found_attr = 0;

        constexpr size_t MAX_LFN = 20;
        std::array<FAT32LongNameEntry, MAX_LFN> lfn_entries{};
        size_t lfn_count = 0;

        while (current_cluster < FAT32_EOC_MIN && current_cluster >= 2) {
            if (read_cluster(ctx, current_cluster, cluster_buf) != 0) {
                delete[] cluster_buf;
                ctx->lock.unlock();
                return -EIO;
            }

            auto* entries = reinterpret_cast<FAT32DirectoryEntry*>(cluster_buf);
            size_t const ENTRIES_COUNT = BYTES_PER_CLUSTER / sizeof(FAT32DirectoryEntry);

            for (size_t i = 0; i < ENTRIES_COUNT; ++i) {
                if (entries[i].name[0] == 0x00) {
                    break;  // End of directory
                }
                if (static_cast<uint8_t>(entries[i].name[0]) == 0xE5) {
                    lfn_count = 0;
                    continue;  // Deleted entry
                }

                // Handle LFN entries
                if ((entries[i].attributes & FAT32_ATTR_LONG_NAME) == FAT32_ATTR_LONG_NAME) {
                    auto* lfn = reinterpret_cast<FAT32LongNameEntry*>(&entries[i]);
                    if ((lfn->order & 0x40) != 0) {
                        lfn_count = 0;
                    }
                    if (lfn_count < MAX_LFN) {
                        lfn_entries[lfn_count++] = *lfn;
                    }
                    continue;
                }

                // Regular entry - compare names
                if (entries[i].attributes == FAT32_ATTR_VOLUME_ID) {
                    lfn_count = 0;
                    continue;
                }

                // Try LFN first, then SFN
                bool name_match = false;
                if (lfn_count > 0) {
                    std::array<char, 256> long_name{};
                    extract_lfn_name(lfn_entries.data(), lfn_count, long_name.data(), long_name.size());
                    if (long_name[0] != '\0') {
                        name_match = (strcasecmp_local(component.data(), long_name.data()) == 0);
                    }
                }
                if (!name_match) {
                    // Compare with short name
                    std::array<char, 13> short_name{};
                    int sn_idx = 0;
                    for (int j = 0; j < 8 && entries[i].name[j] != ' '; ++j) {
                        short_name[sn_idx++] = entries[i].name[j];
                    }
                    if (entries[i].name[8] != ' ') {
                        short_name[sn_idx++] = '.';
                        for (int j = 8; j < 11 && entries[i].name[j] != ' '; ++j) {
                            short_name[sn_idx++] = entries[i].name[j];
                        }
                    }
                    short_name[sn_idx] = '\0';
                    name_match = (strcasecmp_local(component.data(), short_name.data()) == 0);
                }

                lfn_count = 0;

                if (name_match) {
                    found = true;
                    found_cluster = decode_dirent_cluster(entries[i]);
                    found_size = entries[i].file_size;
                    found_attr = entries[i].attributes;
                    break;
                }
            }

            if (found) {
                break;
            }
            current_cluster = get_next_cluster(ctx, current_cluster);
        }

        delete[] cluster_buf;

        if (!found) {
            ctx->lock.unlock();
            return -ENOENT;
        }

        current_cluster = found_cluster;

        // If this is the final component, fill stat buffer
        if (*remaining_path == '\0') {
            statbuf->st_dev = 0;
            statbuf->st_ino = found_cluster;
            statbuf->st_nlink = 1;
            statbuf->st_uid = 0;
            statbuf->st_gid = 0;
            statbuf->st_rdev = 0;
            statbuf->st_size = static_cast<off_t>(found_size);
            statbuf->st_blksize = static_cast<blksize_t>(static_cast<size_t>(ctx->bytes_per_sector) * ctx->sectors_per_cluster);
            statbuf->st_blocks = (found_size + 511) / 512;

            if ((found_attr & FAT32_ATTR_DIRECTORY) != 0) {
                statbuf->st_mode = ker::vfs::S_IFDIR | 0755;
            } else {
                statbuf->st_mode = ker::vfs::S_IFREG | 0644;
            }

            ctx->lock.unlock();
            return 0;
        }
    }

    ctx->lock.unlock();
    return -ENOENT;
}

auto fat32_fstat(File* f, ker::vfs::Stat* statbuf) -> int {
    if (f == nullptr || statbuf == nullptr) {
        return -EINVAL;
    }

    auto* node = static_cast<FAT32Node*>(f->private_data);
    if (node == nullptr) {
        return -EBADF;
    }

    statbuf->st_dev = 0;
    statbuf->st_ino = node->start_cluster;
    statbuf->st_nlink = 1;
    statbuf->st_uid = node->uid;
    statbuf->st_gid = node->gid;
    statbuf->st_rdev = 0;
    statbuf->st_size = static_cast<off_t>(node->file_size);
    if (node->context != nullptr) {
        statbuf->st_blksize =
            static_cast<blksize_t>(static_cast<size_t>(node->context->bytes_per_sector) * node->context->sectors_per_cluster);
    } else {
        statbuf->st_blksize = 4096;
    }
    statbuf->st_blocks = (node->file_size + 511) / 512;

    if (node->is_directory) {
        statbuf->st_mode = ker::vfs::S_IFDIR | node->mode;
    } else {
        statbuf->st_mode = ker::vfs::S_IFREG | node->mode;
    }

    return 0;
}

auto fat32_statvfs(FAT32MountContext* ctx, ker::vfs::Statvfs* buf) -> int {
    if (ctx == nullptr || buf == nullptr) {
        return -EINVAL;
    }

    std::memset(buf, 0, sizeof(ker::vfs::Statvfs));

    uint32_t const CLUSTER_SIZE = ctx->bytes_per_sector * ctx->sectors_per_cluster;
    auto const FAT_ENTRY_COUNT =
        static_cast<uint32_t>((static_cast<uint64_t>(ctx->sectors_per_fat) * ctx->bytes_per_sector) / sizeof(uint32_t));
    uint32_t const DATA_CLUSTERS =
        (ctx->total_sectors > ctx->data_start_sector) ? (ctx->total_sectors - ctx->data_start_sector) / ctx->sectors_per_cluster : 0;
    // Valid cluster indices are [2, 2 + data_clusters); clamp to FAT table size
    uint32_t const MAX_CLUSTER = (FAT_ENTRY_COUNT < 2 + DATA_CLUSTERS) ? FAT_ENTRY_COUNT : 2 + DATA_CLUSTERS;

    uint32_t free_clusters = 0;
    if (ctx->fat_table != nullptr) {
        for (uint32_t i = 2; i < MAX_CLUSTER; i++) {
            if ((ctx->fat_table[i] & FAT32_CLUSTER_MASK) == 0) {
                free_clusters++;
            }
        }
    }

    buf->f_bsize = CLUSTER_SIZE;
    buf->f_frsize = CLUSTER_SIZE;
    buf->f_blocks = DATA_CLUSTERS;
    buf->f_bfree = free_clusters;
    buf->f_bavail = free_clusters;
    buf->f_files = 0;  // FAT32 has no fixed inode table
    buf->f_ffree = 0;
    buf->f_favail = 0;
    buf->f_fsid = 0;
    buf->f_flag = 0;
    buf->f_namemax = 255;
    return 0;
}

// ============================================================================
// FAT32 cluster and directory helpers
// ============================================================================

// Write a cluster to disk
static auto write_cluster(const FAT32MountContext* ctx, uint32_t cluster, const void* buffer) -> int {
    if (ctx == nullptr || ctx->device == nullptr || cluster < 2) {
        return -EINVAL;
    }
    uint64_t const CLUSTER_LBA =
        ctx->partition_offset + ctx->data_start_sector + (static_cast<uint64_t>(cluster - 2) * ctx->sectors_per_cluster);
    return ker::dev::block_write(ctx->device, CLUSTER_LBA, ctx->sectors_per_cluster, buffer) == 0 ? 0 : -EIO;
}

// Calculate total data clusters for bounds checking FAT scans
static auto total_data_clusters(const FAT32MountContext* ctx) -> uint32_t {
    uint32_t const DATA_SECTORS = ctx->total_sectors - ctx->data_start_sector;
    return (DATA_SECTORS / ctx->sectors_per_cluster) + 2;
}

// Allocate a single cluster - find first free FAT entry, mark EOC
static auto allocate_cluster(FAT32MountContext* ctx) -> uint32_t {
    uint32_t const MAX_CLUSTER = total_data_clusters(ctx);
    for (uint32_t i = 2; i < MAX_CLUSTER; ++i) {
        if ((ctx->fat_table[i] & FAT32_EOC) == 0) {
            ctx->fat_table[i] = FAT32_EOC;
            return i;
        }
    }
    return 0;  // disk full
}

// Free an entire cluster chain starting at start_cluster
static auto free_cluster_chain(FAT32MountContext* ctx, uint32_t start_cluster) -> int {
    if (start_cluster < 2) {
        return 0;
    }
    uint32_t cluster = start_cluster;
    while (cluster >= 2 && cluster < FAT32_EOC) {
        uint32_t const NEXT = ctx->fat_table[cluster] & FAT32_EOC;
        ctx->fat_table[cluster] = 0;  // free the entry
        if (NEXT >= FAT32_EOC) {
            break;
        }
        cluster = NEXT;
    }
    return 0;
}

// Truncate/extend a FAT32 file
static auto fat32_truncate(File* f, off_t length) -> int {
    if (f == nullptr || f->private_data == nullptr) {
        return -EINVAL;
    }
    auto* node = static_cast<FAT32Node*>(f->private_data);
    auto* ctx = node->context;
    if (ctx == nullptr) {
        return -EIO;
    }

    uint32_t const CLUSTER_SIZE = ctx->bytes_per_sector * ctx->sectors_per_cluster;
    auto new_size = static_cast<uint32_t>(length);

    if (new_size == node->file_size) {
        return 0;
    }

    if (new_size == 0) {
        // Truncate to zero - free entire chain
        if (node->start_cluster >= 2) {
            free_cluster_chain(ctx, node->start_cluster);
            node->start_cluster = 0;
        }
        node->file_size = 0;
        update_directory_entry(node, 0);
        flush_fat_table(ctx);
        ker::dev::block_flush(ctx->device);
        return 0;
    }

    if (new_size < node->file_size) {
        // Shrinking - walk chain to the cluster containing the last byte of new_size,
        // then free everything after it
        if (node->start_cluster < 2) {
            // File has no data but non-zero old size? Shouldn't happen.
            node->file_size = new_size;
            update_directory_entry(node, new_size);
            return 0;
        }

        uint32_t const CLUSTERS_NEEDED = (new_size + CLUSTER_SIZE - 1) / CLUSTER_SIZE;
        uint32_t current = node->start_cluster;
        for (uint32_t i = 1; i < CLUSTERS_NEEDED; ++i) {
            uint32_t const NEXT = ctx->fat_table[current] & FAT32_EOC;
            if (NEXT >= FAT32_EOC || NEXT < 2) {
                break;
            }
            current = NEXT;
        }
        // 'current' is the last cluster we need to keep
        uint32_t const NEXT_TO_FREE = ctx->fat_table[current] & FAT32_EOC;
        ctx->fat_table[current] = FAT32_EOC;  // terminate chain here
        if (NEXT_TO_FREE >= 2 && NEXT_TO_FREE < FAT32_EOC) {
            free_cluster_chain(ctx, NEXT_TO_FREE);
        }
    } else {
        // Extending - allocate clusters and zero-fill
        if (node->start_cluster < 2) {
            uint32_t const FIRST = allocate_cluster(ctx);
            if (FIRST == 0) {
                return -ENOSPC;
            }
            node->start_cluster = FIRST;
            // Zero-fill the new cluster
            auto* zbuf = new uint8_t[CLUSTER_SIZE];
            if (zbuf != nullptr) {
                memset(zbuf, 0, CLUSTER_SIZE);
                write_cluster(ctx, FIRST, zbuf);
                delete[] zbuf;
            }
        }
        uint32_t const CLUSTERS_NEEDED = (new_size + CLUSTER_SIZE - 1) / CLUSTER_SIZE;
        // Walk to the end of the existing chain
        uint32_t current = node->start_cluster;
        uint32_t current_count = 1;
        while (true) {
            uint32_t const NEXT = ctx->fat_table[current] & FAT32_EOC;
            if (NEXT >= FAT32_EOC || NEXT < 2) {
                break;
            }
            current = NEXT;
            current_count++;
        }
        // Allocate additional clusters
        while (current_count < CLUSTERS_NEEDED) {
            uint32_t const NC = allocate_cluster(ctx);
            if (NC == 0) {
                break;  // out of space
            }
            ctx->fat_table[current] = NC;
            // Zero-fill
            auto* zbuf = new uint8_t[CLUSTER_SIZE];
            if (zbuf != nullptr) {
                memset(zbuf, 0, CLUSTER_SIZE);
                write_cluster(ctx, NC, zbuf);
                delete[] zbuf;
            }
            current = NC;
            current_count++;
        }
    }

    node->file_size = new_size;
    update_directory_entry(node, new_size);
    flush_fat_table(ctx);
    ker::dev::block_flush(ctx->device);
    return 0;
}

// Walk a path to find the parent directory node and final component name.
// Returns nullptr if parent not found. Sets *out_name to final component.
static auto fat32_walk_to_parent(FAT32MountContext* ctx, const char* path, const char** out_name) -> FAT32Node* {
    if (path == nullptr || *path == '\0') {
        return nullptr;
    }

    // Find last '/'
    const char* last_slash = nullptr;
    for (const char* p = path; (*p) != 0; ++p) {
        if (*p == '/') {
            last_slash = p;
        }
    }

    const char* final_name{};

    if (last_slash == nullptr) {
        // Single component - parent is root
        final_name = path;
    } else {
        final_name = last_slash + 1;
        if (*final_name == '\0') {
            return nullptr;  // path ends with /
        }

        // Walk to the parent directory by opening the parent path
        std::array<char, 512> parent_path{};
        auto plen = static_cast<size_t>(last_slash - path);
        if (plen >= parent_path.size()) {
            return nullptr;
        }
        std::memcpy(parent_path.data(), path, plen);
        parent_path[plen] = '\0';

        // Use fat32_open_path to find parent dir
        auto* pf = fat32_open_path(parent_path.data(), 0, 0, ctx);
        if (pf == nullptr) {
            return nullptr;
        }
        auto* pnode = static_cast<FAT32Node*>(pf->private_data);
        if (pnode == nullptr || !pnode->is_directory) {
            // Clean up
            delete static_cast<FAT32Node*>(pf->private_data);
            delete pf;
            return nullptr;
        }

        auto* parent_node = new FAT32Node;
        parent_node->context = ctx;
        parent_node->start_cluster = pnode->start_cluster;
        parent_node->file_size = pnode->file_size;
        parent_node->is_directory = true;
        parent_node->attributes = pnode->attributes;
        parent_node->dir_entry_cluster = pnode->dir_entry_cluster;
        parent_node->dir_entry_offset = pnode->dir_entry_offset;
        parent_node->mode = pnode->mode;

        *out_name = final_name;
        delete static_cast<FAT32Node*>(pf->private_data);
        delete pf;
        *out_name = final_name;
        return parent_node;
    }

    // Parent is root directory
    auto* root_node = new FAT32Node;
    root_node->context = ctx;
    root_node->start_cluster = ctx->root_cluster;
    root_node->file_size = 0;
    root_node->is_directory = true;
    root_node->attributes = FAT32_ATTR_DIRECTORY;
    root_node->dir_entry_cluster = 0;
    root_node->dir_entry_offset = 0;
    root_node->mode = 0755;
    *out_name = final_name;
    return root_node;
}

// Find a directory entry in a directory, returning cluster and offset of SFN entry.
// Also returns the number of associated LFN entries preceding it.
struct DirEntryLocation {
    uint32_t sfn_cluster;        // cluster containing the SFN entry
    uint32_t sfn_offset;         // byte offset within that cluster
    uint32_t lfn_first_cluster;  // cluster of first LFN entry
    uint32_t lfn_first_offset;   // offset of first LFN entry
    uint32_t total_entries;      // total entries including LFN + SFN
    FAT32DirectoryEntry sfn;     // copy of the SFN entry
};

static auto fat32_find_dir_entry(FAT32MountContext* ctx, uint32_t dir_cluster, const char* name, DirEntryLocation* loc) -> int {
    if (ctx == nullptr || name == nullptr || loc == nullptr) {
        return -EINVAL;
    }

    uint32_t const CLUSTER_SIZE = ctx->bytes_per_sector * ctx->sectors_per_cluster;
    uint32_t const ENTRIES_PER_CLUSTER = CLUSTER_SIZE / 32;

    auto* cluster_buf = new uint8_t[CLUSTER_SIZE];
    if (cluster_buf == nullptr) {
        return -ENOMEM;
    }

    // Collect LFN entries as we scan
    constexpr int MAX_LFN = 20;
    std::array<FAT32LongNameEntry, MAX_LFN> lfn_entries{};
    int lfn_count = 0;
    uint32_t lfn_start_cluster = 0;
    uint32_t lfn_start_offset = 0;

    uint32_t cluster = dir_cluster;
    while (cluster >= 2 && cluster < FAT32_EOC) {
        if (read_cluster(ctx, cluster, cluster_buf) != 0) {
            delete[] cluster_buf;
            return -EIO;
        }

        auto* entries = reinterpret_cast<FAT32DirectoryEntry*>(cluster_buf);
        for (uint32_t i = 0; i < ENTRIES_PER_CLUSTER; ++i) {
            if (static_cast<uint8_t>(entries[i].name[0]) == 0x00) {
                // End of directory
                delete[] cluster_buf;
                return -ENOENT;
            }
            if (static_cast<uint8_t>(entries[i].name[0]) == 0xE5) {
                lfn_count = 0;  // reset LFN collection
                continue;
            }

            if (entries[i].attributes == FAT32_ATTR_LONG_NAME) {
                auto* lfn = reinterpret_cast<FAT32LongNameEntry*>(&entries[i]);
                if ((lfn->order & 0x40) != 0) {
                    // First LFN entry (last in sequence)
                    lfn_count = 0;
                    lfn_start_cluster = cluster;
                    lfn_start_offset = i * 32;
                }
                if (lfn_count < MAX_LFN) {
                    lfn_entries[lfn_count++] = *lfn;
                }
                continue;
            }

            // Skip volume ID entries
            if ((entries[i].attributes & FAT32_ATTR_VOLUME_ID) != 0) {
                lfn_count = 0;
                continue;
            }

            // Regular SFN entry - check if name matches
            bool matched = false;

            // Check LFN match first
            if (lfn_count > 0) {
                std::array<char, 256> lfn_name{};
                extract_lfn_name(lfn_entries.data(), static_cast<size_t>(lfn_count), lfn_name.data(), lfn_name.size());
                // Case-insensitive compare
                const char* a = lfn_name.data();
                const char* b = name;
                bool eq = true;
                while (((*a) != 0) && ((*b) != 0)) {
                    char const CA = ascii_lower(*a);
                    char const CB = ascii_lower(*b);
                    if (CA != CB) {
                        eq = false;
                        break;
                    }
                    a++;
                    b++;
                }
                if (eq && *a == '\0' && *b == '\0') {
                    matched = true;
                }
            }

            // Also check SFN match
            if (!matched) {
                matched = compare_fat32_name(entries[i].name, name);
            }

            if (matched) {
                loc->sfn_cluster = cluster;
                loc->sfn_offset = i * 32;
                loc->sfn = entries[i];
                if (lfn_count > 0) {
                    loc->lfn_first_cluster = lfn_start_cluster;
                    loc->lfn_first_offset = lfn_start_offset;
                    loc->total_entries = static_cast<uint32_t>(lfn_count) + 1;
                } else {
                    loc->lfn_first_cluster = cluster;
                    loc->lfn_first_offset = i * 32;
                    loc->total_entries = 1;
                }
                delete[] cluster_buf;
                return 0;
            }

            lfn_count = 0;  // reset for next entry
        }

        cluster = get_next_cluster(ctx, cluster);
    }

    delete[] cluster_buf;
    return -ENOENT;
}

// Check if a directory is empty (only . and .. entries)
static auto fat32_dir_is_empty(FAT32MountContext* ctx, uint32_t dir_cluster) -> bool {
    uint32_t const CLUSTER_SIZE = ctx->bytes_per_sector * ctx->sectors_per_cluster;
    uint32_t const ENTRIES_PER_CLUSTER = CLUSTER_SIZE / 32;

    auto* cluster_buf = new uint8_t[CLUSTER_SIZE];
    if (cluster_buf == nullptr) {
        return false;
    }

    uint32_t cluster = dir_cluster;
    while (cluster >= 2 && cluster < FAT32_EOC) {
        if (read_cluster(ctx, cluster, cluster_buf) != 0) {
            break;
        }
        auto* entries = reinterpret_cast<FAT32DirectoryEntry*>(cluster_buf);
        for (uint32_t i = 0; i < ENTRIES_PER_CLUSTER; ++i) {
            if (static_cast<uint8_t>(entries[i].name[0]) == 0x00) {
                delete[] cluster_buf;
                return true;  // End of directory - it's empty
            }
            if (static_cast<uint8_t>(entries[i].name[0]) == 0xE5) {
                continue;
            }
            if (entries[i].attributes == FAT32_ATTR_LONG_NAME) {
                continue;
            }
            if ((entries[i].attributes & FAT32_ATTR_VOLUME_ID) != 0) {
                continue;
            }
            // Check for . and ..
            if (entries[i].name[0] == '.' && entries[i].name[1] == ' ') {
                continue;
            }
            if (entries[i].name[0] == '.' && entries[i].name[1] == '.' && entries[i].name[2] == ' ') {
                continue;
            }
            // Found a real entry - not empty
            delete[] cluster_buf;
            return false;
        }
        cluster = get_next_cluster(ctx, cluster);
    }

    delete[] cluster_buf;
    return true;
}

// Mark directory entries as deleted (0xE5) - handles LFN + SFN
static auto fat32_delete_dir_entries(FAT32MountContext* ctx, const DirEntryLocation* loc) -> int {
    // For simplicity, we handle the case where all entries are in the same cluster
    // (which is how create_file_in_dir creates them - contiguous in one cluster).
    // A more robust implementation would handle cross-cluster LFN entries.

    uint32_t const CLUSTER_SIZE = ctx->bytes_per_sector * ctx->sectors_per_cluster;
    auto* cluster_buf = new uint8_t[CLUSTER_SIZE];
    if (cluster_buf == nullptr) {
        return -ENOMEM;
    }

    if (read_cluster(ctx, loc->sfn_cluster, cluster_buf) != 0) {
        delete[] cluster_buf;
        return -EIO;
    }

    // Mark the SFN entry as deleted
    cluster_buf[loc->sfn_offset] = 0xE5;

    // If LFN entries are in the same cluster, mark them too
    if (loc->lfn_first_cluster == loc->sfn_cluster && loc->total_entries > 1) {
        uint32_t lfn_off = loc->lfn_first_offset;
        for (uint32_t j = 0; j < loc->total_entries - 1; ++j) {
            cluster_buf[lfn_off] = 0xE5;
            lfn_off += 32;
        }
    }

    // Write cluster back
    if (write_cluster(ctx, loc->sfn_cluster, cluster_buf) != 0) {
        delete[] cluster_buf;
        return -EIO;
    }

    delete[] cluster_buf;
    ker::dev::block_flush(ctx->device);
    return 0;
}

// ============================================================================
// FAT32 unlink, rmdir, rename - called from VFS layer
// ============================================================================

auto fat32_unlink_path(FAT32MountContext* ctx, const char* path) -> int {
    const char* entry_name = nullptr;
    auto* parent = fat32_walk_to_parent(ctx, path, &entry_name);
    if (parent == nullptr) {
        return -ENOENT;
    }

    DirEntryLocation loc{};
    int const RET = fat32_find_dir_entry(ctx, parent->start_cluster, entry_name, &loc);
    delete parent;
    if (RET < 0) {
        return RET;
    }

    // Can't unlink a directory with unlink
    if ((loc.sfn.attributes & FAT32_ATTR_DIRECTORY) != 0) {
        return -EISDIR;
    }

    // Free the file's cluster chain
    uint32_t const START = (static_cast<uint32_t>(loc.sfn.cluster_high) << 16) | loc.sfn.cluster_low;
    if (START >= 2) {
        free_cluster_chain(ctx, START);
        flush_fat_table(ctx);
    }

    // Delete directory entries
    return fat32_delete_dir_entries(ctx, &loc);
}

auto fat32_rmdir_path(FAT32MountContext* ctx, const char* path) -> int {
    const char* entry_name = nullptr;
    auto* parent = fat32_walk_to_parent(ctx, path, &entry_name);
    if (parent == nullptr) {
        return -ENOENT;
    }

    DirEntryLocation loc{};
    int const RET = fat32_find_dir_entry(ctx, parent->start_cluster, entry_name, &loc);
    delete parent;
    if (RET < 0) {
        return RET;
    }

    if ((loc.sfn.attributes & FAT32_ATTR_DIRECTORY) == 0) {
        return -ENOTDIR;
    }

    uint32_t const DIR_START = (static_cast<uint32_t>(loc.sfn.cluster_high) << 16) | loc.sfn.cluster_low;
    if (!fat32_dir_is_empty(ctx, DIR_START)) {
        return -ENOTEMPTY;
    }

    // Free directory cluster chain
    if (DIR_START >= 2) {
        free_cluster_chain(ctx, DIR_START);
        flush_fat_table(ctx);
    }

    return fat32_delete_dir_entries(ctx, &loc);
}

auto fat32_rename_path(FAT32MountContext* ctx, const char* oldpath, const char* newpath) -> int {
    // Step 1: Find old entry
    const char* old_name = nullptr;
    auto* old_parent = fat32_walk_to_parent(ctx, oldpath, &old_name);
    if (old_parent == nullptr) {
        return -ENOENT;
    }

    DirEntryLocation old_loc{};
    int const RET = fat32_find_dir_entry(ctx, old_parent->start_cluster, old_name, &old_loc);
    if (RET < 0) {
        delete old_parent;
        return RET;
    }

    // Step 2: Find new parent directory
    const char* new_name = nullptr;
    auto* new_parent = fat32_walk_to_parent(ctx, newpath, &new_name);
    if (new_parent == nullptr) {
        delete old_parent;
        return -ENOENT;
    }

    // Step 3: If destination already exists, remove it
    DirEntryLocation dest_loc{};
    if (fat32_find_dir_entry(ctx, new_parent->start_cluster, new_name, &dest_loc) == 0) {
        uint32_t const DEST_START = (static_cast<uint32_t>(dest_loc.sfn.cluster_high) << 16) | dest_loc.sfn.cluster_low;
        if (DEST_START >= 2) {
            free_cluster_chain(ctx, DEST_START);
        }
        fat32_delete_dir_entries(ctx, &dest_loc);
    }

    // Step 4: Create new directory entry in destination, pointing to old file's clusters
    uint32_t const FILE_START = (static_cast<uint32_t>(old_loc.sfn.cluster_high) << 16) | old_loc.sfn.cluster_low;
    uint32_t const FILE_SIZE = old_loc.sfn.file_size;
    uint8_t const ATTRS = old_loc.sfn.attributes;

    // Use create_file_in_dir to create entry, then update it
    auto* new_file = create_file_in_directory(ctx, new_parent->start_cluster, new_name);
    if (new_file == nullptr) {
        delete old_parent;
        delete new_parent;
        return -EIO;
    }

    // Update the new entry to point to the original file's clusters
    auto* new_node = static_cast<FAT32Node*>(new_file->private_data);
    new_node->start_cluster = FILE_START;
    new_node->file_size = FILE_SIZE;
    new_node->attributes = ATTRS;

    // Persist these to the directory entry on disk
    update_directory_entry(new_node, FILE_SIZE);

    // Also update attributes in the dir entry
    {
        uint32_t const CS = new_node->context->bytes_per_sector * new_node->context->sectors_per_cluster;
        auto* cbuf = new uint8_t[CS];
        if (cbuf != nullptr) {
            uint64_t const LBA = ctx->partition_offset + ctx->data_start_sector +
                                 (static_cast<uint64_t>(new_node->dir_entry_cluster - 2) * ctx->sectors_per_cluster);
            if (ker::dev::block_read(ctx->device, LBA, ctx->sectors_per_cluster, cbuf) == 0) {
                auto* entry = reinterpret_cast<FAT32DirectoryEntry*>(cbuf + new_node->dir_entry_offset);
                entry->attributes = ATTRS;
                ker::dev::block_write(ctx->device, LBA, ctx->sectors_per_cluster, cbuf);
            }
            delete[] cbuf;
        }
    }

    // Step 5: Delete old directory entry
    fat32_delete_dir_entries(ctx, &old_loc);

    // Clean up
    delete static_cast<FAT32Node*>(new_file->private_data);
    delete new_file;
    delete old_parent;
    delete new_parent;

    flush_fat_table(ctx);
    ker::dev::block_flush(ctx->device);
    return 0;
}

// Static storage for FAT32 FileOperations
namespace {
ker::vfs::FileOperations fat32_fops_instance = {
    .vfs_open = nullptr,             // vfs_open
    .vfs_close = fat32_close,        // vfs_close
    .vfs_read = fat32_read,          // vfs_read
    .vfs_write = fat32_write,        // vfs_write
    .vfs_lseek = fat32_lseek,        // vfs_lseek
    .vfs_isatty = fat32_isatty,      // vfs_isatty
    .vfs_readdir = fat32_readdir,    // vfs_readdir
    .vfs_readlink = nullptr,         // FAT32 doesn't support symlinks
    .vfs_truncate = fat32_truncate,  // FAT32 truncate
    .vfs_poll_check = nullptr,       // Regular files always ready
    .vfs_ioctl = nullptr,
    .vfs_poll_register_waiter = nullptr,
};
}

auto get_fat32_fops() -> ker::vfs::FileOperations* { return &fat32_fops_instance; }

void register_fat32() {
#ifdef FAT32_DEBUG
    log::debug("fat32: register_fat32 called");
#endif
    // Placeholder for registration logic
    // In a full implementation, this would set up the FAT32 filesystem
}

// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access,cppcoreguidelines-pro-bounds-array-to-pointer-decay,misc-use-anonymous-namespace,misc-use-internal-linkage)

}  // namespace ker::vfs::fat32
