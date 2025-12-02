#include "fat32.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <vfs/file.hpp>
#include <vfs/file_operations.hpp>

namespace ker::vfs::fat32 {

// FAT32 filesystem context
namespace {

// Simple file node for FAT32
struct FAT32Node {
    FAT32MountContext* context;  // Pointer to mount context - place first to avoid alignment issues
    uint32_t start_cluster;
    uint32_t file_size;
    char* name;
    uint8_t attributes;
    bool is_directory;
    // Track directory entry location for updating on write
    uint32_t dir_entry_cluster;  // Which cluster contains the directory entry
    uint32_t dir_entry_offset;   // Offset within that cluster (in bytes)
};

}  // namespace

// Initialize FAT32 from a block device and return mount context
auto fat32_init_device(ker::dev::BlockDevice* device, uint64_t partition_start_lba) -> FAT32MountContext* {
    if (device == nullptr) {
        fat32_log("fat32_init_device: invalid device\n");
        return nullptr;
    }

    fat32_log("fat32_init_device: reading boot sector from LBA 0x");
    fat32_log_hex(partition_start_lba);
    fat32_log("\n");

    // Allocate mount context
    auto* context = static_cast<FAT32MountContext*>(ker::mod::mm::dyn::kmalloc::malloc(sizeof(FAT32MountContext)));
    if (context == nullptr) {
        fat32_log("fat32_init_device: failed to allocate mount context\n");
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
    auto* boot_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(device->block_size));
    if (boot_buf == nullptr) {
        fat32_log("fat32_init_device: failed to allocate boot sector buffer\n");
        return nullptr;
    }

    // Read boot sector from device at partition offset
    if (ker::dev::block_read(device, partition_start_lba, 1, boot_buf) != 0) {
        fat32_log("fat32_init_device: failed to read boot sector\n");
        return nullptr;
    }

    auto* boot_sector = reinterpret_cast<FAT32BootSector*>(boot_buf);

    // Debug: Print boot sector signature
    fat32_log("Boot sector signature: 0x");
    fat32_log_hex(*reinterpret_cast<uint16_t*>(boot_buf + 510));
    fat32_log("\n");

    // Debug: Print raw bytes at key offsets
    fat32_log("Raw bytes at offset 0x24 (sectors_per_fat_32): ");
    uint32_t* spt_ptr = reinterpret_cast<uint32_t*>(boot_buf + 0x24);
    fat32_log_hex(*spt_ptr);
    fat32_log("\n");

    context->bytes_per_sector = boot_sector->bytes_per_sector;
    context->sectors_per_cluster = boot_sector->sectors_per_cluster;
    context->reserved_sectors = boot_sector->reserved_sectors;
    context->total_sectors = boot_sector->total_sectors_32;
    context->root_cluster = boot_sector->root_cluster;

    // Debug: Print boot sector values
    fat32_log("bytes_per_sector: ");
    fat32_log_hex(context->bytes_per_sector);
    fat32_log("\n");
    fat32_log("sectors_per_cluster: ");
    fat32_log_hex(context->sectors_per_cluster);
    fat32_log("\n");
    fat32_log("reserved_sectors: ");
    fat32_log_hex(context->reserved_sectors);
    fat32_log("\n");

    // Calculate data start sector (relative to partition start)
    context->sectors_per_fat = boot_sector->sectors_per_fat_32;
    context->num_fats = boot_sector->num_fats;

    fat32_log("sectors_per_fat: ");
    fat32_log_hex(context->sectors_per_fat);
    fat32_log("\n");
    fat32_log("num_fats: ");
    fat32_log_hex(context->num_fats);
    fat32_log("\n");

    context->data_start_sector = context->reserved_sectors + (static_cast<uint32_t>(context->sectors_per_fat * context->num_fats));

    // Allocate and read FAT table
    size_t fat_size = static_cast<size_t>(context->sectors_per_fat) * context->bytes_per_sector;

    fat32_log("FAT size to allocate: ");
    fat32_log_hex(fat_size);
    fat32_log(" bytes\n");

    // Validate boot sector values before allocation
    if (context->bytes_per_sector == 0 || context->bytes_per_sector > 4096 || context->sectors_per_fat == 0 ||
        context->sectors_per_fat > 0xFFFF || fat_size == 0 || fat_size > 64 * 1024 * 1024) {  // Sanity check: max 64MB FAT
        fat32_log("fat32_init_device: invalid boot sector values\n");
        return nullptr;
    }

    context->fat_table = static_cast<uint32_t*>(ker::mod::mm::dyn::kmalloc::malloc(fat_size));

    if (context->fat_table == nullptr) {
        fat32_log("fat32_init_device: failed to allocate FAT table\n");
        return nullptr;
    }

    // Read FAT from device (adjusted for partition offset)
    size_t fat_sectors_to_read = (fat_size + device->block_size - 1) / device->block_size;
    if (ker::dev::block_read(device, partition_start_lba + context->reserved_sectors, fat_sectors_to_read, context->fat_table) != 0) {
        fat32_log("fat32_init_device: failed to read FAT\n");
        context->fat_table = nullptr;
        return nullptr;
    }

    fat32_log("fat32_init_device: initialized successfully\n");
    return context;
}

// Helper to get next cluster in chain
auto get_next_cluster(const FAT32MountContext* ctx, uint32_t cluster) -> uint32_t {
    if (ctx == nullptr || ctx->fat_table == nullptr) {
        fat32_log("get_next_cluster: context or fat_table is null\n");
        return 0;
    }
    if (cluster >= FAT32_EOC) {
        return 0;
    }

    // Sanity check: make sure we're not accessing beyond reasonable bounds
    if (cluster > FAT32_EOC) {
        return 0;
    }

    uint32_t next = ctx->fat_table[cluster] & FAT32_EOC;

    if (next >= FAT32_EOC) {
        return 0;
    }
    return next;
}

// Helper function to read a cluster from the block device
auto read_cluster(const FAT32MountContext* ctx, uint32_t cluster, void* buffer) -> int {
    if (ctx == nullptr || ctx->device == nullptr || cluster < 2) {
        return -1;
    }

    // Calculate the LBA of the cluster
    uint64_t cluster_lba = ctx->partition_offset + ctx->data_start_sector + ((cluster - 2) * ctx->sectors_per_cluster);

    // Read the cluster
    return ker::dev::block_read(ctx->device, cluster_lba, ctx->sectors_per_cluster, buffer);
}

// Helper function to compare FAT32 filenames (8.3 format with spaces)
auto compare_fat32_name(const char* dir_name, const char* search_name) -> bool {
    // dir_name is from directory entry (11 chars, space-padded, no dot)
    // search_name is the filename we're looking for (e.g., "testprog" or "hello.txt")
    std::array<char, FAT32_NAME_PART_LEN> name_part = {0};  // 8 chars + null
    std::array<char, FAT32_EXT_PART_LEN> ext_part = {0};

    // Split search_name into name and extension
    const char* dot = nullptr;
    for (const char* p = search_name; *p; ++p) {
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
            name_part[i] = search_name[i] >= 'a' && search_name[i] <= 'z' ? search_name[i] - 32 : search_name[i];
        }

        size_t ext_len = 0;
        for (const char* p = dot + 1; *p && ext_len < 3; ++p, ++ext_len) {
            ext_part[ext_len] = *p >= 'a' && *p <= 'z' ? *p - 32 : *p;
        }
    } else {
        // No extension
        size_t name_len = 0;
        for (const char* p = search_name; *p && name_len < 8; ++p, ++name_len) {
            name_part[name_len] = *p >= 'a' && *p <= 'z' ? *p - 32 : *p;
        }
    }

    // Compare with directory entry name (space-padded)
    for (int i = 0; i < 8; ++i) {
        char expected = name_part[i] ? name_part[i] : ' ';
        if (dir_name[i] != expected) {
            return false;
        }
    }

    for (int i = 0; i < 3; ++i) {
        char expected = ext_part[i] ? ext_part[i] : ' ';
        if (dir_name[8 + i] != expected) {
            return false;
        }
    }

    return true;
}

// Open a file by path
auto fat32_open_path(const char* path, int /*flags*/, int /*mode*/, FAT32MountContext* ctx) -> ker::vfs::File* {
    fat32_log("fat32_open_path: called with path='");
    fat32_log(path);
    fat32_log("'\n");

    if (ctx == nullptr) {
        fat32_log("fat32_open_path: mount context is null\n");
        return nullptr;
    }

    // Skip mount point prefix (e.g., "/mnt/disk/")
    const char* filename = path;
    if (filename[0] == '/') {
        filename++;
        // Skip "mnt/"
        if (filename[0] == 'm' && filename[1] == 'n' && filename[2] == 't' && filename[3] == '/') {
            filename += 4;
        }
        // Skip "disk/" or similar
        while (*filename && *filename != '/') {
            filename++;
        }
        if (*filename == '/') {
            filename++;
        }
    }

    fat32_log("fat32_open_path: searching for '");
    fat32_log(filename);
    fat32_log("'\n");

    // If filename is empty, we're opening the root directory
    if (filename[0] == '\0') {
        fat32_log("fat32_open_path: opening root directory\n");

        auto* node = new FAT32Node;
        node->start_cluster = ctx->root_cluster;
        node->file_size = 0;  // Directories don't have a meaningful size
        node->is_directory = true;
        node->context = ctx;  // Store mount context

        auto* file = new File;
        file->fd = -1;
        file->private_data = node;
        file->fops = nullptr;  // Will be set by caller
        file->pos = 0;
        file->is_directory = true;
        file->fs_type = FSType::FAT32;

        fat32_log("fat32_open_path: root directory opened\n");
        return file;
    }

    // Search root directory for the file
    size_t cluster_size = ctx->bytes_per_sector * ctx->sectors_per_cluster;
    auto* cluster_buf = new uint8_t[cluster_size];
    if (cluster_buf == nullptr) {
        fat32_log("fat32_open_path: failed to allocate cluster buffer\n");
        return nullptr;
    }

    uint32_t current_cluster = ctx->root_cluster;
    bool found = false;
    FAT32DirectoryEntry found_entry;
    uint32_t found_cluster = 0;  // Track which cluster the entry is in
    uint32_t found_offset = 0;   // Track offset within the cluster

    while (current_cluster >= 2 && current_cluster < FAT32_EOC) {
        // Read the cluster
        if (read_cluster(ctx, current_cluster, cluster_buf) != 0) {
            fat32_log("fat32_open_path: failed to read cluster\n");
            break;
        }

        // Parse directory entries
        size_t num_entries = cluster_size / sizeof(FAT32DirectoryEntry);
        auto* entries = reinterpret_cast<FAT32DirectoryEntry*>(cluster_buf);

        for (size_t i = 0; i < num_entries; ++i) {
            auto* entry = &entries[i];

            // End of directory
            if (entry->name[0] == 0x00) {
                goto search_done;
            }

            // Deleted entry
            if (entry->name[0] == (char)0xE5) {
                continue;
            }

            // Long filename entry or volume ID
            if ((entry->attributes & FAT32_ATTR_LONG_NAME) == FAT32_ATTR_LONG_NAME || ((entry->attributes & FAT32_ATTR_VOLUME_ID) != 0)) {
                continue;
            }

            // Compare filename
            if (compare_fat32_name(entry->name, filename)) {
                found = true;
                found_entry = *entry;
                found_cluster = current_cluster;
                found_offset = i * sizeof(FAT32DirectoryEntry);
                goto search_done;
            }
        }

        // Get next cluster
        if (current_cluster < ctx->total_sectors / ctx->sectors_per_cluster) {
            current_cluster = get_next_cluster(ctx, current_cluster);
        } else {
            break;
        }
    }

search_done:

    if (!found) {
        fat32_log("fat32_open_path: file not found\n");
        delete[] cluster_buf;
        return nullptr;
    }

    // Create file node
    auto* node = new FAT32Node;
    node->start_cluster = ((uint32_t)found_entry.cluster_high << 16) | found_entry.cluster_low;
    node->file_size = found_entry.file_size;
    node->attributes = found_entry.attributes;
    node->name = nullptr;
    node->is_directory = (found_entry.attributes & FAT32_ATTR_DIRECTORY) != 0;
    node->context = ctx;                      // Store mount context
    node->dir_entry_cluster = found_cluster;  // Track directory entry location
    node->dir_entry_offset = found_offset;

    auto* f = new File;
    f->private_data = node;
    f->fd = -1;
    f->pos = 0;
    f->fops = nullptr;  // Will be set by vfs_open
    f->is_directory = node->is_directory;
    f->fs_type = FSType::FAT32;

    fat32_log("fat32_open_path: opened ");
    fat32_log(node->is_directory ? "directory" : "file");
    fat32_log("\n");

    return f;
}

// Read from a FAT32 file
auto fat32_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    fat32_log("fat32_read: called, count=0x");
    fat32_log_hex(count);
    fat32_log(", offset=0x");
    fat32_log_hex(offset);
    fat32_log("\n");

    if ((f == nullptr) || (f->private_data == nullptr)) {
        fat32_log("fat32_read: invalid file or private_data\n");
        return -1;
    }

    auto* node = static_cast<FAT32Node*>(f->private_data);
    if (node->context == nullptr) {
        fat32_log("fat32_read: mount context is null\n");
        return -1;
    }

    const FAT32MountContext* ctx = node->context;

    fat32_log("fat32_read: file_size=0x");
    fat32_log_hex(node->file_size);
    fat32_log(", start_cluster=0x");
    fat32_log_hex(node->start_cluster);
    fat32_log("\n");

    if ((buf == nullptr) || count == 0) {
        return 0;
    }

    // Simple read implementation: traverse cluster chain
    uint32_t bytes_available = node->file_size - std::min(offset, (size_t)node->file_size);
    size_t to_read = std::min(count, (size_t)bytes_available);

    fat32_log("fat32_read: to_read=0x");
    fat32_log_hex(to_read);
    fat32_log("\n");

    if (to_read == 0) {
        return 0;
    }

    // Calculate starting cluster and offset within cluster
    fat32_log("fat32_read: calculating cluster size, bytes_per_sector=0x");
    fat32_log_hex(ctx->bytes_per_sector);
    fat32_log(", sectors_per_cluster=0x");
    fat32_log_hex(ctx->sectors_per_cluster);
    fat32_log("\n");

    size_t cluster_size = static_cast<size_t>(ctx->bytes_per_sector) * ctx->sectors_per_cluster;

    fat32_log("fat32_read: cluster_size=0x");
    fat32_log_hex(cluster_size);
    fat32_log("\n");

    fat32_log("fat32_read: about to calculate cluster_offset\n");
    uint32_t cluster_offset = offset / cluster_size;
    uint32_t byte_offset = offset % cluster_size;
    uint32_t current_cluster = node->start_cluster;

    fat32_log("fat32_read: cluster_offset=0x");
    fat32_log_hex(cluster_offset);
    fat32_log(", byte_offset=0x");
    fat32_log_hex(byte_offset);
    fat32_log(", current_cluster=0x");
    fat32_log_hex(current_cluster);
    fat32_log("\n");

    // Skip to the correct cluster
    for (uint32_t i = 0; i < cluster_offset; ++i) {
        current_cluster = get_next_cluster(ctx, current_cluster);
        if (current_cluster == 0) {
            return -1;
        }
    }

    fat32_log("fat32_read: about to allocate cluster buffer of size 0x");
    fat32_log_hex(cluster_size);
    fat32_log("\n");

    // Allocate buffer for reading clusters
    uint8_t* cluster_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(cluster_size));

    fat32_log("fat32_read: cluster_buf allocated at 0x");
    fat32_log_hex(reinterpret_cast<uintptr_t>(cluster_buf));
    fat32_log("\n");
    if (cluster_buf == nullptr) {
        fat32_log("fat32_read: failed to allocate cluster buffer\n");
        return -1;
    }

    // Read data
    size_t bytes_read = 0;
    auto* dest = reinterpret_cast<uint8_t*>(buf);

    while (bytes_read < to_read && current_cluster >= 2 && current_cluster < FAT32_EOC) {
        // Read the cluster from disk
        if (read_cluster(ctx, current_cluster, cluster_buf) != 0) {
            fat32_log("fat32_read: failed to read cluster\n");
            break;
        }

        size_t bytes_in_cluster = std::min(to_read - bytes_read, cluster_size - byte_offset);
        memcpy(dest, cluster_buf + byte_offset, bytes_in_cluster);

        bytes_read += bytes_in_cluster;
        dest += bytes_in_cluster;
        byte_offset = 0;
        current_cluster = get_next_cluster(ctx, current_cluster);
    }

    // Note: Not freeing cluster_buf to avoid kmalloc::free issues

    return (ssize_t)bytes_read;
}

// Flush FAT table to disk
auto flush_fat_table(const FAT32MountContext* ctx) -> int {
    if (ctx == nullptr || ctx->device == nullptr || ctx->fat_table == nullptr) {
        fat32_log("flush_fat_table: invalid context\n");
        return -1;
    }

    // Calculate how many sectors to write
    size_t fat_size_bytes = static_cast<size_t>(ctx->sectors_per_fat) * ctx->bytes_per_sector;
    size_t fat_sectors = (fat_size_bytes + ctx->bytes_per_sector - 1) / ctx->bytes_per_sector;

    fat32_log("flush_fat_table: writing FAT table, ");
    fat32_log_hex(fat_sectors);
    fat32_log(" sectors\n");

    // Write FAT table to both FAT1 and FAT2
    int result = ker::dev::block_write(ctx->device, ctx->partition_offset + ctx->reserved_sectors, static_cast<uint32_t>(fat_sectors),
                                       ctx->fat_table);

    if (result != 0) {
        fat32_log("flush_fat_table: failed to write FAT1\n");
        return -1;
    }

    // Write to FAT2 as well (backup FAT)
    result = ker::dev::block_write(ctx->device, ctx->partition_offset + ctx->reserved_sectors + ctx->sectors_per_fat,
                                   static_cast<uint32_t>(fat_sectors), ctx->fat_table);

    if (result != 0) {
        fat32_log("flush_fat_table: failed to write FAT2\n");
        return -1;
    }

    fat32_log("flush_fat_table: FAT table flushed successfully\n");
    return 0;
}

// Update file's directory entry on disk
auto update_directory_entry(const FAT32Node* node, uint32_t new_size) -> int {
    if (node == nullptr || node->context == nullptr || node->context->device == nullptr) {
        fat32_log("update_directory_entry: invalid node or context\n");
        return -1;
    }

    const FAT32MountContext* ctx = node->context;
    uint32_t cluster_size = static_cast<uint32_t>(ctx->bytes_per_sector) * ctx->sectors_per_cluster;

    fat32_log("update_directory_entry: updating entry at cluster 0x");
    fat32_log_hex(node->dir_entry_cluster);
    fat32_log(", offset 0x");
    fat32_log_hex(node->dir_entry_offset);
    fat32_log(", new size 0x");
    fat32_log_hex(new_size);
    fat32_log("\n");

    // Allocate buffer for the cluster
    auto* cluster_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(cluster_size));
    if (cluster_buf == nullptr) {
        fat32_log("update_directory_entry: failed to allocate cluster buffer\n");
        return -1;
    }

    // Read the cluster containing the directory entry
    uint64_t cluster_lba =
        ctx->partition_offset + ctx->data_start_sector + ((static_cast<uint64_t>(node->dir_entry_cluster) - 2) * ctx->sectors_per_cluster);
    if (ker::dev::block_read(ctx->device, cluster_lba, ctx->sectors_per_cluster, cluster_buf) != 0) {
        fat32_log("update_directory_entry: failed to read directory cluster\n");
        return -1;
    }

    // Get the directory entry
    auto* entry = reinterpret_cast<FAT32DirectoryEntry*>(cluster_buf + node->dir_entry_offset);

    // Update the file size
    entry->file_size = new_size;

    fat32_log("update_directory_entry: updated entry, writing cluster back\n");

    // Write the cluster back to disk
    if (ker::dev::block_write(ctx->device, cluster_lba, ctx->sectors_per_cluster, cluster_buf) != 0) {
        fat32_log("update_directory_entry: failed to write directory cluster\n");
        return -1;
    }

    fat32_log("update_directory_entry: directory entry updated successfully\n");
    return 0;
}

// Write to a FAT32 file
auto fat32_write(File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    if ((f == nullptr) || (f->private_data == nullptr) || (buf == nullptr)) {
        return -1;
    }

    auto* node = static_cast<FAT32Node*>(f->private_data);
    if (node->context == nullptr) {
        fat32_log("fat32_write: no mount context available\n");
        return -1;
    }

    const FAT32MountContext* ctx = node->context;
    if (ctx->device == nullptr) {
        fat32_log("fat32_write: no block device available\n");
        return -1;
    }

    // Calculate starting cluster and offset within cluster
    size_t cluster_size = static_cast<size_t>(ctx->bytes_per_sector) * ctx->sectors_per_cluster;
    uint32_t cluster_offset = offset / cluster_size;
    uint32_t byte_offset = offset % cluster_size;
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
            fat32_log("fat32_write: no free clusters\n");
            return -1;
        }
        byte_offset = 0;
        cluster_offset = 0;
    }

    // Skip to the correct cluster
    for (uint32_t i = 0; i < cluster_offset; ++i) {
        uint32_t next = ctx->fat_table[current_cluster] & FAT32_EOC;
        if (next >= FAT32_EOC) {
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
            current_cluster = next;
        }
    }

    // Write data
    size_t bytes_written = 0;
    const auto* src = reinterpret_cast<const uint8_t*>(buf);
    auto* cluster_buf = new uint8_t[cluster_size];

    while (bytes_written < count && current_cluster != 0) {
        // Read current cluster first
        uint64_t cluster_sector =
            ctx->partition_offset + ctx->data_start_sector + ((static_cast<uint64_t>(current_cluster) - 2) * ctx->sectors_per_cluster);
        if (ker::dev::block_read(ctx->device, cluster_sector, ctx->sectors_per_cluster, cluster_buf) != 0) {
            fat32_log("fat32_write: failed to read cluster\n");
            delete[] cluster_buf;
            return -1;
        }

        // Write data into cluster buffer
        size_t bytes_in_cluster = std::min(count - bytes_written, cluster_size - byte_offset);
        memcpy(cluster_buf + byte_offset, src, bytes_in_cluster);

        // Write cluster back to device
        if (ker::dev::block_write(ctx->device, cluster_sector, ctx->sectors_per_cluster, cluster_buf) != 0) {
            fat32_log("fat32_write: failed to write cluster\n");
            delete[] cluster_buf;
            return -1;
        }

        bytes_written += bytes_in_cluster;
        src += bytes_in_cluster;
        byte_offset = 0;

        // Update file size
        node->file_size = std::max<size_t>(offset + bytes_written, node->file_size);

        // Get next cluster
        uint32_t next = ctx->fat_table[current_cluster] & FAT32_EOC;
        if (next >= FAT32_EOC) {
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
            current_cluster = next;
        }
    }

    delete[] cluster_buf;

    // Update the file node with the new size
    node->file_size = offset + bytes_written;

    // Flush FAT table to disk
    if (flush_fat_table(ctx) != 0) {
        fat32_log("fat32_write: failed to flush FAT table\n");
        return -1;
    }

    // Update the directory entry on disk with the new file size
    if (update_directory_entry(node, node->file_size) != 0) {
        fat32_log("fat32_write: failed to update directory entry\n");
        return -1;
    }

    // Flush block device
    if (ctx->device != nullptr && ker::dev::block_flush(ctx->device) != 0) {
        fat32_log("fat32_write: failed to flush device\n");
        return -1;
    }

    fat32_log("fat32_write: wrote ");
    fat32_log_hex(bytes_written);
    fat32_log(" bytes, new file size: ");
    fat32_log_hex(node->file_size);
    fat32_log("\n");

    return (ssize_t)bytes_written;
}

// Seek in a FAT32 file
auto fat32_lseek(ker::vfs::File* f, off_t offset, int whence) -> off_t {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -1;
    }

    auto* node = static_cast<FAT32Node*>(f->private_data);
    off_t newpos = f->pos;

    switch (whence) {
        case 0:  // SEEK_SET
            newpos = offset;
            break;
        case 1:  // SEEK_CUR
            newpos = f->pos + offset;
            break;
        case 2:  // SEEK_END
            newpos = (off_t)node->file_size + offset;
            break;
        default:
            return -1;
    }

    if (newpos < 0) {
        return -1;
    }
    f->pos = newpos;
    return f->pos;
}

// Close a FAT32 file
auto fat32_close(ker::vfs::File* f) -> int {
    if ((f == nullptr) || (f->private_data == nullptr)) {
        return -1;
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
auto fat32_readdir(ker::vfs::File* f, DirEntry* entry, size_t index) -> int {
    if (f == nullptr || f->private_data == nullptr || entry == nullptr) {
        return -1;
    }

    auto* node = static_cast<FAT32Node*>(f->private_data);

    // Check if this is a directory
    if (!node->is_directory) {
        fat32_log("fat32_readdir: not a directory\n");
        return -1;
    }

    // Read directory entries from the directory's cluster chain
    if (node->context == nullptr) {
        fat32_log("fat32_readdir: no mount context\n");
        return -1;
    }
    size_t cluster_size = node->context->bytes_per_sector * node->context->sectors_per_cluster;
    auto* cluster_buf = new uint8_t[cluster_size];
    if (cluster_buf == nullptr) {
        return -1;
    }

    uint32_t current_cluster = node->start_cluster;
    size_t entries_seen = 0;
    bool found = false;

    while (current_cluster >= 2 && current_cluster < FAT32_EOC) {
        // Read the cluster
        if (read_cluster(node->context, current_cluster, cluster_buf) != 0) {
            delete[] cluster_buf;
            return -1;
        }

        // Parse directory entries
        size_t num_entries = cluster_size / sizeof(FAT32DirectoryEntry);
        auto* entries = reinterpret_cast<FAT32DirectoryEntry*>(cluster_buf);

        for (size_t i = 0; i < num_entries; ++i) {
            auto* dir_entry = &entries[i];

            // End of directory
            if (dir_entry->name[0] == 0x00) {
                delete[] cluster_buf;
                return -1;  // No more entries
            }

            // Skip deleted entries
            if (dir_entry->name[0] == (char)0xE5) {
                continue;
            }

            // Skip long filename entries and volume IDs
            if ((dir_entry->attributes & FAT32_ATTR_LONG_NAME) == FAT32_ATTR_LONG_NAME ||
                (dir_entry->attributes & FAT32_ATTR_VOLUME_ID) != 0) {
                continue;
            }

            // Skip . and .. entries
            if (dir_entry->name[0] == '.' && (dir_entry->name[1] == ' ' || dir_entry->name[1] == '.')) {
                continue;
            }

            // Check if this is the entry we're looking for
            if (entries_seen == index) {
                // Fill in the dirent structure
                entry->d_ino = ((uint32_t)dir_entry->cluster_high << 16) | dir_entry->cluster_low;
                entry->d_off = index + 1;
                entry->d_reclen = sizeof(DirEntry);
                entry->d_type = (dir_entry->attributes & FAT32_ATTR_DIRECTORY) != 0 ? DT_DIR : DT_REG;

                // Convert FAT32 name to null-terminated string
                size_t name_idx = 0;
                // Copy filename part (8 chars)
                for (int j = 0; j < 8 && dir_entry->name[j] != ' '; ++j) {
                    entry->d_name[name_idx++] = dir_entry->name[j];
                }
                // Add dot if extension exists
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

                found = true;
                break;
            }
            entries_seen++;
        }

        if (found) {
            break;
        }

        // Get next cluster
        current_cluster = get_next_cluster(node->context, current_cluster);
    }

    delete[] cluster_buf;
    return found ? 0 : -1;
}

// Static storage for FAT32 FileOperations
namespace {
ker::vfs::FileOperations fat32_fops_instance = {
    .vfs_open = nullptr,          // vfs_open
    .vfs_close = fat32_close,     // vfs_close
    .vfs_read = fat32_read,       // vfs_read
    .vfs_write = fat32_write,     // vfs_write
    .vfs_lseek = fat32_lseek,     // vfs_lseek
    .vfs_isatty = fat32_isatty,   // vfs_isatty
    .vfs_readdir = fat32_readdir  // vfs_readdir
};
}

auto get_fat32_fops() -> ker::vfs::FileOperations* { return &fat32_fops_instance; }

void register_fat32() {
    fat32_log("fat32: register_fat32 called\n");
    // Placeholder for registration logic
    // In a full implementation, this would set up the FAT32 filesystem
}

}  // namespace ker::vfs::fat32
