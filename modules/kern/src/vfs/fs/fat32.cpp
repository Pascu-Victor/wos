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
// Support for both buffer and block device based I/O
void* disk_buffer = nullptr;
size_t disk_size = 0;
ker::dev::BlockDevice* block_device = nullptr;
uint64_t partition_offset = 0;  // Offset for GPT partitions

FAT32BootSector* boot_sector = nullptr;
uint32_t* fat_table = nullptr;
constexpr size_t min_bytes_per_sector = 512;
size_t bytes_per_sector = min_bytes_per_sector;
size_t sectors_per_cluster = 1;
size_t fat_start_sector = 0;
size_t data_start_sector = 0;
uint32_t total_sectors_32 = 0;

// Simple file node for FAT32
struct FAT32Node {
    uint32_t start_cluster;
    uint32_t file_size;
    uint8_t attributes;
    char* name;
};

// Read a sector from disk (handles both buffer and block device)
auto disk_read_sector(uint64_t sector, void* buffer) -> int {
    if (block_device != nullptr) {
        return ker::dev::block_read(block_device, partition_offset + sector, 1, buffer);
    } else if (disk_buffer != nullptr) {
        size_t offset = sector * min_bytes_per_sector;
        if (offset + min_bytes_per_sector > disk_size) {
            return -1;
        }
        memcpy(buffer, reinterpret_cast<uint8_t*>(disk_buffer) + offset, min_bytes_per_sector);
        return 0;
    }
    return -1;
}

// Write a sector to disk (handles both buffer and block device)
auto disk_write_sector(uint64_t sector, const void* buffer) -> int {
    if (block_device != nullptr) {
        return ker::dev::block_write(block_device, partition_offset + sector, 1, buffer);
    } else if (disk_buffer != nullptr) {
        size_t offset = sector * min_bytes_per_sector;
        if (offset + min_bytes_per_sector > disk_size) return -1;
        memcpy(reinterpret_cast<uint8_t*>(disk_buffer) + offset, buffer, min_bytes_per_sector);
        return 0;
    }
    return -1;
}
}  // namespace

// Initialize FAT32 from disk buffer
auto fat32_init(void* buffer, size_t size) -> int {
    if ((buffer == nullptr) || size < min_bytes_per_sector) {
        mod::io::serial::write("fat32_init: invalid buffer\n");
        return -1;
    }

    disk_buffer = buffer;
    disk_size = size;
    boot_sector = reinterpret_cast<FAT32BootSector*>(buffer);

    // Validate boot sector signature
    if (size < min_bytes_per_sector) {
        mod::io::serial::write("fat32_init: disk too small\n");
        return -1;
    }

    bytes_per_sector = boot_sector->bytes_per_sector;
    sectors_per_cluster = boot_sector->sectors_per_cluster;
    fat_start_sector = boot_sector->reserved_sectors;

    // Calculate data start sector
    uint32_t sectors_per_fat = boot_sector->sectors_per_fat_32;
    uint32_t num_fats = boot_sector->num_fats;
    data_start_sector = fat_start_sector + (static_cast<size_t>(sectors_per_fat * num_fats));

    // Load FAT into memory
    size_t fat_offset = fat_start_sector * bytes_per_sector;
    if (fat_offset + (sectors_per_fat * bytes_per_sector) > size) {
        mod::io::serial::write("fat32_init: disk too small for FAT\n");
        return -1;
    }

    fat_table = reinterpret_cast<uint32_t*>(reinterpret_cast<uint8_t*>(buffer) + fat_offset);

    mod::io::serial::write("fat32_init: initialized successfully\n");
    return 0;
}

// Initialize FAT32 from a block device
auto fat32_init_device(ker::dev::BlockDevice* device, uint64_t partition_start_lba) -> int {
    if (device == nullptr) {
        mod::io::serial::write("fat32_init_device: invalid device\n");
        return -1;
    }

    block_device = device;
    partition_offset = partition_start_lba;
    mod::io::serial::write("fat32_init_device: reading boot sector from LBA 0x");
    mod::io::serial::writeHex(partition_start_lba);
    mod::io::serial::write("\n");

    // Allocate buffer for boot sector
    auto* boot_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(device->block_size));
    if (boot_buf == nullptr) {
        mod::io::serial::write("fat32_init_device: failed to allocate boot sector buffer\n");
        return -1;
    }

    // Read boot sector from device at partition offset
    if (ker::dev::block_read(device, partition_start_lba, 1, boot_buf) != 0) {
        mod::io::serial::write("fat32_init_device: failed to read boot sector\n");
        // Note: Not freeing to avoid kmalloc issues
        return -1;
    }

    boot_sector = reinterpret_cast<FAT32BootSector*>(boot_buf);

    // Debug: Print boot sector signature
    mod::io::serial::write("Boot sector signature: 0x");
    mod::io::serial::writeHex(*reinterpret_cast<uint16_t*>(boot_buf + 510));
    mod::io::serial::write("\n");

    bytes_per_sector = boot_sector->bytes_per_sector;
    sectors_per_cluster = boot_sector->sectors_per_cluster;
    fat_start_sector = boot_sector->reserved_sectors;
    total_sectors_32 = boot_sector->total_sectors_32;

    // Debug: Print boot sector values
    mod::io::serial::write("bytes_per_sector: ");
    mod::io::serial::writeHex(bytes_per_sector);
    mod::io::serial::write("\n");
    mod::io::serial::write("sectors_per_cluster: ");
    mod::io::serial::writeHex(sectors_per_cluster);
    mod::io::serial::write("\n");
    mod::io::serial::write("reserved_sectors: ");
    mod::io::serial::writeHex(fat_start_sector);
    mod::io::serial::write("\n");

    // Calculate data start sector (relative to partition start)
    uint32_t sectors_per_fat = boot_sector->sectors_per_fat_32;
    uint32_t num_fats = boot_sector->num_fats;

    mod::io::serial::write("sectors_per_fat: ");
    mod::io::serial::writeHex(sectors_per_fat);
    mod::io::serial::write("\n");
    mod::io::serial::write("num_fats: ");
    mod::io::serial::writeHex(num_fats);
    mod::io::serial::write("\n");

    data_start_sector = fat_start_sector + (static_cast<size_t>(sectors_per_fat * num_fats));

    // Allocate and read FAT table
    size_t fat_size = sectors_per_fat * bytes_per_sector;

    mod::io::serial::write("FAT size to allocate: ");
    mod::io::serial::writeHex(fat_size);
    mod::io::serial::write(" bytes\n");

    // Validate boot sector values before allocation
    if (bytes_per_sector == 0 || bytes_per_sector > 4096 || sectors_per_fat == 0 || sectors_per_fat > 0xFFFF || fat_size == 0 ||
        fat_size > 64 * 1024 * 1024) {  // Sanity check: max 64MB FAT
        mod::io::serial::write("fat32_init_device: invalid boot sector values\n");
        // Note: Not freeing to avoid kmalloc issues
        return -1;
    }

    fat_table = static_cast<uint32_t*>(ker::mod::mm::dyn::kmalloc::malloc(fat_size));

    if (fat_table == nullptr) {
        mod::io::serial::write("fat32_init_device: failed to allocate FAT table\n");
        // Note: Not freeing boot_buf to avoid kmalloc issues
        return -1;
    }

    // Read FAT from device (adjusted for partition offset)
    size_t fat_sectors_to_read = (fat_size + device->block_size - 1) / device->block_size;
    if (ker::dev::block_read(device, partition_start_lba + fat_start_sector, fat_sectors_to_read, fat_table) != 0) {
        mod::io::serial::write("fat32_init_device: failed to read FAT\n");
        // Note: Not freeing to avoid kmalloc issues
        fat_table = nullptr;
        return -1;
    }

    disk_buffer = nullptr;  // Not using buffer-based I/O
    disk_size = 0;

    mod::io::serial::write("fat32_init_device: initialized successfully\n");
    return 0;
}

// Helper to get cluster data from disk
static auto get_cluster_data(uint32_t cluster) -> uint8_t* {
    if ((disk_buffer == nullptr) || cluster == 0) {
        return nullptr;
    }
    size_t cluster_offset = (data_start_sector + (cluster - 2) * sectors_per_cluster) * bytes_per_sector;
    if (cluster_offset + bytes_per_sector * sectors_per_cluster > disk_size) {
        return nullptr;
    }
    return reinterpret_cast<uint8_t*>(disk_buffer) + cluster_offset;
}

// Helper to get next cluster in chain
static auto get_next_cluster(uint32_t cluster) -> uint32_t {
    if (fat_table == nullptr) {
        mod::io::serial::write("get_next_cluster: fat_table is null\n");
        return 0;
    }
    if (cluster >= FAT32_EOC) {
        return 0;
    }

    // Sanity check: make sure we're not accessing beyond reasonable bounds
    if (cluster > FAT32_EOC) {
        return 0;
    }

    uint32_t next = fat_table[cluster] & FAT32_EOC;

    if (next >= FAT32_EOC) {
        return 0;
    }
    return next;
}

// Helper function to read a cluster from the block device
static auto read_cluster(uint32_t cluster, void* buffer) -> int {
    if (block_device == nullptr || cluster < 2) {
        return -1;
    }

    // Calculate the LBA of the cluster
    uint64_t cluster_lba = partition_offset + data_start_sector + ((cluster - 2) * sectors_per_cluster);

    // Read the cluster
    return ker::dev::block_read(block_device, cluster_lba, sectors_per_cluster, buffer);
}

// Helper function to compare FAT32 filenames (8.3 format with spaces)
static auto compare_fat32_name(const char* dir_name, const char* search_name) -> bool {
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
auto fat32_open_path(const char* path, int flags, int mode) -> ker::vfs::File* {
    mod::io::serial::write("fat32_open_path: called with path='");
    mod::io::serial::write(path);
    mod::io::serial::write("'\n");

    if (boot_sector == nullptr || block_device == nullptr) {
        mod::io::serial::write("fat32_open_path: not initialized\n");
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

    mod::io::serial::write("fat32_open_path: searching for '");
    mod::io::serial::write(filename);
    mod::io::serial::write("'\n");

    // Search root directory for the file
    size_t cluster_size = bytes_per_sector * sectors_per_cluster;
    auto* cluster_buf = new uint8_t[cluster_size];
    if (cluster_buf == nullptr) {
        mod::io::serial::write("fat32_open_path: failed to allocate cluster buffer\n");
        return nullptr;
    }

    uint32_t current_cluster = boot_sector->root_cluster;
    bool found = false;
    FAT32DirectoryEntry found_entry;

    while (current_cluster >= 2 && current_cluster < FAT32_EOC) {
        // Read the cluster
        if (read_cluster(current_cluster, cluster_buf) != 0) {
            mod::io::serial::write("fat32_open_path: failed to read cluster\n");
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
                goto search_done;
            }
        }

        // Get next cluster
        if (current_cluster < total_sectors_32 / sectors_per_cluster) {
            current_cluster = get_next_cluster(current_cluster);
        } else {
            break;
        }
    }

search_done:

    if (!found) {
        mod::io::serial::write("fat32_open_path: file not found\n");
        delete[] cluster_buf;
        return nullptr;
    }

    // Create file node
    auto* node = new FAT32Node;
    node->start_cluster = ((uint32_t)found_entry.cluster_high << 16) | found_entry.cluster_low;
    node->file_size = found_entry.file_size;
    node->attributes = found_entry.attributes;
    node->name = nullptr;

    auto* f = new File;
    f->private_data = node;
    f->fd = -1;
    f->pos = 0;
    f->fops = nullptr;  // Will be set by vfs_open

    return f;
}

// Read from a FAT32 file
auto fat32_read(ker::vfs::File* f, void* buf, size_t count, size_t offset) -> ssize_t {
    mod::io::serial::write("fat32_read: called, count=0x");
    mod::io::serial::writeHex(count);
    mod::io::serial::write(", offset=0x");
    mod::io::serial::writeHex(offset);
    mod::io::serial::write("\n");

    if ((f == nullptr) || (f->private_data == nullptr)) {
        mod::io::serial::write("fat32_read: invalid file or private_data\n");
        return -1;
    }

    auto* node = static_cast<FAT32Node*>(f->private_data);
    mod::io::serial::write("fat32_read: file_size=0x");
    mod::io::serial::writeHex(node->file_size);
    mod::io::serial::write(", start_cluster=0x");
    mod::io::serial::writeHex(node->start_cluster);
    mod::io::serial::write("\n");

    if ((buf == nullptr) || count == 0) {
        return 0;
    }

    // Simple read implementation: traverse cluster chain
    uint32_t bytes_available = node->file_size - std::min(offset, (size_t)node->file_size);
    size_t to_read = std::min(count, (size_t)bytes_available);

    mod::io::serial::write("fat32_read: to_read=0x");
    mod::io::serial::writeHex(to_read);
    mod::io::serial::write("\n");

    if (to_read == 0) {
        return 0;
    }

    // Calculate starting cluster and offset within cluster
    mod::io::serial::write("fat32_read: calculating cluster size, bytes_per_sector=0x");
    mod::io::serial::writeHex(bytes_per_sector);
    mod::io::serial::write(", sectors_per_cluster=0x");
    mod::io::serial::writeHex(sectors_per_cluster);
    mod::io::serial::write("\n");

    size_t cluster_size = bytes_per_sector * sectors_per_cluster;

    mod::io::serial::write("fat32_read: cluster_size=0x");
    mod::io::serial::writeHex(cluster_size);
    mod::io::serial::write("\n");

    mod::io::serial::write("fat32_read: about to calculate cluster_offset\n");
    uint32_t cluster_offset = offset / cluster_size;
    uint32_t byte_offset = offset % cluster_size;
    uint32_t current_cluster = node->start_cluster;

    mod::io::serial::write("fat32_read: cluster_offset=0x");
    mod::io::serial::writeHex(cluster_offset);
    mod::io::serial::write(", byte_offset=0x");
    mod::io::serial::writeHex(byte_offset);
    mod::io::serial::write(", current_cluster=0x");
    mod::io::serial::writeHex(current_cluster);
    mod::io::serial::write("\n");

    // Skip to the correct cluster
    for (uint32_t i = 0; i < cluster_offset; ++i) {
        current_cluster = get_next_cluster(current_cluster);
        if (current_cluster == 0) {
            return -1;
        }
    }

    mod::io::serial::write("fat32_read: about to allocate cluster buffer of size 0x");
    mod::io::serial::writeHex(cluster_size);
    mod::io::serial::write("\n");

    // Allocate buffer for reading clusters
    uint8_t* cluster_buf = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(cluster_size));

    mod::io::serial::write("fat32_read: cluster_buf allocated at 0x");
    mod::io::serial::writeHex(reinterpret_cast<uintptr_t>(cluster_buf));
    mod::io::serial::write("\n");
    if (!cluster_buf) {
        mod::io::serial::write("fat32_read: failed to allocate cluster buffer\n");
        return -1;
    }

    // Read data
    size_t bytes_read = 0;
    auto* dest = reinterpret_cast<uint8_t*>(buf);

    while (bytes_read < to_read && current_cluster >= 2 && current_cluster < FAT32_EOC) {
        // Read the cluster from disk
        if (block_device) {
            if (read_cluster(current_cluster, cluster_buf) != 0) {
                mod::io::serial::write("fat32_read: failed to read cluster\n");
                break;
            }
        } else if (disk_buffer) {
            uint8_t* cluster_data = get_cluster_data(current_cluster);
            if (cluster_data == nullptr) {
                break;
            }
            memcpy(cluster_buf, cluster_data, cluster_size);
        } else {
            break;
        }

        size_t bytes_in_cluster = std::min(to_read - bytes_read, cluster_size - byte_offset);
        memcpy(dest, cluster_buf + byte_offset, bytes_in_cluster);

        bytes_read += bytes_in_cluster;
        dest += bytes_in_cluster;
        byte_offset = 0;
        current_cluster = get_next_cluster(current_cluster);
    }

    // Note: Not freeing cluster_buf to avoid kmalloc::free issues

    return (ssize_t)bytes_read;
}

// Write to a FAT32 file
auto fat32_write(File* f, const void* buf, size_t count, size_t offset) -> ssize_t {
    if ((f == nullptr) || (f->private_data == nullptr) || (buf == nullptr)) {
        return -1;
    }

    auto* node = static_cast<FAT32Node*>(f->private_data);
    if (block_device == nullptr) {
        mod::io::serial::write("fat32_write: no block device available\n");
        return -1;
    }

    // Calculate starting cluster and offset within cluster
    size_t cluster_size = bytes_per_sector * sectors_per_cluster;
    uint32_t cluster_offset = offset / cluster_size;
    uint32_t byte_offset = offset % cluster_size;
    uint32_t current_cluster = node->start_cluster;

    // If starting from cluster 0 (new file), allocate first cluster
    if (current_cluster == 0) {
        // Simple cluster allocation: find first free cluster
        // In a real implementation, use a free cluster bitmap
        for (uint32_t i = 2; i < FAT32_EOC; ++i) {
            if ((fat_table[i] & FAT32_EOC) == 0) {
                current_cluster = i;
                fat_table[i] = FAT32_EOC;
                node->start_cluster = i;
                break;
            }
        }
        if (current_cluster == 0) {
            mod::io::serial::write("fat32_write: no free clusters\n");
            return -1;
        }
        byte_offset = 0;
        cluster_offset = 0;
    }

    // Skip to the correct cluster
    for (uint32_t i = 0; i < cluster_offset; ++i) {
        uint32_t next = fat_table[current_cluster] & FAT32_EOC;
        if (next >= FAT32_EOC) {
            // Need to allocate new cluster
            for (uint32_t j = 2; j < FAT32_EOC; ++j) {
                if ((fat_table[j] & FAT32_EOC) == 0) {
                    fat_table[current_cluster] = j;
                    fat_table[j] = FAT32_EOC;
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
        uint64_t cluster_sector = data_start_sector + ((current_cluster - 2) * sectors_per_cluster);
        if (ker::dev::block_read(block_device, cluster_sector, sectors_per_cluster, cluster_buf) != 0) {
            mod::io::serial::write("fat32_write: failed to read cluster\n");
            delete[] cluster_buf;
            return -1;
        }

        // Write data into cluster buffer
        size_t bytes_in_cluster = std::min(count - bytes_written, cluster_size - byte_offset);
        memcpy(cluster_buf + byte_offset, src, bytes_in_cluster);

        // Write cluster back to device
        if (ker::dev::block_write(block_device, cluster_sector, sectors_per_cluster, cluster_buf) != 0) {
            mod::io::serial::write("fat32_write: failed to write cluster\n");
            delete[] cluster_buf;
            return -1;
        }

        bytes_written += bytes_in_cluster;
        src += bytes_in_cluster;
        byte_offset = 0;

        // Update file size
        node->file_size = std::max<size_t>(offset + bytes_written, node->file_size);

        // Get next cluster
        uint32_t next = fat_table[current_cluster] & FAT32_EOC;
        if (next >= FAT32_EOC) {
            if (bytes_written < count) {
                // Allocate new cluster
                for (uint32_t j = 2; j < FAT32_EOC; ++j) {
                    if ((fat_table[j] & FAT32_EOC) == 0) {
                        fat_table[current_cluster] = j;
                        fat_table[j] = FAT32_EOC;
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

    // Flush FAT table to disk
    if (block_device != nullptr && ker::dev::block_flush(block_device) != 0) {
        mod::io::serial::write("fat32_write: failed to flush device\n");
        return -1;
    }

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
    delete node->name;
    delete node;
    return 0;
}

// Static storage for FAT32 FileOperations
namespace {
ker::vfs::FileOperations fat32_fops_instance = {
    .vfs_open = nullptr,       // vfs_open
    .vfs_close = fat32_close,  // vfs_close
    .vfs_read = fat32_read,    // vfs_read
    .vfs_write = fat32_write,  // vfs_write
    .vfs_lseek = fat32_lseek   // vfs_lseek
};
}

auto get_fat32_fops() -> ker::vfs::FileOperations* { return &fat32_fops_instance; }

void register_fat32() {
    mod::io::serial::write("fat32: register_fat32 called\n");
    // Placeholder for registration logic
    // In a full implementation, this would set up the FAT32 filesystem
}

}  // namespace ker::vfs::fat32
