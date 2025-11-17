#include "block_device.hpp"

#include <cstddef>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <vfs/mount.hpp>
#include <vfs/vfs.hpp>

#include "device.hpp"

namespace ker::dev {

// Block device registry
// TODO: make dynamic
namespace {
constexpr size_t MAX_BLOCK_DEVICES = 16;
BlockDevice* block_devices[MAX_BLOCK_DEVICES] = {};  // NOLINT
size_t device_count = 0;
}  // namespace

auto block_device_register(BlockDevice* bdev) -> int {
    if (bdev == nullptr) {
        mod::io::serial::write("block_device_register: invalid device\n");
        return -1;
    }

    if (device_count >= MAX_BLOCK_DEVICES) {
        mod::io::serial::write("block_device_register: device table full\n");
        return -1;
    }

    block_devices[device_count] = bdev;
    device_count++;

    mod::io::serial::write("block_device_register: registered ");
    mod::io::serial::write(bdev->name.data());
    mod::io::serial::write("\n");

    // Also register as a device node in /dev
    // Create a Device wrapper for this block device
    static Device block_dev_nodes[MAX_BLOCK_DEVICES];  // NOLINT
    Device* dev_node = &block_dev_nodes[device_count - 1];

    dev_node->major = bdev->major;
    dev_node->minor = bdev->minor;
    dev_node->name = bdev->name.data();
    dev_node->type = DeviceType::BLOCK;
    dev_node->private_data = bdev;
    dev_node->char_ops = nullptr;  // Block devices don't use char ops

    dev_register(dev_node);

    return 0;
}

auto block_device_find(unsigned major, unsigned minor) -> BlockDevice* {
    for (size_t i = 0; i < device_count; i++) {
        if (block_devices[i] != nullptr && block_devices[i]->major == major && block_devices[i]->minor == minor) {
            return block_devices[i];
        }
    }
    return nullptr;
}

auto block_device_find_by_name(const char* name) -> BlockDevice* {
    if (name == nullptr) {
        return nullptr;
    }

    for (size_t i = 0; i < device_count; i++) {
        if (block_devices[i] != nullptr && !block_devices[i]->name.empty()) {
            // Simple string comparison
            size_t j = 0;
            while (name[j] != '\0' && block_devices[i]->name[j] != '\0') {
                if (name[j] != block_devices[i]->name[j]) {
                    break;
                }
                j++;
            }
            if (name[j] == '\0' && block_devices[i]->name[j] == '\0') {
                return block_devices[i];
            }
        }
    }
    return nullptr;
}

auto block_read(BlockDevice* bdev, uint64_t block, size_t count, void* buffer) -> int {
    if (bdev == nullptr || buffer == nullptr) {
        return -1;
    }

    if (block + count > bdev->total_blocks) {
        mod::io::serial::write("block_read: read past end of device\n");
        return -1;
    }

    if (bdev->read_blocks == nullptr) {
        mod::io::serial::write("block_read: read_blocks not implemented\n");
        return -1;
    }

    return bdev->read_blocks(bdev, block, count, buffer);
}

auto block_write(BlockDevice* bdev, uint64_t block, size_t count, const void* buffer) -> int {
    if (bdev == nullptr || buffer == nullptr) {
        return -1;
    }

    if (block + count > bdev->total_blocks) {
        mod::io::serial::write("block_write: write past end of device\n");
        return -1;
    }

    if (bdev->write_blocks == nullptr) {
        mod::io::serial::write("block_write: write_blocks not implemented\n");
        return -1;
    }

    return bdev->write_blocks(bdev, block, count, buffer);
}

auto block_flush(BlockDevice* bdev) -> int {
    if (bdev == nullptr) {
        return -1;
    }

    if (bdev->flush == nullptr) {
        return 0;  // No-op if not implemented
    }

    return bdev->flush(bdev);
}

// Initializes block devices and mounts FAT32 filesystems
auto block_device_init() -> void {
    ker::mod::dbg::log("Initializing block devices");

    // Mount FAT32 if block device is available for now just sdb
    // TODO: implement some type of fstab
    BlockDevice* sdb = block_device_find_by_name("sdb");
    if (sdb != nullptr) {
        ker::mod::dbg::log("Found sdb, attempting FAT32 mount at /mnt/disk");
        int mount_result = ker::vfs::mount_filesystem("/mnt/disk", "fat32", sdb);
        if (mount_result == 0) {
            ker::mod::dbg::log("FAT32 mount successful!");
        } else {
            ker::mod::dbg::log("FAT32 mount failed with error: %d", mount_result);
        }
    } else {
        ker::mod::dbg::log("No block devices found (sdb not available)");
    }
}

}  // namespace ker::dev
