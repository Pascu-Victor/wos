#include "block_device.hpp"

#include <cstddef>
#include <cstring>
#include <dev/gpt.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

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

auto block_device_unregister(BlockDevice* bdev) -> int {
    if (bdev == nullptr) {
        return -1;
    }
    for (size_t i = 0; i < device_count; i++) {
        if (block_devices[i] == bdev) {
            block_devices[i] = nullptr;

            // Also remove the corresponding /dev node
            Device* dev_node = dev_find_by_name(bdev->name.data());
            if (dev_node != nullptr && dev_node->private_data == bdev) {
                dev_unregister(dev_node);
            }

            mod::io::serial::write("block_device_unregister: removed ");
            mod::io::serial::write(bdev->name.data());
            mod::io::serial::write("\n");
            return 0;
        }
    }
    return -1;
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

auto block_device_count() -> size_t { return device_count; }

auto block_device_at(size_t index) -> BlockDevice* {
    if (index >= device_count) {
        return nullptr;
    }
    return block_devices[index];
}

auto block_device_find_by_partuuid(const char* uuid_str) -> BlockDevice* {
    if (uuid_str == nullptr) {
        return nullptr;
    }
    for (size_t i = 0; i < device_count; i++) {
        if (block_devices[i] != nullptr && block_devices[i]->is_partition) {
            // Compare PARTUUID strings
            bool match = true;
            for (size_t j = 0; j < PARTUUID_STRING_SIZE - 1; ++j) {
                if (block_devices[i]->partuuid_str[j] != uuid_str[j]) {
                    match = false;
                    break;
                }
                if (uuid_str[j] == '\0') {
                    break;
                }
            }
            if (match && uuid_str[PARTUUID_STRING_SIZE - 1] == '\0') {
                return block_devices[i];
            }
        }
    }
    return nullptr;
}

namespace {
// Read function for partition block devices: offsets LBA by partition start
auto partition_read(BlockDevice* dev, uint64_t block, size_t count, void* buffer) -> int {
    if (dev == nullptr || dev->parent_disk == nullptr) {
        return -1;
    }
    return block_read(dev->parent_disk, dev->partition_start_lba + block, count, buffer);
}

// Write function for partition block devices: offsets LBA by partition start
auto partition_write(BlockDevice* dev, uint64_t block, size_t count, const void* buffer) -> int {
    if (dev == nullptr || dev->parent_disk == nullptr) {
        return -1;
    }
    return block_write(dev->parent_disk, dev->partition_start_lba + block, count, buffer);
}

auto partition_flush(BlockDevice* dev) -> int {
    if (dev == nullptr || dev->parent_disk == nullptr) {
        return -1;
    }
    return block_flush(dev->parent_disk);
}

}  // namespace

auto block_device_create_partition(BlockDevice* parent_disk, uint64_t start_lba, uint64_t end_lba, const uint8_t* partuuid,
                                   uint32_t partition_index) -> BlockDevice* {
    if (parent_disk == nullptr || partuuid == nullptr) {
        return nullptr;
    }

    auto* part = new BlockDevice;
    part->major = parent_disk->major;
    part->minor = static_cast<unsigned>(partition_index + 1);

    // Build name: e.g. "sda" + "1" -> "sda1"
    // Find end of parent name
    size_t parent_name_len = 0;
    while (parent_name_len < BLOCK_NAME_SIZE - 1 && parent_disk->name[parent_name_len] != '\0') {
        parent_name_len++;
    }
    // Copy parent name
    for (size_t i = 0; i < parent_name_len; ++i) {
        part->name[i] = parent_disk->name[i];
    }
    // Append 1-based partition number (simple: single digit for index < 9, two digits otherwise)
    uint32_t part_num = partition_index + 1;
    if (part_num < 10) {
        part->name[parent_name_len] = static_cast<char>('0' + part_num);
        part->name[parent_name_len + 1] = '\0';
    } else {
        part->name[parent_name_len] = static_cast<char>('0' + (part_num / 10));
        part->name[parent_name_len + 1] = static_cast<char>('0' + (part_num % 10));
        part->name[parent_name_len + 2] = '\0';
    }

    part->block_size = parent_disk->block_size;
    part->total_blocks = end_lba - start_lba + 1;
    part->read_blocks = partition_read;
    part->write_blocks = partition_write;
    part->flush = partition_flush;
    part->private_data = nullptr;
    part->remotable = parent_disk->remotable;

    part->is_partition = true;
    for (size_t i = 0; i < 16; ++i) {
        part->partuuid[i] = partuuid[i];
    }
    gpt::guid_to_string(partuuid, part->partuuid_str.data());
    part->parent_disk = parent_disk;
    part->partition_start_lba = start_lba;
    part->partition_end_lba = end_lba;

    block_device_register(part);

    ker::mod::dbg::log("Created partition %s PARTUUID=%s", part->name.data(), part->partuuid_str.data());

    return part;
}

// Initializes block devices and enumerates GPT partitions on all registered disks
auto block_device_init() -> void {
    ker::mod::dbg::log("Initializing block devices");

    // Enumerate GPT partitions on all registered whole-disk block devices
    // We snapshot the current count since enumeration will register new partition devices
    size_t disk_count = device_count;
    for (size_t i = 0; i < disk_count; ++i) {
        BlockDevice* disk = block_devices[i];
        if (disk == nullptr || disk->is_partition) {
            continue;
        }

        gpt::GPTDiskInfo disk_info{};
        if (gpt::gpt_enumerate_partitions(disk, &disk_info) != 0) {
            ker::mod::dbg::log("No GPT found on %s", disk->name.data());
            continue;
        }

        ker::mod::dbg::log("GPT: %s has %d partitions", disk->name.data(), disk_info.partition_count);

        for (uint32_t p = 0; p < disk_info.partition_count; ++p) {
            auto& part = disk_info.partitions[p];
            block_device_create_partition(disk, part.starting_lba, part.ending_lba, part.unique_partition_guid.data(), p);
        }
    }

    ker::mod::dbg::log("Block device init complete: %d devices registered", static_cast<unsigned>(device_count));
}

}  // namespace ker::dev
