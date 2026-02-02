#include "ramdisk.hpp"

#include <cstdint>
#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

#include "dev/block_device.hpp"

namespace ker::dev::ramdisk {

struct RamdiskPrivate {
    uint8_t* buffer;
    size_t size_bytes;
};

namespace {
auto ramdisk_read_blocks(BlockDevice* bdev, uint64_t block, size_t count, void* buffer) -> int {
    if (bdev == nullptr || buffer == nullptr) {
        return -1;
    }

    auto* priv = static_cast<RamdiskPrivate*>(bdev->private_data);
    if (priv == nullptr || priv->buffer == nullptr) {
        return -1;
    }

    // Calculate byte offset
    uint64_t byte_offset = block * bdev->block_size;
    size_t read_size = count * bdev->block_size;

    // Bounds check
    if (byte_offset + read_size > priv->size_bytes) {
        return -1;
    }

    // Copy from ramdisk buffer
    std::memcpy(buffer, priv->buffer + byte_offset, read_size);
    return 0;
}

auto ramdisk_write_blocks(BlockDevice* bdev, uint64_t block, size_t count, const void* buffer) -> int {
    if (bdev == nullptr || buffer == nullptr) {
        return -1;
    }

    auto* priv = static_cast<RamdiskPrivate*>(bdev->private_data);
    if (priv == nullptr || priv->buffer == nullptr) {
        return -1;
    }

    // Calculate byte offset
    uint64_t byte_offset = block * bdev->block_size;
    size_t write_size = count * bdev->block_size;

    // Bounds check
    if (byte_offset + write_size > priv->size_bytes) {
        return -1;
    }

    // Copy to ramdisk buffer
    std::memcpy(priv->buffer + byte_offset, buffer, write_size);
    return 0;
}

auto ramdisk_flush(BlockDevice* bdev) -> int {
    (void)bdev;  // Not used
    return 0;    // No-op for RAM disk
}
}  // namespace

auto ramdisk_create(size_t size_bytes) -> BlockDevice* {
    if (size_bytes == 0) {
        ker::mod::io::serial::write("ramdisk_create: invalid size\n");
        return nullptr;
    }

    // Allocate private data
    auto* priv = new RamdiskPrivate;
    if (priv == nullptr) {
        ker::mod::io::serial::write("ramdisk_create: failed to allocate private data\n");
        return nullptr;
    }

    // Allocate buffer
    priv->buffer = static_cast<uint8_t*>(ker::mod::mm::dyn::kmalloc::malloc(size_bytes));
    if (priv->buffer == nullptr) {
        ker::mod::io::serial::write("ramdisk_create: failed to allocate buffer\n");
        delete priv;
        return nullptr;
    }

    // Initialize buffer to zero
    std::memset(priv->buffer, 0, size_bytes);
    priv->size_bytes = size_bytes;

    // Allocate BlockDevice structure
    auto* bdev = new BlockDevice;
    if (bdev == nullptr) {
        ker::mod::io::serial::write("ramdisk_create: failed to allocate BlockDevice\n");
        ker::mod::mm::dyn::kmalloc::free(priv->buffer);
        delete priv;
        return nullptr;
    }

    // Initialize BlockDevice
    constexpr size_t BLOCK_SIZE = 512;
    bdev->major = 1;
    bdev->minor = 0;
    std::strcpy(bdev->name.data(), "ramdisk0");
    bdev->block_size = BLOCK_SIZE;
    bdev->total_blocks = (size_bytes + BLOCK_SIZE - 1) / BLOCK_SIZE;  // Round up to nearest block
    bdev->private_data = priv;

    bdev->read_blocks = ramdisk_read_blocks;
    bdev->write_blocks = ramdisk_write_blocks;
    bdev->flush = ramdisk_flush;

    ker::mod::io::serial::write("ramdisk_create: created disk with ");
    ker::mod::io::serial::write(bdev->total_blocks);
    ker::mod::io::serial::write(" blocks (");
    ker::mod::io::serial::write(size_bytes);
    ker::mod::io::serial::write(" bytes)\n");

    return bdev;
}

auto ramdisk_destroy(BlockDevice* disk) -> int {
    if (disk == nullptr) {
        return -1;
    }

    auto* priv = static_cast<RamdiskPrivate*>(disk->private_data);
    if (priv != nullptr) {
        if (priv->buffer != nullptr) {
            ker::mod::mm::dyn::kmalloc::free(priv->buffer);
        }
        delete priv;
    }

    delete disk;
    return 0;
}

auto ramdisk_get_buffer(BlockDevice* disk) -> void* {
    if (disk == nullptr) {
        return nullptr;
    }

    auto* priv = static_cast<RamdiskPrivate*>(disk->private_data);
    if (priv == nullptr) {
        return nullptr;
    }

    return priv->buffer;
}

}  // namespace ker::dev::ramdisk
