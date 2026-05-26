#include "ramdisk.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <new>
#include <platform/dbg/dbg.hpp>

#include "dev/block_device.hpp"

namespace ker::dev::ramdisk {

using log = ker::mod::dbg::logger<"ramdisk">;

namespace {

struct RamdiskPrivate {
    uint8_t* buffer;
    size_t size_bytes;
};

auto ramdisk_read_blocks(BlockDevice* bdev, uint64_t block, size_t count, void* buffer) -> int {
    if (bdev == nullptr || buffer == nullptr) {
        return -1;
    }

    auto* priv = static_cast<RamdiskPrivate*>(bdev->private_data);
    if (priv == nullptr || priv->buffer == nullptr) {
        return -1;
    }

    // Calculate byte offset
    uint64_t const BYTE_OFFSET = block * bdev->block_size;
    size_t const READ_SIZE = count * bdev->block_size;

    // Bounds check
    if (BYTE_OFFSET + READ_SIZE > priv->size_bytes) {
        return -1;
    }

    auto* out = static_cast<uint8_t*>(buffer);
    std::copy_n(priv->buffer + BYTE_OFFSET, READ_SIZE, out);
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
    uint64_t const BYTE_OFFSET = block * bdev->block_size;
    size_t const WRITE_SIZE = count * bdev->block_size;

    // Bounds check
    if (BYTE_OFFSET + WRITE_SIZE > priv->size_bytes) {
        return -1;
    }

    const auto* in = static_cast<const uint8_t*>(buffer);
    std::copy_n(in, WRITE_SIZE, priv->buffer + BYTE_OFFSET);
    return 0;
}

auto ramdisk_flush(BlockDevice* bdev) -> int {
    (void)bdev;  // Not used
    return 0;    // No-op for RAM disk
}
}  // namespace

auto ramdisk_create(size_t size_bytes) -> BlockDevice* {
    if (size_bytes == 0) {
        log::warn("ramdisk_create: invalid size");
        return nullptr;
    }

    // Allocate private data
    auto* priv = new (std::nothrow) RamdiskPrivate{};
    if (priv == nullptr) {
        log::warn("ramdisk_create: failed to allocate private data");
        return nullptr;
    }

    // Allocate buffer
    priv->buffer = new (std::nothrow) uint8_t[size_bytes];
    if (priv->buffer == nullptr) {
        log::warn("ramdisk_create: failed to allocate buffer");
        delete priv;
        return nullptr;
    }

    // Initialize buffer to zero
    std::fill_n(priv->buffer, size_bytes, uint8_t{0});
    priv->size_bytes = size_bytes;

    // Allocate BlockDevice structure
    auto* bdev = new (std::nothrow) BlockDevice{};
    if (bdev == nullptr) {
        log::warn("ramdisk_create: failed to allocate BlockDevice");
        delete[] priv->buffer;
        delete priv;
        return nullptr;
    }

    // Initialize BlockDevice
    constexpr size_t BLOCK_SIZE = 512;
    bdev->major = 1;
    bdev->minor = 0;
    constexpr std::array RAMDISK_NAME = {'r', 'a', 'm', 'd', 'i', 's', 'k', '0', '\0'};
    std::copy_n(RAMDISK_NAME.begin(), RAMDISK_NAME.size(), bdev->name.begin());
    bdev->block_size = BLOCK_SIZE;
    bdev->total_blocks = (size_bytes + BLOCK_SIZE - 1) / BLOCK_SIZE;  // Round up to nearest block
    bdev->private_data = priv;

    bdev->read_blocks = ramdisk_read_blocks;
    bdev->write_blocks = ramdisk_write_blocks;
    bdev->flush = ramdisk_flush;

    log::info("ramdisk_create: created disk with %lu blocks (%zu bytes)", static_cast<unsigned long>(bdev->total_blocks), size_bytes);

    return bdev;
}

auto ramdisk_destroy(BlockDevice* disk) -> int {
    if (disk == nullptr) {
        return -1;
    }

    auto* priv = static_cast<RamdiskPrivate*>(disk->private_data);
    if (priv != nullptr) {
        delete[] priv->buffer;

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
