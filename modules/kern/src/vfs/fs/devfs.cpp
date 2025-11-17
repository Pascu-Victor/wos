#include "devfs.hpp"

#include <cstring>
#include <dev/device.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

#include "vfs/file_operations.hpp"

namespace ker::vfs::devfs {

// devfs-specific data stored in File->private_data
struct DevFSFile {
    ker::dev::Device* device;
};

// devfs file operations
namespace {

auto devfs_close(File* f) -> int {
    if (f == nullptr) {
        mod::io::serial::write("devfs_close: file is null\n");
        return -1;
    }

    mod::io::serial::write("devfs_close: closing device file\n");

    // Directory has no private_data
    if (f->private_data == nullptr) {
        mod::io::serial::write("devfs_close: directory or no private_data\n");
        return 0;
    }

    auto* devfs_file = static_cast<DevFSFile*>(f->private_data);
    if (devfs_file->device == nullptr) {
        mod::io::serial::write("devfs_close: device pointer is null\n");
        delete devfs_file;
        f->private_data = nullptr;
        return 0;
    }

    // Call device close if available
    if (devfs_file->device->char_ops != nullptr && devfs_file->device->char_ops->close != nullptr) {
        devfs_file->device->char_ops->close(f);
    }

    // Free devfs-specific data
    delete devfs_file;
    f->private_data = nullptr;
    return 0;
}

auto devfs_read(File* f, void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr) {
        return -1;
    }

    auto* devfs_file = static_cast<DevFSFile*>(f->private_data);

    // Call device read
    if (devfs_file->device != nullptr && devfs_file->device->char_ops != nullptr && devfs_file->device->char_ops->read != nullptr) {
        return devfs_file->device->char_ops->read(f, buf, count);
    }

    return -1;
}

auto devfs_write(File* f, const void* buf, size_t count, size_t /*offset*/) -> ssize_t {
    if (f == nullptr || f->private_data == nullptr) {
        return -1;
    }

    auto* devfs_file = static_cast<DevFSFile*>(f->private_data);

    // Call device write
    if (devfs_file->device != nullptr && devfs_file->device->char_ops != nullptr && devfs_file->device->char_ops->write != nullptr) {
        return devfs_file->device->char_ops->write(f, buf, count);
    }

    return -1;
}

auto devfs_lseek(File* /*f*/, off_t /*offset*/, int /*whence*/) -> off_t {
    // Devices don't support seeking
    return -1;
}

auto devfs_isatty(File* f) -> bool {
    if (f == nullptr || f->private_data == nullptr) {
        return false;
    }

    auto* devfs_file = static_cast<DevFSFile*>(f->private_data);

    // Call device isatty if available
    if (devfs_file->device != nullptr && devfs_file->device->char_ops != nullptr && devfs_file->device->char_ops->isatty != nullptr) {
        return devfs_file->device->char_ops->isatty(f);
    }

    return false;
}

auto devfs_readdir(File* f, DirEntry* entry, size_t index) -> int {
    if (f == nullptr || entry == nullptr) {
        return -1;
    }

    // Only the /dev directory itself supports listing
    // Individual device nodes don't have entries
    if (!f->is_directory) {
        return -1;
    }

    // Get device at index
    ker::dev::Device* device = ker::dev::dev_get_at_index(index);
    if (device == nullptr) {
        return -1;  // No more entries
    }

    // Fill in directory entry
    entry->d_ino = index + 1;
    entry->d_off = index;
    entry->d_reclen = sizeof(DirEntry);

    // Set type based on device type
    if (device->type == ker::dev::DeviceType::CHAR) {
        entry->d_type = DT_CHR;
    } else {
        entry->d_type = DT_BLK;
    }

    // Copy device name
    size_t name_len = 0;
    while (device->name[name_len] != '\0' && name_len < 255) {
        entry->d_name[name_len] = device->name[name_len];
        name_len++;
    }
    entry->d_name[name_len] = '\0';

    return 0;
}

FileOperations devfs_fops = {
    .vfs_open = nullptr,  // Not used directly
    .vfs_close = devfs_close,
    .vfs_read = devfs_read,
    .vfs_write = devfs_write,
    .vfs_lseek = devfs_lseek,
    .vfs_isatty = devfs_isatty,
    .vfs_readdir = devfs_readdir,
};

}  // anonymous namespace

auto get_devfs_fops() -> FileOperations* { return &devfs_fops; }

auto devfs_open_path(const char* path, int /*flags*/, int /*mode*/) -> File* {
    if (path == nullptr) {
        return nullptr;
    }

    // Check if opening /dev itself (as a directory)
    if ((path[0] == '/' && path[1] == 'd' && path[2] == 'e' && path[3] == 'v' && path[4] == '\0') ||
        (path[0] == '/' && path[1] == 'd' && path[2] == 'e' && path[3] == 'v' && path[4] == '/' && path[5] == '\0')) {
        // Opening /dev directory itself
        auto* file = static_cast<File*>(ker::mod::mm::dyn::kmalloc::malloc(sizeof(File)));
        if (file == nullptr) {
            return nullptr;
        }

        file->fd = -1;
        file->private_data = nullptr;  // No device-specific data for directory
        file->fops = &devfs_fops;
        file->pos = 0;
        file->is_directory = true;
        file->fs_type = FSType::DEVFS;
        file->refcount = 1;

        mod::io::serial::write("devfs: opened /dev directory\n");
        return file;
    }

    // Remove /dev/ prefix if present
    const char* device_name = path;
    if (path[0] == '/' && path[1] == 'd' && path[2] == 'e' && path[3] == 'v' && path[4] == '/') {
        device_name = path + 5;  // Skip "/dev/"
    }

    // Look up the device
    ker::dev::Device* device = ker::dev::dev_find_by_name(device_name);
    if (device == nullptr) {
        mod::io::serial::write("devfs: device not found: ");
        mod::io::serial::write(device_name);
        mod::io::serial::write("\n");
        return nullptr;
    }

    // Check that it's a character device
    if (device->type != ker::dev::DeviceType::CHAR) {
        mod::io::serial::write("devfs: not a character device: ");
        mod::io::serial::write(device_name);
        mod::io::serial::write("\n");
        return nullptr;
    }

    // Call device open if available
    auto* file = static_cast<File*>(ker::mod::mm::dyn::kmalloc::malloc(sizeof(File)));
    if (file == nullptr) {
        return nullptr;
    }

    auto* devfs_file = static_cast<DevFSFile*>(ker::mod::mm::dyn::kmalloc::malloc(sizeof(DevFSFile)));
    if (devfs_file == nullptr) {
        ker::mod::mm::dyn::kmalloc::free(file);
        return nullptr;
    }

    devfs_file->device = device;

    file->fd = -1;  // Will be assigned by VFS
    file->private_data = devfs_file;
    file->fops = &devfs_fops;
    file->pos = 0;
    file->is_directory = false;  // Device nodes are not directories
    file->fs_type = FSType::DEVFS;
    file->refcount = 1;

    // Call device open
    if (device->char_ops != nullptr && device->char_ops->open != nullptr) {
        if (device->char_ops->open(file) != 0) {
            ker::mod::mm::dyn::kmalloc::free(devfs_file);
            ker::mod::mm::dyn::kmalloc::free(file);
            return nullptr;
        }
    }

    mod::io::serial::write("devfs: opened device: ");
    mod::io::serial::write(device_name);
    mod::io::serial::write("\n");

    return file;
}

void register_devfs() { mod::io::serial::write("devfs: registered\n"); }

void devfs_init() {
    mod::io::serial::write("devfs: initializing\n");
    register_devfs();
}

}  // namespace ker::vfs::devfs
