#pragma once

#include <sys/types.h>

#include <cstddef>
#include <cstdint>

namespace ker::vfs {
struct File;  // Forward declaration from VFS
}

namespace ker::dev {

// Character device operations
struct CharDeviceOps {
    int (*open)(ker::vfs::File* file);
    int (*close)(ker::vfs::File* file);
    ssize_t (*read)(ker::vfs::File* file, void* buf, size_t count);
    ssize_t (*write)(ker::vfs::File* file, const void* buf, size_t count);
    bool (*isatty)(ker::vfs::File* file);
};

enum class DeviceType : uint8_t {
    CHAR,   // Character device (console, tty, etc)
    BLOCK,  // Block device (disk, partition, etc)
};

struct Device {
    unsigned major;
    unsigned minor;
    const char* name;
    DeviceType type;
    void* private_data;
    CharDeviceOps* char_ops;  // For character devices
};

// Device registration and management
int dev_register(Device* device);
int dev_unregister(Device* device);
Device* dev_find(unsigned major, unsigned minor);
Device* dev_find_by_name(const char* name);
Device* dev_get_at_index(size_t index);
size_t dev_get_count();

// Initialize device subsystem
void dev_init();

}  // namespace ker::dev
