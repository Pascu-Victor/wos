#pragma once

#include <sys/types.h>

#include <cstddef>
#include <cstdint>

namespace ker::vfs {
struct File;  // Forward declaration from VFS
}

namespace ker::mod::sched::task {
enum class WaitChannelKind : uint8_t;
}  // namespace ker::mod::sched::task

namespace ker::dev {

// Character device operations
struct CharDeviceOps {
    int (*open)(ker::vfs::File* file) = nullptr;
    int (*close)(ker::vfs::File* file) = nullptr;
    ssize_t (*read)(ker::vfs::File* file, void* buf, size_t count) = nullptr;
    ssize_t (*write)(ker::vfs::File* file, const void* buf, size_t count) = nullptr;
    bool (*isatty)(ker::vfs::File* file) = nullptr;
    int (*ioctl)(ker::vfs::File* file, unsigned long cmd, unsigned long arg) = nullptr;   // nullptr = not supported
    int (*poll_check)(ker::vfs::File* file, int events) = nullptr;                        // nullptr = always ready
    bool (*poll_register_waiter)(ker::vfs::File* file, uint64_t pid) = nullptr;           // nullptr = no explicit wake registration
    mod::sched::task::WaitChannelKind (*poll_wait_kind)(ker::vfs::File* file) = nullptr;  // nullptr = generic poll wait
    off_t (*lseek)(ker::vfs::File* file, off_t offset, int whence) = nullptr;             // nullptr = not seekable
};

enum class DeviceType : uint8_t {
    CHAR,   // Character device (console, tty, etc)
    BLOCK,  // Block device (disk, partition, etc)
};

struct Device {
    unsigned major = 0;
    unsigned minor = 0;
    const char* name = nullptr;
    DeviceType type = DeviceType::CHAR;
    void* private_data = nullptr;
    CharDeviceOps* char_ops = nullptr;  // For character devices
};

// Device registration and management
auto dev_register(Device* device) -> int;
auto dev_unregister(Device* device) -> int;
auto dev_find(unsigned major, unsigned minor) -> Device*;
auto dev_find_by_name(const char* name) -> Device*;
auto dev_get_at_index(size_t index) -> Device*;
auto dev_get_count() -> size_t;

// Initialize device subsystem
auto dev_init() -> void;

}  // namespace ker::dev
