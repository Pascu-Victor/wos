#include "null_device.hpp"

#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <vfs/file.hpp>

namespace ker::dev::null_device {

namespace {

// --- /dev/null operations ---

int null_open(ker::vfs::File* /*file*/) { return 0; }
int null_close(ker::vfs::File* /*file*/) { return 0; }

ssize_t null_read(ker::vfs::File* /*file*/, void* /*buf*/, size_t /*count*/) {
    return 0;  // EOF — always returns 0 bytes
}

ssize_t null_write(ker::vfs::File* /*file*/, const void* /*buf*/, size_t count) {
    return static_cast<ssize_t>(count);  // Discard all data
}

bool null_isatty(ker::vfs::File* /*file*/) { return false; }

CharDeviceOps null_ops = {
    .open = null_open,
    .close = null_close,
    .read = null_read,
    .write = null_write,
    .isatty = null_isatty,
    .ioctl = nullptr,
    .poll_check = nullptr,
};

// --- /dev/zero operations ---

int zero_open(ker::vfs::File* /*file*/) { return 0; }
int zero_close(ker::vfs::File* /*file*/) { return 0; }

ssize_t zero_read(ker::vfs::File* /*file*/, void* buf, size_t count) {
    if (buf == nullptr) return -1;
    std::memset(buf, 0, count);
    return static_cast<ssize_t>(count);
}

ssize_t zero_write(ker::vfs::File* /*file*/, const void* /*buf*/, size_t count) {
    return static_cast<ssize_t>(count);  // Discard all data
}

bool zero_isatty(ker::vfs::File* /*file*/) { return false; }

CharDeviceOps zero_ops = {
    .open = zero_open,
    .close = zero_close,
    .read = zero_read,
    .write = zero_write,
    .isatty = zero_isatty,
    .ioctl = nullptr,
    .poll_check = nullptr,
};

// Device instances
Device null_dev = {
    .major = 1,
    .minor = 3,
    .name = "null",
    .type = DeviceType::CHAR,
    .private_data = nullptr,
    .char_ops = &null_ops,
};

Device zero_dev = {
    .major = 1,
    .minor = 5,
    .name = "zero",
    .type = DeviceType::CHAR,
    .private_data = nullptr,
    .char_ops = &zero_ops,
};

}  // anonymous namespace

void null_device_init() {
    ker::mod::io::serial::write("null_device: initializing /dev/null and /dev/zero\n");
    dev_register(&null_dev);
    dev_register(&zero_dev);
}

Device* get_null_device() { return &null_dev; }
Device* get_zero_device() { return &zero_dev; }

}  // namespace ker::dev::null_device
