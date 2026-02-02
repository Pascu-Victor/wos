#include "console.hpp"

#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <vfs/file.hpp>

namespace ker::dev::console {

// Forward declarations of device operations
namespace {

// Serial console operations
int serial_open(ker::vfs::File* /*file*/) {
    // Serial is always open
    return 0;
}

int serial_close(ker::vfs::File* /*file*/) {
    // Serial is always open
    return 0;
}

ssize_t serial_read(ker::vfs::File* /*file*/, void* /*buf*/, size_t /*count*/) {
    // TODO: Implement serial input when we have interrupt-driven input
    return 0;
}

ssize_t serial_write(ker::vfs::File* /*file*/, const void* buf, size_t count) {
    if (buf == nullptr) {
        return -1;
    }
    // Use batch write - serial module's internal lock holds for entire buffer
    ker::mod::io::serial::write(static_cast<const char*>(buf), count);
    return static_cast<ssize_t>(count);
}

bool serial_isatty(ker::vfs::File* /*file*/) {
    return true;  // Serial console is a TTY
}

CharDeviceOps serial_ops = {
    .open = serial_open,
    .close = serial_close,
    .read = serial_read,
    .write = serial_write,
    .isatty = serial_isatty,
};

// VGA console operations
int vga_open(ker::vfs::File* /*file*/) { return 0; }

int vga_close(ker::vfs::File* /*file*/) { return 0; }

ssize_t vga_read(ker::vfs::File* /*file*/, void* /*buf*/, size_t /*count*/) {
    // TODO: Implement keyboard input when we have PS/2 driver
    return 0;
}

ssize_t vga_write(ker::vfs::File* /*file*/, const void* buf, size_t count) {
    if (buf == nullptr) {
        return -1;
    }
    // TODO: Implement VGA output when we have VGA text mode driver
    // For now, also write to serial
    // Use batch write - serial module's internal lock holds for entire buffer
    ker::mod::io::serial::write(static_cast<const char*>(buf), count);
    return static_cast<ssize_t>(count);
}

bool vga_isatty(ker::vfs::File* /*file*/) {
    return true;  // VGA console is a TTY
}

CharDeviceOps vga_ops = {
    .open = vga_open,
    .close = vga_close,
    .read = vga_read,
    .write = vga_write,
    .isatty = vga_isatty,
};

// Device instances
Device serial_device = {
    .major = 4,   // Standard TTY major number
    .minor = 64,  // ttyS0 (first serial port)
    .name = "ttyS0",
    .type = DeviceType::CHAR,
    .private_data = nullptr,
    .char_ops = &serial_ops,
};

Device vga_device = {
    .major = 4,  // Standard TTY major number
    .minor = 0,  // tty0 (first VGA console)
    .name = "tty0",
    .type = DeviceType::CHAR,
    .private_data = nullptr,
    .char_ops = &vga_ops,
};

Device console_device = {
    .major = 5,  // /dev/console
    .minor = 1,
    .name = "console",
    .type = DeviceType::CHAR,
    .private_data = nullptr,
    .char_ops = &serial_ops,  // Point to serial for now
};

}  // anonymous namespace

void console_init() {
    ker::mod::io::serial::write("console: initializing console devices\n");

    // Register serial console
    dev_register(&serial_device);

    // Register VGA console
    dev_register(&vga_device);

    // Register main console (points to serial)
    dev_register(&console_device);
}

Device* get_serial_console() { return &serial_device; }

Device* get_vga_console() { return &vga_device; }

Device* get_console() { return &console_device; }

}  // namespace ker::dev::console
