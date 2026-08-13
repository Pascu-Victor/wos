#include "random_device.hpp"

#include <bits/ssize_t.h>

#include <cstddef>
#include <vfs/file.hpp>

#include "dev/device.hpp"
#include "platform/dbg/dbg.hpp"
#include "platform/random/entropy.hpp"

namespace ker::dev::random_device {

namespace {

// --- /dev/urandom operations ---

int urandom_open(ker::vfs::File* /*file*/) { return 0; }
int urandom_close(ker::vfs::File* /*file*/) { return 0; }

ssize_t urandom_read(ker::vfs::File* /*file*/, void* buf, size_t count) {
    if (buf == nullptr) {
        return -1;
    }
    if (count == 0) {
        return 0;
    }

    if (!ker::mod::random::entropy::get_bytes(buf, count)) {
        return -1;
    }

    return static_cast<ssize_t>(count);
}

ssize_t urandom_write(ker::vfs::File* /*file*/, const void* buf, size_t count) {
    if (count == 0) {
        return 0;
    }
    if (buf == nullptr || !ker::mod::random::entropy::mix_bytes(buf, count)) {
        return -1;
    }
    return static_cast<ssize_t>(count);
}

bool urandom_isatty(ker::vfs::File* /*file*/) { return false; }

CharDeviceOps urandom_ops = {
    .open = urandom_open,
    .close = urandom_close,
    .read = urandom_read,
    .write = urandom_write,
    .isatty = urandom_isatty,
    .ioctl = nullptr,
    .poll_check = nullptr,
    .poll_register_waiter = nullptr,
};

Device urandom_dev = {
    .major = 1,
    .minor = 9,
    .name = "urandom",
    .type = DeviceType::CHAR,
    .private_data = nullptr,
    .char_ops = &urandom_ops,
};

}  // anonymous namespace

void random_device_init() {
    if (!ker::mod::random::entropy::initialize()) {
        ker::mod::dbg::logger<"random_device">::warn("Hardware entropy unavailable; kernel DRBG and /dev/urandom disabled");
        return;
    }

    ker::mod::dbg::logger<"random_device">::info("Kernel DRBG ready; initializing /dev/urandom");
    dev_register(&urandom_dev);
}

auto get_urandom_device() -> Device* { return &urandom_dev; }

}  // namespace ker::dev::random_device
