#include "random_device.hpp"

#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <vfs/file.hpp>

namespace ker::dev::random_device {

namespace {

// Fill buffer with random bytes using RDRAND instruction.
// Returns true if all bytes were filled successfully.
bool rdrand_fill(uint8_t* buf, size_t count) {
    size_t offset = 0;

    // Fill 8 bytes at a time
    while (offset + 8 <= count) {
        uint64_t val;
        int ok;
        // Try RDRAND up to 10 times (Intel recommends retries)
        for (int attempt = 0; attempt < 10; attempt++) {
            asm volatile("rdrand %0; setc %1" : "=r"(val), "=qm"(ok));
            if (ok) break;
        }
        if (!ok) return false;
        std::memcpy(buf + offset, &val, 8);
        offset += 8;
    }

    // Fill remaining bytes
    if (offset < count) {
        uint64_t val;
        int ok;
        for (int attempt = 0; attempt < 10; attempt++) {
            asm volatile("rdrand %0; setc %1" : "=r"(val), "=qm"(ok));
            if (ok) break;
        }
        if (!ok) return false;
        std::memcpy(buf + offset, &val, count - offset);
    }

    return true;
}

// --- /dev/urandom operations ---

int urandom_open(ker::vfs::File* /*file*/) { return 0; }
int urandom_close(ker::vfs::File* /*file*/) { return 0; }

ssize_t urandom_read(ker::vfs::File* /*file*/, void* buf, size_t count) {
    if (buf == nullptr) return -1;
    if (count == 0) return 0;

    if (!rdrand_fill(static_cast<uint8_t*>(buf), count)) {
        // RDRAND failed — should not normally happen on modern CPUs
        return -1;
    }

    return static_cast<ssize_t>(count);
}

ssize_t urandom_write(ker::vfs::File* /*file*/, const void* /*buf*/, size_t count) {
    // Writing to /dev/urandom is allowed but data is discarded
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
    // Check RDRAND support via CPUID (leaf 1, ECX bit 30)
    uint32_t eax, ebx, ecx, edx;
    asm volatile("cpuid" : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx) : "a"(1));
    if (!(ecx & (1u << 30))) {
        ker::mod::io::serial::write("random_device: WARNING - RDRAND not supported, /dev/urandom unavailable\n");
        return;
    }

    ker::mod::io::serial::write("random_device: initializing /dev/urandom (RDRAND)\n");
    dev_register(&urandom_dev);
}

Device* get_urandom_device() { return &urandom_dev; }

}  // namespace ker::dev::random_device
