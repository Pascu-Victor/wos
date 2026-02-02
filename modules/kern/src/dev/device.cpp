#include "device.hpp"

#include <mod/io/serial/serial.hpp>

namespace ker::dev {

// Device registry
namespace {
constexpr size_t MAX_DEVICES = 64;
Device* devices[MAX_DEVICES] = {};
size_t device_count = 0;
}  // namespace

auto dev_register(Device* device) -> int {
    if (device == nullptr || device->name == nullptr) {
        mod::io::serial::write("dev_register: invalid device\n");
        return -1;
    }

    if (device_count >= MAX_DEVICES) {
        mod::io::serial::write("dev_register: device table full\n");
        return -1;
    }

    devices[device_count] = device;
    device_count++;

    mod::io::serial::write("dev_register: registered ");
    mod::io::serial::write(device->name);
    mod::io::serial::write(" (");
    mod::io::serial::writeHex(device->major);
    mod::io::serial::write(",");
    mod::io::serial::writeHex(device->minor);
    mod::io::serial::write(")\n");

    return 0;
}

auto dev_find(unsigned major, unsigned minor) -> Device* {
    for (size_t i = 0; i < device_count; ++i) {
        if (devices[i] != nullptr && devices[i]->major == major && devices[i]->minor == minor) {
            return devices[i];
        }
    }
    return nullptr;
}

auto dev_find_by_name(const char* name) -> Device* {
    if (name == nullptr) {
        return nullptr;
    }

    for (size_t i = 0; i < device_count; ++i) {
        if (devices[i] != nullptr && devices[i]->name != nullptr) {
            // Manual string comparison
            const char* dev_name = devices[i]->name;
            size_t j = 0;
            while (name[j] != '\0' && dev_name[j] != '\0') {
                if (name[j] != dev_name[j]) {
                    break;
                }
                j++;
            }
            if (name[j] == '\0' && dev_name[j] == '\0') {
                return devices[i];
            }
        }
    }
    return nullptr;
}

auto dev_get_at_index(size_t index) -> Device* {
    if (index >= device_count) {
        return nullptr;
    }
    return devices[index];
}

auto dev_get_count() -> size_t {
    return device_count;
}

void dev_init() { mod::io::serial::write("dev: initializing device subsystem\n"); }

}  // namespace ker::dev
