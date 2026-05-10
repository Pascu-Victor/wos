#include "device.hpp"

#include <cstddef>
#include <cstdint>
#include <mod/io/serial/serial.hpp>
#include <platform/dbg/journal.hpp>
#include <platform/perf/perf_events.hpp>
#include <util/smallvec.hpp>

#include "platform/dbg/dbg.hpp"

namespace ker::dev {

// Device registry
namespace {
ker::util::SmallVec<Device*, 16> devices;
}  // namespace

auto dev_register(Device* device) -> int {
    if (device == nullptr || device->name == nullptr) {
        mod::io::serial::write("dev_register: invalid device\n");
        return -1;
    }

    if (!devices.push_back(device)) {
        mod::io::serial::write("dev_register: device table full (OOM)\n");
        return -1;
    }

#ifdef DEV_DEBUG
    mod::io::serial::write("dev_register: registered ");
    mod::io::serial::write(device->name);
    mod::io::serial::write(" (");
    mod::io::serial::writeHex(device->major);
    mod::io::serial::write(",");
    mod::io::serial::writeHex(device->minor);
    mod::io::serial::write(")\n");
#endif
    mod::perf::record_container_stat(0, 0, mod::perf::PerfSubsystem::DEVICE_REG, 0, mod::perf::PERF_FLAG_CT_INSERT,
                                     static_cast<int64_t>(devices.size()), 0, 0);
    return 0;
}

auto dev_unregister(Device* device) -> int {
    if (device == nullptr) {
        return -1;
    }
    for (size_t i = 0; i < devices.size(); ++i) {
        if (devices[i] == device) {
            devices.remove_at(i);
            mod::perf::record_container_stat(0, 0, mod::perf::PerfSubsystem::DEVICE_REG, 0, mod::perf::PERF_FLAG_CT_REMOVE,
                                             static_cast<int64_t>(devices.size()), 0, 0);
            return 0;
        }
    }
    return -1;
}

auto dev_find(unsigned major, unsigned minor) -> Device* {
    for (size_t i = 0; i < devices.size(); ++i) {
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

    for (size_t i = 0; i < devices.size(); ++i) {
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
    if (index >= devices.size()) {
        return nullptr;
    }
    return devices[index];
}

auto dev_get_count() -> size_t { return devices.size(); }

void dev_init() {
    ker::mod::dbg::journal::register_devices();
    ker::mod::dbg::logger<"dev">::info("device subsystem initialized");
}

}  // namespace ker::dev
