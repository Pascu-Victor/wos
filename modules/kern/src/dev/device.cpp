#include "device.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/dbg/journal.hpp>
#include <platform/perf/perf_events.hpp>
#include <util/smallvec.hpp>

#include "platform/dbg/dbg.hpp"

namespace ker::dev {

using log = ker::mod::dbg::logger<"dev">;

// Device registry
namespace {
ker::util::SmallVec<Device*, 16> devices;
}  // namespace

auto dev_register(Device* device) -> int {
    if (device == nullptr || device->name == nullptr) {
        log::warn("dev_register: invalid device");
        return -1;
    }

    if (!devices.push_back(device)) {
        log::warn("dev_register: device table full (OOM)");
        return -1;
    }

#ifdef DEV_DEBUG
    log::debug("dev_register: registered %s (%x,%x)", device->name, device->major, device->minor);
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
        if (devices.at(i) == device) {
            devices.remove_at(i);
            mod::perf::record_container_stat(0, 0, mod::perf::PerfSubsystem::DEVICE_REG, 0, mod::perf::PERF_FLAG_CT_REMOVE,
                                             static_cast<int64_t>(devices.size()), 0, 0);
            return 0;
        }
    }
    return -1;
}

auto dev_find(unsigned major, unsigned minor) -> Device* {
    for (auto* device : devices) {
        if (device != nullptr && device->major == major && device->minor == minor) {
            return device;
        }
    }
    return nullptr;
}

auto dev_find_by_name(const char* name) -> Device* {
    if (name == nullptr) {
        return nullptr;
    }

    for (auto* device : devices) {
        if (device != nullptr && device->name != nullptr && std::strcmp(device->name, name) == 0) {
            return device;
        }
    }
    return nullptr;
}

auto dev_get_at_index(size_t index) -> Device* {
    if (index >= devices.size()) {
        return nullptr;
    }
    return devices.at(index);
}

auto dev_get_count() -> size_t { return devices.size(); }

auto dev_init() -> void {
    ker::mod::dbg::journal::register_devices();
    log::info("device subsystem initialized");
}

}  // namespace ker::dev
