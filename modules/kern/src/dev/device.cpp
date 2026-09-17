#include "device.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <platform/dbg/journal.hpp>
#include <platform/perf/perf_events.hpp>
#include <platform/sys/spinlock.hpp>

#include "platform/dbg/dbg.hpp"

namespace ker::dev {

using log = ker::mod::dbg::logger<"dev">;

// Device registry
namespace {
struct RegistryEntry {
    Device* device;
    RegistryEntry* next = nullptr;
};

ker::mod::sys::Spinlock registry_lock;
RegistryEntry* registry_head = nullptr;
RegistryEntry* registry_tail = nullptr;
size_t registry_count = 0;

class RegistryGuard {
   public:
    RegistryGuard() : flags_(registry_lock.lock_irqsave()) {}
    ~RegistryGuard() { registry_lock.unlock_irqrestore(flags_); }
    RegistryGuard(const RegistryGuard&) = delete;
    auto operator=(const RegistryGuard&) -> RegistryGuard& = delete;

   private:
    uint64_t flags_;
};
}  // namespace

auto dev_register(Device* device) -> int {
    if (device == nullptr || device->name == nullptr) {
        log::warn("dev_register: invalid device");
        return -1;
    }

    // Allocation can reenter logging. Prepare the entry before the bounded
    // registry transaction, and never grow/free storage under its IRQ guard.
    auto* entry = new (std::nothrow) RegistryEntry{.device = device};
    if (entry == nullptr) {
        log::warn("dev_register: device table full (OOM)");
        return -1;
    }

    size_t count = 0;
    {
        RegistryGuard const GUARD;
        if (registry_tail != nullptr) {
            registry_tail->next = entry;
        } else {
            registry_head = entry;
        }
        registry_tail = entry;
        count = ++registry_count;
    }

#ifdef DEV_DEBUG
    log::debug("dev_register: registered %s (%x,%x)", device->name, device->major, device->minor);
#endif
    mod::perf::record_container_stat(0, 0, mod::perf::PerfSubsystem::DEVICE_REG, 0, mod::perf::PERF_FLAG_CT_INSERT,
                                     static_cast<int64_t>(count), 0, 0);
    return 0;
}

auto dev_unregister(Device* device) -> int {
    if (device == nullptr) {
        return -1;
    }
    RegistryEntry* retired = nullptr;
    size_t count = 0;
    {
        RegistryGuard const GUARD;
        RegistryEntry* previous = nullptr;
        for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
            if (entry->device == device) {
                if (previous != nullptr) {
                    previous->next = entry->next;
                } else {
                    registry_head = entry->next;
                }
                if (registry_tail == entry) {
                    registry_tail = previous;
                }
                retired = entry;
                count = --registry_count;
                break;
            }
            previous = entry;
        }
    }
    if (retired == nullptr) {
        return -1;
    }
    delete retired;
    mod::perf::record_container_stat(0, 0, mod::perf::PerfSubsystem::DEVICE_REG, 0, mod::perf::PERF_FLAG_CT_REMOVE,
                                     static_cast<int64_t>(count), 0, 0);
    return 0;
}

auto dev_find(unsigned major, unsigned minor) -> Device* {
    RegistryGuard const GUARD;
    for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
        auto* device = entry->device;
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

    RegistryGuard const GUARD;
    for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
        auto* device = entry->device;
        if (device != nullptr && device->name != nullptr && std::strcmp(device->name, name) == 0) {
            return device;
        }
    }
    return nullptr;
}

auto dev_get_at_index(size_t index) -> Device* {
    RegistryGuard const GUARD;
    for (auto* entry = registry_head; entry != nullptr; entry = entry->next) {
        if (index == 0) {
            return entry->device;
        }
        --index;
    }
    return nullptr;
}

auto dev_get_count() -> size_t {
    RegistryGuard const GUARD;
    return registry_count;
}

auto dev_init() -> void {
    ker::mod::dbg::journal::register_devices();
    log::info("device subsystem initialized");
}

}  // namespace ker::dev
