#pragma once
#include <cstddef>
#include <defines/defines.hpp>

namespace ker::mod::boot {

constexpr size_t HANDOVER_MAX_MODULES = 32;

struct HandoverModule {
    void* entry;
    uint64_t size;
    const char* cmdline;
    const char* name;
};

struct HandoverModules {
    uint64_t count;
    HandoverModule modules[HANDOVER_MAX_MODULES];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
};

static_assert(sizeof(HandoverModule) == 32, "HandoverModule is part of the boot handover ABI");
static_assert(offsetof(HandoverModules, count) == 0, "HandoverModules::count must stay first");
static_assert(offsetof(HandoverModules, modules) == sizeof(uint64_t), "HandoverModules::modules ABI offset changed");
static_assert(sizeof(HandoverModules) == sizeof(uint64_t) + (sizeof(HandoverModule) * HANDOVER_MAX_MODULES),
              "HandoverModules boot handover ABI size changed");

}  // namespace ker::mod::boot
