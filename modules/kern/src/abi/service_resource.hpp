#pragma once

#include "resource.hpp"

namespace ker::abi::service_resource {
inline constexpr uint32_t VERSION = 1;
inline constexpr uint32_t MAX_SERVICES = 16;
inline constexpr uint32_t NAME_CAPACITY = 64;
inline constexpr uint32_t RESOURCE_COUNT = 7;
inline constexpr uint64_t NO_SELECTION = UINT64_MAX;
inline constexpr uint32_t ACCOUNTING_INCOMPLETE = 1;
inline constexpr resource::Id RESOURCE_IDS[RESOURCE_COUNT] = {resource::Id::CPU,  resource::Id::FSIZE, resource::Id::STACK,
                                                              resource::Id::CORE, resource::Id::NPROC, resource::Id::NOFILE,
                                                              resource::Id::AS};

// PID 1 configures each slot once. Reconfiguration never resets accounting.
struct Configuration {
    uint32_t size;
    uint32_t version;
    uint32_t slot;
    uint32_t flags;
    char name[NAME_CAPACITY];
    resource::Limit limits[RESOURCE_COUNT];
    uint64_t reserved[2];
};

// CPU is cumulative across every affiliated process and service generation.
// Other rows describe per-process bounds: maximum current, maximum lifetime
// peak, and summed denial counters. Names/limits remain the configured values.
struct Snapshot {
    uint32_t size;
    uint32_t version;
    uint32_t slot;
    uint32_t flags;
    char name[NAME_CAPACITY];
    uint64_t cpu_time_us;
    uint64_t live_processes;
    uint64_t reserved[2];
    resource::Usage usage[RESOURCE_COUNT];
};
static_assert(sizeof(Configuration) == 208);
static_assert(sizeof(Snapshot) == 448);
}  // namespace ker::abi::service_resource
