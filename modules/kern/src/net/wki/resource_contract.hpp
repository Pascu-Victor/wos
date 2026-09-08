#pragma once

#include <abi/resource.hpp>
#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::net::wki {
inline constexpr uint32_t WKI_RESOURCE_VERSION = 2;
inline constexpr uint16_t WKI_RESOURCE_CPU_HARD_NOTIFIED = 1;
inline constexpr unsigned WKI_RESOURCE_STDIO_SHIFT = 1;
inline constexpr uint8_t WKI_RESOURCE_STDIO_ALL = 7;
inline constexpr uint16_t WKI_RESOURCE_STDIO_FLAGS = WKI_RESOURCE_STDIO_ALL << WKI_RESOURCE_STDIO_SHIFT;
// Submission-only: independent service reports account for remote subtree work.
inline constexpr uint16_t WKI_RESOURCE_SERVICE_REPORTS = 1U << 4;
inline constexpr uint16_t WKI_RESOURCE_KNOWN_FLAGS =
    WKI_RESOURCE_CPU_HARD_NOTIFIED | WKI_RESOURCE_STDIO_FLAGS | WKI_RESOURCE_SERVICE_REPORTS;

// Capability-gated suffix. This preserves exec's lifetime observations as
// well as policy: migrating again must not reset an exhausted CPU budget.
struct TaskResourceSnapshot {
    uint32_t version = WKI_RESOURCE_VERSION;
    uint16_t count = abi::resource::COUNT;
    uint16_t flags = 0;
    uint64_t cpu_time_us = 0;
    uint64_t cpu_last_soft_second = UINT64_MAX;
    std::array<abi::resource::Usage, abi::resource::COUNT> entries{};
    uint64_t cpu_user_time_us = 0;
    uint64_t cpu_system_time_us = 0;
    // Completion-only deltas: children reaped during this remote execution
    // segment. Submission records must keep these zero.
    uint64_t reaped_user_time_us = 0;
    uint64_t reaped_system_time_us = 0;
    uint64_t reserved = 0;
} __attribute__((packed, aligned(8)));

static_assert(sizeof(TaskResourceSnapshot) == 832);
// Copy from packet bytes before accessing this record; the wire suffix need
// not be aligned, while Usage and std::array require natural alignment.
static_assert(alignof(TaskResourceSnapshot) == 8);
static_assert(offsetof(TaskResourceSnapshot, entries) == 24);
static_assert(offsetof(TaskResourceSnapshot, cpu_user_time_us) == 792);
static_assert(offsetof(TaskResourceSnapshot, reserved) == 824);
}  // namespace ker::net::wki
