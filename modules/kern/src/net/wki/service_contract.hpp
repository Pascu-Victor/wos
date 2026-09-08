#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <platform/sched/service_resource_model.hpp>

namespace ker::net::wki {
inline constexpr uint16_t WKI_SERVICE_REPORT_VERSION = 1;
inline constexpr uint16_t WKI_SERVICE_REPORT_FINAL = 1;
inline constexpr uint16_t WKI_SERVICE_REPORT_INCOMPLETE = 2;

struct TaskServiceReport {
    uint32_t task_id = 0;
    uint16_t version = WKI_SERVICE_REPORT_VERSION;
    uint16_t flags = 0;
    uint64_t sequence = 0;
    uint64_t cpu_time_us = 0;
    uint64_t live_processes = 0;
    uint64_t members = 0;
    uint64_t pending_remote_children = 0;
    std::array<abi::resource::Usage, abi::service_resource::RESOURCE_COUNT> usage{};
} __attribute__((packed, aligned(8)));
static_assert(sizeof(TaskServiceReport) == 384);
static_assert(offsetof(TaskServiceReport, usage) == 48);

namespace service_contract {
inline auto encode(uint32_t task_id, uint64_t sequence, const mod::sched::resource::RemoteServiceObservation& observation, bool final)
    -> TaskServiceReport {
    TaskServiceReport report{};
    report.task_id = task_id;
    report.sequence = sequence;
    report.flags = (final ? WKI_SERVICE_REPORT_FINAL : 0) |
                   ((observation.flags & abi::service_resource::ACCOUNTING_INCOMPLETE) != 0 ? WKI_SERVICE_REPORT_INCOMPLETE : 0);
    report.cpu_time_us = observation.cpu_time_us;
    report.live_processes = observation.live_processes;
    report.members = observation.members;
    report.pending_remote_children = observation.pending_remote_children;
    report.usage = observation.usage;
    return report;
}

// Packet suffixes may be unaligned. Counter/finality validation is performed
// atomically against the matched stream by ServiceReportTarget::apply.
inline auto decode(const uint8_t* bytes, size_t length, TaskServiceReport& report,
                   mod::sched::resource::RemoteServiceObservation& observation) -> bool {
    if (bytes == nullptr || length != sizeof(report)) return false;
    std::memcpy(&report, bytes, sizeof(report));
    if (report.version != WKI_SERVICE_REPORT_VERSION || report.task_id == 0 || report.sequence == 0 ||
        (report.flags & ~(WKI_SERVICE_REPORT_FINAL | WKI_SERVICE_REPORT_INCOMPLETE)) != 0)
        return false;
    observation.cpu_time_us = report.cpu_time_us;
    observation.live_processes = report.live_processes;
    observation.members = report.members;
    observation.pending_remote_children = report.pending_remote_children;
    observation.flags = (report.flags & WKI_SERVICE_REPORT_INCOMPLETE) != 0 ? abi::service_resource::ACCOUNTING_INCOMPLETE : 0;
    observation.usage = report.usage;
    return true;
}
}  // namespace service_contract
}  // namespace ker::net::wki
