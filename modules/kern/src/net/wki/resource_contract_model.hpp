#pragma once

#include <cstring>
#include <net/wki/resource_contract.hpp>
#include <platform/sched/resource_model.hpp>

namespace ker::net::wki {
namespace resource_contract {
namespace resource = mod::sched::resource;

inline auto snapshot(const resource::State& state) -> TaskResourceSnapshot {
    TaskResourceSnapshot result{};
    result.flags = state.cpu_hard_notified ? WKI_RESOURCE_CPU_HARD_NOTIFIED : 0;
    result.cpu_time_us = state.cpu_time_us;
    result.cpu_user_time_us = state.cpu_user_time_us;
    result.cpu_system_time_us = state.cpu_system_time_us;
    result.cpu_last_soft_second = state.cpu_last_soft_second;
    result.entries = state.entries;
    return result;
}

inline auto valid(const TaskResourceSnapshot& record, uint64_t max_fds) -> bool {
    if (record.version != WKI_RESOURCE_VERSION || record.count != abi::resource::COUNT || (record.flags & ~WKI_RESOURCE_KNOWN_FLAGS) != 0 ||
        record.reserved != 0 || resource::saturating_cpu_add(record.cpu_user_time_us, record.cpu_system_time_us) != record.cpu_time_us)
        return false;
    for (unsigned id = 0; id < record.entries.size(); ++id) {
        const auto& entry = record.entries[id];
        if (entry.limit.soft > entry.limit.hard || entry.current > entry.peak || entry.reserved != 0 ||
            static_cast<uint32_t>(entry.reason) > static_cast<uint32_t>(abi::resource::Denial::HARD_LIMIT))
            return false;
        if (id == static_cast<unsigned>(abi::resource::Id::NOFILE) && entry.limit.hard > max_fds) return false;
    }
    uint64_t const SECONDS = record.cpu_time_us / resource::CPU_US_PER_SECOND;
    return record.entries[static_cast<unsigned>(abi::resource::Id::CPU)].current == SECONDS &&
           (record.cpu_last_soft_second == UINT64_MAX || record.cpu_last_soft_second <= SECONDS);
}

// Length validation and memcpy precede any typed access. In particular the
// variable argv/output prefix can leave the suffix at an unaligned address.
inline auto decode(const void* bytes, size_t length, TaskResourceSnapshot& destination, uint64_t max_fds) -> bool {
    if (bytes == nullptr || length != sizeof(TaskResourceSnapshot)) return false;
    TaskResourceSnapshot candidate;
    std::memcpy(&candidate, bytes, sizeof(candidate));
    if (!valid(candidate, max_fds)) return false;
    destination = candidate;
    return true;
}

inline auto cpu_exhausted(const TaskResourceSnapshot& record) -> bool {
    auto const LIMIT = record.entries[static_cast<unsigned>(abi::resource::Id::CPU)].limit;
    return LIMIT.hard != resource::INFINITY_VALUE && record.cpu_time_us / resource::CPU_US_PER_SECOND >= LIMIT.hard;
}

inline auto valid_submission(const TaskResourceSnapshot& record, uint64_t max_fds) -> bool {
    return valid(record, max_fds) && ((record.flags & WKI_RESOURCE_CHILD_OBSERVATIONS) != 0 ||
                                      (record.reaped_user_time_us == 0 && record.reaped_system_time_us == 0));
}

// Destination is an unpublished, empty receiver process. Physical resources
// are reconstructed under the imported limits; previous peaks and denials
// remain lifetime observations, including when limits were lowered below use.
inline auto restore(const TaskResourceSnapshot& record, resource::State& destination, uint64_t max_fds,
                    TaskChildCpuObservation* observations = nullptr) -> bool {
    bool const IMPORT = (record.flags & WKI_RESOURCE_CHILD_OBSERVATIONS) != 0;
    if (!valid_submission(record, max_fds) || (IMPORT && observations == nullptr)) return false;
    resource::State candidate(max_fds);
    candidate.entries = record.entries;
    candidate.cpu_time_us = record.cpu_time_us;
    candidate.cpu_user_time_us = record.cpu_user_time_us;
    candidate.cpu_system_time_us = record.cpu_system_time_us;
    candidate.cpu_last_soft_second = record.cpu_last_soft_second;
    candidate.cpu_hard_notified = (record.flags & WKI_RESOURCE_CPU_HARD_NOTIFIED) != 0;
    for (unsigned id = 0; id < candidate.entries.size(); ++id) {
        if (id != static_cast<unsigned>(abi::resource::Id::CPU)) candidate.entries[id].current = 0;
    }
    destination = candidate;
    if (observations != nullptr) *observations = {record.reaped_user_time_us, record.reaped_system_time_us};
    return true;
}

// Compare against the immutable snapshot sent with this submission. A reply
// from another execution or a reset counter cannot be used as a delta.
inline auto valid_completion(const TaskResourceSnapshot& baseline, const TaskResourceSnapshot& completion, uint64_t max_fds) -> bool {
    if (!valid_submission(baseline, max_fds) || !valid(completion, max_fds) || (completion.flags & WKI_RESOURCE_CHILD_OBSERVATIONS) != 0 ||
        completion.cpu_time_us < baseline.cpu_time_us || completion.cpu_user_time_us < baseline.cpu_user_time_us ||
        completion.cpu_system_time_us < baseline.cpu_system_time_us)
        return false;
    if ((baseline.flags & WKI_RESOURCE_CPU_HARD_NOTIFIED) != 0 && (completion.flags & WKI_RESOURCE_CPU_HARD_NOTIFIED) == 0) return false;
    if (baseline.cpu_last_soft_second != UINT64_MAX &&
        (completion.cpu_last_soft_second == UINT64_MAX || completion.cpu_last_soft_second < baseline.cpu_last_soft_second))
        return false;
    for (unsigned id = 0; id < baseline.entries.size(); ++id) {
        if (completion.entries[id].peak < baseline.entries[id].peak || completion.entries[id].denied < baseline.entries[id].denied)
            return false;
    }
    return true;
}

// The submit/complete owner calls this exactly once after winning proxy exit.
// Add only remote work to the home state: the home process may have consumed
// CPU or recorded a denial while waiting for acceptance. Local physical
// resources still belong to the proxy's ordinary teardown, so current values
// are not replaced by the receiver's address space/FD/UID observations.
inline auto merge_completion(const TaskResourceSnapshot& baseline, const TaskResourceSnapshot& completion, resource::State& home,
                             uint64_t max_fds, uint64_t& remote_cpu_us) -> bool {
    if (!valid_completion(baseline, completion, max_fds)) return false;
    uint64_t const DELTA = completion.cpu_time_us - baseline.cpu_time_us;
    bool const OVERFLOW = DELTA > UINT64_MAX - home.cpu_time_us;
    home.cpu_time_us = OVERFLOW ? UINT64_MAX : home.cpu_time_us + DELTA;
    home.cpu_user_time_us = resource::saturating_cpu_add(home.cpu_user_time_us, completion.cpu_user_time_us - baseline.cpu_user_time_us);
    home.cpu_system_time_us =
        resource::saturating_cpu_add(home.cpu_system_time_us, completion.cpu_system_time_us - baseline.cpu_system_time_us);
    for (unsigned id = 0; id < home.entries.size(); ++id) {
        auto& entry = home.entries[id];
        auto const& remote = completion.entries[id];
        uint64_t const DENIED = remote.denied - baseline.entries[id].denied;
        entry.limit = remote.limit;
        if (remote.peak > entry.peak) entry.peak = remote.peak;
        entry.denied = DENIED > UINT64_MAX - entry.denied ? UINT64_MAX : entry.denied + DENIED;
        if (DENIED != 0) entry.reason = remote.reason;
    }
    constexpr unsigned CPU = static_cast<unsigned>(abi::resource::Id::CPU);
    home.commit_extent(CPU, home.cpu_time_us / resource::CPU_US_PER_SECOND);
    if (completion.cpu_last_soft_second != UINT64_MAX &&
        (home.cpu_last_soft_second == UINT64_MAX || completion.cpu_last_soft_second > home.cpu_last_soft_second))
        home.cpu_last_soft_second = completion.cpu_last_soft_second;
    home.cpu_hard_notified |= (completion.flags & WKI_RESOURCE_CPU_HARD_NOTIFIED) != 0;
    if (OVERFLOW) {
        if (!home.cpu_hard_notified) home.deny(CPU, abi::resource::Denial::OVERFLOW);
        home.cpu_hard_notified = true;
    }
    remote_cpu_us = DELTA;
    return true;
}

// An unpublished receiver may acquire resources and encounter a denial, but
// cannot execute user code, change policy or add process/child CPU. Rejection
// reports observations only; all acquired physical resources are rolled back.
inline auto valid_rejection(const TaskResourceSnapshot& baseline, const TaskResourceSnapshot& rejection, uint64_t max_fds) -> bool {
    if (!valid_completion(baseline, rejection, max_fds) || rejection.cpu_time_us != baseline.cpu_time_us ||
        rejection.cpu_user_time_us != baseline.cpu_user_time_us || rejection.cpu_system_time_us != baseline.cpu_system_time_us ||
        rejection.cpu_last_soft_second != baseline.cpu_last_soft_second || rejection.reaped_user_time_us != 0 ||
        rejection.reaped_system_time_us != 0 ||
        (rejection.flags & WKI_RESOURCE_CPU_HARD_NOTIFIED) != (baseline.flags & WKI_RESOURCE_CPU_HARD_NOTIFIED))
        return false;
    for (unsigned id = 0; id < baseline.entries.size(); ++id) {
        if (rejection.entries[id].limit.soft != baseline.entries[id].limit.soft ||
            rejection.entries[id].limit.hard != baseline.entries[id].limit.hard)
            return false;
    }
    return true;
}

// Caller owns the one successful response-waiter claim for this submission.
inline auto merge_rejection(const TaskResourceSnapshot& baseline, const TaskResourceSnapshot& rejection, resource::State& home,
                            uint64_t max_fds) -> bool {
    if (!valid_rejection(baseline, rejection, max_fds)) return false;
    for (unsigned id = 0; id < home.entries.size(); ++id) {
        auto& entry = home.entries[id];
        auto const& remote = rejection.entries[id];
        if (remote.peak > entry.peak) entry.peak = remote.peak;
        uint64_t const DENIED = remote.denied - baseline.entries[id].denied;
        entry.denied = DENIED > UINT64_MAX - entry.denied ? UINT64_MAX : entry.denied + DENIED;
        if (DENIED != 0) entry.reason = remote.reason;
    }
    return true;
}
}  // namespace resource_contract
}  // namespace ker::net::wki
