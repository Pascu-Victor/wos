#pragma once

#include <abi/service_resource.hpp>
#include <platform/sched/resource_model.hpp>

namespace ker::mod::sched::resource {
struct RemoteServiceObservation {
    uint64_t cpu_time_us = 0;
    uint64_t live_processes = 0;
    uint64_t members = 0;  // members whose execution accounting is not yet final
    uint64_t pending_remote_children = 0;
    uint64_t flags = 0;
    std::array<Usage, abi::service_resource::RESOURCE_COUNT> usage{};
};

enum class ServiceReportResult : uint8_t { INVALID, DUPLICATE, APPLIED };

// One cumulative report stream, serialized by the destination service lock.
// Current usage stays in last; only cumulative deltas enter service totals.
struct ServiceReportState {
    RemoteServiceObservation last{};
    uint64_t sequence = 0;
    bool finished = false;

    auto apply(uint64_t next_sequence, const RemoteServiceObservation& next, bool final, State& totals, bool& incomplete)
        -> ServiceReportResult {
        if (next_sequence == 0 || (next.flags & ~uint64_t{abi::service_resource::ACCOUNTING_INCOMPLETE}) != 0 ||
            next.usage[0].current != next.cpu_time_us / CPU_US_PER_SECOND ||
            (final && (next.members != 0 || next.pending_remote_children != 0 || next.live_processes != 0)))
            return ServiceReportResult::INVALID;
        for (unsigned i = 0; i < next.usage.size(); ++i) {
            auto const& usage = next.usage[i];
            if (usage.limit.soft > usage.limit.hard || usage.current > usage.peak || usage.reserved != 0 ||
                static_cast<uint32_t>(usage.reason) > static_cast<uint32_t>(Denial::HARD_LIMIT) || (final && i != 0 && usage.current != 0))
                return ServiceReportResult::INVALID;
        }
        if (next_sequence <= sequence) return ServiceReportResult::DUPLICATE;
        if (finished || next.cpu_time_us < last.cpu_time_us || (last.flags & ~next.flags) != 0) return ServiceReportResult::INVALID;
        for (unsigned i = 0; i < next.usage.size(); ++i) {
            if (next.usage[i].denied < last.usage[i].denied || next.usage[i].peak < last.usage[i].peak) return ServiceReportResult::INVALID;
        }
        static_cast<void>(totals.charge_cpu(next.cpu_time_us - last.cpu_time_us));
        for (unsigned i = 0; i < next.usage.size(); ++i) {
            auto& destination = totals.entries[static_cast<unsigned>(abi::service_resource::RESOURCE_IDS[i])];
            auto const& observed = next.usage[i];
            uint64_t const DENIED = observed.denied - last.usage[i].denied;
            destination.denied = DENIED > UINT64_MAX - destination.denied ? UINT64_MAX : destination.denied + DENIED;
            if (DENIED != 0) destination.reason = observed.reason;
            if (observed.peak > destination.peak) destination.peak = observed.peak;
        }
        incomplete |= (next.flags & abi::service_resource::ACCOUNTING_INCOMPLETE) != 0;
        last = next;
        sequence = next_sequence;
        finished = final;
        return ServiceReportResult::APPLIED;
    }
};

// Serialized by the service lock. CPU charges are recorded as they occur;
// completed process observations are folded once when membership is released.
struct ServiceState {
    State totals{};

    [[nodiscard]] auto configure(const abi::service_resource::Configuration& config) -> bool {
        State candidate{};
        for (unsigned i = 0; i < abi::service_resource::RESOURCE_COUNT; ++i) {
            if (candidate.set(static_cast<unsigned>(abi::service_resource::RESOURCE_IDS[i]), config.limits[i], true, 256) != 0) {
                return false;
            }
        }
        totals = candidate;
        return true;
    }

    [[nodiscard]] auto admit_start() -> bool {
        constexpr unsigned CPU = static_cast<unsigned>(Id::CPU);
        auto const& usage = totals.entries[CPU];
        if (totals.cpu_hard_notified || (usage.limit.hard != INFINITY_VALUE && usage.current >= usage.limit.hard)) {
            totals.deny(CPU, Denial::HARD_LIMIT);
            return false;
        }
        return true;
    }

    static void merge(Usage& destination, const Usage& observation, bool live) {
        if (live && observation.current > destination.current) destination.current = observation.current;
        if (observation.peak > destination.peak) destination.peak = observation.peak;
        destination.denied = observation.denied > UINT64_MAX - destination.denied ? UINT64_MAX : destination.denied + observation.denied;
        if (observation.reason != Denial::NONE) destination.reason = observation.reason;
    }

    void retire(const std::array<Usage, abi::resource::COUNT>& observations) {
        for (auto const id : abi::service_resource::RESOURCE_IDS) {
            if (id == Id::CPU) continue;
            auto const index = static_cast<unsigned>(id);
            merge(totals.entries[index], observations[index], false);
        }
    }
};
}  // namespace ker::mod::sched::resource
