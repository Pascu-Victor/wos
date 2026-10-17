#include "capture.hpp"

#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <optional>
#include <print>

#include "perf.hpp"

namespace perf {
namespace {
auto read_capture_status() -> std::optional<CaptureStatus> {
    auto text = read_file(KPERFSTATUS_PATH, PROC_READ_CAPACITY);
    return text ? parse_capture_status(*text) : std::nullopt;
}

auto capture_status_matches(const std::optional<CaptureStatus>& start, const std::optional<CaptureStatus>& status, bool enabled) -> bool {
    return start && start->enabled && start->cpus != 0 && start->control_version != UINT64_MAX && status &&
           status->session == start->session && status->control_version == start->control_version + (enabled ? 0U : 1U) &&
           status->mask == start->mask && status->cpus == start->cpus && status->enabled == enabled;
}
}  // namespace

auto begin_capture(const char* filter) -> CaptureReceipt {
    CaptureReceipt receipt;
    receipt.requested_filter = filter != nullptr ? filter : "all";
    receipt.control_failed = !set_recording_enabled(true, filter);
    receipt.started = read_capture_status();
    return receipt;
}

auto capture_record_clock() -> std::optional<int64_t> {
    timespec stamp{};
    if (clock_gettime(CLOCK_MONOTONIC, &stamp) != 0 || stamp.tv_sec < 0 || stamp.tv_nsec < 0 || stamp.tv_nsec >= NANOSECONDS_PER_SECOND ||
        stamp.tv_sec > (std::numeric_limits<int64_t>::max() - stamp.tv_nsec) / NANOSECONDS_PER_SECOND) {
        return std::nullopt;
    }
    return static_cast<int64_t>(stamp.tv_sec) * NANOSECONDS_PER_SECOND + stamp.tv_nsec;
}

auto check_record_capture(CaptureReceipt& receipt, bool enabled, CaptureStatus* observed) -> bool {
    auto const STATUS = read_capture_status();
    if (!capture_status_matches(receipt.started, STATUS, enabled)) {
        receipt.control_failed = true;
        return false;
    }
    if (observed != nullptr) *observed = *STATUS;
    return true;
}

auto drain_capture_quantum(int fd, CaptureReceipt& receipt, int64_t deadline_ns) -> CaptureDrainResult {
    static_assert(CAPTURE_QUANTUM_READ_LIMIT * PERF_PROC_READ_LIMIT <= CAPTURE_QUANTUM_BYTE_LIMIT);
    uint64_t const BEFORE_BYTES = receipt.saved_bytes;
    for (uint64_t reads = 0; reads < CAPTURE_QUANTUM_READ_LIMIT; ++reads) {
        if (receipt.read_failed || receipt.write_failed || receipt.control_failed || receipt.limit_reached) {
            return CaptureDrainResult::ERROR;
        }
        auto before = capture_record_clock();
        if (!before) {
            receipt.read_failed = true;
            return CaptureDrainResult::ERROR;
        }
        if (*before >= deadline_ns) {
            return CaptureDrainResult::DEADLINE;
        }
        if (!check_record_capture(receipt, true)) {
            return CaptureDrainResult::ERROR;
        }
        before = capture_record_clock();
        if (!before) {
            receipt.read_failed = true;
            return CaptureDrainResult::ERROR;
        }
        if (*before >= deadline_ns) {
            return CaptureDrainResult::DEADLINE;
        }
        // Every read opens a fresh bounded procfs snapshot. Status and snapshot
        // are separate syscalls: external concurrent control is not an owned session.
        bool const DRAINED = drain_capture(fd, receipt);
        CaptureStatus observed{};
        bool const SAME_CAPTURE = check_record_capture(receipt, true, &observed);
        auto after = capture_record_clock();
        if (!after || *after < *before) {
            receipt.read_failed = true;
        }
        if (!SAME_CAPTURE || receipt.read_failed || receipt.write_failed || receipt.limit_reached) {
            return CaptureDrainResult::ERROR;
        }
        if (*after >= deadline_ns) {
            return CaptureDrainResult::DEADLINE;
        }
        if (!DRAINED) {
            if (observed.pending == 0) return CaptureDrainResult::EMPTY;
            // Events may arrive between the empty snapshot and status read.
            // Pending evidence keeps service ahead of metadata or sleep.
            continue;
        }
        if (receipt.saved_bytes - BEFORE_BYTES >= CAPTURE_QUANTUM_BYTE_LIMIT) {
            return CaptureDrainResult::FULL;
        }
    }
    return CaptureDrainResult::FULL;
}

auto drain_capture(int fd, CaptureReceipt& receipt) -> bool {
    if (receipt.read_failed || receipt.write_failed || receipt.limit_reached) {
        return false;
    }
    auto events = read_file(KPERF_PATH, PERF_DRAIN_CAPACITY);
    if (!events) {
        receipt.read_failed = true;
        return false;
    }
    if (events->empty()) {
        return false;
    }
    if (!events->ends_with('\n')) {
        receipt.read_failed = true;
        return false;
    }
    if (events->size() > CAPTURE_EVENT_BYTE_LIMIT - receipt.saved_bytes) {
        receipt.limit_reached = true;
        return false;
    }
    if (!write_all(fd, *events)) {
        receipt.write_failed = true;
        return false;
    }
    receipt.saved_bytes += events->size();
    receipt.saved_events += static_cast<uint64_t>(std::count(events->begin(), events->end(), '\n'));
    return true;
}

void finish_capture(int fd, CaptureReceipt& receipt) {
    auto const BEFORE = read_capture_status();
    bool const OWNED = receipt.started.has_value();
    bool const MAY_DRAIN = !receipt.control_failed && (!OWNED || capture_status_matches(receipt.started, BEFORE, false));
    if (OWNED && !MAY_DRAIN) receipt.control_failed = true;
    // The first observation is the first read's frontier. Later reads get
    // fresh bookends; a known replacement must never reach the consuming open.
    if (MAY_DRAIN && BEFORE && !BEFORE->enabled) {
        uint64_t const READ_LIMIT = std::min(BEFORE->pending + (BEFORE->pending != UINT64_MAX ? 1U : 0U), CAPTURE_FINAL_READ_LIMIT);
        uint64_t reads = 0;
        for (; reads < READ_LIMIT; ++reads) {
            if (OWNED && reads != 0 && !capture_status_matches(receipt.started, read_capture_status(), false)) {
                receipt.control_failed = true;
                break;
            }
            bool const DRAINED = drain_capture(fd, receipt);
            if (OWNED && !capture_status_matches(receipt.started, read_capture_status(), false)) {
                receipt.control_failed = true;
                break;
            }
            if (!DRAINED) {
                break;
            }
        }
        if (reads == READ_LIMIT) {
            receipt.limit_reached = true;
        }
    } else if (MAY_DRAIN && !OWNED && !BEFORE) {
        // Older kernels have no loss counters. Save bounded retained evidence
        // while leaving completeness explicitly unavailable.
        for (uint64_t reads = 0; reads < CAPTURE_FINAL_READ_LIMIT; ++reads) {
            if (!drain_capture(fd, receipt)) {
                break;
            }
            if (reads + 1 == CAPTURE_FINAL_READ_LIMIT) {
                receipt.limit_reached = true;
            }
        }
    }
    receipt.finished = read_capture_status();
    if (OWNED && !capture_status_matches(receipt.started, receipt.finished, false)) receipt.control_failed = true;
    receipt.stopped_ns = static_cast<uint64_t>(read_clock_ns(CLOCK_MONOTONIC).value_or(0));
}

auto write_capture_receipt(int fd, CaptureReceipt& receipt) -> bool {
    std::array<char, 256> node{};
    (void)gethostname(node.data(), node.size() - 1);
    std::string error;
    auto serialized = serialize_capture_receipt(receipt, node.data(), error);
    if (!serialized || !write_all(fd, SECTION_CAPTURE_STATUS) || !write_all(fd, *serialized) || !write_all(fd, "\n") ||
        !write_all(fd, SECTION_CAPTURE_STATUS_END)) {
        receipt.write_failed = true;
        std::println("perf: cannot save capture receipt: {}", error);
        return false;
    }
    bool const COMPLETE = capture_stream_complete(receipt);
    std::println("perf: event stream {} ({} saved, {} overwritten, {} pending; coverage: {})", COMPLETE ? "complete" : "incomplete",
                 receipt.saved_events, receipt.finished ? receipt.finished->overwritten : 0,
                 receipt.finished ? receipt.finished->pending : 0, receipt.requested_filter);
    return COMPLETE;
}
}  // namespace perf
