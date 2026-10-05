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
}  // namespace

auto begin_capture(const char* filter) -> CaptureReceipt {
    CaptureReceipt receipt;
    receipt.requested_filter = filter != nullptr ? filter : "all";
    receipt.control_failed = !set_recording_enabled(true, filter);
    receipt.started = read_capture_status();
    return receipt;
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
    // A live or replaced capture must not make this drain unbounded.
    if (BEFORE && !BEFORE->enabled) {
        uint64_t const READ_LIMIT = std::min(BEFORE->pending + (BEFORE->pending != UINT64_MAX ? 1U : 0U), CAPTURE_FINAL_READ_LIMIT);
        uint64_t reads = 0;
        for (; reads < READ_LIMIT; ++reads) {
            if (!drain_capture(fd, receipt)) {
                break;
            }
        }
        if (reads == READ_LIMIT) {
            receipt.limit_reached = true;
        }
    } else if (!BEFORE) {
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
