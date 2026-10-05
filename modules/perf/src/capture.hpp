#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "observer.hpp"

namespace perf {

struct CaptureStatus {
    uint64_t session{}, control_version{}, cpus{}, written{}, consumed{}, overwritten{}, pending{};
    uint16_t mask{};
    bool enabled{};
};

struct CaptureReceipt {
    std::optional<CaptureStatus> started;
    std::optional<CaptureStatus> finished;
    std::string requested_filter;
    uint64_t saved_events{}, saved_bytes{}, stopped_ns{};
    bool read_failed{}, write_failed{}, control_failed{}, limit_reached{};
    std::optional<RunObserverTrace> run_observer;
};

inline constexpr std::string_view SECTION_CAPTURE_STATUS = "--- SECTION CAPTURE_STATUS ---\n";
inline constexpr std::string_view SECTION_CAPTURE_STATUS_END = "--- END CAPTURE_STATUS ---\n";
inline constexpr uint64_t CAPTURE_EVENT_BYTE_LIMIT = 6 * 1024 * 1024;
inline constexpr uint64_t CAPTURE_FINAL_READ_LIMIT = 1024;

[[nodiscard]] auto parse_capture_status(std::string_view text) -> std::optional<CaptureStatus>;
[[nodiscard]] auto capture_stream_complete(const CaptureReceipt& receipt) -> bool;
[[nodiscard]] auto serialize_capture_receipt(const CaptureReceipt& receipt, std::string_view node_id, std::string& error)
    -> std::optional<std::string>;
[[nodiscard]] auto capture_receipt_from_snapshot(std::string_view snapshot, std::string& error) -> std::optional<std::string_view>;

// I/O helpers shared by record, run, and snapshot saves.
[[nodiscard]] auto begin_capture(const char* filter) -> CaptureReceipt;
auto drain_capture(int fd, CaptureReceipt& receipt) -> bool;
void finish_capture(int fd, CaptureReceipt& receipt);
auto write_capture_receipt(int fd, CaptureReceipt& receipt) -> bool;

}  // namespace perf
