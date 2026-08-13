#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace perf::perf_data {

constexpr std::string_view MAGIC{"WOSPERF\0", 8};
constexpr uint16_t CONTAINER_MAJOR = 1;
constexpr uint16_t CONTAINER_MINOR = 0;
constexpr std::size_t MAX_LEGACY_SNAPSHOT_BYTES = std::size_t{8} * 1024 * 1024;
constexpr std::size_t MAX_TYPED_EVENTS_BYTES = std::size_t{48} * 1024 * 1024;
constexpr std::size_t MAX_CONTAINER_BYTES = std::size_t{64} * 1024 * 1024;
constexpr uint64_t MAX_TYPED_EVENT_RECORDS = 262144;
constexpr uint32_t MAX_SECTIONS = 16;

enum class Format : uint8_t {
    LEGACY_RAW,
    LEGACY_SECTIONED,
    STRUCTURED_V1,
};

enum class LoadStatus : uint8_t {
    OK,
    NOT_FOUND,
    IO_ERROR,
    FORMAT_ERROR,
};

enum class SectionType : uint32_t {
    LEGACY_SNAPSHOT = 1,
    TYPED_EVENTS_JSONL = 2,
};

struct SectionInfo {
    uint32_t type{};
    uint32_t flags{};
    uint64_t payload_bytes{};
    uint64_t record_count{};
    uint32_t payload_crc32c{};
};

struct File {
    Format format{Format::LEGACY_RAW};
    uint16_t container_major{};
    uint16_t container_minor{};
    uint64_t encoded_bytes{};
    std::string legacy_snapshot;
    std::string typed_events_jsonl;
    uint64_t typed_event_records{};
    std::vector<SectionInfo> sections;
};

struct ParseResult {
    std::optional<File> file;
    std::string error;
};

struct LoadResult {
    LoadStatus status{LoadStatus::IO_ERROR};
    std::optional<File> file;
    std::string error;
};

struct EncodeResult {
    std::optional<std::string> bytes;
    std::string error;
};

[[nodiscard]] auto format_name(Format format) -> std::string_view;
[[nodiscard]] auto section_type_name(uint32_t type) -> std::string_view;
[[nodiscard]] auto parse(std::string_view bytes) -> ParseResult;
[[nodiscard]] auto load(std::string_view path) -> LoadResult;
[[nodiscard]] auto encode_v1(std::string_view legacy_snapshot, std::string_view typed_events_jsonl, uint64_t typed_event_records)
    -> EncodeResult;

// Replace a completed legacy file only after the complete structured successor
// has been encoded, written, synced, and closed. On failure the legacy file is
// left in place and the temporary file is removed.
[[nodiscard]] auto convert_legacy_file_atomic(std::string_view path, std::string_view typed_events_jsonl, uint64_t typed_event_records,
                                              std::string& error) -> bool;

}  // namespace perf::perf_data
