#include "perf_data.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <wos/telemetry.hpp>

namespace perf::perf_data {
namespace {

constexpr std::string_view LEGACY_SECTION_PREFIX = "--- SECTION";
constexpr int FILE_MODE = 0644;
constexpr std::size_t IO_CHUNK_BYTES = 4096;
constexpr std::size_t MAX_TYPED_EVENT_LINE_BYTES = 256 * 1024;
constexpr uint32_t KNOWN_SECTION_FLAGS = wos::telemetry::SECTION_REQUIRED;

constexpr auto magic_array() -> std::array<char, 8> {
    std::array<char, 8> magic{};
    for (std::size_t index = 0; index < magic.size(); ++index) {
        magic.at(index) = MAGIC.at(index);
    }
    return magic;
}

constexpr auto container_limits() -> wos::telemetry::ContainerLimits {
    return {
        .max_total_bytes = MAX_CONTAINER_BYTES,
        .max_sections = MAX_SECTIONS,
        .max_section_bytes = MAX_TYPED_EVENTS_BYTES,
    };
}

auto fail(std::string message) -> ParseResult { return ParseResult{.file = std::nullopt, .error = std::move(message)}; }

auto validate_legacy_text(std::string_view bytes, Format& format, std::string& error) -> bool {
    const bool TEXT_ONLY = std::ranges::all_of(
        bytes, [](unsigned char byte) { return byte == '\n' || byte == '\r' || byte == '\t' || (byte >= 0x20U && byte <= 0x7EU); });
    if (!TEXT_ONLY) {
        error = "legacy perf input contains non-text bytes";
        return false;
    }
    if (bytes.empty()) {
        format = Format::LEGACY_RAW;
        return true;
    }

    if (!bytes.starts_with(LEGACY_SECTION_PREFIX)) {
        format = Format::LEGACY_RAW;
        std::size_t position = 0;
        while (position < bytes.size()) {
            const std::size_t NEWLINE = bytes.find('\n', position);
            if (NEWLINE == std::string_view::npos) {
                error = "legacy raw event input ends with a partial record";
                return false;
            }
            const std::string_view LINE = bytes.substr(position, NEWLINE - position);
            if (!LINE.empty() && (LINE.size() < 2 || !std::string_view("SXWBCK").contains(LINE.front()) || LINE[1] != ' ')) {
                error = "legacy raw event input contains an unrecognized record";
                return false;
            }
            position = NEWLINE + 1;
        }
        return true;
    }

    format = Format::LEGACY_SECTIONED;
    constexpr std::string_view HEADER_PREFIX = "--- SECTION ";
    constexpr std::string_view MARKER_SUFFIX = " ---";
    std::size_t position = 0;
    std::size_t sections = 0;
    while (position < bytes.size()) {
        const std::size_t HEADER_NEWLINE = bytes.find('\n', position);
        if (HEADER_NEWLINE == std::string_view::npos) {
            error = "legacy section header is partial";
            return false;
        }
        const std::string_view HEADER = bytes.substr(position, HEADER_NEWLINE - position);
        if (!HEADER.starts_with(HEADER_PREFIX) || !HEADER.ends_with(MARKER_SUFFIX) ||
            HEADER.size() <= HEADER_PREFIX.size() + MARKER_SUFFIX.size()) {
            error = "legacy sectioned input has text outside a section";
            return false;
        }
        const std::string_view NAME = HEADER.substr(HEADER_PREFIX.size(), HEADER.size() - HEADER_PREFIX.size() - MARKER_SUFFIX.size());
        if (!std::ranges::all_of(NAME, [](unsigned char byte) { return (byte >= 'A' && byte <= 'Z') || byte == '_'; })) {
            error = "legacy section name is invalid";
            return false;
        }
        std::string footer = "--- END ";
        footer.append(NAME);
        footer.append(MARKER_SUFFIX);
        position = HEADER_NEWLINE + 1;
        bool closed = false;
        while (position < bytes.size()) {
            const std::size_t LINE_NEWLINE = bytes.find('\n', position);
            if (LINE_NEWLINE == std::string_view::npos) {
                error = "legacy section payload ends with a partial record";
                return false;
            }
            const std::string_view LINE = bytes.substr(position, LINE_NEWLINE - position);
            position = LINE_NEWLINE + 1;
            if (LINE == footer) {
                closed = true;
                break;
            }
            if (LINE.starts_with(HEADER_PREFIX) || LINE.starts_with("--- END ")) {
                error = "legacy section markers are nested or mismatched";
                return false;
            }
        }
        if (!closed) {
            error = "legacy section is unterminated";
            return false;
        }
        if (++sections > MAX_SECTIONS) {
            error = "legacy section count exceeds the limit";
            return false;
        }
    }
    return sections != 0;
}

auto payload_string(const std::vector<std::byte>& payload) -> std::string {
    return std::string(reinterpret_cast<const char*>(payload.data()), payload.size());
}

auto payload_bytes(std::string_view payload) -> std::vector<std::byte> {
    auto bytes = std::as_bytes(std::span(payload.data(), payload.size()));
    return std::vector<std::byte>(bytes.begin(), bytes.end());
}

auto count_and_validate_jsonl(std::string_view jsonl, std::string& error) -> std::optional<uint64_t> {
    if (jsonl.empty()) {
        return uint64_t{0};
    }
    if (jsonl.back() != '\n') {
        error = "typed-events JSONL lacks a final newline";
        return std::nullopt;
    }

    uint64_t count = 0;
    std::size_t line_start = 0;
    while (line_start < jsonl.size()) {
        std::size_t const newline = jsonl.find('\n', line_start);
        if (newline == std::string_view::npos || newline == line_start) {
            error = "typed-events JSONL contains an empty or unterminated record";
            return std::nullopt;
        }
        std::string_view const LINE = jsonl.substr(line_start, newline - line_start);
        if (LINE.size() > MAX_TYPED_EVENT_LINE_BYTES) {
            error = "typed-events JSONL record exceeds the size limit";
            return std::nullopt;
        }

        wos::telemetry::Limits limits{
            .max_input_bytes = MAX_TYPED_EVENT_LINE_BYTES,
            .max_depth = 16,
            .max_nodes = 256,
            .max_object_members = 128,
            .max_array_elements = 128,
            .max_string_bytes = 64 * 1024,
        };
        auto parsed = wos::telemetry::parse(LINE, limits);
        wos::telemetry::Error envelope_error;
        if (!parsed || !wos::telemetry::validate_envelope(*parsed.value, &envelope_error) ||
            wos::telemetry::object_string(*parsed.value, "source") != std::optional<std::string_view>{"perf"}) {
            error = parsed ? (envelope_error.message.empty() ? "typed-events record has a non-perf source" : envelope_error.message)
                           : parsed.error.message;
            return std::nullopt;
        }

        ++count;
        if (count > MAX_TYPED_EVENT_RECORDS) {
            error = "typed-events record count exceeds the limit";
            return std::nullopt;
        }
        line_start = newline + 1;
    }
    return count;
}

auto read_bounded(std::string_view path, std::size_t max_bytes, LoadStatus& status, std::string& error) -> std::optional<std::string> {
    std::string const owned_path(path);
    int const fd = open(owned_path.c_str(), O_RDONLY, 0);  // NOLINT(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
    if (fd < 0) {
        status = errno == ENOENT ? LoadStatus::NOT_FOUND : LoadStatus::IO_ERROR;
        error = errno == ENOENT ? "file not found" : std::string("open failed: ") + strerror(errno);
        return std::nullopt;
    }

    std::string out;
    out.reserve(std::min<std::size_t>(max_bytes, 131072));
    std::array<char, IO_CHUNK_BYTES> chunk{};
    for (;;) {
        std::size_t const remaining = max_bytes - out.size();
        if (remaining == 0) {
            char extra{};
            ssize_t const count = read(fd, &extra, 1);
            if (count < 0 && errno == EINTR) {
                continue;
            }
            if (count != 0) {
                error = count < 0 ? std::string("read failed: ") + strerror(errno) : "file exceeds the size limit";
                status = LoadStatus::IO_ERROR;
                (void)close(fd);
                return std::nullopt;
            }
            break;
        }

        ssize_t const count = read(fd, chunk.data(), std::min(chunk.size(), remaining));
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            error = std::string("read failed: ") + strerror(errno);
            status = LoadStatus::IO_ERROR;
            (void)close(fd);
            return std::nullopt;
        }
        if (count == 0) {
            break;
        }
        out.append(chunk.data(), static_cast<std::size_t>(count));
    }

    if (close(fd) != 0) {
        error = std::string("close failed: ") + strerror(errno);
        status = LoadStatus::IO_ERROR;
        return std::nullopt;
    }
    status = LoadStatus::OK;
    return out;
}

auto write_all_checked(int fd, std::string_view bytes, std::string& error) -> bool {
    std::size_t written = 0;
    while (written < bytes.size()) {
        ssize_t const count = write(fd, bytes.data() + written, bytes.size() - written);
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count <= 0) {
            error = count < 0 ? std::string("write failed: ") + strerror(errno) : "write made no progress";
            return false;
        }
        written += static_cast<std::size_t>(count);
    }
    return true;
}

auto make_temp_path(std::string_view path, uint32_t attempt) -> std::string {
    std::string temp(path);
    temp += ".structured.tmp.";
    temp += std::to_string(getpid());
    temp += '.';
    temp += std::to_string(attempt);
    return temp;
}

}  // namespace

auto format_name(Format format) -> std::string_view {
    switch (format) {
        case Format::LEGACY_RAW:
            return "legacy-raw";
        case Format::LEGACY_SECTIONED:
            return "legacy-sectioned";
        case Format::STRUCTURED_V1:
            return "structured";
    }
    return "unknown";
}

auto section_type_name(uint32_t type) -> std::string_view {
    switch (static_cast<SectionType>(type)) {
        case SectionType::LEGACY_SNAPSHOT:
            return "legacy-snapshot";
        case SectionType::TYPED_EVENTS_JSONL:
            return "typed-events-jsonl";
    }
    return "unknown";
}

auto parse(std::string_view bytes) -> ParseResult {
    bool const PARTIAL_MAGIC = !bytes.empty() && bytes.size() < MAGIC.size() && MAGIC.substr(0, bytes.size()) == bytes;
    bool const HAS_MAGIC = bytes.size() >= MAGIC.size() && bytes.substr(0, MAGIC.size()) == MAGIC;
    if (!HAS_MAGIC) {
        if (PARTIAL_MAGIC) {
            return fail("structured magic is truncated");
        }
        if (bytes.size() > MAX_LEGACY_SNAPSHOT_BYTES) {
            return fail("legacy snapshot exceeds the size limit");
        }
        Format format = Format::LEGACY_RAW;
        std::string legacy_error;
        if (!validate_legacy_text(bytes, format, legacy_error)) {
            return fail(std::move(legacy_error));
        }
        File file{};
        file.format = format;
        file.encoded_bytes = bytes.size();
        file.legacy_snapshot = std::string(bytes);
        ParseResult result{};
        result.file = std::move(file);
        return result;
    }

    auto input = std::as_bytes(std::span(bytes.data(), bytes.size()));
    auto decoded = wos::telemetry::decode_container(input, magic_array(), container_limits());
    if (!decoded) {
        return fail(decoded.error.message);
    }
    if (decoded.container->major != CONTAINER_MAJOR) {
        return fail("unsupported structured container major version");
    }
    if (decoded.container->flags != 0) {
        return fail("unsupported structured container flags");
    }

    File file{};
    file.format = Format::STRUCTURED_V1;
    file.container_major = decoded.container->major;
    file.container_minor = decoded.container->minor;
    file.encoded_bytes = bytes.size();
    bool have_legacy = false;
    bool have_typed_events = false;

    for (const auto& section : decoded.container->sections) {
        if ((section.flags & ~KNOWN_SECTION_FLAGS) != 0U) {
            return fail("unsupported structured section flags");
        }
        file.sections.push_back(SectionInfo{
            .type = section.type,
            .flags = section.flags,
            .payload_bytes = section.payload.size(),
            .record_count = 0,
            .payload_crc32c = wos::telemetry::crc32c(section.payload),
        });

        if (section.type == static_cast<uint32_t>(SectionType::LEGACY_SNAPSHOT)) {
            if (have_legacy || (section.flags & wos::telemetry::SECTION_REQUIRED) == 0U ||
                section.payload.size() > MAX_LEGACY_SNAPSHOT_BYTES) {
                return fail("invalid or duplicate legacy-snapshot section");
            }
            file.legacy_snapshot = payload_string(section.payload);
            file.sections.back().record_count = 1;
            have_legacy = true;
        } else if (section.type == static_cast<uint32_t>(SectionType::TYPED_EVENTS_JSONL)) {
            if (have_typed_events || (section.flags & wos::telemetry::SECTION_REQUIRED) == 0U ||
                section.payload.size() > MAX_TYPED_EVENTS_BYTES) {
                return fail("invalid or duplicate typed-events section");
            }
            file.typed_events_jsonl = payload_string(section.payload);
            std::string json_error;
            auto count = count_and_validate_jsonl(file.typed_events_jsonl, json_error);
            if (!count.has_value()) {
                return fail(std::move(json_error));
            }
            file.typed_event_records = *count;
            file.sections.back().record_count = *count;
            have_typed_events = true;
        } else if ((section.flags & wos::telemetry::SECTION_REQUIRED) != 0U) {
            return fail("unknown required structured section");
        }
    }

    if (!have_legacy || !have_typed_events) {
        return fail("structured container is missing a required section");
    }
    Format embedded_format = Format::LEGACY_RAW;
    std::string legacy_error;
    if (!validate_legacy_text(file.legacy_snapshot, embedded_format, legacy_error)) {
        return fail("structured legacy snapshot is invalid: " + legacy_error);
    }
    ParseResult result{};
    result.file = std::move(file);
    return result;
}

auto load(std::string_view path) -> LoadResult {
    LoadStatus status = LoadStatus::IO_ERROR;
    std::string error;
    auto bytes = read_bounded(path, MAX_CONTAINER_BYTES, status, error);
    if (!bytes.has_value()) {
        return LoadResult{.status = status, .file = std::nullopt, .error = std::move(error)};
    }
    ParseResult parsed = parse(*bytes);
    if (!parsed.file.has_value()) {
        return LoadResult{.status = LoadStatus::FORMAT_ERROR, .file = std::nullopt, .error = std::move(parsed.error)};
    }
    return LoadResult{.status = LoadStatus::OK, .file = std::move(parsed.file), .error = {}};
}

auto encode_v1(std::string_view legacy_snapshot, std::string_view typed_events_jsonl, uint64_t typed_event_records) -> EncodeResult {
    if (legacy_snapshot.size() > MAX_LEGACY_SNAPSHOT_BYTES) {
        return EncodeResult{.bytes = std::nullopt, .error = "legacy snapshot exceeds the size limit"};
    }
    if (typed_events_jsonl.size() > MAX_TYPED_EVENTS_BYTES || typed_event_records > MAX_TYPED_EVENT_RECORDS) {
        return EncodeResult{.bytes = std::nullopt, .error = "typed-events payload exceeds a container limit"};
    }
    Format legacy_format = Format::LEGACY_RAW;
    std::string legacy_error;
    if (!validate_legacy_text(legacy_snapshot, legacy_format, legacy_error)) {
        return EncodeResult{.bytes = std::nullopt, .error = "legacy snapshot is invalid: " + legacy_error};
    }
    std::string json_error;
    auto actual_count = count_and_validate_jsonl(typed_events_jsonl, json_error);
    if (!actual_count.has_value()) {
        return EncodeResult{.bytes = std::nullopt, .error = std::move(json_error)};
    }
    if (*actual_count != typed_event_records) {
        return EncodeResult{.bytes = std::nullopt, .error = "typed-events record count mismatch"};
    }

    wos::telemetry::BinaryContainer container{
        .magic = magic_array(),
        .major = CONTAINER_MAJOR,
        .minor = CONTAINER_MINOR,
        .flags = 0,
        .sections =
            {
                wos::telemetry::BinarySection{
                    .type = static_cast<uint32_t>(SectionType::LEGACY_SNAPSHOT),
                    .flags = wos::telemetry::SECTION_REQUIRED,
                    .payload = payload_bytes(legacy_snapshot),
                },
                wos::telemetry::BinarySection{
                    .type = static_cast<uint32_t>(SectionType::TYPED_EVENTS_JSONL),
                    .flags = wos::telemetry::SECTION_REQUIRED,
                    .payload = payload_bytes(typed_events_jsonl),
                },
            },
    };
    auto encoded = wos::telemetry::encode_container(container, container_limits());
    if (!encoded) {
        return EncodeResult{.bytes = std::nullopt, .error = std::move(encoded.error.message)};
    }
    return EncodeResult{.bytes = payload_string(*encoded.bytes), .error = {}};
}

auto convert_legacy_file_atomic(std::string_view path, std::string_view typed_events_jsonl, uint64_t typed_event_records,
                                std::string& error) -> bool {
    LoadResult loaded = load(path);
    if (loaded.status != LoadStatus::OK || !loaded.file.has_value()) {
        error = loaded.error.empty() ? "cannot load completed legacy snapshot" : std::move(loaded.error);
        return false;
    }
    if (loaded.file->format == Format::STRUCTURED_V1) {
        error = "input is already a structured perf container";
        return false;
    }

    EncodeResult encoded = encode_v1(loaded.file->legacy_snapshot, typed_events_jsonl, typed_event_records);
    if (!encoded.bytes.has_value()) {
        error = std::move(encoded.error);
        return false;
    }

    int temp_fd = -1;
    std::string temp_path;
    for (uint32_t attempt = 0; attempt < 32; ++attempt) {
        temp_path = make_temp_path(path, attempt);
        temp_fd =
            open(temp_path.c_str(), O_WRONLY | O_CREAT | O_EXCL, FILE_MODE);  // NOLINT(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
        if (temp_fd >= 0 || errno != EEXIST) {
            break;
        }
    }
    if (temp_fd < 0) {
        error = std::string("cannot create structured temporary file: ") + strerror(errno);
        return false;
    }

    bool ok = write_all_checked(temp_fd, *encoded.bytes, error);
    if (ok && fsync(temp_fd) != 0) {
        error = std::string("cannot sync structured temporary file: ") + strerror(errno);
        ok = false;
    }
    if (close(temp_fd) != 0 && ok) {
        error = std::string("cannot close structured temporary file: ") + strerror(errno);
        ok = false;
    }
    if (ok) {
        std::string const owned_path(path);
        if (rename(temp_path.c_str(), owned_path.c_str()) != 0) {
            error = std::string("cannot atomically install structured container: ") + strerror(errno);
            ok = false;
        }
    }
    if (!ok) {
        (void)unlink(temp_path.c_str());
    }
    return ok;
}

}  // namespace perf::perf_data
