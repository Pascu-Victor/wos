#include "perf_data.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <wos/telemetry.hpp>

#include "perf_event_json.hpp"

namespace {

using perf::TypedEventRecord;
using perf::perf_data::Format;
using perf::perf_data::SectionType;
constexpr std::string_view VALID_LEGACY_RAW = "S 1 0 2 0x3 4 0\n";

auto as_bytes(std::string_view text) -> std::vector<std::byte> {
    auto bytes = std::as_bytes(std::span(text.data(), text.size()));
    return {bytes.begin(), bytes.end()};
}

auto magic_array() -> std::array<char, 8> {
    std::array<char, 8> magic{};
    std::ranges::copy(perf::perf_data::MAGIC, magic.begin());
    return magic;
}

auto make_event(char type) -> TypedEventRecord {
    return TypedEventRecord{
        .type = type,
        .ts_ns = 123456789,
        .cpu = 3,
        .pid = 42,
        .other_pid = 84,
        .data = 0x1234,
        .callsite = "sched.cpp:9",
        .subsystem = "fd_table",
        .scope = "remote_ipc",
        .operation = "write",
        .phase = "end",
        .lag = -7,
        .flags = 5,
        .aux = 91,
        .peer = 2,
        .channel = 4,
        .correlation = 99,
        .status = -5,
        .wait_channel = "pipe_wait",
    };
}

auto event_line(char type) -> std::string {
    std::string error;
    auto line = perf::serialize_typed_perf_event(make_event(type), -123, "node-a", error);
    EXPECT_TRUE(line.has_value()) << error;
    return line.value_or(std::string{});
}

auto all_event_jsonl() -> std::string {
    std::string jsonl;
    for (char type : {'S', 'X', 'W', 'B', 'C', 'K'}) {
        jsonl += event_line(type);
        jsonl.push_back('\n');
    }
    return jsonl;
}

auto object_at(const wos::telemetry::Value& value, std::string_view name) -> const wos::telemetry::Value& {
    const auto* member = wos::telemetry::object_member(value, name);
    EXPECT_NE(member, nullptr) << name;
    return *member;
}

TEST(PerfTypedEventTest, EveryLegacyKindProducesAValidatedEnvelope) {
    constexpr std::array<std::pair<char, std::string_view>, 6> CASES{{
        {'S', "perf.sample"},
        {'X', "perf.switch"},
        {'W', "perf.wake"},
        {'B', "perf.sleep"},
        {'C', "perf.container_stat"},
        {'K', "perf.wki"},
    }};

    for (const auto& [type, kind] : CASES) {
        auto line = event_line(type);
        auto parsed = wos::telemetry::parse(line);
        ASSERT_TRUE(parsed) << parsed.error.message;
        EXPECT_TRUE(wos::telemetry::validate_envelope(*parsed.value));
        EXPECT_EQ(wos::telemetry::object_string(*parsed.value, "source"), "perf");
        EXPECT_EQ(wos::telemetry::object_string(*parsed.value, "kind"), kind);

        const auto& identity = object_at(*parsed.value, "identity");
        EXPECT_EQ(wos::telemetry::decimal_u64(object_at(identity, "pid")), 42);
        const auto* cpu = object_at(identity, "cpu").as_number();
        ASSERT_NE(cpu, nullptr);
        EXPECT_EQ(cpu->lexeme, "3");
        EXPECT_EQ(wos::telemetry::object_string(identity, "node_id"), "node-a");
        EXPECT_EQ(wos::telemetry::object_member(identity, "tid"), nullptr);

        const auto& clock = object_at(*parsed.value, "clock");
        EXPECT_EQ(wos::telemetry::object_string(clock, "domain"), "boot_monotonic");
        EXPECT_EQ(wos::telemetry::decimal_u64(object_at(clock, "value")), 123456789);
    }
}

TEST(PerfTypedEventTest, UnrecoverableLegacyFieldsAreExplicit) {
    auto parsed = wos::telemetry::parse(event_line('C'));
    ASSERT_TRUE(parsed);
    const auto& payload = object_at(*parsed.value, "payload");
    EXPECT_TRUE(object_at(payload, "instance_id").is_null());
    EXPECT_EQ(wos::telemetry::object_string(payload, "instance_id_quality"), "unavailable_in_legacy_kperf");
    EXPECT_EQ(wos::telemetry::object_string(payload, "thread_identity_quality"), "unavailable_in_legacy_kperf");
}

TEST(PerfDataTest, LegacyDetectionIsExplicit) {
    auto raw = perf::perf_data::parse("S 1 0 2 0x3 4 0\n");
    ASSERT_TRUE(raw.file.has_value());
    EXPECT_EQ(raw.file->format, Format::LEGACY_RAW);

    auto sectioned = perf::perf_data::parse("--- SECTION EVENTS ---\n--- END EVENTS ---\n");
    ASSERT_TRUE(sectioned.file.has_value());
    EXPECT_EQ(sectioned.file->format, Format::LEGACY_SECTIONED);

    for (std::size_t length = 1; length < perf::perf_data::MAGIC.size(); ++length) {
        auto truncated = perf::perf_data::parse(perf::perf_data::MAGIC.substr(0, length));
        EXPECT_FALSE(truncated.file.has_value()) << length;
    }
}

TEST(PerfDataTest, StructuredRoundTripRetainsExactLegacySnapshotAndTypedEvents) {
    std::string const LEGACY =
        "--- SECTION EVENTS ---\nS 1 0 2 0x3 4 0\n--- END EVENTS ---\n"
        "--- SECTION FUTURE ---\nopaque=value\n--- END FUTURE ---\n";
    std::string const JSONL = all_event_jsonl();
    auto encoded = perf::perf_data::encode_v1(LEGACY, JSONL, 6);
    ASSERT_TRUE(encoded.bytes.has_value()) << encoded.error;

    auto second = perf::perf_data::encode_v1(LEGACY, JSONL, 6);
    ASSERT_TRUE(second.bytes.has_value());
    EXPECT_EQ(*encoded.bytes, *second.bytes);

    auto parsed = perf::perf_data::parse(*encoded.bytes);
    ASSERT_TRUE(parsed.file.has_value()) << parsed.error;
    EXPECT_EQ(parsed.file->format, Format::STRUCTURED_V1);
    EXPECT_EQ(parsed.file->container_major, 1);
    EXPECT_EQ(parsed.file->container_minor, 0);
    EXPECT_EQ(parsed.file->legacy_snapshot, LEGACY);
    EXPECT_EQ(parsed.file->typed_events_jsonl, JSONL);
    EXPECT_EQ(parsed.file->typed_event_records, 6);
}

TEST(PerfDataTest, EveryTruncationBoundaryAndChecksumCorruptionIsRejected) {
    auto encoded = perf::perf_data::encode_v1(VALID_LEGACY_RAW, all_event_jsonl(), 6);
    ASSERT_TRUE(encoded.bytes.has_value());

    for (std::size_t length = 1; length < encoded.bytes->size(); ++length) {
        auto parsed = perf::perf_data::parse(std::string_view(*encoded.bytes).substr(0, length));
        EXPECT_FALSE(parsed.file.has_value()) << length;
    }

    for (std::size_t offset : {std::size_t{12}, std::size_t{32}, encoded.bytes->size() - 1}) {
        std::string corrupt = *encoded.bytes;
        corrupt.at(offset) ^= 0x5a;
        auto parsed = perf::perf_data::parse(corrupt);
        EXPECT_FALSE(parsed.file.has_value()) << offset;
    }

    for (std::size_t offset = 0; offset < perf::perf_data::MAGIC.size(); ++offset) {
        std::string corrupt = *encoded.bytes;
        corrupt.at(offset) ^= 0x01;
        auto parsed = perf::perf_data::parse(corrupt);
        EXPECT_FALSE(parsed.file.has_value()) << "magic byte " << offset;
    }
}

TEST(PerfDataTest, UnknownOptionalSectionsSkipAndUnknownRequiredSectionsFail) {
    std::string const JSONL = all_event_jsonl();
    auto make_container = [&](uint32_t unknown_flags) {
        return wos::telemetry::BinaryContainer{
            .magic = magic_array(),
            .major = 1,
            .minor = 7,
            .sections =
                {
                    {static_cast<uint32_t>(SectionType::LEGACY_SNAPSHOT), wos::telemetry::SECTION_REQUIRED, as_bytes(VALID_LEGACY_RAW)},
                    {777, unknown_flags, as_bytes("future")},
                    {static_cast<uint32_t>(SectionType::TYPED_EVENTS_JSONL), wos::telemetry::SECTION_REQUIRED, as_bytes(JSONL)},
                },
        };
    };

    auto optional = wos::telemetry::encode_container(make_container(0));
    ASSERT_TRUE(optional);
    auto parsed = perf::perf_data::parse(std::string_view(reinterpret_cast<const char*>(optional.bytes->data()), optional.bytes->size()));
    ASSERT_TRUE(parsed.file.has_value()) << parsed.error;
    EXPECT_EQ(parsed.file->container_minor, 7);
    ASSERT_EQ(parsed.file->sections.size(), 3);
    EXPECT_EQ(parsed.file->sections.at(1).type, 777);

    auto required = wos::telemetry::encode_container(make_container(wos::telemetry::SECTION_REQUIRED));
    ASSERT_TRUE(required);
    auto rejected = perf::perf_data::parse(std::string_view(reinterpret_cast<const char*>(required.bytes->data()), required.bytes->size()));
    EXPECT_FALSE(rejected.file.has_value());
}

TEST(PerfDataTest, InvalidJsonlAndRecordCountAreRejectedBeforeEncoding) {
    auto invalid = perf::perf_data::encode_v1(VALID_LEGACY_RAW, "not-json\n", 1);
    EXPECT_FALSE(invalid.bytes.has_value());

    auto wrong_count = perf::perf_data::encode_v1(VALID_LEGACY_RAW, all_event_jsonl(), 5);
    EXPECT_FALSE(wrong_count.bytes.has_value());
}

TEST(PerfDataTest, PartialLegacyRecordsAndSectionMarkersAreRejected) {
    EXPECT_FALSE(perf::perf_data::parse("S 1 0 2 0x3 4 0").file.has_value());
    EXPECT_FALSE(perf::perf_data::parse("--- SECTION EVENTS ---\nS 1 0 2 0x3 4 0\n").file.has_value());
    EXPECT_FALSE(perf::perf_data::parse("--- SECTION EVENTS ---\n--- END PROC_MAP ---\n").file.has_value());

    auto invalid_snapshot = perf::perf_data::encode_v1("--- SECTION EVENTS ---\n", all_event_jsonl(), 6);
    EXPECT_FALSE(invalid_snapshot.bytes.has_value());
}

TEST(PerfDataTest, AtomicConversionPreservesInputOnRejection) {
    std::array<char, 64> directory_template{};
    std::ranges::copy(std::string_view("/tmp/wos-perf-data-test-XXXXXX"), directory_template.begin());
    char* created = mkdtemp(directory_template.data());
    ASSERT_NE(created, nullptr);
    std::filesystem::path const directory(created);
    struct Cleanup {
        std::filesystem::path path;
        ~Cleanup() { std::filesystem::remove_all(path); }
    } cleanup{directory};
    std::filesystem::path const PATH = directory / "perf.data";
    std::string const LEGACY = "--- SECTION EVENTS ---\n--- END EVENTS ---\n";
    {
        std::ofstream output(PATH, std::ios::binary);
        output << LEGACY;
    }

    std::string error;
    EXPECT_FALSE(perf::perf_data::convert_legacy_file_atomic(PATH.string(), "not-json\n", 1, error));
    std::ifstream rejected_input(PATH, std::ios::binary);
    std::string const REJECTED_BYTES((std::istreambuf_iterator<char>(rejected_input)), std::istreambuf_iterator<char>());
    EXPECT_EQ(REJECTED_BYTES, LEGACY);

    error.clear();
    EXPECT_TRUE(perf::perf_data::convert_legacy_file_atomic(PATH.string(), {}, 0, error)) << error;
    auto loaded = perf::perf_data::load(PATH.string());
    ASSERT_EQ(loaded.status, perf::perf_data::LoadStatus::OK) << loaded.error;
    ASSERT_TRUE(loaded.file.has_value());
    EXPECT_EQ(loaded.file->format, Format::STRUCTURED_V1);
    EXPECT_EQ(loaded.file->legacy_snapshot, LEGACY);

    std::ifstream input(PATH, std::ios::binary);
    std::string const BEFORE((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    error.clear();
    EXPECT_FALSE(perf::perf_data::convert_legacy_file_atomic(PATH.string(), {}, 0, error));
    std::ifstream input_after(PATH, std::ios::binary);
    std::string const AFTER((std::istreambuf_iterator<char>(input_after)), std::istreambuf_iterator<char>());
    EXPECT_EQ(AFTER, BEFORE);
}

}  // namespace
