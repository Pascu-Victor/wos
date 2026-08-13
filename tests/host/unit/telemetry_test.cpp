#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>
#include <wos/telemetry.hpp>

namespace {
namespace telemetry = wos::telemetry;

TEST(TelemetryJson, RoundTripsWideIntegersUnknownFieldsAndHostileText) {
    auto envelope = telemetry::make_envelope(
        "journal", 1, "journal.record",
        {{"boot_id", telemetry::Value::unsigned_integer(UINT64_MAX)},
         {"node_id", "node-0"},
         {"pid", telemetry::Value::unsigned_integer(9007199254740993ULL)},
         {"tid", telemetry::Value::unsigned_integer(42)},
         {"cpu", telemetry::Value::number("7")}},
        {{"domain", "boot_monotonic"}, {"value", telemetry::Value::unsigned_integer(UINT64_MAX - 1)}, {"unit", "ns"}, {"quality", "local"}},
        {{"trace_id", telemetry::Value::unsigned_integer(9007199254740999ULL)}},
        {{"message", telemetry::Value("line\n\t\"quoted\" ð")}},
        {{"future_extension", telemetry::Value::Object{{"enabled", true}}}});

    const auto first = telemetry::serialize(envelope);
    ASSERT_TRUE(first) << first.error.message;
    const auto parsed = telemetry::parse(*first.text);
    ASSERT_TRUE(parsed) << parsed.error.message;
    telemetry::Error validation_error;
    EXPECT_TRUE(telemetry::validate_envelope(*parsed.value, &validation_error)) << validation_error.message;
    const auto second = telemetry::serialize(*parsed.value);
    ASSERT_TRUE(second) << second.error.message;
    EXPECT_EQ(*first.text, *second.text);

    const auto* identity = telemetry::object_member(*parsed.value, "identity");
    ASSERT_NE(identity, nullptr);
    const auto* boot = telemetry::object_member(*identity, "boot_id");
    ASSERT_NE(boot, nullptr);
    EXPECT_EQ(telemetry::decimal_u64(*boot), UINT64_MAX);
    EXPECT_NE(telemetry::object_member(*parsed.value, "future_extension"), nullptr);
}

TEST(TelemetryJson, RejectsMalformedDuplicateInvalidUtf8AndBounds) {
    const auto duplicate = telemetry::parse(R"({"a":1,"a":2})");
    EXPECT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error.code, telemetry::ErrorCode::DUPLICATE_KEY);

    std::string invalid_utf8{"\""};
    invalid_utf8.push_back(static_cast<char>(0xc0));
    invalid_utf8.push_back(static_cast<char>(0x80));
    invalid_utf8.push_back('"');
    const auto utf8 = telemetry::parse(invalid_utf8);
    EXPECT_FALSE(utf8);
    EXPECT_EQ(utf8.error.code, telemetry::ErrorCode::INVALID_UTF8);

    EXPECT_FALSE(telemetry::parse(R"({"a":[1,2})"));

    telemetry::Limits tiny;
    tiny.max_input_bytes = 8;
    const auto oversized = telemetry::parse(R"({"value":1})", tiny);
    EXPECT_FALSE(oversized);
    EXPECT_EQ(oversized.error.code, telemetry::ErrorCode::INPUT_TOO_LARGE);

    telemetry::Limits shallow;
    shallow.max_depth = 1;
    const auto deep = telemetry::parse(R"({"a":{"b":1}})", shallow);
    EXPECT_FALSE(deep);
    EXPECT_EQ(deep.error.code, telemetry::ErrorCode::DEPTH_LIMIT);
}

TEST(TelemetryJson, RejectsUnknownEnvelopeMajor) {
    const auto future = telemetry::parse(
        R"({"clock":{},"correlation":{},"format":"wos.telemetry","identity":{},"kind":"test","payload":{"new":true},"source":"fixture","source_version":1,"version":2})");
    ASSERT_TRUE(future) << future.error.message;
    telemetry::Error error;
    EXPECT_FALSE(telemetry::validate_envelope(*future.value, &error));
    EXPECT_EQ(error.code, telemetry::ErrorCode::UNSUPPORTED_VERSION);
}

TEST(TelemetryJson, RejectsIllTypedKnownIdentityAndClockFields) {
    auto envelope = telemetry::make_envelope("fixture", 1, "test", {{"pid", telemetry::Value::unsigned_integer(1)}},
                                             {{"domain", "boot_monotonic"}, {"value", telemetry::Value::unsigned_integer(2)}}, {}, {});
    telemetry::Error error;
    ASSERT_TRUE(telemetry::validate_envelope(envelope, &error)) << error.message;

    auto* root = envelope.as_object();
    ASSERT_NE(root, nullptr);
    auto* identity = root->at("identity").as_object();
    ASSERT_NE(identity, nullptr);
    identity->insert_or_assign("pid", telemetry::Value::number("1"));
    EXPECT_FALSE(telemetry::validate_envelope(envelope, &error));
    EXPECT_EQ(error.code, telemetry::ErrorCode::INVALID_ENVELOPE);

    identity->insert_or_assign("pid", telemetry::Value::unsigned_integer(1));
    identity->insert_or_assign("cpu", telemetry::Value::number("4294967296"));
    EXPECT_FALSE(telemetry::validate_envelope(envelope, &error));
    EXPECT_EQ(error.code, telemetry::ErrorCode::INVALID_ENVELOPE);

    identity->erase("cpu");
    auto* clock = root->at("clock").as_object();
    ASSERT_NE(clock, nullptr);
    clock->insert_or_assign("value", telemetry::Value::number("2"));
    EXPECT_FALSE(telemetry::validate_envelope(envelope, &error));
    EXPECT_EQ(error.code, telemetry::ErrorCode::INVALID_ENVELOPE);

    clock->insert_or_assign("value", telemetry::Value::unsigned_integer(2));
    clock->insert_or_assign("realtime_offset_ns", telemetry::Value("not-an-integer"));
    EXPECT_FALSE(telemetry::validate_envelope(envelope, &error));
    EXPECT_EQ(error.code, telemetry::ErrorCode::INVALID_ENVELOPE);
}

TEST(TelemetryContainer, UsesStandardCrc32c) { EXPECT_EQ(telemetry::crc32c("123456789"), 0xe3069283U); }

TEST(TelemetryContainer, RoundTripsUnknownSectionsAndDetectsIntegrityFailure) {
    constexpr std::array<char, 8> MAGIC{'W', 'O', 'S', 'T', 'E', 'S', 'T', '\0'};
    telemetry::BinaryContainer input{
        .magic = MAGIC,
        .major = 1,
        .minor = 7,
        .flags = 3,
        .sections =
            {
                {.type = 1, .flags = telemetry::SECTION_REQUIRED, .payload = {std::byte{'a'}, std::byte{'b'}, std::byte{'c'}}},
                {.type = 0x80000001U, .flags = 0, .payload = {std::byte{0}, std::byte{1}}},
            },
    };
    const auto encoded = telemetry::encode_container(input);
    ASSERT_TRUE(encoded) << encoded.error.message;
    const auto decoded = telemetry::decode_container(*encoded.bytes, MAGIC);
    ASSERT_TRUE(decoded) << decoded.error.message;
    EXPECT_EQ(*decoded.container, input);

    for (size_t length = 0; length < encoded.bytes->size(); ++length) {
        const auto partial = telemetry::decode_container(std::span(encoded.bytes->data(), length), MAGIC);
        EXPECT_FALSE(partial) << "accepted truncation at " << length;
    }

    auto header_corrupt = *encoded.bytes;
    header_corrupt[24] ^= std::byte{1};
    EXPECT_FALSE(telemetry::decode_container(header_corrupt, MAGIC));

    auto payload_corrupt = *encoded.bytes;
    payload_corrupt.back() ^= std::byte{1};
    const auto bad_payload = telemetry::decode_container(payload_corrupt, MAGIC);
    EXPECT_FALSE(bad_payload);
    EXPECT_EQ(bad_payload.error.code, telemetry::ErrorCode::CHECKSUM_MISMATCH);
}

TEST(TelemetryContainer, EnforcesDeclaredBoundsBeforeAllocation) {
    telemetry::BinaryContainer input{
        .magic = {'W', 'O', 'S', 'T', 'E', 'S', 'T', '\0'},
        .sections = {{.type = 1, .payload = std::vector<std::byte>(17, std::byte{0})}},
    };
    telemetry::ContainerLimits limits{.max_total_bytes = 256, .max_sections = 4, .max_section_bytes = 16};
    const auto encoded = telemetry::encode_container(input, limits);
    EXPECT_FALSE(encoded);
    EXPECT_EQ(encoded.error.code, telemetry::ErrorCode::CONTAINER_TOO_LARGE);
}

}  // namespace
