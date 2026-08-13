#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace wos::telemetry {

inline constexpr std::string_view ENVELOPE_FORMAT = "wos.telemetry";
inline constexpr uint32_t ENVELOPE_VERSION = 1;

struct Limits {
    size_t max_input_bytes{1024 * 1024};
    size_t max_depth{16};
    size_t max_nodes{4096};
    size_t max_object_members{1024};
    size_t max_array_elements{4096};
    size_t max_string_bytes{256 * 1024};
};

enum class ErrorCode {
    NONE,
    INPUT_TOO_LARGE,
    UNEXPECTED_END,
    INVALID_SYNTAX,
    INVALID_UTF8,
    INVALID_ESCAPE,
    DUPLICATE_KEY,
    DEPTH_LIMIT,
    NODE_LIMIT,
    MEMBER_LIMIT,
    ARRAY_LIMIT,
    STRING_LIMIT,
    TRAILING_DATA,
    INVALID_ENVELOPE,
    UNSUPPORTED_VERSION,
    INTEGER_RANGE,
    OUTPUT_TOO_LARGE,
    INVALID_CONTAINER,
    CONTAINER_TOO_LARGE,
    SECTION_LIMIT,
    CHECKSUM_MISMATCH,
};

struct Error {
    ErrorCode code{ErrorCode::NONE};
    size_t offset{0};
    std::string message;

    [[nodiscard]] explicit operator bool() const { return code != ErrorCode::NONE; }
};

struct Number {
    std::string lexeme;

    auto operator<=>(const Number&) const = default;
};

struct Value {
    using Array = std::vector<Value>;
    using Object = std::map<std::string, Value, std::less<>>;
    using Storage = std::variant<std::nullptr_t, bool, Number, std::string, Array, Object>;

    Storage data{nullptr};

    Value() = default;
    Value(std::nullptr_t) : data(nullptr) {}
    Value(bool value) : data(value) {}
    Value(const char* value) : data(std::string(value)) {}
    Value(std::string value) : data(std::move(value)) {}
    Value(std::string_view value) : data(std::string(value)) {}
    Value(Array value) : data(std::move(value)) {}
    Value(Object value) : data(std::move(value)) {}

    [[nodiscard]] static auto number(std::string lexeme) -> Value;
    [[nodiscard]] static auto unsigned_integer(uint64_t value) -> Value;
    [[nodiscard]] static auto signed_integer(int64_t value) -> Value;

    [[nodiscard]] auto is_null() const -> bool;
    [[nodiscard]] auto is_bool() const -> bool;
    [[nodiscard]] auto is_number() const -> bool;
    [[nodiscard]] auto is_string() const -> bool;
    [[nodiscard]] auto is_array() const -> bool;
    [[nodiscard]] auto is_object() const -> bool;

    [[nodiscard]] auto as_bool() const -> const bool*;
    [[nodiscard]] auto as_number() const -> const Number*;
    [[nodiscard]] auto as_string() const -> const std::string*;
    [[nodiscard]] auto as_array() const -> const Array*;
    [[nodiscard]] auto as_object() const -> const Object*;
    [[nodiscard]] auto as_array() -> Array*;
    [[nodiscard]] auto as_object() -> Object*;

    auto operator==(const Value&) const -> bool = default;
};

struct ParseResult {
    std::optional<Value> value;
    Error error;

    [[nodiscard]] explicit operator bool() const { return value.has_value(); }
};

struct SerializeResult {
    std::optional<std::string> text;
    Error error;

    [[nodiscard]] explicit operator bool() const { return text.has_value(); }
};

[[nodiscard]] auto parse(std::string_view input, Limits limits = {}) -> ParseResult;
[[nodiscard]] auto serialize(const Value& value, Limits limits = {}) -> SerializeResult;

[[nodiscard]] auto validate_envelope(const Value& value, Error* error = nullptr) -> bool;
[[nodiscard]] auto make_envelope(std::string source, uint32_t source_version, std::string kind, Value::Object identity, Value::Object clock,
                                 Value::Object correlation, Value::Object payload, Value::Object extensions = {}) -> Value;

[[nodiscard]] auto object_member(const Value& value, std::string_view name) -> const Value*;
[[nodiscard]] auto object_string(const Value& value, std::string_view name) -> std::optional<std::string_view>;
[[nodiscard]] auto decimal_u64(const Value& value) -> std::optional<uint64_t>;
[[nodiscard]] auto decimal_i64(const Value& value) -> std::optional<int64_t>;
[[nodiscard]] auto decimal_u64_string(uint64_t value) -> std::string;
[[nodiscard]] auto decimal_i64_string(int64_t value) -> std::string;

[[nodiscard]] auto crc32c(std::span<const std::byte> bytes) -> uint32_t;
[[nodiscard]] auto crc32c(std::string_view bytes) -> uint32_t;

inline constexpr uint32_t SECTION_REQUIRED = 1U << 0U;

struct BinarySection {
    uint32_t type{0};
    uint32_t flags{0};
    std::vector<std::byte> payload;

    auto operator==(const BinarySection&) const -> bool = default;
};

struct BinaryContainer {
    std::array<char, 8> magic{};
    uint16_t major{1};
    uint16_t minor{0};
    uint32_t flags{0};
    std::vector<BinarySection> sections;

    auto operator==(const BinaryContainer&) const -> bool = default;
};

struct ContainerLimits {
    size_t max_total_bytes{64 * 1024 * 1024};
    size_t max_sections{64};
    size_t max_section_bytes{32 * 1024 * 1024};
};

struct EncodeContainerResult {
    std::optional<std::vector<std::byte>> bytes;
    Error error;

    [[nodiscard]] explicit operator bool() const { return bytes.has_value(); }
};

struct DecodeContainerResult {
    std::optional<BinaryContainer> container;
    Error error;

    [[nodiscard]] explicit operator bool() const { return container.has_value(); }
};

[[nodiscard]] auto encode_container(const BinaryContainer& container, ContainerLimits limits = {}) -> EncodeContainerResult;
[[nodiscard]] auto decode_container(std::span<const std::byte> input, std::optional<std::array<char, 8>> expected_magic = std::nullopt,
                                    ContainerLimits limits = {}) -> DecodeContainerResult;

}  // namespace wos::telemetry
