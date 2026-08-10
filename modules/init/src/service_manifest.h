#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>

namespace wos::init {

constexpr uint32_t SERVICE_MANIFEST_VERSION = 1;
constexpr size_t MAX_MANIFEST_BYTES = 32 * 1024;
constexpr size_t MAX_MANIFEST_LINE_BYTES = 512;
constexpr size_t MAX_SERVICES = 16;
constexpr size_t MAX_SERVICE_NAME_BYTES = 32;
constexpr size_t MAX_PATH_BYTES = 256;
constexpr size_t MAX_ARGUMENT_BYTES = 256;
constexpr size_t MAX_ENVIRONMENT_BYTES = 256;
constexpr size_t MAX_INTERFACE_NAME_BYTES = 16;
constexpr size_t MAX_ARGUMENTS = 16;
constexpr size_t MAX_ENVIRONMENT = 16;
constexpr size_t MAX_DEPENDENCIES = 8;
constexpr uint8_t INVALID_SERVICE_ID = UINT8_MAX;

template <size_t Capacity>
struct FixedString {
    static_assert(Capacity > 1);

    std::array<char, Capacity> storage{};
    uint16_t length{};

    auto assign(std::string_view value) -> bool {
        if (value.size() >= storage.size()) {
            return false;
        }
        for (size_t i = 0; i < value.size(); ++i) {
            storage.at(i) = value[i];
        }
        length = static_cast<uint16_t>(value.size());
        storage.at(value.size()) = '\0';
        return true;
    }

    [[nodiscard]] auto view() const -> std::string_view { return {storage.data(), length}; }
    [[nodiscard]] auto c_str() const -> const char* { return storage.data(); }
    [[nodiscard]] auto empty() const -> bool { return length == 0; }
};

enum class ServiceType : uint8_t {
    DAEMON,
    ONESHOT,
};

enum class ServiceClass : uint8_t {
    JOURNAL,
    NETWORK_PROVIDER,
    NETWORK_CONSUMER,
    NORMAL,
};

enum class EnvironmentProfile : uint8_t {
    INIT,
    EMPTY,
};

enum class ReadinessKind : uint8_t {
    IMMEDIATE,
    IPV4,
    PROCESS_EXIT_SUCCESS,
};

enum class RestartPolicy : uint8_t {
    NEVER,
    ON_FAILURE,
    ALWAYS,
};

enum class StdioPolicy : uint8_t {
    INHERIT,
    NULL_DEVICE,
    JOURNAL,
};

enum class EnablementKind : uint8_t {
    ALWAYS,
    DISABLED,
    PATH_EXISTS,
    PATH_MISSING,
};

struct ServiceSpec {
    FixedString<MAX_SERVICE_NAME_BYTES> name{};
    FixedString<MAX_PATH_BYTES> executable{};
    std::array<FixedString<MAX_ARGUMENT_BYTES>, MAX_ARGUMENTS> arguments{};
    std::array<FixedString<MAX_ENVIRONMENT_BYTES>, MAX_ENVIRONMENT> environment{};
    std::array<FixedString<MAX_SERVICE_NAME_BYTES>, MAX_DEPENDENCIES> dependency_names{};
    FixedString<MAX_INTERFACE_NAME_BYTES> readiness_interface{};
    FixedString<MAX_PATH_BYTES> enablement_path{};

    uint8_t argument_count{};
    uint8_t environment_count{};
    uint8_t dependency_count{};
    uint16_t dependency_mask{};
    uint16_t dependent_mask{};

    ServiceType type{ServiceType::DAEMON};
    ServiceClass service_class{ServiceClass::NORMAL};
    EnvironmentProfile environment_profile{EnvironmentProfile::INIT};
    ReadinessKind readiness{ReadinessKind::IMMEDIATE};
    RestartPolicy restart{RestartPolicy::NEVER};
    StdioPolicy stdio{StdioPolicy::INHERIT};
    EnablementKind enablement{EnablementKind::ALWAYS};

    int32_t priority{};
    uint32_t readiness_timeout_ms{};
    uint32_t backoff_initial_ms{};
    uint32_t backoff_max_ms{};
    uint32_t restart_budget{};
    uint32_t restart_window_ms{};
    uint32_t stable_run_ms{};
    uint32_t stop_term_ms{};
    uint32_t stop_kill_ms{};
    uint32_t drain_timeout_ms{};
};

struct ServiceManifest {
    uint32_t version{};
    uint8_t service_count{};
    std::array<ServiceSpec, MAX_SERVICES> services{};
};

struct ServiceTopology {
    uint8_t service_count{};
    uint8_t journal_service{INVALID_SERVICE_ID};
    std::array<uint8_t, MAX_SERVICES> start_order{};
    std::array<uint8_t, MAX_SERVICES> stop_order{};
    std::array<uint16_t, MAX_SERVICES> transitive_dependencies{};
};

enum class ManifestParseError : uint8_t {
    NONE,
    EMPTY,
    TOO_LARGE,
    LINE_TOO_LONG,
    INVALID_CHARACTER,
    EXPECTED_ASSIGNMENT,
    VERSION_REQUIRED_FIRST,
    DUPLICATE_VERSION,
    UNSUPPORTED_VERSION,
    MALFORMED_SERVICE_HEADER,
    TOO_MANY_SERVICES,
    FIELD_OUTSIDE_SERVICE,
    UNKNOWN_FIELD,
    DUPLICATE_FIELD,
    INVALID_VALUE,
    VALUE_TOO_LONG,
    TOO_MANY_ARGUMENTS,
    TOO_MANY_ENVIRONMENT_ENTRIES,
    TOO_MANY_DEPENDENCIES,
    MISSING_REQUIRED_FIELD,
};

struct ManifestParseResult {
    ManifestParseError error{ManifestParseError::NONE};
    size_t line{};

    [[nodiscard]] explicit operator bool() const { return error == ManifestParseError::NONE; }
};

enum class ManifestValidationError : uint8_t {
    NONE,
    EMPTY,
    INVALID_NAME,
    DUPLICATE_NAME,
    UNSAFE_EXECUTABLE,
    INVALID_ARGUMENT_VECTOR,
    INVALID_ENVIRONMENT,
    INVALID_RANGE,
    INVALID_ENABLEMENT,
    INVALID_READINESS,
    UNKNOWN_DEPENDENCY,
    DUPLICATE_DEPENDENCY,
    SELF_DEPENDENCY,
    DISABLED_DEPENDENCY,
    DEPENDENCY_CYCLE,
    JOURNAL_INVARIANT,
    NETWORK_INVARIANT,
};

struct ManifestValidationResult {
    ManifestValidationError error{ManifestValidationError::NONE};
    uint8_t service{INVALID_SERVICE_ID};
    uint8_t related_service{INVALID_SERVICE_ID};

    [[nodiscard]] explicit operator bool() const { return error == ManifestValidationError::NONE; }
};

auto parse_service_manifest(std::span<const char> input, ServiceManifest& manifest) -> ManifestParseResult;
auto validate_service_manifest(ServiceManifest& manifest, ServiceTopology& topology) -> ManifestValidationResult;

}  // namespace wos::init
