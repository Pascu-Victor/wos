#include "service_manifest.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <utility>

namespace wos::init {
namespace {

using namespace std::string_view_literals;

enum FieldBit : uint64_t {
    FIELD_EXEC = 1ULL << 0,
    FIELD_ARGUMENT = 1ULL << 1,
    FIELD_TYPE = 1ULL << 2,
    FIELD_CLASS = 1ULL << 3,
    FIELD_PRIORITY = 1ULL << 4,
    FIELD_ENVIRONMENT_PROFILE = 1ULL << 5,
    FIELD_READINESS = 1ULL << 6,
    FIELD_READINESS_TIMEOUT = 1ULL << 7,
    FIELD_RESTART = 1ULL << 8,
    FIELD_BACKOFF_INITIAL = 1ULL << 9,
    FIELD_BACKOFF_MAX = 1ULL << 10,
    FIELD_RESTART_BUDGET = 1ULL << 11,
    FIELD_RESTART_WINDOW = 1ULL << 12,
    FIELD_STABLE_RUN = 1ULL << 13,
    FIELD_STOP_TERM = 1ULL << 14,
    FIELD_STOP_KILL = 1ULL << 15,
    FIELD_DRAIN_TIMEOUT = 1ULL << 16,
    FIELD_STDIO = 1ULL << 17,
    FIELD_ENABLEMENT = 1ULL << 18,
    FIELD_READINESS_INTERFACE = 1ULL << 19,
    FIELD_ENABLEMENT_PATH = 1ULL << 20,
};

constexpr uint64_t REQUIRED_FIELDS = FIELD_EXEC | FIELD_ARGUMENT | FIELD_TYPE | FIELD_CLASS | FIELD_PRIORITY | FIELD_ENVIRONMENT_PROFILE |
                                     FIELD_READINESS | FIELD_READINESS_TIMEOUT | FIELD_RESTART | FIELD_BACKOFF_INITIAL | FIELD_BACKOFF_MAX |
                                     FIELD_RESTART_BUDGET | FIELD_RESTART_WINDOW | FIELD_STABLE_RUN | FIELD_STOP_TERM | FIELD_STOP_KILL |
                                     FIELD_DRAIN_TIMEOUT | FIELD_STDIO | FIELD_ENABLEMENT;

constexpr uint32_t MAX_READINESS_TIMEOUT_MS = 10U * 60U * 1000U;
constexpr uint32_t MAX_BACKOFF_MS = 60U * 60U * 1000U;
constexpr uint32_t MAX_RESTART_BUDGET = 32;
constexpr uint32_t MAX_RESTART_WINDOW_MS = 24U * 60U * 60U * 1000U;
constexpr uint32_t MAX_STABLE_RUN_MS = 24U * 60U * 60U * 1000U;
constexpr uint32_t MAX_STOP_TIMEOUT_MS = 60U * 1000U;

auto parse_failure(ManifestParseError error, size_t line) -> ManifestParseResult { return {.error = error, .line = line}; }

auto validation_failure(ManifestValidationError error, uint8_t service, uint8_t related = INVALID_SERVICE_ID) -> ManifestValidationResult {
    return {.error = error, .service = service, .related_service = related};
}

auto trim(std::string_view value) -> std::string_view {
    while (!value.empty() && value.front() == ' ') {
        value.remove_prefix(1);
    }
    while (!value.empty() && value.back() == ' ') {
        value.remove_suffix(1);
    }
    return value;
}

auto valid_manifest_character(unsigned char value) -> bool { return value == '\n' || value == '\r' || (value >= 0x20 && value <= 0x7e); }

auto parse_u32(std::string_view value, uint32_t& out) -> bool {
    if (value.empty()) {
        return false;
    }
    uint32_t result = 0;
    for (char const c : value) {
        if (c < '0' || c > '9') {
            return false;
        }
        uint32_t const digit = static_cast<uint32_t>(c - '0');
        if (result > (std::numeric_limits<uint32_t>::max() - digit) / 10U) {
            return false;
        }
        result = (result * 10U) + digit;
    }
    out = result;
    return true;
}

auto parse_i32(std::string_view value, int32_t& out) -> bool {
    if (value.empty()) {
        return false;
    }
    bool const negative = value.front() == '-';
    if (negative) {
        value.remove_prefix(1);
    }
    uint32_t magnitude = 0;
    if (!parse_u32(value, magnitude)) {
        return false;
    }
    uint32_t constexpr MIN_MAGNITUDE = static_cast<uint32_t>(std::numeric_limits<int32_t>::max()) + 1U;
    if ((!negative && magnitude > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) || (negative && magnitude > MIN_MAGNITUDE)) {
        return false;
    }
    if (negative && magnitude == MIN_MAGNITUDE) {
        out = std::numeric_limits<int32_t>::min();
    } else {
        auto const signed_magnitude = static_cast<int32_t>(magnitude);
        out = negative ? -signed_magnitude : signed_magnitude;
    }
    return true;
}

auto safe_service_name(std::string_view name) -> bool {
    if (name.empty() || name.size() >= MAX_SERVICE_NAME_BYTES) {
        return false;
    }
    auto alpha = [](char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'); };
    if (!alpha(name.front())) {
        return false;
    }
    for (char const c : name) {
        if (!alpha(c) && !(c >= '0' && c <= '9') && c != '_' && c != '-') {
            return false;
        }
    }
    return true;
}

auto set_once(uint64_t& fields, uint64_t field) -> bool {
    if ((fields & field) != 0) {
        return false;
    }
    fields |= field;
    return true;
}

template <typename Enum, size_t Count>
auto assign_enum(std::string_view value, const std::array<std::pair<std::string_view, Enum>, Count>& choices, Enum& out) -> bool {
    for (auto const& [name, choice] : choices) {
        if (value == name) {
            out = choice;
            return true;
        }
    }
    return false;
}

auto parse_service_field(ServiceSpec& service, uint64_t& fields, std::string_view key, std::string_view value, ManifestParseError& error)
    -> bool {
    auto scalar = [&](uint64_t bit) {
        if (!set_once(fields, bit)) {
            error = ManifestParseError::DUPLICATE_FIELD;
            return false;
        }
        return true;
    };
    auto fixed = [&](auto& destination, uint64_t bit) {
        if (!scalar(bit)) {
            return false;
        }
        if (value.empty() || !destination.assign(value)) {
            error = ManifestParseError::VALUE_TOO_LONG;
            return false;
        }
        return true;
    };
    auto unsigned_value = [&](uint32_t& destination, uint64_t bit) {
        if (!scalar(bit)) {
            return false;
        }
        if (!parse_u32(value, destination)) {
            error = ManifestParseError::INVALID_VALUE;
            return false;
        }
        return true;
    };

    if (key == "exec") {
        return fixed(service.executable, FIELD_EXEC);
    }
    if (key == "arg") {
        if (service.argument_count >= service.arguments.size()) {
            error = ManifestParseError::TOO_MANY_ARGUMENTS;
            return false;
        }
        if (value.empty() || !service.arguments.at(service.argument_count).assign(value)) {
            error = ManifestParseError::VALUE_TOO_LONG;
            return false;
        }
        ++service.argument_count;
        fields |= FIELD_ARGUMENT;
        return true;
    }
    if (key == "env") {
        if (service.environment_count >= service.environment.size()) {
            error = ManifestParseError::TOO_MANY_ENVIRONMENT_ENTRIES;
            return false;
        }
        if (value.empty() || !service.environment.at(service.environment_count).assign(value)) {
            error = ManifestParseError::VALUE_TOO_LONG;
            return false;
        }
        ++service.environment_count;
        return true;
    }
    if (key == "depends") {
        if (service.dependency_count >= service.dependency_names.size()) {
            error = ManifestParseError::TOO_MANY_DEPENDENCIES;
            return false;
        }
        if (value.empty() || !service.dependency_names.at(service.dependency_count).assign(value)) {
            error = ManifestParseError::VALUE_TOO_LONG;
            return false;
        }
        ++service.dependency_count;
        return true;
    }
    if (key == "type") {
        if (!scalar(FIELD_TYPE)) {
            return false;
        }
        constexpr std::array CHOICES{
            std::pair{"daemon"sv, ServiceType::DAEMON},
            std::pair{"oneshot"sv, ServiceType::ONESHOT},
        };
        if (!assign_enum(value, CHOICES, service.type)) {
            error = ManifestParseError::INVALID_VALUE;
            return false;
        }
        return true;
    }
    if (key == "class") {
        if (!scalar(FIELD_CLASS)) {
            return false;
        }
        constexpr std::array CHOICES{
            std::pair{"journal"sv, ServiceClass::JOURNAL},
            std::pair{"network-provider"sv, ServiceClass::NETWORK_PROVIDER},
            std::pair{"network-consumer"sv, ServiceClass::NETWORK_CONSUMER},
            std::pair{"normal"sv, ServiceClass::NORMAL},
        };
        if (!assign_enum(value, CHOICES, service.service_class)) {
            error = ManifestParseError::INVALID_VALUE;
            return false;
        }
        return true;
    }
    if (key == "priority") {
        if (!scalar(FIELD_PRIORITY)) {
            return false;
        }
        if (!parse_i32(value, service.priority)) {
            error = ManifestParseError::INVALID_VALUE;
            return false;
        }
        return true;
    }
    if (key == "environment") {
        if (!scalar(FIELD_ENVIRONMENT_PROFILE)) {
            return false;
        }
        constexpr std::array CHOICES{
            std::pair{"init"sv, EnvironmentProfile::INIT},
            std::pair{"empty"sv, EnvironmentProfile::EMPTY},
        };
        if (!assign_enum(value, CHOICES, service.environment_profile)) {
            error = ManifestParseError::INVALID_VALUE;
            return false;
        }
        return true;
    }
    if (key == "readiness") {
        if (!scalar(FIELD_READINESS)) {
            return false;
        }
        constexpr std::array CHOICES{
            std::pair{"immediate"sv, ReadinessKind::IMMEDIATE},
            std::pair{"ipv4"sv, ReadinessKind::IPV4},
            std::pair{"exit-success"sv, ReadinessKind::PROCESS_EXIT_SUCCESS},
        };
        if (!assign_enum(value, CHOICES, service.readiness)) {
            error = ManifestParseError::INVALID_VALUE;
            return false;
        }
        return true;
    }
    if (key == "readiness-interface") {
        return fixed(service.readiness_interface, FIELD_READINESS_INTERFACE);
    }
    if (key == "readiness-timeout-ms") {
        return unsigned_value(service.readiness_timeout_ms, FIELD_READINESS_TIMEOUT);
    }
    if (key == "restart") {
        if (!scalar(FIELD_RESTART)) {
            return false;
        }
        constexpr std::array CHOICES{
            std::pair{"never"sv, RestartPolicy::NEVER},
            std::pair{"on-failure"sv, RestartPolicy::ON_FAILURE},
            std::pair{"always"sv, RestartPolicy::ALWAYS},
        };
        if (!assign_enum(value, CHOICES, service.restart)) {
            error = ManifestParseError::INVALID_VALUE;
            return false;
        }
        return true;
    }
    if (key == "backoff-initial-ms") {
        return unsigned_value(service.backoff_initial_ms, FIELD_BACKOFF_INITIAL);
    }
    if (key == "backoff-max-ms") {
        return unsigned_value(service.backoff_max_ms, FIELD_BACKOFF_MAX);
    }
    if (key == "restart-budget") {
        return unsigned_value(service.restart_budget, FIELD_RESTART_BUDGET);
    }
    if (key == "restart-window-ms") {
        return unsigned_value(service.restart_window_ms, FIELD_RESTART_WINDOW);
    }
    if (key == "stable-run-ms") {
        return unsigned_value(service.stable_run_ms, FIELD_STABLE_RUN);
    }
    if (key == "stop-term-ms") {
        return unsigned_value(service.stop_term_ms, FIELD_STOP_TERM);
    }
    if (key == "stop-kill-ms") {
        return unsigned_value(service.stop_kill_ms, FIELD_STOP_KILL);
    }
    if (key == "drain-timeout-ms") {
        return unsigned_value(service.drain_timeout_ms, FIELD_DRAIN_TIMEOUT);
    }
    if (key == "stdio") {
        if (!scalar(FIELD_STDIO)) {
            return false;
        }
        constexpr std::array CHOICES{
            std::pair{"inherit"sv, StdioPolicy::INHERIT},
            std::pair{"null"sv, StdioPolicy::NULL_DEVICE},
            std::pair{"journal"sv, StdioPolicy::JOURNAL},
        };
        if (!assign_enum(value, CHOICES, service.stdio)) {
            error = ManifestParseError::INVALID_VALUE;
            return false;
        }
        return true;
    }
    if (key == "enable") {
        if (!scalar(FIELD_ENABLEMENT)) {
            return false;
        }
        constexpr std::array CHOICES{
            std::pair{"always"sv, EnablementKind::ALWAYS},
            std::pair{"disabled"sv, EnablementKind::DISABLED},
            std::pair{"path-exists"sv, EnablementKind::PATH_EXISTS},
            std::pair{"path-missing"sv, EnablementKind::PATH_MISSING},
        };
        if (!assign_enum(value, CHOICES, service.enablement)) {
            error = ManifestParseError::INVALID_VALUE;
            return false;
        }
        return true;
    }
    if (key == "enable-path") {
        return fixed(service.enablement_path, FIELD_ENABLEMENT_PATH);
    }

    error = ManifestParseError::UNKNOWN_FIELD;
    return false;
}

auto safe_absolute_path(std::string_view path) -> bool {
    if (path.size() < 2 || path.front() != '/' || path.back() == '/') {
        return false;
    }
    size_t segment_start = 1;
    for (size_t i = 1; i <= path.size(); ++i) {
        if (i != path.size() && path[i] != '/') {
            char const c = path[i];
            bool const valid =
                (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.' || c == '+';
            if (!valid) {
                return false;
            }
            continue;
        }
        std::string_view const segment = path.substr(segment_start, i - segment_start);
        if (segment.empty() || segment == "." || segment == "..") {
            return false;
        }
        segment_start = i + 1;
    }
    return true;
}

auto valid_environment_entry(std::string_view entry) -> bool {
    size_t const equals = entry.find('=');
    if (equals == std::string_view::npos || equals == 0) {
        return false;
    }
    auto const key = entry.substr(0, equals);
    auto alpha_or_underscore = [](char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'; };
    if (!alpha_or_underscore(key.front())) {
        return false;
    }
    for (char const c : key) {
        if (!alpha_or_underscore(c) && !(c >= '0' && c <= '9')) {
            return false;
        }
    }
    return true;
}

auto environment_key(std::string_view entry) -> std::string_view { return entry.substr(0, entry.find('=')); }

auto valid_interface_name(std::string_view name) -> bool {
    if (name.empty() || name.size() >= MAX_INTERFACE_NAME_BYTES) {
        return false;
    }
    for (char const c : name) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.')) {
            return false;
        }
    }
    return true;
}

auto bit_count(uint16_t value) -> uint8_t {
    uint8_t count = 0;
    while (value != 0) {
        count = static_cast<uint8_t>(count + (value & 1U));
        value = static_cast<uint16_t>(value >> 1U);
    }
    return count;
}

}  // namespace

auto parse_service_manifest(std::span<const char> input, ServiceManifest& manifest) -> ManifestParseResult {
    manifest = {};
    if (input.empty()) {
        return parse_failure(ManifestParseError::EMPTY, 0);
    }
    if (input.size() > MAX_MANIFEST_BYTES) {
        return parse_failure(ManifestParseError::TOO_LARGE, 0);
    }
    for (char const c : input) {
        if (!valid_manifest_character(static_cast<unsigned char>(c))) {
            return parse_failure(ManifestParseError::INVALID_CHARACTER, 0);
        }
    }

    std::array<uint64_t, MAX_SERVICES> fields{};
    bool version_seen = false;
    bool content_seen = false;
    int current_service = -1;
    size_t offset = 0;
    size_t line_number = 0;
    while (offset < input.size()) {
        ++line_number;
        size_t end = offset;
        while (end < input.size() && input[end] != '\n') {
            ++end;
        }
        size_t line_size = end - offset;
        if (line_size != 0 && input[offset + line_size - 1] == '\r') {
            --line_size;
        }
        if (line_size > MAX_MANIFEST_LINE_BYTES) {
            return parse_failure(ManifestParseError::LINE_TOO_LONG, line_number);
        }
        std::string_view line(input.data() + offset, line_size);
        offset = end < input.size() ? end + 1 : end;
        line = trim(line);
        if (line.empty() || line.front() == '#') {
            continue;
        }

        if (line.front() == '[') {
            content_seen = true;
            if (!version_seen) {
                return parse_failure(ManifestParseError::VERSION_REQUIRED_FIRST, line_number);
            }
            constexpr auto PREFIX = "[service "sv;
            if (!line.starts_with(PREFIX) || line.size() <= PREFIX.size() + 1 || line.back() != ']') {
                return parse_failure(ManifestParseError::MALFORMED_SERVICE_HEADER, line_number);
            }
            auto const name = trim(line.substr(PREFIX.size(), line.size() - PREFIX.size() - 1));
            if (!safe_service_name(name)) {
                return parse_failure(ManifestParseError::MALFORMED_SERVICE_HEADER, line_number);
            }
            if (manifest.service_count >= manifest.services.size()) {
                return parse_failure(ManifestParseError::TOO_MANY_SERVICES, line_number);
            }
            current_service = manifest.service_count++;
            if (!manifest.services.at(static_cast<size_t>(current_service)).name.assign(name)) {
                return parse_failure(ManifestParseError::VALUE_TOO_LONG, line_number);
            }
            continue;
        }

        size_t const equals = line.find('=');
        if (equals == std::string_view::npos) {
            return parse_failure(ManifestParseError::EXPECTED_ASSIGNMENT, line_number);
        }
        auto const key = trim(line.substr(0, equals));
        auto const value = trim(line.substr(equals + 1));
        if (!content_seen) {
            content_seen = true;
            if (key != "version") {
                return parse_failure(ManifestParseError::VERSION_REQUIRED_FIRST, line_number);
            }
            uint32_t version = 0;
            if (!parse_u32(value, version)) {
                return parse_failure(ManifestParseError::INVALID_VALUE, line_number);
            }
            if (version != SERVICE_MANIFEST_VERSION) {
                return parse_failure(ManifestParseError::UNSUPPORTED_VERSION, line_number);
            }
            manifest.version = version;
            version_seen = true;
            continue;
        }
        if (key == "version") {
            return parse_failure(ManifestParseError::DUPLICATE_VERSION, line_number);
        }
        if (current_service < 0) {
            return parse_failure(ManifestParseError::FIELD_OUTSIDE_SERVICE, line_number);
        }
        ManifestParseError error = ManifestParseError::NONE;
        size_t const index = static_cast<size_t>(current_service);
        if (!parse_service_field(manifest.services.at(index), fields.at(index), key, value, error)) {
            return parse_failure(error, line_number);
        }
    }

    if (!version_seen) {
        return parse_failure(content_seen ? ManifestParseError::VERSION_REQUIRED_FIRST : ManifestParseError::EMPTY, 0);
    }
    for (size_t i = 0; i < manifest.service_count; ++i) {
        uint64_t const present = fields.at(i);
        if ((present & REQUIRED_FIELDS) != REQUIRED_FIELDS) {
            return parse_failure(ManifestParseError::MISSING_REQUIRED_FIELD, 0);
        }
        auto const& service = manifest.services.at(i);
        if (service.readiness == ReadinessKind::IPV4 && (present & FIELD_READINESS_INTERFACE) == 0) {
            return parse_failure(ManifestParseError::MISSING_REQUIRED_FIELD, 0);
        }
        bool const conditional = service.enablement == EnablementKind::PATH_EXISTS || service.enablement == EnablementKind::PATH_MISSING;
        if (conditional && (present & FIELD_ENABLEMENT_PATH) == 0) {
            return parse_failure(ManifestParseError::MISSING_REQUIRED_FIELD, 0);
        }
        if (!conditional && (present & FIELD_ENABLEMENT_PATH) != 0) {
            return parse_failure(ManifestParseError::INVALID_VALUE, 0);
        }
        if (service.readiness != ReadinessKind::IPV4 && (present & FIELD_READINESS_INTERFACE) != 0) {
            return parse_failure(ManifestParseError::INVALID_VALUE, 0);
        }
    }
    return {};
}

auto validate_service_manifest(ServiceManifest& manifest, ServiceTopology& topology) -> ManifestValidationResult {
    topology = {};
    topology.journal_service = INVALID_SERVICE_ID;
    if (manifest.version != SERVICE_MANIFEST_VERSION || manifest.service_count == 0 || manifest.service_count > MAX_SERVICES) {
        return validation_failure(ManifestValidationError::EMPTY, INVALID_SERVICE_ID);
    }
    topology.service_count = manifest.service_count;

    for (uint8_t i = 0; i < manifest.service_count; ++i) {
        auto& service = manifest.services.at(i);
        service.dependency_mask = 0;
        service.dependent_mask = 0;
        if (!safe_service_name(service.name.view())) {
            return validation_failure(ManifestValidationError::INVALID_NAME, i);
        }
        for (uint8_t other = 0; other < i; ++other) {
            if (service.name.view() == manifest.services.at(other).name.view()) {
                return validation_failure(ManifestValidationError::DUPLICATE_NAME, i, other);
            }
        }
        if (!safe_absolute_path(service.executable.view())) {
            return validation_failure(ManifestValidationError::UNSAFE_EXECUTABLE, i);
        }
        if (service.argument_count == 0 || service.argument_count > MAX_ARGUMENTS ||
            service.arguments.front().view() != service.executable.view()) {
            return validation_failure(ManifestValidationError::INVALID_ARGUMENT_VECTOR, i);
        }
        for (uint8_t env = 0; env < service.environment_count; ++env) {
            auto const entry = service.environment.at(env).view();
            if (!valid_environment_entry(entry)) {
                return validation_failure(ManifestValidationError::INVALID_ENVIRONMENT, i);
            }
            for (uint8_t previous = 0; previous < env; ++previous) {
                if (environment_key(entry) == environment_key(service.environment.at(previous).view())) {
                    return validation_failure(ManifestValidationError::INVALID_ENVIRONMENT, i);
                }
            }
        }
        if (service.priority < -20 || service.priority > 19 || service.readiness_timeout_ms == 0 ||
            service.readiness_timeout_ms > MAX_READINESS_TIMEOUT_MS || service.backoff_initial_ms == 0 ||
            service.backoff_initial_ms > service.backoff_max_ms || service.backoff_max_ms > MAX_BACKOFF_MS || service.restart_budget == 0 ||
            service.restart_budget > MAX_RESTART_BUDGET || service.restart_window_ms == 0 ||
            service.restart_window_ms > MAX_RESTART_WINDOW_MS || service.stable_run_ms == 0 || service.stable_run_ms > MAX_STABLE_RUN_MS ||
            service.stop_term_ms == 0 || service.stop_term_ms > MAX_STOP_TIMEOUT_MS || service.stop_kill_ms == 0 ||
            service.stop_kill_ms > MAX_STOP_TIMEOUT_MS || service.drain_timeout_ms == 0 || service.drain_timeout_ms > MAX_STOP_TIMEOUT_MS) {
            return validation_failure(ManifestValidationError::INVALID_RANGE, i);
        }
        bool const conditional = service.enablement == EnablementKind::PATH_EXISTS || service.enablement == EnablementKind::PATH_MISSING;
        if ((conditional && !safe_absolute_path(service.enablement_path.view())) || (!conditional && !service.enablement_path.empty())) {
            return validation_failure(ManifestValidationError::INVALID_ENABLEMENT, i);
        }
        if ((service.type == ServiceType::ONESHOT && service.readiness != ReadinessKind::PROCESS_EXIT_SUCCESS) ||
            (service.type == ServiceType::DAEMON && service.readiness == ReadinessKind::PROCESS_EXIT_SUCCESS) ||
            (service.type == ServiceType::ONESHOT && service.restart == RestartPolicy::ALWAYS) ||
            (service.readiness == ReadinessKind::IPV4 &&
             (service.service_class != ServiceClass::NETWORK_PROVIDER || !valid_interface_name(service.readiness_interface.view()))) ||
            (service.readiness != ReadinessKind::IPV4 && !service.readiness_interface.empty())) {
            return validation_failure(ManifestValidationError::INVALID_READINESS, i);
        }
    }

    for (uint8_t i = 0; i < manifest.service_count; ++i) {
        auto& service = manifest.services.at(i);
        for (uint8_t dependency_index = 0; dependency_index < service.dependency_count; ++dependency_index) {
            auto const dependency_name = service.dependency_names.at(dependency_index).view();
            uint8_t resolved = INVALID_SERVICE_ID;
            for (uint8_t candidate = 0; candidate < manifest.service_count; ++candidate) {
                if (manifest.services.at(candidate).name.view() == dependency_name) {
                    resolved = candidate;
                    break;
                }
            }
            if (resolved == INVALID_SERVICE_ID) {
                return validation_failure(ManifestValidationError::UNKNOWN_DEPENDENCY, i);
            }
            if (resolved == i) {
                return validation_failure(ManifestValidationError::SELF_DEPENDENCY, i, resolved);
            }
            uint16_t const bit = static_cast<uint16_t>(1U << resolved);
            if ((service.dependency_mask & bit) != 0) {
                return validation_failure(ManifestValidationError::DUPLICATE_DEPENDENCY, i, resolved);
            }
            if (service.enablement != EnablementKind::DISABLED && manifest.services.at(resolved).enablement == EnablementKind::DISABLED) {
                return validation_failure(ManifestValidationError::DISABLED_DEPENDENCY, i, resolved);
            }
            service.dependency_mask = static_cast<uint16_t>(service.dependency_mask | bit);
            manifest.services.at(resolved).dependent_mask =
                static_cast<uint16_t>(manifest.services.at(resolved).dependent_mask | static_cast<uint16_t>(1U << i));
        }
    }

    std::array<uint8_t, MAX_SERVICES> indegree{};
    std::array<bool, MAX_SERVICES> emitted{};
    for (uint8_t i = 0; i < manifest.service_count; ++i) {
        indegree.at(i) = bit_count(manifest.services.at(i).dependency_mask);
    }
    for (uint8_t order = 0; order < manifest.service_count; ++order) {
        uint8_t next = INVALID_SERVICE_ID;
        for (uint8_t candidate = 0; candidate < manifest.service_count; ++candidate) {
            if (!emitted.at(candidate) && indegree.at(candidate) == 0) {
                next = candidate;
                break;
            }
        }
        if (next == INVALID_SERVICE_ID) {
            return validation_failure(ManifestValidationError::DEPENDENCY_CYCLE, INVALID_SERVICE_ID);
        }
        emitted.at(next) = true;
        topology.start_order.at(order) = next;
        topology.stop_order.at(static_cast<size_t>(manifest.service_count - order - 1)) = next;
        uint16_t dependents = manifest.services.at(next).dependent_mask;
        for (uint8_t dependent = 0; dependent < manifest.service_count; ++dependent) {
            if ((dependents & static_cast<uint16_t>(1U << dependent)) != 0) {
                --indegree.at(dependent);
            }
        }
    }

    for (uint8_t order = 0; order < manifest.service_count; ++order) {
        uint8_t const service_id = topology.start_order.at(order);
        uint16_t transitive = manifest.services.at(service_id).dependency_mask;
        for (uint8_t dependency = 0; dependency < manifest.service_count; ++dependency) {
            if ((manifest.services.at(service_id).dependency_mask & static_cast<uint16_t>(1U << dependency)) != 0) {
                transitive = static_cast<uint16_t>(transitive | topology.transitive_dependencies.at(dependency));
            }
        }
        topology.transitive_dependencies.at(service_id) = transitive;
    }

    uint8_t journal_count = 0;
    uint16_t provider_mask = 0;
    for (uint8_t i = 0; i < manifest.service_count; ++i) {
        auto const& service = manifest.services.at(i);
        if (service.service_class == ServiceClass::JOURNAL) {
            ++journal_count;
            topology.journal_service = i;
        }
        if (service.enablement != EnablementKind::DISABLED && service.service_class == ServiceClass::NETWORK_PROVIDER) {
            provider_mask = static_cast<uint16_t>(provider_mask | static_cast<uint16_t>(1U << i));
        }
    }
    if (journal_count != 1) {
        return validation_failure(ManifestValidationError::JOURNAL_INVARIANT, topology.journal_service);
    }
    auto const& journal = manifest.services.at(topology.journal_service);
    if (journal.type != ServiceType::DAEMON || journal.enablement != EnablementKind::ALWAYS ||
        journal.readiness != ReadinessKind::IMMEDIATE || journal.stdio == StdioPolicy::JOURNAL || journal.dependency_mask != 0) {
        return validation_failure(ManifestValidationError::JOURNAL_INVARIANT, topology.journal_service);
    }
    uint16_t const journal_bit = static_cast<uint16_t>(1U << topology.journal_service);
    for (uint8_t i = 0; i < manifest.service_count; ++i) {
        auto const& service = manifest.services.at(i);
        if (service.enablement == EnablementKind::DISABLED || i == topology.journal_service) {
            continue;
        }
        if ((topology.transitive_dependencies.at(i) & journal_bit) == 0) {
            return validation_failure(ManifestValidationError::JOURNAL_INVARIANT, i, topology.journal_service);
        }
        if (service.service_class == ServiceClass::NETWORK_PROVIDER && service.readiness != ReadinessKind::IPV4) {
            return validation_failure(ManifestValidationError::NETWORK_INVARIANT, i);
        }
        if (service.service_class == ServiceClass::NETWORK_CONSUMER && (topology.transitive_dependencies.at(i) & provider_mask) == 0) {
            return validation_failure(ManifestValidationError::NETWORK_INVARIANT, i);
        }
    }
    return {};
}

}  // namespace wos::init
