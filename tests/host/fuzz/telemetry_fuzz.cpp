#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <wos/telemetry.hpp>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    namespace telemetry = wos::telemetry;
    telemetry::Limits json_limits{.max_input_bytes = 4096,
                                  .max_depth = 12,
                                  .max_nodes = 512,
                                  .max_object_members = 128,
                                  .max_array_elements = 128,
                                  .max_string_bytes = 1024};
    const std::string_view input(reinterpret_cast<const char*>(data), size);
    auto parsed = telemetry::parse(input, json_limits);
    if (parsed) {
        auto serialized = telemetry::serialize(*parsed.value, json_limits);
        if (serialized) {
            (void)telemetry::parse(*serialized.text, json_limits);
        }
        telemetry::Error error;
        (void)telemetry::validate_envelope(*parsed.value, &error);
    }
    telemetry::ContainerLimits container_limits{.max_total_bytes = 4096, .max_sections = 16, .max_section_bytes = 2048};
    (void)telemetry::decode_container(std::as_bytes(std::span(reinterpret_cast<const char*>(data), size)), std::nullopt, container_limits);
    return 0;
}
