#include <iostream>
#include <iterator>
#include <span>
#include <string>

#include "service_manifest.h"

// Exercise the same parser and validator that init uses for injected fixtures.
auto main() -> int {
    std::string const input{std::istreambuf_iterator<char>{std::cin}, std::istreambuf_iterator<char>{}};
    wos::init::ServiceManifest manifest{};
    wos::init::ServiceTopology topology{};
    auto const parsed = wos::init::parse_service_manifest({input.data(), input.size()}, manifest);
    if (!parsed) {
        std::cout << "{\"parse_error\":" << static_cast<unsigned>(parsed.error) << ",\"line\":" << parsed.line << "}\n";
        return 1;
    }
    auto const validated = wos::init::validate_service_manifest(manifest, topology);
    std::cout << "{\"parse_error\":0,\"validation_error\":" << static_cast<unsigned>(validated.error)
              << ",\"service\":" << static_cast<unsigned>(validated.service)
              << ",\"services\":" << static_cast<unsigned>(manifest.service_count) << "}\n";
    return validated ? 0 : 2;
}
