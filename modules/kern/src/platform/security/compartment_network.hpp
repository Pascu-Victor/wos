#pragma once

#include <cstdint>

namespace ker::mod::security::compartment {

// Ambient network access is immutable in an exact retained private PID view.
// Explicitly inherited sockets remain shared object capabilities.
enum class Network : uint8_t { DENIED, SHARED_HOST };

}  // namespace ker::mod::security::compartment
