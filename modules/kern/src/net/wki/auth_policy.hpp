#pragma once

#include <cstdint>
#include <net/wki/auth.hpp>
#include <net/wki/wire.hpp>

namespace ker::net::wki::auth_policy {

constexpr uint64_t INVALID = UINT64_MAX;

// Returns the complete permission mask required to admit a request. Zero means
// the message is a response or lifecycle/control frame. INVALID is malformed
// or names an unknown privileged resource/operation.
auto required_permissions(MsgType message, const void* payload, uint16_t payload_length) -> uint64_t;

}  // namespace ker::net::wki::auth_policy
