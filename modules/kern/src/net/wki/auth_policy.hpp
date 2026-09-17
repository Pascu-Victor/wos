#pragma once

#include <cstdint>
#include <net/wki/auth.hpp>
#include <net/wki/wire.hpp>
#include <platform/security/credentials.hpp>

namespace ker::net::wki::auth_policy {

constexpr uint64_t INVALID = UINT64_MAX;

// Returns the complete permission mask required to admit a request. Zero means
// the message is a response or lifecycle/control frame. INVALID is malformed
// or names an unknown privileged resource/operation.
auto required_permissions(MsgType message, const void* payload, uint16_t payload_length) -> uint64_t;

// Map an already decoded credential using the receiver's configured peer
// policy. Errors leave mapped unchanged; no ambient worker authority is used.
auto map_net_control_identity(uint64_t policy, const ker::mod::security::Snapshot& supplied, ker::mod::security::Snapshot& mapped) -> int;

auto map_compute_identity(uint64_t policy, const ker::mod::security::Snapshot& supplied, ker::mod::security::Snapshot& mapped) -> int;

auto map_vfs_mutation_identity(uint64_t policy, const ker::mod::security::Snapshot& supplied, ker::mod::security::Snapshot& mapped) -> int;

auto map_vfs_read_identity(uint64_t policy, const ker::mod::security::Snapshot& supplied, ker::mod::security::Snapshot& mapped) -> int;

// Includes identity trust and all access/create/truncate operation bits.
// INVALID denotes an invalid access mode.
auto vfs_open_permissions(uint32_t flags) -> uint64_t;
auto map_vfs_open_identity(uint64_t policy, const ker::mod::security::Snapshot& supplied, uint32_t flags,
                           ker::mod::security::Snapshot& mapped) -> int;

}  // namespace ker::net::wki::auth_policy
