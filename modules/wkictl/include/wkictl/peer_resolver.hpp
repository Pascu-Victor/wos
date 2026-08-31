#pragma once

#include <cstddef>
#include <cstdint>

namespace wkictl {

constexpr std::size_t WKI_PEER_PROC_SNAPSHOT_CAPACITY = 4096;

// Resolve one connected hostname from a complete /proc/wki/peers snapshot.
// The parser rejects malformed, duplicate, or producer-truncated snapshots.
auto resolve_peer_hostname_snapshot(const char* snapshot, std::size_t size, const char* hostname, uint16_t* node_id) -> bool;

// Read /proc/wki/peers with a fixed bound and apply the same strict resolver.
auto resolve_peer_hostname(const char* hostname, uint16_t* node_id) -> bool;

}  // namespace wkictl
