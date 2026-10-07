#pragma once

#include <cerrno>
#include <cstdint>
#include <net/wki/wire.hpp>

namespace ker::net::wki {

constexpr uint32_t WKI_SECURE_ZONE_WORKLOAD_MAX_BYTES = 4096;

struct WkiSecureZoneWorkloadResult {
    int status = -ENODATA;
    int create_status = -ENODATA;
    int prepare_status = -ENODATA;
    int write_status = -ENODATA;
    int read_status = -ENODATA;
    int destroy_status = -ENODATA;
    uint32_t requested_bytes = 0;
    uint32_t written_bytes = 0;
    uint32_t read_bytes = 0;
    uint32_t mismatch_offset = UINT32_MAX;
    bool created = false;
    bool verified = false;
    // Exact local destroy accepted; remote ACK/backing drain is separate.
    bool destroyed = false;
    ZoneTypeHint type = ZoneTypeHint::BUFFER;
};

// Task-only, with no caller-held peer/zone/IPC/spinlock gates. The boot-gated
// diagnostic dispatcher admits callers. Creates a fresh one-page BUFFER zone,
// writes/reads 1..4096 pattern bytes through exact retained-allocation APIs,
// then destroys that allocation. Never exposes or takes over existing memory.
// Secure/fallback completion is measured by parent-owned core diagnostics,
// never inferred from availability. Returns first failure, then cleanup error.
// require_secure waits at most50ms for this exact allocation's current full
// authenticated grant before WRITE. A ready snapshot is not a completion.
auto wki_secure_zone_workload_run(uint16_t peer, uint32_t zone_id, uint32_t bytes, uint64_t seed, WkiSecureZoneWorkloadResult& result,
                                  bool require_secure = false) -> int;

}  // namespace ker::net::wki
