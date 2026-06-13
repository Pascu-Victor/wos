#pragma once

#include <cstdint>
#include <span>

namespace tracebench {

enum class WorkerTileDecision : uint8_t {
    Accepted,
    Duplicate,
    Foreign,
    OutOfRange,
};

auto decide_worker_tile(uint32_t tile_index, int worker_id, std::span<unsigned char> tile_seen, std::span<const int> tile_owner)
    -> WorkerTileDecision;

}  // namespace tracebench
