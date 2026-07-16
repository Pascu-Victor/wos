#pragma once

#include <cstddef>
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

auto persistent_batch_tile_count(size_t remaining_tiles, size_t regular_batch_size, size_t fine_grained_tail_threshold) -> size_t;

}  // namespace tracebench
