#include "render_protocol.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>

namespace tracebench {

auto decide_worker_tile(uint32_t tile_index, int worker_id, std::span<unsigned char> tile_seen, std::span<const int> tile_owner)
    -> WorkerTileDecision {
    if (tile_index >= tile_seen.size() || tile_index >= tile_owner.size()) {
        return WorkerTileDecision::OutOfRange;
    }

    auto const INDEX = static_cast<size_t>(tile_index);
    int const OWNER = tile_owner[INDEX];
    if (OWNER >= 0 && OWNER != worker_id) {
        return WorkerTileDecision::Foreign;
    }

    auto& seen = tile_seen[INDEX];
    if (seen != 0) {
        return WorkerTileDecision::Duplicate;
    }

    seen = 1;
    return WorkerTileDecision::Accepted;
}

auto persistent_batch_tile_count(size_t remaining_tiles, size_t regular_batch_size, size_t fine_grained_tail_threshold) -> size_t {
    if (remaining_tiles == 0U) {
        return 0U;
    }

    size_t const REGULAR_COUNT = std::min(std::max(size_t{1}, regular_batch_size), remaining_tiles);
    if (fine_grained_tail_threshold == 0U) {
        return REGULAR_COUNT;
    }
    if (remaining_tiles <= fine_grained_tail_threshold) {
        return 1U;
    }
    return std::min(REGULAR_COUNT, remaining_tiles - fine_grained_tail_threshold);
}

}  // namespace tracebench
