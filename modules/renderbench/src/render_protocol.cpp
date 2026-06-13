#include "render_protocol.hpp"

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

}  // namespace tracebench
