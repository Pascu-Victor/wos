#pragma once

// Host shim: perf instrumentation is a no-op in unit tests.

#include <cstdint>

namespace ker::mod::perf {

enum class WkiPerfScope : uint8_t {
    LOCAL_XFS = 11,
};

enum class WkiPerfLocalXfsOp : uint8_t {
    READ = 1,
    WRITE = 2,
    READ_BMAP = 3,
    READ_IO = 4,
    WRITE_BMAP = 5,
    WRITE_ALLOC = 6,
    WRITE_IO = 7,
    WRITE_ILOG = 8,
    WRITE_HOLE_ITER = 9,
    WRITE_MAP_ITER = 10,
    DIRECT_READ = 11,
    DIRECT_WRITE = 12,
    BUFFERED_READ = 13,
    BUFFERED_WRITE = 14,
    BUF_ALLOC = 15,
    BUF_READ_HIT = 16,
    BUF_READ_MISS = 17,
    BUF_GET_HIT = 18,
    BUF_GET_MISS = 19,
    BUF_DISK_READ = 20,
    BUF_DISK_WRITE = 21,
    BUF_DIRTY = 22,
    BUF_FLUSH = 23,
    BUF_DISCARD = 24,
    SYNC_BLOCKDEV = 25,
};

inline auto is_wki_scope_recording_enabled(WkiPerfScope, uint8_t) -> bool { return false; }
inline auto is_local_xfs_recording_enabled() -> bool { return false; }
inline void record_local_xfs_summary(WkiPerfLocalXfsOp, int32_t, uint32_t, bool, uint64_t) {}

}  // namespace ker::mod::perf
