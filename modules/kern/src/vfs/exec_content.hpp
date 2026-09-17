#pragma once

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <vfs/stat.hpp>

namespace ker::vfs {
// A private read view owns one backend pin until its File's final close.
struct ExecReadView {
    Stat metadata{};
    uint64_t content_id{};
};

// Caller holds the canonical inode's sleeping I/O lock. The id is global so
// stale page-cache entries cannot alias an inode or mount incarnation.
struct ExecContentState {
    uint64_t readers{};
    uint64_t content_id{};
    uint64_t direct_writers{};

    auto acquire(ExecReadView& view) -> int {
        if (direct_writers != 0) return -ETXTBSY;
        if (readers == UINT64_MAX) return -EOVERFLOW;
        if (readers == 0) {
            content_id = allocate_identity();
            if (content_id == 0) return -EOVERFLOW;
        }
        ++readers;
        view.content_id = content_id;
        return 0;
    }
    void release() { --readers; }
    auto can_mutate() const -> bool { return readers == 0 && direct_writers == 0; }

    // Remote views need a client-local identity distinct from every local
    // inode interval too. Zero indicates exhaustion; the counter never wraps.
    static auto allocate_identity() -> uint64_t {
        uint64_t next = next_id.load(std::memory_order_relaxed);
        do {
            if (next == UINT64_MAX) return 0;
        } while (!next_id.compare_exchange_weak(next, next + 1, std::memory_order_relaxed));
        return next;
    }

   private:
    static inline std::atomic<uint64_t> next_id{1};
};
}  // namespace ker::vfs
