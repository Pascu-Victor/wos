#pragma once

#ifdef WOS_SELFTEST
#include <cstdint>

namespace ker::mod::mm::lifecycle_storage {

// Heap allocation blocks, not constructed elements. Embedded/placement objects
// belong to their enclosing allocator and are outside this typed-storage census.
enum class Kind : uint8_t { TASK, FILE, FAMILY, BINDING };

struct Counts {
    uint64_t live_blocks{};  // Includes destruction and the entire real free.
    uint64_t allocating{};   // Incremented before entering the real allocator.
};

struct Snapshot {
    Counts task;
    Counts file;
    Counts family;
    Counts binding;
    uint64_t sequence{};
    bool valid{true};  // Sticky false on impossible arithmetic; never reset.
};

// Internal hooks only. No counter lock is retained across allocation, free,
// object destruction, owner locks, callbacks, I/O or logging.
void begin_allocation(Kind kind) noexcept;
void finish_allocation(Kind kind, bool allocated) noexcept;
void finish_deallocation(Kind kind) noexcept;

// Owns no Task/File/object references; equality is only one part of the
// controller's quiescent bracket, together with actual RETURNED/executor debt.
auto snapshot() noexcept -> Snapshot;

}  // namespace ker::mod::mm::lifecycle_storage
#endif
