#pragma once

#include <cstdint>
#include <type_traits>

namespace ker::mod::sched {

// Stable scheduler transition boundaries understood by the lock-held
// run-queue validator.  The values are deliberately independent of Task and
// RunQueue so deterministic host tests can consume failure snapshots without
// pulling in kernel headers.
enum class TransitionPhase : uint8_t {
    STABLE = 0,
    PREPUBLICATION,
    HANDOFF_RESERVED,
    DETACHED_TRANSFER,
    EXITING_CURRENT,
    DEAD_ENQUEUE,
    GC_DETACH,
};

enum class TransitionInvariant : uint64_t {
    NONE = 0,
    HEAP_SIZE = 1ULL << 0,
    HEAP_NULL_ENTRY = 1ULL << 1,
    HEAP_INDEX = 1ULL << 2,
    HEAP_OWNER = 1ULL << 3,
    HEAP_QUEUE_TAG = 1ULL << 4,
    HEAP_UNPUBLISHED = 1ULL << 5,
    HEAP_IDLE = 1ULL << 6,
    HEAP_STATE = 1ULL << 7,
    WAIT_SCAN_BOUND = 1ULL << 8,
    WAIT_COUNT = 1ULL << 9,
    WAIT_OWNER = 1ULL << 10,
    WAIT_QUEUE_TAG = 1ULL << 11,
    WAIT_HEAP_OVERLAP = 1ULL << 12,
    WAIT_UNPUBLISHED = 1ULL << 13,
    WAIT_IDLE = 1ULL << 14,
    WAIT_STATE = 1ULL << 15,
    DEAD_SCAN_BOUND = 1ULL << 16,
    DEAD_COUNT = 1ULL << 17,
    DEAD_QUEUE_TAG = 1ULL << 18,
    DEAD_HEAP_OVERLAP = 1ULL << 19,
    DEAD_STATE = 1ULL << 20,
    DEAD_GC_FLAG = 1ULL << 21,
    DEAD_EPOCH = 1ULL << 22,
    DEAD_REFCOUNT = 1ULL << 23,
    DEAD_IDLE = 1ULL << 24,
    EEVDF_TOTAL_WEIGHT = 1ULL << 25,
    EEVDF_WEIGHTED_VRUNTIME = 1ULL << 26,
    CURRENT_OWNER = 1ULL << 27,
    HANDOFF_OWNER = 1ULL << 28,
    CURRENT_IDLE_MISMATCH = 1ULL << 29,
    HANDOFF_IDLE_MISMATCH = 1ULL << 30,
    RESERVED_DEAD = 1ULL << 31,
    IDLE_MEMBERSHIP = 1ULL << 32,
    LIST_MEMBERSHIP_CONFLICT = 1ULL << 33,
    DEAD_UNPUBLISHED = 1ULL << 34,
    CURRENT_UNPUBLISHED = 1ULL << 35,
    CURRENT_STATE = 1ULL << 36,
    HANDOFF_UNPUBLISHED = 1ULL << 37,
};

[[nodiscard]] constexpr auto transition_invariant_bit(TransitionInvariant invariant) -> uint64_t {
    return static_cast<uint64_t>(invariant);
}

enum class TransitionInspection : uint8_t {
    NONE = 0,
    WAIT_LIST_TRUNCATED = 1U << 0,
    DEAD_LIST_TRUNCATED = 1U << 1,
};

[[nodiscard]] constexpr auto transition_inspection_bit(TransitionInspection inspection) -> uint8_t {
    return static_cast<uint8_t>(inspection);
}

// Bounded, trivially copyable diagnostic payload.  Addresses are captured as
// integers and every pointed-to scalar is copied while the run-queue lock is
// held; consumers must never dereference an address from this snapshot.
struct TransitionFailure {
    uint64_t violations;
    uint64_t cpu;
    uint64_t task_address;
    uint64_t task_pid;
    uint64_t task_owner_cpu;
    uint64_t current_address;
    uint64_t handoff_address;
    uint64_t idle_address;
    uint64_t death_epoch;
    uint64_t observed_total_weight;
    uint64_t observed_total_weighted_vruntime;
    uint64_t expected_total_weight;
    uint64_t expected_total_weighted_vruntime;
    uint32_t heap_size;
    uint32_t wait_count;
    uint32_t dead_count;
    uint32_t wait_scanned;
    uint32_t dead_scanned;
    uint32_t task_ref_count;
    int32_t task_heap_index;
    uint8_t phase;
    uint8_t task_state;
    uint8_t task_queue;
    uint8_t task_published;
    uint8_t task_gc_queued;
    uint8_t current_is_handoff;
    uint8_t inspection_flags;
    uint8_t reserved[5];
};

static_assert(std::is_trivial_v<TransitionFailure>);
static_assert(std::is_standard_layout_v<TransitionFailure>);

// Validation-disabled builds return false and otherwise retain their default
// scheduler behavior.  Enabled builds expose the first captured failure for a
// CPU slot; callers should clear slots only from a quiescent test/debug path.
[[nodiscard]] auto read_scheduler_transition_failure(uint64_t cpu, TransitionFailure& out) -> bool;
void clear_scheduler_transition_failure(uint64_t cpu);

}  // namespace ker::mod::sched
