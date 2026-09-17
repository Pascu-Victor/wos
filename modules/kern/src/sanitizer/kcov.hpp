#pragma once

// WOS Kernel Coverage (KCOV) - raw PC traces and selftest hit bitmaps.
//
// When WOS_KCOV=1 the compiler inserts a call to __sanitizer_cov_trace_pc()
// at every basic-block entry. This module provides raw PC buffers for future
// userspace tracing and a bounded set of hit PCs for kernel selftests.
//
// Gated behind cmake -DWOS_KCOV=ON.  When WOS_KCOV is off, no coverage
// callbacks are emitted and selftests/panic dumps simply omit KCOV data.

#ifdef WOS_KCOV

#include <cstddef>
#include <cstdint>

namespace ker::sanitizer::kcov {

// Default PCs for callers that want a fixed-size buffer (8M entries = 64MB).
// Passing 0 to alloc_buffer() requests the largest practical buffer the
// current allocator state can satisfy while keeping a memory reserve.
constexpr size_t KCOV_DEFAULT_ENTRIES = static_cast<size_t>(1024) * 1024 * 8;

// The current collector is global, allocated before collection is enabled.
struct KcovBuffer {
    uint64_t* pcs = nullptr;  // Ring buffer of recorded PCs
    uint64_t count = 0;       // Number of PCs recorded so far
    uint64_t capacity = 0;    // Allocated size (in entries)
    bool enabled = false;     // Coverage collection active?
    bool truncated = false;   // At least one callback dropped a PC

    // Selftests need the set of hit PCs, not a trace of repeated visits.
    // Bitmap mode leaves pcs null and uses one bit per byte of kernel text.
    uint64_t* bitmap = nullptr;
    uint64_t text_start = 0;
    uint64_t text_bytes = 0;
    uint64_t active_writers = 0;
};

// Initialize the KCOV subsystem (called from kernel init)
void init();

// Allocate a KCOV buffer for the current task.
// Returns 0 on success, negative errno on failure.
int alloc_buffer(size_t num_entries);

// Allocate a zeroed text bitmap before enabling selftest collection.
// Control operations are serialized by the caller. Published buffers remain
// alive for the boot lifetime, including callbacks racing with disable().
int alloc_text_bitmap(uintptr_t text_start, uintptr_t text_end);

// Enable/disable coverage collection for the current task.
// disable() drains bitmap writers; call from the selftest control path, never
// from inside a coverage callback. count then holds the number of unique PCs.
void enable();
void disable();

// Reset the buffer (clear recorded PCs, keep allocation)
void reset();

// Get the current task's KCOV buffer (nullptr if none allocated)
KcovBuffer* current_buffer();

#ifdef WOS_KCOV_PANIC_TRACE
// Dump a small recent-PC ring for the given CPU to the panic log.
void dump_panic_trace_for_cpu(uint64_t cpu_id);
#endif

}  // namespace ker::sanitizer::kcov

#endif  // WOS_KCOV
