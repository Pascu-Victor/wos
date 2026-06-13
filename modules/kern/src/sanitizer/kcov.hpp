#pragma once

// WOS Kernel Coverage (KCOV) - per-task PC ring buffer for Syzkaller.
//
// When WOS_KCOV=1 the compiler inserts a call to __sanitizer_cov_trace_pc()
// at every basic-block entry.  This module records those PCs into a per-task
// ring buffer that userspace can read via a procfs node (/proc/kcov).
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

// Per-task KCOV buffer.  Allocated lazily when userspace enables coverage
// collection for a task via ioctl/procfs.
struct KcovBuffer {
    uint64_t* pcs = nullptr;  // Ring buffer of recorded PCs
    uint64_t count = 0;       // Number of PCs recorded so far
    uint64_t capacity = 0;    // Allocated size (in entries)
    bool enabled = false;     // Coverage collection active?
    bool truncated = false;   // At least one callback dropped a PC
};

// Initialize the KCOV subsystem (called from kernel init)
void init();

// Allocate a KCOV buffer for the current task.
// Returns 0 on success, negative errno on failure.
int alloc_buffer(size_t num_entries);

// Enable/disable coverage collection for the current task.
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
