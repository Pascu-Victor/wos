// WOS Kernel Coverage (KCOV) — Syzkaller-compatible coverage tracing.
//
// The compiler (-fsanitize-coverage=trace-pc) inserts a call to
// __sanitizer_cov_trace_pc() at every basic-block entry.  We record the
// return address (the PC of the instrumented block) into a per-task ring
// buffer.
//
// Userspace (syz-executor) enables coverage via a procfs node, executes
// syscalls, then reads the buffer to extract coverage PCs.  Syzkaller uses
// these to guide mutation.
//
// Gated behind cmake -DWOS_KCOV=ON.

#include <cstddef>
#include <cstdint>
#include <new>
#ifdef WOS_KCOV

#include <sanitizer/kcov.hpp>

namespace {

// Per-CPU KCOV buffer pointer.  In a full implementation this would be
// stored in the Task struct (per-task).  For the initial bring-up we use
// a single global buffer — sufficient for single-threaded syz-executor.
ker::sanitizer::kcov::KcovBuffer* s_current_buffer = nullptr;

}  // namespace

namespace ker::sanitizer::kcov {

void init() {
    // Nothing to do at boot — buffers are allocated on demand.
}

int alloc_buffer(size_t num_entries) {
    if (num_entries == 0 || num_entries > KCOV_MAX_ENTRIES) {
        num_entries = KCOV_MAX_ENTRIES;
    }

    auto* buf = new (std::nothrow) KcovBuffer{};
    if (buf == nullptr) {
        return -1;  // ENOMEM
    }

    buf->pcs = new (std::nothrow) uint64_t[num_entries];
    if (buf->pcs == nullptr) {
        delete buf;
        return -1;
    }

    buf->count = 0;
    buf->capacity = num_entries;
    buf->enabled = false;
    s_current_buffer = buf;
    return 0;
}

void enable() {
    if (s_current_buffer != nullptr) {
        s_current_buffer->enabled = true;
    }
}

void disable() {
    if (s_current_buffer != nullptr) {
        s_current_buffer->enabled = false;
    }
}

void reset() {
    if (s_current_buffer != nullptr) {
        s_current_buffer->count = 0;
    }
}

KcovBuffer* current_buffer() { return s_current_buffer; }

}  // namespace ker::sanitizer::kcov

// --------------------------------------------------------------------------
// Compiler callback — called at every basic-block entry
// --------------------------------------------------------------------------
// NOLINTNEXTLINE(readability-identifier-naming)
extern "C" void __sanitizer_cov_trace_pc() {
    auto* buf = s_current_buffer;
    if ((buf == nullptr) || !buf->enabled) {
        return;
    }

    // __builtin_return_address(0) gives us the PC of the call site,
    // which is the instrumented basic block.
    auto pc = reinterpret_cast<uint64_t>(__builtin_return_address(0));

    uint64_t const IDX = buf->count;
    if (IDX < buf->capacity) {
        buf->pcs[IDX] = pc;
        buf->count = IDX + 1;
    }
    // If buffer is full, silently drop (Syzkaller handles partial coverage)
}

#endif  // WOS_KCOV
